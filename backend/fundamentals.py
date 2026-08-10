"""Reading the facts table: as-of, and nothing else.

The table is append-only, so a restatement is an extra row rather than a correction — on the
first twelve tickers ingested, 1,880 (concept, period) combinations already carried more than
one filing. That is 21% of them. Storing "the current value" would have been wrong in one
cell out of five, and wrong in the direction that always flatters: a company's numbers get
restated when the first version was optimistic.

So there is exactly one read path:

    as_of(ticker, concept, decision_time) → the latest value whose filed <= decision_time

Every other access raises. `facts()` returns the raw table only under `raw=True`, which the
study layer never passes; the point is that a study cannot accidentally join on `period_end`
and quietly gain 30-45 days of hindsight.

Derived series (dilution, runway, burn) are computed FROM as_of and never materialised onto
bars. The moment a per-bar `cash_runway` column exists, the point-in-time decision that made
it is invisible to whoever reads the column next.
"""
from __future__ import annotations

import os

import duckdb
import numpy as np
import pandas as pd

from data_contract import ContractError

FDB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                   "fundamentals.duckdb")

CONCEPTS = ("shares_outstanding", "shares_outstanding_bs", "shares_wavg", "cash",
            "short_investments", "assets_current", "liabilities_current", "liabilities",
            "debt_lt", "equity", "cfo", "operating_income")

# A quarterly series carries ~20 observations in five years. A study that treats each of the
# 1,260 bars it touches as an independent fact is not measuring what it thinks.
MIN_FILINGS = 8


def _con():
    if not os.path.exists(FDB):
        raise ContractError("fundamentals.duckdb missing — run sec_xbrl_feed.refresh()")
    return duckdb.connect(FDB, read_only=True)


def facts(*, raw: bool = False, **kw) -> pd.DataFrame:
    if not raw:
        raise ContractError(
            "direct access to `facts` is refused. Use as_of(): the table holds every "
            "revision of every figure, so a plain join silently mixes what was known with "
            "what was learned later. raw=True exists for ingest inspection only.")
    con = _con()
    df = con.execute("SELECT * FROM facts").fetch_df()
    con.close()
    return df


def _load(concept: str, unit: str | None = None) -> pd.DataFrame:
    if concept not in CONCEPTS:
        raise ContractError(f"unknown concept {concept!r} — have {list(CONCEPTS)}")
    con = _con()
    f = con.execute(
        "SELECT ticker, cast(filed AS DATE) filed, cast(period_end AS DATE) period_end, "
        f"val, unit FROM facts WHERE concept = '{concept}'"
        + (f" AND unit = '{unit}'" if unit else "")).fetch_df()
    con.close()
    if f.empty:
        return f
    if unit is None and f["unit"].nunique() > 1:
        keep = f["unit"].value_counts().idxmax()      # never mix scales silently
        f = f[f["unit"] == keep]
    f["filed"] = pd.to_datetime(f["filed"]).astype("datetime64[ns]")
    f["period_end"] = pd.to_datetime(f["period_end"]).astype("datetime64[ns]")
    return f


def as_of(tickers, concept: str, dates, *, unit: str | None = None) -> pd.Series:
    """The most recently REPORTED value a reader could have had on each date.

    A single filing carries several periods — the quarter being reported and the prior-year
    comparative — so "the latest filing" is not enough to identify a number. DJCO's 10-Q of
    2024-05-15 contains cash for BOTH 2024-03-31 ($10.56M) and 2023-09-30 ($20.84M), and an
    earlier version of this function kept whichever row happened to sort last. It returned
    values that belonged to no period anyone had asked about, and the known-answer test
    caught it on the first run.

    Two questions were being conflated, and both are legitimate:

        as_of()         what is the company's latest reported figure, as known on D
                        → the FEATURE. Frontier period, latest vintage of it.
        as_of_period()  what was believed about a SPECIFIC period on D
                        → the AUDIT. Used to verify vintages, not to build features.

    Here the frontier is a running maximum of period_end over filings ordered by `filed`, so
    a later filing that restates an OLDER period updates that period's vintage without
    dragging the company's reported position backwards in time.
    """
    f = _load(concept, unit)
    left = pd.DataFrame({"ticker": pd.Series(tickers).astype(str).to_numpy(),
                         "_dt": pd.to_datetime(pd.Series(dates)).astype("datetime64[ns]")})
    if f.empty:
        return pd.Series(np.nan, index=left.index, name=concept)

    f = f.sort_values(["ticker", "filed", "period_end"], kind="stable")
    frontier = f.groupby("ticker", sort=False)["period_end"].cummax()
    f = f[f["period_end"].to_numpy() >= frontier.to_numpy()]          # only the frontier
    f = f.drop_duplicates(["ticker", "filed"], keep="last")           # newest period wins

    out = pd.merge_asof(left.sort_values("_dt", kind="stable"),
                        f.sort_values("filed", kind="stable"),
                        left_on="_dt", right_on="filed", by="ticker", direction="backward")
    leak = (out["filed"] > out["_dt"]).sum()
    if leak:
        raise ContractError(f"as_of({concept}): {leak:,} rows resolved to a filing dated "
                            f"after the decision — the join leaked")
    return out.sort_index()["val"].rename(concept)


def as_of_period(tickers, concept: str, period_end, dates,
                 *, unit: str | None = None) -> pd.Series:
    """What was believed about ONE period on each date — the vintage, not the frontier.

    762,851 facts in the table are re-filings of a period already reported, so this is the
    function that makes the append-only design checkable: the same period asked on two dates
    must return the two different numbers that were current then.
    """
    f = _load(concept, unit)
    left = pd.DataFrame({"ticker": pd.Series(tickers).astype(str).to_numpy(),
                         "pe": pd.to_datetime(pd.Series(period_end)).astype("datetime64[ns]"),
                         "_dt": pd.to_datetime(pd.Series(dates)).astype("datetime64[ns]")})
    if f.empty:
        return pd.Series(np.nan, index=left.index, name=concept)
    f = (f.sort_values(["ticker", "period_end", "filed"], kind="stable")
           .drop_duplicates(["ticker", "period_end", "filed"], keep="last")
           .rename(columns={"period_end": "pe"}))
    # merge_asof takes `by` OR (left_by, right_by), never both — so the right side is
    # renamed to share the key name rather than passing two spellings of it
    out = pd.merge_asof(left.sort_values("_dt", kind="stable"),
                        f.sort_values("filed", kind="stable"),
                        left_on="_dt", right_on="filed", by=["ticker", "pe"],
                        direction="backward")
    return out.sort_index()["val"].rename(concept)


def coverage(concept: str | None = None) -> pd.DataFrame:
    con = _con()
    q = """SELECT concept, count(*) n, count(DISTINCT ticker) tickers,
                  min(filed) first_filed, max(filed) last_filed,
                  count(DISTINCT unit) units FROM facts """
    if concept:
        q += f"WHERE concept = '{concept}' "
    q += "GROUP BY 1 ORDER BY 2 DESC"
    df = con.execute(q).fetch_df()
    rest = con.execute("""SELECT count(*) FROM (SELECT ticker, concept, period_end,
                          count(DISTINCT accn) k FROM facts GROUP BY 1,2,3 HAVING k>1)"""
                       ).fetchone()[0]
    tot = con.execute("""SELECT count(*) FROM (SELECT DISTINCT ticker, concept, period_end
                         FROM facts)""").fetchone()[0]
    con.close()
    print(f"  restated (period reported more than once): {rest:,} of {tot:,} "
          f"({rest / max(tot, 1):.1%}) — kept as separate rows", flush=True)
    return df


# ── derived series: computed from as_of, never stored per bar ────────────────
def dilution_yoy(tickers, dates) -> pd.Series:
    """Year-on-year growth in shares outstanding, as it was knowable on each date.

    This is the variable the price buckets were standing in for. A stock at $8-21 carries a
    1.50% chance of a >25% five-bar drawdown against 0.59% at $21-89, and price is a proxy;
    the reverse splits that dominated the catastrophe tail (LICN, AIRE, SOBR, FLYE, SNTG)
    are the visible end of a dilution that was reported quarters earlier.
    """
    d = pd.to_datetime(pd.Series(dates))
    now = as_of(tickers, "shares_outstanding", d)
    if now.isna().mean() > 0.5:                       # dei tag is patchy on smaller filers
        now = now.fillna(as_of(tickers, "shares_outstanding_bs", d))
    year_ago = as_of(tickers, "shares_outstanding", d - pd.Timedelta(days=365))
    if year_ago.isna().mean() > 0.5:
        year_ago = year_ago.fillna(as_of(tickers, "shares_outstanding_bs",
                                         d - pd.Timedelta(days=365)))
    out = (now / year_ago - 1) * 100
    return out.replace([np.inf, -np.inf], np.nan).rename("dilution_yoy")


def cash_runway_quarters(tickers, dates) -> pd.Series:
    """Quarters of cash at the current burn. Positive cash flow returns inf, mapped to NaN.

    Deliberately crude: cash + short-term investments over the quarterly operating outflow.
    A precise runway would need guidance we do not have, and a fragile balance sheet does not
    need three decimals to be recognised.
    """
    d = pd.to_datetime(pd.Series(dates))
    cash = as_of(tickers, "cash", d).fillna(0) + as_of(tickers, "short_investments",
                                                       d).fillna(0)
    cfo = as_of(tickers, "cfo", d)
    burn = -cfo.where(cfo < 0)                        # only burners have a runway
    out = cash / burn
    return out.replace([np.inf, -np.inf], np.nan).clip(upper=99).rename("cash_runway_q")


def leverage(tickers, dates) -> pd.Series:
    d = pd.to_datetime(pd.Series(dates))
    liab = as_of(tickers, "liabilities", d)
    eq = as_of(tickers, "equity", d)
    out = liab / eq.where(eq > 0)
    return out.replace([np.inf, -np.inf], np.nan).rename("liab_over_equity")


DERIVED = {"dilution_yoy": dilution_yoy, "cash_runway_q": cash_runway_quarters,
           "liab_over_equity": leverage}


def attach(df: pd.DataFrame, series: tuple = ("dilution_yoy",), *,
           verbose: bool = True) -> pd.DataFrame:
    """Add derived fundamental series to a bars frame, resolved as-of each bar's date."""
    if "_dt" not in df or "ticker" not in df:
        raise ContractError("expects a frame from sources.bars()")
    out = df.copy()
    for s in series:
        if s not in DERIVED:
            raise ContractError(f"unknown series {s!r} — have {list(DERIVED)}")
        out[s] = DERIVED[s](out["ticker"], out["_dt"]).to_numpy()
        if verbose:
            v = out[s].dropna()
            print(f"  ── {s}: {len(v) / len(out):.1%} of bars covered · "
                  f"median {v.median():+.2f} · p10 {v.quantile(.10):+.2f} · "
                  f"p90 {v.quantile(.90):+.2f}", flush=True)
    return out
