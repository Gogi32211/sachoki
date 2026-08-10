"""One door to the data — phase 1 of the Analytic Studio.

The bug of 2026-08-09 was not statistical. `edge_replay._pull` deduplicated the bars table
(one row per index membership, 39.6% of all rows) and excluded ETFs; `NakedStudy`, written
the same day, did neither. Two paths to the same data with different contracts, and the
second one silently made `shift(-1)` return the SAME bar. Everything downstream — clustered
intervals, effective-n, matched controls, mined/OOS — validated arithmetic on a frame that
described the wrong world.

So the fix is not a third path. It is one door, and a rule that every source must walk
through it declaring four things:

    grain          what makes a row unique — the thing dedup is defined against
    time_key       the observation date
    public_key     the moment the information became KNOWABLE, which is not the same
                   thing: a Form 4 trade happens 65 days before it is filed, a quarter
                   ends 36 days before it is reported
    primitives     an allowlist, because the bars table has 415 columns and most of them
                   store a conclusion of ours rather than a property of the bar

Sparse sources (earnings, insider filings, XBRL facts) never get joined on their event date.
They are merged as-of the PUBLIC key, backwards only, so a value cannot appear on a bar
before the day it could have been read. That is a property of the join, not of anyone's
discipline.

    from sources import bars, attach, describe
    df = bars("1d", min_price=5, min_dollar_vol=3_000_000, columns=("t_sig", "z_sig"))
    df = attach(df, "earnings")      # days_since / days_until, from filing dates
    describe()                        # what is connected, its contract, its freshness
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

import duckdb
import numpy as np
import pandas as pd

from data_contract import ContractError, assert_contract

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "..", "data")
DB = {"1d": "studio_analytics.duckdb", "1w": "studio_1w.duckdb", "4h": "studio_4h.duckdb",
      "1h": "studio_1h.duckdb", "15m": "studio_15m.duckdb"}
JOURNAL = os.path.join(DATA, "ai_journal.duckdb")


@dataclass(frozen=True)
class Contract:
    """What a source promises. Nothing loads without one."""
    name: str
    grain: tuple                    # uniqueness key — dedup is defined against this
    time_key: str                   # when it was observed
    public_key: str                 # when it became knowable (may equal time_key)
    primitives: frozenset           # allowlist of loadable columns
    forbidden: dict = field(default_factory=dict)   # column -> why it is refused
    has_delisted: bool | None = None
    note: str = ""

    def check(self, columns: tuple) -> None:
        bad = {c: self.forbidden[c] for c in columns if c in self.forbidden}
        if bad:
            raise ContractError(f"{self.name}: refused columns — "
                                + "; ".join(f"{k} ({v})" for k, v in bad.items()))
        unknown = [c for c in columns if c not in self.primitives]
        if unknown:
            raise ContractError(
                f"{self.name}: {unknown} are not primitives. A primitive describes what the "
                f"row IS; scores, tiers, zones, book setups and our own forward labels are "
                f"conclusions from earlier research. Add one deliberately, in a commit, with "
                f"the reason.")


# ── bars ─────────────────────────────────────────────────────────────────────
BAR_PRIMITIVES = frozenset({
    # mechanical labels — the tokens a bar carries, no scoring attached
    "t_sig", "z_sig", "l_sig", "g_sig", "b_sig", "fly_sig", "vol_sig", "combo_sig",
    "ne_suffix", "wick_suffix", "penetration_suffix", "close_suffix", "full_suffix",
    "bar_body_wick", "bar_gap_range", "bar_gap_class", "bar_range_class",
    "setup_tokens", "context_tokens", "swing_type", "swing_type_3", "swing_type_5",
    # standard public indicators — not ours, and no threshold implied
    "rsi_14", "cci_20", "atr_14", "avg_vol_20d", "change_pct",
    # plain facts about the instrument
    "sector", "universe",
})
BAR_FORBIDDEN = {
    **{c: "a score we fitted — using it as an input tests our old ideas, not the market"
       for c in ("turbo_score", "ultra_score", "ultra_score_v3", "buy_score", "gog_score",
                 "beta_score", "profile_score", "rtb_total", "prebreak_score", "aes_score",
                 "final_bull_score")},
    **{c: "a tier/zone we assigned" for c in ("gog_tier", "beta_zone", "rtb_phase",
                                              "mcap_bucket", "prebreak_v2_band",
                                              "acc_exit_class", "aes_stage", "final_regime")},
    **{c: "OUR forward label — bakes in a horizon this study should re-make from OHLC"
       for c in ("fwd_1d", "fwd_3d", "fwd_5d", "fwd_10d", "fwd_20d", "fwd_30d", "fwd_60d",
                 "fwd_90d", "mfe_5d", "mfe_10d", "mfe_20d", "mfe_30d", "mfe_60d",
                 "mae_5d", "mae_10d", "mae_20d", "mae_30d", "fwd_swing_ret_3",
                 "fwd_swing_ret_5", "fwd_swing_bars_3", "fwd_swing_bars_5",
                 "pct_to_next_hl_3", "pct_to_next_hh_3", "pct_to_next_hl_5",
                 "pct_to_next_hh_5", "bars_to_next_hl_3", "bars_to_next_hh_3",
                 "bars_to_next_hl_5", "bars_to_next_hh_5", "acc_exit_in_n",
                 "next_pivot_is_hl_3", "next_pivot_is_hh_3", "next_pivot_is_hl_5",
                 "next_pivot_is_hh_5")},
}

BARS = Contract(
    name="bars", grain=("ticker", "date"), time_key="date", public_key="date",
    primitives=BAR_PRIMITIVES | {"ticker", "date", "open", "high", "low", "close", "volume"},
    forbidden=BAR_FORBIDDEN, has_delisted=False,
    note="delisting is NOT represented: 0.7% of tickers end early against an expected "
         "5-10%, so 367-719 names are missing and every worst-year reads high.")

_CACHE: dict = {}


def bars(tf: str = "1d", *, columns: tuple = (), start: str | None = None,
         end: str | None = None, min_price: float | None = None,
         min_dollar_vol: float | None = None, require_adjacency: float | None = None,
         verbose: bool = True) -> pd.DataFrame:
    """Bars for one timeframe, deduplicated, contract-asserted, with prev_ok attached.

    Filters that came from research are allowed but never silent: pass them explicitly and
    they are recorded on the frame's `.attrs` so a study can print what it inherited.
    """
    BARS.check(tuple(columns))
    key = (tf, tuple(columns), start, end, min_price, min_dollar_vol)
    if key in _CACHE:
        return _CACHE[key].copy()
    if tf not in DB:
        raise ContractError(f"unknown timeframe {tf!r} — have {list(DB)}")

    sel = ", ".join(("ticker", "date", "open", "high", "low", "close", "volume")
                    + tuple(columns))
    where = ["universe <> 'index'"]           # ETFs and indices are not stocks
    if start:
        where.append(f"date >= DATE '{start}'")
    if end:
        where.append(f"date <= DATE '{end}'")
    if min_price:
        where.append(f"close >= {min_price}")
    if min_dollar_vol:
        where.append(f"close*volume >= {min_dollar_vol}")
    # ONE ROW PER GRAIN. The table stores a row per index membership, so a name in three
    # universes appears three times with identical OHLCV — and sorted by (ticker, date)
    # those copies are adjacent, which is what made shift(-1) return the same bar.
    q = (f"SELECT {sel} FROM (SELECT *, row_number() OVER "
         f"(PARTITION BY ticker, date ORDER BY universe) rn FROM bars "
         f"WHERE {' AND '.join(where)}) WHERE rn = 1 ORDER BY ticker, date")
    con = duckdb.connect(os.path.join(DATA, DB[tf]), read_only=True)
    df = con.execute(q).fetch_df()
    con.close()

    df["_dt"] = pd.to_datetime(df["date"])
    gapd = df.groupby("ticker", sort=False)["_dt"].diff().dt.days
    # A liquidity floor deletes bars from the MIDDLE of a history, so the previous ROW can
    # be months from the previous BAR. Sequences must use this, never a bare shift.
    df["prev_ok"] = (gapd <= (4 if tf in ("1d", "1w") else 1)).fillna(False)
    df.attrs["source"] = "bars"
    df.attrs["tf"] = tf
    df.attrs["filters"] = {k: v for k, v in
                           dict(min_price=min_price, min_dollar_vol=min_dollar_vol,
                                start=start, end=end).items() if v}
    assert_contract(df, name=f"bars[{tf}]", require_adjacency=require_adjacency,
                    verbose=verbose)
    _CACHE[key] = df
    return df.copy()


# ── sparse sources: joined as-of the PUBLIC key, never the event date ────────
EARNINGS = Contract(
    name="earnings", grain=("ticker", "filing_date"), time_key="filing_date",
    public_key="filing_date",
    primitives=frozenset({"ticker", "filing_date"}), has_delisted=False,
    note="SEC filingDate of 10-Q / 10-K / 8-K item 2.02 — the day it became public, which "
         "is what makes it usable. 3,532 tickers, 120,721 dates.")

INSIDER = Contract(
    name="insider", grain=("accession", "ticker", "insider", "tx_date"),
    time_key="tx_date", public_key="filed_date",
    primitives=frozenset({"ticker", "filed_date", "code", "acq_disp", "shares", "value",
                          "title"}),
    forbidden={"tx_date": "the trade date is not public until filed_date — median lag 65 "
                          "days. Join on filed_date; tx_date is available inside the row "
                          "for description only."},
    has_delisted=None,
    note="Form 4. 65,004 rows, 3,318 tickers, but filed_date spans only ~2.5 months — "
         "there is almost no history here yet.")

META = Contract(
    name="meta", grain=("ticker",), time_key="updated_at", public_key="updated_at",
    primitives=frozenset({"ticker", "sector", "industry", "sic_code", "employees"}),
    forbidden={"market_cap": "a SNAPSHOT with no history — using it on past bars is "
                             "lookahead and survivorship at once",
               "mcap_bucket": "derived from that snapshot — same fault",
               "updated_at": "not a property of any bar"},
    has_delisted=False, note="4,931 tickers. Static attributes only.")

REGISTRY = {c.name: c for c in (BARS, EARNINGS, INSIDER, META)}


def _earnings_events() -> pd.DataFrame:
    p = os.path.join(DATA, "earnings_dates.json")
    if not os.path.exists(p):
        raise ContractError("earnings_dates.json missing — run earnings_feed.refresh()")
    d = json.load(open(p))
    rows = [(t, dt) for t, ds in d.items() for dt in (ds or [])]
    e = pd.DataFrame(rows, columns=["ticker", "filing_date"])
    e["filing_date"] = pd.to_datetime(e["filing_date"])
    return e.sort_values(["ticker", "filing_date"], ignore_index=True)


def _insider_events() -> pd.DataFrame:
    con = duckdb.connect(JOURNAL, read_only=True)
    e = con.execute("""SELECT ticker, cast(filed_date AS DATE) filed_date, code, acq_disp,
                              shares, value FROM insider_tx WHERE filed_date IS NOT NULL""",
                    ).fetch_df()
    con.close()
    e["filed_date"] = pd.to_datetime(e["filed_date"])
    return e.sort_values(["ticker", "filed_date"], ignore_index=True)


_EVENTS = {"earnings": (_earnings_events, "filing_date"),
           "insider": (_insider_events, "filed_date")}


def attach(df: pd.DataFrame, source: str, *, verbose: bool = True) -> pd.DataFrame:
    """Merge a sparse source onto bars AS-OF its public key, backwards only.

    Backwards-only is the whole point: a row can carry the most recent filing that had
    already happened, and can never see the next one. `days_since` is therefore always
    ≥ 0, and any negative value would mean the join leaked — which is asserted below
    rather than trusted.
    """
    if source not in _EVENTS:
        raise ContractError(f"no sparse source {source!r} — have {list(_EVENTS)}")
    builder, pub = _EVENTS[source]
    ev = builder().rename(columns={pub: "_pub"})
    con = REGISTRY[source]
    # DuckDB hands back datetime64[us] while the json path builds [ns]; merge_asof refuses
    # to compare them, and a silent cast in either direction is exactly the kind of quiet
    # coercion this module exists to avoid — so both sides are pinned explicitly.
    left = df.copy()
    left["_dt"] = left["_dt"].astype("datetime64[ns]")
    ev["_pub"] = ev["_pub"].astype("datetime64[ns]")
    out = pd.merge_asof(
        left.sort_values("_dt", kind="stable"),
        ev.sort_values("_pub", kind="stable"),
        left_on="_dt", right_on="_pub", by="ticker", direction="backward",
        suffixes=("", f"_{source}"))
    out[f"{source}_days_since"] = (out["_dt"] - out["_pub"]).dt.days
    leak = (out[f"{source}_days_since"] < 0).sum()
    if leak:
        raise ContractError(f"{source}: {leak:,} rows carry a value published AFTER the bar "
                            f"— the as-of join leaked")
    out = out.drop(columns=["_pub"]).sort_values(["ticker", "_dt"], ignore_index=True)
    if verbose:
        cov = out[f"{source}_days_since"].notna().mean()
        print(f"  ── attached {source} (as-of {con.public_key}) — {cov:.1%} of bars carry "
              f"a prior event · median age "
              f"{out[f'{source}_days_since'].median():.0f} days", flush=True)
    return out


# ── what is connected ────────────────────────────────────────────────────────
def describe() -> pd.DataFrame:
    rows = []
    print("=" * 112, flush=True)
    print("  SOURCES", flush=True)
    print("=" * 112, flush=True)
    for name, c in REGISTRY.items():
        fresh, size = "", ""
        try:
            if name == "bars":
                con = duckdb.connect(os.path.join(DATA, DB["1d"]), read_only=True)
                r = con.execute("SELECT max(date), count(DISTINCT ticker) FROM bars").fetchone()
                con.close()
                fresh, size = str(r[0])[:10], f"{r[1]:,} tickers"
            elif name == "earnings":
                e = _earnings_events()
                fresh, size = str(e.filing_date.max())[:10], f"{len(e):,} filings"
            elif name == "insider":
                e = _insider_events()
                fresh, size = str(e.filed_date.max())[:10], f"{len(e):,} filings"
            elif name == "meta":
                con = duckdb.connect(JOURNAL, read_only=True)
                r = con.execute("SELECT count(*), max(updated_at) FROM ticker_meta").fetchone()
                con.close()
                fresh, size = str(r[1])[:10], f"{r[0]:,} tickers"
        except Exception as e:                       # a missing source is a fact, not a crash
            fresh, size = "—", f"unavailable ({str(e)[:40]})"
        print(f"\n  {name:10s} {size:>22s}   fresh to {fresh}")
        print(f"    grain      {c.grain}")
        print(f"    observed   {c.time_key}    public   {c.public_key}"
              + ("   ← differs: never join on the event date"
                 if c.time_key != c.public_key else ""))
        print(f"    delisted   {'yes' if c.has_delisted else 'NO' if c.has_delisted is False else 'unknown'}")
        print(f"    primitives {len(c.primitives)} · forbidden {len(c.forbidden)}")
        if c.note:
            print(f"    note       {c.note}")
        rows.append(dict(source=name, size=size, fresh=fresh, grain=c.grain,
                         time_key=c.time_key, public_key=c.public_key,
                         n_primitives=len(c.primitives), n_forbidden=len(c.forbidden)))
    print("\n" + "=" * 112, flush=True)
    print("  NOT YET CONNECTED: facts (SEC XBRL) — dilution, cash, debt, burn. Phase 2.",
          flush=True)
    print("=" * 112, flush=True)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    describe()
