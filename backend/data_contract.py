"""The layer none of the statistics could reach.

On 2026-08-09 every statistical guard passed on a study that was measuring nothing. Clustered
intervals, effective-n, matched controls, a noise reference, a mined/OOS split — all of them
validated numbers that were internally consistent and described a world that did not exist.
The bars table carries one row per index membership, so a name in three universes appears
three times with identical OHLCV (39.6% of all rows). Sorted by (ticker, date) those copies
sit next to each other, and `shift(-1)` returned the SAME bar. "Next open" became this bar's
open, so gap = O/C−1 and the following return = C/O−1 were exact mirrors by construction —
which is what produced a 73%/26% asymmetry that looked like a market law and was arithmetic.

A second, quieter one rode along: a liquidity floor deletes bars from the middle of a ticker's
history, so the previous ROW can be months from the previous DAY. A "three-bar sequence" read
off rows was, for illiquid names, three quarters apart. The user found this by opening one
chart and seeing Z10 → T5 → T6 where the code had written T2 → T2 → T6.

Neither is a statistical error, so no statistical test can catch either. What catches them is
a contract on the frame and an independent recomputation of a sample straight from the source.

Two entry points:

    assert_contract(df)                 — hard checks; raises before any analysis runs
    verify_sample(df, mask, ...)        — re-derives a handful of hits from raw DuckDB and
                                          prints the surrounding bars, so a claim can be
                                          checked against a chart in ten seconds

The second one is the important one. It is the only guard here that would have caught BOTH
bugs, because it does not trust the frame at all.
"""
from __future__ import annotations

import os

import duckdb
import numpy as np
import pandas as pd

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                  "studio_analytics.duckdb")


class ContractError(AssertionError):
    """A frame that violates the contract cannot produce a meaningful number."""


def _con():
    return duckdb.connect(DB, read_only=True)


def assert_contract(df: pd.DataFrame, *, name: str = "frame",
                    require_adjacency: float | None = None,
                    verbose: bool = True) -> dict:
    """Hard checks that must hold before a frame is analysed.

    require_adjacency: if set, the share of rows whose previous row is also the previous
    trading day must reach it. Leave None when the study never looks at a neighbouring row;
    set it (0.95+) for anything that builds sequences, gaps or next-bar entries.
    """
    rep, fail = {}, []

    n = len(df)
    rep["rows"] = n
    if n == 0:
        raise ContractError(f"{name}: empty frame")

    # 1 — one row per bar. The failure that started all of this.
    if {"ticker", "date"} <= set(df.columns):
        dup = n - len(df[["ticker", "date"]].drop_duplicates())
        rep["duplicate_bars"] = dup
        if dup:
            fail.append(f"{dup:,} duplicate (ticker,date) rows ({dup / n:.1%}). The bars "
                        f"table has one row per index membership — dedupe with "
                        f"row_number() OVER (PARTITION BY ticker,date ORDER BY universe). "
                        f"Adjacent copies make shift(-1) return the SAME bar.")

    # 2 — dates sorted within each ticker, or every shift is meaningless
    if {"ticker", "date"} <= set(df.columns):
        d = pd.to_datetime(df["date"])
        back = (d.groupby(df["ticker"], sort=False).diff().dt.days < 0).sum()
        rep["backwards_steps"] = int(back)
        if back:
            fail.append(f"{back:,} rows go BACKWARDS in time within a ticker — sort by "
                        f"(ticker, date) before anything else")

    # 3 — index/ETF rows are not stocks
    if "universe" in df.columns:
        idx = int((df["universe"] == "index").sum())
        rep["index_rows"] = idx
        if idx:
            fail.append(f"{idx:,} rows from universe='index' — ETFs and indices must not "
                        f"enter stock statistics")

    # 4 — row adjacency vs calendar adjacency
    if {"ticker", "date"} <= set(df.columns):
        d = pd.to_datetime(df["date"])
        gapd = d.groupby(df["ticker"], sort=False).diff().dt.days
        adj = float((gapd <= 4).mean())
        rep["calendar_adjacent"] = adj
        rep["median_row_gap_days"] = float(gapd.median()) if len(gapd.dropna()) else np.nan
        if require_adjacency is not None and adj < require_adjacency:
            fail.append(f"only {adj:.2%} of rows follow the previous TRADING DAY (need "
                        f"{require_adjacency:.0%}). A filter has deleted bars from the "
                        f"middle of histories, so 'previous row' is not 'previous bar'. "
                        f"Use the prev_ok flag instead of assuming adjacency.")

    # 5 — unadjusted corporate actions, which masquerade as catastrophic losses
    if {"ticker", "close"} <= set(df.columns):
        ch = df.groupby("ticker", sort=False)["close"].pct_change()
        wild = int((ch.abs() > 0.45).sum())
        rep["bars_over_45pct"] = wild
        rep["bars_over_45pct_share"] = wild / n
        if wild / n > 0.002:
            fail.append(f"{wild:,} bars move more than 45% ({wild / n:.2%}) — that is a "
                        f"rate only unadjusted splits produce; check before reading any tail")

    if verbose:
        print(f"  ── data contract · {name} ──", flush=True)
        print(f"     rows {rep['rows']:,}"
              + (f" · duplicate bars {rep.get('duplicate_bars', 0):,}" if "duplicate_bars"
                 in rep else "")
              + (f" · calendar-adjacent {rep['calendar_adjacent']:.2%}"
                 if "calendar_adjacent" in rep else "")
              + (f" · >45% bars {rep.get('bars_over_45pct', 0):,}"
                 if "bars_over_45pct" in rep else ""), flush=True)
    if fail:
        raise ContractError(f"{name} violates the data contract:\n  - "
                            + "\n  - ".join(fail))
    return rep


def verify_sample(df: pd.DataFrame, mask, *, cols=("t_sig", "z_sig"), n: int = 8,
                  context: int = 4, seed: int = 0, label: str = "cell") -> pd.DataFrame:
    """Re-derive a few hits straight from DuckDB and print the bars around them.

    This is the guard that does not trust the frame. It takes the (ticker, date) of a handful
    of rows the mask selected, goes back to the source for the surrounding bars WITHOUT any
    filter or reshaping, and prints them. Ten seconds against a chart settles whether the
    thing being measured is the thing that was meant.

    It also checks the one property a filtered frame silently breaks: whether the bar the
    frame treats as "previous" really is the previous trading day.
    """
    m = np.asarray(mask, bool)
    idx = np.where(m)[0]
    if not len(idx):
        print(f"  verify_sample({label}): mask selects nothing", flush=True)
        return pd.DataFrame()
    rng = np.random.default_rng(seed)
    pick = rng.choice(idx, min(n, len(idx)), replace=False)
    sub = df.iloc[pick][["ticker", "date"]].copy()
    con = _con()
    have = [c for c in cols if c]
    out, broken = [], 0
    print(f"\n  ── verify_sample · {label} · {len(pick)} random hits, read from source ──",
          flush=True)
    for _, r in sub.iterrows():
        tkr, dte = r["ticker"], str(r["date"])[:10]
        q = (f"SELECT date, round(close,2) cl, round(close*volume/1e6,2) dvm, "
             + ", ".join(f"coalesce({c},'') {c}" for c in have) +
             f" FROM (SELECT *, row_number() OVER (PARTITION BY ticker,date "
             f"ORDER BY universe) rn FROM bars WHERE ticker='{tkr}' "
             f"AND universe <> 'index') WHERE rn=1 AND date BETWEEN "
             f"DATE '{dte}' - INTERVAL {context * 2 + 4} DAY AND "
             f"DATE '{dte}' + INTERVAL 4 DAY ORDER BY date")
        raw = con.execute(q).fetch_df()
        if raw.empty:
            continue
        raw["date"] = raw["date"].astype(str).str[:10]
        j = raw.index[raw["date"] == dte]
        j = int(j[0]) if len(j) else len(raw) - 1
        lo = max(0, j - context)
        print(f"\n    {tkr}  (hit on {dte})", flush=True)
        for k in range(lo, min(len(raw), j + 2)):
            rr = raw.iloc[k]
            toks = " ".join(f"{rr[c]:>5s}" for c in have if str(rr[c]))
            mark = "  ← THE HIT" if k == j else ""
            print(f"      {rr['date']}  ${rr['cl']:>9.2f}  dv ${rr['dvm']:>7.2f}M  "
                  f"{toks:<14s}{mark}", flush=True)
        # the frame's idea of "previous bar" against the source's
        row_i = int(np.where((df["ticker"].to_numpy() == tkr)
                             & (df["date"].astype(str).str[:10].to_numpy() == dte))[0][0])
        if row_i > 0 and df["ticker"].iloc[row_i - 1] == tkr:
            frame_prev = str(df["date"].iloc[row_i - 1])[:10]
            true_prev = raw["date"].iloc[j - 1] if j > 0 else None
            if true_prev and frame_prev != true_prev:
                broken += 1
                print(f"      ⚠ the frame's previous row is {frame_prev}, the real previous "
                      f"bar is {true_prev}", flush=True)
        out.append(dict(ticker=tkr, date=dte))
    con.close()
    if broken:
        print(f"\n  ⚠ {broken} of {len(pick)} sampled hits have a previous row that is NOT "
              f"the previous trading day. Any sequence built on row adjacency is wrong here.",
              flush=True)
    else:
        print(f"\n  ✅ all {len(pick)} sampled hits: the frame's previous row is the real "
              f"previous bar", flush=True)
    return pd.DataFrame(out)


def sequence_mask(df: pd.DataFrame, tokens: list, *, col: str = "tok") -> np.ndarray:
    """A run of consecutive tokens that is genuinely consecutive IN TIME.

    Rows are not bars. This ANDs the token pattern with the calendar-adjacency of every step,
    so a sequence can never be assembled out of bars that a filter left months apart. Build
    sequences with this, never with a bare shift.
    """
    if "prev_ok" not in df.columns:
        raise ContractError("frame has no prev_ok column — sequences need calendar "
                            "adjacency, and it must be computed before any filtering "
                            "reshapes the rows")
    v = df[col].fillna("").astype(str).to_numpy()
    tk = df["ticker"].to_numpy()
    ok = df["prev_ok"].to_numpy(bool)
    k = len(tokens)
    m = np.ones(len(df), bool)
    for back, want in enumerate(reversed(tokens)):   # last token is the hit bar
        shifted = np.r_[np.full(back, ""), v[:len(v) - back]] if back else v
        same = np.r_[np.zeros(back, bool), tk[:len(tk) - back] == tk[back:]] if back \
            else np.ones(len(df), bool)
        hit = np.isin(shifted, want) if isinstance(want, (list, tuple, set)) \
            else (shifted == want)
        m &= hit & same
        if back:                       # every step must be calendar-adjacent
            step = np.r_[np.zeros(back, bool), ok[back:]]
            m &= step
    return m
