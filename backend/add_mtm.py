"""C2e — add mark-to-market returns so an early close can be priced honestly.

THE BUG THIS REPAIRS

allocator_v2 booked a position's return at the moment it was OPENED, using the full-hold
outcome. Replacing that position later removed it from the slot but never corrected the
number, so the simulator could hold more positions while still collecting every one of them
in full. That is what produced +11.24 on hold-60-with-replacement against +2.63 without it.

WHAT IS ADDED

For every opportunity, `mtm_N` = the return if the position is closed at the end of bar N —
with the crucial detail that the EXIT RULE MAY HAVE FIRED FIRST. If the trailing stop took
the trade out at bar 12, then mtm_20, mtm_30 and mtm_60 all equal that realised exit. Closing
"early" cannot rescue a trade that was already stopped, and it must not be able to.

So mtm_N is: exit at min(N, the bar the rule fired), with the same slippage on both sides.

⚠️ THE RIGHT EDGE — repaired 2026-09-13. The fill loop used to carry the last available mark
forward unconditionally, so a trade that had only 14 bars of data left still got a number in
`mtm_60`, indistinguishable from a real 60-bar outcome. Measured on the 2026-08-09 build: for
signals in 2026-07, 96.6 % of rows were censored that way and 99.9 % had mtm_60 == mtm_50 ==
mtm_40 — `mtm_60` was, in truth, a 14-bar return. A ranking study read those as resolved
outcomes and reported a "recent breakdown" that was the censoring, not the market.

Carry-forward is CORRECT when the trade has already exited: closing later cannot change a
realised exit, which is the whole point above. It is WRONG when the series simply ran out while
the position was still open. The two are now separated:

    mtm_N is resolved  iff  the exit fired at or before bar N,  OR  N real bars exist.
    otherwise mtm_N is NaN — an unreached horizon is not an outcome.

`bars_priced` (real marks available) and `mtm_exit_bar` (NaN when never stopped) are stored so
any consumer can re-derive the resolution of any horizon instead of trusting this file.

Grid is denser early because that is where replacement decisions actually happen — a swap on
day 3 is common, a swap on day 55 is not.

Paths come from UNFILTERED bars, matching ret_true, so a name that fell through the screen
mid-trade is still priced.
"""
from __future__ import annotations

import os
import sys
import time

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er            # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
GRID = [1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60]
SLIP, MAXH = er.SLIP, 60


def price_trade(o, hi, lo, cl, j0: int, risk: float, maxh: int = MAXH, slip: float = None):
    """Price one trade bar by bar. Returns (mtm, bars_priced, exit_bar).

    mtm[b] is the return from closing at the end of bar b, or the realised exit if the trailing
    rule already fired. Carry-forward applies ONLY after a realised exit — past the end of the
    series with the position still open, mtm[b] is NaN, because that horizon was never reached.
    exit_bar is None when the trade never stopped.
    """
    slip = SLIP if slip is None else slip
    n = len(cl)
    entry = o[j0] * (1 + slip)
    if not np.isfinite(entry) or entry <= 0:
        return None
    trail = float(risk) if np.isfinite(risk) and risk > 0 else 0.25
    end = min(j0 + 1 + maxh, n)
    pk = entry
    exit_bar, exit_ret = None, None
    mtm = np.full(maxh + 1, np.nan)
    for j in range(j0 + 1, end):
        b = j - j0
        if exit_bar is None:
            if o[j] <= pk * (1 - trail):
                exit_bar, exit_ret = b, o[j] / entry - 1 - slip
            else:
                pk = max(pk, hi[j])
                if lo[j] <= pk * (1 - trail):
                    exit_bar, exit_ret = b, pk * (1 - trail) / entry - 1 - slip
        mtm[b] = exit_ret if exit_bar is not None else (cl[j] / entry - 1 - slip)
    # Carry forward ONLY past a realised exit. Past the series end with the position still
    # open the horizon was never reached, and NaN is the only honest answer.
    bars_priced = end - (j0 + 1)
    last = np.nan
    for b in range(1, maxh + 1):
        if np.isfinite(mtm[b]):
            last = mtm[b]
        elif exit_bar is not None and b > exit_bar:
            mtm[b] = last
        else:
            mtm[b] = np.nan
    return mtm, bars_priced, exit_bar


if __name__ == "__main__":
    t0 = time.time()
    O = pd.read_parquet(OPP)
    print(f"opportunities {len(O):,}", flush=True)
    if any(f"mtm_{g}" in O.columns for g in GRID):
        print("mtm columns already present — nothing to do"); sys.exit(0)

    print("loading unfiltered paths...", flush=True)
    con = duckdb.connect(DB, read_only=True)
    raw = con.execute("""SELECT DISTINCT ticker, date, open, high, low, close FROM bars
                         WHERE universe <> 'index' AND close > 0 ORDER BY ticker, date""").fetchdf()
    con.close()
    PATH = {tk: (g["date"].astype(str).to_numpy(), g["open"].to_numpy(float),
                 g["high"].to_numpy(float), g["low"].to_numpy(float), g["close"].to_numpy(float))
            for tk, g in raw.groupby("ticker", sort=False)}
    del raw
    print(f"  {len(PATH):,} tickers · {time.time()-t0:.0f}s", flush=True)

    # unique (ticker, entry) — several setups share one trade, so price it once
    U = O[["ticker", "date_in", "risk"]].drop_duplicates(subset=["ticker", "date_in"])
    U = U.reset_index(drop=True)
    print(f"unique trades to price: {len(U):,}", flush=True)

    M = np.full((len(U), len(GRID)), np.nan, dtype=np.float32)
    NB = np.zeros(len(U), dtype=np.int16)     # real bars priced
    XB = np.full(len(U), -1, dtype=np.int16)  # exit bar, -1 = never stopped
    for i, (tk, din, risk) in enumerate(zip(U.ticker.to_numpy(), U.date_in.astype(str).to_numpy(),
                                            U.risk.to_numpy(float))):
        p = PATH.get(tk)
        if p is None:
            continue
        d, o, hi, lo, cl = p
        j0 = int(np.searchsorted(d, din[:10]))
        if j0 >= len(d) - 2:
            continue
        res = price_trade(o, hi, lo, cl, j0, risk)
        if res is None:
            continue
        mtm, n_marks, exit_bar = res
        M[i] = [mtm[g] for g in GRID]
        NB[i] = n_marks
        XB[i] = exit_bar if exit_bar is not None else -1
        if i % 100_000 == 0 and i:
            print(f"  {i:,}/{len(U):,} · {time.time()-t0:.0f}s", flush=True)

    U2 = pd.DataFrame(M, columns=[f"mtm_{g}" for g in GRID])
    U2["bars_priced"] = NB
    U2["mtm_exit_bar"] = np.where(XB >= 0, XB, np.nan)
    U2["ticker"] = U.ticker.to_numpy()
    U2["date_in"] = U.date_in.to_numpy()
    print(f"\npriced {np.isfinite(M[:, -1]).sum():,} of {len(U):,} trades · "
          f"{time.time()-t0:.0f}s", flush=True)

    O = O.merge(U2, on=["ticker", "date_in"], how="left", validate="m:1")
    O.to_parquet(OPP, index=False, compression="zstd")
    print(f"\nwrote {OPP} · {os.path.getsize(OPP)/1e6:.0f} MB", flush=True)

    print(f"\n  sanity — median mark-to-market by horizon:", flush=True)
    for g in GRID:
        s = O[f"mtm_{g}"].astype(float)
        print(f"    bar {g:>2d}: median {s.median()*100:>+7.2f}%  "
              f"(RESOLVED {s.notna().mean()*100:5.1f}%)", flush=True)
    print("\n  unresolved rows are the right edge: a horizon the data has not reached yet.", flush=True)
    if "sig_date" in O.columns:
        late = O[O["sig_date"] >= O["sig_date"].max()[:7] + "-01"]
        if len(late):
            print(f"    newest month {late['sig_date'].max()[:7]}: "
                  f"mtm_60 resolved {late['mtm_60'].notna().mean()*100:.1f}% of {len(late):,}", flush=True)
    r = O["ret"].astype(float)
    m60 = O["mtm_60"].astype(float)
    gap = (m60 - r).abs()
    print(f"\n  mtm_60 vs the stored ret: median |gap| {gap.median()*100:.3f}pp "
          f"(they differ because ret uses the FILTERED path, mtm the unfiltered one)", flush=True)
    print("\nDONE", flush=True)
