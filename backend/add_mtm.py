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
for i, (tk, din, risk) in enumerate(zip(U.ticker.to_numpy(), U.date_in.astype(str).to_numpy(),
                                        U.risk.to_numpy(float))):
    p = PATH.get(tk)
    if p is None:
        continue
    d, o, hi, lo, cl = p
    j0 = int(np.searchsorted(d, din[:10]))
    if j0 >= len(d) - 2:
        continue
    entry = o[j0] * (1 + SLIP)
    if not np.isfinite(entry) or entry <= 0:
        continue
    trail = float(risk) if np.isfinite(risk) and risk > 0 else 0.25
    end = min(j0 + 1 + MAXH, len(d))
    pk = entry
    exit_bar, exit_ret = None, None
    mtm = np.full(MAXH + 1, np.nan)
    for j in range(j0 + 1, end):
        b = j - j0                                   # bar number since entry
        if exit_bar is None:
            if o[j] <= pk * (1 - trail):             # gapped through overnight
                exit_bar, exit_ret = b, o[j] / entry - 1 - SLIP
            else:
                pk = max(pk, hi[j])
                if lo[j] <= pk * (1 - trail):
                    exit_bar, exit_ret = b, pk * (1 - trail) / entry - 1 - SLIP
        # closing HERE gives the realised exit if the rule already fired, else the close
        mtm[b] = exit_ret if exit_bar is not None else (cl[j] / entry - 1 - SLIP)
    last = np.nan
    for b in range(1, MAXH + 1):                     # carry forward past the series end
        if np.isfinite(mtm[b]):
            last = mtm[b]
        else:
            mtm[b] = last
    M[i] = [mtm[g] for g in GRID]
    if i % 100_000 == 0 and i:
        print(f"  {i:,}/{len(U):,} · {time.time()-t0:.0f}s", flush=True)

U2 = pd.DataFrame(M, columns=[f"mtm_{g}" for g in GRID])
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
          f"(finite {s.notna().mean()*100:.1f}%)", flush=True)
r = O["ret"].astype(float)
m60 = O["mtm_60"].astype(float)
gap = (m60 - r).abs()
print(f"\n  mtm_60 vs the stored ret: median |gap| {gap.median()*100:.3f}pp "
      f"(they differ because ret uses the FILTERED path, mtm the unfiltered one)", flush=True)
print("\nDONE", flush=True)
