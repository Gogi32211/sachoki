"""
validate_edge_season.py — do the validated Edge setups suffer the Dec-Mar season
the way the whole universe (and K0) does, or are they strong enough to pass?
Blocks: GOOD Apr-Jun+Sep-Nov · BAD Dec-Mar · MID Jul-Aug (from the K0 seasonality
study, confirmed universe-wide by the all-bars control). Per-block med/win/PF +
per-year medians of the BAD block (the actionable question: skip Dec-Mar or not).
READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _prep, _pathsim, SETUPS
from edge_echo import pull

GOOD = {"04", "05", "06", "09", "10", "11"}; BAD = {"12", "01", "02", "03"}


def main():
    t0 = time.time()
    df = pull("1d")
    df = _prep(df)
    grp = {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}
    print(f"rows={sum(len(g) for g in grp.values()):,} prepped ({time.time()-t0:.0f}s)\n", flush=True)
    print(f"{'setup':16s} {'blk':4s} {'n':>6} {'med':>6} {'win%':>5} {'PF':>5}   BAD per-yr med")
    for name, col in SETUPS:
        if "🌀" in name:
            continue
        tr = _pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60)
        if len(tr) == 0:
            continue
        tr["mo"] = tr["date_in"].str[5:7]
        tr["blk"] = np.where(tr.mo.isin(GOOD), "GOOD", np.where(tr.mo.isin(BAD), "BAD", "MID"))
        lines = {}
        for blk, g in tr.groupby("blk"):
            w = g.ret > 0
            pf = g.loc[w, "ret"].sum() / max(-g.loc[~w, "ret"].sum(), 1e-9)
            lines[blk] = f"{name:16s} {blk:4s} {len(g):>6,} {g.ret.median()*100:>+6.2f} {w.mean()*100:>5.1f} {pf:>5.2f}"
        bad = tr[tr.blk == "BAD"]
        yr = " ".join(f"{y[2:]}:{gg.ret.median()*100:+4.1f}" for y, gg in bad.groupby("yr")) if len(bad) else ""
        for blk in ("GOOD", "BAD", "MID"):
            if blk in lines:
                print(lines[blk] + (f"   {yr}" if blk == "BAD" else ""), flush=True)
        print()
    print(f"done {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
