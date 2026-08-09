"""P1 — re-verify the six per-family maxh candidates before touching the engine.

The original sweep (maxh_sweep.py) ran before today's bug hunt. Its _pathsim calls were
honest — the accounting bugs were in the allocator, not the simulator — but two things were
wrong around it:

  · intervals were row-level, so 20,000 clustered trades looked like 20,000 facts
  · the slot break-even was computed against the POOLED median, not each setup's own, so a
    setup earning +6 was judged by whether the book's +3.51 median justified the extra hold

Both are fixed here. A candidate is only kept if, at 90 bars, it improves on ALL FOUR:
median, worst year, its own slot break-even, and a day-clustered interval that still excludes
zero. And 120 must agree with 90 — one horizon winning alone is a peak, not a shelf.

The controls are the setups the sweep said get WORSE at 90 (Spring, T1-CapBounce). If they do
not degrade here, the split is not real and the whole per-family idea drops.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er                              # noqa: E402
from analysis_kit import bootstrap_ci_clustered      # noqa: E402

pd.set_option("display.width", 215)

CAND = ["🥇L43·LEAD-in-LAG", "L43-TRIPLE🕐DR", "L43-TRIPLE🏆RS", "Z11-T11🏆RS",
        "🥇G3·LEAD-in-LAG", "🥇G3A·LEAD-in-LAG"]
CTRL = ["Spring", "T1-CapBounce", "QZ-Capit-Rev", "Washout", "🧊Coil-Floor"]
COLS = {n: c for n, c in er.SETUPS}

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}\n", flush=True)


def run(col, H):
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, H, atr_k=12.0)
    if len(tr) < 100:
        return None
    ym = tr.groupby("yr")["ret"].median() * 100
    d = pd.to_datetime(tr["date_in"]).dt.strftime("%Y-%m-%d")
    lo, hi = bootstrap_ci_clustered(tr["ret"] * 100, d, stat="median")
    return dict(n=len(tr), med=tr["ret"].median() * 100, lo=lo, hi=hi,
                hold=tr["hold"].mean(), worst=ym.min(), yrs=int((ym > 0).sum()),
                nyr=len(ym))


print(f"  {'setup':26s} {'H':>4s} {'n':>7s} {'med':>7s} {'CI(days)':>17s} {'hold':>6s} "
      f"{'yrs':>5s} {'worst':>7s} | {'Δmed':>7s} {'Δworst':>8s} {'break-even':>11s} {'':>8s}",
      flush=True)
rows = []
for name in CAND + CTRL:
    col = COLS.get(name)
    if col is None:
        print(f"  {name:26s} not in the registry", flush=True); continue
    base = run(col, 60)
    if base is None:
        print(f"  {name:26s} thin", flush=True); continue
    tag = "CAND" if name in CAND else "ctrl"
    print(f"  {name:26s} {60:>4d} {base['n']:>7,} {base['med']:>+7.2f} "
          f"[{base['lo']:>+6.2f},{base['hi']:>+6.2f}] {base['hold']:>6.1f} "
          f"{base['yrs']}/{base['nyr']:<3d} {base['worst']:>+7.2f} | {'—':>7s} {'—':>8s} "
          f"{'':>11s} {tag:>8s}", flush=True)
    for H in (90, 120):
        r = run(col, H)
        if r is None:
            continue
        # break-even against THIS setup's own median, not the book's
        thr = (r["hold"] / base["hold"] - 1) * base["med"]
        dm, dw = r["med"] - base["med"], r["worst"] - base["worst"]
        ok = (dm > 0) and (dw >= 0) and (dm >= thr) and (r["lo"] > 0)
        print(f"  {'':26s} {H:>4d} {r['n']:>7,} {r['med']:>+7.2f} "
              f"[{r['lo']:>+6.2f},{r['hi']:>+6.2f}] {r['hold']:>6.1f} "
              f"{r['yrs']}/{r['nyr']:<3d} {r['worst']:>+7.2f} | {dm:>+7.2f} {dw:>+8.2f} "
              f"{thr:>+11.2f} {'✅' if ok else '—':>8s}", flush=True)
        rows.append(dict(setup=name, kind=tag, H=H, dm=dm, dw=dw, thr=thr,
                         lo=r["lo"], ok=ok))
    print(flush=True)

R = pd.DataFrame(rows)
print("=" * 120, flush=True)
if len(R):
    p90 = R[(R.H == 90) & R.ok]
    p120 = R[(R.H == 120) & R.ok]
    both = set(p90.setup) & set(p120.setup)
    print(f"  pass at 90:  {sorted(set(p90.setup))}", flush=True)
    print(f"  pass at 120: {sorted(set(p120.setup))}", flush=True)
    print(f"\n  ✅ PLATEAU (both horizons agree): {sorted(both) if both else 'NONE'}",
          flush=True)
    peak = set(p90.setup) - set(p120.setup)
    if peak:
        print(f"  ⚠ 90 only, 120 disagrees — peak not shelf: {sorted(peak)}", flush=True)
    c = R[(R.kind == 'ctrl') & (R.H == 90)]
    print(f"\n  controls at 90 — Δmed: " +
          " · ".join(f"{r.setup} {r.dm:+.2f}" for _, r in c.iterrows()), flush=True)
    print(f"  controls that degrade (Δmed<0): {int((c.dm < 0).sum())} of {len(c)} "
          f"— the split is only real if these get WORSE", flush=True)
print("=" * 120, flush=True)
print("\nDONE", flush=True)
