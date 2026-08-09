"""Which TZ signal is actually good — the one question the packages have the n to answer.

The sequence database cannot answer it: at a median n of 58 the error on a single rule is
±1.07pp while the entire true spread between all 66,989 rules is 0.61pp, so ranking them
sorts noise. The per-signal baselines are the opposite case — 26K to 294K observations per
cell — and there are only 25 signals × 3 universes × 3 timeframes = 225 cells in total, a
search small enough to survive being counted.

Reference point: the packages contain no all-bars row, so a raw median of +0.11 means
nothing on its own. The n-weighted mean across every signal inside the same (universe,
timeframe) is used instead — the signals cover most of the tape, so that average IS
approximately the population, and every number below is a lift against it.

A signal is judged on nine independent measurements (3 universes × 3 timeframes) by its
WORST one, not its best or its average. One good cell out of nine is what a search
produces; nine out of nine is what an effect produces.

Precision comes from the funnel fit in tz_package_audit.py: per-trade σ ≈ 8.16pp on 1D,
5.93 on 4H, 3.00 on 1H. The standard error of a median is ≈ 1.2533·σ/√n, so every lift is
printed with the ± that actually applies to it.
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 210)

S = ("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
     "aba6fbf4-ff3b-4f32-9bd8-48f188d02d96/scratchpad/tz")
PKG = {"1D": f"{S}/_TZ_ANALYTICS_5YR/_TZ ANALYTICS 5YR",
       "4H": f"{S}/_TZ_ANALYTICS_5YR_4H/_TZ ANALYTICS 5YR 4H",
       "1H": f"{S}/_TZ_ANALYTICS_5YR_1H/_TZ ANALYTICS 5YR 1H"}
SIGMA = {"1D": 8.16, "4H": 5.93, "1H": 3.00}      # from the funnel fit
SE_K = 1.2533                                      # SE of a median vs of a mean


def se(tf, n):
    return SE_K * SIGMA[tf] / np.sqrt(n)


rows = []
for tf, d in PKG.items():
    b = pd.read_csv(os.path.join(d, "rich_baseline_all.csv"))
    b["tf"] = tf
    for u, g in b.groupby("universe"):
        w = g.n / g.n.sum()
        ref_m = float((g.m10 * w).sum())           # n-weighted = ≈ the population
        ref_w = float((g.win * w).sum())
        for _, r in g.iterrows():
            rows.append(dict(tf=tf, universe=u, signal=r.signal, n=int(r.n),
                             m10=r.m10, a10=r.a10, win=r.win, mfe=r.mfe, mae=r.mae,
                             lift=r.m10 - ref_m, wlift=r.win - ref_w,
                             se=se(tf, r.n), ref=ref_m))
R = pd.DataFrame(rows)
R["t"] = R.lift / R.se
R["sig"] = R.t.abs() >= 2

print("=" * 122)
print("  PER-SIGNAL BASELINES — 25 signals × 3 universes × 3 timeframes = "
      f"{len(R)} cells, n from {R.n.min():,} to {R.n.max():,}")
print("  lift = 10-bar median minus the n-weighted average signal in the same "
      "(universe, timeframe)")
print("=" * 122)

piv = R.pivot_table(index="signal", columns=["tf", "universe"], values="lift")
piv = piv[[c for c in [(t, u) for t in ("1D", "4H", "1H")
                       for u in ("sp500", "nasdaq", "russell2k")] if c in piv.columns]]
summ = R.groupby("signal").agg(cells=("lift", "size"), n_min=("n", "min"),
                               worst=("lift", "min"), best=("lift", "max"),
                               med=("lift", "median"), pos=("lift", lambda s: (s > 0).sum()),
                               wlift=("wlift", "median"),
                               se_med=("se", "median"))
summ["worst_t"] = R.groupby("signal").apply(
    lambda g: (g.lift / g.se).min(), include_groups=False)
summ = summ.sort_values("worst", ascending=False)

print(f"\n  {'signal':>7s} {'cells+':>7s} {'worst':>8s} {'median':>8s} {'best':>8s} "
      f"{'±SE(med)':>9s} {'worst t':>8s} {'win lift':>9s} {'n min':>9s}   verdict")
for s, r in summ.iterrows():
    if r.pos == r.cells and r.worst > 0:
        v = "✅ POSITIVE on all 9"
    elif r.pos >= 8:
        v = "◐ 8/9"
    elif r.pos <= 1:
        v = "🔴 NEGATIVE on all 9" if r.pos == 0 else "🔴 1/9"
    else:
        v = f"— {int(r.pos)}/9 (no sign agreement)"
    print(f"  {s:>7s} {int(r.pos)}/{int(r.cells):<5d} {r.worst:>+8.3f} {r.med:>+8.3f} "
          f"{r.best:>+8.3f} {r.se_med:>9.3f} {r.worst_t:>+8.2f} {r.wlift:>+9.2f} "
          f"{r.n_min:>9,}   {v}")

print("\n  the same table as the raw grid (lift per cell):")
print(piv.round(3).to_string())

# ── what a signal must clear to be worth acting on ───────────────────────────
print("\n" + "=" * 122)
print("  WHAT CLEARS THE BAR")
print("=" * 122)
strong = summ[(summ.pos == summ.cells) & (summ.worst > 0)]
weak = summ[(summ.pos == 0)]
print(f"    positive in all 9 measurements : {len(strong)} signals "
      f"{list(strong.index) if len(strong) else ''}")
print(f"    negative in all 9 measurements : {len(weak)} signals "
      f"{list(weak.index) if len(weak) else ''}")
print(f"    expected by chance if signals were coin flips (9 same-sign out of 25): "
      f"{25 * 2 * 0.5 ** 9:.2f} signals")
print("\n    NOTE the sign: a signal that is negative on all nine is as usable as one that")
print("    is positive on all nine — it is a veto, and vetoes are cheaper to trade than")
print("    entries because they cost nothing when wrong.")

# ── composites: only those with enough n to be readable ─────────────────────
print("\n" + "=" * 122)
print("  COMPOSITES — after the n filter the funnel implies")
print("=" * 122)
NEED = {tf: int((SE_K * SIGMA[tf] / 0.5) ** 2) for tf in PKG}   # n for SE ≤ 0.5pp
print(f"    n needed for a ±0.5pp standard error: "
      + " · ".join(f"{tf} {v:,}" for tf, v in NEED.items()))
C = []
for tf, d in PKG.items():
    c = pd.read_csv(os.path.join(d, "rule_database_composites_5yr.csv"))
    c["tf"] = tf
    C.append(c)
C = pd.concat(C, ignore_index=True)
for tf in PKG:
    sub = C[C.tf == tf]
    ok = sub[sub.n >= NEED[tf]]
    print(f"    {tf}: {len(sub):,} composites · {len(ok):,} have n ≥ {NEED[tf]:,} "
          f"({len(ok) / len(sub):.1%})")
big = C[C.apply(lambda r: r.n >= NEED[r.tf], axis=1)].copy()
if len(big):
    k = ["universe", "signal", "composite"]
    w = big.pivot_table(index=k, columns="tf", values="m10")
    w = w.dropna(thresh=2)
    w["cells"] = w[[c for c in ("1D", "4H", "1H") if c in w.columns]].notna().sum(axis=1)
    tfc = [c for c in ("1D", "4H", "1H") if c in w.columns]
    w["worst"] = w[tfc].min(axis=1)
    w["pos"] = (w[tfc] > 0).sum(axis=1)
    good = w[(w.pos == w.cells) & (w.worst > 0)].sort_values("worst", ascending=False)
    print(f"\n    composites measured on ≥2 timeframes with enough n: {len(w):,}")
    print(f"    positive on EVERY timeframe where they were measured: {len(good):,}")
    if len(good):
        print(f"\n      {'universe':>10s} {'sig':>5s} {'composite':>16s} "
              + " ".join(f"{t:>8s}" for t in tfc) + f" {'worst':>8s}")
        for idx, r in good.head(25).iterrows():
            u, s, cp = idx
            print(f"      {u:>10s} {s:>5s} {str(cp):>16s} "
                  + " ".join(f"{r[t]:>+8.3f}" if pd.notna(r[t]) else f"{'—':>8s}"
                             for t in tfc) + f" {r.worst:>+8.3f}")
        good.to_csv("tz_composite_survivors.csv")
        print(f"\n    written: tz_composite_survivors.csv")
R.to_csv("tz_signal_grid.csv", index=False)
print("\n    written: tz_signal_grid.csv")
print("\nDONE")
