"""P0 — the two setups that failed deflation: is the effect fitted, or does it generalise?

WHY A PLAIN OOS TEST IS IMPOSSIBLE HERE

Both searches ran on `_frame(60, ...)` — the whole five years, no train/test split. HighBase
came out of a 5,389-cell motif search; 🥇G3 out of ~151 macro-regime cells. Neither has a
window the search could not see, so "test it out of sample" has no data to point at. That is
itself part of the finding and it is stated rather than worked around.

TWO DIAGNOSTICS THAT ARE AVAILABLE, AND WHAT EACH CAN AND CANNOT SHOW

  1. ERA CONCENTRATION. A parameter fitted to the whole window usually leaves a fingerprint:
     the effect clusters in part of the period. Split 2021-23 vs 2024-26 with day-clustered
     intervals, and compare the setup's era-split to the BOOK'S OWN distribution of era-splits.
     A setup more era-concentrated than its peers is suspect; one no more concentrated than
     average is not exonerated, but nothing points at it either.

  2. PARAMETER PLATEAU. A mined optimum sitting on a spike is fitted; one sitting on a plateau
     describes something real. Vary each of HighBase's thresholds one at a time and look at
     the neighbourhood — this is the strongest evidence obtainable without a clean window.

Neither can prove the setup is real. Together they can show it is fragile, which is the
decision-relevant direction: the question on the table is whether to keep trading it.
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

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
pd.set_option("display.width", 210)

O = pd.read_parquet(OPP, columns=["setup", "family", "ret", "date_in"])
O["d"] = O["date_in"].astype(str).str[:10]
O["era"] = np.where(O["d"] < "2024-01-01", "2021-23", "2024-26")
O["ret_pct"] = O["ret"].astype(float) * 100

TARGETS = ["HighBase-15mDip", "🥇G3", "🥇G3A", "🥇L43"]

# ── 1. era concentration, against the book's own distribution ────────────────
print("### 1. era split — is the effect concentrated where the search looked?\n", flush=True)
rows = []
for fam, g in O.groupby("family"):
    a = g[g.era == "2021-23"]["ret_pct"]
    b = g[g.era == "2024-26"]["ret_pct"]
    if len(a) < 150 or len(b) < 150:
        continue
    rows.append(dict(family=fam, n_a=len(a), n_b=len(b),
                     m_a=a.mean(), m_b=b.mean(), gap=b.mean() - a.mean()))
E = pd.DataFrame(rows)
print(f"  book-wide: {len(E)} families with ≥150 trades in both eras", flush=True)
print(f"  era gap (2024-26 minus 2021-23): median {E.gap.median():+.2f}pp · "
      f"p10 {E.gap.quantile(.1):+.2f} · p90 {E.gap.quantile(.9):+.2f}", flush=True)
print(f"\n  {'family':20s} {'n 21-23':>8s} {'n 24-26':>8s} {'mean 21-23':>11s} "
      f"{'mean 24-26':>11s} {'gap':>8s} {'percentile':>11s}", flush=True)
for t in TARGETS:
    r = E[E.family == t]
    if r.empty:
        print(f"  {t:20s} not enough trades in both eras", flush=True); continue
    r = r.iloc[0]
    pct = (E.gap <= r.gap).mean() * 100
    flag = "  ⚠ unusually concentrated" if pct <= 10 or pct >= 90 else ""
    print(f"  {t:20s} {r.n_a:>8,} {r.n_b:>8,} {r.m_a:>+11.2f} {r.m_b:>+11.2f} "
          f"{r.gap:>+8.2f} {pct:>10.0f}%{flag}", flush=True)

# ── 2. parameter plateau for HighBase ────────────────────────────────────────
print("\n\n### 2. HighBase parameter plateau — spike or shelf?\n", flush=True)
print("  as built: e200 rising · 1D RSI 40-60 · ≤15% off the 20d high · green bar · "
      "min 15m RSI ≤28\n", flush=True)
grp, as_of = er._frame(60, 3_000_000)
g0 = next(iter(grp.values()))
have = "E_highbase15" in g0
if not have:
    print("  E_highbase15 not in the frame — cannot vary its parameters here", flush=True)
else:
    def score(mask_fn, label):
        for tk, g in grp.items():
            g["_P"] = pd.Series(mask_fn(g), index=g.index).fillna(False).astype(bool)
        tr = er._pathsim(grp, "_P", "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
        if len(tr) < 120:
            print(f"    {label:28s} n={len(tr)} thin", flush=True); return np.nan
        ym = tr.groupby("yr")["ret"].median() * 100
        lo, hi = bootstrap_ci_clustered(tr["ret"] * 100,
                                        pd.to_datetime(tr["date_in"]).dt.strftime("%Y-%m-%d"),
                                        stat="mean")
        m = tr["ret"].mean() * 100
        print(f"    {label:28s} n={len(tr):>6,} mean{m:>+7.2f} [{lo:>+6.2f},{hi:>+6.2f}] "
              f"{int((ym>0).sum())}/{len(ym)}yr worst{ym.min():>+6.2f}", flush=True)
        return m

    e200 = "e200" if "e200" in g0 else None
    hi20 = None
    for c in ("hi20", "high_20d"):
        if c in g0:
            hi20 = c
    print("  varying the 1D RSI band (built = 40-60):", flush=True)
    base = []
    for lo_, hi_ in [(30, 50), (35, 55), (40, 60), (45, 65), (50, 70)]:
        base.append(score(lambda g, a=lo_, b=hi_: g["E_highbase15"].fillna(False)
                          & g["rsi_14"].between(a, b), f"RSI {lo_}-{hi_}"))
    arr = np.array(base, float)
    if np.isfinite(arr).sum() >= 3:
        i = int(np.nanargmax(arr))
        nb = [arr[j] for j in (i - 1, i + 1) if 0 <= j < len(arr) and np.isfinite(arr[j])]
        if nb:
            ratio = arr[i] / max(np.median(nb), 1e-9)
            verdict = ("PLATEAU" if ratio < 1.35 else
                       "⚠ SPIKE — the built value is a peak, not a shelf" if ratio >= 1.8
                       else "⚠ borderline")
            print(f"    peak {arr[i]:+.2f} vs neighbour median {np.median(nb):+.2f} "
                  f"= {ratio:.2f}× → {verdict}", flush=True)

print("\n" + "=" * 100, flush=True)
print("WHAT THIS CAN AND CANNOT SETTLE", flush=True)
print("  Neither test can prove a mined setup is real — only a window the search never saw")
print("  could do that, and for these two no such window exists.")
print("  What they CAN show is fragility: era concentration beyond the book's norm, or a")
print("  parameter sitting on a spike. Absence of both is not exoneration; presence of")
print("  either is enough to demote.", flush=True)
print("=" * 100, flush=True)
print("\nDONE", flush=True)
