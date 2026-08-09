"""B3 — retro-deflation of the whole book, with the question posed correctly.

WHY THE OBVIOUS VERSION IS WRONG

Running DSR over all 119 registry entries with n_trials=119 asks every setup "are you the
best of 119?". A setup ranked sixtieth cannot clear the expected maximum of 119 draws BY
CONSTRUCTION, so it returns 0.000 — as all eight probed setups did earlier today. That output
reads as "the whole book is dead", and it follows from the question, not from the data.

THE TWO-LEVEL STRUCTURE THAT MATCHES WHAT WE ACTUALLY DID

  WITHIN a family we searched variants and kept the best. `Washout` has seven entries — one
  edge wearing six gates. DSR is exactly the right tool here: "I tried k variants and picked
  the winner; is the winner real?" with k = the family's own trial count.

  ACROSS families we are asking many independent questions at once. That is a
  multiple-comparisons problem, not a maximum-of-N problem, so it wants FDR
  (Benjamini-Hochberg), which controls the share of FALSE claims among the families we keep.

Using DSR where FDR belongs (or the reverse) is what produced the nonsense.

TRIAL COUNTS COME FROM THREE PLACES, AND THE RANGE IS REPORTED, NOT AVERAGED

  k_low   the family's variant count in the registry — indisputable, it is on disk
  k_mid   + trials the ledger attributes to that family
  k_high  + a share of the 5,916 historical cells that no script attributed to any family

Every family is deflated at all three, and a family that only survives at k_low is labelled
`fragile` rather than passed. Runs made in a shell and never committed are invisible to all
three, so every k is a FLOOR.

Computed from opportunities.parquet — no re-simulation, seconds instead of an hour.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
from scipy import stats as sps

sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import overfit_stats as ofs                          # noqa: E402
from analysis_kit import bootstrap_ci_clustered      # noqa: E402
from ledger import read_ledger                       # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
UNATTRIBUTED = 5_916          # historical cells the backfill could not map to a family
FDR_Q = 0.10
pd.set_option("display.width", 235)

O = pd.read_parquet(OPP, columns=["setup", "family", "ret", "date_in", "yr"])
O["ret_pct"] = O["ret"].astype(float) * 100
O["d"] = O["date_in"].astype(str).str[:10]
print(f"opportunities {len(O):,} · setups {O.setup.nunique()} · "
      f"families {O.family.nunique()}\n", flush=True)

# ── trial counts per family ──────────────────────────────────────────────────
variants = O.groupby("family")["setup"].nunique()
led = {}
for r in read_ledger():
    if r.get("rerun_of"):
        continue
    led[r.get("parent", "?")] = led.get(r.get("parent", "?"), 0) + int(r.get("n_cells", 1))
n_fam = O.family.nunique()
share = UNATTRIBUTED / max(n_fam, 1)          # spread the unattributed history evenly
print(f"trial counts · variants on disk {variants.sum()} · ledger-attributed "
      f"{sum(led.values()):,} · unattributed history {UNATTRIBUTED:,} "
      f"(→ {share:.0f} per family at k_high)\n", flush=True)

# the spread of Sharpes across EVERYTHING tried — the search distribution
ALL_SR = [ofs.sharpe(gs["ret"].to_numpy(float))
          for _, gs in O.groupby("setup") if len(gs) >= 150]
print(f"search distribution: {len(ALL_SR)} setup Sharpes · "
      f"sd {np.std(ALL_SR, ddof=1):.4f} · max {max(ALL_SR):.3f}\n", flush=True)

# ── per-family: the best variant, deflated at three trial counts ─────────────
rows = []
for fam, g in O.groupby("family"):
    best, best_sr = None, -np.inf
    for st, gs in g.groupby("setup"):
        r = gs["ret"].to_numpy(float)
        if len(r) < 150:
            continue
        sr = ofs.sharpe(r)
        if sr > best_sr:
            best, best_sr, best_r, best_d = st, sr, r, gs["d"].to_numpy()
    if best is None:
        continue
    k_low = int(variants.get(fam, 1))
    k_mid = k_low + int(led.get(fam, 0))
    k_high = k_mid + int(share)
    # ⚠ trial_srs must be the SPREAD of everything tried (it estimates var(SR) under
    # search); n_trials is how many draws THIS family absorbed. Passing the family's own
    # 1-2 variants makes var_sr = 0, sr_star = 0, and DSR collapses to "is SR > 0" — which
    # returned 1.000 for all 65 families. Passing all 119 as N gave the mirror error (0.000).
    d = {k: ofs.dsr(best_r, ALL_SR, n_trials=k)["dsr"] for k in (k_low, k_mid, k_high)}
    lo, hi = bootstrap_ci_clustered(pd.Series(best_r * 100), pd.Series(best_d), stat="mean")
    rows.append(dict(family=fam, best=best, n=len(best_r), sharpe=best_sr,
                     mean=float(best_r.mean() * 100), lo=lo, hi=hi,
                     k_low=k_low, k_mid=k_mid, k_high=k_high,
                     dsr_low=d[k_low], dsr_mid=d[k_mid], dsr_high=d[k_high]))

R = pd.DataFrame(rows)
R["p"] = 1.0 - R["dsr_mid"].clip(0, 1)        # DSR is P(true SR > 0) after deflation
R = R.sort_values("p").reset_index(drop=True)

# ── Benjamini-Hochberg across families ───────────────────────────────────────
m = len(R)
R["bh_thr"] = (R.index + 1) / m * FDR_Q
passing = R[R.p <= R.bh_thr]
cut = passing.index.max() if len(passing) else -1
R["fdr_pass"] = R.index <= cut if cut >= 0 else False


def cls(r):
    if r.dsr_high >= 0.6 and r.fdr_pass:
        return "robust"
    if r.dsr_mid >= 0.6 and r.fdr_pass:
        return "solid"
    if r.dsr_low >= 0.6:
        return "fragile"
    return "suspect"


R["class"] = R.apply(cls, axis=1)

print(f"{'='*140}")
print(f"BOOK DEFLATION · {m} families · within-family DSR, across-family BH-FDR at q={FDR_Q}")
print(f"{'='*140}")
print(f"  {'family':22s} {'best variant':26s} {'n':>7s} {'mean':>7s} {'CI(days)':>17s} "
      f"{'SR':>6s} {'k_lo/mid/hi':>14s} {'DSR lo':>7s}{'mid':>7s}{'high':>7s} {'FDR':>5s} "
      f"{'class':>8s}", flush=True)
for _, r in R.iterrows():
    print(f"  {r.family:22s} {r.best:26s} {r.n:>7,} {r['mean']:>+7.2f} "
          f"[{r.lo:>+6.2f},{r.hi:>+6.2f}] {r.sharpe:>6.3f} "
          f"{r.k_low:>4d}/{r.k_mid:>4d}/{r.k_high:>4d} "
          f"{r.dsr_low:>7.3f}{r.dsr_mid:>7.3f}{r.dsr_high:>7.3f} "
          f"{'✅' if r.fdr_pass else '—':>5s} {r['class']:>8s}", flush=True)

print(f"\n{'='*140}", flush=True)
vc = R["class"].value_counts()
for k in ("robust", "solid", "fragile", "suspect"):
    print(f"  {k:8s}: {int(vc.get(k,0)):>3d} families", flush=True)
print(f"\n  survive BH-FDR at q={FDR_Q}: {int(R.fdr_pass.sum())} of {m}", flush=True)
print(f"  survive at the PESSIMISTIC trial count (k_high, DSR≥0.6): "
      f"{int((R.dsr_high>=0.6).sum())} of {m}", flush=True)
print(f"\n  ⚠ every k is a FLOOR — shell runs and abandoned notebooks are invisible.", flush=True)
print(f"  ⚠ families whose CI crosses zero are not rescued by any DSR: "
      f"{int(((R.lo<=0)&(R.hi>=0)).sum())} of {m}", flush=True)
print("=" * 140, flush=True)
R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "book_deflation.csv"),
         index=False)
print("\n  → book_deflation.csv\nDONE", flush=True)
