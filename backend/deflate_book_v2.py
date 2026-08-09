"""B3b — the deflation, with the two knowable errors in v1 removed.

v1 got the shape right and two details wrong.

  ERROR 1 · the trial count was spread EVENLY. 5,916 unattributed historical cells divided
  by 65 families = 82 each. But we know the distribution is extremely skewed: one script,
  hb_step4_validate.py, declared 5,389 of them — 91% of the total, and all of it spent on the
  High-Base motif search. Charging that search to Washout, Spring and Atomic is not
  conservatism, it is a knowably wrong assumption. Scripts are attributed to families by name
  where the name says so, and only the genuine residual is spread.

  ERROR 2 · the gates ran in the wrong order. v1 ranked by DSR, and DSR put three families in
  the top thirteen whose day-clustered mean cannot be distinguished from zero
  (🌉v2 Z1G→T3/T6 [−1.37,+25.90], 👑Z1G-CROWN [−0.80,+24.40], 🧲Z9-HL [−3.64,+19.52]).

  DSR IS BLIND TO CLUSTERING — it reads n=188 trades as 188 independent observations, and A6
  measured that overstatement at roughly 6× on data like ours. So the interval comes FIRST:
  if the mean is not distinguishable from zero, no deflation argument can rescue it, and how
  many trials were spent is irrelevant.
"""
from __future__ import annotations

import os
import re
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import overfit_stats as ofs                          # noqa: E402
from analysis_kit import bootstrap_ci_clustered      # noqa: E402
from ledger import read_ledger                       # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
FDR_Q = 0.10
pd.set_option("display.width", 240)

# script-name → family, for the history the backfill logged under a filename
ATTRIB = [
    (r"^hb_", "HighBase-15mDip"), (r"highbase", "HighBase-15mDip"),
    (r"engulf", "Engulf-Abs"), (r"atomic", "Atomic"), (r"washout", "Washout"),
    (r"spring", "Spring"), (r"coil", "coil_floor"), (r"zone_retest|zoneretest", "Zone-Retest"),
    (r"t1_capbounce|capbounce|gem1", "T1-CapBounce"), (r"l43", "L43-TRIPLE"),
    (r"z11|t11", "Z11-T11"), (r"p55|parabola", "P55"), (r"rtb", "RTB-Base"),
    (r"g3", "G3-gap"),
    (r"macro|lead", "🥇G3"), (r"t1gnb|t1g_nb", "🪨T1G-NB"), (r"divergence|rsidiv", "📐RSI-Div"),
]
# Deliberately NOT attributed: adx · mtf/ema · sequences · short-interest · wavetrend ·
# trendlines. Those searches ended in NULL or VETO and never became a family, so their
# trials belong to the GLOBAL residual — a search that found nothing still adds to the
# book's multiple-testing burden, it just cannot be charged to a survivor.


def attribute(script: str) -> str | None:
    s = script.lower()
    for pat, fam in ATTRIB:
        if re.search(pat, s):
            return fam
    return None


def check_targets(fams: list[str]) -> None:
    """A target that does not exist as a family is silently dropped by `f in fams`, and the
    trials it should have carried land in the residual instead. That is exactly how the
    5,389-cell High-Base search escaped its own family on the first run."""
    missing = sorted({f for _, f in ATTRIB if f not in fams})
    if missing:
        raise SystemExit(f"attribution targets not present as families: {missing}")


O = pd.read_parquet(OPP, columns=["setup", "family", "ret", "date_in"])
O["d"] = O["date_in"].astype(str).str[:10]
variants = O.groupby("family")["setup"].nunique()
fams = sorted(O.family.unique())

check_targets(fams)
named, residual = {}, 0
for r in read_ledger():
    if r.get("rerun_of"):
        continue
    n = int(r.get("n_cells", 1))
    f = attribute(str(r.get("script", "")) + " " + str(r.get("family", "")))
    if f and f in fams:
        named[f] = named.get(f, 0) + n
    else:
        residual += n
spread = residual / max(len(fams), 1)
print(f"trial attribution · named to a family {sum(named.values()):,} · "
      f"genuine residual {residual:,} (→ {spread:.0f} per family)", flush=True)
top = sorted(named.items(), key=lambda kv: -kv[1])[:6]
print("  largest attributed: " + " · ".join(f"{k} {v:,}" for k, v in top) + "\n", flush=True)

ALL_SR = [ofs.sharpe(g["ret"].to_numpy(float)) for _, g in O.groupby("setup") if len(g) >= 150]
print(f"search distribution: {len(ALL_SR)} Sharpes · sd {np.std(ALL_SR, ddof=1):.4f}\n",
      flush=True)

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
    k_mid = k_low + int(named.get(fam, 0))
    k_high = k_mid + int(spread)
    lo, hi = bootstrap_ci_clustered(pd.Series(best_r * 100), pd.Series(best_d), stat="mean")
    d = {k: ofs.dsr(best_r, ALL_SR, n_trials=k)["dsr"] for k in (k_low, k_mid, k_high)}
    rows.append(dict(family=fam, best=best, n=len(best_r), mean=float(best_r.mean() * 100),
                     lo=lo, hi=hi, sharpe=best_sr, k_low=k_low, k_mid=k_mid, k_high=k_high,
                     dsr_low=d[k_low], dsr_mid=d[k_mid], dsr_high=d[k_high]))

R = pd.DataFrame(rows)
# GATE 1 — the interval. Nothing that fails here can be argued back in.
R["ci_ok"] = R.lo > 0
R["p"] = 1.0 - R["dsr_mid"].clip(0, 1)
R = R.sort_values(["ci_ok", "p"], ascending=[False, True]).reset_index(drop=True)
elig = R[R.ci_ok].reset_index(drop=True)
elig["bh_thr"] = (elig.index + 1) / max(len(elig), 1) * FDR_Q
pas = elig[elig.p <= elig.bh_thr]
cut = pas.index.max() if len(pas) else -1
ok_fams = set(elig.loc[:cut, "family"]) if cut >= 0 else set()
R["fdr_pass"] = R.family.isin(ok_fams)


def cls(r):
    if not r.ci_ok:
        return "⛔ CI≈0"                      # the interval decides first
    if r.dsr_high >= 0.6 and r.fdr_pass:
        return "robust"
    if r.dsr_mid >= 0.6 and r.fdr_pass:
        return "solid"
    return "fragile"


R["class"] = R.apply(cls, axis=1)

print("=" * 148)
print(f"BOOK DEFLATION v2 · gate 1 = day-clustered CI · gate 2 = DSR at three trial counts "
      f"· gate 3 = BH-FDR q={FDR_Q}")
print("=" * 148)
print(f"  {'family':22s} {'best variant':26s} {'n':>7s} {'mean':>7s} {'CI(days)':>17s} "
      f"{'SR':>6s} {'k lo/mid/high':>16s} {'DSRlo':>6s}{'mid':>7s}{'high':>7s} {'FDR':>5s} "
      f"{'class':>9s}", flush=True)
for _, r in R.iterrows():
    print(f"  {r.family:22s} {r.best:26s} {r.n:>7,} {r['mean']:>+7.2f} "
          f"[{r.lo:>+6.2f},{r.hi:>+6.2f}] {r.sharpe:>6.3f} "
          f"{r.k_low:>4d}/{r.k_mid:>5d}/{r.k_high:>5d} "
          f"{r.dsr_low:>6.3f}{r.dsr_mid:>7.3f}{r.dsr_high:>7.3f} "
          f"{'✅' if r.fdr_pass else '—':>5s} {r['class']:>9s}", flush=True)

print("\n" + "=" * 148, flush=True)
vc = R["class"].value_counts()
for k in ("robust", "solid", "fragile", "⛔ CI≈0"):
    print(f"  {k:10s}: {int(vc.get(k,0)):>3d} families", flush=True)
print(f"\n  gate 1 (CI excludes zero):        {int(R.ci_ok.sum())} of {len(R)}", flush=True)
print(f"  + gate 3 (BH-FDR q={FDR_Q}):          {int(R.fdr_pass.sum())}", flush=True)
print(f"  + gate 2 at the pessimistic count: {int(((R.dsr_high>=0.6)&R.ci_ok).sum())}",
      flush=True)
print(f"\n  ⚠ k is still a FLOOR — shell runs and abandoned notebooks are invisible to any"
      f" scan of committed files.", flush=True)
print("=" * 148, flush=True)
R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "book_deflation_v2.csv"),
         index=False)
print("\n  → book_deflation_v2.csv\nDONE", flush=True)
