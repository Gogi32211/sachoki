"""1d — the decisive experiment: is the 1.63× a defect, or the design of the harness?

diag1 measured SE_boot / SD_MC = 1.63× on the median and 1.59× on the proportion, with
coverage 98.8% and 99.6%. Read naively that says the bootstrap overstates uncertainty. There is
an arithmetic explanation that requires no bug at all, and it must be excluded before anything
is called broken.

The Monte Carlo draws 70% of the dates from a FIXED set of 1,284, without replacement. That is
sampling from a finite population, and its variance carries the finite-population correction:

    Var_MC   = (σ²/n)·(1 − f)          f = fraction of clusters drawn
    Var_boot =  σ²/n                   the superpopulation question: what would OTHER five
                                       years have given
    ratio    = 1 / √(1 − f)

At f = 0.7 that is 1.826×, against 1.63× observed — the bootstrap is NARROWER than the design
alone demands. If true, the whole diagnosis inverts: coverage of 99.7% in harness_power is not
a broken interval, it is an interval of superpopulation width being scored against a Monte
Carlo scatter that the FPC shrank by √0.3.

PRE-REGISTERED PREDICTION, written before the run and not adjusted after:

    f = 0.30 → 1.195×      f = 0.50 → 1.414×
    f = 0.70 → 1.826×      f = 0.90 → 3.162×

The shape is what matters, not any single point. If the observed ratios track 1/√(1−f) across
a 2.6× span, the bootstrap is exonerated and the harness is the thing to fix. If they sit flat
near 1.6× regardless of f, the FPC is irrelevant and suspicion returns to the construction —
which would be the opposite conclusion from the same data, so the test is not rigged toward the
answer I now expect.

Treated arm only, median only: no control, no difference, nothing that could import a second
mechanism. The bootstrap resamples exactly the clusters the estimate was computed on.
"""
from __future__ import annotations

import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sources as srcs                                          # noqa: E402
from studio_verdict import Substreams                           # noqa: E402

HOR, N_SIM, N_BOOT = 10, 300, 200
FRACS = [0.30, 0.50, 0.70, 0.90]
PRED = {f: 1 / np.sqrt(1 - f) for f in FRACS}
pd.set_option("display.width", 205)


def wmed(v_sorted, w):
    c = np.cumsum(w)
    return float(v_sorted[np.searchsorted(c, c[-1] / 2.0)]) if c[-1] > 0 else np.nan


print("loading", flush=True)
df = srcs.bars("1d", columns=("t_sig", "z_sig"), min_price=5.0, min_dollar_vol=3_000_000,
               verbose=False)
tk = df["ticker"].to_numpy()
c, o = df["close"].to_numpy(float), df["open"].to_numpy(float)
ent = np.where(np.r_[tk[1:] == tk[:-1], False], np.r_[o[1:], np.nan], np.nan)
okh = np.r_[tk[HOR:] == tk[:-HOR], np.zeros(HOR, bool)]
ex = np.where(okh, np.r_[c[HOR:], np.full(HOR, np.nan)], np.nan)
base = (ex / ent - 1) * 100
T = df["t_sig"].fillna("").astype(str).to_numpy()
Z = df["z_sig"].fillna("").astype(str).to_numpy()
tok = np.where((T != "") & (T != "nan"), T, Z)
keep = np.isfinite(base)
RET, DATE = base[keep], df["date"].astype(str).str[:10].to_numpy()[keep]
TRT = np.where(tok[keep] == "T6")[0]
A_RET, A_DATE = RET[TRT], DATE[TRT]
DTS = np.unique(A_DATE)
bar = "=" * 118
print(f"  treated {len(TRT):,} rows over {len(DTS):,} dates — control excluded so nothing but "
      f"the\n  cluster-sampling design can move these numbers\n", flush=True)

print(bar, flush=True)
print("  PRE-REGISTERED  ratio = 1/√(1−f)   " +
      " · ".join(f"f={f:.2f}→{PRED[f]:.3f}×" for f in FRACS), flush=True)
print(bar, flush=True)
print(f"  {'f':>6s} {'n dates':>8s} {'SD_MC':>9s} {'SE_boot':>9s} {'observed':>9s} "
      f"{'predicted':>10s} {'obs/pred':>9s} {'coverage':>9s}", flush=True)

out = []
for f in FRACS:
    t0 = time.time()
    est, se, lo, hi = [], [], [], []
    for i in range(N_SIM):
        sub = Substreams(11000 + i)
        pick = sub("sampling").choice(DTS, size=int(len(DTS) * f), replace=False)
        m = np.isin(A_DATE, pick)
        a, da = A_RET[m], A_DATE[m]
        oa = np.argsort(a, kind="stable")
        a_s = a[oa]
        uq, gi = np.unique(da[oa], return_inverse=True)
        p = np.full(len(uq), 1 / len(uq))
        bs = sub("bootstrap")
        d = np.array([wmed(a_s, bs.multinomial(len(uq), p).astype(float)[gi])
                      for _ in range(N_BOOT)])
        est.append(float(np.median(a)))
        se.append(float(d.std(ddof=1)))
        q = np.percentile(d, [2.5, 97.5])
        lo.append(q[0])
        hi.append(q[1])
    est, se = np.asarray(est), np.asarray(se)
    theta = float(np.median(A_RET))          # the finite-population truth the MC scatters about
    cov = float(np.mean((np.asarray(lo) <= theta) & (np.asarray(hi) >= theta)))
    obs = se.mean() / est.std(ddof=1)
    out.append(dict(f=f, n_dates=int(len(DTS) * f), sd_mc=est.std(ddof=1), se=se.mean(),
                    obs=obs, pred=PRED[f], cov=cov))
    print(f"  {f:>6.2f} {int(len(DTS)*f):>8,d} {est.std(ddof=1):>9.4f} {se.mean():>9.4f} "
          f"{obs:>8.3f}× {PRED[f]:>9.3f}× {obs/PRED[f]:>9.2f} {cov:>9.1%}   "
          f"({time.time()-t0:.0f}s)", flush=True)

O = pd.DataFrame(out)
r = float(np.corrcoef(O.obs, O.pred)[0, 1])
slope = float(np.polyfit(O.pred, O.obs, 1)[0])
mean_ratio = float((O.obs / O.pred).mean())

print("\n" + bar, flush=True)
print("  VERDICT", flush=True)
print(bar, flush=True)
print(f"    observed spans {O.obs.min():.2f}× → {O.obs.max():.2f}× as f goes {FRACS[0]:.2f} → "
      f"{FRACS[-1]:.2f}", flush=True)
print(f"    predicted spans {O.pred.min():.2f}× → {O.pred.max():.2f}×", flush=True)
print(f"    correlation {r:+.4f} · slope {slope:.3f} · mean obs/pred {mean_ratio:.2f}",
      flush=True)
tracks = r > 0.98 and 0.80 < mean_ratio < 1.20
flat = O.obs.max() / O.obs.min() < 1.3
print()
if tracks:
    print("    → THE FPC EXPLAINS IT. The ratio is not a property of the bootstrap; it is a",
          flush=True)
    print("      property of a Monte Carlo that redraws clusters from a fixed finite set. The",
          flush=True)
    print("      bootstrap answers repeated-sampling uncertainty UNDER THE EMPIRICAL CLUSTER-",
          flush=True)
    print("      RESAMPLING MODEL — reshuffling the regimes that occurred — and against that",
          flush=True)
    print("      question its width is right. It is NOT 'what other five years would give':",
          flush=True)
    print("      resampling observed clusters cannot produce an unseen regime, a structural",
          flush=True)
    print("      break, or a crisis of a new type. That claim belongs to frozen forward/OOS.",
          flush=True)
    print(f"\n      Consequence: coverage of 98.8% and 99.7% is an ARTEFACT OF THE HARNESS,",
          flush=True)
    print("      not evidence of a broken interval, and inference_v2 has no defect to fix here.",
          flush=True)
    print("      What needs correcting is how the control samples score coverage.", flush=True)
elif flat:
    print("    → THE FPC IS NOT THE MECHANISM. The ratio ignores f, so it cannot be finite-",
          flush=True)
    print("      population sampling, and suspicion returns to the bootstrap construction.",
          flush=True)
else:
    print(f"    → PARTIAL. The ratio moves with f but not as 1/√(1−f) (mean obs/pred "
          f"{mean_ratio:.2f}).", flush=True)
    print("      Part of the width is design, part is not, and the residual needs its own test.",
          flush=True)

print(f"\n    NOTE on what this does NOT settle: even fully exonerated, the bootstrap's width is",
      flush=True)
print("    conditional on these 1,284 dates being a fair sample of market regimes. Five years",
      flush=True)
print("    containing one 2022 is not five draws of a 2022.", flush=True)
O.to_csv("inference_diag4.csv", index=False)
print("\nDONE", flush=True)
