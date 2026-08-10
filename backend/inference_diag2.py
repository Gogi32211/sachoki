"""1b — the apples-to-apples check the pooled ratio cannot give.

`inference_diag.py` compares SD_MC against SE_boot, and that comparison is contaminated by
design: the control arm is drawn ONCE and held fixed across every replication (as it is in
harness_power and harness_v1_v2), while the bootstrap resamples both arms. So SE_boot is
answering a wider question than SD_MC is, and a ratio above 1 is partly correct behaviour
rather than a defect. Reading that ratio as evidence would be the same attribution error as
blaming L2 as a whole before splitting the threshold from the interval.

Three comparisons, each with the confound removed a different way:

 A  TREATED ARM ONLY. MC variability of median(treated) across replications, against the
    bootstrap's SE for the same quantity with control weights held at 1. Both sides now answer
    the identical question, and this is the honest test of whether the cluster bootstrap
    overstates uncertainty.

 B  CONTROL REDRAWN. The full difference, with a fresh control subsample per replication, so
    MC finally carries the same two sources of variation the bootstrap does.

 C  IID vs CLUSTERED. The same rows resampled by ROW instead of by DATE. The ratio between the
    two widths is the design effect the clustering is buying, and it separates "wide because
    dates are correlated" from "wide because the estimator is inefficient".

Still diagnosis. Nothing in inference_v1 changes here either.
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

HOR, N_SIM, N_BOOT, DELTA, CTL_MULT = 10, 200, 200, 0.60, 3
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
EXP = tok[keep] == "T6"
TRT_ALL, CTL_POOL = np.where(EXP)[0], np.where(~EXP)[0]
CTL_FIX = np.sort(np.random.default_rng(0).choice(CTL_POOL, size=CTL_MULT * len(TRT_ALL),
                                                  replace=False))
print(f"  treated pool {len(TRT_ALL):,} · fixed control {len(CTL_FIX):,}\n", flush=True)
bar = "=" * 118


# ── A · treated arm only ─────────────────────────────────────────────────────
def arm_only(sub) -> dict:
    rng = sub("sampling")
    dts = np.unique(DATE[TRT_ALL])
    pick = rng.choice(dts, size=int(len(dts) * 0.7), replace=False)
    trt = TRT_ALL[np.isin(DATE[TRT_ALL], pick)]
    a = RET[trt] + DELTA
    oa = np.argsort(a, kind="stable")
    a_s, da = a[oa], DATE[trt][oa]
    uq, gi = np.unique(da, return_inverse=True)
    p = np.full(len(uq), 1 / len(uq))
    bs = sub("bootstrap")
    d = np.array([wmed(a_s, bs.multinomial(len(uq), p).astype(float)[gi])
                  for _ in range(N_BOOT)])
    # the same rows, resampled by ROW instead of by DATE — the design effect
    n = len(a_s)
    di = np.array([wmed(a_s, bs.multinomial(n, np.full(n, 1 / n)).astype(float))
                   for _ in range(N_BOOT)])
    lo, hi = np.percentile(d, [2.5, 97.5])
    ilo, ihi = np.percentile(di, [2.5, 97.5])
    return dict(est=float(np.median(a)), se_boot=float(d.std(ddof=1)), width=float(hi - lo),
                se_iid=float(di.std(ddof=1)), width_iid=float(ihi - ilo),
                n=len(a), n_dates=len(uq), n_distinct=int(len(np.unique(np.round(d, 9)))))


print(bar, flush=True)
print(f"  A · TREATED ARM ONLY — both sides answering the identical question "
      f"({N_SIM} replications)", flush=True)
print(bar, flush=True)
t0 = time.time()
A = pd.DataFrame([arm_only(Substreams(5000 + i)) for i in range(N_SIM)])
sd_mc, se_bt = A.est.std(ddof=1), A.se_boot.mean()
print(f"    MC SD of median(treated) across replications   {sd_mc:.4f}pp", flush=True)
print(f"    bootstrap SE for the same quantity             {se_bt:.4f}pp", flush=True)
print(f"    ratio                                          {se_bt/sd_mc:.2f}×   "
      f"({time.time()-t0:.0f}s)", flush=True)
print(f"\n    → {'the cluster bootstrap OVERSTATES the arm variability' if se_bt/sd_mc > 1.25 else 'the cluster bootstrap is calibrated on a single arm — the width is real'}",
      flush=True)
print(f"      caveat that runs the other way: the 70% date subsample is itself a resample of "
      f"the SAME\n      1,284 dates, so MC understates what a fresh five years would do. This "
      f"ratio is a floor,\n      not a point estimate.", flush=True)

# ── C · design effect ────────────────────────────────────────────────────────
print("\n" + bar, flush=True)
print("  C · CLUSTERED vs IID — what the date clustering costs, on the same rows", flush=True)
print(bar, flush=True)
deff = (A.width / A.width_iid) ** 2
print(f"    clustered width {A.width.mean():.4f}pp · iid width {A.width_iid.mean():.4f}pp",
      flush=True)
print(f"    width ratio {(A.width/A.width_iid).mean():.2f}× → design effect "
      f"{deff.mean():.1f}", flush=True)
print(f"    n {A.n.mean():,.0f} rows over {A.n_dates.mean():.0f} dates → effective n "
      f"≈ {A.n.mean()/deff.mean():,.0f}", flush=True)
print(f"\n    → {'clustering is doing the widening, and it is doing it correctly: returns on one date share a market' if deff.mean() > 3 else 'clustering is NOT the widening mechanism'}",
      flush=True)


# ── B · control redrawn ──────────────────────────────────────────────────────
def full(sub) -> dict:
    rng = sub("sampling")
    dts = np.unique(DATE[TRT_ALL])
    pick = rng.choice(dts, size=int(len(dts) * 0.7), replace=False)
    trt = TRT_ALL[np.isin(DATE[TRT_ALL], pick)]
    ctl = np.sort(rng.choice(CTL_POOL, size=CTL_MULT * len(trt), replace=False))
    a, b = RET[trt] + DELTA, RET[ctl]
    oa, ob = np.argsort(a, kind="stable"), np.argsort(b, kind="stable")
    a_s, b_s = a[oa], b[ob]
    est = float(np.median(a) - np.median(b))
    uq, gi = np.unique(np.r_[DATE[trt][oa], DATE[ctl][ob]], return_inverse=True)
    gi_a, gi_b = gi[:len(a)], gi[len(a):]
    p = np.full(len(uq), 1 / len(uq))
    bs = sub("bootstrap")
    d = np.empty(N_BOOT)
    for k in range(N_BOOT):
        w = bs.multinomial(len(uq), p).astype(float)
        d[k] = wmed(a_s, w[gi_a]) - wmed(b_s, w[gi_b])
    lo, hi = np.percentile(d, [2.5, 97.5])
    return dict(est=est, lo=float(lo), hi=float(hi), se_boot=float(d.std(ddof=1)),
                width=float(hi - lo))


print("\n" + bar, flush=True)
print(f"  B · CONTROL REDRAWN EACH REPLICATION — MC now carries both sources of variation",
      flush=True)
print(bar, flush=True)
t0 = time.time()
B = pd.DataFrame([full(Substreams(6000 + i)) for i in range(N_SIM)])
sd_b, se_b = B.est.std(ddof=1), B.se_boot.mean()
cov = ((B.lo <= DELTA) & (B.hi >= DELTA)).mean()
print(f"    MC SD of the difference   {sd_b:.4f}pp", flush=True)
print(f"    bootstrap SE              {se_b:.4f}pp", flush=True)
print(f"    ratio                     {se_b/sd_b:.2f}×   coverage {cov:.1%}   mean width "
      f"{B.width.mean():.3f}pp   ({time.time()-t0:.0f}s)", flush=True)

print("\n" + bar, flush=True)
print("  VERDICT ON THE MECHANISM", flush=True)
print(bar, flush=True)
r_arm, r_full = se_bt / sd_mc, se_b / sd_b
print(f"    treated arm alone   {r_arm:.2f}×", flush=True)
print(f"    full difference     {r_full:.2f}×", flush=True)
if max(r_arm, r_full) < 1.20:
    print(f"\n    The bootstrap's SE matches the real sampling variability. The interval is "
          f"wide because\n    {A.n.mean():,.0f} trades on {A.n_dates.mean():.0f} dates are "
          f"worth ~{A.n.mean()/deff.mean():,.0f} independent observations, and a median "
          f"difference\n    of that precision cannot separate +0.60 from +0.30. That is the "
          f"instrument answering,\n    not failing — and no change to the bootstrap will "
          f"recover it.", flush=True)
else:
    print(f"\n    The bootstrap claims more uncertainty than the experiment shows. The excess "
          f"is\n    {max(r_arm, r_full):.2f}×, and it is worth fixing before any gate is "
          f"re-tuned.", flush=True)
A.to_csv("inference_diag2_arm.csv", index=False)
B.to_csv("inference_diag2_full.csv", index=False)
print("\nDONE", flush=True)
