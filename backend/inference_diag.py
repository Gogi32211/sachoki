"""inference_v1, diagnosed: is the interval wide because the world is, or because we are?

Both branches of the v1→v2 comparison stopped at the same wall, and it is no longer the
decision layer. RETURN: MATERIALITY blocks 72 of 100 at δ = 0.60 because the interval straddles
δ*. RISK: NON_INFERIORITY blocks 96 of 100 because the cost interval cannot exclude −ε. The
estimator resolves the effect 98-100% of the time in both. What it cannot do is say how large
it is, and coverage of 99.7% against a nominal 95% says the fault may be ours.

May be. That is the whole question, and it has exactly two answers with opposite remedies:

    the bootstrap overstates uncertainty      → fix the estimator
    five years of data really are this vague  → the instrument is answering, not failing

Nothing is changed here. inference_v1 is frozen and this file only measures it.

Four diagnostics, ordered so the cheapest one that could end the enquiry runs first:

 1  SD_MC vs SE_boot. With a known truth and hundreds of replications the actual sampling
    variability of the estimate can be computed directly and compared to what the bootstrap
    claims about itself. A ratio near 1 exonerates the bootstrap and the answer becomes "the
    data are vague". A ratio well above 1 localises the defect immediately.

 2  paired cluster weights. If the two arms receive DIFFERENT date resamples, the positive
    covariance between them is destroyed and Var(T−C) = Var(T)+Var(C) comes out too wide.
    Reading the code says the weights are shared — one multinomial draw indexed into both arms
    — but reading is not measuring, and this week has been one long argument for the
    difference. Measured by recomputing the same replications both ways.

 3  discreteness. A median is a step function of order statistics. Under heavy date clustering
    — one date contributing hundreds of rows, another three — the set of medians a resample can
    produce may be coarse, and a lumpy bootstrap distribution is a wide one. The count of
    DISTINCT values among the draws answers this directly.

 4  per-estimand coverage. `bootstrap_ci_clustered` cannot be called validated or broken as a
    function: it runs at 99.7% on median differences and 88-95% on proportions. The unit of
    validation is (estimand, estimator, dependence, structure), so the median and the
    proportion are measured separately on the SAME rows and the SAME clusters, and the only
    thing that differs between them is the functional.
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

HOR = 10
N_SIM = 250                 # replications for diagnostic 1/3/4
N_PAIR = 80                 # replications for the paired/unpaired contrast
N_BOOT = 200                # bootstrap draws — the same number inference_v1 uses
DELTA = 0.60                # the planted effect, in pp; the size RETURN could not resolve
CTL_MULT = 3                # control arm size, as a multiple of treated
pd.set_option("display.width", 205)


def wmed(v_sorted, w):
    """Weighted median of an already-sorted array."""
    c = np.cumsum(w)
    return float(v_sorted[np.searchsorted(c, c[-1] / 2.0)]) if c[-1] > 0 else np.nan


# ── the population, built once ───────────────────────────────────────────────
t0 = time.time()
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
RET = base[keep]
DATE = df["date"].astype(str).str[:10].to_numpy()[keep]
EXP = tok[keep] == "T6"

TRT_ALL = np.where(EXP)[0]
rs = np.random.default_rng(0)
CTL = np.sort(rs.choice(np.where(~EXP)[0], size=CTL_MULT * len(TRT_ALL), replace=False))
CTL_ORD = np.argsort(RET[CTL], kind="stable")
B_SORTED = RET[CTL][CTL_ORD]
print(f"  {len(RET):,} trades · treated pool {len(TRT_ALL):,} · control {len(CTL):,} "
      f"({time.time()-t0:.0f}s)", flush=True)

cnt = pd.Series(DATE[TRT_ALL]).value_counts()
print(f"  treated dates {len(cnt):,} · rows per date: median {cnt.median():.0f} · "
      f"p90 {cnt.quantile(.9):.0f} · max {cnt.max():,} · "
      f"HHI {((cnt / cnt.sum()) ** 2).sum():.5f}", flush=True)
print(f"  the cluster structure above is what every interval below is conditioned on\n",
      flush=True)

# a threshold for the proportion estimand, chosen once so both estimands sit on the same rows
Q = float(np.median(RET))


def one(sub: Substreams, paired: bool = True, do_prop: bool = True) -> dict:
    """One replication: the estimate, and the bootstrap's opinion of its own uncertainty.

    Both estimands — the median difference and the proportion difference — are computed from
    the SAME resample weights, so any difference in their calibration is a property of the
    functional and not of the resampling scheme.
    """
    rng = sub("sampling")
    dts = np.unique(DATE[TRT_ALL])
    pick = rng.choice(dts, size=int(len(dts) * 0.7), replace=False)
    trt = TRT_ALL[np.isin(DATE[TRT_ALL], pick)]

    a = RET[trt] + DELTA
    da = DATE[trt]
    oa = np.argsort(a, kind="stable")
    a_s = a[oa]

    est = float(np.median(a) - np.median(B_SORTED))
    ind_a = (a_s > Q).astype(float)                      # P(return > Q), treated
    ind_b = (B_SORTED > Q).astype(float)                 # ... and control
    est_p = float(ind_a.mean() - ind_b.mean()) * 100

    bs = sub("bootstrap")
    uq, gi = np.unique(np.r_[da, DATE[CTL]], return_inverse=True)
    na = len(a)
    gi_a, gi_b = gi[:na][oa], gi[na:][CTL_ORD]
    p = np.full(len(uq), 1 / len(uq))
    d = np.empty(N_BOOT)
    dp = np.empty(N_BOOT)
    for k in range(N_BOOT):
        w = bs.multinomial(len(uq), p).astype(float)
        wa = w[gi_a]
        wb = w[gi_b] if paired else bs.multinomial(len(uq), p).astype(float)[gi_b]
        d[k] = wmed(a_s, wa) - wmed(B_SORTED, wb)
        if do_prop:
            sa, sb = wa.sum(), wb.sum()
            dp[k] = ((ind_a @ wa / sa) - (ind_b @ wb / sb)) * 100 if sa and sb else np.nan
    lo, hi = np.percentile(d, [2.5, 97.5])
    plo, phi = (np.percentile(dp, [2.5, 97.5]) if do_prop else (np.nan, np.nan))
    return dict(est=est, lo=float(lo), hi=float(hi), se_boot=float(d.std(ddof=1)),
                width=float(hi - lo), n_distinct=int(len(np.unique(np.round(d, 9)))),
                est_p=est_p, p_lo=float(plo), p_hi=float(phi),
                se_p=float(np.nanstd(dp, ddof=1)) if do_prop else np.nan,
                n=int(na), n_dates=int(len(np.unique(da))))


bar = "=" * 118
print(bar, flush=True)
print(f"  1 · MONTE-CARLO TRUTH vs BOOTSTRAP OPINION   (true δ = {DELTA:+.2f}pp, "
      f"{N_SIM} replications)", flush=True)
print(bar, flush=True)
t0 = time.time()
R = pd.DataFrame([one(Substreams(5000 + i)) for i in range(N_SIM)])
print(f"  ({time.time()-t0:.0f}s)\n", flush=True)

sd_mc, se_bt = R.est.std(ddof=1), R.se_boot.mean()
ratio = se_bt / sd_mc
cov = ((R.lo <= DELTA) & (R.hi >= DELTA)).mean()
print(f"    SD of the estimate across replications (the truth)   {sd_mc:.4f}pp", flush=True)
print(f"    mean SE the bootstrap reports about itself           {se_bt:.4f}pp", flush=True)
print(f"    ratio  SE_boot / SD_MC                               {ratio:.2f}×", flush=True)
print(f"    mean 95% width {R.width.mean():.3f}pp · empirical coverage {cov:.1%} "
      f"(nominal 95%)", flush=True)
print(f"    mean estimate {R.est.mean():+.3f}pp · bias {R.est.mean()-DELTA:+.3f}pp", flush=True)
v1 = ("the bootstrap OVERSTATES uncertainty — the defect is ours, and it is this"
      if ratio > 1.25 else
      "the bootstrap is HONEST — its SE matches the real sampling variability, so the width "
      "is the data speaking" if ratio < 1.10 else "borderline — neither reading is clean")
print(f"\n    → {v1}", flush=True)
if ratio < 1.10 and cov > 0.97:
    print(f"      note: SE is right but coverage is {cov:.1%}. A correct SE with excessive "
          f"coverage means\n      the interval is not built from the SE — the PERCENTILE "
          f"method is the suspect, not the\n      resampling.", flush=True)

print("\n" + bar, flush=True)
print(f"  2 · PAIRED CLUSTER WEIGHTS — reading the code said shared; this measures it",
      flush=True)
print(bar, flush=True)
t0 = time.time()
P = pd.DataFrame([one(Substreams(7000 + i), paired=True, do_prop=False)
                  for i in range(N_PAIR)])
U = pd.DataFrame([one(Substreams(7000 + i), paired=False, do_prop=False)
                  for i in range(N_PAIR)])
print(f"    paired   (one draw indexed into both arms)   mean width {P.width.mean():.3f}pp",
      flush=True)
print(f"    unpaired (two independent draws)             mean width {U.width.mean():.3f}pp",
      flush=True)
infl = U.width.mean() / P.width.mean()
print(f"    inflation if the pairing were lost           {infl:.2f}×   ({time.time()-t0:.0f}s)",
      flush=True)
print(f"\n    → the harness uses the PAIRED form. Losing it would cost "
      f"{(infl-1)*100:+.0f}% of width,\n      so the pairing is intact and is "
      f"{'not' if infl < 1.15 else 'still worth'} the explanation.", flush=True)

print("\n" + bar, flush=True)
print("  3 · DISCRETENESS — a median is a step function of order statistics", flush=True)
print(bar, flush=True)
frac = R.n_distinct.median() / N_BOOT
print(f"    distinct values among {N_BOOT} bootstrap draws: median "
      f"{R.n_distinct.median():.0f} · min {R.n_distinct.min()} · max {R.n_distinct.max()}",
      flush=True)
print(f"    that is {frac:.0%} of draws taking a distinct value", flush=True)
v3 = ("LUMPY — the resample cannot express fine differences, and a step-function bootstrap "
      "distribution\n      is a wide one. This IS a mechanism."
      if frac < 0.40 else
      "smooth enough — the median moves freely under resampling, so discreteness is not the "
      "mechanism")
print(f"\n    → {v3}", flush=True)

print("\n" + bar, flush=True)
print("  4 · PER-ESTIMAND — same rows, same clusters, same weights, different functional",
      flush=True)
print(bar, flush=True)
cov_p = ((R.p_lo <= R.est_p) & (R.p_hi >= R.est_p)).mean()   # trivially 1; use MC truth instead
mu_p = R.est_p.mean()
cov_p = ((R.p_lo <= mu_p) & (R.p_hi >= mu_p)).mean()
sd_p, se_p = R.est_p.std(ddof=1), R.se_p.mean()
print(f"    {'estimand':<26s} {'SD_MC':>9s} {'SE_boot':>9s} {'ratio':>7s} {'width':>9s} "
      f"{'coverage':>9s}", flush=True)
print(f"    {'median difference (pp)':<26s} {sd_mc:>9.4f} {se_bt:>9.4f} {ratio:>7.2f} "
      f"{R.width.mean():>9.3f} {cov:>9.1%}", flush=True)
print(f"    {'proportion difference (pp)':<26s} {sd_p:>9.4f} {se_p:>9.4f} "
      f"{se_p/sd_p:>7.2f} {(R.p_hi-R.p_lo).mean():>9.3f} {cov_p:>9.1%}", flush=True)
print(f"\n    → the two estimands are calibrated "
      f"{'DIFFERENTLY' if abs(ratio - se_p/sd_p) > 0.15 else 'the same way'} on identical "
      f"data.\n      Validation is therefore a property of (estimand, estimator, dependence), "
      f"not of the function.", flush=True)

print("\n" + bar, flush=True)
print("  WHAT THIS COSTS THE GATES", flush=True)
print(bar, flush=True)
need = 0.30
now = (R.lo > need).mean()
hw = 1.96 * sd_mc
would = (R.est - hw > need).mean()
print(f"    δ* = {need:.2f}pp · true δ = {DELTA:+.2f}pp", flush=True)
print(f"    MATERIALITY passes now                          {now:>6.0%}", flush=True)
print(f"    ... if the interval were the Monte-Carlo truth  {would:>6.0%}   "
      f"(half-width {hw:.3f}pp vs {R.width.mean()/2:.3f}pp)", flush=True)
print(f"    → the achievable ceiling on this population is {would:.0%}; anything the "
      f"estimator\n      recovers is bounded by that, and the gap to {now:.0%} is what an "
      f"inference fix could buy.", flush=True)

R.to_csv("inference_diag.csv", index=False)
print(f"\n  wrote inference_diag.csv · {len(R)} replications", flush=True)
print("  DIAGNOSIS ONLY — inference_v1 is unchanged, and the remedy depends on which of the "
      "four\n  numbers above is out of place.", flush=True)
print("\nDONE", flush=True)
