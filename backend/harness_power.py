"""Can the engine say YES? Operating characteristics of our own research pipeline.

Two days were spent building a machine that is very good at saying NO, and not one minute
checking whether it can say YES. A framework strict enough that nothing passes is
indistinguishable from a correct one — both produce "no findings" — and the only thing that
tells them apart is a planted effect of known size.

So: inject a known δ, run the real pipeline, and measure four things nobody has ever measured
about our own engine.

    power(δ)     P(the verdict passes | a true effect of δ exists)
    bias         E[δ̂] − δ            — is the estimator right, not just alarmed
    coverage     P(δ ∈ CI)           — do our 95% intervals contain the truth 95% of the time
    gate cost    Shapley over L1/L2/L3 — which of OUR OWN RULES costs the most sensitivity

WHERE the injection goes matters more than its size. Adding δ to the outcome just before the
verdict tests the last 5% of the pipeline. Adding it to the bars from the entry date onward
would shift the entry price too, so the return effect could vanish entirely — and worse, it
would change RSI/ATR/state on every LATER bar of the same ticker, altering which rows are even
eligible. The treatment would leak into sample membership and δ would stop being ground truth.

The honest construction is a SHADOW FUTURE. Everything knowable at entry is untouched; only
the path used to compute outcomes is modified, and it is modified for the treated arm and the
control arm through exactly the same code, so a difference in machinery cannot masquerade as
an effect. That last point is not hypothetical: an unmatched baseline is precisely what
manufactured a +1.3pp phantom two days ago.

Three predictions, written before the first run:

    1  empirical coverage will come in BELOW nominal, around 88-92%, because our dates are
       wildly unequal in size (300 events one day, 3 the next) and multinomial reweighting is
       optimistic under that imbalance
    2  L1 — "positive in 4 of 6 years" — will be the largest sensitivity tax, not n
    3  the primary clustered inference will be anti-conservative: empirical size above 5%

If any of these is wrong the file says so; they are recorded here so the interpretation cannot
be adjusted afterwards.
"""
from __future__ import annotations

import os
import sys
import math
from itertools import product

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
import sources as srcs                                          # noqa: E402
from analysis_kit import bootstrap_ci_clustered, effective_n     # noqa: E402

HOR = 10                       # bars held, the horizon everything is measured at
DELTA_STAR = 0.30              # pp — the smallest effect worth acting on (2× round-trip cost)
N_SIM = 120
N_BOOT = 200
SEED = 0
pd.set_option("display.width", 200)


# ── the frame, loaded ONCE ───────────────────────────────────────────────────
def load(min_price=5.0, min_dv=3_000_000):
    df = srcs.bars("1d", columns=("t_sig", "z_sig", "rsi_14"), min_price=min_price,
                   min_dollar_vol=min_dv, verbose=False)
    tk = df["ticker"].to_numpy()
    c = df["close"].to_numpy(float)
    o = df["open"].to_numpy(float)
    n = len(df)
    ent = np.where(np.r_[tk[1:] == tk[:-1], False], np.r_[o[1:], np.nan], np.nan)
    ok_h = np.r_[tk[HOR:] == tk[:-HOR], np.zeros(HOR, bool)]
    exitp = np.where(ok_h, np.r_[c[HOR:], np.full(HOR, np.nan)], np.nan)
    base = (exitp / ent - 1) * 100
    T = df["t_sig"].fillna("").astype(str).to_numpy()
    Z = df["z_sig"].fillna("").astype(str).to_numpy()
    tok = np.where((T != "") & (T != "nan"), T, Z)
    keep = np.isfinite(base) & np.isfinite(df["rsi_14"].to_numpy(float))
    return dict(ret=base[keep], tok=tok[keep], date=df["date"].astype(str).str[:10].to_numpy()[keep],
                yr=pd.to_datetime(df["date"]).dt.year.to_numpy()[keep],
                close=c[keep], rsi=df["rsi_14"].to_numpy(float)[keep],
                ticker=tk[keep])


print("loading the frame once — every simulation reuses it", flush=True)
D = load()
print(f"  {len(D['ret']):,} trades · {len(np.unique(D['date'])):,} dates\n", flush=True)

# a fixed, pre-chosen exposure: one token, so the cell is realistic in size and clustering
EXPOSED = D["tok"] == "T6"
print(f"  exposure = T6 · {EXPOSED.sum():,} treated ({EXPOSED.mean():.1%}) · "
      f"{len(np.unique(D['date'][EXPOSED])):,} distinct dates", flush=True)
# The control arm is subsampled to 5x the treated size, once, with a fixed seed. Resampling
# 2.77M rows 250 times per replication took hours per delta — and a suite that runs for hours
# is a suite nobody runs, which protects nothing. 5x is what a matched control uses anyway.
_rs = np.random.default_rng(SEED)
_ctl_all = np.where(~EXPOSED)[0]
CTL = np.sort(_rs.choice(_ctl_all, size=min(len(_ctl_all), 5 * int(EXPOSED.sum())),
                         replace=False))
TRT_POOL = np.where(EXPOSED)[0]
CTL_ORDER = np.argsort(D["ret"][CTL], kind="stable")   # the control never changes
BASE_MED = np.median(D["ret"][CTL])
print(f"  control arm subsampled to {len(CTL):,} (5x treated) · median at {HOR} bars "
      f"{BASE_MED:+.3f}%\n", flush=True)


def _wmedian_sorted(v_sorted, w_sorted):
    """Weighted median on ALREADY SORTED values.

    The sort is the expensive part and the values do not change between bootstrap draws —
    only the weights do. Sorting inside the loop meant 200 argsorts of 274k rows per
    replication, which is what turned a validation suite into an overnight job. Sort once
    per replication, then each draw is a cumsum and a searchsorted.
    """
    c = np.cumsum(w_sorted)
    if c[-1] <= 0:
        return np.nan
    return float(v_sorted[np.searchsorted(c, c[-1] / 2.0)])


def simulate(delta: float, rng) -> dict:
    """One replication: plant δ in a SHADOW future, then run the real measurement.

    Which dates carry the treatment is resampled every replication, so the answer is not one
    lucky cell, and the clustering of the exposure is preserved. Both arms go through the same
    code and differ only by δ — an unmatched baseline is exactly what manufactured a +1.3pp
    phantom two days ago.
    """
    dts = np.unique(D["date"][TRT_POOL])
    pick = set(rng.choice(dts, size=int(len(dts) * 0.7), replace=False))
    trt = TRT_POOL[np.isin(D["date"][TRT_POOL], list(pick))]

    a = D["ret"][trt] + delta                 # shadow future: outcomes only
    b = D["ret"][CTL]
    da, db = D["date"][trt], D["date"][CTL]
    est = np.median(a) - np.median(b)

    uq, gi = np.unique(np.r_[da, db], return_inverse=True)
    na = len(a)
    # sort ONCE per replication, not once per bootstrap draw — the values are fixed across
    # draws and only the weights change. Sorting inside the loop is what made the first
    # version an overnight job.
    oa = np.argsort(a, kind="stable")
    a_s, b_s = a[oa], b[CTL_ORDER]
    gi_a, gi_b = gi[:na][oa], gi[na:][CTL_ORDER]
    p = np.full(len(uq), 1 / len(uq))
    diffs = np.empty(N_BOOT)
    for k in range(N_BOOT):
        w = rng.multinomial(len(uq), p).astype(float)
        diffs[k] = _wmedian_sorted(a_s, w[gi_a]) - _wmedian_sorted(b_s, w[gi_b])
    lo, hi = np.percentile(diffs[np.isfinite(diffs)], [2.5, 97.5])

    yr_t, yr_c = D["yr"][trt], D["yr"][CTL]
    per = []
    for y in np.unique(D["yr"]):
        mt, mc = yr_t == y, yr_c == y
        if mt.sum() > 20 and mc.sum() > 20:
            per.append(np.median(a[mt]) - np.median(b[mc]))
    per = np.asarray(per)
    eff = effective_n(a, da)

    L1 = bool(len(per) and (per > 0).sum() >= len(per) - 2 and per.min() >= -2)
    L2 = bool(est >= 1.0 and (lo > 0 or hi < 0))
    L3 = bool(eff["n_eff"] >= 80)
    return dict(est=est, lo=lo, hi=hi, L1=L1, L2=L2, L3=L3,
                n_eff=int(eff["n_eff"]), n=len(a))


def run_grid(deltas, n_sim=N_SIM):
    rows = []
    for d in deltas:
        rng = np.random.default_rng(SEED + int(d * 1000))
        for i in range(n_sim):
            r = simulate(d, rng)
            r["delta"] = d
            rows.append(r)
        R = pd.DataFrame([x for x in rows if x["delta"] == d])
        passed = (R.L1 & R.L2 & R.L3).mean()
        print(f"  δ = {d:+.2f}pp   power {passed:>6.1%}   est {R.est.mean():>+6.3f} "
              f"(bias {R.est.mean()-d:>+6.3f})   coverage "
              f"{((R.lo <= d) & (R.hi >= d)).mean():>6.1%}   n_eff {R.n_eff.median():>7,.0f}",
              flush=True)
    return pd.DataFrame(rows)


print("=" * 118, flush=True)
print(f"  OPERATING CHARACTERISTICS · {N_SIM} replications per δ · horizon {HOR} bars",
      flush=True)
print("=" * 118, flush=True)
GRID = [0.0, DELTA_STAR, 2 * DELTA_STAR, 4 * DELTA_STAR]
R = run_grid(GRID)
R.to_csv("harness_power.csv", index=False)

print("\n" + "=" * 118, flush=True)
print("  THE THREE PREDICTIONS", flush=True)
print("=" * 118, flush=True)
z = R[R.delta == 0.0]
size = ((z.lo > 0) | (z.hi < 0)).mean()
cov = R[R.delta > 0].apply(lambda r: r.lo <= r.delta <= r.hi, axis=1).mean()
print(f"  1 · coverage at δ>0: {cov:.1%}   (predicted 88-92%, nominal 95%)  "
      f"{'✅ as predicted' if 0.88 <= cov <= 0.92 else '❌ prediction wrong'}", flush=True)
print(f"  3 · size at δ=0:     {size:.1%}   (predicted >5%)                 "
      f"{'✅ as predicted' if size > 0.05 else '❌ prediction wrong'}", flush=True)
print(f"      engine FPR at δ=0 (full verdict): {(z.L1 & z.L2 & z.L3).mean():.2%}  "
      f"— a conjunctive verdict is far stricter than its primary test", flush=True)

print("\n" + "=" * 118, flush=True)
print("  GATE SENSITIVITY — Shapley over L1/L2/L3, computed from stored outcomes", flush=True)
print("=" * 118, flush=True)
GATES = ["L1", "L2", "L3"]
for d in GRID[1:]:
    S = R[R.delta == d]
    def power_of(subset):
        if not subset:
            return 1.0
        m = np.ones(len(S), bool)
        for g in subset:
            m &= S[g].to_numpy()
        return m.mean()
    shap = {g: 0.0 for g in GATES}
    for g in GATES:
        others = [x for x in GATES if x != g]
        for k in range(len(others) + 1):
            for sub in [others[:k]] if k in (0, len(others)) else [others[:k], others[k:]]:
                w = (math.factorial(len(sub)) *
                     math.factorial(len(GATES) - len(sub) - 1) /
                     math.factorial(len(GATES)))
                shap[g] += w * (power_of(list(sub)) - power_of(list(sub) + [g]))
    full = power_of(GATES)
    print(f"  δ = {d:+.2f}pp   full power {full:>6.1%}", flush=True)
    for g in GATES:
        print(f"      {g} costs {shap[g]*100:>6.1f}pp of sensitivity  "
              f"(raw failure rate {1-S[g].mean():>5.1%})", flush=True)
    top = max(shap, key=shap.get)
    print(f"      → largest tax: {top}"
          + ("   ✅ prediction 2 holds" if top == "L1" else "   ❌ prediction 2 wrong"),
          flush=True)

print("\n" + "=" * 118, flush=True)
print("  MDE — the smallest effect this engine can see", flush=True)
print("=" * 118, flush=True)
pw = R.groupby("delta").apply(lambda g: (g.L1 & g.L2 & g.L3).mean(), include_groups=False)
print("   " + " · ".join(f"δ={k:+.2f} → {v:.0%}" for k, v in pw.items()), flush=True)
hit = pw[pw >= 0.8]
if len(hit):
    print(f"\n  MDE@80% ≤ {hit.index[0]:+.2f}pp", flush=True)
else:
    print(f"\n  MDE@80% > {GRID[-1]:+.2f}pp — the engine cannot reach 80% power anywhere on "
          f"this grid.\n  Every NULL at or below that size is UNRESOLVED, not evidence of "
          f"absence.", flush=True)
print("\n  NOTE: this is MDE for an ADDITIVE CONSTANT effect. An effect concentrated in a few "
      "regimes,\n  or living only in an interaction, is a different shape and this number does "
      "not describe it.", flush=True)
print("\nDONE", flush=True)
