"""NULL_SHAPE_DIAGNOSTIC_V1 — is the studentized family pivotal? EXPLORATORY.

DIAGNOSTIC ONLY. No new test, no historical theta.

THE QUESTION

Analytic studentization fixed the scale problem: predicted_sd reconstructs each claim's null
scale at R^2 0.98, and it is permutation-invariant so it carries no winner's curse. Yet the
max-Z 95th percentile came out at 10.46 on 200 permutations. For 840 tests that are pivotal —
each Z roughly standard, whatever the dependence — the p95 of the max is about 3.7, and
DEPENDENCE ONLY PUSHES IT DOWN, never up. So 10.46 cannot be a multiplicity number. Something
about the shape of the standardized null is left.

SEPARATING SHAPE FROM DEPENDENCE

Two references are computed against the observed max-Z:

    gaussianized   each claim's Z column is rank-transformed to exact Gaussian marginals,
                   which preserves the rank-dependence across claims untouched. The gap
                   between observed and gaussianized is PURE MARGINAL TAIL SHAPE.
    independent    p95 of the max of k iid standard normals, closed form. The gap between
                   gaussianized and independent is PURE DEPENDENCE, and should be negative.

That decomposition is the whole point: a single number 10.46 cannot say which of the two it
is, and the remedy differs completely.

WHY THE DENOMINATOR IS NOT ESTIMATED HERE

predicted_sd comes from NULL_VARIANCE_DECOMPOSITION_V1, an analytic identity over block
geometry and block Y multisets, both invariant to the permutation. So Z is not
self-standardized and the extreme permutations do not inflate their own denominator. The
earlier self-standardized 7.61 was biased DOWNWARD for exactly that reason.

TAIL RESOLUTION IS BOUNDED BY 1/1999

P(Z>5) can only be resolved to 5e-4. Anything rarer than that reads as zero here and is not
evidence of a light tail. Hence: exploratory geometry, not a calibrated tail estimate.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, resource, sys, tempfile, time                   # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R, t5_compute_qual as Q, t5_jit_engine as JE    # noqa: E402

SPEC = json.load(open("T5_CAPABILITY_Q50_V1.json"))
SEAL = json.load(open("T5_SEQUENCE_ESTIMAND_SEAL_V1.json"))
DATA = os.path.join(os.path.dirname(HERE), "data")
VAR = os.path.join(DATA, "t5_null_variance_per_claim.parquet")
MAT = os.path.join(DATA, "t5_null_shape_theta_perm.parquet")
PER = os.path.join(DATA, "t5_null_shape_per_claim.parquet")
OUT = os.path.join(HERE, "T5_NULL_SHAPE_DIAGNOSTIC_V1.json")

N_PERM = 1999
RNG_OUTER = [20260825, 0, 0]
RNG_INNER = [20260826, 0]


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def atomic_parquet(df, path):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    os.close(fd)
    df.to_parquet(tmp, index=False)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


def ndtri(p):
    """Inverse standard normal CDF, Acklam's rational approximation (|err| < 1.2e-9)."""
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    p = np.asarray(p, float); x = np.empty_like(p)
    lo, hi = p < 0.02425, p > 1 - 0.02425
    mid = ~(lo | hi)
    q = np.sqrt(-2 * np.log(p[lo]))
    x[lo] = (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q = np.sqrt(-2 * np.log(1 - p[hi]))
    x[hi] = -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q = p[mid] - 0.5; r = q * q
    x[mid] = (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
             (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
    return x


def main():
    t0 = time.time()
    print(f"NULL_SHAPE_DIAGNOSTIC_V1 · δ=0 · 1 null world · {N_PERM} perms · "
          "analytic SE fixed", flush=True)
    V = pd.read_parquet(VAR).sort_values("j").reset_index(drop=True)
    sd_a = V.predicted_sd.to_numpy()
    if len(V) != 840 or not np.all(np.isfinite(sd_a)) or sd_a.min() <= 0:
        raise RuntimeError("analytic predicted_sd unusable")

    y, b, nb, S, claims, order = R.build_state(verbose=False)
    flat = Q.FlatState(claims); del claims
    import gc; gc.collect()
    print(f"  state ready · index {flat.nbytes/1e6:.0f} MB · RSS {rss():.2f} GB", flush=True)
    for nm, ok in (("estimand_digest", SPEC["estimand_digest"] == SEAL["seal_digest"]),
                   ("claim_order_hash", SPEC["claim_order_hash"] == SEAL["claim_order_hash"]),
                   ("population_hash", SPEC["population_hash"] == SEAL["population_hash"]),
                   ("k_840", SPEC["k"] == 840 == len(order)),
                   ("fastmath_false", JE._theta6.targetoptions.get("fastmath") is False)):
        print(f"    {'✓' if ok else '✗'} {nm}", flush=True)
        if not ok:
            raise RuntimeError(f"identity gate failed: {nm}")

    DL = np.array([0.0]); z = np.zeros(len(y))
    J = JE.gate(flat, flat.theta, y, z, DL)
    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    idxf = np.arange(len(y), dtype=float)
    base = R.perm_fast(y, bo, st, np.random.default_rng(RNG_OUTER))

    TH = np.empty((N_PERM, 840)); tp = time.time()
    for p in range(N_PERM):
        pm = R.perm_fast(idxf, bo, st,
                         np.random.default_rng(RNG_INNER + [p])).astype(np.int64)
        TH[p] = J.theta6(base[pm], z, DL)[0]
        if (p + 1) % 250 == 0:
            print(f"    perm {p+1}/{N_PERM} · {(time.time()-tp)/60:.1f}m", flush=True)
    atomic_parquet(pd.DataFrame(TH, columns=[f"j{j}" for j in range(840)]), MAT)

    # ── standardize with the ANALYTIC SE, centre on the permutation mean ─────
    ctr = TH.mean(0)
    Z = (TH - ctr) / sd_a
    mu, sg = Z.mean(0), Z.std(0, ddof=1)
    zc = Z - mu
    skew = (zc ** 3).mean(0) / sg ** 3
    exk = (zc ** 4).mean(0) / sg ** 4 - 3.0
    zmax = Z.max(axis=1); am = Z.argmax(axis=1)
    amc = np.bincount(am, minlength=840)

    P = pd.DataFrame(dict(
        j=np.arange(840), claim_id=V.claim_id, support=V.support, family=V.family,
        share_blocks_nt1=V.share_blocks_nt1, effective_blocks=V.effective_blocks,
        block_tail_ratio=V.weighted_block_MFE_scale / V.weighted_block_IQR,
        analytic_sd=sd_a, sd_Z=sg,
        q90=np.percentile(Z, 90, axis=0), q95=np.percentile(Z, 95, axis=0),
        q99=np.percentile(Z, 99, axis=0), max_Z=Z.max(0),
        skew=skew, excess_kurtosis=exk,
        P_gt3=(Z > 3).mean(0), P_gt4=(Z > 4).mean(0), P_gt5=(Z > 5).mean(0),
        argmax_Z_count=amc, argmax_Z_share=amc / N_PERM))
    atomic_parquet(P, PER)

    # ── shape vs dependence ─────────────────────────────────────────────────
    rk = np.argsort(np.argsort(Z, axis=0), axis=0) + 1
    ZG = ndtri((rk - 0.5) / N_PERM)                # Gaussian marginals, dependence preserved
    zgmax = ZG.max(axis=1)
    k = 840
    indep_p95 = float(ndtri(np.array([0.95 ** (1.0 / k)]))[0])
    obs95, g95 = float(np.percentile(zmax, 95)), float(np.percentile(zgmax, 95))
    # effective number of independent tests implied by the gaussianized max
    import math
    ke = float(np.log(0.95) / np.log(max(1e-12, min(1 - 1e-12,
              0.5 * (1 + math.erf(g95 / math.sqrt(2)))))))

    top = P.sort_values("argmax_Z_count", ascending=False).head(20)
    cum = top.argmax_Z_count.cumsum().to_numpy() / N_PERM
    winners = int((P.argmax_Z_count > 0).sum())

    def corr(a, bb):
        return round(float(pd.Series(a).corr(pd.Series(bb), method="spearman")), 3)

    rep = dict(
        spec_id="NULL_SHAPE_DIAGNOSTIC_V1", status="EXPLORATORY_DIAGNOSTIC_ONLY",
        estimand_digest=SPEC["estimand_digest"], k=k, n_perm=N_PERM, delta_pp=0.0,
        theta_perm_hash=hashlib.sha256(TH.tobytes()).hexdigest()[:16],
        denominator="analytic predicted_sd from NULL_VARIANCE_DECOMPOSITION_V1 "
                    "(permutation-invariant; not self-standardized)",
        max_Z=dict(observed_p95=round(obs95, 3), observed_p50=round(float(np.percentile(zmax, 50)), 3),
                   observed_max=round(float(zmax.max()), 3),
                   gaussianized_p95=round(g95, 3), independent_gaussian_p95=round(indep_p95, 3),
                   shape_excess=round(obs95 - g95, 3), dependence_effect=round(g95 - indep_p95, 3),
                   implied_effective_tests=round(ke, 1),
                   reading="observed - gaussianized is pure marginal tail shape; "
                           "gaussianized - independent is pure dependence and should be <= 0"),
        marginal_shape=dict(
            sd_Z={f"p{q}": round(float(np.percentile(sg, q)), 3) for q in (10, 50, 90)},
            excess_kurtosis={f"p{q}": round(float(np.percentile(exk, q)), 2)
                             for q in (10, 50, 90, 99)},
            skew={f"p{q}": round(float(np.percentile(skew, q)), 2) for q in (10, 50, 90)},
            P_gt3=dict(median=round(float(np.median(P.P_gt3)), 5), gaussian=0.00135),
            P_gt4=dict(median=round(float(np.median(P.P_gt4)), 5), gaussian=3.17e-5),
            P_gt5=dict(median=round(float(np.median(P.P_gt5)), 5), gaussian=2.87e-7,
                       resolution_floor=round(1.0 / N_PERM, 5))),
        max_Z_concentration=dict(claims_ever_winning=winners,
                                 top10_share=round(float(cum[9]), 3),
                                 top20_share=round(float(cum[-1]), 3),
                                 top=top[["j", "claim_id", "support", "share_blocks_nt1",
                                          "excess_kurtosis", "max_Z", "argmax_Z_share"]]
                                 .round(3).to_dict("records")),
        kurtosis_correlates=dict(
            vs_share_blocks_nt1=corr(exk, P.share_blocks_nt1),
            vs_log_support=corr(exk, np.log(P.support)),
            vs_log_effective_blocks=corr(exk, np.log(P.effective_blocks)),
            vs_block_tail_ratio=corr(exk, P.block_tail_ratio)),
        sd_Z_check="sd_Z near 1.0 confirms the analytic SE is the right SCALE; any max-Z "
                   "excess above the gaussianized reference is then shape, not scale.",
        tail_resolution=f"P(Z>x) resolves only to 1/{N_PERM} = {1/N_PERM:.2e}; rarer events "
                        "read as zero and that is not evidence of a light tail.",
        historical_theta="NOT COMPUTED, NOT EXPOSED",
        minutes=round((time.time() - t0) / 60, 1))
    json.dump(rep, open(OUT, "w"), indent=2, default=str)

    m = rep["max_Z"]
    print(f"\n  MAX-Z  observed p95 {m['observed_p95']:.2f} · p50 {m['observed_p50']:.2f} "
          f"· max {m['observed_max']:.2f}")
    print(f"    gaussianized marginals, same dependence   {m['gaussianized_p95']:.2f}")
    print(f"    independent gaussian, k=840               {m['independent_gaussian_p95']:.2f}")
    print(f"    → marginal TAIL SHAPE excess              {m['shape_excess']:+.2f}")
    print(f"    → DEPENDENCE effect                       {m['dependence_effect']:+.2f}"
          f"   (implied effective tests {m['implied_effective_tests']:.0f})")
    ms = rep["marginal_shape"]
    print(f"\n  MARGINAL SHAPE  sd(Z) p10/p50/p90 " +
          "/".join(f"{ms['sd_Z'][f'p{q}']:.2f}" for q in (10, 50, 90)))
    print(f"    excess kurtosis p10/p50/p90/p99 " +
          "/".join(f"{ms['excess_kurtosis'][f'p{q}']:.1f}" for q in (10, 50, 90, 99)))
    print(f"    P(Z>3) median {ms['P_gt3']['median']:.5f} vs gaussian 0.00135  "
          f"({ms['P_gt3']['median']/0.00135:.1f}x)")
    print(f"    P(Z>4) median {ms['P_gt4']['median']:.5f} vs gaussian 0.0000317")
    c = rep["max_Z_concentration"]
    print(f"\n  MAX-Z CONCENTRATION  {c['claims_ever_winning']} of 840 ever win · "
          f"top10 {c['top10_share']:.1%} · top20 {c['top20_share']:.1%}")
    print("       j  claim_id                        support  nt1sh  exkurt   argmax")
    for r in top.head(10).itertuples():
        print(f"      {int(r.j):>4}  {r.claim_id:<30} {int(r.support):>7} "
              f"{r.share_blocks_nt1:>6.2f} {r.excess_kurtosis:>7.1f} {r.argmax_Z_share:>7.1%}")
    print(f"\n  KURTOSIS CORRELATES (spearman) " +
          "  ".join(f"{k2.replace('vs_',''):s} {v:+.3f}"
                    for k2, v in rep["kurtosis_correlates"].items()))
    print(f"\n  WROTE {OUT}\n        {PER}\n        {MAT}", flush=True)


if __name__ == "__main__":
    main()
