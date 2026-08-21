"""RANK_NULL_GEOMETRY_DIAGNOSTIC_V1 — does a bounded block score remove the heavy tail?

DIAGNOSTIC ONLY. No new test is registered, no historical theta is computed or exposed.

WHAT IS HELD FIXED

The SAME 1,999 permutation mappings as NULL_SHAPE_DIAGNOSTIC_V1 (identical RNG streams), the
same frozen population, blocks, claims and weights. Only the block-level contrast changes:

    raw     Delta_b = median(Y_T) - median(Y_C)        unbounded, inherits the block's tail
    rank    R_b     = U_b - 1/2                        bounded in [-1/2, +1/2]

So any difference in tail geometry is attributable to the statistic, not to the resampling.

THE SCORE AND ITS EXACT NULL MOMENTS

    U_b = [ #(Y_T > Y_C) + 1/2 #(Y_T = Y_C) ] / (n_t n_c)

Under the within-block permutation null, E[U_b] = 1/2 exactly, and

    Var(U_b) = [ (N+1) - sum(t^3 - t) / (N(N-1)) ] / (12 n_t n_c),   N = n_t + n_c

with the tie-group sizes t taken from the block's own Y multiset. Both are exact combinatorial
facts, not approximations: no MFE magnitude enters, and no calibration permutations are
needed for the denominator. Because the multiset is invariant under within-block permutation,
the tie correction is a property of the block, computed once.

    Var(R_s) = sum_b w_sb^2 Var(U_b)          blocks independent under permutation

COMPUTED VIA MIDRANKS, GATED AGAINST BRUTE FORCE

U is evaluated with the rank-sum identity U = (sum of treated midranks - n_t(n_t+1)/2) /
(n_t n_c). Midranks within a block are permutation-invariant, so they are computed once and
merely re-indexed. The identity is checked against a direct pairwise count on random segments
before the run; three engine rewrites earlier in this project looked obviously right and were
wrong, so "obviously right" is not accepted as evidence.

WHAT WOULD COUNT AS THE BOUNDED SCORE WORKING

Not p95(max Z) ~= 3.48. Boundedness is not finite-sample normality — discreteness, unequal
weights and n_t = 1 all survive it. The question is narrower: does the mechanism that put
P(Z>4) at ~205x the Gaussian rate disappear.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, resource, sys, tempfile, time                   # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R, t5_compute_qual as Q                    # noqa: E402
import t5_null_shape as NS                                             # noqa: E402

SPEC = json.load(open("T5_CAPABILITY_Q50_V1.json"))
SEAL = json.load(open("T5_SEQUENCE_ESTIMAND_SEAL_V1.json"))
DATA = os.path.join(os.path.dirname(HERE), "data")
VAR = os.path.join(DATA, "t5_null_variance_per_claim.parquet")
MAT = os.path.join(DATA, "t5_rank_null_R_perm.parquet")
PER = os.path.join(DATA, "t5_rank_null_per_claim.parquet")
OUT = os.path.join(HERE, "T5_RANK_NULL_GEOMETRY_DIAGNOSTIC_V1.json")

N_PERM = NS.N_PERM                      # 1999 — identical to the raw-shape diagnostic
RNG_OUTER = NS.RNG_OUTER                # identical mappings, so only the statistic differs
RNG_INNER = NS.RNG_INNER


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def atomic_parquet(df, path):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    os.close(fd)
    df.to_parquet(tmp, index=False)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


def midranks_and_ties(v):
    """Average ranks within one block, and sum(t^3 - t) over its tie groups."""
    n = len(v)
    o = np.argsort(v, kind="stable")
    s = v[o]
    mr = np.empty(n)
    tie = 0.0
    i = 0
    while i < n:
        j = i
        while j + 1 < n and s[j + 1] == s[i]:
            j += 1
        m = j - i + 1
        mr[o[i:j + 1]] = (i + j) / 2.0 + 1.0        # 1-based average rank
        if m > 1:
            tie += m ** 3 - m
        i = j + 1
    return mr, tie


def main():
    t0 = time.time()
    print(f"RANK_NULL_GEOMETRY_DIAGNOSTIC_V1 · same {N_PERM} mappings · bounded block score",
          flush=True)
    y, b, nb, S, claims, order = R.build_state(verbose=False)
    # block id per (claim, block) segment, in FlatState's own iteration order
    cb = np.concatenate([np.asarray(ob) for ob, rt, rc, w in claims])
    flat = Q.FlatState(claims); del claims
    import gc; gc.collect()
    nt = np.diff(flat.segT).astype(np.int64)
    nc = np.diff(flat.segC).astype(np.int64)
    if len(cb) != len(nt):
        raise RuntimeError("segment order mismatch between cb and FlatState")
    print(f"  state ready · segments {len(nt):,} · index {flat.nbytes/1e6:.0f} MB "
          f"· RSS {rss():.2f} GB", flush=True)
    for nm, ok in (("estimand_digest", SPEC["estimand_digest"] == SEAL["seal_digest"]),
                   ("claim_order_hash", SPEC["claim_order_hash"] == SEAL["claim_order_hash"]),
                   ("population_hash", SPEC["population_hash"] == SEAL["population_hash"]),
                   ("k_840", SPEC["k"] == 840 == len(order))):
        print(f"    {'✓' if ok else '✗'} {nm}", flush=True)
        if not ok:
            raise RuntimeError(f"identity gate failed: {nm}")

    # ── the null world: identical construction and identical seed ───────────
    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    idxf = np.arange(len(y), dtype=float)
    base = R.perm_fast(y, bo, st, np.random.default_rng(RNG_OUTER))

    # ── midranks within block + tie sums (permutation-invariant) ────────────
    mr = np.empty(len(base)); tie = np.zeros(nb)
    for k2, (a, c) in enumerate(zip(st[:-1], st[1:])):
        rows = bo[a:c]
        m, t_ = midranks_and_ties(base[rows])
        mr[rows] = m; tie[b[rows[0]]] = t_
    nties = int((tie > 0).sum())
    print(f"  midranks built · blocks with ties {nties:,}/{nb:,} ({nties/nb:.1%})", flush=True)

    N = (nt + nc).astype(float)
    varU = ((N + 1.0) - tie[cb] / (N * (N - 1.0))) / (12.0 * nt * nc)
    varR = np.add.reduceat(flat.w ** 2 * varU, flat.coff[:-1])
    sdR = np.sqrt(varR)
    half = nt * (nt + 1.0) / 2.0
    denom = (nt * nc).astype(float)

    def rank_stat(mrp):
        s = np.add.reduceat(mrp[flat.T], flat.segT[:-1])
        U = (s - half) / denom
        return np.add.reduceat(flat.w * (U - 0.5), flat.coff[:-1])

    # ── equivalence gate: rank-sum identity vs direct pairwise count ────────
    g = np.random.default_rng(99)
    pm0 = R.perm_fast(idxf, bo, st, np.random.default_rng(RNG_INNER + [0])).astype(np.int64)
    yv = base[pm0]; mrp0 = mr[pm0]
    worst = 0.0
    for s_ in g.choice(len(nt), 400, replace=False):
        T = flat.T[flat.segT[s_]:flat.segT[s_ + 1]]
        C = flat.C[flat.segC[s_]:flat.segC[s_ + 1]]
        a_, b_ = yv[T][:, None], yv[C][None, :]
        u_direct = ((a_ > b_).sum() + 0.5 * (a_ == b_).sum()) / (len(T) * len(C))
        u_fast = (mrp0[T].sum() - half[s_]) / denom[s_]
        worst = max(worst, abs(u_direct - u_fast))
    if worst > 1e-10:
        raise RuntimeError(f"rank-sum identity != pairwise count, max |diff| {worst:.3e}")
    print(f"  equivalence PASS · 400 segments · max |diff| {worst:.2e}", flush=True)

    # ── 1999 permutations ───────────────────────────────────────────────────
    RR = np.empty((N_PERM, 840)); tp = time.time()
    for p in range(N_PERM):
        pm = R.perm_fast(idxf, bo, st,
                         np.random.default_rng(RNG_INNER + [p])).astype(np.int64)
        RR[p] = rank_stat(mr[pm])
        if (p + 1) % 250 == 0:
            print(f"    perm {p+1}/{N_PERM} · {(time.time()-tp)/60:.1f}m", flush=True)
    atomic_parquet(pd.DataFrame(RR, columns=[f"j{j}" for j in range(840)]), MAT)

    Z = RR / sdR                                   # analytic denominator, no calibration perms
    mu, sg = Z.mean(0), Z.std(0, ddof=1)
    zc = Z - mu
    skew = (zc ** 3).mean(0) / sg ** 3
    exk = (zc ** 4).mean(0) / sg ** 4 - 3.0
    zmax = Z.max(axis=1); amc = np.bincount(Z.argmax(axis=1), minlength=840)

    V = pd.read_parquet(VAR).sort_values("j").reset_index(drop=True)
    P = pd.DataFrame(dict(
        j=np.arange(840), claim_id=V.claim_id, support=V.support, family=V.family,
        share_blocks_nt1=V.share_blocks_nt1, analytic_sd_R=sdR,
        mean_Z=mu, sd_Z=sg, q95=np.percentile(Z, 95, axis=0), max_Z=Z.max(0),
        skew=skew, excess_kurtosis=exk,
        P_gt3=(Z > 3).mean(0), P_gt4=(Z > 4).mean(0), P_gt5=(Z > 5).mean(0),
        argmax_Z_count=amc, argmax_Z_share=amc / N_PERM))
    atomic_parquet(P, PER)

    rk = np.argsort(np.argsort(Z, axis=0), axis=0) + 1
    ZG = NS.ndtri((rk - 0.5) / N_PERM)
    obs95 = float(np.percentile(zmax, 95)); g95 = float(np.percentile(ZG.max(axis=1), 95))
    indep95 = float(NS.ndtri(np.array([0.95 ** (1.0 / 840)]))[0])
    top = P.sort_values("argmax_Z_count", ascending=False).head(20)
    cum = top.argmax_Z_count.cumsum().to_numpy() / N_PERM

    RAW = json.load(open("T5_NULL_SHAPE_DIAGNOSTIC_V1.json"))
    r_ms = RAW["marginal_shape"]; r_mz = RAW["max_Z"]
    cmp_ = dict(
        max_Z_p95=dict(raw=r_mz["observed_p95"], rank=round(obs95, 3)),
        gaussianized_p95=dict(raw=r_mz["gaussianized_p95"], rank=round(g95, 3)),
        median_excess_kurtosis=dict(raw=r_ms["excess_kurtosis"]["p50"],
                                    rank=round(float(np.median(exk)), 2)),
        P_gt3_median=dict(raw=r_ms["P_gt3"]["median"], rank=round(float(np.median(P.P_gt3)), 5),
                          gaussian=0.00135),
        P_gt4_median=dict(raw=r_ms["P_gt4"]["median"], rank=round(float(np.median(P.P_gt4)), 5),
                          gaussian=3.17e-5),
        top20_max_share=dict(raw=RAW["max_Z_concentration"]["top20_share"],
                             rank=round(float(cum[-1]), 3)))

    rep = dict(
        spec_id="RANK_NULL_GEOMETRY_DIAGNOSTIC_V1", status="EXPLORATORY_DIAGNOSTIC_ONLY",
        estimand_digest=SPEC["estimand_digest"], k=840, n_perm=N_PERM,
        mappings="identical RNG streams to NULL_SHAPE_DIAGNOSTIC_V1 — only the block "
                 "contrast differs",
        block_score="U_b = [#(Y_T>Y_C) + 0.5#(Y_T=Y_C)]/(n_t n_c); R_b = U_b - 1/2",
        denominator="exact combinatorial Var(U_b) with rank-sum tie correction; no MFE "
                    "magnitude and no calibration permutations enter it",
        blocks_with_ties=dict(n=nties, share=round(nties / nb, 4)),
        equivalence_gate=dict(segments=400, max_abs_diff=float(worst), passed=True),
        sd_Z={f"p{q}": round(float(np.percentile(sg, q)), 3) for q in (10, 50, 90)},
        mean_Z_p50=round(float(np.median(mu)), 4),
        excess_kurtosis={f"p{q}": round(float(np.percentile(exk, q)), 2)
                         for q in (10, 50, 90, 99)},
        skew={f"p{q}": round(float(np.percentile(skew, q)), 2) for q in (10, 50, 90)},
        max_Z=dict(observed_p95=round(obs95, 3), observed_max=round(float(zmax.max()), 3),
                   gaussianized_p95=round(g95, 3), independent_gaussian_p95=round(indep95, 3),
                   note="gaussianization preserves the rank-dependence and normalises the "
                        "marginals; a large drop indicates a marginal-tail mechanism. This is "
                        "an attribution reading, NOT an additive decomposition of quantiles."),
        max_Z_concentration=dict(claims_ever_winning=int((amc > 0).sum()),
                                 top10_share=round(float(cum[9]), 3),
                                 top20_share=round(float(cum[-1]), 3)),
        raw_vs_rank=cmp_,
        tail_resolution=f"P(Z>x) resolves only to 1/{N_PERM} = {1/N_PERM:.2e}",
        historical_theta="NOT COMPUTED, NOT EXPOSED",
        selection_vs_effect="If a claim is ever selected by a rank statistic, an ordinary "
                            "bootstrap interval on theta is NOT a selection-adjusted 95% "
                            "interval. Descriptive only unless a simultaneous or selective "
                            "interval is built.",
        minutes=round((time.time() - t0) / 60, 1))
    json.dump(rep, open(OUT, "w"), indent=2, default=str)

    print(f"\n  CONTROL   sd(Z) p10/p50/p90 " +
          "/".join(f"{rep['sd_Z'][f'p{q}']:.3f}" for q in (10, 50, 90)) +
          f"   mean(Z) p50 {rep['mean_Z_p50']:+.4f}")
    print(f"\n  RAW  vs  RANK   (same {N_PERM} mappings)")
    print(f"    {'':<26} {'raw θ':>10} {'rank':>10}")
    for k2, v in cmp_.items():
        print(f"    {k2:<26} {v['raw']:>10} {v['rank']:>10}"
              + (f"   gaussian {v['gaussian']}" if "gaussian" in v else ""))
    print(f"\n  max-Z  observed p95 {obs95:.2f} · gaussianized {g95:.2f} · "
          f"independent {indep95:.2f} · max {zmax.max():.2f}")
    print(f"  concentration {int((amc>0).sum())} of 840 ever win · top20 {cum[-1]:.1%}")
    print(f"\n  WROTE {OUT}\n        {PER}\n        {MAT}", flush=True)


if __name__ == "__main__":
    main()
