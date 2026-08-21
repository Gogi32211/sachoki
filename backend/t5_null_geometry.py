"""NULL_GEOMETRY_DIAGNOSTIC_V1 — what creates the 28.7pp max-null band.

DIAGNOSTIC, NOT INFERENTIAL. 200 permutations is far too few to calibrate a 95th percentile
of a max over 840 claims for any test. It is ample to measure the SHAPE of the null: per-claim
scale, how scale relates to support, and whether a handful of claims own the maximum. Nothing
here registers a new test and nothing here is a fix.

WHAT THE CAPABILITY RUN ESTABLISHED, AND WHAT IT DID NOT

Established: on a median-support claim (n=682), injections of +0.5 .. +5.0pp are detected
0/20 times. In that range the registered detector is practically insensitive.

NOT established: where historical theta[840] sits. It has never been computed. The band is a
property of the null; a claim's position against it is not knowable without computing it, and
this file does not compute it.

WHY delta = 0

The band was measured to be invariant to the needle injection (28.668 -> 28.676 under a 5pp
injection, a 0.008pp move). So the geometry under study is the geometry of the null itself,
and the needle plays no part in it. One delta instead of six makes each permutation ~6x
cheaper.

THE STANDARDIZED MAX IS SELF-STANDARDIZED

Z uses mean_j and sd_j estimated from the same 200 permutations that Z is then maximised over.
That is mildly optimistic and would be illegitimate in a test. As a comparison of scale
between raw-max and standardized-max geometry it is fine, and it is labelled everywhere it
appears so it cannot be quoted as a calibrated threshold later.
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
MAT = os.path.join(DATA, "t5_null_geometry_theta_perm.parquet")
PER = os.path.join(DATA, "t5_null_geometry_per_claim.parquet")
OUT = os.path.join(HERE, "T5_NULL_GEOMETRY_DIAGNOSTIC_V1.json")

N_PERM = 200
RNG_OUTER = [20260822, 0, 0]      # distinct from the capability run's [20260820, ...]
RNG_INNER = [20260823, 0]         # distinct from the capability run's [20260821, ...]


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def atomic_parquet(df, path):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    os.close(fd)
    df.to_parquet(tmp, index=False)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


def identity_gates(order):
    """The diagnostic must describe the SAME null the capability run measured. If any of these
    drift, the geometry reported here would belong to a different estimand."""
    g = {"estimand_digest": SPEC["estimand_digest"] == SEAL["seal_digest"],
         "claim_order_hash": SPEC["claim_order_hash"] == SEAL["claim_order_hash"],
         "population_hash": SPEC["population_hash"] == SEAL["population_hash"],
         "block_hash": SPEC["block_assignment_hash"] == SEAL["block_assignment_hash"],
         "k_840": SPEC["k"] == 840 == len(order),
         "blas_threads_1": os.environ.get("OMP_NUM_THREADS") == "1",
         "fastmath_false": JE._theta6.targetoptions.get("fastmath") is False,
         "single_process": "multiprocessing" not in sys.modules}
    for k, v in g.items():
        print(f"    {'✓' if v else '✗'} {k}", flush=True)
    if not all(g.values()):
        raise RuntimeError(f"identity gates failed: {[k for k, v in g.items() if not v]}")


def main():
    t0 = time.time()
    print("NULL_GEOMETRY_DIAGNOSTIC_V1 · 1 outer null world · δ=0 · "
          f"{N_PERM} perms · full θ matrix", flush=True)
    y, b, nb, S, claims, order = R.build_state(verbose=False)

    # per-claim structure BEFORE claims is freed: support and block count are what the
    # scale-vs-support question is about.
    support = np.array([len(np.concatenate([rt[bb] for bb in ob]))
                        for ob, rt, rc, w in claims], dtype=np.int64)
    n_blocks = np.array([len(ob) for ob, rt, rc, w in claims], dtype=np.int64)

    flat = Q.FlatState(claims); del claims
    import gc; gc.collect()
    print(f"  state ready · index {flat.nbytes/1e6:.0f} MB · RSS {rss():.2f} GB", flush=True)
    identity_gates(order)

    DL = np.array([0.0])                      # delta = 0: the null's own geometry
    z = np.zeros(len(y))
    J = JE.gate(flat, flat.theta, y, z, DL)   # equivalence still enforced, atol 1e-10
    print(f"  equivalence PASS · {time.time()-t0:.0f}s", flush=True)

    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    idxf = np.arange(len(y), dtype=float)

    # one association-destroyed outer world, exactly as the capability run builds one
    base = R.perm_fast(y, bo, st, np.random.default_rng(RNG_OUTER))

    TH = np.empty((N_PERM, 840))
    tp = time.time()
    for p in range(N_PERM):
        pm = R.perm_fast(idxf, bo, st,
                         np.random.default_rng(RNG_INNER + [p])).astype(np.int64)
        TH[p] = J.theta6(base[pm], z, DL)[0]
        if (p + 1) % 50 == 0:
            print(f"    perm {p+1}/{N_PERM} · {(time.time()-tp)/60:.1f}m", flush=True)

    atomic_parquet(pd.DataFrame(TH, columns=[f"j{j}" for j in range(840)]), MAT)

    # ── per claim ────────────────────────────────────────────────────────────
    rep = order.sort_values("j").representative.to_numpy()
    fam = np.array([r.split("|")[0] for r in rep])
    ln = np.array([int(r.split("|")[1]) for r in rep])
    am = TH.argmax(axis=1)                                # winner of each permutation's max
    amc = np.bincount(am, minlength=840)
    P = pd.DataFrame(dict(
        j=order.sort_values("j").j.to_numpy(), claim_id=rep, support=support,
        family=fam, length=ln, n_overlap_blocks=n_blocks,
        null_mean=TH.mean(0), null_median=np.median(TH, 0), null_sd=TH.std(0, ddof=1),
        null_MAD=np.median(np.abs(TH - np.median(TH, 0)), 0),
        q05=np.percentile(TH, 5, axis=0), q95=np.percentile(TH, 95, axis=0),
        argmax_count=amc, argmax_share=amc / N_PERM))
    atomic_parquet(P, PER)

    rmax = TH.max(axis=1)
    sd = P.null_sd.to_numpy()
    Z = (TH - TH.mean(0)) / np.where(sd > 0, sd, np.nan)   # SELF-STANDARDIZED, diagnostic only
    zmax = np.nanmax(Z, axis=1)

    # ── 1 · null_sd distribution ─────────────────────────────────────────────
    qs = [0, 10, 25, 50, 75, 90, 95, 100]
    sd_dist = {f"p{q}": round(float(np.percentile(sd, q)), 3) for q in qs}

    # ── 2 · null_sd vs support ───────────────────────────────────────────────
    dec = pd.qcut(P.support, 10, labels=False, duplicates="drop")
    by_dec = (P.assign(dec=dec).groupby("dec")
              .agg(n=("j", "size"), support_med=("support", "median"),
                   sd_med=("null_sd", "median"), sd_max=("null_sd", "max")).reset_index())
    ls, lsd = np.log(P.support.to_numpy()), np.log(sd)
    ok = np.isfinite(lsd)
    slope = float(np.polyfit(ls[ok], lsd[ok], 1)[0])
    spear = float(pd.Series(sd).corr(P.support, method="spearman"))

    # ── 3 · max domination ───────────────────────────────────────────────────
    top = P.sort_values("argmax_count", ascending=False).head(20)
    cum = top.argmax_count.cumsum().to_numpy() / N_PERM
    dominators = int((P.argmax_count > 0).sum())

    # ── 4 · raw max-null ─────────────────────────────────────────────────────
    raw = {f"q{q}": round(float(np.percentile(rmax, q)), 3) for q in (50, 90, 95)}
    raw["max"] = round(float(rmax.max()), 3)
    zst = {f"q{q}": round(float(np.percentile(zmax, q)), 3) for q in (50, 90, 95)}
    zst["max"] = round(float(zmax.max()), 3)

    rep_json = dict(
        spec_id="NULL_GEOMETRY_DIAGNOSTIC_V1", status="DIAGNOSTIC_ONLY",
        estimand_digest=SPEC["estimand_digest"], claim_order_hash=SPEC["claim_order_hash"],
        population_hash=SPEC["population_hash"], k=840, n_perm=N_PERM, delta_pp=0.0,
        rng_outer=str(RNG_OUTER), rng_inner=f"{RNG_INNER}+[p]",
        theta_perm_hash=hashlib.sha256(TH.tobytes()).hexdigest()[:16],
        null_sd_distribution=sd_dist,
        sd_vs_support=dict(loglog_slope=round(slope, 3),
                           spearman_sd_support=round(spear, 3),
                           sqrt_n_prediction=-0.5,
                           by_support_decile=by_dec.round(3).to_dict("records")),
        max_domination=dict(claims_ever_winning_max=dominators,
                            top10_cumulative_share=round(float(cum[9]), 3) if len(cum) > 9 else None,
                            top20_cumulative_share=round(float(cum[-1]), 3),
                            top=top[["j", "claim_id", "support", "null_sd",
                                     "argmax_count", "argmax_share"]].round(3)
                            .to_dict("records")),
        raw_max_null=raw,
        standardized_max_null=zst,
        standardization_caveat="Z uses mean_j and sd_j from the SAME 200 permutations it is "
                               "maximised over. Self-standardized: a diagnostic comparison of "
                               "geometry, NOT a calibrated threshold and not a registered test.",
        historical_theta="NOT COMPUTED, NOT EXPOSED",
        capability_scope="Established: median-support claim, +0.5..+5.0pp injections detected "
                         "0/20 — the detector is practically insensitive in that range. NOT "
                         "established: the position of historical theta[840] relative to the "
                         "band, which has never been computed.",
        minutes=round((time.time() - t0) / 60, 1))
    json.dump(rep_json, open(OUT, "w"), indent=2, default=str)

    # ── report ───────────────────────────────────────────────────────────────
    print(f"\n  1 · null_sd (pp)  " + "  ".join(f"{k} {v}" for k, v in sd_dist.items()))
    print(f"\n  2 · null_sd vs support · log-log slope {slope:+.3f} "
          f"(sqrt-n predicts -0.500) · spearman {spear:+.3f}")
    print("      dec  n    support_med    sd_med   sd_max")
    for r in by_dec.itertuples():
        print(f"      {int(r.dec):>3} {int(r.n):>4} {int(r.support_med):>12,} "
              f"{r.sd_med:>9.3f} {r.sd_max:>8.3f}")
    print(f"\n  3 · max domination · {dominators} of 840 claims ever win a permutation max")
    if len(cum) > 9:
        print(f"      top10 {cum[9]:.1%} · top20 {cum[-1]:.1%} of all {N_PERM} maxima")
    print("       j  claim_id                          support   null_sd  argmax")
    for r in top.head(10).itertuples():
        print(f"      {int(r.j):>4}  {r.claim_id:<32} {int(r.support):>7,} "
              f"{r.null_sd:>9.3f} {r.argmax_share:>6.1%}")
    print(f"\n  4 · raw max-null (pp)     " + "  ".join(f"{k} {v}" for k, v in raw.items()))
    print(f"      standardized max (Z)  " + "  ".join(f"{k} {v}" for k, v in zst.items())
          + "   [self-standardized, diagnostic only]")
    print(f"\n  WROTE {OUT}\n        {PER}\n        {MAT}", flush=True)


if __name__ == "__main__":
    main()
