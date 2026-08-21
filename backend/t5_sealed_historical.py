"""T5_1H_SEALED_HISTORICAL_V2 — compute the historical result. Show nobody.

The machine reads mfe_10d and computes the full observed and permutation objects. The human
sees shapes, hashes and integrity verdicts. Nothing else. Exposure is a separate command.

WHAT IS FORBIDDEN IN THIS SCRIPT'S OUTPUT

    claim names · observed Z_rank · observed theta · any top-N · the observed maximum ·
    the band · the number of survivors

A survivor count is a result. "0 of 840" and "6 of 840" are different findings and printing
either one is exposure. So the integrity block reports only that the arrays exist, are
complete, are finite, and hash to a recorded value.

FULL MATRICES ARE PERSISTED, NOT JUST THE MAXIMA

The Combo Miner run discarded theta_perm and kept only its maximum, and the same class of
loss hit the grammar survivors. Both had to be recomputed. Here the complete
Z_rank_perm[999 x 840] is written, along with the claim order it is indexed by, the RNG
stream identifiers and every spec hash needed to prove which world produced it.

theta_perm IS STORED BUT THE CONTRACT DOES NOT DEPEND ON IT

Under SEQUENCE_INFERENCE_V2 promotion is decided by max Z_rank alone. The raw-theta
permutation matrix is kept because it is cheap to produce now and expensive to reproduce
later, not because any registered decision consults it. observed_theta_raw IS part of the
contract — as the reported effect size, never as a promotion criterion.

THE NULL HERE IS THE REAL DATA PERMUTED

Unlike the capability runs there is no outer association-destroying world. The observed
statistic is computed on the population as it stands; the null permutes Y within blocks.
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
import t5_rank_engine as RE, t5_jit_engine as JE                       # noqa: E402

V2 = json.load(open("SEQUENCE_INFERENCE_V2.json"))
CAP = json.load(open("T5_CAPABILITY_RANK_V2.json"))
SEAL = json.load(open("T5_SEQUENCE_ESTIMAND_SEAL_V1.json"))
DATA = os.path.join(os.path.dirname(HERE), "data")
OBS = os.path.join(DATA, "t5_hist_observed.parquet")
ZP = os.path.join(DATA, "t5_hist_Z_rank_perm.parquet")
TP = os.path.join(DATA, "t5_hist_theta_perm.parquet")
OUT = os.path.join(HERE, "T5_1H_SEALED_HISTORICAL_V2.json")

N_PERM = CAP["n_perm_inner"]                 # 999, exactly as frozen
RNG_NULL = [20260905]


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def atomic_parquet(df, path):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    os.close(fd)
    df.to_parquet(tmp, index=False)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


def arm_components(flat, y):
    """Weighted treated and control medians per claim — the descriptive halves of theta."""
    T, C, sT, sC, co, w = flat.T, flat.C, flat.segT, flat.segC, flat.coff, flat.w
    mt = np.empty(len(sT) - 1); mc = np.empty(len(sT) - 1)
    for s in range(len(sT) - 1):
        mt[s] = np.median(y[T[sT[s]:sT[s + 1]]])
        mc[s] = np.median(y[C[sC[s]:sC[s + 1]]])
    wt = np.add.reduceat(w * mt, co[:-1])
    wc = np.add.reduceat(w * mc, co[:-1])
    return wt, wc


def main():
    t0 = time.time()
    print("T5_1H_SEALED_HISTORICAL_V2 · sealed compute · no result is printed", flush=True)
    y, b, nb, S, claims, order = R.build_state(verbose=False)
    cb = np.concatenate([np.asarray(ob) for ob, rt, rc, w in claims])
    flat = Q.FlatState(claims); del claims
    import gc; gc.collect()
    E = RE.RankEngine(y, b, nb, flat, cb)

    # ── identity gates ──────────────────────────────────────────────────────
    prev = pd.read_parquet(os.path.join(DATA, "t5_rank_null_per_claim.parquet")) \
             .sort_values("j").analytic_sd_R.to_numpy()
    g = {"estimand_digest": CAP["estimand_digest"] == SEAL["seal_digest"],
         "inference_v2_digest": CAP["inference_digest"] == V2["spec_digest"],
         "capability_v2_digest": CAP["spec_digest"] == "e16ea5101b4cbd85",
         "population_hash": CAP["population_hash"] == SEAL["population_hash"],
         "claim_order_hash": CAP["claim_order_hash"] == SEAL["claim_order_hash"],
         "block_assignment_hash": CAP["block_assignment_hash"] == SEAL["block_assignment_hash"],
         "k_840": len(order) == 840 == len(E.sdR),
         "registered_SE_unchanged": bool(np.max(np.abs(E.sdR - prev)) < 1e-12),
         "capability_qualified": CAP["status"] == "FROZEN_BEFORE_RUN"}
    for k2, v in g.items():
        print(f"    {'✓' if v else '✗'} {k2}", flush=True)
    if not all(g.values()):
        raise RuntimeError(f"identity gates failed: {[k2 for k2, v in g.items() if not v]}")

    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    idxf = np.arange(len(y), dtype=float)

    # ── observed ────────────────────────────────────────────────────────────
    mr = E.midranks(y)
    Z_obs = E.Z(mr); R_obs = E.R(mr)
    zj = np.zeros(len(y)); DL0 = np.array([0.0])
    J = JE.gate(flat, flat.theta, y, zj, DL0)          # equivalence enforced, atol 1e-10
    th_obs = J.theta6(y, zj, DL0)[0].copy()
    wt, wc = arm_components(flat, y)
    print(f"  observed computed · engine equivalence PASS · RSS {rss():.2f} GB", flush=True)

    # ── registered null ─────────────────────────────────────────────────────
    ZP_ = np.empty((N_PERM, 840)); TP_ = np.empty((N_PERM, 840))
    seen = set(); tp = time.time()
    for p in range(N_PERM):
        pm = R.perm_fast(idxf, bo, st,
                         np.random.default_rng(RNG_NULL + [p])).astype(np.int64)
        seen.add(hashlib.sha256(pm.tobytes()).hexdigest())
        ZP_[p] = E.Z(mr[pm])
        TP_[p] = J.theta6(y[pm], zj, DL0)[0]
        if (p + 1) % 200 == 0:
            print(f"    perm {p+1}/{N_PERM} · {(time.time()-tp)/60:.1f}m", flush=True)

    ordj = order.sort_values("j")
    OB = pd.DataFrame(dict(
        j=ordj.j.to_numpy(), claim_id=ordj.representative.to_numpy(),
        aliases=ordj.aliases.to_numpy(), support=S.sum(axis=1).astype(np.int64),
        analytic_sd_R=E.sdR, R_rank_obs=R_obs, Z_rank_obs=Z_obs,
        theta_raw_obs=th_obs, weighted_treated_median=wt, weighted_control_median=wc))
    atomic_parquet(OB, OBS)
    atomic_parquet(pd.DataFrame(ZP_, columns=[f"j{j}" for j in range(840)]), ZP)
    atomic_parquet(pd.DataFrame(TP_, columns=[f"j{j}" for j in range(840)]), TP)

    # ── integrity, and nothing else ─────────────────────────────────────────
    finite = bool(np.isfinite(Z_obs).all() and np.isfinite(R_obs).all()
                  and np.isfinite(th_obs).all() and np.isfinite(ZP_).all()
                  and np.isfinite(TP_).all())
    complete = bool(ZP_.shape == (N_PERM, 840) and TP_.shape == (N_PERM, 840))
    unique = len(seen) == N_PERM
    ah = hashlib.sha256(
        Z_obs.tobytes() + R_obs.tobytes() + th_obs.tobytes()
        + ZP_.tobytes() + TP_.tobytes()).hexdigest()[:16]

    rep = dict(
        spec_id="T5_1H_SEALED_HISTORICAL_V2", status="SEALED — NOT EXPOSED",
        inference_spec=V2["spec_id"], inference_digest=V2["spec_digest"],
        capability_spec=CAP["spec_id"], capability_digest=CAP["spec_digest"],
        estimand_digest=SEAL["seal_digest"], population_hash=SEAL["population_hash"],
        claim_order_hash=SEAL["claim_order_hash"],
        block_assignment_hash=SEAL["block_assignment_hash"],
        n_population=int(len(y)), n_claims=840, n_permutations=N_PERM, n_blocks=int(nb),
        observed_shapes=dict(Z_rank_obs=[840], R_rank_obs=[840], theta_raw_obs=[840],
                             weighted_treated_median=[840], weighted_control_median=[840]),
        permutation_shapes=dict(Z_rank_perm=[N_PERM, 840], theta_perm=[N_PERM, 840]),
        rng=dict(null_stream=f"{RNG_NULL}+[p], p in 0..{N_PERM-1}",
                 distinct_mappings=len(seen)),
        integrity=dict(identity_gates=g, finite_values=finite, null_matrix_complete=complete,
                       rng_streams_unique=unique,
                       engine_equivalence="JitTheta6 gated to atol 1e-10 against reference",
                       rank_identity="rank-sum identity gated in "
                                     "RANK_CAPABILITY_ENGINE_QUALIFICATION"),
        artifact_hash=ah,
        artifacts=dict(observed=os.path.basename(OBS), Z_rank_perm=os.path.basename(ZP),
                       theta_perm=os.path.basename(TP)),
        theta_perm_note="stored for reuse; NO registered decision consults it. Promotion is "
                        "decided by max Z_rank alone.",
        contains_but_does_not_report=["claim ranking", "observed maxima", "band",
                                      "survivor count", "any claim's Z or theta"],
        exposure="REQUIRES A SEPARATE EXPOSE COMMAND",
        minutes=round((time.time() - t0) / 60, 1))
    json.dump(rep, open(OUT, "w"), indent=2, default=str)

    print(f"\n  n_population {len(y):,} · n_claims 840 · n_blocks {nb:,} · "
          f"n_permutations {N_PERM}")
    print(f"  observed vectors      5 x [840]")
    print(f"  permutation matrices  Z_rank_perm [{N_PERM}, 840] · theta_perm [{N_PERM}, 840]")
    for nm, v in (("population hash", g["population_hash"]),
                  ("claim order hash", g["claim_order_hash"]),
                  ("block hash", g["block_assignment_hash"]),
                  ("V2 spec digest", g["inference_v2_digest"]),
                  ("finite values", finite), ("null matrix complete", complete),
                  ("RNG streams unique", unique)):
        print(f"    {nm:<22} {'PASS' if v else 'FAIL'}")
    print(f"  artifact hash {ah}")
    print(f"\n  SEALED. Nothing about the result has been printed.")
    print(f"  EXPOSE is a separate command.")
    print(f"  WROTE {OUT}", flush=True)


if __name__ == "__main__":
    main()
