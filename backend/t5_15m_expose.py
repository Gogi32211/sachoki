"""EXPOSE — T5_15M_HISTORICAL. Provenance gates first, then the registered result.

TWO DEFECTS ARE RECORDED BEFORE ANYTHING IS SHOWN

1. PRE_EXPOSURE_METADATA_LEAK. The sealed run printed "theta computed for 733 claims
   (survivors + top 500)". theta_scope = survivors ∪ top500-by-the-same-Z. If survivors were
   500 or fewer they would all sit inside top500 and the union would be exactly 500. It is
   733, so survivors > 500, so top500 is entirely inside survivors, so the union IS the
   survivor set. The count was therefore exposed exactly — not bounded below — before the
   EXPOSE command. It does not touch the sealed inference: the spec was frozen before the
   compute and the result was already sealed.

2. CUTOFF_NOT_PERSISTED. The sealed run computed max_null and the p95 band in float64, used
   them for the `survives` flags, and then wrote only the flags and a float32 copy of the
   permutation matrix. The band itself was never stored. Recovering it from the float32 matrix
   is exactly the precision hazard to avoid, so it is recomputed in float64 from the same
   deterministic RNG stream and then RECONCILED against the stored flags on all 31,583 claims.
   A single disagreement stops the exposure.

NOTHING ELSE IS DERIVED HERE

No clustering, no token-family breakdown, no 1H matching, no support slicing, no volatility
decomposition, no threshold reconsideration. The survivor count is already known, so any of
those would be post-exposure adaptation dressed as part of the first report.
"""
from __future__ import annotations
import os, sys
os.environ["NUMBA_NUM_THREADS"] = "8"
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, time                                            # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402
import t5_15m_par_qualify as PQ                                       # noqa: E402
import t5_15m_historical as H                                         # noqa: E402
from numba import get_num_threads                                     # noqa: E402

SEALED = json.load(open("T5_15M_SEALED_HISTORICAL.json"))
SPEC   = json.load(open("T5_15M_HISTORICAL_SPEC.json"))
ORD    = json.load(open("T5_15M_CLAIM_ORDER_V2.json"))
CAPRES = json.load(open("T5_15M_CAPABILITY_RANK_V2_RESULT.json"))
DATA   = os.path.join(D.ROOT, "data")
OBS_PQ = os.path.join(DATA, "t5_15m_hist_observed.parquet")
SURV_PQ = os.path.join(DATA, "t5_15m_survivors.parquet")
AUDIT  = "T5_15M_EXPOSURE_AUDIT.json"
OUT    = "T5_15M_HISTORICAL_EXPOSED.json"
TOP_SHOW = 25


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    OB = pd.read_parquet(OBS_PQ)
    k = len(OB)
    if k != SEALED["n_claims"] or k != ORD["k"]:
        raise RuntimeError("observed table does not match the sealed k")

    # ── recompute max_null in float64 from the same deterministic stream ────
    print("EXPOSE — T5_15M_HISTORICAL")
    print("  recomputing the float64 max-null from the sealed RNG stream …", flush=True)
    z = np.load(PQ.BUNDLE)
    A = {n: z[n] for n in PQ.ARRS}
    nE = int(z["nE"][0])
    eidx, seg_ptr, seg_nc = A["eidx"], A["seg_ptr"], A["seg_nc"]
    csp, N_T, const = A["claim_seg_ptr"], A["N_T"], A["const"]

    P = pd.read_parquet(os.path.join(DATA, "t5_15m_population.parquet"))
    pop_hash = hashlib.sha256("|".join(P.episode_id).encode()).hexdigest()[:16]
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    blk = P.block.to_numpy(np.int64)
    nb = int(blk.max()) + 1
    starts = np.r_[0, np.cumsum(np.bincount(blk, minlength=nb))[:-1]].astype(np.float64)
    OY = pd.read_parquet(os.path.join(DATA, "t5_episode_outcomes.parquet"),
                         columns=["episode_id", "mfe_10d"])
    y = P.merge(OY, on="episode_id", how="left").mfe_10d.to_numpy(float) * 100.0

    sd = OB.analytic_sd.to_numpy()
    r2, _ = H.midranks_ties(y, blk, nb, starts)
    ones = np.ones(k)
    B = 8
    R = np.empty((k, B)); Zb = np.empty((k, B))
    ranksT = np.empty((nE, B), np.int32)
    mx = np.empty(H.N_PERM)
    p = 0
    while p < H.N_PERM:
        lane = min(B, H.N_PERM - p)
        for b in range(lane):
            gg = np.random.default_rng([H.RNG_NULL, p + b])
            pm = np.lexsort((gg.random(nE), blk))
            ranksT[:, b] = r2[pm]
        for b in range(lane, B):
            ranksT[:, b] = ranksT[:, 0]
        H.batch(ranksT, eidx, seg_ptr, seg_nc, csp, N_T, const, ones, R, Zb)
        for b in range(lane):
            mx[p + b] = float(np.max(R[:, b] / sd))
        p += lane
    band = float(np.percentile(mx, 95))

    Z = OB.Z_rank_obs.to_numpy()
    recomputed = Z > band                       # strictly greater, as frozen
    stored = OB.survives.to_numpy()
    disagree = int((recomputed != stored).sum())

    g = dict(
        population_hash=pop_hash == ORD["population_hash"],
        claim_order_hash=SPEC["claim_order_hash"] == ORD["claim_order_hash"],
        outcome_source_digest=SPEC["outcome_source_digest"] ==
            ART.file_digest(os.path.join(DATA, "t5_episode_outcomes.parquet")),
        se_gate_digest=SEALED["se_gate_digest"] == ART.file_digest("T5_15M_PRODUCTION_SE_GATE.json"),
        historical_spec_digest=SEALED["historical_spec_digest"] ==
            ART.file_digest("T5_15M_HISTORICAL_SPEC.json"),
        threads_8=int(get_num_threads()) == 8,
        promotion_source_float64=True,
        stored_flags_reconcile=disagree == 0)
    for kk, vv in g.items():
        print(f"    {'✓' if vv else '✗'} {kk}", flush=True)
    print(f"    flags reconciled on all {k:,} claims · disagreements {disagree}", flush=True)
    if not all(g.values()):
        raise RuntimeError(f"exposure gate failed: {[kk for kk, vv in g.items() if not vv]}")

    n_surv = int(stored.sum())

    # ── audit ───────────────────────────────────────────────────────────────
    ART.seal(dict(
        spec_id="T5_15M_EXPOSURE_AUDIT",
        defects=[
            dict(id="PRE_EXPOSURE_METADATA_LEAK",
                 field="theta_scope_count = 733",
                 logical_consequence="theta_scope = survivors ∪ top500_by_the_same_Z. If "
                                     "survivors <= 500 every survivor would lie inside top500 "
                                     "and the union would be exactly 500. The union is 733, so "
                                     "survivors > 500, so top500 is entirely inside survivors, "
                                     "so the union IS the survivor set: survivor_count = 733 "
                                     "exactly, not a lower bound.",
                 effect_on_sealed_inference="NONE — the spec was frozen before the compute and "
                                            "the result was sealed before this was noticed",
                 effect_on_exposure_status="the survivor count was exposed before the EXPOSE "
                                           "command",
                 remediation="no redesign and no extra slicing; proceed directly to the "
                             "registered exposure",
                 my_error="I first reported this as '>= 233', which understates it. The "
                          "deduction is exact."),
            dict(id="CUTOFF_NOT_PERSISTED",
                 description="the sealed run computed max_null and the p95 band in float64 and "
                             "used them for the survives flags, but stored only the flags and "
                             "a float32 copy of the permutation matrix. The band itself was "
                             "never written.",
                 hazard="recovering the band from the float32 matrix could move a claim across "
                        "the boundary",
                 effect_on_sealed_inference="NONE — the stored flags were always float64-derived",
                 remediation="the band is recomputed in float64 from the same deterministic RNG "
                             "stream and reconciled against the stored flags on every claim; a "
                             "single disagreement aborts the exposure",
                 reconciliation_disagreements=disagree)],
        promotion_source="SEALED_FLOAT64_MAX_NULL, recomputed deterministically and reconciled",
        gates=g), AUDIT, required=("spec_id", "defects"))

    # ── 1 · SEARCH-WIDE RESULT ──────────────────────────────────────────────
    print(f"\n1 · SEARCH-WIDE RESULT")
    print(f"    k                       {k:,}")
    print(f"    historical max-null p95 {band:.4f}")
    print(f"    max observed Z_rank     {Z.max():.4f}")
    print(f"    survivors (Z > p95)     {n_surv:,}")
    print(f"    rule                    strictly greater, one-sided positive, this run's own "
          f"999 permutations")

    # ── 2 · SURVIVORS ───────────────────────────────────────────────────────
    S = OB[OB.survives].sort_values("Z_rank_obs", ascending=False).reset_index(drop=True)
    S.to_parquet(SURV_PQ, index=False)
    print(f"\n2 · SURVIVORS — top {TOP_SHOW} of {len(S):,} by Z_rank "
          f"(full table → {os.path.basename(SURV_PQ)})")
    print(f"    {'j':>6} {'claim':<34} {'fam':<6} {'supp':>7} {'Z_rank':>8} "
          f"{'θ pp':>8} {'trtMed':>8} {'ctlMed':>8}")
    for r in S.head(TOP_SHOW).itertuples():
        print(f"    {int(r.j):>6} {r.claim_id:<34} {r.position_family:<6} "
              f"{int(r.support):>7,} {r.Z_rank_obs:>8.3f} {r.theta_raw_obs:>8.2f} "
              f"{r.weighted_treated_median:>8.2f} {r.weighted_control_median:>8.2f}")

    # ── 3 · CAPABILITY CONTEXT ──────────────────────────────────────────────
    print(f"\n3 · CAPABILITY CONTEXT — registered pre-exposure")
    for nm, nd in SPEC["capability"]["surface"].items():
        print(f"    {nm:<5} " + "  ".join(f"{d} {v}" for d, v in nd.items()))

    payload = dict(
        spec_id="T5_15M_HISTORICAL_EXPOSED",
        sealed_artifact_hash=SEALED["artifact_hash"],
        historical_spec_digest=SEALED["historical_spec_digest"],
        se_gate_digest=SEALED["se_gate_digest"],
        exposure_audit_digest=ART.file_digest(AUDIT),
        k=k, n_permutations=H.N_PERM,
        historical_max_null_p95=round(band, 6),
        max_observed_Z_rank=round(float(Z.max()), 6),
        survivors=n_surv,
        promotion_rule="Z_rank_obs > p95(max-null), one-sided positive, strictly greater",
        promotion_source="float64 max-null recomputed from the sealed deterministic RNG "
                         "stream, reconciled against the stored flags on all claims",
        reconciliation_disagreements=disagree,
        survivor_table=os.path.basename(SURV_PQ),
        top=S.head(100)[["j", "claim_id", "position_family", "support", "Z_rank_obs",
                         "theta_raw_obs", "weighted_treated_median",
                         "weighted_control_median"]].round(5).to_dict("records"),
        capability_context=SPEC["capability"]["surface"],
        capability_scope=SPEC["capability"]["scope"],
        forbidden_claims=SPEC["forbidden_claims"],
        not_derived_here=["clustering", "token-family breakdown", "1H matching",
                          "support slicing", "volatility decomposition",
                          "threshold reconsideration"],
        interpretation=dict(
            what_it_means="historical associations that survived the registered search-wide "
                          "multiplicity control",
            what_it_does_not_mean="not a 15m edge, not a validated setup, not confirmation of "
                                  "any 1H claim",
            overlap_caveat="733 of 31,583 is a far larger surviving set than 1H's 9 of 840. "
                           "Until the band, the Z distribution and the overlap structure are "
                           "examined this must NOT be read as hundreds of independent effects "
                           "— large exact or near-overlap families are the obvious "
                           "alternative. That examination is post-exposure characterization.",
            theta_scope=SPEC["theta_scope"]),
        minutes=round((time.time() - t0) / 60, 1))
    dig = ART.seal(payload, OUT, required=("spec_id", "survivors", "historical_max_null_p95"))
    print(f"\n  WROTE {OUT} · {dig}")
    print(f"        {SURV_PQ}")
    print(f"        {AUDIT}")


if __name__ == "__main__":
    main()
