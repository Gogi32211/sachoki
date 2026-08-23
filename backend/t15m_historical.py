"""{F}_15M_HISTORICAL — the observed statistic, and the search-wide null it must beat.

TWO INVOCATIONS, deliberately. The spec is sealed FIRST, on its own, and the run refuses to
start without it. Freezing the promotion rule in the same breath as computing the number it
judges is how a threshold quietly becomes a description of the result.

    python t15m_historical.py t9 --seal-spec
    python t15m_historical.py t9 --run

THE STATISTIC, unchanged from the T5 15m precedent and from this family's own capability:

    observed      Z_rank on the REAL MFE_10D, all k claims, one-sided positive
    null          999 permutations WITHIN the frozen blocks. ONE mapping per permutation is
                  applied to every claim, so the search-wide maximum keeps the joint
                  dependence between claims — independent per-claim permutations would
                  destroy exactly the structure the maximum is meant to price.
    promotion     Z_obs > p95(max-null) from THIS run's own permutations, STRICTLY greater
    theta         NOT computed here. It is magnitude only and never a promotion criterion.

Midranks and the tie-corrected SE are properties of each block's MULTISET, and within-block
permutation leaves every multiset untouched — so both are computed ONCE and the 999
permutations merely re-index the rank vector.

The kernels are imported from t5_15m_capability, the implementation the 15m rank-engine /
parallel / batch qualifications already cover. There is no second implementation.
"""
from __future__ import annotations
import os, sys                                                         # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, importlib, json, time                                  # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
import t5_artifact as ART                                              # noqa: E402
import t5_15m_capability as K                                          # noqa: E402
import t15m_gates as G                                                 # noqa: E402

N_PERM = 999
PERM_NS = "T15M_HISTORICAL_PERM_V1"


def spec_path(F):
    return f"{F}_15M_HISTORICAL_SPEC_V1.json"


def seal_spec(fam):
    F = fam.upper()
    ORD = json.load(open(f"{F}_15M_CLAIM_ORDER_V1.json"))
    EST = json.load(open(f"{F}_15M_ESTIMAND_V1.json"))
    ST = json.load(open(f"{F}_15M_ENGINE_STATE_V1.json"))
    CAPR = json.load(open(f"{F}_15M_CAPABILITY_RESULT_V1.json"))
    root = G.rng_root(F)
    d = ART.seal(dict(
        spec_id=f"{F}_15M_HISTORICAL_SPEC_V1", status="FROZEN_BEFORE_COMPUTE", family=F,
        governing=dict(
            estimand=ART.file_digest(f"{F}_15M_ESTIMAND_V1.json"),
            claim_order=ART.file_digest(f"{F}_15M_CLAIM_ORDER_V1.json"),
            engine_state=ART.file_digest(f"{F}_15M_ENGINE_STATE_V1.json"),
            capability_protocol=ART.file_digest(
                f"{F}_15M_CAPABILITY_PROTOCOL_V1.json"),
            capability_result=ART.file_digest(f"{F}_15M_CAPABILITY_RESULT_V1.json"),
            eligibility_ruling=ART.file_digest("T15M_ELIGIBILITY_RULING_V1.json"),
            population_hash=ST["population_hash"],
            claim_order_hash=ORD["order"]["claim_order_hash"]),
        k=ORD["k"],
        primary_outcome=EST["primary_outcome"],
        secondary_outcome=EST["secondary_outcome"],
        statistic=EST["statistic"], direction=EST["direction"],
        multiplicity="search-wide max Z_rank over all k claims, ONE permutation mapping "
                     "applied to every claim",
        why_one_mapping="the search-wide maximum prices the joint dependence between claims; "
                        "independent per-claim permutations would destroy exactly the "
                        "structure the maximum exists to account for",
        n_perm=N_PERM,
        promotion="Z_rank_obs > p95( max-null Z_rank ) from THIS run's own permutations, "
                  "STRICTLY greater",
        band_note="the capability's ~4.64 band is calibration and sanity only, never the "
                  "cutoff — the cutoff comes from this run's own permutations",
        raw_theta="NOT computed in this run. Magnitude only, never a promotion criterion.",
        se_treatment="midranks and the tie-corrected SE are properties of each block's "
                     "multiset; within-block permutation leaves every multiset untouched, so "
                     "both are computed ONCE and the permutations re-index the rank vector",
        rng=dict(root=root.hex()[:16], namespace=PERM_NS,
                 rule="SHA256 over the sealed chain (prespec, x_only, k_closure, ruling, "
                      "claim_order) — derived, never chosen",
                 per_permutation_key=f"[{PERM_NS}, root, p]"),
        capability_evidence=dict(
            detections=CAPR["detections"],
            note="the capability establishes that the design CAN detect a registered "
                 "additive shift; it says nothing about whether one is present"),
        frozen_and_may_not_change=[
            "k", "claim order", "population", "blocks", "statistic", "direction",
            "n_perm", "the promotion rule", "the outcome definition"],
        forbidden=["choosing the threshold after seeing the observed statistic",
                   "using theta or any magnitude for promotion",
                   "reporting a survivor before the max-null is computed"],
        y_status="NO OUTCOME STATISTIC COMPUTED — this artifact freezes the rule, it does "
                 "not apply it",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        spec_path(F), required=("spec_id", "governing", "promotion", "n_perm"),
        supersede=os.path.exists(spec_path(F)))
    print(f"{F}_15M_HISTORICAL_SPEC_V1 · {d} · FROZEN_BEFORE_COMPUTE")
    return d


def run(fam):
    t0 = time.time()
    F = fam.upper()
    if not os.path.exists(spec_path(F)):
        raise SystemExit(f"{spec_path(F)} does not exist — seal the spec BEFORE computing.")
    SPEC = json.load(open(spec_path(F)))
    D = importlib.import_module(f"{fam}_dna")
    E1 = importlib.import_module(f"{fam}_sequence_estimand")
    DATA = os.path.join(os.path.dirname(HERE), "data")
    ORD = json.load(open(f"{F}_15M_CLAIM_ORDER_V1.json"))

    z = np.load(os.path.join(DATA, f"{fam}_15m_engine_state.npz"))
    eidx, seg_ptr, seg_nc = z["eidx"], z["seg_ptr"], z["seg_nc"]
    csp, N_T, const = z["claim_seg_ptr"], z["N_T"], z["const"]
    k, nE = int(z["k"][0]), int(z["nE"][0])

    P = pd.read_parquet(os.path.join(DATA, f"{fam}_15m_population.parquet"))
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    pop_hash = hashlib.sha256("|".join(sorted(P.episode_id)).encode()).hexdigest()[:16]
    blk, _ = pd.factorize(P.block)
    blk = blk.astype(np.int64)
    nb = int(blk.max()) + 1
    counts = np.bincount(blk, minlength=nb)
    starts = np.r_[0, np.cumsum(counts)[:-1]].astype(np.float64)

    y_digest = ART.file_digest(E1.OC)
    OY = pd.read_parquet(E1.OC, columns=["episode_id", "mfe_10d"])
    y = (P.merge(OY, on="episode_id", how="left").mfe_10d.to_numpy(float) * 100.0)
    if not np.isfinite(y).all():
        raise RuntimeError("MFE_10D missing for some sealed-population episodes")

    g = {"spec_frozen_before_compute": SPEC["status"] == "FROZEN_BEFORE_COMPUTE",
         "population_hash": pop_hash == SPEC["governing"]["population_hash"],
         "claim_order_hash": (ORD["order"]["claim_order_hash"]
                              == SPEC["governing"]["claim_order_hash"]),
         "k_matches": k == SPEC["k"] == ORD["k"],
         "outcome_vintage": y_digest == json.load(
             open(f"{F}_15M_CAPABILITY_RESULT_V1.json"))["integrity"]
             ["outcome_source_digest"]}
    for a, b in g.items():
        print(f"    {'PASS' if b else 'FAIL'}  {a}", flush=True)
    if not all(g.values()):
        raise RuntimeError("preflight failed")

    seg_blk = blk[eidx[seg_ptr[:-1]]]
    nt_seg = np.diff(seg_ptr).astype(np.float64)
    nc_seg = seg_nc.astype(np.float64)
    N_seg = nt_seg + nc_seg
    seg_claim = np.repeat(np.arange(k), np.diff(csp))
    w2_seg = (nt_seg / N_T[seg_claim].astype(np.float64)) ** 2
    del seg_claim
    ones = np.ones(k)

    # ONE multiset per block -> midranks and the tie-corrected SE computed ONCE
    r2, tie = K.midranks_ties(y, blk, nb, counts, starts)
    varU = ((N_seg + 1.0) - tie[seg_blk] / (N_seg * (N_seg - 1.0))) / (12.0 * nt_seg * nc_seg)
    sd = np.sqrt(np.add.reduceat(w2_seg * varU, csp[:-1]))

    B = 1
    R = np.empty((k, B)); Zc = np.empty((k, B))
    ranksT = np.empty((nE, B), np.int32)
    ranksT[:, 0] = r2
    K.batch(ranksT, eidx, seg_ptr, seg_nc, csp, N_T, const, ones, R, Zc)
    z_obs = R[:, 0] / sd
    print(f"  observed statistic computed for {k:,} claims · {(time.time()-t0)/60:.1f} min",
          flush=True)

    root = G.rng_root(F)
    mx = np.empty(N_PERM)
    for p in range(N_PERM):
        rng = np.random.default_rng([int.from_bytes(
            hashlib.sha256((PERM_NS + "|" + root.hex()).encode()).digest()[:8], "big"), p])
        pm = np.lexsort((rng.random(nE), blk))      # ONE mapping, all k claims
        ranksT[:, 0] = r2[pm]
        K.batch(ranksT, eidx, seg_ptr, seg_nc, csp, N_T, const, ones, R, Zc)
        mx[p] = float(np.max(R[:, 0] / sd))
        if (p + 1) % 100 == 0:
            print(f"    perm {p+1}/{N_PERM} · {(time.time()-t0)/60:.1f} min", flush=True)

    p95 = float(np.percentile(mx, 95))
    surv = np.flatnonzero(z_obs > p95)              # STRICTLY greater
    O = pd.read_parquet(os.path.join(DATA, f"{fam}_15m_claim_order.parquet")) \
          .sort_values("j").reset_index(drop=True)
    O["z_obs"] = z_obs
    O["survivor"] = False
    O.loc[surv, "survivor"] = True
    out = os.path.join(DATA, f"{fam}_15m_historical.parquet")
    O.to_parquet(out, index=False)

    top = O.sort_values("z_obs", ascending=False).head(20)
    d = ART.seal(dict(
        spec_id=f"{F}_15M_HISTORICAL_EXPOSED_V1", status="HISTORICAL_EXPOSED", family=F,
        spec=ART.file_digest(spec_path(F)),
        claim_order=ART.file_digest(f"{F}_15M_CLAIM_ORDER_V1.json"),
        engine_state=ART.file_digest(f"{F}_15M_ENGINE_STATE_V1.json"),
        capability_result=ART.file_digest(f"{F}_15M_CAPABILITY_RESULT_V1.json"),
        k=k, n_perm=N_PERM, population_hash=pop_hash, outcome_source_digest=y_digest,
        max_null=dict(p95=round(p95, 4), median=round(float(np.median(mx)), 4),
                      min=round(float(mx.min()), 4), max=round(float(mx.max()), 4),
                      p99=round(float(np.percentile(mx, 99)), 4),
                      digest=hashlib.sha256(mx.tobytes()).hexdigest()[:16]),
        observed=dict(max=round(float(z_obs.max()), 4),
                      p99=round(float(np.percentile(z_obs, 99)), 4),
                      median=round(float(np.median(z_obs)), 4),
                      min=round(float(z_obs.min()), 4),
                      n_nan=int(np.isnan(z_obs).sum())),
        promotion="Z_obs > p95(max-null), STRICTLY greater",
        survivors=dict(
            n=int(len(surv)),
            claims=[dict(j=int(r.j), representative=r.representative,
                         position_family=r.position_family, support=int(r.support),
                         z_obs=round(float(r.z_obs), 4))
                    for r in O.loc[surv].sort_values("z_obs", ascending=False)
                    .itertuples()][:50]),
        top20_by_z=[dict(j=int(r.j), representative=r.representative,
                         support=int(r.support), z_obs=round(float(r.z_obs), 4),
                         survivor=bool(r.survivor)) for r in top.itertuples()],
        theta="NOT COMPUTED — magnitude only, never a promotion criterion",
        table=dict(path=os.path.basename(out), digest=ART.file_digest(out)),
        runtime_min=round((time.time() - t0) / 60, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        f"{F}_15M_HISTORICAL_EXPOSED_V1.json",
        required=("spec_id", "max_null", "observed", "survivors", "promotion"),
        supersede=os.path.exists(f"{F}_15M_HISTORICAL_EXPOSED_V1.json"))
    print(f"\n  max-null p95 {p95:.4f} · observed max {z_obs.max():.4f} · "
          f"SURVIVORS {len(surv)} / {k:,}")
    print(f"{F}_15M_HISTORICAL_EXPOSED_V1 · {d} · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    fam = next((a for a in sys.argv[1:] if not a.startswith("-")), "t9")
    if "--seal-spec" in sys.argv:
        seal_spec(fam)
    elif "--run" in sys.argv:
        run(fam)
    else:
        raise SystemExit("use --seal-spec first, then --run")
