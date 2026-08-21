"""T5_CAPABILITY_RANK_V2 — empirical-noise capability for the V2 selection statistic.

Freezes its own spec from the MEASURED cost, then runs it. Three needles instead of one,
because the rank engine made the whole design cost 1.1 h where V1 spent 8.3 h on q50 alone —
so q10 and q90, deferred in V1 purely for compute, are qualified BEFORE any historical
exposure rather than after it, where they could no longer strengthen the qualification.

NO INTERIM DETECTION SURFACE IS PRINTED. Progress only. Watching a surface accumulate world by
world is sequential peeking at the evidence the run exists to produce.

DELTA ENTERS ON THE RAW OUTCOME

    Y_delta = base + delta * needle_membership   ->   re-rank within block   ->   Z_rank[840]

never on U or R. The denominator stays the registered analytic SE at every delta, so all six
share one ruler and the grid keeps meaning MFE percentage points.

WHAT THIS RUN CAN AND CANNOT ESTABLISH

It measures whether an additive raw-MFE effect of a given size converts into enough rank
displacement to clear the search-wide band. It says nothing about whether such an effect
exists. Historical theta and historical Z_rank are not computed here.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, resource, sys, tempfile, time                   # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R, t5_compute_qual as Q, t5_rank_engine as RE   # noqa: E402

V2 = json.load(open("SEQUENCE_INFERENCE_V2.json"))
QUAL = json.load(open("RANK_CAPABILITY_ENGINE_QUALIFICATION.json"))
SEAL = json.load(open("T5_SEQUENCE_ESTIMAND_SEAL_V1.json"))
DATA = os.path.join(os.path.dirname(HERE), "data")
SPEC_OUT = os.path.join(HERE, "T5_CAPABILITY_RANK_V2.json")
RESULT = os.path.join(HERE, "T5_CAPABILITY_RANK_V2_RESULT.json")
LEDGER = os.path.join(DATA, "t5_capability_rank_v2_ledger.parquet")

DL = np.array(V2["delta_grid_pp"], float)
NW, NP_ = 20, 999
NEEDLES = list(V2["needles"].items())


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def atomic_parquet(df, path):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    os.close(fd)
    df.to_parquet(tmp, index=False)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


def freeze_spec():
    body = dict(
        spec_id="T5_CAPABILITY_RANK_V2",
        status="FROZEN_BEFORE_RUN",
        inference_spec=V2["spec_id"], inference_digest=V2["spec_digest"],
        estimand_digest=SEAL["seal_digest"], claim_order_hash=SEAL["claim_order_hash"],
        population_hash=SEAL["population_hash"],
        block_assignment_hash=SEAL["block_assignment_hash"], k=840,
        engine_qualification=dict(
            gates=QUAL["gates"], minutes_per_world=QUAL["cost"]["minutes_per_world"],
            projected_total_hours=QUAL["cost"]["projected_total_hours"],
            note="qualification produced no capability statistic; its fixture world used "
                 "seeds disjoint from this run's"),
        needles={k: v for k, v in NEEDLES},
        delta_grid_pp=list(DL), worlds=NW, n_perm_inner=NP_,
        logical_cells=len(NEEDLES) * NW * len(DL),
        why_three_needles="q10 and q90 were deferred in V1 for compute cost alone. The rank "
                          "engine removes that constraint, and running them now keeps them "
                          "PRE-exposure: after historical exposure they could only be "
                          "post-exposure characterization and could not strengthen the "
                          "qualification.",
        detection_rule="Z_rank(needle, delta) > p95( max_s Z_rank over 840 claims ) in the "
                       "same inner-null world, one-sided positive",
        denominator="registered analytic SE, identical at every delta and in every world",
        injection="delta added to the raw outcome, midranks recomputed within block",
        rng=dict(outer="[20260903, needle_index, world]",
                 inner="[20260904, needle_index, world, p], shared across the six deltas"),
        verdict_scope="Detection of additive raw-MFE injections at low, median and high claim "
                      "support for the V2 rank selection statistic. This is instrument "
                      "sensitivity, not evidence that any sequence effect exists.",
        forbidden_claims=V2["forbidden_claims"],
        y_status="HISTORICAL RANKING NOT COMPUTED, NOT EXPOSED")
    body["spec_digest"] = hashlib.sha256(
        json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()[:16]
    json.dump(body, open(SPEC_OUT, "w"), indent=2, ensure_ascii=False)
    return body


def world_identity(spec, ni, wi):
    return hashlib.sha256(f"{spec['spec_digest']}|{spec['estimand_digest']}|{ni}|{wi}|"
                          f"{NP_}|{list(DL)}".encode()).hexdigest()[:16]


def main():
    t0 = time.time()
    SPEC = freeze_spec()
    print(f"{SPEC['spec_id']} {SPEC['spec_digest']} · {len(NEEDLES)} needles × {NW} worlds "
          f"× {NP_} perms × {len(DL)} δ", flush=True)
    y, b, nb, S, claims, order = R.build_state(verbose=False)
    cb = np.concatenate([np.asarray(ob) for ob, rt, rc, w in claims])
    flat = Q.FlatState(claims); del claims
    import gc; gc.collect()
    E = RE.RankEngine(y, b, nb, flat, cb)

    prev = pd.read_parquet(os.path.join(DATA, "t5_rank_null_per_claim.parquet")) \
             .sort_values("j").analytic_sd_R.to_numpy()
    g = {"estimand_seal": SPEC["estimand_digest"] == SEAL["seal_digest"],
         "claim_order_hash": SPEC["claim_order_hash"] == SEAL["claim_order_hash"],
         "population_hash": SPEC["population_hash"] == SEAL["population_hash"],
         "k_840": len(order) == 840 == len(E.sdR),
         "registered_SE_unchanged": bool(np.max(np.abs(E.sdR - prev)) < 1e-12),
         "engine_qualification_passed": QUAL["passed"] is True,
         "single_process": "multiprocessing" not in sys.modules,
         "blas_threads_1": os.environ.get("OMP_NUM_THREADS") == "1"}
    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    idxf = np.arange(len(y), dtype=float)
    cols = {}
    for name, nd in NEEDLES:
        c = int(np.flatnonzero(order.j.to_numpy() == nd["sealed_j"])[0])
        cols[name] = c
        g[f"needle_{name}_support"] = int(S[c].sum()) == nd["treated_overlap"]
    pm0 = R.perm_fast(idxf, bo, st, np.random.default_rng([20260903, 0, 0])).astype(np.int64)
    g["identity_gate"] = E.gate(y + DL[-1] * S[cols["q50"]].astype(float), pm0,
                                n_seg=200, seed=11) < 1e-10
    for k2, v in g.items():
        print(f"    {'✓' if v else '✗'} {k2}", flush=True)
    if not all(g.values()):
        raise RuntimeError(f"preflight failed: {[k2 for k2, v in g.items() if not v]}")
    print(f"  state ready · RSS {rss():.2f} GB · preflight PASS", flush=True)

    rows, done = [], set()
    if os.path.exists(LEDGER):
        L = pd.read_parquet(LEDGER); rows = L.to_dict("records")
        for (nn, wi), grp in L.groupby(["needle", "world_id"]):
            ni = [k2 for k2, _ in NEEDLES].index(nn)
            if (len(grp) == len(DL) and (grp.n_perm == NP_).all()
                    and (grp.spec_digest == SPEC["spec_digest"]).all()
                    and (grp.world_identity == world_identity(SPEC, ni, int(wi))).all()):
                done.add((nn, int(wi)))
        print(f"  resume · {len(done)} complete (needle, world) cells verified", flush=True)

    total = len(NEEDLES) * NW
    for ni, (name, nd) in enumerate(NEEDLES):
        col = cols[name]
        memb = S[col].astype(float)
        for wi in range(NW):
            if (name, wi) in done:
                continue
            tw = time.time()
            base = R.perm_fast(y, bo, st, np.random.default_rng([20260903, ni, wi]))
            MR = [E.midranks(base + d * memb) for d in DL]          # re-rank per delta
            z_obs = np.array([E.Z(m)[col] for m in MR])
            mx = np.empty((len(DL), NP_))
            for p in range(NP_):
                pm = R.perm_fast(idxf, bo, st,
                                 np.random.default_rng([20260904, ni, wi, p])).astype(np.int64)
                for k2, m in enumerate(MR):
                    mx[k2, p] = E.Z(m[pm]).max()
            wid = world_identity(SPEC, ni, wi)
            for k2, d in enumerate(DL):
                b95 = float(np.percentile(mx[k2], 95))
                rows.append(dict(needle=name, needle_claim=nd["claim"],
                                 support=nd["treated_overlap"], delta_pp=float(d),
                                 world_id=wi, needle_Z=float(z_obs[k2]), band_p95=b95,
                                 detected=bool(z_obs[k2] > b95), n_perm=NP_,
                                 spec_digest=SPEC["spec_digest"],
                                 estimand_digest=SPEC["estimand_digest"],
                                 inference_digest=V2["spec_digest"], world_identity=wid,
                                 rng_outer=f"[20260903,{ni},{wi}]",
                                 rng_inner=f"[20260904,{ni},{wi},p]",
                                 inner_max_null_hash=hashlib.sha256(
                                     mx[k2].tobytes()).hexdigest()[:16]))
            atomic_parquet(pd.DataFrame(rows), LEDGER)
            nd_done = len(done) + len(rows) // len(DL) - len([1 for _ in done])
            print(f"  {name} world {wi+1}/{NW} checkpointed · {(time.time()-tw)/60:.1f}m "
                  f"· total {(time.time()-t0)/60:.0f}m · RSS {rss():.2f} GB", flush=True)

    L = pd.DataFrame(rows)
    surf = {n: {f"{d:+.1f}": f"{int(L[(L.needle==n)&(L.delta_pp==d)].detected.sum())}/{NW}"
                for d in DL} for n, _ in NEEDLES}
    json.dump(dict(spec_id=SPEC["spec_id"], spec_digest=SPEC["spec_digest"],
                   inference_spec=V2["spec_id"], inference_digest=V2["spec_digest"],
                   needles={k2: dict(claim=v["claim"], support=v["treated_overlap"])
                            for k2, v in NEEDLES},
                   worlds=NW, n_perm=NP_, k=840, surface=surf,
                   verdict_scope=SPEC["verdict_scope"],
                   forbidden_claims=SPEC["forbidden_claims"],
                   historical_ranking="NOT COMPUTED, NOT EXPOSED",
                   hours=round((time.time()-t0)/3600, 2)), open(RESULT, "w"), indent=2)
    print(f"\n  ALL CELLS COMPLETE · sealed {RESULT}", flush=True)
    print("  V2 RANK DETECTION SURFACE", flush=True)
    for n, v in NEEDLES:
        print(f"    {n}  {v['claim']}  n={v['treated_overlap']}", flush=True)
        for d in DL:
            print(f"      δ {d:>+4.1f} pp   {surf[n][f'{d:+.1f}']}", flush=True)


if __name__ == "__main__":
    main()
