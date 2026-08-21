"""q50 empirical-noise capability — single process, JIT engine, sealed.

Every gate below runs BEFORE the first world. The run took the machine down twice when
parallelism was added ahead of measurement; the answer turned out to be a 32.7x algorithmic
speedup, after which one process suffices and the whole class of problems disappears.

NO INTERIM DETECTION SURFACE IS PRINTED. Watching "7/12 detected at +3pp" accumulate is
sequential peeking at the very evidence this run exists to produce. Progress only.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS","MKL_NUM_THREADS","OPENBLAS_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, resource, sys, tempfile, time                  # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R, t5_compute_qual as Q, t5_jit_engine as JE   # noqa: E402

SPEC = json.load(open("T5_CAPABILITY_Q50_V1.json"))
SEAL = json.load(open("T5_SEQUENCE_ESTIMAND_SEAL_V1.json"))
LEDGER = os.path.join(os.path.dirname(HERE), "data", "t5_capability_ledger.parquet")
OUT = os.path.join(HERE, "T5_CAPABILITY_Q50_RESULT_V1.json")
DL = np.array(SPEC["delta_grid_pp"], float); NP_ = SPEC["n_perm_inner"]; NW = SPEC["worlds"]
NEEDLE_IDX = 1


def rss(): return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def atomic_parquet(df, path):
    """tmp -> fsync -> rename. A shutdown mid-write must not corrupt completed worlds."""
    d = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    os.close(fd)
    df.to_parquet(tmp, index=False)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


def preflight(y, b, S, order, flat):
    g = {}
    g["estimand_digest"] = SPEC["estimand_digest"] == SEAL["seal_digest"]
    g["claim_order_hash"] = SPEC["claim_order_hash"] == SEAL["claim_order_hash"]
    g["population_hash"] = SPEC["population_hash"] == SEAL["population_hash"]
    g["block_hash"] = SPEC["block_assignment_hash"] == SEAL["block_assignment_hash"]
    g["k_840"] = (SPEC["k"] == 840 == len(order))
    g["needle_j_692"] = SPEC["needle"]["sealed_j"] == 692
    g["blas_threads_1"] = os.environ.get("OMP_NUM_THREADS") == "1"
    g["fastmath_false"] = JE._theta6.targetoptions.get("fastmath") is False
    g["parallel_false"] = JE._theta6.targetoptions.get("parallel") in (False, None)
    g["single_process"] = "multiprocessing" not in sys.modules
    col = int(np.flatnonzero(order.j.to_numpy() == SPEC["needle"]["sealed_j"])[0])
    z = S[col].astype(float)
    t = time.time(); J = JE.gate(flat, flat.theta, y, z, DL)          # 840 x 6, atol 1e-10
    g["jit_equivalence"] = True
    print(f"  equivalence PASS ({time.time()-t:.0f}s)", flush=True)
    for k, v in g.items():
        print(f"    {'✓' if v else '✗'} {k}", flush=True)
    if not all(g.values()):
        raise RuntimeError(f"preflight failed: {[k for k,v in g.items() if not v]}")
    return J, z, col


def world_identity(wi):
    return hashlib.sha256(
        f"{SPEC['spec_digest']}|{SPEC['estimand_digest']}|{NEEDLE_IDX}|{wi}|"
        f"{NP_}|{list(DL)}".encode()).hexdigest()[:16]


def main():
    t0 = time.time()
    print(f"{SPEC['spec_id']} {SPEC['spec_digest']} · needle {SPEC['needle_claim']} "
          f"· k={SPEC['k']} · {NW} worlds x {NP_} perms x {len(DL)} deltas", flush=True)
    y, b, nb, S, claims, order = R.build_state(verbose=False)
    flat = Q.FlatState(claims); del claims
    import gc; gc.collect()
    print(f"  state ready · index {flat.nbytes/1e6:.0f} MB · RSS {rss():.2f} GB", flush=True)
    J, z, col = preflight(y, b, S, order, flat)

    rows, done = [], set()
    if os.path.exists(LEDGER):
        L = pd.read_parquet(LEDGER); rows = L.to_dict("records")
        for wi, grp in L.groupby("world_id"):
            ok = (len(grp) == len(DL) and (grp.n_perm == NP_).all()
                  and (grp.spec_digest == SPEC["spec_digest"]).all()
                  and (grp.estimand_digest == SPEC["estimand_digest"]).all()
                  and (grp.world_identity == world_identity(int(wi))).all())
            if ok:
                done.add(int(wi))
        print(f"  resume · {len(done)} complete worlds verified on disk", flush=True)

    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    idxf = np.arange(len(y), dtype=float)

    for wi in range(NW):
        if wi in done:
            continue
        tw = time.time()
        base = R.perm_fast(y, bo, st, np.random.default_rng([20260820, NEEDLE_IDX, wi]))
        th_obs = J.theta6(base, z, DL)[:, col].copy()
        mx = np.empty((len(DL), NP_))
        for p in range(NP_):
            pm = R.perm_fast(idxf, bo, st,
                             np.random.default_rng([20260821, NEEDLE_IDX, wi, p])).astype(np.int64)
            mx[:, p] = J.theta6(base[pm], z[pm], DL).max(axis=1)
            if (p + 1) % 250 == 0:
                print(f"    world {wi} · perm {p+1}/{NP_} · {(time.time()-tw)/60:.1f}m "
                      f"· RSS {rss():.2f} GB", flush=True)
        wid = world_identity(wi)
        for k, d in enumerate(DL):
            b95 = float(np.percentile(mx[k], 95))
            rows.append(dict(needle="q50", delta_pp=float(d), world_id=wi,
                             needle_theta=float(th_obs[k]), band_p95=b95,
                             detected=bool(th_obs[k] > b95), n_perm=NP_,
                             spec_digest=SPEC["spec_digest"],
                             estimand_digest=SPEC["estimand_digest"],
                             world_identity=wid,
                             rng_outer=f"[20260820,{NEEDLE_IDX},{wi}]",
                             rng_inner=f"[20260821,{NEEDLE_IDX},{wi},p]",
                             inner_max_null_hash=hashlib.sha256(mx[k].tobytes()).hexdigest()[:16]))
        atomic_parquet(pd.DataFrame(rows), LEDGER)
        print(f"  world {wi+1 if wi in done else len(done)+len([r for r in rows])//len(DL)}"
              f"/{NW} checkpointed · {(time.time()-tw)/60:.0f}m · total "
              f"{(time.time()-t0)/3600:.1f}h", flush=True)

    L = pd.DataFrame(rows)
    surf = {f"{d:+.1f}": f"{int(L[(L.delta_pp==d)].detected.sum())}/{NW}" for d in DL}
    json.dump(dict(spec_id=SPEC["spec_id"], spec_digest=SPEC["spec_digest"],
                   needle=SPEC["needle_claim"], needle_support=SPEC["needle_support"],
                   k=SPEC["k"], n_perm=NP_, worlds=NW, surface=surf,
                   scope=SPEC["verdict_scope"], forbidden_claim=SPEC["forbidden_claim"],
                   deferred=SPEC["deferred"], engine="JitTheta6 (equivalence-gated, 32.7x)",
                   historical_ranking="NOT COMPUTED, NOT EXPOSED",
                   hours=round((time.time()-t0)/3600, 2)), open(OUT, "w"), indent=2)
    print(f"\n  ALL {NW} WORLDS COMPLETE · sealed {OUT}", flush=True)
    print("  DETECTION SURFACE (q50, treated n=682)", flush=True)
    for d in DL:
        print(f"    δ {d:>+4.1f} pp   {surf[f'{d:+.1f}']}", flush=True)


if __name__ == "__main__":
    main()
