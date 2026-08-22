"""{F}_15M_CAPABILITY_RESULT_V1 — the registered 15m capability, on real Y.

THE KERNELS ARE NOT REWRITTEN. batch() and midranks_ties() are imported from
t5_15m_capability, the implementation the 15m rank-engine / parallel / batch qualifications
already cover. A second numerical implementation here would be free to drift from the one
that was qualified, so there is not one. Only the family's frozen inputs change.

THE SEMANTICS, unchanged from the T5 15m precedent:

    outer world   real MFE_10D with the sequence<->Y association destroyed by permutation
                  WITHIN the frozen blocks
    injection     additive shift of the raw MFE_10D on the needle's treated episodes
    ranks + SE    midranks and the tie-corrected SE are properties of the block MULTISET,
                  and within-block permutation leaves every multiset untouched, so both are
                  computed ONCE per (needle, world, delta)
    inner null    999 permutations. Each one draws ONE mapping pm and re-indexes the whole
                  rank matrix with it — that single mapping serves all k claims and all six
                  delta lanes. Independent per-claim permutations are never generated.
    max-Z         over ALL k claims, per lane
    detection     needle Z > p95( max-Z ) in that same inner-null world

The ledger is checkpointed per (needle, world) and carries the outcome-source digest inline,
so a resume cannot silently join two data vintages.

    usage:  python t15m_capability.py t9
"""
from __future__ import annotations
import os, sys                                                         # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
# Thread count comes from the QUALIFIED engine, not from here. t5_15m_capability sets
# NUMBA_NUM_THREADS itself, and numba refuses any change once its threads are launched, so
# that module is imported FIRST and its configuration is simply adopted. Trying to lock a
# different count ahead of it only produces a config conflict — and the count cannot change
# a value anyway, since the kernel writes disjoint per-claim outputs.
import hashlib, importlib, json, math, resource, time                  # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
import t5_artifact as ART                                              # noqa: E402
import t5_15m_capability as K                                          # noqa: E402
import numba                                                           # noqa: E402
_THREADS = int(numba.get_num_threads())

BATCH_B = 8
RNG_OUTER, RNG_INNER = 0xA9F1, 0x5C3D


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def run(fam: str):
    t0 = time.time()
    F = fam.upper()
    D = importlib.import_module(f"{fam}_dna")
    DATA = os.path.join(os.path.dirname(HERE), "data")
    PROT = json.load(open(f"{F}_15M_CAPABILITY_PROTOCOL_V1.json"))
    EST = json.load(open(f"{F}_15M_ESTIMAND_V1.json"))
    ST = json.load(open(f"{F}_15M_ENGINE_STATE_V1.json"))
    ORD = json.load(open(f"{F}_15M_CLAIM_ORDER_V1.json"))
    DL = np.array(PROT["delta_grid_pp"], float)
    NW, NP_ = PROT["worlds"], PROT["n_perm_inner"]
    LEDGER = os.path.join(DATA, f"{fam}_15m_capability_ledger.parquet")
    OUT = f"{F}_15M_CAPABILITY_RESULT_V1.json"

    z = np.load(os.path.join(DATA, f"{fam}_15m_engine_state.npz"))
    eidx, seg_ptr, seg_nc = z["eidx"], z["seg_ptr"], z["seg_nc"]
    csp, N_T, const = z["claim_seg_ptr"], z["N_T"], z["const"]
    k, nE = int(z["k"][0]), int(z["nE"][0])
    print(f"{F}_15M_CAPABILITY · k {k:,} · nE {nE:,} · nnz {len(eidx):,} · nseg "
          f"{len(seg_nc):,} · threads {_THREADS} · FIRST 15m Y ACCESS", flush=True)

    P = pd.read_parquet(os.path.join(DATA, f"{fam}_15m_population.parquet"))
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    pop_hash = hashlib.sha256("|".join(sorted(P.episode_id)).encode()).hexdigest()[:16]
    blk, _ = pd.factorize(P.block)
    blk = blk.astype(np.int64)
    nb = int(blk.max()) + 1
    counts = np.bincount(blk, minlength=nb)
    starts = np.r_[0, np.cumsum(counts)[:-1]].astype(np.float64)

    O = pd.read_parquet(os.path.join(DATA, f"{fam}_15m_claim_order.parquet")) \
          .sort_values("j").reset_index(drop=True)
    NEEDLES = [(q, PROT["needles"][q]) for q in ("q10", "q50", "q90")]
    cols = {q: int(np.flatnonzero(O.j.to_numpy() == nd["sealed_j"])[0])
            for q, nd in NEEDLES}

    # ── FIRST 15m Y ACCESS ──────────────────────────────────────────────────
    E1 = importlib.import_module(f"{fam}_sequence_estimand")
    y_digest = ART.file_digest(E1.OC)
    OY = pd.read_parquet(E1.OC, columns=["episode_id", "mfe_10d"])
    y = (P.merge(OY, on="episode_id", how="left").mfe_10d.to_numpy(float) * 100.0)
    if not np.isfinite(y).all():
        raise RuntimeError("MFE_10D missing for some sealed-population episodes")

    # ── preflight ───────────────────────────────────────────────────────────
    g = {
        "engine_state_binds_claim_order":
            ST["governing"]["claim_order_hash"] == ORD["order"]["claim_order_hash"],
        "protocol_binds_estimand": PROT["governing"]["estimand"] ==
            ART.file_digest(f"{F}_15M_ESTIMAND_V1.json"),
        "population_hash": pop_hash == ST["population_hash"],
        "k_matches_seal": k == ORD["k"] == PROT["search_universe"]["k"],
        "nnz_matches_seal": len(eidx) == ST["nnz_treated_memberships"],
        "pre_y_gates_passed":
            json.load(open(f"{F}_15M_PRE_Y_GATES_V1.json"))["result"] == "PASS",
        "protocol_declared_no_y": PROT["y_status"].startswith("NO OUTCOME"),
    }
    for kk, vv in g.items():
        print(f"    {'PASS' if vv else 'FAIL'}  {kk}", flush=True)
    if not all(g.values()):
        raise RuntimeError(f"preflight failed: {[a for a, b in g.items() if not b]}")

    seg_blk = blk[eidx[seg_ptr[:-1]]]
    nt_seg = np.diff(seg_ptr).astype(np.float64)
    nc_seg = seg_nc.astype(np.float64)
    N_seg = nt_seg + nc_seg
    seg_claim = np.repeat(np.arange(k), np.diff(csp))
    w2_seg = (nt_seg / N_T[seg_claim].astype(np.float64)) ** 2
    del seg_claim                      # 0.5 GB, and w2_seg is all it was ever for
    ones = np.ones(k)

    def se_for(tie):
        varU = ((N_seg + 1.0) - tie[seg_blk] / (N_seg * (N_seg - 1.0))) \
            / (12.0 * nt_seg * nc_seg)
        return np.sqrt(np.add.reduceat(w2_seg * varU, csp[:-1]))

    rows, done = [], set()
    if os.path.exists(LEDGER):
        L = pd.read_parquet(LEDGER)
        bad = sorted(set(L.outcome_source_digest[L.outcome_source_digest != y_digest]))
        if bad:
            raise RuntimeError(f"ledger rows carry outcome digest {bad} but the file now "
                               f"hashes to {y_digest}; two vintages may not be joined")
        rows = L.to_dict("records")
        done = {(r["needle"], r["world_id"]) for r in rows}
        print(f"  resume · {len(done)} (needle, world) cells already complete", flush=True)

    R = np.empty((k, BATCH_B)); Zc = np.empty((k, BATCH_B))
    ranksT = np.empty((nE, BATCH_B), np.int32)
    r2_base = np.empty((nE, BATCH_B), np.int32)

    for ni, (nm, nd) in enumerate(NEEDLES):
        col = cols[nm]
        memb = np.zeros(nE, bool)
        a, b = csp[col], csp[col + 1]
        memb[eidx[seg_ptr[a]:seg_ptr[b]]] = True
        if int(memb.sum()) != nd["treated_overlap"]:
            raise RuntimeError(f"{nm}: membership {int(memb.sum())} != sealed "
                               f"{nd['treated_overlap']}")
        for wi in range(NW):
            if (nm, wi) in done:
                continue
            tw = time.time()
            og = np.random.default_rng([RNG_OUTER, ni, wi])
            base = y[np.lexsort((og.random(nE), blk))]      # association destroyed
            sds = []
            for di, d in enumerate(DL):
                r2, tie = K.midranks_ties(base + d * memb, blk, nb, counts, starts)
                r2_base[:, di] = r2
                sds.append(se_for(tie))
            for di in range(len(DL), BATCH_B):
                r2_base[:, di] = r2_base[:, 0]              # padding lanes, discarded
            K.batch(r2_base, eidx, seg_ptr, seg_nc, csp, N_T, const, ones, R, Zc)
            obs = [float(R[col, di] / sds[di][col]) for di in range(len(DL))]

            mx = np.empty((len(DL), NP_))
            for p in range(NP_):
                ig = np.random.default_rng([RNG_INNER, ni, wi, p])
                pm = np.lexsort((ig.random(nE), blk))       # ONE mapping, all k claims
                np.take(r2_base, pm, axis=0, out=ranksT)
                K.batch(ranksT, eidx, seg_ptr, seg_nc, csp, N_T, const, ones, R, Zc)
                for di in range(len(DL)):
                    mx[di, p] = float(np.max(R[:, di] / sds[di]))
                if (p + 1) % 250 == 0:
                    print(f"    {nm} w{wi} · perm {p+1}/{NP_} · "
                          f"{(time.time()-tw)/60:.1f}m · RSS {rss():.2f} GB", flush=True)
            wid = hashlib.sha256(
                f"{ART.file_digest(f'{F}_15M_CAPABILITY_PROTOCOL_V1.json')}|{nm}|{wi}|"
                f"{NP_}|{list(DL)}".encode()).hexdigest()[:16]
            for di, d in enumerate(DL):
                b95 = float(np.percentile(mx[di], 95))
                rows.append(dict(
                    needle=nm, sealed_j=nd["sealed_j"], support=nd["treated_overlap"],
                    delta_pp=float(d), world_id=wi, needle_Z=obs[di], band_p95=b95,
                    band_median=float(np.percentile(mx[di], 50)),
                    band_max=float(mx[di].max()), detected=bool(obs[di] > b95),
                    n_perm=NP_, claim_order_hash=ORD["order"]["claim_order_hash"],
                    world_identity=wid, outcome_source_digest=y_digest,
                    population_hash=pop_hash, threads=_THREADS,
                    rng_outer=f"[{RNG_OUTER},{ni},{wi}]",
                    rng_inner=f"[{RNG_INNER},{ni},{wi},p]",
                    null_max_hash=hashlib.sha256(mx[di].tobytes()).hexdigest()[:16]))
            K.atomic_parquet(pd.DataFrame(rows), LEDGER)
            print(f"  {nm} world {wi+1}/{NW} checkpointed · {(time.time()-tw)/60:.1f}m · "
                  f"total {(time.time()-t0)/3600:.2f}h", flush=True)

    L = pd.DataFrame(rows)
    surf = {nm: {f"{d:+.1f}": f"{int(L[(L.needle==nm)&(L.delta_pp==d)].detected.sum())}/{NW}"
                 for d in DL} for nm, _ in NEEDLES}
    bands = {nm: {f"{d:+.1f}": dict(
        p95_median=round(float(L[(L.needle == nm) & (L.delta_pp == d)].band_p95.median()), 3),
        p95_min=round(float(L[(L.needle == nm) & (L.delta_pp == d)].band_p95.min()), 3),
        p95_max=round(float(L[(L.needle == nm) & (L.delta_pp == d)].band_p95.max()), 3))
        for d in DL} for nm, _ in NEEDLES}
    cells = L.groupby(["needle", "world_id"]).ngroups
    integ = dict(
        expected_worlds=3 * NW, completed_worlds=int(cells),
        missing=int(3 * NW - cells),
        duplicates=int(len(L) - len(L.drop_duplicates(["needle", "world_id", "delta_pp"]))),
        permutations_per_world=sorted(L.n_perm.unique().tolist()),
        distinct_null_max_vectors=int(L.null_max_hash.nunique()),
        expected_rows=3 * NW * len(DL), rows=int(len(L)),
        one_mapping_per_permutation="a single pm re-indexes the whole rank matrix, so all "
                                    f"{k:,} claims and all {len(DL)} delta lanes share it",
        membership_identity="each needle's rebuilt membership was asserted equal to the "
                            "sealed treated_overlap before its worlds ran",
        outcome_source_digest=y_digest,
        population_hash=pop_hash,
        claim_order_hash=ORD["order"]["claim_order_hash"],
        engine_equivalence="kernels imported from t5_15m_capability — the implementation "
                           "the 15m rank-engine / parallel / batch qualifications cover; "
                           "no second implementation exists to diverge",
        threads=_THREADS,
        threads_note="engineering only; not in frozen_and_may_not_change, and the kernel "
                     "writes disjoint per-claim outputs so it cannot change a value")
    d = ART.seal(dict(
        spec_id=f"{F}_15M_CAPABILITY_RESULT_V1", status="CAPABILITY_ONLY", family=F,
        capability_protocol=ART.file_digest(f"{F}_15M_CAPABILITY_PROTOCOL_V1.json"),
        estimand=ART.file_digest(f"{F}_15M_ESTIMAND_V1.json"),
        engine_state=ART.file_digest(f"{F}_15M_ENGINE_STATE_V1.json"),
        claim_order=ART.file_digest(f"{F}_15M_CLAIM_ORDER_V1.json"),
        k=k, worlds=NW, n_perm=NP_, delta_grid_pp=list(DL),
        needles={nm: dict(sealed_j=nd["sealed_j"], representative=nd["representative"],
                          support=nd["treated_overlap"]) for nm, nd in NEEDLES},
        detections=surf, max_null_geometry=bands, integrity=integ,
        hard_state={f"{F} 15M OBSERVED Z": "NOT COMPUTED",
                    f"{F} 15M theta": "NOT COMPUTED",
                    f"{F} 15M WINNERS/SURVIVORS": "UNKNOWN"},
        outcome_exposure="CAPABILITY ONLY — every Z here is on an outcome whose association "
                         "was destroyed by within-block permutation before anything was "
                         "injected; no historical association is computed",
        runtime_hours=round((time.time() - t0) / 3600, 2)),
        OUT, required=("spec_id", "detections", "integrity", "hard_state"),
        supersede=os.path.exists(OUT))
    print(f"\n  DETECTIONS / {NW}")
    for nm, _ in NEEDLES:
        print(f"    {nm}: " + " · ".join(f"{d:+.1f}={surf[nm][f'{d:+.1f}']}" for d in DL))
    print(f"\n{F}_15M_CAPABILITY_RESULT_V1 · {d} · {(time.time()-t0)/3600:.2f} h")


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9"]):
        run(f)
