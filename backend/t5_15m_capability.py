"""T5_15M_CAPABILITY_RANK_V2 — the empirical-noise capability. FIRST 15m Y ACCESS.

The machine reads MFE_10D. The human sees detections per (needle, delta), null band summaries,
and integrity — nothing else. There is no historical ranking in this run at all: every world
begins by destroying the sequence<->Y association, so the only "observed" quantity is a needle
whose effect was injected by us.

CHECKPOINT GRANULARITY IS (needle, world), NOT (needle, world, delta)

The six deltas are evaluated in ONE batched kernel invocation and share the same permutation
stream, so a cell cannot be interrupted between deltas. A crash costs one (needle, world) —
about 6.7 minutes — not the run.

THE DENOMINATOR IS REBUILT PER CELL, PER DELTA

Injection shifts the raw outcome, which can break ties apart or create new ones, so the
conditional randomization variance must belong to the multiset it divides:

    Y_delta -> within-block midranks -> per-block tie sums -> analytic SE
    -> the 999 inner permutations of that (needle, world, delta) use THAT fixed SE

Never estimated from the permutation numerator. The kernel is handed sd = 1 and Z is formed
outside it, because the six lanes carry six different SEs while the kernel takes one per
claim — passing the qualified kernel a per-lane SE would have meant editing code that the
equivalence gate already certified.
"""
from __future__ import annotations
import os, sys
os.environ["NUMBA_NUM_THREADS"] = "8"
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, resource, tempfile, time                        # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402
import t5_15m_par_qualify as PQ                                       # noqa: E402
os.environ["NUMBA_NUM_THREADS"] = "8"
from numba import njit, prange, get_num_threads                       # noqa: E402

SPEC_PATH = "T5_15M_CAPABILITY_RANK_V2.json"
SPEC = json.load(open(SPEC_PATH))
ORD = json.load(open("T5_15M_CLAIM_ORDER_V2.json"))
DATA = os.path.join(D.ROOT, "data")
LEDGER = os.path.join(DATA, "t5_15m_capability_ledger.parquet")
OUT = os.path.join(HERE, "T5_15M_CAPABILITY_RANK_V2_RESULT.json")

SPEC_DIGEST = None      # set in main() once t5_artifact is importable
DL = np.array(SPEC["delta_grid_pp"], float)
NW, NP_ = SPEC["worlds"], SPEC["n_perm_inner"]
BATCH_B = SPEC["engine"]["batch_B"]
NEEDLES = list(SPEC["needles"].items())
RNG_OUTER, RNG_INNER = 20260921, 20260922


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def atomic_parquet(df, path):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    os.close(fd)
    df.to_parquet(tmp, index=False)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


@njit(cache=True, fastmath=False, nogil=True, parallel=True)
def batch(ranksT, eidx, seg_ptr, seg_nc, csp, N_T, const, sd, R, Z):
    nb = ranksT.shape[1]
    for c in prange(csp.shape[0] - 1):
        acc = np.zeros(nb)
        tot = np.zeros(nb)
        for s in range(csp[c], csp[c + 1]):
            for b in range(nb):
                tot[b] = 0.0
            for i in range(seg_ptr[s], seg_ptr[s + 1]):
                e = eidx[i]
                for b in range(nb):
                    tot[b] += ranksT[e, b]
            inv = 0.5 / seg_nc[s]
            for b in range(nb):
                acc[b] += tot[b] * inv
        nt = N_T[c]; cn = const[c]; sc = sd[c]
        for b in range(nb):
            r = acc[b] / nt + cn
            R[c, b] = r; Z[c, b] = r / sc


def midranks_ties(Y, blk, nb, counts, starts):
    """Within-block average ranks (doubled, exact in int32) and per-block sum(t^3 - t)."""
    o = np.lexsort((Y, blk))
    bs, ys = blk[o], Y[o]
    pos = np.arange(len(o), dtype=np.float64) - starts[bs]
    new = np.empty(len(o), bool); new[0] = True
    np.not_equal(bs[1:], bs[:-1], out=new[1:])
    new[1:] |= ys[1:] != ys[:-1]
    gid = np.cumsum(new) - 1
    gsum = np.bincount(gid, weights=pos + 1.0)
    gcnt = np.bincount(gid).astype(np.float64)
    mr = np.empty(len(o))
    mr[o] = (gsum / gcnt)[gid]
    tie = np.bincount(bs[new], weights=gcnt ** 3 - gcnt, minlength=nb)
    r2 = np.rint(mr * 2.0).astype(np.int32)
    if not np.allclose(r2, mr * 2.0, atol=1e-9):
        raise RuntimeError("midrank doubling is not integral — rank2 would lose information")
    return r2, tie


def main():
    global SPEC_DIGEST
    t0 = time.time()
    ART.smoke_test(verbose=False)
    SPEC_DIGEST = ART.file_digest(SPEC_PATH)
    print(f"T5_15M_CAPABILITY_RANK_V2 {SPEC_DIGEST} · "
          f"3 needles x {NW} worlds x {NP_} perms x {len(DL)} δ · FIRST 15m Y ACCESS",
          flush=True)

    z = np.load(PQ.BUNDLE)
    A = {n: z[n] for n in PQ.ARRS}
    k = int(z["k"][0]); nE = int(z["nE"][0]); nseg = int(z["nseg"][0])
    eidx, seg_ptr, seg_nc = A["eidx"], A["seg_ptr"], A["seg_nc"]
    csp, N_T, const, blk_n, blk_start = (A["claim_seg_ptr"], A["N_T"], A["const"],
                                         A["blk_n"], A["blk_start"])

    P = pd.read_parquet(os.path.join(DATA, "t5_15m_population.parquet"))
    pop_hash = hashlib.sha256("|".join(P.episode_id).encode()).hexdigest()[:16]
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    blk = P.block.to_numpy(np.int64)
    nb = int(blk.max()) + 1
    counts = np.bincount(blk, minlength=nb)
    starts = np.r_[0, np.cumsum(counts)[:-1]].astype(np.float64)

    O = pd.read_parquet(os.path.join(DATA, "t5_15m_order.parquet")).sort_values("j")
    cols = {nm: int(np.flatnonzero(O.j.to_numpy() == nd["sealed_j"])[0])
            for nm, nd in NEEDLES}

    # ── FIRST 15m Y ACCESS ──────────────────────────────────────────────────
    OY_PATH = os.path.join(DATA, "t5_episode_outcomes.parquet")
    y_digest = ART.file_digest(OY_PATH)
    OY = pd.read_parquet(OY_PATH, columns=["episode_id", "mfe_10d"])
    y = (P.merge(OY, on="episode_id", how="left").mfe_10d.to_numpy(float) * 100.0)
    if not np.isfinite(y).all():
        raise RuntimeError("MFE_10D missing for some sealed-population episodes")

    # ── hard gates ──────────────────────────────────────────────────────────
    g = {
        "capability_spec_hash": SPEC_DIGEST == "918d274553594289",
        "claim_order_hash": SPEC["governing"]["claim_order"] == ORD["claim_order_hash"]
                            == "3a0c637ae5828243",
        "population_hash": pop_hash == ORD["population_hash"],
        "engine_config": (SPEC["engine"]["threads"] == 8 and BATCH_B == 8
                          and int(get_num_threads()) == 8),
        "k_31583": k == ORD["k"] == 31583,
        "no_historical_exposure": SPEC["y_status"].startswith("NO 15m Y ACCESS"),
    }
    # tie-corrected SE path, against exhaustive enumeration, before any cell runs
    import t5_15m_needles as NDL
    tg = NDL.tie_gate()
    g["tie_corrected_se_path"] = tg["passed"]
    for kk, vv in g.items():
        print(f"    {'✓' if vv else '✗'} {kk}", flush=True)
    if not all(g.values()):
        raise RuntimeError(f"preflight failed: {[kk for kk, vv in g.items() if not vv]}")

    seg_blk = blk[eidx[seg_ptr[:-1]]]
    nt_seg = np.diff(seg_ptr).astype(np.float64)
    nc_seg = seg_nc.astype(np.float64)
    N_seg = nt_seg + nc_seg
    seg_claim = np.repeat(np.arange(k), np.diff(csp))
    w2_seg = (nt_seg / N_T[seg_claim].astype(np.float64)) ** 2
    ones = np.ones(k)
    print(f"  state ready · k {k:,} · nnz {len(eidx):,} · n_seg {nseg:,} · "
          f"RSS {rss():.2f} GB · preflight PASS", flush=True)

    def se_for(tie):
        varU = ((N_seg + 1.0) - tie[seg_blk] / (N_seg * (N_seg - 1.0))) / (12.0 * nt_seg * nc_seg)
        return np.sqrt(np.add.reduceat(w2_seg * varU, csp[:-1]))

    rows, done = [], set()
    if os.path.exists(LEDGER):
        L = pd.read_parquet(LEDGER); rows = L.to_dict("records")
        # A resume re-reads the outcome sidecar, so it could silently join rows computed on
        # two different data vintages. Rows written after this patch carry the digest inline;
        # rows written before it are covered by T5_15M_CAPABILITY_DATA_VINTAGE, which attests
        # the vintage they used. Either way the digest must equal today's, or the resume stops.
        if "outcome_source_digest" in L.columns:
            bad = sorted(set(L.outcome_source_digest[L.outcome_source_digest != y_digest]))
            if bad:
                raise RuntimeError(
                    f"ledger rows carry outcome digest {bad} but the file now hashes to "
                    f"{y_digest}. Two data vintages may not be joined — restore the vintage, "
                    f"or delete the ledger and restart.")
        else:
            vf = "T5_15M_CAPABILITY_DATA_VINTAGE.json"
            if not os.path.exists(vf):
                raise RuntimeError(
                    "ledger has no outcome_source_digest and no vintage attestation exists; "
                    "the vintage of these rows cannot be established. Delete and restart.")
            att = json.load(open(vf))["outcome_source_digest"]
            if att != y_digest:
                raise RuntimeError(f"attested vintage {att} != current {y_digest}")
        for (nn, wi), grp in L.groupby(["needle", "world_id"]):
            ok = (len(grp) == len(DL) and (grp.n_perm == NP_).all()
                  and (grp.spec_digest == SPEC_DIGEST).all()
                  and (grp.claim_order_hash == ORD["claim_order_hash"]).all()
                  and (grp.engine_config == "C/8/B8").all())
            if ok and "population_hash" in grp.columns:
                ok = bool((grp.population_hash == ORD["population_hash"]).all()
                          and (grp.block_assignment_hash
                               == ORD["block_assignment_hash"]).all())
            if ok:
                done.add((nn, int(wi)))
        print(f"  resume · {len(done)} complete (needle, world) cells verified", flush=True)

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
            # Midranks and the tie-corrected SE are properties of the MULTISET, and within-
            # block permutation leaves every block's multiset untouched. So both are computed
            # ONCE per (needle, world, delta) and the permutation merely re-indexes the rank
            # vector. Recomputing them inside the 999-loop would pay for six lexsorts per
            # permutation to arrive at the same numbers.
            sds = []
            for di, d in enumerate(DL):
                r2, tie = midranks_ties(base + d * memb, blk, nb, counts, starts)
                r2_base[:, di] = r2
                sds.append(se_for(tie))
            for di in range(len(DL), BATCH_B):
                r2_base[:, di] = r2_base[:, 0]              # padding lanes, discarded
            batch(r2_base, eidx, seg_ptr, seg_nc, csp, N_T, const, ones, R, Zc)
            obs = [float(R[col, di] / sds[di][col]) for di in range(len(DL))]

            mx = np.empty((len(DL), NP_))
            for p in range(NP_):
                ig = np.random.default_rng([RNG_INNER, ni, wi, p])
                pm = np.lexsort((ig.random(nE), blk))
                np.take(r2_base, pm, axis=0, out=ranksT)
                batch(ranksT, eidx, seg_ptr, seg_nc, csp, N_T, const, ones, R, Zc)
                for di in range(len(DL)):
                    mx[di, p] = float(np.max(R[:, di] / sds[di]))
                if (p + 1) % 250 == 0:
                    print(f"    {nm} world {wi} · perm {p+1}/{NP_} · "
                          f"{(time.time()-tw)/60:.1f}m · RSS {rss():.2f} GB", flush=True)
            wid = hashlib.sha256(f"{SPEC_DIGEST}|{nm}|{wi}|{NP_}|"
                                 f"{list(DL)}".encode()).hexdigest()[:16]
            for di, d in enumerate(DL):
                b95 = float(np.percentile(mx[di], 95))
                rows.append(dict(needle=nm, sealed_j=nd["sealed_j"],
                                 support=nd["treated_overlap"], delta_pp=float(d),
                                 world_id=wi, needle_Z=obs[di], band_p95=b95,
                                 band_median=float(np.percentile(mx[di], 50)),
                                 band_max=float(mx[di].max()),
                                 detected=bool(obs[di] > b95), n_perm=NP_,
                                 spec_digest=SPEC_DIGEST,
                                 claim_order_hash=ORD["claim_order_hash"],
                                 engine_config="C/8/B8", world_identity=wid,
                                 outcome_source_digest=y_digest,
                                 population_hash=ORD["population_hash"],
                                 block_assignment_hash=ORD["block_assignment_hash"],
                                 rng_outer=f"[{RNG_OUTER},{ni},{wi}]",
                                 rng_inner=f"[{RNG_INNER},{ni},{wi},p]",
                                 null_max_hash=hashlib.sha256(
                                     mx[di].tobytes()).hexdigest()[:16]))
            atomic_parquet(pd.DataFrame(rows), LEDGER)
            print(f"  {nm} world {wi+1}/{NW} checkpointed · {(time.time()-tw)/60:.1f}m · "
                  f"total {(time.time()-t0)/3600:.2f}h", flush=True)

    L = pd.DataFrame(rows)
    surf = {nm: {f"{d:+.1f}": f"{int(L[(L.needle==nm)&(L.delta_pp==d)].detected.sum())}/{NW}"
                 for d in DL} for nm, _ in NEEDLES}
    bands = {nm: {f"{d:+.1f}": dict(
        p95_median=round(float(L[(L.needle==nm)&(L.delta_pp==d)].band_p95.median()), 3),
        p95_min=round(float(L[(L.needle==nm)&(L.delta_pp==d)].band_p95.min()), 3),
        p95_max=round(float(L[(L.needle==nm)&(L.delta_pp==d)].band_p95.max()), 3))
        for d in DL} for nm, _ in NEEDLES}
    payload = dict(
        spec_id="T5_15M_CAPABILITY_RANK_V2_RESULT",
        capability_spec=SPEC_DIGEST, claim_order_hash=ORD["claim_order_hash"],
        k=k, worlds=NW, n_perm=NP_, needles={nm: dict(
            sealed_j=nd["sealed_j"], representative=nd["representative"],
            support=nd["treated_overlap"]) for nm, nd in NEEDLES},
        surface=surf, null_band_summary=bands,
        engine=dict(kernel="Numba fused segmented", threads=8, batch_B=BATCH_B,
                    padding_lanes=BATCH_B - len(DL)),
        denominator="tie-corrected exact randomization variance, rebuilt per "
                    "(needle, world, delta) on that injected multiset",
        integrity=dict(preflight=g, tie_gate=tg,
                       cells=len(L), expected_cells=3 * NW * len(DL)),
        contains_no="historical observed Z, theta, ranking or winner — every world destroys "
                    "the association before injecting, so no historical quantity exists here",
        hours=round((time.time() - t0) / 3600, 2))
    dig = ART.seal(payload, OUT, required=("spec_id", "surface", "needles"))
    print(f"\n  ALL CELLS COMPLETE · {len(L)}/{3*NW*len(DL)} · sealed {OUT} · {dig}")
    print("  15m RANK DETECTION SURFACE")
    for nm, nd in NEEDLES:
        print(f"    {nm}  {nd['representative']}  n={nd['treated_overlap']:,}")
        for d in DL:
            print(f"      δ {d:>+4.1f} pp   {surf[nm][f'{d:+.1f}']}", flush=True)


if __name__ == "__main__":
    main()
