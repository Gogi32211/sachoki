"""T5_15M_SEALED_HISTORICAL — production SE gates, then the historical compute. Show nobody.

The machine reads the real 15m MFE_10D and computes the observed and permutation objects. The
human sees shapes, hashes and integrity verdicts. Exposure is a separate command.

THE PRODUCTION DENOMINATOR IS NOT COVERED BY THE ENGINE QUALIFICATION

The engine was qualified with the NO-TIES variance, which needs only block and arm sizes. The
capability run rebuilt a tie-corrected SE per injected multiset. Neither certifies the SE built
once on the REAL historical multiset, so it gets its own gates here: positivity, finiteness,
agreement with the sealed order's own counts, a deterministic rebuild, and a sample checked
against a blockwise reference that recomputes tie sums from the block's raw Y rather than from
the precomputed per-block array.

THE HISTORICAL BAND IS THE HISTORICAL RUN'S OWN

The capability measured ~4.60 for the max-null p95. That is calibration and sanity, NOT the
cutoff. Promotion uses the p95 of THIS run's own 999 permutations, strictly greater.

RAW THETA IS COMPUTED FOR SURVIVORS AND A TOP SLICE, NOT FOR ALL 31,583

theta is a median difference, so it needs the CONTROL episodes of every (claim, block): 1.48
billion indices, 5.9 GB, and 48.2M pairs of medians — which in NumPy is days, and in a new JIT
kernel is a new unvalidated kernel. theta is magnitude-only under the contract and is reported
for survivors, so it is computed for the survivors plus the top slice by Z_rank. That is a
stated limit of this run, not a silent one.
"""
from __future__ import annotations
import os, sys
os.environ["NUMBA_NUM_THREADS"] = "8"
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, math, resource, tempfile, time                  # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402
import t5_15m_par_qualify as PQ                                       # noqa: E402
os.environ["NUMBA_NUM_THREADS"] = "8"
from numba import njit, prange, get_num_threads                       # noqa: E402

ORD = json.load(open("T5_15M_CLAIM_ORDER_V2.json"))
CAP = json.load(open("T5_15M_CAPABILITY_RANK_V2.json"))
CAPRES = json.load(open("T5_15M_CAPABILITY_RANK_V2_RESULT.json"))
DATA = os.path.join(D.ROOT, "data")
SE_OUT   = os.path.join(HERE, "T5_15M_PRODUCTION_SE_GATE.json")
SPEC_OUT = os.path.join(HERE, "T5_15M_HISTORICAL_SPEC.json")
RES_OUT  = os.path.join(HERE, "T5_15M_SEALED_HISTORICAL.json")
OBS_PQ   = os.path.join(DATA, "t5_15m_hist_observed.parquet")
ZP_PQ    = os.path.join(DATA, "t5_15m_hist_Z_rank_perm.parquet")

N_PERM   = 999
BATCH_B  = 8
RNG_NULL = 20260931
SE_SAMPLE = 400
THETA_TOP = 500


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


def midranks_ties(Y, blk, nb, starts):
    o = np.lexsort((Y, blk))
    bs, ys = blk[o], Y[o]
    pos = np.arange(len(o), dtype=np.float64) - starts[bs]
    new = np.empty(len(o), bool); new[0] = True
    np.not_equal(bs[1:], bs[:-1], out=new[1:])
    new[1:] |= ys[1:] != ys[:-1]
    gid = np.cumsum(new) - 1
    gsum = np.bincount(gid, weights=pos + 1.0)
    gcnt = np.bincount(gid).astype(np.float64)
    mr = np.empty(len(o)); mr[o] = (gsum / gcnt)[gid]
    tie = np.bincount(bs[new], weights=gcnt ** 3 - gcnt, minlength=nb)
    r2 = np.rint(mr * 2.0).astype(np.int32)
    if not np.allclose(r2, mr * 2.0, atol=1e-9):
        raise RuntimeError("midrank doubling is not integral")
    return r2, tie


def se_reference(c, eidx, seg_ptr, seg_nc, csp, y, blk, blk_start, blk_n):
    """Var(R_s) rebuilt for ONE claim from the raw Y of each block.

    Independent of the vectorised path: tie groups are recounted from the block's own values
    rather than read from the precomputed per-block tie array, and the weights are formed per
    block instead of by reduceat.
    """
    a, b = csp[c], csp[c + 1]
    NT = 0
    for s in range(a, b):
        NT += seg_ptr[s + 1] - seg_ptr[s]
    V = 0.0
    for s in range(a, b):
        T = eidx[seg_ptr[s]:seg_ptr[s + 1]]
        nt = float(len(T)); nc = float(seg_nc[s]); N = nt + nc
        bi = blk[T[0]]
        vals = y[blk_start[bi]:blk_start[bi] + blk_n[bi]]
        _, cnts = np.unique(vals, return_counts=True)
        tie = float(np.sum(cnts.astype(np.float64) ** 3 - cnts.astype(np.float64)))
        varU = ((N + 1.0) - tie / (N * (N - 1.0))) / (12.0 * nt * nc)
        w = nt / NT
        V += w * w * varU
    return math.sqrt(V)


def theta_for(claims, eidx, seg_ptr, seg_nc, csp, y, blk, blk_start, blk_n):
    """Weighted median difference for a handful of claims. Controls are derived from the
    block-sorted layout rather than materialised: 1.48e9 control indices do not fit."""
    out_t = np.empty(len(claims)); out_c = np.empty(len(claims)); out = np.empty(len(claims))
    for k, c in enumerate(claims):
        a, b = csp[c], csp[c + 1]
        NT = 0
        for s in range(a, b):
            NT += seg_ptr[s + 1] - seg_ptr[s]
        wt = wc = 0.0
        for s in range(a, b):
            T = eidx[seg_ptr[s]:seg_ptr[s + 1]]
            bi = blk[T[0]]
            lo = blk_start[bi]; n = blk_n[bi]
            m = np.zeros(n, bool); m[T - lo] = True
            vals = y[lo:lo + n]
            w = len(T) / NT
            wt += w * np.median(vals[m])
            wc += w * np.median(vals[~m])
        out_t[k] = wt; out_c[k] = wc; out[k] = wt - wc
    return out, out_t, out_c


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    print("T5_15M_SEALED_HISTORICAL · production SE gates then the sealed compute", flush=True)

    z = np.load(PQ.BUNDLE)
    A = {n: z[n] for n in PQ.ARRS}
    k = int(z["k"][0]); nE = int(z["nE"][0])
    eidx, seg_ptr, seg_nc = A["eidx"], A["seg_ptr"], A["seg_nc"]
    csp, N_T, const = A["claim_seg_ptr"], A["N_T"], A["const"]

    P = pd.read_parquet(os.path.join(DATA, "t5_15m_population.parquet"))
    pop_hash = hashlib.sha256("|".join(P.episode_id).encode()).hexdigest()[:16]
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    blk = P.block.to_numpy(np.int64)
    nb = int(blk.max()) + 1
    blk_n = np.bincount(blk, minlength=nb)
    blk_start = np.r_[0, np.cumsum(blk_n)[:-1]]
    starts = blk_start.astype(np.float64)

    O = pd.read_parquet(os.path.join(DATA, "t5_15m_order.parquet")).sort_values("j")
    OY_PATH = os.path.join(DATA, "t5_episode_outcomes.parquet")
    y_digest = ART.file_digest(OY_PATH)
    OY = pd.read_parquet(OY_PATH, columns=["episode_id", "mfe_10d"])
    y = P.merge(OY, on="episode_id", how="left").mfe_10d.to_numpy(float) * 100.0
    if not np.isfinite(y).all():
        raise RuntimeError("MFE_10D missing for some sealed-population episodes")

    # ── PHASE 1 · production analytic_sd + gates ────────────────────────────
    seg_blk = blk[eidx[seg_ptr[:-1]]]
    nt_seg = np.diff(seg_ptr).astype(np.float64)
    nc_seg = seg_nc.astype(np.float64)
    N_seg  = nt_seg + nc_seg
    seg_claim = np.repeat(np.arange(k), np.diff(csp))
    w2_seg = (nt_seg / N_T[seg_claim].astype(np.float64)) ** 2

    def build_sd(yy):
        _, tie = midranks_ties(yy, blk, nb, starts)
        varU = ((N_seg + 1.0) - tie[seg_blk] / (N_seg * (N_seg - 1.0))) / (12.0 * nt_seg * nc_seg)
        return np.sqrt(np.add.reduceat(w2_seg * varU, csp[:-1])), tie

    sd, tie_hist = build_sd(y)
    sd2, _ = build_sd(y)                       # deterministic rebuild
    d1 = hashlib.sha256(sd.tobytes()).hexdigest()[:16]
    d2 = hashlib.sha256(sd2.tobytes()).hexdigest()[:16]

    g = {"sd_positive": bool((sd > 0).all()),
         "sd_finite": bool(np.isfinite(sd).all()),
         "treated_overlap_matches_order": bool(np.array_equal(
             np.add.reduceat(nt_seg, csp[:-1]).astype(np.int64), O.treated_overlap.to_numpy())),
         "n_overlap_blocks_matches_order": bool(np.array_equal(
             np.diff(csp).astype(np.int64), O.n_overlap_blocks.to_numpy())),
         "deterministic_rebuild": d1 == d2,
         "population_hash": pop_hash == ORD["population_hash"],
         "k_31583": k == ORD["k"] == 31583,
         "threads_8": int(get_num_threads()) == 8}

    gsam = np.random.default_rng(20260932).choice(k, SE_SAMPLE, replace=False)
    worst = 0.0
    for c in gsam:
        ref = se_reference(int(c), eidx, seg_ptr, seg_nc, csp, y, blk, blk_start, blk_n)
        worst = max(worst, abs(ref - sd[c]) / max(sd[c], 1e-18))
    g["sd_vs_blockwise_reference"] = worst <= 1e-10

    for kk, vv in g.items():
        print(f"    {'✓' if vv else '✗'} {kk}", flush=True)
    print(f"    sample reference: {SE_SAMPLE} claims · max relative |Δ| {worst:.2e}", flush=True)
    if not all(g.values()):
        raise RuntimeError(f"SE gate failed: {[kk for kk, vv in g.items() if not vv]}")

    n_tie_blocks = int((tie_hist > 0).sum())
    ART.seal(dict(
        spec_id="T5_15M_PRODUCTION_SE_GATE", status="PASS",
        built_on="the real historical MFE_10D multiset, once",
        formula="Var_0(U_sb) = [ (N+1) - sum(t^3-t)/(N(N-1)) ] / (12 n_t n_c); "
                "V_s = sum_b w_sb^2 Var_0(U_sb)",
        not_covered_by="the engine qualification used the NO-TIES variance; the capability "
                       "rebuilt an SE per injected multiset. Neither certifies this one.",
        gates=g, sample_claims=SE_SAMPLE, max_relative_diff=float(worst),
        reference_method="per claim, per block: tie groups recounted from the block's raw Y, "
                         "weights formed per block — not read from the precomputed arrays",
        analytic_sd_digest=d1, blocks_with_ties=n_tie_blocks, blocks=nb,
        outcome_source_digest=y_digest, population_hash=pop_hash,
        claim_order_hash=ORD["claim_order_hash"], k=k),
        SE_OUT, required=("spec_id", "gates", "analytic_sd_digest"))
    print(f"  SE gate PASS · blocks with ties {n_tie_blocks:,}/{nb:,} · "
          f"sd digest {d1} · RSS {rss():.2f} GB", flush=True)

    # ── PHASE 2 · freeze the historical spec ───────────────────────────────
    spec = dict(
        spec_id="T5_15M_HISTORICAL_SPEC", status="FROZEN_BEFORE_COMPUTE",
        population_hash=ORD["population_hash"], claim_order_hash=ORD["claim_order_hash"],
        block_assignment_hash=ORD["block_assignment_hash"], k=k,
        statistic="one-sided Z_rank = R_s / sqrt(V_s), bounded blockwise Mann-Whitney",
        direction="one-sided positive",
        multiplicity="search-wide max Z_rank over all k claims, one permutation mapping "
                     "applied to every claim",
        n_perm=N_PERM,
        promotion="Z_rank_obs > p95( max-null Z_rank ) from THIS run's own permutations, "
                  "STRICTLY greater",
        band_note="the capability's ~4.60 is calibration and sanity only, never the cutoff",
        primary_outcome="MFE_10D",
        raw_theta="magnitude only; never a promotion criterion",
        mfe_atr="registered secondary characterization only; cannot alter promotion",
        theta_scope=f"computed for survivors plus the top {THETA_TOP} by Z_rank. All k would "
                    f"need 1,481,544,346 control indices (5.9 GB) and 48.2M median pairs — a "
                    f"new unvalidated kernel. Stated limit, not a silent one.",
        capability=dict(artifact="T5_15M_CAPABILITY_RANK_V2_RESULT",
                        digest=ART.file_digest("T5_15M_CAPABILITY_RANK_V2_RESULT.json"),
                        surface=CAPRES["surface"],
                        scope="Under the registered homogeneous additive MFE-shift "
                              "alternative, the q50 needle detected +1pp in 20/20 finite "
                              "capability worlds. Measured finite-suite sensitivity, NOT "
                              "guaranteed power against any real +1pp effect."),
        se_gate=ART.file_digest(SE_OUT),
        outcome_source_digest=y_digest,
        forbidden_claims=["the 15m result confirms or validates any 1H claim",
                          "15m is an independent replication",
                          "a surviving claim is a 15m edge"],
        y_status="HISTORICAL 15m RANKING NOT COMPUTED AT THE TIME OF THIS FREEZE")
    spec_dig = ART.seal(spec, SPEC_OUT, required=("spec_id", "k", "promotion"))
    print(f"  historical spec FROZEN · {spec_dig}", flush=True)

    # ── PHASE 3 · observed + null ──────────────────────────────────────────
    r2, _ = midranks_ties(y, blk, nb, starts)
    ones = np.ones(k)
    R = np.empty((k, BATCH_B)); Zb = np.empty((k, BATCH_B))
    ranksT = np.empty((nE, BATCH_B), np.int32)
    for b in range(BATCH_B):
        ranksT[:, b] = r2
    batch(ranksT, eidx, seg_ptr, seg_nc, csp, N_T, const, ones, R, Zb)
    R_obs = R[:, 0].copy()
    Z_obs = R_obs / sd
    print(f"  observed computed · RSS {rss():.2f} GB", flush=True)

    ZP = np.empty((N_PERM, k))
    seen = set()
    tp = time.time(); p = 0
    while p < N_PERM:
        nb_lane = min(BATCH_B, N_PERM - p)
        for b in range(nb_lane):
            gg = np.random.default_rng([RNG_NULL, p + b])
            pm = np.lexsort((gg.random(nE), blk))
            seen.add(hashlib.sha256(pm.tobytes()).hexdigest())
            ranksT[:, b] = r2[pm]
        for b in range(nb_lane, BATCH_B):
            ranksT[:, b] = ranksT[:, 0]
        batch(ranksT, eidx, seg_ptr, seg_nc, csp, N_T, const, ones, R, Zb)
        for b in range(nb_lane):
            ZP[p + b] = R[:, b] / sd
        p += nb_lane
        if p % 200 < BATCH_B:
            print(f"    perm {p}/{N_PERM} · {(time.time()-tp)/60:.1f}m", flush=True)

    max_null = ZP.max(axis=1)
    band = float(np.percentile(max_null, 95))
    surv = Z_obs > band                                  # STRICTLY greater
    n_surv = int(surv.sum())

    # ── PHASE 4 · theta for survivors + top slice ──────────────────────────
    top = np.argsort(-Z_obs)[:THETA_TOP]
    want = np.unique(np.concatenate([np.flatnonzero(surv), top]))
    th, tmed, cmed = theta_for(want, eidx, seg_ptr, seg_nc, csp, y, blk, blk_start, blk_n)
    theta = np.full(k, np.nan); wtr = np.full(k, np.nan); wct = np.full(k, np.nan)
    theta[want] = th; wtr[want] = tmed; wct[want] = cmed

    ordj = O.reset_index(drop=True)
    OB = pd.DataFrame(dict(
        j=ordj.j.to_numpy(), claim_id=ordj.representative.to_numpy(),
        position_family=ordj.position_family.to_numpy(),
        support=ordj.treated_overlap.to_numpy(), analytic_sd=sd,
        R_rank_obs=R_obs, Z_rank_obs=Z_obs, survives=surv,
        theta_raw_obs=theta, weighted_treated_median=wtr, weighted_control_median=wct,
        theta_computed=np.isin(np.arange(k), want)))
    atomic_parquet(OB, OBS_PQ)
    atomic_parquet(pd.DataFrame(ZP.astype(np.float32),
                                columns=[f"j{i}" for i in range(k)]), ZP_PQ)

    ah = hashlib.sha256(R_obs.tobytes() + Z_obs.tobytes() + ZP.tobytes()).hexdigest()[:16]
    payload = dict(
        spec_id="T5_15M_SEALED_HISTORICAL", status="SEALED — NOT EXPOSED",
        historical_spec=SPEC_OUT, historical_spec_digest=spec_dig,
        se_gate_digest=ART.file_digest(SE_OUT),
        population_hash=pop_hash, claim_order_hash=ORD["claim_order_hash"],
        block_assignment_hash=ORD["block_assignment_hash"],
        outcome_source_digest=y_digest,
        n_population=nE, n_claims=k, n_permutations=N_PERM, n_blocks=nb,
        observed_shapes=dict(R_rank_obs=[k], Z_rank_obs=[k], analytic_sd=[k]),
        permutation_shapes=dict(Z_rank_perm=[N_PERM, k], max_null=[N_PERM]),
        theta_computed_for=int(len(want)),
        rng=dict(null_stream=f"[{RNG_NULL}, p]", distinct_mappings=len(seen)),
        integrity=dict(se_gates=g, finite_observed=bool(np.isfinite(Z_obs).all()),
                       finite_null=bool(np.isfinite(ZP).all()),
                       null_matrix_complete=bool(ZP.shape == (N_PERM, k)),
                       rng_streams_unique=bool(len(seen) == N_PERM),
                       batch_lanes=BATCH_B, threads=8),
        artifact_hash=ah,
        artifacts=dict(observed=os.path.basename(OBS_PQ),
                       Z_rank_perm=os.path.basename(ZP_PQ)),
        z_perm_dtype="float32 on disk (252 MB as float64); the max-null and every gate above "
                     "were computed in float64 before the cast",
        contains_but_does_not_report=["claim ranking", "the observed maximum", "the band",
                                      "the survivor count", "any claim's Z or theta"],
        exposure="REQUIRES A SEPARATE EXPOSE COMMAND",
        minutes=round((time.time() - t0) / 60, 1))
    dig = ART.seal(payload, RES_OUT, required=("spec_id", "artifact_hash", "n_claims"))

    print(f"\n  n_population {nE:,} · n_claims {k:,} · n_blocks {nb:,} · "
          f"n_permutations {N_PERM}")
    print(f"  observed vectors      3 x [{k:,}]")
    print(f"  permutation matrix    Z_rank_perm [{N_PERM}, {k:,}] · max_null [{N_PERM}]")
    print(f"  theta computed for    {len(want):,} claims (survivors + top {THETA_TOP})")
    for nm, v in (("SE gate", all(g.values())), ("finite observed", np.isfinite(Z_obs).all()),
                  ("finite null", np.isfinite(ZP).all()),
                  ("null matrix complete", ZP.shape == (N_PERM, k)),
                  ("RNG streams unique", len(seen) == N_PERM),
                  ("population hash", g["population_hash"])):
        print(f"    {nm:<22} {'PASS' if v else 'FAIL'}")
    print(f"  artifact hash {ah} · sealed {dig}")
    print(f"\n  SEALED. Nothing about the result has been printed.")
    print(f"  EXPOSE is a separate command.  ({(time.time()-t0)/60:.1f} min)", flush=True)


if __name__ == "__main__":
    main()
