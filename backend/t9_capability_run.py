"""T9 finite-suite capability — under the FROZEN protocol (T9_CAPABILITY_PROTOCOL_V1).

    needles      q10 / q50 / q90 from the SEALED claim order
    alternative  registered homogeneous additive raw-MFE shift
    grid         +0.5 +1 +2 +3 +4 +5 pp · 20 worlds per (needle, delta) · 999 inner perms

THE CAUSAL ORDER IS THE PROTOCOL

    sealed X/membership + historical raw MFE
      -> DESTROY the observed sequence<->Y association          (permute Y within blocks
                                                                 BEFORE anything else)
      -> inject delta ONLY into the frozen needle membership
      -> rebuild raw-MFE midranks per block
      -> rebuild the tie-corrected analytic SD
      -> 999 inner max-Z permutations over all k=890 claims
      -> detection: Z_needle > p95(max-Z null), STRICT

FORBIDDEN, STRUCTURALLY: the engine permutes FIRST, always — the observed historical
assignment never meets a statistic. No observed Z_rank, no survivor count, no winner
names, no theta ranking is computed, stored or printed anywhere in this module.

    Y values used for capability noise  !=  historical T9 result exposed

STATISTIC (the qualified V2 design, rebuilt per world because injection changes Y):

    per block b:  U_sb = (sum midranks of treated - n_t(N_b+1)/2) / (n_t n_c) + 0.5
                  Var0(U_sb) = [(N_b+1) - sum(t^3-t)/(N_b(N_b-1))] / (12 n_t n_c)
    R_s = sum_b w_sb (U_sb - 0.5),  w_sb = n_t,sb / N_t,s
    Z_s = R_s / sqrt(sum_b w_sb^2 Var0(U_sb))

ALLOWED WORDING ONLY: "Under the registered homogeneous additive MFE-shift alternative,
the qXX needle detected +Xpp in Y/20 finite capability worlds."
"""
from __future__ import annotations
import hashlib, json, math, os, resource, sys, time                    # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t9_dna as D9                                # noqa: E402
import t9_sequence_grammar as G9, t9_sequence_estimand as E9           # noqa: E402

OUT = "T9_CAPABILITY_RESULT_V1.json"
RUN_TAG = "T9_CAPABILITY_EVIDENCE_V2"          # the SHA256-derived, fully replayable run
ORDER_PARQ = os.path.join(D9.ROOT, "data", "t9_claim_order_v1.parquet")
POP_PARQ = os.path.join(D9.ROOT, "data", "t9_estimand_population.parquet")
DELTAS = (0.5, 1.0, 2.0, 3.0, 4.0, 5.0)          # pp on raw MFE (which is a fraction)
WORLDS = 20
NPERM = 999
SEED0 = 20260821                              # superseded engineering run's root; kept for
                                              # the record only — the stochastic path below
                                              # never uses it and never uses builtin hash()


def _u64(digest_bytes):
    return int.from_bytes(digest_bytes[:8], "big")


def world_seed(hashes, needle_membership_hash, delta, w):
    m = hashlib.sha256()
    for part in ("T9_CAPABILITY_WORLD_V1", hashes["claim_order_hash"],
                 hashes["population_hash"], hashes["block_assignment_hash"],
                 hashes["capability_protocol_hash"], needle_membership_hash,
                 f"{delta:.1f}", str(w)):
        m.update(part.encode()); m.update(b"|")
    return m.digest()


def perm_seed(wseed_bytes, p):
    m = hashlib.sha256()
    m.update(b"T9_CAPABILITY_PERM_V1|"); m.update(wseed_bytes)
    m.update(b"|"); m.update(str(p).encode())
    return _u64(m.digest())


def build_state(verbose=True):
    ORDER = pd.read_parquet(ORDER_PARQ)
    P = pd.read_parquet(POP_PARQ)
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    ids = P.episode_id.to_numpy()
    bcode, blocks = pd.factorize(P.block)
    nb = len(blocks)

    # Y: raw MFE_10D for the population — VALUES for the engine only, never printed
    OC = pd.read_parquet(E9.OC, columns=["episode_id", "mfe_10d"])
    y = P[["episode_id"]].merge(OC, on="episode_id", how="left").mfe_10d.to_numpy()
    assert not np.isnan(y).any(), "population must be MFE10-AVAILABLE by construction"

    # membership matrix over the 890 sealed classes (via each class representative)
    C = pd.read_parquet(G9.SURV)
    toks = pd.read_parquet(G9.TOKS).token.tolist()
    MEM = E9.membership(toks, C)
    MEM = MEM.set_index("episode_id").reindex(ids)
    rep_cols = (ORDER.family + "|" + ORDER.length.astype(str) + "|" + ORDER.representative)
    S = np.zeros((len(ids), len(ORDER)), dtype=bool)
    for k, col in enumerate(rep_cols):
        S[:, k] = MEM[col].to_numpy()
    if verbose:
        print(f"  state · pop {len(ids):,} · blocks {nb:,} · k {S.shape[1]} · "
              f"membership nnz {int(S.sum()):,}", flush=True)
    return ids, bcode, nb, y, S, ORDER


def block_structs(bcode, nb):
    order = np.argsort(bcode, kind="stable")
    ptr = np.zeros(nb + 1, np.int64)
    np.add.at(ptr, bcode + 1, 1)
    ptr = np.cumsum(ptr)
    return order, ptr


def midranks_and_ties(yv, border, bptr, nb):
    """Per-block midranks of yv, plus the tie term sum(t^3-t) per block."""
    r = np.empty(len(yv))
    tie = np.zeros(nb)
    for b in range(nb):
        idx = border[bptr[b]:bptr[b + 1]]
        vals = yv[idx]
        o = np.argsort(vals, kind="stable")
        sv = vals[o]
        rk = np.empty(len(sv))
        i = 0
        while i < len(sv):
            j = i
            while j + 1 < len(sv) and sv[j + 1] == sv[i]:
                j += 1
            rk[i:j + 1] = 0.5 * (i + j) + 1.0
            t = j - i + 1
            if t > 1:
                tie[b] += t ** 3 - t
            i = j + 1
        r[idx[o]] = rk
    return r, tie


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    PROT = json.load(open("T9_CAPABILITY_PROTOCOL_V1.json"))
    CO = json.load(open("T9_1H_CLAIM_ORDER_V1.json"))
    ids, bcode, nb, y_hist, S, ORDER = build_state()
    Nb = np.bincount(bcode, minlength=nb).astype(float)
    border, bptr = block_structs(bcode, nb)
    needles = CO["needles"]

    # ── THE FOLDED ENGINE (qualified T5 design): R_s = A·r − c_s ──────────
    # a_si = 1/(Nt_s · nc_s,b(i)) for treated member i in an overlap block; both the
    # sparse pattern and c_s are FIXED across worlds and permutations, because injection
    # changes Y values, not membership, and permutation changes rank ASSIGNMENT only.
    # Per world only sd_s moves (tie correction). Per inner permutation only r moves.
    from scipy import sparse
    print("  folding claim geometry into the sparse operator …", flush=True)
    NT = np.zeros((nb, S.shape[1]))
    for k in range(S.shape[1]):
        NT[:, k] = np.bincount(bcode[S[:, k]], minlength=nb)
    NC = Nb[:, None] - NT
    OV = (NT > 0) & (NC > 0)
    Ntot = (NT * OV).sum(0)                                   # N_t,s over overlap blocks
    rows, cols, vals = [], [], []
    for k in range(S.shape[1]):
        m = np.flatnonzero(S[:, k])
        bm = bcode[m]
        keep = OV[bm, k]
        m = m[keep]; bm = bm[keep]
        rows.append(np.full(len(m), k)); cols.append(m)
        vals.append(1.0 / (Ntot[k] * NC[bm, k]))
    A = sparse.csr_matrix((np.concatenate(vals),
                           (np.concatenate(rows), np.concatenate(cols))),
                          shape=(S.shape[1], len(ids)))
    c_s = np.array([(NT[:, k] * OV[:, k] * (Nb + 1) / 2
                     / (Ntot[k] * np.where(NC[:, k] > 0, NC[:, k], 1))
                     )[OV[:, k]].sum() for k in range(S.shape[1])])
    print(f"  A nnz {A.nnz:,} · k {S.shape[1]} · fixed across all worlds", flush=True)

    bc_at_border = bcode[border].astype(float)                # for vectorized block shuffles

    def sd_from_tie(tie):
        with np.errstate(divide="ignore", invalid="ignore"):
            var = ((Nb[:, None] + 1) - (tie / np.where(Nb > 1, Nb * (Nb - 1), 1))[:, None]) \
                  / (12 * NT * NC)
        var = np.where(OV, var, 0.0)
        w = np.where(OV, NT, 0.0)
        w = w / np.where(w.sum(0) > 0, w.sum(0), 1)
        return np.sqrt((w ** 2 * var).sum(0))

    def shuffle_within_blocks(r, rng):
        key = bc_at_border + rng.random(len(border))
        return_positions = border[np.argsort(key, kind="stable")]
        rp = np.empty_like(r)
        rp[border] = r[return_positions]
        return rp

    hashes = dict(claim_order_hash=CO["claim_order_hash"],
                  population_hash=CO["population_hash"],
                  block_assignment_hash=CO["block_assignment_hash"],
                  capability_protocol_hash=ART.file_digest(
                      "T9_CAPABILITY_PROTOCOL_V1.json"))
    seed_manifest = {}
    results, p95s = [], {}
    det_tbl = {}
    for nk, nd in needles.items():
        col = int(nd["j"])
        # sealed j indexes the ORDER frame; column k in S is ORDER row order
        krow = int(ORDER.index[ORDER.j == nd["j"]][0])
        for delta in DELTAS:
            det = 0
            bands = []
            for w in range(WORLDS):
                wseed = world_seed(hashes, nd["membership_hash"], delta, w)
                seed_manifest[f"{nk}|{delta:.1f}|{w}"] = wseed.hex()[:16]
                rng = np.random.default_rng(_u64(wseed))
                # 1) DESTROY the observed association: permute Y within blocks
                y = y_hist.copy()
                for b in range(nb):
                    idx = border[bptr[b]:bptr[b + 1]]
                    y[idx] = y[idx][rng.permutation(len(idx))]
                # 2) inject delta ONLY into the frozen needle membership
                y[S[:, krow]] += delta / 100.0
                # 3) rebuild midranks + tie-corrected structures
                r, tie = midranks_and_ties(y, border, bptr, nb)
                sd = sd_from_tie(tie)
                z_obs = ((A @ r - c_s) / np.where(sd > 0, sd, 1.0))[krow]
                # 4) inner 999 max-Z permutations (ranks permute within blocks; the block
                #    rank multiset — and therefore the tie term — is fixed)
                mx = np.empty(NPERM)
                inv_sd = 1.0 / np.where(sd > 0, sd, 1.0)
                for p in range(NPERM):
                    rng_p = np.random.default_rng(perm_seed(wseed, p))
                    rp = shuffle_within_blocks(r, rng_p)
                    mx[p] = (((A @ rp) - c_s) * inv_sd).max()
                p95 = np.sort(mx)[math.ceil(0.95 * NPERM) - 1]
                bands.append(float(p95))
                if z_obs > p95:
                    det += 1
            det_tbl[f"{nk}|+{delta}pp"] = det
            p95s[f"{nk}|+{delta}pp"] = dict(median=float(np.median(bands)),
                                            min=float(np.min(bands)),
                                            max=float(np.max(bands)))
            print(f"  {nk} +{delta}pp · detections {det}/20 · p95 med "
                  f"{np.median(bands):.3f} [{np.min(bands):.3f},{np.max(bands):.3f}] · "
                  f"{time.time()-t0:.0f}s", flush=True)

    peak_gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**30
    ART.seal(dict(
        spec_id="T9_CAPABILITY_RESULT_V1", status="COMPLETE",
        protocol_digest=ART.file_digest("T9_CAPABILITY_PROTOCOL_V1.json"),
        claim_order_digest=ART.file_digest("T9_1H_CLAIM_ORDER_V1.json"),
        k=int(S.shape[1]), population=int(len(ids)), blocks=int(nb),
        needles={k: dict(j=v["j"], representative=v["representative"],
                         support=v["support"]) for k, v in needles.items()},
        deltas_pp=list(DELTAS), worlds=WORLDS, inner_permutations=NPERM,
        detections=det_tbl, max_null_p95=p95s,
        wording="Under the registered homogeneous additive MFE-shift alternative, each "
                "cell reports detections out of 20 finite capability worlds.",
        forbidden_confirmed=["no observed Z_rank computed", "no survivor ranking",
                             "no winner inspection — the engine permutes before any "
                             "statistic touches the historical assignment"],
        runtime_min=round((time.time() - t0) / 60, 1), peak_rss_gb=round(peak_gb, 2),
        rng=dict(scheme="SHA256-derived, namespaces T9_CAPABILITY_WORLD_V1 / "
                        "T9_CAPABILITY_PERM_V1; first 8 digest bytes as unsigned int; "
                        "builtin hash() nowhere in the stochastic path",
                 seed_manifest_digest=hashlib.sha256(json.dumps(
                     seed_manifest, sort_keys=True).encode()).hexdigest()[:16],
                 n_world_seeds=len(seed_manifest)),
        superseded_engineering_run=dict(
            commit="0e5954e", status="SUPERSEDED / ENGINEERING / NON-EVIDENTIARY",
            defect="world seeds derived via process-salted builtin hash(); externally "
                   "non-replayable",
            observed_cells={"q10|+0.5pp": "4/20", "q10|+1.0pp": "19/20",
                            "q10|+2.0pp": "20/20", "q10|+3.0pp": "20/20",
                            "q10|+4.0pp": "20/20"},
            exclusion="Observed during superseded non-replayable engineering run; excluded "
                      "from capability qualification and not used to modify protocol, "
                      "needles, grid, worlds, permutations, or acceptance.")),
        OUT, required=("spec_id", "detections", "max_null_p95", "needles"))
    print(f"\nT9_CAPABILITY_RESULT_V1 · {ART.file_digest(OUT)} · "
          f"{(time.time()-t0)/60:.1f} min · peak {peak_gb:.2f} GB")


if __name__ == "__main__":
    main()
