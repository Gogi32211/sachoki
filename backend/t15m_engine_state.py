"""{F}_15M_ENGINE_STATE_V1 — the immutable index structure the capability kernel runs on.

This is a DERIVATION of the sealed claim order, not a revision of it. The order artifact is
already approved and is not rewritten; what the engine needs and the order does not carry —
per-claim segment counts and the global nnz / n_segments totals — is recorded here and
sealed on its own, so a later rebuild has something to be checked against.

Structure and formulas are lifted from t5_15m_engine_qualify.State verbatim:

    episodes sorted BY BLOCK, so a claim's treated indices come out already grouped
    OVERLAP RESTRICTION: a block whose every episode is treated has no control arm; it
        contributes no contrast and n_c = 0 would make the coefficient infinite
    const   -sum( n_t(n_t+1) / (2 N_T n_c) ) - 0.5
    sd      sqrt( sum w^2 (N_b+1)/(12 n_t n_c) )   — the NO-TIES X-only form; the run
            recomputes the tie-corrected SE per (needle, world, delta)

Every claim's built treated count is asserted equal to the SEALED order's support. A claim
whose membership cannot be rebuilt from X to the number the order sealed is a hard failure.

No Y value is read anywhere in this module.

    usage:  python t15m_engine_state.py t9
"""
from __future__ import annotations
import hashlib, importlib, json, os, sys, time                          # noqa: E402
import numpy as np, pandas as pd                                        # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402
import t15m_x as X                                                      # noqa: E402

DATA = os.path.join(os.path.dirname(HERE), "data")


def build(fam: str):
    t0 = time.time()
    F = fam.upper()
    D = importlib.import_module(f"{fam}_dna")
    E1 = importlib.import_module(f"{fam}_sequence_estimand")
    order_art = json.load(open(f"{F}_15M_CLAIM_ORDER_V1.json"))
    O = pd.read_parquet(os.path.join(DATA, f"{fam}_15m_claim_order.parquet")) \
          .sort_values("j").reset_index(drop=True)
    k = len(O)
    assert k == order_art["k"], (k, order_art["k"])
    print(f"{F} 15m engine state · k {k:,}", flush=True)

    # ── the X cube and the population, from the SAME code path the order used ──
    E, M, T = X.load_opening_hour(fam, D, verbose=False)
    REG, keep = X.searchable(M, T, D)
    tok = [D.TOKENS_1H[i].token_id for i in keep]
    tix = {t: i for i, t in enumerate(tok)}
    piv = M.pivot_table(index="episode_id", columns="pos", values="et_time",
                        aggfunc="first")
    for p in (1, 2, 3, 4):
        if p not in piv.columns:
            piv[p] = np.nan
    ep_ids = piv.index.to_numpy()
    eix0 = {e: i for i, e in enumerate(ep_ids)}
    B0 = np.zeros((4, len(ep_ids), len(tok)), dtype=bool)
    B0[M.pos.to_numpy() - 1, M.episode_id.map(eix0).to_numpy()] = \
        T.to_numpy(dtype=bool)[:, keep]
    del T, M

    P = X.build_population(fam, D, E1, ep_ids)
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    nE = len(P)
    blk, _ = pd.factorize(P.block)          # already block-sorted -> codes ascend
    blk = blk.astype(np.int64)
    blk_n = np.bincount(blk)
    blk_start = np.r_[0, np.cumsum(blk_n)[:-1]]
    # rows of B0 for the population, IN BLOCK ORDER
    row_of = np.array([eix0[e] for e in P.episode_id.to_numpy()])
    B = B0[:, row_of, :]
    del B0
    pop_hash = hashlib.sha256("|".join(sorted(P.episode_id)).encode()).hexdigest()[:16]
    print(f"  population {nE:,} · blocks {len(blk_n):,} · cube {B.nbytes/1e6:.0f} MB · "
          f"{time.time()-t0:.0f}s", flush=True)

    # ── pass 1: sizes ───────────────────────────────────────────────────────
    exp_nt = O.support.to_numpy()
    parts = []
    nnz = nseg = 0
    for c, rep in enumerate(O.representative.to_numpy()):
        pf, seq = rep.split("|")
        a, b = pf.split("→"); ta_, tb_ = seq.split("→")
        v = B[int(a[1:]) - 1][:, tix[ta_]] & B[int(b[1:]) - 1][:, tix[tb_]]
        idx = np.flatnonzero(v)
        bl = blk[idx]
        cut = np.r_[0, np.flatnonzero(bl[1:] != bl[:-1]) + 1, len(bl)]
        nt = np.diff(cut)
        nb_ = blk_n[bl[cut[:-1]]]
        nc = nb_ - nt
        good = nc > 0
        if not good.all():
            idx = idx[np.repeat(good, nt)]
            nt = nt[good]; nc = nc[good]; nb_ = nb_[good]
        NT = int(nt.sum())
        if NT != int(exp_nt[c]):
            raise RuntimeError(f"claim {c} ({rep}): built n_t {NT} but the SEALED order "
                               f"says {int(exp_nt[c])}")
        parts.append((idx, nt.astype(np.int64), nc.astype(np.int64),
                      nb_.astype(np.float64)))
        nnz += len(idx); nseg += len(nt)
        if (c + 1) % 6000 == 0:
            print(f"    {c+1:,}/{k:,} claims · nnz {nnz:,} · nseg {nseg:,} · "
                  f"{time.time()-t0:.0f}s", flush=True)
    del B

    # ── pass 2: pack ────────────────────────────────────────────────────────
    eidx = np.empty(nnz, np.int32)
    seg_ptr = np.empty(nseg + 1, np.int64)
    seg_nc = np.empty(nseg, np.int32)
    claim_seg_ptr = np.empty(k + 1, np.int64)
    N_T = np.empty(k, np.int32)
    const = np.empty(k, np.float64)
    sd = np.empty(k, np.float64)
    n_seg_per_claim = np.empty(k, np.int32)
    pe = ps = 0
    seg_ptr[0] = 0; claim_seg_ptr[0] = 0
    for c, (idx, nt, nc, nb_) in enumerate(parts):
        ns = len(nt); NT = int(nt.sum())
        eidx[pe:pe + len(idx)] = idx
        seg_ptr[ps + 1:ps + 1 + ns] = pe + np.cumsum(nt)
        seg_nc[ps:ps + ns] = nc
        N_T[c] = NT
        n_seg_per_claim[c] = ns
        const[c] = -float(np.sum(nt * (nt + 1.0) / (2.0 * NT * nc))) - 0.5
        w = nt / NT
        sd[c] = float(np.sqrt(np.sum(w * w * (nb_ + 1.0) / (12.0 * nt * nc))))
        pe += len(idx); ps += ns
        claim_seg_ptr[c + 1] = ps
    assert pe == nnz and ps == nseg
    del parts

    bundle = os.path.join(DATA, f"{fam}_15m_engine_state.npz")
    np.savez(bundle, eidx=eidx, seg_ptr=seg_ptr, seg_nc=seg_nc,
             claim_seg_ptr=claim_seg_ptr, N_T=N_T, const=const, sd=sd,
             blk_n=blk_n, blk_start=blk_start, nE=np.array([nE]), k=np.array([k]),
             nnz=np.array([nnz]), nseg=np.array([nseg]))
    P[["episode_id", "ticker", "t5_date", "block"]].to_parquet(
        os.path.join(DATA, f"{fam}_15m_population.parquet"), index=False)

    struct_digest = hashlib.sha256(
        N_T.tobytes() + n_seg_per_claim.tobytes()
        + np.round(const, 12).tobytes() + np.round(sd, 12).tobytes()).hexdigest()[:16]
    d = ART.seal(dict(
        spec_id=f"{F}_15M_ENGINE_STATE_V1", status="X_ONLY_PRE_Y", family=F,
        role="index structure DERIVED from the sealed claim order; the order artifact is "
             "not rewritten",
        governing=dict(claim_order=ART.file_digest(f"{F}_15M_CLAIM_ORDER_V1.json"),
                       claim_order_hash=order_art["order"]["claim_order_hash"],
                       capability_protocol=ART.file_digest(
                           f"{F}_15M_CAPABILITY_PROTOCOL_V1.json"),
                       estimand=ART.file_digest(f"{F}_15M_ESTIMAND_V1.json"),
                       reference_state_code=ART.file_digest("t5_15m_engine_qualify.py")),
        k=k, nE=nE, n_blocks=int(len(blk_n)),
        nnz_treated_memberships=int(nnz), n_segments=int(nseg),
        population_hash=pop_hash,
        structure_digest=struct_digest,
        per_claim_totals_recorded=["N_T", "n_segments", "const", "sd"],
        overlap_restriction="blocks with n_c == 0 are dropped — no control arm, no contrast",
        se_form=dict(built_here="NO-TIES X-only form",
                     used_at_run="tie-corrected SE recomputed per (needle, world, delta) on "
                                 "that injected multiset"),
        verification="every claim's built treated count was asserted equal to the SEALED "
                     "order's support; a claim that could not be rebuilt from X to its "
                     "sealed number is a hard failure",
        bundle=dict(path=os.path.basename(bundle),
                    size_gb=round(os.path.getsize(bundle) / 1e9, 2)),
        outcome_exposure="NOT_EXPOSED",
        runtime_min=round((time.time() - t0) / 60, 1)),
        f"{F}_15M_ENGINE_STATE_V1.json",
        required=("spec_id", "governing", "k", "nnz_treated_memberships", "n_segments"),
        supersede=os.path.exists(f"{F}_15M_ENGINE_STATE_V1.json"))
    print(f"\n  {F}_15M_ENGINE_STATE_V1 · {d} · nnz {nnz:,} · nseg {nseg:,} · "
          f"{os.path.getsize(bundle)/1e9:.2f} GB · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9"]):
        build(f)
