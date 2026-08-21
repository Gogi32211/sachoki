"""T5_15M_CLAIM_ORDER_V2 — seal the 2-bar claim order, population and blocks. X-only.

The order must not depend on enumeration order. Sorting by the estimand membership hash gives
a deterministic sequence that is independent of how the sweep happened to visit token pairs,
and uncorrelated with support, family or anything else that could later look like a choice.

WHAT IS SEALED HERE IS WHAT EVERY LATER STEP INDEXES BY

    claim_order_hash        the ordered membership hashes
    population_hash         the ordered episode ids of the estimand population
    block_assignment_hash   the block label of every episode, in that same order

A needle, a capability run and a historical compute all address claims by j. If any of these
three drifts, j means something different and the artifacts stop being comparable — so they
are hashed once, here, rather than recomputed per script.

NO OUTCOME VALUE IS READ. path_status_10d is consulted for AVAILABILITY only, exactly as the
1H estimand did.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, sys, time                                       # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D, t5_sequence_estimand as EST    # noqa: E402

SCOPE = json.load(open("T5_15M_SEARCH_SCOPE_V2.json"))
ENUM2 = json.load(open("T5_15M_X_ONLY_ENUMERATION_2BAR.json"))
SUB = json.load(open("T5_15M_SCOPE_SUBSET_EQUIVALENCE.json"))
DATA = os.path.join(D.ROOT, "data")
CL = os.path.join(DATA, "t5_15m_claims_2bar.parquet")
MS = os.path.join(DATA, "t5_15m_opening_hour.parquet")
OUT_ORD = os.path.join(DATA, "t5_15m_order.parquet")
OUT_POP = os.path.join(DATA, "t5_15m_population.parquet")
OUT = os.path.join(HERE, "T5_15M_CLAIM_ORDER_V2.json")


def population():
    """The estimand population and blocks, rebuilt by the enumerator's own steps."""
    M = pd.read_parquet(MS, columns=["episode_id", "pos"])
    per = M.groupby("episode_id").pos.agg(["count", "nunique"])
    ep_ids = np.array(sorted(per.index[(per["count"] == 4) & (per["nunique"] == 4)]))
    O = pd.read_parquet(os.path.join(DATA, "t5_episode_outcomes.parquet"),
                        columns=["episode_id", "path_status_10d"])     # AVAILABILITY only
    P = pd.DataFrame(dict(episode_id=ep_ids)).merge(
        pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date"]),
        on="episode_id", how="left").merge(O, on="episode_id", how="left")
    P = P[P.path_status_10d == "AVAILABLE"]
    P = P.merge(EST.anchors(P), on="episode_id", how="inner")
    P = P[P.liq_anchor.notna() & P.vol_anchor.notna() & (P.n20 == 20)]
    return EST.blocks(P), ep_ids


def recover_aliases(F):
    """Rebuild the alias NAMES the streaming sweep discarded, and prove the rebuild.

    The sweep kept only (representative, count) per membership class, so which other syntactic
    claims collapsed into a class was lost. It is recoverable: the class is defined by the
    full-population membership vector, and that vector is a function of the stored opening-hour
    tokens alone.

    The enumerator is NOT re-run and NOT edited — the subset-equivalence gate has already
    passed against it, and editing it between the FULL and TWO_BAR runs would retract the one
    thing that gate established. This reconstructs from the artifact instead.

    THE RECONSTRUCTION PROVES ITSELF. Every recovered class must have exactly the n_names the
    sweep recorded. A divergent reimplementation would produce different groupings and the
    check would fail rather than quietly substituting plausible aliases.
    """
    M = pd.read_parquet(MS, columns=["episode_id", "pos", "token_set", "ticker", "t5_date"])
    per = M.groupby("episode_id").pos.agg(["count", "nunique"])
    complete = set(per.index[(per["count"] == 4) & (per["nunique"] == 4)])
    M = M[M.episode_id.isin(complete)]
    ep_ids = np.array(sorted(complete)); eix = {e: i for i, e in enumerate(ep_ids)}
    nE = len(ep_ids)

    # searchable(): registry ORDER, and all three of the enumerator's exclusions. Sorting the
    # tokens or dropping the umbrella rule would renumber the columns, change which candidate
    # np.argwhere visits first, and hand back a different representative per class.
    UMBRELLA = {"ANY_T", "ANY_Z", "ANY_L", "L5_ANY", "AD_ANY", "ANY_P", "ANY_D"}
    cnt = pd.Series([t for row in M.token_set.str.split() for t in row]).value_counts()
    keep = [t.token_id for t in D.TOKENS_1H
            if t.token_id not in UMBRELLA and cnt.get(t.token_id, 0) > 0
            and cnt.get(t.token_id, 0) / len(M) <= 0.99]
    tix = {t: i for i, t in enumerate(keep)}
    B = np.zeros((4, nE, len(keep)), dtype=bool)
    for e, p, ts in zip(M.episode_id.to_numpy(), M.pos.to_numpy(), M.token_set.to_numpy()):
        r = eix[e]
        for t in ts.split():
            jx = tix.get(t)
            if jx is not None:
                B[p - 1, r, jx] = True

    first = M.drop_duplicates("episode_id").set_index("episode_id").loc[ep_ids]
    ep_tk = pd.factorize(first.ticker)[0]
    ep_dt = pd.factorize(first.t5_date)[0]
    n_tk, n_dt = int(ep_tk.max()) + 1, int(ep_dt.max()) + 1

    def passes(v):
        nt = int(v.sum())
        if nt < 300 or nE - nt < 300:
            return False
        tc = np.bincount(ep_tk[v], minlength=n_tk)
        dc = np.bincount(ep_dt[v], minlength=n_dt)
        return bool((tc > 0).sum() >= 50 and (dc > 0).sum() >= 50
                    and dc.max() / nt <= 0.10)

    # group by FULL-population hash, in the sweep's own visiting order, so element 0 of each
    # group is the representative the sweep chose
    byhash = {}
    for a, b in (("M1", "M2"), ("M2", "M3"), ("M3", "M4")):
        i, jx = int(a[1]) - 1, int(b[1]) - 1
        C = B[i].astype(np.int32).T @ B[jx].astype(np.int32)
        for x, yy in np.argwhere((C >= 300) & (nE - C >= 300)):
            v = B[i][:, x] & B[jx][:, yy]
            if not passes(v):
                continue
            h = hashlib.sha256(np.packbits(v).tobytes()).hexdigest()[:16]
            byhash.setdefault(h, []).append(f"{a}→{b}|2|{keep[x]}→{keep[yy]}")
    # key by the representative NAME, which is what the artifact carries
    return {g[0]: g[1:] for g in byhash.values()}


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)                     # the writer works before the long part
    if not all(ENUM2["gates"].values()) or not SUB["passed"]:
        raise RuntimeError("2-bar enumeration gates or the subset-equivalence gate did not "
                           "pass; the claim order may not be sealed on top of them")

    C = pd.read_parquet(CL)
    F = C[C.is_final_representative].copy()
    if len(F) != ENUM2["k_15m_final"]:
        raise RuntimeError(f"final representatives {len(F)} != k_15m_final "
                           f"{ENUM2['k_15m_final']}")
    # deterministic, enumeration-order-independent
    F = F.sort_values("estimand_membership_hash", kind="stable").reset_index(drop=True)
    F.insert(0, "j", np.arange(len(F), dtype=np.int64))

    rec = recover_aliases(F)
    miss = [r for r in F.representative if r not in rec]
    F["aliases"] = [" ".join(rec.get(r, [])) for r in F.representative]
    bad = int((F.aliases.str.split().apply(len) + 1 != F.n_names).sum())
    print(f"  alias recovery · classes matched {len(F)-len(miss):,}/{len(F):,} · "
          f"n_names mismatches {bad}", flush=True)
    if miss or bad:
        raise RuntimeError(
            f"alias reconstruction does not reproduce the sweep: {len(miss)} representatives "
            f"absent, {bad} classes with the wrong member count. The recovery diverges from "
            f"the enumerator and must NOT be sealed as if it were the sweep's own output.")

    P, _ = population()
    est_ids = P.episode_id.to_numpy()
    blk, blk_lab = pd.factorize(P.block)

    claim_order_hash = hashlib.sha256(
        "|".join(F.estimand_membership_hash).encode()).hexdigest()[:16]
    population_hash = hashlib.sha256("|".join(est_ids).encode()).hexdigest()[:16]
    block_assignment_hash = hashlib.sha256(
        np.ascontiguousarray(blk, np.int64).tobytes()).hexdigest()[:16]

    F[["j", "estimand_membership_hash", "representative", "aliases", "n_names",
       "position_family", "length", "treated_overlap", "control_overlap",
       "n_overlap_blocks", "overlap_retention"]].to_parquet(OUT_ORD, index=False)
    pd.DataFrame(dict(episode_id=est_ids, block=blk)).to_parquet(OUT_POP, index=False)

    nnz = int(F.treated_overlap.sum())
    n_seg = int(F.n_overlap_blocks.sum())
    payload = dict(
        spec_id="T5_15M_CLAIM_ORDER_V2", status="SEALED — X-ONLY, NO MFE VALUE READ",
        scope_spec=SCOPE["spec_id"], scope_digest=SCOPE["scope_digest"],
        enumeration="T5_15M_X_ONLY_ENUMERATION_2BAR",
        enumeration_class_digest=ENUM2["class_digest"],
        subset_equivalence_passed=bool(SUB["passed"]),
        order_rule="ascending estimand_membership_hash, stable — independent of the order the "
                   "sweep visited token pairs",
        k=int(len(F)), population=int(len(est_ids)), blocks=int(blk.max() + 1),
        claim_order_hash=claim_order_hash, population_hash=population_hash,
        block_assignment_hash=block_assignment_hash,
        nnz_treated_memberships=nnz, n_segments=n_seg,
        nnz_per_claim=round(nnz / len(F), 1), nnz_per_segment=round(nnz / n_seg, 2),
        segments_per_claim=round(n_seg / len(F), 1),
        support=dict(min=int(F.treated_overlap.min()),
                     q25=int(F.treated_overlap.quantile(.25)),
                     median=int(F.treated_overlap.median()),
                     q75=int(F.treated_overlap.quantile(.75)),
                     max=int(F.treated_overlap.max())),
        y_status="NO MFE VALUE READ; path_status_10d consulted for availability only",
        minutes=round((time.time() - t0) / 60, 1))
    dig = ART.seal(payload, OUT, required=("spec_id", "k", "claim_order_hash",
                                           "population_hash", "block_assignment_hash"))

    print(f"T5_15M_CLAIM_ORDER_V2  sealed · artifact digest {dig}")
    print(f"  k {len(F):,} · population {len(est_ids):,} · blocks {blk.max()+1:,}")
    print(f"  claim_order_hash      {claim_order_hash}")
    print(f"  population_hash       {population_hash}")
    print(f"  block_assignment_hash {block_assignment_hash}")
    print(f"\n  nnz  {nnz:,}          n_seg {n_seg:,}")
    print(f"  nnz/claim {nnz/len(F):,.0f} · nnz/seg {nnz/n_seg:.2f} · "
          f"seg/claim {n_seg/len(F):,.0f}")
    mem = dict(episode_idx_int32=nnz * 4, seg_ptr_int64=(n_seg + 1) * 8,
               seg_nc_int32=n_seg * 4, claim_seg_ptr_int64=(len(F) + 1) * 8,
               N_T_int32=len(F) * 4, constant_f64=len(F) * 8, analytic_sd_f64=len(F) * 8)
    print(f"\n  immutable state")
    for k2, v in mem.items():
        print(f"    {k2:<22} {v/1e6:>9.1f} MB")
    print(f"    {'TOTAL':<22} {sum(mem.values())/1e6:>9.1f} MB")
    print(f"    (a per-nnz float64 coef would add {nnz*8/1e6:,.0f} MB on top)")
    print(f"\n  WROTE {OUT}\n        {OUT_ORD}\n        {OUT_POP}")


if __name__ == "__main__":
    main()
