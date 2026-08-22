"""Frozen 1H membership caches for T9 / T3 / T1, with the same identity guard as T5's.

The memberships are the OVERLAP-RESTRICTED ones — exactly what the historical inference
used, not a fresh evaluation of the token engine. A second semantics here would be free to
drift from the frozen one, so the sets are rebuilt through the family's own estimand path
and then CHECKED against the sealed support counts. Any mismatch is a hard failure, not a
warning.

Layout matches t5_1h_survivor_membership.parquet so the PT builder needs no per-family
branch: rows of kind='population' carry the frozen population, kind='member' the per-claim
membership keyed by the sealed j and claim_id.

    usage:  python pt_membership_cache.py t9 t3 t1

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, importlib, json, os, sys, time                       # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import t5_sequence_estimand as E5                                    # noqa: E402
import pt_family as PF                                               # noqa: E402

FORBIDDEN = {"mfe_10d", "mae_10d", "ret_10d", "mfe_atr_10d", "theta", "z_rank", "max_z"}


class CacheGateFailure(RuntimeError):
    pass


def build(fam: str):
    F = fam.upper()
    D = PF.dna(F)
    G = importlib.import_module(f"{fam}_sequence_grammar")
    E = importlib.import_module(f"{fam}_sequence_estimand")
    dcol = PF.date_col(F)

    # population, exactly as the estimand builds it
    Ep = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", dcol])
    Ep = Ep[Ep.episode_id.isin(E.analyzable_episodes())]
    OCC = pd.read_parquet(E.OC, columns=["episode_id", "path_status_10d"])
    Ep = Ep[Ep.episode_id.isin(set(OCC.loc[OCC.path_status_10d == "AVAILABLE",
                                           "episode_id"]))]
    P = Ep.rename(columns={dcol: "t5_date"})
    P = P.merge(E5.anchors(P), on="episode_id", how="inner")
    P = P[P.n20 >= 20]
    P = E5.blocks(P)

    C = pd.read_parquet(G.SURV)
    toks = pd.read_parquet(G.TOKS).token.tolist()
    MEM = E.membership(toks, C).merge(P[["episode_id", "block"]], on="episode_id",
                                      how="inner")
    bad = set(MEM.columns) & FORBIDDEN
    if bad:
        raise CacheGateFailure(f"outcome column reached the membership cache: {bad}")
    bcode, _ = pd.factorize(MEM.block)
    nb = bcode.max() + 1
    ids = MEM.episode_id.to_numpy()

    meds = PF.medoids(F)
    sealed = PF.sealed_supports(F)
    V = pd.read_parquet(os.path.join(D.ROOT, "data",
                                     f"{fam}_historical_vectors.parquet"))
    surv = V[V.survives]
    rows = [dict(kind="population", j=-1, claim_id="", episode_id=e) for e in ids]
    detail = {}
    for jkey, claim in meds.items():
        if claim not in MEM.columns:
            raise CacheGateFailure(f"{F}: medoid {claim} absent from the membership frame")
        s = MEM[claim].to_numpy()
        nt = np.bincount(bcode, weights=s, minlength=nb)
        nc = np.bincount(bcode, minlength=nb) - nt
        inov = ((nt > 0) & (nc > 0))[bcode]
        m = s & inov                       # OVERLAP-RESTRICTED, as the inference used
        n = int(m.sum())
        if n != sealed[claim]:
            raise CacheGateFailure(
                f"{F}: {claim} membership does not reproduce the sealed support — "
                f"{n} vs {sealed[claim]}")
        j = int(jkey[1:]) if jkey.startswith("j") else -1
        rows += [dict(kind="member", j=j, claim_id=claim, episode_id=e)
                 for e in ids[m]]
        detail[claim] = dict(j=j, members=n,
                             membership_hash=hashlib.sha256(
                                 "|".join(sorted(ids[m])).encode()).hexdigest()[:16])
    T = pd.DataFrame(rows)
    out = os.path.join(D.ROOT, "data", f"{fam}_1h_survivor_membership.parquet")
    T.to_parquet(out, index=False)

    CO = json.load(open(f"{F}_1H_CLAIM_ORDER_V1.json"))
    ident = dict(
        spec_id=f"PT{F[1:]}_1H_MEMBERSHIP_CACHE_V1", status="FROZEN",
        source_role="FROZEN_1H_RESEARCH_STATE",
        family=F,
        source_population_hash=hashlib.sha256(
            "|".join(sorted(ids)).encode()).hexdigest()[:16],
        claim_order_hash=CO["claim_order_hash"],
        block_assignment_hash=CO["block_assignment_hash"],
        definition_hash=PF.definition_hash(F),
        historical_exposure=ART.file_digest(f"{F}_HISTORICAL_EXPOSED_V1.json"),
        characterization=ART.file_digest(f"{F}_SURVIVOR_CHARACTERIZATION_V1.json"),
        builder_code_digest=ART.file_digest("pt_membership_cache.py"),
        n_population=int(len(ids)),
        n_survivors_sealed=int(len(surv)),
        n_medoids_used=len(meds),
        medoid_detail=detail,
        why_medoids_only="within-cluster aliases must not make a preview stronger; only the "
                         "frozen overlap-cluster medoids enter",
        cache_file=os.path.basename(out), cache_file_digest=ART.file_digest(out),
        membership_definition="OVERLAP-RESTRICTED memberships on the estimand population — "
                              "exactly what the frozen historical evidence used",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(ident, f"PT{F[1:]}_1H_MEMBERSHIP_CACHE_V1.json",
                 required=("spec_id", "source_population_hash", "medoid_detail"),
                 supersede=os.path.exists(f"PT{F[1:]}_1H_MEMBERSHIP_CACHE_V1.json"))
    print(f"{F}: population {len(ids):,} · medoids {len(meds)} · cache {d}")
    for c, v in detail.items():
        print(f"    {c:<34} j{v['j']:<4} members {v['members']:>6,} · {v['membership_hash']}")
    return d


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9", "t3", "t1"]):
        build(f)
