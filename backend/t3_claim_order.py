"""T3_1H_CLAIM_ORDER_V1 — the sealed order, its identities, and the three frozen needles.

    k = 853 membership-distinct claims on the FINAL estimand population
    j = deterministic: classes sorted by (family, length, representative) lexicographic
    representative = lexicographically smallest canonical sequence in the class (X-only)

IDENTITIES VERIFIED, NOT ASSUMED
    unique j == 853 · unique membership_hash == 853 · two builds -> same claim_order_hash

NEEDLES (per the approval): sort support ASCENDING, tie-break sealed j, nearest-rank
    q10 -> index 85 · q50 -> index 426 · q90 -> index 767   [zero-based, ceil(q*853)-1]

NO OUTCOME VALUE READ. The provenance note carried forward: the original sufficiency
predicate omitted the bullCode==0 failure state; the predicate was corrected during
pre-capability review before historical outcome exposure.
"""
from __future__ import annotations
import hashlib, json, math, os, sys, time                              # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t3_dna as D9                                # noqa: E402
import t3_sequence_grammar as G9, t3_sequence_estimand as E9           # noqa: E402
import t5_sequence_estimand as E5                                      # noqa: E402

OUT = "T3_1H_CLAIM_ORDER_V1.json"
ORDER_PARQ = os.path.join(D9.ROOT, "data", "t3_claim_order_v1.parquet")
POP_PARQ = os.path.join(D9.ROOT, "data", "t3_estimand_population.parquet")


def build_final_state():
    """Estimand population + blocks + final membership matrix over the 853 classes."""
    E = pd.read_parquet(D9.OUT_EP, columns=["episode_id", "ticker", "t3_date"])
    E = E[E.episode_id.isin(E9.analyzable_episodes())]
    OCC = pd.read_parquet(E9.OC, columns=["episode_id", "path_status_10d"])
    E = E[E.episode_id.isin(set(OCC.loc[OCC.path_status_10d == "AVAILABLE", "episode_id"]))]
    P = E.rename(columns={"t3_date": "t5_date"})
    P = P.merge(E5.anchors(P), on="episode_id", how="inner")
    P = P[P.n20 >= 20]
    P = E5.blocks(P)

    C = pd.read_parquet(G9.SURV)
    toks = pd.read_parquet(G9.TOKS).token.tolist()
    MEM = E9.membership(toks, C)
    MEM = MEM.merge(P[["episode_id", "block"]], on="episode_id", how="inner")
    R = pd.read_parquet(os.path.join(D9.ROOT, "data",
                                     "t3_sequence_estimand_claims.parquet"))
    return P, MEM, C, R


def final_classes(MEM, R):
    bcode, _ = pd.factorize(MEM.block)
    nb = bcode.max() + 1
    rows = []
    ok = set(R.loc[R.support_ok, "claim"])
    for cl in [c for c in MEM.columns if "|" in c]:
        if cl not in ok:
            continue
        s = MEM[cl].to_numpy()
        nt = np.bincount(bcode, weights=s, minlength=nb)
        nc = np.bincount(bcode, minlength=nb) - nt
        inov = ((nt > 0) & (nc > 0))[bcode]
        m = s & inov
        sig = hashlib.sha256(m.tobytes()).hexdigest()[:16]
        fam, L, name = cl.split("|", 2)
        rows.append(dict(claim=cl, family=fam, length=int(L), name=name,
                         membership_hash=sig, support=int(m.sum())))
    F = pd.DataFrame(rows)
    cls = (F.sort_values("name").groupby("membership_hash", sort=False)
           .agg(representative=("name", "first"), family=("family", "first"),
                length=("length", "first"), support=("support", "first"),
                n_names=("name", "size"),
                aliases=("name", lambda x: ";".join(sorted(x)[1:]))).reset_index())
    cls = cls.sort_values(["family", "length", "representative"]).reset_index(drop=True)
    cls.insert(0, "j", np.arange(len(cls)))
    cls["claim_id"] = [hashlib.sha256(f"{r.family}|{r.length}|{r.representative}"
                                      .encode()).hexdigest()[:16] for r in cls.itertuples()]
    return cls


def order_hash(cls):
    key = (cls.j.astype(str) + "|" + cls.claim_id + "|" + cls.membership_hash + "|"
           + cls.support.astype(str))
    return hashlib.sha256("\n".join(key).encode()).hexdigest()[:16]


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    P, MEM, C, R = build_final_state()
    cls = final_classes(MEM, R)
    cls2 = final_classes(MEM, R)                          # determinism: build twice
    h1, h2 = order_hash(cls), order_hash(cls2)

    k = len(cls)
    assert k == 853, k
    assert cls.j.nunique() == 853 and cls.membership_hash.nunique() == 853
    assert h1 == h2, "claim order not deterministic"

    pop_hash = hashlib.sha256("|".join(sorted(P.episode_id)).encode()).hexdigest()[:16]
    blk_hash = hashlib.sha256("|".join(sorted(P.episode_id + ":" + P.block))
                              .encode()).hexdigest()[:16]

    # ── needles: sort support ASCENDING, tie-break sealed j, nearest-rank ──
    srt = cls.sort_values(["support", "j"]).reset_index(drop=True)
    needles = {}
    for q in (0.10, 0.50, 0.90):
        idx = math.ceil(q * k) - 1
        r = srt.iloc[idx]
        needles[f"q{int(q*100)}"] = dict(
            index_zero_based=idx, j=int(r.j), claim_id=r.claim_id,
            membership_hash=r.membership_hash, representative=r.representative,
            family=r.family, length=int(r.length), support=int(r.support))
        print(f"  needle q{int(q*100)}: idx {idx} · j {int(r.j)} · {r.family}|{r.length}|"
              f"{r.representative} · support {int(r.support)}")

    cls.to_parquet(ORDER_PARQ, index=False)
    P[["episode_id", "ticker", "t5_date", "block"]].rename(
        columns={"t5_date": "t3_date"}).to_parquet(POP_PARQ, index=False)

    ART.seal(dict(
        spec_id="T3_1H_CLAIM_ORDER_V1", status="SEALED",
        family_id="T3_MICROSTRUCTURE_DNA_V1",
        k_final=k, unique_j=int(cls.j.nunique()),
        unique_membership_hash=int(cls.membership_hash.nunique()),
        order_rule="classes sorted by (family, length, representative) lexicographic; "
                   "representative = lexicographically smallest canonical sequence in the "
                   "class — X-only, deterministic",
        claim_order_hash=h1, deterministic="two builds identical",
        population_hash=pop_hash, n_population=int(len(P)),
        block_assignment_hash=blk_hash, n_blocks=int(P.block.nunique()),
        needles=needles,
        needle_rule="sort support ASCENDING, tie-break sealed j, nearest-rank "
                    "ceil(q*k)-1 zero-based (q10->85, q50->426, q90->767)",
        artifacts=dict(order=os.path.basename(ORDER_PARQ),
                       order_digest=ART.file_digest(ORDER_PARQ),
                       population=os.path.basename(POP_PARQ),
                       population_digest=ART.file_digest(POP_PARQ)),
        provenance_note="The original sufficiency predicate omitted the bullCode==0 failure "
                        "state; the predicate was corrected during pre-capability review "
                        "before historical outcome exposure.",
        outcome_exposure="NOT_EXPOSED — no outcome value read"),
        OUT, required=("spec_id", "k_final", "claim_order_hash", "population_hash",
                       "block_assignment_hash", "needles"))
    print(f"\nT3_1H_CLAIM_ORDER_V1 · {ART.file_digest(OUT)} · k={k} · order {h1} · "
          f"pop {pop_hash} · blocks {blk_hash} · {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
