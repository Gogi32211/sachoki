"""{F}_15M_CLAIM_ORDER_V1 — the sealed order, its identities, and the three frozen needles.

    k               membership-distinct claims on the FINAL ESTIMAND POPULATION
    eligibility     whichever predicate the sealed ruling names; this module refuses to run
                    without T15M_ELIGIBILITY_RULING_V1.json, so the surface is never a
                    choice made inside the run
    j               deterministic: classes sorted by (position_family, representative)
    representative  lexicographically smallest claim_id in the class (X-only)

IDENTITIES VERIFIED, NOT ASSUMED
    unique j == k · unique membership_hash == k · two builds -> same claim_order_hash

NEEDLES  support ASCENDING, tie-break the sealed j, nearest rank:  i_q = ceil(q*k) - 1

No Y value is read anywhere in this module — its only input is the X-only k closure.

    usage:  python t15m_order.py [t9 t3 t1]
"""
from __future__ import annotations
import hashlib, importlib, json, math, os, sys, time                   # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

RULING = "T15M_ELIGIBILITY_RULING_V1.json"


def classes(C: pd.DataFrame) -> pd.DataFrame:
    cls = (C.sort_values("claim_id").groupby("membership_hash", sort=False)
           .agg(representative=("claim_id", "first"),
                position_family=("position_family", "first"),
                support=("support", "first"), n_names=("claim_id", "size"),
                aliases=("claim_id", lambda x: ";".join(sorted(x)[1:]))).reset_index())
    cls = cls.sort_values(["position_family", "representative"]).reset_index(drop=True)
    cls.insert(0, "j", np.arange(len(cls)))
    cls["claim_uid"] = [hashlib.sha256(f"{r.position_family}|{r.representative}".encode())
                        .hexdigest()[:16] for r in cls.itertuples()]
    return cls


def order_hash(cls: pd.DataFrame) -> str:
    key = (cls.j.astype(str) + "|" + cls.claim_uid + "|" + cls.membership_hash + "|"
           + cls.support.astype(str))
    return hashlib.sha256("\n".join(key).encode()).hexdigest()[:16]


def run(fam: str):
    t0 = time.time()
    F = fam.upper()
    D = importlib.import_module(f"{fam}_dna")
    if not os.path.exists(RULING):
        raise SystemExit(f"{RULING} is absent — the eligibility surface is a sealed ruling, "
                         "not a runtime choice. Nothing runs until it exists.")
    rule = json.load(open(RULING))
    assert rule["ruling"] == "ADOPT_FROZEN_PREDICATE_SURFACE", rule["ruling"]
    surface = rule["eligibility_surface"]
    assert surface == "FINAL_ESTIMAND_POPULATION", surface
    # the closure's S2 columns ARE the final estimand surface; s2_support IS t_ov, which
    # the ruling requires the order to sort on — never the broader S0 support
    okcol, supcol = "s2_ok", "s2_support"
    kclosure = f"{F}_15M_K_CLOSURE_V1.json"
    KC = json.load(open(kclosure))
    expect_k = rule["families"][F]["k_final_inferential"]
    expect_needles = rule["families"][F]["needles_zero_based"]

    C = pd.read_parquet(os.path.join(D.ROOT, "data", f"{fam}_15m_k_closure.parquet"))
    C = C[C[okcol]].rename(columns={"s2_hash": "membership_hash", supcol: "support"})
    cls, cls2 = classes(C), classes(C)
    k = len(cls)
    h1, h2 = order_hash(cls), order_hash(cls2)
    assert k == cls.j.nunique() == cls.membership_hash.nunique(), "k identity broken"
    assert h1 == h2, "claim order not deterministic"
    if k != expect_k:
        raise SystemExit(f"{F}: k = {k:,} but the ruling sealed {expect_k:,} — HOLD")

    srt = cls.sort_values(["support", "j"]).reset_index(drop=True)
    needles = {}
    for q in (0.10, 0.50, 0.90):
        idx = math.ceil(q * k) - 1
        assert idx == expect_needles[f"q{int(q*100)}"], (
            f"{F} q{int(q*100)}: computed {idx} != ruling {expect_needles}")
        r = srt.iloc[idx]
        needles[f"q{int(q*100)}"] = dict(
            index_zero_based=int(idx), j=int(r.j), claim_uid=r.claim_uid,
            membership_hash=r.membership_hash, representative=r.representative,
            position_family=r.position_family, support=int(r.support))
        print(f"  needle q{int(q*100)}: idx {idx:,} · j {int(r.j):,} · {r.representative} "
              f"· support {int(r.support):,}")

    parq = os.path.join(D.ROOT, "data", f"{fam}_15m_claim_order.parquet")
    cls.to_parquet(parq, index=False)
    d = ART.seal(dict(
        spec_id=f"{F}_15M_CLAIM_ORDER_V1", status="X_ONLY_PRE_Y", family=F,
        k=k, eligibility_surface=surface,
        eligibility_ruling=ART.file_digest(RULING),
        k_closure=ART.file_digest(kclosure),
        class_identity_surface=KC["CLASS_IDENTITY_SURFACE"],
        order=dict(rule="classes sorted by (position_family, representative); j is the "
                        "position in that order",
                   representative="lexicographically smallest claim_id in the class",
                   claim_order_hash=h1, deterministic_two_builds=bool(h1 == h2)),
        identities=dict(unique_j=int(cls.j.nunique()),
                        unique_membership_hash=int(cls.membership_hash.nunique()),
                        syntactic_names=int(len(C)), aliases=int(len(C) - k),
                        reconciles=bool(len(C) == k + (len(C) - k))),
        needles=dict(rule="support ASCENDING, tie-break the sealed j, nearest rank "
                          "i_q = ceil(q*k) - 1", **needles),
        by_position_family={pf: int((cls.position_family == pf).sum())
                            for pf in sorted(cls.position_family.unique())},
        support_quantiles={f"q{int(x*100)}": int(cls.support.quantile(x))
                           for x in (0.0, .1, .5, .9, 1.0)},
        table=dict(path=os.path.basename(parq), digest=ART.file_digest(parq)),
        y_status="NO OUTCOME VALUE READ",
        runtime_min=round((time.time() - t0) / 60, 1)),
        f"{F}_15M_CLAIM_ORDER_V1.json",
        required=("spec_id", "k", "order", "identities", "needles"),
        supersede=os.path.exists(f"{F}_15M_CLAIM_ORDER_V1.json"))
    print(f"  {F}_15M_CLAIM_ORDER_V1 · {d} · k {k:,} · order {h1} · "
          f"{(time.time()-t0)/60:.1f} min\n", flush=True)
    return d


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9", "t3", "t1"]):
        run(f)
