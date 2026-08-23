"""{F}_15M_CLUSTER_REPRESENTATIVES_V1 — the medoids, in the shape the PT layer already reads.

Nothing is decided here. The components, the medoids and the rule that picked them are all
sealed in {F}_15M_SURVIVOR_STRUCTURE_V1; this module only writes them out in the schema
pt_build already consumes for T5, so a second family's 15m phase needs no second semantics.

    representative   the component medoid — MAX MEAN JACCARD to the rest of its component,
                     tie-broken by the frozen claim order. Never by Z, never by magnitude.
    support          the sealed treated-overlap count, which the PT builder uses as its
                     reproduction gate: rebuilding the membership from the engine bundle
                     must land on exactly this number or the build refuses.

No outcome value is read: the survivor set was selected on Y and is already sealed, but
everything this module touches is membership, claim order and component structure.

    usage:  python t15m_cluster_representatives.py t9
"""
from __future__ import annotations
import importlib, json, os, sys, time                                  # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np, pandas as pd                                       # noqa: E402
import t5_artifact as ART                                              # noqa: E402


def build(fam: str):
    F = fam.upper()
    D = importlib.import_module(f"{fam}_dna")
    DATA = os.path.join(os.path.dirname(HERE), "data")
    ST = json.load(open(f"{F}_15M_SURVIVOR_STRUCTURE_V1.json"))
    S = pd.read_parquet(os.path.join(DATA, f"{fam}_15m_survivor_structure.parquet"))
    M = S[S.is_medoid].sort_values(["component"]).reset_index(drop=True)
    M.insert(0, "cluster", np.arange(len(M)))
    csize = S.component.value_counts()
    M["cluster_size"] = M.component.map(csize).astype(int)
    M["claim_id"] = M.position_family + "|" + M.representative.str.split("|").str[1]

    # the reproduction gate the PT builder will apply: rebuild each medoid's membership
    # from the sealed engine bundle and demand exactly the sealed support
    z = np.load(os.path.join(DATA, f"{fam}_15m_engine_state.npz"))
    eidx, seg_ptr, csp = z["eidx"], z["seg_ptr"], z["claim_seg_ptr"]
    P = pd.read_parquet(os.path.join(DATA, f"{fam}_15m_population.parquet"))
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    ids = P.episode_id.to_numpy()
    bad = []
    for r in M.itertuples():
        a, b = csp[r.j], csp[r.j + 1]
        n = len(set(ids[eidx[seg_ptr[a]:seg_ptr[b]]]))
        if n != int(r.support):
            bad.append((int(r.j), n, int(r.support)))
    if bad:
        raise RuntimeError(f"membership does not reproduce sealed support: {bad[:5]}")

    out = os.path.join(DATA, f"{fam}_15m_cluster_representatives.parquet")
    M[["cluster", "j", "claim_id", "representative", "position_family", "support",
       "component", "cluster_size"]].to_parquet(out, index=False)

    d = ART.seal(dict(
        spec_id=f"{F}_15M_CLUSTER_REPRESENTATIVES_V1", status="POST_EXPOSURE_DESCRIPTIVE",
        family=F,
        promotion="FROZEN / UNCHANGED — this artifact promotes nothing; the survivor set was "
                  "sealed by the historical exposure and is not revisited",
        rule="within each frozen J>=0.50 component, the member with the highest MEAN Jaccard "
             "to the other members (membership medoid); tie-break the frozen claim order",
        selection_is_x_only="the representative WITHIN a component is chosen on membership "
                            "geometry and claim order alone — never on Z, magnitude or "
                            "support. Survivor selection already used Y once.",
        source=dict(
            survivor_structure=ART.file_digest(f"{F}_15M_SURVIVOR_STRUCTURE_V1.json"),
            historical_exposed=ART.file_digest(f"{F}_15M_HISTORICAL_EXPOSED_V1.json"),
            engine_state=ART.file_digest(f"{F}_15M_ENGINE_STATE_V1.json"),
            claim_order=ART.file_digest(f"{F}_15M_CLAIM_ORDER_V1.json")),
        survivors=ST["n_survivors"], clusters=int(len(M)),
        representatives=int(len(M)),
        cluster_size=dict(largest=int(csize.max()), singletons=int((csize == 1).sum()),
                          median=int(csize.median())),
        reproduction_gate="the PT builder rebuilds each representative's membership from the "
                          "sealed engine bundle and requires exactly the support below; a "
                          "mismatch refuses the build rather than warning",
        support=dict(min=int(M.support.min()), median=int(M.support.median()),
                     max=int(M.support.max())),
        by_position_family=M.position_family.value_counts().to_dict(),
        artifact=dict(path=os.path.basename(out), digest=ART.file_digest(out)),
        outcome_exposure="NOT_EXPOSED — membership, claim order and component structure only",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        f"{F}_15M_CLUSTER_REPRESENTATIVES_V1.json",
        required=("spec_id", "rule", "clusters", "representatives"),
        supersede=os.path.exists(f"{F}_15M_CLUSTER_REPRESENTATIVES_V1.json"))
    print(f"{F}_15M_CLUSTER_REPRESENTATIVES_V1 · {d} · {len(M)} representatives · "
          f"membership reproduced for all")
    print(f"  by position family: {M.position_family.value_counts().to_dict()}")


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9"]):
        build(f)
