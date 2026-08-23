"""{F}_15M_SURVIVOR_STRUCTURE — how many DISTINCT structures are in the survivor set?

A count of threshold-passing claims is not a count of discoveries. 533 survivors could be
533 mechanisms or one mechanism wearing 533 names, and nothing in the exposure artifact
distinguishes those. This module answers that question and nothing else.

    533 survivors  !=  533 independent discoveries
                   !=  533 mechanisms
                   !=  533 forward signals

OUTCOME-FREE BY CONSTRUCTION. The survivor SET came from Y — that is already done and
sealed. But everything here operates on membership sets and the frozen claim order alone:
no MFE, MAE, RET or theta is read, and in particular the representative of a component is
NOT the highest-Z member. Picking the winner inside a component by its statistic would be a
second selection on the same outcome, on top of the one already made.

    edges        pairwise Jaccard on overlap-restricted membership, edge if J >= 0.50
    components   connected components of that graph
    medoid       the member with the MAXIMUM MEAN JACCARD to the rest of its component;
                 ties broken by the frozen claim order, never by Z

DENOMINATORS ARE REPORTED. "M1->M2 dominates" is not established by 353 of 533 — it needs
the eligible population of each position family. The same for L3.

    usage:  python t15m_survivor_structure.py t9 --seal-spec
            python t15m_survivor_structure.py t9 --run
"""
from __future__ import annotations
import hashlib, importlib, json, os, sys, time                         # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np, pandas as pd                                       # noqa: E402
import t5_artifact as ART                                              # noqa: E402

J_EDGE = 0.50
J_NEAR_ALIAS = 0.90
POPCNT = np.array([bin(i).count("1") for i in range(256)], dtype=np.uint16)


def spec_path(F):
    return f"{F}_15M_SURVIVOR_STRUCTURE_SPEC_V1.json"


def seal_spec(fam):
    F = fam.upper()
    d = ART.seal(dict(
        spec_id=f"{F}_15M_SURVIVOR_STRUCTURE_SPEC_V1", status="FROZEN_BEFORE_COMPUTE",
        family=F,
        question="how many DISTINCT structures are in the survivor set?",
        governing=dict(
            historical_exposed=ART.file_digest(f"{F}_15M_HISTORICAL_EXPOSED_V1.json"),
            claim_order=ART.file_digest(f"{F}_15M_CLAIM_ORDER_V1.json"),
            engine_state=ART.file_digest(f"{F}_15M_ENGINE_STATE_V1.json")),
        overlap=dict(
            measure="pairwise Jaccard on the overlap-restricted treated membership",
            edge_threshold=J_EDGE,
            near_alias_threshold=J_NEAR_ALIAS,
            components="connected components of the J >= 0.50 graph",
            subset="A is a strict subset of B iff |A & B| == |A| and |A| < |B|"),
        medoid=dict(
            rule="the member with the MAXIMUM MEAN JACCARD to the other members of its "
                 "component",
            tie_break="frozen claim order (sealed j), ascending",
            forbidden="choosing the representative by Z, by magnitude, or by support — "
                      "survivor selection already used Y once; a second selection inside "
                      "the component would compound it",
            singletons="a one-member component is its own medoid"),
        outcome_free=dict(
            reads=["membership sets", "frozen claim order", "position family", "support"],
            does_not_read=["MFE_10D", "MFE_ATR", "MAE", "RET", "theta", "Z_obs"],
            why="the survivor set is already a selection on Y; structure must not add a "
                "second one"),
        denominators_required=[
            "survivors per position family DIVIDED BY eligible claims per position family",
            "P(survive | claim contains M1:L3) vs P(survive | it does not)"],
        interpretation_limits=[
            "a survivor count is not a count of independent discoveries",
            "position-family dominance is not established without its denominator",
            "a motif appearing at the top of a Z-ordered table may be one correlated "
            "cluster rather than a repeated structure"],
        descriptive_only="every incidence statistic here is POST-EXPOSURE DESCRIPTIVE "
                         "CHARACTERIZATION, never a new promotion test",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        spec_path(F), required=("spec_id", "overlap", "medoid", "outcome_free"),
        supersede=os.path.exists(spec_path(F)))
    print(f"{F}_15M_SURVIVOR_STRUCTURE_SPEC_V1 · {d} · FROZEN_BEFORE_COMPUTE")
    return d


def run(fam):
    t0 = time.time()
    F = fam.upper()
    if not os.path.exists(spec_path(F)):
        raise SystemExit("seal the spec first")
    D = importlib.import_module(f"{fam}_dna")
    DATA = os.path.join(os.path.dirname(HERE), "data")
    O = pd.read_parquet(os.path.join(DATA, f"{fam}_15m_historical.parquet")) \
          .sort_values("j").reset_index(drop=True)
    z = np.load(os.path.join(DATA, f"{fam}_15m_engine_state.npz"))
    eidx, seg_ptr, csp = z["eidx"], z["seg_ptr"], z["claim_seg_ptr"]
    nE = int(z["nE"][0])
    S = O[O.survivor].reset_index(drop=True)
    n = len(S)
    print(f"{F} survivor structure · {n} survivors of {len(O):,} · nE {nE:,}", flush=True)

    # ── packed membership bitsets ───────────────────────────────────────────
    nbytes = (nE + 7) // 8
    Pk = np.zeros((n, nbytes), dtype=np.uint8)
    sizes = np.empty(n, dtype=np.int64)
    for i, jj in enumerate(S.j.to_numpy()):
        idx = eidx[seg_ptr[csp[jj]]:seg_ptr[csp[jj + 1]]]
        v = np.zeros(nE, dtype=bool)
        v[idx] = True
        Pk[i] = np.packbits(v)
        sizes[i] = len(idx)

    # ── pairwise intersection, Jaccard ──────────────────────────────────────
    inter = np.zeros((n, n), dtype=np.int64)
    for i in range(n):
        inter[i] = POPCNT[np.bitwise_and(Pk[i], Pk)].sum(axis=1)
    union = sizes[:, None] + sizes[None, :] - inter
    with np.errstate(divide="ignore", invalid="ignore"):
        J = np.where(union > 0, inter / union, 0.0)
    np.fill_diagonal(J, 1.0)
    print(f"  pairwise Jaccard done · {(time.time()-t0)/60:.1f} min", flush=True)

    # ── components ──────────────────────────────────────────────────────────
    from scipy.sparse import csr_matrix
    from scipy.sparse.csgraph import connected_components
    A = (J >= J_EDGE)
    np.fill_diagonal(A, False)
    ncomp, lab = connected_components(csr_matrix(A), directed=False)
    S["component"] = lab

    # ── medoid: max mean Jaccard within component, tie-break frozen j ───────
    med_idx = []
    for c in range(ncomp):
        m = np.flatnonzero(lab == c)
        if len(m) == 1:
            med_idx.append(int(m[0])); continue
        sub = J[np.ix_(m, m)].copy()
        np.fill_diagonal(sub, np.nan)
        mean_j = np.nanmean(sub, axis=1)
        best = np.flatnonzero(mean_j == mean_j.max())
        med_idx.append(int(m[best[np.argmin(S.j.to_numpy()[m[best]])]]))
    S["is_medoid"] = False
    S.loc[med_idx, "is_medoid"] = True

    # ── relations ───────────────────────────────────────────────────────────
    iu = np.triu_indices(n, 1)
    edges = int((J[iu] >= J_EDGE).sum())
    near = int((J[iu] >= J_NEAR_ALIAS).sum())
    subset = int(((inter == sizes[:, None]) & (sizes[:, None] < sizes[None, :])).sum())
    csize = pd.Series(lab).value_counts()

    # ── denominators ────────────────────────────────────────────────────────
    elig = O.position_family.value_counts()
    surv_pf = S.position_family.value_counts()
    med_pf = S[S.is_medoid].position_family.value_counts()
    comp_pf = S.groupby("position_family").component.nunique()
    by_pf = {pf: dict(eligible=int(elig.get(pf, 0)), survivors=int(surv_pf.get(pf, 0)),
                      survival_rate=round(float(surv_pf.get(pf, 0)) / int(elig.get(pf, 1)), 5),
                      components=int(comp_pf.get(pf, 0)), medoids=int(med_pf.get(pf, 0)))
             for pf in sorted(elig.index)}

    # ── M1:L3 incidence, with its denominator ───────────────────────────────
    def has_m1_l3(rep):
        pf, toks = rep.split("|")
        return pf.startswith("M1") and toks.split("→")[0] == "L3"
    O["m1_l3"] = O.representative.map(has_m1_l3)
    S["m1_l3"] = S.representative.map(has_m1_l3)
    n_with = int(O.m1_l3.sum()); n_without = int((~O.m1_l3).sum())
    s_with = int(S.m1_l3.sum()); s_without = int((~S.m1_l3).sum())
    l3 = dict(
        eligible_claims_with_M1_L3=n_with, eligible_claims_without=n_without,
        survivors_with_M1_L3=s_with, survivors_without=s_without,
        p_survive_given_M1_L3=round(s_with / n_with, 5) if n_with else None,
        p_survive_given_not=round(s_without / n_without, 5) if n_without else None,
        lift=round((s_with / n_with) / (s_without / n_without), 2)
        if n_with and n_without and s_without else None,
        distinct_components_containing_M1_L3=int(S[S.m1_l3].component.nunique()),
        distinct_medoids_with_M1_L3=int(S[S.m1_l3 & S.is_medoid].shape[0]),
        reading="whether M1:L3 is a motif REPEATED across independent structures, or one "
                "large correlated cluster, is decided by the component count — not by how "
                "many rows carry it")

    out = os.path.join(DATA, f"{fam}_15m_survivor_structure.parquet")
    S.to_parquet(out, index=False)
    meds = S[S.is_medoid].sort_values("j")
    d = ART.seal(dict(
        spec_id=f"{F}_15M_SURVIVOR_STRUCTURE_V1", status="STRUCTURE_CHARACTERIZED",
        family=F, spec=ART.file_digest(spec_path(F)),
        historical_exposed=ART.file_digest(f"{F}_15M_HISTORICAL_EXPOSED_V1.json"),
        n_survivors=n, n_overlap_components=int(ncomp), n_medoids=int(len(meds)),
        component_size_distribution=dict(
            singletons=int((csize == 1).sum()), largest=int(csize.max()),
            median=int(csize.median()), mean=round(float(csize.mean()), 2),
            top10=[int(x) for x in csize.head(10)]),
        relations=dict(edges_J_ge_050=edges, near_alias_J_ge_090=near,
                       strict_subset_pairs=subset,
                       total_pairs=int(n * (n - 1) // 2)),
        by_position_family=by_pf,
        M1_L3=l3,
        medoids=[dict(j=int(r.j), representative=r.representative,
                      position_family=r.position_family, support=int(r.support),
                      component=int(r.component),
                      component_size=int(csize[r.component]))
                 for r in meds.itertuples()][:80],
        interpretation=dict(
            survivors_are_not_discoveries=f"{n} threshold-passing claims resolve to "
                                          f"{ncomp} overlap components",
            medoid_rule="max mean Jaccard within component, tie-break frozen j — never Z",
            descriptive_only="every incidence figure is post-exposure descriptive "
                             "characterization, not a new promotion test"),
        theta="NOT COMPUTED", magnitude="NOT COMPUTED",
        table=dict(path=os.path.basename(out), digest=ART.file_digest(out)),
        runtime_min=round((time.time() - t0) / 60, 1)),
        f"{F}_15M_SURVIVOR_STRUCTURE_V1.json",
        required=("spec_id", "n_survivors", "n_overlap_components", "by_position_family"),
        supersede=os.path.exists(f"{F}_15M_SURVIVOR_STRUCTURE_V1.json"))
    print(f"  components {ncomp} · medoids {len(meds)} · singletons "
          f"{int((csize==1).sum())} · largest {int(csize.max())}")
    print(f"  edges(J>=.50) {edges:,} · near-alias(J>=.90) {near:,} · strict subsets {subset:,}")
    print(f"{F}_15M_SURVIVOR_STRUCTURE_V1 · {d} · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    fam = next((a for a in sys.argv[1:] if not a.startswith("-")), "t9")
    if "--seal-spec" in sys.argv:
        seal_spec(fam)
    elif "--run" in sys.argv:
        run(fam)
    else:
        raise SystemExit("use --seal-spec first, then --run")
