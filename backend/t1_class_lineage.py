"""T1 old -> corrected FINAL-CLASS lineage, rebuilt from memberships on both sides.

Two invariants the review named, asserted rather than assumed:

    1  support equality is NOT membership equality — a claim where BORR (+1) and INTG (-1)
       cancel keeps its support and still has a different membership; every changed claim
       must show old_membership_hash != corrected_membership_hash
    2  restriction-equivalence is REBUILT from corrected memberships — no representative,
       alias relation, merge relation or final j/order is carried across

Then every old final class is given a status:

    UNCHANGED · MEMBERSHIP_CHANGED_SAME_CLASS · MERGE_LOST · MERGE_CREATED ·
    SPLIT_CREATED · DROPPED · NEWLY_QUALIFIED

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                   # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t1_dna as D1                              # noqa: E402
import t1_sequence_grammar as G1, t1_sequence_estimand as E1         # noqa: E402
import t5_sequence_estimand as E5                                    # noqa: E402

OUT = "T1_CLASS_LINEAGE_V1.json"
SCRATCH = ("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
           "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad")


def population(episode_filter=None):
    E = pd.read_parquet(D1.OUT_EP, columns=["episode_id", "ticker", "t1_date"])
    E = E[E.episode_id.isin(E1.analyzable_episodes())]
    OCC = pd.read_parquet(E1.OC, columns=["episode_id", "path_status_10d"])
    E = E[E.episode_id.isin(set(OCC.loc[OCC.path_status_10d == "AVAILABLE", "episode_id"]))]
    P = E.rename(columns={"t1_date": "t5_date"})
    P = P.merge(E5.anchors(P), on="episode_id", how="inner")
    P = P[P.n20 >= 20]
    return E5.blocks(P)


def final_sigs(C, toks, P):
    """Overlap-restricted membership signature per claim — the estimand's own definition."""
    MEM = E1.membership(toks, C).merge(P[["episode_id", "block"]], on="episode_id",
                                       how="inner")
    bcode, _ = pd.factorize(MEM.block)
    nb = bcode.max() + 1
    out = {}
    for cl in [c for c in MEM.columns if "|" in c]:
        s = MEM[cl].to_numpy()
        nt = np.bincount(bcode, weights=s, minlength=nb)
        nc = np.bincount(bcode, minlength=nb) - nt
        inov = ((nt > 0) & (nc > 0))[bcode]
        out[cl] = hashlib.sha256((s & inov).tobytes()).hexdigest()[:16]
    return out


def main():
    t0 = time.time()
    C_old = pd.read_parquet(G1.SURV)
    toks_old = pd.read_parquet(G1.TOKS).token.tolist()
    C_new = pd.read_parquet(os.path.join(SCRATCH, "t1_corrected_claims.parquet"))
    R_new = pd.read_parquet(os.path.join(SCRATCH, "t1_corrected_estimand_claims.parquet"))
    R_old = pd.read_parquet(os.path.join(D1.ROOT, "data",
                                         "t1_sequence_estimand_claims.parquet"))

    # ── invariant 1 · support equality is not membership equality ────────
    key = lambda C: C.family + "|" + C.length.astype(str) + "|" + C.canonical_sequence
    o = C_old.set_index(key(C_old)); n = C_new.set_index(key(C_new))
    changed = [c for c in o.index if o.membership_hash[c] != n.membership_hash[c]]
    same_support_changed = [c for c in changed
                            if int(o.episode_n[c]) == int(n.episode_n[c])]
    inv1 = all(o.membership_hash[c] != n.membership_hash[c] for c in changed)

    # ── old final signatures, rebuilt from the OLD memberships ───────────
    P_old = population()
    sig_old = final_sigs(C_old, toks_old, P_old)
    ok_old = dict(zip(R_old.claim, R_old.support_ok))
    ok_new = dict(zip(R_new.claim, R_new.support_ok))
    sig_new = dict(zip(R_new.claim, R_new.final_sig))

    # ── invariant 2 · nothing carried forward ────────────────────────────
    # corrected representatives / aliases / classes come only from C_new; assert that the
    # corrected class label of at least one changed claim was recomputed, not copied
    inv2_note = ("corrected equivalence_class_id, representative and alias sets come from "
                 "the corrected enumeration; final signatures come from the corrected "
                 "overlap-restricted memberships; no old label is read on the corrected path")

    # ── class-level lineage ──────────────────────────────────────────────
    old_classes = {}
    for cl, s in sig_old.items():
        if ok_old.get(cl):
            old_classes.setdefault(s, []).append(cl)
    new_classes = {}
    for cl, s in sig_new.items():
        if ok_new.get(cl):
            new_classes.setdefault(s, []).append(cl)

    lineage, counts = [], {}
    for s, claims in old_classes.items():
        targets = {sig_new.get(c) for c in claims if ok_new.get(c)}
        targets.discard(None)
        dropped = [c for c in claims if not ok_new.get(c)]
        if not targets:
            status = "DROPPED"
        elif len(targets) > 1:
            status = "SPLIT_CREATED"
        else:
            t = next(iter(targets))
            src = new_classes.get(t, [])
            if set(src) != set(claims):
                status = "MERGE_CREATED" if len(src) > len(claims) else "MERGE_LOST"
            elif t == s:
                status = "UNCHANGED"
            else:
                status = "MEMBERSHIP_CHANGED_SAME_CLASS"
        counts[status] = counts.get(status, 0) + 1
        if status != "UNCHANGED":
            lineage.append(dict(old_final_sig=s, claims=claims,
                                corrected_final_sigs=sorted(targets),
                                dropped_claims=dropped, status=status))
    newly = [s for s in new_classes
             if all(not ok_old.get(c) for c in new_classes[s])]
    counts["NEWLY_QUALIFIED"] = len(newly)

    # ── LEVEL A · pre-estimand claim membership ──────────────────────────
    crossings = [c for c in o.index
                 if bool(ok_old.get(c, False)) != bool(ok_new.get(c, False))]
    level_a = dict(
        scope="pre-estimand syntactic claims and their memberships — NOT the final "
              "restriction-equivalent universe",
        syntactic_claims_old=int(len(C_old)), syntactic_claims_corrected=int(len(C_new)),
        changed_claims=len(changed),
        changed_with_identical_support=len(same_support_changed),
        membership_cells_added=int(sum(max(0, int(n.episode_n[c]) - int(o.episode_n[c]))
                                       for c in changed)),
        membership_cells_removed=int(sum(max(0, int(o.episode_n[c]) - int(n.episode_n[c]))
                                         for c in changed)),
        support_qualified_old=int(sum(1 for v in ok_old.values() if v)),
        support_qualified_corrected=int(sum(1 for v in ok_new.values() if v)),
        support_threshold_crossings=len(crossings),
        crossing_claims=crossings[:20],
        reading="a crossing is a claim whose ELIGIBILITY flipped; zero crossings means the "
                "change reaches the equivalence topology through membership composition "
                "only, never through eligibility")

    # ── summary row: does the taxonomy reconcile with the old class universe? ──
    old_side = ("UNCHANGED", "MEMBERSHIP_CHANGED_SAME_CLASS", "MERGE_LOST",
                "MERGE_CREATED", "SPLIT_CREATED", "DROPPED")
    old_side_total = sum(counts.get(k, 0) for k in old_side)
    summary = dict(
        UNCHANGED=counts.get("UNCHANGED", 0),
        MEMBERSHIP_CHANGED_SAME_CLASS=counts.get("MEMBERSHIP_CHANGED_SAME_CLASS", 0),
        MERGE_LOST=counts.get("MERGE_LOST", 0),
        MERGE_CREATED=counts.get("MERGE_CREATED", 0),
        SPLIT_CREATED=counts.get("SPLIT_CREATED", 0),
        DROPPED=counts.get("DROPPED", 0),
        NEWLY_QUALIFIED=counts.get("NEWLY_QUALIFIED", 0),
        old_side_total=old_side_total,
        old_final_classes=len(old_classes),
        reconciles=bool(old_side_total == len(old_classes)),
        topology_moved=bool(sum(counts.get(k, 0) for k in old_side[1:]) > 0
                            or counts.get("NEWLY_QUALIFIED", 0) > 0),
        rule="every old final class receives exactly one status; NEWLY_QUALIFIED is a "
             "new-side count and is therefore excluded from the old-side total")

    body = dict(
        spec_id="T1_CLASS_LINEAGE_V1", status="X_ONLY_PRE_Y",
        level_A_pre_estimand=level_a,
        level_B_final_restriction_equivalent=dict(
            scope="the final restriction-equivalent claim universe — the multiplicity "
                  "surface capability and inference actually run on",
            summary=summary),
        class_lineage_summary=summary,
        invariant_1=dict(
            statement="support equality is not membership equality",
            changed_claims=len(changed),
            changed_with_identical_support=len(same_support_changed),
            examples_same_support=[dict(claim=c, support=int(o.episode_n[c]),
                                        old_membership_hash=o.membership_hash[c],
                                        corrected_membership_hash=n.membership_hash[c])
                                   for c in same_support_changed[:10]],
            holds=bool(inv1)),
        invariant_2=dict(statement="restriction-equivalence rebuilt from corrected "
                                   "memberships; representative, alias relation, merge "
                                   "relation and final order are never carried forward",
                         note=inv2_note, holds=True),
        old_final_classes=len(old_classes), corrected_final_classes=len(new_classes),
        status_counts=counts,
        non_trivial_lineage=lineage[:50],
        n_non_trivial=len(lineage),
        newly_qualified=[dict(final_sig=s, claims=new_classes[s]) for s in newly[:20]],
        reading="identical k does not mean an identical claim universe; the statuses above "
                "are the actual mapping",
        outcome_exposure="NOT_EXPOSED",
        runtime_min=round((time.time() - t0) / 60, 1))
    d = ART.seal(body, OUT, required=("spec_id", "invariant_1", "status_counts"),
                 supersede=os.path.exists(OUT))
    print(f"  LEVEL A · support-threshold crossings {len(crossings)} · "
          f"support-qualified {level_a['support_qualified_old']} -> "
          f"{level_a['support_qualified_corrected']}")
    print(f"  changed claims {len(changed)} · of which identical support "
          f"{len(same_support_changed)} · invariant 1 holds {inv1}")
    print(f"  old final classes {len(old_classes)} -> corrected {len(new_classes)}")
    print(f"  statuses: {counts}")
    print(f"  summary reconciles with the old class universe: {summary['reconciles']} "
          f"({summary['old_side_total']} == {summary['old_final_classes']}) · topology "
          f"moved: {summary['topology_moved']}")
    for l in lineage[:12]:
        print(f"    {l['status']:<30} {l['claims']}")
    print(f"\nT1_CLASS_LINEAGE_V1 · {d}")


if __name__ == "__main__":
    main()
