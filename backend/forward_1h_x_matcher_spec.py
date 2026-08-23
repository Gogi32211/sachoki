"""FORWARD_1H_X_MATCHER_SPEC_V1 — the architecture, frozen BEFORE anything is built.

Nothing is implemented here. This exists so the matcher cannot be designed later, under
pressure, in a way that lets a HOLD-interval preview quietly become forward evidence.

THE CORRECTION THIS SPEC EXISTS TO FIX. An earlier description of the matcher said a HOLD
result would carry `evidence_state = FORWARD_SOURCE_HOLD` and be recorded alongside
qualified marks. That blurs the one line that matters: source qualification must be
satisfied BEFORE the signal and its X are constructed, not annotated afterwards. A mark made
from an unqualified source is not weak evidence — it is not evidence, and it can never be
promoted or backfilled into the canonical record once the source later qualifies.

    new prospective episode
        -> SOURCE QUALIFICATION CHECK        <-- happens FIRST, gates everything after it

    IF SOURCE QUALIFIED
        immutable source snapshot
        -> X construction
        -> frozen medoid predicate
        -> match TRUE / FALSE
        -> outcome matures later
        -> CANONICAL FORWARD EVIDENCE

    IF SOURCE ON HOLD
        optional engineering preview only
        -> clearly marked NON-EVIDENTIARY
        -> CANNOT later be promoted or backfilled

WHY THE MATCHER IS NEEDED AT ALL. 1H medoid membership is X. It should not have to wait for
a 10-day outcome path to be known. The historical membership caches are built over episodes
whose path_status is AVAILABLE, which is right for reconstructing the past and wrong as a
forward detector — it would make membership first determinable ten days late, together with
the outcome it must stay independent of.

CURRENT BLOCKER, stated so it is not rediscovered. The 1H microstructure ends at 2026-08-20
and the first post-boundary episode is 2026-08-21, so building X for a new episode requires
reading the CURRENT mutable 1H store — which is the very source under qualification HOLD.
Until that source qualifies, this matcher can produce engineering previews only.

No Y value is read. No code path is created.
"""
from __future__ import annotations
import json, os, sys, time                                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402


def main():
    fams = [f for f in ("T5", "T9", "T3", "T1")
            if os.path.exists(f"{f}_FORWARD_1H_V1.json")]
    src_state = {}
    for f in fams:
        d = json.load(open(f"{f}_FORWARD_1H_V1.json"))
        src_state[f] = dict(seal_date=d["no_backfill"]["seal_date"],
                            verdict=d["source_gate_state"]["verdict"])
    d = ART.seal(dict(
        spec_id="FORWARD_1H_X_MATCHER_SPEC_V1", status="FROZEN_BEFORE_IMPLEMENTATION",
        role="architecture only — no implementation exists and none is authorised by this "
             "artifact",
        why_needed=dict(
            statement="1H medoid membership is X and must be determinable the moment a new "
                      "episode appears",
            defect_it_replaces="the historical membership caches are built over episodes "
                               "whose path_status is AVAILABLE. That is correct for "
                               "reconstructing the past and wrong as a forward detector: it "
                               "would make membership first determinable ten days late, "
                               "together with the very outcome it must stay independent of",
            current_operational_value="UNEVALUATED — reported as three-valued, never as "
                                      "FALSE, because nothing has been looked at"),
        pipeline=[
            "new prospective episode",
            "SOURCE QUALIFICATION CHECK — first, and it gates everything after it",
            "IF QUALIFIED: immutable source snapshot -> X construction -> frozen medoid "
            "predicate -> match TRUE/FALSE -> outcome matures later -> CANONICAL FORWARD "
            "EVIDENCE",
            "IF HOLD: optional engineering preview ONLY, marked NON-EVIDENTIARY, and it can "
            "never be promoted or backfilled"],
        the_binding_rule=dict(
            statement="source qualification must be satisfied BEFORE signal and X are "
                      "constructed — never annotated afterwards",
            corrects="an earlier description in which a HOLD result carried "
                     "evidence_state = FORWARD_SOURCE_HOLD and sat alongside qualified "
                     "marks. A mark made from an unqualified source is not weak evidence; "
                     "it is not evidence.",
            no_backfill="a HOLD-interval mark does NOT become canonical forward evidence "
                        "when the source later qualifies. The interval stays permanently "
                        "outside the evidentiary record.",
            why="allowing retroactive promotion would make the qualification gate optional "
                "in practice — anything produced during a HOLD could be admitted later, "
                "which is the same as having no gate"),
        hold_preview=dict(
            permitted=True,
            constraints=["clearly marked NON-EVIDENTIARY at every surface that shows it",
                         "never counted in any accrual ledger",
                         "never eligible for promotion or backfill",
                         "never usable as a filter that selects trades"],
            precedent="the PT operational layer already follows this: its marks are amber, "
                      "labelled OPERATIONAL PREVIEW / forward source HOLD, and excluded "
                      "from the screener chips because a chip is a filter"),
        current_blocker=dict(
            microstructure_ends="2026-08-20",
            first_post_boundary_episode="2026-08-21",
            consequence="building X for a new episode requires reading the CURRENT mutable "
                        "1H store, which is the source under qualification HOLD",
            therefore="until that source qualifies, this matcher can produce engineering "
                      "previews only"),
        source_gate_state=src_state,
        related=dict(
            operational_preview=ART.file_digest("PT_OPERATIONAL_PREVIEW_V1.json"),
            source_qualification=ART.file_digest(
                "FORWARD_1H_SOURCE_QUALIFICATION_V1.json")
            if os.path.exists("FORWARD_1H_SOURCE_QUALIFICATION_V1.json") else None),
        not_authorised="this artifact freezes an architecture; it does not authorise "
                       "building the matcher, materialising 1H microstructure for "
                       "post-boundary episodes, or touching the 1H source",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        "FORWARD_1H_X_MATCHER_SPEC_V1.json",
        required=("spec_id", "pipeline", "the_binding_rule", "hold_preview"),
        supersede=os.path.exists("FORWARD_1H_X_MATCHER_SPEC_V1.json"))
    print(f"FORWARD_1H_X_MATCHER_SPEC_V1 · {d} · FROZEN_BEFORE_IMPLEMENTATION")
    print(f"  source gate state: {src_state}")


if __name__ == "__main__":
    main()
