"""MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_3 — availability-reason precedence, frozen.

The independent verifier found that the two qualified port implementations disagreed on one
case: the first bar of a security's history when that bar is ALSO not COMPLETE. Both blocking
conditions hold at once, and nothing had ever registered which reason wins. The vectorised path
said CURRENT_BAR_NOT_COMPLETE because its current-bar assignment ran last; the scalar path said
UNAVAILABLE_REQUIRED_HISTORY because it tested i == 0 first. 189 rows of 15,445,454, and no T
label was ever affected — but availability taxonomy is part of the production contract, so a
misclassified reason is still a defect.

THE RULE IS CHOSEN ON SEMANTIC ORDER, NOT ON WHAT THE BUILD ALREADY EMITTED. Production already
wrote the other answer, and adopting it for that reason would be implementation-result-driven
resolution — deciding what is correct by looking at what was produced. The registered ground is
independent of both implementations: the absence of a required operand is logically prior to
validating the other operand. If the predecessor does not exist at all, the dependency T needs
is structurally missing, and the current bar's coverage cannot change that.

THIS IS DELIBERATELY NARROW AND IS NOT "HISTORY ALWAYS WINS". Only NO_PRIOR_BAR — the structural
startup condition — takes precedence. Where a predecessor EXISTS but is contaminated, the
current bar's incompleteness still wins, exactly as before. Two negative fixtures pin both
halves, so a later reading cannot widen this into a general precedence of history over current.

NO_PRIOR_BAR IS WELL DEFINED HERE BECAUSE ABSENCE IS A PREFIX. Measured across the store: all
476 securities have prefix/suffix-only absence and ZERO interior gaps, so "the predecessor does
not exist" is exactly the head of each security's series and never an ambiguous mid-history hole.

CHANGES CLASSIFICATION ONLY. Not the predicates, the priority chain, the final T labels, the
canonical config, the caller/session-continuous semantics, the research timeframe or provenance.
The port hash necessarily changes, so the whole qualification chain re-runs — no shortcut.
"""
from __future__ import annotations
import glob, json, os, sys, time                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

BIND = {
    "MASSIVE_T_FEATURE_PORT_SPEC_V1.json": "545d293b4f1e4653",
    "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2.json": "bd9292853ee10f2c",
    "MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1.json": "c6358576c5c9411b",
    "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json": "21166d63a73181bd",
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json": "4c9b15eb8cfed786",
    "MASSIVE_T_FEATURE_PRODUCTION_V1.json": "e80b2817df28c6fd",
    "MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1.json": "e9e26a777fab7e79",
}
OLD_PORT_HASH = "5ab5390171e0d5d2"


def main():
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding digest mismatch:", bad); return 1

    p = dict(
        amendment_id="MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_3",
        status="MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_3_FROZEN",
        task_class="AVAILABILITY_CLASSIFICATION_ONLY",
        purpose="resolve availability-reason precedence when multiple blocking conditions "
                "coexist",
        amends=dict(artifact="MASSIVE_T_FEATURE_PORT_SPEC_V1", digest=BIND[
            "MASSIVE_T_FEATURE_PORT_SPEC_V1.json"], disposition="FROZEN — not edited"),
        bindings=BIND,

        defect_that_prompted_this=dict(
            found_by="the independent production verifier, not by review",
            symptom="the two qualified port implementations disagreed on the first bar of a "
                    "security's history when that bar is also not COMPLETE",
            vectorised_said="CURRENT_BAR_NOT_COMPLETE (its current-bar assignment ran last)",
            scalar_said="UNAVAILABLE_REQUIRED_HISTORY (it tested i == 0 first)",
            rows_affected=189, of_rows=15445454,
            t_labels_affected=0,
            why_still_a_defect="availability taxonomy is part of the production contract",
            why_smoke_missed_it="Layer 1 compared only AVAILABLE rows, and fixture P11 never "
                                "placed an unobserved bar at position 0"),

        frozen_rule=dict(
            ordered_steps=[
                dict(step=1,
                     condition="the required predecessor does not exist structurally",
                     availability="UNAVAILABLE_REQUIRED_HISTORY",
                     unavailable_reason="NO_PRIOR_BAR",
                     precedence="takes precedence over current-bar coverage state"),
                dict(step=2,
                     condition="the predecessor exists AND the current bar is not COMPLETE",
                     availability="UNAVAILABLE_CURRENT_INPUT",
                     unavailable_reason="CURRENT_BAR_NOT_COMPLETE"),
                dict(step=3,
                     condition="the current bar is COMPLETE AND the predecessor is not "
                               "COMPLETE",
                     availability="UNAVAILABLE_REQUIRED_HISTORY",
                     unavailable_reason="PRIOR_INPUT_NOT_COMPLETE"),
                dict(step=4, condition="otherwise", availability="EVALUABLE",
                     unavailable_reason=None)],
            none_semantics="NONE remains EVALUABLE and is never an unavailable state",
            explicitly_not="history always beats current — only the structural startup "
                           "condition NO_PRIOR_BAR carries precedence",
            basis="the absence of a required operand is logically prior to validating the "
                  "other operand",
            explicitly_not_basis="matching whichever answer the existing build already "
                                 "emitted, which would be implementation-result-driven "
                                 "semantic resolution"),

        no_prior_bar_is_well_defined=dict(
            measured_over_store=True, securities=476,
            prefix_or_suffix_only_absence=476, interior_gaps=0,
            consequence="'the predecessor does not exist' is exactly the head of each "
                        "security's series and never an ambiguous mid-history hole"),

        required_negative_fixtures=[
            dict(id="N_START_DOUBLE_FAILURE",
                 setup="first row of a security: no prior row AND current is "
                       "UNOBSERVED_CONTAMINATED",
                 expected=dict(availability="UNAVAILABLE_REQUIRED_HISTORY",
                               reason="NO_PRIOR_BAR"),
                 forbidden=["UNAVAILABLE_CURRENT_INPUT", "EVALUABLE_NO_T_STATE"]),
            dict(id="N_EXISTING_PRIOR_DOUBLE_FAILURE",
                 setup="the prior row EXISTS but is contaminated, and the current row is also "
                       "contaminated",
                 expected=dict(availability="UNAVAILABLE_CURRENT_INPUT",
                               reason="CURRENT_BAR_NOT_COMPLETE"),
                 purpose="prevents this amendment from being widened into a general "
                         "'history always wins' rule")],

        unchanged=["T predicates", "priority chain", "final T labels", "canonical config",
                   "caller / session-continuous semantics", "research timeframe",
                   "provenance classification"],

        port_hash_consequence=dict(
            old_port_hash=OLD_PORT_HASH, new_port_hash="TO_BE_COMPUTED",
            shortcut_forbidden=True,
            required_chain=["full smoke / capability re-qualification, not only the new "
                            "fixtures",
                            "Layer 1 final T labels must remain EXACT",
                            "availability census re-run",
                            "acceptability decision rebound / recomputed on the new port hash",
                            "production rebuilt",
                            "independent verification",
                            "only then PASS"],
            fix_scope="the VECTORISED port is corrected so NO_PRIOR_BAR is explicit; the "
                      "scalar verifier is aligned to the same frozen rule",
            explicitly_not="fixing only the verifier to match the existing build"),

        expected_accounting_effect=dict(
            unavailable_current_input=-189, unavailable_required_history=+189,
            must_remain_unchanged=["T evaluable", "T fired", "EVALUABLE_NO_T_STATE",
                                   "all twelve T state counts", "final T labels",
                                   "priority winners"],
            status="CROSS_CHECK_EXPECTATION_ONLY",
            explicitly_not="a hardcoded production target — the new run must derive it"),

        superseded_runs=dict(
            production=dict(artifact="MASSIVE_T_FEATURE_PRODUCTION_V1",
                            digest=BIND["MASSIVE_T_FEATURE_PRODUCTION_V1.json"],
                            disposition="HISTORICAL — BUILT_BUT_NOT_QUALIFIED, retained"),
            verification=dict(artifact="MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1",
                              digest=BIND["MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1.json"],
                              disposition="HISTORICAL HOLD, retained")),

        gates=dict(current_production="HOLD_NOT_QUALIFIED", new_port_qualification="REQUIRED",
                   acceptability_rebind="REQUIRED", new_production="REQUIRED",
                   z="HOLD", y="HOLD"),
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_3.json",
                 required=("amendment_id", "status", "frozen_rule",
                           "required_negative_fixtures", "port_hash_consequence",
                           "no_prior_bar_is_well_defined"),
                 supersede=os.path.exists(
                     "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_3.json"))
    print(f"MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_3 · {d} · {p['status']}")
    print("  rule        1 NO_PRIOR_BAR -> REQUIRED_HISTORY (structural, wins)")
    print("              2 prior exists + current not COMPLETE -> CURRENT_INPUT")
    print("              3 current COMPLETE + prior not COMPLETE -> REQUIRED_HISTORY")
    print("              4 otherwise -> EVALUABLE")
    print("  NOT         'history always wins' — narrow by construction, 2 fixtures pin it")
    print(f"  defect      189 / 15,445,454 rows · 0 T labels affected")
    print(f"  well-def    476/476 prefix-only absence · 0 interior gaps")
    print(f"  consequence port hash changes -> FULL re-qualification chain")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
