"""SESSION_FILTER_UNIFICATION_AMENDMENT_V1 — one authoritative session-completeness helper.

Three different implementations of "is this session complete?" existed on the evidence
path, and no test could have caught the divergence because each was internally consistent:

    grammar      modal over (session_date, ticker) sessions      family-local
    estimand     modal over ROWS grouped by session_date only    row-weighted
    registered   modal across ALL TICKERS that traded the date   the actual contract

They disagree on 9 rows in the whole 1H history, all on 2022-06-10 — and there the
estimand's version happened to be RIGHT while the grammar's was wrong. A correct number
from an unregistered implementation is not a correct implementation.

This amendment fixes the rule and the architecture: ONE helper, consumed identically by
both sides, so the divergence becomes constructively impossible rather than merely absent.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, inspect, json, os, sys, time                          # noqa: E402
import pandas as pd                                                   # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402
import session_calendar as SC                                         # noqa: E402

OUT = "SESSION_FILTER_UNIFICATION_AMENDMENT_V1.json"


def main():
    body = dict(
        spec_id="SESSION_FILTER_UNIFICATION_AMENDMENT_V1", status="FROZEN",
        amends=["ADJACENCY_REMEDIATION_V1",
                "SESSION_VALIDITY_REMEDIATION_AMENDMENT_V1"],
        amends_digests=dict(
            adjacency=ART.file_digest("ADJACENCY_REMEDIATION_V1.json"),
            session_validity=ART.file_digest(
                "SESSION_VALIDITY_REMEDIATION_AMENDMENT_V1.json")),
        trigger="the corrected T1 reconciliation showed 10 grammar-level membership "
                "changes and zero estimand-level changes; checking why exposed a THIRD "
                "session-completeness implementation — the estimand's own — which "
                "disagreed with the grammar's on 9 rows, all on 2022-06-10, and which "
                "happened to be right there",
        finding=dict(
            grammar_implementation="modal over (session_date, ticker) sessions — "
                                   "family-local",
            estimand_implementation="modal over ROWS grouped by session_date only — "
                                    "row-weighted; present INDEPENDENTLY in both "
                                    "analyzable_episodes() and membership()",
            third_site="analyzable_episodes() carried its own copy, found only after the "
                       "first two were unified — which is why the inventory below counts "
                       "consumers rather than trusting a shared-helper assumption",
            registered_rule="modal bar count across ALL TICKERS that traded the date",
            rows_in_disagreement=9, dates=["2022-06-10"],
            lesson="a correct number from an unregistered implementation is not a correct "
                   "implementation; the estimand's accidental correctness on that date is "
                   "not evidence of compliance"),
        registered_rule=dict(
            statement="grammar eligibility and estimand membership eligibility MUST "
                      "consume the SAME market-wide expected_bars(date) mapping",
            mapping="SESSION_REFERENCE_UNIVERSE_V1 with SESSION_MODE_TIE_POLICY_V1",
            reference_universe=ART.file_digest("SESSION_REFERENCE_UNIVERSE_V1.json"),
            tie_policy=ART.file_digest("SESSION_MODE_TIE_POLICY_V1.json")),
        forbidden=["family-local modal in the grammar",
                   "row-weighted / session_date-only modal in the estimand",
                   "any independently recomputed expected session length",
                   "silent fallback to either legacy implementation",
                   "keeping a diagnostic mirror on the production evidence path"],
        architecture=dict(
            helper="session_calendar.py",
            helper_digest=ART.file_digest("session_calendar.py"),
            api=["expected_bars_by_date()", "session_is_complete(frame)",
                 "complete_sessions(frame)"],
            call_site_inventory=dict(
                active_consumers_switched=12,
                per_family=["<fam>_sequence_grammar — the enumeration's session filter",
                            "<fam>_sequence_estimand.analyzable_episodes — the population "
                            "filter",
                            "<fam>_sequence_estimand.membership — the estimand membership "
                            "filter"],
                families=["T5", "T9", "T3", "T1"],
                note="three ACTIVE consumers per family x 4 families = 12; each had its "
                     "own inline modal computation, so 12 legacy modal implementations "
                     "disappeared and 12 call sites now read one helper. They were not "
                     "three variants of one shared function — analyzable_episodes and "
                     "membership each recomputed their own, and the grammar a third.",
                legacy_modal_sites_after=0),
            consumed_identically_by=["<fam>_sequence_grammar",
                                     "<fam>_sequence_estimand.analyzable_episodes",
                                     "<fam>_sequence_estimand.membership"],
            source=inspect.getsource(SC.session_is_complete),
            guarantee="both sides call one pure function over one sealed mapping, so a "
                      "divergence is constructively impossible rather than merely absent"),
        local_mirror_status=dict(
            role="REFERENCE ORACLE / REGRESSION TEST",
            not_role="PRODUCTION MEMBERSHIP IMPLEMENTATION",
            requirement="production membership == independent reference mirror for EVERY "
                        "supported syntactic name, not only the changed ones"),
        supersedes_own_earlier_count="an earlier draft of this amendment implied "
                                     "8 call sites; the verified inventory is 12",
        required_closure=[
            "1 grammar uses the shared expected_bars mapping",
            "2 estimand uses the same mapping",
            "3 expected_bars digest identical on both sides",
            "4 active family-local modal code paths == 0",
            "5 active row-weighted modal code paths == 0",
            "6 production membership == independent reference mirror for every supported "
            "claim",
            "7 re-run the T1 final-evidence identity reconciliation on the unified path",
            "8 classify Branch A/B from FINAL EVIDENCE INPUTS, per the pre-frozen rule"],
        branch_classification_note="the pre-frozen Branch A/B rule decides on "
                                   "evidence-bearing inputs — eligible population, blocks, "
                                   "final claim memberships, claim identities and order, k, "
                                   "outcome sidecar — not on an upstream intermediate "
                                   "artifact. T1's branch is therefore NOT pre-assigned "
                                   "here; provenance will record the upstream grammar "
                                   "defect either way.",
        other_families=dict(
            scope="T5 / T9 / T3 are not reopened; the shared refactor requires only a "
                  "regression closure",
            requirement="final memberships after the unified helper == sealed",
            if_exact="their RETAINED status is unchanged; no new Z, theta or permutation"),
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "registered_rule", "forbidden",
                                      "architecture", "required_closure"),
                 supersede=os.path.exists(OUT))
    print(f"SESSION_FILTER_UNIFICATION_AMENDMENT_V1 · {d}")


if __name__ == "__main__":
    main()
