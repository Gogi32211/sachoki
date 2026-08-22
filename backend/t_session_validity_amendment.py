"""SESSION_VALIDITY_REMEDIATION_AMENDMENT_V1 — sealed before any claim-level corrected
result is opened. ADJACENCY_REMEDIATION_V1 is NOT rewritten; this amends it.

WHAT THE FROZEN GRAMMAR ACTUALLY REGISTERED (t5_sequence_grammar docstring, verbatim):

    "SESSION VALIDITY IS THE CALENDAR, NOT `bars_in_session == 7`
     An early close is a complete session with fewer bars, and discarding it would delete
     real market days. The exchange's session length for a date is taken as the modal bar
     count ACROSS ALL TICKERS THAT TRADED IT; a ticker's session is complete when it
     matches that. A ticker with 2 bars on a day the market ran 7 is incomplete — a data
     gap, not a short session."

    "...adjacency means adjacent observed bars inside the declared window."

So the registered design is TWO rules working together: a completeness filter defined by
the MARKET-WIDE modal, and adjacency over observed bars INSIDE a complete session. Under a
correct completeness filter the two coincide — a complete session has every scheduled slot,
so adjacent observed bars ARE adjacent scheduled slots.

THE DEFECT IS THEREFORE NARROWER AND MORE PRECISE THAN FIRST STATED

    registered   modal bar count across ALL TICKERS THAT TRADED THAT DATE
    implemented  modal bar count across THIS FAMILY'S OWN EPISODES on that date

On dates where a family has few episodes the family-local modal is unstable, and pandas'
mode() returns the smallest value on a tie. Observed: T1 on 2022-06-10 has three sessions
of lengths {7, 2, 6}; the family-local modal came out 2, so the genuine 7-bar session was
REJECTED as incomplete and a 2-bar session carrying an interior gap was ADMITTED. That
admitted session is the origin of the single gap-spanning adjacency found in T1.

CORRECTION OF MY OWN EARLIER WORDING

ADJACENCY_REMEDIATION_V1 states "STRICT_ADJACENT means consecutive EXPECTED SESSION SLOTS,
never consecutive observed rows". Measured against the frozen text that is an overreach:
the registered adjacency is over observed bars inside a COMPLETE session. Slot adjacency
is not a redefinition — it is the INVARIANT that a correct completeness filter implies,
and it stays as acceptance invariant D. The remediation restores the registered rule; it
does not install a new design.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                   # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402

OUT = "SESSION_VALIDITY_REMEDIATION_AMENDMENT_V1.json"

REGISTERED = ("The exchange's session length for a date is taken as the modal bar count "
              "across all tickers that traded it; a ticker's session is complete when it "
              "matches that. A ticker with 2 bars on a day the market ran 7 is incomplete "
              "— a data gap, not a short session.")


def main():
    body = dict(
        spec_id="SESSION_VALIDITY_REMEDIATION_AMENDMENT_V1", status="FROZEN",
        amends="ADJACENCY_REMEDIATION_V1",
        amends_digest=ART.file_digest("ADJACENCY_REMEDIATION_V1.json"),
        frozen_before="any claim-level corrected enumeration result is opened",
        trigger="family-local modal bars_in_session was found to be an implementation "
                "substitute for the registered market-wide calendar rule",
        registered_rule=dict(
            text=REGISTERED,
            source="t5_sequence_grammar.py module docstring — the frozen grammar contract",
            source_digest=ART.file_digest("t5_sequence_grammar.py"),
            reading="session validity comes from the exchange calendar as evidenced by the "
                    "modal bar count ACROSS ALL TICKERS THAT TRADED THAT DATE, never from "
                    "the empirical distribution of bars among one family's episodes"),
        registered_adjacency=dict(
            text="adjacency means adjacent observed bars inside the declared window",
            consequence="inside a session that is complete under the registered rule, "
                        "adjacent observed bars ARE adjacent scheduled slots; slot "
                        "adjacency is the invariant, not a new definition"),
        forbidden=[
            "family-specific mode",
            "date-local mode over an episode subset",
            "smallest-value tie-breaking of a mode",
            "using observed peer episodes of the same family to determine expected "
            "session length",
            "redefining adjacency as slot arithmetic in place of the registered rule",
            "widening completeness from session-complete to merely window-complete "
            "because it is more convenient"],
        corrected_procedure=[
            "1 compute expected session length per date from the 1H store across ALL "
            "tickers that traded that date (the registered population)",
            "2 a ticker-session is COMPLETE iff its bar count equals that date's expected "
            "length — early closes stay valid, data gaps do not",
            "3 enumerate adjacency over observed bars inside complete sessions, exactly as "
            "registered",
            "4 verify acceptance invariant D: admitted windows with slot delta > 1 == 0"],
        decision_rule_frozen_now=dict(
            note="fixed BEFORE the impact table is opened, so neither branch can be chosen "
                 "to suit a result",
            BRANCH_A=dict(
                condition="the corrected implementation produces EXACTLY the same eligible "
                          "population, block assignment, claim identities and order, every "
                          "claim membership set, k, and outcome sidecar identity",
                consequence="the evidence inputs never moved; the existing capability and "
                            "historical statistics stand",
                required_proof=["exact hash identity of population, blocks, claim order and "
                                "every membership set",
                                "deterministic replay under the ORIGINAL frozen RNG and "
                                "permutation identities",
                                "observed Z, max-null vector and survivor booleans "
                                "bit-identical"],
                forbidden=["new needle selection", "new RNG root", "any new statistical "
                           "search"],
                status_wording="Registered implementation defect confirmed, but no realized "
                               "evidence-input impact for this family; original result "
                               "retained after exact deterministic remediation replay."),
            BRANCH_B=dict(
                condition="even one claim membership, eligibility, block or k changes",
                consequence="old artifact -> SUPERSEDED_FOR_REGISTERED_SESSION_SEMANTICS",
                sequence=["rebuild the full grammar", "recompute k",
                          "reseal the claim order", "reseal the capability needles from "
                          "the corrected universe", "rerun capability",
                          "seal the corrected historical spec",
                          "rerun the historical inference",
                          "compare old vs corrected descriptively"])),
        t1_status="Branch B is already determined for T1 on the evidence in hand: one "
                  "admitted gap-spanning adjacency and a mis-classified session on "
                  "2022-06-10. T1 has never had an outcome exposed, so this remains a "
                  "clean pre-Y remediation.",
        t5_t9_t3_status="UNDETERMINED pending the exact corrected-input identity proof; "
                        "gap-spanning admitted == 0 alone is NOT sufficient — the calendar "
                        "validity correction must also pass the full comparison",
        regression_fixture=dict(
            name="INTG_2022_06_10",
            date="2022-06-10", ticker="INTG",
            observed_family_session_lengths={"7": 1, "2": 1, "6": 1},
            old_behaviour="family mode -> 2; the 2-bar session with an interior gap "
                          "(09:30 slot 0, 12:45 slot 3) was admitted and the 7-bar "
                          "complete session was rejected",
            corrected_behaviour="the market-wide expected length governs; the interior-gap "
                                "session is rejected and the calendar-complete session is "
                                "retained",
            purpose="a permanent regression fixture that catches BOTH defects together"),
        fifteen_minute=dict(
            preliminary="the 15m enumeration selects bars by exact ET time "
                        "(09:30/09:45/10:00/10:15 -> M1..M4) and claims are "
                        "position-anchored names, so no dense re-ranking exists",
            required_machine_proof=["missing M2 -> M1->M2 absent and 10:00 never falls "
                                    "back into M2",
                                    "missing M3 -> M2->M3 absent, no positional compression",
                                    "selector is exact-time only; fallback-to-next-observed "
                                    "count == 0"],
            status="PASS THIS DEFECT CLASS pending that closure"),
        impact_report_requirements=dict(
            session_validity=["dates examined", "family-local modal expected length",
                              "calendar expected length", "dates where modal != calendar",
                              "episodes affected", "old-valid -> corrected-invalid",
                              "old-invalid -> corrected-valid",
                              "of those: sequence-analyzable affected, membership cells "
                              "affected"],
            grammar=["old/new names", "old/new support-qualified", "old/new classes",
                     "old/new k", "membership unchanged/changed", "cells added/removed",
                     "merges", "splits", "support threshold crossings",
                     "Jaccard old vs corrected"]),
        outcome_exposure="NOT_EXPOSED — no outcome value read; no Z, theta or survivor is "
                         "computed anywhere in this remediation phase",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "registered_rule", "forbidden",
                                      "decision_rule_frozen_now"),
                 supersede=os.path.exists(OUT))
    print(f"SESSION_VALIDITY_REMEDIATION_AMENDMENT_V1 · {d}")


if __name__ == "__main__":
    main()
