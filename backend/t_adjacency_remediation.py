"""ADJACENCY_REMEDIATION_V1 — the correction rule, frozen BEFORE any corrected result.

A registered-specification / implementation mismatch was confirmed on 2026-08-22:
STRICT_ADJACENT was registered as "consecutive bars of the session", and the
implementation compared `session_position`, a DENSE rank of OBSERVED rows. When an
expected intraday slot is absent, dense ranking closes the hole and the two surrounding
bars are admitted as adjacent. Census on T1: 6,313 admitted pairs span a missing hour.

This artifact fixes the correction rule and the acceptance invariants. It is sealed while
the corrected enumeration output is still unread, so no corrected number can have
influenced the rule. Nothing else changes: no token set, no support floor, no block
definition, no outcome, no statistic, no threshold.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, inspect, json, os, sys, time                          # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "ADJACENCY_REMEDIATION_V1.json"

SESSION_OPEN_MIN = 9 * 60 + 30                 # 09:30 America/New_York
SESSION_CLOSE_MIN = 16 * 60                    # 16:00
SLOT_MINUTES = 60
N_SLOTS_1H = 7                                 # 09:30..15:30


def slot_of(ts_et):
    """THE FROZEN SLOT MAPPING for the 1H grammar.

    A bar belongs to the scheduled hour that contains its timestamp:

        slot = floor((minute_of_day - 09:30) / 60),  slot in 0..6

    An irregular timestamp inside an hour (a first print at 13:45 rather than 13:30, which
    is ordinary for thin names) maps to that hour's slot and therefore does NOT create a
    gap. A gap exists only when a scheduled slot has no bar at all.
    """
    t = pd.to_datetime(ts_et)
    m = t.dt.hour * 60 + t.dt.minute
    return np.floor((m - SESSION_OPEN_MIN) / SLOT_MINUTES).astype(int)


def main():
    rule_src = inspect.getsource(slot_of)
    rule_digest = hashlib.sha256(rule_src.encode()).hexdigest()[:16]
    body = dict(
        spec_id="ADJACENCY_REMEDIATION_V1", status="FROZEN",
        frozen_before="any corrected enumeration output is read",
        defect=dict(
            kind="REGISTERED SPECIFICATION / IMPLEMENTATION MISMATCH",
            registered="STRICT_ADJACENT — consecutive bars of the session",
            implemented="session_position[j+1] == session_position[j] + 1, where "
                        "session_position is a DENSE rank of OBSERVED rows",
            consequence="a window spanning an absent scheduled slot is admitted as "
                        "strictly adjacent; dense ranking silently closes the hole",
            first_census="T1 1H: 6,313 of 1,876,962 position-adjacent pairs span a "
                         "missing hour (0.336%), touching 5,342 sessions and 4,003 "
                         "episodes; 0 pairs sit inside one hour",
            severity="the smallness of the share is NOT a validity argument — the "
                     "registered estimand was not executed exactly, and claim-level "
                     "impact can be disproportionate near the support floor and in "
                     "3-bar windows"),
        registered_intent=dict(
            statement="STRICT_ADJACENT means CONSECUTIVE EXPECTED SESSION SLOTS, never "
                      "consecutive observed rows",
            slot_identity="the scheduled 1H slots of the regular session: 09:30..15:30 "
                          "America/New_York, 7 slots, one per hour",
            pair_rule="slot[j+1] - slot[j] == 1",
            triple_rule="slot[2]-slot[1] == 1 AND slot[3]-slot[2] == 1",
            interior_missing_slot="a window spanning it is INADMISSIBLE",
            short_session="a late start or an early close is valid across the slots that "
                          "were actually scheduled for that session; no window is required "
                          "to reach slots that never existed",
            irregular_timestamp="maps to its canonical slot by the frozen mapping below; "
                                "a minute offset inside the hour must NEVER create a gap",
            slot_mapping_source=rule_src,
            slot_mapping_digest=rule_digest),
        acceptance_invariants={
            "A": "remove one interior bar -> every window spanning it disappears and no "
                 "bridging window is created",
            "B": "remove a terminal bar (a true early close) -> earlier contiguous windows "
                 "are unchanged",
            "C": "row shuffle -> identical memberships",
            "D": "admitted windows with slot delta > 1 == EXACTLY ZERO"},
        scope=dict(
            automatic=["T5 1H", "T9 1H", "T3 1H", "T1 1H"],
            reason="the same builder and the same adjacency predicate are shared by all "
                   "four families, so scope follows the code path and is fixed here "
                   "BEFORE any impact number is known",
            separate_audit="15m — its own position/adjacency implementation must be read "
                           "and reported; no assumption either way is registered here"),
        forbidden_in_this_remediation=[
            "token set changes", "support floor changes", "block definition changes",
            "outcome definition changes", "inference statistic changes",
            "threshold changes", "any selection informed by corrected outcomes"],
        execution_order_for_exposed_families=[
            "1 fix adjacency semantics only",
            "2 rebuild the full grammar exhaustively",
            "3 recompute k from corrected memberships",
            "4 re-freeze the claim order and the capability needles FROM THE CORRECTED "
            "universe — the old q10/q50/q90 may not be carried over when k, supports or "
            "memberships move",
            "5 re-run capability on the corrected claim universe",
            "6 seal the corrected historical spec",
            "7 only then compute the corrected observed Z and max-null",
            "8 compare old vs corrected descriptively"],
        classification=dict(
            T1="still a clean pre-Y study — no T1 outcome has ever been observed",
            T3_T9_T5="POST-EXPOSURE DETERMINISTIC REMEDIATION OF A REGISTERED "
                     "IMPLEMENTATION DEFECT; the correction rule is frozen before the "
                     "corrected results are computed and is not chosen using corrected "
                     "outcomes. Provenance must state that the correction happened AFTER "
                     "exposure.",
            old_artifacts="SUPERSEDED_FOR_REGISTERED_STRICT_ADJACENCY — preserved for "
                          "audit, never deleted or silently overwritten; the recorded "
                          "implementation result stands as what that implementation "
                          "produced, and is not accepted as registered-grammar evidence"),
        t5_forward=dict(
            x_ingestion="MAY CONTINUE — outcome-blind storage is not affected",
            outcome_access_and_finalization="HOLD",
            promotion_and_status_computation="HOLD",
            reason="the frozen 1H medoid memberships may move under the corrected "
                   "adjacency, so forward evidence must not keep accruing under the old "
                   "semantics as evidence"),
        next_artifact="ADJACENCY_IMPACT_REPORT_V1 — X and membership level only, for "
                      "T5/T9/T3/T1, with no new outcome, Z or theta computation",
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "registered_intent",
                                      "acceptance_invariants", "scope"),
                 supersede=os.path.exists(OUT))
    print(f"ADJACENCY_REMEDIATION_V1 · {d} · slot rule {rule_digest}")


if __name__ == "__main__":
    main()
