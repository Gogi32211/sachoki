"""MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1 — rebind onto the new qualification.

The original decision (4c9b15eb8cfed786) was computed against port hash 5ab5390171e0d5d2.
AMENDMENT_3 corrected the availability-reason precedence, which necessarily changed the port
hash, so that decision no longer describes the thing being accepted. It is NOT edited; this
amendment re-derives every requirement against the new qualification and states what moved.

THE POINT IS TO PROVE THE CHANGE WAS ONLY WHAT WAS AUTHORISED. Re-running the hard requirements
would catch an outright regression, but not a semantic drift smuggled in beside the intended
fix. So this also diffs the new smoke against the original decision's own recorded evidence and
requires that everything except availability-reason classification is byte-identical: the same
Layer 1 exactness, the same evaluable count, the same fired count, the same twelve state counts.
If any of those moved, the amendment holds rather than accepting.

THE EXPECTED DELTA IS A CHECK, NOT A TARGET. AMENDMENT_3 predicted -189 / +189 between the two
unavailable reasons. That number is compared against what the new run actually produced; it was
never fed into it.
"""
from __future__ import annotations
import json, os, sys, time                                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

DEC, DEC_D = "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json", "4c9b15eb8cfed786"
AM3, AM3_D = "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_3.json", "a5edf6e6806d4e0b"
SMOKE, SMOKE_D = "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json", "49eb9658eda72773"
OLD_PORT, NEW_PORT = "5ab5390171e0d5d2", "0ca86eeb1f8ee8fe"


def fx(sm, fid):
    for r in sm["fixtures"]:
        if r["fixture"] == fid:
            return r["passed"] is True
    return False


def main():
    for f, w in ((DEC, DEC_D), (AM3, AM3_D), (SMOKE, SMOKE_D)):
        if ART.file_digest(f) != w:
            print(f"HOLD — digest mismatch {f}: {ART.file_digest(f)}"); return 1
    dec = json.load(open(DEC))
    sm = json.load(open(SMOKE))
    am3 = json.load(open(AM3))

    if sm["status"] != "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1_PASS":
        print("HOLD — new smoke did not pass"); return 1
    if sm["port_implementation_hash"][:16] != NEW_PORT:
        print("HOLD — smoke port hash mismatch"); return 1

    L1 = sm["layer_1_semantic_correctness"]
    cap = sm["layers"]["layer_3b_massive_15m_capability"]
    old = dec["semantic_exactness_evidence"]
    oldcap = dec["massive_15m_capability"]

    # ---- every hard requirement, re-derived against the NEW qualification ---
    hard = [
        ("same-input semantic verification EXACT", L1["result"] == "EXACT"),
        ("independent implementation agreement exact",
         sm["independent_verifier"]["same_input_mismatches"] == 0),
        ("the two implementations agree on AMENDMENT_3 precedence", fx(sm, "L1c")),
        ("N_START_DOUBLE_FAILURE", fx(sm, "N18")),
        ("N_EXISTING_PRIOR_DOUBLE_FAILURE", fx(sm, "N19")),
        ("priority semantics exact", fx(sm, "P2") and fx(sm, "P9") and fx(sm, "N7")),
        ("state mapping exact", fx(sm, "P9") and fx(sm, "N8")),
        ("session-continuous semantics exact",
         fx(sm, "P10") and fx(sm, "P12") and fx(sm, "N13")),
        ("FALSE vs UNAVAILABLE preserved",
         fx(sm, "N10") and fx(sm, "N11") and fx(sm, "P11")),
        ("supplemental / new-vintage leakage = 0", fx(sm, "N16")),
        ("no Z-family leakage", fx(sm, "N14")),
        ("research timeframe still 15m alone",
         sm["timeframe_registry_guard"]["research_registry"] == ["15m"]),
        ("canonical config unchanged during the run",
         sm["frozen_config_integrity"]["unchanged"] is True),
        ("Y_EXPOSED = 0", sm["y_exposed"] == 0 and fx(sm, "N17")),
        ("all fixtures ran and passed",
         all(r["passed"] is True for r in sm["fixtures"])),
    ]
    hard = [dict(requirement=k, met=bool(v)) for k, v in hard]
    hard_ok = all(h["met"] for h in hard)

    # ---- nothing but availability classification may have moved -------------
    inv = [
        ("Layer 1 producer_vs_port", old["producer_vs_port"], L1["producer_vs_port"]),
        ("Layer 1 port_vs_verifier", old["port_vs_verifier"], L1["port_vs_verifier"]),
        ("Layer 1 producer_vs_verifier", old["producer_vs_verifier"],
         L1["producer_vs_verifier"]),
        ("T evaluable", oldcap["t_evaluable"], cap["totals"]["evaluable"]),
        ("T fired", oldcap["t_fired"], cap["totals"]["fired"]),
        ("current COMPLETE", oldcap["current_complete"], cap["totals"]["current_complete"]),
        ("zero-evaluable securities", oldcap["zero_evaluable_securities"],
         cap["per_security_evaluable"]["zero_evaluable_securities"]),
    ]
    invariants = [dict(quantity=k, before=a, after=b, unchanged=a == b) for k, a, b in inv]
    states_same = dec["t_state_prevalence"]["counts"] == cap["per_state"]
    invariants.append(dict(quantity="twelve T state counts",
                           before="(12 counts)", after="(12 counts)",
                           unchanged=states_same))
    inv_ok = all(i["unchanged"] for i in invariants)

    d_hist = (cap["totals"]["unavailable_required_history"]
              - oldcap["unavailable_required_history"])
    d_cur = cap["totals"]["unavailable_current"] - oldcap["unavailable_current_incomplete"]
    exp = am3["expected_accounting_effect"]
    delta_ok = (d_hist == exp["unavailable_required_history"]
                and d_cur == exp["unavailable_current_input"])

    ok = hard_ok and inv_ok and delta_ok
    status = ("ACCEPT_FOR_REPLICATION_LIKE_SOURCE_PORT" if ok
              else "HOLD_INSUFFICIENT_X_EVIDENCE")

    p = dict(
        amendment_id="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1",
        status=status,
        task_class="ACCEPTABILITY_REBIND_ONLY",
        amends=dict(artifact="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1", digest=DEC_D,
                    disposition="FROZEN — not edited",
                    why="that decision was computed against port hash " + OLD_PORT +
                        ", which AMENDMENT_3 necessarily changed"),
        rebound_to=dict(port_implementation_hash=NEW_PORT,
                        previous_port_hash=OLD_PORT,
                        smoke=dict(artifact="MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1",
                                   digest=SMOKE_D, status=sm["status"]),
                        harness_hash=sm["harness_hash"][:16],
                        amendment_3=AM3_D),
        hard_requirements=hard, hard_requirements_met=hard_ok,
        unchanged_invariants=invariants, invariants_hold=inv_ok,
        authorised_change_only=dict(
            what_moved="availability-reason classification only",
            unavailable_required_history_delta=d_hist,
            unavailable_current_input_delta=d_cur,
            expected_by_amendment_3=dict(
                unavailable_required_history=exp["unavailable_required_history"],
                unavailable_current_input=exp["unavailable_current_input"]),
            matches_expectation=delta_ok,
            expectation_was="A CROSS-CHECK, never fed into the run"),
        allowed_claim=dec["allowed_claim"], forbidden_claims=dec["forbidden_claims"],
        accepted_object=dec["accepted_object"],
        fifteen_minute_feed_divergence=dec["fifteen_minute_feed_divergence"],
        limitation_statement=dec["limitation_statement"],
        carried_forward_unchanged=["allowed claim and provenance classification",
                                   "forbidden claims", "15m feed non-estimability",
                                   "multiplicity ledger status",
                                   "no evaluability threshold invented",
                                   "canonical config frozen"],
        authorizes=("MASSIVE_T_FEATURE_PRODUCTION_V1 as a SEPARATE gate, on port hash "
                    + NEW_PORT if ok else None),
        does_not_authorize=["Z", "the final T/Z analysis dictionary", "Y exposure"],
        y_exposed=0, t_production_writes=0, outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    dg = ART.seal(p, "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1.json",
                  required=("amendment_id", "status", "rebound_to", "hard_requirements",
                            "unchanged_invariants", "authorised_change_only"),
                  supersede=os.path.exists(
                      "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1.json"))
    print(f"MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1 · {dg} · {status}")
    print(f"  rebound     port {OLD_PORT} -> {NEW_PORT}")
    print(f"  smoke       {SMOKE_D} PASS")
    print(f"  hard reqs   {sum(h['met'] for h in hard)}/{len(hard)}")
    print(f"  invariants  {sum(i['unchanged'] for i in invariants)}/{len(invariants)} "
          f"unchanged (Layer 1, evaluable, fired, 12 state counts)")
    print(f"  delta       req-history {d_hist:+d} · current {d_cur:+d} · "
          f"matches AMENDMENT_3 expectation {delta_ok}")
    print(f"  authorizes  {p['authorizes']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
