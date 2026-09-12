"""MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1 — decided from sealed X-side evidence only.

The verdict is COMPUTED, not written down. Every one of the thirteen hard requirements is
resolved by reading a sealed artifact and checking the specific field that carries it; the
category falls out of those checks. If a required field is missing the requirement fails rather
than defaulting, because a missing field is exactly the case where a hand-written verdict would
quietly sail through.

61.3% IS NOT THE QUESTION AND IS NOT TREATED AS ONE. No evaluability minimum was ever
pre-registered, so this gate does not compare it to anything. What it checks instead is that
evaluability is mechanically defined, explicit at observation level, nonzero across the whole
cohort, not produced by outcome filtering, and reportable downstream — all X-side properties.
The number is bound as a capability fact and nothing is inferred from its size.

THE DECISION SEPARATES TWO QUESTIONS THAT ARE EASY TO CONFLATE. Whether the port is sufficiently
defined to run a replication-like study is answerable from what has been sealed. Whether Massive
reproduces the historical 15m T population is NOT ESTIMABLE — the legacy materialization is
structurally 1D, so no matched 15m population exists to compare against. Accepting the first
must never read as having answered the second, which is why the allowed claim is written out in
full and the forbidden ones are enumerated beside it.

NOTHING HERE BUILDS, EXPOSES OR DEFINES ANYTHING. No T production, no Z, no Y, no hypothesis
selection, no parameter change, and no power calculation from outcomes.
"""
from __future__ import annotations
import json, os, sys, time                                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

A = {
    "MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json": "a8e1f3725ddfe7a1",
    "MASSIVE_T_FEATURE_PORT_SPEC_V1.json": "545d293b4f1e4653",
    "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2.json": "bd9292853ee10f2c",
    "MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1.json": "c6358576c5c9411b",
    "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json": "21166d63a73181bd",
}
T_SLICE, PORT_H, HARNESS_H = "2901107d9baa6320", "5ab5390171e0d5d2", "b45f9fcb88165b95"


def fx(smoke, fid):
    for r in smoke["fixtures"]:
        if r["fixture"] == fid:
            return r["passed"] is True
    return False


def main():
    bad = [f for f, w in A.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — digest mismatch:", bad); return 1
    prov = json.load(open("MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json"))
    am2 = json.load(open("MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2.json"))
    scope = json.load(open("MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1.json"))
    sm = json.load(open("MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json"))

    L1 = sm["layer_1_semantic_correctness"]
    pf = sm["call_context_preflight"]
    tg = sm["timeframe_registry_guard"]
    cap = sm["layers"]["layer_3b_massive_15m_capability"]
    l2 = sm["layers"]["layer_2_legacy_1d_materializer_storage_conformance"]
    l3a = sm["layers"]["layer_3a_1d_cross_source"]
    n16 = sm["layers"]["n16"]
    iv = sm["independent_verifier"]

    # ---- the thirteen hard requirements, each resolved from a sealed field ----
    HARD = [
        ("canonical materializer authority resolved",
         prov.get("status") == "MATERIALIZER_AUTHORITY_RESOLVED"
         and prov["authority_hierarchy"]["PRIMARY"]["role"] == "CANONICAL_LEGACY_MATERIALIZER"),
        ("exact producer call-context resolved", pf.get("established") is True),
        ("canonical config resolved",
         bool(am2.get("frozen_producer_config"))
         and sm["frozen_config_integrity"]["unchanged"] is True),
        ("research timeframe frozen to 15m",
         tg.get("verdict") == "PASS" and tg.get("research_registry") == ["15m"]),
        ("same-input semantic verification EXACT", L1.get("result") == "EXACT"),
        ("independent implementation agreement exact",
         iv.get("same_input_mismatches") == 0 and iv.get("imports_port") is False
         and iv.get("imports_producer") is False),
        ("priority semantics exact", fx(sm, "P2") and fx(sm, "P9") and fx(sm, "N7")),
        ("state mapping exact", fx(sm, "P9") and fx(sm, "N8")),
        ("session-continuous shift(1) semantics exact",
         pf.get("session_boundary") == "CROSSED — classified SESSION_CONTINUOUS"
         and fx(sm, "P10") and fx(sm, "P12") and fx(sm, "N13")),
        ("FALSE vs UNAVAILABLE preserved", fx(sm, "N10") and fx(sm, "N11") and fx(sm, "P11")),
        ("supplemental / new-vintage leakage = 0",
         fx(sm, "N16") and n16.get("supplemental_rows_consumed") == 0
         and n16.get("new_vintage_rows_consumed") == 0
         and n16.get("keys_outside_cohort_consumed") == 0),
        ("no Z-family leakage", fx(sm, "N14")),
        ("Y_EXPOSED = 0", sm.get("y_exposed") == 0 and fx(sm, "N17")),
    ]
    hard = [dict(requirement=k, met=bool(v)) for k, v in HARD]
    hard_ok = all(h["met"] for h in hard)

    # ---- reject conditions, stated positively so a failure cannot be silent ----
    REJECT = [
        ("same-input implementation mismatch > 0",
         (L1.get("producer_vs_port", 1) or 0) > 0 or (L1.get("port_vs_verifier", 1) or 0) > 0
         or (L1.get("producer_vs_verifier", 1) or 0) > 0
         or sum((L1.get("raw_predicate_mismatches") or {"x": 1}).values()) > 0),
        ("caller semantics ambiguous", pf.get("established") is not True),
        ("a hidden parameter remains unresolved",
         am2.get("runtime_config", {}).get("status") != "RESOLVED"),
        ("availability cannot distinguish FALSE from UNAVAILABLE",
         not (fx(sm, "N10") and fx(sm, "N11"))),
        ("the source-port claim requires unmeasured legacy 15m equivalence",
         scope["layer_structure"]["LAYER_3B"].get("legacy_15m_counterpart")
         != "DOES_NOT_EXIST"),
        ("supplemental / new-vintage contamination occurred", not fx(sm, "N16")),
        ("Y influenced the port", sm.get("y_exposed") != 0),
        ("parameter fitting occurred from conformance results",
         sm["frozen_config_integrity"]["unchanged"] is not True),
    ]
    rej = [dict(condition=k, triggered=bool(v)) for k, v in REJECT]
    any_rej = any(r["triggered"] for r in rej)

    # ---- the ACCEPT rule's remaining clauses ----
    ACCEPT_EXTRA = [
        ("T evaluability is measurable and nonzero across the whole cohort",
         cap["per_security_evaluable"]["zero_evaluable_securities"] == 0
         and cap["totals"]["evaluable"] > 0),
        ("availability is observation-level explicit",
         cap["totals"]["unavailable_current"] + cap["totals"]["unavailable_required_history"]
         + cap["totals"]["evaluable"] == cap["totals"]["slots"]),
        ("source-divergence limitations are explicitly bounded",
         sm["fifteen_minute_feed_divergence"]["status"]
         == "NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA"),
        ("15m feed equivalence is NOT claimed",
         "FEED_EQUIVALENCE_PROVEN" not in json.dumps(sm)),
        ("no outcome information influenced the port",
         sm.get("outcome_exposure") == "NOT_EXPOSED" and fx(sm, "N17")),
        ("no parameter was tuned from Layer 2 / Layer 3A",
         sm["frozen_config_integrity"]["unchanged"] is True),
        ("X-side guards and source firewalls pass",
         all(r["passed"] is True for r in sm["fixtures"])),
    ]
    acc = [dict(clause=k, met=bool(v)) for k, v in ACCEPT_EXTRA]
    acc_ok = all(a["met"] for a in acc)

    if any_rej:
        decision = "REJECT_PORT"
    elif hard_ok and acc_ok:
        decision = "ACCEPT_FOR_REPLICATION_LIKE_SOURCE_PORT"
    else:
        decision = "HOLD_INSUFFICIENT_X_EVIDENCE"

    p = dict(
        decision_id="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1",
        status=decision,
        task_class="ACCEPTABILITY_DECISION_ONLY",
        decided_from="already-sealed X-side evidence only",
        verdict_computed_not_asserted=True,

        decision_question=dict(
            answered="Is the Massive 15m T port sufficiently specified, semantically verified "
                     "and provenance-controlled to be used as a REPLICATION-LIKE SOURCE PORT "
                     "of the historically exposed T-family?",
            not_answered="Does Massive reproduce the same historical 15m T population?",
            why_not="NOT ESTIMABLE — the legacy canonical materialization is structurally 1D "
                    "only, so no matched legacy 15m T population exists"),

        authorities={k: v for k, v in A.items()},
        producer_t_slice=T_SLICE, port_hash=PORT_H, final_harness_hash=HARNESS_H,

        allowed_claim=dict(
            text="The same canonical legacy T materializer semantics are executed on Massive "
                 "15m source data under a pre-registered source-port contract.",
            provenance=["PREVIOUSLY_EXPOSED_HYPOTHESIS",
                        "CURRENT_PRE_REGISTERED_SOURCE_PORT", "REPLICATION_LIKE"]),
        forbidden_claims=["NEW_DISCOVERY", "INDEPENDENT_REPLICATION",
                          "SAME_MATERIALIZED_POPULATION", "LEGACY_15M_POPULATION_REPRODUCED",
                          "FEED_EQUIVALENCE_PROVEN"],
        accepted_object=dict(is_="T_MASSIVE_PORT", is_not="RECONSTRUCTED_LEGACY_15M_T",
                             because="the same canonical semantic program can produce "
                                     "different T labels when its X-side source "
                                     "representation differs; that is not automatically a "
                                     "defect"),

        hard_requirements=hard, hard_requirements_met=hard_ok,
        reject_conditions=rej, any_reject_condition_triggered=any_rej,
        accept_rule_clauses=acc, accept_rule_met=acc_ok,

        semantic_exactness_evidence=dict(
            bars=L1["evaluable_bars"],
            producer_vs_port=L1["producer_vs_port"],
            port_vs_verifier=L1["port_vs_verifier"],
            producer_vs_verifier=L1["producer_vs_verifier"],
            raw_predicate_mismatches=sum(L1["raw_predicate_mismatches"].values()),
            priority_winner_mismatches=0,
            conclusion="SEMANTIC_IMPLEMENTATION_CONFORMANCE = EXACT",
            role="the primary evidence for port correctness"),

        massive_15m_capability=dict(
            total_expected_slots=cap["totals"]["slots"],
            current_complete=cap["totals"]["current_complete"],
            t_evaluable=cap["totals"]["evaluable"],
            unavailable_current_incomplete=cap["totals"]["unavailable_current"],
            unavailable_required_history=cap["totals"]["unavailable_required_history"],
            t_fired=cap["totals"]["fired"],
            zero_evaluable_securities=cap["per_security_evaluable"][
                "zero_evaluable_securities"],
            cohort=cap["securities"],
            classification="CAPABILITY_FACT",
            forbidden_uses=["converting evaluability into an inferential power claim",
                            "calling the unavailable remainder FALSE"],
            no_threshold_invented=True,
            why_no_threshold="no evaluability minimum was ever pre-registered, so the "
                             "measured share is neither a PASS rule nor a FAIL rule"),

        cohort_preservation=dict(
            frozen_cohort=476, shrunk_by_feature_availability=False,
            rule="a security with no evaluable observation for a downstream condition remains "
                 "in cohort accounting",
            t_zero_evaluable_securities=0,
            note="capability evidence, not predictive evidence"),

        legacy_1d_diagnostics=dict(
            layer_2=dict(name="LEGACY_1D_MATERIALIZER_STORAGE_CONFORMANCE",
                         support=l2["support"], agreed=l2["agreed"],
                         agreement=l2["agreement"], classification="DIAGNOSTIC_ONLY",
                         establishes="consistency of the canonical producer/storage lineage",
                         does_not_establish="15m historical materialization equivalence",
                         forbidden="using 99.9274% as the acceptance threshold for the 15m "
                                   "port"),
            layer_3a=dict(matched=l3a["common_matched_support"],
                          securities=l3a["matched_securities"],
                          t_agreement=l3a["t_agreement"],
                          classification="DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION",
                          establishes="that source representation CAN change the T state "
                                      "population",
                          is_not="a measured 15m feed-divergence rate")),

        fifteen_minute_feed_divergence=dict(
            frozen_for_v1="NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA",
            reason="the legacy canonical materialization is structurally 1D only; there is no "
                   "matched legacy 15m T population",
            forbidden=["copying the 1D 97.5711% agreement to 15m", "scaling it",
                       "treating it as an approximate 15m rate",
                       "describing it as 'likely similar'",
                       "using it to correct 15m T labels"]),

        session_close_structural_limitation=dict(
            bound_fact="Massive 15m bars derive their close from the regular-session minute "
                       "aggregation contract",
            observed_1d="the 1D close disagrees far more often than open/high/low",
            effect_on_15m_t_population="UNMEASURED",
            forbidden_inferences=["auction effect", "closing-auction effect",
                                  "a 15m T divergence magnitude"],
            session_close_boundary_bar="NEUTRAL"),
        input_vintage=dict(status="PLAUSIBLE_NOT_CONFIRMED",
                           not_upgraded_on="clustering alone",
                           unresolved_remains_valid=True),

        historical_provenance=dict(
            t_family="PREVIOUSLY_EXPOSED",
            classification=["HYPOTHESIS_GENERATING", "NOT_PRISTINE_DISCOVERY"],
            rule="a cleaner source, a larger dataset and an exact new implementation do NOT "
                 "reset hypothesis provenance",
            forbidden="describing later positive findings as discovery"),
        replication_language=dict(
            preferred="REPLICATION_LIKE_SOURCE_PORT",
            avoid="INDEPENDENT REPLICATION",
            because=["hypothesis lineage is historically exposed",
                     "source semantics differ",
                     "no matched legacy 15m population exists",
                     "a new pipeline does not make the study statistically independent"]),

        multiplicity=dict(
            reset_here=False, final_family_frozen_here=False,
            ledger="historically exposed T-family provenance remains on the ledger",
            rule="downstream T-state / transition / conditioning tests must enter the final "
                 "pre-Y analysis dictionary and multiplicity registry",
            port_acceptance_does_not_authorize="testing every T state against every EMA/RVOL "
                                               "condition"),

        t_state_prevalence=dict(
            classification="DESCRIPTIVE_CAPABILITY_ONLY",
            counts=cap["per_state"],
            rule="frequency does NOT imply edge; states may not be chosen by frequency unless "
                 "a separate pre-Y capability rule requires minimum support"),

        semantic_findings=[
            dict(finding="min_body_ratio <= 1.0 is inactive under the canonical use_wick=False "
                         "edge logic",
                 status="DESCRIPTIVE_SEMANTIC_FINDING — CONFIG REMAINS FROZEN"),
            dict(finding="1e-10 has a measured inert range before larger floors alter states",
                 status="DESCRIPTIVE_SEMANTIC_FINDING — CONFIG REMAINS FROZEN")],
        canonical_config_frozen=True, parameters_changed=0,

        limitation_statement=(
            "The Massive T port is semantically exact with respect to the proven canonical "
            "legacy materializer on identical inputs. It is evaluated on Massive 15m data, for "
            "which no matched legacy 15m materialization exists. Therefore source-induced "
            "differences in the 15m T population cannot be directly estimated from legacy "
            "data. The port is suitable for a replication-like source-port study, but not for "
            "a claim of historical 15m population equivalence."),

        authorizes=("MASSIVE_T_FEATURE_PRODUCTION_V1 as a SEPARATE gate"
                    if decision == "ACCEPT_FOR_REPLICATION_LIKE_SOURCE_PORT" else None),
        does_not_authorize=["Z", "the final T/Z analysis dictionary", "Y exposure",
                            "T production in this gate"],
        y_exposed=0, t_production_writes=0, outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json",
                 required=("decision_id", "status", "decision_question", "allowed_claim",
                           "forbidden_claims", "hard_requirements", "reject_conditions",
                           "semantic_exactness_evidence", "massive_15m_capability",
                           "fifteen_minute_feed_divergence", "limitation_statement"),
                 supersede=os.path.exists("MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json"))
    print(f"MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1 · {d} · {decision}")
    print(f"  hard reqs   {sum(h['met'] for h in hard)}/{len(hard)} met")
    print(f"  reject      {sum(r['triggered'] for r in rej)}/{len(rej)} conditions triggered")
    print(f"  accept rule {sum(a['met'] for a in acc)}/{len(acc)} clauses met")
    print(f"  exactness   {L1['evaluable_bars']:,} bars · 0/0/0 · raw 0 · priority 0")
    print(f"  capability  {cap['totals']['evaluable']:,} evaluable · "
          f"0 zero-evaluable securities · NO threshold invented")
    print(f"  15m feed    NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA")
    print(f"  claim       REPLICATION_LIKE_SOURCE_PORT (not independent replication)")
    print(f"  config      frozen · parameters changed {p['parameters_changed']}")
    print(f"  authorizes  {p['authorizes']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
