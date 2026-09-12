"""MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_2 — rebind after the premise failed.

One of the accepted decision's premises turned out to be false: it recorded 15m source
divergence as NOT ESTIMABLE, and it has since been measured directly. That is enough to require
a recomputation rather than an assertion that the earlier ACCEPT still stands, so every
invariant is re-derived from the sealed artifacts and the verdict falls out of those checks.

THE QUESTION IS NARROW AND IT IS NOT THE ONE THE NUMBER TEMPTS YOU TO ASK. Measured divergence
of 5.4461% does not bear on whether the port is what it claims to be. The accepted claim was
always "the same canonical legacy T materializer semantics executed on Massive 15m source data",
never "Massive reproduces legacy 15m labels". A source-port claim is falsified by the engine
being wrong, the config drifting, or outcomes leaking into construction — not by two different
feeds producing different populations under the same engine. That is the thing being measured,
not a defect in the thing being accepted.

WHAT THE MEASUREMENT DOES CHANGE IS THE LIMITATION, AND IT TIGHTENS IT. Previously the size of
the source effect was unknown; now it is known to be material and position-dependent within
exactly the opening hour the historical family is anchored on. So the replication-like framing
is strengthened and the population-equivalence framing is forbidden more firmly than before.

NO REBUILD. The diagnostic touched no engine, no port hash, no config and no Massive input
semantics, and no label was corrected toward legacy — each of which is checked here rather than
assumed. T production stays byte-valid at ccdb0e35490794da; only its downstream authorization
was held, and this amendment is what releases it.
"""
from __future__ import annotations
import json, os, sys, time                                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

A = {
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json": "4c9b15eb8cfed786",
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1.json": "cdd94787f424b5cb",
    "MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2.json": "1582a8d21042c6a8",
    "MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1.json": "53c92f9df3b40d26",
    "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json": "49eb9658eda72773",
    "MASSIVE_T_FEATURE_PRODUCTION_V1.json": "ccdb0e35490794da",
    "MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1.json": "2bc48ac417c2cbc9",
}
PORT = "0ca86eeb1f8ee8fe"


def main():
    bad = [f for f, w in A.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — digest mismatch:", bad); return 1
    dec = json.load(open("MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json"))
    am1 = json.load(open("MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1.json"))
    sc2 = json.load(open("MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2.json"))
    diag = json.load(open("MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1.json"))
    sm = json.load(open("MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json"))
    prod = json.load(open("MASSIVE_T_FEATURE_PRODUCTION_V1.json"))
    ver = json.load(open("MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1.json"))
    L1 = sm["layer_1_semantic_correctness"]
    ns = prod["no_source_normalization"]

    inv = [
        ("port hash unchanged", sm["port_implementation_hash"][:16] == PORT
         and prod["port_implementation_hash"] == PORT
         and am1["rebound_to"]["port_implementation_hash"] == PORT),
        ("canonical config unchanged during qualification",
         sm["frozen_config_integrity"]["unchanged"] is True),
        ("canonical config unchanged during production",
         prod["config_guard"]["unchanged"] is True),
        ("same-input Layer 1 EXACT", L1["result"] == "EXACT"
         and L1["producer_vs_port"] == 0 and L1["port_vs_verifier"] == 0
         and L1["producer_vs_verifier"] == 0),
        ("T production artifact is the accepted one",
         ART.file_digest("MASSIVE_T_FEATURE_PRODUCTION_V1.json") == A[
             "MASSIVE_T_FEATURE_PRODUCTION_V1.json"]),
        ("T production independently verified",
         ver["status"] == "MASSIVE_T_FEATURE_PRODUCTION_V1_PASS"),
        ("parameter changes = 0", prod.get("parameters_changed", 0) == 0
         and dec["parameters_changed"] == 0),
        ("no source correction — Massive not normalised toward legacy",
         ns["rounded_to_2dp"] is False and ns["closes_replaced_from_legacy"] is False
         and ns["labels_corrected_from_1d"] is False),
        ("no label was corrected using the new 15m diagnostic",
         "authorize correcting Massive T labels toward legacy" in diag["does_not"]),
        ("Y_EXPOSED = 0", sm["y_exposed"] == 0 and prod["y_exposed"] == 0
         and diag["y_exposed"] == 0),
    ]
    invariants = [dict(invariant=k, holds=bool(v)) for k, v in inv]
    inv_ok = all(i["holds"] for i in invariants)

    # ---- does the measured divergence trigger any REJECT condition? ---------
    rej = [
        ("same-input implementation mismatch > 0", L1["producer_vs_port"] > 0),
        ("the source-port claim requires unmeasured legacy 15m equivalence",
         "same materialized population" in dec["allowed_claim"]["text"]),
        ("outcomes influenced construction", sm["y_exposed"] != 0),
        ("parameters were fitted from conformance results",
         prod["config_guard"]["unchanged"] is not True),
        ("Massive labels were corrected toward legacy",
         ns["labels_corrected_from_1d"] is not False),
    ]
    rejects = [dict(condition=k, triggered=bool(v)) for k, v in rej]
    any_rej = any(r["triggered"] for r in rejects)

    ok = inv_ok and not any_rej
    status = ("ACCEPT_FOR_REPLICATION_LIKE_SOURCE_PORT" if ok
              else "HOLD_INSUFFICIENT_X_EVIDENCE")

    p = dict(
        amendment_id="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_2",
        status=status,
        task_class="ACCEPTABILITY_REBIND_ONLY",
        verdict_computed_not_asserted=True,
        amends=dict(decision="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1",
                    digest=A["MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json"],
                    amendment_1=A[
                        "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1.json"],
                    disposition="FROZEN — neither edited"),
        why_recomputed="one of the accepted decision's premises was false — it recorded 15m "
                       "source divergence as NOT ESTIMABLE, and it has since been measured",

        decision_question=dict(
            asked="does directly measured 15m source divergence invalidate the accepted "
                  "object T_MASSIVE_PORT under its source-port claim?",
            answer="NO" if ok else "UNRESOLVED",
            reasoning="the accepted claim was always 'the same canonical legacy T "
                      "materializer semantics executed on Massive 15m source data', never "
                      "'Massive reproduces legacy 15m labels'. A source-port claim is "
                      "falsified by a wrong engine, drifted config, or outcome leakage — not "
                      "by two feeds producing different populations under the same engine, "
                      "which is the thing being MEASURED rather than a defect in the thing "
                      "being ACCEPTED"),

        invariants=invariants, invariants_hold=inv_ok,
        reject_conditions=rejects, any_reject_triggered=any_rej,

        measured_divergence=dict(
            authority="MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1",
            digest=A["MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1.json"],
            matched_bars=diag["support"]["bars_compared"],
            matched_securities=diag["support"]["securities_compared"],
            t_agreement=diag["agreement"]["t_label_agreement"],
            t_divergence=round(1 - diag["agreement"]["t_label_agreement"], 6),
            by_session_position={k: v["agreement"]
                                 for k, v in diag["by_session_position"].items()}),

        new_limitation=(
            "The same canonical semantic engine produces materially different 15m T "
            "populations across the two available 15m OHLC sources. Overall matched-state "
            "divergence is 5.4461%, and the divergence is position-dependent within the "
            "opening hour. Therefore Massive results test the historically exposed semantic "
            "hypotheses on a different X-source realization; they do not reconstruct the "
            "historical candidate population."),
        limitation_effect=dict(
            replication_like_framing="STRENGTHENED",
            population_equivalence_framing="FORBIDDEN MORE FIRMLY THAN BEFORE",
            why="the size of the source effect is no longer unknown, and it falls in exactly "
                "the opening-hour positions the historical family is anchored on"),

        carried_forward_unchanged=dict(
            allowed_claim=dec["allowed_claim"], forbidden_claims=dec["forbidden_claims"],
            accepted_object=dec["accepted_object"],
            multiplicity_ledger=dec["multiplicity"],
            historical_provenance=dec["historical_provenance"],
            no_evaluability_threshold_invented=True,
            canonical_config_frozen=True),

        production_disposition=dict(
            artifact="MASSIVE_T_FEATURE_PRODUCTION_V1",
            digest=A["MASSIVE_T_FEATURE_PRODUCTION_V1.json"],
            rebuild_required=False,
            status="BYTE_VALID — remains the accepted materialization",
            provenance_rebind="downstream authorization now references THIS amendment",
            why_no_rebuild="the diagnostic changed no engine, no port hash, no config and no "
                           "Massive input semantics, and no label was corrected toward "
                           "legacy — each checked above, not assumed"),

        authorizes=("downstream use of MASSIVE_T_FEATURE_PRODUCTION_V1 under the corrected "
                    "limitation" if ok else None),
        does_not_authorize=["Z", "the final T/Z analysis dictionary", "Y exposure",
                            "reconstructing the historical candidate population"],
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    dg = ART.seal(p, "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_2.json",
                  required=("amendment_id", "status", "decision_question", "invariants",
                            "reject_conditions", "measured_divergence", "new_limitation",
                            "production_disposition"),
                  supersede=os.path.exists(
                      "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_2.json"))
    print(f"MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_2 · {dg} · {status}")
    print(f"  question    does measured 15m divergence invalidate the port? "
          f"{p['decision_question']['answer']}")
    print(f"  invariants  {sum(i['holds'] for i in invariants)}/{len(invariants)} hold")
    print(f"  rejects     {sum(r['triggered'] for r in rejects)}/{len(rejects)} triggered")
    print(f"  divergence  {p['measured_divergence']['t_divergence']} on "
          f"{p['measured_divergence']['matched_bars']:,} bars")
    print(f"  framing     replication-like STRENGTHENED · population-equivalence FORBIDDEN")
    print(f"  production  rebuild_required = False · {A['MASSIVE_T_FEATURE_PRODUCTION_V1.json']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
