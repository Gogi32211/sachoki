"""MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_CORRECTION_REBIND — decided on direct evidence.

Every earlier acceptability verdict rested partly on limitations that turned out to be false:
that no legacy 15m materialized T existed, that population equivalence was not estimable, that
source divergence was ~5.4%, and that an unexplained provenance residual sat under the port. All
four are now settled, three of them because they were my errors. So this recomputes rather than
carrying an ACCEPT forward.

WHAT IS DIFFERENT THIS TIME IS THE KIND OF EVIDENCE. The canonical-materializer claim is no
longer an inference from a daily call path: the canonical engine, on the true historical input
surface, reproduces the historical materialized 15m label surface EXACTLY on qualified support.
The one deviation is explained down to its mechanism. Source divergence is measured directly at
the research timeframe on the correct surface. Registered position-pair portability is measured.

THE VERDICT IS COMPUTED. Each requirement resolves from a sealed field and the category falls
out; a missing field fails rather than defaulting, and no outcome is consulted.

AND THE CLAIM STAYS A PORT. Portability of ~99.87% does not make the Massive population the
historical one. It remains a source and cohort port of a historically exposed hypothesis, and
population-equivalence language stays forbidden.
"""
from __future__ import annotations
import json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART

A = {
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json": "4c9b15eb8cfed786",
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1.json": "cdd94787f424b5cb",
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_2.json": "e8908bdf5d5d8a23",
    "MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1.json": "5bb9cdc3167e2ff7",
    "MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2_REQUALIFIED.json": "e70a777da6000394",
    "MASSIVE_T_LEGACY_15M_TRANSITION_BOUNDARY_INVESTIGATION_V1.json": "38e044124d4f21ad",
    "MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json": "5ebcad54f619f97d",
    "MASSIVE_T_LEGACY_15M_AUTHORITY_CORRECTION_V1.json": "24ac4562a96aa869",
    "MASSIVE_T_FEATURE_PRODUCTION_V1.json": "ccdb0e35490794da",
    "MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1.json": "2bc48ac417c2cbc9",
    "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json": "49eb9658eda72773",
}
PORT = "0ca86eeb1f8ee8fe"

bad = [f for f, w in A.items() if ART.file_digest(f) != w]
if bad:
    print("HOLD — digest mismatch:", bad); raise SystemExit(1)
dec = json.load(open("MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json"))
prov = json.load(open("MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1.json"))
v2 = json.load(open("MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2_REQUALIFIED.json"))
bnd = json.load(open("MASSIVE_T_LEGACY_15M_TRANSITION_BOUNDARY_INVESTIGATION_V1.json"))
inv = json.load(open("MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json"))
sm = json.load(open("MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json"))
prod = json.load(open("MASSIVE_T_FEATURE_PRODUCTION_V1.json"))
ver = json.load(open("MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1.json"))
ab = v2["invariant_A_equals_B"]; sp = v2["source_port_divergence"]

HARD = [
    ("canonical 15m materializer writer chain resolved",
     prov["status"] == "SAME_MATERIALIZER_CONFIRMED"),
    ("true historical input surface resolved",
     prov["upstream_ohlc_surface"].startswith("studio_15m_base")),
    ("A==B EXACT on qualified materializer support",
     ab["qualified_mismatches"] == 0 and ab["holds"] is True),
    ("every raw deviation has a sealed structural authority",
     v2["exclusion_guard"]["mismatches_without_authority"] == 0),
    ("the boundary deviation is explained mechanistically",
     bnd["status"] == "LOCALIZED_MATERIALIZATION_BOUNDARY_ANOMALY_CONFIRMED"),
    ("A<->C and B<->C are row-identical, as A==B requires",
     v2["invariant_AC_equals_BC"]["row_level_identical"] is True
     and v2["invariant_AC_equals_BC"]["confusion_identical"] is True),
    ("same-engine legacy<->Massive divergence measured at 15m",
     sp["agreement"] is not None),
    ("registered position-pair portability measured",
     len(v2["registered_position_pairs"]) == 3),
    ("evidence surface enumeration complete",
     inv["completeness_guard"]["enumeration_complete"] is True),
    ("Layer 1 same-input semantics EXACT",
     sm["layer_1_semantic_correctness"]["result"] == "EXACT"),
    ("port hash unchanged", sm["port_implementation_hash"][:16] == PORT
     and prod["port_implementation_hash"] == PORT),
    ("canonical config unchanged", sm["frozen_config_integrity"]["unchanged"] is True
     and prod["config_guard"]["unchanged"] is True),
    ("Massive production independently verified",
     ver["status"] == "MASSIVE_T_FEATURE_PRODUCTION_V1_PASS"),
    ("no label corrected toward legacy",
     prod["no_source_normalization"]["labels_corrected_from_1d"] is False
     and prod["no_source_normalization"]["rounded_to_2dp"] is False),
    ("no parameter fitted from conformance", prod.get("parameters_changed", 0) == 0),
    ("Y_EXPOSED = 0", sm["y_exposed"] == 0 and prod["y_exposed"] == 0 and v2["y_exposed"] == 0),
]
hard = [dict(requirement=k, met=bool(v)) for k, v in HARD]
ok = all(h["met"] for h in hard)
status = "ACCEPT_FOR_REPLICATION_LIKE_SOURCE_PORT" if ok else "HOLD_INSUFFICIENT_X_EVIDENCE"

p = dict(
    decision_id="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_CORRECTION_REBIND",
    status=status, verdict_computed_not_asserted=True,
    task_class="ACCEPTABILITY_REBIND_ONLY",
    amends=dict(decision=A["MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json"],
                amendment_1=A["MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1.json"],
                amendment_2=A["MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_2.json"],
                disposition="FROZEN — none edited"),
    why_recomputed="the earlier verdicts rested partly on limitations now known to be false",

    superseded_false_premises=[
        dict(premise="a legacy 15m materialized T population does not exist",
             verdict="FALSE", now="studio_15m.duckdb :: bars.t_sig, bar-grain, 12 labels"),
        dict(premise="15m population equivalence is NOT_ESTIMABLE",
             verdict="FALSE", now="measured directly at 15m"),
        dict(premise="15m source divergence is ~5.4461%",
             verdict="WRONG_INPUT_SURFACE",
             now=f"{round(1 - sp['agreement'], 6)} on the true lean-base surface"),
        dict(premise="an unexplained A<->B provenance residual sits under the port",
             verdict="RESOLVED",
             now="the residual was my wrong input surface; A==B is EXACT on qualified support"),
    ],

    direct_evidence=dict(
        materializer_writer_chain=prov["writer_chain"],
        A_equals_B=dict(qualified_support=ab["qualified_support"],
                        qualified_mismatches=ab["qualified_mismatches"],
                        raw_support=ab["raw_support"], raw_mismatches=ab["raw_mismatches"],
                        conformance=ab["semantic_materializer_conformance"]),
        boundary_exclusion=dict(rows=v2["exclusion_guard"]["excluded"],
                                authority=v2["exclusion_guard"]["authority"],
                                causal_not_blacklist=True),
        source_port_divergence=dict(support=sp["support"], agreement=sp["agreement"],
                                    divergence=sp["divergence"]),
        registered_pair_portability={k: v["pair_portability"]
                                     for k, v in v2["registered_position_pairs"].items()},
        by_position={k: v["A_vs_C"] for k, v in v2["by_position"].items()},
        layer_1="EXACT", production_verified=True),

    hard_requirements=hard, all_met=ok,

    allowed_claim=dict(
        text=("The Massive T port uses the same canonical 15m materializer semantics as the "
              "historical programme, verified EXACTLY against the historical materialized "
              "signal surface on its true input surface. On jointly usable legacy-versus-"
              "Massive support the resulting T-token population is highly portable, while "
              "remaining a source and cohort port rather than an identical historical-"
              "population reconstruction."),
        provenance=["PREVIOUSLY_EXPOSED_HYPOTHESIS", "CURRENT_PRE_REGISTERED_SOURCE_PORT",
                    "REPLICATION_LIKE"]),
    forbidden_claims=dec["forbidden_claims"] + [
        "identical historical population reconstruction",
        "availability-semantic equivalence with the legacy blank representation"],
    accepted_object=dec["accepted_object"],

    limitations_carried=dict(
        blank_semantics="legacy '' means NO_MATERIALIZED_T_LABEL, never EVALUABLE_NO_T_STATE",
        population_equivalence="portability is measured; identity is NOT claimed",
        live_store="the legacy enriched store is written by a nightly pipeline, so its "
                   "denominators drift between runs; the invariant held on both observed",
        boundary_rows="3 legacy rows excluded from materializer conformance only — they "
                      "remain in the Massive research population"),

    production_disposition=dict(artifact="MASSIVE_T_FEATURE_PRODUCTION_V1",
                                digest=A["MASSIVE_T_FEATURE_PRODUCTION_V1.json"],
                                rebuild_required=False,
                                status="BYTE_VALID / SEMANTICALLY_VERIFIED",
                                downstream_authorization=("RELEASED" if ok else "HOLD")),
    authorizes=("MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1 (C3)" if ok else None),
    does_not_authorize=["Z", "the pre-Y analysis dictionary", "Y exposure",
                        "reconstructing the historical candidate population"],
    y_exposed=0, outcome_exposure="NOT_EXPOSED",
    sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

d = ART.seal(p, "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_CORRECTION_REBIND.json",
             required=("decision_id", "status", "superseded_false_premises",
                       "direct_evidence", "hard_requirements", "allowed_claim",
                       "limitations_carried"),
             supersede=os.path.exists(
                 "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_CORRECTION_REBIND.json"))
print(f"MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_CORRECTION_REBIND · {d} · {status}")
print(f"  hard reqs   {sum(h['met'] for h in hard)}/{len(hard)} met")
print(f"  A==B        {ab['qualified_mismatches']} / {ab['qualified_support']:,} -> "
      f"{ab['semantic_materializer_conformance']}")
print(f"  divergence  {sp['divergence']} (agreement {sp['agreement']})")
print(f"  pairs       {p['direct_evidence']['registered_pair_portability']}")
print(f"  4 false premises superseded · production rebuild_required = False")
print(f"  authorizes  {p['authorizes']}")
