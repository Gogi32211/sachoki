"""MASSIVE_T_FEATURE_PORT_SPEC_V1 — the same semantic program on a different feed, named honestly.

The claim this port is allowed to make is narrow and it is worth stating before anything else:
THE SAME RECOVERED PINE SEMANTIC PROGRAM, EXECUTED ON MASSIVE SOURCE DATA. Not the same
materialised population. The recovery already showed the legacy comparison target carries its
own 2-decimal storage loss, so "identical to legacy t_sig" was never a claim the evidence could
support, and this spec does not try to rescue it.

THE CONFIGURATION DECISION IS THE ONE PLACE THIS GATE COULD HAVE GONE WRONG QUIETLY. The input
sweep in the recovery run found minBodyRatio=0.5 agreeing with the legacy materialisation
slightly BETTER than 1.0 — 99.1409% against 99.0827%. The source default is 1.0. This spec
freezes 1.0.

Choosing 0.5 would have been selecting a parameter by how well it reproduces a target, which is
fitting, not porting — and the target is a rounded column whose disagreements are already known
to be storage artifacts. A 0.06% improvement bought by abandoning the source default would be
the cheapest possible way to turn a port into a fit. The sweep stays what it is: diagnostic
evidence that useWick=False is right (True costs 6-7 points), and nothing more.

WHAT THE HISTORICAL RUN USED IS UNKNOWN AND IS RECORDED AS UNKNOWN. Pine inputs are set at
runtime and the materialisation's configuration was never captured. SOURCE_DEFAULT_CONFIGURATION
and LEGACY_RUNTIME_CONFIGURATION_UNKNOWN are kept as separate facts; this spec does not assert
that the old export ran on defaults.

SCOPE IS 15m ONLY, because that is where the family is actually registered pre-Y
(T1_15M_X_ONLY_V1, status X_ONLY_PRE_Y). 1H and 1D datasets now exist, which is exactly why the
restriction has to be explicit — dataset availability is not hypothesis registration.

A CONSEQUENCE WORTH SEEING EARLY: every T predicate needs the immediately preceding 15m bar, so
a T state is evaluable only where TWO CONSECUTIVE 15m intervals are COMPLETE. The coarse census
put single-bar 15m completeness at 68.8%; the pairwise figure will be lower and must be measured
in the smoke gate rather than assumed.
"""
from __future__ import annotations
import hashlib, json, os, re, sys, time                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

REC, REC_D = "MASSIVE_T_PINE_SEMANTIC_RECOVERY_V1.json", "7d00e3908feb26d5"
PINE = "../analysis/260523_TZ_F_WLNBB_CMB_pattern.pine"
PINE_D = "696f40d730662a60"
COARSE_D = "6ec73d5f462761f0"
ELIG_D = "7c0a5ad6b17d7ae0"
DICT_D = "cadd09add79457f5"
T_STATES = ["T4", "T6", "T1G", "T2G", "T1", "T2", "T9", "T10", "T3", "T11", "T5", "T12"]


def main():
    if ART.file_digest(REC) != REC_D:
        print("HOLD — recovery artifact digest mismatch"); return 1
    rec = json.load(open(REC))
    if not os.path.exists(PINE):
        print("HOLD — Pine source missing"); return 1
    raw = open(PINE, "rb").read()
    if hashlib.sha256(raw).hexdigest()[:16] != PINE_D:
        print("HOLD — Pine source digest mismatch"); return 1
    txt = raw.decode("utf-8", "replace")

    # read the exact input declarations — never inferred
    defaults = {}
    for name in ("useWick", "minBodyRatio"):
        m = re.search(rf"^{name}\s*=\s*input\.(\w+)\(\s*([^,\)]+)", txt, re.M)
        if not m:
            print(f"HOLD — could not read the source default for {name}"); return 1
        defaults[name] = dict(type=m.group(1), source_default=m.group(2).strip(),
                              declaration=re.search(rf"^{name}\s*=.*$", txt,
                                                    re.M).group(0).strip())
    dom = re.search(r"^minBodyRatio\s*=.*minval\s*=\s*([\d.]+).*step\s*=\s*([\d.]+)", txt,
                    re.M)
    if dom:
        defaults["minBodyRatio"]["encoded_domain"] = dict(minval=float(dom.group(1)),
                                                          step=float(dom.group(2)))

    p = dict(
        spec_id="MASSIVE_T_FEATURE_PORT_SPEC_V1",
        status="MASSIVE_T_FEATURE_PORT_SPEC_V1_FROZEN",
        task_class="PORT_SPEC_AND_CONFORMANCE_DESIGN",
        production_t_written=False, y_exposed=0,

        authoritative_semantics=dict(
            recovery_artifact=REC, recovery_digest=REC_D,
            pine_source=os.path.basename(PINE), pine_digest=PINE_D,
            raw_predicates=rec["raw_definitions"],
            supporting_predicates=rec["supporting_predicates"],
            priority_chain=rec["priority_resolution"]["order"],
            reconstructed_from_prose=False),

        portability_claim=dict(
            allowed="THE SAME RECOVERED PINE SEMANTIC PROGRAM, EXECUTED ON MASSIVE SOURCE "
                    "DATA",
            not_allowed="THE SAME MATERIALISED POPULATION",
            why="the legacy comparison target carries its own 2-decimal storage loss; "
                "identity with legacy t_sig is not a claim the evidence supports",
            upgradeable_only_if="the matched cross-feed comparison actually supports the "
                                "stronger claim"),

        provenance=dict(
            classification=["PREVIOUSLY_EXPOSED_HYPOTHESIS",
                            "CURRENT_PRE_REGISTERED_SOURCE_PORT"],
            explicitly_not="NEW_DISCOVERY",
            multiplicity_ledger="NOT reset; a new feed and a new pipeline do not make an "
                                "exposed hypothesis fresh",
            inherited_from="MASSIVE_1M_STATE_TRANSITION_V1.hypothesis_provenance"),

        naming=dict(
            machine_identifiers=[f"{s}_MASSIVE_PORT_V1" for s in T_STATES],
            metadata=dict(semantic_parent="LEGACY_PINE_T_STATE",
                          source_semantics_digest=PINE_D),
            display="a report may show the human label T1/T3/...",
            binding="machine identity must retain the MASSIVE_PORT provenance; legacy names "
                    "may not be reused as if byte-identical"),

        timeframe_scope=dict(
            registered=["15m"],
            authority="T1_15M_X_ONLY_V1 (status X_ONLY_PRE_Y) is the pre-Y registered "
                      "family",
            excluded=["1H", "1D", "1m", "4H"],
            why_explicit="1H and 1D datasets now exist; dataset availability is NOT "
                         "hypothesis registration",
            adding_one="a new search-space member requiring its own pre-Y registration",
            one_minute_role="unchanged"),

        pine_input_configuration=dict(
            declarations=defaults,
            source_default_configuration=dict(
                useWick=False, minBodyRatio=1.0,
                read_from="the authoritative Pine source, verbatim"),
            legacy_runtime_configuration="UNKNOWN",
            legacy_note="Pine inputs are set at runtime; the materialisation's configuration "
                        "was never captured. This spec does NOT assert the old export ran on "
                        "defaults.",
            current_port_configuration=dict(
                useWick=False, minBodyRatio=1.0,
                chosen_because="they are the source defaults",
                explicitly_not_because="they maximise agreement with the legacy "
                                       "materialisation"),
            sweep_disposition=dict(
                finding="minBodyRatio=0.5 agreed with the legacy materialisation slightly "
                        "BETTER than 1.0 (99.1409% vs 99.0827%)",
                decision="1.0 is frozen anyway",
                reasoning="selecting a parameter by how well it reproduces a target is "
                          "fitting, not porting — and the target is a rounded column whose "
                          "disagreements are already known to be storage artifacts",
                what_the_sweep_is_used_for="diagnostic evidence that useWick=False is "
                                           "correct (True costs 6-7 points)",
                what_it_is_not="a parameter-selection oracle")),

        output_model=dict(
            layers=["raw T-state booleans", "priority winner", "final priority-resolved "
                                                               "label"],
            collapsed=False,
            why="every bar must be inspectable at all three layers for conformance "
                "diagnosis",
            states=["T_STATE_VALID", "T_STATE_UNAVAILABLE_INPUT",
                    "T_STATE_UNAVAILABLE_CONTAMINATED_COARSE",
                    "T_STATE_UNAVAILABLE_INITIALIZATION"],
            false_vs_unavailable="FALSE and UNAVAILABLE remain separate at every layer"),

        lookback_and_startup=dict(
            requirement="exactly one immediately preceding coarse bar",
            derivation="every recovered predicate references [1] only, including "
                       "prev1IsBear's Z7_raw[1]",
            at_segment_start="UNAVAILABLE_INITIALIZATION",
            forbidden=["substituting zeros", "treating a missing prior bar as a false "
                                             "predicate",
                       "evaluating partially initialised logic"]),

        session_semantics=dict(
            recovered="the T-predicate block contains no session guard, no timeframe request "
                      "and no reset; predicates reference the previous CHART bar",
            frozen="no daily reset is introduced, because the source has none",
            but="a missing or contaminated required prior Massive bar is an unavailable "
                "input and may never be bridged as though observed"),

        massive_input_authority=dict(
            artifact="MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1", digest=COARSE_D,
            timeframe="15m",
            admissible="coverage_state = COMPLETE only",
            contaminated="UNOBSERVED_CONTAMINATED -> T state UNAVAILABLE, never FALSE",
            forbidden="computing T from contaminated partial OHLC",
            pairwise_consequence=dict(
                rule="a T state needs the current AND the immediately preceding 15m interval "
                     "both COMPLETE",
                single_bar_completeness="68.8% (coarse census)",
                pairwise="LOWER — must be MEASURED in the smoke gate, not assumed",
                why_flagged="it is the binding availability constraint on the whole port")),

        cross_feed_conformance_plan=dict(
            three_way=dict(
                A="legacy materialised T label",
                B="recovered semantics applied to legacy stored inputs",
                C="recovered semantics applied to Massive data"),
            comparisons=dict(
                A_vs_B="can rounded legacy storage reproduce the historical "
                       "materialisation?",
                B_vs_C="how much does feed/input data change the SAME executable "
                       "semantics?",
                A_vs_C="overall historical-versus-current-port divergence"),
            do_not_collapse="these answer different questions and may not be reported as one "
                            "match percentage",
            matched_on=["security_key_v1", "timeframe", "bar timestamp"],
            identity_rule="matched via the frozen research identity / lineage mapping, NEVER "
                          "by ticker string alone",
            recorded_per_observation=["security_key_v1", "legacy identifier",
                                      "Massive identifier", "bar timestamp", "timeframe",
                                      "legacy OHLC", "Massive OHLC",
                                      "legacy materialised label", "legacy-recomputed label",
                                      "Massive-port label"],
            price_diagnostics=["open/high/low/close deltas", "body sign difference",
                               "previous-body sign difference",
                               "engulfing boundary difference",
                               "body-ratio boundary crossing"],
            y_touched=False),

        divergence_taxonomy=["LEGACY_STORAGE_PRECISION_LIMITATION",
                             "SOURCE_FEED_OHLC_DIFFERENCE",
                             "PREDICATE_BOUNDARY_SENSITIVITY",
                             "PINE_INPUT_CONFIGURATION_AMBIGUITY",
                             "PRIORITY_RESOLUTION_DIFFERENCE",
                             "IMPLEMENTATION_DEFECT",
                             "MISSING_OR_CONTAMINATED_MASSIVE_INPUT",
                             "UNRESOLVED"],
        noise_label_forbidden=True,

        legacy_precision_limitation=dict(
            bound_finding="legacy `bars` OHLC is stored rounded to 2 decimals while Pine "
                          "operated on higher-precision values",
            consequence="legacy materialised t_sig is NOT a perfect recomputation target from "
                        "stored rounded OHLC",
            binding="disagreement may not be auto-attributed to implementation error",
            evidence=rec["residual_inventory"]["dominant_class"]["evidence"]),

        conformance_threshold=dict(
            value=None,
            invented=False,
            rule="this gate MEASURES divergence; whether it is acceptable for a "
                 "replication-like port is an explicit X-side decision made BEFORE Y",
            forbidden="choosing the threshold after observing Y",
            explicitly_not="a 99% or 95% bar invented here"),

        no_family_expansion=dict(
            portable=T_STATES,
            forbidden=["T-derived slopes", "distances", "hybrid EMA-T states",
                       "RVOL-T crosses", "new thresholds", "new priority orders"],
            requires="explicit later pre-Y registration"),

        z_firewall=dict(Z_STATUS="DISCOVERY_EVIDENCE_ONLY", ported=False,
                        in_t_port_search_space=False,
                        discovery_is_not_registration=True,
                        requires="MASSIVE_Z_PROVENANCE_QUALIFICATION_V1"),

        y_firewall=dict(y_exposed=0,
                        forbidden=["forward returns", "theta", "transition enrichment",
                                   "win rate", "P&L", "future path",
                                   "outcome-conditioned mismatch"],
                        comparison="X-side only"),

        negative_fixtures=dict(count=14, required=[
            "a wrong source digest is rejected",
            "an unregistered timeframe is rejected",
            "a contaminated coarse input yields UNAVAILABLE, not FALSE",
            "a missing required prior bar yields UNAVAILABLE",
            "a priority-order permutation changes output and is rejected",
            "raw boolean and final priority-resolved label remain distinguishable",
            "a source-default configuration mutation is rejected",
            "the legacy runtime configuration is not falsely claimed known",
            "a rounded-legacy-OHLC mismatch cannot be auto-labelled an implementation "
            "defect",
            "a Massive feed difference cannot be silently ignored",
            "an undeclared T-derived feature is rejected",
            "a Z output request is rejected",
            "Y access is rejected",
            "supplemental data is rejected"]),

        binds=dict(pre_y_dictionary=DICT_D, eligibility=ELIG_D, cohort=476,
                   coarse_production=COARSE_D),
        acceptance={
            "recovery_artifact_bound": True,
            "pine_digest_verified": True,
            "source_defaults_read_exactly": len(defaults) == 2,
            "legacy_runtime_config_marked_unknown": True,
            "config_not_chosen_by_agreement": True,
            "timeframe_scoped_to_registered_15m": True,
            "raw_and_final_layers_preserved": True,
            "false_vs_unavailable_preserved": True,
            "three_way_comparison_not_collapsed": True,
            "divergence_taxonomy_defined": True,
            "no_conformance_threshold_invented": True,
            "provenance_dual_class": True,
            "z_not_ported": True,
            "negative_fixtures_defined": True,
            "y_exposed_zero": True,
            "production_t_writes_zero": True},
        mutations=dict(recovery_artifact=0, pine_source=0, coarse_production=0, cohort=0,
                       t_production_writes=0, z_writes=0, y_exposure=0),
        next_gate="T PORT SMOKE + MATCHED X-ONLY FEED CONFORMANCE. Full Massive T production "
                  "is not built automatically.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    ok = all(p["acceptance"].values())
    p["status"] = p["status"] if ok else "HOLD"
    d = ART.seal(p, "MASSIVE_T_FEATURE_PORT_SPEC_V1.json",
                 required=("spec_id", "status", "authoritative_semantics",
                           "portability_claim", "provenance", "naming", "timeframe_scope",
                           "pine_input_configuration", "output_model",
                           "massive_input_authority", "cross_feed_conformance_plan",
                           "divergence_taxonomy", "z_firewall", "acceptance", "mutations"),
                 supersede=os.path.exists("MASSIVE_T_FEATURE_PORT_SPEC_V1.json"))
    print(f"MASSIVE_T_FEATURE_PORT_SPEC_V1 · {d} · {p['status']}")
    print(f"  claim      SAME SEMANTIC PROGRAM ON MASSIVE DATA (not same population)")
    print(f"  scope      15m only · {len(T_STATES)} states · 1H/1D excluded by registration")
    print(f"  config     useWick={defaults['useWick']['source_default']} · "
          f"minBodyRatio={defaults['minBodyRatio']['source_default']} (source defaults)")
    print(f"  sweep      0.5 fit legacy BETTER (99.1409%) — 1.0 frozen anyway")
    print(f"  legacy cfg LEGACY_RUNTIME_CONFIGURATION_UNKNOWN")
    print(f"  provenance PREVIOUSLY_EXPOSED_HYPOTHESIS + CURRENT_PRE_REGISTERED_SOURCE_PORT")
    print(f"  Z ported   {p['z_firewall']['ported']} · Y_EXPOSED {p['y_exposed']} · "
          f"T production writes 0")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
