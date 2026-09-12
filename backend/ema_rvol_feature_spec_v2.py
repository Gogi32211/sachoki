"""MASSIVE_EMA_RVOL_FEATURE_SPEC_V2 — EMA resolved and ready; RVOL held for the reason the gate named.

The EMA half is complete. The V1 conflict is resolved the clean way: EMA is defined on the
registered coarse parents that now exist as production data, one_minute_role is untouched, and
the family is frozen at 9/20/50/200 across 15m/1H/1D — twelve streams, declared before Y.

The RVOL half cannot be frozen, and the gate itself said so in advance: "If any required RVOL
parameter is not explicit in the frozen authority: HOLD. Do not invent it."

I checked every sealed artifact rather than assuming. What IS frozen is real and useful —
family names, "divide by a per-slot baseline", exact baseline MEMBERSHIP (OBSERVED and
STRUCTURAL_NO_TRADE enter, UNOBSERVED excluded from both numerator and denominator), the
cumulative-curve contamination flag, and the requirement that every value carry its
excluded-minute count. What is absent is everything needed to actually compute a number: how
many prior sessions form the baseline, whether it is a mean or a median, the minimum sample
count, whether the current session is excluded, and how CUM-RVOL's denominator is built.

That absence is deliberate upstream, not an oversight. The charter says feature_families is
"NAMED ONLY — the dictionary that defines them ... is a later step and is not frozen here", and
lists FEATURE / CLOCK DICTIONARY under not_yet_frozen. So these parameters were always going to
be chosen here; the gate simply forbids choosing them silently, and it is right to.

ONE GAP IS SHARPER THAN THE REST AND WOULD HAVE BEEN EASY TO MISS. An early-close session has
210 slots, a normal one 390. Nothing frozen says whether a normal session's slot-100 baseline
may include early-close sessions' slot-100 observations. Volume shape near a 13:00 close is not
the shape near the same clock time on a full day, so the choice materially changes every RVOL
value on the ten early-close sessions and, if pooled, contaminates the baseline for all the
others too.

AND A SECOND: the probe spec flags first-slot and last-slot RVOL as containing an unattributed
auction print, while the close-boundary amendment records that the auction reading is
"plausible and undemonstrated". So the two most economically interesting slots of the session
carry a semantic that the programme has explicitly declined to assert. A slot-RVOL definition
has to say what it does there.
"""
from __future__ import annotations
import json, os, sys, time                                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

BIND = {
    "MASSIVE_1M_STATE_TRANSITION_V1": "d73a88d836f36eea",
    "MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1": "5591685cfa33b62a",
    "MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1": "51540a1cefe2ec2b",
    "MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1": "da30cf75b791a6cb",
    "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1": "6ec73d5f462761f0",
    "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1": "7c0a5ad6b17d7ae0",
    "MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1": "8aaef7196c420136",
    "MASSIVE_1M_EMA_RVOL_FEATURE_SPEC_V1": "da7249fc5e362b9f",
    "MASSIVE_1M_PROBE_SPEC_V1": None,
    "MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1": None,
}
PERIODS = [9, 20, 50, 200]
TFS = ["15m", "1H", "1D"]


def main():
    binding, bad = {}, []
    for n, want in BIND.items():
        got = ART.file_digest(n + ".json")
        binding[n] = dict(cited=want, actual=got, match=(want is None or got == want))
        if want and got != want:
            bad.append(n)
    if bad:
        print("HOLD — digest mismatch:", bad); return 1
    charter = json.load(open("MASSIVE_1M_STATE_TRANSITION_V1.json"))
    dca = json.load(open("MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.json"))
    cap = json.load(open("MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1.json"))
    prod = json.load(open("MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json"))

    ema = dict(
        status="READY_TO_FREEZE",
        registry=dict(
            periods=PERIODS, classification="NEWLY_PRE_REGISTERED_BEFORE_Y",
            frozen_while_y_exposed_is_zero=True,
            rationale_without_reference_to_Y=[
                "the compact EMA family already used in prior coarse-timeframe T/Z logic",
                "preserves continuity with prior programme logic",
                "avoids expanding into additional legacy EMA families",
                "fixed before any outcome exposure"],
            explicitly_not_because="frequency of occurrence in the legacy codebase",
            excluded_periods=[8, 13, 21, 34, 55, 89],
            legacy_ribbon=dict(periods=[8, 13, 21, 34, 55, 89, 200],
                               classification="LEGACY_REFERENCE_ONLY",
                               importable=False),
            post_Y_expansion="forbidden without a new registered search family, its own "
                             "multiplicity treatment and fresh/forward confirmation"),
        timeframe_registry=dict(
            timeframes=TFS, source="MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1",
            uniform_across_tfs=True,
            no_per_tf_period_selection="availability differences are REPORTED, never "
                                       "optimised away",
            streams=len(PERIODS) * len(TFS)),
        search_space_declaration=dict(
            raw_streams=len(PERIODS) * len(TFS),
            enumeration=[f"EMA{p}@{tf}" for tf in TFS for p in PERIODS],
            frozen_as_provenance=True),
        input_eligibility=dict(
            consumes_only="coarse observations with coverage_state = COMPLETE",
            forbidden=["UNOBSERVED_CONTAMINATED", "INELIGIBLE",
                       "partial descriptive aggregate", "extended-hours input",
                       "close-boundary input", "supplemental input", "synthetic input"]),
        contamination_tolerance=dict(
            value="NONE", configurable_parameter=False, defaulted=False,
            forbidden=["allow N missing minutes", "allow X% missing", "90% completeness",
                       "95% completeness"],
            rule="a coarse interval containing any required UNOBSERVED constituent minute "
                 "is not EMA input"),
        segmentation=dict(
            grain="security_key_v1 x timeframe x period",
            segment="a contiguous run of COMPLETE coarse bars",
            break_on=["UNOBSERVED_CONTAMINATED", "feature-ineligible interval"],
            after_break="the next COMPLETE bar begins a NEW segment",
            forbidden=["bridging the gap", "forward-filling EMA",
                       "carrying pre-gap EMA state into the new segment"],
            overnight="a normal transition between eligible sessions does NOT itself break "
                      "a segment"),
        formula=dict(
            alpha="2 / (p + 1)",
            seed="EMA_1 = close_1 at the first COMPLETE bar of a new segment",
            recurrence="EMA_t = alpha * close_t + (1 - alpha) * EMA_(t-1), within the same "
                       "valid segment only"),
        emitted_fields=["ema_value", "ema_period", "ema_timeframe", "ema_segment_id",
                        "ema_age_valid_bars", "ema_validity_state", "ema_reset_reason"],
        warm_up_validity=dict(
            rule={"1 <= ema_age_valid_bars < period": "INITIALIZATION_SENSITIVE",
                  "ema_age_valid_bars >= period": "VALID",
                  "contaminated / ineligible interval": "UNAVAILABLE"},
            example="EMA200 requires 200 consecutive COMPLETE coarse bars before becoming "
                    "VALID",
            not_loosened_for_low_coverage=True,
            is_feature_validity_not_cohort_membership=True),
        causal_availability=dict(
            rule="EMA[t] inherits the information-availability timestamp of its input coarse "
                 "bar t",
            consequence="EMA on a 15m bar is unavailable before that 15m bar completes; 1H "
                        "and 1D behave analogously",
            backward_projection_forbidden="a completed coarse value may not be projected "
                                          "back into its constituent 1m minutes",
            as_a_clock="the earliest eligible 1m timestamp is at or after the frozen coarse "
                       "information-availability timestamp"),
        no_derived_search_features=dict(
            permitted="the 12 EMA series plus validity metadata",
            forbidden=["EMA slope", "EMA acceleration", "distance-to-EMA thresholds",
                       "percent-distance buckets", "EMA spread thresholds",
                       "EMA ordering states", "cross signals", "reclaim thresholds",
                       "touch thresholds", "cluster thresholds"],
            why="feature production must not silently become search-space construction"),
        availability_expectation=dict(
            source="MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1 / capability census",
            bound_as="pre-Y design provenance",
            facts=dict(
                coarse_complete_share={tf: prod["by_timeframe"][tf]["complete_share"]
                                       for tf in TFS},
                daily_longest_complete_run_median=prod["daily_report"][
                    "longest_complete_daily_run"]["median"],
                securities_with_complete_daily_run=prod["daily_report"][
                    "securities_with_a_complete_run_of_at_least"]),
            binding="these justify transparent availability accounting. They do NOT change "
                    "the EMA registry.",
            ema200_not_dropped=dict(
                decision="EMA200 stays in the registry although only 72 of 476 securities "
                         "have any 200-bar COMPLETE daily run",
                why="that is an AVAILABILITY problem, not a parameter-selection problem. "
                    "Dropping it because capability is low would let X-side capability "
                    "reshape the hypothesis family — technically pre-Y, but unnecessary.",
                instead="the definition is fixed, VALID wherever age >= 200, UNAVAILABLE "
                        "elsewhere, and availability is reported exactly",
                also_forbidden="loosening contamination semantics to raise its "
                               "availability")))

    rvol_frozen = [
        dict(item="family names",
             source="MASSIVE_1M_STATE_TRANSITION_V1.feature_families",
             value=["slot-RVOL", "CUM-RVOL"]),
        dict(item="baseline shape",
             source="charter.aggregation_contract.sparse_minutes.why_it_matters",
             value="slot-RVOL and CUM-RVOL divide by a PER-SLOT baseline"),
        dict(item="baseline membership",
             source="MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.rvol_consequence.slot_rvol",
             value=dca["rvol_consequence"]["slot_rvol"]),
        dict(item="cumulative contamination flag",
             source="MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.rvol_consequence.cum_rvol",
             value=dca["rvol_consequence"]["cum_rvol"]),
        dict(item="mandatory reporting",
             source="MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.rvol_consequence.reported",
             value=dca["rvol_consequence"]["reported"]),
        dict(item="auction contamination flag",
             source="MASSIVE_1M_PROBE_SPEC_V1",
             value="first-slot and last-slot RVOL are flagged as containing an "
                   "unattributed auction print"),
        dict(item="UNOBSERVED is never zero volume",
             source="MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.states.UNOBSERVED",
             value=dca["states"]["UNOBSERVED"]["explicitly_not_zero"]),
    ]
    rvol_missing = [
        dict(parameter="baseline lookback length",
             question="how many prior eligible sessions form the per-slot baseline"),
        dict(parameter="baseline statistic",
             question="mean, median, or trimmed mean of the per-slot observations"),
        dict(parameter="minimum sample count",
             question="how many valid per-slot observations are required before a value is "
                      "emitted rather than UNAVAILABLE"),
        dict(parameter="current-session exclusion",
             question="prior-only is implied by 'baseline' but is nowhere stated explicitly"),
        dict(parameter="rolling vs expanding baseline",
             question="fixed-length trailing window or all prior eligible history"),
        dict(parameter="slot granularity",
             question="is a slot one minute, or a coarse interval position"),
        dict(parameter="cross-session-type pooling",
             severity="SHARPEST GAP",
             question="an early-close session has 210 slots and a normal one 390. May a "
                      "normal session's slot-100 baseline include early-close sessions' "
                      "slot-100 observations?",
             why_it_matters="volume shape near a 13:00 close is not the shape at the same "
                            "clock time on a full day. Pooling changes every RVOL value on "
                            "the ten early-close sessions and contaminates the baseline for "
                            "all the others too."),
        dict(parameter="CUM-RVOL denominator construction",
             question="how the cumulative baseline at a given session position is built"),
        dict(parameter="first/last-slot auction handling",
             severity="SEMANTIC, NOT NUMERIC",
             question="the probe spec flags these slots as containing an unattributed "
                      "auction print, and MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1 records "
                      "that the auction reading is 'plausible and undemonstrated'. A "
                      "slot-RVOL definition must state what it does at the two most "
                      "economically interesting slots of the session."),
    ]

    rvol = dict(
        status="BLOCKED — parameters are not present in the frozen authority",
        permitted_families=["slot-RVOL", "CUM-RVOL"],
        classification="CURRENT_PROGRAM_PRE_REGISTERED (families) / "
                       "PARAMETERS_NOT_FROZEN",
        frozen_and_binding=rvol_frozen,
        not_frozen_anywhere=rvol_missing,
        verified_by="a search of every sealed artifact for a lookback, sample requirement or "
                    "baseline statistic returned nothing",
        upstream_intent=dict(
            feature_families_status=charter["feature_families_status"],
            not_yet_frozen=charter["not_yet_frozen"],
            reading="the absence is deliberate upstream, not an oversight — these parameters "
                    "were always going to be chosen in the feature dictionary. The gate "
                    "forbids choosing them silently, which is the right instruction."),
        gate_instruction_applied="If any required RVOL parameter is not explicit in the "
                                 "frozen authority: HOLD. Do not invent it.",
        legacy_exclusions=dict(
            volume_over_rolling_20_bar_average="LEGACY_REFERENCE_ONLY",
            legacy_thresholds=["1.4x", "1.8x", "2.0x"],
            thresholds_classification="LEGACY_REFERENCE_ONLY",
            importable=False,
            why="occurrence in historical code is not pre-registration"),
        thresholds=dict(feature_layer_emits="continuous quantities plus validity metadata "
                                            "only",
                        threshold_search=False, inferred_from_legacy_code=False),
        causality_when_defined=dict(
            no_future_information=["future session information", "future minute",
                                   "future cumulative volume", "future outcome"],
            availability="CLOSE_OF_BAR_T",
            prior_only_baseline="the current observation/session must not leak into a "
                                "prior-only baseline unless the frozen definition explicitly "
                                "says otherwise"))

    schemas = dict(
        outputs=["coarse_ema_features", "rvol_clock_features", "feature_validity"],
        additive=True,
        must_not_mutate=["1m derived base", "coarse production", "raw archive"],
        identity="security_key_v1; ticker is metadata only",
        provenance_level="FILE_LEVEL")

    negatives = [
        "a 1m-native EMA request fails",
        "an unregistered EMA period fails",
        "an unregistered timeframe fails",
        "a contaminated coarse bar cannot enter EMA",
        "a contaminated gap breaks the EMA segment",
        "pre-gap EMA state cannot survive the break",
        "EMA cannot be forward-filled across an unavailable interval",
        "EMA cannot become VALID before age >= period",
        "a future coarse close cannot affect EMA[t]",
        "a coarse EMA cannot become visible before coarse bar completion",
        "extended / close-boundary / WI / NOT_YET / supplemental inputs cannot affect EMA",
        "an UNOBSERVED RVOL minute cannot become a zero-volume input",
        "future volume cannot affect RVOL",
        "an invalid RVOL baseline cannot silently lower its sample requirement",
        "feature invalidity cannot remove a security from the cohort",
        "a legacy EMA/ribbon period outside the registry cannot enter production",
        "a legacy RVOL threshold cannot appear automatically",
    ]

    acceptance = {
        "charter_unchanged": binding["MASSIVE_1M_STATE_TRANSITION_V1"]["match"],
        "one_minute_role_unchanged": True,
        "cohort_476_unchanged": True,
        "coarse_production_pass_bound": binding[
            "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1"]["match"],
        "ema_registry_frozen_before_Y": True,
        "no_additional_ema_periods": True,
        "timeframes_15m_1H_1D_only": True,
        "contamination_tolerance_none": True,
        "complete_only_ema_inputs": True,
        "segment_break_on_contaminated": True,
        "age_ge_period_required_for_valid": True,
        "rvol_definitions_copied_exactly_from_frozen_authority": False,
        "no_legacy_rvol_thresholds_imported": True,
        "feature_validity_distinct_from_cohort": True,
        "negative_fixtures_frozen": True,
        "y_exposed_zero": True,
        "feature_writes_zero": True,
    }

    p = dict(
        spec_id="MASSIVE_EMA_RVOL_FEATURE_SPEC_V2",
        status="HOLD",
        verdict_reason="EMA is fully specified and ready to freeze. RVOL cannot be frozen "
                       "because its parameters are not present in the frozen authority — "
                       "the exact condition the gate instructed me to HOLD on.",
        task_class="SPECIFICATION_ONLY",
        authoritative_inputs=binding,
        prior_spec=dict(
            artifact="MASSIVE_1M_EMA_RVOL_FEATURE_SPEC_V1",
            digest="da7249fc5e362b9f", disposition="IMMUTABLE HOLD — not edited",
            why_it_held="it correctly detected the charter conflict with 1m-native EMA",
            how_v2_resolves_it="EMA is assigned to registered coarse-TF parents that now "
                               "exist as production data; the charter is untouched"),
        one_minute_role=dict(unchanged=True, amended=False,
                             forbidden=charter["one_minute_role"]["forbidden"],
                             rvol_permission="only to the extent already authorised by the "
                                             "frozen charter as a session/coarse-state "
                                             "clock; not broadened here"),
        ema=ema,
        rvol=rvol,
        feature_schemas=schemas,
        feature_validity_vs_cohort=dict(
            cohort=476,
            rule="a security remains one of the frozen 476 even when EMA is unavailable, "
                 "initialization-sensitive, or RVOL is unavailable/contaminated",
            forbidden="constructing a cleaner security cohort from feature availability",
            downstream="analysis must report observation-level availability"),
        negative_fixtures=dict(count=len(negatives), required=negatives,
                               standard="each must show the dangerous version FAILS and the "
                                        "correct version PASSES"),
        capability_plan=dict(
            label_free=True, runs_before="full feature production",
            per_ema_stream=["candidate COMPLETE inputs", "INITIALIZATION_SENSITIVE outputs",
                            "VALID outputs", "UNAVAILABLE outputs", "segment count",
                            "segment-length distribution", "reset count",
                            "share of coarse observations with VALID EMA",
                            "securities with any VALID EMA"],
            rvol="the same, once its definition exists",
            y=0),
        y_firewall=dict(y_exposed=0, forbidden=["future returns", "T outcomes",
                                                "Z outcomes", "P&L", "win rate",
                                                "transition enrichment",
                                                "outcome-conditioned availability"],
                        capability="X-side only"),
        acceptance=acceptance,
        failed_acceptance=[k for k, v in acceptance.items() if not v],
        mutations=dict(charter=0, cohort=0, coarse_production=0, derived_base=0,
                       feature_writes=0, y_exposure=0),
        feature_production_writes=0,
        decision_required=dict(
            summary="pre-register the nine missing RVOL parameters while Y_EXPOSED = 0",
            count=len(rvol_missing),
            items=[m["parameter"] for m in rvol_missing],
            sharpest=["cross-session-type pooling", "first/last-slot auction handling"],
            options=[
                dict(id="A", name="freeze EMA now, RVOL in a short follow-up amendment",
                     effect="the EMA half becomes buildable immediately; slot/CUM RVOL "
                            "follows once its nine parameters are declared",
                     recommended=True),
                dict(id="B", name="hold both until RVOL is declared",
                     effect="one artifact, one freeze, but the EMA layer waits",
                     recommended=False)]),
        next_step_if_resolved="EMA/RVOL FEATURE SMOKE + LABEL-FREE CAPABILITY. Full feature "
                              "production is not started automatically.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_EMA_RVOL_FEATURE_SPEC_V2.json",
                 required=("spec_id", "status", "authoritative_inputs", "ema", "rvol",
                           "negative_fixtures", "acceptance", "mutations",
                           "decision_required"),
                 supersede=os.path.exists("MASSIVE_EMA_RVOL_FEATURE_SPEC_V2.json"))
    print(f"MASSIVE_EMA_RVOL_FEATURE_SPEC_V2 · {d} · {p['status']}")
    print(f"  EMA  READY_TO_FREEZE · {PERIODS} x {TFS} = "
          f"{len(PERIODS)*len(TFS)} streams · tolerance NONE · age>=period for VALID")
    print(f"  RVOL BLOCKED · {len(rvol_frozen)} items frozen · "
          f"{len(rvol_missing)} parameters absent from the authority")
    print(f"  failed acceptance: {p['failed_acceptance']}")
    print(f"  Y_EXPOSED 0 · feature writes 0")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
