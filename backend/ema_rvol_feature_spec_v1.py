"""MASSIVE_1M_EMA_RVOL_FEATURE_SPEC_V1 — parameter provenance resolved, and one blocking conflict.

The gate asked me to inventory legacy EMA/RVOL definitions, pick the authoritative ones, and
freeze them while Y_EXPOSED = 0. The inventory is done and it resolved cleanly. But reading the
frozen charter to check whether it already fixed parameters surfaced a clause the gate could not
have known about, and that clause makes part of this gate unfreezable as specified.

    MASSIVE_1M_STATE_TRANSITION_V1 · one_minute_role · enforcement:
      "the FEATURE / CLOCK DICTIONARY must name, for every 1m-derived quantity, the coarser-TF
       definition it measures. A 1m quantity with no coarse-TF parent is inadmissible by
       construction."

    forbidden: "searching for thresholds, patterns or rules AT 1m granularity",
               "promoting any 1m-native construct to a signal"

An EMA over 1-minute closes with a 20-minute span has no coarse-timeframe parent. It is not a
measurement of a 15m/1H/1D object; it is a new 1m-native object. Under the charter it is
inadmissible by construction — not disfavoured, inadmissible. Freezing 1m EMA periods here
would silently overrule a sealed charter, and the stated rationale for that charter clause is
the multiplicity burden of searching ~10^9 bars, which is exactly the failure this programme
has spent every gate avoiding.

RVOL IS NOT BLOCKED. slot-RVOL and CUM-RVOL are named charter families whose missing-data
semantics are ALREADY frozen in MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1 — UNOBSERVED minutes
excluded from both numerator and denominator, STRUCTURAL_NO_TRADE included, every RVOL value
reporting its excluded-minute count. Their coarse parent is the session-level volume state, and
they measure when within the session that state is reached. That is precisely the permitted use
of 1m.

So this artifact freezes everything that can honestly be frozen — the parameter registry, the
causal contract, the missing-data semantics, validity states, schemas, negative fixtures — and
returns HOLD on the one thing it cannot: EMA periods on a 1m base. The decision is the user's
and there are exactly two clean ways to take it, both recorded below. Converting this into a
PASS by quietly redefining "coarse-TF parent" would be the kind of move the whole programme is
built to prevent.
"""
from __future__ import annotations
import json, os, sys, time                                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

BIND = {
    "MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1": "5591685cfa33b62a",
    "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2": "3af8cc42370fda86",
    "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_AMENDMENT_1": "317759f7fc843ccd",
    "MASSIVE_1M_DERIVED_BASE_SMOKE_V2": "bce44a9000ca3a5f",
    "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1": "7c0a5ad6b17d7ae0",
    "MASSIVE_1M_STATE_TRANSITION_V1": None,
    "MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1": None,
    "MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1": "3eb4e01940b62e92",
}


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
    prod = json.load(open("MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1.json"))

    registry = [
        dict(family="EMA", parameters=[9, 20, 50, 200],
             classification="CURRENT_PROGRAM_CANDIDATE_BLOCKED",
             evidence="the dominant EMA family in this codebase by a wide margin — e9 164, "
                      "e200 148, e20 137, e50 122 occurrences; the ribbon periods are "
                      "vestigial beside them",
             existing_coarse_parents="already defined at 15m / 1H / 4H by the MTF EMA stack "
                                     "work, so this family DOES have coarse-TF parents",
             blocked_because="a 1m-native EMA of these periods would still have no coarse-TF "
                             "parent; only the coarse-TF definitions are admissible",
             admissible_form="EMA defined at 15m / 1H / 1D per the charter's "
                             "aggregation_contract, with 1m used to time when the coarse "
                             "condition first became true"),
        dict(family="EMA_RIBBON", parameters=[8, 13, 21, 34, 55, 89, 200],
             classification="LEGACY_REFERENCE_ONLY",
             evidence="near-vestigial in code — EMA89 8, ewm(span=89) 3, ewm(span=34) 2, "
                      "EMA21 2, ewm(span=8) 1",
             origin="TZ_WLNBB Pine-script era",
             not_carried_forward="not pre-registered for this programme; using it later "
                                 "requires a new hypothesis family with its own multiplicity "
                                 "treatment"),
        dict(family="VOLUME_20_BAR_AVERAGE", parameters=["volume / rolling(20).mean"],
             classification="LEGACY_REFERENCE_ONLY",
             evidence="rolling(20).mean 29 occurrences, vol_ratio 32",
             superseded_by="slot-RVOL and CUM-RVOL, which are charter-named families whose "
                           "baseline membership is already frozen",
             why="a flat 20-bar mean ignores minute-of-session shape entirely, and the "
                 "charter's own rationale for slot baselines is exactly that"),
        dict(family="SLOT_RVOL", parameters=["per-slot baseline"],
             classification="CURRENT_PROGRAM_PRE_REGISTERED",
             source="MASSIVE_1M_STATE_TRANSITION_V1.feature_families",
             semantics_already_frozen_in="MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1",
             frozen_rule=dca["rvol_consequence"]["slot_rvol"],
             coarse_parent="session-level volume state; 1m measures WHEN within the session "
                           "it is reached",
             blocked=False),
        dict(family="CUM_RVOL", parameters=["cumulative session curve"],
             classification="CURRENT_PROGRAM_PRE_REGISTERED",
             source="MASSIVE_1M_STATE_TRANSITION_V1.feature_families",
             frozen_rule=dca["rvol_consequence"]["cum_rvol"],
             coarse_parent="session-level cumulative volume state",
             blocked=False),
    ]

    conflict = dict(
        severity="BLOCKING — this is why the verdict is HOLD",
        discovered="while checking whether the charter already fixed parameters, as the gate "
                   "instructed",
        charter="MASSIVE_1M_STATE_TRANSITION_V1",
        charter_digest=binding["MASSIVE_1M_STATE_TRANSITION_V1"]["actual"],
        clause="one_minute_role",
        quoted_enforcement=charter["one_minute_role"]["enforcement"],
        quoted_forbidden=charter["one_minute_role"]["forbidden"],
        quoted_rationale=charter["one_minute_role"]["rationale"],
        consequence="an EMA over 1-minute closes with an N-minute span has no coarse-TF "
                    "parent. It is not a measurement of a 15m/1H/1D object; it is a new "
                    "1m-native object, and the charter calls that inadmissible by "
                    "construction.",
        why_not_overruled_here="freezing 1m EMA periods would silently overrule a sealed "
                               "charter, and the charter's stated reason for the clause is "
                               "the multiplicity burden of searching ~10^9 bars — the exact "
                               "failure this programme exists to prevent",
        also_relevant=dict(
            feature_families_status=charter["feature_families_status"],
            not_yet_frozen=charter["not_yet_frozen"],
            note="the FEATURE / CLOCK DICTIONARY is itself listed as not yet frozen, and "
                 "this gate is a legitimate place to write it — but it must obey "
                 "one_minute_role while doing so"),
        prerequisite_missing=dict(
            what="the 1m -> 15m / 1H / 1D aggregation layer",
            why="Builder Spec V2 deliberately set higher_timeframe_rule.aggregated_in_v2 = "
                "false, so no coarse bars exist yet for a coarse EMA to be defined on",
            already_specified_by="the charter's aggregation_contract, which fixes the "
                                 "hierarchy, left-closed left-labelled boundaries, "
                                 "session-aligned 1H with a flagged 30-minute stub, and the "
                                 "UNOBSERVED_CONTAMINATED propagation rule",
            so="the aggregation gate is specified but not built"),
        options=[
            dict(id="A", recommended=True,
                 name="build the coarse aggregation layer first, then define EMA there",
                 effect="EMA is defined at 15m/1H/1D from the same 1m atoms; the 1m layer "
                        "times when a coarse EMA condition first became true",
                 charter_amendment_required=False,
                 cost="one additional build gate before features",
                 why_recommended="it is what the charter already contemplates, the "
                                 "aggregation contract is already frozen in detail, and it "
                                 "keeps the countable inferential claims at the coarse layer "
                                 "where multiplicity can be deflated honestly"),
            dict(id="B", recommended=False,
                 name="amend one_minute_role to admit a named 1m-native EMA family",
                 effect="1m EMA periods become admissible as an explicitly registered "
                        "hypothesis family",
                 charter_amendment_required=True,
                 cost="the new family carries its own multiplicity and provenance burden, "
                      "and the ~10^9-bar search-space rationale must be answered rather than "
                      "bypassed",
                 why_not_recommended="it reopens the constraint that keeps this programme's "
                                     "claim count countable; it should be a deliberate "
                                     "decision, never a side effect of a feature spec")],
        not_blocking=["SLOT_RVOL", "CUM_RVOL"],
        rvol_may_proceed="both RVOL families have session-level coarse parents and already "
                         "have their missing-data semantics frozen; nothing in this conflict "
                         "prevents them")

    causal = dict(
        feature_information_cutoff="CLOSE_OF_BAR_T",
        rule="a feature value at regular minute t may use only information available through "
             "the close of minute t",
        never=["t+1 price", "t+1 volume", "future session volume", "future daily totals",
               "future realized volatility", "future membership/reference state"],
        observability="a state using feature[t] is known only AFTER bar t completes",
        earliest_execution_interpretation="NEXT ACTIONABLE MARKET OPPORTUNITY",
        forbidden_shortcut="treating a close-derived feature as executable at the same bar's "
                           "open, or at a price already used to construct it",
        charter_alignment=charter["phase_structure"]["leakage_rule"])

    inputs = dict(
        permitted=["observed_regular_bars within REGULAR_WAY_EXPECTED"],
        forbidden=["SESSION_CLOSE_BOUNDARY_BAR", "EXTENDED_HOURS_BAR",
                   "WHEN_ISSUED_EXCLUDED", "NOT_YET_REGULAR_WAY", "synthetic bars",
                   "supplemental / new-vintage bars"],
        source_policy="ORIGINAL_VINTAGE_ONLY",
        derived_base=dict(artifact="MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1",
                          digest=BIND["MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1"],
                          observed_regular_bars=prod["dataset_rows"][
                              "observed_regular_bars"],
                          unobserved_minutes=prod["coverage_report"][
                              "unobserved_regular_minutes"]))

    missing = dict(
        unobserved_count=prod["coverage_report"]["unobserved_regular_minutes"],
        never_means=["zero volume", "unchanged price", "no trade"],
        forbidden=["skip the minute and pretend time was contiguous",
                   "carry OHLCV through it", "insert a zero-volume candle",
                   "forward-fill close", "interpolate price"],
        observed_zero_volume_vs_unobserved=dict(
            observed_zero="an actual archived vendor bar with volume 0 is an OBSERVED "
                          "zero-volume input",
            unobserved="no source bar for an expected minute",
            must_remain_distinct_throughout=["EMA", "RVOL"]),
        structural_no_trade=dict(
            capability="UNAVAILABLE",
            consequence="the charter's STRUCTURAL_NO_TRADE state exists and enters RVOL "
                        "baselines, but this programme cannot yet prove its preconditions, "
                        "so no minute may be promoted into it",
            baseline_effect="in practice the slot-RVOL baseline is computed over OBSERVED "
                            "minutes only, and the excluded-minute count must be reported "
                            "alongside every value"))

    ema_contract = dict(
        status="DRAFTED_BUT_NOT_FROZEN — blocked by the one_minute_role conflict",
        alpha="2 / (p + 1)",
        time_axis="the ordered sequence of EXPECTED REGULAR minutes for one security across "
                  "eligible sessions",
        overnight="non-session time is NOT an expected-minute gap; 15:59 and the next "
                  "eligible 09:30 may remain consecutive in the regular-market stream",
        session_policy_default="CARRY_ACROSS_ELIGIBLE_SESSIONS",
        session_policy_note="frozen explicitly rather than left implicit; if the T/Z "
                            "semantics later require a daily reset, that becomes the "
                            "authoritative rule",
        break_rule="an expected REGULAR_WAY minute marked UNOBSERVED BREAKS input "
                   "continuity; the next observed minute begins a NEW segment",
        no_silent_bridging=True,
        emitted_fields=["ema_value", "ema_period", "ema_segment_id",
                        "ema_age_observed_bars", "ema_reset_reason"],
        warm_up="no universal threshold is invented here; ema_age_observed_bars is preserved "
                "so a downstream contract can require an explicitly pre-registered minimum "
                "age",
        at_unobserved_minute="ema_value = UNAVAILABLE, never carried forward",
        validity_states=["VALID", "INITIALIZATION_SENSITIVE", "UNAVAILABLE_INPUT",
                         "INELIGIBLE"],
        null_policy="unavailable EMA is never a bare NULL; it carries a reason")

    rvol_contract = dict(
        status="READY_TO_FREEZE — not blocked",
        families=["SLOT_RVOL", "CUM_RVOL"],
        label_discipline="the word RVOL is not used until its denominator is fully specified",
        slot_rvol=dict(
            numerator="observed volume in minute-of-session slot s of the current session",
            denominator="baseline over the same minute-of-session slot s across prior "
                        "eligible sessions",
            lookback_unit="same-minute-of-session observations",
            lookback_length="TO BE FROZEN IN THE SAME DECISION AS THE EMA RESOLUTION — no "
                            "value is invented here",
            current_session_excluded_from_own_baseline=True,
            prior_only=True,
            baseline_membership=dca["rvol_consequence"]["slot_rvol"],
            minimum_sample_count="explicit, uniform, and NOT lowered per security",
            zero_denominator="value UNAVAILABLE with reason, never division by zero",
            missing_history="excluded from the denominator and counted, never treated as "
                            "zero volume",
            early_close="slot positions come from the XNYS calendar; an early-close session "
                        "has 210 slots and no post-close slot may be invented"),
        cum_rvol=dict(
            definition="cumulative session volume relative to the cumulative baseline at the "
                       "same session position",
            contamination=dca["rvol_consequence"]["cum_rvol"],
            flagged_forward="once the curve crosses an UNOBSERVED minute it is flagged from "
                            "that point forward for that session"),
        reported_with_every_value=dca["rvol_consequence"]["reported"],
        emitted_fields=["rvol_value", "rvol_baseline_value", "rvol_sample_count",
                        "rvol_expected_sample_count", "rvol_excluded_unobserved_count",
                        "rvol_validity_state", "rvol_unavailable_reason"])

    schemas = dict(
        outputs=["ema_features", "rvol_features", "feature_validity"],
        additive=True,
        must_not_mutate=["minute_states", "observed_regular_bars",
                         "session_close_boundary_bars", "extended_hours_bars"],
        identity_fields=["security_key_v1", "session_date", "minute_ts",
                         "feature_definition_version", "input_validity_state"],
        ticker="descriptive metadata only; never the primary identity",
        provenance_binding=["derived-base artifact 5591685cfa33b62a", "feature spec digest",
                            "feature implementation hash", "cohort digest",
                            "calendar authority", "parameter registry digest"],
        provenance_level="FILE_LEVEL — no stronger claim than is implemented")

    negatives = [
        "future bar cannot affect EMA[t]",
        "future volume cannot affect RVOL[t]",
        "UNOBSERVED minute cannot become a zero-volume observation",
        "EMA cannot bridge an UNOBSERVED expected minute silently",
        "EMA unavailable minute cannot be forward-filled",
        "extended-hours bar cannot affect EMA",
        "close-boundary bar cannot affect EMA",
        "WI bar cannot affect a regular-way feature",
        "NOT_YET bar cannot affect a feature",
        "RVOL denominator cannot use a current or future observation under prior-only "
        "semantics",
        "missing historical RVOL input cannot become zero",
        "early-close session cannot create nonexistent late-session positions",
        "feature-unavailable observation cannot silently remove a security from the cohort",
        "a parameter outside the frozen registry must fail",
    ]

    capability = dict(
        label_free=True, runs_before="production feature construction",
        may_answer=["what fraction of expected eligible observations can receive each EMA",
                    "how often each EMA resets because of UNOBSERVED minutes",
                    "the distribution of ema_age_observed_bars",
                    "what fraction can receive valid RVOL",
                    "RVOL sample counts by minute / session / security"],
        must_not_answer=["does EMA/RVOL predict Y"],
        classification="X / DATA CAPABILITY DIAGNOSTIC — not evidence of predictive edge",
        if_it_changes_parameters="record as pre-Y design provenance and freeze before any Y "
                                 "calculation",
        why_before_full_build="with 17.7M UNOBSERVED minutes, long-period EMAs may reset "
                              "often. That would not make a period bad — it would tell us "
                              "where the feature is actually defined, and we should know "
                              "that before seeing T/Z")

    cohort_rule = dict(
        cohort=476, membership_source="SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1",
        rule="a security is NOT removed from the cohort because a feature is unavailable at "
             "some timestamps",
        separation="cohort membership and feature validity are different dimensions",
        downstream="analysis may use feature-valid observations only, but must report "
                   "availability and exclusion accounting")

    p = dict(
        spec_id="MASSIVE_1M_EMA_RVOL_FEATURE_SPEC_V1",
        status="HOLD",
        verdict_reason="the EMA family cannot be frozen on a 1m base without overruling the "
                       "charter's one_minute_role clause. Everything else is resolved and "
                       "recorded.",
        task_class="SPECIFICATION_ONLY",
        authoritative_inputs=binding,
        parameter_provenance_registry=registry,
        registry_resolution=dict(
            resolved=True,
            current_program_pre_registered=["SLOT_RVOL", "CUM_RVOL"],
            legacy_reference_only=["EMA_RIBBON 8/13/21/34/55/89/200",
                                   "VOLUME_20_BAR_AVERAGE"],
            blocked_pending_decision=["EMA 9/20/50/200"],
            rejected_for_current_program=[],
            method="occurrence inventory across the codebase plus a read of the frozen "
                   "charter; conflicting historical implementations were NOT merged",
            charter_wins="the charter names slot-RVOL and CUM-RVOL as feature families and "
                         "their baseline semantics are already frozen, so they take "
                         "precedence over the legacy 20-bar volume ratio"),
        blocking_conflict=conflict,
        causal_timestamp_contract=causal,
        input_bar_policy=inputs,
        missing_data_semantics=missing,
        ema_contract=ema_contract,
        rvol_contract=rvol_contract,
        feature_schemas=schemas,
        negative_fixtures=dict(count=len(negatives), required=negatives,
                               standard="each must show the dangerous version FAILS and the "
                                        "correct version PASSES"),
        capability_plan=capability,
        cohort_and_feature_validity=cohort_rule,
        parameter_search_rule=dict(
            registry_is_part_of_the_search_space=True,
            after_freeze="adding another EMA period or RVOL lookback after observing T/Z/Y "
                         "requires registering a NEW hypothesis family with its own "
                         "multiplicity and provenance treatment",
            no_post_Y_expansion=True),
        y_exposed=0,
        feature_production_writes=0,
        acceptance={
            "production_derived_base_bound": True,
            "cohort_476_unchanged": True,
            "supplemental_prohibited": True,
            "parameter_registry_frozen": False,
            "legacy_conflicts_resolved_explicitly": True,
            "ema_causality_frozen": False,
            "rvol_causality_frozen": True,
            "unobserved_never_no_trade": True,
            "ema_gap_reset_explicit": True,
            "rvol_missing_baseline_explicit": True,
            "early_close_explicit": True,
            "validity_separate_from_cohort": True,
            "negative_fixtures_defined": True,
            "capability_diagnostics_defined": True,
            "y_exposed_zero": True,
            "feature_production_writes_zero": True},
        mutations=dict(derived_base=0, cohort=0, charter=0, canonical_raw=0,
                       feature_writes=0, y_exposure=0),
        decision_required="choose option A (build the coarse aggregation layer, then define "
                          "EMA there) or option B (amend one_minute_role to admit a named "
                          "1m-native EMA family). RVOL is unblocked either way.",
        next_step_if_A="freeze MASSIVE_1M_COARSE_AGGREGATION_SPEC (the charter's "
                       "aggregation_contract is already detailed), build it, then reissue "
                       "this feature spec with EMA defined at 15m/1H/1D",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_EMA_RVOL_FEATURE_SPEC_V1.json",
                 required=("spec_id", "status", "authoritative_inputs",
                           "parameter_provenance_registry", "blocking_conflict",
                           "causal_timestamp_contract", "missing_data_semantics",
                           "rvol_contract", "negative_fixtures", "acceptance", "mutations"),
                 supersede=os.path.exists("MASSIVE_1M_EMA_RVOL_FEATURE_SPEC_V1.json"))
    print(f"MASSIVE_1M_EMA_RVOL_FEATURE_SPEC_V1 · {d} · {p['status']}")
    print(f"  registry: RVOL families PRE-REGISTERED · ribbon + 20-bar volume "
          f"LEGACY_REFERENCE_ONLY · EMA 9/20/50/200 BLOCKED")
    print(f"  blocking: charter one_minute_role — a 1m quantity with no coarse-TF parent is "
          f"inadmissible by construction")
    print(f"  RVOL not blocked · Y_EXPOSED 0 · feature writes 0")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
