"""MASSIVE_RVOL_DICTIONARY_SPEC_V1 — ten decisions, none of them mine.

The charter named slot-RVOL and CUM-RVOL and froze their baseline MEMBERSHIP, but deliberately
left the dictionary that computes them to a later step. MASSIVE_EMA_RVOL_FEATURE_SPEC_V2
(3228c4025dbfcc39) held rather than fill the gaps with conventional values. This artifact
records the ten answers that were supplied, and adds nothing.

TWO OF THE TEN CARRY CONSEQUENCES WORTH STATING PLAINLY, because both look like bugs later if
they are not written down as decisions now.

First, early-close RVOL is UNAVAILABLE BY CONSTRUCTION in this window. Normal and early-close
baselines are separate (answer 7) and a value needs 15 valid prior same-type observations
(answer 3), but the five-year window contains only ten early-close sessions. Ten can never
supply fifteen. Every RVOL value on those sessions is therefore unavailable, permanently, and
that is the accepted price of not pooling two different volume shapes into one baseline.

Second, a zero baseline yields no number at all. Where 15+ genuine observed zero-volume minutes
produce a baseline mean of 0, the emission is UNAVAILABLE / ZERO_BASELINE — not 0, not infinity,
not 1, and not a bare null. The numerator, the zero baseline, the sample count and the
excluded-UNOBSERVED accounting are all retained so a downstream reader can see exactly why no
ratio exists. Epsilon floors are forbidden by name: `max(mean, 1)` or `1e-9` would be an
unregistered numerical regularization that silently converts an undefined ratio into a
plausible-looking finite one.

THE AUCTION QUESTION IS ANSWERED BY DECLINING TO ANSWER IT. The first and last regular minutes
are included normally and carry `boundary_print_attribution = UNRESOLVED`. Excluding them would
have been just as unproven an assumption as treating them as ordinary — the programme has real
vendor volume there and no evidence about what fraction is an auction print, so it records the
volume and refuses the attribution.
"""
from __future__ import annotations
import json, os, sys, time                                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

BIND = {
    "MASSIVE_1M_STATE_TRANSITION_V1": "d73a88d836f36eea",
    "MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1": "8aaef7196c420136",
    "MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1": "5591685cfa33b62a",
    "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1": "6ec73d5f462761f0",
    "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1": "7c0a5ad6b17d7ae0",
    "MASSIVE_EMA_RVOL_FEATURE_SPEC_V2": "3228c4025dbfcc39",
    "MASSIVE_1M_PROBE_SPEC_V1": None,
    "MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1": None,
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
    dca = json.load(open("MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.json"))

    answers = [
        dict(n=1, parameter="baseline_lookback",
             value="20 prior eligible sessions, same security_key_v1, same session type"),
        dict(n=2, parameter="baseline_statistic", value="arithmetic mean",
             explicitly_not=["median", "trimmed mean"],
             rationale="an 'expected volume' baseline; the mean preserves the additive "
                       "interpretation that CUM-RVOL also relies on"),
        dict(n=3, parameter="minimum_valid_sample_count", value=15,
             rule="< 15 valid prior observations at that slot -> UNAVAILABLE",
             never="lowered security by security to raise availability"),
        dict(n=4, parameter="current_session_excluded", value=True,
             rule="the baseline is strictly prior-only; the current session may never enter "
                  "its own denominator"),
        dict(n=5, parameter="window", value="fixed trailing rolling 20",
             explicitly_not="expanding",
             rule="at most the last 20 matching prior eligible sessions"),
        dict(n=6, parameter="slot_granularity",
             value="1 regular-session minute, session-open-relative position "
                   "(slot_1, slot_2, ...)",
             explicitly_not="coarse interval position",
             charter_position="this remains charter-authorised 1m CLOCK measurement, not a "
                              "1m-native signal family"),
        dict(n=7, parameter="cross_session_type_pooling", value=False,
             rule="normal-session and early-close baselines are independent"),
        dict(n=8, parameter="cum_rvol_denominator",
             value="arithmetic mean of prior-session CUMULATIVE volumes through the same "
                   "slot",
             explicitly_not="sum of per-slot means, which would be a shortcut",
             historical_sample_validity="a prior session contributes only if its cumulative "
                                        "path from slot 1 through slot k is uninterrupted by "
                                        "any UNOBSERVED expected minute",
             same_policy="last 20 same-session-type prior sessions, minimum 15 valid "
                         "cumulative samples, arithmetic mean",
             current_session="once the current cumulative path crosses an UNOBSERVED minute "
                             "it carries the charter's contaminated/unavailable state from "
                             "that point forward; missing volume is never 0"),
        dict(n=9, parameter="first_last_regular_slot",
             value="INCLUDE NORMALLY + EXPLICIT NEUTRAL FLAG",
             flag="boundary_print_attribution = UNRESOLVED",
             scope=dict(first_regular_minute="included",
                        last_regular_minute="included",
                        close_boundary_bar="excluded — separate bar_scope",
                        auction_claim="NOT MADE"),
             rationale="real regular-session vendor volume exists there and no evidence "
                       "identifies what fraction is an auction print; EXCLUDING them would "
                       "be an equally unproven semantic assumption"),
        dict(n=10, parameter="zero_denominator_handling",
             value="UNAVAILABLE / ZERO_BASELINE",
             explicitly_not=["0", "infinity", "1", "bare numeric NULL without a reason"]),
    ]

    zero = dict(
        contract={
            "valid_sample_count >= 15 and baseline_mean > 0":
                dict(rvol="numerator / baseline_mean", validity="VALID"),
            "valid_sample_count >= 15 and baseline_mean == 0":
                dict(rvol_numeric_value="NOT EMITTED", validity="UNAVAILABLE",
                     unavailable_reason="ZERO_BASELINE")},
        applies_to=["slot-RVOL", "CUM-RVOL"],
        zero_samples_remain_valid="the 15+ genuine observed zero-volume samples stay valid "
                                  "observations; they are NOT ejected from the baseline "
                                  "merely because their mean is 0",
        cases={"numerator == 0 and denominator == 0":
               "UNAVAILABLE / ZERO_BASELINE — never RVOL = 1, never 0",
               "numerator > 0 and denominator == 0":
               "UNAVAILABLE / ZERO_BASELINE — never +infinity"},
        retained_fields=["numerator_volume", "baseline_value (= 0)", "sample_count",
                         "excluded_unobserved_count", "unavailable_reason"],
        epsilon_floor_forbidden=dict(
            examples=["max(mean, 1)", "1e-9", "any smoothing constant"],
            why="an unregistered numerical regularization that silently converts an "
                "undefined ratio into a plausible-looking finite one"))

    early = dict(
        finding="early-close RVOL is UNAVAILABLE BY CONSTRUCTION in this V1 window",
        arithmetic=dict(early_close_sessions_in_window=10,
                        minimum_valid_prior_samples_required=15,
                        pooling_allowed=False,
                        conclusion="ten prior same-type sessions can never supply fifteen"),
        classification="PRE-REGISTERED DESIGN CONSEQUENCE, NOT AN IMPLEMENTATION DEFECT",
        accepted_tradeoff="not pooling two different volume shapes into one baseline, and "
                          "not inventing a lower minimum for early-close sessions",
        recorded_now_because="it would otherwise be reported later as a bug")

    inherited = dict(
        baseline_membership=dca["rvol_consequence"]["slot_rvol"],
        cumulative_contamination=dca["rvol_consequence"]["cum_rvol"],
        mandatory_reporting=dca["rvol_consequence"]["reported"],
        unobserved_never_zero=dca["states"]["UNOBSERVED"]["explicitly_not_zero"],
        structural_no_trade=dict(
            capability="UNAVAILABLE",
            consequence="the charter admits STRUCTURAL_NO_TRADE into the baseline, but this "
                        "programme cannot prove its preconditions, so in practice the "
                        "baseline is built from OBSERVED minutes only and the excluded count "
                        "is reported"))

    p = dict(
        spec_id="MASSIVE_RVOL_DICTIONARY_SPEC_V1",
        status="MASSIVE_RVOL_DICTIONARY_SPEC_V1_FROZEN",
        task_class="SPECIFICATION_ONLY",
        scope="RVOL ONLY — the EMA family is specified and frozen separately in "
              "MASSIVE_COARSE_EMA_FEATURE_SPEC_V1; no EMA parameter is referenced here",
        resolves=dict(artifact="MASSIVE_EMA_RVOL_FEATURE_SPEC_V2",
                      digest="3228c4025dbfcc39",
                      disposition="IMMUTABLE HOLD — not edited",
                      undeclared_parameters_then=9,
                      parameters_declared_now=10,
                      tenth="zero-denominator handling, which surfaced during this freeze "
                            "and was not among the original nine"),
        authoritative_inputs=binding,
        families=["slot-RVOL", "CUM-RVOL"],
        classification="CURRENT_PROGRAM_PRE_REGISTERED",
        pre_registered_before_y=True, y_exposed=0,
        decisions=answers,
        decision_provenance="every value was supplied explicitly; no default, convention or "
                            "'standard' value was added",
        zero_denominator=zero,
        early_close_consequence=early,
        inherited_from_frozen_authority=inherited,
        causality=dict(
            availability="CLOSE_OF_BAR_T",
            no_future=["future session information", "future minute",
                       "future cumulative volume", "future outcome"],
            prior_only="the current session never enters its own denominator"),
        one_minute_role=dict(
            unchanged=True, amended=False,
            justification="slot-RVOL and CUM-RVOL measure a session-level volume state and "
                          "time when within the session it is reached — the permitted use "
                          "of 1m under the charter"),
        legacy_exclusions=dict(
            volume_over_rolling_20_bar_average="LEGACY_REFERENCE_ONLY",
            legacy_thresholds=["1.4x", "1.8x", "2.0x"],
            classification="LEGACY_REFERENCE_ONLY", importable=False,
            why="occurrence in historical code is not pre-registration"),
        thresholds=dict(emitted="continuous quantities plus validity metadata only",
                        threshold_search=False, inferred_from_legacy_code=False),
        emitted_fields=["security_key_v1", "session_date", "slot_index", "minute_ts",
                        "feature_information_available_at", "rvol_family",
                        "numerator_volume", "baseline_value", "rvol_value",
                        "sample_count", "excluded_unobserved_count",
                        "rvol_validity_state", "unavailable_reason",
                        "boundary_print_attribution", "session_type"],
        feature_validity_vs_cohort=dict(
            cohort=476,
            rule="a security stays in the frozen cohort even when RVOL is UNAVAILABLE",
            forbidden="constructing a cleaner cohort from feature availability"),
        governance_guard=dict(
            rule="if implementation surfaces a further required semantic parameter that is "
                 "neither in these ten decisions nor in the frozen charter, HOLD",
            never="pick a value silently",
            precedent="the tenth parameter was found exactly this way"),
        acceptance={
            "families_charter_named": True,
            "ten_parameters_declared": len(answers) == 10,
            "no_default_added": True,
            "baseline_membership_inherited_verbatim": True,
            "zero_denominator_explicit": True,
            "epsilon_floor_forbidden": True,
            "early_close_consequence_recorded": True,
            "auction_attribution_unasserted": True,
            "legacy_thresholds_excluded": True,
            "one_minute_role_unchanged": True,
            "no_ema_parameter_referenced": True,
            "y_exposed_zero": True,
            "rvol_production_writes_zero": True},
        mutations=dict(charter=0, cohort=0, ema_spec=0, coarse_production=0,
                       rvol_writes=0, y_exposure=0),
        rvol_production_writes=0,
        next_step="RVOL feature smoke + label-free capability, then RVOL production. Not "
                  "started here.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    ok = all(p["acceptance"].values())
    p["status"] = p["status"] if ok else "HOLD"
    d = ART.seal(p, "MASSIVE_RVOL_DICTIONARY_SPEC_V1.json",
                 required=("spec_id", "status", "scope", "authoritative_inputs",
                           "decisions", "zero_denominator", "early_close_consequence",
                           "inherited_from_frozen_authority", "acceptance", "mutations"),
                 supersede=os.path.exists("MASSIVE_RVOL_DICTIONARY_SPEC_V1.json"))
    print(f"MASSIVE_RVOL_DICTIONARY_SPEC_V1 · {d} · {p['status']}")
    for a in answers:
        print(f"  {a['n']:2d}. {a['parameter']:<32s} {str(a['value'])[:62]}")
    print(f"  early-close RVOL: UNAVAILABLE BY CONSTRUCTION (10 sessions < 15 required)")
    print(f"  Y_EXPOSED 0 · RVOL writes 0")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
