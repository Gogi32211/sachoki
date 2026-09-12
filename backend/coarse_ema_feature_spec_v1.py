"""MASSIVE_COARSE_EMA_FEATURE_SPEC_V1 — the EMA half, frozen alone and clean of RVOL.

Split out of MASSIVE_EMA_RVOL_FEATURE_SPEC_V2 (3228c4025dbfcc39), which stays an immutable
HOLD. EMA and RVOL are independently specifiable feature families, and RVOL's nine undeclared
parameters are no reason to stall a family whose every binding decision is already made.

THE ONE THING THIS FILE MUST NOT DO IS CARRY AN RVOL DEFAULT IN BY ACCIDENT. Splitting a spec
is exactly when a stray key travels with the copy, and an RVOL lookback that arrived here as a
side effect of a refactor would be indistinguishable from a pre-registered one later. So the
sealed payload is scanned for any rvol/volume-baseline token before it is written, and the
build refuses if one is found. The check is mechanical because trusting my own care is the
weaker of the two options.

AUCTION SEMANTICS STAY UNASSERTED. SESSION_CLOSE_BOUNDARY_BAR remains neutral, and nothing here
reads it as an auction print. That bar is not an EMA input in any case — EMA consumes only
COMPLETE coarse observations — but the neutrality is recorded rather than left implicit,
because the RVOL dictionary will have to work on what is actually known rather than on a
presumed attribution.
"""
from __future__ import annotations
import json, os, re, sys, time                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

BIND = {
    "MASSIVE_1M_STATE_TRANSITION_V1": "d73a88d836f36eea",
    "MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1": "5591685cfa33b62a",
    "MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1": "51540a1cefe2ec2b",
    "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1": "6ec73d5f462761f0",
    "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1": "7c0a5ad6b17d7ae0",
    "MASSIVE_EMA_RVOL_FEATURE_SPEC_V2": "3228c4025dbfcc39",
}
PERIODS = [9, 20, 50, 200]
TFS = ["15m", "1H", "1D"]
# The scan targets a PARAMETER DEFAULT, not the word "RVOL". A first version matched raw
# text and flagged the artifact's own sentence saying RVOL is specified separately — which is
# precisely what this artifact should say. So it now walks KEYS and only objects when a
# parameter-shaped key carries an actual numeric value.
PARAM_KEY = re.compile(r"rvol|lookback|baseline|threshold|per_?slot|slot_count|"
                       r"sample_count|relative_volume", re.I)
# `mutations` holds mutation COUNTS, not parameter declarations — a zero there is
# the opposite of a leaked default.
ALLOWED_SUBTREES = ("rvol_branch", "mutations")


def scan_leaks(node, path=()):
    """Any parameter-shaped key carrying a real value, outside the rvol_branch subtree."""
    out = []
    if any(a in path for a in ALLOWED_SUBTREES):
        return out
    if isinstance(node, dict):
        for k, v in node.items():
            if (PARAM_KEY.search(str(k)) and isinstance(v, (int, float))
                    and not isinstance(v, bool)):
                out.append(f"{'.'.join(path + (str(k),))} = {v}")
            out += scan_leaks(v, path + (str(k),))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            out += scan_leaks(v, path + (str(i),))
    return out


def main():
    binding, bad = {}, []
    for n, want in BIND.items():
        got = ART.file_digest(n + ".json")
        binding[n] = dict(cited=want, actual=got, match=got == want)
        if got != want:
            bad.append(n)
    if bad:
        print("HOLD — digest mismatch:", bad); return 1
    charter = json.load(open("MASSIVE_1M_STATE_TRANSITION_V1.json"))
    prod = json.load(open("MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json"))
    el = json.load(open("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))

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
        "extended-hours / close-boundary / WHEN_ISSUED / NOT_YET / supplemental inputs "
        "cannot affect EMA",
        "feature invalidity cannot remove a security from the frozen cohort",
        "a legacy ribbon period outside the registry cannot enter production",
    ]

    p = dict(
        spec_id="MASSIVE_COARSE_EMA_FEATURE_SPEC_V1",
        status="MASSIVE_COARSE_EMA_FEATURE_SPEC_V1_FROZEN",
        task_class="SPECIFICATION_ONLY",
        scope="EMA ONLY — the RVOL family is specified separately and is not referenced "
              "here as a default, a fallback or an assumption",
        split_from=dict(
            artifact="MASSIVE_EMA_RVOL_FEATURE_SPEC_V2", digest="3228c4025dbfcc39",
            disposition="IMMUTABLE HOLD — not edited",
            why="EMA and RVOL are independently specifiable families; RVOL's nine "
                "undeclared parameters are no reason to stall a family whose binding "
                "decisions are all made"),
        authoritative_inputs=binding,

        one_minute_role=dict(
            unchanged=True, amended=False,
            invariant=charter["one_minute_role"]["enforcement"],
            satisfied_by="EMA is defined on the registered coarse parents 15m/1H/1D, which "
                         "exist as production data; 1m may only time when a coarse state "
                         "became observable"),

        registry=dict(
            periods=PERIODS,
            classification="NEWLY_PRE_REGISTERED_BEFORE_Y",
            frozen_while_y_exposed_is_zero=True,
            rationale_without_reference_to_Y=[
                "the compact EMA family already used in prior coarse-timeframe T/Z logic",
                "preserves continuity with prior programme logic",
                "avoids expanding into additional legacy EMA families",
                "fixed before any outcome exposure"],
            explicitly_not_because="frequency of occurrence in the legacy codebase",
            excluded_periods=[8, 13, 21, 34, 55, 89],
            legacy_ribbon=dict(periods=[8, 13, 21, 34, 55, 89, 200],
                               classification="LEGACY_REFERENCE_ONLY", importable=False),
            post_Y_expansion="forbidden without a new registered search family, its own "
                             "multiplicity treatment, and fresh/forward confirmation"),

        timeframe_registry=dict(
            timeframes=TFS,
            source="MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1",
            uniform_across_tfs=True,
            per_tf_period_selection=False,
            note="availability differences between timeframes are REPORTED, never optimised "
                 "away by choosing different periods per timeframe"),

        search_space_declaration=dict(
            raw_streams=len(PERIODS) * len(TFS),
            enumeration=[f"EMA{p}@{tf}" for tf in TFS for p in PERIODS],
            frozen_as_provenance=True),

        input_eligibility=dict(
            consumes_only="coarse observations with coverage_state = COMPLETE",
            forbidden=["UNOBSERVED_CONTAMINATED", "INELIGIBLE",
                       "partial descriptive aggregate", "extended-hours input",
                       "SESSION_CLOSE_BOUNDARY_BAR", "WHEN_ISSUED_EXCLUDED",
                       "NOT_YET_REGULAR_WAY", "supplemental input", "synthetic input"]),

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

        formula=dict(alpha="2 / (p + 1)",
                     seed="EMA_1 = close_1 at the first COMPLETE bar of a new segment",
                     recurrence="EMA_t = alpha * close_t + (1 - alpha) * EMA_(t-1), within "
                                "the same valid segment only"),

        emitted_fields=["security_key_v1", "session_date", "timeframe", "bar_start",
                        "bar_end", "feature_information_available_at", "ema_period",
                        "ema_value", "ema_segment_id", "ema_age_valid_bars",
                        "ema_validity_state", "ema_reset_reason"],

        warm_up_validity=dict(
            rule={"1 <= ema_age_valid_bars < period": "INITIALIZATION_SENSITIVE",
                  "ema_age_valid_bars >= period": "VALID",
                  "contaminated / ineligible interval": "UNAVAILABLE"},
            example="EMA200 requires 200 consecutive COMPLETE coarse bars before becoming "
                    "VALID",
            not_loosened_for_low_coverage=True,
            initialization_sensitivity_not_hidden=True),

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

        feature_validity_vs_cohort=dict(
            cohort=el["cohort"]["eligible_count"],
            cohort_digest=el["cohort"]["cohort_security_key_sha256"][:16],
            rule="a security remains one of the frozen 476 even when EMA is UNAVAILABLE or "
                 "INITIALIZATION_SENSITIVE",
            forbidden="constructing a cleaner security cohort from feature availability",
            downstream="analysis must report observation-level availability"),

        availability_expectation=dict(
            bound_as="pre-Y design provenance",
            coarse_complete_share={tf: prod["by_timeframe"][tf]["complete_share"]
                                   for tf in TFS},
            daily_longest_complete_run_median=prod["daily_report"][
                "longest_complete_daily_run"]["median"],
            securities_with_complete_daily_run=prod["daily_report"][
                "securities_with_a_complete_run_of_at_least"],
            binding="these justify transparent availability accounting; they do NOT change "
                    "the registry",
            ema200_retained=dict(
                decision="EMA200 stays although only 72 of 476 securities have any 200-bar "
                         "COMPLETE daily run",
                why="that is an AVAILABILITY problem, not a parameter-selection problem. "
                    "Dropping it would let X-side capability reshape the hypothesis family.",
                instead="definition fixed; VALID where age >= 200, UNAVAILABLE elsewhere; "
                        "availability reported exactly",
                also_forbidden="loosening contamination semantics to raise its "
                               "availability")),

        auction_semantics=dict(
            asserted=False,
            close_boundary_bar="SESSION_CLOSE_BOUNDARY_BAR remains neutral; the auction "
                               "reading is recorded upstream as plausible and undemonstrated",
            effect_on_ema="none — the close-boundary bar is not an EMA input under any "
                          "circumstance",
            recorded_because="the RVOL dictionary must work on what is actually known "
                             "rather than on a presumed attribution"),

        negative_fixtures=dict(count=len(negatives), required=negatives,
                               standard="each must show the dangerous version FAILS and the "
                                        "correct version PASSES"),

        capability_plan=dict(
            label_free=True, runs_before="full EMA production",
            per_stream=["candidate COMPLETE inputs", "INITIALIZATION_SENSITIVE outputs",
                        "VALID outputs", "UNAVAILABLE outputs", "segment count",
                        "segment-length distribution", "reset count",
                        "share of coarse observations with VALID EMA",
                        "securities with any VALID EMA"],
            must_not_answer="does EMA predict Y"),

        rvol_branch=dict(
            status="BLOCKED_PENDING_DECISION",
            artifact="MASSIVE_EMA_RVOL_FEATURE_SPEC_V2", digest="3228c4025dbfcc39",
            undeclared_parameters=9,
            no_rvol_default_present_in_this_artifact=True,
            leak_scan="the sealed payload is walked before writing and the build refuses "
                      "if any parameter-shaped key (rvol / lookback / baseline / threshold "
                      "/ per-slot / sample_count) carries a numeric value outside the "
                      "rvol_branch and mutations subtrees",
            leak_scan_targets="a parameter DEFAULT, not the word RVOL — a first version "
                              "matched raw text and flagged this artifact's own sentence "
                              "saying RVOL is specified separately"),

        y_firewall=dict(y_exposed=0, capability="X-side only",
                        forbidden=["future returns", "T outcomes", "Z outcomes", "P&L",
                                   "win rate", "transition enrichment",
                                   "outcome-conditioned availability"]),

        acceptance={
            "charter_unchanged": True,
            "one_minute_role_unchanged": True,
            "cohort_476_unchanged": el["cohort"]["eligible_count"] == 476,
            "coarse_production_pass_bound": binding[
                "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1"]["match"],
            "periods_frozen_before_Y": PERIODS == [9, 20, 50, 200],
            "no_additional_periods": True,
            "timeframes_15m_1H_1D_only": TFS == ["15m", "1H", "1D"],
            "twelve_streams": len(PERIODS) * len(TFS) == 12,
            "contamination_tolerance_none": True,
            "complete_only_inputs": True,
            "segment_break_on_contaminated": True,
            "age_ge_period_for_valid": True,
            "no_derived_search_features": True,
            "feature_validity_distinct_from_cohort": True,
            "auction_semantics_unasserted": True,
            "negative_fixtures_frozen": len(negatives) == 13,
            "no_rvol_defaults": True,
            "y_exposed_zero": True,
            "feature_writes_zero": True},
        mutations=dict(charter=0, cohort=0, coarse_production=0, derived_base=0,
                       rvol_spec=0, feature_writes=0, y_exposure=0),
        feature_production_writes=0,
        next_step="EMA feature smoke + label-free capability, then EMA production. RVOL "
                  "remains on its own branch and is not started.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    # mechanical leak scan — trusting my own care is the weaker option
    leaks = scan_leaks(p)
    p["rvol_branch"]["leak_scan_hits_outside_allowlist"] = len(leaks)
    if leaks:
        p["acceptance"]["no_rvol_defaults"] = False
        print(f"HOLD — {len(leaks)} RVOL token(s) leaked into the EMA artifact:")
        for x in leaks[:5]:
            print("   ", x)
        return 1

    ok = all(p["acceptance"].values())
    p["status"] = p["status"] if ok else "HOLD"
    d = ART.seal(p, "MASSIVE_COARSE_EMA_FEATURE_SPEC_V1.json",
                 required=("spec_id", "status", "scope", "authoritative_inputs", "registry",
                           "timeframe_registry", "input_eligibility",
                           "contamination_tolerance", "segmentation", "formula",
                           "warm_up_validity", "causal_availability", "negative_fixtures",
                           "acceptance", "mutations"),
                 supersede=os.path.exists("MASSIVE_COARSE_EMA_FEATURE_SPEC_V1.json"))
    print(f"MASSIVE_COARSE_EMA_FEATURE_SPEC_V1 · {d} · {p['status']}")
    print(f"  registry   {PERIODS} x {TFS} = {len(PERIODS)*len(TFS)} streams "
          f"(NEWLY_PRE_REGISTERED_BEFORE_Y)")
    print(f"  inputs     COMPLETE only · tolerance NONE · gap breaks segment")
    print(f"  validity   age < p -> INITIALIZATION_SENSITIVE · age >= p -> VALID")
    print(f"  negatives  {len(negatives)} · RVOL leak scan: {len(leaks)} hits")
    print(f"  acceptance {'ALL PASS' if ok else p['acceptance']}")
    print(f"  Y_EXPOSED 0 · feature writes 0")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
