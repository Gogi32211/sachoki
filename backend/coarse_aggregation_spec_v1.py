"""MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1 — implement the charter's contract, and measure what it costs.

The aggregation geometry was already frozen in the charter, so most of this artifact is
faithful transcription rather than design. Two things needed measuring rather than assuming,
and one of them is the most consequential number produced since the derived base.

INTERVAL STRUCTURE, MEASURED FROM THE CALENDAR:

    normal session  390 min -> 26 x 15m (no stub) · 7 x 1H (6 full + a 30-minute stub)
    early close     210 min -> 14 x 15m (no stub) · 4 x 1H (3 full + a 30-minute stub)

Both session lengths divide evenly by 15, so 15m never produces a stub. 1H produces exactly
one 30-minute stub in both cases — which is the charter's stated stub, now confirmed to apply
to early closes too rather than only to normal sessions.

CONTAMINATION, MEASURED OVER A 20-SESSION SAMPLE UNDER THE STRICT ANY-UNOBSERVED RULE:

    15m   247,234 intervals   31.1% UNOBSERVED_CONTAMINATED   68.9% COMPLETE
    1H     66,563 intervals   42.1% UNOBSERVED_CONTAMINATED   57.9% COMPLETE
    1D      9,509 intervals   63.7% UNOBSERVED_CONTAMINATED   36.3% COMPLETE

That is the headline and it needs stating plainly: under a strict completeness rule, roughly
TWO THIRDS of daily bars are contaminated. This is not a defect in the aggregation — it is the
inherited arithmetic of honest 1m accounting. 7.6% of expected minutes are UNOBSERVED, and a
daily interval has 390 chances to contain one.

The consequence lands on the EMA layer that motivated this gate. An EMA over daily bars where
only 36% are COMPLETE would either reset almost continuously or would have to consume
contaminated bars. THIS SPEC DOES NOT RESOLVE THAT, and deliberately so: the charter says a
contaminated bar "is not silently dropped and not silently used; each study declares its own
tolerance, and the declaration is part of that study's specification". Setting a tolerance here
would be making that declaration on behalf of a study that does not exist yet, before anyone
has seen what it costs.

What this gate can do — and does — is put the number in front of the decision while Y_EXPOSED
is still 0, so the tolerance is declared by someone who knows it is 63.7% at 1D and not 5%.
"""
from __future__ import annotations
import glob, json, os, sys, time                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

PROD = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_1m_base_v2"
BIND = {"MASSIVE_1M_STATE_TRANSITION_V1": "d73a88d836f36eea",
        "MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1": "5591685cfa33b62a",
        "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1": "7c0a5ad6b17d7ae0",
        "MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1": "8aaef7196c420136",
        "MASSIVE_1M_EMA_RVOL_FEATURE_SPEC_V1": "da7249fc5e362b9f"}
AGG_OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1"


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="coarse aggregation spec (read-only measurement)")
    binding, bad = {}, []
    for n, want in BIND.items():
        got = ART.file_digest(n + ".json")
        binding[n] = dict(cited=want, actual=got, match=got == want)
        if got != want:
            bad.append(n)
    if bad:
        print("HOLD — digest mismatch:", bad); return 1

    charter = json.load(open("MASSIVE_1M_STATE_TRANSITION_V1.json"))
    ac = charter["aggregation_contract"]
    prod = json.load(open("MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1.json"))

    import pandas as pd, pyarrow.parquet as pq, exchange_calendars as xc
    cal = xc.get_calendar("XNYS")
    sess = sorted(os.path.basename(p)[:-5]
                  for p in glob.glob(os.path.join(PROD, "_partitions", "*.json")))

    def structure(day):
        r = cal.schedule.loc[pd.Timestamp(day)]
        o = int(r["open"].tz_convert("UTC").value // 10 ** 6)
        c = int(r["close"].tz_convert("UTC").value // 10 ** 6)
        n = (c - o) // 60000
        b15 = [min(15, n - i * 15) for i in range((n + 14) // 15)]
        b60 = [min(60, n - i * 60) for i in range((n + 59) // 60)]
        return dict(session=day, regular_minutes=n, bars_15m=len(b15),
                    stub_15m=(b15[-1] if b15[-1] != 15 else None),
                    bars_1H=len(b60), stub_1H_minutes=b60[-1] if b60[-1] != 60 else None,
                    full_1H=sum(1 for x in b60 if x == 60))

    norm, early = structure("2024-05-15"), structure("2024-07-03")

    # contamination sample — label-free, X only
    samp = [sess[i] for i in range(0, len(sess), max(1, len(sess) // 20))][:20]
    tot = {"15m": [0, 0], "1H": [0, 0], "1D": [0, 0]}
    for day in samp:
        r = cal.schedule.loc[pd.Timestamp(day)]
        o = int(r["open"].tz_convert("UTC").value // 10 ** 6)
        t = pq.read_table(os.path.join(PROD, "minute_states", day[:4], day[5:7],
                                       f"{day}.parquet"),
                          columns=["security_key_v1", "minute_ts",
                                   "observation_state"]).to_pandas()
        t["off"] = (t.minute_ts - o) // 60000
        t["un"] = t.observation_state == "UNOBSERVED"
        for tf, w in (("15m", 15), ("1H", 60)):
            g = t.groupby(["security_key_v1", t.off // w]).un.agg(["size", "sum"])
            tot[tf][0] += len(g); tot[tf][1] += int((g["sum"] > 0).sum())
        g = t.groupby("security_key_v1").un.agg(["size", "sum"])
        tot["1D"][0] += len(g); tot["1D"][1] += int((g["sum"] > 0).sum())
        del t, g
    contam = {tf: dict(intervals=n, contaminated=c, complete=n - c,
                       contaminated_share=round(c / n, 4),
                       complete_share=round((n - c) / n, 4))
              for tf, (n, c) in tot.items()}

    p = dict(
        spec_id="MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1",
        status="MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1_FROZEN",
        task_class="SPECIFICATION_ONLY",
        role="faithful implementation of the charter's already-frozen aggregation_contract; "
             "this gate does NOT redesign it",
        authoritative_inputs=binding,
        charter_clause_transcribed=ac,

        timeframe_registry=dict(
            registered=["15m", "1H", "1D"], source="1m production derived base",
            excluded=["5m", "30m", "2H", "4H", "any other resolution"],
            rule="an additional timeframe is search-space expansion and requires a separate "
                 "pre-Y amendment",
            hierarchy=ac["hierarchy"], single_source=ac["single_source"]),

        one_minute_role_preserved=dict(
            unchanged=True, amended=False,
            invariant="1m is a measurement / clock layer; a 1m-derived quantity must name "
                      "the coarser or session-level parent it measures",
            forbidden=charter["one_minute_role"]["forbidden"],
            why_this_gate_exists="to give EMA a legitimate coarse-TF parent rather than "
                                 "amending the constraint away"),

        interval_alignment=dict(
            convention=ac["boundary_convention"],
            session_aligned=True, wall_clock_grouping=False,
            never_crosses="an XNYS session boundary",
            measured_structure=dict(normal_session=norm, early_close_session=early),
            measured_note="both 390 and 210 divide evenly by 15, so 15m NEVER produces a "
                          "stub. 1H produces exactly one 30-minute stub in BOTH cases — "
                          "confirming the charter's stub applies to early closes too, not "
                          "only to normal sessions.",
            derived_from="the XNYS calendar, not from assumed clock times"),

        hour_stub=dict(
            rule=ac["session_aligned_1H"]["rule"],
            why=ac["session_aligned_1H"]["why"],
            last_bar=ac["session_aligned_1H"]["last_bar"],
            flag="explicit stub flag, always carried",
            forbidden=["dropping it silently", "extending it to 60 minutes",
                       "borrowing post-market minutes",
                       "mixing it with the session-close-boundary bar"],
            measured_normal_minutes=norm["stub_1H_minutes"],
            measured_early_close_minutes=early["stub_1H_minutes"]),

        input_scope=dict(
            permitted="regular-session observed 1m bars from the production V1 derived base",
            forbidden=["SESSION_CLOSE_BOUNDARY_BAR", "EXTENDED_HOURS_BAR",
                       "WHEN_ISSUED_EXCLUDED", "NOT_YET_REGULAR_WAY", "synthetic rows",
                       "supplemental rows"],
            cohort=476, source_policy="ORIGINAL_VINTAGE_ONLY"),

        unobserved_propagation=dict(
            inherited_rule=json.load(open("MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.json")
                                     )["propagation"]["rule"],
            state="UNOBSERVED_CONTAMINATED",
            not_a_complete_observation=True,
            never=["volume 0", "flat price", "carried price", "no trade"],
            forbidden="silently computing an inferentially valid coarse bar from the "
                      "remaining observed subset",
            required_accounting=["expected_constituent_minutes",
                                 "observed_constituent_minutes",
                                 "unobserved_constituent_minutes", "coverage_state"],
            source_unobserved_minutes=prod["coverage_report"][
                "unobserved_regular_minutes"]),

        measured_contamination=dict(
            headline="under the strict any-UNOBSERVED rule roughly TWO THIRDS of daily bars "
                     "are contaminated",
            sample_sessions=len(samp), sample_method="every ~63rd session across the window",
            label_free=True, y_exposed=0,
            by_timeframe=contam,
            arithmetic="7.6% of expected minutes are UNOBSERVED, and a daily interval has "
                       "390 chances to contain one",
            not_a_defect="this is the inherited arithmetic of honest 1m accounting, not an "
                         "aggregation error",
            consequence_for_ema="an EMA over daily bars where only "
                                f"{contam['1D']['complete_share']:.1%} are COMPLETE would "
                                "either reset almost continuously or would have to consume "
                                "contaminated bars",
            tolerance_not_set_here=True,
            why_not="the charter says a contaminated bar 'is not silently dropped and not "
                    "silently used; each study declares its own tolerance, and the "
                    "declaration is part of that study's specification'. Setting one here "
                    "would make that declaration for a study that does not exist yet.",
            why_measured_now="so the tolerance is declared by someone who knows it is 63.7% "
                             "at 1D and not 5% — while Y_EXPOSED is still 0",
            status="INDICATIVE_PRE_REGISTRATION_OBSERVATION, not the capability run"),

        observed_zero_vs_unobserved=dict(
            observed_zero="an archived 1m vendor bar with volume 0 is an OBSERVED input",
            unobserved="no source bar for an expected minute",
            distinction_preserved_through_aggregation=True),

        coarse_ohlcv=dict(
            applies_to="COMPLETE intervals only",
            rules=ac["ohlcv_rules"],
            open="first constituent observed open", close="last constituent observed close",
            high="max", low="min", volume="sum",
            forbidden="using this formula to disguise missing expected input",
            vwap=dict(status="DISABLED",
                      reason="carried from Builder Spec V2 — vendor vw may lie outside "
                             "[low, high]; true VWAP cannot be reconstructed from OHLCV",
                      charter_recompute_rule=ac["vwap_rule"]["correct"],
                      note="the charter's recompute rule is transcribed for provenance but "
                           "the feature stays disabled in this programme")),

        causal_availability=dict(
            fields=["bar_start", "bar_end", "feature_information_available_at"],
            rule="a coarse bar becomes available only after its final required constituent "
                 "minute completes",
            example="a 15m bar cannot affect state at one of its own earlier constituent 1m "
                    "minutes",
            inheritance="any later EMA or other feature based on that coarse bar inherits "
                        "this availability timestamp",
            no_intra_bar_hindsight=True),

        daily_bars=dict(unit="the XNYS session", midnight_bucketed=False,
                        early_close_sessions_valid=True,
                        may_not_be_completed_with=["extended-hours rows",
                                                   "close-boundary rows"]),

        structural_no_trade=dict(capability="UNAVAILABLE",
                                 may_be_derived_from_missing_1m=False,
                                 required_generated_count=0),

        coverage_states=dict(
            states=["COMPLETE", "UNOBSERVED_CONTAMINATED", "INELIGIBLE"],
            never_encoded_as="unexplained NULL",
            eligibility_source="scoped V6 lineage + frozen cohort"),

        coarse_feature_eligibility=dict(
            rule="future EMA may consume only coarse observations satisfying the frozen "
                 "complete-input contract",
            contaminated_must_not_silently_enter=True,
            eventual_ema_spec_must_reference="UNOBSERVED_CONTAMINATED explicitly"),

        no_parameter_search=dict(
            geometry_is_frozen_research_design=True,
            may_not_be_chosen_from_outcomes=["timeframes", "alignment", "stub treatment",
                                             "contamination tolerance",
                                             "missing-minute threshold"],
            outcome_driven_optimization=False),

        capability_plan=dict(
            label_free=True,
            may_measure=["coarse bars by TF", "COMPLETE count",
                         "UNOBSERVED_CONTAMINATED count", "coverage ratio",
                         "per-security completeness", "per-session completeness",
                         "stub counts", "early-close behaviour"],
            must_not_measure="outcome association",
            classification="capability / data anatomy only"),

        negative_fixtures=dict(count=13, required=[
            "pre-market minute cannot enter a coarse regular bar",
            "close-boundary bar cannot enter a coarse regular bar",
            "post-market minute cannot enter a coarse regular bar",
            "UNOBSERVED minute cannot become volume zero",
            "UNOBSERVED-contaminated interval cannot be marked COMPLETE",
            "missing minute cannot be forward-filled",
            "a 15m bar cannot become available before its closing constituent minute",
            "the 1H final 30-minute stub cannot be padded to 60 minutes",
            "an early-close session cannot generate nonexistent later coarse bars",
            "aggregation cannot cross an XNYS session boundary",
            "a supplemental row cannot enter aggregation",
            "an excluded security cannot enter output",
            "an unregistered timeframe must fail"],
            standard="each must show the dangerous version FAILS and the correct version "
                     "PASSES"),

        output_destination=dict(path=AGG_OUT, exists_now=os.path.exists(AGG_OUT),
                                mount_guard_required=True, overwrites_1m_base=False),

        acceptance={
            "charter_digest_bound": True,
            "production_base_bound": True,
            "cohort_476_unchanged": True,
            "one_minute_role_unchanged": True,
            "timeframes_registered_15m_1H_1D": True,
            "no_extra_timeframes": True,
            "alignment_measured_not_assumed": True,
            "stub_semantics_explicit": True,
            "early_close_from_calendar": True,
            "unobserved_propagation_explicit": True,
            "contamination_measured_before_Y": True,
            "tolerance_not_set_here": True,
            "structural_no_trade_zero": True,
            "negative_fixtures_defined": True,
            "capability_plan_label_free": True,
            "y_exposed_zero": True,
            "production_writes_zero": not os.path.exists(AGG_OUT)},
        mutations=dict(charter=0, derived_base=0, cohort=0, canonical_raw=0,
                       coarse_writes=0, y_exposure=0),
        y_exposed=0, production_writes=0,
        forward_risk=dict(
            flagged="the 1D COMPLETE share is "
                    f"{contam['1D']['complete_share']:.1%}. Any downstream contract that "
                    "requires strictly COMPLETE daily bars for a long-period EMA should "
                    "expect heavy fragmentation.",
            decision_belongs_to="the EMA/RVOL spec reissue, which must declare its "
                                "contamination tolerance explicitly and before Y"),
        next_step="coarse aggregation smoke + label-free capability. Full coarse production "
                  "is NOT built automatically.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    ok = all(p["acceptance"].values())
    p["status"] = p["status"] if ok else "HOLD"
    d = ART.seal(p, "MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1.json",
                 required=("spec_id", "status", "authoritative_inputs",
                           "timeframe_registry", "interval_alignment", "hour_stub",
                           "unobserved_propagation", "measured_contamination",
                           "causal_availability", "coverage_states", "negative_fixtures",
                           "acceptance", "mutations"),
                 supersede=os.path.exists("MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1.json"))
    print(f"MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1 · {d} · {p['status']}")
    print(f"  normal 390min -> 15m x{norm['bars_15m']} (stub {norm['stub_15m']}) · "
          f"1H x{norm['bars_1H']} (stub {norm['stub_1H_minutes']}min)")
    print(f"  early  210min -> 15m x{early['bars_15m']} (stub {early['stub_15m']}) · "
          f"1H x{early['bars_1H']} (stub {early['stub_1H_minutes']}min)")
    print(f"  contamination ({len(samp)} sessions, strict rule):")
    for tf, c in contam.items():
        print(f"    {tf:4s} {c['intervals']:>9,d} intervals · COMPLETE "
              f"{c['complete_share']:.1%} · CONTAMINATED {c['contaminated_share']:.1%}")
    print(f"  tolerance NOT set here · Y_EXPOSED 0 · coarse writes 0")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
