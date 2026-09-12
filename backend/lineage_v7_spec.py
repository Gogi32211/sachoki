"""MASSIVE_SECURITY_LINEAGE_V7_SPEC — the model, frozen before any V7 row exists.

V6's defect has a mechanism, and the mechanism is now measured rather than asserted. V6
detected identity on a 22-date quarterly anchor grid. Every one of the eight truncated
securities has its V6 coverage starting exactly ON an anchor — 8/8. And the two worst
displacements explain themselves the moment you line the dates up:

    BLK   legal 2024-10-01 (an anchor)   FIGI switched 2024-10-02, ONE DAY LATER
          -> the 10-01 anchor still saw the old identity, so the next anchor
             that could notice was 2025-01-02.  64 sessions lost.

    XOM   legal 2026-07-01 (an anchor)   FIGI switched 2026-07-02, ONE DAY LATER
          -> same miss; next anchor is the window end.  37 sessions lost.

A grid that samples the day before the change cannot see the change. That is not a tuning
problem to be fixed with more anchors; it is why V7 must not rest coverage on sampled
identity at all.

THE PARENT IS security_key_v1 AND NOTHING BELOW IT MAY REDEFINE IT. Eleven time-varying
dimensions hang underneath, each with its own intervals, its own boundaries and its own
precision. The boundary audit proved they genuinely disagree — BLK moves on four different
dates — so the schema is built to let them disagree rather than to reconcile them.

TWO V6 STRUCTURES FOUND WHILE READING IT THAT V7 MUST NOT FLATTEN.

First, V6 carries TWO status fields, and they differ for exactly four securities:

    status            462 SINGLE / 19 LISTED_LATER / 22 RENAMED     (structural)
    canonical_status  462 SINGLE / 23 LISTED_LATER / 18 RENAMED     (WI excluded)

The four divergent rows are precisely GEHC, GEV, SOLV and VLTO — the four when-issued
securities. Structurally a WI line is a second ticker interval, so `status` reads RENAMED;
canonically the WI interval is excluded, so the security reads LISTED_LATER. Both are
correct answers to different questions. A future anomaly audit that sees 19 != 23 and
"repairs" it would be destroying real semantics, so this is pre-registered as a KNOWN
NON-ANOMALY.

Second, the four WI securities have regular-way starts OFF the anchor grid (2023-01-05,
2024-04-02, 2024-04-02, 2023-10-03) because WI detection was structural rather than sampled.
V6 was capable of exact boundaries where it had a structural signal — which is further
evidence that the anchor grid, not the model, is what failed the eight.

WHAT THIS SPEC DELIBERATELY DOES NOT DECIDE. V7 extends expected coverage backward over
5,069 security-sessions whose bars live only in the supplemental quarantine, at a different
retrieval vintage. Whether those may ever be consumed is a SEPARATE gate. This spec fixes
`canonical_use_authorised = false` and stops there, knowing that a Builder run under V7 alone
would classify almost that entire recovered history as UNOBSERVED. That is the correct
behaviour of an honest builder, not a bug to be patched inside V7.
"""
from __future__ import annotations
import json, os, sys, time                                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

BOUND = "MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1.json"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
EIGHT = ["APO", "J", "CRH", "BG", "LH", "FERG", "BLK", "XOM"]

INPUTS = {
    "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json": "04e28f9570a39282",
    "MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1.json": "1d3ed5c970ea64c0",
    "APO_PRIMARY_SECURITY_CONTINUITY_RESOLUTION_V1.json": "b98ab1801c60b84a",
    "MASSIVE_LINEAGE_PRIMARY_EVIDENCE_RESOLUTION_V1.json": "9a95328aa59a678e",
    "MASSIVE_TICKER_LINEAGE_V6.json": "8c961aa3a0c67934",
    "SP500_CURRENT_SNAPSHOT_V1.json": "ffabd63a1aad0538",
    "MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1.json": "3eb4e01940b62e92",
    "MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1.json": "e8c8a648923e22a3",
}


def main():
    binding, mismatched = {}, []
    for f, cited in INPUTS.items():
        actual = ART.file_digest(f)
        binding[f.replace(".json", "")] = dict(cited=cited, actual=actual,
                                               match=actual == cited)
        if actual != cited:
            mismatched.append(f)
    if mismatched:
        print("HOLD — cited digest mismatch:", mismatched)
        return 1

    v6 = json.load(open(LINEAGE))
    aud = json.load(open(BOUND))
    by = {r["current_ticker"]: r for r in aud["securities"]}
    S = {s["current_ticker"]: s for s in v6["securities"]}
    anchors = sorted(S["A"]["anchor_profile"])

    # eight-case repair rules, built from audited evidence rather than by shifting V6 dates
    repairs = []
    for tk in EIGHT:
        r, s = by[tk], S[tk]
        v6_start = s["regular_way_intervals"][0]["effective_from"]
        ref = {a: b for a, b in r["vendor_reference_boundaries"].items()
               if b.get("first_session_state_after")}
        acc = r["vendor_aggregate_access"]
        repairs.append(dict(
            security_key_v1=r["security_key_v1"], current_ticker=tk,
            v6_status=s["status"], v6_canonical_status=s["canonical_status"],
            v6_original_start=v6_start, v6_original_end="2026-08-24",
            v6_start_on_anchor_grid=v6_start in anchors,
            v6_not_yet_regular_way_before=s.get("not_yet_regular_way_before"),
            v7_research_coverage_start="2021-08-25",
            v7_research_coverage_end="2026-08-24",
            v7_derivation="constructed from audited security-level continuity, NOT by "
                          "moving the V6 start backward",
            v6_displacement_sessions=r["v6_anchor_displacement_sessions"],
            recovered_sessions=acc["sessions_with_bars"],
            continuity_classification=r["continuity_classification"],
            attribute_boundaries={a: dict(
                value_before=b.get("value_before"), value_after=b.get("value_after"),
                last_before=b.get("last_session_state_before"),
                first_after=b.get("first_session_state_after"),
                precision=b.get("precision")) for a, b in ref.items()},
            attributes_without_transition=[a for a, b
                                           in r["vendor_reference_boundaries"].items()
                                           if not b.get("first_session_state_after")],
            legal_event=r["legal_boundary"],
            economic_composition_transition=r["economic_composition_transition"],
            gap_sessions=r["gap_overlap"]["gap_sessions"]))

    p = dict(
        spec_id="MASSIVE_SECURITY_LINEAGE_V7_SPEC",
        status="MASSIVE_SECURITY_LINEAGE_V7_SPEC_FROZEN",
        task_class="SPECIFICATION_ONLY",
        supersedes=dict(
            lineage_semantics_from="MASSIVE_TICKER_LINEAGE_V6",
            v6_retained_as="IMMUTABLE_PROVENANCE",
            v6_overwritten=False),

        authoritative_inputs=dict(
            binding=binding, all_digests_verified=True,
            superseded_artifacts=[dict(
                artifact="MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1",
                digest="2f372b2f0fab898f",
                classification="SUPERSEDED_ARTIFACT_ONLY",
                on_disk="MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1.json."
                        "superseded.2f372b2f0fab898f.json",
                current_authority="e8c8a648923e22a3",
                binding_rule="NO downstream contract may bind to 2f372b2f0fab898f as "
                             "current authority; it exists solely as provenance")]),

        why_v7_exists=dict(
            defect_classes={
                "cik_stable_figi_changes": ["J", "LH"],
                "figi_stable_cik_changes": ["BG", "FERG"],
                "both_change": ["APO", "BLK", "XOM"],
                "instrument_representation_changes_ADRC_to_CS": ["CRH"],
                "v6_boundary_is_a_periodic_detection_anchor": EIGHT},
            anchor_grid=dict(
                dates=anchors, count=len(anchors),
                v6_starts_on_anchor_grid=f"{sum(1 for r in repairs if r['v6_start_on_anchor_grid'])}/8",
                mechanism="V6 sampled identity on a quarterly grid. A grid that samples the "
                          "day BEFORE a change cannot see the change.",
                worked_examples=[
                    dict(security="BLK", legal="2024-10-01 (itself an anchor)",
                         figi_switch="2024-10-02", missed_by="1 day",
                         next_anchor="2025-01-02", sessions_lost=64),
                    dict(security="XOM", legal="2026-07-01 (itself an anchor)",
                         figi_switch="2026-07-02", missed_by="1 day",
                         next_anchor="2026-08-24 (window end)", sessions_lost=37)],
                conclusion="this is not fixable by adding anchors; coverage must not rest "
                           "on sampled identity at all"),
            counter_evidence_that_v6_model_was_capable=dict(
                observation="the four when-issued securities have regular-way starts OFF "
                            "the anchor grid (2023-01-05, 2024-04-02, 2024-04-02, "
                            "2023-10-03)",
                because="WI detection was STRUCTURAL, not sampled",
                implication="the anchor grid failed the eight, not the lineage model")),

        core_model=dict(
            master_entity="security_key_v1",
            constant_for="one frozen current research security",
            children=["research_coverage_intervals", "registrant_cik_intervals",
                      "share_class_figi_intervals", "composite_figi_intervals",
                      "ticker_intervals", "instrument_type_intervals",
                      "vendor_reference_intervals", "vendor_aggregate_access_intervals",
                      "structural_transition_events", "legal_corporate_events",
                      "economic_composition_events"],
            invariant="no child attribute may redefine the parent identity"),

        security_key_discipline=dict(
            uses="the already frozen security_key_v1 from "
                 "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1 (04e28f9570a39282)",
            recipe_unchanged=True,
            historical_keys_recomputed=False,
            rule="old and new CIK may both attach to the SAME security_key_v1; old and new "
                 "FIGI likewise. V7 adds time-varying historical aliases BENEATH the key; "
                 "it does not change the key recipe.",
            forbidden="recomputing historical keys from historical CIK/FIGI"),

        logical_datasets=[
            dict(name="research_securities", grain="one row per security_key_v1", rows=503,
                 primary_key="security_key_v1",
                 fields=["security_key_v1", "identity_basis", "current_frozen_ticker",
                         "current_frozen_cik", "current_snapshot_identity",
                         "continuity_classification", "known_structural_caveats",
                         "v6_status", "v6_canonical_status"],
                 forbidden_keys=["ticker", "cik"]),
            dict(name="research_coverage_intervals",
                 grain="security_key_v1 x coverage interval",
                 determines="whether a security-session is EXPECTED in the historical "
                            "research coverage domain",
                 not_determined_by=["current CIK", "current FIGI", 'type == "CS"',
                                    "vendor response availability",
                                    "supplemental payload availability"]),
            dict(name="registrant_cik_intervals", grain="security_key_v1 x interval"),
            dict(name="share_class_figi_intervals", grain="security_key_v1 x interval"),
            dict(name="composite_figi_intervals", grain="security_key_v1 x interval"),
            dict(name="ticker_intervals", grain="security_key_v1 x interval",
                 note="carries ticker-at-time, including WHEN_ISSUED lines flagged as such"),
            dict(name="instrument_type_intervals", grain="security_key_v1 x interval"),
            dict(name="vendor_reference_intervals", grain="security_key_v1 x interval",
                 vintage_required=True),
            dict(name="vendor_aggregate_access_intervals",
                 grain="security_key_v1 x interval", vintage_required=True),
            dict(name="structural_transition_events",
                 grain="security_key_v1 x event",
                 types=["MATERIAL_ECONOMIC_COMPOSITION_TRANSITION",
                        "INSTRUMENT_REPRESENTATION_TRANSITION",
                        "REGISTRANT_REORGANISATION", "REDOMICILIATION"]),
            dict(name="lineage_evidence_links",
                 grain="any row x evidence reference",
                 fields=["evidence_source", "evidence_artifact_digest", "document_sha256",
                         "precision", "vintage"])],
        no_flat_row="dimensions whose boundaries differ are NOT forced into one flat row",

        interval_convention=dict(
            frozen="[start_session, end_session_exclusive)",
            unit="XNYS session",
            applies_to="every interval dataset without exception",
            mixing_forbidden="inclusive/exclusive semantics must never differ between "
                             "datasets",
            required_fields_per_interval=["security_key_v1", "attribute_value",
                                          "start_boundary", "end_boundary",
                                          "boundary_precision", "evidence_source",
                                          "evidence_artifact_digest"],
            ordering="non-overlapping and ordered within (security_key_v1, attribute)"),

        research_coverage_semantics=dict(
            window=dict(SOURCE_HISTORY_START="2021-08-25",
                        SOURCE_HISTORY_END="2026-08-24"),
            eight_case_coverage="2021-08-25 .. 2026-08-24 for all eight, subject only to an "
                                "actual exchange/session exclusion supported by the "
                                "boundary audit (none was found)",
            entitlement_rule="the expired 2021-08-25 supplemental request does NOT change "
                             "eligibility. ENTITLEMENT_UNAVAILABLE != NOT_YET_REGULAR_WAY.",
            separation=dict(
                research_coverage_eligibility="what SHOULD exist",
                vendor_source_access="what the vendor SERVES",
                example=dict(session="2021-08-25", securities=8,
                             research_coverage="EXPECTED",
                             supplemental_retrieval="ENTITLEMENT_UNAVAILABLE",
                             result="expected coverage survives; observation is "
                                    "unavailable"))),

        v6_preservation_for_unaffected=dict(
            scope="the 495 securities outside the known repair set",
            rule="inherit V6 research-coverage semantics unless V7 conformance detects a "
                 "concrete contradiction",
            silent_broadening_forbidden=True, silent_shrinking_forbidden=True,
            new_defect_rule="a NEW candidate defect outside the eight STOPS canonical V7 "
                            "finalization and creates a separate evidence packet; it is "
                            "NEVER repaired on the fly"),

        eight_case_repair_rules=repairs,

        legal_vs_trading=dict(
            rule="legal/corporate timestamps are NEVER coerced to XNYS sessions; they are "
                 "stored in their own dataset",
            established_examples=[
                dict(security="APO", legal="2022-01-01 01:00 ET",
                     first_successor_session="2022-01-03", figi="2022-01-03",
                     cik="2022-01-03"),
                dict(security="BLK", legal="2024-10-01", figi="2024-10-02",
                     cik="2024-10-03", v6_anchor="2025-01-02", distinct_dates=4),
                dict(security="XOM", legal="2026-07-01", figi="2026-07-02",
                     cik="2026-07-08", v6_anchor="2026-08-24", distinct_dates=4)],
            binding="these are different facts and must not be collapsed"),

        apo_structural_break=dict(
            event_type="MATERIAL_ECONOMIC_COMPOSITION_TRANSITION",
            security_continuity=True, economic_composition_continuity=False,
            legal_transition="2022-01-01 01:00 ET (AAM) / 01:01 ET (AHL)",
            first_successor_trading_session="2022-01-03",
            session_precision="EXACT_XNYS_SESSION",
            primary_evidence=dict(form="8-K12B", filed="2022-01-03",
                                  accession="0001193125-22-000274",
                                  sha256="7cbaf800866e0819"),
            forbidden_encoding='"same security" encoded as "same economic object with no '
                               'structural break"',
            must_survive_downstream=True,
            v7_makes_no_inferential_decision="a later research contract decides whether and "
                                             "how analyses may cross this boundary"),

        crh_instrument_transition=dict(
            event_type="INSTRUMENT_REPRESENTATION_TRANSITION",
            instrument_type="ADRC -> CS", research_security_continuity=True,
            instrument_representation_continuity=False,
            ads_ratio="1:1 — each ADS represented one Ordinary Share",
            documented="ADSs cancelled and delisted from the NYSE; Ordinary Shares listed "
                       "on the NYSE under the same symbol",
            boundary="2023-09-25",
            legal_precision="CALENDAR_DATE_ONLY_PROSPECTIVELY_STATED",
            vendor_corroboration="the unfiltered reference endpoint switches type on exactly "
                                 "2023-09-25; corroborative, and NOT grounds to upgrade the "
                                 "legal precision",
            forbidden="relabelling historical ADRC observations as CS",
            bridge_note="the deterministic 1:1 economic bridge supports research-security "
                        "continuity but does not erase instrument-type history"),

        not_audited_legal_dates=dict(
            securities=["J", "BG", "LH", "FERG"],
            value="legal_event_date = NOT_AUDITED",
            reason="the boundary audit deliberately scoped these four to identifier and "
                   "vendor boundaries only",
            may_not_be_populated_from=["FIGI change", "CIK change", "reference change",
                                       "V6 anchor", "ticker continuity"],
            coverage_still_valid="the security-level coverage boundary is unambiguous "
                                 "(single monotone transition, gap_sessions = 0)",
            forbidden="converting NOT_AUDITED into NULL-without-reason"),

        ticker_intervals_preservation=dict(
            rule="V7 must preserve V6's confirmed ticker-at-time lineage while repairing "
                 "registrant/FIGI/type truncation",
            v6_renamed_in_window_canonical=18, v6_renamed_structural=22,
            examples=[[x["regular_way"][0][0], x["ticker"]]
                      for x in v6["renamed_in_window"]][:24],
            eight_cases_ticker_unchanged=True,
            warning="the eight repaired securities happen to have unchanged ticker strings. "
                    "That fact must NOT become a general rule."),

        when_issued_policy=dict(
            preserved_from="MASSIVE_TICKER_LINEAGE_V6",
            intervals=v6["when_issued_intervals"],
            recorded_as="lineage/source attributes",
            excluded_from="canonical REGULAR_WAY research coverage",
            binding="the V7 security-level continuity model must NOT absorb WI intervals "
                    "into expected regular-way coverage",
            applied_before_availability=True),

        dual_status_semantics=dict(
            discovered_in="V6 while preparing this spec",
            fields=dict(status="structural — a WI line counts as a ticker interval",
                        canonical_status="WI excluded"),
            distributions=dict(
                status={"SINGLE_TICKER_WHOLE_WINDOW": 462, "LISTED_LATER": 19,
                        "RENAMED_IN_WINDOW": 22},
                canonical_status={"SINGLE_TICKER_WHOLE_WINDOW": 462, "LISTED_LATER": 23,
                                  "RENAMED_IN_WINDOW": 18}),
            divergent_rows=["GEHC", "GEV", "SOLV", "VLTO"],
            divergent_count=4,
            equals_when_issued_set=True,
            v6_summary_block_reports="canonical_status",
            classification="KNOWN_NON_ANOMALY",
            pre_registered_because="an anomaly audit that sees 19 != 23 and 'repairs' it "
                                   "would destroy real semantics",
            v7_requirement="both fields are carried; they are NOT flattened into one"),

        supplemental_quarantine_policy=dict(
            artifact="MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1",
            digest="e8c8a648923e22a3",
            may_reference="as evidence that historical source responses were retrievable",
            may_not=["promote payloads to canonical raw evidence",
                     "authorize derived consumption"],
            supplemental_data_status="QUARANTINED_DIFFERENT_RETRIEVAL_VINTAGE",
            canonical_use_authorised=False,
            decided_by="a separate gate",
            known_facts=dict(attempted=5077, payloads_with_bars=5069, empty_responses=0,
                             entitlement_unavailable=8,
                             entitlement_session_per_security="2021-08-25",
                             sha256_recorded_at_creation="5069/5069",
                             independent_rehash_sample=40, mismatches=0,
                             role="provenance/integrity facts — they do NOT promote the "
                                  "data")),

        vendor_access_vintage_discipline=dict(
            audit_found="no historical vendor-access transition across the 5,069 previously "
                        "truncated sessions",
            forbidden_restatement='"vendor behavior was identical at original ingest time"',
            because="supplemental responses are LATER-VINTAGE evidence",
            v7_may_record="currently observed aggregate access across interval, WITH "
                          "retrieval vintage",
            original_ingest_provenance_rewritten=False),

        boundary_precision=dict(
            source="MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1 (1d3ed5c970ea64c0)",
            audit_result=aud["boundary_counts"],
            per_fact_rule="precision is a property of each FACT, not of the security",
            forbidden_upgrades=["NOT_AUDITED legal dates upgraded because a vendor boundary "
                                "is exact",
                                "prospective filing dates upgraded because a vendor boundary "
                                "is exact"],
            vocabulary=aud["precision_vocabulary"]),

        gap_overlap_validation=dict(
            per_coverage_interval=["no unexplained gap", "no conflicting overlap"],
            eight_expected_gap_sessions=0,
            measured_gap_sessions={r["current_ticker"]: r["gap_sessions"]
                                   for r in repairs},
            attribute_intervals_may_transition_on_different_sessions=True),

        candidate_build_plan=dict(
            canonical=False, output="versioned staging / scratch",
            overwrites_v6=False, must_include="all 503 security identities",
            then="full conformance before any canonical seal",
            seal_is_a_separate_gate=True),

        global_anomaly_audit=dict(
            required_before_v7_finalization=True, scope="all 503",
            rationale="CRH exposed a defect class that could exist outside the known eight",
            not_permission_to_auto_repair=True,
            checks=[
                "V6 says NOT_YET_REGULAR_WAY but historical aggregates exist",
                "current-identifier filtering excludes historical same-security reference "
                "candidates",
                "type changes such as ADRC -> CS around V6 coverage start",
                "FIGI/CIK changes associated with suspicious coverage truncation",
                "one-day or implausibly short full-window coverage for longstanding "
                "current securities",
                "research coverage beginning at periodic anchor boundaries with older "
                "same-security evidence available"],
            known_non_anomalies=[
                dict(pattern="status vs canonical_status differ for GEHC/GEV/SOLV/VLTO",
                     reason="when-issued dual semantics — pre-registered above"),
                dict(pattern="the eight known cases start on the anchor grid",
                     reason="already diagnosed and repaired by this spec"),
                dict(pattern="19 LISTED_LATER by status vs 23 by canonical_status",
                     reason="same when-issued dual semantics")],
            on_new_candidate="V7 FINALIZATION = HOLD; return the candidate evidence; do NOT "
                             "append it to the eight and silently repair"),

        negative_guards=[
            "current CIK as timeless master identity",
            "current FIGI as timeless master identity",
            'type == "CS" as universal identity/coverage filter',
            "ticker as timeless identity",
            "V6 anchor automatically treated as event date",
            "legal event date forced to equal FIGI/CIK/reference date",
            "CRH ADR period dropped",
            "CRH ADR rows relabeled as CS",
            "APO structural composition break erased",
            "eight repaired securities left truncated",
            "supplemental payload promoted to canonical automatically",
            "2021-08-25 entitlement failure interpreted as NOT_YET_LISTED",
            "new anomaly outside known eight auto-repaired",
            "V6 artifact overwritten"],
        negative_guards_binding="the V7 implementation/conformance suite MUST REJECT each "
                                "of the above; a guard that is documented but not tested "
                                "does not satisfy this spec",

        acceptance={
            "503_research_identities_preserved": True,
            "security_key_v1_unchanged": True,
            "no_time_varying_identifier_as_master": True,
            "eight_repair_rules_explicit": len(repairs) == 8,
            "audited_boundaries_represented_independently": True,
            "apo_composition_break_explicit": True,
            "crh_adrc_cs_explicit": True,
            "not_audited_legal_dates_preserved": True,
            "v6_ticker_at_time_preserved": True,
            "wi_exclusions_preserved": True,
            "supplemental_remains_non_canonical": True,
            "global_503_anomaly_audit_specified": True,
            "candidate_first_workflow_specified": True,
            "negative_guards_specified": True,
            "no_v7_data_written": True},

        mutations=dict(v6=0, security_key_v1=0, universe=0, canonical_raw=0,
                       supplemental_quarantine=0, derived_writes=0, v7_rows_written=0,
                       v7_canonical_sealed=False, builder_spec_v2_sealed=False,
                       smoke_executed=False),

        downstream_sequence=["V7 candidate construction (NON-CANONICAL)",
                             "FULL LINEAGE CONFORMANCE / 503 anomaly audit",
                             "MASSIVE_MIXED_VINTAGE_SUPPLEMENTATION_POLICY_V1",
                             "supplemental integration decision",
                             "Builder Spec V2"],
        why_builder_cannot_follow_v7_directly=dict(
            problem="V7 extends expected coverage over 5,069 security-sessions whose bars "
                    "exist only in the quarantine at a different retrieval vintage",
            consequence="expected minute + canonical original bar absent -> UNOBSERVED, so "
                        "nearly the entire recovered history would be unusable",
            magnitude_rows=1875525,
            resolution="this is the CORRECT behaviour of an honest builder; the fix belongs "
                       "in the mixed-vintage policy gate, NOT inside V7"),
        next_step="build NON-CANONICAL V7 candidate, then run full-universe conformance. "
                  "Do NOT seal canonical V7 automatically.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_SECURITY_LINEAGE_V7_SPEC.json",
                 required=("spec_id", "status", "authoritative_inputs", "core_model",
                           "logical_datasets", "interval_convention",
                           "eight_case_repair_rules", "apo_structural_break",
                           "crh_instrument_transition", "supplemental_quarantine_policy",
                           "global_anomaly_audit", "negative_guards", "acceptance",
                           "mutations"),
                 supersede=os.path.exists("MASSIVE_SECURITY_LINEAGE_V7_SPEC.json"))
    ok = all(p["acceptance"].values())
    print(f"MASSIVE_SECURITY_LINEAGE_V7_SPEC · {d} · {p['status']}")
    print(f"  input digests verified : {len(binding)}/{len(binding)} MATCH")
    print(f"  logical datasets       : {len(p['logical_datasets'])}")
    print(f"  eight repair rules     : {len(repairs)}")
    print(f"  negative guards        : {len(p['negative_guards'])}")
    print(f"  acceptance             : {'ALL PASS' if ok else p['acceptance']}")
    print(f"  v7 rows written        : 0 · canonical sealed: False")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
