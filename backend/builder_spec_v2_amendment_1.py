"""MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_AMENDMENT_1 — one guard, added for the right reason.

The gap was not found by review. It was found by a negative fixture failing: fixture J's first
run duplicated a pre-market bar, no guard fired, and the fixture reported FAIL. The fixture's
injection was itself wrong — it is specified to duplicate a REGULAR source key — but the run
had already demonstrated that `extended_hours_bars` has no uniqueness contract at all.

The tempting response was to fix the fixture and move on, arguing that canonical raw integrity
already PASSed so a duplicated source bar cannot occur in practice. That argument quietly
relocates part of the builder's correctness into an upstream artifact: it makes the derived
layer correct only for as long as the ingest gate's guarantee holds, without saying so
anywhere. A uniqueness contract that exists only as a property of the input is not a contract.

THE KEY IS THREE PARTS, NOT TWO. Duplicate detection is scoped to
(security_key_v1, source_ticker_at_time, timestamp) rather than to the security and timestamp
alone. This programme has already established that one research security can have two
legitimate simultaneous trading lines — GEHC, SOLV and VLTO each traded a when-issued line and
a regular-way line on the same session under different tickers. A two-part key would treat that
coexistence as a duplicate and fail closed on correct data. The guard targets one source line
repeating one timestamp; it does not forbid distinct source lines from sharing a minute.

NO DEDUPLICATION IS PERMITTED. Not keep-first, not keep-last, not aggregate, not average. A
duplicated raw bar means the source is not what the archive claims, and silently resolving it
would destroy the evidence that something is wrong while producing output that looks fine.
"""
from __future__ import annotations
import json, os, sys, time                                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

BASE = "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2.json"
BASE_DIGEST = "3af8cc42370fda86"
SMOKE = "MASSIVE_1M_DERIVED_BASE_SMOKE_V2.json"
SMOKE_DIGEST = "9901acea7485ee7e"


def main():
    if ART.file_digest(BASE) != BASE_DIGEST:
        print("HOLD — base spec digest mismatch"); return 1
    if ART.file_digest(SMOKE) != SMOKE_DIGEST:
        print("HOLD — smoke digest mismatch"); return 1
    base = json.load(open(BASE))

    p = dict(
        spec_id="MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_AMENDMENT_1",
        status="MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_AMENDMENT_1_FROZEN",
        task_class="SPECIFICATION_AMENDMENT_ONLY",
        scope="NARROW — adds exactly one fail-closed condition and one negative fixture",
        amends=dict(base_spec="MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2",
                    base_digest=BASE_DIGEST, base_overwritten=False,
                    everything_else_unchanged=True),

        provenance_of_the_finding=dict(
            found_by="a negative fixture failing, not by review",
            what_happened="fixture J's first run duplicated results[100], which for AAPL is "
                          "a pre-market bar (134 precede the open). It landed in extended "
                          "hours, no guard fired, and the fixture reported FAIL.",
            two_separate_facts=[
                "the fixture's injection was wrong — it is specified to duplicate a REGULAR "
                "source key — and has been corrected",
                "extended_hours_bars genuinely has no uniqueness contract, which the failing "
                "run exposed"],
            rejected_argument=dict(
                argument="canonical raw integrity already PASSed, so a duplicated source bar "
                         "cannot occur in practice",
                why_rejected="that quietly relocates part of the builder's correctness into "
                             "an upstream artifact. It makes the derived layer correct only "
                             "while the ingest gate's guarantee holds, without recording "
                             "that dependency anywhere. A uniqueness contract that exists "
                             "only as a property of the input is not a contract.")),

        added_fail_closed_condition=dict(
            dataset="EXTENDED_HOURS_BAR source rows",
            rule="duplicate raw source keys are FATAL",
            duplicate_key=["security_key_v1", "source_ticker_at_time", "timestamp"],
            on_violation="HOLD",
            forbidden_resolutions=["deduplicate by keeping first",
                                   "deduplicate by keeping last",
                                   "aggregate duplicates", "average OHLCV",
                                   "silently drop one row"],
            why_no_deduplication="a duplicated raw bar means the source is not what the "
                                 "archive claims; silently resolving it destroys the "
                                 "evidence while producing output that looks fine"),

        why_the_key_has_three_parts=dict(
            rejected_key=["security_key_v1", "timestamp"],
            reason="one research security can have two legitimate simultaneous trading "
                   "lines. GEHC, SOLV and VLTO each traded a when-issued line and a "
                   "regular-way line on the same session under different tickers — "
                   "established by MASSIVE_SECURITY_LINEAGE_ANOMALY_QUALIFICATION_V1.",
            consequence_of_two_part_key="correct data would fail closed, because coexistence "
                                        "would read as duplication",
            target="one source line repeating one timestamp",
            not_forbidden="distinct source lines sharing a minute"),

        uniqueness_invariants_consolidated=dict(
            REGULAR_SESSION="duplicate source key => HOLD",
            SESSION_CLOSE_BOUNDARY=">1 eligible boundary bar per security-session => HOLD",
            EXTENDED_HOURS="duplicate source key within the same source line => HOLD",
            note="the first two already existed; the third is added here. They are stated "
                 "together so no output class is left without a uniqueness contract."),

        added_negative_fixture=dict(
            id="M", name="DUPLICATE_EXTENDED_SOURCE_KEY", kind="NEGATIVE",
            construction="take one CONFIRMED pre- or post-market source bar and duplicate "
                         "that exact source row",
            precondition="the fixture MUST first prove the chosen bar is genuinely an "
                         "EXTENDED_HOURS_BAR before duplicating it",
            why_the_precondition="fixture J failed precisely because it assumed the class of "
                                 "the bar it picked instead of verifying it; repeating that "
                                 "mistake in the fixture written to catch it would be "
                                 "absurd",
            dangerous_implementation="accepts both rows => fixture FAILS",
            correct_implementation="raises the duplicate-extended-source-key guard => "
                                   "fixture PASSES"),

        smoke_consequence=dict(
            rule="the amendment changes the implementation, so the frozen rule applies in "
                 "full",
            old_implementation_hash="0a5dd258897cf344",
            requires="a FULL smoke rerun from scratch under a new implementation hash",
            partial_rerun_forbidden="running only the new fixture is not sufficient",
            fixture_count_after=13,
            composition="7 positive + 5 existing negative + 1 new negative"),

        supersedes_for_production_authorization=dict(
            artifact="MASSIVE_1M_DERIVED_BASE_SMOKE_V2", digest=SMOKE_DIGEST,
            disposition="PASS_UNDER_PRE_AMENDMENT_SPEC",
            status_for_production="SUPERSEDED_FOR_PRODUCTION_AUTHORIZATION",
            historically_valid=True, turned_into_a_failure=False, deleted=False,
            artifact_bytes_mutated=False,
            recording_method="recorded here and in the successor smoke rather than by "
                             "editing the sealed artifact, so its digest stays "
                             "verifiable"),

        unchanged_from_base=[
            "cohort binding (476, 1f86d09a76d3c6e9)", "scoped V6 authority",
            "source restriction to original canonical raw",
            "STRUCTURAL_NO_TRADE remains UNAVAILABLE", "no synthetic OHLCV",
            "calendar-determined close boundary", "extended hours stored separately",
            "VWAP disabled", "transaction-count features HOLD",
            "file-level provenance", "partitioning and restart semantics",
            "claim scope"],
        base_fail_closed_count_before=len(base["fail_closed_conditions"]),
        base_fail_closed_count_after=len(base["fail_closed_conditions"]) + 1,

        acceptance={
            "narrow_scope_one_guard": True,
            "three_part_key": True,
            "no_deduplication_permitted": True,
            "fixture_M_defined": True,
            "fixture_M_requires_class_precondition": True,
            "full_smoke_rerun_required": True,
            "prior_smoke_preserved_not_failed": True,
            "base_spec_not_overwritten": True,
            "production_writes_zero": not os.path.exists(
                "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_1m_base_v2")},
        mutations=dict(base_spec=0, prior_smoke_artifact=0, canonical_raw=0,
                       derived_writes=0, cohort=0),
        next_step="freeze the new implementation hash, rerun the FULL smoke (13 fixtures) "
                  "from scratch, then independent verification. Production remains HOLD "
                  "until that rerun PASSes.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    ok = all(p["acceptance"].values())
    p["status"] = (p["status"] if ok else "HOLD")
    d = ART.seal(p, "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_AMENDMENT_1.json",
                 required=("spec_id", "status", "amends", "added_fail_closed_condition",
                           "why_the_key_has_three_parts", "added_negative_fixture",
                           "smoke_consequence", "supersedes_for_production_authorization",
                           "acceptance", "mutations"),
                 supersede=os.path.exists(
                     "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_AMENDMENT_1.json"))
    print(f"MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_AMENDMENT_1 · {d} · {p['status']}")
    print(f"  added guard      : EXTENDED_HOURS duplicate source key -> HOLD")
    print(f"  key              : (security_key_v1, source_ticker_at_time, timestamp)")
    print(f"  fail-closed      : {p['base_fail_closed_count_before']} -> "
          f"{p['base_fail_closed_count_after']}")
    print(f"  fixtures after   : 13 (7 pos + 6 neg)")
    print(f"  prior smoke      : {SMOKE_DIGEST} PASS_UNDER_PRE_AMENDMENT_SPEC, preserved")
    print(f"  acceptance       : {'ALL PASS' if ok else p['acceptance']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
