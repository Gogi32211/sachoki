"""MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V1 — freeze the builder before it writes anything.

Sealing the spec (and the builder's code hash) before production means a later run cannot
quietly change semantics and still claim this spec's authority: a modified builder produces
a different code hash, which fails partition validation and forces a reseal.

Nothing is built here. The next authorised step is a scratch-only smoke build; the full
1254-session production run stays HOLD until that passes.
"""
from __future__ import annotations
import json, os, sys, time                                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402
from derived_base_builder import (BUILDER_VERSION, DATASETS, OUT, SCRATCH,  # noqa: E402
                                  builder_code_hash)

INPUTS = {
    "raw_integrity": "MASSIVE_1M_RAW_INTEGRITY_V1.json",
    "universe_snapshot": "SP500_CURRENT_SNAPSHOT_V1.json",
    "ticker_lineage": "MASSIVE_TICKER_LINEAGE_V6.json",
    "source_ticker_boundary": "MASSIVE_SOURCE_TICKER_BOUNDARY_AMENDMENT_V1.json",
    "derived_semantics": "MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1.json",
    "security_identity": "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json",
    "session_calendar": "US_EQUITY_SESSION_CALENDAR_V1.json",
}
IDENTITY_DIGEST = "04e28f9570a39282"

# Smoke selection — frozen HERE, so it cannot be chosen later to suit the outcome.
SMOKE = {
    "2024-05-15": "normal full session; boundary bars; extended hours; sparse names",
    "2024-11-29": "early close (210 slots); boundary bar at 13:00 ET",
    "2023-06-07": "frozen rename-boundary UNOBSERVED case (FISV/FI)",
    "2023-01-05": "NOT_YET_REGULAR_WAY securities present (GEHC listed 2023-01)",
    "2024-04-02": "WHEN_ISSUED interval adjacent (GEV/SOLV spin-offs)",
}


def main():
    missing = [k for k, v in INPUTS.items() if not os.path.exists(v)]
    if missing:
        raise SystemExit(f"HOLD — unresolved frozen inputs: {missing}")
    digests = {k: ART.file_digest(v) for k, v in INPUTS.items()}
    ident = json.load(open(INPUTS["security_identity"]))
    if digests["security_identity"] != IDENTITY_DIGEST:
        raise SystemExit(f"HOLD — identity amendment digest is "
                         f"{digests['security_identity']}, expected {IDENTITY_DIGEST}")
    basis = {}
    for r in ident["mapping"]:
        basis[r["identity_basis"]] = basis.get(r["identity_basis"], 0) + 1

    p = dict(
        spec_id="MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V1",
        status="DERIVED_BASE_BUILDER_SPEC_FROZEN",
        builder=dict(module="backend/derived_base_builder.py",
                     version=BUILDER_VERSION, code_hash=builder_code_hash(),
                     rule="any semantic or execution-relevant change requires a new code "
                          "hash and a spec reseal; modified code must never run under this "
                          "sealed identity"),
        frozen_inputs=dict(artifacts=INPUTS, digests=digests,
                           resolution="exact — a missing or differing digest is HOLD, "
                                      "never substitution of a newer nearby artifact"),
        identity=dict(
            key="security_key_v1", amendment_digest=IDENTITY_DIGEST,
            recipe='SHA256(UTF-8("CIK=" + <10-digit CIK> + "|SCFIGI=" + '
                   '<share_class_figi or frozen sentinel>))',
            inventory=dict(securities=len(ident["mapping"]),
                           distinct_keys=len({r["security_key_v1"]
                                              for r in ident["mapping"]}),
                           identity_basis=basis),
            builder_must_not=["invent another identity key",
                              "use ticker as permanent identity", "use CIK alone",
                              "join on NULL-bearing (CIK, share_class_figi)",
                              "populate or redefine legacy security_id",
                              "alter the serialisation or the sentinel",
                              "recompute identity under different semantics"],
            builder_must_preserve=["security_key_v1", "identity_basis", "cik_normalized",
                                   "share_class_figi_normalized"],
            identity_basis_is_not_decoration="474 keys rest on a share-class identifier "
                                             "and 29 on a CIK being a singleton in this "
                                             "universe; these are different evidentiary "
                                             "strengths and the field must survive into "
                                             "canonical derived tables"),
        logical_datasets={
            "security_sessions": dict(
                primary_key=["security_key_v1", "session_date"],
                grain="one row per in-scope security-session pair",
                fields=["security_key_v1", "identity_basis", "cik_normalized",
                        "share_class_figi_normalized", "frozen_current_ticker",
                        "session_date", "eligibility_state",
                        "source_ticker_access_state",
                        "source_ticker_used_in_original_archive", "raw_ingest_pair_state",
                        "expected_regular_minute_count", "xnys_open_utc",
                        "xnys_close_utc", "raw_payload_sha256", "raw_manifest"]),
            "minute_states": dict(
                primary_key=["security_key_v1", "session_date", "bar_start_ms_utc"],
                grain="expected regular-minute lattice for REGULAR_WAY_EXPECTED only",
                carries_no_ohlcv=True,
                fields=["security_key_v1", "identity_basis", "session_date",
                        "bar_start_ms_utc", "eligibility_state", "observation_state",
                        "unobserved_reason", "source_ticker_access_state",
                        "observed_bar_present"]),
            "observed_regular_bars": dict(
                primary_key=["security_key_v1", "bar_start_ms_utc"],
                grain="ONLY emitted original-vintage vendor rows on expected regular slots",
                preservation="source OHLCV exactly — no rounding, repair, winsorisation "
                             "or fill",
                fields=["security_key_v1", "identity_basis", "session_date",
                        "bar_start_ms_utc", "bar_scope", "open", "high", "low", "close",
                        "volume", "source_ticker", "raw_payload_sha256"]),
            "session_close_boundary_bars": dict(
                grain="the optional bar stamped exactly at session close",
                expected_per_security_session="0 or 1; more than 1 is HOLD",
                never=["counted as a 391st or 211th regular minute",
                       "merged into the final regular bar", "labelled an auction bar"]),
            "extended_hours_bars": dict(
                grain="observations outside the XNYS regular lattice",
                never=["satisfying regular-minute expected coverage",
                       "converting an RTH UNOBSERVED minute to OBSERVED"]),
        },
        session_lattice=dict(
            source="XNYS calendar only — never vendor bars",
            convention="BAR_START, interval [session_open, session_close)",
            normal=390, early_close=210,
            not_hardcoded="lengths are generated from the frozen calendar; an unexpected "
                          "calendar-defined length is reported, never coerced"),
        observation_rules=dict(
            observed="an original-vintage vendor regular bar exists under valid access",
            unobserved="absent expected minute, or access not established as VALID",
            default_reason="SOURCE_COMPLETENESS_UNPROVEN",
            structural_no_trade=dict(
                defined=True, classification_capability="UNAVAILABLE",
                production_invariant="STRUCTURAL_NO_TRADE rows = 0",
                enforcement="emitting one is a BUILD FAILURE")),
        five_boundary_sessions=dict(
            treatment="remain REGULAR_WAY_EXPECTED and inside expected coverage, with "
                      "observation_state UNOBSERVED and reason SOURCE_REFERENCE_CONFLICT",
            forbidden=["consuming diagnostic successor-ticker bars",
                       "repairing the raw archive",
                       "removing the sessions from expected coverage"]),
        no_synthetic_market_data=dict(
            absolute=True,
            forbidden=["volume = 0 for a missing minute", "flat candle",
                       "previous close", "OHLC carry-forward", "backfill",
                       "interpolation", "synthetic observation"],
            allowed="expected-minute metadata rows in minute_states, carrying no OHLCV"),
        source_row_exhaustiveness=dict(
            scopes=["REGULAR_SESSION_MINUTE", "SESSION_CLOSE_BOUNDARY_BAR",
                    "EXTENDED_HOURS_BAR"],
            unclassified_allowed=0,
            ambiguity="HOLD — no fourth production scope may be invented during execution"),
        vwap_and_transaction_count=dict(
            canonical_vwap="DISABLED — vendor vw failed P7 and a true VWAP cannot be "
                           "reconstructed from OHLCV",
            transaction_count="HOLD — n semantics unresolved",
            derived_base_schema="omits both from canonical feature-facing tables; they "
                                "remain in the immutable raw archive"),
        provenance=dict(
            per_observed_bar=["security_key_v1", "session_date", "bar_start_ms_utc",
                              "source_ticker", "raw_payload_sha256"],
            per_partition=["input_raw_payload_sha256", "builder_code_hash", "spec_digest",
                           "row counts by dataset", "per-dataset output digest"],
            level="FILE-level provenance to the raw session payload; row-level byte "
                  "provenance is NOT implemented and is not claimed"),
        output=dict(
            production_path=OUT, smoke_path=SCRATCH,
            namespace="new versioned namespace; overwrites no raw, Studio, research or "
                      "existing derived data",
            format="parquet, zstd", partitioning="session-major, one directory per session",
            manifest="_partition_manifest.json per partition",
            digest_algorithm="sha256"),
        write_safety=dict(
            mount_guard="must PASS before any mkdir or write",
            no_import_time_mkdir=True, no_internal_fallback=True,
            no_phantom_volume_recovery=True),
        resume=dict(
            skip_only_if=["partition manifest exists", "finalized=true",
                          "all expected files present", "every output digest matches",
                          "spec_digest matches", "builder_code_hash matches"],
            on_mismatch="HOLD — never overwrite a mismatched finalized partition",
            partial_output="staging directories can never masquerade as finalized"),
        atomic_finalization=dict(
            protocol="write to _staging/<session>, validate, then atomically rename into "
                     "the canonical partition path",
            crash_behaviour="an interrupted run leaves a clearly non-canonical staging "
                            "directory"),
        smoke_plan=dict(
            sessions=list(SMOKE), rationale=SMOKE,
            frozen_here="the selection is fixed in this sealed spec so it cannot later be "
                        "chosen to suit the outcome",
            output="SCRATCH / NON-CANONICAL ONLY",
            coverage=["normal session", "early close", "boundary bar present",
                      "extended hours", "NOT_YET_REGULAR_WAY", "WHEN_ISSUED",
                      "frozen rename-boundary UNOBSERVED case",
                      "dual-class same-CIK identities", "sparse session"]),
        negative_guards=[
            "CIK-only collision", "ticker permanent identity",
            "NULL composite identity joins", "missing minute becomes zero volume",
            "missing minute gets OHLC carry-forward",
            "close-boundary bar occupies a regular slot",
            "extended-hours bar occupies a regular slot",
            "duplicate regular bars silently deduplicated",
            "rename-boundary sessions removed from coverage",
            "new-vintage successor bars consumed",
            "STRUCTURAL_NO_TRADE generated", "write while external mount absent",
            "mismatched finalized partition overwritten",
            "partial partition treated as finalized"],
        count_reporting=dict(
            never_report_only="rows",
            report_separately=["security_session_pairs", "expected_regular_minute_states",
                               "observed_regular_bars", "unobserved_expected_minutes",
                               "boundary_bars", "extended_hours_bars"],
            because="an expected-minute state row is not an observed market bar, and that "
                    "distinction must survive into every downstream API"),
        no_research_claim="completing this builder proves engineering integrity, semantic "
                          "conformance and source provenance. It proves nothing about "
                          "signal validity, predictability, edge, profitability, "
                          "statistical power or replication.",
        known_limitations=[
            "STRUCTURAL_NO_TRADE has no classification capability, so UNOBSERVED will be "
            "a large share of expected minutes — a consequence of the frozen semantics, "
            "not a defect",
            "provenance is file-level, not row-level",
            "29 of 503 identities rest on the frozen-singleton basis"],
        authorises=dict(smoke_build=True, production_build=False,
                        feature_layer=False,
                        note="the next authorised step is a scratch-only real-data smoke "
                             "build; the full 1254-session run stays HOLD until it passes"),
        production_partitions_created=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V1.json",
                 required=("spec_id", "status", "builder", "frozen_inputs", "identity",
                           "logical_datasets", "observation_rules", "smoke_plan",
                           "negative_guards"),
                 supersede=os.path.exists("MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V1.json"))
    print(f"MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V1 · {d} · {p['status']}")
    print(f"  builder code hash : {p['builder']['code_hash'][:24]}…")
    print(f"  identity amendment: {IDENTITY_DIGEST} · basis {basis}")
    print(f"  datasets          : {', '.join(DATASETS)}")
    print(f"  smoke sessions    : {list(SMOKE)}")
    print(f"  production build  : {p['authorises']['production_build']}")


if __name__ == "__main__":
    main()
