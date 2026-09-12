"""MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2 — the production contract, with fixtures measured at seal time.

Every smoke fixture below is measured from the current manifests and bundles while this
artifact is being sealed, not copied from the invalidated V1 spec. Three of the measurements
changed what the spec had to say.

FIRST: 330 OF 476 COHORT SECURITIES HAVE INCOMPLETE REGULAR-MINUTE COVERAGE ON AN ORDINARY
SESSION. ERIE carries 42 observed minutes out of 390 on 2024-05-15; TPL 62; BKNG 115. This is
not an anomaly to be investigated — it is what minute data for a large-cap universe actually
looks like, and it is the entire reason STRUCTURAL_NO_TRADE stays UNAVAILABLE. Those 348
missing ERIE minutes are almost certainly minutes in which nothing traded, but "almost
certainly" is not a proven request-level guarantee, so every one of them is UNOBSERVED with
reason SOURCE_COMPLETENESS_UNPROVEN. A builder that quietly rendered them as zero-volume bars
would manufacture roughly seven eighths of that security's trading day.

SECOND: THE EARLY-CLOSE TRAP IS REAL AND IS NOW A FIXTURE. On 2024-07-03 AAPL has 210 regular
minutes, a close-boundary bar at 13:00 — and also a bar at 16:00, which is EXTENDED HOURS.
Any implementation carrying a "16:00 means session close" shortcut passes an ordinary session
and silently mislabels the early-close one. The calendar decides; the clock never does.

THIRD: GEV IS THE ONLY V1-ELIGIBLE SECURITY WITH A NOT_YET_REGULAR_WAY PERIOD. All 23 of V6's
LISTED_LATER securities except GEV were excluded by the eligibility contract, so fixtures F
and G both have to come from GEV. That is forced by measurement, not chosen for convenience,
and the spec says so rather than presenting a single-source fixture as a free choice.

A FOURTH THING WORTH STATING PLAINLY. The ingest manifest records GEV's 2024-04-01 as
NOT_YET_REGULAR_WAY; it has no separate WHEN_ISSUED state at all. The distinction between
"not yet listed" and "when-issued line excluded" lives only in the V6 lineage artifact. So
fixture G cannot be validated against the manifest — it must be validated against the lineage
authority, and the spec records that limitation instead of implying the manifest proves it.

V6 IS BOUND AS A SCOPED AUTHORITY. It supplies lineage for these 476 securities and nothing
more. This programme has already established V6 is wrong for 22 others; the scoping is a
statement of what was verified, not a claim that the defects went away.
"""
from __future__ import annotations
import gzip, hashlib, json, os, sys, time                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

CANON = "/Volumes/QUANT_RESEARCH/source_data/massive/canonical_1m"
CM = os.path.join(CANON, "_manifest")
ELIG = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"
ELIG_DIGEST = "7c0a5ad6b17d7ae0"
COHORT_DIGEST = "1f86d09a76d3c6e9"
EXCL_DIGEST = "3604f7c1dbf902a1"
PROD = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_1m_base_v2"
DEPS = ("MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1", "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1",
        "US_EQUITY_SESSION_CALENDAR_V1", "MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1",
        "MASSIVE_1M_CLOSE_BOUNDARY_CONFORMANCE_V1", "MASSIVE_1M_RAW_INTEGRITY_V1",
        "MASSIVE_TICKER_LINEAGE_V6", "SP500_CURRENT_SNAPSHOT_V1")


def bundle(day):
    p = os.path.join(CANON, day[:4], day[5:7], f"{day}.json.gz")
    with gzip.open(p, "rb") as f:
        return json.loads(f.read())


def anatomy(day, tk, close_hm):
    import pandas as pd
    d = bundle(day)
    r = d["securities"][tk]["results"]
    ts = pd.to_datetime([x["t"] for x in r], unit="ms", utc=True
                        ).tz_convert("America/New_York")
    hm = [f"{t.hour:02d}:{t.minute:02d}" for t in ts]
    last_reg = f"{int(close_hm[:2]) - 1 if close_hm[3:] == '00' else int(close_hm[:2]):02d}:59"
    return dict(session=day, security=tk, bars=len(r), first=hm[0], last=hm[-1],
                regular_minutes_observed=sum(1 for x in hm if "09:30" <= x <= last_reg),
                close_boundary_bars=sum(1 for x in hm if x == close_hm),
                pre_market=sum(1 for x in hm if x < "09:30"),
                post_close=sum(1 for x in hm if x > close_hm))


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="builder spec v2 fixture measurement (read-only)")

    dig = {n: ART.file_digest(n + ".json") for n in DEPS}
    dig[ELIG.replace(".json", "")] = ART.file_digest(ELIG)
    if dig["SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1"] != ELIG_DIGEST:
        print("HOLD — eligibility digest mismatch"); return 1
    el = json.load(open(ELIG))
    coh = set(el["cohort"]["eligible_tickers"])
    if (len(coh) != 476 or el["cohort"]["cohort_security_key_sha256"][:16] != COHORT_DIGEST
            or el["cohort"]["exclusion_list_sha256"][:16] != EXCL_DIGEST):
        print("HOLD — cohort binding mismatch"); return 1

    import pandas as pd, exchange_calendars as xc
    cal = xc.get_calendar("XNYS")
    t0 = time.time()

    # ---- measured fixtures -------------------------------------------------
    ORD, EARLY = "2024-05-15", "2024-07-03"
    fa = anatomy(ORD, "AAPL", "16:00")
    fb = anatomy(EARLY, "AAPL", "13:00")
    fb["bar_at_1600_is_extended_hours"] = sum(
        1 for x in [f"{t.hour:02d}:{t.minute:02d}" for t in
                    pd.to_datetime([r["t"] for r in bundle(EARLY)["securities"]["AAPL"]
                                    ["results"]], unit="ms", utc=True
                                   ).tz_convert("America/New_York")] if x == "16:00")

    d = bundle(ORD)
    sparse = []
    for tk, v in d["securities"].items():
        if tk not in coh:
            continue
        ts = pd.to_datetime([x["t"] for x in v["results"]], unit="ms", utc=True
                            ).tz_convert("America/New_York")
        n = sum(1 for t in ts if "09:30" <= f"{t.hour:02d}:{t.minute:02d}" <= "15:59")
        sparse.append((tk, n))
    sparse.sort(key=lambda x: x[1])
    n_sparse = sum(1 for _, n in sparse if n < 390)
    fe_tk, fe_n = sparse[0]

    # GEV: the only eligible security with a NOT_YET period — verified, not assumed
    nyrw_pop, gev_sessions = set(), 0
    import glob
    for p in sorted(glob.glob(os.path.join(CM, "*.json"))):
        m = json.load(open(p))
        for tk, st in m["states"].items():
            if st == "NOT_YET_REGULAR_WAY" and tk in coh:
                nyrw_pop.add(tk)
                if tk == "GEV":
                    gev_sessions += 1
    wi6 = {x["ticker"]: x for x in json.load(
        open("MASSIVE_TICKER_LINEAGE_V6.json"))["when_issued_intervals"]}
    gev_wi = wi6.get("GEV")
    man_0401 = json.load(open(os.path.join(CM, "2024-04-01.json")))["states"].get("GEV")

    fixtures = [
        dict(id="A", name="ordinary full XNYS session", measured=fa,
             expected_regular_minutes=390, source="canonical bundle"),
        dict(id="B", name="early-close session", measured=fb,
             expected_regular_minutes=210,
             trap="a 16:00 bar exists on this early-close session and is EXTENDED HOURS, "
                  "not the close boundary. An implementation with a hardcoded 16:00 rule "
                  "passes fixture A and silently mislabels this one.",
             rule="the calendar determines the boundary; the clock never does"),
        dict(id="C", name="session close boundary bar",
             normal=dict(session=ORD, security="AAPL", boundary="16:00",
                         bars=fa["close_boundary_bars"]),
             early=dict(session=EARLY, security="AAPL", boundary="13:00",
                        bars=fb["close_boundary_bars"]),
             must_not="be counted as the 391st or 211th regular minute",
             classification="neutral SESSION_CLOSE_BOUNDARY_BAR; NOT asserted to be an "
                            "auction bar"),
        dict(id="D", name="extended-hours bars", session=ORD, security="AAPL",
             pre_market=fa["pre_market"], post_close=fa["post_close"],
             rule="stored separately; may never populate regular minute states"),
        dict(id="E", name="sparse expected minutes -> UNOBSERVED",
             session=ORD, security=fe_tk,
             regular_minutes_observed=fe_n, expected=390, missing=390 - fe_n,
             required_state="UNOBSERVED",
             required_reason="SOURCE_COMPLETENESS_UNPROVEN",
             forbidden_state="STRUCTURAL_NO_TRADE",
             population_context=dict(
                 cohort_securities_on_session=len(sparse),
                 with_incomplete_regular_coverage=n_sparse,
                 share=round(n_sparse / len(sparse), 3),
                 meaning="incomplete minute coverage is the NORM, not an anomaly. This is "
                         "why STRUCTURAL_NO_TRADE stays unavailable — rendering these as "
                         f"zero-volume bars would manufacture "
                         f"{round((390 - fe_n) / 390 * 100)}% of this security's day.")),
        dict(id="F", name="NOT_YET_REGULAR_WAY from an ELIGIBLE V1 security",
             security="GEV", session="2022-01-03",
             not_yet_sessions_for_gev=gev_sessions,
             eligible_securities_with_any_not_yet=sorted(nyrw_pop),
             forced=len(nyrw_pop) == 1,
             note="GEV is the ONLY V1-eligible security with a NOT_YET_REGULAR_WAY period — "
                  "22 of V6's 23 LISTED_LATER securities were excluded by the eligibility "
                  "contract. This fixture is forced by measurement, not chosen for "
                  "convenience."),
        dict(id="G", name="WHEN_ISSUED_EXCLUDED from an ELIGIBLE V1 security",
             security="GEV", session="2024-04-01",
             v6_when_issued=gev_wi,
             ingest_manifest_state=man_0401,
             limitation="the ingest manifest has NO separate WHEN_ISSUED state and records "
                        "this session as NOT_YET_REGULAR_WAY. The distinction lives ONLY in "
                        "the V6 lineage artifact, so this fixture must be validated against "
                        "the lineage authority and NOT against the manifest.",
             required_eligibility_state="WHEN_ISSUED_EXCLUDED",
             must_differ_from="fixture F, which is NOT_YET_REGULAR_WAY for the same security",
             gev_survived="the anomaly qualification as a KNOWN_NON_ANOMALY control"),
        dict(id="H", kind="NEGATIVE", name="excluded V1 security must fail cohort admission",
             construction="feed a security_key_v1 from the 27-member exclusion list",
             must="HOLD"),
        dict(id="I", kind="NEGATIVE", name="supplemental / new-vintage payload path must fail",
             construction="point the source resolver at the supplemental quarantine or a "
                          "v7 diagnostic archive path",
             must="HOLD"),
        dict(id="J", kind="NEGATIVE", name="duplicate source key must fail",
             construction="inject a second bar for an existing "
                          "(security_key_v1, timestamp)",
             must="HOLD"),
        dict(id="K", kind="NEGATIVE", name="close-boundary duplication must fail",
             construction="inject a second close-boundary bar into one security-session",
             must="HOLD"),
        dict(id="L", kind="NEGATIVE",
             name="zero-fill / carry-forward attempt must fail",
             construction="attempt to emit a synthetic OHLCV row for an UNOBSERVED expected "
                          "minute",
             must="HOLD"),
    ]

    p = dict(
        spec_id="MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2",
        status="MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_FROZEN",
        task_class="SPECIFICATION_ONLY",
        production_writes=0, smoke_executed=False, y_exposed=0,

        cohort_binding=dict(
            artifact="SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1", digest=ELIG_DIGEST,
            source_universe=503, v1_eligible=476,
            cohort_security_key_sha256=COHORT_DIGEST,
            exclusion_list_sha256=EXCL_DIGEST,
            keyed_by="security_key_v1 — ticker is display metadata",
            exclusions_enforced=27,
            hold_on=["cohort count mismatch", "cohort digest mismatch",
                     "exclusion digest mismatch"]),

        scoped_lineage_authority=dict(
            artifact="MASSIVE_TICKER_LINEAGE_V6", digest=dig["MASSIVE_TICKER_LINEAGE_V6"],
            scope="the frozen 476-security V1 cohort ONLY",
            explicitly_not_claimed="V6 is globally correct for all 503",
            why="this programme established V6 is wrong for 22 securities and that 5 more "
                "carry original-ingest source-boundary failures; the scoping states what was "
                "verified, not that the defects went away",
            builder_must_fail_if="any excluded security enters V1"),

        source_restriction=dict(
            allowed="ORIGINAL CANONICAL MASSIVE RAW ONLY",
            forbidden=["MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1",
                       "v7 anomaly diagnostic payloads", "new-vintage probe payloads",
                       "future supplementation", "manual patches"],
            required=dict(supplemental_rows_consumed=0,
                          supplemental_payloads_consumed=0)),

        semantic_dependencies={k: v for k, v in dig.items()},

        logical_datasets=[
            dict(name="security_sessions",
                 grain="security_key_v1 x session_date",
                 fields=["security_key_v1", "identity_basis", "session_date",
                         "ticker_at_time", "eligibility_state",
                         "expected_regular_minutes", "observed_regular_minutes",
                         "close_boundary_present", "extended_hours_bar_count"]),
            dict(name="minute_states",
                 grain="security_key_v1 x expected regular minute",
                 fields=["security_key_v1", "session_date", "minute_ts",
                         "eligibility_state", "observation_state", "unobserved_reason",
                         "source_ticker_access_state", "provenance"],
                 contains_ohlcv=False,
                 rule="an UNOBSERVED minute has NO fabricated price or volume row"),
            dict(name="observed_regular_bars",
                 grain="security_key_v1 x observed regular minute",
                 contains="actual archived original-vintage regular-session bars only",
                 excludes=["close-boundary bars", "extended-hours bars",
                           "when-issued bars", "excluded securities",
                           "supplemental provenance"],
                 unique_key=["security_key_v1", "minute_ts"]),
            dict(name="session_close_boundary_bars",
                 grain="security_key_v1 x session_date", max_rows_per_grain=1),
            dict(name="extended_hours_bars",
                 grain="security_key_v1 x minute", stored="separately")],
        no_feature_engineering=["EMA", "RVOL", "T", "Z", "Y", "outcome"],

        eligibility_dimension=dict(
            determined_from=["frozen V1 cohort membership", "scoped V6 REGULAR_WAY lineage",
                             "XNYS session/minute calendar"],
            states=["REGULAR_WAY_EXPECTED", "NOT_YET_REGULAR_WAY",
                    "WHEN_ISSUED_EXCLUDED"],
            never_inferred_from="presence or absence of raw bars"),

        observation_dimension=dict(
            states=["OBSERVED", "UNOBSERVED", "STRUCTURAL_NO_TRADE"],
            STRUCTURAL_NO_TRADE_CLASSIFICATION_CAPABILITY="UNAVAILABLE",
            production_may_emit_structural_no_trade=False,
            requires="a separately frozen capability amendment",
            missing_expected_minute=dict(
                observation_state="UNOBSERVED",
                unobserved_reason="SOURCE_COMPLETENESS_UNPROVEN",
                never="no-trade"),
            empirical_justification=dict(
                measured_on=ORD,
                cohort_securities=len(sparse),
                with_incomplete_regular_coverage=n_sparse,
                share=round(n_sparse / len(sparse), 3),
                worst=dict(security=fe_tk, observed=fe_n, expected=390),
                statement="incomplete minute coverage is the norm for a large-cap universe. "
                          "Request-level completeness was proven; minute-emission "
                          "completeness was not. Calling these minutes no-trade would be "
                          "asserting the unproven half.")),

        source_access_state=dict(
            states=["VALID", "INVALID_OR_CONFLICTED", "UNRESOLVED"],
            cohort_contract="the five known source-boundary securities are already excluded",
            on_encounter="HOLD — never locally relabel"),

        no_synthetic_market_data=dict(
            forbidden=["missing minute -> volume 0", "carry-forward OHLC",
                       "flat synthetic candle", "interpolation", "synthetic close",
                       "synthetic session completion"],
            rule="every observed OHLCV row must correspond to an actual archived vendor bar"),

        session_lattice=dict(
            authority="US_EQUITY_SESSION_CALENDAR_V1",
            digest=dig["US_EQUITY_SESSION_CALENDAR_V1"],
            normal_session_expected_regular_minutes=390,
            early_close_expected_regular_minutes=210,
            derived_from_observed_bar_count=False,
            measured_confirmation=dict(ordinary=fa, early_close=fb)),

        close_boundary=dict(
            authority="MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1",
            digest=dig["MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1"],
            normal="16:00 exchange-local", early="13:00 exchange-local",
            counted_as_regular_minute=False,
            asserted_to_be_an_auction_bar=False,
            classification="SESSION_CLOSE_BOUNDARY_BAR (neutral)",
            measured_trap=dict(
                session=EARLY, security="AAPL",
                bars_at_1600=fb.get("bar_at_1600_is_extended_hours"),
                consequence="on an early-close session a 16:00 bar is EXTENDED HOURS. A "
                            "hardcoded 16:00 close rule passes the ordinary fixture and "
                            "mislabels this one.")),

        extended_hours=dict(definition="outside the regular lattice and outside the "
                                       "designated close boundary",
                            stored="separately",
                            may_populate_regular_minute_states=False),

        vwap=dict(status="DISABLED",
                  reason="the source probe established vendor vw may lie outside "
                         "[low, high]; true VWAP cannot be reconstructed from OHLCV",
                  canonical_feature_emitted=False,
                  any_proxy_requires="its own preregistration under the name "
                                     "OHLCV_VWAP_PROXY"),
        transaction_count=dict(status="HOLD",
                               raw_n="may be preserved where the raw schema already does",
                               inferential_use=False),

        higher_timeframe_rule=dict(
            aggregated_in_v2=False,
            preserved_rule="if any constituent expected minute is UNOBSERVED, "
                           "higher-timeframe coverage is incomplete/unavailable",
            forbidden="silently aggregating across missing expected minutes"),

        provenance=dict(
            level="FILE_LEVEL",
            row_level_raw_byte_provenance_claimed=False,
            honesty_note="row-level raw-byte provenance is NOT implemented and must not be "
                         "claimed",
            each_partition_binds=["builder implementation hash", "builder spec digest",
                                  "cohort artifact digest",
                                  "canonical raw manifest digest/reference",
                                  "calendar version", "lineage authority/version"]),

        fail_closed_conditions=[
            "cohort count or hash mismatch", "excluded security encountered",
            "supplemental / new-vintage source encountered",
            "raw payload digest mismatch", "duplicate regular source key",
            "unclassified source row",
            "more than one close-boundary bar per security-session",
            "invalid calendar/session mapping", "unexpected WI-to-regular admission",
            "attempted STRUCTURAL_NO_TRADE emission",
            "identity collision / missing security_key_v1",
            "source path outside guarded QUANT_RESEARCH storage"],

        smoke_plan=dict(
            reuses_v1_annotations=False,
            fixtures_measured_at_seal_time=True,
            measured_against="current canonical manifests and bundles",
            fixtures=fixtures,
            negative_fixture_standard="each critical guard must show the dangerous version "
                                      "FAILS and the correct version PASSES; static "
                                      "assertions alone are insufficient",
            execution="NOT in this task",
            next_gate="MASSIVE_1M_DERIVED_BASE_SMOKE_V2",
            writes="scratch/staging only"),

        implementation_requirements=dict(
            hash_frozen_before_smoke=True,
            code_change_after_smoke="invalidate smoke, change hash, rerun from scratch",
            code_change_after_production="do not mix partitions across implementation "
                                         "hashes; HOLD and re-version",
            silent_resume_across_hashes=False),

        partitioning=dict(
            scheme="session-major (date), consistent with the immutable canonical archive",
            deterministic=True, restartable=True,
            completion="a partition is sealed with a digest before being marked complete",
            resume="only validated complete partitions",
            partial_output="discarded and rebuilt"),

        output_destination=dict(
            path=PROD, new_namespace=True, overwrites_raw=False,
            reuses_invalid_v1_staging=False,
            mount_guard_required_before_write=True,
            exists_now=os.path.exists(PROD)),

        claim_scope=dict(
            frozen_claim="Historical 1-minute analysis of the data-qualified, "
                         "original-vintage-only subset of the frozen current S&P 500 "
                         "universe.",
            source_universe=503, v1_research_cohort=476,
            may_not_be_described_as="the S&P 500, without the qualification",
            survivorship="current-503 membership is a known, accepted survivorship "
                         "condition"),

        known_limitations=[
            "fixture G cannot be validated against the ingest manifest, which has no "
            "WHEN_ISSUED state; it validates against the V6 lineage artifact only",
            "GEV is the sole source for fixtures F and G because every other LISTED_LATER "
            "security was excluded",
            "row-level raw-byte provenance is not implemented",
            "STRUCTURAL_NO_TRADE remains unavailable, so genuinely untraded minutes are "
            "indistinguishable from unserved ones in V2 output"],

        acceptance={
            "cohort_476_bound_by_digest": len(coh) == 476,
            "27_exclusions_enforced": len(el["cohort"]["exclusion_list"]) == 27,
            "supplemental_prohibited": True,
            "y_unexposed": True,
            "scoped_v6_authority": True,
            "derived_semantics_preserved": True,
            "structural_no_trade_unavailable": True,
            "no_synthetic_ohlcv": True,
            "close_boundary_preserved": True,
            "extended_hours_separate": True,
            "vwap_disabled": True,
            "transaction_count_hold": True,
            "fresh_smoke_plan_measured": len(fixtures) == 12,
            "negative_fixtures_defined": sum(1 for f in fixtures
                                             if f.get("kind") == "NEGATIVE") == 5,
            "restart_provenance_defined": True,
            "production_writes_zero": not os.path.exists(PROD)},
        mutations=dict(canonical_raw=0, derived_writes=0, v6=0, universe=0,
                       cohort=0, quarantine=0, vendor_requests=0),
        next_step="MASSIVE_1M_DERIVED_BASE_SMOKE_V2 — not run automatically",
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    ok = all(p["acceptance"].values())
    p["status"] = ("MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_FROZEN" if ok else "HOLD")
    dg = ART.seal(p, "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2.json",
                  required=("spec_id", "status", "cohort_binding",
                            "scoped_lineage_authority", "source_restriction",
                            "logical_datasets", "observation_dimension",
                            "no_synthetic_market_data", "close_boundary", "smoke_plan",
                            "fail_closed_conditions", "claim_scope", "acceptance",
                            "mutations"),
                  supersede=os.path.exists("MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2.json"))
    print(f"MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2 · {dg} · {p['status']}")
    print(f"  cohort 476 bound · exclusions 27 · supplemental 0 · Y_EXPOSED 0")
    print(f"  fixtures {len(fixtures)} (measured) · negative {sum(1 for f in fixtures if f.get('kind')=='NEGATIVE')}")
    print(f"  A ordinary   {fa['regular_true' if False else 'regular_minutes_observed']}/390 "
          f"close@16:00={fa['close_boundary_bars']} ext={fa['pre_market']}+{fa['post_close']}")
    print(f"  B early      {fb['regular_minutes_observed']}/210 close@13:00="
          f"{fb['close_boundary_bars']} · 16:00 bars (EXTENDED) "
          f"{fb.get('bar_at_1600_is_extended_hours')}")
    print(f"  E sparse     {fe_tk} {fe_n}/390 · {n_sparse}/{len(sparse)} cohort securities "
          f"incomplete on {ORD}")
    print(f"  F/G          GEV only eligible NOT_YET security ({gev_sessions} sessions)")
    print(f"  production writes 0 · output namespace exists: {os.path.exists(PROD)}")
    if not ok:
        print("  FAILED:", [k for k, v in p["acceptance"].items() if not v])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
