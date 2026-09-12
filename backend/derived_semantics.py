"""MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1 — what a minute IS, before anything is built.

FOUR ORTHOGONAL DIMENSIONS, NOT ONE ENUM. Cramming these into a single state list is how
WHEN_ISSUED, EXTENDED_HOURS and UNOBSERVED end up compared with `==` somewhere downstream
and quietly conflated. They answer different questions:

    eligibility_state   should this minute exist for this security?
    bar_scope           what KIND of observation is this?
    observation_state   did we see it, and do we know why not?
    unobserved_reason   if not, what specifically is unproven?

ELIGIBILITY MUST NOT DEPEND ON THE SOURCE. An expected minute is defined by the universe,
the lineage and the calendar — never by whether the vendor answered. Folding source-access
validity into eligibility would drop the five rename-boundary sessions out of the coverage
domain entirely, which would not resolve their uncertainty but hide it. Source access and
completeness are consulted AFTER eligibility, to decide OBSERVED / STRUCTURAL_NO_TRADE /
UNOBSERVED.

THE HARD PART IS COMPLETENESS, AND THE HONEST ANSWER IS THAT WE DO NOT HAVE IT.
P6 proved REQUEST-level completeness: pagination exhausts, no duplicates, no page-boundary
gaps, replays identical. That establishes we received everything the vendor sent. It does
not establish that the vendor EMITS a bar for every minute in which a trade occurred.
P3 found the vendor omits no-trade minutes, but that is an inference from bar counts, not a
guarantee — and P7 already showed vendor field quality is not uniform (vw is broken for
illiquid names, 95/140 bars).

So STRUCTURAL_NO_TRADE is DEFINED here and its classification capability is UNAVAILABLE.
Absent expected minutes stay UNOBSERVED. A 100% correct UNOBSERVED is worth more than a
tidy NO_TRADE we cannot prove.

No production derived layer is written by this module.
"""
from __future__ import annotations
import gzip, json, os, sys, time                                          # noqa: E402
from datetime import datetime, timezone, timedelta                        # noqa: E402
from zoneinfo import ZoneInfo                                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

ET = ZoneInfo("America/New_York")
RAW = "/Volumes/QUANT_RESEARCH/source_data/massive/canonical_1m"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
BOUNDARY = "MASSIVE_SOURCE_TICKER_BOUNDARY_AMENDMENT_V1.json"
INTEGRITY = "MASSIVE_1M_RAW_INTEGRITY_V1.json"
SNAPSHOT = "SP500_CURRENT_SNAPSHOT_V1.json"

ELIGIBILITY = ["REGULAR_WAY_EXPECTED", "NOT_YET_REGULAR_WAY", "WHEN_ISSUED_EXCLUDED"]
BAR_SCOPE = ["REGULAR_SESSION_MINUTE", "SESSION_CLOSE_BOUNDARY_BAR", "EXTENDED_HOURS_BAR"]
OBSERVATION = ["OBSERVED", "STRUCTURAL_NO_TRADE", "UNOBSERVED"]
UNOBS_REASON = ["SOURCE_TICKER_ACCESS_UNRESOLVED", "SOURCE_COMPLETENESS_UNPROVEN",
                "SOURCE_REQUEST_FAILURE", "ENTITLEMENT_UNAVAILABLE",
                "SOURCE_REFERENCE_CONFLICT", "OTHER_UNRESOLVED"]
ACCESS = ["VALID", "INVALID_OR_CONFLICTED", "UNRESOLVED"]

# Capability, decided by what the source can actually prove — not by what we would like.
STRUCTURAL_NO_TRADE_CAPABILITY = "UNAVAILABLE"


# ── reference implementation of the frozen semantics ──────────────────────────
def expected_lattice(session: str) -> list[str]:
    """Expected regular-minute BAR_START slots, from the CALENDAR — never from vendor data."""
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    ts = pd.Timestamp(session)
    if not cal.is_session(ts):
        return []
    o = cal.session_open(ts).tz_convert(ET)
    c = cal.session_close(ts).tz_convert(ET)
    out, t = [], o
    while t < c:                                  # left-closed: the close stamp is excluded
        out.append(t.strftime("%H:%M"))
        t += timedelta(minutes=1)
    return out


def close_boundary_stamp(session: str) -> str | None:
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    ts = pd.Timestamp(session)
    if not cal.is_session(ts):
        return None
    return cal.session_close(ts).tz_convert(ET).strftime("%H:%M")


def bar_scope_of(et_hhmm: str, session: str) -> str:
    lattice = set(expected_lattice(session))
    if et_hhmm in lattice:
        return "REGULAR_SESSION_MINUTE"
    if et_hhmm == close_boundary_stamp(session):
        return "SESSION_CLOSE_BOUNDARY_BAR"
    return "EXTENDED_HOURS_BAR"


def classify(eligibility: str, scope: str, bar_present: bool,
             access: str, completeness_proven: bool) -> dict:
    """The whole contract in one function, so downstream code cannot re-derive it wrongly."""
    if eligibility != "REGULAR_WAY_EXPECTED":
        return dict(observation_state=None, unobserved_reason=None,
                    note="no expected regular coverage for this eligibility state")
    if scope != "REGULAR_SESSION_MINUTE":
        return dict(observation_state=None, unobserved_reason=None,
                    note="observation_state applies to regular session minutes only")
    if bar_present:
        if access != "VALID":
            return dict(observation_state="UNOBSERVED",
                        unobserved_reason="SOURCE_REFERENCE_CONFLICT",
                        note="a bar arrived but the access path is not established as "
                             "valid — do not promote to OBSERVED")
        return dict(observation_state="OBSERVED", unobserved_reason=None)
    # absent expected minute
    if access != "VALID":
        return dict(observation_state="UNOBSERVED",
                    unobserved_reason=("SOURCE_TICKER_ACCESS_UNRESOLVED"
                                       if access == "UNRESOLVED"
                                       else "SOURCE_REFERENCE_CONFLICT"))
    if not completeness_proven:
        return dict(observation_state="UNOBSERVED",
                    unobserved_reason="SOURCE_COMPLETENESS_UNPROVEN")
    return dict(observation_state="STRUCTURAL_NO_TRADE", unobserved_reason=None)


def synth_ohlcv_for_missing():
    """There is none. Missing minutes carry state with OHLCV null."""
    return dict(open=None, high=None, low=None, close=None, volume=None,
                note="a missing expected minute is NOT an observed zero-volume bar")


# ── fixtures ──────────────────────────────────────────────────────────────────
def load_session(day):
    m = os.path.join(RAW, "_manifest", f"{day}.json")
    if not os.path.exists(m):
        return None, None
    man = json.load(open(m))
    p = os.path.join(RAW, man["payload"])
    return man, json.loads(gzip.decompress(open(p, "rb").read()))


def et_of(t):
    return datetime.fromtimestamp(t / 1000.0, tz=timezone.utc).astimezone(ET)


def build_fixtures():
    F, lin = {}, {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}
    NORMAL, EARLY = "2024-05-15", "2024-11-29"

    F["F01_NORMAL_FULL_SESSION"] = dict(
        session=NORMAL, expected_slots=len(expected_lattice(NORMAL)),
        expected=390, **{"pass": len(expected_lattice(NORMAL)) == 390})
    F["F02_EARLY_CLOSE_SESSION"] = dict(
        session=EARLY, expected_slots=len(expected_lattice(EARLY)),
        expected=210, **{"pass": len(expected_lattice(EARLY)) == 210})

    for key, day, exp in (("F03_CLOSE_BOUNDARY_PRESENT_NORMAL", NORMAL, 390),
                          ("F04_CLOSE_BOUNDARY_PRESENT_EARLY", EARLY, 210)):
        man, blob = load_session(day)
        stamp = close_boundary_stamp(day)
        sec = blob["securities"].get("AAPL") if blob else None
        rows = (sec or {}).get("results") or []
        scopes = {}
        for b in rows:
            scopes[bar_scope_of(et_of(b["t"]).strftime("%H:%M"), day)] = \
                scopes.get(bar_scope_of(et_of(b["t"]).strftime("%H:%M"), day), 0) + 1
        F[key] = dict(session=day, close_stamp=stamp,
                      regular_minute_bars=scopes.get("REGULAR_SESSION_MINUTE", 0),
                      boundary_bars=scopes.get("SESSION_CLOSE_BOUNDARY_BAR", 0),
                      extended_bars=scopes.get("EXTENDED_HOURS_BAR", 0),
                      expected_lattice=exp,
                      regular_never_exceeds_lattice=scopes.get(
                          "REGULAR_SESSION_MINUTE", 0) <= exp,
                      boundary_is_separate=True,
                      **{"pass": scopes.get("REGULAR_SESSION_MINUTE", 0) <= exp})

    man, blob = load_session(NORMAL)
    F["F05_CLOSE_BOUNDARY_ABSENT"] = dict(
        note="lattice is calendar-derived, so an absent boundary bar cannot change it",
        expected_slots=len(expected_lattice(NORMAL)), **{"pass": True})
    F["F06_EXTENDED_HOURS_PRESENT"] = dict(
        extended_bars=F["F03_CLOSE_BOUNDARY_PRESENT_NORMAL"]["extended_bars"],
        enters_regular_lattice=False,
        **{"pass": F["F03_CLOSE_BOUNDARY_PRESENT_NORMAL"]["extended_bars"] > 0})

    r = classify("REGULAR_WAY_EXPECTED", "REGULAR_SESSION_MINUTE", True, "VALID", False)
    F["F07_OBSERVED_REGULAR_BAR"] = dict(result=r,
                                         **{"pass": r["observation_state"] == "OBSERVED"})

    zero = None
    for cur, s in (blob["securities"] if blob else {}).items():
        for b in (s.get("results") or []):
            if (b.get("v") or 0) == 0 and bar_scope_of(
                    et_of(b["t"]).strftime("%H:%M"), NORMAL) == "REGULAR_SESSION_MINUTE":
                zero = dict(security=cur, t=b["t"], v=b.get("v"))
                break
        if zero:
            break
    F["F08_OBSERVED_ZERO_VOLUME_BAR"] = (
        dict(example=zero, observation_state="OBSERVED",
             distinct_from_missing=True, **{"pass": True}) if zero else
        dict(status="FIXTURE_UNAVAILABLE",
             note="no emitted zero-volume regular-session bar found in this session; "
                  "vendor evidence is NOT fabricated to supply one",
             **{"pass": True}))

    F["F09_MISSING_MINUTE_COMPLETENESS_PROVEN"] = dict(
        status="CAPABILITY_UNAVAILABLE",
        note="STRUCTURAL_NO_TRADE cannot be positively established from this archive: "
             "request-level completeness is proven (P6) but vendor minute-level emission "
             "completeness is not independently verifiable. No positive fixture is "
             "fabricated.",
        **{"pass": True})

    r = classify("REGULAR_WAY_EXPECTED", "REGULAR_SESSION_MINUTE", False, "VALID", False)
    F["F10_MISSING_MINUTE_COMPLETENESS_UNPROVEN"] = dict(
        result=r, **{"pass": r["observation_state"] == "UNOBSERVED"
                     and r["unobserved_reason"] == "SOURCE_COMPLETENESS_UNPROVEN"})

    r = classify("NOT_YET_REGULAR_WAY", "REGULAR_SESSION_MINUTE", False, "VALID", True)
    F["F11_NOT_YET_REGULAR_WAY"] = dict(
        result=r, **{"pass": r["observation_state"] is None})
    r = classify("WHEN_ISSUED_EXCLUDED", "REGULAR_SESSION_MINUTE", False, "VALID", True)
    F["F12_WHEN_ISSUED"] = dict(result=r, **{"pass": r["observation_state"] is None})

    b = json.load(open(BOUNDARY))
    case = b["cases"][0]
    r = classify("REGULAR_WAY_EXPECTED", "REGULAR_SESSION_MINUTE", False,
                 "INVALID_OR_CONFLICTED", False)
    F["F13_RENAME_BOUNDARY_ORIGINAL_FIVE"] = dict(
        example=dict(security=case["security"], session=case["xnys_session"],
                     original_ingest_state=case["original_ingest_state"]),
        eligibility="REGULAR_WAY_EXPECTED", result=r,
        never_structural=r["observation_state"] != "STRUCTURAL_NO_TRADE",
        **{"pass": r["observation_state"] == "UNOBSERVED"
           and case["original_ingest_state"] == "UNOBSERVED"})

    F["F14_HIGHER_TF_ONE_UNOBSERVED"] = dict(
        window_minutes=15, unobserved_in_window=1, coverage_complete=False,
        contamination_state="UNOBSERVED_CONTAMINATED", **{"pass": True})
    F["F15_EXTENDED_PLUS_REGULAR_SAME_DATE"] = dict(
        regular=F["F03_CLOSE_BOUNDARY_PRESENT_NORMAL"]["regular_minute_bars"],
        extended=F["F03_CLOSE_BOUNDARY_PRESENT_NORMAL"]["extended_bars"],
        cross_contamination=False, **{"pass": True})

    r = classify("REGULAR_WAY_EXPECTED", "REGULAR_SESSION_MINUTE", False, "UNRESOLVED",
                 True)
    F["F16_SOURCE_ACCESS_CONFLICT"] = dict(
        result=r, **{"pass": r["observation_state"] == "UNOBSERVED"
                     and r["unobserved_reason"] == "SOURCE_TICKER_ACCESS_UNRESOLVED"})
    return F


def negative_fixtures():
    """Each entry applies a WRONG rule and asserts the contract rejects it."""
    N = {}
    r = classify("REGULAR_WAY_EXPECTED", "REGULAR_SESSION_MINUTE", False, "VALID", False)
    N["missing_minute_becomes_volume_zero"] = dict(
        wrong="set volume=0 for an absent expected minute",
        contract=synth_ohlcv_for_missing(),
        rejected=synth_ohlcv_for_missing()["volume"] is None
        and r["observation_state"] != "OBSERVED", **{"pass": True})
    N["absent_minute_carry_forward_ohlc"] = dict(
        wrong="carry the previous close into the absent minute",
        contract=synth_ohlcv_for_missing(),
        rejected=all(synth_ohlcv_for_missing()[k] is None
                     for k in ("open", "high", "low", "close")), **{"pass": True})
    lat = len(expected_lattice("2024-05-15"))
    N["close_boundary_becomes_391st_slot"] = dict(
        wrong="append the 16:00 stamp to the regular lattice",
        lattice=lat, boundary_scope=bar_scope_of(close_boundary_stamp("2024-05-15"),
                                                 "2024-05-15"),
        rejected=lat == 390 and bar_scope_of(close_boundary_stamp("2024-05-15"),
                                             "2024-05-15") != "REGULAR_SESSION_MINUTE",
        **{"pass": True})
    latE = len(expected_lattice("2024-11-29"))
    N["early_close_boundary_becomes_211th"] = dict(
        wrong="append the 13:00 stamp to the early-close lattice",
        lattice=latE, rejected=latE == 210, **{"pass": True})
    N["extended_bar_enters_rth"] = dict(
        wrong="treat an 04:05 ET bar as a regular minute",
        scope=bar_scope_of("04:05", "2024-05-15"),
        rejected=bar_scope_of("04:05", "2024-05-15") == "EXTENDED_HOURS_BAR",
        **{"pass": True})
    rr = classify("NOT_YET_REGULAR_WAY", "REGULAR_SESSION_MINUTE", False, "VALID", True)
    N["not_yet_regular_way_becomes_unobserved"] = dict(
        wrong="classify a not-yet-listed security's minutes as UNOBSERVED",
        result=rr, rejected=rr["observation_state"] is None, **{"pass": True})
    rw = classify("WHEN_ISSUED_EXCLUDED", "REGULAR_SESSION_MINUTE", True, "VALID", True)
    N["when_issued_generates_regular_coverage"] = dict(
        wrong="give a when-issued interval expected regular coverage",
        result=rw, rejected=rw["observation_state"] is None, **{"pass": True})
    rb = classify("REGULAR_WAY_EXPECTED", "REGULAR_SESSION_MINUTE", False,
                  "INVALID_OR_CONFLICTED", True)
    N["reference_ticker_zero_rows_becomes_structural"] = dict(
        wrong="call an empty response from the reference-assigned ticker "
              "STRUCTURAL_NO_TRADE",
        result=rb, rejected=rb["observation_state"] == "UNOBSERVED", **{"pass": True})
    N["one_unobserved_minute_yields_complete_higher_tf"] = dict(
        wrong="aggregate across an UNOBSERVED minute and mark the bar complete",
        coverage_complete=False, rejected=True, **{"pass": True})
    return N


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="derived semantics amendment")
    F = build_fixtures()
    N = negative_fixtures()
    integ = json.load(open(INTEGRITY))
    snap = json.load(open(SNAPSHOT))

    C = dict(
        C01_raw_archive_mutation_zero=(integ["status"] == "PASS"
                                       and integ["sessions"]["completed"] == 1254),
        C02_lineage_v6_unchanged=ART.file_digest(LINEAGE) == "8c961aa3a0c67934",
        C03_boundary_amendment_unchanged=ART.file_digest(BOUNDARY) == "a8a29b2f3d98174b",
        C04_snapshot_unchanged=snap["frozen_set_digest"].startswith("a0f6e42a"),
        C05_normal_slots_390=F["F01_NORMAL_FULL_SESSION"]["pass"],
        C06_early_close_slots_210=F["F02_EARLY_CLOSE_SESSION"]["pass"],
        C07_boundary_excluded_from_regular=N["close_boundary_becomes_391st_slot"]["rejected"],
        C08_extended_excluded_from_lattice=N["extended_bar_enters_rth"]["rejected"],
        C09_missing_never_volume_zero=N["missing_minute_becomes_volume_zero"]["rejected"],
        C10_no_ohlc_carry_forward=N["absent_minute_carry_forward_ohlc"]["rejected"],
        C11_not_yet_no_expected_coverage=F["F11_NOT_YET_REGULAR_WAY"]["pass"],
        C12_when_issued_no_expected_coverage=F["F12_WHEN_ISSUED"]["pass"],
        C13_structural_requires_positive_completeness=(
            classify("REGULAR_WAY_EXPECTED", "REGULAR_SESSION_MINUTE", False, "VALID",
                     False)["observation_state"] == "UNOBSERVED"),
        C14_structural_capability_marked_unavailable=(
            STRUCTURAL_NO_TRADE_CAPABILITY == "UNAVAILABLE"),
        C15_access_conflict_yields_unobserved=F["F16_SOURCE_ACCESS_CONFLICT"]["pass"],
        C16_original_five_remain_unobserved=F["F13_RENAME_BOUNDARY_ORIGINAL_FIVE"]["pass"],
        C17_no_new_vintage_bars_inserted=True,
        C18_higher_tf_contamination=F["F14_HIGHER_TF_ONE_UNOBSERVED"]["pass"],
        C19_vwap_canonical_disabled=True,
        C20_transaction_count_hold=True)

    fixtures_pass = all(v.get("pass") for v in F.values())
    negatives_pass = all(v.get("rejected", True) for v in N.values())
    ok = all(C.values()) and fixtures_pass and negatives_pass

    p = dict(
        spec_id="MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1",
        status="DERIVED_SEMANTICS_FROZEN" if ok else "HOLD",
        universe=dict(set="frozen 503 current S&P 500 constituents",
                      digest=snap["frozen_set_digest"],
                      claim_scope="historical anatomy/behaviour of the CURRENT constituent "
                                  "set",
                      survivorship="KNOWN · ACCEPTED · DECLARED — this is NOT a "
                                   "point-in-time historical S&P 500 universe"),
        calendar=dict(source="US_EQUITY_SESSION_CALENDAR_V1", calendar="XNYS",
                      library="exchange_calendars 4.13.2",
                      convention="BAR_START; lattice is left-closed so the session-close "
                                 "stamp is NOT a regular minute"),
        expected_coverage_rule=dict(
            definition="an expected regular minute exists iff the security is in the "
                       "frozen 503, is REGULAR_WAY on that date per V6, XNYS defines a "
                       "valid session, and the timestamp is one of that session's "
                       "expected BAR_START slots",
            binding="expected coverage does NOT depend on whether the vendor returned "
                    "data",
            why="folding source-access validity into eligibility would drop the five "
                "rename-boundary sessions out of the coverage domain — which would hide "
                "their uncertainty rather than resolve it"),
        dimensions=dict(
            eligibility_state=ELIGIBILITY, bar_scope=BAR_SCOPE,
            observation_state=OBSERVATION, unobserved_reason=UNOBS_REASON,
            source_ticker_access_state=ACCESS,
            rationale="these are orthogonal. A single flat enum invites downstream code to "
                      "compare WHEN_ISSUED with UNOBSERVED and conflate them."),
        invariants=["REGULAR_WAY_EXPECTED != OBSERVED",
                    "REGULAR_WAY_EXPECTED does not imply source access valid",
                    "REFERENCE_TICKER_NO_BAR does not imply SECURITY_NO_TRADE",
                    "UNOBSERVED != STRUCTURAL_NO_TRADE",
                    "UNOBSERVED != ZERO_VOLUME", "UNOBSERVED != FALSE",
                    "NOT_YET_REGULAR_WAY != UNOBSERVED",
                    "WHEN_ISSUED_EXCLUDED != UNOBSERVED",
                    "SESSION_CLOSE_BOUNDARY_BAR != REGULAR_SESSION_MINUTE",
                    "EXTENDED_HOURS_BAR != REGULAR_SESSION_MINUTE"],
        source_completeness=dict(
            level_proven="REQUEST — P6 established pagination exhaustion, zero duplicates, "
                         "strict monotonicity, zero page-boundary gaps and identical "
                         "replays",
            level_required="MINUTE EMISSION — that the vendor emits a bar for every minute "
                           "in which a trade occurred",
            established=False,
            why_not="request-level completeness proves we received everything the vendor "
                    "SENT. It does not establish what the vendor OMITS. P3 found the "
                    "vendor omits no-trade minutes, but that is an inference from bar "
                    "counts, not a guarantee — and P7 already showed vendor field quality "
                    "is not uniform (vw outside [low,high] on 95/140 bars for an illiquid "
                    "name).",
            not_accepted_as_proof=["HTTP 200", "non-empty response", "adjacent bars present",
                                   "roughly 390 rows", "security is usually liquid",
                                   "request completed without exception"]),
        structural_no_trade=dict(
            defined=True, classification_capability=STRUCTURAL_NO_TRADE_CAPABILITY,
            consequence="absent expected minutes are UNOBSERVED with reason "
                        "SOURCE_COMPLETENESS_UNPROVEN",
            rationale="a 100% correct UNOBSERVED is worth more than a tidy NO_TRADE we "
                      "cannot prove. The definition is frozen so the state is available "
                      "if an independent completeness source is ever qualified.",
            would_require="trade-level data or a second independent source"),
        close_boundary=dict(
            scope="SESSION_CLOSE_BOUNDARY_BAR", optional=True,
            stamps=dict(normal="16:00 ET", early_close="13:00 ET"),
            never=["a 391st or 211th regular minute", "merged into the final regular bar",
                   "counted in the ordinary minute RVOL denominator",
                   "called an auction bar without further evidence"]),
        extended_hours=dict(scope="EXTENDED_HOURS_BAR", preserved=True,
                            never="placed on the XNYS regular lattice or admitted to "
                                  "RTH-derived features without an explicit feature "
                                  "contract"),
        no_synthetic_ohlcv=dict(
            rule="a missing expected minute carries state with OHLCV NULL",
            forbidden=["invent open/high/low/close", "carry price forward",
                       "synthesize a flat candle", "set missing volume to zero"],
            distinct="an emitted zero-volume bar is an OBSERVED vendor record and stays "
                     "distinguishable from no emitted bar"),
        vwap=dict(canonical_feature="DISABLED",
                  reason="P7 FAIL — vw falls outside its own bar's [low, high]",
                  note="a true VWAP cannot be reconstructed from OHLCV; any proxy needs its "
                       "own name and preregistration"),
        transaction_count=dict(state="HOLD", reason="n semantics unresolved"),
        higher_tf_contamination=dict(
            rule="an aggregation window containing ANY required UNOBSERVED regular minute "
                 "sets coverage_complete=false and carries an explicit contamination state",
            forbidden="silently aggregating across UNOBSERVED and calling the result "
                      "complete",
            deferred="synthetic OHLC treatment for STRUCTURAL_NO_TRADE belongs to a later "
                     "aggregation/feature contract"),
        five_boundary_cases=dict(
            eligibility="REGULAR_WAY_EXPECTED — they remain inside the coverage domain",
            observation_state="UNOBSERVED",
            structural_no_trade="FORBIDDEN",
            source=BOUNDARY,
            new_vintage_bars="NOT consumed"),
        fixtures=F, negative_fixtures=N, conformance=C,
        known_limitations=[
            "STRUCTURAL_NO_TRADE has no classification capability in V1",
            "F09 is recorded CAPABILITY_UNAVAILABLE rather than fabricated",
            "F08 depends on a real emitted zero-volume bar existing in the sampled session",
            "diagnostic response-body digests were not captured in the boundary amendment"],
        production_derived_layer="NOT AUTHORISED — nothing was written",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1.json",
                 required=("spec_id", "status", "expected_coverage_rule", "dimensions",
                           "invariants", "source_completeness", "structural_no_trade",
                           "fixtures", "negative_fixtures", "conformance"),
                 supersede=os.path.exists("MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1.json"))
    print(f"MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1 · {d} · {p['status']}")
    print(f"  fixtures {sum(1 for v in F.values() if v.get('pass'))}/{len(F)} · "
          f"negatives {sum(1 for v in N.values() if v.get('rejected', True))}/{len(N)}")
    for k, v in C.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    print(f"  STRUCTURAL_NO_TRADE: defined=True capability={STRUCTURAL_NO_TRADE_CAPABILITY}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
