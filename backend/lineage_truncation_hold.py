"""MASSIVE_1M_LINEAGE_TRUNCATION_HOLD_V1 — V2 is not sealed, because V6 truncates 8 histories.

The V2 fixture check found BLK, LH and XOM sitting in the NOT_YET_REGULAR_WAY set on
2023-01-05. Those are long-standing S&P 500 companies that certainly traded that day, so
the fixture list was not the problem — the lineage was.

THE MECHANISM IS THE MIRROR IMAGE OF FB -> META. There the TICKER changed while the issuer
stayed the same, and requesting the current string returned nothing for the old period.
Here the CIK changed while the ticker stayed the same: a corporate reorganisation creates a
NEW registrant, and V6 — which anchored identity on the current ticker's CURRENT CIK — can
only see history from the moment that new registrant began reporting.

    BLK   cik 0002012383   BlackRock's long-standing CIK is 0001364742
    XOM   cik 0002115436   ExxonMobil's is 0000034088

V6 therefore assigns XOM a regular-way interval of 2026-08-24 -> 2026-08-24: one session of
expected coverage across a five-year window, for one of the largest companies in the index.

MEASURED, NOT INFERRED. For every LISTED_LATER security the current ticker was queried on
2021-09-15, well before its V6 regular-way start. Eight returned bars:

    APO · J · CRH · BG · LH · FERG · BLK · XOM

The other fifteen returned nothing and are genuine later listings — CEG, WBD, GEHC, KVUE,
TKO, VLTO, RDDT, GEV, SOLV, SW, SNDK, PSKY, Q, FDXF, HONA. So this is not a blanket
LISTED_LATER problem; it is specifically issuers that re-registered.

WHY THIS BLOCKS THE BUILD RATHER THAN THE FIXTURE. Under the frozen semantics,
NOT_YET_REGULAR_WAY generates no expected-minute lattice at all. Building now would silently
drop years of real trading for eight securities and record it as "not expected" — the one
outcome that produces no UNOBSERVED rows, no warning, and no trace.

WHAT IS NOT WRONG. The identity KEY is sound: all eight have unique, stable security_key_v1
values. The defect is the TEMPORAL EXTENT of their regular-way intervals, not who they are.
V6 is frozen and is not modified here.
"""
from __future__ import annotations
import json, os, sys, time                                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
SPEC_V1 = "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V1.json"
TRUNCATED = {
    "APO":  dict(v6_start="2022-01-03", bars_2021_09_15=5),
    "J":    dict(v6_start="2022-10-03", bars_2021_09_15=5),
    "CRH":  dict(v6_start="2023-10-02", bars_2021_09_15=5),
    "BG":   dict(v6_start="2024-01-02", bars_2021_09_15=5),
    "LH":   dict(v6_start="2024-07-01", bars_2021_09_15=5),
    "FERG": dict(v6_start="2024-10-01", bars_2021_09_15=5),
    "BLK":  dict(v6_start="2025-01-02", bars_2021_09_15=5),
    "XOM":  dict(v6_start="2026-08-24", bars_2021_09_15=5),
}
GENUINE_LATER = ["CEG", "WBD", "GEHC", "KVUE", "TKO", "VLTO", "RDDT", "GEV", "SOLV",
                 "SW", "SNDK", "PSKY", "Q", "FDXF", "HONA"]


def main():
    lin = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}
    keys = {r["frozen_current_ticker"]: r for r in
            json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]}

    detail = {}
    for tk, m in TRUNCATED.items():
        s = lin[tk]
        detail[tk] = dict(
            current_cik=s.get("cik"),
            v6_regular_way_start=m["v6_start"],
            bars_under_current_ticker_2021_09_15=m["bars_2021_09_15"],
            v6_intervals=[(i["massive_ticker_at_time"], i["effective_from"],
                           i["effective_to"], i["lineage_state"])
                          for i in (s.get("intervals") or [])],
            security_key_v1=keys[tk]["security_key_v1"],
            identity_basis=keys[tk]["identity_basis"],
            expected_coverage_sessions_under_v6="1" if tk == "XOM" else "partial")

    p = dict(
        report_id="MASSIVE_1M_LINEAGE_TRUNCATION_HOLD_V1",
        status="DERIVED_BASE_LINEAGE_TRUNCATION_HOLD",
        discovered_by="the V2 smoke-fixture pre-seal check, which found BLK, LH and XOM "
                      "classified NOT_YET_REGULAR_WAY on 2023-01-05",
        mechanism=dict(
            summary="a corporate reorganisation creates a NEW registrant with a NEW CIK; "
                    "V6 anchored identity on the current ticker's CURRENT CIK and so sees "
                    "history only from when that registrant began reporting",
            mirror_of="FB -> META, inverted: there the TICKER changed while the issuer "
                      "stayed the same; here the CIK changed while the ticker stayed the "
                      "same",
            examples=dict(
                BLK=dict(v6_cik="0002012383", long_standing_cik="0001364742"),
                XOM=dict(v6_cik="0002115436", long_standing_cik="0000034088"))),
        measurement=dict(
            method="for every LISTED_LATER security the CURRENT ticker was queried on "
                   "2021-09-15, well before its V6 regular-way start",
            listed_later_total=23, truncated=len(TRUNCATED),
            genuine_later_listings=len(GENUINE_LATER),
            truncated_securities=sorted(TRUNCATED),
            genuine_later=GENUINE_LATER,
            note="not a blanket LISTED_LATER problem — specifically issuers that "
                 "re-registered"),
        detail=detail,
        impact_if_built_now=dict(
            rule="under the frozen semantics NOT_YET_REGULAR_WAY generates NO expected "
                 "minute lattice",
            consequence="years of real trading for eight securities would be recorded as "
                        "'not expected' rather than missing",
            why_that_is_the_worst_outcome="it produces no UNOBSERVED rows, no warning and "
                                          "no trace — the coverage report would look "
                                          "clean",
            worst_case="XOM would contribute 1 session of expected coverage out of 1254"),
        what_is_not_wrong=dict(
            identity_key="sound — all eight have unique, stable security_key_v1 values",
            defect_is="the TEMPORAL EXTENT of the regular-way intervals, not who the "
                      "securities are",
            semantics_amendment="unaffected", identity_amendment="unaffected",
            boundary_amendment="unaffected", raw_archive="unaffected"),
        v6_not_modified=dict(
            digest=ART.file_digest(LINEAGE),
            reason="V6 is frozen; correcting it is a decision, not a build-time patch"),
        blocked=dict(
            spec_v2_sealed=False, smoke_executed=False, scratch_written=False,
            production_build="HOLD",
            why="sealing V2 and running the smoke would paper over a defect that affects "
                "the production build, not the fixture"),
        options_for_the_decision=[
            "amend lineage (V7) to extend regular-way intervals for re-registered issuers, "
            "anchoring on the security rather than the current CIK — requires deciding "
            "what evidence establishes predecessor identity",
            "accept the truncation explicitly and record these eight as known-truncated in "
            "the coverage contract, so downstream cannot mistake absence for non-existence",
            "exclude the eight from the V1 research universe and declare the universe 495"],
        recommendation="option 1 or 2; option 3 changes the frozen universe and would "
                       "invalidate the snapshot digest",
        source_mutations=dict(raw_archive=0, lineage=0, identity=0, semantics=0,
                              boundary=0, snapshot=0, note="this check is read-only"),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_LINEAGE_TRUNCATION_HOLD_V1.json",
                 required=("report_id", "status", "mechanism", "measurement", "detail",
                           "impact_if_built_now", "blocked"),
                 supersede=os.path.exists("MASSIVE_1M_LINEAGE_TRUNCATION_HOLD_V1.json"))
    print(f"MASSIVE_1M_LINEAGE_TRUNCATION_HOLD_V1 · {d} · {p['status']}")
    print(f"  truncated {len(TRUNCATED)}/503 · genuine later listings "
          f"{len(GENUINE_LATER)}/23 LISTED_LATER")
    for tk, v in detail.items():
        print(f"    {tk:5s} cik={v['current_cik']} v6_start={v['v6_regular_way_start']} "
              f"bars@2021-09-15={v['bars_under_current_ticker_2021_09_15']}")
    print(f"  spec V2 sealed: False · smoke executed: False · production: HOLD")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
