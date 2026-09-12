"""MASSIVE_1M_PROBE_EXECUTION_V1 — the consolidated result of P1-P9, and what it settles.

Assembled from the two sealed execution artifacts; it computes nothing new and re-runs
nothing. Its job is to state, in one place, which source semantics are now FIXED, which are
UNRESOLVED, and what each unresolved item blocks.

TWO IMPLEMENTATION DEFECTS ARE RECORDED HERE RATHER THAN QUIETLY CORRECTED. The first run of
Stage B reported P1 as FAIL_AMBIGUOUS. The frozen spec has an explicit UNRESOLVED branch for
"any required session is missing", and the runner simply did not implement it. Fixing that
is conformance to the frozen spec, not a change to it, and the distinction matters enough to
write down: no acceptance boundary moved, a missing branch was added. The second is P9,
where the observed vendor behaviour is explicable and the frozen rule still scores it FAIL —
the explanation is recorded, the verdict is not touched.

THE FINDING NOBODY ASKED FOR. P1 failed because AAPL's 2020 sessions return HTTP 403, not
because they are empty. Bisection puts the entitlement boundary at exactly 2021-08-25, which
is today minus five years. The 1-minute history is a FIVE-YEAR ROLLING WINDOW, and the word
that matters is rolling: the oldest data available today will not be available next year.
That is a constraint on the programme's design, not a detail of one probe.
"""
from __future__ import annotations
import json, os, sys, time                                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

A = "MASSIVE_1M_PROBE_EXECUTION_STAGE_A_V1.json"
B = "MASSIVE_1M_PROBE_EXECUTION_STAGE_B_V1.json"
SPEC = "MASSIVE_1M_PROBE_SPEC_V1.json"


def payload():
    a, b = json.load(open(A)), json.load(open(B))
    pa = {p["probe"]: p for p in a["probes"]}
    pb = {p["probe"]: p for p in b["probes"]}
    allp = {**pa, **pb}

    results = {}
    for k in ("P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9"):
        p = allp[k]
        results[k] = dict(topic=p["topic"], verdict=p["verdict"],
                          classification=p.get("classification")
                          or p.get("closing_classification") or p.get("vendor_side")
                          or p.get("label"))

    return dict(
        report_id="MASSIVE_1M_PROBE_EXECUTION_V1",
        status="PROBES EXECUTED · SOURCE SEMANTICS PARTIALLY RESOLVED",
        spec=dict(artifact=SPEC, digest=ART.file_digest(SPEC),
                  boundaries_unchanged="no acceptance boundary, band or threshold was "
                                       "altered after any response was seen"),
        stages=dict(
            stage_a=dict(artifact=A, digest=ART.file_digest(A),
                         gate=a["foundational_gate"]),
            stage_b=dict(artifact=B, digest=ART.file_digest(B))),
        foundational_gate=a["foundational_gate"],
        results=results,

        source_semantics=dict(
            timestamp=dict(state="RESOLVED",
                           value="epoch MILLISECONDS, labelling the bar START, genuinely "
                                 "timezone-aware",
                           evidence="first RTH bar is 14:30 UTC on 2024-01-16 (EST) and "
                                    "13:30 UTC on 2024-07-16 (EDT); both decode to "
                                    "09:30:00 America/New_York",
                           note="asserted on the UTC value, which is arithmetic from the "
                                "epoch, with the ET conversion as corroboration"),
            pagination=dict(state="RESOLVED",
                            value="50,000 rows per page; next_url pagination terminates "
                                  "with an explicit exhaustion signal",
                            evidence="AAPL 2024 returned 187,547 rows over 4 pages with 0 "
                                     "duplicates, strictly increasing timestamps, 0 "
                                     "page-boundary gaps across 3 boundaries, and two "
                                     "independent replays identical by ordered row digest",
                            licenses="this is the completeness evidence the minute-"
                                     "availability amendment requires before an absent "
                                     "minute may be promoted to STRUCTURAL_NO_TRADE"),
            sparse_minute_emission=dict(state="RESOLVED", value="SPARSE — minutes with no "
                                        "trades are OMITTED, not emitted as zero-volume "
                                        "bars",
                                        evidence="RTH bar counts ranged 19 to 390 across 20 "
                                                 "symbol-sessions on 2024-05-15, with ZERO "
                                                 "zero-volume bars anywhere",
                                        consequence="the three-valued availability "
                                                    "semantics of "
                                                    "MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1 "
                                                    "are LIVE and must be implemented"),
            session=dict(state="RESOLVED",
                         value="extended hours ARE included, roughly 04:00-19:59 ET",
                         consequence="the RTH filter is MANDATORY at ingestion; "
                                     "extended-hours bars are retained but flagged and "
                                     "never mixed into RTH aggregates"),
            adjustment=dict(state="UNRESOLVED",
                            why="the AAPL 2020 split sessions the spec requires are "
                                "outside the plan's entitlement window and return HTTP "
                                "403. The spec requires BOTH names to agree.",
                            indicative="the NVDA 2024 10-for-1 case alone gives r = 1.0041, "
                                       "which is the ADJUSTED band — indicative only, and "
                                       "one name cannot separate an adjusted series from a "
                                       "coincidence",
                            established="the `adjusted` parameter is NOT inert: true and "
                                        "false return different response digests, so it "
                                        "does control something",
                            blocks="any multi-year feature, because an unadjusted series "
                                   "has a discontinuity at every split"),
            vw=dict(state="RESOLVED AS UNUSABLE",
                    value="FAIL_NOT_WITHIN_BAR — vw falls outside the bar's own [low, high] "
                          "range",
                    evidence="AAPL 1/390 bars, MSFT 5/390, and NVR 95/140 — 68% of bars "
                             "for the low-liquidity name",
                    consequence="per the frozen spec, vw is DISCARDED and every VWAP "
                                "feature must be recomputed from OHLCV or removed. No "
                                "VWAP-family feature may rest on the vendor field.",
                    severity="this is the probe that earned its keep — the field looks "
                             "usable on liquid names and is broken where it would have "
                             "been trusted silently"),
            n=dict(state="INTERNALLY CONSISTENT / SEMANTICALLY UNRESOLVED",
                   value="no bar carries volume with a missing n",
                   why_unresolved="no independent external quantity was available to "
                                  "confirm what n COUNTS. The frozen spec forbids "
                                  "resolving this by vendor Polygon-compatibility.",
                   consequence="transaction-count features stay out of the dictionary "
                               "until the meaning is established"),
            calendar=dict(state="UNRESOLVED",
                          vendor_side="FAIL per the frozen rule",
                          detail="all 10 full-closure dates returned zero bars, which is "
                                 "correct. All 3 early-close dates end at a bar stamped "
                                 "13:00, where the rule demanded 12:59.",
                          observed_explanation="P4 finds the same shape on a normal "
                                               "session: a bar stamped 16:00 carrying "
                                               "auction-scale volume. The vendor appears to "
                                               "emit the closing auction as a bar stamped "
                                               "at the closing minute, consistently. "
                                               "211 = 210 regular minutes + 1 auction bar.",
                          governance="the explanation does NOT convert FAIL into PASS. "
                                     "Reconciling the rule with this behaviour requires a "
                                     "NEW amendment explicitly marked as written after "
                                     "P1-P9 exposure.",
                          calendar_candidate="NOT EVALUATED — no exchange-calendar library "
                                             "or dataset is installed, and the spec's fixed "
                                             "2024 dates are the standard, so they cannot "
                                             "also be the candidate"),
            auction=dict(state="PARTIALLY RESOLVED",
                         closing="CLOSING_IN_1600 — a bar stamped 16:00 exists on all three "
                                 "names with volume above the pre-committed 5x median",
                         caveat="the 5x threshold was exceeded by BOTH the 15:59 and the "
                                "16:00 bar on all three names, so the classification rests "
                                "on the EXISTENCE of a 16:00 bar rather than on clean "
                                "separation. The probe was less discriminating than its "
                                "rule assumed.",
                         opening="UNRESOLVED BY CONSTRUCTION, as the spec anticipated",
                         binding="no VWAP may be anchored to the open until trade-condition "
                                 "data is obtained"),
            cross_source_comparability=dict(
                state="RESOLVED — NOT_COMPARABLE",
                evidence="20 overlapping symbol-sessions: 0 within 1%, mean ratio 0.803, "
                         "stdev 0.167. Liquid names cluster 0.89-0.96 while GWW runs "
                         "0.50-0.59 and TDY 0.55-0.85.",
                consequence="the existing DuckDB store and Massive 1m are kept STRICTLY "
                            "SEPARATE. No study may mix them, and the existing store "
                            "CANNOT serve as an ingestion validation for the new one.",
                note="part of the gap is structural — Massive RTH-only summed against a "
                     "full-day store figure — but the spread across names is far too wide "
                     "for that alone, and the frozen rule was applied as written")),

        entitlement_finding=dict(
            discovered_via="diagnostic follow-up to P1's missing sessions",
            behaviour="dates before the boundary return HTTP 403, not empty results",
            boundary="2021-08-25",
            today="2026-08-25",
            window="FIVE YEARS, ROLLING",
            bisection="2021-08-24 → 403 · 2021-08-25 → 200",
            consequences=[
                "the 1m research window is 2021-08-25 to present, about 5 years",
                "ROLLING means the oldest available data recedes daily; what is fetchable "
                "today will not be fetchable next year",
                "reproducibility therefore depends on INGESTING AND RETAINING the data, "
                "because a re-fetch will silently lose the oldest part",
                "the charter's vintage law becomes operationally load-bearing rather than "
                "precautionary",
                "the AAPL 2020 split case in P1 is permanently unavailable on this plan, so "
                "P1 cannot be resolved as written"]),

        implementation_defects_recorded=[
            dict(where="Stage B P1 classifier",
                 defect="the runner lacked the spec's UNRESOLVED branch for a missing "
                        "required session and reported FAIL_AMBIGUOUS",
                 fix="the branch was added; no acceptance boundary was altered",
                 nature="conformance to the frozen spec, NOT a change to it"),
            dict(where="Stage B P9",
                 defect="none — the rule was applied correctly",
                 fix="an explanatory note on the observed 13:00 auction bar was added",
                 nature="the FAIL verdict was NOT altered")],

        canonical_1m_rows_downloaded=0,
        blocking_for_ingestion=[
            "adjustment regime UNRESOLVED — blocks any multi-year feature",
            "calendar source not chosen, and the vendor calendar rule recorded FAIL",
            "n semantics unresolved — blocks transaction-count features only"],
        non_blocking_resolved=[
            "vw discarded; VWAP recomputed from OHLCV or removed",
            "existing store and Massive kept strictly separate",
            "RTH filter mandatory; extended hours retained but flagged",
            "three-valued minute availability is live"],
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))


def main():
    p = payload()
    d = ART.seal(p, "MASSIVE_1M_PROBE_EXECUTION_V1.json",
                 required=("report_id", "status", "results", "source_semantics",
                           "entitlement_finding", "foundational_gate"),
                 supersede=os.path.exists("MASSIVE_1M_PROBE_EXECUTION_V1.json"))
    print(f"MASSIVE_1M_PROBE_EXECUTION_V1 · {d}")
    print(f"  FOUNDATIONAL GATE (P2/P6): {p['foundational_gate']}")
    for k, v in p["results"].items():
        print(f"    {k}  {v['verdict']:20s} {v['topic'][:44]}")
    print(f"\n  entitlement window: {p['entitlement_finding']['window']} "
          f"(boundary {p['entitlement_finding']['boundary']})")
    print(f"  canonical 1m rows downloaded: {p['canonical_1m_rows_downloaded']}")
    print(f"  blocking for ingestion: {len(p['blocking_for_ingestion'])}")


if __name__ == "__main__":
    main()
