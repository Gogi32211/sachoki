"""MASSIVE_1M_SOURCE_SEMANTICS_CLOSURE_V1 — every remaining downstream policy, settled.

Written after P1-P9 exposure and after the two replacement amendments. It changes no earlier
verdict. P1 stays UNRESOLVED, P7 stays FAIL, P9 stays FAIL in their original artifacts; what
is decided here is what the programme DOES about each.

THE vw CORRECTION THAT MATTERS. The frozen probe spec said a vw failure means "every VWAP
feature is recomputed from OHLCV or removed". The first half of that is not possible. A
true volume-weighted average price is a property of the trades inside the bar, and no
function of open/high/low/close/volume recovers it — HLC3 x volume and its relatives are
proxies for a different quantity that happens to have similar units. Writing "recompute
from OHLCV" into a frozen spec was an error in the spec, and the honest resolution is to
disable the feature family rather than to substitute something that would inherit the name
VWAP without the meaning.

THE CLOSE-BOUNDARY CONFORMANCE CAME BACK UNSTABLE, AND THAT IS USED RATHER THAN REPAIRED.
The lattice assertion held perfectly — 390 regular minutes on every normal session, 210 on
every early close, 30 of 30. The other two did not: the boundary bar was absent for XOM on
three of six sessions, and on normal sessions its volume ranged from 0.03x to 19.24x the
preceding median. A bar at 0.03x is not an auction by any reading. So the policy below
treats the boundary bar as OPTIONAL and keeps the neutral name, which is what the evidence
supports.

THE WINDOW RECEDES WHILE YOU READ THIS. The five-year entitlement boundary moves forward
one day per day. Freezing an absolute start date does not stop the vendor from withdrawing
it; it only makes the loss visible. The operational consequence is stated plainly below:
ingest oldest-first, because the oldest data is the only part that is perishable.
"""
from __future__ import annotations
import os, sys, time                                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

SOURCE_HISTORY_START = "2021-08-25"


def last_closed_session():
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    now = pd.Timestamp.utcnow()
    sessions = cal.sessions_in_range(pd.Timestamp("2026-01-01"),
                                     pd.Timestamp("2026-12-31"))
    closed = [s for s in sessions if cal.session_close(s) < now]
    return str(closed[-1].date()) if len(closed) else None


def payload():
    end = last_closed_session()
    return dict(
        spec_id="MASSIVE_1M_SOURCE_SEMANTICS_CLOSURE_V1",
        status="POLICY FROZEN",
        written_after_exposure="THIS ARTIFACT WAS WRITTEN AFTER P1-P9 EXPOSURE. It changes "
                               "no earlier verdict: P1 remains UNRESOLVED, P7 remains "
                               "FAIL, P9 remains FAIL in their original artifacts.",

        adjustment=dict(
            state="RESOLVED — PASS_ADJUSTED",
            evidence="MASSIVE_1M_P1B_EXECUTION_V1: TSLA r = 0.9828 against an unadjusted "
                     "band of 2.70-3.30, AMZN r = 0.9769 against 18.00-22.00. Both land in "
                     "the adjusted band and agree.",
            parameter="the `adjusted` request parameter is NOT inert — true and false "
                      "return different response digests",
            consequence="no split-adjustment layer is required. Vendor RESTATEMENT becomes "
                        "the live concern instead: an adjusted history can change "
                        "retroactively, so every extract records its vintage and a "
                        "restatement is stored as a NEW vintage alongside the old one, "
                        "never in place of it.",
            unblocks="multi-year features"),

        vw=dict(
            state="REJECTED FOR V1",
            evidence="P7: vw falls outside its own bar's [low, high] on 1/390 AAPL bars, "
                     "5/390 MSFT, and 95/140 NVR",
            correction_to_frozen_spec=dict(
                spec_said="vw is discarded and every VWAP feature is recomputed from "
                          "OHLCV or removed",
                defect="a true VWAP CANNOT be reconstructed from OHLCV — it is a property "
                       "of the trades within the bar, and HLC3 x volume and its relatives "
                       "are proxies for a different quantity",
                resolution="the VWAP feature family is DISABLED for V1 rather than "
                           "substituted"),
            proxy_rule=dict(
                permitted_later="an OHLCV-based construct may be built, but it must be "
                                "named OHLCV_VWAP_PROXY, never VWAP",
                requires="its own preregistration stating what it measures and what it "
                         "does not",
                why="a proxy carrying the name VWAP would inherit the meaning without the "
                    "measurement, in every study that ever reads the column"),
            reopening="a trade-level source could qualify a real VWAP later; that would be "
                      "a separate branch",
            blocks="the VWAP feature family ONLY — OHLCV research is unaffected"),

        n=dict(
            state="RAW PRESERVED / INTERPRETATION UNRESOLVED",
            evidence="P7: no bar carries volume with a missing n, so the field is "
                     "internally consistent; no independent external quantity was "
                     "available to establish what it COUNTS",
            policy=dict(ingestion="n is stored raw and unchanged",
                        features="transaction-count features are on HOLD and stay out of "
                                 "the V1 feature dictionary",
                        resolution_path="a pre-frozen supplemental probe against a "
                                        "trade-level endpoint, if one is available"),
            blocking="NOT blocking for canonical OHLCV ingestion"),

        session_close_boundary=dict(
            canonical_name="SESSION_CLOSE_BOUNDARY_BAR",
            never_called="CLOSE_AUCTION_BAR",
            conformance="MASSIVE_1M_CLOSE_BOUNDARY_CONFORMANCE_V1 — verdict UNSTABLE, 11 "
                        "failures across 30 combinations",
            what_is_stable=dict(
                lattice="30/30 — every normal session has exactly 390 regular minutes "
                        "(09:30-15:59) and every early close exactly 210 (09:30-12:59)",
                status="this is the assertion the downstream policy actually rests on, and "
                       "it held without exception"),
            what_is_not_stable=dict(
                presence="the boundary bar was ABSENT for XOM on 3 of its 6 sessions — "
                         "27/30 present overall",
                magnitude="on normal sessions the boundary bar ranged from 0.03x to 19.24x "
                          "the preceding 30-minute median (median 4.60x), while on "
                          "early-close sessions it ran 28.86x to 87.07x",
                likely_explanation="the early-close ratios are inflated by a THIN "
                                   "denominator — the last 30 minutes of a half day are "
                                   "quiet, whereas the last 30 minutes of a normal session "
                                   "are already the busiest of the day. Stated as the "
                                   "plausible reading, not as a demonstrated one.",
                bearing_on_the_name="a boundary bar at 0.03x the preceding median is not "
                                    "an auction under any reading. The neutral name is "
                                    "now supported by evidence, not only by caution."),
            policy=dict(
                optional="the boundary bar is OPTIONAL — its absence is a normal outcome "
                         "and must never be treated as an error or as missing data",
                three_valued="PRESENT / ABSENT / UNKNOWN, following the same discipline as "
                             "minute availability",
                not_a_regular_minute="never counted as a 391st or 211th regular minute",
                excluded_from=["regular-minute counts", "slot-RVOL and CUM-RVOL "
                               "denominators", "any per-minute baseline"],
                higher_tf="carried into the final session bucket as an explicit, separately "
                          "labelled endpoint component",
                forbidden="any feature that assumes the boundary bar exists, or that reads "
                          "it as auction volume")),

        calendar=dict(
            state="RESOLVED",
            artifact="US_EQUITY_SESSION_CALENDAR_V1",
            pinned="exchange_calendars 4.13.2, XNYS",
            qualification="6/6 against the P9 fixtures frozen before the library was chosen",
            authority="the calendar defines sessions; vendor data is measured against it, "
                      "never the reverse"),

        cross_store=dict(
            state="RESOLVED — NOT_COMPARABLE, NON-BLOCKING",
            rule="the legacy studio store and Massive 1m are different measurement systems; "
                 "cross-store equality is NOT ESTABLISHED",
            forbidden="using the legacy store as a validation oracle for Massive, or "
                      "pooling the two in any study"),

        source_window=dict(
            rule="the research horizon is stored as ABSOLUTE DATES, never as 'today minus "
                 "five years'",
            SOURCE_HISTORY_START=SOURCE_HISTORY_START,
            SOURCE_HISTORY_END=end,
            end_definition="last fully closed session at freeze time, per the qualified "
                           "XNYS calendar",
            entitlement=dict(
                observed="dates before the boundary return HTTP 403",
                boundary_when_measured="2021-08-25, measured 2026-08-25",
                window="five years, ROLLING forward one day per day"),
            recession_risk=dict(
                statement="freezing an absolute start does not stop the vendor withdrawing "
                          "it — it only makes the loss visible",
                consequence=f"every day of delay costs one day at the start. If ingestion "
                            f"begins later than {SOURCE_HISTORY_START} + the elapsed delay, "
                            f"the earliest frozen sessions will already be unavailable and "
                            f"the frozen window becomes partially unfulfillable.",
                mitigation="INGEST OLDEST-FIRST. The oldest data is the only perishable "
                           "part; the recent end is not going anywhere.",
                reporting="any session in the frozen window that cannot be retrieved is "
                          "recorded as UNAVAILABLE with its 403, never as empty and never "
                          "silently narrowed"),
            archive_becomes_authoritative="once ingested, the raw archive is the "
                                          "authoritative historical source, because the "
                                          "same interval may be unfetchable later"),

        ingestion_gate=dict(
            adjustment="CLEARED",
            calendar="CLEARED",
            close_boundary_policy="CLEARED",
            vw="feature family disabled — does not block ingestion",
            n="feature family on hold — does not block ingestion",
            cross_store="non-blocking",
            remaining_blockers=[],
            verdict="ALL BLOCKING ITEMS CLEARED — canonical 1m ingestion is technically "
                    "unblocked and awaits explicit authorisation, which this artifact does "
                    "NOT grant"),
        canonical_1m_rows_downloaded=0,
        not_authorised="this artifact freezes policy. It does not authorise ingestion.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))


def main():
    p = payload()
    d = ART.seal(p, "MASSIVE_1M_SOURCE_SEMANTICS_CLOSURE_V1.json",
                 required=("spec_id", "status", "adjustment", "vw", "n",
                           "session_close_boundary", "source_window", "ingestion_gate"),
                 supersede=os.path.exists("MASSIVE_1M_SOURCE_SEMANTICS_CLOSURE_V1.json"))
    print(f"MASSIVE_1M_SOURCE_SEMANTICS_CLOSURE_V1 · {d} · {p['status']}")
    print(f"  adjustment        {p['adjustment']['state']}")
    print(f"  vw                {p['vw']['state']}")
    print(f"  n                 {p['n']['state']}")
    print(f"  close boundary    OPTIONAL · lattice stable 30/30 · presence 27/30")
    print(f"  calendar          {p['calendar']['state']} ({p['calendar']['pinned']})")
    print(f"  window            {p['source_window']['SOURCE_HISTORY_START']} -> "
          f"{p['source_window']['SOURCE_HISTORY_END']}")
    print(f"  remaining blockers {p['ingestion_gate']['remaining_blockers']}")
    print(f"  {p['ingestion_gate']['verdict']}")


if __name__ == "__main__":
    main()
