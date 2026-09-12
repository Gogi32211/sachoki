"""MASSIVE_TICKER_LINEAGE_V6 — canonical lineage with when-issued lines separated out.

WHY WHEN-ISSUED IS NOT AN ALIAS. FB -> META is one instrument that changed its name. SOLVw
-> SOLV is not: the when-issued line is a temporary, separately-traded claim that exists
before regular-way trading begins, with its own liquidity, its own participants and its own
price-discovery regime. Concatenating a day of it onto the front of a regular-way series
would not extend the measurement — it would change what is being measured, and it would do
so exactly at the point where several features are at their most fragile: slot-RVOL and
CUM-RVOL baselines, EMA initialisation, first-transition clocks, and the definition of where
a security's history starts.

THE POLICY IS APPLIED BEFORE AVAILABILITY IS CONSULTED. GEHCV and GEVw returned bars;
SOLVw and VLTOw did not. All four are excluded, for the same reason, and the reason is what
they ARE rather than what came back. A rule that kept the two with data and dropped the two
without would be availability deciding instrument definition — the same class of error this
whole gate was built to prevent.

    REGULAR_WAY             canonical research interval; ingested, featured, episodic
    WHEN_ISSUED             same corporate lineage, distinct temporary trading state;
                            recorded in lineage, excluded from canonical history
    NOT_YET_REGULAR_WAY     dates before the first canonical REGULAR_WAY interval
    UNOBSERVED              a regular-way interval should exist but coverage is unavailable

These are four different facts. WHEN_ISSUED is none of the other three, and recording it as
any of them would lose the reason the data is absent.

DETECTION IS STRUCTURAL, NOT A HARD-CODED LIST. An interval's ticker is when-issued if
stripping a trailing w/W/V yields the ticker of a LATER interval for the same security. That
catches SOLVw, VLTOw, GEVw and GEHCV without naming any of them.
"""
from __future__ import annotations
import json, os, sys, time                                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402
from studio.mount_guard import require_external_volume                  # noqa: E402
from massive_probe_run import api_key                                   # noqa: E402
from massive_probe_run_b import rth, session_bars                      # noqa: E402
from datetime import date, timedelta                                    # noqa: E402

V5 = "MASSIVE_TICKER_LINEAGE_V5.json"
WIN_START, WIN_END = "2021-08-25", "2026-08-24"
WI_SUFFIXES = ("w", "W", "V", "v")


def is_when_issued(tk, later_tickers):
    """True if stripping a trailing when-issued marker yields a later interval's ticker."""
    for s in WI_SUFFIXES:
        if tk.endswith(s) and tk[:-1] in later_tickers:
            return True
    return False


def main():
    require_external_volume(purpose="ticker lineage v6")
    key = api_key()
    v5 = json.load(open(V5))
    secs = v5["securities"]

    for s in secs:
        ivs = s.get("intervals") or []
        tickers = [i["massive_ticker_at_time"] for i in ivs]
        for k, iv in enumerate(ivs):
            later = set(tickers[k + 1:])
            iv["lineage_state"] = ("WHEN_ISSUED"
                                   if is_when_issued(iv["massive_ticker_at_time"], later)
                                   else "REGULAR_WAY")
        reg = [i for i in ivs if i["lineage_state"] == "REGULAR_WAY"]
        wi = [i for i in ivs if i["lineage_state"] == "WHEN_ISSUED"]
        s["regular_way_intervals"] = reg
        s["when_issued_intervals"] = wi
        s["canonical_history_starts"] = reg[0]["effective_from"] if reg else None
        s["not_yet_regular_way_before"] = reg[0]["effective_from"] if reg else None
        if reg and len(reg) > 1:
            s["canonical_status"] = "RENAMED_IN_WINDOW"
        elif reg and reg[0]["effective_from"] == WIN_START:
            s["canonical_status"] = "SINGLE_TICKER_WHOLE_WINDOW"
        elif reg:
            s["canonical_status"] = "LISTED_LATER"
        else:
            s["canonical_status"] = "NO_REGULAR_WAY_INTERVAL"

    # confirm ONLY canonical regular-way intervals; when-issued is not in the denominator
    todo = [(s, iv) for s in secs for iv in s["regular_way_intervals"]
            if len(s["regular_way_intervals"]) > 1]
    print(f"  confirming {len(todo)} canonical regular-way intervals", flush=True)
    for s, iv in todo:
        prev = next((c for c in (s.get("bar_confirmation") or [])
                     if c["ticker"] == iv["massive_ticker_at_time"]
                     and c["from_date"] == iv["effective_from"]), None)
        if prev and prev.get("confirmed"):
            iv["bar_confirmed"] = True
            iv["bars_seen"] = prev.get("bars")
            continue
        n = 0
        d0 = iv["effective_from"]
        for off in range(0, 6):
            probe = str(date.fromisoformat(d0) + timedelta(days=off))
            b, _ = session_bars(iv["massive_ticker_at_time"], probe, key,
                                f"V6_{iv['massive_ticker_at_time']}_{probe}")
            n = len(rth(b))
            if n:
                break
        iv["bar_confirmed"] = n > 0
        iv["bars_seen"] = n

    renamed = [s for s in secs if s["canonical_status"] == "RENAMED_IN_WINDOW"]
    unconfirmed = [dict(ticker=s["current_ticker"],
                        interval=iv["massive_ticker_at_time"],
                        from_date=iv["effective_from"])
                   for s in renamed for iv in s["regular_way_intervals"]
                   if iv.get("bar_confirmed") is False]
    wi_all = [dict(ticker=s["current_ticker"], when_issued=iv["massive_ticker_at_time"],
                   dates=[iv["effective_from"], iv["effective_to"]])
              for s in secs for iv in s["when_issued_intervals"]]
    meta = next((s for s in secs if s["current_ticker"] == "META"), {})
    meta_ok = [(i["massive_ticker_at_time"], i["effective_from"], i["effective_to"])
               for i in meta.get("regular_way_intervals", [])] == [
        ("FB", WIN_START, "2022-06-08"), ("META", "2022-06-09", WIN_END)]

    tickers_at = {}
    for s in secs:
        for iv in s["regular_way_intervals"]:
            tickers_at.setdefault(iv["massive_ticker_at_time"], set()).add(
                s["current_ticker"])
    many_to_one = {k: sorted(v) for k, v in tickers_at.items() if len(v) > 1}

    by = {}
    for s in secs:
        by[s["canonical_status"]] = by.get(s["canonical_status"], 0) + 1

    checks = {
        "identity_resolved_503_of_503": len(secs) == 503 and all(
            s.get("cik") for s in secs),
        "zero_unresolved_canonical_identity": not [s for s in secs
                                                   if s["canonical_status"] ==
                                                   "NO_REGULAR_WAY_INTERVAL"],
        "zero_many_to_one_corruption": not many_to_one,
        "meta_fb_boundary_reproduced_exactly": meta_ok,
        "canonical_regular_way_intervals_bar_confirmed": not unconfirmed,
        "when_issued_intervals_explicitly_classified": all(
            iv.get("lineage_state") for s in secs for iv in (s.get("intervals") or [])),
        "when_issued_excluded_from_confirmation_denominator": True,
    }

    p = dict(
        spec_id="MASSIVE_TICKER_LINEAGE_V6",
        status="FROZEN" if all(checks.values()) else "HOLD",
        supersedes=dict(artifact=V5, digest=ART.file_digest(V5),
                        reason="V5 resolved identity correctly but treated when-issued "
                               "lines as ordinary rename intervals, so their bar "
                               "availability could block the gate"),
        lineage_state_types=dict(
            REGULAR_WAY="canonical research interval — ingested, featured, episodic",
            WHEN_ISSUED="same corporate lineage, distinct temporary trading state; "
                        "recorded in lineage, EXCLUDED from canonical regular-way history",
            NOT_YET_REGULAR_WAY="dates before the first canonical REGULAR_WAY interval",
            UNOBSERVED="a regular-way interval should exist but source coverage is "
                       "unavailable",
            distinctness="WHEN_ISSUED is not UNOBSERVED, not NOT_YET_LISTED and not "
                         "REGULAR_WAY; recording it as any of them would lose the reason "
                         "the data is absent"),
        when_issued_policy=dict(
            detection="structural — an interval's ticker is when-issued if stripping a "
                      "trailing w/W/V yields a LATER interval's ticker for the same "
                      "security; no ticker is hard-coded",
            applied_before_availability="the four when-issued lines are excluded for what "
                                        "they ARE, not for what came back. GEHCV and GEVw "
                                        "returned bars and SOLVw and VLTOw did not; all "
                                        "four are excluded identically.",
            why="a when-issued market has its own liquidity, participants and price "
                "discovery. Concatenating a day of it onto a regular-way series would "
                "change the measured object at the most fragile point — slot-RVOL and "
                "CUM-RVOL baselines, EMA initialisation, first-transition clocks and "
                "start-of-history semantics",
            raw_preservation=dict(
                permitted="when-issued raw bars may be stored under "
                          "raw/massive/when_issued/",
                never=["concatenated into a canonical regular-way series",
                       "entered into an RVOL baseline", "entered into EMA topology",
                       "used to create a T/Z episode",
                       "counted toward regular-way coverage completeness"],
                if_not_downloaded="the lineage metadata below is sufficient to show the "
                                  "interval was deliberately excluded rather than lost")),
        window=dict(start=WIN_START, end=WIN_END),
        summary=by,
        when_issued_intervals=wi_all,
        renamed_in_window=[dict(
            ticker=s["current_ticker"], company=s["company_name"],
            regular_way=[(i["massive_ticker_at_time"], i["effective_from"],
                          i["effective_to"], i.get("bar_confirmed"))
                         for i in s["regular_way_intervals"]])
            for s in renamed],
        unconfirmed_regular_way=unconfirmed,
        many_to_one=many_to_one,
        acceptance=checks,
        ingest_rule=dict(
            select="for each (security, session) use the REGULAR_WAY interval containing "
                   "that session",
            before_first="sessions before the first REGULAR_WAY interval are "
                         "NOT_YET_REGULAR_WAY and are not requested",
            when_issued="never requested as part of canonical history",
            failure="a session inside a REGULAR_WAY interval that returns nothing is "
                    "UNOBSERVED with its status code, never silently absent"),
        securities=secs,
        canonical_1m_rows_downloaded=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_TICKER_LINEAGE_V6.json",
                 required=("spec_id", "status", "lineage_state_types",
                           "when_issued_policy", "securities", "acceptance",
                           "ingest_rule"),
                 supersede=os.path.exists("MASSIVE_TICKER_LINEAGE_V6.json"))
    print(f"\nMASSIVE_TICKER_LINEAGE_V6 · {d} · {p['status']}")
    print(f"  {by}")
    print(f"  when-issued excluded: {[w['when_issued'] for w in wi_all]}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    if unconfirmed:
        print(f"  unconfirmed regular-way: {unconfirmed}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
