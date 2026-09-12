"""US_EQUITY_SESSION_CALENDAR_V1 — a pinned session calendar, qualified against the frozen
P9 fixtures.

P9's calendar side was never a research problem. It was an absent dependency: no exchange
calendar was installed, so there was no candidate to evaluate, and inventing a date list
would have made the standard and the candidate the same object. A library is now pinned and
put through the fixtures that were frozen long before it was chosen.

THE CALENDAR IS THE SESSION AUTHORITY, NOT THE VENDOR. Massive emits a bar stamped at the
session close, and on early-close days that bar sits at 13:00. Inferring session boundaries
from where vendor data happens to stop would make the vendor's emission quirks into our
definition of a trading day. Sessions come from this calendar; vendor data is measured
against it.

    normal session    09:30 - 16:00 America/New_York
    early close       09:30 - 13:00 America/New_York
    holiday           not a session at all

WHAT QUALIFICATION MEANS HERE. The candidate must reproduce all ten 2024 full closures and
all three early closes with the correct 13:00 close, exactly. One mismatch rejects it — the
same standard applied to every other candidate in this programme.
"""
from __future__ import annotations
import os, sys, time                                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

FULL_CLOSURES_2024 = ["2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29",
                      "2024-05-27", "2024-06-19", "2024-07-04", "2024-09-02",
                      "2024-11-28", "2024-12-25"]
EARLY_CLOSES_2024 = ["2024-07-03", "2024-11-29", "2024-12-24"]
NORMAL_CONTROL = ["2024-05-15", "2024-09-18", "2025-03-12"]


def main():
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    ET = "America/New_York"

    closures, early, normal = {}, {}, {}
    for d in FULL_CLOSURES_2024:
        ts = pd.Timestamp(d)
        closures[d] = dict(is_session=bool(cal.is_session(ts)))
    for d in EARLY_CLOSES_2024:
        ts = pd.Timestamp(d)
        s = bool(cal.is_session(ts))
        ct = cal.session_close(ts).tz_convert(ET).strftime("%H:%M") if s else None
        early[d] = dict(is_session=s, close_et=ct)
    for d in NORMAL_CONTROL:
        ts = pd.Timestamp(d)
        s = bool(cal.is_session(ts))
        ct = cal.session_close(ts).tz_convert(ET).strftime("%H:%M") if s else None
        op = cal.session_open(ts).tz_convert(ET).strftime("%H:%M") if s else None
        normal[d] = dict(is_session=s, open_et=op, close_et=ct)

    checks = dict(
        all_closures_are_not_sessions=all(not v["is_session"] for v in closures.values()),
        all_early_closes_are_sessions=all(v["is_session"] for v in early.values()),
        all_early_closes_at_1300=all(v["close_et"] == "13:00" for v in early.values()),
        normal_controls_are_sessions=all(v["is_session"] for v in normal.values()),
        normal_controls_open_0930=all(v["open_et"] == "09:30" for v in normal.values()),
        normal_controls_close_1600=all(v["close_et"] == "16:00" for v in normal.values()))
    qualified = all(checks.values())

    p = dict(
        spec_id="US_EQUITY_SESSION_CALENDAR_V1",
        status="QUALIFIED" if qualified else "REJECTED",
        role="THE SESSION AUTHORITY for this programme — sessions are defined here, never "
             "inferred from where vendor data stops",
        pinned_source=dict(library="exchange_calendars", version=xc.__version__,
                           calendar="XNYS",
                           coverage=f"{cal.first_session.date()} to "
                                    f"{cal.last_session.date()}",
                           pinning_rule="the version is recorded; a version change is a "
                                        "new artifact and must be re-qualified against "
                                        "these same fixtures"),
        session_definition=dict(normal="09:30-16:00 America/New_York",
                                early_close="09:30-13:00 America/New_York",
                                holiday="not a session"),
        fixtures=dict(
            source="the 2024 dates frozen in MASSIVE_1M_PROBE_SPEC_V1 P9, fixed before any "
                   "calendar candidate existed",
            full_closures=closures, early_closes=early, normal_controls=normal),
        acceptance=checks,
        acceptance_rule="the candidate must reproduce all ten full closures and all three "
                        "early closes exactly; one mismatch rejects it",
        relationship_to_vendor=dict(
            rule="the calendar defines the session; Massive data is MEASURED against it",
            forbidden="inferring a session boundary from where vendor bars stop",
            close_boundary_bar="Massive's bar stamped at the session close is interpreted "
                               "USING this calendar (16:00 normal, 13:00 early), not the "
                               "other way round",
            p9_note="P9's vendor side remains FAIL in its original artifact; this calendar "
                    "does not overwrite that verdict"),
        licenses="a qualified calendar is one of the preconditions the minute-availability "
                 "amendment requires before an absent minute may be promoted to "
                 "STRUCTURAL_NO_TRADE",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "US_EQUITY_SESSION_CALENDAR_V1.json",
                 required=("spec_id", "status", "pinned_source", "acceptance", "fixtures"),
                 supersede=os.path.exists("US_EQUITY_SESSION_CALENDAR_V1.json"))
    print(f"US_EQUITY_SESSION_CALENDAR_V1 · {d} · {p['status']}")
    print(f"  pinned: exchange_calendars {xc.__version__} · XNYS")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    return 0 if qualified else 1


if __name__ == "__main__":
    raise SystemExit(main())
