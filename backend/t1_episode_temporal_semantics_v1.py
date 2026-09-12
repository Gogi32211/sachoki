"""MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1 — which daily date selects which 15m session.

Sealed now, ahead of the correction chain, because it rests on POSITIVE evidence read out of the
generator's own SQL. It asserts no absence, so the negative-search defect that is holding
everything else does not touch it.

THE JOIN ANSWERS IT WITHOUT INTERPRETATION. t15m_x.py::load_opening_hour joins episodes to 15m
bars on `CAST(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS DATE) = CAST(e.d AS
DATE)`, where e.d is the episode date — the daily session on which bars.t_sig equals 'T1'. So the
daily anchor on date D selects the opening hour of THAT SAME date D, filtered to exactly 09:30,
09:45, 10:00 and 10:15 ET.

WHICH MAKES THE INFORMATION ORDER EXPLICIT. t_sig[D] is a daily state computed from date D's own
daily OHLC, so it cannot be settled until D's close. The bars it selects are the first four
15-minute bars of D, all before 10:30. The selector therefore uses end-of-day information to
choose bars from the beginning of that same day.

THIS DOES NOT MAKE THE HISTORICAL ESTIMAND INVALID, AND SAYING SO MATTERS AS MUCH AS THE FINDING.
If the registered question is "on days that turned out to be T1 days, what did that day's opening
hour look like?", then this is a well-defined post-hoc localization estimand and the ordering is
intentional. What it forbids is describing the same construct as though the T1 day were known at
09:30 — that would be a lookahead claim, and it is prohibited here in writing.
"""
from __future__ import annotations
import json, os, re, sys, time                                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

BIND = {"MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1.json": "73375ec0d8ab5344",
        "MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1.json": "6d733e1fa3bf3c3d",
        "T1_15M_PRESPEC_V1.json": "71d49dcde8921eb3"}


def main():
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding mismatch:", bad); return 1
    x = open("t15m_x.py").read()
    import t5_dna as D5
    sql = D5._EPISODE_SQL.replace("l.t_sig = 'T5'", "l.t_sig = 'T1'")

    # every claim below is a POSITIVE match against source, never an absence
    ev = [
        ("the opening-hour join is on the SAME date as the episode",
         "AS DATE)\n                 = CAST(e.d AS DATE)" in x
         or re.search(r"AS DATE\)\s*=\s*CAST\(e\.d AS DATE\)", x) is not None),
        ("the episode date column is the family's daily date",
         'dcol = f"{fam}_date"' in x and 'columns=["episode_id", "ticker", dcol]' in x),
        ("the selected clock times are exactly the four opening-hour slots",
         "IN ('09:30','09:45','10:00','10:15')" in x),
        ("times are evaluated in America/New_York",
         "AT TIME ZONE 'America/New_York'" in x),
        ("the daily anchor is the daily t_sig state", "l.t_sig = 'T1'" in sql),
        ("the daily state is a per-session daily row (t_sig from bars by ticker+date)",
         "coalesce(t_sig,'') t_sig" in sql and "FROM bars WHERE universe IN" in sql),
    ]
    ev = [dict(evidence=k, verified=bool(v)) for k, v in ev]
    if not all(e["verified"] for e in ev):
        print("HOLD — temporal evidence did not verify:",
              [e["evidence"] for e in ev if not e["verified"]]); return 1

    p = dict(
        report_id="MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1",
        status="ANCHOR_DATE_SEMANTICS_RESOLVED",
        task_class="TEMPORAL_SEMANTICS_RECOVERY_ONLY",
        why_sealable_now="every claim here is POSITIVE evidence read from the generator's own "
                         "SQL; it asserts no absence, so the negative-search defect holding "
                         "the rest of the chain does not apply",

        anchor_date_semantics="SAME_SESSION_DAILY_T1",
        mapping=dict(
            daily_anchor_date="D",
            fifteen_m_session_selected="the SAME calendar/session date D",
            M1="D 09:30 ET", M2="D 09:45 ET", M3="D 10:00 ET", M4="D 10:15 ET",
            join_predicate="CAST(x.date AT TIME ZONE 'UTC' AT TIME ZONE "
                           "'America/New_York' AS DATE) = CAST(e.d AS DATE)",
            clock_filter="strftime(...) IN ('09:30','09:45','10:00','10:15')",
            source="t15m_x.py :: load_opening_hour"),

        information_order=dict(
            daily_t1_requires="date D's own daily OHLC, so it cannot be settled until D's "
                              "close",
            selected_bars="the first four 15-minute bars of D, all before 10:30 ET",
            consequence="the selector uses END-OF-DAY information to choose bars from the "
                        "BEGINNING of that same day",
            final_state_known_during_M1_M4=False),

        classification="POST_EXPOSURE_SESSION_LOCALIZATION",
        explicitly_not=["REAL_TIME_PREDICTOR", "OPENING_HOUR_AVAILABLE_SIGNAL",
                        "SAME_TIME_EXECUTION_SIGNAL"],

        does_not_invalidate_the_historical_estimand=dict(
            reasoning="if the registered question is 'on days that turned out to be T1 days, "
                      "what did that day's opening hour look like?', the ordering is "
                      "intentional and the estimand is well defined",
            what_is_forbidden="describing the same construct as though the T1 day were known "
                              "at 09:30 — that is a lookahead claim",
            forbidden_phrasings=["we knew at 09:30 that this would be a T1 day",
                                 "the opening hour predicted the daily T1",
                                 "tradable at M1"]),

        binding_on_downstream=dict(
            any_Y_interpretation="must respect POST_EXPOSURE_SESSION_LOCALIZATION",
            execution_semantics="NOT established by this construct",
            tradeability="a separate estimand that this study does not address"),

        evidence=ev, all_evidence_positive=True,
        bindings=BIND,
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1.json",
                 required=("report_id", "status", "anchor_date_semantics", "mapping",
                           "information_order", "classification", "evidence"),
                 supersede=os.path.exists("MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1.json"))
    print(f"MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1 · {d} · {p['status']}")
    print("  mapping     daily T1 on D  ->  M1..M4 on the SAME date D")
    print("              M1 09:30 · M2 09:45 · M3 10:00 · M4 10:15 ET")
    print("  order       t_sig[D] needs D's close; the selected bars are all before 10:30")
    print("  class       POST_EXPOSURE_SESSION_LOCALIZATION")
    print("  NOT         real-time predictor · opening-hour-available signal · tradable at M1")
    print("  note        this does NOT invalidate the historical estimand; it forbids one way")
    print("              of describing it")
    print(f"  evidence    {sum(e['verified'] for e in ev)}/{len(ev)} positive matches")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
