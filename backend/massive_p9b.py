"""MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1 — what the 16:00 / 13:00 bar IS, named carefully.

WRITTEN AFTER P1-P9 EXPOSURE, and it does not touch P9's verdict. P9 recorded FAIL because
the frozen rule demanded the last RTH bar be 12:59 on an early-close day and it is 13:00.
That stays FAIL. What this amendment settles is the DOWNSTREAM POLICY: what to do with a bar
stamped exactly at the session close.

THE NAME IS DELIBERATELY NEUTRAL.

    SESSION_CLOSE_BOUNDARY_BAR          not     CLOSE_AUCTION_BAR

P4 was INDICATIVE ONLY by its own frozen text, and its 5x threshold was satisfied by both
the 15:59 and the 16:00 bar, so it never established that this bar IS the closing auction.
It is entirely plausible that it is. It is not demonstrated, and a canonical field name that
asserts a mechanism would quietly convert an unproven reading into a fact that every later
study inherits. So the name describes the bar's POSITION, which is observed, rather than its
CAUSE, which is not.

THE LATTICE RULE THIS FIXES. A regular session is 390 one-minute intervals, 09:30 through
15:59. An early close is 210, 09:30 through 12:59. The bar stamped at the close boundary is
NOT a 391st or 211th regular minute — under BAR_START labelling it begins AT the close — and
treating it as one would silently inflate every per-minute denominator, RVOL baseline
included.

    preserved separately        never silently dropped
    excluded from regular-minute counts and RVOL denominators
    included in the final session bucket as an explicit endpoint component

    usage:  python massive_p9b.py spec | run
"""
from __future__ import annotations
import json, os, statistics, sys, time                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402
from massive_probe_run import api_key                                    # noqa: E402
from massive_probe_run_b import et_of, rth, session_bars                 # noqa: E402

SPEC_ART = "MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1.json"
SYMBOLS = ["AAPL", "MSFT", "JPM", "XOM", "KO"]
NORMAL = ["2024-05-15", "2024-09-18", "2025-03-12"]
EARLY = ["2024-07-03", "2024-11-29", "2024-12-24"]


def frozen_rule():
    return dict(
        regular_minute_lattice=dict(
            normal="09:30..15:59 = 390 one-minute intervals",
            early_close="09:30..12:59 = 210 one-minute intervals"),
        boundary_bar=dict(
            normal_stamp="16:00", early_close_stamp="13:00",
            canonical_name="SESSION_CLOSE_BOUNDARY_BAR",
            name_rationale="describes the bar's POSITION, which is observed, not its "
                           "CAUSE, which P4 did not establish"),
        stability_assertions=[
            "A: a bar stamped exactly at the session close exists for every (symbol, "
            "session)",
            "B: the count of REGULAR minutes equals 390 on a normal session and 210 on an "
            "early close, for these liquid symbols",
            "C: the boundary bar's volume exceeds 5x the median volume of the preceding 30 "
            "regular minutes"],
        verdict_rule="STABLE only if A, B and C hold for every (symbol, session) "
                     "combination; otherwise UNSTABLE",
        frozen_before="any query for these sessions")


def do_spec():
    require_external_volume(purpose="close boundary amendment")
    p = dict(
        spec_id="MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1",
        status="FROZEN BEFORE QUERY",
        written_after_exposure=dict(
            declaration="THIS AMENDMENT WAS WRITTEN AFTER P1-P9 EXPOSURE",
            p9_verdict_unchanged="P9 remains FAIL in "
                                 "MASSIVE_1M_PROBE_EXECUTION_STAGE_B_V1; this amendment "
                                 "settles downstream policy, not that verdict",
            naming_discipline="the bar is called SESSION_CLOSE_BOUNDARY_BAR and NOT "
                              "CLOSE_AUCTION_BAR. P4 was INDICATIVE ONLY and its 5x "
                              "threshold was met by both the 15:59 and the 16:00 bar, so "
                              "the auction reading is plausible and undemonstrated. A "
                              "canonical name asserting a mechanism would convert an "
                              "unproven reading into an inherited fact."),
        frozen_rule=frozen_rule(),
        downstream_policy=dict(
            not_a_regular_minute="the boundary bar is NOT a 391st or 211th regular "
                                 "minute; under BAR_START labelling it begins AT the close",
            preservation="preserved separately and never silently dropped",
            excluded_from=["regular-minute counts", "slot-RVOL and CUM-RVOL denominators",
                           "any per-minute baseline"],
            higher_tf="included in the FINAL session bucket as an explicit endpoint "
                      "component, declared rather than absorbed",
            why="counting it as an extra minute would inflate every per-minute denominator "
                "by one and shift every RVOL baseline downward"),
        test_matrix=dict(symbols=SYMBOLS, normal_sessions=NORMAL,
                         early_close_sessions=EARLY,
                         combinations=len(SYMBOLS) * (len(NORMAL) + len(EARLY))),
        massive_queried="NO",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, SPEC_ART, required=("spec_id", "status", "frozen_rule",
                                        "downstream_policy", "written_after_exposure"),
                 supersede=os.path.exists(SPEC_ART))
    print(f"MASSIVE_1M_CLOSE_BOUNDARY_AMENDMENT_V1 · {d} · {p['status']}")
    print(f"  name: SESSION_CLOSE_BOUNDARY_BAR (not CLOSE_AUCTION_BAR)")
    print(f"  matrix: {p['test_matrix']['combinations']} combinations · massive queried NO")


def do_run():
    require_external_volume(purpose="close boundary conformance")
    spec = json.load(open(SPEC_ART))
    if spec["status"] != "FROZEN BEFORE QUERY":
        raise SystemExit("spec must be frozen before execution")
    key = api_key()
    rows, fails = [], []
    for day, kind in [(d, "normal") for d in NORMAL] + [(d, "early") for d in EARLY]:
        stamp = "16:00" if kind == "normal" else "13:00"
        expect = 390 if kind == "normal" else 210
        for tk in SYMBOLS:
            bars, _ = session_bars(tk, day, key, f"P9B_{tk}_{day}")
            byet = {et_of(b["t"]).strftime("%H:%M"): b for b in bars}
            regular = [(k, b) for k, b in byet.items()
                       if "09:30" <= k <= ("15:59" if kind == "normal" else "12:59")]
            bb = byet.get(stamp)
            base = sorted(regular)[-30:]
            med = statistics.median([b["v"] for _, b in base]) if base else 0
            ratio = (bb["v"] / med) if (bb and med) else None
            a = bb is not None
            b = len(regular) == expect
            c = bool(ratio and ratio > 5)
            r = dict(ticker=tk, session=day, kind=kind, boundary_stamp=stamp,
                     regular_minutes=len(regular), expected_regular=expect,
                     boundary_bar_present=a, boundary_volume=bb["v"] if bb else None,
                     median_last30=med, boundary_ratio=round(ratio, 2) if ratio else None,
                     A_boundary_exists=a, B_lattice_count=b, C_volume_5x=c)
            rows.append(r)
            if not (a and b and c):
                fails.append(r)
            time.sleep(0.1)

    stable = not fails
    p = dict(
        report_id="MASSIVE_1M_CLOSE_BOUNDARY_CONFORMANCE_V1",
        spec=dict(artifact=SPEC_ART, digest=ART.file_digest(SPEC_ART),
                  rule_unchanged="assertions were frozen before any of these sessions was "
                                 "requested"),
        verdict="STABLE" if stable else "UNSTABLE",
        combinations_tested=len(rows), failures=len(fails), failing_rows=fails[:10],
        summary=dict(
            normal_regular_minutes=sorted({r["regular_minutes"] for r in rows
                                           if r["kind"] == "normal"}),
            early_regular_minutes=sorted({r["regular_minutes"] for r in rows
                                          if r["kind"] == "early"}),
            boundary_present=sum(1 for r in rows if r["boundary_bar_present"]),
            boundary_ratio_min=min((r["boundary_ratio"] or 0) for r in rows),
            boundary_ratio_median=statistics.median([r["boundary_ratio"] or 0
                                                     for r in rows])),
        rows=rows,
        interpretation_limit="STABLE means the boundary bar is consistently present and "
                             "consistently large. It does NOT establish that the bar is a "
                             "closing auction; that reading remains undemonstrated and the "
                             "canonical name stays SESSION_CLOSE_BOUNDARY_BAR.",
        canonical_1m_rows_downloaded=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_CLOSE_BOUNDARY_CONFORMANCE_V1.json",
                 required=("report_id", "verdict", "rows", "spec"),
                 supersede=os.path.exists("MASSIVE_1M_CLOSE_BOUNDARY_CONFORMANCE_V1.json"))
    print(f"MASSIVE_1M_CLOSE_BOUNDARY_CONFORMANCE_V1 · {d} · {p['verdict']}")
    s = p["summary"]
    print(f"  combinations {len(rows)} · failures {len(fails)}")
    print(f"  regular minutes: normal {s['normal_regular_minutes']} · "
          f"early {s['early_regular_minutes']}")
    print(f"  boundary bar present {s['boundary_present']}/{len(rows)} · "
          f"volume ratio min {s['boundary_ratio_min']} median {s['boundary_ratio_median']}")
    for f in fails[:5]:
        print(f"    FAIL {f['ticker']} {f['session']} A={f['A_boundary_exists']} "
              f"B={f['B_lattice_count']}({f['regular_minutes']}/{f['expected_regular']}) "
              f"C={f['C_volume_5x']}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "spec"
    do_spec() if cmd == "spec" else do_run()
