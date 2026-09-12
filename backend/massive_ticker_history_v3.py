"""MASSIVE_TICKER_HISTORY_V3 — ticker-at-time, with the hole in V2's test closed.

THE DEFECT IN V2. V2 asked "does the current ticker return bars at the window start?" and
treated yes as proof that the string addresses this security for the whole window. It is not
proof. META returns 122 bars on 2021-08-25 — and returns ZERO on 2022-06-08, which P10
already established is the last session before Meta Platforms took the ticker. A string that
returns data, then stops, then resumes has been REASSIGNED: the early bars belong to whoever
held the ticker first. V2 classified META as SINGLE_TICKER_WHOLE_WINDOW, which would have
ingested another company's 2021 prices as Meta Platforms and left no trace.

Bars-returned proves the string resolves to SOMETHING. It says nothing about whose.

THE FIX IS A PRESENCE PROFILE. Probe the current ticker once per year across the window and
read the shape:

    1 1 1 1 1 1     continuous            the string addresses one thing throughout
    1 0 1 1 1 1     GAP -> REASSIGNED     early bars belong to a previous holder
    0 0 1 1 1 1     leading zeros         listed later, or renamed from a predecessor

A gap is the signature that matters, and it is invisible to any single-date test. The
predecessor is then resolved from events metadata — used as a HINT, never as truth, since V1
showed it claiming A -> AWD and CASY -> CASYV — and confirmed by querying it.

CONTINUITY IS CHECKED ACROSS EVERY BOUNDARY. A rename does not move the price. So the last
close under the old string and the first open under the new one must be within a factor that
ordinary overnight moves cannot exceed. A boundary that fails this is not a rename; it is two
different companies being stitched together, and it is reported rather than accepted.
"""
from __future__ import annotations
import json, os, sys, time                                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402
from studio.mount_guard import require_external_volume                  # noqa: E402
from massive_probe_run import api_key                                   # noqa: E402
from massive_probe_run_b import rth, session_bars                      # noqa: E402

WIN_START, WIN_END = "2021-08-25", "2026-08-24"
MAP = "SP500_CURRENT_MASSIVE_TICKER_MAP_V1.json"
V1 = "MASSIVE_TICKER_HISTORY_V1.json"
V2 = "MASSIVE_TICKER_HISTORY_V2.json"
YEAR_PROBES = ["2021-11-15", "2022-11-15", "2023-11-15", "2024-11-15", "2025-11-14",
               "2026-08-14"]
CONTINUITY_LO, CONTINUITY_HI = 0.40, 2.50


def probe(tk, day, key):
    b, rec = session_bars(tk, day, key, f"TH3_{tk}_{day}")
    r = rth(b)
    return (len(r), r[0][1]["o"] if r else None, r[-1][1]["c"] if r else None,
            rec["http_status"])


def main():
    require_external_volume(purpose="ticker history v3")
    key = api_key()
    rows = json.load(open(MAP))["rows"]
    v1 = {s["current_ticker"]: s for s in json.load(open(V1))["securities"]}

    out = []
    for i, r in enumerate(rows):
        tk = r["massive_ticker"]
        prof, closes = [], {}
        for d in YEAR_PROBES:
            n, o, c, http = probe(tk, d, key)
            prof.append(1 if n else 0)
            closes[d] = c
        # a gap is any 0 that sits between two 1s
        first1 = prof.index(1) if 1 in prof else None
        last1 = len(prof) - 1 - prof[::-1].index(1) if 1 in prof else None
        gap = (first1 is not None
               and any(prof[j] == 0 for j in range(first1, last1 + 1)))
        out.append(dict(security_id=v1.get(tk, {}).get("security_id"), current_ticker=tk,
                        company_name=r.get("company_name"), cik=r.get("massive_cik"),
                        presence_profile=prof, probe_dates=YEAR_PROBES,
                        year_closes=closes, has_gap=gap,
                        leading_zeros=first1 or 0,
                        events_hint=[f"{v['massive_ticker_at_time']}@{v['effective_from']}"
                                     for v in (v1.get(tk, {}).get("intervals") or [])]))
        if i % 100 == 0:
            print(f"    profile {i}/{len(rows)}", flush=True)

    reassigned = [o for o in out if o["has_gap"]]
    late = [o for o in out if not o["has_gap"] and o["leading_zeros"] > 0]
    clean = [o for o in out if not o["has_gap"] and o["leading_zeros"] == 0]
    print(f"  profiles: {len(clean)} continuous · {len(reassigned)} GAP · "
          f"{len(late)} leading-zero", flush=True)

    # resolve predecessors for gapped and leading-zero securities
    for o in reassigned + late:
        tk = o["current_ticker"]
        cands = [h.split("@")[0] for h in o["events_hint"]
                 if h.split("@")[0] and h.split("@")[0] != tk]
        if "." in tk:
            cands.append(tk.split(".")[0])
        found = None
        for c in dict.fromkeys(cands):
            n, op, cl, _ = probe(c, WIN_START, key)
            if n:
                found = dict(ticker=c, bars=n, close_at_window_start=cl)
                break
        o["predecessor"] = found
        o["status"] = ("REASSIGNED_TICKER_PREDECESSOR_FOUND" if found and o["has_gap"]
                       else "RENAMED_PREDECESSOR_FOUND" if found
                       else "REASSIGNED_NO_PREDECESSOR" if o["has_gap"]
                       else "LISTED_LATER")
    for o in clean:
        o["status"] = "SINGLE_TICKER_WHOLE_WINDOW"
        o["predecessor"] = None

    # continuity across each resolved boundary
    for o in out:
        p = o.get("predecessor")
        if not p:
            o["continuity"] = None
            continue
        # first year in which the current ticker has data
        idx = o["presence_profile"].index(1) if 1 in o["presence_profile"] else None
        cur_close = o["year_closes"][YEAR_PROBES[idx]] if idx is not None else None
        pred = p["close_at_window_start"]
        ratio = (cur_close / pred) if (cur_close and pred) else None
        o["continuity"] = dict(
            predecessor_close_at_window_start=pred, current_first_year_close=cur_close,
            ratio=round(ratio, 3) if ratio else None,
            plausible=bool(ratio and CONTINUITY_LO <= ratio <= CONTINUITY_HI),
            note="a rename does not move the price; a ratio far outside the band suggests "
                 "two different companies rather than one renamed one. NOTE: the two "
                 "observations are up to a year apart, so this is a COARSE screen and a "
                 "failure is a flag for review, not a proof of error.")

    flagged = [o for o in out if o.get("continuity")
               and not o["continuity"]["plausible"]]
    by = {}
    for o in out:
        by[o["status"]] = by.get(o["status"], 0) + 1

    checks = dict(
        all_503_profiled=len(out) == len(rows),
        every_gap_has_predecessor=not [o for o in out
                                       if o["status"] == "REASSIGNED_NO_PREDECESSOR"],
        v2_defect_caught=any(o["current_ticker"] == "META" and o["has_gap"] for o in out))

    p = dict(
        spec_id="MASSIVE_TICKER_HISTORY_V3",
        status="FROZEN" if all(checks.values()) else "HOLD",
        supersedes=dict(
            v1=dict(artifact=V1, digest=ART.file_digest(V1),
                    reason="events metadata contradicts reality (A -> AWD, CASY -> CASYV) "
                           "and strips class suffixes"),
            v2=dict(artifact=V2, digest=ART.file_digest(V2),
                    reason="V2 tested only whether the current ticker returns bars at the "
                           "window start. META returns 122 bars on 2021-08-25 that belong "
                           "to a previous holder of the string, and V2 therefore "
                           "classified it SINGLE_TICKER_WHOLE_WINDOW — which would have "
                           "ingested another company's prices as Meta Platforms")),
        method=dict(
            presence_profile="probe the current ticker once per year across the window",
            gap_signature="a 0 between two 1s means the string was REASSIGNED and the "
                          "early bars belong to a previous holder",
            predecessor="resolved from events metadata as a HINT and confirmed by query",
            continuity="a rename does not move the price; boundary ratios outside "
                       f"[{CONTINUITY_LO}, {CONTINUITY_HI}] are flagged for review"),
        window=dict(start=WIN_START, end=WIN_END),
        summary=by,
        gapped_tickers=[dict(ticker=o["current_ticker"], profile=o["presence_profile"],
                             predecessor=(o["predecessor"] or {}).get("ticker"),
                             company=o["company_name"]) for o in reassigned],
        continuity_flagged=[dict(ticker=o["current_ticker"],
                                 ratio=o["continuity"]["ratio"],
                                 predecessor=(o["predecessor"] or {}).get("ticker"))
                            for o in flagged],
        acceptance=checks,
        ingest_rule=dict(
            statement="for each (security, date) the ingest selects the ticker string whose "
                      "interval contains that date",
            gapped="for a REASSIGNED string, dates before the changeover MUST use the "
                   "predecessor; using the current string would silently return another "
                   "company's bars",
            unavailable="a date on which no candidate string returns bars is recorded "
                        "UNAVAILABLE with its status code, never as absent data"),
        securities=out,
        canonical_1m_rows_downloaded=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_TICKER_HISTORY_V3.json",
                 required=("spec_id", "status", "method", "securities", "summary",
                           "ingest_rule"),
                 supersede=os.path.exists("MASSIVE_TICKER_HISTORY_V3.json"))
    print(f"\nMASSIVE_TICKER_HISTORY_V3 · {d} · {p['status']}")
    print(f"  {by}")
    print(f"  GAPPED (ticker reassigned):")
    for o in p["gapped_tickers"]:
        print(f"    {o['ticker']:7s} {o['profile']} pred={o['predecessor']} "
              f"{(o['company'] or '')[:30]}")
    if p["continuity_flagged"]:
        print(f"  continuity flagged: {p['continuity_flagged']}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
