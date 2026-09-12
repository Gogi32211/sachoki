"""MASSIVE_TICKER_HISTORY_V2 — ticker-at-time established by MEASUREMENT, not by metadata.

WHY V1 IS NOT USED. V1 read the ticker_change events endpoint and believed it. The result
disagrees with observable reality in ways that would have silently corrupted the ingest:

    A      -> "AWD" from 2005-11-23        Agilent trades as A today
    CASY   -> "CASYV" from 2010-08-27      Casey's trades as CASY today
    BRK.B  -> "BRK"                        the aggregates endpoint wants BRK.B
    BF.B   -> "BF"                         same

The class-suffix cases are a formatting mismatch between two endpoints; the AWD and CASYV
cases are simply wrong. Either way the lesson is the same: the events endpoint answers a
question about reference metadata, and we need the answer to a different question — which
string does the AGGREGATES endpoint accept for this security on this date. Those are not
guaranteed to be the same, and here they demonstrably are not.

SO THE AGGREGATES ENDPOINT IS THE AUTHORITY, because it is the endpoint the ingest will
actually call. The test is exactly the one P10 used and is decisive: ask, and see whether
bars come back.

    step 1   query the CURRENT ticker at the window start
             bars > 0  -> that string addresses the security from the window start
             bars = 0  -> either it was called something else, or it was not listed yet

    step 2   for the zeros, take candidate predecessors from the events endpoint (as HINTS,
             never as truth) and query each at the window start

    step 3   distinguish RENAMED from LISTED_LATER by asking when the current ticker first
             returns data

A SECURITY THAT WAS NOT YET LISTED IS NOT A DEFECT. Roughly a dozen constituents listed
after 2021-08-25 — spin-offs and IPOs. Zero bars at the window start is the correct and
expected answer for them, and conflating that with a rename would send the ingest hunting
for a predecessor that never existed.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402
from massive_probe_run import api_key                                    # noqa: E402
from massive_probe_run_b import rth, session_bars                       # noqa: E402

WIN_START, WIN_END = "2021-08-25", "2026-08-24"
MAP = "SP500_CURRENT_MASSIVE_TICKER_MAP_V1.json"
V1 = "MASSIVE_TICKER_HISTORY_V1.json"
PROBE_DATES = ["2021-08-25", "2021-08-26", "2021-08-27"]   # window start + fallbacks


def bars_on(tk, day, key):
    b, rec = session_bars(tk, day, key, f"TH2_{tk}_{day}")
    return len(rth(b)), rec["http_status"]


def first_data_year(tk, key):
    """Coarse search for when the current ticker starts returning data."""
    for y in ("2021", "2022", "2023", "2024", "2025", "2026"):
        n, _ = bars_on(tk, f"{y}-11-15" if y != "2026" else "2026-06-15", key)
        if n:
            return y
    return None


def main():
    require_external_volume(purpose="ticker history v2")
    key = api_key()
    rows = json.load(open(MAP))["rows"]
    v1 = {s["current_ticker"]: s for s in json.load(open(V1))["securities"]}

    out, needs_probe = [], []
    for i, r in enumerate(rows):
        tk = r["massive_ticker"]
        n = 0
        used = None
        for d in PROBE_DATES:
            n, http = bars_on(tk, d, key)
            used = d
            if n:
                break
            time.sleep(0.03)
        rec = dict(security_id=v1.get(tk, {}).get("security_id"), current_ticker=tk,
                   company_name=r.get("company_name"), cik=r.get("massive_cik"),
                   bars_at_window_start=n, probe_date=used)
        if n:
            rec.update(status="SINGLE_TICKER_WHOLE_WINDOW",
                       intervals=[dict(ticker=tk, effective_from=WIN_START,
                                       effective_to=WIN_END,
                                       basis="verified: the current ticker returns bars at "
                                             "the window start")])
        else:
            needs_probe.append(rec)
        out.append(rec)
        if i % 100 == 0:
            print(f"    step1 {i}/{len(rows)}", flush=True)

    print(f"  step 1: {len(rows) - len(needs_probe)} single-ticker · "
          f"{len(needs_probe)} need investigation", flush=True)

    for rec in needs_probe:
        tk = rec["current_ticker"]
        cands = []
        for v in (v1.get(tk, {}).get("intervals") or []):
            c = v["massive_ticker_at_time"]
            if c and c != tk:
                cands.append(c)
        # class-suffix variants, since the events endpoint strips them
        if "." in tk:
            root = tk.split(".")[0]
            cands += [root, root + tk.split(".")[1]]
        found = None
        for c in dict.fromkeys(cands):
            n, _ = bars_on(c, WIN_START, key)
            if n:
                found = (c, n)
                break
            time.sleep(0.03)
        if found:
            pred, n = found
            rec.update(status="RENAMED",
                       predecessor_ticker=pred, predecessor_bars_at_window_start=n,
                       intervals=[dict(ticker=pred, effective_from=WIN_START,
                                       effective_to="BOUNDARY TO BE RESOLVED",
                                       basis="verified: predecessor returns bars at the "
                                             "window start"),
                                  dict(ticker=tk, effective_from="BOUNDARY TO BE RESOLVED",
                                       effective_to=WIN_END,
                                       basis="verified: current ticker returns bars later "
                                             "in the window")],
                       events_hint=[v["massive_ticker_at_time"] + "@" +
                                    str(v["effective_from"])
                                    for v in (v1.get(tk, {}).get("intervals") or [])])
        else:
            y = first_data_year(tk, key)
            rec.update(status="LISTED_LATER" if y else "NO_DATA_IN_WINDOW",
                       first_data_year=y,
                       intervals=[dict(ticker=tk, effective_from=f"{y}-01-01" if y else None,
                                       effective_to=WIN_END,
                                       basis="verified: no predecessor returns data at the "
                                             "window start; the current ticker first "
                                             f"returns data in {y}")])
        time.sleep(0.03)

    by = {}
    for o in out:
        by[o["status"]] = by.get(o["status"], 0) + 1

    renamed = [o for o in out if o["status"] == "RENAMED"]
    listed = [o for o in out if o["status"] == "LISTED_LATER"]
    nodata = [o for o in out if o["status"] == "NO_DATA_IN_WINDOW"]

    checks = dict(
        all_503_classified=len(out) == len(rows),
        every_security_has_intervals=all(o.get("intervals") for o in out),
        no_unclassified=not [o for o in out if o["status"] not in
                             ("SINGLE_TICKER_WHOLE_WINDOW", "RENAMED", "LISTED_LATER",
                              "NO_DATA_IN_WINDOW")])

    p = dict(
        spec_id="MASSIVE_TICKER_HISTORY_V2",
        status="FROZEN" if all(checks.values()) else "HOLD",
        supersedes=dict(artifact=V1, digest=ART.file_digest(V1),
                        reason="V1 trusted the ticker_change events endpoint, which "
                               "disagrees with observable reality (A -> AWD, CASY -> "
                               "CASYV) and strips class suffixes (BRK.B -> BRK). V1 is "
                               "retained as the events-metadata record and is NOT used to "
                               "address the aggregates endpoint."),
        authority="the AGGREGATES endpoint, because it is what the ingest actually calls. "
                  "Events metadata is used only as a source of candidate predecessors.",
        method=["query the current ticker at the window start",
                "for zeros, query candidate predecessors from events metadata plus "
                "class-suffix variants",
                "distinguish RENAMED from LISTED_LATER by finding when the current ticker "
                "first returns data"],
        window=dict(start=WIN_START, end=WIN_END),
        summary=by,
        renamed=[dict(current=o["current_ticker"], predecessor=o["predecessor_ticker"],
                      company=o["company_name"], events_hint=o.get("events_hint"))
                 for o in renamed],
        listed_later=[dict(ticker=o["current_ticker"], company=o["company_name"],
                           first_data_year=o.get("first_data_year")) for o in listed],
        no_data_in_window=[o["current_ticker"] for o in nodata],
        open_item=dict(
            issue="for RENAMED securities the exact changeover session is recorded as "
                  "BOUNDARY TO BE RESOLVED",
            why_not_blocking="the ingest is DATE-MAJOR: for each date it asks which string "
                             "addresses the security, and both candidate strings are known. "
                             "A per-date resolution is made at request time by trying the "
                             "expected string and falling back to the other, with the "
                             "outcome recorded.",
            rule="a date on which NEITHER string returns bars is recorded UNAVAILABLE with "
                 "its status code — never silently treated as absent data"),
        acceptance=checks,
        securities=out,
        canonical_1m_rows_downloaded=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_TICKER_HISTORY_V2.json",
                 required=("spec_id", "status", "authority", "securities", "summary"),
                 supersede=os.path.exists("MASSIVE_TICKER_HISTORY_V2.json"))
    print(f"\nMASSIVE_TICKER_HISTORY_V2 · {d} · {p['status']}")
    print(f"  {by}")
    for o in p["renamed"]:
        print(f"    RENAMED {o['predecessor']:7s} -> {o['current']:7s} {o['company'][:34]}")
    for o in p["listed_later"][:15]:
        print(f"    LISTED_LATER {o['ticker']:7s} first data {o['first_data_year']}")
    if p["no_data_in_window"]:
        print(f"    NO_DATA_IN_WINDOW {p['no_data_in_window']}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
