"""MASSIVE_TICKER_HISTORY_V1 — which ticker string addresses each security, on each date.

REQUIRED BY P10. Massive's aggregates are keyed by TICKER-AT-TIME: requesting META for
2022-06-08 returns zero bars, while FB returns 390. Ingesting the frozen universe with its
CURRENT tickers would therefore blank the pre-rename history of every renamed security, and
those sessions would enter the pipeline as UNOBSERVED — the right label for a coverage gap
and the wrong one for having asked with the wrong name.

    security_id                 composite FIGI, stable across ticker changes
    massive_ticker_at_time      the string that addresses it in a given interval
    effective_from / to         the interval, inclusive

CIK CORROBORATES BUT CANNOT BE THE KEY. A dual-class issuer maps ONE CIK to SEVERAL
securities — GOOGL and GOOG share a CIK, as do FOX/FOXA and NWS/NWSA in the frozen set. So
identity is carried by the FIGI and the CIK is used only to confirm the issuer matches.

The intervals are clipped to the ingestion window. Anything before it is recorded but
irrelevant; the vendor cannot serve it in any case.
"""
from __future__ import annotations
import csv, hashlib, json, os, sys, time                                 # noqa: E402
from datetime import date, timedelta                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                          # noqa: E402
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402
from massive_probe_run import api_key                                    # noqa: E402

BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
ARCHIVE = "/Volumes/QUANT_RESEARCH/artifacts/provenance/massive_ticker_events"
MAP = "SP500_CURRENT_MASSIVE_TICKER_MAP_V1.json"
WIN_START, WIN_END = "2021-08-25", "2026-08-24"


def main():
    require_external_volume(purpose="massive ticker history")
    os.makedirs(ARCHIVE, exist_ok=True)
    key = api_key()
    rows = json.load(open(MAP))["rows"]

    out, failures, renamed = [], [], []
    for i, r in enumerate(rows):
        tk = r["massive_ticker"]
        resp = requests.get(f"{BASE}/vX/reference/tickers/{tk}/events",
                            params={"apiKey": key}, timeout=45)
        raw = resp.content
        sha = hashlib.sha256(raw).hexdigest()
        with open(os.path.join(ARCHIVE, f"{tk}_{sha[:8]}.json"), "wb") as f:
            f.write(raw)
        if resp.status_code != 200:
            failures.append(dict(ticker=tk, http=resp.status_code))
            out.append(dict(security_id=None, current_ticker=tk,
                            company_name=r.get("company_name"), cik=r.get("massive_cik"),
                            intervals=[dict(massive_ticker_at_time=tk,
                                            effective_from=None, effective_to=None,
                                            basis="NO EVENT DATA — http "
                                                  f"{resp.status_code}")],
                            events_available=False))
            continue
        res = (resp.json() or {}).get("results") or {}
        evs = [e for e in (res.get("events") or []) if e.get("type") == "ticker_change"]
        evs.sort(key=lambda e: e["date"])
        ivs = []
        for k, e in enumerate(evs):
            t = e["ticker_change"]["ticker"]
            frm = e["date"]
            if k + 1 < len(evs):
                to = str(date.fromisoformat(evs[k + 1]["date"]) - timedelta(days=1))
            else:
                to = None
            ivs.append(dict(massive_ticker_at_time=t, effective_from=frm,
                            effective_to=to, basis="ticker_change event"))
        if not ivs:
            ivs = [dict(massive_ticker_at_time=tk, effective_from=None, effective_to=None,
                        basis="no ticker_change events — single interval")]
        rec = dict(security_id=res.get("composite_figi"), current_ticker=tk,
                   company_name=r.get("company_name"), cik=res.get("cik"),
                   massive_name=res.get("name"), intervals=ivs, events_available=True)
        # the most recent interval must be the ticker we already froze
        if ivs[-1]["massive_ticker_at_time"] != tk:
            failures.append(dict(ticker=tk, issue="latest interval ticker differs",
                                 latest=ivs[-1]["massive_ticker_at_time"]))
        # does any interval boundary fall inside the ingestion window?
        inwin = [v for v in ivs if (v["effective_to"] or "9999") >= WIN_START
                 and (v["effective_from"] or "0000") <= WIN_END]
        rec["intervals_in_window"] = inwin
        rec["ticker_changes_in_window"] = len(inwin) > 1
        if len(inwin) > 1:
            renamed.append(dict(current=tk, intervals=inwin))
        out.append(rec)
        if i % 100 == 0:
            print(f"    {i}/{len(rows)}", flush=True)
        time.sleep(0.05)

    hist_digest = hashlib.sha256(json.dumps(
        [(o["current_ticker"], [(v["massive_ticker_at_time"], v["effective_from"])
                                for v in o["intervals"]]) for o in
         sorted(out, key=lambda x: x["current_ticker"])], sort_keys=True).encode()
    ).hexdigest()

    csvp = os.path.join(ARCHIVE, "massive_ticker_history.csv")
    with open(csvp, "w") as f:
        f.write("security_id,current_ticker,ticker_at_time,effective_from,effective_to\n")
        for o in sorted(out, key=lambda x: x["current_ticker"]):
            for v in o["intervals"]:
                f.write(f"{o['security_id'] or ''},{o['current_ticker']},"
                        f"{v['massive_ticker_at_time']},{v['effective_from'] or ''},"
                        f"{v['effective_to'] or ''}\n")

    checks = dict(
        all_503_present=len(out) == len(rows),
        events_available_for_all=all(o["events_available"] for o in out),
        latest_interval_matches_frozen_map=not [f for f in failures
                                                if f.get("issue")],
        security_id_present=sum(1 for o in out if o["security_id"]) == len(out))

    p = dict(
        spec_id="MASSIVE_TICKER_HISTORY_V1",
        status="FROZEN" if all(checks.values()) else "HOLD",
        required_by="MASSIVE_1M_P10_EXECUTION_V1 — classification TICKER_AT_TIME",
        rule="a request for security S covering date D MUST use the ticker string whose "
             "interval contains D. Using the current ticker outside its own interval "
             "returns zero bars and would be indistinguishable from absent data.",
        identity=dict(
            security_id="composite FIGI — stable across ticker changes",
            cik_role="corroborates the issuer only; a dual-class issuer maps ONE CIK to "
                     "SEVERAL securities (GOOGL/GOOG, FOX/FOXA, NWS/NWSA in this set), so "
                     "CIK alone cannot key identity",
            current_ticker="provenance and join key back to the frozen universe"),
        window=dict(start=WIN_START, end=WIN_END),
        n_securities=len(out),
        securities_with_ticker_change_in_window=len(renamed),
        renamed_in_window=renamed,
        acceptance=checks,
        failures=failures,
        history_digest=hist_digest,
        source=dict(endpoint=f"{BASE}/vX/reference/tickers/{{ticker}}/events",
                    archive_dir=ARCHIVE, csv=os.path.basename(csvp),
                    raw_archived="one raw response per security, hashed"),
        universe=dict(artifact=MAP, digest=ART.file_digest(MAP)),
        securities=out,
        canonical_1m_rows_downloaded=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_TICKER_HISTORY_V1.json",
                 required=("spec_id", "status", "rule", "identity", "securities",
                           "acceptance"),
                 supersede=os.path.exists("MASSIVE_TICKER_HISTORY_V1.json"))
    print(f"MASSIVE_TICKER_HISTORY_V1 · {d} · {p['status']}")
    print(f"  securities {len(out)} · with a ticker change inside the window: {len(renamed)}")
    for x in renamed:
        seq = " -> ".join(f"{v['massive_ticker_at_time']}({v['effective_from']})"
                          for v in x["intervals"])
        print(f"    {x['current']:6s} {seq}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    if failures:
        print(f"  failures: {failures[:5]}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
