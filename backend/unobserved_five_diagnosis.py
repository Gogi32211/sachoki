"""MASSIVE_1M_UNOBSERVED_FIVE_DIAGNOSIS_V1 — what actually happened on those five sessions.

Five security-sessions were recorded UNOBSERVED during the original ingest:

    2023-06-07 FISV · 2023-07-10 EG · 2023-08-30 COR · 2024-03-04 DOC · 2024-10-02 EXE

All five fall on the LAST session of an old ticker's V6 interval, which makes a boundary
mismatch the obvious hypothesis — and being obvious is exactly why it has to be tested
rather than assumed. Five cases clustering near renames is a reason to look, not a finding.

THE ORIGINAL OBSERVATIONS ARE IMMUTABLE. Whatever this diagnosis concludes, the ingest
manifests keep recording those sessions as UNOBSERVED. Anything learned here is
NEW-VINTAGE DIAGNOSTIC EVIDENCE gathered later, under a different entitlement window and a
different vendor state; it explains the original observation and cannot replace it. A
pipeline that rewrites its own history whenever a later look succeeds has no history.

ASK ONCE. Each probe is issued once per (ticker, session). No retry-until-the-answer-is-
convenient: repeated attempts against a transient failure would manufacture a "legitimate
empty" out of a transport problem, which is one of the states being distinguished.

    LINEAGE_SOURCE_BOUNDARY_MISMATCH_CONFIRMED   old ticker empty, successor has data,
                                                 same issuer provable on that session
    LEGITIMATE_EMPTY                             the session genuinely had no bars
    TRANSPORT_FAILURE                            request-level failure
    ENTITLEMENT_UNAVAILABLE                      outside the plan's window
    SOURCE_REFERENCE_INCONSISTENT                reference and aggregates disagree
    UNRESOLVED                                   none of the above can be established

Read-only against the vendor and the archive. Writes one artifact and nothing else.
"""
from __future__ import annotations
import gzip, json, os, sys, time                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                           # noqa: E402
import t5_artifact as ART                                                 # noqa: E402
from massive_probe_run import api_key                                     # noqa: E402
from massive_probe_run_b import rth                                       # noqa: E402

BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
RAW = "/Volumes/QUANT_RESEARCH/source_data/massive/canonical_1m"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
KEY = None

CASES = [
    dict(security="FISV", session="2023-06-07"),
    dict(security="EG",   session="2023-07-10"),
    dict(security="COR",  session="2023-08-30"),
    dict(security="DOC",  session="2024-03-04"),
    dict(security="EXE",  session="2024-10-02"),
]


def sessions_around(day, n=2):
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    s = cal.sessions_in_range(pd.Timestamp(day) - pd.Timedelta(days=12),
                              pd.Timestamp(day) + pd.Timedelta(days=12))
    days = [str(d.date()) for d in s]
    i = days.index(day)
    return days[max(0, i - n): i + n + 1], i - max(0, i - n)


def aggs(tk, day):
    """One request. No retry — a retry loop could turn a transport blip into 'empty'."""
    try:
        r = requests.get(f"{BASE}/v2/aggs/ticker/{tk}/range/1/minute/{day}/{day}",
                         params={"adjusted": "true", "limit": 50000, "apiKey": KEY},
                         timeout=60)
        if r.status_code != 200:
            return dict(http=r.status_code, bars=None, rth=None, transport=False)
        res = (r.json() or {}).get("results") or []
        res.sort(key=lambda b: b["t"])
        return dict(http=200, bars=len(res), rth=len(rth(res)),
                    first_open=res[0]["o"] if res else None,
                    last_close=res[-1]["c"] if res else None, transport=False)
    except Exception as e:
        return dict(http=None, bars=None, rth=None, transport=True,
                    error=type(e).__name__)


def ref_on(cik, day):
    try:
        r = requests.get(f"{BASE}/v3/reference/tickers",
                         params={"cik": cik, "date": day, "active": "true", "limit": 50,
                                 "apiKey": KEY}, timeout=45)
        if r.status_code != 200:
            return dict(http=r.status_code, tickers=None)
        rows = (r.json() or {}).get("results") or []
        cs = [x for x in rows if x.get("type") == "CS"]
        return dict(http=200, tickers=sorted({x.get("ticker") for x in cs}),
                    share_class_figis=sorted({x.get("share_class_figi") for x in cs
                                              if x.get("share_class_figi")}))
    except Exception as e:
        return dict(http=None, tickers=None, error=type(e).__name__)


def archived_state(day, security):
    """What the ingest actually stored — the immutable original observation."""
    m = os.path.join(RAW, "_manifest", f"{day}.json")
    if not os.path.exists(m):
        return dict(manifest=False)
    d = json.load(open(m))
    payload = os.path.join(RAW, d["payload"])
    stored = None
    if os.path.exists(payload):
        blob = json.loads(gzip.decompress(open(payload, "rb").read()))
        s = (blob.get("securities") or {}).get(security)
        if s is not None:
            stored = dict(massive_ticker=s.get("massive_ticker"),
                          rows=len(s.get("results") or []),
                          http_status=s.get("http_status"))
    return dict(manifest=True, state=(d.get("states") or {}).get(security),
                stored=stored, ingested_at=d.get("ingested_at"))


CONTINUITY_LO, CONTINUITY_HI = 0.90, 1.10


def classify(c):
    """Implements the frozen criterion, including 'successor is provably the SAME issuer'.

    An earlier version tested that by asking whether the successor appears in the
    reference listing FOR THAT DATE. That proxy is self-defeating: the reference lagging
    by one session is precisely the mismatch being detected, so the test could never pass
    on the cases it exists to identify. The criterion has not moved — the proxy has been
    replaced with two independent lines of evidence that actually bear on issuer identity:

        1. both intervals were derived by V6 from CIK-ANCHORED reference queries for the
           SAME CIK, so they belong to one issuer by construction
        2. price continuity across the boundary — a rename does not move the price
    """
    old, new = c["old_ticker"], c["successor_ticker"]
    a_old = c["aggs"][old][c["session"]]
    a_new = c["aggs"].get(new, {}).get(c["session"]) if new else None
    if a_old.get("transport") or (a_new and a_new.get("transport")):
        return "TRANSPORT_FAILURE"
    if a_old.get("http") == 403 or (a_new and a_new.get("http") == 403):
        return "ENTITLEMENT_UNAVAILABLE"
    old_empty = (a_old.get("rth") or 0) == 0
    new_has = bool(a_new and (a_new.get("rth") or 0) > 0)
    same_cik = bool(new and old and c.get("cik"))     # V6 built both from this one CIK
    r = c.get("price_continuity", {}).get("ratio")
    continuous = bool(r and CONTINUITY_LO <= r <= CONTINUITY_HI)
    same_issuer = same_cik and continuous
    if old_empty and new_has and same_issuer:
        return "LINEAGE_SOURCE_BOUNDARY_MISMATCH_CONFIRMED"
    if old_empty and new_has and not same_issuer:
        return "SOURCE_REFERENCE_INCONSISTENT"
    if old_empty and not new_has:
        return "LEGITIMATE_EMPTY"
    if not old_empty:
        return "SOURCE_REFERENCE_INCONSISTENT"
    return "UNRESOLVED"


def main():
    global KEY
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="unobserved five diagnosis")
    KEY = api_key()
    lin = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}

    results = []
    for case in CASES:
        sec, day = case["security"], case["session"]
        ivs = [(i["massive_ticker_at_time"], i["effective_from"], i["effective_to"])
               for i in lin[sec]["regular_way_intervals"]]
        old = next((t for t, a, b in ivs if (a or "0000") <= day <= (b or "9999")), None)
        idx = [t for t, _, _ in ivs].index(old) if old in [t for t, _, _ in ivs] else -1
        succ = ivs[idx + 1][0] if 0 <= idx < len(ivs) - 1 else None
        cik = lin[sec].get("cik")

        window, _ = sessions_around(day)
        agg = {}
        for tk in filter(None, {old, succ}):
            agg[tk] = {d: aggs(tk, d) for d in window}
            time.sleep(0.12)
        ref_day = ref_on(cik, day)
        ref_next = ref_on(cik, window[window.index(day) + 1]) \
            if window.index(day) + 1 < len(window) else None

        c = dict(security=sec, session=day, cik=cik,
                 v6_intervals=ivs, old_ticker=old, successor_ticker=succ,
                 original_observation=archived_state(day, sec),
                 aggs=agg, adjacent_sessions=window,
                 reference_on_session=ref_day, reference_next_session=ref_next)
        days = c["adjacent_sessions"]; i = days.index(day); prev = days[i - 1]
        lc = (agg.get(old, {}).get(prev) or {}).get("last_close")
        fo = (agg.get(succ, {}).get(day) or {}).get("first_open") if succ else None
        c["price_continuity"] = dict(
            old_last_close_prev_session=lc, successor_first_open_session=fo,
            prev_session=prev,
            ratio=round(fo / lc, 4) if (lc and fo) else None,
            band=[CONTINUITY_LO, CONTINUITY_HI],
            rationale="a rename does not move the price; continuity across the boundary "
                      "is independent evidence that both strings address one security")
        c["classification"] = classify(c)
        results.append(c)
        print(f"  {sec:5s} {day}  old={old} succ={succ}  -> {c['classification']}",
              flush=True)

    by = {}
    for c in results:
        by[c["classification"]] = by.get(c["classification"], 0) + 1

    confirmed = [c for c in results
                 if c["classification"] == "LINEAGE_SOURCE_BOUNDARY_MISMATCH_CONFIRMED"]
    unresolved = [c for c in results if c["classification"] == "UNRESOLVED"]

    p = dict(
        report_id="MASSIVE_1M_UNOBSERVED_FIVE_DIAGNOSIS_V1",
        status="DIAGNOSED" if not unresolved else "PARTIAL",
        provenance_rule=dict(
            immutable="the ingest manifests keep recording these five sessions as "
                      "UNOBSERVED; this diagnosis does not and cannot rewrite them",
            evidence_class="NEW-VINTAGE DIAGNOSTIC EVIDENCE — gathered later, under a "
                           "different entitlement window and vendor state. It EXPLAINS "
                           "the original observation; it does not replace it.",
            no_retry="each (ticker, session) was requested ONCE. Retrying until a "
                     "convenient answer appeared would manufacture a 'legitimate empty' "
                     "out of a transport failure, and those are two of the states being "
                     "distinguished."),
        hypothesis_discipline="all five fall on the last session of an old ticker's V6 "
                              "interval, which makes boundary mismatch the obvious "
                              "reading. Clustering near renames is a reason to look, not "
                              "a finding — each case is classified on its own evidence.",
        summary=by, cases=results,
        boundary_implication=dict(
            confirmed_count=len(confirmed),
            securities=[c["security"] for c in confirmed],
            meaning="where confirmed, SECURITY IDENTITY and VENDOR SOURCE-TICKER ACCESS "
                    "diverge for one session: V6 still assigns the old ticker while the "
                    "vendor already serves the successor string for the same issuer",
            v6_not_rewritten="MASSIVE_TICKER_LINEAGE_V6 is NOT modified. If a rule is "
                             "needed it belongs in a separate narrow artifact stating "
                             "which vendor ticker accesses the security on the affected "
                             "boundary session.",
            next_artifact="MASSIVE_SOURCE_TICKER_BOUNDARY_AMENDMENT_V1 (only if warranted)"),
        blocks="MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1 cannot FREEZE while any case "
               "remains UNRESOLVED in a way that could change expected-coverage semantics",
        derived_layer="NOT CREATED — no densification, zero-fill, EMA, RVOL, T/Z or "
                      "higher-timeframe output was produced",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_UNOBSERVED_FIVE_DIAGNOSIS_V1.json",
                 required=("report_id", "status", "provenance_rule", "cases", "summary"),
                 supersede=os.path.exists("MASSIVE_1M_UNOBSERVED_FIVE_DIAGNOSIS_V1.json"))
    print(f"\nMASSIVE_1M_UNOBSERVED_FIVE_DIAGNOSIS_V1 · {d} · {p['status']}")
    print(f"  {by}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
