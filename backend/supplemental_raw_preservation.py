"""MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1 — save the bytes before the window eats them.

The entitlement cliff is not hypothetical here: 2021-08-25 was retrievable at ingest and
returns 403 now. For the eight securities V6 truncated, the pre-truncation sessions were
never canonically ingested at all, so those bytes exist only at the vendor and are expiring
one session per day.

Lineage semantics can be corrected next week. These responses cannot be re-obtained.

QUARANTINE, NOT REPAIR. Everything lands in a separate versioned namespace marked
SUPPLEMENTAL_RAW_QUARANTINE. The canonical archive is not touched, no session manifest is
rewritten, and no frozen UNOBSERVED / NOT_YET_REGULAR_WAY / REGULAR_WAY state changes.
Preserving bytes is not the same act as admitting them as evidence, and conflating the two
is how a vintage boundary quietly disappears.

ATTRIBUTION IS CARRIED, NOT ASSUMED. Each response records the candidate security_key_v1
AND an attribution_state. For APO that state is UNRESOLVED: its continuity is not
established, so preserving its bytes explicitly does not classify them as APO research
evidence — it only stops potentially relevant source from vanishing while the question is
open.

OLDEST FIRST, because that is the end that is perishable. A 403 is recorded as
ENTITLEMENT_UNAVAILABLE and never retried — retrying an expired session would only
manufacture the appearance of an empty response.
"""
from __future__ import annotations
import gzip, hashlib, json, os, sys, time                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                          # noqa: E402
import t5_artifact as ART                                                # noqa: E402
from massive_probe_run import api_key                                    # noqa: E402

BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
OUT = ("/Volumes/QUANT_RESEARCH/source_data/massive/"
       "supplemental_lineage_quarantine_v1")
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"
WIN_START = "2021-08-25"
KEY = None

ATTRIBUTION = {
    "J": "CONTINUITY_CONFIRMED", "BG": "CONTINUITY_CONFIRMED",
    "LH": "CONTINUITY_CONFIRMED", "FERG": "CONTINUITY_CONFIRMED",
    "BLK": "CONTINUITY_CONFIRMED", "XOM": "CONTINUITY_CONFIRMED",
    "CRH": "CONTINUITY_CONFIRMED_WITH_INSTRUMENT_TRANSITION",
    "APO": "UNRESOLVED",
}


def sessions_before(day):
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    s = cal.sessions_in_range(pd.Timestamp(WIN_START),
                              pd.Timestamp(day) - pd.Timedelta(days=1))
    return [str(d.date()) for d in s]


def fetch(tk, day):
    url = f"{BASE}/v2/aggs/ticker/{tk}/range/1/minute/{day}/{day}"
    try:
        r = requests.get(url, params={"adjusted": "true", "limit": 50000,
                                      "apiKey": KEY}, timeout=60)
    except Exception as e:
        return dict(http=None, error=type(e).__name__, raw=None)
    raw = r.content
    rows = None
    if r.status_code == 200:
        try:
            rows = len((r.json() or {}).get("results") or [])
        except Exception:
            rows = None
    return dict(http=r.status_code, raw=raw, rows=rows,
                url=url, params=dict(adjusted="true", limit=50000))


def main():
    global KEY
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="supplemental lineage raw preservation")
    KEY = api_key()
    lin = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}
    keys = {r["frozen_current_ticker"]: r["security_key_v1"]
            for r in json.load(open(IDENTITY))["mapping"]}
    os.makedirs(OUT, exist_ok=True)

    per, t0 = {}, time.time()
    totals = dict(attempted=0, with_bars=0, empty=0, entitlement_unavailable=0,
                  other_failure=0, raw_rows=0, bytes=0)

    for tk in sorted(ATTRIBUTION, key=lambda x: lin[x]["regular_way_intervals"][0]
                     ["effective_from"]):
        start = lin[tk]["regular_way_intervals"][0]["effective_from"]
        days = sessions_before(start)                 # oldest first
        d = os.path.join(OUT, tk)
        os.makedirs(d, exist_ok=True)
        man_p = os.path.join(d, "_manifest.jsonl")
        done = set()
        if os.path.exists(man_p):
            for line in open(man_p):
                try:
                    done.add(json.loads(line)["session_date"])
                except Exception:
                    pass
        c = dict(security=tk, candidate_security_key_v1=keys[tk],
                 attribution_state=ATTRIBUTION[tk], v6_regular_way_start=start,
                 eligible_sessions=len(days), attempted=0, with_bars=0, empty=0,
                 entitlement_unavailable=0, other_failure=0, rows=0, bytes=0)
        with open(man_p, "a") as man:
            for day in days:
                if day in done:
                    continue
                r = fetch(tk, day)
                totals["attempted"] += 1
                c["attempted"] += 1
                rec = dict(security=tk, candidate_security_key_v1=keys[tk],
                           attribution_state=ATTRIBUTION[tk],
                           ticker_queried=tk, session_date=day,
                           request_url=r.get("url"), params=r.get("params"),
                           adjusted=True, http_status=r.get("http"),
                           retrieval_vintage="SUPPLEMENTAL_QUARANTINE",
                           retrieval_timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                             time.gmtime()))
                if r.get("http") == 403:
                    rec["entitlement_state"] = "ENTITLEMENT_UNAVAILABLE"
                    totals["entitlement_unavailable"] += 1
                    c["entitlement_unavailable"] += 1
                elif r.get("http") == 200 and r.get("raw") is not None:
                    gz = gzip.compress(r["raw"], 6)
                    sha = hashlib.sha256(r["raw"]).hexdigest()
                    fn = f"{day}_{sha[:8]}.json.gz"
                    with open(os.path.join(d, fn), "wb") as f:
                        f.write(gz)
                    rec.update(payload=fn, sha256_uncompressed=sha,
                               uncompressed_bytes=len(r["raw"]),
                               compressed_bytes=len(gz), rows=r.get("rows"),
                               entitlement_state="AVAILABLE")
                    c["bytes"] += len(gz); totals["bytes"] += len(gz)
                    if (r.get("rows") or 0) > 0:
                        totals["with_bars"] += 1; c["with_bars"] += 1
                        totals["raw_rows"] += r["rows"]; c["rows"] += r["rows"]
                    else:
                        totals["empty"] += 1; c["empty"] += 1
                        rec["empty_note"] = ("an empty response is preserved as "
                                             "provenance; it is NOT translated to NO_TRADE")
                else:
                    rec["entitlement_state"] = "OTHER_FAILURE"
                    totals["other_failure"] += 1; c["other_failure"] += 1
                man.write(json.dumps(rec, separators=(",", ":")) + "\n")
                man.flush()
                time.sleep(0.03)
        per[tk] = c
        print(f"  {tk:5s} eligible {c['eligible_sessions']:4d} · bars {c['with_bars']:4d} "
              f"· empty {c['empty']:4d} · 403 {c['entitlement_unavailable']:4d} "
              f"· rows {c['rows']:>9,} · {c['bytes']/1e6:6.1f}MB "
              f"| {time.time()-t0:.0f}s", flush=True)

    p = dict(
        report_id="MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1",
        status="SUPPLEMENTAL_RAW_PRESERVATION_COMPLETE",
        classification="SUPPLEMENTAL_RAW_QUARANTINE",
        explicitly_not=["CANONICAL_RAW", "DERIVED_DATA", "RESEARCH_EVIDENCE", "V6_REPAIR"],
        why_now=dict(
            perishability="Massive's entitlement window rolls forward one session per day; "
                          "2021-08-25 was retrievable at ingest and returns 403 now",
            gap="for these eight securities the pre-truncation sessions were never "
                "canonically ingested, so the bytes exist only at the vendor",
            tradeoff="lineage semantics can be corrected later; these responses cannot be "
                     "re-obtained"),
        scope=dict(securities=sorted(ATTRIBUTION), count=len(ATTRIBUTION),
                   not_expanded="the other 495 were not touched",
                   interval="SOURCE_HISTORY_START through the session immediately before "
                            "each security's existing V6 REGULAR_WAY start"),
        attribution=dict(
            states=ATTRIBUTION,
            binding="preserving APO's bytes does NOT classify them as APO research "
                    "evidence; its continuity is UNRESOLVED and the bytes are held only "
                    "so the question stays answerable"),
        order="oldest XNYS session first — that is the perishable end",
        retry_policy="a 403 is recorded ENTITLEMENT_UNAVAILABLE and never retried; "
                     "retrying an expired session would manufacture the appearance of an "
                     "empty response",
        empty_responses="preserved as provenance and never translated to NO_TRADE",
        no_processing=["RTH filtering", "densification", "zero-fill", "carry-forward",
                       "minute classification", "session lattice", "EMA/RVOL/T-Z",
                       "V6 relabelling"],
        totals=totals, per_security=per,
        output=dict(path=OUT, layout="one directory per security; one gzipped raw response "
                                     "per session; append-only _manifest.jsonl",
                    mount_guard="required and passed before any write"),
        mutations=dict(canonical_raw=0, v6=0, security_key_v1=0, derived_writes=0,
                       session_manifests_rewritten=0, frozen_states_changed=0),
        verdict_meaning="currently accessible source bytes were preserved. This does NOT "
                        "authorise their use in canonical or inferential research.",
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1.json",
                 required=("report_id", "status", "classification", "scope",
                           "attribution", "totals", "mutations"),
                 supersede=os.path.exists(
                     "MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1.json"))
    print(f"\nMASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1 · {d} · {p['status']}")
    print(f"  attempted {totals['attempted']:,} · with bars {totals['with_bars']:,} · "
          f"empty {totals['empty']:,} · 403 {totals['entitlement_unavailable']:,}")
    print(f"  raw rows {totals['raw_rows']:,} · {totals['bytes']/1e9:.3f} GB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
