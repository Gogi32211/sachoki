"""Canonical raw 1-minute preservation ingest — DATE-MAJOR, OLDEST-FIRST.

WHY THE ORDER IS NOT A DETAIL. The entitlement window is five years and it rolls forward one
day per day, so the oldest session in the window is the only part of this dataset that is
perishable. Ticker-major ingestion (AAPL 2021->2026, then MSFT 2021->2026, ...) would spend
hours on securities whose recent history is in no danger while the earliest sessions expired
out from under it. Date-major means the most perishable layer is secured first and every
completed session is permanently safe.

    2021-08-25  ->  all eligible securities
    2021-08-26  ->  all eligible securities
    ...

FROZEN PARAMETERS, NOT CHOICES MADE HERE. adjusted=true (P1B: PASS_ADJUSTED), millisecond
BAR_START timestamps (P2), XNYS sessions (US_EQUITY_SESSION_CALENDAR_V1), and the
ticker-at-time map (MASSIVE_TICKER_LINEAGE_V6). This module executes a contract; it does not
add to it.

WHAT IS AND IS NOT WRITTEN. Raw vendor payloads, gzipped, one file per session, hashed. No
densification, no zero-filling, no RTH filtering, no derived layer — the sparse-minute
densification contract still references a superseded universe and is being corrected
separately. Extended-hours bars are preserved exactly as returned; the RTH policy is a read
concern, not a storage one.

EVERY SECURITY-SESSION GETS A STATE, INCLUDING THE ABSENT ONES.

    OBSERVED               bars returned
    UNOBSERVED             a regular-way interval covers this session but nothing came back
    NOT_YET_REGULAR_WAY    the session precedes the security's first regular-way interval

An empty response is never silently equivalent to "not eligible". Resumable: a session is
skipped only when its manifest entry and its payload digest both already exist.

    usage:  python massive_ingest.py [max_sessions]
"""
from __future__ import annotations
import gzip, hashlib, json, os, sys, time                               # noqa: E402
from concurrent.futures import ThreadPoolExecutor                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                         # noqa: E402
from studio.mount_guard import require_external_volume                  # noqa: E402
from massive_probe_run import api_key                                   # noqa: E402

BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
ROOT = "/Volumes/QUANT_RESEARCH/source_data/massive/canonical_1m"
MANIFEST = os.path.join(ROOT, "_manifest")
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
WIN_START, WIN_END = "2021-08-25", "2026-08-24"
WORKERS = 10
KEY = None
_sess = requests.Session()


def sessions():
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    s = cal.sessions_in_range(pd.Timestamp(WIN_START), pd.Timestamp(WIN_END))
    return [str(d.date()) for d in s]


def lineage():
    """security -> ordered REGULAR_WAY intervals (when-issued already excluded by V6)."""
    d = json.load(open(LINEAGE))
    if d["status"] != "FROZEN":
        raise SystemExit(f"lineage status is {d['status']} — refusing to ingest")
    out = {}
    for s in d["securities"]:
        out[s["current_ticker"]] = dict(
            cik=s.get("cik"), figi=s.get("composite_figi"),
            intervals=[(i["massive_ticker_at_time"], i["effective_from"],
                        i["effective_to"]) for i in s["regular_way_intervals"]])
    return out


def ticker_for(iv, day):
    for tk, a, b in iv:
        if (a or "0000") <= day <= (b or "9999"):
            return tk
    return None


def fetch(args):
    tk, day = args
    url = f"{BASE}/v2/aggs/ticker/{tk}/range/1/minute/{day}/{day}"
    for k in range(4):
        try:
            r = _sess.get(url, params={"adjusted": "true", "limit": 50000,
                                       "apiKey": KEY}, timeout=60)
            if r.status_code == 200:
                j = r.json() or {}
                return tk, (j.get("results") or []), 200, j.get("next_url")
            if r.status_code in (429, 500, 502, 503):
                time.sleep(1.5 * (k + 1)); continue
            return tk, [], r.status_code, None
        except Exception:
            time.sleep(1.0 * (k + 1))
    return tk, [], -1, None


def main():
    global KEY
    require_external_volume(purpose="canonical 1m preservation ingest")
    KEY = api_key()
    os.makedirs(MANIFEST, exist_ok=True)
    lin = lineage()
    days = sessions()
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else len(days)
    print(f"  sessions {len(days)} ({days[0]} .. {days[-1]}) · securities {len(lin)}",
          flush=True)

    t0, done_sessions, total_rows, total_bytes = time.time(), 0, 0, 0
    for day in days[:limit]:
        mpath = os.path.join(MANIFEST, f"{day}.json")
        ddir = os.path.join(ROOT, day[:4], day[5:7])
        payload_path = os.path.join(ddir, f"{day}.json.gz")
        if os.path.exists(mpath) and os.path.exists(payload_path):
            continue
        os.makedirs(ddir, exist_ok=True)

        work, not_yet = [], []
        for cur, meta in lin.items():
            tk = ticker_for(meta["intervals"], day)
            if tk:
                work.append((tk, day))
            else:
                not_yet.append(cur)
        rev = {}
        for cur, meta in lin.items():
            tk = ticker_for(meta["intervals"], day)
            if tk:
                rev[tk] = cur

        bars, states = {}, {}
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for tk, res, code, nxt in ex.map(fetch, work):
                cur = rev.get(tk, tk)
                if code == 200 and res:
                    bars[cur] = dict(massive_ticker=tk, results=res,
                                     truncated=bool(nxt))
                    states[cur] = "OBSERVED"
                else:
                    states[cur] = "UNOBSERVED"
                    bars[cur] = dict(massive_ticker=tk, results=[], http_status=code)
        for cur in not_yet:
            states[cur] = "NOT_YET_REGULAR_WAY"

        blob = json.dumps(dict(session=day, adjusted=True, source="massive /v2/aggs 1m",
                               securities=bars), separators=(",", ":")).encode()
        gz = gzip.compress(blob, 6)
        with open(payload_path, "wb") as f:
            f.write(gz)
        sha = hashlib.sha256(gz).hexdigest()
        nrows = sum(len(v["results"]) for v in bars.values())
        man = dict(session=day, payload=os.path.relpath(payload_path, ROOT),
                   sha256=sha, bytes=len(gz), uncompressed_bytes=len(blob),
                   rows=nrows,
                   observed=sum(1 for v in states.values() if v == "OBSERVED"),
                   unobserved=sum(1 for v in states.values() if v == "UNOBSERVED"),
                   not_yet_regular_way=len(not_yet),
                   truncated=[k for k, v in bars.items() if v.get("truncated")],
                   states=states,
                   ingested_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        with open(mpath, "w") as f:
            json.dump(man, f, separators=(",", ":"))

        done_sessions += 1
        total_rows += nrows
        total_bytes += len(gz)
        el = time.time() - t0
        rate = done_sessions / el * 3600
        print(f"  {day}  obs {man['observed']:3d} unobs {man['unobserved']:3d} "
              f"notyet {man['not_yet_regular_way']:3d}  rows {nrows:>7,}  "
              f"{len(gz)/1e6:5.1f}MB  |  {done_sessions} sessions in {el/60:.1f}m "
              f"({rate:.0f}/h)", flush=True)

    el = time.time() - t0
    print(f"\n  DONE {done_sessions} sessions · {total_rows:,} rows · "
          f"{total_bytes/1e9:.2f} GB · {el/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
