"""Execute the frozen Massive probes. Stage A = P2, P6 (foundational).

FOUR EXECUTION RULES, ENFORCED IN CODE RATHER THAN INTENDED.

1. ARCHIVE THEN READ. Every response is written to disk and hashed BEFORE anything parses
   it, and the analysis reads the archived bytes. A parse of a live response leaves a digest
   describing a document that is merely similar to the one actually analysed.

2. PROBE DATA IS NOT INGESTION. Everything lands under massive_probe_raw/. Nothing touches
   source_data/massive/ or any production partition.

3. FROZEN BUCKETS ONLY. A response that does not land exactly in a pre-committed case is
   UNRESOLVED. No threshold is invented here and nothing is reported as "approximately".

4. THE SPEC DOES NOT MOVE. If a probe turns out to be structurally unable to separate two
   candidate semantics, its correct result is PROBE_INSUFFICIENT / UNRESOLVED. Any
   additional probe would be a new amendment that states it was written after P1-P9
   exposure.

P2 DECODES TIMESTAMPS THREE WAYS ON PURPOSE — raw vendor integer, decoded UTC, decoded
America/New_York — and the DST assertion is made on the UTC value, which is pure arithmetic
from the epoch. A timezone library bug would then show up as a disagreement between the UTC
and ET columns rather than being silently absorbed into "the vendor is tz-aware".

P6 COMPARES REPLAYS BY ORDERED ROW DIGEST, not by row count. Two paginations can agree on
how many rows they returned and disagree on which.

    usage:  python massive_probe_run.py a
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                      # noqa: E402
from datetime import datetime, timezone                                  # noqa: E402
from zoneinfo import ZoneInfo                                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                          # noqa: E402
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402

RAW = "/Volumes/QUANT_RESEARCH/artifacts/provenance/massive_probe_raw"
BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
SPEC = "MASSIVE_1M_PROBE_SPEC_V1.json"
ET = ZoneInfo("America/New_York")
_log: list = []


def api_key():
    for line in open(".env"):
        if line.strip().startswith("MASSIVE_API_KEY="):
            return line.strip().split("=", 1)[1].strip()
    raise RuntimeError("MASSIVE_API_KEY not found")


def get_archived(url, params, tag, key):
    """Fetch, archive raw bytes, hash, THEN return the parsed body from those bytes."""
    os.makedirs(RAW, exist_ok=True)
    p = dict(params or {})
    p["apiKey"] = key
    safe = {k: v for k, v in p.items() if k != "apiKey"}
    r = requests.get(url, params=p, timeout=90)
    raw = r.content
    sha = hashlib.sha256(raw).hexdigest()
    fn = f"{tag}_{sha[:10]}.json"
    with open(os.path.join(RAW, fn), "wb") as f:
        f.write(raw)
    rec = dict(tag=tag, endpoint=url.split("?")[0], params=safe,
               retrieved_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               http_status=r.status_code, raw_filename=fn, sha256=sha,
               bytes=len(raw))
    _log.append(rec)
    if r.status_code != 200:
        return None, rec
    with open(os.path.join(RAW, fn), "rb") as f:          # read back from the archive
        return json.loads(f.read()), rec


def agg_url(tk, d1, d2):
    return f"{BASE}/v2/aggs/ticker/{tk}/range/1/minute/{d1}/{d2}"


def rowdigest(rows):
    """Ordered canonical digest of raw rows — identity of WHICH rows, not how many."""
    h = hashlib.sha256()
    for b in rows:
        h.update(f"{b.get('t')}|{b.get('o')}|{b.get('h')}|{b.get('l')}|{b.get('c')}|"
                 f"{b.get('v')}|{b.get('vw')}|{b.get('n')}\n".encode())
    return h.hexdigest()


# ── P2 ─────────────────────────────────────────────────────────────────────────
def probe_p2(key):
    out = dict(probe="P2", topic="timestamp unit, labelling and timezone")
    sessions = {"winter": "2024-01-16", "summer": "2024-07-16"}
    per = {}
    for name, day in sessions.items():
        j, rec = get_archived(agg_url("AAPL", day, day), {"limit": 50000}, f"P2_{name}", key)
        if not j or not j.get("results"):
            per[name] = dict(error="no results", archive=rec)
            continue
        ts = [b["t"] for b in j["results"]]
        mn = min(ts)
        unit = ("MILLISECONDS" if 1e12 <= mn < 1e13 else
                "NANOSECONDS" if mn >= 1e18 else "UNRESOLVED")
        div = 1000.0 if unit == "MILLISECONDS" else (1e9 if unit == "NANOSECONDS" else None)
        if div is None:
            per[name] = dict(unit="UNRESOLVED", min_t=mn, archive=rec)
            continue
        dec = []
        for t in ts:
            u = datetime.fromtimestamp(t / div, tz=timezone.utc)
            dec.append((t, u, u.astimezone(ET)))
        dec.sort(key=lambda x: x[0])
        first_rth = next((d for d in dec if (d[2].hour, d[2].minute) >= (9, 30)), None)
        per[name] = dict(
            unit=unit, n_bars=len(ts),
            first_bar_raw=dec[0][0], first_bar_utc=dec[0][1].strftime("%Y-%m-%d %H:%M:%S"),
            first_bar_et=dec[0][2].strftime("%Y-%m-%d %H:%M:%S %Z"),
            first_rth_raw=first_rth[0] if first_rth else None,
            first_rth_utc=first_rth[1].strftime("%H:%M:%S") if first_rth else None,
            first_rth_et=first_rth[2].strftime("%H:%M:%S") if first_rth else None,
            last_bar_et=dec[-1][2].strftime("%Y-%m-%d %H:%M:%S %Z"),
            archive=rec)
    out["sessions"] = per

    units = {per[k].get("unit") for k in per}
    out["unit"] = units.pop() if len(units) == 1 else "UNRESOLVED"

    et_times = {k: per[k].get("first_rth_et") for k in per}
    if all(v == "09:30:00" for v in et_times.values()):
        out["label"] = "BAR_START"
    elif all(v == "09:31:00" for v in et_times.values()):
        out["label"] = "BAR_END"
    else:
        out["label"] = "UNRESOLVED"

    wu, su = per.get("winter", {}).get("first_rth_utc"), per.get("summer", {}).get(
        "first_rth_utc")
    if wu and su and wu == su:
        out["timezone"] = "FAIL_FIXED_OFFSET"
    elif wu == "14:30:00" and su == "13:30:00" and out["label"] == "BAR_START":
        out["timezone"] = "PASS"
    else:
        out["timezone"] = "UNRESOLVED"
    out["dst_assertion"] = dict(
        method="asserted on the UTC value, which is pure arithmetic from the epoch; the "
               "ET column corroborates and would disagree if the tz library were wrong",
        expected_winter_utc="14:30:00", observed_winter_utc=wu,
        expected_summer_utc="13:30:00", observed_summer_utc=su)
    out["verdict"] = ("PASS" if out["unit"] != "UNRESOLVED" and out["label"] != "UNRESOLVED"
                      and out["timezone"] == "PASS" else "UNRESOLVED")
    return out


# ── P6 ─────────────────────────────────────────────────────────────────────────
def paginate(tk, d1, d2, key, tag):
    rows, pages, n, url, params = [], [], 0, agg_url(tk, d1, d2), {"limit": 50000}
    explicit_end = False
    while url and n < 40:
        j, rec = get_archived(url, params if n == 0 else {}, f"{tag}_p{n:02d}", key)
        pages.append(rec)
        if not j:
            break
        got = j.get("results") or []
        rows.extend(got)
        nxt = j.get("next_url")
        if not nxt:
            explicit_end = True
            break
        url, params, n = nxt, {}, n + 1
        time.sleep(0.2)
    return rows, pages, explicit_end


def probe_p6(key):
    out = dict(probe="P6", topic="pagination limit and interval completeness")

    j, rec = get_archived(agg_url("AAPL", "2024-05-15", "2024-05-15"), {"limit": 50000},
                          "P6_short", key)
    short = j.get("results") or [] if j else []
    st = sorted(b["t"] for b in short)
    out["short_session"] = dict(
        date="2024-05-15", n_bars=len(short),
        strictly_increasing=all(b < a for b, a in zip(st, st[1:])),
        duplicates=len(st) - len(set(st)), archive=rec)

    r1, p1, e1 = paginate("AAPL", "2024-01-01", "2024-12-31", key, "P6_long_replay1")
    r2, p2, e2 = paginate("AAPL", "2024-01-01", "2024-12-31", key, "P6_long_replay2")
    d1, d2 = rowdigest(r1), rowdigest(r2)
    t1 = [b["t"] for b in r1]
    dup = len(t1) - len(set(t1))
    mono = all(b < a for b, a in zip(t1, t1[1:]))

    # page-boundary gap test: re-request the 10 minutes spanning each boundary alone
    boundaries, gaps = [], []
    cum = 0
    for rec_ in p1[:-1]:
        cum += 0
    idx, acc = [], 0
    for k, pg in enumerate(p1):
        acc += 0
    # boundary timestamps = last t of each page except the final one
    per_page_counts = []
    off = 0
    for k in range(len(p1)):
        fn = p1[k].get("raw_filename")
        if not fn:
            continue
        with open(os.path.join(RAW, fn), "rb") as f:
            pj = json.loads(f.read())
        cnt = len(pj.get("results") or [])
        per_page_counts.append(cnt)
        off += cnt
        if k < len(p1) - 1 and cnt:
            boundaries.append((pj["results"][-1]["t"], k))

    have = set(t1)
    for bt, k in boundaries:
        lo = datetime.fromtimestamp(bt / 1000.0, tz=timezone.utc)
        d = lo.strftime("%Y-%m-%d")
        j2, rec2 = get_archived(agg_url("AAPL", d, d), {"limit": 50000},
                                f"P6_boundary_p{k:02d}", key)
        iso = {b["t"] for b in (j2.get("results") or [])} if j2 else set()
        window = {t for t in iso if abs(t - bt) <= 10 * 60 * 1000}
        missing = sorted(window - have)
        if missing:
            gaps.append(dict(page=k, boundary_t=bt, missing=len(missing),
                             examples=missing[:3]))
        time.sleep(0.2)

    ok = (dup == 0 and mono and not gaps and d1 == d2 and e1 and e2)
    out.update(
        long_range=dict(
            range="2024-01-01..2024-12-31", rows_replay1=len(r1), rows_replay2=len(r2),
            pages_replay1=len(p1), pages_replay2=len(p2),
            max_rows_per_page=max(per_page_counts) if per_page_counts else 0,
            duplicates=dup, strictly_increasing=mono,
            explicit_exhaustion_replay1=e1, explicit_exhaustion_replay2=e2),
        replay=dict(
            method="ordered canonical raw-row digest, not row count",
            digest_replay1=d1, digest_replay2=d2, identical=(d1 == d2)),
        page_boundary_test=dict(boundaries_tested=len(boundaries), gaps_found=len(gaps),
                                gaps=gaps),
        verdict=("PASS" if ok else
                 "UNRESOLVED" if not (e1 and e2) else "FAIL"))
    return out


def main():
    require_external_volume(purpose="massive probe stage A")
    key = api_key()
    spec_digest = ART.file_digest(SPEC)
    t0 = time.time()

    p2 = probe_p2(key)
    print(f"  P2 {p2['verdict']}  unit={p2['unit']} label={p2['label']} "
          f"tz={p2['timezone']}")
    p6 = probe_p6(key)
    print(f"  P6 {p6['verdict']}  rows={p6['long_range']['rows_replay1']} "
          f"pages={p6['long_range']['pages_replay1']} "
          f"dup={p6['long_range']['duplicates']} "
          f"replay_identical={p6['replay']['identical']} "
          f"gaps={p6['page_boundary_test']['gaps_found']}")

    gate = ("PASS" if p2["verdict"] == "PASS" and p6["verdict"] == "PASS"
            else "HOLD — SOURCE_SEMANTICS_HOLD")
    p = dict(
        report_id="MASSIVE_1M_PROBE_EXECUTION_STAGE_A_V1",
        stage="A — foundational (P2 timestamp, P6 pagination/completeness)",
        spec=dict(artifact=SPEC, digest=spec_digest,
                  unchanged="no acceptance boundary was altered; the spec was frozen at "
                            "this digest before any response was seen"),
        foundational_gate=gate,
        probes=[p2, p6],
        rules_applied=["raw response archived and hashed before it was parsed; analysis "
                       "read the archived bytes",
                       "probe data written only under massive_probe_raw/, never to "
                       "source_data/massive/ or any production partition",
                       "classification confined to frozen buckets; anything else is "
                       "UNRESOLVED",
                       "no spec change after exposure"],
        raw_archive=dict(dir=RAW, requests=len(_log), log=_log),
        canonical_1m_rows_downloaded=0,
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_PROBE_EXECUTION_STAGE_A_V1.json",
                 required=("report_id", "stage", "foundational_gate", "probes"),
                 supersede=os.path.exists("MASSIVE_1M_PROBE_EXECUTION_STAGE_A_V1.json"))
    print(f"\nMASSIVE_1M_PROBE_EXECUTION_STAGE_A_V1 · {d}")
    print(f"  FOUNDATIONAL GATE {gate}")
    print(f"  requests archived: {len(_log)} · canonical 1m rows downloaded: 0")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
