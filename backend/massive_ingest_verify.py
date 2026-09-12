"""MASSIVE_1M_RAW_INTEGRITY_V1 — did the preservation ingest preserve what it claims?

READ-ONLY. This module opens payloads and manifests, recomputes digests, and counts. It
repairs nothing, re-fetches nothing into the archive, and never rewrites a manifest. An
ingest that verifies itself by fixing what it finds is not verifying anything.

A FAILED SECURITY-SESSION IS EVIDENCE, NOT A TASK. Where UNOBSERVED appears, the first
failure and its status code stay in the manifest exactly as recorded. Retry may later be a
legitimate operational recovery, but it is a SEPARATE act with its own record: the original
failure must remain visible, because the difference between a transport blip, an entitlement
gap, a legitimately empty response and a lineage defect is only diagnosable from the first
observation.

THE REPLAY CHECK IS A SAMPLE, AND SAYS SO. A handful of sessions are re-requested and
compared by ordered row digest against what was stored. It can demonstrate drift; it cannot
demonstrate its absence across 1,254 sessions, and the report states the sample size rather
than implying full coverage.
"""
from __future__ import annotations
import gzip, hashlib, json, os, sys, time                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                         # noqa: E402
import t5_artifact as ART                                               # noqa: E402
from studio.mount_guard import require_external_volume                  # noqa: E402
from massive_probe_run import api_key                                   # noqa: E402

BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
ROOT = "/Volumes/QUANT_RESEARCH/source_data/massive/canonical_1m"
MANIFEST = os.path.join(ROOT, "_manifest")
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
WIN_START, WIN_END = "2021-08-25", "2026-08-24"
REPLAY_SESSIONS = 5


def expected_sessions():
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    return [str(d.date()) for d in
            cal.sessions_in_range(pd.Timestamp(WIN_START), pd.Timestamp(WIN_END))]


def rowdigest(rows):
    h = hashlib.sha256()
    for b in rows:
        h.update(f"{b.get('t')}|{b.get('o')}|{b.get('h')}|{b.get('l')}|{b.get('c')}|"
                 f"{b.get('v')}|{b.get('vw')}|{b.get('n')}\n".encode())
    return h.hexdigest()


def main():
    require_external_volume(purpose="raw integrity verification")
    exp = expected_sessions()
    lin = json.load(open(LINEAGE))
    n_sec = len(lin["securities"])
    wi_count = len(lin["when_issued_intervals"])
    rw_intervals = sum(len(s["regular_way_intervals"]) for s in lin["securities"])

    man_files = sorted(f[:-5] for f in os.listdir(MANIFEST) if f.endswith(".json"))
    payloads, dup = {}, []
    for dp, _, fn in os.walk(ROOT):
        if "_manifest" in dp:
            continue
        for f in fn:
            if f.endswith(".json.gz"):
                d = f[:-8]
                if d in payloads:
                    dup.append(d)
                payloads[d] = os.path.join(dp, f)

    states = dict(OBSERVED=0, UNOBSERVED=0, NOT_YET_REGULAR_WAY=0)
    rows_total = bytes_total = 0
    mismatches, unreadable, truncated = [], [], []
    unobserved_detail = []
    for d in man_files:
        m = json.load(open(os.path.join(MANIFEST, f"{d}.json")))
        rows_total += m.get("rows", 0)
        bytes_total += m.get("bytes", 0)
        for k, v in (m.get("states") or {}).items():
            states[v] = states.get(v, 0) + 1
        if m.get("truncated"):
            truncated.append(dict(session=d, securities=m["truncated"]))
        if m.get("unobserved"):
            unobserved_detail.append(dict(
                session=d, count=m["unobserved"],
                securities=[k for k, v in (m.get("states") or {}).items()
                            if v == "UNOBSERVED"][:20]))
        p = payloads.get(d)
        if not p:
            continue
        try:
            raw = open(p, "rb").read()
        except OSError as e:
            unreadable.append(dict(session=d, error=str(e)))
            continue
        if hashlib.sha256(raw).hexdigest() != m.get("sha256"):
            mismatches.append(d)

    missing_payload = [d for d in man_files if d not in payloads]
    missing_manifest = [d for d in payloads if d not in set(man_files)]
    not_ingested = [d for d in exp if d not in set(man_files)]

    # replay sample — evenly spaced, compared by ordered row digest
    key = api_key()
    replay_started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    replay, sample = [], (man_files[:: max(1, len(man_files) // REPLAY_SESSIONS)]
                          [:REPLAY_SESSIONS] if man_files else [])
    for d in sample:
        try:
            blob = json.loads(gzip.decompress(open(payloads[d], "rb").read()))
        except Exception as e:
            replay.append(dict(session=d, error=f"payload unreadable: {e}")); continue
        picks = [k for k, v in blob["securities"].items() if v.get("results")][:3]
        for cur in picks:
            stored = blob["securities"][cur]
            tk = stored["massive_ticker"]
            r = requests.get(f"{BASE}/v2/aggs/ticker/{tk}/range/1/minute/{d}/{d}",
                             params={"adjusted": "true", "limit": 50000, "apiKey": key},
                             timeout=60)
            if r.status_code != 200:
                replay.append(dict(session=d, security=cur, ticker=tk,
                                   state="REPLAY_UNAVAILABLE", http_status=r.status_code,
                                   stored_rows=len(stored["results"]),
                                   note="the source could not be re-read; this is NEITHER "
                                        "a match NOR a difference"))
                time.sleep(0.1); continue
            live = (r.json() or {}).get("results") or []
            sd, ld = rowdigest(stored["results"]), rowdigest(live)
            replay.append(dict(session=d, security=cur, ticker=tk,
                               state="REPLAY_MATCH" if sd == ld else "REPLAY_DIFFERENT",
                               stored_rows=len(stored["results"]), live_rows=len(live),
                               stored_digest=sd[:16], live_digest=ld[:16]))
            time.sleep(0.1)
    drift = [x for x in replay if x.get("state") == "REPLAY_DIFFERENT"]
    unavail = [x for x in replay if x.get("state") == "REPLAY_UNAVAILABLE"]

    pairs_expected = len(man_files) * n_sec
    pairs_seen = sum(states.values())
    checks = dict(
        all_expected_sessions_ingested=not not_ingested,
        no_missing_payloads=not missing_payload,
        no_missing_manifests=not missing_manifest,
        no_digest_mismatches=not mismatches,
        no_duplicate_session_files=not dup,
        no_unreadable_payloads=not unreadable,
        every_pair_has_a_state=pairs_seen == pairs_expected,
        no_truncated_pages=not truncated)

    p = dict(
        report_id="MASSIVE_1M_RAW_INTEGRITY_V1",
        status="PASS" if all(checks.values()) else "HOLD",
        read_only="this verifier repairs nothing, re-fetches nothing into the archive, and "
                  "rewrites no manifest",
        window=dict(start=WIN_START, end=WIN_END),
        sessions=dict(expected=len(exp), completed=len(man_files),
                      not_ingested=len(not_ingested),
                      earliest_preserved=man_files[0] if man_files else None,
                      latest_preserved=man_files[-1] if man_files else None),
        security_session_pairs=dict(expected=pairs_expected, accounted=pairs_seen,
                                    **states),
        raw=dict(rows_total=rows_total, compressed_bytes=bytes_total,
                 compressed_gb=round(bytes_total / 1e9, 3),
                 payload_files=len(payloads), manifest_entries=len(man_files)),
        integrity=dict(missing_payloads=missing_payload[:20],
                       missing_manifests=missing_manifest[:20],
                       digest_mismatches=mismatches[:20],
                       duplicate_session_files=dup[:20],
                       unreadable_payloads=unreadable[:20],
                       truncated_pages=truncated[:20],
                       canonical_raw_corruption=len(mismatches) + len(unreadable)
                       + len(dup)),
        unobserved=dict(
            total=states.get("UNOBSERVED", 0), sessions_affected=len(unobserved_detail),
            detail=unobserved_detail[:20],
            handling="the first failure and its status code remain in the manifest as "
                     "recorded. No silent retry-until-success was performed. Classifying a "
                     "failure as transport blip / entitlement gap / legitimately empty / "
                     "lineage defect is a SEPARATE diagnosis, and any recovery must carry "
                     "its own record without erasing the original observation."),
        replay_check=dict(
            classification=("SOURCE_DRIFT_DETECTED" if drift else
                            "NO_DRIFT_IN_SAMPLE" if replay and not unavail else
                            "REPLAY_UNAVAILABLE" if unavail and not replay else
                            "PARTIAL"),
            method="ordered canonical row digest, stored versus re-requested",
            replay_scope="SAMPLE_ONLY", sample_n=len(sample),
            selection_rule="evenly_spaced_sessions",
            retrieval_time=replay_started,
            comparisons=len(replay), matches=len(replay) - len(drift) - len(unavail),
            differences=len(drift), unavailable=len(unavail),
            detail=drift[:10], results=replay[:20],
            NOT_LOCAL_CORRUPTION="a replay difference means the VENDOR now returns "
                                 "something else for an interval whose stored bytes are "
                                 "internally consistent. That is SOURCE DRIFT or "
                                 "RESTATEMENT, not archive corruption, and it is excluded "
                                 "from canonical_raw_corruption and from the integrity "
                                 "acceptance gate.",
            archived_bytes_remain_authoritative="the stored payload is the authoritative "
                                                "evidence for ITS vintage; a fresh response "
                                                "is a different vintage, not a correction "
                                                "of the old one",
            limitation=f"this is a SAMPLE of {len(sample)} sessions. It can demonstrate "
                       f"drift; it cannot demonstrate its absence across "
                       f"{len(man_files)} sessions."),
        lineage=dict(artifact=LINEAGE, digest=ART.file_digest(LINEAGE),
                     securities=n_sec, regular_way_intervals=rw_intervals,
                     when_issued_excluded=wi_count,
                     states_encountered=sorted(states)),
        derived_layer="HOLD — no densification, zero-fill, RTH filtering or derived "
                      "product was created",
        acceptance=checks,
        acceptance_scope="LOCAL ARCHIVE INTEGRITY ONLY — stored-vs-manifest digests, "
                         "missing payloads, missing manifests, duplicate files, "
                         "unreadable payloads, truncated pages. Vendor replay differences "
                         "are reported separately and do NOT affect this gate.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_RAW_INTEGRITY_V1.json",
                 required=("report_id", "status", "sessions", "security_session_pairs",
                           "raw", "integrity", "acceptance"),
                 supersede=os.path.exists("MASSIVE_1M_RAW_INTEGRITY_V1.json"))
    print(f"MASSIVE_1M_RAW_INTEGRITY_V1 · {d} · {p['status']}")
    s = p["sessions"]
    print(f"  sessions {s['completed']}/{s['expected']} · {s['earliest_preserved']} .. "
          f"{s['latest_preserved']}")
    print(f"  pairs {p['security_session_pairs']['accounted']:,}/"
          f"{p['security_session_pairs']['expected']:,} · "
          f"OBSERVED {states.get('OBSERVED',0):,} · "
          f"NOT_YET {states.get('NOT_YET_REGULAR_WAY',0):,} · "
          f"UNOBSERVED {states.get('UNOBSERVED',0):,}")
    print(f"  rows {rows_total:,} · {p['raw']['compressed_gb']} GB · "
          f"payloads {len(payloads)} · manifests {len(man_files)}")
    rc = p["replay_check"]
    print(f"  LOCAL corruption {p['integrity']['canonical_raw_corruption']}")
    print(f"  replay ({rc['replay_scope']} n={rc['sample_n']}): {rc['classification']} · "
          f"match {rc['matches']} · different {rc['differences']} · "
          f"unavailable {rc['unavailable']}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
