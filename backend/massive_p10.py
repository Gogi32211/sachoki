"""MASSIVE_1M_P10_TICKER_LINEAGE_AMENDMENT_V1 — does a current ticker retrieve its own past?

THE DATA-LOSS THIS EXISTS TO PREVENT. The frozen universe is a list of CURRENT tickers. If
Massive's aggregates are keyed by ticker-at-time, then requesting META for 2021 returns
nothing — because in 2021 the security traded as FB — and an ingestion loop would record
those sessions as absent. They would flow into the pipeline as UNOBSERVED minutes, which is
the correct label for a coverage gap and the WRONG label for "we asked using the wrong
name". The error would be invisible: a real security with a real history, silently blank for
part of the window.

F5 IS ALREADY FROZEN AND INDEPENDENT. The FB -> META change was confirmed from Meta's own
investor relations before any candidate or vendor was queried, with the CUSIP explicitly
unchanged. No new ground truth is needed and no outcome is touched.

    2022-06-08  FB      last session under the old ticker
    2022-06-09  META    first session under the new one

Four queries, two tickers on each side of the boundary, classified into buckets fixed before
any of them ran.

    usage:  python massive_p10.py spec | run
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402
from massive_probe_run import api_key                                    # noqa: E402
from massive_probe_run_b import rth, session_bars                       # noqa: E402

SPEC_ART = "MASSIVE_1M_P10_TICKER_LINEAGE_AMENDMENT_V1.json"
PRE, POST = "2022-06-08", "2022-06-09"


def do_spec():
    require_external_volume(purpose="p10 spec")
    p = dict(
        spec_id="MASSIVE_1M_P10_TICKER_LINEAGE_AMENDMENT_V1",
        status="FROZEN BEFORE QUERY",
        written_after_exposure=dict(
            declaration="WRITTEN AFTER P1-P9 EXPOSURE",
            why="the frozen universe is a list of CURRENT tickers; whether a current ticker "
                "retrieves its own pre-rename history is a source-capability question that "
                "P1-P9 never asked, and getting it wrong would silently blank part of a "
                "security's history",
            no_new_ground_truth="fixture F5 (FB -> META, CUSIP unchanged) was confirmed "
                                "from Meta investor relations and sealed in "
                                "SP500_PIT_FIXTURES_V1 long before this",
            outcome="no Y value is involved"),
        fixture=dict(security="Meta Platforms, Inc.", ticker_before="FB",
                     ticker_after="META", effective="2022-06-09",
                     entity_continuity="PRESERVED", cusip="unchanged",
                     last_old_ticker_session=PRE, first_new_ticker_session=POST),
        queries=[f"{PRE} using FB", f"{PRE} using META",
                 f"{POST} using FB", f"{POST} using META"],
        classification_buckets=dict(
            CURRENT_TICKER_BACKFILLED="META retrieves the pre-change session; the current "
                                      "ticker reaches its own past",
            TICKER_AT_TIME="FB is required before 2022-06-09 and META after; requests are "
                           "keyed by the ticker as it stood",
            AMBIGUOUS="both tickers return data for a session and the payloads conflict",
            UNRESOLVED="entitlement or error prevents classification"),
        consequences=dict(
            CURRENT_TICKER_BACKFILLED="the current 503-ticker map may be used as-is for the "
                                      "whole window — as CONFIRMED vendor behaviour, not as "
                                      "an assumption",
            TICKER_AT_TIME="canonical identity becomes (security_id, "
                           "massive_ticker_at_time, effective_from, effective_to) and the "
                           "full ticker history of all 503 securities must be frozen from "
                           "Massive reference data BEFORE ingestion. CIK corroborates "
                           "issuer identity but is NOT sufficient alone, because a "
                           "dual-class issuer maps one CIK to several securities."),
        frozen_before="any of the four queries",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, SPEC_ART, required=("spec_id", "status", "fixture",
                                        "classification_buckets", "consequences"),
                 supersede=os.path.exists(SPEC_ART))
    print(f"MASSIVE_1M_P10_TICKER_LINEAGE_AMENDMENT_V1 · {d} · {p['status']}")
    print(f"  fixture FB -> META effective 2022-06-09 · 4 queries · massive queried NO")


def do_run():
    require_external_volume(purpose="p10 run")
    spec = json.load(open(SPEC_ART))
    if spec["status"] != "FROZEN BEFORE QUERY":
        raise SystemExit("spec must be frozen first")
    key = api_key()
    obs = {}
    for day in (PRE, POST):
        for tk in ("FB", "META"):
            bars, rec = session_bars(tk, day, key, f"P10_{tk}_{day}")
            r = rth(bars)
            dig = hashlib.sha256("".join(f"{b['t']}|{b['c']}" for _, b in r).encode()
                                 ).hexdigest()[:16] if r else None
            obs[f"{tk}@{day}"] = dict(rth_bars=len(r), http=rec["http_status"],
                                      first_open=r[0][1]["o"] if r else None,
                                      last_close=r[-1][1]["c"] if r else None,
                                      row_digest=dig)
            time.sleep(0.15)

    fb_pre, meta_pre = obs[f"FB@{PRE}"], obs[f"META@{PRE}"]
    fb_post, meta_post = obs[f"FB@{POST}"], obs[f"META@{POST}"]
    has = lambda o: o["rth_bars"] > 0                                    # noqa: E731

    if any(o["http"] not in (200, 404) for o in obs.values()):
        cls = "UNRESOLVED"
    elif has(meta_pre) and has(fb_pre) and meta_pre["row_digest"] != fb_pre["row_digest"]:
        cls = "AMBIGUOUS"
    elif has(meta_pre) and has(meta_post):
        cls = "CURRENT_TICKER_BACKFILLED"
    elif has(fb_pre) and not has(meta_pre) and has(meta_post) and not has(fb_post):
        cls = "TICKER_AT_TIME"
    else:
        cls = "UNRESOLVED"

    p = dict(
        report_id="MASSIVE_1M_P10_EXECUTION_V1",
        spec=dict(artifact=SPEC_ART, digest=ART.file_digest(SPEC_ART),
                  buckets_unchanged="fixed before any of the four queries ran"),
        classification=cls,
        observations=obs,
        identical_payload_pre=(fb_pre["row_digest"] == meta_pre["row_digest"]
                               if has(fb_pre) and has(meta_pre) else None),
        consequence=spec["consequences"].get(cls, "canonical identity work required before "
                                                  "ingestion"),
        ingestion_impact=("the frozen 503-ticker map is usable as-is across "
                          "2021-08-25..2026-08-24" if cls == "CURRENT_TICKER_BACKFILLED"
                          else "ticker-history freezing is REQUIRED before ingestion"),
        canonical_1m_rows_downloaded=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_P10_EXECUTION_V1.json",
                 required=("report_id", "classification", "observations"),
                 supersede=os.path.exists("MASSIVE_1M_P10_EXECUTION_V1.json"))
    print(f"MASSIVE_1M_P10_EXECUTION_V1 · {d} · {cls}")
    for k, v in obs.items():
        print(f"  {k:14s} http {v['http']} bars {v['rth_bars']:4d} "
              f"open {v['first_open']} close {v['last_close']} dig {v['row_digest']}")
    print(f"  identical payload on {PRE}: {p['identical_payload_pre']}")
    print(f"  {p['ingestion_impact']}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "spec"
    do_spec() if cmd == "spec" else do_run()
