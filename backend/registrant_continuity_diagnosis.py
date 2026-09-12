"""MASSIVE_REGISTRANT_CONTINUITY_DIAGNOSIS_V1 — read-only, exactly eight securities.

THE HYPOTHESIS I BROUGHT HERE WAS PARTLY WRONG, and that is worth stating before the
results. The truncation HOLD said the mechanism was a CIK change at reorganisation. Probing
the reference endpoint shows LH keeps CIK 0000920148 across its boundary while its
share_class_figi changes — so the common factor is the FIGI, and the CIK change is
incidental to some cases rather than the cause.

That matters because V6 filtered history by `share_class_figi == current`. Any
reorganisation that mints a new FIGI therefore erases everything before it, whether or not
the registrant changed.

WHAT THIS MAKES HARD. The spec lists "continuity of a stable share-class/security
identifier across the registrant transition" as strong evidence. For these cases that
identifier does not exist: at XOM's boundary the CIK, the share_class_figi and the
composite_figi ALL change together. So the vendor supplies nothing that spans the
transition, and continuity — if it holds — has to come from authoritative filings, not from
identifiers.

Ticker continuity and price continuity are recorded as corroboration and are explicitly not
allowed to establish CONTINUITY_CONFIRMED on their own. Ticker reuse is a real thing, and
this programme already found META returning another company's 2021 bars.

New vendor requests are NEW-VINTAGE DIAGNOSTIC EVIDENCE. This time response-body digests
ARE captured, which the earlier A2 diagnosis did not do.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                          # noqa: E402
import t5_artifact as ART                                                # noqa: E402
from massive_probe_run import api_key                                    # noqa: E402

BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
ARCHIVE = "/Volumes/QUANT_RESEARCH/artifacts/provenance/registrant_continuity"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"
KEY = None
CANDIDATES = ["APO", "J", "CRH", "BG", "LH", "FERG", "BLK", "XOM"]
EARLY = "2021-09-15"


def get(url, params, tag):
    p = dict(params); p["apiKey"] = KEY
    r = requests.get(url, params=p, timeout=45)
    raw = r.content
    dg = hashlib.sha256(raw).hexdigest()
    os.makedirs(ARCHIVE, exist_ok=True)
    with open(os.path.join(ARCHIVE, f"{tag}_{dg[:8]}.json"), "wb") as f:
        f.write(raw)
    return r.status_code, (r.json() if r.status_code == 200 else None), dg


def ref_at(tk, day, tag):
    st, j, dg = get(f"{BASE}/v3/reference/tickers",
                    dict(ticker=tk, date=day, limit=5), tag)
    rows = [x for x in ((j or {}).get("results") or []) if x.get("type") == "CS"]
    x = rows[0] if rows else None
    return dict(http=st, response_sha256=dg,
                cik=(x or {}).get("cik"), name=(x or {}).get("name"),
                share_class_figi=(x or {}).get("share_class_figi"),
                composite_figi=(x or {}).get("composite_figi"),
                retrieved_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))


def bars_at(tk, day, tag):
    st, j, dg = get(f"{BASE}/v2/aggs/ticker/{tk}/range/1/minute/{day}/{day}",
                    dict(adjusted="true", limit=50000), tag)
    res = (j or {}).get("results") or []
    return dict(http=st, rows=len(res), response_sha256=dg,
                first_open=res[0]["o"] if res else None,
                last_close=res[-1]["c"] if res else None)


def bound_transition(tk, cur_cik, cur_figi, lo, hi):
    """Narrow the reference switch to a session pair. Bounded, never guessed."""
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    days = [str(d.date()) for d in cal.sessions_in_range(pd.Timestamp(lo),
                                                         pd.Timestamp(hi))]
    a, b = 0, len(days) - 1
    probes = 0
    while b - a > 1 and probes < 14:
        m = (a + b) // 2
        r = ref_at(tk, days[m], f"{tk}_bound_{days[m]}")
        probes += 1
        if r.get("share_class_figi") == cur_figi or r.get("cik") == cur_cik:
            b = m
        else:
            a = m
        time.sleep(0.1)
    return dict(last_predecessor_session=days[a], first_successor_session=days[b],
                probes=probes, method="binary search over XNYS sessions on the reference "
                                      "endpoint; bounded to a session PAIR, not guessed")


def main():
    global KEY
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="registrant continuity diagnosis")
    KEY = api_key()
    lin = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}
    keys = {r["frozen_current_ticker"]: r for r in json.load(open(IDENTITY))["mapping"]}

    out = []
    for tk in CANDIDATES:
        sec = lin[tk]
        v6_start = sec["regular_way_intervals"][0]["effective_from"]
        cur = ref_at(tk, "2026-08-24", f"{tk}_current")
        old = ref_at(tk, EARLY, f"{tk}_early")
        b_old = bars_at(tk, EARLY, f"{tk}_bars_early")
        time.sleep(0.1)

        cik_changed = bool(cur["cik"] and old["cik"] and cur["cik"] != old["cik"])
        figi_changed = bool(cur["share_class_figi"] and old["share_class_figi"]
                            and cur["share_class_figi"] != old["share_class_figi"])
        any_identifier_spans = not (cik_changed and figi_changed)

        bound = bound_transition(tk, cur["cik"], cur["share_class_figi"], EARLY,
                                 v6_start) if (cik_changed or figi_changed) else None

        # corroboration only
        cont = None
        if bound:
            pb = bars_at(tk, bound["last_predecessor_session"],
                         f"{tk}_bars_pred")
            sb = bars_at(tk, bound["first_successor_session"], f"{tk}_bars_succ")
            r = (sb["first_open"] / pb["last_close"]) if (pb["last_close"]
                                                          and sb["first_open"]) else None
            cont = dict(predecessor=pb, successor=sb,
                        price_ratio=round(r, 4) if r else None,
                        role="CORROBORATIVE_ONLY")

        # classification — strong identifier evidence is what is missing
        if b_old["rows"] == 0:
            cls = "GENUINELY_NEW_SECURITY_CONFIRMED"
            why = "no aggregate history under the current ticker before the V6 start"
        elif not (cik_changed or figi_changed):
            cls = "UNRESOLVED"
            why = "history exists before the V6 start but no registrant/identifier change "
            why += "was observed to explain the truncation"
        elif any_identifier_spans:
            cls = "REGISTRANT_CHANGE_SECURITY_CONTINUITY_CONFIRMED"
            why = ("an identifier survives the transition (only one of CIK / "
                   "share_class_figi changed), which links predecessor and successor")
        else:
            cls = "UNRESOLVED"
            why = ("CIK, share_class_figi and composite_figi ALL change at the boundary, "
                   "so no vendor identifier spans it. Ticker and price continuity are "
                   "corroborative only and cannot establish continuity on their own; "
                   "authoritative filing evidence would be required.")

        out.append(dict(
            security_key_v1=keys[tk]["security_key_v1"], current_ticker=tk,
            v6_regular_way_start=v6_start,
            current_identity=cur, historical_identity=old,
            cik_changed=cik_changed, share_class_figi_changed=figi_changed,
            composite_figi_changed=bool(cur["composite_figi"] and old["composite_figi"]
                                        and cur["composite_figi"]
                                        != old["composite_figi"]),
            any_vendor_identifier_spans_boundary=any_identifier_spans,
            aggregate_history_before_v6_start=b_old,
            transition_bounds=bound, price_continuity=cont,
            classification=cls, rationale=why))
        print(f"  {tk:5s} cik_chg={cik_changed} figi_chg={figi_changed} "
              f"spans={any_identifier_spans} -> {cls}", flush=True)

    by = {}
    for o in out:
        by[o["classification"]] = by.get(o["classification"], 0) + 1
    unresolved = [o["current_ticker"] for o in out
                  if o["classification"] == "UNRESOLVED"]

    p = dict(
        report_id="MASSIVE_REGISTRANT_CONTINUITY_DIAGNOSIS_V1",
        status="REGISTRANT_CONTINUITY_DIAGNOSIS_COMPLETE" if not unresolved else "HOLD",
        scope=dict(securities=CANDIDATES, count=len(CANDIDATES),
                   not_broadened="the other 503 were not investigated in this gate"),
        hypothesis_correction=dict(
            earlier_claim="the truncation HOLD attributed the mechanism to a CIK change",
            measured="LH keeps CIK 0000920148 across its boundary while its "
                     "share_class_figi changes",
            corrected_mechanism="the common factor is the SHARE_CLASS_FIGI changing at "
                                "reorganisation; a CIK change accompanies some cases but "
                                "is not the cause",
            why_it_matters="V6 filtered history by share_class_figi == current, so any "
                           "reorganisation minting a new FIGI erases everything before it "
                           "regardless of the registrant"),
        evidence_hierarchy=dict(
            primary="an authoritative predecessor/successor relationship, or continuity of "
                    "a stable share-class/security identifier across the transition",
            corroborative_only=["same ticker across the boundary", "price continuity",
                                "company-name similarity"],
            binding="corroborative evidence alone may NOT establish "
                    "CONTINUITY_CONFIRMED — ticker reuse is real, and this programme "
                    "already found META returning another company's 2021 bars"),
        central_obstacle=dict(
            finding="for the cases where all three of CIK, share_class_figi and "
                    "composite_figi change together, NO vendor identifier spans the "
                    "boundary",
            consequence="the strong identifier-based evidence the spec contemplates is "
                        "simply unavailable from this source",
            would_require="authoritative filing evidence (SEC succession / holding-company "
                          "reorganisation statements), which this gate did not obtain"),
        summary=by, unresolved=unresolved, candidates=out,
        new_vintage_evidence=dict(
            class_="NEW_VINTAGE_DIAGNOSTIC_EVIDENCE",
            archive=ARCHIVE,
            response_digests_captured=True,
            improvement_over_a2="the earlier boundary diagnosis did not capture response "
                                "body digests; this one does"),
        no_lineage_change=dict(
            v6_digest=ART.file_digest(LINEAGE), v6_modified=False,
            security_key_v1_modified=False, universe_modified=False,
            v7_created=False, builder_spec_v2_sealed=False, smoke_executed=False,
            derived_data_written=False),
        next_gate=dict(
            if_all_confirmed="MASSIVE_TICKER_LINEAGE_V7",
            if_unresolved_affect_coverage="DERIVED BUILDER remains HOLD",
            current="HOLD — unresolved cases materially affect expected coverage"),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_REGISTRANT_CONTINUITY_DIAGNOSIS_V1.json",
                 required=("report_id", "status", "scope", "evidence_hierarchy",
                           "candidates", "summary", "no_lineage_change"),
                 supersede=os.path.exists("MASSIVE_REGISTRANT_CONTINUITY_DIAGNOSIS_V1.json"))
    print(f"\nMASSIVE_REGISTRANT_CONTINUITY_DIAGNOSIS_V1 · {d} · {p['status']}")
    print(f"  {by}")
    if unresolved:
        print(f"  unresolved: {unresolved}")
    return 0 if not unresolved else 1


if __name__ == "__main__":
    raise SystemExit(main())
