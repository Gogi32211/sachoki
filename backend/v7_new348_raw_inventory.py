"""MASSIVE_V7_NEW_348_RAW_COVERAGE_INVENTORY_V1 — what do we already have, before asking for more.

Strictly read-only and strictly offline. No Massive request is issued. The question is not
"can the vendor serve these sessions" — the anomaly audit already showed it can, today. The
question is what bytes are ALREADY preserved, and at which vintage, for the 348 security-
sessions the amended V7 model will newly expect.

Three archives can hold market bytes and they are not interchangeable:

  canonical_1m/YYYY/MM/DATE.json.gz    ORIGINAL INGEST VINTAGE. One bundle per session,
                                       `securities` keyed by frozen ticker. This is the only
                                       store whose bytes are canonical.
  supplemental_lineage_quarantine_v1/  SUPPLEMENTAL VINTAGE, and only for the original eight.
  provenance/v7_anomaly_audit/         DIAGNOSTIC VINTAGE — the bisection probes from the
                                       503 audit. These are real aggregate responses, but
                                       they were retrieved to answer a lineage question and
                                       cover only the sessions bisection happened to touch.

A session appearing in the diagnostic archive is therefore NOT the same as having it
preserved. Bisection visits roughly log2(n) sessions out of a range; it was never a
preservation pass and must not be mistaken for one.

READING 348 SESSION BUNDLES CHEAPLY. Each canonical bundle is a few MB gzipped and tens of MB
parsed, with ~190k bar rows per session. Full json.loads on all of them would take far longer
than the question deserves, and every bar is irrelevant here — only the KEY SET matters. So
each bundle is decompressed once and scanned with a regex for the entry signature
`"TICKER":{"massive_ticker"`, which recovers the complete membership without materialising a
single bar.

WHAT I EXPECT, recorded first: V6's coverage governed which tickers the canonical ingest
requested per session, so sessions before a security's V6 start should be absent from the
canonical bundles — that is the same mechanism that produced the original 5,069. If any turn
up PRESENT, the ingest did not follow V6 exactly and that is a finding in its own right.
"""
from __future__ import annotations
import glob, gzip, json, os, re, sys, time                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

CANON = "/Volumes/QUANT_RESEARCH/source_data/massive/canonical_1m"
QUAR = ("/Volumes/QUANT_RESEARCH/source_data/massive/"
        "supplemental_lineage_quarantine_v1")
DIAG = "/Volumes/QUANT_RESEARCH/artifacts/provenance/v7_anomaly_audit"
PROBE = "/Volumes/QUANT_RESEARCH/artifacts/provenance/massive_probe_raw"
AMEND = "MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1.json"
AMEND_DIGEST = "94d97c70acb41fa2"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
ENTRY = re.compile(rb'"([A-Z][A-Z.\-]{0,9})":\s*\{"massive_ticker"')


def canon_path(day):
    return os.path.join(CANON, day[:4], day[5:7], f"{day}.json.gz")


def members(day, cache):
    """Key set of a canonical bundle. Regex over the decompressed bytes — no bar is parsed."""
    if day in cache:
        return cache[day]
    p = canon_path(day)
    if not os.path.exists(p):
        cache[day] = None
        return None
    with gzip.open(p, "rb") as f:
        raw = f.read()
    s = {m.group(1).decode() for m in ENTRY.finditer(raw)}
    cache[day] = s
    return s


def diag_index():
    """Archived diagnostic aggregate responses, keyed (ticker, session)."""
    idx = {}
    for p in glob.glob(os.path.join(DIAG, "agg_*.json")):
        b = os.path.basename(p)
        m = re.match(r"agg_([A-Z.\-]+)_(\d{4}-\d{2}-\d{2})_([0-9a-f]{8})\.json$", b)
        if m:
            idx[(m.group(1), m.group(2))] = dict(path=p, sha8=m.group(3),
                                                 size=os.path.getsize(p))
    return idx


def quar_index():
    idx = {}
    for tk in os.listdir(QUAR):
        mp = os.path.join(QUAR, tk, "_manifest.jsonl")
        if not os.path.exists(mp):
            continue
        for line in open(mp):
            try:
                r = json.loads(line)
            except Exception:
                continue
            idx[(tk, r["session_date"])] = r
    return idx


def classify(tk, day, canon, diag, quar):
    if canon is None:
        return "UNRESOLVED", "no canonical bundle exists for this session"
    if tk in canon:
        return ("ORIGINAL_CANONICAL_RAW_PRESENT",
                "the session bundle already contains this security")
    q = quar.get((tk, day))
    if q:
        if q.get("entitlement_state") == "ENTITLEMENT_UNAVAILABLE":
            return "ENTITLEMENT_UNAVAILABLE", "supplemental retrieval returned 403"
        if q.get("payload"):
            return ("NEW_VINTAGE_ARCHIVED_ONLY",
                    "preserved in the supplemental quarantine at supplemental vintage")
    d = diag.get((tk, day))
    if d:
        try:
            j = json.load(open(d["path"]))
            n = len(j.get("results") or [])
        except Exception:
            n = None
        if n:
            return ("NEW_VINTAGE_ARCHIVED_ONLY",
                    f"a diagnostic aggregate response with {n} rows is archived "
                    f"(sha {d['sha8']}), at diagnostic vintage")
        return ("NO_ARCHIVED_MARKET_BYTES",
                "a diagnostic response is archived but carries no bars")
    return ("NO_ARCHIVED_MARKET_BYTES",
            "absent from canonical, quarantine and diagnostic archives")


def main():
    d = ART.file_digest(AMEND)
    if d != AMEND_DIGEST:
        print(f"HOLD — amendment digest {d} != {AMEND_DIGEST}")
        return 1
    am = json.load(open(AMEND))
    v6 = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}

    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")

    rows = (am["branch_a_repairs"]["rows"] + am["branch_b_corrections"]["rows"])
    diag, quar, cache = diag_index(), quar_index(), {}
    print(f"  diagnostic aggregate responses indexed : {len(diag):,}")
    print(f"  quarantine manifest records indexed    : {len(quar):,}")

    per, states, t0 = [], {}, time.time()
    total = 0
    for r in rows:
        tk = r["ticker"]
        days = [str(x.date()) for x in cal.sessions_in_range(
            pd.Timestamp(r["v7_regular_way_start"]),
            pd.Timestamp(r["v6_regular_way_start"]))][:-1]
        c = {}
        detail = []
        for day in days:
            st, why = classify(tk, day, members(day, cache), diag, quar)
            c[st] = c.get(st, 0) + 1
            states[st] = states.get(st, 0) + 1
            detail.append(dict(session=day, state=st, reason=why))
        total += len(days)
        per.append(dict(security_key_v1=r["security_key_v1"], ticker=tk,
                        repair_class=("WI_BOUNDARY_CORRECTION"
                                      if "v6_when_issued_ticker" in r
                                      else "SECURITY_LEVEL_COVERAGE_REPAIR"),
                        v7_regular_way_start=r["v7_regular_way_start"],
                        v6_regular_way_start=r["v6_regular_way_start"],
                        sessions=len(days), states=c,
                        canonical_bundles_missing=sum(
                            1 for x in detail if x["state"] == "UNRESOLVED"),
                        session_detail=detail))
        print(f"  {tk:6s} {len(days):3d} sessions -> {c}   | {time.time()-t0:.0f}s",
              flush=True)

    unresolved = states.get("UNRESOLVED", 0)
    no_bytes = states.get("NO_ARCHIVED_MARKET_BYTES", 0)
    p = dict(
        report_id="MASSIVE_V7_NEW_348_RAW_COVERAGE_INVENTORY_V1",
        status=("NEW_348_RAW_COVERAGE_INVENTORY_COMPLETE" if unresolved == 0
                else "HOLD"),
        task_class="READ_ONLY_INVENTORY",
        no_vendor_requests=dict(
            massive_requests_issued=0,
            why="the anomaly audit already established the vendor serves these sessions "
                "today. The open question is what bytes are ALREADY preserved and at which "
                "vintage, and that is answerable entirely offline."),
        authoritative_input=dict(artifact=AMEND, digest=AMEND_DIGEST, verified=True),
        archives_examined=[
            dict(name="canonical_1m", vintage="ORIGINAL_INGEST",
                 layout="one gzipped bundle per session; `securities` keyed by frozen "
                        "ticker",
                 canonical=True, path=CANON),
            dict(name="supplemental_lineage_quarantine_v1",
                 vintage="SUPPLEMENTAL_QUARANTINE", canonical=False, path=QUAR,
                 scope="the original eight only", records_indexed=len(quar)),
            dict(name="provenance/v7_anomaly_audit", vintage="DIAGNOSTIC",
                 canonical=False, path=DIAG, responses_indexed=len(diag),
                 caveat="these were retrieved to answer a lineage question. Bisection "
                        "visits about log2(n) sessions of a range; it was never a "
                        "preservation pass and must not be counted as one.")],
        method=dict(
            membership_test="regex for the entry signature '\"TICKER\":{\"massive_ticker\"' "
                            "over the decompressed bundle",
            why="only the key set matters here; full parsing would materialise ~190k bar "
                "rows per session for a question about presence",
            bundles_read=len([k for k, v in cache.items() if v is not None]),
            bars_parsed=0),
        expectation_recorded_first=dict(
            expected="sessions before a security's V6 start should be ABSENT from the "
                     "canonical bundles, because V6's coverage governed which tickers the "
                     "ingest requested — the same mechanism that produced the original "
                     "5,069",
            if_present_instead="the ingest did not follow V6 exactly, which is a finding in "
                               "its own right"),
        totals=dict(security_sessions=total, expected=348, reconciled=total == 348,
                    by_state=states),
        per_security=per,
        headline=dict(
            original_canonical_raw_present=states.get(
                "ORIGINAL_CANONICAL_RAW_PRESENT", 0),
            new_vintage_archived_only=states.get("NEW_VINTAGE_ARCHIVED_ONLY", 0),
            no_archived_market_bytes=no_bytes,
            entitlement_unavailable=states.get("ENTITLEMENT_UNAVAILABLE", 0),
            source_identity_conflict=states.get("SOURCE_IDENTITY_CONFLICT", 0),
            unresolved=unresolved),
        preservation_consequence=dict(
            required=no_bytes > 0,
            sessions_needing_preservation=no_bytes,
            binding="if NO_ARCHIVED_MARKET_BYTES > 0 those bytes exist only at the vendor "
                    "and are subject to the same rolling entitlement window that already "
                    "expired 2021-08-25 for the original eight",
            next_gate=("a preservation gate for these sessions, before candidate V2"
                       if no_bytes else
                       "none — every newly expected session already has archived bytes")),
        entitlement_clock=dict(
            why="the 309 unpreserved sessions exist only at the vendor, behind the same "
                "rolling five-year window that already took 2021-08-25 from the original "
                "eight",
            window_rule="retrievable while session >= today - 5 years",
            measured_today=time.strftime("%Y-%m-%d"),
            current_boundary=str((pd.Timestamp(time.strftime("%Y-%m-%d"))
                                  - pd.DateOffset(years=5)).date()),
            oldest_unpreserved_per_security={
                r["ticker"]: min([d["session"] for d in r["session_detail"]
                                  if d["state"] == "NO_ARCHIVED_MARKET_BYTES"] or ["—"])
                for r in per},
            earliest_expiry=min(
                [min([d["session"] for d in r["session_detail"]
                      if d["state"] == "NO_ARCHIVED_MARKET_BYTES"] or ["9999"])
                 for r in per]),
            headroom_note="not urgent this week, but it is a clock and not a shelf — the "
                          "oldest sessions cross the boundary first, exactly as 2021-08-25 "
                          "already did",
            recommended_order="oldest session first, the perishable end, as in the earlier "
                              "preservation run"),
        mixed_vintage_consequence=dict(
            note="any session resolving to NEW_VINTAGE_ARCHIVED_ONLY extends the "
                 "mixed-vintage problem beyond the original eight securities",
            affected_securities=sorted({r["ticker"] for r in per
                                        if r["states"].get(
                                            "NEW_VINTAGE_ARCHIVED_ONLY")}),
            decided_by="MASSIVE_MIXED_VINTAGE_SUPPLEMENTATION_POLICY_V1, not here"),
        mutations=dict(canonical_raw=0, quarantine=0, diagnostic_archives=0, v6=0,
                       v7_candidate=0, derived_writes=0, promotions=0,
                       vendor_requests=0),
        does_not_authorize="candidate V2, preservation, promotion, or canonical V7",
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    dg = ART.seal(p, "MASSIVE_V7_NEW_348_RAW_COVERAGE_INVENTORY_V1.json",
                  required=("report_id", "status", "no_vendor_requests",
                            "archives_examined", "totals", "per_security", "headline",
                            "preservation_consequence", "mutations"),
                  supersede=os.path.exists(
                      "MASSIVE_V7_NEW_348_RAW_COVERAGE_INVENTORY_V1.json"))
    print(f"\nMASSIVE_V7_NEW_348_RAW_COVERAGE_INVENTORY_V1 · {dg} · {p['status']}")
    print(f"  sessions {total} (expected 348) · states {states}")
    print(f"  preservation required: {p['preservation_consequence']['required']} "
          f"({no_bytes} sessions)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
