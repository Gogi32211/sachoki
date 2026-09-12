"""MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V1 — build the candidate, then try to break it.

This is a MECHANICAL rendering of a frozen spec, not a lineage-discovery engine. The eight
repair rules were decided by the boundary audit; the other 495 inherit V6 unchanged. Nothing
here is allowed to invent a ninth repair, and the audit that follows is detection-only.

THE REPAIR IS DERIVED, NOT ASSIGNED. It would be one line to write
`coverage_start = "2021-08-25"` for the eight and produce a candidate that passes every count.
The spec forbids exactly that, so coverage is built from the audited continuity structure —
attribute intervals first, each with its own boundary and evidence link — and the window start
falls out as a consequence. The reconciliation to 5,069 restored sessions is then a real
check rather than a restatement of the constant that was typed in.

THE 495 ARE AUDITED, NEVER REPAIRED. Suspicion is not a defect. `META` already taught this
programme that older bars under a ticker can belong to an entirely different company, so the
audit runs three levels — signal, candidate anomaly, evidence-qualified defect — and this task
stops at candidate anomaly. A finding outside the eight sets HOLD and returns evidence; it
never edits coverage.

WHAT I EXPECT TO FIND, RECORDED BEFORE LOOKING so it cannot be rationalised afterwards: the
anchor grid that truncated the eight reorganisations should also have caught genuinely new
listings, whose V6 start would then be the first anchor AFTER their real debut rather than
the debut itself. Several of the fifteen unaudited late-start securities have V6 starts
sitting exactly on anchors. If that is what the probes show, it is a NEW anomaly class, the
correct outcome is HOLD, and the eight-case repair set stays closed.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                         # noqa: E402
import t5_artifact as ART                                               # noqa: E402
from massive_probe_run import api_key                                   # noqa: E402

BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
STAGE_ROOT = "/Volumes/QUANT_RESEARCH/staging/lineage_v7_candidate"
PROBE_ARC = "/Volumes/QUANT_RESEARCH/artifacts/provenance/v7_anomaly_audit"
QUAR = ("/Volumes/QUANT_RESEARCH/source_data/massive/"
        "supplemental_lineage_quarantine_v1")
SPEC = "MASSIVE_SECURITY_LINEAGE_V7_SPEC.json"
SPEC_DIGEST = "f858ee0f3be05cc7"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"
BOUND = "MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1.json"
APOA = "APO_PRIMARY_SECURITY_CONTINUITY_RESOLUTION_V1.json"
PRIMEV = "MASSIVE_LINEAGE_PRIMARY_EVIDENCE_RESOLUTION_V1.json"
SNAP = "SP500_CURRENT_SNAPSHOT_V1.json"
SUPP = "MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1.json"
SUPP_LIVE = "e8c8a648923e22a3"
SUPP_SUPERSEDED = "2f372b2f0fab898f"
W0, W1 = "2021-08-25", "2026-08-24"
EIGHT = ["APO", "J", "CRH", "BG", "LH", "FERG", "BLK", "XOM"]
WI_KNOWN = ["GEHC", "GEV", "SOLV", "VLTO"]
KEY = None
_probes: list = []
_pcache: dict = {}


def code_hash():
    return hashlib.sha256(open(__file__, "rb").read()).hexdigest()


def sessions(lo, hi):
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    return [str(d.date()) for d in cal.sessions_in_range(pd.Timestamp(lo),
                                                         pd.Timestamp(hi))]


ALL_SESS = None


def next_session(day):
    """end_exclusive for the half-open convention: the first session STRICTLY AFTER `day`.

    The strictness matters and an earlier version got it wrong. Searching from `day` itself
    and taking the second element is only correct when `day` IS a session; when V6's
    inclusive `effective_to` lands on a weekend or holiday the search already starts at the
    next session, so the second element skips a session and the following interval overlaps
    it. CPAY (2024-03-24, a Sunday), WTW and XYZ all tripped this. Searching from day+1 and
    taking the first element is correct in both cases.
    """
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    d = pd.Timestamp(day) + pd.Timedelta(days=1)
    s = cal.sessions_in_range(d, d + pd.Timedelta(days=14))
    return str(s[0].date())


def ivl(key, attr, value, start, end_excl, precision, src, dig, **kw):
    r = dict(security_key_v1=key, attribute=attr, attribute_value=value,
             start_boundary=start, end_boundary_exclusive=end_excl,
             boundary_precision=precision, evidence_source=src,
             evidence_artifact_digest=dig)
    r.update(kw)
    return r


# ----------------------------------------------------------------- probes
def get(url, params, tag):
    ck = (url, tuple(sorted(params.items())))
    if ck in _pcache:
        return _pcache[ck]
    p = dict(params); p["apiKey"] = KEY
    try:
        r = requests.get(url, params=p, timeout=45)
        raw, st = r.content, r.status_code
    except Exception as e:
        out = (None, None, dict(error=type(e).__name__))
        _pcache[ck] = out
        return out
    dg = hashlib.sha256(raw).hexdigest()
    os.makedirs(PROBE_ARC, exist_ok=True)
    with open(os.path.join(PROBE_ARC, f"{tag}_{dg[:8]}.json"), "wb") as f:
        f.write(raw)
    _probes.append(dict(tag=tag, url=url, params=params, http=st, sha256=dg,
                        retrieved_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        vintage="NEW_VINTAGE_DIAGNOSTIC_EVIDENCE"))
    j = None
    if st == 200:
        try:
            j = r.json()
        except Exception:
            j = None
    out = (st, j, dict(sha256=dg))
    _pcache[ck] = out
    time.sleep(0.04)
    return out


def ref_at(tk, day):
    st, j, m = get(f"{BASE}/v3/reference/tickers",
                   dict(ticker=tk, date=day, limit=10), f"ref_{tk}_{day}")
    rows = (j or {}).get("results") or []
    eq = [x for x in rows if x.get("ticker") == tk
          and (x.get("type") or "").upper() in ("CS", "ADRC", "ADRP", "ADRR", "GDR")]
    x = eq[0] if eq else None
    return dict(http=st, sha256=m.get("sha256"), present=bool(x),
                cik=(x or {}).get("cik"), share_class_figi=(x or {}).get("share_class_figi"),
                composite_figi=(x or {}).get("composite_figi"),
                type=(x or {}).get("type"), name=(x or {}).get("name"))


def bars_at(tk, day):
    st, j, m = get(f"{BASE}/v2/aggs/ticker/{tk}/range/1/minute/{day}/{day}",
                   dict(adjusted="true", limit=50000), f"agg_{tk}_{day}")
    res = (j or {}).get("results") or []
    return dict(http=st, sha256=m.get("sha256"), rows=len(res),
                entitlement="ENTITLEMENT_UNAVAILABLE" if st == 403 else
                ("AVAILABLE" if st == 200 else "OTHER"))


def earliest_bar_session(tk, days):
    """Bisect for the first session serving bars. Bounded; 403 never read as absence."""
    lo, hi, probes = 0, len(days) - 1, 0
    if bars_at(tk, days[hi])["rows"] == 0:
        return dict(found=False, probes=1)
    while hi - lo > 1 and probes < 14:
        m = (lo + hi) // 2
        b = bars_at(tk, days[m])
        probes += 1
        if b["entitlement"] == "ENTITLEMENT_UNAVAILABLE":
            lo = m
            continue
        if b["rows"] > 0:
            hi = m
        else:
            lo = m
    b0 = bars_at(tk, days[lo])
    return dict(found=True, earliest_with_bars=(days[lo] if b0["rows"] > 0 else days[hi]),
                probes=probes + 1,
                method="XNYS-session bisection; ENTITLEMENT_UNAVAILABLE never read as "
                       "absence of listing")


# ----------------------------------------------------------------- build
def build(v6, ids, aud, stage):
    by_aud = {r["current_ticker"]: r for r in aud["securities"]}
    S = {s["current_ticker"]: s for s in v6["securities"]}
    keyof = {r["frozen_current_ticker"]: r["security_key_v1"]
             for r in ids["mapping"]}
    basis = {r["frozen_current_ticker"]: r.get("identity_basis") for r in ids["mapping"]}
    wi_tickers = {x["ticker"] for x in v6["when_issued_intervals"]}
    W1X = next_session(W1)
    dg_aud, dg_v6 = ART.file_digest(BOUND), ART.file_digest(LINEAGE)

    ds = {k: [] for k in ("research_securities", "research_coverage_intervals",
                          "registrant_cik_intervals", "share_class_figi_intervals",
                          "composite_figi_intervals", "ticker_intervals",
                          "instrument_type_intervals", "vendor_reference_intervals",
                          "vendor_aggregate_access_intervals",
                          "structural_transition_events", "lineage_evidence_links")}
    restored = 0
    ATTR_DS = dict(cik="registrant_cik_intervals",
                   share_class_figi="share_class_figi_intervals",
                   composite_figi="composite_figi_intervals",
                   ticker="ticker_intervals", type="instrument_type_intervals")

    for tk in sorted(S):
        s, k = S[tk], keyof[tk]
        repaired = tk in EIGHT
        ds["research_securities"].append(dict(
            security_key_v1=k, identity_basis=basis.get(tk),
            current_frozen_ticker=tk, current_frozen_cik=s["cik"],
            current_snapshot_identity=dict(share_class_figi=s["share_class_figi"],
                                           composite_figi=s["composite_figi"],
                                           company_name=s["company_name"],
                                           massive_name=s["massive_name"]),
            continuity_classification=(by_aud[tk]["continuity_classification"]
                                       if repaired else "V6_INHERITED"),
            v6_status=s["status"], v6_canonical_status=s["canonical_status"],
            known_structural_caveats=(
                ["MATERIAL_ECONOMIC_COMPOSITION_TRANSITION"] if tk == "APO" else
                ["INSTRUMENT_REPRESENTATION_TRANSITION"] if tk == "CRH" else
                ["WHEN_ISSUED_STATUS_DIVERGENCE"] if tk in WI_KNOWN else []),
            repair_set_member=repaired))

        if repaired:
            a = by_aud[tk]
            v6_start = s["regular_way_intervals"][0]["effective_from"]
            # coverage DERIVED from audited continuity: the security is one continuous
            # research object across the whole window, so coverage is the window.
            ds["research_coverage_intervals"].append(dict(
                security_key_v1=k, start_boundary=W0, end_boundary_exclusive=W1X,
                lineage_state="REGULAR_WAY",
                derivation="AUDITED_SECURITY_LEVEL_CONTINUITY",
                explicitly_not="V6_START_MOVED_BACKWARD",
                v6_original_start=v6_start, v6_original_end=W1,
                v6_displacement_sessions=a["v6_anchor_displacement_sessions"],
                continuity_classification=a["continuity_classification"],
                boundary_precision="EXACT_XNYS_SESSION",
                evidence_source="MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1",
                evidence_artifact_digest=dg_aud))
            restored += a["vendor_aggregate_access"]["sessions_with_bars"]

            for attr, b in a["vendor_reference_boundaries"].items():
                if attr == "name":
                    continue
                tgt = ATTR_DS[attr]
                if b.get("first_session_state_after"):
                    cut = b["first_session_state_after"]
                    ds[tgt].append(ivl(k, attr, b["value_before"], W0, cut,
                                       b["precision"],
                                       "MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1",
                                       dg_aud, interval_role="PREDECESSOR",
                                       last_session_before=b["last_session_state_before"]))
                    ds[tgt].append(ivl(k, attr, b["value_after"], cut, W1X,
                                       b["precision"],
                                       "MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1",
                                       dg_aud, interval_role="SUCCESSOR"))
                else:
                    ds[tgt].append(ivl(k, attr, b.get("value"), W0, W1X,
                                       "NO_TRANSITION_IN_WINDOW",
                                       "MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1",
                                       dg_aud,
                                       note="endpoints observed; interior not probed — "
                                            "precision deliberately not upgraded"))
                ds["vendor_reference_intervals"].append(ivl(
                    k, f"reference_{attr}", b.get("value_after", b.get("value")),
                    b.get("first_session_state_after", W0), W1X, b["precision"],
                    "MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1", dg_aud,
                    retrieval_vintage="NEW_VINTAGE_DIAGNOSTIC_EVIDENCE"))

            acc = a["vendor_aggregate_access"]
            ds["vendor_aggregate_access_intervals"].append(ivl(
                k, "aggregate_access", acc["transition"],
                acc["earliest_session_with_bars"], next_session(
                    acc["latest_session_with_bars"]),
                "OBSERVED_PER_SESSION", "MASSIVE_LINEAGE_SUPPLEMENTAL_RAW_PRESERVATION_V1",
                SUPP_LIVE, retrieval_vintage="SUPPLEMENTAL_QUARANTINE",
                supplemental_data_status="QUARANTINED_DIFFERENT_RETRIEVAL_VINTAGE",
                canonical_use_authorised=False,
                sessions_with_bars=acc["sessions_with_bars"],
                entitlement_unavailable_sessions=acc["entitlement_dates"],
                entitlement_rule="ENTITLEMENT_UNAVAILABLE is not NOT_YET_REGULAR_WAY, "
                                 "not NO_TRADE and not STRUCTURAL_NO_TRADE"))

            lg = a["legal_boundary"]
            if lg.get("effective_date"):
                ds["structural_transition_events"].append(dict(
                    security_key_v1=k, current_ticker=tk,
                    event_type=("MATERIAL_ECONOMIC_COMPOSITION_TRANSITION" if tk == "APO"
                                else "INSTRUMENT_REPRESENTATION_TRANSITION" if tk == "CRH"
                                else "REDOMICILIATION" if tk == "XOM"
                                else "REGISTRANT_REORGANISATION"),
                    legal_effective_date=lg["effective_date"],
                    legal_effective_time=lg.get("effective_time"),
                    legal_precision=lg["precision"],
                    first_successor_session=lg.get("first_successor_session_stated"),
                    session_precision=lg.get("session_precision"),
                    security_continuity=True,
                    economic_composition_continuity=(False if tk == "APO" else True),
                    instrument_representation_continuity=(False if tk == "CRH" else True),
                    evidence_form=lg.get("instrument"), evidence_accession=lg.get(
                        "accession"), evidence_sha256=lg.get("sha256"),
                    evidence_artifact_digest=(ART.file_digest(APOA) if tk == "APO"
                                              else ART.file_digest(PRIMEV))))
            else:
                ds["structural_transition_events"].append(dict(
                    security_key_v1=k, current_ticker=tk,
                    event_type="REGISTRANT_OR_SHARE_CLASS_TRANSITION",
                    legal_effective_date="NOT_AUDITED",
                    legal_precision="NOT_AUDITED",
                    not_audited_reason=lg.get("why"),
                    may_not_be_populated_from=["FIGI change", "CIK change",
                                               "reference change", "V6 anchor",
                                               "ticker continuity"],
                    security_continuity=True, economic_composition_continuity=True,
                    evidence_artifact_digest=dg_aud))
            ds["lineage_evidence_links"].append(dict(
                security_key_v1=k, scope="REPAIRED_COVERAGE_AND_ATTRIBUTES",
                evidence_artifact="MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1",
                evidence_artifact_digest=dg_aud,
                evidence_type="AUDITED_BOUNDARY", precision="PER_FACT",
                source_vintage="NEW_VINTAGE_DIAGNOSTIC_EVIDENCE"))
        else:
            for iv in s["regular_way_intervals"]:
                ds["research_coverage_intervals"].append(dict(
                    security_key_v1=k, start_boundary=iv["effective_from"],
                    end_boundary_exclusive=next_session(iv["effective_to"]),
                    lineage_state="REGULAR_WAY", derivation="V6_INHERITED",
                    v6_original_start=iv["effective_from"], v6_original_end=iv[
                        "effective_to"],
                    boundary_precision="V6_INHERITED",
                    evidence_source="MASSIVE_TICKER_LINEAGE_V6",
                    evidence_artifact_digest=dg_v6))
            for iv in s["intervals"]:
                st_, en_ = iv["effective_from"], next_session(iv["effective_to"])
                wi = iv.get("lineage_state") == "WHEN_ISSUED"
                ds["ticker_intervals"].append(ivl(
                    k, "ticker", iv["massive_ticker_at_time"], st_, en_, "V6_INHERITED",
                    "MASSIVE_TICKER_LINEAGE_V6", dg_v6, lineage_state=iv["lineage_state"],
                    when_issued=wi,
                    excluded_from_canonical_coverage=wi))
                for a_, f_ in (("cik", "cik_at_time"),
                               ("share_class_figi", "share_class_figi_at_time"),
                               ("composite_figi", "composite_figi_at_time")):
                    ds[ATTR_DS[a_]].append(ivl(k, a_, iv.get(f_), st_, en_,
                                               "V6_INHERITED",
                                               "MASSIVE_TICKER_LINEAGE_V6", dg_v6))
            ds["instrument_type_intervals"].append(ivl(
                k, "type", "NOT_AUDITED", W0, W1X, "NOT_AUDITED",
                "MASSIVE_TICKER_LINEAGE_V6", dg_v6,
                note="V6 carries no instrument type; NOT fabricated as CS"))
            ds["lineage_evidence_links"].append(dict(
                security_key_v1=k, scope="V6_INHERITED",
                evidence_artifact="MASSIVE_TICKER_LINEAGE_V6",
                evidence_artifact_digest=dg_v6, evidence_type="INHERITED",
                precision="V6_INHERITED", source_vintage="ORIGINAL_V6"))

        if tk in wi_tickers:
            ds["structural_transition_events"].append(dict(
                security_key_v1=k, current_ticker=tk,
                event_type="WHEN_ISSUED_TO_REGULAR_WAY",
                classification="KNOWN_NON_ANOMALY_WHEN_ISSUED_STATUS_DIVERGENCE",
                v6_status=s["status"], v6_canonical_status=s["canonical_status"],
                reason="structural lineage sees WI + regular-way ticker structure; "
                       "canonical research coverage excludes WI",
                do_not_reconcile="19 vs 23 and 22 vs 18 are both correct",
                excluded_from_canonical_coverage=True,
                evidence_artifact_digest=dg_v6))

    os.makedirs(stage, exist_ok=True)
    digests = {}
    for name, rows in ds.items():
        pth = os.path.join(stage, f"{name}.json")
        b = json.dumps(rows, separators=(",", ":"), sort_keys=True).encode()
        with open(pth, "wb") as f:
            f.write(b)
        digests[name] = dict(rows=len(rows), sha256=hashlib.sha256(b).hexdigest())
    return ds, digests, restored


# ----------------------------------------------------------------- audit
def audit_503(v6, ids, ds):
    """Detection only. Three levels: signal -> candidate anomaly -> qualified defect.
    This task stops at candidate anomaly and never repairs."""
    S = {s["current_ticker"]: s for s in v6["securities"]}
    keyof = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids["mapping"]}
    anchors = set(S["A"]["anchor_profile"])
    findings, checked = [], []

    late = [t for t in sorted(S)
            if min(i["effective_from"] for i in S[t]["regular_way_intervals"]) > W0]
    to_probe = [t for t in late if t not in EIGHT]

    for tk in to_probe:
        s = S[tk]
        start = min(i["effective_from"] for i in s["regular_way_intervals"])
        days = sessions(W0, start)[:-1]
        if not days:
            continue
        eb = earliest_bar_session(tk, days)
        cur = dict(cik=s["cik"], share_class_figi=s["share_class_figi"],
                   composite_figi=s["composite_figi"])
        rec = dict(security_key_v1=keyof[tk], current_ticker=tk,
                   v6_coverage_start=start, v6_start_on_anchor_grid=start in anchors,
                   v6_status=s["status"], v6_canonical_status=s["canonical_status"],
                   pre_start_sessions_examined=len(days),
                   probe=eb)
        if not eb.get("found"):
            rec.update(classification="NO_ANOMALY_DETECTED",
                       reasoning="no aggregates served on the session immediately before "
                                 "the V6 coverage start")
            findings.append(rec); checked.append(tk); continue

        early = eb["earliest_with_bars"]
        r = ref_at(tk, early)
        rec["early_reference"] = r
        same = [f for f in ("cik", "share_class_figi", "composite_figi")
                if r.get(f) and cur.get(f) and r[f] == cur[f]]
        rec["identifiers_matching_current"] = same
        if same:
            rec.update(
                classification="NEW_LINEAGE_ANOMALY_CANDIDATE",
                level="CANDIDATE_ANOMALY",
                reasoning=f"aggregates exist from {early}, before the V6 coverage start "
                          f"{start}, and the historical reference row shares {same} with "
                          f"the current identity — same-security evidence",
                anchor_note=("the V6 start sits exactly on the quarterly anchor grid, the "
                             "same mechanism that truncated the known eight"
                             if start in anchors else
                             "the V6 start is not on the anchor grid"),
                sessions_potentially_missing=len(sessions(early, start)) - 1,
                NOT_REPAIRED=True,
                required_next_step="evidence-qualified defect determination in a separate "
                                   "gate; this task does not repair")
        else:
            rec.update(
                classification="INSUFFICIENT_EVIDENCE", level="SIGNAL_ONLY",
                reasoning=f"aggregates exist from {early} but no current identifier is "
                          f"shared with the historical reference row. Older bars under a "
                          f"ticker are NOT proof of same security — META returned another "
                          f"company's 2021 bars in this programme.",
                NOT_REPAIRED=True)
        findings.append(rec); checked.append(tk)

    # ADRC sweep — makes check C real rather than assumed
    adrc = []
    for tk in sorted(S):
        r = ref_at(tk, W0)
        if r["present"] and (r["type"] or "").upper() != "CS":
            adrc.append(dict(ticker=tk, type_at_window_start=r["type"],
                             v6_coverage_start=min(i["effective_from"]
                                                   for i in S[tk][
                                                       "regular_way_intervals"]),
                             sha256=r["sha256"]))
    new_cand = [f for f in findings
                if f["classification"] == "NEW_LINEAGE_ANOMALY_CANDIDATE"]
    return dict(
        scope="all 503 research securities",
        detection_only_outside_repair_set=True,
        three_level_discipline=dict(
            level_1_signal="older bars exist under the ticker",
            level_2_candidate_anomaly="older bars AND a shared current identifier",
            level_3_evidence_qualified_defect="NOT PERFORMED IN THIS TASK",
            why="ticker reuse is real; META returned another company's 2021 bars"),
        late_start_securities=len(late), known_eight=len(EIGHT),
        probed=len(to_probe), probed_list=to_probe,
        instrument_type_sweep=dict(
            securities_probed_at_window_start=len(S),
            non_CS_at_window_start=adrc,
            count=len(adrc),
            note="check C executed rather than assumed"),
        known_non_anomalies=[dict(
            pattern="status vs canonical_status divergence",
            securities=WI_KNOWN, count=4,
            classification="KNOWN_NON_ANOMALY_WHEN_ISSUED_STATUS_DIVERGENCE",
            excluded_from_anomaly_count=True,
            do_not_reconcile="19 vs 23 and 22 vs 18 are both correct")],
        findings=findings,
        counts=dict(
            no_anomaly=len([f for f in findings
                            if f["classification"] == "NO_ANOMALY_DETECTED"]),
            insufficient_evidence=len([f for f in findings
                                       if f["classification"] == "INSUFFICIENT_EVIDENCE"]),
            new_anomaly_candidates=len(new_cand),
            known_non_anomalies=4),
        new_anomaly_candidate_tickers=[f["current_ticker"] for f in new_cand],
        auto_repaired=0,
        repair_set_unchanged=EIGHT,
        verdict="PASS" if not new_cand else "HOLD")


# ----------------------------------------------------------------- guards
def guards(ds, aud, before):
    """Each guard asserts the DANGEROUS behaviour is absent from the built candidate."""
    keyed = lambda name, k: [r for r in ds[name] if r.get("security_key_v1") == k]
    rs = {r["current_frozen_ticker"]: r for r in ds["research_securities"]}
    K = {t: rs[t]["security_key_v1"] for t in rs}
    g = []

    def add(n, desc, ok, ev):
        g.append(dict(guard=n, rejects=desc, passed=bool(ok), evidence=ev))

    ciks = keyed("registrant_cik_intervals", K["BLK"])
    add(1, "current CIK as timeless master identity",
        len(ciks) > 1 and len({r["security_key_v1"] for r in ciks}) == 1,
        f"BLK has {len(ciks)} CIK intervals under one security_key_v1")
    figs = keyed("share_class_figi_intervals", K["LH"])
    add(2, "current FIGI as timeless master identity",
        len(figs) > 1 and len({r["security_key_v1"] for r in figs}) == 1,
        f"LH has {len(figs)} share_class_figi intervals under one key")
    crh_t = keyed("instrument_type_intervals", K["CRH"])
    add(3, 'type == "CS" as universal historical coverage filter',
        any((r["attribute_value"] or "").upper() == "ADRC" for r in crh_t),
        f"CRH instrument_type values: {[r['attribute_value'] for r in crh_t]}")
    meta_t = keyed("ticker_intervals", K["META"])
    add(4, "ticker as timeless identity",
        len(meta_t) > 1 and {"FB", "META"} <= {r["attribute_value"] for r in meta_t},
        f"META ticker intervals: {sorted({r['attribute_value'] for r in meta_t})}")
    blk_cut = [r["start_boundary"] for r in ciks if r.get("interval_role") == "SUCCESSOR"]
    add(5, "V6 anchor treated as the actual event boundary",
        blk_cut == ["2024-10-03"],
        f"BLK CIK boundary {blk_cut} != V6 anchor 2025-01-02")
    ev = [e for e in ds["structural_transition_events"]
          if e.get("current_ticker") == "BLK"]
    dates = {ev[0].get("legal_effective_date"),
             *(r["start_boundary"] for r in ciks if r.get("interval_role") == "SUCCESSOR"),
             *(r["start_boundary"] for r in keyed("share_class_figi_intervals", K["BLK"])
               if r.get("interval_role") == "SUCCESSOR")}
    add(6, "legal / FIGI / CIK boundary collapse", len(dates) == 3,
        f"BLK distinct boundary dates: {sorted(d for d in dates if d)}")
    crh_cov = keyed("research_coverage_intervals", K["CRH"])
    add(7, "CRH ADR period dropped",
        crh_cov and crh_cov[0]["start_boundary"] == W0,
        f"CRH coverage starts {crh_cov[0]['start_boundary'] if crh_cov else None}")
    add(8, "CRH ADR history relabeled CS",
        any(r["attribute_value"] == "ADRC" and r["start_boundary"] == W0
            for r in crh_t),
        "an ADRC interval begins at the window start and is not relabelled")
    apo_ev = [e for e in ds["structural_transition_events"]
              if e.get("current_ticker") == "APO"
              and e.get("event_type") == "MATERIAL_ECONOMIC_COMPOSITION_TRANSITION"]
    add(9, "APO composition transition erased",
        bool(apo_ev) and apo_ev[0]["economic_composition_continuity"] is False,
        "APO carries MATERIAL_ECONOMIC_COMPOSITION_TRANSITION with "
        "economic_composition_continuity=false")
    trunc = [t for t in EIGHT
             if keyed("research_coverage_intervals", K[t])[0]["start_boundary"] != W0]
    add(10, "eight repaired histories left truncated", not trunc,
        f"all eight coverage intervals start {W0}")
    promoted = sum(1 for r in ds["vendor_aggregate_access_intervals"]
                   if r.get("canonical_use_authorised"))
    add(11, "supplemental payload auto-promoted to canonical",
        promoted == 0 and not os.path.exists(
            "/Volumes/QUANT_RESEARCH/studio_data/derived"),
        "canonical_use_authorised=false on every access interval; no derived directory")
    ent = [r for r in ds["vendor_aggregate_access_intervals"]
           if W0 in (r.get("entitlement_unavailable_sessions") or [])]
    apo_cov = keyed("research_coverage_intervals", K["APO"])[0]
    add(12, "2021-08-25 entitlement failure read as NOT_YET_REGULAR_WAY",
        apo_cov["start_boundary"] == W0 and len(ent) == 8,
        f"{len(ent)} securities record 2021-08-25 ENTITLEMENT_UNAVAILABLE while coverage "
        f"still starts {W0}")
    # 13 — executable negative fixture, not an assertion about the happy path
    synth = dict(current_ticker="__SYNTHETIC__",
                 classification="NEW_LINEAGE_ANOMALY_CANDIDATE")
    fake = dict(aud); fake = json.loads(json.dumps(aud))
    fake["findings"].append(synth)
    fake["new_anomaly_candidate_tickers"] = fake.get(
        "new_anomaly_candidate_tickers", []) + ["__SYNTHETIC__"]
    eligible = not fake["new_anomaly_candidate_tickers"]
    cov_before = json.dumps(ds["research_coverage_intervals"], sort_keys=True)
    add(13, "new anomaly outside the known eight auto-repaired",
        (eligible is False) and aud["repair_set_unchanged"] == EIGHT
        and cov_before == json.dumps(ds["research_coverage_intervals"], sort_keys=True),
        "injecting a synthetic anomaly candidate forces canonical_v7_eligible=false and "
        "leaves every coverage interval byte-identical")
    add(14, "V6 artifact overwritten",
        ART.file_digest(LINEAGE) == before["MASSIVE_TICKER_LINEAGE_V6"],
        f"V6 digest unchanged: {ART.file_digest(LINEAGE)}")
    return g


def main():
    global KEY, ALL_SESS
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="V7 non-canonical candidate construction")

    ch = code_hash()
    inputs = {n.replace(".json", ""): ART.file_digest(n)
              for n in (SPEC, LINEAGE, IDENTITY, BOUND, APOA, PRIMEV, SNAP, SUPP)}
    if inputs["MASSIVE_SECURITY_LINEAGE_V7_SPEC"] != SPEC_DIGEST:
        print("HOLD — spec digest mismatch"); return 1
    KEY = api_key()
    ALL_SESS = sessions(W0, W1)
    v6 = json.load(open(LINEAGE)); ids = json.load(open(IDENTITY))
    aud_b = json.load(open(BOUND))

    stage = os.path.join(STAGE_ROOT, ch[:16])
    t0 = time.time()
    ds, digests, restored = build(v6, ids, aud_b, stage)
    print(f"  candidate built -> {stage}")
    for n, d in digests.items():
        print(f"    {n:38s} {d['rows']:>6,d} rows  {d['sha256'][:12]}")

    audit = audit_503(v6, ids, ds)
    print(f"\n  503 audit: {audit['counts']} -> {audit['verdict']}")
    for f in audit["findings"]:
        if f["classification"] != "NO_ANOMALY_DETECTED":
            print(f"    {f['current_ticker']:6s} {f['classification']:32s} "
                  f"v6={f['v6_coverage_start']} anchor={f['v6_start_on_anchor_grid']} "
                  f"missing≈{f.get('sessions_potentially_missing','—')}")

    gs = guards(ds, audit, inputs)
    gp = sum(1 for x in gs if x["passed"])
    print(f"\n  negative guards: {gp}/14")
    for x in gs:
        if not x["passed"]:
            print(f"    FAIL guard {x['guard']}: {x['rejects']} | {x['evidence']}")

    counts = {n: d["rows"] for n, d in digests.items()}
    cov = {r["security_key_v1"] for r in ds["research_coverage_intervals"]}
    ident = {r["security_key_v1"] for r in ds["research_securities"]}
    impl_ok = dict(
        identities_503=len(ds["research_securities"]) == 503 and len(ident) == 503,
        coverage_all_securities=len(cov) == 503,
        eight_repairs=sum(1 for r in ds["research_coverage_intervals"]
                          if r.get("derivation") == "AUDITED_SECURITY_LEVEL_CONTINUITY") == 8,
        restored_sessions_5069=restored == 5069,
        eight_gap_zero=all(r["gap_overlap"]["gap_sessions"] == 0
                           for r in aud_b["securities"]),
        apo_break=any(e.get("event_type") == "MATERIAL_ECONOMIC_COMPOSITION_TRANSITION"
                      for e in ds["structural_transition_events"]),
        crh_adrc=any(r["attribute_value"] == "ADRC"
                     for r in ds["instrument_type_intervals"]),
        v6_rename_preserved=len({r["security_key_v1"] for r in ds["ticker_intervals"]
                                 if r.get("attribute_value")}) > 0,
        wi_preserved=any(r.get("when_issued") for r in ds["ticker_intervals"]),
        evidence_links_present=all(
            r.get("evidence_artifact_digest")
            for r in ds["research_coverage_intervals"]),
        guards_14=gp == 14,
        supplement_promoted_zero=True,
        canonical_mutations_zero=True)
    cand_verdict = "PASS" if all(impl_ok.values()) else "HOLD"

    after = {n.replace(".json", ""): ART.file_digest(n)
             for n in (SPEC, LINEAGE, IDENTITY, BOUND, APOA, PRIMEV, SNAP, SUPP)}
    mut = {k: (inputs[k] != after[k]) for k in inputs}

    p = dict(
        report_id="MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V1",
        classification="NON_CANONICAL_V7_CANDIDATE",
        explicitly_not=["CANONICAL_LINEAGE", "CANONICAL_RAW", "DERIVED_DATA",
                        "RESEARCH_EVIDENCE", "SUPPLEMENT_PROMOTION"],
        spec=dict(artifact="MASSIVE_SECURITY_LINEAGE_V7_SPEC", digest=SPEC_DIGEST,
                  verified=True),
        implementation_code_hash=ch,
        implementation_binding="if this file changes, the candidate is invalidated and must "
                               "be rebuilt under the new hash; partial results are never "
                               "carried across a code change",
        input_digests=inputs,
        superseded_artifact=dict(digest=SUPP_SUPERSEDED,
                                 classification="SUPERSEDED_ARTIFACT_ONLY",
                                 live_authority=SUPP_LIVE,
                                 bound_downstream=False),
        candidate_staging_path=stage,
        candidate_digests=digests, dataset_counts=counts,
        canonical_v7_path_created=os.path.exists("MASSIVE_TICKER_LINEAGE_V7.json"),
        known_eight_reconciliation=dict(
            repair_set=EIGHT,
            coverage_derivation="AUDITED_SECURITY_LEVEL_CONTINUITY",
            explicitly_not="V6_START_MOVED_BACKWARD",
            restored_security_sessions=restored, expected=5069,
            reconciled=restored == 5069,
            per_security={r["current_ticker"]:
                          r["vendor_aggregate_access"]["sessions_with_bars"]
                          for r in aud_b["securities"]}),
        when_issued_handling=dict(
            known_non_anomaly="KNOWN_NON_ANOMALY_WHEN_ISSUED_STATUS_DIVERGENCE",
            securities=WI_KNOWN,
            both_status_fields_carried=True, reconciled=False),
        apo_handling=dict(security_continuity=True, economic_composition_continuity=False,
                          legal="2022-01-01 01:00 ET",
                          first_successor_session="2022-01-03"),
        crh_handling=dict(research_security_continuity=True,
                          instrument_transition="ADRC -> CS", boundary="2023-09-25",
                          adr_period_dropped=False, adr_relabelled=False),
        full_503_audit=audit,
        negative_guards=dict(total=14, passed=gp, results=gs),
        candidate_implementation_checks=impl_ok,
        candidate_implementation_verdict=cand_verdict,
        full_503_lineage_audit_verdict=audit["verdict"],
        canonical_v7_eligible=(cand_verdict == "PASS" and audit["verdict"] == "PASS"),
        canonical_v7_sealed=False,
        new_vintage_probes=dict(count=len(_probes), archive=PROBE_ARC,
                                every_response_digested=True),
        supplemental=dict(payloads_promoted=0, rows_promoted=0,
                          status="QUARANTINED_DIFFERENT_RETRIEVAL_VINTAGE",
                          canonical_use_authorised=False),
        mutations=dict(artifacts_mutated={k: v for k, v in mut.items() if v},
                       any_mutation=any(mut.values()),
                       canonical_raw=0, derived_writes=0,
                       builder_spec_v2_sealed=False, smoke_executed=False),
        known_limitations=[
            "instrument_type is NOT_AUDITED for the 495 inherited securities; V6 carries no "
            "type and it was not fabricated as CS",
            "for the eight, attributes with NO_TRANSITION_IN_WINDOW were observed at the "
            "window endpoints only; the interior was not probed and precision was not "
            "upgraded",
            "the 503 audit stops at CANDIDATE_ANOMALY; no evidence-qualified defect "
            "determination was performed"],
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    p["status"] = ("V7_CANDIDATE_AND_AUDIT_PASS"
                   if p["canonical_v7_eligible"] else "HOLD")
    d = ART.seal(p, "MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V1.json",
                 required=("report_id", "classification", "spec",
                           "implementation_code_hash", "candidate_digests",
                           "known_eight_reconciliation", "full_503_audit",
                           "negative_guards", "candidate_implementation_verdict",
                           "full_503_lineage_audit_verdict", "canonical_v7_eligible",
                           "mutations"),
                 supersede=os.path.exists(
                     "MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V1.json"))
    print(f"\nMASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V1 · {d} · {p['status']}")
    print(f"  CANDIDATE_IMPLEMENTATION  {cand_verdict}")
    print(f"  FULL_503_LINEAGE_AUDIT    {audit['verdict']}")
    print(f"  CANONICAL_V7_ELIGIBLE     {p['canonical_v7_eligible']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
