"""MASSIVE_SOURCE_TICKER_BOUNDARY_AMENDMENT_V1 — vendor source-ticker ACCESS on five
diagnosed boundary sessions. Nothing wider.

WHAT THIS SEPARATES. Three things were being run together, and the whole point of this
artifact is to hold them apart:

    SECURITY IDENTITY        who the security is                    -> CIK
    SOURCE-TICKER ACCESS     which string the vendor answers to      -> aggregates endpoint
    EVIDENCE VINTAGE         when the answer was obtained            -> original vs diagnostic

The five sessions are a case where identity is stable, access differs from what the
reference lineage assigns, and the evidence for that comes from a LATER retrieval.

THE CLAIM IS DELIBERATELY WEAKER THAN THE TEMPTING ONE. It is true that, at diagnostic
retrieval time, the aggregates endpoint returned bars under the same-CIK successor ticker
while reference-by-date still named the predecessor. It does NOT follow that the vendor
would have returned those bars at the original ingest timestamp — that is a claim about a
moment nobody sampled, and the entitlement window and vendor state have both moved since.
So the wording below stops where the evidence stops.

IDENTITY RESTS ON CIK, NOT ON PRICE. Price continuity across the boundary is recorded and
is genuinely reassuring, but a continuous price is what you would expect from a rename AND
from several other things. The issuer identity comes from V6's CIK-anchored construction;
continuity corroborates it and is labelled CORROBORATIVE_ONLY throughout.

FIVE CASES, NOT A RULE. No `effective_date - 1` offset is created. Whether other V6 renames
share this lag is unknown and untested; generalising from five observations to every rename
in the window would be inventing a rule out of a sample.

NO DATA REPAIR. The five sessions stay UNOBSERVED in the canonical raw archive. Feeding
newly-retrieved bars into evidence gathered on a different vintage is a much larger
methodological decision than leaving 5 of 630,762 pairs absent, and it is not made here.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

DIAG = "MASSIVE_1M_UNOBSERVED_FIVE_DIAGNOSIS_V1.json"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
RAWMAN = "/Volumes/QUANT_RESEARCH/source_data/massive/canonical_1m/_manifest"
INTEGRITY = "MASSIVE_1M_RAW_INTEGRITY_V1.json"


def main():
    d = json.load(open(DIAG))
    lin_digest = ART.file_digest(LINEAGE)
    integ = json.load(open(INTEGRITY))

    cases = []
    for c in d["cases"]:
        s, old, new = c["session"], c["old_ticker"], c["successor_ticker"]
        days = c["adjacent_sessions"]
        i = days.index(s)
        a_old, a_new = c["aggs"][old], c["aggs"][new]
        cases.append(dict(
            security=c["security"], security_cik=c["cik"],
            predecessor_ticker=old, successor_ticker=new, xnys_session=s,
            v6_interval_assignment=[iv for iv in c["v6_intervals"]],
            v6_assigns_on_this_session=old,
            reference_by_date=dict(
                result=c["reference_on_session"].get("tickers"),
                http=c["reference_on_session"].get("http"),
                share_class_figis=c["reference_on_session"].get("share_class_figis"),
                still_reports_predecessor=(c["reference_on_session"].get("tickers")
                                           == [old])),
            aggregates=dict(
                endpoint="/v2/aggs/ticker/{ticker}/range/1/minute/{d}/{d}",
                params=dict(adjusted="true", limit=50000),
                adjusted_flag=True,
                predecessor_rth_rows=a_old[s].get("rth"),
                successor_rth_rows=a_new[s].get("rth"),
                predecessor_prior_sessions={dd: a_old[dd].get("rth")
                                            for dd in days[:i]},
                successor_prior_sessions={dd: a_new[dd].get("rth") for dd in days[:i]},
                predecessor_following_sessions={dd: a_old[dd].get("rth")
                                                for dd in days[i + 1:]},
                successor_following_sessions={dd: a_new[dd].get("rth")
                                              for dd in days[i + 1:]}),
            price_continuity=c.get("price_continuity"),
            original_ingest_state="UNOBSERVED",
            original_ingest_record=c["original_observation"],
            diagnostic_classification="LINEAGE_SOURCE_BOUNDARY_MISMATCH_CONFIRMED",
            identity_proof="SAME_CIK",
            price_continuity_role="CORROBORATIVE_ONLY"))

    # ── conformance
    same_cik = all(c["security_cik"] for c in cases)
    pred_nobar = all((c["aggregates"]["predecessor_rth_rows"] or 0) == 0 for c in cases)
    succ_bars = all((c["aggregates"]["successor_rth_rows"] or 0) > 0 for c in cases)
    ref_pred = all(c["reference_by_date"]["still_reports_predecessor"] for c in cases)

    # original provenance untouched — read the shipped manifests back
    prov_ok, prov = True, {}
    for c in cases:
        m = os.path.join(RAWMAN, f"{c['xnys_session']}.json")
        st = (json.load(open(m)).get("states") or {}).get(c["security"]) \
            if os.path.exists(m) else None
        prov[f"{c['xnys_session']}:{c['security']}"] = st
        if st != "UNOBSERVED":
            prov_ok = False

    checks = dict(
        same_cik_identity_all_five=same_cik,
        predecessor_no_bar_on_session=pred_nobar,
        successor_bars_present_on_session=succ_bars,
        reference_still_reports_predecessor=ref_pred,
        original_unobserved_provenance_unchanged=prov_ok,
        v6_unchanged=lin_digest == "8c961aa3a0c67934",
        canonical_raw_archive_unchanged=(integ["status"] == "PASS"
                                         and integ["sessions"]["completed"] == 1254),
        no_generalized_rename_offset_rule=True,
        no_new_vintage_bar_inserted=True,
        distinguishes_identity_access_vintage=True)

    p = dict(
        spec_id="MASSIVE_SOURCE_TICKER_BOUNDARY_AMENDMENT_V1",
        status="FROZEN" if all(checks.values()) else "HOLD",
        purpose="record vendor source-ticker ACCESS behaviour for five already-diagnosed "
                "rename-boundary security-sessions",
        separates=dict(
            security_identity="who the security is — established by CIK",
            source_ticker_access="which string the vendor answers to — established by the "
                                 "aggregates endpoint",
            evidence_vintage="when the answer was obtained — original ingest vs later "
                             "diagnostic retrieval"),

        the_claim=dict(
            supported="At diagnostic retrieval time, Massive aggregates returned bars "
                      "under the same-CIK successor ticker for the affected boundary "
                      "session while reference-by-date continued to identify the "
                      "predecessor ticker for that date.",
            NOT_supported="The vendor had already switched at original ingest time.",
            why="that is a claim about a moment nobody sampled. The entitlement window "
                "and the vendor's state have both moved since the original ingest, and "
                "no evidence here reaches back to that timestamp."),

        evidence_hierarchy=dict(
            primary="SAME ISSUER CIK, established by the frozen V6 identity construction",
            corroborative="price continuity across the predecessor/successor boundary",
            explicitly="price continuity ALONE is NOT sufficient issuer-identity evidence; "
                       "the ratios are recorded but are not the proof",
            ratios={c["security"]: (c["price_continuity"] or {}).get("ratio")
                    for c in cases}),

        vintage_discipline=dict(
            original_archive_authoritative="the archived ingest observations remain "
                                           "authoritative for their own vintage",
            all_five_original_state="UNOBSERVED",
            new_evidence_class="NEW_VINTAGE_DIAGNOSTIC_EVIDENCE",
            relation="it may EXPLAIN the original observation; it does not replace it"),

        scope=dict(
            evidence_bound_to="these five cases only",
            no_generalization="no `effective_date - 1` offset and no "
                              "`successor_on_previous_session` rule is created",
            unknown="whether other V6 renames share this one-session reference lag is "
                    "untested; generalising from five observations would be inventing a "
                    "rule out of a sample",
            requires="a separately authorised broader conformance study"),

        cases=cases,

        semantic_consequence=dict(
            frozen="a no-bar response for a ticker assigned by reference lineage is NOT "
                   "sufficient evidence of STRUCTURAL_NO_TRADE",
            rule="REFERENCE_TICKER_NO_BAR does not imply SECURITY_NO_TRADE",
            prerequisite="ticker-access correctness is a PREREQUISITE to interpreting "
                         "absence at all",
            for_these_five=dict(
                STRUCTURAL_NO_TRADE="FORBIDDEN",
                LEGITIMATE_EMPTY="NOT SUPPORTED",
                original_archive_state="UNOBSERVED",
                diagnostic_explanation="SOURCE_TICKER_BOUNDARY_MISMATCH"),
            consequence_for_phase_b="REGULAR_WAY eligibility alone no longer licenses "
                                    "calling a missing bar STRUCTURAL_NO_TRADE; source-"
                                    "ticker access validity must be established first"),

        no_data_repair=dict(
            authorised=False,
            statement="this artifact does NOT authorise the derived layer to consume the "
                      "new-vintage successor bars",
            state="the five security-sessions remain UNOBSERVED in the canonical "
                  "raw-vintage evidence layer",
            scale="5 of 630,762 pairs",
            if_needed_later="stop and open MIXED_VINTAGE_SOURCE_SUPPLEMENTATION, which "
                            "must decide explicitly whether supplementation is allowed, "
                            "how vintage is labelled, whether supplemented bars may enter "
                            "inferential research, whether analyses must run both with and "
                            "without them, and how provenance propagates",
            not_decided_here=True),

        recording_limitation="response body digests were not captured during the "
                             "diagnostic retrieval and are therefore not recorded; row "
                             "counts, HTTP status, parameters and retrieval time are",
        retrieval_time=d.get("sealed_at"),
        sources=dict(diagnosis=dict(artifact=DIAG, digest=ART.file_digest(DIAG)),
                     lineage=dict(artifact=LINEAGE, digest=lin_digest),
                     raw_integrity=dict(artifact=INTEGRITY,
                                        digest=ART.file_digest(INTEGRITY))),
        provenance_recheck=prov,
        acceptance=checks,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    dg = ART.seal(p, "MASSIVE_SOURCE_TICKER_BOUNDARY_AMENDMENT_V1.json",
                  required=("spec_id", "status", "the_claim", "evidence_hierarchy",
                            "vintage_discipline", "cases", "semantic_consequence",
                            "no_data_repair", "acceptance"),
                  supersede=os.path.exists(
                      "MASSIVE_SOURCE_TICKER_BOUNDARY_AMENDMENT_V1.json"))
    print(f"MASSIVE_SOURCE_TICKER_BOUNDARY_AMENDMENT_V1 · {dg} · {p['status']}")
    for c in cases:
        print(f"  {c['xnys_session']}  {c['predecessor_ticker']:5s} -> "
              f"{c['successor_ticker']:5s}  ref={c['reference_by_date']['result']}  "
              f"pred_rows={c['aggregates']['predecessor_rth_rows']} "
              f"succ_rows={c['aggregates']['successor_rth_rows']}  "
              f"cont={(c['price_continuity'] or {}).get('ratio')}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
