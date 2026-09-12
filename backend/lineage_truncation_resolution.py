"""MASSIVE_LINEAGE_TRUNCATION_RESOLUTION_V1 — two branches, and only one of them closes.

BRANCH B (CRH) IS RESOLVED, AND THE MECHANISM IS A THIRD ONE. Neither CIK nor FIGI changes
across CRH's truncation boundary. Filter ablation on the reference endpoint shows the
security TYPE changes:

    2021-09-15   type = ADRC   depositary receipt   share_class_figi BBG001S5VPG8
    2023-09-29   type = CS     direct listing       share_class_figi None

V6 filters `type == "CS"`, so the entire ADR period was discarded. That predicate is right
for excluding preferred lines and ETFs and wrong here, because the same economic listing was
represented as an ADR before CRH moved its primary listing to the NYSE.

But "V6's filter caused it" does not settle whether the ADR period BELONGS to this security.
An ADR is a receipt over foreign shares; the CS row is the share itself. Whether those are
one research security is a semantic decision needing the same authoritative treatment as the
others, so CRH is root-caused and still not resolved for coverage.

BRANCH A IS NOT CLOSED, AND I AM NOT GOING TO OVERSTATE WHAT I HAVE. SEC registrant
metadata was retrieved for all three. It is genuinely informative:

    APO   CIK 0001858681   formerly "Tango Holdings, Inc."      S-4 filed 2021-05-07
    BLK   CIK 0002012383   formerly "BlackRock Funding, Inc."   earliest filings 2025-08
    XOM   CIK 0002115436   "ExxonMobil Holdings Corp"           earliest S-8 POS 2026-07-01

An S-4 registers securities issued in a business combination, and S-8 POS amendments are how
a successor adopts a predecessor's benefit-plan registrations. Both patterns are what
reorganisations look like. But the spec requires an authoritative filed statement of the
predecessor/successor relationship and of what happened to existing shares — and I read
filing METADATA, not filing BODIES. The EDGAR index pages render client-side, so no document
text was retrieved and no filing bytes were preserved.

Inferring succession from the presence of a form type is exactly the corroborative-standing-
in-for-primary move the evidence hierarchy forbids. So APO, BLK and XOM stay UNRESOLVED,
with the retrieval path recorded so the next gate starts from the document text rather than
repeating the metadata pass.
"""
from __future__ import annotations
import json, os, sys, time                                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

DIAG = "MASSIVE_REGISTRANT_CONTINUITY_DIAGNOSIS_V1.json"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"


def main():
    branch_b = dict(
        security="CRH", security_key_v1=None,
        v6_regular_way_start="2023-10-02",
        identifiers=dict(cik_changed=False, share_class_figi_changed=False,
                         composite_figi_changed=False,
                         note="none of V6's identity anchors move across the boundary"),
        filter_ablation=dict(
            method="reproduce the V6 reference query read-only and remove predicates one "
                   "at a time",
            results={
                "cik + date + active + type==CS + scfigi": dict(
                    at_2021_09_15="rows=1, CS=0 -> excluded",
                    at_2023_09_29="rows=1, CS=1 -> admitted"),
                "without active filter": "identical — active=True on both dates",
                "by ticker instead of cik": "identical — the CIK is not the discriminator"},
            minimal_exclusion_predicate='type == "CS"',
            evidence=dict(
                at_2021_09_15=dict(ticker="CRH", type="ADRC", active=True,
                                   cik="0000849395",
                                   share_class_figi="BBG001S5VPG8",
                                   name="CRH Public Limited Company"),
                at_2023_09_29=dict(ticker="CRH", type="CS", active=True,
                                   cik="0000849395", share_class_figi=None,
                                   name="CRH Public Limited Company"))),
        root_cause_classification="V6_FILTER_DEFECT_CONFIRMED",
        affected_interval="everything before 2023-10-02 under type ADRC",
        defect_not_fixed_here="the exact predicate and its affected interval are recorded; "
                              "no code change is made in this gate",
        coverage_treatment="UNRESOLVED — a filter defect explains the exclusion but does "
                           "not establish that the ADR period belongs to this research "
                           "security. An ADR is a receipt over foreign shares; the CS row "
                           "is the share. Same instrument-continuity question as Branch A.",
        broader_risk="any other security that transitioned ADR -> direct listing inside "
                     "the window would be truncated the same way. NOT investigated here — "
                     "the gate forbids broadening to all 503.")

    branch_a = {
        "APO": dict(
            current_cik="0001858681", registrant_name="Apollo Global Management, Inc.",
            sec_former_names=["Tango Holdings, Inc."],
            filings_seen=[dict(form="S-4", filed="2021-05-07",
                               accession="0001193125-21-153747",
                               significance="registers securities issued in a business "
                                            "combination")],
            evidence_level="REGISTRANT METADATA + FILING INDEX ONLY",
            document_body_read=False, filing_bytes_preserved=False,
            classification="UNRESOLVED",
            why="the S-4's existence and the former name 'Tango Holdings' are consistent "
                "with a merger vehicle becoming the successor parent, but no filed "
                "statement of share treatment was read"),
        "BLK": dict(
            current_cik="0002012383", registrant_name="BlackRock, Inc.",
            sec_former_names=["BlackRock, Inc.", "BlackRock Funding, Inc. /DE"],
            filings_seen=[dict(form="13F-HR", filed="2025-08-12",
                               significance="adviser filing, not the reorganisation")],
            evidence_level="REGISTRANT METADATA ONLY",
            document_body_read=False, filing_bytes_preserved=False,
            classification="UNRESOLVED",
            why="'BlackRock Funding, Inc.' as a former name of the current registrant is "
                "suggestive of a holding-company reorganisation, but registrant naming is "
                "not a statement about what happened to existing shares"),
        "XOM": dict(
            current_cik="0002115436", registrant_name="ExxonMobil Holdings Corp",
            sec_former_names=[],
            filings_seen=[dict(form="S-8 POS", filed="2026-07-01",
                               accession="0001193125-26-292536",
                               significance="post-effective amendments are how a successor "
                                            "adopts a predecessor's benefit-plan "
                                            "registrations")],
            evidence_level="FILING TYPE ONLY",
            document_body_read=False, filing_bytes_preserved=False,
            classification="UNRESOLVED",
            why="the S-8 POS pattern is what succession looks like, but inferring "
                "succession from a form type is corroborative evidence standing in for "
                "primary evidence, which the hierarchy forbids"),
    }

    p = dict(
        report_id="MASSIVE_LINEAGE_TRUNCATION_RESOLUTION_V1",
        status="HOLD",
        references=dict(diagnosis=dict(artifact=DIAG, digest=ART.file_digest(DIAG)),
                        lineage=dict(artifact=LINEAGE, digest=ART.file_digest(LINEAGE))),
        preserved_classifications=dict(
            confirmed_previously=["J", "BG", "LH", "FERG"],
            classification="REGISTRANT_CHANGE_SECURITY_CONTINUITY_CONFIRMED",
            not_reopened=True),
        branch_b_crh=branch_b,
        branch_a=branch_a,
        third_mechanism_found=dict(
            summary="three distinct truncation mechanisms now known across eight cases",
            mechanisms={
                "CIK survives, FIGI changes": ["J", "LH"],
                "FIGI survives, CIK changes": ["BG", "FERG"],
                "neither survives": ["APO", "BLK", "XOM"],
                "both survive, TYPE changes (ADRC -> CS)": ["CRH"]},
            implication="no single identifier-based lineage rule covers these cases; a V7 "
                        "model would need security continuity to sit ABOVE registrant, "
                        "share-class and ticker intervals rather than resting on any one "
                        "of them"),
        evidence_gap=dict(
            what_is_missing="filed document TEXT establishing predecessor/successor and "
                            "share treatment for APO, BLK, XOM",
            why_not_obtained="EDGAR index pages render client-side; only submission "
                             "metadata was retrieved",
            not_done="no filing bytes preserved, no SHA256 of any filing document",
            next_step="retrieve the primary documents directly by accession path and "
                      "preserve their bytes and digests before classifying"),
        v7_precondition=dict(
            rule="all eight known truncation candidates need an explicit treatment before "
                 "V7",
            resolved=4, unresolved=4,
            unresolved_list=["APO", "BLK", "XOM", "CRH"],
            met=False),
        no_changes=dict(v6_modified=False, security_key_v1_modified=False,
                        universe_modified=False, raw_archive_modified=False,
                        v7_created=False, builder_spec_v2_sealed=False,
                        smoke_executed=False, derived_data_written=False,
                        v6_filter_defect_fixed=False),
        not_generalized="the other 495 securities were not scanned for the ADRC pattern or "
                        "any other mechanism; a full-universe lineage audit would be a "
                        "separately frozen gate",
        final="HOLD — four cases remain unresolved in ways that materially affect expected "
              "coverage; the derived builder stays blocked",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_LINEAGE_TRUNCATION_RESOLUTION_V1.json",
                 required=("report_id", "status", "branch_b_crh", "branch_a",
                           "evidence_gap", "v7_precondition", "no_changes"),
                 supersede=os.path.exists("MASSIVE_LINEAGE_TRUNCATION_RESOLUTION_V1.json"))
    print(f"MASSIVE_LINEAGE_TRUNCATION_RESOLUTION_V1 · {d} · {p['status']}")
    print(f"  BRANCH B  CRH -> {branch_b['root_cause_classification']} "
          f"(predicate {branch_b['filter_ablation']['minimal_exclusion_predicate']})")
    print(f"            coverage treatment: UNRESOLVED")
    for tk, v in branch_a.items():
        print(f"  BRANCH A  {tk:4s} -> {v['classification']}  "
              f"({v['evidence_level']}, body_read={v['document_body_read']})")
    print(f"  v7 precondition met: {p['v7_precondition']['met']} "
          f"({p['v7_precondition']['resolved']}/8 resolved)")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
