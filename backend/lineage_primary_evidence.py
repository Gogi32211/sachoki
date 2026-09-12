"""MASSIVE_LINEAGE_PRIMARY_EVIDENCE_RESOLUTION_V1 — filed document text, three of four.

Every classification below rests on retrieved filing BYTES, archived with a SHA-256, and on
quoted language from the document itself. Filing metadata, form types and EDGAR's
formerNames field were used only to FIND the documents — the earlier gate showed how easily
those slide into standing in for evidence they cannot provide.

    XOM   CONFIRMED   Rule 414(d) / Rule 12g-3(a) successor registrant; shareholders of the
                      Company became shareholders of the Registrant
    BLK   CONFIRMED   Rule 414 successor/predecessor named explicitly; each predecessor
                      share "converted automatically into one" successor share
    CRH   CONFIRMED   ADS each represented ONE ordinary share; ADSs cancelled and delisted
                      2023-09-25 and the ordinary shares listed on NYSE under the same
                      ticker — a deterministic 1:1 instrument bridge
    APO   UNRESOLVED  the documents retrieved did not contain share-treatment language

CRH KEEPS ITS INSTRUMENT TRANSITION VISIBLE. ADRC and CS are not the same instrument type,
and the classification says so: CONTINUITY_CONFIRMED_WITH_INSTRUMENT_TRANSITION. The bridge
is deterministic because the ratio is 1:1, but pretending the type never changed would erase
a real corporate event from the lineage.

AND CRH EXPOSES A SECOND V6 ERROR. V6 dated the boundary 2023-10-02; the filing puts the
cancellation at 2023-09-25. V6's date is its quarterly anchor, not the event — so the
transition dates it produces are snapped to whichever anchor first observed the change, not
to the change itself. That affects any V7 that reuses those dates.

APO STAYS OPEN RATHER THAN BEING ROUNDED UP. Three of four is not four of four, and the
V7 precondition requires all eight known truncation cases to have an explicit treatment.
"""
from __future__ import annotations
import json, os, sys, time                                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

ARC = "/Volumes/QUANT_RESEARCH/artifacts/provenance/lineage_primary_evidence"
PRIOR = "MASSIVE_LINEAGE_TRUNCATION_RESOLUTION_V1.json"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"


def main():
    keys = {r["frozen_current_ticker"]: r["security_key_v1"]
            for r in json.load(open(IDENTITY))["mapping"]}

    cases = {
        "XOM": dict(
            security_key_v1=keys["XOM"], v6_regular_way_start="2026-08-24",
            predecessor_entity="Exxon Mobil Corporation, a New Jersey corporation",
            successor_entity="ExxonMobil Holdings Corporation",
            predecessor_cik="0000034088", successor_cik="0002115436",
            instrument_type_before="CS", instrument_type_after="CS",
            filings=[dict(form="S-8 POS", filed="2026-07-01",
                          accession="0001193125-26-292536",
                          document="d159056ds8pos.htm", http=200,
                          sha256="a10664dec585d21f", archived_in=ARC)],
            quoted_language=[
                "...is being filed pursuant to Rule 414(d) under the Securities Act of "
                "1933...",
                "...of the Company becoming shareholders of the Registrant. The Registrant "
                "is deemed to be the successor registrant of the Company's common stock "
                "pursuant to Rule 12g-3(a) under the Securities Exchange Act of 1934..."],
            share_treatment="shareholders of the predecessor became shareholders of the "
                            "successor registrant",
            classification="SECURITY_CONTINUITY_CONFIRMED",
            temporal_precision=dict(
                legal_effective="2026 reorganisation approved at the Company's 2026 annual "
                                "meeting; S-8 POS filed 2026-07-01",
                last_predecessor_xnys_session="NOT ESTABLISHED from the filing",
                first_successor_xnys_session="NOT ESTABLISHED from the filing",
                bounded="the filing establishes succession, not an exchange-session "
                        "boundary; V6's 2026-08-24 is an anchor artefact, not evidence")),
        "BLK": dict(
            security_key_v1=keys["BLK"], v6_regular_way_start="2025-01-02",
            predecessor_entity="BlackRock Finance, Inc. (formerly named BlackRock, Inc.)",
            successor_entity="BlackRock, Inc. (formerly named BlackRock Funding, Inc.)",
            predecessor_cik="0001364742", successor_cik="0002012383",
            instrument_type_before="CS", instrument_type_after="CS",
            filings=[dict(form="S-8 POS", filed="2024-10-01",
                          accession="0001193125-24-230229",
                          document="d871701ds8pos.htm", http=200,
                          sha256="9202a8344b8dc0f7", archived_in=ARC),
                     dict(form="8-K", filed="2024-10-01",
                          accession="0001193125-24-229654", document="d856279d8k.htm",
                          http=200, sha256="ec81adf30f81a1f0", archived_in=ARC)],
            quoted_language=[
                "This Post-Effective Amendment ... is being filed pursuant to Rule 414 ... "
                "by BlackRock, Inc. (formerly named BlackRock Funding, Inc.) ... as the "
                "successor registrant to BlackRock Finance, Inc. (formerly named "
                "BlackRock, Inc.) ... to reflect a holding company reorganization",
                "each share of common stock of the Predecessor Registrant outstanding "
                "immediately prior to the effective time of the Merger ... was converted "
                "automatically into one validly is[sued] ..."],
            share_treatment="each predecessor share converted automatically into one "
                            "successor share (one-for-one)",
            classification="SECURITY_CONTINUITY_CONFIRMED",
            temporal_precision=dict(
                legal_effective="Merger Effective Time, 2024-10-01 filings",
                last_predecessor_xnys_session="NOT ESTABLISHED from the filing",
                first_successor_xnys_session="NOT ESTABLISHED from the filing",
                bounded="V6's 2025-01-02 is a quarterly anchor, roughly three months after "
                        "the filed reorganisation — anchor artefact, not the event")),
        "CRH": dict(
            security_key_v1=keys["CRH"], v6_regular_way_start="2023-10-02",
            predecessor_entity="CRH public limited company (ADS on NYSE)",
            successor_entity="CRH public limited company (ordinary shares on NYSE)",
            predecessor_cik="0000849395", successor_cik="0000849395",
            cik_unchanged=True,
            instrument_type_before="ADRC", instrument_type_after="CS",
            v6_exclusion_predicate='type == "CS"',
            filings=[dict(form="424B7", filed="2023-09-20",
                          accession="0001193125-23-238082",
                          document="d549047d424b7.htm", http=200,
                          sha256="68206e0ac8755f0d", archived_in=ARC)],
            quoted_language=[
                "Our American Depositary Shares, each representing one Ordinary Share (the "
                "\"ADS\") are listed on the New York Stock Exchange (\"NYSE\") under the "
                "symbol \"CRH\".",
                "On September 25, 2023, the ADSs will be cancelled and delisted from the "
                "NYSE and our Ordinary Shares will become listed on the NYSE under the "
                "symbol \"CRH\"."],
            adr_ratio="1 ADS : 1 Ordinary Share",
            share_treatment="ADSs cancelled and delisted; ordinary shares listed on NYSE "
                            "under the same ticker, at a 1:1 ratio",
            classification="CONTINUITY_CONFIRMED_WITH_INSTRUMENT_TRANSITION",
            instrument_transition_preserved="ADRC -> CS is recorded, not erased; the "
                                            "instrument type genuinely changed even though "
                                            "the economic bridge is deterministic",
            temporal_precision=dict(
                filed_transition_date="2023-09-25",
                v6_regular_way_start="2023-10-02",
                discrepancy="V6 is 5 sessions late — it snapped to its quarterly anchor "
                            "rather than the filed event date")),
        "APO": dict(
            security_key_v1=keys["APO"], v6_regular_way_start="2022-01-03",
            predecessor_entity="NOT ESTABLISHED",
            successor_entity="Apollo Global Management, Inc. (formerly Tango Holdings, "
                             "Inc.)",
            successor_cik="0001858681",
            filings=[dict(form="8-K", filed="2022-01-07",
                          accession="0000950142-22-000221",
                          document="eh220215516_8k-apo.htm", http=200,
                          sha256="bf24455fdc5cb4c3", archived_in=ARC,
                          result="no share-treatment or succession language found"),
                     dict(form="S-8 POS", filed="2022-03-16",
                          accession="0000950142-22-001046",
                          document="eh220235203_s8pos1.htm", http=200,
                          sha256="1064d3a21f4a87e8", archived_in=ARC,
                          result="no share-treatment or succession language found")],
            leads_not_evidence=dict(
                former_name="Tango Holdings, Inc.",
                s4="filed 2021-05-07, accession 0001193125-21-153747",
                note="an S-4 registers securities in a business combination and the former "
                     "name is a classic merger-vehicle pattern — but neither is a filed "
                     "statement of what happened to existing APO shares"),
            classification="UNRESOLVED",
            why="the two documents retrieved contain no predecessor/successor or share-"
                "conversion language; the S-4 body was not read",
            next_step="read the S-4 (0001193125-21-153747) merger-consideration section, "
                      "or locate the closing 8-K describing share treatment"),
    }

    by = {}
    for c in cases.values():
        by[c["classification"]] = by.get(c["classification"], 0) + 1
    unresolved = [k for k, c in cases.items() if c["classification"] == "UNRESOLVED"]

    p = dict(
        report_id="MASSIVE_LINEAGE_PRIMARY_EVIDENCE_RESOLUTION_V1",
        status="HOLD" if unresolved else "LINEAGE_PRIMARY_EVIDENCE_RESOLUTION_COMPLETE",
        scope=["APO", "BLK", "XOM", "CRH"],
        prior=dict(artifact=PRIOR, digest=ART.file_digest(PRIOR)),
        evidence_standard=dict(
            primary="retrieved filing BYTES, archived with SHA-256, with quoted language "
                    "from the document itself",
            discovery_aids_only=["filing metadata", "form type", "EDGAR formerNames",
                                 "search snippets"],
            why="the previous gate showed how easily a form type slides into standing in "
                "for evidence it cannot provide",
            archive=ARC),
        cases=cases, summary=by, unresolved=unresolved,
        second_v6_defect_found=dict(
            finding="V6 transition dates are QUARTERLY ANCHORS, not event dates",
            evidence="CRH's filed cancellation is 2023-09-25; V6 says 2023-10-02. BLK's "
                     "filed reorganisation is 2024-10-01; V6 says 2025-01-02.",
            mechanism="V6 detected changes at quarterly anchors and binary-searched only "
                      "between anchors, so a boundary lands on the first anchor that "
                      "observed the change",
            implication="any V7 reusing V6's dates would inherit boundaries that are days "
                        "to months late — this is separate from the truncation defect and "
                        "affects the four already-confirmed cases too",
            not_investigated="whether J / BG / LH / FERG boundaries are similarly late"),
        v7_precondition=dict(rule="all eight truncation candidates need explicit treatment",
                             resolved=7, unresolved=1, unresolved_list=unresolved,
                             met=False),
        no_changes=dict(v6_modified=False, security_key_v1_modified=False,
                        universe_modified=False, raw_archive_modified=False,
                        derived_output=0, v7_created=False,
                        builder_spec_v2_sealed=False, smoke_executed=False),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_LINEAGE_PRIMARY_EVIDENCE_RESOLUTION_V1.json",
                 required=("report_id", "status", "cases", "evidence_standard",
                           "v7_precondition", "no_changes"),
                 supersede=os.path.exists(
                     "MASSIVE_LINEAGE_PRIMARY_EVIDENCE_RESOLUTION_V1.json"))
    print(f"MASSIVE_LINEAGE_PRIMARY_EVIDENCE_RESOLUTION_V1 · {d} · {p['status']}")
    for k, c in cases.items():
        print(f"  {k:4s} -> {c['classification']}")
    print(f"  summary: {by}")
    print(f"  second V6 defect: transition dates are quarterly anchors, not event dates")
    print(f"  v7 precondition met: {p['v7_precondition']['met']} (7/8)")
    return 1 if unresolved else 0


if __name__ == "__main__":
    raise SystemExit(main())
