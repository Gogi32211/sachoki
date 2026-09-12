"""APO_PRIMARY_SECURITY_CONTINUITY_RESOLUTION_V1 — the last of the eight, with a caveat.

The filed 424B3 settles the mechanics:

    "each outstanding AGM Class A Share will be converted automatically into the right to
     receive one duly authorized, validly issued, fully paid and nonassessable HoldCo Share"

    "HoldCo ... will be renamed Apollo Global Management, Inc. The HoldCo Shares will be
     listed on the NYSE under the symbol 'APO.'"

and the successor's own charter carries continuity-of-holding language: a stockholder who
received HoldCo shares on conversion "shall be deemed to have continuously owned such HoldCo
Common Shares through the period during which the stockholder continuously owned the AGM
Class A Shares so converted".

One-for-one, automatic, same ticker, same exchange. Security lineage is continuous.

BUT THIS IS NOT THE SAME SHAPE AS BLK OR XOM, AND THE ARTIFACT SAYS SO. Those were
single-company holding reorganisations: the business on either side of the boundary is the
same business. Apollo is a MERGER OF TWO COMPANIES — AGM and Athene Holding — into a new
parent. AHL Class A shares converted into the same HoldCo stock at 1.149.

So while the SECURITY is continuous, the ISSUER'S COMPOSITION is not. Pre-2022 APO prices
are Apollo standalone; post-2022 prices are Apollo plus Athene. A study that extends APO's
history across that boundary and treats the series as one economic object will be comparing
two different companies and will see a structural break that is real rather than a data
artefact.

That caveat is recorded as a first-class field, not a footnote, because "continuity
confirmed" is exactly the phrase that would otherwise carry it silently into a five-year
anatomy study.

CHECKED FOR COLLAPSE. Extending APO backward to AGM Class A does not merge two frozen
securities: AGM Class A is the unique 1:1 predecessor of the APO listing, and Athene traded
separately under ATH, which is not in the frozen 503.
"""
from __future__ import annotations
import json, os, sys, time                                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

ARC = "/Volumes/QUANT_RESEARCH/artifacts/provenance/lineage_primary_evidence"
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"


def main():
    keys = {r["frozen_current_ticker"]: r["security_key_v1"]
            for r in json.load(open(IDENTITY))["mapping"]}

    p = dict(
        spec_id="APO_PRIMARY_SECURITY_CONTINUITY_RESOLUTION_V1",
        status="APO_PRIMARY_CONTINUITY_RESOLVED",
        classification="SECURITY_CONTINUITY_CONFIRMED",
        security_key_v1=keys["APO"], current_ticker="APO",
        v6_regular_way_start="2022-01-03",

        entities=dict(
            predecessor_listed_security="Apollo Global Management, Inc. Class A Shares "
                                        "(AGM Class A), NYSE: APO",
            successor_listed_security="HoldCo Shares of Tango Holdings, Inc., renamed "
                                      "Apollo Global Management, Inc., NYSE: APO",
            predecessor_cik="0001411494",
            successor_cik="0001858681",
            successor_former_name="Tango Holdings, Inc.",
            role_of_tango="the registrant that became the new parent entity to AGM and "
                          "AHL and was renamed Apollo Global Management, Inc.",
            cik_evidence="EDGAR company search for 10-K filers named 'Apollo Global "
                         "Management' returns exactly two CIKs: 0001411494 (predecessor) "
                         "and 0001858681 (successor)",
            earlier_conversion_out_of_window="the 424B3 also references Apollo Global "
                                             "Management, LLC's earlier conversion to AGM "
                                             "(2019, same CIK 0001411494). That predates "
                                             "the 2021-08-25 window start and is not a "
                                             "boundary this programme has to treat."),

        evidence=[dict(form="424B3", filed="2021-11-05",
                       accession="0001193125-21-320872",
                       document="d168064d424b3.htm",
                       path="/Archives/edgar/data/1858681/000119312521320872/"
                            "d168064d424b3.htm",
                       http=200, bytes=5164800, sha256="5a60bccd79590c2b",
                       archived_in=ARC),
                  dict(form="8-K", filed="2022-01-07",
                       accession="0000950142-22-000221", sha256="bf24455fdc5cb4c3",
                       archived_in=ARC, result="retrieved; no share-treatment language"),
                  dict(form="S-8 POS", filed="2022-03-16",
                       accession="0000950142-22-001046", sha256="1064d3a21f4a87e8",
                       archived_in=ARC, result="retrieved; no share-treatment language")],

        quoted_mechanics=[
            "As a result of the transactions contemplated by the merger agreement, each "
            "outstanding AGM Class A Share will be converted automatically into the right "
            "to receive one duly authorized, validly issued, fully paid and nonassessable "
            "HoldCo Share.",
            "HoldCo ... will be renamed Apollo Global Management, Inc. The HoldCo Shares "
            "will be listed on the NYSE under the symbol \"APO.\"",
            "...to any HoldCo Common Shares that were issued upon, and have been "
            "continuously owned by a stockholder since, the conversion of an AGM Class A "
            "Share ... such stockholder shall be deemed to have continuously owned such "
            "HoldCo Common Shares through the period during which the stockholder "
            "continuously owned the AGM Class A Shares so converted..."],

        share_treatment=dict(
            mechanism="automatic conversion",
            ratio="1 HoldCo Share per 1 AGM Class A Share",
            cash_consideration="none for AGM Class A holders (AOG unit holders received "
                               "$3.66 cash plus one share, a separate Up-C elimination)",
            different_classes="AGM Class B and Class C were single shares held internally; "
                              "the listed public security is AGM Class A",
            listing="continuous on NYSE under APO"),

        economic_composition_caveat=dict(
            severity="MATERIAL — recorded as a first-class field, not a footnote",
            statement="the SECURITY is continuous but the ISSUER'S COMPOSITION is not",
            detail="this was a merger of TWO companies — AGM and Athene Holding Ltd — into "
                   "a new parent, with AHL Class A shares converting into the same HoldCo "
                   "stock at 1.149. Pre-2022 APO prices are Apollo standalone; post-2022 "
                   "prices are Apollo plus Athene.",
            contrast="BLK and XOM were single-company holding reorganisations, where the "
                     "business on either side of the boundary is the same business",
            research_consequence="a study extending APO across this boundary and treating "
                                 "the series as one economic object is comparing two "
                                 "different companies; the structural break there is REAL, "
                                 "not a data artefact",
            why_recorded_here="'continuity confirmed' is exactly the phrase that would "
                              "otherwise carry this silently into a five-year anatomy "
                              "study"),

        collapse_check=dict(
            question="would extending APO backward merge two frozen securities?",
            answer="no",
            reasoning="AGM Class A is the unique 1:1 predecessor of the APO listing; "
                      "Athene traded separately under ATH at a 1.149 ratio and ATH is not "
                      "in the frozen 503",
            simultaneous_distinct_securities_collapsed=0),

        temporal_boundary=dict(
            legal_effective="the mergers closed following the AGM special meeting of "
                            "2021-12-17; the filing does not state an exchange-session "
                            "boundary",
            last_predecessor_xnys_session="NOT ESTABLISHED from the filing",
            first_successor_xnys_session="NOT ESTABLISHED from the filing",
            bounded="between the 2021-12-17 special meeting and the successor's first "
                    "Form 3/4 filings dated 2022-01-03",
            v6_start_coincides="V6's 2022-01-03 falls inside that bound, but V6's dates "
                               "are anchor-detection artefacts and are NOT treated as "
                               "authority here",
            resolve_in="MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1"),

        corroborative_only=dict(
            ticker="APO on both sides",
            massive_aggregates="89 of 90 pre-boundary sessions returned bars during "
                               "supplemental preservation",
            note="recorded separately; none of this upgraded the primary evidence"),

        eight_case_status=dict(
            J="CONTINUITY_CONFIRMED", BG="CONTINUITY_CONFIRMED",
            LH="CONTINUITY_CONFIRMED", FERG="CONTINUITY_CONFIRMED",
            BLK="SECURITY_CONTINUITY_CONFIRMED", XOM="SECURITY_CONTINUITY_CONFIRMED",
            CRH="CONTINUITY_CONFIRMED_WITH_INSTRUMENT_TRANSITION",
            APO="SECURITY_CONTINUITY_CONFIRMED",
            all_eight_treated=True),

        immutability=dict(v6_mutation=0, security_key_v1_mutation=0,
                          universe_mutation=0, canonical_raw_mutation=0,
                          supplemental_quarantine_mutation=0, derived_writes=0,
                          v7_created=False, builder_spec_v2_sealed=False,
                          smoke_executed=False),
        limitations=["exchange-session boundary not established from the filing",
                     "the economic-composition break is real and unresolved by any "
                     "lineage repair — it is a research-design constraint, not a data fix"],
        does_not_authorize="V7. The boundary audit must run first: V6's dates for all eight "
                           "are anchor artefacts.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "APO_PRIMARY_SECURITY_CONTINUITY_RESOLUTION_V1.json",
                 required=("spec_id", "status", "classification", "evidence",
                           "quoted_mechanics", "share_treatment",
                           "economic_composition_caveat", "immutability"),
                 supersede=os.path.exists(
                     "APO_PRIMARY_SECURITY_CONTINUITY_RESOLUTION_V1.json"))
    print(f"APO_PRIMARY_SECURITY_CONTINUITY_RESOLUTION_V1 · {d} · {p['status']}")
    print(f"  classification : {p['classification']}")
    print(f"  mechanism      : {p['share_treatment']['mechanism']} · "
          f"{p['share_treatment']['ratio']}")
    print(f"  evidence       : 424B3 sha 5a60bccd79590c2b (5.16 MB, archived)")
    print(f"  CAVEAT         : {p['economic_composition_caveat']['statement']}")
    print(f"  8/8 treated    : {p['eight_case_status']['all_eight_treated']}")
    print(f"  V7             : not created — boundary audit first")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
