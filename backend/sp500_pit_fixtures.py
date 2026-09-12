"""SP500_PIT_FIXTURES_V1 — independently confirmed membership transitions, frozen BEFORE any
candidate source is queried.

STAGE 2 OF THE TWO-STAGE FREEZE. SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1 froze the
fixture DESIGN — which transition types must be covered and what must be known about each —
precisely because freezing dates recalled from memory would have made a wrong date the
standard every candidate source was judged against. This artifact supplies the instances,
each confirmed from a primary source, and only now are they ground truth.

THE CORRECTION THAT JUSTIFIES THE SPLIT. The proposal listed the Tesla addition as
"replacing AIV" on the strength of the 2020-11-16 announcement. That release does not name
the replaced company at all — it explicitly defers it: the company to be removed was named
in a SEPARATE release on 2020-12-11. Had the design and the instances been frozen together
from recollection, the fixture would have asserted a fact its own cited source did not
contain. It is now sourced to the release that actually states it.

A SECOND PROPOSAL WAS DROPPED RATHER THAN REPAIRED. The Hertz bankruptcy case could not be
confirmed as an S&P 500 membership transition, so it is not included and nothing was
substituted by assumption. SVB Financial replaces it, sourced to S&P DJI directly.

THE BOUNDARY IS THE POINT. Every membership fixture pins BOTH sides:

    effective prior to the open of session E
      -> E-1 is the LAST session of old membership
      -> E   is the FIRST session of new membership

A source with an off-by-one convention passes a one-sided test and fails this one. That is
the whole reason both columns exist.

CALENDAR INDEPENDENCE, CHECKED DELIBERATELY. E-1 is a TRADING session, not a calendar day,
so in general it depends on the exchange calendar — which is still unresolved under probe
P9. Every fixture here was chosen so that E-1 is the immediately preceding weekday with no
US market holiday adjacent, making each boundary determinable without P9. This is a
property of the selected instances, not a general licence to compute E-1 by subtracting a
day.

No candidate source supplied any fact below. Nothing is selected. Nothing is queried.
"""
from __future__ import annotations
import os, sys, time                                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

QUAL = "SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1.json"
RETRIEVED = "2026-08-24"


def fixtures():
    return [
        dict(
            fixture_id="F1", transition_type="scheduled addition",
            entity=dict(incoming=dict(name="Tesla Inc.", ticker="TSLA", exchange="NASDAQ"),
                        outgoing=dict(name="Apartment Investment and Management Co.",
                                      ticker="AIV", exchange="NYSE")),
            announcement_date="2020-11-16 (addition announced; replaced company NOT named)",
            second_announcement="2020-12-11 17:59 ET — names Apartment Investment and "
                                "Management Co. (AIV) as the removed company",
            effective_wording="effective prior to the open of trading on Monday, "
                              "December 21, to coincide with the December quarterly "
                              "rebalance",
            effective_date="2020-12-21",
            first_new_membership_session="2020-12-21",
            last_old_membership_session="2020-12-18",
            tests=["announcement_date != effective_date, with a 35-day separation",
                   "a single transition can carry MORE THAN ONE announcement; a source "
                   "keyed on 'the' announcement date will mis-handle this",
                   "boundary: AIV must be a constituent on 2020-12-18 and not on "
                   "2020-12-21; TSLA the reverse"],
            primary_source=dict(
                url="https://press.spglobal.com/2020-11-16-Tesla-Set-to-Join-S-P-500",
                publisher="S&P Dow Jones Indices",
                quote="Tesla Inc. (NASD:TSLA) will be added to the S&P 500 effective prior "
                      "to the open of trading on Monday, December 21 to coincide with the "
                      "December quarterly rebalance.",
                note="does NOT name the removed company"),
            secondary_source=dict(
                url="https://www.prnewswire.com/news-releases/tesla-set-to-join-sp-500--"
                    "100-apartment-income-reit-to-join-sp-midcap-400-301191493.html",
                publisher="S&P Dow Jones Indices release distributed via PR Newswire",
                quote="Apartment Investment and Management is spinning off Apartment "
                      "Income REIT in a transaction expected to be completed post close on "
                      "Monday, December 14."),
            retrieved_at=RETRIEVED),

        dict(
            fixture_id="F2", transition_type="spin-off (companion to F1)",
            entity=dict(parent=dict(name="Apartment Investment and Management Co.",
                                    ticker="AIV"),
                        spun_off=dict(name="Apartment Income REIT Corp.", ticker="AIRC")),
            announcement_date="2020-12-11",
            effective_wording="spin-off expected to be completed post close on Monday, "
                              "December 14; AIRC added to S&P MidCap 400 replacing Dunkin' "
                              "Brands Group Inc.",
            effective_date="2020-12-14 (spin-off completion, post close)",
            first_new_membership_session="NOT APPLICABLE — AIRC enters MidCap 400, not the "
                                         "S&P 500",
            last_old_membership_session="NOT APPLICABLE",
            tests=["entity continuity across a spin-off: does the source treat AIV "
                   "post-spin as the SAME entity, and AIRC as a NEW one",
                   "a spun-off entity may enter a DIFFERENT index than its parent",
                   "the removal reason is structural, not market drift: 'Post spin-off, "
                   "Apartment Investment and Management will no longer be representative "
                   "of the S&P Composite 1500 indices market cap ranges'"],
            primary_source=dict(
                url="https://www.prnewswire.com/news-releases/tesla-set-to-join-sp-500--"
                    "100-apartment-income-reit-to-join-sp-midcap-400-301191493.html",
                publisher="S&P Dow Jones Indices release distributed via PR Newswire",
                quote="Post spin-off, Apartment Investment and Management will no longer "
                      "be representative of the S&P Composite 1500 indices market cap "
                      "ranges."),
            secondary_source=None,
            status="SUPPORTING — exercises spin-off identity; not one of the six required "
                   "types",
            retrieved_at=RETRIEVED),

        dict(
            fixture_id="F3", transition_type="scheduled deletion (market capitalisation)",
            entity=dict(incoming=dict(name="AppLovin Corp.", ticker="APP"),
                        outgoing=dict(name="MarketAxess Holdings Inc.", ticker="MKTX")),
            announcement_date="2025-09-05",
            effective_wording="effective prior to the open of trading on Monday, "
                              "September 22, to coincide with the quarterly rebalance",
            effective_date="2025-09-22",
            first_new_membership_session="2025-09-22",
            last_old_membership_session="2025-09-19",
            tests=["a deletion driven by index representativeness rather than a corporate "
                   "action — the removed company continues to exist and trade",
                   "the removed company moves DOWN to another index rather than "
                   "disappearing; a source modelling deletion as 'end of life' fails here",
                   "boundary: MKTX constituent on 2025-09-19, not on 2025-09-22"],
            companion_changes="the same release moves Robinhood Markets (HOOD) in for "
                              "Caesars Entertainment (CZR) and Emcor Group (EME) in for "
                              "Enphase Energy (ENPH) on the same effective date",
            primary_source=dict(
                url="https://press.spglobal.com/2025-09-05-AppLovin,-Robinhood-Markets-"
                    "and-Emcor-Group-Set-to-Join-S-P-500-Others-to-Join-S-P-100,-S-P-"
                    "MidCap-400-and-S-P-SmallCap-600",
                publisher="S&P Dow Jones Indices",
                quote="The changes ensure each index is more representative of its market "
                      "capitalization range."),
            secondary_source=None, retrieved_at=RETRIEVED),

        dict(
            fixture_id="F4", transition_type="M&A-driven replacement",
            entity=dict(incoming=dict(name="Arch Capital Group Ltd.", ticker="ACGL"),
                        outgoing=dict(name="Twitter Inc.", ticker="TWTR")),
            announcement_date="2022-10-27",
            effective_wording="effective prior to the opening of trading on Tuesday, "
                              "November 1",
            effective_date="2022-11-01",
            first_new_membership_session="2022-11-01",
            last_old_membership_session="2022-10-31",
            tests=["removal caused by an acquisition CLOSING, not by index policy",
                   "the acquisition was expected to close 2022-10-28 while index removal "
                   "took effect 2022-11-01 — deal close date and membership effective date "
                   "are DIFFERENT and a source conflating them fails",
                   "boundary: TWTR constituent on 2022-10-31, not on 2022-11-01"],
            primary_source=dict(
                url="https://press.spglobal.com/2022-10-27-Arch-Capital-Group-Set-to-Join-"
                    "S-P-500-RXO-to-Join-S-P-MidCap-400-Bread-Financial-Holdings-to-Join-"
                    "S-P-SmallCap-600",
                publisher="S&P Dow Jones Indices",
                quote="effective prior to the opening of trading on Tuesday, November 1",
                reason="Elon Musk is acquiring Twitter in a transaction expected to close "
                       "on October 28."),
            secondary_source=None, retrieved_at=RETRIEVED),

        dict(
            fixture_id="F5", transition_type="ticker change with entity continuity",
            entity=dict(name="Meta Platforms, Inc.",
                        ticker_before="FB", ticker_after="META", exchange="NASDAQ",
                        entity_continuity="PRESERVED", security_continuity="PRESERVED"),
            announcement_date="2022-05-31",
            effective_wording="ticker symbol change effective prior to market open on "
                              "June 9, 2022",
            effective_date="2022-06-09",
            first_new_membership_session="2022-06-09 (first session under META)",
            last_old_membership_session="2022-06-08 (last session under FB)",
            membership_change="NONE — the company remained an S&P 500 constituent "
                              "throughout; this fixture tests IDENTITY, not membership",
            tests=["the CUSIP is explicitly unchanged, so entity and security identity are "
                   "continuous while the ticker is not",
                   "a source keyed on ticker will show a spurious deletion of FB and a "
                   "spurious addition of META on 2022-06-09 — this is the single most "
                   "direct test of requirement 2 (security identity)",
                   "a correct source reports ONE continuous membership with a "
                   "ticker_at_time change"],
            primary_source=dict(
                url="https://investor.atmeta.com/investor-news/press-release-details/2022/"
                    "Meta-Platforms-Inc.-to-Change-Ticker-Symbol-to-META-on-June-9/"
                    "default.aspx",
                publisher="Meta Platforms investor relations",
                quote="The company's Class A common stock will continue to be listed on "
                      "NASDAQ and its CUSIP number will remain unchanged."),
            secondary_source=None, retrieved_at=RETRIEVED),

        dict(
            fixture_id="F6", transition_type="share-class / security transition",
            entity=dict(name="Google Inc. (later Alphabet Inc.)",
                        class_a=dict(ticker="GOOGL"), class_c=dict(ticker="GOOG"),
                        entity_continuity="PRESERVED",
                        securities="ONE entity, TWO index securities"),
            announcement_date="2014-03-11",
            effective_wording="new Class C shares distributed as a dividend on "
                              "June 20, 2014",
            effective_date="2014-06-20",
            first_new_membership_session="UNCONFIRMED",
            last_old_membership_session="UNCONFIRMED",
            boundary_status="EXCLUDED FROM THE BOUNDARY TEST. The exact first session on "
                            "which the S&P 500 carried two Alphabet index lines was not "
                            "established from a primary source, and is NOT derived by "
                            "assuming it equals the distribution date. This fixture tests "
                            "identity representation only.",
            tests=["can the source represent ONE entity holding TWO distinct index "
                   "securities simultaneously",
                   "a schema of ticker|start|end cannot express this and will either drop "
                   "a line or duplicate the entity",
                   "S&P DJI reversed a PREVIOUSLY ANNOUNCED treatment in this same "
                   "release, so a source that recorded the earlier plan without the "
                   "revision carries a stale decision"],
            policy_context="S&P DJI announced that all eligible trading lines meeting "
                           "liquidity and materiality thresholds would be included, "
                           "transitioning from September 2015; indices therefore hold more "
                           "share lines than companies",
            primary_source=dict(
                url="https://press.spglobal.com/2014-03-11-S-P-Dow-Jones-Indices-Announces-"
                    "Changes-in-Treatment-of-Multiple-Share-Classes-in-U-S-Indices-and-"
                    "Revises-Previously-Announced-Treatment-of-Google-Stock-Split",
                publisher="S&P Dow Jones Indices",
                quote="Both Google Class A and Google Class C will be included in the "
                      "S&P 500 and S&P 100."),
            secondary_source=None, retrieved_at=RETRIEVED),

        dict(
            fixture_id="F7", transition_type="bankruptcy / delisting removal",
            entity=dict(incoming=dict(name="Insulet Corp.", ticker="PODD"),
                        outgoing=dict(name="SVB Financial Group", ticker="SIVB")),
            announcement_date="2023-03-10",
            effective_wording="effective prior to the opening of trading on Wednesday, "
                              "March 15",
            effective_date="2023-03-15",
            first_new_membership_session="2023-03-15",
            last_old_membership_session="2023-03-14",
            tests=["removal for INELIGIBILITY following FDIC receivership — neither a "
                   "scheduled rebalance nor a completed acquisition",
                   "the announcement-to-effective gap is only 5 calendar days, versus 35 "
                   "in F1; a source assuming a fixed lead time fails on one of the two",
                   "boundary: SIVB constituent on 2023-03-14, not on 2023-03-15"],
            replaces_dropped_proposal="the originally proposed Hertz (HTZ) bankruptcy case "
                                      "could NOT be confirmed as an S&P 500 membership "
                                      "transition and was dropped rather than assumed",
            primary_source=dict(
                url="https://press.spglobal.com/2023-03-10-Insulet-Set-to-Join-S-P-500",
                publisher="S&P Dow Jones Indices",
                quote="The Federal Deposit Insurance Corporation (FDIC) announced that it "
                      "has taken SVB Financial Group into FDIC Receivership and therefore "
                      "SVB Financial Group is no longer eligible for inclusion."),
            secondary_source=dict(
                note="a companion S&P DJI release of 2023-03-13 records Bunge Limited (BG) "
                     "replacing Signature Bank (SBNY) on the same effective date, "
                     "2023-03-15, for the same receivership reason"),
            retrieved_at=RETRIEVED),
    ]


def payload():
    fx = fixtures()
    req = ["scheduled addition", "scheduled deletion (market capitalisation)",
           "M&A-driven replacement", "ticker change with entity continuity",
           "share-class / security transition", "bankruptcy / delisting removal"]
    covered = {f["transition_type"] for f in fx}
    return dict(
        spec_id="SP500_PIT_FIXTURES_V1",
        status="FROZEN BEFORE SOURCE SELECTION — no candidate source has been queried",
        stage="stage 2 of the two-stage freeze defined in "
              "SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1",
        qualification=dict(artifact=QUAL, digest=ART.file_digest(QUAL)
                           if os.path.exists(QUAL) else None),
        required_types=req,
        required_types_covered=sorted(t for t in req if t in covered),
        all_required_types_covered=all(t in covered for t in req),
        n_fixtures=len(fx),
        evidence_rule="every fact below comes from a primary publisher — S&P Dow Jones "
                      "Indices for index membership, the issuer for security identity. No "
                      "candidate PIT source supplied any fixture fact, and none may.",
        boundary_convention=dict(
            rule="effective prior to the open of session E means E-1 is the last session "
                 "of old membership and E is the first session of new membership",
            two_sided="both columns are asserted, so an off-by-one convention cannot pass "
                      "a one-sided check",
            calendar_independence="every membership fixture was selected so that E-1 is "
                                  "the immediately preceding weekday with no adjacent US "
                                  "market holiday, making the boundary determinable "
                                  "without the still-unresolved P9 exchange calendar",
            not_a_general_rule="this is a property of the chosen instances; E-1 is a "
                               "TRADING session and is not computed by subtracting one day"),
        acceptance=dict(
            rule="a candidate source must reproduce every fixture exactly, including both "
                 "boundary sessions where asserted",
            one_miss="a single boundary miss is a systematic off-by-one, not a rounding "
                     "error, and classifies the source REJECTED",
            excluded="F6's boundary is UNCONFIRMED and is excluded from the boundary test; "
                     "F6 tests identity representation only",
            f2_scope="F2 is SUPPORTING and does not count toward the six required types"),
        open_items=[
            "F6 boundary sessions remain UNCONFIRMED and were deliberately not derived "
            "from the distribution date",
            "raw HTML/PDF snapshots of every primary source must be archived to the "
            "external research volume with a sha256 before these fixtures are used to "
            "judge a candidate; at present the evidence is a URL plus a verbatim quote "
            "retrieved on " + RETRIEVED],
        fixtures=fx,
        not_authorised="no candidate source is named, retrieved or evaluated here",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))


def main():
    p = payload()
    d = ART.seal(p, "SP500_PIT_FIXTURES_V1.json",
                 required=("spec_id", "status", "fixtures", "required_types",
                           "boundary_convention", "acceptance"),
                 supersede=os.path.exists("SP500_PIT_FIXTURES_V1.json"))
    print(f"SP500_PIT_FIXTURES_V1 · {d} · {p['status']}")
    for f in p["fixtures"]:
        b = (f"{f['last_old_membership_session']} -> {f['first_new_membership_session']}"
             if f["last_old_membership_session"] not in ("UNCONFIRMED", "NOT APPLICABLE")
             else f["last_old_membership_session"])
        print(f"  {f['fixture_id']}  {f['transition_type'][:38]:38s} {b}")
    print(f"  all six required types covered: {p['all_required_types_covered']}")
    print(f"  candidate sources queried     : 0")


if __name__ == "__main__":
    main()
