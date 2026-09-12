"""SP500_PIT_FIXTURES_V2 — V1 unchanged, plus the pre-2012 deletion coverage it lacked.

WHY EXPANDING THE FIXTURE SET IS LEGITIMATE HERE, AND USUALLY IS NOT. Adding a test after
testing has begun is normally how a standard gets bent toward a preferred answer. It is
admissible in this one case because of WHERE the gap was found: in the qualification
design, while enumerating what sources exist, with zero candidates evaluated, zero
membership rows read and zero price data touched. Nothing had produced a result that this
addition could be responding to.

THE FAILURE MODE V1 COULD NOT SEE. A source can begin reliable change-event tracking at
some year T while still showing pre-T join dates for constituents who never left. Such a
source looks excellent:

    survivors carry long, plausible member_since dates
    every post-T addition and deletion is correct
    but constituents DELETED before T are simply absent

That is asymmetric historical coverage, and it is survivorship bias in exactly the period
where it is hardest to notice. V1's fixtures span 2014-2025 and every one of them would
pass. The question a survivor-only history cannot answer is: show me a security that was
unquestionably in the S&P 500 before 2012 and unquestionably left before 2012.

F8 AND F9 ALSO TEST A SECOND THING, BY ACCIDENT OF HOW S&P WROTE THEM. Every V1 fixture is
phrased "effective prior to the open of trading on E". Both V2 fixtures are phrased "after
the close of trading on D". These describe the same boundary from opposite sides, and a
source that hard-codes one reading lands one session off on the other. V1 could not detect
that either.

V1 IS NOT WRONG AND IS NOT REWRITTEN. It stays sealed at its own digest. V2 supersedes it
for source qualification only, and inherits its fixtures by LOADING them, not by retyping
them, so "unchanged" is a property of the code rather than a claim in a docstring.
"""
from __future__ import annotations
import json, os, sys, time                                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

V1 = "SP500_PIT_FIXTURES_V1.json"
ARC = "SP500_PIT_FIXTURE_SOURCE_ARCHIVE_V2.json"
QUAL = "SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1.json"
RETRIEVED = "2026-08-25"


def new_fixtures():
    """The two pre-2012 deletions. Selection rule was fixed BEFORE searching: at most two,
    chosen for primary-source boundary certainty alone."""
    return [
        dict(
            fixture_id="F8", transition_type="PRE_TRACKING_HISTORY_DELETION",
            entity=dict(outgoing=dict(name="Genzyme Corp.", ticker="GENZ",
                                      exchange="NASDAQ"),
                        incoming=dict(name="BlackRock Inc.", ticker="BLK",
                                      exchange="NYSE")),
            announcement_date="2011-03-29",
            effective_wording="after the close of trading on Friday, April 1, 2011",
            effective_boundary_side="AFTER THE CLOSE of D — the opposite phrasing from "
                                    "every V1 fixture",
            last_old_membership_session="2011-04-01",
            first_non_member_session="2011-04-04",
            deletion_reason="Sanofi-aventis (NYSE: SNY) acquiring Genzyme; deal expected "
                            "to complete pending final conditions",
            ceased_membership_before_2012="YES",
            unquestionably_a_member_before_E="YES — Genzyme was a large-cap S&P 500 "
                                             "biotechnology constituent",
            tests=["a source whose change history begins after 2011 cannot produce this "
                   "departure at all; survivor-only history fails here and passes every "
                   "V1 fixture",
                   "'after the close of D' must map to the same boundary semantics as "
                   "'prior to the open of E'; a source hard-coding one reading is one "
                   "session off",
                   "boundary: GENZ a constituent on 2011-04-01, not on 2011-04-04"],
            calendar_note="2011-04-01 was a Friday; the next session is Monday 2011-04-04. "
                          "Good Friday 2011 fell on April 22, so no holiday intervenes and "
                          "the boundary is determinable without the unresolved P9 calendar.",
            primary_source=dict(
                url="https://www.prnewswire.com/news-releases/standard--poors-announces-"
                    "change-to-us-index-118873854.html",
                publisher="Standard & Poor's Index Services, distributed via PR Newswire",
                quote="BlackRock Inc. (NYSE: BLK) will replace Genzyme Corp. (NASD: GENZ) "
                      "in the S&P 500 index after the close of trading on Friday, "
                      "April 1, 2011."),
            secondary_source=None, retrieved_at=RETRIEVED),

        dict(
            fixture_id="F9", transition_type="PRE_TRACKING_HISTORY_DELETION",
            entity=dict(outgoing=dict(name="Cephalon Inc.", ticker="CEPH",
                                      exchange="NASDAQ"),
                        incoming=dict(name="TE Connectivity Ltd.", ticker="TEL",
                                      exchange="NYSE")),
            announcement_date="2011-10-11",
            effective_wording="after the close of trading on Friday, October 14",
            effective_boundary_side="AFTER THE CLOSE of D",
            last_old_membership_session="2011-10-14",
            first_non_member_session="2011-10-17",
            deletion_reason="Teva Pharmaceutical Industries (NASD: TEVA) acquiring "
                            "Cephalon; deal expected to complete on or about that date",
            ceased_membership_before_2012="YES",
            unquestionably_a_member_before_E="YES — Cephalon was an S&P 500 "
                                             "biopharmaceutical constituent",
            tests=["a second, independent pre-2012 departure, so the coverage test does "
                   "not rest on one company or one month",
                   "the announcement omits the YEAR in its effective sentence ('Friday, "
                   "October 14'), which the release date resolves — a source parsing "
                   "announcement text must handle that",
                   "boundary: CEPH a constituent on 2011-10-14, not on 2011-10-17"],
            calendar_note="2011-10-14 was a Friday; the next session is Monday 2011-10-17. "
                          "Columbus Day is not an NYSE holiday, so no holiday intervenes.",
            primary_source=dict(
                url="https://www.prnewswire.com/news-releases/sp-indices-announces-change-"
                    "to-us-index-131552863.html",
                publisher="S&P Indices, distributed via PR Newswire",
                quote="Cephalon is being acquired by Teva Pharmaceutical Industries Ltd. "
                      "(NASD: TEVA) in a deal expected to be completed on or about that "
                      "date pending final approvals."),
            secondary_source=None, retrieved_at=RETRIEVED),
    ]


def payload():
    v1 = json.load(open(V1))
    inherited = v1["fixtures"]                      # loaded, not retyped
    added = new_fixtures()
    return dict(
        spec_id="SP500_PIT_FIXTURES_V2",
        status="FROZEN BEFORE SOURCE SELECTION — no candidate source has been evaluated",
        supersedes=dict(
            artifact=V1, digest=ART.file_digest(V1),
            scope="supersedes V1 FOR SOURCE QUALIFICATION ONLY",
            v1_state="VALID and PRESERVED — V1 is not rewritten, not withdrawn, and not "
                     "wrong; it was incomplete for one specific failure mode"),
        reason_for_v2=dict(
            gap="pre-2012 deletion-history coverage was not tested by V1",
            failure_mode="a source may begin reliable change tracking at year T while "
                         "retaining pre-T join dates for surviving constituents, so that "
                         "constituents deleted before T are absent entirely",
            why_v1_could_not_see_it="V1's fixtures span 2014-2025; a survivor-only history "
                                    "passes all of them",
            discovered_from="the qualification design during candidate enumeration",
            NOT_discovered_from="any candidate result, membership row, price series or "
                                "outcome — zero candidates had been evaluated when this "
                                "gap was identified"),
        lineage=dict(
            inherits="all V1 fixtures, unchanged",
            mechanism="the inherited block is LOADED from the sealed V1 artifact rather "
                      "than retyped, so equality is structural",
            inherited_ids=[f["fixture_id"] for f in inherited],
            added_ids=[f["fixture_id"] for f in added]),
        selection_rule_fixed_in_advance=dict(
            maximum="at most 2 pre-2012 deletion fixtures",
            criterion="best primary-source boundary certainty",
            explicitly_not="candidate behaviour — this rule was fixed before any source "
                           "was searched, so a second fixture cannot be added later "
                           "because the first proved inconvenient",
            outcome="two found with equal boundary certainty; both included"),
        insufficient_coverage_rule=dict(
            statement="absence of the deleted security or event is NOT evidence that it "
                      "was never a constituent",
            rule="if a candidate's historical coverage begins after a fixture's effective "
                 "deletion date, the classification is INSUFFICIENT_HISTORICAL_COVERAGE",
            explicitly_not_PASS="silence must never be scored as agreement",
            explicitly_not_NOT_A_MEMBER="silence must never be scored as a factual "
                                        "negative",
            why="this is the same FALSE vs UNAVAILABLE distinction the minute-availability "
                "amendment draws for market data. A universe history needs it for the same "
                "reason: 'we have no record' and 'it was not a member' are different "
                "statements, and conflating them manufactures a clean-looking universe.",
            related="MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1 three-valued availability"),
        boundary_convention=dict(
            our_rule="effective prior to the open of session E means E-1 is the last "
                     "session of old membership and E is the first session of new "
                     "membership",
            v2_addition="F8 and F9 are phrased 'after the close of trading on D', which "
                        "means D is the last session of old membership and the next "
                        "session is the first without it",
            equivalence="both phrasings describe the same boundary from opposite sides; a "
                        "source must map them identically",
            calendar_independence="both V2 fixtures were checked to fall on a Friday whose "
                                  "following Monday is a trading day, so neither depends "
                                  "on the unresolved P9 exchange calendar"),
        residual_limitation=dict(
            issue="both pre-2012 fixtures are M&A-driven deletions",
            consequence="a pre-2012 MARKET-CAP-driven deletion is not separately covered, "
                        "so a source that records pre-2012 acquisitions but not pre-2012 "
                        "rebalance removals would still pass",
            status="RECORDED, NOT PATCHED — the selection rule caps this set at two, and "
                   "that cap was fixed before searching. Recording the residue is "
                   "preferable to quietly exceeding the rule that makes the set credible.",
            note="the defect class V2 targets — no pre-T deletions at all — is covered"),
        n_fixtures=len(inherited) + len(added),
        fixtures=inherited + added,
        source_archive=dict(artifact=ARC, digest=ART.file_digest(ARC)
                            if os.path.exists(ARC) else None,
                            state="all nine fixtures have a raw snapshot whose bytes were "
                                  "verified to contain the cited sentence"),
        qualification=dict(artifact=QUAL, digest=ART.file_digest(QUAL)
                           if os.path.exists(QUAL) else None),
        candidate_source_evaluations=0,
        price_joins=0,
        episode_or_outcome_access=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))


def main():
    p = payload()
    v1 = json.load(open(V1))
    n_inh = len(p["lineage"]["inherited_ids"])
    same = (json.dumps(p["fixtures"][:n_inh], sort_keys=True)
            == json.dumps(v1["fixtures"], sort_keys=True))
    if not same:
        raise SystemExit("REFUSING TO SEAL: inherited V1 fixtures are not identical")
    d = ART.seal(p, "SP500_PIT_FIXTURES_V2.json",
                 required=("spec_id", "status", "supersedes", "lineage", "fixtures",
                           "insufficient_coverage_rule", "boundary_convention"),
                 supersede=os.path.exists("SP500_PIT_FIXTURES_V2.json"))
    print(f"SP500_PIT_FIXTURES_V2 · {d} · {p['status']}")
    print(f"  inherited from V1 unchanged: {n_inh} fixtures — verified identical")
    for f in p["fixtures"][n_inh:]:
        print(f"  + {f['fixture_id']}  {f['transition_type']}  "
              f"{f['entity']['outgoing']['ticker']} "
              f"{f['last_old_membership_session']} -> {f['first_non_member_session']}")
    print(f"  total fixtures: {p['n_fixtures']}")
    print(f"  candidate evaluations: {p['candidate_source_evaluations']} · "
          f"price joins: {p['price_joins']}")


if __name__ == "__main__":
    main()
