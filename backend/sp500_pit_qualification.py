"""SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1 — what a point-in-time membership source must
prove, frozen BEFORE any candidate source is named, retrieved or inspected.

WHY THIS COMES FIRST. Source selection looks like an engineering chore and behaves like a
research decision. Every candidate will disagree with the others somewhere, and once the
disagreements are visible there is always a defensible reason to prefer the one that
happens to give cleaner coverage — which is to say, the one that gives better results.
That preference would never appear in any output. Freezing the acceptance criteria while
no candidate has been examined is the only structural defence against it.

THE FAILURE THIS UNIVERSE EXISTS TO AVOID. Using today's S&P 500 over history selects
companies for having survived and been promoted into the index. It is the largest single
source of spurious edge in equity research, it inflates everything uniformly, and it is
invisible in-sample. Two shortcuts reproduce it exactly and are therefore FATAL rather
than discouraged: backfilling the current constituent list, and using price-history
availability as a membership proxy.

IDENTITY IS NOT A TICKER. Over a decade a ticker can move to a different company, a
company can persist while its security or share class changes, and an index seat can pass
between entities through a corporate action. A `ticker | start | end` table cannot express
any of that, so the canonical form here is an EVENT-SOURCED, IDENTITY-AWARE history from
which a daily eligibility table is derived deterministically.

THE ENFORCEMENT THAT MAKES "DO NOT SELECT ON YIELD" REAL. A rule saying the source must
not be chosen for producing more episodes is unenforceable as a promise. It is enforceable
as an ORDER OF OPERATIONS: the source is selected and sealed BEFORE its membership table
is ever joined to price data, outcomes, or episode counts. If the join cannot have
happened yet, the choice cannot have been informed by it.

Nothing is selected here. No source is named, retrieved, or evaluated.
"""
from __future__ import annotations
import os, sys, time                                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

CHARTER = "MASSIVE_1M_STATE_TRANSITION_V1.json"


def payload():
    return dict(
        spec_id="SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1",
        status="FROZEN BEFORE SOURCE SELECTION",
        programme="MASSIVE_1M_STATE_TRANSITION_V1",
        blocks="UNIVERSE_CONTRACT_V1, and through it all canonical ingestion",
        charter=dict(artifact=CHARTER, digest=ART.file_digest(CHARTER)
                     if os.path.exists(CHARTER) else None),

        # ── the ten minimum requirements ───────────────────────────────────────
        requirements={
            "1_effective_membership": dict(
                requires=["addition effective date", "deletion effective date"],
                rule="announcement_date is NEVER used as effective_date",
                why="index changes are announced days to weeks before they take effect; "
                    "treating the announcement as the effective date would place a "
                    "constituent in the index before it was one, in exactly the window "
                    "where the announcement itself moves the price"),
            "2_security_identity": dict(
                requires=["stable security/company identifier independent of ticker",
                          "explicit handling of ticker changes",
                          "explicit handling of share-class changes"],
                rule="a ticker string is not an identity and may not be the join key"),
            "3_event_types": dict(
                must_represent=["scheduled rebalance", "merger/acquisition",
                                "bankruptcy/delisting", "spin-off", "replacement",
                                "ticker/name change", "share-class substitution"],
                rule="an event the source cannot type is not silently mapped to the "
                     "nearest available type; it is UNRESOLVED"),
            "4_daily_eligibility": dict(
                requires="for every trading date D, a deterministic answer to 'was "
                         "security X an S&P 500 constituent for session D?'",
                rule="the answer is a function of the sealed event history alone"),
            "5_time_semantics": dict(
                our_convention="an index change effective PRIOR TO THE OPEN of session E "
                               "means E is the FIRST session with the new membership, and "
                               "E-1 is the LAST session with the old membership",
                requires="the source's OWN convention must be established explicitly and "
                         "mapped onto ours",
                rule="never inferred from the announcement date, and never assumed to "
                     "match ours because it looks like it does",
                boundary_test="every fixture must pin the last old-member session AND the "
                              "first new-member session, so an off-by-one convention "
                              "cannot pass"),
            "6_coverage": dict(
                requires="the full intended research period with no unexplained gaps",
                rule="a gap is either explained by a sealed reason or the source is "
                     "UNRESOLVED for that interval; partial coverage is never extrapolated"),
            "7_provenance": dict(
                requires=["source", "retrieval timestamp", "source/version identifier "
                          "where available", "raw input retained verbatim"],
                rule="the raw evidence is archived before any parsing, so a later dispute "
                     "is settled against the original bytes"),
            "8_reconstructability": dict(
                requires="the same raw event history must produce the same daily "
                         "membership table, deterministically",
                test="build the daily table twice from the archived raw evidence in "
                     "independent processes; the two tables must be byte-identical by "
                     "digest",
                rule="any nondeterminism — ordering, tie-breaking, locale, hash iteration "
                     "— is a defect in our builder, not a property of the source"),
            "9_contradictions": dict(
                rule="where independent sources conflict on an event, the event is "
                     "UNRESOLVED",
                forbidden="silent source preference, majority voting without a frozen "
                          "rule, or choosing the source that agrees with the outcome",
                required="a conflict rule frozen BEFORE conflicts are seen; if no frozen "
                         "rule covers a conflict, it stays UNRESOLVED and the affected "
                         "dates are excluded rather than guessed"),
            "10_no_survivorship_workaround": dict(
                FATAL=["backfilling the current S&P 500 constituent list over history",
                       "using price-history availability as a membership proxy",
                       "reconstructing membership from market-cap ranking",
                       "inferring membership from the vendor's data coverage"],
                rule="these are not degraded options to be flagged; a candidate relying on "
                     "any of them is REJECTED outright"),
        },

        # ── canonical schema ───────────────────────────────────────────────────
        canonical_event_table=dict(
            rationale="event-sourced and identity-aware, because ticker|start|end cannot "
                      "express a company persisting through a security change, a ticker "
                      "moving between companies, or a seat passing by corporate action",
            grain="one row per membership EVENT",
            columns=dict(
                entity_id="stable company/entity identifier, survives ticker and "
                          "share-class change",
                security_id="stable security identifier; distinct share classes are "
                            "distinct securities",
                ticker_at_time="the ticker as it stood at effective_time — descriptive, "
                               "never a join key",
                membership_effective_from="first session of membership under this event",
                membership_effective_to="last session of membership; null while open",
                event_type="one of the seven enumerated types",
                announcement_time="nullable; recorded for provenance, NEVER used for "
                                  "eligibility",
                effective_time="the authority for eligibility",
                source_record_id="identifier of the underlying source record",
                source_version="source version or snapshot identifier",
                retrieved_at="wall-clock retrieval time — the vintage")),
        derived_daily_eligibility=dict(
            grain="one row per (security_id, trading_date)",
            built_by="a pure function of the sealed event table and the sealed exchange "
                     "calendar",
            columns=["security_id", "entity_id", "trading_date", "is_constituent",
                     "ticker_at_time", "source_event_id"],
            rule="never hand-edited; regenerated from events and compared by digest"),

        # ── how candidates are ranked ──────────────────────────────────────────
        source_hierarchy=dict(
            BEST="official or institutional point-in-time constituent history carrying "
                 "effective dates and stable identifiers",
            GOOD="authoritative constituent-CHANGE event history from which membership can "
                 "be deterministically reconstructed",
            ACCEPTABLE_WITH_QUALIFICATION="multiple independent historical sources "
                                          "reconciled under a conflict rule frozen in "
                                          "advance",
            NOT_ACCEPTABLE=["a current constituent list",
                            "a current-only encyclopedia page reconstructed backwards",
                            "price existence", "market-cap ranking reconstruction",
                            "an unattributed historical CSV with no provenance"],
            note_on_secondary_use="a NOT_ACCEPTABLE source may still serve as a "
                                  "cross-check FIXTURE. Being useful for disagreement "
                                  "detection is not the same as being canonical evidence, "
                                  "and the two roles are never merged."),

        selection_criteria=dict(
            admissible=["historical correctness", "coverage", "identity resolution",
                        "effective-date precision", "reproducibility", "provenance"],
            INADMISSIBLE=["episode yield", "number of constituents produced",
                          "coverage of any particular motif, family or ticker",
                          "downstream statistical result of any kind"],
            enforcement=dict(
                rule="the canonical source is SELECTED AND SEALED before its membership "
                     "table is joined to price data, outcomes or episode counts",
                why="a prohibition on selecting for yield is unenforceable as a promise "
                    "and enforceable as an order of operations: if the join has not "
                    "happened, the choice cannot have been informed by it",
                audit="the selection artifact's seal time must precede the first join, and "
                      "both are recorded")),

        classification=dict(
            QUALIFIED="all ten requirements met and all fixtures reproduced exactly",
            QUALIFIED_WITH_LIMITATIONS="requirements met over a NAMED sub-period or a "
                                       "NAMED event-type subset, with the excluded region "
                                       "stated; the limitation propagates into the "
                                       "universe contract rather than being absorbed",
            UNRESOLVED="a requirement can neither be demonstrated nor refuted from "
                       "available evidence — the default when in doubt",
            REJECTED="any FATAL survivorship workaround, or a fixture contradicted on "
                     "effective-date semantics"),

        # ── fixtures ───────────────────────────────────────────────────────────
        fixtures=dict(
            purpose="a candidate is judged against transitions whose answers are known "
                    "independently, chosen for TYPE COVERAGE and fixed before any "
                    "candidate is examined",
            required_types=["scheduled addition", "scheduled deletion",
                            "M&A-driven replacement", "ticker change with entity "
                            "continuity", "share-class or security transition",
                            "bankruptcy or delisting removal"],
            required_facts_per_fixture=["announcement date", "effective session",
                                        "last session of old membership",
                                        "first session of new membership",
                                        "outgoing security", "incoming security",
                                        "entity continuity: preserved or broken"],
            two_stage_freeze=dict(
                stage_1_now="the fixture DESIGN is frozen by this artifact: which "
                            "transition types must be covered, and what must be known "
                            "about each",
                stage_2_before_selection="the fixture INSTANCES — specific transitions "
                                         "with specific dates — are confirmed from "
                                         "independent sources and sealed in "
                                         "SP500_PIT_FIXTURES_V1 BEFORE any candidate "
                                         "source is queried",
                why_split="stating a transition date from memory and freezing it as ground "
                          "truth would make a wrong date the standard every candidate is "
                          "judged against. The design can be frozen now because it does "
                          "not depend on any date being right; the instances cannot.",
                rule="no fixture instance is admissible until independently confirmed, and "
                     "confirmation may not come from a candidate source"),
            candidate_instances_UNCONFIRMED=dict(
                status="PROPOSED ONLY — none of these is ground truth until stage 2 "
                       "confirms it; dates here are recollected, not verified",
                proposed=[
                    dict(type="scheduled addition", note="TSLA addition, announced "
                         "2020-11, effective December 2020, replacing AIV — a large, "
                         "heavily documented single-name addition"),
                    dict(type="ticker change with entity continuity",
                         note="FB -> META, 2022, entity continuous, security continuous"),
                    dict(type="M&A-driven replacement",
                         note="TWTR removal on completion of acquisition, 2022"),
                    dict(type="share-class transition",
                         note="Alphabet GOOG/GOOGL class structure — the multi-share-class "
                              "case, where one entity holds more than one index security"),
                    dict(type="bankruptcy/delisting removal",
                         note="a Chapter 11 removal such as HTZ in 2020"),
                    dict(type="scheduled deletion",
                         note="to be chosen in stage 2, distinct from the M&A case")]),
            acceptance="a candidate must reproduce EVERY confirmed fixture exactly, "
                       "including the last-old-session / first-new-session boundary; one "
                       "boundary miss is a systematic off-by-one, not a rounding error, "
                       "and classifies the source REJECTED"),

        sequence=["freeze this artifact",
                  "seal SP500_PIT_FIXTURES_V1 with independently confirmed instances",
                  "enumerate candidate sources",
                  "preserve raw evidence per candidate",
                  "run the frozen fixtures",
                  "classify QUALIFIED / QUALIFIED_WITH_LIMITATIONS / UNRESOLVED / REJECTED",
                  "select the canonical source under the frozen ranking and seal it",
                  "build the deterministic PIT membership event table",
                  "generate daily eligibility",
                  "validate against fixtures plus random manual samples",
                  "freeze UNIVERSE_CONTRACT_V1"],
        current_step="step 1 complete on seal; nothing beyond it is authorised",
        not_authorised="this artifact names no candidate source, retrieves nothing, and "
                       "does not begin selection",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))


def main():
    p = payload()
    d = ART.seal(p, "SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1.json",
                 required=("spec_id", "status", "requirements", "canonical_event_table",
                           "source_hierarchy", "selection_criteria", "classification",
                           "fixtures"),
                 supersede=os.path.exists("SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1.json"))
    print(f"SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1 · {d} · {p['status']}")
    print(f"  requirements     : {len(p['requirements'])} · "
          f"{len(p['requirements']['10_no_survivorship_workaround']['FATAL'])} FATAL "
          f"workarounds")
    print(f"  fixture types    : {len(p['fixtures']['required_types'])} required · "
          f"instances UNCONFIRMED (stage 2)")
    print(f"  yield enforcement: select and seal BEFORE any join to price/outcome")
    print(f"  candidates named : 0")


if __name__ == "__main__":
    main()
