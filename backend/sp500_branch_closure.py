"""SP500_PIT_UNIVERSE_BRANCH_CLOSURE_V1 — the PIT branch is closed, and the charter's
universe contract is amended to say what the research will actually be.

WHAT CHANGED AND WHY IT IS LEGITIMATE. The charter froze the universe as historical
point-in-time S&P 500 membership and listed, among FATAL survivorship workarounds,
"backfilling the current S&P 500 constituent list over history". The programme now does
precisely that, deliberately.

That is not a reversal of the rule. The rule was never about the data — it was about the
CLAIM. Backfilling today's constituents is fatal when the result is then described as
evidence about the S&P 500 universe, because the selection is invisible in the output and
inflates everything uniformly. The same data is perfectly sound when the claim is narrowed
to match it. So the claim is narrowed, in writing, before any data exists:

    ALLOWED     historical behaviour of CURRENT S&P 500 constituents
    FORBIDDEN   historical behaviour of the S&P 500 constituent universe
    FORBIDDEN   point-in-time S&P 500 evidence

WHICH CONCLUSIONS SURVIVE THIS, AND WHICH DO NOT. Worth stating plainly, because
"survivorship bias: accepted" is easy to write and easy to forget:

    SURVIVES    how a transition forms and resolves intraday in large, currently-listed
                US companies — structure, timing, sequence, microstructure anatomy
    SURVIVES    relative comparisons WITHIN the frozen set, where every name carries the
                same selection
    DOES NOT    any statement about expected return, hit rate or strategy performance
                "historically", since every name in the set is one that survived to be
                large today
    DOES NOT    any claim about base rates, failure frequency, or how often a setup ends
                badly — the companies that ended badly are the ones missing
    DOES NOT    anything about de-listed, acquired, or shrunken companies, which are
                absent by construction rather than by measurement

The programme's stated purpose — characterising how a state transition forms and persists,
with 1m as the timing instrument — sits in the first group. Performance claims sit in the
second and are out of scope for V1.

THE PIT WORK IS CLOSED, NOT DELETED. Every artifact stays sealed and valid. If a
point-in-time universe is ever wanted, the qualification contract, the nine confirmed
fixtures, their verified snapshots and the blindness harness are all still there and still
frozen. None of it is retracted; it is simply not used by V1.
"""
from __future__ import annotations
import os, sys, time                                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

CHARTER = "MASSIVE_1M_STATE_TRANSITION_V1.json"
BRANCH = ["SP500_PIT_UNIVERSE_SOURCE_QUALIFICATION_V1.json",
          "SP500_PIT_FIXTURES_V1.json", "SP500_PIT_FIXTURES_V2.json",
          "SP500_PIT_FIXTURE_SOURCE_ARCHIVE_V1.json",
          "SP500_PIT_FIXTURE_SOURCE_ARCHIVE_V2.json",
          "SP500_PIT_CANDIDATE_INVENTORY_V1.json",
          "SP500_PIT_CANDIDATE_ACCESSIBILITY_V1.json"]


def payload():
    return dict(
        spec_id="SP500_PIT_UNIVERSE_BRANCH_CLOSURE_V1",
        status="BRANCH CLOSED · NOT USED FOR V1",
        decision="the research universe is intentionally changed to a FIXED SNAPSHOT of "
                 "CURRENT S&P 500 constituents",

        amends=dict(
            artifact=CHARTER,
            digest=ART.file_digest(CHARTER) if os.path.exists(CHARTER) else None,
            clauses=["universe_contract.definition",
                     "universe_contract.explicit",
                     "universe_contract.BLOCKING_DEPENDENCY",
                     "requirements.10_no_survivorship_workaround (of the qualification "
                     "contract), insofar as it forbade backfilling the current list"],
            original_wording="historical POINT-IN-TIME S&P 500 membership; current S&P 500 "
                             "membership is NOT the research universe",
            charter_not_rewritten="the charter remains sealed at its own digest with its "
                                  "original text; this artifact supersedes its universe "
                                  "contract and nothing else"),

        why_this_is_not_a_rule_reversal=dict(
            the_rule_was_about_the_claim="backfilling today's constituents is fatal when "
                                         "the output is then described as evidence about "
                                         "the S&P 500 universe, because the selection is "
                                         "invisible in the result",
            what_makes_it_admissible="the claim is narrowed to match the data, in writing, "
                                     "before any data exists",
            what_would_make_it_fatal_again="describing any V1 result as point-in-time "
                                           "evidence, or as a statement about the index "
                                           "universe rather than about these specific "
                                           "companies"),

        universe=dict(
            spec_id="SP500_CURRENT_SNAPSHOT_V1",
            definition="all securities that are constituents of the S&P 500 on the "
                       "snapshot date",
            historical_treatment="the frozen current constituent set is carried BACKWARD "
                                 "through the historical period",
            membership_changes_during_history="IGNORED BY DESIGN",
            survivorship_bias="KNOWN · ACCEPTED · DECLARED",
            immutability="the set is frozen with a digest and does NOT change mid-research, "
                         "even if S&P adds or removes a constituent tomorrow",
            allowed_claim="historical behaviour of CURRENT S&P 500 constituents",
            forbidden_claims=["historical behaviour of the S&P 500 constituent universe",
                              "point-in-time S&P 500 evidence",
                              "any framing in which the universe is the index rather than "
                              "this fixed list of companies"]),

        conclusion_scope=dict(
            survives=["how a transition forms and resolves intraday in large, "
                      "currently-listed US companies — structure, timing, sequence, "
                      "microstructure anatomy",
                      "relative comparisons WITHIN the frozen set, where every name "
                      "carries the same selection"],
            does_not_survive=["expected return, hit rate or strategy performance "
                              "'historically'",
                              "base rates, failure frequency, or how often a setup ends "
                              "badly — the companies that ended badly are the ones missing",
                              "anything about de-listed, acquired or shrunken companies, "
                              "absent by construction rather than by measurement"],
            programme_purpose_sits_in="survives — the charter's stated purpose is "
                                      "characterising how a transition forms and persists, "
                                      "not estimating what it pays",
            binding="performance estimation is OUT OF SCOPE for V1 and no V1 artifact may "
                    "report one"),

        branch_state=dict(
            closed=["CRSP / WRDS", "Compustat", "EODHD membership product",
                    "FMP membership product", "S&P historical press corpus reconstruction",
                    "PIT fixture evaluation", "C1-C9 qualification"],
            preserved_artifacts=[dict(artifact=a, digest=ART.file_digest(a))
                                 for a in BRANCH if os.path.exists(a)],
            preservation_rule="every PIT artifact stays sealed and VALID. Nothing is "
                              "retracted or deleted; it is simply not used by V1.",
            reusable_if_needed="the qualification contract, nine confirmed fixtures with "
                               "byte-verified snapshots, and the blindness harness remain "
                               "frozen and would be reusable without redoing the work",
            unresolved_at_closure=["the C4 circularity finding — raised, never resolved, "
                                   "and now moot for V1",
                                   "F6 boundary sessions — UNCONFIRMED, and now moot",
                                   "no candidate was ever evaluated: content rows 0, "
                                   "fixture queries 0, price joins 0"]),

        money_not_spent="no EODHD subscription was purchased; no candidate membership "
                        "product was bought or queried",

        next_sequence=["freeze today's constituent list with a raw source snapshot and a "
                       "digest (SP500_CURRENT_SNAPSHOT_V1)",
                       "resolve ticker aliases only as far as Massive requires",
                       "execute the frozen Massive probes P1-P9",
                       "download Massive 1m for the frozen securities",
                       "build deterministic 15m / 1H / 1D",
                       "run the state-transition research"],
        note_on_order="the Massive probes P1-P9 stay frozen and unexecuted, and are "
                      "unaffected by this change — they govern source semantics, not the "
                      "universe",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))


def main():
    p = payload()
    d = ART.seal(p, "SP500_PIT_UNIVERSE_BRANCH_CLOSURE_V1.json",
                 required=("spec_id", "status", "amends", "universe", "conclusion_scope",
                           "branch_state"),
                 supersede=os.path.exists("SP500_PIT_UNIVERSE_BRANCH_CLOSURE_V1.json"))
    print(f"SP500_PIT_UNIVERSE_BRANCH_CLOSURE_V1 · {d} · {p['status']}")
    print(f"  amends {p['amends']['artifact']} ({p['amends']['digest']}) universe_contract")
    print(f"  preserved PIT artifacts: {len(p['branch_state']['preserved_artifacts'])}")
    print(f"  allowed claim   : {p['universe']['allowed_claim']}")
    print(f"  forbidden       : {p['universe']['forbidden_claims'][0]}")
    print(f"  out of scope V1 : performance estimation")


if __name__ == "__main__":
    main()
