"""Gate 3A · HistoricalRankingPolicy v1, registered PROSPECTIVELY.

The phrasing this file exists to avoid is "preregister the current 31". It cannot be done, by
this project's own definition of exposure: the numerical qualification made 31 θ and 31
intervals available, and whether anyone opened a table is irrelevant. They are exposed. A ranking
policy frozen afterwards is frozen after the universe it would order was already seen.

    CURRENT 31        evidence exposed first, policy registered after
                      → POST_EXPOSURE_EXPLORATORY_ONLY, permanently

    NEXT SNAPSHOT     policy fixed before the evidence exists
                      → PROSPECTIVELY_REGISTERED

Exactly the shape already settled at Gate 2: qualifying the engine does not clean old evidence,
and freezing a ranking does not turn an already-seen universe into a preregistered selection.
`RetroactiveRankingRegistrationError` refuses the relabelling, and it is the same error class as
`RetroactiveEvidenceUpgradeError` because it is the same mistake one level along.

WHY θ DESCENDING AND NOTHING CLEVERER. `θ/SE`, a lower confidence bound, `θ × support`, or
"BUILD first, then θ" each blend effect size with uncertainty, sample size or the decision rule,
and then "what does rank 1 mean" has no short answer. Here it has one:

    rank 1 = the largest estimated incremental median-return effect among eligible cells

That is not "the most certain edge", and the policy says so rather than letting the column
imply it.

THE INVARIANT THAT KEEPS THIS AN EVIDENCE RANKING. Same evidence claims, same θ, a different
decision policy — and the order must not move. If it does, the ranking was a decision ranking
wearing an evidence ranking's name, and the `evidence_claim_hash` / `decision_spec_hash`
separation established earlier would be undone by the table that displays it.

RANKING IS NOT EXPOSURE. Ordering 31 cells and authorising 5 rows are different policies with
different consequences, and they stay in different objects. Showing a ranking of results already
exposed creates no new statistical exposure of those same claims — but it does create a
selection path, and that is recorded.
"""
from __future__ import annotations

import hashlib
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "HISTORICAL_RANKING_POLICY.json")
EXPOSURE_LOG = os.path.join(HERE, "EVIDENCE_EXPOSURE_LOG.json")

POLICY_VERSION = "historical_ranking_policy_v1"

POST_EXPOSURE_EXPLORATORY_ONLY = "POST_EXPOSURE_EXPLORATORY_ONLY"
PROSPECTIVELY_REGISTERED = "PROSPECTIVELY_REGISTERED"

# The whole policy. Every line is a decision that would otherwise be taken silently by whoever
# writes the ORDER BY.
POLICY = {
    "policy_version": POLICY_VERSION,
    "eligible_population": ("all OPPORTUNITY_LEVEL cells passing the frozen support "
                            "eligibility; nothing else enters the ranking universe"),
    "ranking_estimand": "incremental median return effect, theta",
    "direction": "descending",
    "verdict_affects_rank": False,
    "uncertainty_affects_rank": False,
    "support_affects_rank": "eligibility_only",
    "tie_break": "canonical cell_identity ascending",
    "top_k": ("not part of this policy — how many rows are authorised for display is an "
              "exposure policy and lives with the search run"),
    "decision_policy_invariant": ("a change of decision policy must not change the order; if it "
                                  "does, this is a decision ranking and not an evidence one"),
    "what_rank_one_means": ("the largest estimated incremental median-return effect among "
                            "eligible cells. NOT the most certain, NOT the most supported, NOT "
                            "the most likely to be an edge"),
}


class RetroactiveRankingRegistrationError(RuntimeError):
    """A ranking policy frozen after the evidence, claimed as preregistered for it."""


class RankingPolicyError(RuntimeError):
    """The ranking was asked to do something the policy does not permit."""


def policy_hash() -> str:
    return hashlib.sha256(
        json.dumps(POLICY, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


# ── what was already seen before this policy existed ────────────────────────
def evidence_exposed_before_policy() -> list:
    """Every exposure recorded before this file froze an order over it."""
    if not os.path.exists(EXPOSURE_LOG):
        return []
    with open(EXPOSURE_LOG) as f:
        return [{"exposure_id": e["exposure_id"], "cells": len(e.get("cells", [])),
                 "result_role": e["evidence_status"]["result_role"]}
                for e in json.load(f)["exposures"]]


def register_prospectively(*, registered_at: str, registered_by: str, note: str = "") -> dict:
    """Freeze the policy for evidence that does not exist yet, and say so about the evidence
    that does."""
    prior = evidence_exposed_before_policy()
    rec = {
        "policy": POLICY,
        "policy_hash": policy_hash(),
        "registered_at": registered_at,
        "registered_by": registered_by,
        "note": note,
        "registered_after_current_historical_exposure": bool(prior),
        "current_snapshot_usage": POST_EXPOSURE_EXPLORATORY_ONLY,
        "future_usage": PROSPECTIVELY_REGISTERED,
        "evidence_that_predates_this_policy": prior,
        "why": ("the qualification made 31 theta and 31 intervals available before this order "
                "was fixed. Whether anyone opened a table is irrelevant — exposure is delivery. "
                "A policy frozen afterwards cannot be preregistered with respect to a universe "
                "that was already seen."),
    }
    rec["record_hash"] = hashlib.sha256(
        json.dumps(rec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
    with open(RECORD, "w") as f:
        json.dump(rec, f, indent=1, sort_keys=True)
    return rec


def is_registered() -> bool:
    return os.path.exists(RECORD)


def record() -> dict:
    with open(RECORD) as f:
        return json.load(f)


def usage_for(snapshot_exposed_before_policy: bool) -> str:
    """Which of the two standings this ranking has, for the evidence it is about to order."""
    return (POST_EXPOSURE_EXPLORATORY_ONLY if snapshot_exposed_before_policy
            else PROSPECTIVELY_REGISTERED)


def assert_not_claimed_preregistered(usage: str, claimed: str) -> None:
    """The refusal. Same class of mistake as upgrading exposed evidence, one level along."""
    if usage == POST_EXPOSURE_EXPLORATORY_ONLY and claimed == PROSPECTIVELY_REGISTERED:
        raise RetroactiveRankingRegistrationError(
            f"this evidence was exposed before {POLICY_VERSION} was registered, so an order over "
            f"it is exploratory and cannot be labelled preregistered. Freezing a ranking does "
            f"not turn an already-seen universe into a preregistered selection — the same "
            f"refusal as qualifying an engine not re-qualifying what it already produced. The "
            f"legitimate path is to apply this policy to evidence that does not exist yet.")


# ── the ranking itself ──────────────────────────────────────────────────────
def rank(cells: dict) -> list:
    """`cells` maps cell_identity → {"theta": float}. Nothing else is read, on purpose.

    A verdict, an interval or a support count passed in here would be ignored, and that is the
    contract rather than an oversight: if any of them reached the sort key, the order would move
    when the decision policy moved.
    """
    for cid, row in cells.items():
        if "theta" not in row:
            raise RankingPolicyError(f"{cid} has no theta; the ranking estimand is theta")
    ordered = sorted(cells.items(), key=lambda kv: (-float(kv[1]["theta"]), kv[0]))
    return [{"rank": i + 1, "cell_identity": cid, "theta": float(row["theta"])}
            for i, (cid, row) in enumerate(ordered)]


def ranking_hash(ordered: list) -> str:
    return hashlib.sha256(
        "|".join(r["cell_identity"] for r in ordered).encode()).hexdigest()[:16]
