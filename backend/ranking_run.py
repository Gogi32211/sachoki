"""Gate 3B · ordering evidence that is already exposed, without pretending anything new happened.

The input is the existing immutable artifact, not a fresh execution:

    run_v2(HISTORICAL_RESEARCH)          ✗  not needed, and not harmless
    exposed qualification artifact  →  HistoricalRankingPolicy v1  →  RankingRunArtifact

Re-running the estimator to obtain numbers that are already on disk would be a rerun after Gate 2
of evidence exposed before it, which is exactly the laundering `evidence_fingerprint` refuses.
The cheapest way not to launder is not to re-execute.

WHAT CHANGES AND WHAT DOES NOT. The 31 results keep `result_role = ENGINE_QUALIFICATION_EVIDENCE`
permanently. Ranking does not touch their role; it adds a USE of them, recorded separately:

    evidence role      unchanged, immutable, in EVIDENCE_EXPOSURE_LOG.json
    selection use      new, in this artifact and in the trial log

TWO NUMBERS THAT MUST NOT MERGE.

    new claim exposure      0     every (evidence_claim, decision_spec) pair was already exposed
    new selection operation 1     a person now has an ORDER over them

`k_exposed` does not move, because sorting a list a researcher could already read gives them no
statistical claim they did not have. The selection path does move, because a person choosing the
top row of a list sorted by effect is selecting on the outcome. Charging the first would
double-count; charging nothing would make the second free.

AND IT IS NOT FREE. Try θ descending, then a lower bound, then BUILD-first, then θ/SE, on the same
31, and the claims really are old while the search is new — a search over ranking policies. So
every distinct policy applied to a given evidence set appends a trial, and the count is reported
next to the ranking rather than kept somewhere a reader has to go looking for.

THE CEILING IS INHERITED, NOT DECLARED. A ranking cannot grant what the evidence under it does
not have, so the permitted actions start from the input's own `EvidenceStatus.ceiling()` and can
only lose members. `promote_as_validated_edge` and `book` are absent from an exploratory ranking
not because ranking forbids them, but because ENGINE_QUALIFICATION_EVIDENCE never had them —
which is the same ceiling-not-grant rule one level up, and the reason it survives a future
evidence type nobody has thought of yet.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field

import evidence_fingerprint as FP
import evidence_status as ES
import historical_ranking_policy as RP

HERE = os.path.dirname(os.path.abspath(__file__))
TRIAL_LOG = os.path.join(HERE, "RANKING_SELECTION_PATH.json")

POST_EXPOSURE_EXPLORATORY = "POST_EXPOSURE_EXPLORATORY"
PROSPECTIVE = "PROSPECTIVE"

RECORD_EXPLORATORY_RANKING = "record_exploratory_ranking"
LABEL_PREREGISTERED_SELECTION = "label_preregistered_selection"
RETROACTIVE_REGISTER = "retroactive_register"

# never reachable from a ranking over evidence that was exposed first, under any status
NEVER_FROM_AN_EXPLORATORY_RANKING = frozenset({LABEL_PREREGISTERED_SELECTION,
                                               RETROACTIVE_REGISTER})

SELECTION_PATH_EFFECT = "SELECTION_PATH_CHANGE"


class RankingInputError(RuntimeError):
    """The evidence handed to the ranking is not what the declared use requires."""


class RankingActionError(RuntimeError):
    """An action above what this ranking, over this evidence, can support."""


def _h(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


# ── the action ceiling ──────────────────────────────────────────────────────
def ceiling(input_status: dict, ranking_use: str) -> set:
    """Inherited from the evidence, then narrowed by the use. Never widened by either."""
    base = ES.EvidenceStatus(
        evidence_origin=input_status["evidence_origin"],
        instrument_validation_basis=input_status["instrument_validation_basis"],
        application_maturity=input_status["application_maturity"],
        result_role=input_status["result_role"]).ceiling()
    if ranking_use == POST_EXPOSURE_EXPLORATORY:
        return (base | {RECORD_EXPLORATORY_RANKING}) - NEVER_FROM_AN_EXPLORATORY_RANKING
    return (base | {RECORD_EXPLORATORY_RANKING, LABEL_PREREGISTERED_SELECTION}
            ) - {RETROACTIVE_REGISTER}


# ── the artifact ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RankingRunArtifact:
    ranking_policy_hash: str
    ranking_policy_version: str
    input_exposure_ids: tuple
    input_evidence_fingerprint: str
    input_evidence_status: dict
    input_evidence_standing: str          # ALREADY_EXPOSED / NOVEL
    ranking_use: str
    selection_path_effect: str
    ranking_metric: str
    decision_policy_invariant: bool
    rows: tuple
    ranking_hash: str
    claim_exposure_delta: int = 0
    trial_index: int = 0
    recorded_at: str = ""
    display_banner: tuple = ()

    def as_dict(self) -> dict:
        return {"artifact": "RankingRunArtifact",
                "ranking_policy_hash": self.ranking_policy_hash,
                "ranking_policy_version": self.ranking_policy_version,
                "input_exposure_ids": list(self.input_exposure_ids),
                "input_evidence_fingerprint": self.input_evidence_fingerprint,
                "input_evidence_status": self.input_evidence_status,
                "input_evidence_standing": self.input_evidence_standing,
                "ranking_use": self.ranking_use,
                "selection_path_effect": self.selection_path_effect,
                "ranking_metric": self.ranking_metric,
                "decision_policy_invariant": self.decision_policy_invariant,
                "claim_exposure_delta": self.claim_exposure_delta,
                "trial_index": self.trial_index,
                "recorded_at": self.recorded_at,
                "display_banner": list(self.display_banner),
                "permitted_actions": sorted(ceiling(self.input_evidence_status,
                                                    self.ranking_use)),
                "refused_actions": sorted(NEVER_FROM_AN_EXPLORATORY_RANKING
                                          | {ES.PROMOTE_AS_VALIDATED_EDGE, ES.BOOK}
                                          - ceiling(self.input_evidence_status,
                                                    self.ranking_use)),
                "ranking_hash": self.ranking_hash,
                "rows": list(self.rows)}

    def assert_permits(self, action: str) -> None:
        allowed = ceiling(self.input_evidence_status, self.ranking_use)
        if action in allowed:
            return
        raise RankingActionError(
            f"{action!r} is above what a {self.ranking_use} ranking over "
            f"{self.input_evidence_status['result_role']} can support. An order over results is "
            f"not a stronger fact about them; it inherits their ceiling and narrows it. "
            f"At most, here: {', '.join(sorted(allowed))}.")


# ── the trial log · a search over ranking policies is still a search ────────
def _load_trials() -> dict:
    if not os.path.exists(TRIAL_LOG):
        return {"policy": "every distinct ranking policy applied to a given evidence set is a "
                          "selection-path trial, whether or not the claims under it are old",
                "trials": []}
    with open(TRIAL_LOG) as f:
        return json.load(f)


def record_trial(*, ranking_policy_hash: str, ranking_metric: str, evidence_fingerprint: str,
                 ranking_hash: str, recorded_at: str, note: str = "") -> dict:
    """Idempotent per (policy, evidence). A second METRIC on the same evidence is a new trial."""
    log = _load_trials()
    for t in log["trials"]:
        if (t["ranking_policy_hash"] == ranking_policy_hash
                and t["evidence_fingerprint"] == evidence_fingerprint):
            return t
    trial = {"trial_index": len(log["trials"]) + 1,
             "ranking_policy_hash": ranking_policy_hash, "ranking_metric": ranking_metric,
             "evidence_fingerprint": evidence_fingerprint, "ranking_hash": ranking_hash,
             "recorded_at": recorded_at, "note": note}
    log["trials"].append(trial)
    with open(TRIAL_LOG, "w") as f:
        json.dump(log, f, indent=1, sort_keys=True)
    return trial


def selection_path_accounting(evidence_fingerprint: str = "") -> dict:
    log = _load_trials()
    trials = [t for t in log["trials"]
              if not evidence_fingerprint or t["evidence_fingerprint"] == evidence_fingerprint]
    return {"new_claim_exposure": 0,
            "selection_path_operations": len(trials),
            "distinct_ranking_policies_tried": len({t["ranking_policy_hash"] for t in trials}),
            "ranking_metrics_tried": sorted({t["ranking_metric"] for t in trials}),
            "why_zero_and_not_zero": ("no (evidence_claim, decision_spec) pair became available "
                                      "that was not already available, so k_exposed does not "
                                      "move. An order over them is a new selection operation, "
                                      "and trying a second order is a search over orders")}


# ── the run ─────────────────────────────────────────────────────────────────
EXPLORATORY_BANNER = ("EXPLORATORY RANKING",
                      "Policy registered after evidence exposure",
                      "Not preregistered for this snapshot")
PROSPECTIVE_BANNER = ("PROSPECTIVE RANKING",
                      "Policy registered before this evidence existed")


def rank_exposed_evidence(*, fingerprint: FP.EvidenceFingerprint, theta: dict,
                          exposure_ids, evidence_status: dict, recorded_at: str,
                          declared_use: str = POST_EXPOSURE_EXPLORATORY,
                          registry: FP.FingerprintRegistry | None = None,
                          note: str = "") -> RankingRunArtifact:
    """Order evidence already on record. The input must BE on record — that is the point."""
    reg = registry if registry is not None else FP.FingerprintRegistry.load()
    verdict = reg.classify(fingerprint)
    already_exposed = verdict["classification"] != FP.NOVEL_EVIDENCE

    if declared_use == POST_EXPOSURE_EXPLORATORY and not already_exposed:
        raise RankingInputError(
            "this ranking declares itself exploratory over exposed evidence, and the evidence it "
            "was handed is not on the exposure register. Either it is novel — in which case the "
            "policy applies prospectively and this is Gate 3C — or it was produced by a fresh "
            "execution, in which case the fresh execution is the problem.")

    # the refusal the whole gate exists for, delegated to where it was already written
    RP.assert_not_claimed_preregistered(
        RP.usage_for(already_exposed),
        RP.PROSPECTIVELY_REGISTERED if declared_use == PROSPECTIVE
        else RP.POST_EXPOSURE_EXPLORATORY_ONLY)

    if declared_use == PROSPECTIVE and already_exposed:
        raise RankingInputError(
            f"declared PROSPECTIVE over evidence classified {verdict['classification']}: "
            f"{verdict['why']}")

    ordered = RP.rank({c: {"theta": t} for c, t in theta.items()})
    rhash = RP.ranking_hash(ordered)
    trial = record_trial(ranking_policy_hash=RP.policy_hash(),
                         ranking_metric=f"{RP.POLICY['ranking_estimand']} {RP.POLICY['direction']}",
                         evidence_fingerprint=fingerprint.fingerprint(), ranking_hash=rhash,
                         recorded_at=recorded_at, note=note)

    return RankingRunArtifact(
        ranking_policy_hash=RP.policy_hash(), ranking_policy_version=RP.POLICY_VERSION,
        input_exposure_ids=tuple(exposure_ids),
        input_evidence_fingerprint=fingerprint.fingerprint(),
        input_evidence_status=dict(evidence_status),
        input_evidence_standing="ALREADY_EXPOSED" if already_exposed else "NOVEL",
        ranking_use=declared_use, selection_path_effect=SELECTION_PATH_EFFECT,
        ranking_metric=f"{RP.POLICY['ranking_estimand']} {RP.POLICY['direction']}",
        decision_policy_invariant=True, rows=tuple(ordered), ranking_hash=rhash,
        claim_exposure_delta=0, trial_index=trial["trial_index"], recorded_at=recorded_at,
        display_banner=(EXPLORATORY_BANNER if declared_use == POST_EXPOSURE_EXPLORATORY
                        else PROSPECTIVE_BANNER))


# ── 3B · the run itself ─────────────────────────────────────────────────────
RUN_3B = os.path.join(HERE, "RANKING_RUN_3B.json")


def run_3b(*, recorded_at: str) -> dict:
    """Order the 31 already-exposed cells. Reads two immutable artifacts; executes nothing."""
    import exposed_evidence as EE                                     # noqa: PLC0415
    reg = FP.FingerprintRegistry.load()
    if not reg.entries:
        raise RankingInputError(
            "no evidence fingerprint is registered, so there is nothing on record to rank. "
            "Deriving one is `exposed_evidence.py`; producing one by re-executing is the "
            "laundering path.")
    fp = FP.EvidenceFingerprint.from_dict(reg.entries[0])
    art = rank_exposed_evidence(
        fingerprint=fp, theta=EE.exposed_theta(), exposure_ids=EE.exposure_ids(),
        evidence_status=reg.entries[0]["evidence_status"], recorded_at=recorded_at,
        note="Gate 3B — theta descending over the 31 cells exposed by the oracle and the "
             "numerical qualification")
    d = art.as_dict()
    intervals = EE.exposed_intervals()
    for row in d["rows"]:
        lo, hi = intervals.get(row["cell_identity"], (None, None))
        # carried for display only; neither reached the sort key, and t6 proves it
        row["interval_display_only"] = [lo, hi]
    d["selection_path_accounting"] = selection_path_accounting(fp.fingerprint())
    d["what_this_is_not"] = [
        "not a preregistered selection — the policy was registered after these 31 were exposed",
        "not new evidence — no claim became available that was not already available",
        "not a promotion — the 31 keep result_role ENGINE_QUALIFICATION_EVIDENCE permanently",
        "not a ranking by certainty — the interval is displayed and never sorted on",
    ]
    with open(RUN_3B, "w") as f:
        json.dump(d, f, indent=1, sort_keys=True)
    return d


if __name__ == "__main__":
    import sys                                                        # noqa: PLC0415
    at = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
    out = run_3b(recorded_at=at)
    print("=" * 100, flush=True)
    for line in out["display_banner"]:
        print(f"  {line}", flush=True)
    print("=" * 100, flush=True)
    print(f"  policy {out['ranking_policy_hash']} · metric {out['ranking_metric']}", flush=True)
    print(f"  evidence {out['input_evidence_fingerprint']} · {out['input_evidence_standing']} · "
          f"{', '.join(out['input_exposure_ids'])}", flush=True)
    print(f"  role     {out['input_evidence_status']['result_role']} · "
          f"{out['input_evidence_status']['application_maturity']}", flush=True)
    acc = out["selection_path_accounting"]
    print(f"  k_exposed delta {out['claim_exposure_delta']} · selection-path operations "
          f"{acc['selection_path_operations']} · ranking policies tried "
          f"{acc['distinct_ranking_policies_tried']}", flush=True)
    print(f"\n  {'rank':>4}  {'cell':<38s}{'theta':>9s}   {'95% interval (display only)':<26s}",
          flush=True)
    for r in out["rows"]:
        lo, hi = r["interval_display_only"]
        span = f"[{lo:+.2f}, {hi:+.2f}]" if lo is not None else ""
        print(f"  {r['rank']:>4}  {r['cell_identity']:<38s}{r['theta']:>+9.3f}   {span:<26s}",
              flush=True)
    print(f"\n  ranking hash {out['ranking_hash']} · written to {os.path.basename(RUN_3B)}",
          flush=True)
    for line in out["what_this_is_not"]:
        print(f"  · {line}", flush=True)
