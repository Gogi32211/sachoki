"""ForwardObservationPolicy v1 · WHEN we look, frozen before there is anything to look at.

Everything statistical is frozen: hypothesis, estimand, support, bootstrap, decision rule,
ranking, evidence boundary, adapter. One freedom is left, and on its own it is enough to undo
all of them —

    run 3C every evening · watch θ, the interval and the verdict move · stop when it looks good

That is optional stopping, and no amount of preregistration upstream survives it. So the last
thing to freeze is the looking.

    FIRST LOOK    after 30 novel trading days, and for no other reason

Thirty because the frozen eligibility already needs 25 distinct dates in BOTH arms; a look before
that cannot produce anything but INSUFFICIENT_FORWARD_SUPPORT, so an earlier trigger would only
be an opportunity to peek. Any trigger that reads an outcome — "when the interval clears zero",
"when the first BUILD appears", "when five cells look attractive" — is named in the record as
forbidden, because those are the ones that will suggest themselves.

THE PART THAT REMOVES THE INCENTIVE RATHER THAN FORBIDDING THE ACT. The look evaluates exactly
the FIRST 30 novel trading days, whichever day it is actually run on. Waiting longer adds no
rows. A rule that only said "do not peek and do not delay" would rely on discipline at the one
moment discipline is most expensive; this way, delaying gains nothing, so there is nothing to
resist. It also means a late look and a punctual one produce the same artifact, which is what
makes the result checkable by someone who was not there.

BETWEEN LOOKS, NOTHING STATISTICAL IS VISIBLE. `operational_status()` answers "how many novel
trading days so far" and nothing else, and `assert_no_outcome_fields` is applied to what it
returns rather than trusted. Counting the days is not peeking; seeing a θ move is.

REPEATED LOOKS ARE FORBIDDEN IN v1. A second look needs a sequential design — alpha spending, a
stopping boundary, a stated error rate — and inventing that after the first look is choosing the
correction with the answer in view. v1 has one look. If a second is wanted, it is v2, registered
before the first look is taken.
"""
from __future__ import annotations

import hashlib
import json
import os

import numpy as np

import forward_evaluation as FE
import forward_v2_adapter as AD
import historical_ranking_policy as RP

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "FORWARD_OBSERVATION_POLICY.json")
LOOK_LEDGER = os.path.join(HERE, "FORWARD_LOOK_LEDGER.json")

POLICY_VERSION = "forward_observation_policy_v1"
FIRST_LOOK_TRADING_DAYS = 30

WAITING, READY, CONSUMED = "WAITING_FOR_NOVEL_EVIDENCE", "READY_FOR_REGISTERED_LOOK", "LOOK_TAKEN"

# what a status payload may never carry between looks
FORBIDDEN_IN_OPERATIONAL_STATUS = ("theta", "theta_hex", "interval", "interval_hex", "ci",
                                   "ci_low", "ci_high", "verdict", "ranking", "rank", "effect",
                                   "estimate", "p_value", "cells")


class ForwardObservationPolicyError(RuntimeError):
    """The observation policy is absent, or being written after it was needed."""


class PrematureLookError(RuntimeError):
    """A registered look before its outcome-independent trigger."""


class RepeatedLookError(RuntimeError):
    """A second look under a policy that registered one."""


class OutcomeLeakError(RuntimeError):
    """Something statistical reached a surface that is only allowed to count days."""


def _h(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


# ── the policy ──────────────────────────────────────────────────────────────
def build_policy(*, registered_at: str, registered_by: str, note: str = "") -> dict:
    f, a = FE.record(), AD.record()
    p = {
        "policy_version": POLICY_VERSION,
        "registered_at": registered_at,
        "registered_by": registered_by,
        "binds_to": {"forward_evaluation_spec_hash": f["spec_hash"],
                     "forward_adapter_hash": a["adapter_hash"],
                     "ranking_policy_hash": RP.policy_hash()},
        "evidence_boundary": f["data_cutoff_at_registration"],
        "first_look": {
            "rule": "after N novel trading days beyond the frozen boundary",
            "n_novel_trading_days": FIRST_LOOK_TRADING_DAYS,
            "why_this_n": ("the frozen eligibility needs 25 distinct dates in both arms, so a "
                           "look before then can only return INSUFFICIENT_FORWARD_SUPPORT and "
                           "would exist purely as an opportunity to peek"),
            "trigger_is_outcome_independent": True,
            "forbidden_triggers": ["when the interval clears zero",
                                   "when the first BUILD appears",
                                   "when N cells look attractive",
                                   "when the ranking looks stable",
                                   "any rule that reads theta, an interval or a verdict"],
            "population_at_the_look": ("exactly the first N novel trading days, whichever day the "
                                       "look is actually run on. Waiting longer adds no rows, so "
                                       "delaying the look gains nothing"),
        },
        "evaluation_population": "pure-forward only",
        "between_looks": "no theta, interval, verdict or ranking is produced or displayed",
        "repeated_looks": "FORBIDDEN_IN_V1",
        "why_repeated_looks_are_forbidden": (
            "a second look needs a sequential design — alpha spending, a stopping boundary, a "
            "stated error rate — and writing that after the first look is choosing the "
            "correction with the answer visible. A second look is v2, registered before the "
            "first look is taken"),
        "insufficient_support": {"result": FE.INSUFFICIENT_FORWARD_SUPPORT,
                                 "historical_backfill": "FORBIDDEN",
                                 "the_look_is_not_postponed_for_it": True},
        "note": note,
    }
    p["policy_hash"] = _h(p)
    return p


def freeze(policy: dict) -> dict:
    if os.path.exists(RECORD):
        existing = record()
        if existing["policy_hash"] != policy["policy_hash"]:
            raise ForwardObservationPolicyError(
                f"an observation policy is already frozen at {existing['policy_hash']} and this "
                f"one is {policy['policy_hash']}. Changing WHEN we look is the last freedom that "
                f"could undo every freeze above it.")
        return existing
    with open(RECORD, "w") as f:
        json.dump(policy, f, indent=1, sort_keys=True)
    return policy


def is_frozen() -> bool:
    return os.path.exists(RECORD)


def record() -> dict:
    if not is_frozen():
        raise ForwardObservationPolicyError(
            "no forward observation policy is frozen, so there is no registered moment to look. "
            "Deciding when to look after the outcomes exist is optional stopping with extra "
            "steps.")
    with open(RECORD) as f:
        return json.load(f)


def assert_bound() -> dict:
    p = record()
    b = p["binds_to"]
    for name, got, want in (("forward spec", b["forward_evaluation_spec_hash"],
                             FE.record()["spec_hash"]),
                            ("adapter", b["forward_adapter_hash"], AD.record()["adapter_hash"]),
                            ("ranking policy", b["ranking_policy_hash"], RP.policy_hash())):
        if got != want:
            raise ForwardObservationPolicyError(
                f"the observation policy binds to {name} {got} and the frozen one is {want}. "
                f"Four halves of one frozen path cannot be from different registrations.")
    return p


# ── novel trading days, and the window the look will use ────────────────────
def novel_trading_days(dates) -> list:
    """Distinct dates strictly after the frozen boundary, in order. No outcome is read."""
    cutoff = record()["evidence_boundary"]
    d = np.asarray([str(x) for x in dates])
    return sorted(set(d[d > cutoff].tolist()))


def look_window(dates) -> list:
    """The FIRST N novel trading days. Fixed by the trigger, not by when the look happens."""
    n = record()["first_look"]["n_novel_trading_days"]
    return novel_trading_days(dates)[:n]


def look_population(dates) -> np.ndarray:
    """Row indices inside the look window. Later dates are not in it, however long we wait."""
    window = set(look_window(dates))
    d = np.asarray([str(x) for x in dates])
    return np.flatnonzero(np.isin(d, list(window))) if window else np.array([], dtype=int)


# ── the look ledger ─────────────────────────────────────────────────────────
def _looks() -> dict:
    if not os.path.exists(LOOK_LEDGER):
        return {"policy_version": POLICY_VERSION, "looks": []}
    with open(LOOK_LEDGER) as f:
        return json.load(f)


def looks_taken() -> list:
    return _looks()["looks"]


def record_look(*, taken_at: str, artifact_hash: str, novel_days_available: int,
                look_window_end: str, note: str = "") -> dict:
    log = _looks()
    entry = {"look_index": len(log["looks"]) + 1, "taken_at": taken_at,
             "artifact_hash": artifact_hash,
             "novel_trading_days_available_at_look": novel_days_available,
             "look_window_end": look_window_end,
             "policy_hash": record()["policy_hash"], "note": note}
    log["looks"].append(entry)
    with open(LOOK_LEDGER, "w") as f:
        json.dump(log, f, indent=1, sort_keys=True)
    return entry


# ── the gate ────────────────────────────────────────────────────────────────
def assert_look_permitted(dates) -> dict:
    """The only thing that permits a first prospective evaluation to run."""
    p = assert_bound()
    if looks_taken():
        raise RepeatedLookError(
            f"look {len(looks_taken())} was already taken under {p['policy_version']}, which "
            f"registered one. A second look needs a sequential design registered BEFORE the "
            f"first, and adding one now would be choosing the correction with the answer in "
            f"view. {p['why_repeated_looks_are_forbidden']}")
    days = novel_trading_days(dates)
    need = p["first_look"]["n_novel_trading_days"]
    if len(days) < need:
        raise PrematureLookError(
            f"{len(days)} of {need} novel trading days beyond {p['evidence_boundary']}. The "
            f"trigger is outcome-independent and is not met; running early would be a look, and "
            f"looks are what this policy counts.")
    return {"novel_trading_days": len(days), "required": need,
            "look_window": (days[0], days[need - 1])}


# ── what may be shown while waiting ─────────────────────────────────────────
def assert_no_outcome_fields(payload: dict) -> None:
    """Applied to the status payload rather than trusted about it."""
    blob = json.dumps(payload, sort_keys=True).lower()
    hit = [k for k in FORBIDDEN_IN_OPERATIONAL_STATUS if f'"{k}"' in blob]
    if hit:
        raise OutcomeLeakError(
            f"the operational status carries {hit}. Between looks a person may know how many "
            f"novel trading days have accumulated and nothing else; a visible interval is a look "
            f"whatever the screen calls it.")


def operational_status(dates=None) -> dict:
    """Counts and dates. Structurally incapable of carrying an estimate."""
    p = record()
    need = p["first_look"]["n_novel_trading_days"]
    days = novel_trading_days(dates) if dates is not None else []
    taken = looks_taken()
    state = CONSUMED if taken else (READY if len(days) >= need else WAITING)
    out = {
        "policy_version": p["policy_version"], "policy_hash": p["policy_hash"],
        "state": state,
        "evidence_boundary": p["evidence_boundary"],
        "novel_trading_days": len(days),
        "novel_trading_days_required": need,
        "novel_trading_days_remaining": max(0, need - len(days)),
        "first_novel_day": days[0] if days else "",
        "latest_novel_day": days[-1] if days else "",
        "looks_taken": len(taken),
        "repeated_looks": p["repeated_looks"],
        "binds_to": p["binds_to"],
        "displayed_between_looks": ("novel trading days only — no estimate, interval, verdict or "
                                    "ranking is produced before the registered look"),
    }
    assert_no_outcome_fields(out)
    return out


# ── the run ─────────────────────────────────────────────────────────────────
def run_first_prospective_look(*, O, dates, y, masks, taken_at: str,
                               deferred_day_level=()) -> dict:
    """novel rows → frozen spec → frozen adapter → frozen ranking. Nothing left to decide."""
    gate = assert_look_permitted(dates)
    f, a = FE.record(), AD.record()
    art = AD.evaluate(O=O, dates=dates, y=y, masks=masks,
                      cutoff=f["data_cutoff_at_registration"],
                      purpose=AD.FIRST_PROSPECTIVE_EVALUATION,
                      deferred_day_level=deferred_day_level,
                      forward_spec_hash=f["spec_hash"], adapter_hash=a["adapter_hash"])
    art["observation_policy_hash"] = record()["policy_hash"]
    art["look_index"] = 1
    art["novel_trading_days_available_at_look"] = gate["novel_trading_days"]
    record_look(taken_at=taken_at, artifact_hash=art["artifact_hash"],
                novel_days_available=gate["novel_trading_days"],
                look_window_end=gate["look_window"][1],
                note="first registered prospective look")
    return art


if __name__ == "__main__":
    import sys                                                        # noqa: PLC0415
    at = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
    p = freeze(build_policy(registered_at=at, registered_by="forward-observation-freeze",
                            note="frozen before any novel observation exists; one look, "
                                 "outcome-independent trigger, fixed population"))
    print("=" * 100, flush=True)
    print(f"  FORWARD OBSERVATION POLICY {p['policy_hash']} · {p['policy_version']}", flush=True)
    print("=" * 100, flush=True)
    print(f"  evidence boundary    {p['evidence_boundary']}", flush=True)
    print(f"  first look           after {p['first_look']['n_novel_trading_days']} novel trading "
          f"days", flush=True)
    print(f"  trigger              outcome-independent", flush=True)
    print(f"  population at look   {p['first_look']['population_at_the_look']}", flush=True)
    print(f"  between looks        {p['between_looks']}", flush=True)
    print(f"  repeated looks       {p['repeated_looks']}", flush=True)
    print(f"  binds to             spec {p['binds_to']['forward_evaluation_spec_hash']} · "
          f"adapter {p['binds_to']['forward_adapter_hash']} · "
          f"ranking {p['binds_to']['ranking_policy_hash']}", flush=True)
    print("\n  forbidden triggers:", flush=True)
    for t in p["first_look"]["forbidden_triggers"]:
        print(f"    · {t}", flush=True)
    s = operational_status()
    print(f"\n  STATE  {s['state']} · {s['novel_trading_days']}/{s['novel_trading_days_required']}"
          f" novel trading days", flush=True)
