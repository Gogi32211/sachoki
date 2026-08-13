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
import research_store as RS

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "FORWARD_OBSERVATION_POLICY.json")
# The look ledger is durable, checksummed and hash-chained — the same store the rest of the
# governance uses — and it is witnessed by a second file that must agree with it.
LOOK_LEDGER = os.path.join(HERE, "FORWARD_LOOK_LEDGER.jsonl")
LOOK_WITNESS = os.path.join(HERE, "FORWARD_LOOK_WITNESS.json")

LOOK_SESSION = "forward-observation-v1"
LOOK_TAKEN_EVENT = "FORWARD_LOOK_TAKEN"

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


class LookLedgerTamperError(RuntimeError):
    """The look record does not verify, or its two witnesses disagree."""


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


# ── the look ledger · durable, chained, and witnessed twice ─────────────────
# A plain JSON list was enough to record a look and not enough to make one irreversible: delete
# the file and the system reads "no look has been taken" and looks again. That is the same
# failure as a torn ledger elsewhere in this project, so it gets the same store — checksummed
# per line, hash-chained, CORRUPT on damage before the end.
#
# The chain alone still cannot see its own deletion, so the look is witnessed by a SECOND file
# and the two must agree. Any surviving witness blocks a second look:
#
#     ledger 1 · witness 1     the look happened
#     ledger 1 · witness 0     tampering, or a crash mid-record → REFUSE
#     ledger 0 · witness 1     tampering → REFUSE
#     ledger CORRUPT           REFUSE — absence cannot be proven from damage
#     ledger 0 · witness 0     no look has been taken
#
# The write order is ledger first, then witness, so an interrupted record leaves ledger > witness
# — the state that REFUSES. A crash can therefore wedge the system into needing a human, and that
# is the correct direction to fail: a wedged system asks, a permissive one looks twice.
#
# What this does NOT claim: a coordinated deletion of every witness is indistinguishable from
# never having looked. No local file can prove its own absence was not chosen. The out-of-band
# record is git, where the ledger, the witness and the artifact are committed together.


def _ledger() -> RS.DurableLedger:
    return RS.DurableLedger(LOOK_LEDGER)


def _chain_state(n_looks: int, last_artifact: str) -> str:
    return hashlib.sha256(f"{LOOK_SESSION}|{n_looks}|{last_artifact}".encode()).hexdigest()[:16]


def _witness() -> dict:
    """An unreadable witness is DAMAGED, not absent.

    Found by the crash test: `open(path, "w")` truncates before `json.dump` writes, so an
    interrupted witness write leaves a zero-byte file. Parsing it raised, which is the one
    outcome worse than either answer — a raw JSONDecodeError from a governance gate says nothing
    about whether a look happened.
    """
    if not os.path.exists(LOOK_WITNESS):
        return {"looks": 0, "last_artifact_hash": "", "chain_head": "", "status": "ABSENT"}
    try:
        with open(LOOK_WITNESS) as f:
            w = json.load(f)
    except Exception:                                                # noqa: BLE001
        return {"looks": -1, "last_artifact_hash": "", "chain_head": "", "status": "DAMAGED"}
    w["status"] = "PRESENT"
    return w


def _write_witness(payload: dict) -> None:
    """Atomically. A torn witness would be indistinguishable from a missing one."""
    tmp = LOOK_WITNESS + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=1, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, LOOK_WITNESS)


def ledger_health() -> dict:
    """Everything that must line up before the gate will answer at all."""
    w = _witness()
    if not os.path.exists(LOOK_LEDGER):
        return {"ledger": "ABSENT", "ledger_looks": 0, "witness_looks": w["looks"],
                "witness": w["status"], "agree": w["looks"] == 0 and w["status"] != "DAMAGED",
                "chain_head": ""}
    status, events = _ledger().status()
    looks = [e for e in events if e.event_type == LOOK_TAKEN_EVENT]
    head = looks[-1].new_state_hash if looks else ""
    return {"ledger": status, "ledger_looks": len(looks), "witness_looks": w["looks"],
            "witness": w["status"],
            "agree": (status != RS.CORRUPT and w["status"] != "DAMAGED"
                      and len(looks) == w["looks"] and head == w.get("chain_head", "")),
            "chain_head": head}


def assert_look_record_verifies() -> dict:
    h = ledger_health()
    if h["ledger"] == RS.CORRUPT:
        raise LookLedgerTamperError(
            f"{os.path.basename(LOOK_LEDGER)} does not verify. Whether a look has already been "
            f"taken cannot be established from damaged history, and the permissive reading — "
            f"'probably none' — is exactly the one that would produce a second look.")
    if not h["agree"]:
        raise LookLedgerTamperError(
            f"the look ledger records {h['ledger_looks']} look(s) and the witness records "
            f"{h['witness_looks']}. Either one was removed or a record was interrupted between "
            f"the two writes. This refuses rather than picking the smaller number, because the "
            f"smaller number is always the one that permits another look.")
    return h


def looks_taken() -> list:
    """The recorded looks, after both witnesses have agreed."""
    assert_look_record_verifies()
    if not os.path.exists(LOOK_LEDGER):
        return []
    return [{"look_index": e.event_id + 1, **e.payload}
            for e in _ledger().read_all() if e.event_type == LOOK_TAKEN_EVENT]


def record_look(*, taken_at: str, artifact_hash: str, novel_days_available: int,
                look_window_end: str, note: str = "") -> dict:
    """Ledger first, then the witness. The gap between them fails closed."""
    h = assert_look_record_verifies()
    n = h["ledger_looks"]
    payload = {"taken_at": taken_at, "artifact_hash": artifact_hash,
               "novel_trading_days_available_at_look": novel_days_available,
               "look_window_end": look_window_end, "policy_hash": record()["policy_hash"],
               "note": note}
    ev = _ledger().append(
        LOOK_SESSION, LOOK_SESSION, LOOK_TAKEN_EVENT, event_id=n,
        prior_state_hash=h["chain_head"], new_state_hash=_chain_state(n + 1, artifact_hash),
        payload=payload, state="LOOK_TAKEN", code_hash=record()["policy_hash"],
        idempotency_key=f"{LOOK_SESSION}:look:{n}",
        request_hash=RS.request_hash(LOOK_SESSION, "look", payload))
    _write_witness({"looks": n + 1, "last_artifact_hash": artifact_hash,
                    "chain_head": ev.new_state_hash, "taken_at": taken_at,
                    "policy_hash": record()["policy_hash"],
                    "why": "a second witness, so that deleting the ledger is detected rather "
                           "than read as 'no look has been taken'"})
    return {"look_index": n + 1, **payload}


# ── the gate ────────────────────────────────────────────────────────────────
def assert_look_permitted(dates) -> dict:
    """The only thing that permits a first prospective evaluation to run."""
    p = assert_bound()
    assert_look_record_verifies()          # a damaged or half-deleted record refuses outright
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
    h = ledger_health()
    try:
        taken = looks_taken()
    except LookLedgerTamperError:
        # the counter still counts days; it does not get to claim the look is available
        taken = [{"look_index": "UNVERIFIED"}] * max(h["ledger_looks"], h["witness_looks"])
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
        "look_record_integrity": "OK" if h["agree"] else "DISAGREES",
        "look_record_status": h["ledger"],
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
