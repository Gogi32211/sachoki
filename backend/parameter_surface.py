"""Twenty knobs, five behaviours. The role decides, not the control.

The vertical slice wired two parameters by hand, and hand-wiring eighteen more is how twenty
different UI controls become twenty different ways around one ledger. Each one would grow its
own little path — this one updates the hash, that one forgot to, the third calls preview but
commits something else — and every path is a place where the accounting can quietly differ from
what the screen did.

So a parameter has ONE canonical record and behaviour is derived from it:

    parameter_id · semantic_role
    affects_claim_identity · affects_search_space · affects_decision_policy
    allowed_in_explore · allowed_after_register
    canonicalizer

and the surface computes three hashes from the values:

    claim_hash            everything that can change the answer
    search_space_hash     what the algorithm was permitted to choose among
    decision_policy_hash  the rules by which a winner is called a winner

THE ACCEPTANCE TEST IS ABOUT SAMENESS. Two knobs with the same role must behave identically with
no per-parameter code. If `horizon` and `universe` need different handling to both count as
CLAIM_CHANGE, then the role is decoration and the real behaviour lives in whichever branch
someone wrote last.

A UI NUMBER SAYS NOTHING ABOUT ITS ROLE. Showing 5 rows instead of 10 out of the same ranked 31
is presentation. Letting the algorithm choose among 37 instead of 31 changes the multiplicity a
verdict must survive. Both are "a number went up" on screen, so the registry carries them as two
different parameters — `displayed_top_k` and `selection_top_k` — rather than one control whose
meaning depends on which code path happens to read it.

CANONICALIZATION IS PART OF IDENTITY. `"20"`, `" 20"` and `20` are the same horizon, and if they
hash differently then reopening a specification invents a new claim and `k` drifts upward on
whitespace. The canonicalizer belongs in the registry beside the role, because both answer the
same question: what makes two settings the same setting.

ONE CLASSIFIER, TWO CALLERS. `preview()` and `apply()` do not each decide what a change means;
`classify()` decides, and both call it. Preview that disagrees with commit is worse than no
preview, because the screen would then be showing a promise the ledger does not keep.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace

from research_session import (CLAIM_CHANGE, DESIGN_CHANGE, PARAMETERS,  # noqa: E402
                              POLICY_CHANGE, PRESENTATION_ONLY, SEARCH_SPACE_CHANGE,
                              classify_change)

# ── what a role does, stated once ───────────────────────────────────────────
#
# The table IS the implementation. A component asks what its knob costs and gets an answer from
# here; nothing downstream re-decides it.
ROLE_EFFECTS = {
    PRESENTATION_ONLY: {
        "multiplicity_effect": "NONE",
        "changes": (),
        "registered_effect": "ALLOW",
        "note": "a view of a result that already exists; the research object is untouched",
    },
    CLAIM_CHANGE: {
        "multiplicity_effect": "NEW_SELECTABLE_CLAIM",
        "changes": ("claim_hash",),
        "registered_effect": "REJECT",
        "note": "a different question. It is counted, and a frozen study cannot ask it",
    },
    DESIGN_CHANGE: {
        "multiplicity_effect": "NEW_STATISTICAL_OBJECT",
        "changes": ("claim_hash",),
        "registered_effect": "REJECT",
        "note": "the estimand or its support changes, so the previous answer is not comparable",
    },
    SEARCH_SPACE_CHANGE: {
        "multiplicity_effect": "SEARCH_SPACE_CHANGED",
        "changes": ("search_space_hash",),
        "registered_effect": "REJECT",
        "note": "the algorithm may now choose from a different set; multiplicity moves with it",
    },
    POLICY_CHANGE: {
        "multiplicity_effect": "DECISION_POLICY_CHANGED",
        "changes": ("decision_policy_hash",),
        "registered_effect": "REJECT",
        "note": "the rule that turns a number into a verdict; a frozen study declared one",
    },
}


class ParameterSurfaceError(RuntimeError):
    """A parameter used in a way its declared role does not permit."""


# ── canonicalizers ──────────────────────────────────────────────────────────
def canon_str(v) -> str:
    return str(v).strip()


def canon_int(v) -> str:
    """`"20"`, `" 20 "` and `20` are one setting. Anything unparseable stays itself, visibly."""
    try:
        return str(int(str(v).strip()))
    except (TypeError, ValueError):
        return canon_str(v)


def canon_float(v) -> str:
    try:
        f = float(str(v).strip())
    except (TypeError, ValueError):
        return canon_str(v)
    return f"{f:.6g}"


def canon_set(v) -> str:
    """A selection of things is unordered; the order it was clicked in is not part of it."""
    if isinstance(v, str):
        parts = [p.strip() for p in v.split(",")]
    else:
        parts = [canon_str(p) for p in v]
    return ",".join(sorted(p for p in parts if p))


def canon_lower(v) -> str:
    return canon_str(v).lower()


CANONICALIZERS = {
    "horizon": canon_int, "top_k": canon_int, "selection_top_k": canon_int,
    "displayed_top_k": canon_int, "support_cutoff": canon_int,
    "conditioning_tolerance": canon_float, "equivalence_margin": canon_float,
    "setup_subset": canon_set, "universe": canon_lower, "layout": canon_lower,
    "theme": canon_lower, "direction": canon_lower, "null_family": canon_lower,
    "column_order": canon_set,
}


# ── the canonical record ────────────────────────────────────────────────────
@dataclass(frozen=True)
class ParameterRecord:
    parameter_id: str
    semantic_role: str
    affects_claim_identity: bool
    affects_search_space: bool
    affects_decision_policy: bool
    allowed_in_explore: bool
    allowed_after_register: bool
    canonicalizer: object = field(default=canon_str, repr=False)

    def canonical(self, value) -> str:
        return self.canonicalizer(value)

    @property
    def effects(self) -> dict:
        return ROLE_EFFECTS[self.semantic_role]


def _record(d: ParameterDefinition) -> ParameterRecord:
    """One place where a declaration becomes a record. Everything else reads the record."""
    return ParameterRecord(
        parameter_id=d.parameter_id, semantic_role=d.semantic_role,
        affects_claim_identity=d.affects_claim_identity,
        affects_search_space=d.affects_search_space,
        affects_decision_policy=d.affects_decision_policy,
        # Exploration may turn anything. A registered study may turn only what cannot change the
        # answer — which is exactly the PRESENTATION_ONLY set, so this is derived from the role
        # rather than typed out per parameter and able to disagree with it.
        allowed_in_explore=True,
        allowed_after_register=(d.semantic_role == PRESENTATION_ONLY),
        canonicalizer=CANONICALIZERS.get(d.parameter_id, canon_str))


# ONE registry, in `research_session`. This module derives records from it and adds none of its
# own: a parameter declared here and not there would be turnable through the surface and refused
# by the ledger, which is two sources of truth about the same knob — the exact defect this
# milestone exists to prevent, arriving through the file meant to prevent it.
REGISTRY: dict = {pid: _record(d) for pid, d in PARAMETERS.items()}


def registry_has_one_home() -> bool:
    return set(REGISTRY) == set(PARAMETERS)


def record(parameter_id: str) -> ParameterRecord:
    if parameter_id not in REGISTRY:
        classify_change(parameter_id)          # raises with the hidden-degree-of-freedom message
        raise ParameterSurfaceError(parameter_id)
    return REGISTRY[parameter_id]


def by_role(role: str) -> tuple:
    return tuple(sorted(p for p, r in REGISTRY.items() if r.semantic_role == role))


# ── the surface ─────────────────────────────────────────────────────────────
def _hash(d: dict) -> str:
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()[:16]


@dataclass(frozen=True)
class ParameterSurface:
    """All twenty values, and the three hashes they determine."""
    values: dict = field(default_factory=dict)

    @classmethod
    def initial(cls, **overrides) -> "ParameterSurface":
        vals = {pid: "" for pid in REGISTRY}
        vals.update(overrides)
        return cls(values={pid: record(pid).canonical(v) for pid, v in vals.items()})

    def _slice(self, attr: str) -> dict:
        return {pid: self.values.get(pid, "") for pid, r in REGISTRY.items()
                if getattr(r, attr)}

    @property
    def claim_hash(self) -> str:
        return _hash(self._slice("affects_claim_identity"))

    @property
    def search_space_hash(self) -> str:
        return _hash(self._slice("affects_search_space"))

    @property
    def decision_policy_hash(self) -> str:
        return _hash(self._slice("affects_decision_policy"))

    @property
    def hashes(self) -> dict:
        return {"claim_hash": self.claim_hash, "search_space_hash": self.search_space_hash,
                "decision_policy_hash": self.decision_policy_hash}

    def with_value(self, parameter_id: str, value) -> "ParameterSurface":
        r = record(parameter_id)
        vals = dict(self.values)
        vals[parameter_id] = r.canonical(value)
        return replace(self, values=vals)


# ── the one classifier both callers use ─────────────────────────────────────
def classify(surface: ParameterSurface, parameter_id: str, new_value, *,
             state: str = "EXPLORE") -> dict:
    """What this change means, decided once.

    `preview` returns this and `apply` acts on it. They cannot disagree, because there is only
    one of them — a preview that disagreed with the commit would be a promise the screen makes
    and the ledger does not keep.
    """
    r = record(parameter_id)
    after = surface.with_value(parameter_id, new_value)
    before_h, after_h = surface.hashes, after.hashes
    moved = tuple(k for k in before_h if before_h[k] != after_h[k])

    registered = state in ("REGISTERED", "ACTIVE_REGISTERED")
    permitted = r.allowed_after_register if registered else r.allowed_in_explore

    # A change whose value did not actually change is a no-op whatever its role: reselecting the
    # horizon that is already set is not a new claim, and charging for it would let k grow by
    # clicking the current setting.
    identical = surface.values.get(parameter_id, "") == r.canonical(new_value)

    return {
        "parameter_id": parameter_id,
        "role": r.semantic_role,
        "old_value": surface.values.get(parameter_id, ""),
        "new_value": r.canonical(new_value),
        "no_op": identical,
        "old_claim_hash": before_h["claim_hash"], "new_claim_hash": after_h["claim_hash"],
        "old_search_space_hash": before_h["search_space_hash"],
        "new_search_space_hash": after_h["search_space_hash"],
        "old_decision_policy_hash": before_h["decision_policy_hash"],
        "new_decision_policy_hash": after_h["decision_policy_hash"],
        "hashes_moved": moved,
        "multiplicity_effect": "NONE" if identical else r.effects["multiplicity_effect"],
        "registered_effect": "ALLOW" if permitted else "REJECT",
        "permitted": permitted,
        "note": r.effects["note"],
    }


def apply(surface: ParameterSurface, parameter_id: str, new_value, *,
          state: str = "EXPLORE") -> tuple:
    """(new_surface, classification). Refuses exactly what `classify` said it would refuse."""
    c = classify(surface, parameter_id, new_value, state=state)
    if not c["permitted"]:
        raise ParameterSurfaceError(
            f"{parameter_id} is {c['role']} and this session is {state}. "
            f"{c['note']}. A frozen study does not mutate; fork it to continue from here.")
    return surface.with_value(parameter_id, new_value), c


def declared_effects_are_consistent() -> list:
    """Every place a role could disagree with itself. Empty is the only acceptable answer."""
    problems = []
    for pid, r in REGISTRY.items():
        e = r.effects
        if ("claim_hash" in e["changes"]) != r.affects_claim_identity:
            problems.append((pid, "role changes claim_hash but the flag disagrees"))
        if ("search_space_hash" in e["changes"]) != r.affects_search_space:
            problems.append((pid, "role changes search_space_hash but the flag disagrees"))
        if ("decision_policy_hash" in e["changes"]) != r.affects_decision_policy:
            problems.append((pid, "role changes decision_policy_hash but the flag disagrees"))
        if r.allowed_after_register != (r.semantic_role == PRESENTATION_ONLY):
            problems.append((pid, "a non-cosmetic parameter is turnable after the freeze"))
    return problems
