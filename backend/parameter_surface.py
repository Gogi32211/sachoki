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

def registry_hash() -> str:
    """The declared semantics of every knob, as one value.

    A plan approved against one registry must not be committed against another. Roles are code,
    so a deployment can change them between a preview and a commit, and the person who approved
    the preview approved what it said the change would cost.
    """
    blob = json.dumps({pid: [r.semantic_role, r.affects_claim_identity, r.affects_search_space,
                             r.affects_decision_policy, r.allowed_after_register]
                       for pid, r in sorted(REGISTRY.items())}, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


# ── presentation metadata ───────────────────────────────────────────────────
#
# Deliberately separate from `semantic_role`, and served alongside it. The frontend needs to know
# how to render an input; it must never work out what the input COSTS. `ui_kind` answers the
# first question and nothing else — a NUMBER control looks the same whether the number is a view
# or a multiplicity, which is exactly why the role travels beside it instead of being inferred.
HYPOTHESIS, POPULATION, SEARCH, DECISION, VIEW = (
    "Hypothesis", "Population & design", "Search", "Decision", "View")

PRESENTATION: dict = {
    "horizon": (HYPOTHESIS, "Horizon", "NUMBER", "bars held before the outcome is read",
                {"min": 5, "max": 120, "step": 5}),
    "conditioning_tolerance": (HYPOTHESIS, "RSI tolerance", "NUMBER",
                               "half-width of the RSI band the claim conditions on",
                               {"min": 1, "max": 10, "step": 1}),
    "outcome_metric": (HYPOTHESIS, "Outcome", "ENUM", "what the claim is about",
                       {"options": ["median_return", "mean_return", "win_rate", "mtm"]}),
    "conditioning_feature": (HYPOTHESIS, "Conditioning feature", "ENUM",
                             "the feature the band is measured on",
                             {"options": ["rsi_14", "rsi_2", "atr_14", "vol_20"]}),
    "date_range": (POPULATION, "Date range", "ENUM", "the slice the claim is made over",
                   {"options": ["2021-2023", "2021-2026", "2024-2026"]}),
    "universe": (POPULATION, "Universe", "ENUM", "which names are in scope",
                 {"options": ["russell", "sp500", "nasdaq100", "all"]}),
    "base_setup_conditioning": (POPULATION, "Base setup", "ENUM",
                                "the setup the increment is measured against",
                                {"options": ["none", "capitulation", "engulf", "spring"]}),
    "weighting": (POPULATION, "Weighting", "ENUM", "how observations are weighted",
                  {"options": ["equal", "by_name", "by_date"]}),
    "support_cutoff": (POPULATION, "Support minimum", "NUMBER",
                       "smallest cell that may carry an estimate",
                       {"min": 25, "max": 500, "step": 25}),
    "setup_subset": (SEARCH, "Search classes", "MULTI",
                     "which setup classes the algorithm may rank",
                     {"options": ["a", "b", "c", "d", "e"]}),
    "selection_top_k": (SEARCH, "Selection top-K", "NUMBER",
                        "how many classes the algorithm may choose a winner from",
                        {"min": 5, "max": 60, "step": 1}),
    "top_k": (SEARCH, "Legacy top-K", "NUMBER", "kept for the frozen v2 spaces",
              {"min": 5, "max": 60, "step": 1}),
    "rank_metric": (SEARCH, "Rank metric", "ENUM", "how candidates are ordered",
                    {"options": ["ic", "rank_ic", "median", "sharpe"]}),
    "sort_by_new_outcome_metric": (SEARCH, "Re-rank by new metric", "ENUM",
                                   "ranking an existing list by a NEW outcome is a selection "
                                   "path, not a view",
                                   {"options": ["", "sharpe", "pf", "mae"]}),
    "equivalence_margin": (DECISION, "Materiality margin", "NUMBER",
                           "how large an effect has to be to matter",
                           {"min": 0.0, "max": 5.0, "step": 0.25}),
    "null_family": (DECISION, "Null family", "ENUM",
                    "which null model the p-value is read against",
                    {"options": ["opportunity_level", "day_level"]}),
    "direction": (DECISION, "Direction", "ENUM", "which tail the verdict is read from",
                  {"options": ["long", "short", "two_sided"]}),
    "displayed_top_k": (VIEW, "Displayed top-K", "NUMBER",
                        "how many of the ranked results are drawn on screen",
                        {"min": 1, "max": 50, "step": 1}),
    "sort_by_displayed_column": (VIEW, "Sort", "ENUM", "re-orders what is already computed",
                                 {"options": ["score", "effect", "n", "pf"]}),
    "column_order": (VIEW, "Column order", "MULTI", "which columns, in which order",
                     {"options": ["score", "effect", "n", "pf", "dsr"]}),
    "layout": (VIEW, "Layout", "ENUM", "how the results are arranged",
               {"options": ["grid", "list", "compact"]}),
    "theme": (VIEW, "Theme", "ENUM", "light or dark", {"options": ["dark", "light"]}),
}

GROUP_ORDER = (HYPOTHESIS, POPULATION, SEARCH, DECISION, VIEW)


def presentation(parameter_id: str) -> dict:
    g, label, kind, desc, extra = PRESENTATION.get(
        parameter_id, (VIEW, parameter_id, "TEXT", "", {}))
    return {"group": g, "label": label, "ui_kind": kind, "description": desc, **extra}


class StaleChangePlanError(RuntimeError):
    """The session or the registry moved between the preview and the commit."""


@dataclass(frozen=True)
class ChangePlan:
    """What a preview promised, in a form a commit can be checked against.

    Sharing the classifier removes one disagreement and not the other. A person reads a preview
    computed at state S1, thinks, and clicks; by then the session may be at S2. Same classifier,
    different transition — and the change applied is not the one anybody approved.

    So the preview returns a plan pinned to `prior_state_hash` and to the registry it was
    classified under, and the commit presents `plan_hash`. A mismatch is refused rather than
    silently recomputed: the correct answer to "the world moved" is a new preview, not a quietly
    different action.
    """
    plan_id: str
    session_id: str
    prior_state_hash: str
    parameter_id: str
    old_value: str
    new_value: str
    semantic_role: str
    old_claim_hash: str
    new_claim_hash: str
    old_search_space_hash: str
    new_search_space_hash: str
    old_decision_policy_hash: str
    new_decision_policy_hash: str
    multiplicity_effect: str
    registered_effect: str
    parameter_registry_hash: str
    no_op: str

    @property
    def plan_hash(self) -> str:
        blob = "|".join(str(v) for v in (
            self.session_id, self.prior_state_hash, self.parameter_id, self.old_value,
            self.new_value, self.semantic_role, self.new_claim_hash,
            self.new_search_space_hash, self.new_decision_policy_hash,
            self.multiplicity_effect, self.registered_effect, self.parameter_registry_hash))
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    def as_dict(self) -> dict:
        from dataclasses import asdict as _asdict
        d = _asdict(self)
        d["plan_hash"] = self.plan_hash
        return d


def plan_for(session_id: str, prior_state_hash: str, surface: "ParameterSurface",
             parameter_id: str, new_value, *, state: str = "EXPLORE") -> ChangePlan:
    c = classify(surface, parameter_id, new_value, state=state)
    return ChangePlan(
        plan_id=f"{session_id}:{parameter_id}:{prior_state_hash}",
        session_id=session_id, prior_state_hash=prior_state_hash,
        parameter_id=parameter_id, old_value=c["old_value"], new_value=c["new_value"],
        semantic_role=c["role"],
        old_claim_hash=c["old_claim_hash"], new_claim_hash=c["new_claim_hash"],
        old_search_space_hash=c["old_search_space_hash"],
        new_search_space_hash=c["new_search_space_hash"],
        old_decision_policy_hash=c["old_decision_policy_hash"],
        new_decision_policy_hash=c["new_decision_policy_hash"],
        multiplicity_effect=c["multiplicity_effect"],
        registered_effect=c["registered_effect"],
        parameter_registry_hash=registry_hash(), no_op="YES" if c["no_op"] else "NO")



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
