"""Multiplicity integrity: the UI may not turn exploration into confirmation by clicking.

Presentation integrity was the last layer to be defended. This is the next one, and it is more
dangerous, because here a person creates statistical objects with a mouse.

The governing principle, and it is not "count the clicks":

    k counts DISTINCT SELECTABLE CLAIM IDENTITIES the researcher had access to.

Twenty clicks may be twenty new claims, ten re-openings of one, four tolerance changes and six
technical re-runs. Only a canonical identity can tell them apart, so the identity comes first
and the session is built on top of it.

THREE DIFFERENT k, never one field:

    k_declared    what the frozen search space permitted looking at
    k_exposed     distinct claims whose results the researcher actually saw
    k_selectable  the space the ALGORITHM could pick a winner from

The third is the one that governs a search verdict, and it is not what the screen displayed.
ComboLab ranks 31 classes and shows five; the multiplicity is 31. A UI history would say five
and be wrong in the direction that flatters.

THE STATE MACHINE HAS ONE IRREVERSIBLE EDGE. Looking at results and then pressing "register" is
the whole failure mode this module exists to prevent, so once a session has exposed a result it
can never become registered — registration must open a NEW session, and the attempt raises
rather than warns.

THE BACKEND CLASSIFIES, THE UI REPORTS. A component says "the user did X". Whether X moves
multiplicity is decided here, from a parameter registry declared in advance, because a frontend
that decides what counts will eventually decide that nothing does.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field

# ── errors that stop execution rather than annotate it ───────────────────────
class CannotRegisterAfterExposureError(RuntimeError):
    """Results were seen; registration would be a claim about the past."""


class UnregisteredSelectionError(RuntimeError):
    """A confirmatory verdict reached for a claim outside the frozen search space."""


class SearchSpaceDriftError(RuntimeError):
    """The space actually searched is not the space that was registered."""


class SessionStateError(RuntimeError):
    """An event was appended that this state does not permit."""


# ── what makes two questions the same question ───────────────────────────────
@dataclass(frozen=True)
class ClaimIdentity:
    """The canonical identity of a statistical claim.

    `RSI 45 ±5` and `RSI 45 ±1` are different claims — on one real bar they select 3,940 rows and
    108 — so their conditioning hashes differ and both count. Opening the same specification a
    second time is the same claim and counts once. Everything that can change the answer belongs
    here; nothing that only changes the picture does.
    """
    estimand: str
    outcome: str
    horizon: str
    population: str
    conditioning_hash: str
    feature_rule_hash: str
    support_policy_hash: str
    null_family: str
    decision_policy_version: str

    def __post_init__(self):
        for f, v in asdict(self).items():
            if not v:
                raise ValueError(f"ClaimIdentity.{f} is empty — an identity with a hole in it "
                                 f"cannot tell two claims apart, which is its only job")

    @property
    def claim_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()[:16]


# ── which knobs are dangerous, declared before anyone turns one ──────────────
PRESENTATION_ONLY = "PRESENTATION_ONLY"
DESIGN_CHANGE = "DESIGN_CHANGE"
CLAIM_CHANGE = "CLAIM_CHANGE"
SEARCH_SPACE_CHANGE = "SEARCH_SPACE_CHANGE"
POLICY_CHANGE = "POLICY_CHANGE"


@dataclass(frozen=True)
class ParameterDefinition:
    parameter_id: str
    semantic_role: str
    affects_claim_identity: bool
    affects_search_space: bool
    affects_decision_policy: bool


PARAMETERS: dict = {p.parameter_id: p for p in (
    # cosmetic
    ParameterDefinition("layout", PRESENTATION_ONLY, False, False, False),
    ParameterDefinition("theme", PRESENTATION_ONLY, False, False, False),
    ParameterDefinition("column_order", PRESENTATION_ONLY, False, False, False),
    # sorting is NOT automatically harmless: re-ranking an already-computed list by a new
    # outcome-derived metric and then choosing is a selection path, not a view.
    ParameterDefinition("sort_by_displayed_column", PRESENTATION_ONLY, False, False, False),
    ParameterDefinition("sort_by_new_outcome_metric", SEARCH_SPACE_CHANGE, False, True, False),
    # the answer changes
    ParameterDefinition("conditioning_tolerance", CLAIM_CHANGE, True, False, False),
    ParameterDefinition("conditioning_feature", CLAIM_CHANGE, True, False, False),
    ParameterDefinition("horizon", CLAIM_CHANGE, True, False, False),
    ParameterDefinition("outcome_metric", CLAIM_CHANGE, True, False, False),
    ParameterDefinition("base_setup_conditioning", CLAIM_CHANGE, True, False, False),
    ParameterDefinition("universe", CLAIM_CHANGE, True, False, False),
    ParameterDefinition("date_range", CLAIM_CHANGE, True, False, False),
    ParameterDefinition("weighting", CLAIM_CHANGE, True, False, False),
    ParameterDefinition("support_cutoff", DESIGN_CHANGE, True, False, False),
    # the space changes
    ParameterDefinition("setup_subset", SEARCH_SPACE_CHANGE, False, True, False),
    ParameterDefinition("top_k", SEARCH_SPACE_CHANGE, False, True, False),
    ParameterDefinition("rank_metric", SEARCH_SPACE_CHANGE, False, True, False),
    # the rules change
    ParameterDefinition("null_family", POLICY_CHANGE, False, False, True),
    ParameterDefinition("equivalence_margin", POLICY_CHANGE, False, False, True),
    ParameterDefinition("direction", POLICY_CHANGE, False, False, True),
)}


def classify_change(parameter_id: str) -> ParameterDefinition:
    if parameter_id not in PARAMETERS:
        raise KeyError(
            f"{parameter_id!r} has no declared semantic role. An undeclared knob is a hidden "
            f"degree of freedom; register it before it can be turned.")
    return PARAMETERS[parameter_id]


# ── the ledger ───────────────────────────────────────────────────────────────
SESSION_CREATED = "SESSION_CREATED"
SEARCH_SPACE_DECLARED = "SEARCH_SPACE_DECLARED"
CONDITION_CHANGED = "CONDITION_CHANGED"
CLAIM_REGISTERED = "CLAIM_REGISTERED"
QUERY_EXECUTED = "QUERY_EXECUTED"
RESULT_EXPOSED = "RESULT_EXPOSED"
CLAIM_REVISITED = "CLAIM_REVISITED"
SEARCH_RUN = "SEARCH_RUN"
PROMOTION_REQUESTED = "PROMOTION_REQUESTED"
SESSION_FROZEN = "SESSION_FROZEN"
SESSION_FORKED = "SESSION_FORKED"
SESSION_CLOSED = "SESSION_CLOSED"


@dataclass(frozen=True)
class Event:
    event_id: int
    session_id: str
    event_type: str
    claim_hash: str = ""
    payload: dict = field(default_factory=dict)
    prior_state_hash: str = ""
    new_state_hash: str = ""
    code_hash: str = ""


NEW, EXPLORE, REGISTERED, ACTIVE_REGISTERED, CLOSED, CLOSED_EXPLORATORY = (
    "NEW", "EXPLORE", "REGISTERED", "ACTIVE_REGISTERED", "CLOSED", "CLOSED_EXPLORATORY")


class ResearchSession:
    """Event-sourced. `k` is computed from the ledger, never reported by a caller."""

    def __init__(self, session_id: str, code_hash: str = "unset"):
        self.session_id = session_id
        self.code_hash = code_hash
        self.state = NEW
        self.events: list = []
        self.declared_space: dict = {}
        # lineage: what this session inherited from the one it was forked out of
        self.parent_session_id: str = ""
        self.parent_state_hash: str = ""
        self.lineage: tuple = ()          # ancestors, oldest first
        self.inherited_exposed: int = 0   # results already seen upstream
        self._append(SESSION_CREATED)

    # ── ledger mechanics ────────────────────────────────────────────────────
    def _state_hash(self) -> str:
        return hashlib.sha256(
            f"{self.state}|{len(self.events)}|{self.session_id}".encode()).hexdigest()[:16]

    def _append(self, etype: str, claim_hash: str = "", **payload) -> Event:
        prior = self._state_hash()
        e = Event(event_id=len(self.events), session_id=self.session_id, event_type=etype,
                  claim_hash=claim_hash, payload=payload, prior_state_hash=prior,
                  new_state_hash="", code_hash=self.code_hash)
        self.events.append(e)
        object.__setattr__(e, "new_state_hash", self._state_hash())
        return e

    # ── mode ────────────────────────────────────────────────────────────────
    def start_exploration(self):
        if self.state != NEW:
            raise SessionStateError(f"cannot start exploration from {self.state}")
        self.state = EXPLORE
        return self

    def declare_search_space(self, space_id: str, size: int, space_hash: str):
        """What the algorithm will be permitted to choose among. Size, not what is displayed."""
        self.declared_space = {"space_id": space_id, "size": int(size), "hash": space_hash}
        self._append(SEARCH_SPACE_DECLARED, space_id=space_id, size=int(size), hash=space_hash)
        return self

    def assert_registerable(self):
        """Every reason registration would be refused, checked without touching the ledger.

        Split out of `register()` because a caller has to declare a search space before freezing,
        and a caller that declares first and is refused second has appended an event describing
        a registration that never happened. A refusal must cost nothing — the same rule as a
        preview.
        """
        if any(e.event_type == RESULT_EXPOSED for e in self.events):
            raise CannotRegisterAfterExposureError(
                f"session {self.session_id} has already exposed "
                f"{sum(1 for e in self.events if e.event_type == RESULT_EXPOSED)} result(s). "
                f"Registering now would claim these hypotheses were declared in advance. Open a "
                f"NEW session to preregister; this one stays exploratory forever.")
        if self.parent_session_id:
            # A fork is an exploratory instrument by construction, and this holds even when the
            # parent exposed nothing. What a fork inherits is not only numbers, it is a CHOICE of
            # specification — made by someone who had been looking at something, in a session
            # that existed for a reason. Preregistering that choice would claim it arrived from
            # nowhere.
            #
            # The first version of this rule only refused when `inherited_exposed > 0`, which let
            # a fork of a clean parent register. That also opened a path nothing accounts for:
            # two registered studies, siblings in one lineage, each declaring k = 31 with no
            # record connecting them. Cross-session multiplicity does not exist yet, so the
            # honest move is to keep forks out of the confirmatory track entirely.
            #
            # The cost is one extra step: to preregister, open a session with no parent and state
            # the specification from nothing. That step is the point.
            seen = (f", and {self.inherited_exposed} result(s) were already exposed upstream"
                    if self.inherited_exposed else "")
            raise CannotRegisterAfterExposureError(
                f"session {self.session_id} was forked from {self.parent_session_id}{seen}. A "
                f"fork inherits a starting point, never a clean slate, and a starting point is "
                f"itself a choice someone made while looking. Preregistration requires a session "
                f"with no parent — open one and state the specification from nothing.")
        if self.state not in (NEW, EXPLORE):
            raise SessionStateError(f"cannot register from {self.state}")
        return self

    def register(self):
        """Freeze. Impossible once anything has been seen — that is the point of the module."""
        self.assert_registerable()
        if not self.declared_space:
            raise SessionStateError("a registered session needs a declared search space")
        self.state = REGISTERED
        self._append(SESSION_FROZEN, **self.declared_space)
        return self

    # ── activity ────────────────────────────────────────────────────────────
    def change_parameter(self, parameter_id: str, old, new):
        d = classify_change(parameter_id)
        if self.state in (REGISTERED, ACTIVE_REGISTERED) and d.semantic_role != PRESENTATION_ONLY:
            raise SessionStateError(
                f"{parameter_id} is {d.semantic_role} and this session is {self.state}. A "
                f"registered study does not mutate; changing this creates a different claim and "
                f"belongs to a new exploration session.")
        self._append(CONDITION_CHANGED, parameter_id=parameter_id, role=d.semantic_role,
                     old=str(old), new=str(new))
        return d

    def execute(self, claim: ClaimIdentity):
        seen = {e.claim_hash for e in self.events if e.event_type == QUERY_EXECUTED}
        etype = CLAIM_REVISITED if claim.claim_hash in seen else QUERY_EXECUTED
        self._append(etype, claim_hash=claim.claim_hash, estimand=claim.estimand)
        return self

    def expose(self, claim: ClaimIdentity):
        """The moment a result becomes knowable. Everything before this is still preregisterable."""
        if self.state == REGISTERED:
            self.state = ACTIVE_REGISTERED
        self._append(RESULT_EXPOSED, claim_hash=claim.claim_hash)
        return self

    def search_run(self, space_id: str, space_size: int, space_hash: str, displayed: int):
        """The algorithm chose among `space_size`; `displayed` is a fact about the screen only."""
        if self.state in (REGISTERED, ACTIVE_REGISTERED):
            if space_hash != self.declared_space.get("hash"):
                raise SearchSpaceDriftError(
                    f"registered space {self.declared_space.get('hash')} but searched "
                    f"{space_hash}. The multiplicity that was declared is not the multiplicity "
                    f"that was paid.")
            if self.state == REGISTERED:
                self.state = ACTIVE_REGISTERED
        self._append(SEARCH_RUN, space_id=space_id, space_size=int(space_size),
                     space_hash=space_hash, displayed=int(displayed))
        return self

    def request_promotion(self, claim: ClaimIdentity, registered_claims=None):
        if self.state in (REGISTERED, ACTIVE_REGISTERED):
            allowed = set(registered_claims or ())
            if claim.claim_hash not in allowed:
                raise UnregisteredSelectionError(
                    f"claim {claim.claim_hash} is not in the frozen search space of session "
                    f"{self.session_id}. A confirmatory verdict on an unregistered claim is not "
                    f"UNRESOLVED and not a warning — it is an integrity failure.")
        self._append(PROMOTION_REQUESTED, claim_hash=claim.claim_hash)
        return self

    def fork(self, child_session_id: str, reason: str) -> "ResearchSession":
        """The legal way out of a frozen session.

        A registered study does not mutate, and a user who wants to change one anyway will find
        a way — reopen the app, retype the parameters, and the ledger never learns that the new
        "study" grew out of the old one's results. A prohibition without a sanctioned path is a
        prohibition that gets routed around, so governance has to offer the path.

        The child inherits a STARTING POINT and nothing else:

            new session_id, its own ledger, state EXPLORE
            parent_session_id + parent_state_hash, so the lineage is a chain and not a rumour
            inherited_exposed, carried and accumulated — the reason a fork cannot launder k
            NO registration and NO declared space; a preregistration is not inheritable

        `reason` is mandatory. The fork is a legitimate move and it is also the move most worth
        being able to read back later, so it does not happen anonymously.
        """
        if self.state == NEW:
            raise SessionStateError(
                f"session {self.session_id} has not started; there is no starting point to fork")
        if not reason or not reason.strip():
            raise SessionStateError(
                "a fork must say why. This is the one action that carries results across a "
                "freeze boundary, and an unexplained one is indistinguishable from a reset.")

        parent_hash = self._state_hash()          # the state that is being inherited
        upstream = self.accounting()["k_exposed"] + self.inherited_exposed

        child = ResearchSession(child_session_id, code_hash=self.code_hash)
        child.parent_session_id = self.session_id
        child.parent_state_hash = parent_hash
        child.lineage = tuple(self.lineage) + (self.session_id,)
        child.inherited_exposed = upstream
        # deliberately NOT inherited: declared_space, state, events, confirmatory standing
        child._append(SESSION_FORKED, parent_session_id=self.session_id,
                      parent_state_hash=parent_hash, child_session_id=child_session_id,
                      reason=reason.strip(), inherited_k_exposed=upstream,
                      parent_state=self.state, lineage_depth=len(child.lineage))
        child.start_exploration()

        # the parent records it too: being forked is a fact about the parent, and a ledger that
        # only the child knows about is a lineage one side can deny
        self._append(SESSION_FORKED, parent_session_id=self.session_id,
                     parent_state_hash=parent_hash, child_session_id=child_session_id,
                     reason=reason.strip(), inherited_k_exposed=upstream,
                     parent_state=self.state, lineage_depth=len(child.lineage))
        return child

    def close(self):
        self.state = CLOSED if self.state in (REGISTERED, ACTIVE_REGISTERED) \
            else CLOSED_EXPLORATORY
        self._append(SESSION_CLOSED, final_state=self.state)
        return self

    # ── the accountant ──────────────────────────────────────────────────────
    def accounting(self) -> dict:
        """Three k, computed from events. A caller cannot assert any of them."""
        exposed = {e.claim_hash for e in self.events
                   if e.event_type == RESULT_EXPOSED and e.claim_hash}
        executed = {e.claim_hash for e in self.events
                    if e.event_type == QUERY_EXECUTED and e.claim_hash}
        runs = [e for e in self.events if e.event_type == SEARCH_RUN]
        # the space the algorithm could pick a winner from — not what the screen showed
        k_selectable = max([e.payload["space_size"] for e in runs], default=len(exposed))
        displayed = max([e.payload["displayed"] for e in runs], default=0)
        changes = [e for e in self.events if e.event_type == CONDITION_CHANGED]
        return {
            "session_id": self.session_id, "state": self.state,
            "k_declared": self.declared_space.get("size", 0),
            "k_exposed": len(exposed),
            # what the whole lineage has seen. For an unforked session the two are equal, and
            # the moment they differ is the moment `k_exposed` alone understates the search.
            "k_exposed_lineage": len(exposed) + self.inherited_exposed,
            "inherited_exposed": self.inherited_exposed,
            "parent_session_id": self.parent_session_id,
            "lineage_depth": len(self.lineage),
            "k_selectable": k_selectable,
            "distinct_claims_executed": len(executed),
            "revisits": sum(1 for e in self.events if e.event_type == CLAIM_REVISITED),
            "displayed_at_most": displayed,
            "changes_by_role": {r: sum(1 for e in changes if e.payload["role"] == r)
                                for r in (PRESENTATION_ONLY, DESIGN_CHANGE, CLAIM_CHANGE,
                                          SEARCH_SPACE_CHANGE, POLICY_CHANGE)},
            "confirmatory_eligible": self.state in (REGISTERED, ACTIVE_REGISTERED, CLOSED),
            "events": len(self.events),
        }


def preview_design_change(parameter_id: str, before: ClaimIdentity | None,
                          after: ClaimIdentity | None) -> dict:
    """What the UI must ask BEFORE it runs anything. The backend answers; the UI obeys.

    Stronger than a post-hoc ledger: the classification exists before the result does, so a
    change cannot be re-labelled once its answer turns out to be attractive.
    """
    d = classify_change(parameter_id)
    old_h = before.claim_hash if before else ""
    new_h = after.claim_hash if after else ""
    effect = "NONE"
    if d.affects_claim_identity and old_h != new_h:
        effect = "NEW_SELECTABLE_CLAIM"
    elif d.affects_search_space:
        effect = "SEARCH_SPACE_CHANGED"
    elif d.affects_decision_policy:
        effect = "DECISION_POLICY_CHANGED"
    return {"parameter_id": parameter_id, "change_type": d.semantic_role,
            "old_claim_hash": old_h, "new_claim_hash": new_h, "multiplicity_effect": effect}
