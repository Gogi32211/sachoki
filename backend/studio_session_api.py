"""Transport for the research session: the browser receives sanctioned state, not a ledger.

The same architectural move as `SemanticMetricView`, one level up. There the danger was a
component subtracting two incompatible estimates; here it is a component deriving `k` from the
events it happens to have seen. Both end the same way — a second accountant with different
rules, which is how the 2026-08-09 bug worked.

    DOMAIN        ResearchSession        the append-only ledger, server only
    TRANSPORT     ResearchSessionView    sanctioned state, every field a string
    PRESENTATION  SessionAccountingPanel

The counts are strings for the reason the statistical values were: a number in the browser
invites the browser to derive one. `k_exposed` is computed by the ledger engine from classified
events, and the screen's only job is to show what it was told.

The ledger itself never crosses. A frontend holding the event stream could count claims its own
way, and the first time its count differed from the backend's, the more convenient one would
win.
"""
from __future__ import annotations

import os
import sys
from dataclasses import asdict, dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from research_session import (ClaimIdentity, ResearchSession,  # noqa: E402
                             CannotRegisterAfterExposureError, SearchSpaceDriftError,
                             SessionStateError, UnregisteredSelectionError, classify_change,
                             preview_design_change)

# in-memory for now; a session is a working object, not a record to survive a restart
_SESSIONS: dict = {}
_CODE = "research_session@fd0aa1b"


@dataclass(frozen=True)
class ResearchSessionView:
    """Sanctioned session state. Every field is a string; the accountant stays on the server."""
    session_id: str
    mode: str
    k_declared: str
    k_exposed: str
    k_exposed_lineage: str
    inherited_exposed: str
    parent_session_id: str
    lineage_depth: str
    k_selectable: str
    revisits: str
    displayed_at_most: str
    changes_claim: str
    changes_search_space: str
    changes_policy: str
    changes_presentation: str
    confirmatory_eligible: str
    events: str
    state_hash: str

    def __post_init__(self):
        for f, v in asdict(self).items():
            if not isinstance(v, str):
                raise TypeError(f"ResearchSessionView.{f} is {type(v).__name__}; every field is "
                                f"a string, so no component can recompute an accounting number")


def to_view(s: ResearchSession) -> ResearchSessionView:
    a = s.accounting()
    c = a["changes_by_role"]
    return ResearchSessionView(
        session_id=a["session_id"], mode=a["state"],
        k_declared=str(a["k_declared"]), k_exposed=str(a["k_exposed"]),
        k_exposed_lineage=str(a["k_exposed_lineage"]),
        inherited_exposed=str(a["inherited_exposed"]),
        parent_session_id=a["parent_session_id"], lineage_depth=str(a["lineage_depth"]),
        k_selectable=str(a["k_selectable"]), revisits=str(a["revisits"]),
        displayed_at_most=str(a["displayed_at_most"]),
        changes_claim=str(c["CLAIM_CHANGE"]),
        changes_search_space=str(c["SEARCH_SPACE_CHANGE"]),
        changes_policy=str(c["POLICY_CHANGE"]),
        changes_presentation=str(c["PRESENTATION_ONLY"]),
        confirmatory_eligible="YES" if a["confirmatory_eligible"] else "NO",
        events=str(a["events"]), state_hash=s._state_hash())


# ── the claim under study in this slice: two knobs, both CLAIM_CHANGE ────────
def _claim(horizon: str, tolerance: str) -> ClaimIdentity:
    return ClaimIdentity(
        estimand="incremental_return_pp", outcome="median_return", horizon=str(horizon),
        population="price_21_89", conditioning_hash=f"rsi45pm{tolerance}",
        feature_rule_hash="rsi_14", support_policy_hash="6f825ca4763fea76",
        null_family="OPPORTUNITY_LEVEL", decision_policy_version="verdict_v2")


def create() -> dict:
    sid = f"s{len(_SESSIONS) + 1:04d}"
    s = ResearchSession(sid, code_hash=_CODE).start_exploration()
    _SESSIONS[sid] = s
    return {"session": asdict(to_view(s))}


def _get(sid: str) -> ResearchSession:
    if sid not in _SESSIONS:
        raise KeyError(sid)
    return _SESSIONS[sid]


def preview(sid: str, parameter_id: str, horizon: str, tolerance: str,
            new_value: str) -> dict:
    """What the UI must ask BEFORE running anything. The answer precedes the result."""
    _get(sid)
    before = _claim(horizon, tolerance)
    after = _claim(new_value, tolerance) if parameter_id == "horizon" \
        else _claim(horizon, new_value)
    return preview_design_change(parameter_id, before, after)


def change_and_run(sid: str, parameter_id: str, horizon: str, tolerance: str,
                   new_value: str, space_size: int = 31, displayed: int = 5) -> dict:
    """One user action, end to end: classify → record → execute → expose → search."""
    s = _get(sid)
    before = _claim(horizon, tolerance)
    if parameter_id == "horizon":
        horizon, after = new_value, _claim(new_value, tolerance)
    elif parameter_id == "conditioning_tolerance":
        tolerance, after = new_value, _claim(horizon, new_value)
    else:
        after = before
    d = classify_change(parameter_id)
    s.change_parameter(parameter_id, str(before.claim_hash), str(after.claim_hash))
    s.execute(after)
    s.expose(after)
    # the algorithm ranked `space_size` and the screen will show `displayed`; the ledger records
    # the first, which is the number multiplicity is paid on
    s.search_run("combolab_v2", space_size, "3600ae3dd52a25e6", displayed)
    return {"session": asdict(to_view(s)), "claim_hash": after.claim_hash,
            "change_type": d.semantic_role, "horizon": str(horizon), "tolerance": str(tolerance)}


def revisit(sid: str, horizon: str, tolerance: str) -> dict:
    """Opening the same specification again. Free, and recorded as free."""
    s = _get(sid)
    c = _claim(horizon, tolerance)
    s.execute(c)
    s.expose(c)
    return {"session": asdict(to_view(s)), "claim_hash": c.claim_hash}


def accounting(sid: str) -> dict:
    return {"session": asdict(to_view(_get(sid)))}


# ── preregistration and the way back out of it ──────────────────────────────
SPACE = ("combolab_v2", 31, "3600ae3dd52a25e6")


def register(sid: str) -> dict:
    """Declare the space, then freeze. Irreversible, and refused once anything has been seen."""
    s = _get(sid)
    s.assert_registerable()      # refuse BEFORE declaring, so a refusal leaves no trace
    s.declare_search_space(*SPACE)
    s.register()
    return {"session": asdict(to_view(s))}


def fork(sid: str, reason: str, horizon: str, tolerance: str) -> dict:
    """A new exploratory session that starts where this one stopped.

    The spec travels back to the client rather than living in the session object: the ledger
    accounts for claims, and the current position of two sliders is the client's business. What
    the ledger does carry is that this position was inherited from a named parent at a named
    state, which is the part that could otherwise be denied.
    """
    parent = _get(sid)
    child_id = f"s{len(_SESSIONS) + 1:04d}"
    child = parent.fork(child_id, reason=reason)
    _SESSIONS[child_id] = child
    return {"session": asdict(to_view(child)),
            "parent": asdict(to_view(parent)),
            "inherited": {"horizon": str(horizon), "tolerance": str(tolerance)},
            "reason": reason.strip()}


def refusal(e: Exception) -> dict:
    """A refusal the UI can render as a sentence, not as a disabled button.

    A control that is merely greyed out teaches nothing; the user concludes the app is broken,
    or worse, works out which sequence of clicks avoids the grey. Every refusal here says what
    happened, why the rule exists, and what the legitimate next move is — and `offers_fork` is
    how the UI knows a legitimate next move exists at all.
    """
    kind = type(e).__name__
    # Which legal move exists is a property of WHICH rule fired, and getting it wrong is worse
    # than saying nothing: telling someone to fork when forking cannot help sends them around a
    # loop. FORK continues the work; NEW_SESSION is the only route back into the confirmatory
    # track, and it is deliberately not a shortcut — nothing is carried over.
    next_action = {
        "SessionStateError": "FORK",
        "CannotRegisterAfterExposureError": "NEW_SESSION",
    }.get(kind, "NONE")
    remedy = {
        "SessionStateError":
            "This study is frozen. Changing a claim-defining parameter would silently turn a "
            "preregistered result into an exploratory one. Fork it into a new exploratory "
            "session to continue from here.",
        "CannotRegisterAfterExposureError":
            "This lineage can never become confirmatory — either results have been seen in it, "
            "or it starts from a specification someone else chose. Preregistration needs a "
            "session with no parent and no history; forking will not get you there.",
        "UnregisteredSelectionError":
            "This claim is outside the frozen search space. A confirmatory verdict on it would "
            "not be a weak result — it would be an unaccounted one.",
        "SearchSpaceDriftError":
            "The space actually searched is not the space that was registered, so the "
            "multiplicity declared is not the multiplicity paid.",
    }.get(kind, "")
    return {"error": kind, "detail": str(e), "remedy": remedy, "next_action": next_action,
            "offers_fork": "YES" if next_action == "FORK" else "NO"}


# Request models live at MODULE level, not inside build_router(). FastAPI resolves handler
# annotations against the module globals; a class defined in a local scope is invisible there and
# the parameter silently degrades to a query field, which surfaces as 422 "Field required" on a
# body that was in fact sent. Nothing in the type checker or the tests sees this — only a real
# request does.
from pydantic import BaseModel                                      # noqa: E402


class ChangeBody(BaseModel):
    parameter_id: str
    horizon: str
    tolerance: str
    new_value: str
    space_size: int = 31
    displayed: int = 5


class RevisitBody(BaseModel):
    horizon: str
    tolerance: str


class ForkBody(BaseModel):
    reason: str
    horizon: str
    tolerance: str


def build_router():
    from fastapi import APIRouter, HTTPException

    router = APIRouter(prefix="/api/studio/session", tags=["studio-session"])

    @router.post("/create")
    def _create():
        return create()

    @router.get("/{sid}")
    def _get_session(sid: str):
        try:
            return accounting(sid)
        except KeyError:
            raise HTTPException(404, f"no session {sid}")

    @router.post("/{sid}/preview")
    def _preview(sid: str, b: ChangeBody):
        try:
            return preview(sid, b.parameter_id, b.horizon, b.tolerance, b.new_value)
        except KeyError as e:
            raise HTTPException(404, str(e))

    @router.post("/{sid}/change")
    def _change(sid: str, b: ChangeBody):
        try:
            return change_and_run(sid, b.parameter_id, b.horizon, b.tolerance, b.new_value,
                                  b.space_size, b.displayed)
        except KeyError as e:
            raise HTTPException(404, str(e))
        except (SessionStateError, CannotRegisterAfterExposureError,
                UnregisteredSelectionError, SearchSpaceDriftError) as e:
            raise HTTPException(409, refusal(e))

    @router.post("/{sid}/revisit")
    def _revisit(sid: str, b: RevisitBody):
        try:
            return revisit(sid, b.horizon, b.tolerance)
        except KeyError as e:
            raise HTTPException(404, str(e))

    @router.post("/{sid}/register")
    def _register(sid: str):
        try:
            return register(sid)
        except KeyError as e:
            raise HTTPException(404, str(e))
        except (SessionStateError, CannotRegisterAfterExposureError) as e:
            raise HTTPException(409, refusal(e))

    @router.post("/{sid}/fork")
    def _fork(sid: str, b: ForkBody):
        try:
            return fork(sid, b.reason, b.horizon, b.tolerance)
        except KeyError as e:
            raise HTTPException(404, str(e))
        except SessionStateError as e:
            raise HTTPException(409, refusal(e))

    return router
