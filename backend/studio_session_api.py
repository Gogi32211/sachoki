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
            raise HTTPException(409, {"error": type(e).__name__, "detail": str(e)})

    @router.post("/{sid}/revisit")
    def _revisit(sid: str, b: RevisitBody):
        try:
            return revisit(sid, b.horizon, b.tolerance)
        except KeyError as e:
            raise HTTPException(404, str(e))

    return router
