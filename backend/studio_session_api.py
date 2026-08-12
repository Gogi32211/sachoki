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

import research_store as RS                                        # noqa: E402
from evidence_boundary import (DataWindow, EvidenceBoundary,  # noqa: E402
                               EvidenceBoundaryError)
from research_family import ResearchFamily                          # noqa: E402

# Durable, because REGISTERED is a promise. Sessions are no longer held in a dict that a restart
# empties; they are restored from the ledger on every request, which is slower and is the only
# version that can be trusted after the process dies.
_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "research_ledger")
LEDGER = RS.DurableLedger(os.path.join(_DIR, "research_events.jsonl"))
RESPONSES = RS.ResponseLog(os.path.join(_DIR, "response_log.jsonl"))
_CODE = "research_session@0e45b53"

# What this slice reads. Declared, not assumed: an exposure whose footprint is unknown is what
# EvidenceBoundary must treat as contamination, so the window is part of the session from the
# first event rather than something added later when someone remembers.
DEV_WINDOW = {"data_id": "bars_1d", "start": "2021-01-01", "end": "2023-12-31"}


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


def _next_id() -> str:
    return f"s{len(LEDGER.sessions()) + 1:04d}"


def create(family_id: str = "", window: dict | None = None) -> dict:
    """A new session. `family_id` decides whether this continues a selection history.

    Two different questions wear the same button in most tools. Opening a fresh session to keep
    working is the normal case and belongs to the same family; declaring the work independent
    mints a new one. The system records which was claimed and never lets that claim decide
    confirmatory standing on its own — that is what EvidenceBoundary is for.
    """
    sid = _next_id()
    fam = family_id or f"F{sid[1:]}"
    s = ResearchSession(sid, code_hash=_CODE, store=LEDGER, family_id=fam)
    s.data_window = dict(window or DEV_WINDOW)
    s.start_exploration()
    return {"session": asdict(to_view(s)), "family_id": fam,
            "data_window": dict(s.data_window)}


def _get(sid: str) -> ResearchSession:
    return ResearchSession.restore(sid, LEDGER)


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


def _idempotent(key: str, action: str, sid: str, payload: dict, fn):
    """One user action, one outcome, however many times the request is delivered."""
    if not key:
        return fn()
    rh = RS.request_hash(sid, action, payload)
    cached = RESPONSES.recall(key, rh)      # raises IdempotencyConflictError on a reused key
    if cached is not None:
        return cached
    out = fn()
    RESPONSES.remember(key, rh, out)
    return out


def confirmatory(sid: str, validation_start: str, validation_end: str,
                 data_id: str = "bars_1d", available: str = "",
                 development: dict | None = None) -> dict:
    """May this family's registered claim be reported as confirmatory, and on what evidence."""
    s = _get(sid)
    fam = ResearchFamily(s.family_id, LEDGER.read_all())
    if not available:
        # No default is safe here. Defaulting to the end of the development window would make
        # every historical holdout look FORWARD, which is the strongest verdict in the system
        # produced by a missing field. This is a commitment about what data existed when the
        # claim was frozen; the caller states it or gets nothing.
        return {"eligible": False, "status": "INVALID_BOUNDARY",
                "why": ("data_available_at_registration was not supplied. It cannot be defaulted: "
                        "any default would decide FORWARD or CLEAN on the system's behalf, and "
                        "FORWARD is the one verdict that does not depend on the ledger being "
                        "complete.")}
    try:
        b = EvidenceBoundary(
            development=DataWindow(**(development or DEV_WINDOW)),
            validation=DataWindow(data_id, validation_start, validation_end),
            claim_registered_at=f"{available}T00:00:00",
            data_available_at_registration=available)
    except EvidenceBoundaryError as e:
        return {"eligible": False, "status": "INVALID_BOUNDARY", "why": str(e)}
    v = fam.confirmatory(b)
    acc = fam.accounting()
    v["family_id"] = s.family_id
    v["k_family_selectable_is_bound"] = acc.k_family_selectable_is_bound
    v["k_family_selectable"] = acc.k_family_selectable
    return v


def family(sid: str) -> dict:
    s = _get(sid)
    a = ResearchFamily(s.family_id, LEDGER.read_all()).accounting()
    return {"family": {k: (list(v) if isinstance(v, tuple) else v)
                       for k, v in asdict(a).items()}, "family_hash": a.family_hash}


def fork(sid: str, reason: str, horizon: str, tolerance: str) -> dict:
    """A new exploratory session that starts where this one stopped.

    The spec travels back to the client rather than living in the session object: the ledger
    accounts for claims, and the current position of two sliders is the client's business. What
    the ledger does carry is that this position was inherited from a named parent at a named
    state, which is the part that could otherwise be denied.
    """
    parent = _get(sid)
    child = parent.fork(_next_id(), reason=reason)
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
        "IdempotencyConflictError":
            "This action key was already used for a different request. Reload the screen so it "
            "can issue a fresh key rather than replaying an old one.",
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
    # generated by the CLIENT, once per user action. Absent means "not retryable", which is
    # honest; a server-side default would silently coalesce two deliberate repeats.
    idempotency_key: str = ""


class RevisitBody(BaseModel):
    horizon: str
    tolerance: str
    idempotency_key: str = ""


class ForkBody(BaseModel):
    reason: str
    horizon: str
    tolerance: str
    idempotency_key: str = ""


class CreateBody(BaseModel):
    family_id: str = ""
    # what this session will READ. A session that does not say leaves every exposure with an
    # unknown footprint, which EvidenceBoundary must read as contamination.
    data_id: str = "bars_1d"
    window_start: str = ""
    window_end: str = ""


class BoundaryBody(BaseModel):
    validation_start: str
    validation_end: str
    data_id: str = "bars_1d"
    data_available_at_registration: str = ""


def build_router():
    from fastapi import APIRouter, HTTPException

    router = APIRouter(prefix="/api/studio/session", tags=["studio-session"])

    @router.post("/create")
    def _create(b: CreateBody | None = None):
        w = None
        if b and b.window_start and b.window_end:
            w = {"data_id": b.data_id, "start": b.window_start, "end": b.window_end}
        return create(b.family_id if b else "", w)

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
            return _idempotent(
                b.idempotency_key, "change", sid,
                {"parameter_id": b.parameter_id, "horizon": b.horizon,
                 "tolerance": b.tolerance, "new_value": b.new_value},
                lambda: change_and_run(sid, b.parameter_id, b.horizon, b.tolerance, b.new_value,
                                       b.space_size, b.displayed))
        except KeyError as e:
            raise HTTPException(404, str(e))
        except RS.IdempotencyConflictError as e:
            raise HTTPException(409, refusal(e))
        except (SessionStateError, CannotRegisterAfterExposureError,
                UnregisteredSelectionError, SearchSpaceDriftError) as e:
            raise HTTPException(409, refusal(e))

    @router.post("/{sid}/revisit")
    def _revisit(sid: str, b: RevisitBody):
        try:
            return _idempotent(b.idempotency_key, "revisit", sid,
                               {"horizon": b.horizon, "tolerance": b.tolerance},
                               lambda: revisit(sid, b.horizon, b.tolerance))
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
            return _idempotent(b.idempotency_key, "fork", sid, {"reason": b.reason},
                               lambda: fork(sid, b.reason, b.horizon, b.tolerance))
        except KeyError as e:
            raise HTTPException(404, str(e))
        except RS.IdempotencyConflictError as e:
            raise HTTPException(409, refusal(e))
        except SessionStateError as e:
            raise HTTPException(409, refusal(e))

    @router.get("/{sid}/family")
    def _family(sid: str):
        try:
            return family(sid)
        except KeyError:
            raise HTTPException(404, f"no session {sid}")

    @router.post("/{sid}/confirmatory")
    def _confirmatory(sid: str, b: BoundaryBody):
        try:
            return confirmatory(sid, b.validation_start, b.validation_end, b.data_id,
                                b.data_available_at_registration)
        except KeyError:
            raise HTTPException(404, f"no session {sid}")

    return router
