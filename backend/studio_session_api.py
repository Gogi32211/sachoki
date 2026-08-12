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
from data_access import (CATALOG, DEVELOPMENT, VALIDATION,  # noqa: E402
                         DataAccessLayer, DataAccessSpec, SourceUnavailableError,
                         duckdb_bars_provider)
import data_gateway as GW                                          # noqa: E402
import parameter_surface as PSURF                                  # noqa: E402
import search_run as SR                                            # noqa: E402
from evidence_boundary import (EvidenceBoundary,  # noqa: E402
                               EvidenceBoundaryDriftError, EvidenceBoundaryError,
                               freeze_boundary)
from research_family import ResearchFamily                          # noqa: E402

# Durable, because REGISTERED is a promise. Sessions are no longer held in a dict that a restart
# empties; they are restored from the ledger on every request, which is slower and is the only
# version that can be trusted after the process dies.
_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "research_ledger")
LEDGER = RS.DurableLedger(os.path.join(_DIR, "research_events.jsonl"))
RESPONSES = RS.ResponseLog(os.path.join(_DIR, "response_log.jsonl"))
_CODE = "research_session@0e45b53"

# The source the server can speak for. Registering the provider here rather than inside the
# boundary code keeps the cutoff a fact about THIS deployment's database, and read-only because
# the bars writer is the nightly launchd job and must stay the only one.
_BARS_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "data", "studio_analytics.duckdb")
CATALOG.register("bars_1d", duckdb_bars_provider(_BARS_DB))


def _bars_reader(path, start, end, columns=()):
    """The ONLY way this slice reaches bars. Read-only; the nightly job stays the sole writer."""
    import duckdb
    con = duckdb.connect(path, read_only=True)
    try:
        rows = con.execute(
            "SELECT date FROM bars WHERE date BETWEEN ? AND ? LIMIT 1", [start, end]).fetchall()
    finally:
        con.close()
    return rows, len(rows)


GW.REGISTRY.register(GW.SourceRegistration(
    source_id="bars_1d", path=_BARS_DB, reader=_bars_reader, universe="russell"))
# Armed only while an execution is open, so the rest of the application keeps reading its own
# databases normally. See data_gateway for why this is ENFORCED_IN_PROCESS and not ISOLATED.
GW.install_guards()

# The default DECLARATION for this slice. It is a starting point for the form, not a global
# truth: what governs contamination is the footprint the access layer records, and a session may
# declare something else entirely.
DEFAULT_DEV_SPEC = {"source_id": "bars_1d", "universe": "russell", "start": "2021-01-01",
                    "end": "2023-12-31", "temporal_resolution": "1d", "purpose": DEVELOPMENT}


def _now() -> str:
    """Server clock. Never the browser's — `registered_at` is half of what makes FORWARD real."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class ResearchSessionView:
    """Sanctioned session state. Every field is a string; the accountant stays on the server."""
    session_id: str
    mode: str
    k_declared: str
    k_exposed: str
    # k_exposed counts the (evidence, decision) pair. These two say WHICH of them multiplied it,
    # so "k = 7" can be answered rather than only reported.
    distinct_evidence_claims: str
    distinct_decision_specs: str
    accounting_policy: str
    k_exposed_lineage: str
    inherited_exposed: str
    parent_session_id: str
    lineage_depth: str
    k_selectable: str
    revisits: str
    displayed_at_most: str
    changes_claim: str
    changes_design: str
    changes_search_space: str
    changes_selection_path: str
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
        distinct_evidence_claims=str(a["distinct_evidence_claims_exposed"]),
        distinct_decision_specs=str(a["distinct_decision_specs_exposed"]),
        accounting_policy=a["accounting_policy_version"],
        k_exposed_lineage=str(a["k_exposed_lineage"]),
        inherited_exposed=str(a["inherited_exposed"]),
        parent_session_id=a["parent_session_id"], lineage_depth=str(a["lineage_depth"]),
        k_selectable=str(a["k_selectable"]), revisits=str(a["revisits"]),
        displayed_at_most=str(a["displayed_at_most"]),
        changes_claim=str(c["CLAIM_CHANGE"]),
        # DESIGN_CHANGE existed in the ledger and stopped at the transport boundary, so a whole
        # accounting category was uncountable on screen. Found by driving all 22 controls rather
        # than one representative per role — the representative for DESIGN was the only member.
        changes_design=str(c["DESIGN_CHANGE"]),
        changes_search_space=str(c["SEARCH_SPACE_CHANGE"]),
        changes_selection_path=str(c["SELECTION_PATH_CHANGE"]),
        changes_policy=str(c["POLICY_CHANGE"]),
        changes_presentation=str(c["PRESENTATION_ONLY"]),
        confirmatory_eligible="YES" if a["confirmatory_eligible"] else "NO",
        events=str(a["events"]), state_hash=s._state_hash())


# ── the claim under study ───────────────────────────────────────────────────
def _claim(horizon: str, tolerance: str) -> ClaimIdentity:
    """The two-knob form, kept for callers that only move those two."""
    return ClaimIdentity(
        estimand="incremental_return_pp", outcome="median_return", horizon=str(horizon),
        population="price_21_89", conditioning_hash=f"rsi45pm{tolerance}",
        feature_rule_hash="rsi_14", support_policy_hash="6f825ca4763fea76",
        null_family="OPPORTUNITY_LEVEL", decision_policy_version="verdict_v2")


def _claim_from(surface) -> ClaimIdentity:
    """The claim as the WHOLE surface defines it.

    The exhaustive browser matrix found this: six of the eight CLAIM_CHANGE parameters moved the
    surface's claim_hash and exposed an identity built from `horizon` and `tolerance` alone, so
    the ledger saw the same claim twice and `k_exposed` did not move. Two notions of "the claim",
    and the one that counted was the smaller — which under-counts multiplicity, the direction
    that flatters a result.

    `conditioning_hash` now carries `surface.claim_hash`, which covers every claim-identity
    parameter by construction, and `decision_policy_version` carries the policy hash. A policy
    change therefore also produces a distinct exposed claim; that counts MORE, and between the
    two directions the conservative one is the only defensible default.
    """
    v = surface.values
    return ClaimIdentity(
        estimand="incremental_return_pp",
        outcome=v.get("outcome_metric") or "median_return",
        horizon=v.get("horizon") or "20",
        population=v.get("universe") or "price_21_89",
        conditioning_hash=surface.claim_hash,
        feature_rule_hash=v.get("conditioning_feature") or "rsi_14",
        support_policy_hash=v.get("support_cutoff") or "6f825ca4763fea76",
        null_family=v.get("null_family") or "OPPORTUNITY_LEVEL",
        decision_policy_version=surface.decision_policy_hash)


def _next_id() -> str:
    return f"s{len(LEDGER.sessions()) + 1:04d}"


def create(family_id: str = "", spec: dict | None = None) -> dict:
    """A new session. `family_id` decides whether this continues a selection history.

    Two different questions wear the same button in most tools. Opening a fresh session to keep
    working is the normal case and belongs to the same family; declaring the work independent
    mints a new one. The system records which was claimed and never lets that claim decide
    confirmatory standing on its own — that is what EvidenceBoundary is for.
    """
    sid = _next_id()
    fam = family_id or f"F{sid[1:]}"
    s = ResearchSession(sid, code_hash=_CODE, store=LEDGER, family_id=fam)
    d = dict(spec or DEFAULT_DEV_SPEC)
    d["purpose"] = DEVELOPMENT
    access = DataAccessSpec.from_dict(d)
    s.access_spec = access.as_dict()
    s.data_window = {"data_id": access.source_id, "start": access.start, "end": access.end}
    s.start_exploration()
    return {"session": asdict(to_view(s)), "family_id": fam,
            "access_spec": s.access_spec}


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
                   new_value: str, space_size: int = 31, displayed: int = 5,
                   over: tuple = ()) -> dict:
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
    _touch(s, over)
    return {"session": asdict(to_view(s)), "claim_hash": after.claim_hash,
            "change_type": d.semantic_role, "horizon": str(horizon), "tolerance": str(tolerance)}


def _touch(s: ResearchSession, over: tuple = (), execution_id: str = "") -> None:
    """The study's read, through the gateway. There is no other path from here to the data.

    `over` is a read outside the declared window. It stays expressible on purpose: the reason
    this layer records instead of refusing is that overreach happens, and a system that cannot
    represent it cannot be tested against it.
    """
    if not s.access_spec:
        return
    spec = DataAccessSpec.from_dict(s.access_spec)
    cap = GW.capability_for(s, execution_id or f"{s.session_id}-e{len(s.events)}", ("bars_1d",))
    with GW.ExecutionContext(s, cap, code_hash=_CODE) as ex:
        h = ex.open("bars_1d")
        h.read(spec.start, spec.end)
        if over:
            h.read(over[0], over[1])


def revisit(sid: str, horizon: str, tolerance: str) -> dict:
    """Opening the same specification again. Free, and recorded as free."""
    s = _get(sid)
    c = _claim(horizon, tolerance)
    s.execute(c)
    s.expose(c)
    _touch(s)
    return {"session": asdict(to_view(s)), "claim_hash": c.claim_hash}


def accounting(sid: str) -> dict:
    return {"session": asdict(to_view(_get(sid)))}


# ── the full parameter surface ──────────────────────────────────────────────
_SURFACES: dict = {}


def _surface(sid: str, s: ResearchSession) -> PSURF.ParameterSurface:
    """The session's current settings. Reconstructed from its ledger, not held in a variable.

    A restart must not silently reset the knobs to their defaults while the ledger remembers
    every change that was made to them — that is the third time this shape of bug would appear,
    so the values are replayed from CONDITION_CHANGED events.
    """
    if sid in _SURFACES:
        return _SURFACES[sid]
    surface = PSURF.ParameterSurface.initial(horizon="20", conditioning_tolerance="5",
                                             selection_top_k="31", displayed_top_k="5")
    for e in s.events:
        if e.event_type == "CONDITION_CHANGED" and e.payload.get("parameter_id"):
            pid = e.payload["parameter_id"]
            if pid in PSURF.REGISTRY and "value" in e.payload:
                surface = surface.with_value(pid, e.payload["value"])
    _SURFACES[sid] = surface
    return surface


# Plans live for as long as the state they were computed against. Losing them on a restart is
# correct: a plan whose session moved is stale anyway, and an unknown plan is refused the same
# way as a stale one rather than being reconstructed from what the request happens to say.
_PLANS: dict = {}


def parameters(sid: str = "") -> dict:
    """Every knob: how to render it, and what it costs. The UI reads both and decides neither."""
    s = _get(sid) if sid else None
    surface = _surface(sid, s) if s else None
    out = []
    for pid in sorted(PSURF.REGISTRY):
        r = PSURF.REGISTRY[pid]
        pres = PSURF.presentation(pid)
        eff = PSURF.strictest_role(pid, SURFACE_CAPS)
        out.append({
            "parameter_id": pid, "label": pres["label"], "description": pres["description"],
            "ui_kind": pres["ui_kind"], "group": pres["group"],
            "options": [str(o) for o in pres.get("options", [])],
            "min": str(pres.get("min", "")), "max": str(pres.get("max", "")),
            "step": str(pres.get("step", "")),
            "current_value": (surface.values.get(pid, "") if surface else ""),
            # the statistical half. Served, never derived on the other side of the wire.
            # the EFFECTIVE role on this surface, not the declared one. A `view` badge on a
            # control that costs exposure here would be the screen telling a comfortable lie.
            "semantic_role": eff,
            "declared_role": r.semantic_role,
            "role_is_conditional": "YES" if PSURF.role_is_conditional(pid) else "NO",
            "mutable_in_explore": "YES" if r.allowed_in_explore else "NO",
            "mutable_in_registered": "YES" if eff == PSURF.PRESENTATION_ONLY else "NO",
            "multiplicity_effect": PSURF.ROLE_EFFECTS[eff]["multiplicity_effect"],
            "registered_effect": PSURF.ROLE_EFFECTS[eff]["registered_effect"],
            "note": PSURF.ROLE_EFFECTS[eff]["note"]})
    return {"parameters": out,
            "groups": list(PSURF.GROUP_ORDER),
            "roles": {role: list(PSURF.by_role(role)) for role in PSURF.ROLE_EFFECTS},
            "parameter_registry_hash": PSURF.registry_hash()}


# What the Combo Lab screen lets a person do with a row today: nothing, because there are no
# rows. The results table will pass RESULTS_SURFACE, and that one flag reclassifies outcome
# sorting from free to costed without a line of new classification logic.
# The screen now has rows a person can inspect and promote, so `displayed_top_k` and an
# outcome sort stop being free — by the predicate, not by a decision taken here.
SURFACE_CAPS = PSURF.RESULTS_SURFACE

# run_id → SearchRunArtifact. Server side only, and never serialised: it holds all 31 ranked ids
# and shipping it would expose twenty-six claims nobody accounted for.
_RUNS: dict = {}


def _int(v, default: int) -> int:
    try:
        return int(float(str(v)))
    except (TypeError, ValueError):
        return default


def search(sid: str, sort_key: str = "") -> dict:
    """Run the search, record the exposure, return only what may be seen.

    The exposure accounting is transactional with delivery: the rows that leave this function are
    the rows the ledger just charged for, in the same call. Charging afterwards would leave a
    window in which a payload existed and the accounting did not; charging more would count
    claims nobody could reach.
    """
    s = _get(sid)
    surface = _surface(sid, s)
    v = surface.values
    selectable = _int(v.get("selection_top_k"), 31)
    displayed = _int(v.get("displayed_top_k"), 5)
    key = sort_key or v.get("sort_by_displayed_column") or "effect"

    claim = _claim_from(surface)
    run_id = f"{sid}-r{len([e for e in s.events if e.event_type == 'SEARCH_RUN']) + 1:03d}"
    artifact = SR.rank_and_authorise(
        run_id=run_id, session_id=sid, family_id=s.family_id,
        input_state_hash=surface.specification_hash,
        search_space_hash=surface.search_space_hash,
        selectable_count=selectable, displayed_count=displayed,
        evidence_hash=claim.evidence_claim_hash, decision_hash=claim.decision_spec_hash,
        sort_key=key, null_family=v.get("null_family") or "OPPORTUNITY_LEVEL")

    _touch(s)
    s.search_run("combolab_v2", artifact.selectable_count, artifact.search_space_hash,
                 artifact.displayed_count)
    # one exposure per authorised row, because one row is one claim made available
    for cid in artifact.authorised_ids:
        row_claim = ClaimIdentity(
            estimand="incremental_return_pp", outcome=v.get("outcome_metric") or "median_return",
            horizon=v.get("horizon") or "20", population=v.get("universe") or "russell",
            conditioning_hash=f"{surface.claim_hash}:{cid}",
            feature_rule_hash=v.get("conditioning_feature") or "rsi_14",
            support_policy_hash=v.get("support_cutoff") or "100",
            null_family=v.get("null_family") or "OPPORTUNITY_LEVEL",
            decision_policy_version=surface.decision_policy_hash)
        s.execute(row_claim)
        s.expose(row_claim)

    _RUNS[run_id] = artifact
    view = SR.to_view(artifact, surface.specification_hash, claim.evidence_claim_hash,
                      claim.decision_spec_hash)
    return {"run": view.as_dict(), "session": asdict(to_view(s))}


def get_run(sid: str, run_id: str) -> dict:
    """Re-read an existing run. Freshness is recomputed against the session as it is NOW."""
    s = _get(sid)
    artifact = _RUNS.get(run_id)
    if artifact is None:
        raise KeyError(run_id)
    surface = _surface(sid, s)
    claim = _claim_from(surface)
    view = SR.to_view(artifact, surface.specification_hash, claim.evidence_claim_hash,
                      claim.decision_spec_hash)
    return {"run": view.as_dict(), "session": asdict(to_view(s))}


def promote(sid: str, run_id: str, claim_id: str) -> dict:
    """Acting on a row. A stale table may be read and may not be promoted."""
    s = _get(sid)
    artifact = _RUNS.get(run_id)
    if artifact is None:
        raise KeyError(run_id)
    surface = _surface(sid, s)
    claim = _claim_from(surface)
    view = SR.to_view(artifact, surface.specification_hash, claim.evidence_claim_hash,
                      claim.decision_spec_hash)
    SR.assert_promotable(view)   # SyntheticEvidenceActionError, then StaleSearchRunError
    if claim_id not in artifact.authorised_ids:
        raise SR.ExposureAuthorisationError(
            f"{claim_id} was ranked but never authorised for display, so nobody saw it and it "
            f"cannot be promoted from this run.")
    return {"promoted": claim_id, "run_id": run_id, "session": asdict(to_view(s))}


def preview_parameter(sid: str, parameter_id: str, new_value: str) -> dict:
    """A ChangePlan, pinned to the state and the registry it was computed under."""
    s = _get(sid)
    plan = PSURF.plan_for(sid, s._state_hash(), _surface(sid, s), parameter_id, new_value,
                          state=s.state, caps=SURFACE_CAPS)
    _PLANS[plan.plan_hash] = plan
    return {"plan": plan.as_dict()}


def _check_plan(s: ResearchSession, plan_hash: str, parameter_id: str, new_value: str):
    """A commit must be the transition that was approved, not merely the same request."""
    if not plan_hash:
        return None
    plan = _PLANS.get(plan_hash)
    if plan is None:
        raise PSURF.StaleChangePlanError(
            f"plan {plan_hash} is unknown here. It was issued against a state this process no "
            f"longer holds, so what it promised cannot be checked. Take a fresh preview.")
    if plan.prior_state_hash != s._state_hash():
        raise PSURF.StaleChangePlanError(
            f"the preview was computed at state {plan.prior_state_hash} and the session is now "
            f"at {s._state_hash()}. The classifier is the same; the transition is not. What was "
            f"approved is not what would happen, so it is refused rather than recomputed.")
    if plan.parameter_registry_hash != PSURF.registry_hash():
        raise PSURF.StaleChangePlanError(
            f"the plan was classified under registry {plan.parameter_registry_hash} and this "
            f"process runs {PSURF.registry_hash()}. A knob's declared role changed between the "
            f"preview and the commit.")
    if plan.parameter_id != parameter_id or plan.new_value != PSURF.record(
            parameter_id).canonical(new_value):
        raise PSURF.StaleChangePlanError(
            f"plan {plan_hash} approves {plan.parameter_id}={plan.new_value}, and the commit "
            f"asks for {parameter_id}={new_value}")
    return plan


def set_parameter(sid: str, parameter_id: str, new_value: str,
                  space_size: int = 31, displayed: int = 5, plan_hash: str = "") -> dict:
    """Turn a knob. Behaviour comes from the role, so there is no per-parameter branch here."""
    s = _get(sid)
    plan = _check_plan(s, plan_hash, parameter_id, new_value)
    surface = _surface(sid, s)
    after, c = PSURF.apply(surface, parameter_id, new_value, state=s.state, caps=SURFACE_CAPS)
    if plan is not None:
        _PLANS.pop(plan_hash, None)          # single use; a second commit needs a fresh preview
    _SURFACES[sid] = after

    if c["no_op"] or c["multiplicity_effect"] == "NONE":
        # A view. It is recorded nowhere, because recording it would make a free action cost a
        # ledger entry and the whole point of the role is that it does not.
        return {"session": asdict(to_view(s)), "classification": c,
                "surface": dict(after.values), "recorded": "NO"}

    s.change_parameter(parameter_id, c["old_claim_hash"], c["new_claim_hash"],
                       value=c["new_value"], role=c["role"])

    if c["role"] in (PSURF.SEARCH_SPACE_CHANGE, PSURF.SELECTION_PATH_CHANGE):
        s.search_run("combolab_v2", int(new_value) if str(new_value).isdigit() else space_size,
                     c["new_search_space_hash"], displayed)
    else:
        claim = _claim_from(after)
        s.execute(claim)
        s.expose(claim)
        s.search_run("combolab_v2", space_size, "3600ae3dd52a25e6", displayed)
    _touch(s)
    return {"session": asdict(to_view(s)), "classification": c,
            "surface": dict(after.values), "recorded": "YES", "plan_hash": plan_hash}


# ── preregistration and the way back out of it ──────────────────────────────
SPACE = ("combolab_v2", 31, "3600ae3dd52a25e6")


def register(sid: str, validation: dict, horizon: str = "20", tolerance: str = "5") -> dict:
    """Declare the space AND the evidence boundary, then freeze. All three or none.

    The validation window is named here, before any result exists, and the two fields that decide
    FORWARD — the clock and the source cutoff — are filled in by the server on the way past.
    """
    s = _get(sid)
    v = dict(validation)
    v["purpose"] = VALIDATION
    dev = DataAccessSpec.from_dict(s.access_spec or DEFAULT_DEV_SPEC)
    val = DataAccessSpec.from_dict(v)
    b = freeze_boundary(dev, val, now=_now(), catalog=CATALOG)   # raises if the source is mute

    s.assert_registerable_shape()   # everything except the boundary, before anything is written
    s.declare_evidence_boundary(b.as_dict())
    s.declare_search_space(*SPACE)
    s.register(claim_hash=_claim(horizon, tolerance).claim_hash)
    return {"session": asdict(to_view(s)), "boundary": b.as_dict()}


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


def validate(sid: str, boundary_hash: str = "") -> dict:
    """Evaluate a registered session. Takes a session id and nothing else.

    It used to take a boundary, which meant the window could be chosen after the answer was
    known. The boundary now comes out of the frozen ledger; `boundary_hash` is accepted only so
    a caller can state which boundary it believes it is evaluating, and a mismatch is fatal.
    """
    s = _get(sid)
    if not s.boundary:
        return {"eligible": False, "status": "NO_BOUNDARY",
                "why": ("this session never froze an evidence boundary, so there is no declared "
                        "evidence to evaluate against")}
    b = EvidenceBoundary.from_dict(s.boundary)
    if boundary_hash and boundary_hash != b.boundary_hash:
        raise EvidenceBoundaryDriftError(
            f"session {sid} froze boundary {b.boundary_hash} and is being evaluated against "
            f"{boundary_hash}. The boundary declared is the boundary paid for; a different one "
            f"is a different study.")
    fam = ResearchFamily(s.family_id, LEDGER.read_all())
    v = fam.confirmatory(b)
    acc = fam.accounting()
    v["family_id"] = s.family_id
    v["k_family_selectable"] = acc.k_family_selectable
    v["k_family_selectable_is_bound"] = acc.k_family_selectable_is_bound
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
        "SyntheticEvidenceActionError": "NONE",
        "StaleSearchRunError": "RERUN",
        "StaleChangePlanError": "REPREVIEW",
        "ParameterSurfaceError": "FORK",
        "SessionStateError": "FORK",
        "CannotRegisterAfterExposureError": "NEW_SESSION",
    }.get(kind, "NONE")
    remedy = {
        "SyntheticEvidenceActionError":
            "These results are a fixture, not a finding. They can be read and re-run; carrying "
            "one outward into a verdict, a freeze or the book needs evidence a search produced.",
        "StaleSearchRunError":
            "These results were produced under a specification the session no longer has. They "
            "stay readable as history; run the search again to act on the current one.",
        "ExposureAuthorisationError":
            "That result was ranked but never made available, so nothing was seen and there is "
            "nothing to act on.",
        "StaleChangePlanError":
            "The session moved between the preview and this click, so the change you approved is "
            "not the change that would happen. Nothing was applied; take a fresh preview.",
        "ParameterSurfaceError":
            "This control is frozen with the study. Its role means turning it would change what "
            "is being claimed, so a registered session refuses it — fork into a new exploratory "
            "session to continue from here.",
        "EvidenceBoundaryDriftError":
            "This study froze the data it may be confirmed on. Evaluating it against a different "
            "window would be a different study; open a new session to ask that question.",
        "SourceUnavailableError":
            "The server could not establish this source's cutoff, and without it a validation "
            "window cannot be certified as forward. Nothing was frozen.",
        "EvidenceBoundaryError":
            "The declared boundary cannot support a confirmatory claim as stated.",
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
    # a read outside the declared spec. It is expressible on purpose: the whole reason the access
    # layer records rather than refuses is that overreach happens, and a system that cannot even
    # represent it cannot be tested against it.
    overreach_start: str = ""
    overreach_end: str = ""
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
    # what this session DECLARES it will read. What it actually reads is recorded by the access
    # layer, and the second one is what contaminates.
    source_id: str = "bars_1d"
    universe: str = "russell"
    window_start: str = ""
    window_end: str = ""


class RegisterBody(BaseModel):
    """Preregistration names the validation window. The clock and the cutoff are the server's."""
    validation_start: str
    validation_end: str
    source_id: str = "bars_1d"
    universe: str = "russell"
    horizon: str = "20"
    tolerance: str = "5"
    idempotency_key: str = ""


class ParameterBody(BaseModel):
    parameter_id: str
    new_value: str
    space_size: int = 31
    displayed: int = 5
    # the plan the user actually approved. Empty is accepted for the preview endpoint and for
    # callers that do not preview; a plan that IS supplied must still be current.
    plan_hash: str = ""
    idempotency_key: str = ""


class SearchBody(BaseModel):
    sort_key: str = ""


class PromoteBody(BaseModel):
    run_id: str
    claim_id: str


class ValidateBody(BaseModel):
    # optional, and only so a caller can state which boundary it thinks it is evaluating; a
    # mismatch is fatal rather than accepted
    boundary_hash: str = ""


def build_router():
    from fastapi import APIRouter, HTTPException

    router = APIRouter(prefix="/api/studio/session", tags=["studio-session"])

    @router.post("/create")
    def _create(b: CreateBody | None = None):
        sp = None
        if b and b.window_start and b.window_end:
            sp = {"source_id": b.source_id, "universe": b.universe,
                  "start": b.window_start, "end": b.window_end}
        return create(b.family_id if b else "", sp)

    # Declared BEFORE `/{sid}`, and that is not style. FastAPI matches routes in declaration
    # order, so a path parameter declared first swallows every literal that comes after it —
    # GET /parameters answered "no session parameters" until this moved up.
    @router.get("/parameters")
    def _parameters():
        return parameters()

    @router.get("/{sid}/parameters")
    def _session_parameters(sid: str):
        try:
            return parameters(sid)
        except KeyError:
            raise HTTPException(404, f"no session {sid}")

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
                                       b.space_size, b.displayed,
                                       over=((b.overreach_start, b.overreach_end)
                                             if b.overreach_start and b.overreach_end else ())))
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
    def _register(sid: str, b: RegisterBody):
        try:
            return _idempotent(
                b.idempotency_key, "register", sid,
                {"validation_start": b.validation_start, "validation_end": b.validation_end},
                lambda: register(sid, {"source_id": b.source_id, "universe": b.universe,
                                       "start": b.validation_start, "end": b.validation_end},
                                 b.horizon, b.tolerance))
        except KeyError as e:
            raise HTTPException(404, str(e))
        except RS.IdempotencyConflictError as e:
            raise HTTPException(409, refusal(e))
        except (EvidenceBoundaryError, SourceUnavailableError) as e:
            raise HTTPException(409, refusal(e))
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

    @router.post("/{sid}/parameter/preview")
    def _param_preview(sid: str, b: ParameterBody):
        try:
            return preview_parameter(sid, b.parameter_id, b.new_value)
        except KeyError as e:
            raise HTTPException(404, str(e))

    @router.post("/{sid}/parameter")
    def _param_set(sid: str, b: ParameterBody):
        try:
            return _idempotent(b.idempotency_key, "parameter", sid,
                               {"parameter_id": b.parameter_id, "new_value": b.new_value},
                               lambda: set_parameter(sid, b.parameter_id, b.new_value,
                                                     b.space_size, b.displayed, b.plan_hash))
        except KeyError as e:
            raise HTTPException(404, str(e))
        except RS.IdempotencyConflictError as e:
            raise HTTPException(409, refusal(e))
        except PSURF.StaleChangePlanError as e:
            raise HTTPException(409, refusal(e))
        except (PSURF.ParameterSurfaceError, SessionStateError) as e:
            raise HTTPException(409, refusal(e))

    @router.post("/{sid}/search")
    def _search(sid: str, b: SearchBody | None = None):
        try:
            return search(sid, b.sort_key if b else "")
        except KeyError as e:
            raise HTTPException(404, str(e))

    @router.get("/{sid}/run/{run_id}")
    def _run(sid: str, run_id: str):
        try:
            return get_run(sid, run_id)
        except KeyError as e:
            raise HTTPException(404, str(e))

    @router.post("/{sid}/promote")
    def _promote(sid: str, b: PromoteBody):
        try:
            return promote(sid, b.run_id, b.claim_id)
        except KeyError as e:
            raise HTTPException(404, str(e))
        except (SR.SyntheticEvidenceActionError, SR.StaleSearchRunError,
                SR.ExposureAuthorisationError) as e:
            raise HTTPException(409, refusal(e))

    @router.get("/{sid}/family")
    def _family(sid: str):
        try:
            return family(sid)
        except KeyError:
            raise HTTPException(404, f"no session {sid}")

    @router.post("/{sid}/validate")
    def _validate(sid: str, b: ValidateBody | None = None):
        try:
            return validate(sid, b.boundary_hash if b else "")
        except KeyError:
            raise HTTPException(404, f"no session {sid}")
        except EvidenceBoundaryDriftError as e:
            raise HTTPException(409, refusal(e))

    return router
