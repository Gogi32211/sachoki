"""4A · `run_v2()` — orchestration, and deliberately boring.

Everything statistical is already either frozen upstream or proven in `v2_kernel`, so this layer
is allowed exactly four kinds of work:

    1  validation and compatibility
    2  construction of already-proven objects
    3  orchestration of already-proven kernel calls
    4  packaging provenance and results into an artifact

If a new statistical branch, a fallback, or a formula appears in this file's diff, the
orchestration claim is already broken — that is what makes "boring" a specification rather than
a style note.

SEALED ONLY, ON PURPOSE. `HISTORICAL_RESEARCH` is a known mode with a defined provider and it is
refused here, because the existence of an enum member is not a licence to run it. Two gates
stand between the type and the mode: sealed execution equivalence, and the registered real-y
numerical qualification. Until both pass, asking for the historical mode returns
`HistoricalApplicationNotQualifiedError` rather than a result, and the system is expected to
live in that intermediate state — extraction qualified, application not.

SUPPORT IS BUILT ONCE. Three call sites each constructing their own `Support` is three slightly
different eligibility computations waiting to happen; the artifact would still look coherent.
One instance, and every downstream object takes it.

CELL ORDER IS RECORDED, not chosen. The old path iterated `sup.cells` in whatever order the
support produced; a tidy `sorted()` here would be a different RNG consumption order for the same
set. `executed_cell_order_hash` makes "same set, different order" visible instead of silent.

NOTHING ABOUT DISPLAY. No `displayed_top_k`, no ranking, no exposure. The engine computes what
the registered space contains, and `rank_and_authorise` decides what a person may see. A
structural test asserts those names do not appear in this signature, because the first UI
parameter accepted here would put presentation policy inside the estimator.
"""
from __future__ import annotations

import hashlib
import json

import numpy as np

import combolab_v2 as E
import v2_engine_contract as C
import v2_kernel as K

ENGINE_VERSION = "combolab_v2_engine_v1"


class HistoricalApplicationNotQualifiedError(RuntimeError):
    """The mode exists and has not earned the right to run."""


class CellResultIdentityMismatchError(RuntimeError):
    """The results do not belong to the cells they are filed under."""


class PartialExecutionError(RuntimeError):
    """Fewer cells completed than the registered space contains."""


def _h(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


def cell_order_hash(cells) -> str:
    """Order, not membership. `sorted()` here would be a different execution."""
    return hashlib.sha256("|".join(cells).encode()).hexdigest()[:16]


# ── A · compatibility, before anything is computed ──────────────────────────
def assert_compatible(spec: C.V2RunSpec, context: C.ExecutionContext, outcome: C.OutcomeVector,
                      *, registered_space_hash: str, expected_alignment: str, n_rows: int
                      ) -> None:
    """Every refusal that must happen before a single bootstrap draw."""
    if context.execution_mode == C.HISTORICAL_RESEARCH:
        raise HistoricalApplicationNotQualifiedError(
            "HISTORICAL_RESEARCH is defined and not yet permitted. Two gates stand in front of "
            "it: sealed execution equivalence for the extraction, and the registered real-y "
            "numerical qualification for the application. Extraction being qualified does not "
            "open the historical client — that is the whole point of keeping the two axes "
            "apart.")

    C.assert_outcome_allowed(context.execution_mode, outcome)
    C.assert_row_alignment(outcome, expected_alignment, n_rows)

    if outcome.source_snapshot_id != context.data_snapshot_id:
        raise C.OutcomeSemanticsError(
            f"the outcome was built from snapshot {outcome.source_snapshot_id} and the execution "
            f"declares {context.data_snapshot_id}. One of them is describing a different dataset.")

    if spec.search_space_manifest_hash != registered_space_hash:
        raise C.SpecIdentityError(
            f"the spec names manifest {spec.search_space_manifest_hash} and the registered space "
            f"is {registered_space_hash}. Equal counts would not have caught this.")


# ── B–H · the pipeline ──────────────────────────────────────────────────────
def run_v2(spec: C.V2RunSpec, context: C.ExecutionContext, outcome: C.OutcomeVector, *,
           support, frozen_cells, meta, n_dates, cell_order, rng_provider,
           registered_space_hash: str, executed_space_hash: str, expected_alignment: str,
           n_rows: int, n_boot: int, delta_star: float,
           deferred_claim_families: dict | None = None) -> C.EngineResultArtifact:
    """Orchestration only. Every statistical call below is a kernel function proven elsewhere.

    The already-constructed `support`, `frozen_cells` and `meta` are passed in rather than built
    here on purpose: constructing them inside would give this function a second, independent
    eligibility computation, and the artifact would look just as coherent either way.
    """
    assert_compatible(spec, context, outcome, registered_space_hash=registered_space_hash,
                      expected_alignment=expected_alignment, n_rows=n_rows)

    y = np.asarray(outcome.values)
    order = tuple(cell_order)
    results = []
    for cell in order:                       # the order given, never re-sorted
        f = frozen_cells[cell]
        theta = f.theta(y)                                          # kernel
        est = K.bootstrap_cell(y, f, cell, rng_provider.open_stream(cell), n_dates, n_boot,
                               meta, spec.estimand_version)          # kernel
        final = K.verdict(est, cell, meta, delta_star)               # kernel

        row = meta.loc[cell]
        results.append(C.CellResult(
            cell_identity=cell,
            evidence_claim_hash=_h({"cell": cell, "estimand": spec.estimand_version,
                                    "support": spec.support_policy_hash,
                                    "outcome": spec.outcome_definition}),
            decision_spec_hash=_h({"null": spec.null_family,
                                   "policy": spec.decision_policy_hash,
                                   "delta_star": delta_star}),
            computation=C.CellComputation(
                theta_hex=float(theta).hex(),
                interval_hex=(float(est.ci_low).hex(), float(est.ci_high).hex()),
                bootstrap_summary={"n_boot": int(n_boot), "level": float(est.level),
                                   "method": est.method, "cluster_unit": est.cluster_unit},
                support_identity=f.hash,
                eligibility={"eligible_setups": int(row["eligible_setups"]),
                             "eligible_cell_opportunities":
                                 int(row["eligible_cell_opportunities"]),
                             "eligible_dates": int(row["eligible_dates"])}),
            decision=C.CellDecision(stages=(final,), final_verdict=final,
                                    decision_policy_hash=spec.decision_policy_hash)))

    assert_results_match_cells(order, results)
    assert_complete(order, results)

    return C.EngineResultArtifact(
        engine_version=ENGINE_VERSION, execution_mode=context.execution_mode,
        spec_hash=spec.spec_hash, input_outcome_hash=outcome.outcome_hash,
        data_snapshot_id=context.data_snapshot_id,
        registered_search_space_hash=registered_space_hash,
        executed_search_space_hash=executed_space_hash,
        executed_cell_order_hash=cell_order_hash(order),
        estimand_version=spec.estimand_version, support_policy_hash=spec.support_policy_hash,
        null_family=spec.null_family, decision_policy_hash=spec.decision_policy_hash,
        bootstrap_policy_hash=spec.bootstrap_policy_hash,
        rng_policy_id=context.rng_policy.policy_id,
        rng_provenance_hash=context.rng_provenance_hash,
        cell_results=tuple(results),
        deferred_claim_families=dict(deferred_claim_families or {}))


# ── the two orchestration failures worth naming ─────────────────────────────
def assert_results_match_cells(order, results) -> None:
    """Count is not identity. Thirty-one results for thirty-one cells can still be misfiled."""
    got = tuple(r.cell_identity for r in results)
    if got != tuple(order):
        wrong = [(a, b) for a, b in zip(order, got) if a != b][:3]
        raise CellResultIdentityMismatchError(
            f"results are filed against the wrong cells: {wrong}. The counts matched, which is "
            f"exactly why this is checked by identity and in order.")


def assert_complete(order, results) -> None:
    """A partial execution is a failed execution, not a shorter one.

    Publishing 30 of 31 would be a result set the multiplicity was never paid on, and the one
    that dropped out is not a random one — it is the awkward one.
    """
    if len(results) != len(order):
        raise PartialExecutionError(
            f"{len(results)} of {len(order)} registered cells completed. The artifact is FAILED; "
            f"a shorter result set is not a smaller version of this run, it is a different "
            f"search that nobody registered.")


def default_deferred() -> dict:
    """The six DAY_LEVEL claims are absent by declaration, not by filtering."""
    return {"DAY_LEVEL": {"count": 6,
                          "reason": "INCOMPATIBLE_NULL_FAMILY / ComboLab v2.1",
                          "status": "NOT_IN_SEARCH_SPACE"}}
