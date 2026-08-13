"""Gate 3C, frozen before the evidence · ForwardEvaluationSpec v1.

`compare_lineage` and `novel_window` answer "is there unexposed data here". They do not answer
the question that actually decides standing:

    WHAT DATA IS THE ESTIMATE COMPUTED ON?

    exposed window     2021-05-27 → 2026-08-06
    new snapshot       2021-05-27 → 2026-09-30
    novel window       2026-08-07 → 2026-09-30      ← correct, and not sufficient

If θ is then recomputed over the whole 2021–2026 snapshot, almost all of the estimator's weight
sits on observations the ranking policy was chosen after seeing. The result would carry a
beautiful provenance chain and still be mostly old evidence. A new snapshot does not clean the
rows it contains.

SO v1 IS PURE FORWARD EVALUATION, AND POOLING IS FORBIDDEN. An estimator that combines exposed
history with novel observations is defensible in principle — and would have to declare, in
advance and in detail, how the update works and what it licenses. Writing that after the new
outcomes exist is choosing the combination rule with the answer visible. v1 does not offer it.

    estimation population = observations strictly after the frozen cutoff

The frozen historical support is design metadata: it fixes WHICH 31 claims are evaluated and by
what rule. It contributes no observations to the estimate.

WHICH MAKES UNDERPOWERED THE NORMAL RESULT, and that is not a defect to be engineered around. A
few weeks of forward data will fail `n ≥ 100 · dates ≥ 25 · top-date share ≤ 0.20` for most
cells, and the honest output is

    31 registered · 7 computable · 24 INSUFFICIENT_FORWARD_SUPPORT

The one thing that must never happen is the repair that suggests itself immediately — "add some
history so the sample is big enough". That is contamination with a helpful face, so
`historical_backfill: FORBIDDEN` is in the spec and `ProspectiveEvidenceContaminationError` is
raised by the evaluator rather than left as a rule someone remembers.

THE INVARIANT THAT PROVES THIS IS REALLY FORWARD EVALUATION. Take the same future rows, attach
different amounts of exposed history to the snapshot around them, and the prospective result must
be identical. If it moves, the old data was inside the estimate and the provenance was decorative.
"""
from __future__ import annotations

import hashlib
import json
import os

import numpy as np

import combolab_v2_spec as V2
import historical_ranking_policy as RP

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "FORWARD_EVALUATION_SPEC.json")

SPEC_VERSION = "forward_evaluation_spec_v1"

COMPUTABLE = "COMPUTABLE"
INSUFFICIENT_FORWARD_SUPPORT = "INSUFFICIENT_FORWARD_SUPPORT"


class ForwardSpecError(RuntimeError):
    """The forward specification is absent, incomplete, or being written too late."""


class ProspectiveEvidenceContaminationError(RuntimeError):
    """An exposed observation reached a population that claims to be prospective."""


def _h(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


# ── the spec ────────────────────────────────────────────────────────────────
def build_spec(*, registered_at: str, registered_by: str, data_cutoff: str,
               source_lineage: dict, evidence_fingerprint: str, registered_claims,
               deferred_day_level, note: str = "") -> dict:
    """Everything that must be fixed before a single novel outcome exists.

    `data_cutoff` is server-derived from the source, never supplied by whoever wants the
    evaluation to come out well — the same rule and the same reason as `EvidenceBoundary`: the
    field that decides whether a window is forward is the field a caller most benefits from
    misstating.
    """
    spec = {
        "spec_version": SPEC_VERSION,
        "registered_at": registered_at,
        "registered_by": registered_by,
        "data_cutoff_at_registration": data_cutoff,
        "source_lineage": source_lineage,
        "evidence_fingerprint_exposed_before_this_spec": evidence_fingerprint,
        "registered_claims": sorted(registered_claims),
        "registered_claim_count": len(list(registered_claims)),
        "deferred_day_level": sorted(deferred_day_level),
        "null_family": "OPPORTUNITY_LEVEL",

        "evaluation_population": ("observations strictly after data_cutoff_at_registration, and "
                                  "present in no exposed EvidenceFingerprint"),
        "estimator_input": "FORWARD_ONLY",
        "frozen_historical_support_is": ("design metadata — it fixes which claims are evaluated "
                                         "and by what rule, and contributes no observations"),

        "ranking_policy_hash": RP.policy_hash(),
        "ranking_policy_version": RP.POLICY_VERSION,
        "ranking": "theta descending",
        "decision_policy_hash": "verdict_v2",
        "eligibility": dict(V2.ELIGIBILITY),
        "insufficient_support_result": INSUFFICIENT_FORWARD_SUPPORT,

        "historical_backfill": "FORBIDDEN",
        "pooled_old_and_new_estimator": "FORBIDDEN_IN_V1",
        "why_pooling_is_forbidden": ("an estimator that merges exposed history with novel "
                                     "observations must declare its update rule and inferential "
                                     "interpretation in advance. Writing that rule after the new "
                                     "outcomes exist is choosing the combination with the answer "
                                     "in view"),
        "underpowered_is_a_result": ("if the forward window cannot meet the frozen eligibility, "
                                     "the cell returns INSUFFICIENT_FORWARD_SUPPORT. Adding "
                                     "history to reach the floor is contamination, not a repair"),
        "note": note,
    }
    spec["spec_hash"] = _h(spec)
    return spec


def freeze(spec: dict) -> dict:
    """Write it once. A second freeze with different content is refused, not merged."""
    if os.path.exists(RECORD):
        existing = record()
        if existing["spec_hash"] != spec["spec_hash"]:
            raise ForwardSpecError(
                f"a forward evaluation spec is already frozen at {existing['spec_hash']} and this "
                f"one is {spec['spec_hash']}. A specification that can be edited after "
                f"registration is not frozen; register v2 alongside it and say which evidence "
                f"each one governs.")
        return existing
    with open(RECORD, "w") as f:
        json.dump(spec, f, indent=1, sort_keys=True)
    return spec


def is_frozen() -> bool:
    return os.path.exists(RECORD)


def record() -> dict:
    with open(RECORD) as f:
        return json.load(f)


def assert_frozen_before(observation_dates) -> None:
    """The whole point of a prospective spec: it predates the outcomes it will be judged on."""
    if not is_frozen():
        raise ForwardSpecError(
            "no forward evaluation spec is frozen, so nothing here can be prospective. Freezing "
            "one after seeing the observations is the retroactive registration this system "
            "refuses everywhere else.")
    cutoff = record()["data_cutoff_at_registration"]
    early = [str(d) for d in observation_dates if str(d) <= cutoff]
    if early:
        raise ProspectiveEvidenceContaminationError(
            f"{len(early)} of the observations offered as prospective are dated on or before the "
            f"frozen cutoff {cutoff} (earliest {min(early)}). They existed when the specification "
            f"was written, so they are not evidence this specification predates.")


# ── population selection ────────────────────────────────────────────────────
def forward_index(dates, cutoff: str) -> np.ndarray:
    """Strictly after. The one selection rule, and it takes no options."""
    d = np.asarray([str(x) for x in dates])
    return np.flatnonzero(d > cutoff)


def assert_pure_forward(dates, population, cutoff: str) -> None:
    """The refusal that makes `historical_backfill: FORBIDDEN` a mechanism rather than a note."""
    d = np.asarray([str(x) for x in dates])
    chosen = d[np.asarray(population, dtype=int)]
    bad = chosen[chosen <= cutoff]
    if bad.size:
        raise ProspectiveEvidenceContaminationError(
            f"{bad.size} observation(s) on or before the frozen cutoff {cutoff} reached a "
            f"population declared prospective (earliest {min(bad.tolist())}). The estimate would "
            f"rest "
            f"partly on rows the ranking policy was chosen after seeing, and no amount of "
            f"provenance around it would make the result forward evidence. If the forward window "
            f"is too small, the answer is {INSUFFICIENT_FORWARD_SUPPORT}, never more history.")


# ── eligibility, on the forward window only ─────────────────────────────────
def assess_forward_support(dates, in_cell) -> dict:
    """The frozen floors, applied to the forward slice. Failing them is a result, not an error."""
    d = np.asarray([str(x) for x in dates])
    m = np.asarray(in_cell, dtype=bool)
    el = V2.ELIGIBILITY
    arms = {}
    for name, mask in (("treatment", m), ("control", ~m)):
        dd = d[mask]
        uniq, counts = np.unique(dd, return_counts=True)
        arms[name] = {"n": int(mask.sum()), "dates": int(uniq.size),
                      "top_date_share": float(counts.max() / dd.size) if dd.size else 1.0}
    reasons = []
    for name, a in arms.items():
        if a["n"] < el["n_min"]:
            reasons.append(f"{name} n={a['n']} < {el['n_min']}")
        if a["dates"] < el["dates_min"]:
            reasons.append(f"{name} dates={a['dates']} < {el['dates_min']}")
        if a["top_date_share"] > el["max_single_date_share"]:
            reasons.append(f"{name} top-date share={a['top_date_share']:.2f} > "
                           f"{el['max_single_date_share']}")
    return {"status": INSUFFICIENT_FORWARD_SUPPORT if reasons else COMPUTABLE,
            "arms": arms, "reasons": reasons}


# ── the evaluation ──────────────────────────────────────────────────────────
def evaluate_forward(*, dates, membership: dict, estimator, cutoff: str,
                     population=None) -> dict:
    """Evaluate the registered claims on the forward window and nothing else.

    `population` exists so that a caller CAN offer one, and so that offering a contaminated one
    is refused loudly rather than silently honoured. `estimator(cell, idx)` receives only indices
    already proven to be forward.
    """
    idx = forward_index(dates, cutoff) if population is None else np.asarray(population, dtype=int)
    assert_pure_forward(dates, idx, cutoff)

    d = np.asarray([str(x) for x in dates])[idx]
    cells, computable = {}, {}
    for cell in sorted(membership):
        m = np.asarray(membership[cell], dtype=bool)[idx]
        sup = assess_forward_support(d, m)
        row = {"cell_identity": cell, "status": sup["status"], "reasons": sup["reasons"],
               "arms": sup["arms"]}
        if sup["status"] == COMPUTABLE:
            theta = float(estimator(cell, idx))
            row["theta"] = theta
            computable[cell] = {"theta": theta}
        cells[cell] = row

    return {"spec_version": SPEC_VERSION, "cutoff": cutoff,
            "forward_rows": int(idx.size),
            "forward_window": (min(d.tolist()), max(d.tolist())) if idx.size else ("", ""),
            "registered": len(membership), "computable": len(computable),
            "insufficient": len(membership) - len(computable),
            "ranking": RP.rank(computable) if computable else [],
            "ranking_policy_hash": RP.policy_hash(),
            "cells": cells,
            "historical_backfill": "FORBIDDEN",
            "population_hash": _h(sorted(int(i) for i in idx))}


# ── freezing it, from what is already on record ─────────────────────────────
def freeze_from_registry(*, registered_at: str, registered_by: str, note: str = "") -> dict:
    """The cutoff is read from the measured lineage, not typed in by whoever wants a result."""
    import evidence_fingerprint as FP                                 # noqa: PLC0415
    import exposed_evidence as EE                                     # noqa: PLC0415
    reg = FP.FingerprintRegistry.load()
    if not reg.entries:
        raise ForwardSpecError(
            "there is no registered evidence fingerprint, so there is nothing to be forward OF")
    e = reg.entries[0]
    lin = e["data_lineage"]
    if not lin.get("coverage_end"):
        raise ForwardSpecError(
            "the exposed lineage has no coverage window, so a cutoff cannot be derived from the "
            "source. A caller-supplied cutoff is exactly the field this refuses to accept.")
    return freeze(build_spec(
        registered_at=registered_at, registered_by=registered_by,
        data_cutoff=lin["coverage_end"], source_lineage=lin,
        evidence_fingerprint=e["fingerprint"],
        registered_claims=EE.claim_cells(),
        deferred_day_level=EE._read(EE.QUALIFICATION)["deferred_day_level"], note=note))


if __name__ == "__main__":
    import sys                                                        # noqa: PLC0415
    at = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
    s = freeze_from_registry(
        registered_at=at, registered_by="gate-3c-freeze",
        note="frozen before any novel observation exists; v1 is pure forward evaluation and "
             "offers no pooled old+new estimator")
    print("=" * 100, flush=True)
    print(f"  FORWARD EVALUATION SPEC {s['spec_hash']} · {s['spec_version']}", flush=True)
    print("=" * 100, flush=True)
    print(f"  registered at        {s['registered_at']}", flush=True)
    print(f"  data cutoff          {s['data_cutoff_at_registration']}  (server-derived)", flush=True)
    print(f"  evidence predating   {s['evidence_fingerprint_exposed_before_this_spec']}", flush=True)
    print(f"  registered claims    {s['registered_claim_count']} OPPORTUNITY_LEVEL", flush=True)
    print(f"  deferred DAY_LEVEL   {len(s['deferred_day_level'])}", flush=True)
    print(f"  ranking policy       {s['ranking_policy_hash']} · {s['ranking']}", flush=True)
    print(f"  eligibility          n≥{s['eligibility']['n_min']} · "
          f"dates≥{s['eligibility']['dates_min']} · top-date share ≤"
          f"{s['eligibility']['max_single_date_share']}", flush=True)
    print(f"  estimator input      {s['estimator_input']}", flush=True)
    print(f"  historical backfill  {s['historical_backfill']}", flush=True)
    print(f"  pooled estimator     {s['pooled_old_and_new_estimator']}", flush=True)
    print(f"\n  evaluation population: {s['evaluation_population']}", flush=True)
    print(f"  underpowered: {s['underpowered_is_a_result']}", flush=True)
