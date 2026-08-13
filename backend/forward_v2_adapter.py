"""FORWARD ENGINE WIRING · ForwardV2Adapter v1, frozen before any novel evidence exists.

`evaluate_forward(..., estimator=callback)` was too wide a contract. A caller could hand it any
estimator at all, which means the statistical choice was still open at the moment the data
arrives — and once the first forward outcomes exist we would know exactly where support and the
bootstrap hurt. An "engineering" adjustment to the adapter at that point is an outcome-informed
statistical choice wearing overalls. So the callback is gone:

    ForwardEvaluationSpec → ForwardV2Adapter → the proven v2 kernel

THE ADAPTER HAS NO STATISTICAL FREEDOM. Estimand, claim universe, population rule, support
policy, replicate count, uncomputable policy, decision kernel and ranking are constants in this
file, hashed into `FORWARD_ADAPTER_SPEC.json`, and bound to the ForwardEvaluationSpec by hash.
There is no parameter to turn. What remains is plumbing, and plumbing is what an adapter is for.

SUPPORT IS BUILT ON THE FORWARD ROWS, AND ONLY THOSE. This was the decision that had to be made
now rather than in September. `E.Support` applies the frozen eligibility itself — n ≥ 100, dates
≥ 25, top-date share ≤ 0.20, per stratum, both arms — so building it on the forward slice gives
pure-forward eligibility AND a pure-forward estimand in one object, with no second
implementation of the rules to drift from the first. A cell that survives historically and not
forward simply is not in `sup.cells`, and that is the honest answer rather than a rescue.

The historical support keeps exactly one job: DESIGN. It fixes which 31 claims exist and by what
rule. It contributes no rows, no counts, and no weights to a forward estimate.

WHERE THE RNG IS KEYED, AND THE TRAP THERE. The stream root is
(forward spec hash, adapter hash, digest of the FORWARD rows). Not the snapshot's digest — key it
on the snapshot and attaching historical rows would move every interval while θ stayed still, so
the metamorphic test would pass on θ and fail on the CI, and the leak would look like Monte-Carlo
noise. Not the session, run id, or clock either: a technical re-run must return the identical
interval, or `revisit` becomes a way to shop for a wider one.

TWO PURPOSES, AND THE EXEMPTION IS THE DISQUALIFICATION. `FORWARD_ADAPTER_REGRESSION` may be
handed an explicit population from inside the exposed window — that is how the wiring is proven
against known-good computation without waiting for September. The same switch that permits it
stamps the artifact `evidence_role = ENGINEERING_FIXTURE`, `prospective_claim = false`, and
`may_enter_evidence_ledger = false`. One flag, both consequences, so no run can have the
convenience without the label.
"""
from __future__ import annotations

import hashlib
import json
import os

import numpy as np

import combolab_v2 as E
import combolab_v2_spec as V2
import forward_evaluation as FE
import historical_ranking_policy as RP
import v2_kernel as K

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "FORWARD_ADAPTER_SPEC.json")

ADAPTER_VERSION = "forward_v2_adapter_v1"

# every statistical decision, as a constant. None of these is a parameter.
ESTIMAND = V2.ESTIMAND
N_BOOT = 2000
DELTA_STAR = 0.50
RNG_POLICY_ID = "historical_research_rng_v1"
DECISION_POLICY_HASH = "verdict_v2"
UNCOMPUTABLE_POLICY = ("a replication whose arm is unrepresented returns NaN and is dropped by "
                       "the frozen bootstrap; the percentile is taken over the survivors")

# execution purposes
FIRST_PROSPECTIVE_EVALUATION = "FIRST_PROSPECTIVE_EVALUATION"
FORWARD_ADAPTER_REGRESSION = "FORWARD_ADAPTER_REGRESSION"
SYNTHETIC_WIRING_FIXTURE = "SYNTHETIC_WIRING_FIXTURE"

PURPOSES = {
    # purpose: (may take an explicit non-forward population, evidence_role, prospective_claim)
    FIRST_PROSPECTIVE_EVALUATION: (False, "PROSPECTIVE_FORWARD_EVIDENCE", True),
    FORWARD_ADAPTER_REGRESSION: (True, "ENGINEERING_FIXTURE", False),
    SYNTHETIC_WIRING_FIXTURE: (False, "ENGINEERING_FIXTURE", False),
}

INSUFFICIENT_FORWARD_SUPPORT = FE.INSUFFICIENT_FORWARD_SUPPORT
COMPUTABLE = FE.COMPUTABLE


class ForwardAdapterError(RuntimeError):
    """The adapter was asked for something it has no freedom to give."""


def _h(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


# ── the adapter's identity ──────────────────────────────────────────────────
def adapter_spec(*, forward_spec_hash: str) -> dict:
    spec = {
        "adapter_version": ADAPTER_VERSION,
        "binds_to_forward_evaluation_spec": forward_spec_hash,
        "estimand": ESTIMAND,
        "claim_universe": "the frozen 31 OPPORTUNITY_LEVEL cells; 6 DAY_LEVEL deferred",
        "population": "pure-forward rows only",
        "support_policy": ("v2 Support, rebuilt on the forward rows. Eligibility and estimand "
                           "share one implementation, so they cannot disagree"),
        "eligibility": dict(V2.ELIGIBILITY),
        "historical_support_role": "DESIGN ONLY — fixes which claims exist, contributes no rows",
        "bootstrap": {"rng_policy_id": RNG_POLICY_ID, "replicates": N_BOOT,
                      "keyed_on": "forward spec hash · adapter hash · digest of the FORWARD rows",
                      "not_keyed_on": "session, run id, wall clock, or the snapshot digest",
                      "uncomputable_policy": UNCOMPUTABLE_POLICY},
        "decision": {"kernel": "v2_kernel.verdict", "policy_hash": DECISION_POLICY_HASH,
                     "delta_star": DELTA_STAR},
        "ranking": {"policy_hash": RP.policy_hash(), "policy_version": RP.POLICY_VERSION,
                    "metric": "theta descending"},
        "historical_backfill": "FORBIDDEN",
        "statistical_freedom": "NONE — every choice above is a constant in forward_v2_adapter.py",
    }
    spec["adapter_hash"] = _h(spec)
    return spec


def freeze(spec: dict) -> dict:
    if os.path.exists(RECORD):
        existing = record()
        if existing["adapter_hash"] != spec["adapter_hash"]:
            raise ForwardAdapterError(
                f"an adapter is already frozen at {existing['adapter_hash']} and this one is "
                f"{spec['adapter_hash']}. Changing the computational path after registration is "
                f"the move this milestone exists to make impossible; register v2 alongside it.")
        return existing
    with open(RECORD, "w") as f:
        json.dump(spec, f, indent=1, sort_keys=True)
    return spec


def is_frozen() -> bool:
    return os.path.exists(RECORD)


def record() -> dict:
    with open(RECORD) as f:
        return json.load(f)


def assert_bound_to_forward_spec() -> dict:
    a = record()
    f = FE.record()
    if a["binds_to_forward_evaluation_spec"] != f["spec_hash"]:
        raise ForwardAdapterError(
            f"the adapter binds to forward spec {a['binds_to_forward_evaluation_spec']} and the "
            f"frozen spec is {f['spec_hash']}. Two halves of one frozen path cannot be from "
            f"different registrations.")
    return a


# ── the RNG, keyed on the forward rows and nothing situational ──────────────
class ForwardBootstrapRNGProvider:
    """Deterministic from the question and the FORWARD data. Not from when it was run."""

    POLICY_ID = RNG_POLICY_ID

    def __init__(self, keyed_root: str):
        self.keyed_root = keyed_root

    def semantic_key(self, cell) -> tuple:
        return (self.POLICY_ID, self.keyed_root, str(cell), "forward_claim_bootstrap")

    def open_stream(self, cell):
        h = hashlib.sha256("|".join(self.semantic_key(cell)).encode()).digest()
        return np.random.default_rng(int.from_bytes(h[:8], "big"))


def forward_rows_digest(y, dates) -> str:
    """The forward slice's own identity. Attached history is not in it, by construction."""
    v = np.ascontiguousarray(np.asarray(y, dtype=float))
    d = "|".join(str(x) for x in np.asarray(dates))
    return hashlib.sha256(v.tobytes() + d.encode()).hexdigest()[:32]


def diagnose_forward_support(dates, in_cell) -> dict:
    """REPORTING ONLY, and never a gate.

    `E.Support` is the single authority on eligibility. A second implementation that also decided
    would be two eligibility rules waiting to disagree, so this one only explains a decision
    already taken elsewhere.
    """
    d = np.asarray([str(x) for x in dates])
    m = np.asarray(in_cell, dtype=bool)
    out = {}
    for name, mask in (("treatment", m), ("control", ~m)):
        dd = d[mask]
        uniq, counts = np.unique(dd, return_counts=True)
        out[name] = {"n": int(mask.sum()), "dates": int(uniq.size),
                     "top_date_share": round(float(counts.max() / dd.size), 4) if dd.size else 1.0}
    return out


# ── the evaluation ──────────────────────────────────────────────────────────
def evaluate(*, O, dates, y, masks, cutoff: str, purpose: str,
             deferred_day_level=(), population=None, forward_spec_hash: str = "",
             adapter_hash: str = "") -> dict:
    """The whole forward path. No estimator argument, because there is no choice left to make."""
    if purpose not in PURPOSES:
        raise ForwardAdapterError(f"unknown execution purpose {purpose!r}")
    may_use_explicit_population, evidence_role, prospective = PURPOSES[purpose]

    overlap = sorted(set(masks) & set(deferred_day_level))
    if overlap:
        raise ForwardAdapterError(
            f"{len(overlap)} DAY_LEVEL claims reached the forward evaluation ({overlap[:3]}). "
            f"They are deferred by declaration, not filtered at the end, and a search space that "
            f"quietly regrew is not the one that was registered.")

    if population is not None and not may_use_explicit_population:
        raise ForwardAdapterError(
            f"{purpose} may not be handed an explicit population; the forward window is derived "
            f"from the frozen cutoff. Offering one is how a chosen slice would enter a run that "
            f"claims to be prospective.")

    if prospective:
        # WHEN we look is frozen too, and the gate is consulted here rather than remembered by
        # whoever runs it. The population is the look window — the FIRST N novel trading days —
        # so waiting longer adds no rows and delaying the look gains nothing.
        import forward_observation_policy as OP                       # noqa: PLC0415
        OP.assert_look_permitted(dates)
        idx = OP.look_population(dates)
    else:
        idx = (np.asarray(population, dtype=int) if population is not None
               else FE.forward_index(dates, cutoff))
    if prospective:
        FE.assert_pure_forward(dates, idx, cutoff)
    if idx.size == 0:
        raise ForwardAdapterError(
            f"the forward window after {cutoff} is empty. There is nothing prospective to "
            f"evaluate yet, and that is a state to wait in rather than to work around.")

    # ── the slice. Everything downstream sees only these rows ──────────────
    Of = O.iloc[idx].reset_index(drop=True)
    df = np.asarray([str(x) for x in np.asarray(dates)[idx]])
    yf = np.asarray(y, dtype=float)[idx]
    mf = {c: np.asarray(m, dtype=bool)[idx] for c, m in masks.items()}

    sup = E.Support(Of, df, mf, verbose=False)
    _, gi = np.unique(df, return_inverse=True)
    n_dates = int(gi.max() + 1)
    meta = sup.meta.set_index("cell") if len(sup.meta) else None

    root = hashlib.sha256(
        f"{forward_spec_hash}|{adapter_hash}|{forward_rows_digest(yf, df)}".encode()).hexdigest()
    rng = ForwardBootstrapRNGProvider(root)

    cells, computable = {}, {}
    for cell in sorted(masks):
        row = {"cell_identity": cell, "arms": diagnose_forward_support(df, mf[cell])}
        if cell not in sup.cells:
            row["status"] = INSUFFICIENT_FORWARD_SUPPORT
            row["reason"] = ("the frozen eligibility is not met on the forward rows. Adding "
                             "history to reach the floor is contamination, not a repair")
            cells[cell] = row
            continue
        f = K.Frozen(sup, cell, gi)
        est = K.bootstrap_cell(yf, f, cell, rng.open_stream(cell), n_dates, N_BOOT, meta, ESTIMAND)
        theta = float(f.theta(yf))
        row.update({"status": COMPUTABLE,
                    "theta_hex": theta.hex(),
                    "interval_hex": [float(est.ci_low).hex(), float(est.ci_high).hex()],
                    "verdict": K.verdict(est, cell, meta, DELTA_STAR),
                    "support_identity": f.hash,
                    "eligible_setups": int(meta.loc[cell, "eligible_setups"]),
                    "eligible_dates": int(meta.loc[cell, "eligible_dates"]),
                    "eligible_cell_opportunities":
                        int(meta.loc[cell, "eligible_cell_opportunities"])})
        cells[cell] = row
        computable[cell] = {"theta": theta}

    ranking = RP.rank(computable) if computable else []
    art = {
        "artifact": "ForwardEvaluationArtifact",
        "adapter_version": ADAPTER_VERSION, "adapter_hash": adapter_hash,
        "forward_evaluation_spec_hash": forward_spec_hash,
        "execution_purpose": purpose,
        "evidence_role": evidence_role,
        "prospective_claim": prospective,
        "may_enter_evidence_ledger": prospective,
        "cutoff": cutoff,
        "forward_rows": int(idx.size),
        "forward_window": [min(df.tolist()), max(df.tolist())],
        "forward_rows_digest": forward_rows_digest(yf, df),
        "rng_policy_id": RNG_POLICY_ID, "rng_root": root[:16],
        "registered_claims": len(masks), "computable": len(computable),
        "insufficient": len(masks) - len(computable),
        "deferred_day_level": sorted(deferred_day_level),
        "replicates": N_BOOT, "delta_star": DELTA_STAR,
        "decision_policy_hash": DECISION_POLICY_HASH,
        "ranking_policy_hash": RP.policy_hash(),
        "ranking": ranking,
        "cells": cells,
        "historical_backfill": "FORBIDDEN",
    }
    art["artifact_hash"] = _h({k: v for k, v in art.items() if k != "artifact_hash"})
    return art


def assert_not_evidence(artifact: dict) -> None:
    """An engineering run proves the wiring and never enters the evidence record."""
    if artifact["may_enter_evidence_ledger"]:
        return
    raise ForwardAdapterError(
        f"{artifact['execution_purpose']} produces {artifact['evidence_role']} and cannot be "
        f"recorded as a forward observation. It ran to show the adapter computes what the kernel "
        f"computes, and it was allowed to see rows a prospective run may not.")


def run_evaluation(*, O, dates, y, masks, purpose: str, deferred_day_level=(),
                   population=None) -> dict:
    """The only entry point that reads the frozen records rather than taking them as arguments."""
    a = assert_bound_to_forward_spec()
    f = FE.record()
    return evaluate(O=O, dates=dates, y=y, masks=masks, cutoff=f["data_cutoff_at_registration"],
                    purpose=purpose, deferred_day_level=deferred_day_level,
                    population=population, forward_spec_hash=f["spec_hash"],
                    adapter_hash=a["adapter_hash"])


if __name__ == "__main__":
    s = freeze(adapter_spec(forward_spec_hash=FE.record()["spec_hash"]))
    print("=" * 100, flush=True)
    print(f"  FORWARD V2 ADAPTER {s['adapter_hash']} · {s['adapter_version']}", flush=True)
    print("=" * 100, flush=True)
    print(f"  binds to spec        {s['binds_to_forward_evaluation_spec']}", flush=True)
    print(f"  estimand             {s['estimand']}", flush=True)
    print(f"  population           {s['population']}", flush=True)
    print(f"  support              {s['support_policy']}", flush=True)
    print(f"  historical support   {s['historical_support_role']}", flush=True)
    print(f"  bootstrap            {s['bootstrap']['replicates']} reps · "
          f"{s['bootstrap']['rng_policy_id']}", flush=True)
    print(f"    keyed on           {s['bootstrap']['keyed_on']}", flush=True)
    print(f"    NOT keyed on       {s['bootstrap']['not_keyed_on']}", flush=True)
    print(f"  decision             {s['decision']['kernel']} · δ*={s['decision']['delta_star']}",
          flush=True)
    print(f"  ranking              {s['ranking']['policy_hash']} · {s['ranking']['metric']}",
          flush=True)
    print(f"  statistical freedom  {s['statistical_freedom']}", flush=True)
