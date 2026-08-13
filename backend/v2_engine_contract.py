"""The types that must exist before `run_v2()` does, so one array cannot mean two things.

`Frozen.theta(y)` is about to be handed two outcome vectors with nothing in common but their
shape:

    SEALED_ACCEPTANCE     Y = μ_setup + γ_date + ε, with a planted δ
    HISTORICAL_RESEARCH   Y = O["ret"] · 100

Left as a bare `np.ndarray`, an artifact can be computationally perfect and scientifically
mislabelled, and nothing in the code would notice. The largest risk in this extraction was never
a Python bug; it is giving the same array two different scientific meanings. So the meaning
travels with the values and the modes refuse each other's data.

WHAT / UNDER WHAT / WHAT CAME OUT — three objects, deliberately not one:

    V2RunSpec           WHAT is computed. No seed, no snapshot, no session, no timestamp.
    ExecutionContext    UNDER WHAT conditions. Mode, snapshot, code, RNG.
    EngineResultArtifact WHAT came out. Server-only, and not a SearchRunArtifact.

The last distinction matters now rather than later: the engine computes, and `rank_and_authorise`
decides what a person may see. Merging them would put exposure policy inside the estimator.

RNG SPLITS INTO POLICY AND MATERIAL. A policy is the derivation scheme; material is the seeds it
derives from. `SealedRNGReuseError` checks the PROVENANCE of the material, not whether two
integers happen to be equal — an accidental numeric collision is not a breach, and copying the
sealed namespace through an arithmetic transform very much is.

ARM COMPUTABILITY IS A FACT ABOUT GEOMETRY, NOT ABOUT OUTCOMES. Whether a resample retains both
arms of a stratum depends on membership and drawn dates. If that answer ever changes between a
synthetic and a real `y`, the defect is not "real returns are awkward" — it is outcome-dependent
eligibility, which is far more serious. `arm_computability_mask` therefore does not accept `y`
at all, and cannot be made to.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

import numpy as np

# ── outcome semantics ───────────────────────────────────────────────────────
SYNTHETIC_COMPOSITION_WORLD = "SYNTHETIC_COMPOSITION_WORLD"
SYNTHETIC_INJECTED_WORLD = "SYNTHETIC_INJECTED_WORLD"
HISTORICAL_OBSERVED = "HISTORICAL_OBSERVED"

SYNTHETIC_KINDS = (SYNTHETIC_COMPOSITION_WORLD, SYNTHETIC_INJECTED_WORLD)

SEALED_ACCEPTANCE = "SEALED_ACCEPTANCE"
HISTORICAL_RESEARCH = "HISTORICAL_RESEARCH"
EXECUTION_MODES = (SEALED_ACCEPTANCE, HISTORICAL_RESEARCH)

# Which outcome each mode is allowed to see. Both directions are closed: a sealed acceptance run
# on real returns would spend the seal on data it was never about, and a historical run on a
# composition world would produce a beautifully-formed HISTORICAL_RESEARCH artifact describing
# noise.
MODE_ALLOWS_OUTCOME = {
    SEALED_ACCEPTANCE: set(SYNTHETIC_KINDS),
    HISTORICAL_RESEARCH: {HISTORICAL_OBSERVED},
}


class OutcomeSemanticsError(RuntimeError):
    """An outcome vector used in a mode that must not see it."""


class SealedRNGReuseError(RuntimeError):
    """Sealed randomness reached a run that is not the sealed acceptance."""


class SpecIdentityError(RuntimeError):
    """A specification field that must not vary, varied."""


def _h(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


@dataclass(frozen=True)
class OutcomeVector:
    """Values plus what they mean. The meaning is not optional and does not travel separately."""
    values: np.ndarray
    outcome_id: str
    outcome_semantics: str          # prose: what this vector IS
    source_kind: str                # SYNTHETIC_* | HISTORICAL_OBSERVED
    source_snapshot_id: str
    units: str
    row_alignment_hash: str         # ties these values to a specific row ordering
    construction_hash: str          # how they were produced

    def __post_init__(self):
        if self.source_kind not in (*SYNTHETIC_KINDS, HISTORICAL_OBSERVED):
            raise OutcomeSemanticsError(f"unknown source_kind {self.source_kind!r}")
        for f in ("outcome_id", "outcome_semantics", "source_snapshot_id", "units",
                  "row_alignment_hash", "construction_hash"):
            if not getattr(self, f):
                raise OutcomeSemanticsError(
                    f"OutcomeVector.{f} is empty. An outcome vector without it can be attached "
                    f"to the wrong rows, or to the wrong story about where it came from.")

    @property
    def is_synthetic(self) -> bool:
        return self.source_kind in SYNTHETIC_KINDS

    @property
    def outcome_hash(self) -> str:
        return _h({"id": self.outcome_id, "kind": self.source_kind,
                   "snap": self.source_snapshot_id, "align": self.row_alignment_hash,
                   "constr": self.construction_hash, "n": int(np.asarray(self.values).size)})


def assert_outcome_allowed(mode: str, outcome: OutcomeVector) -> None:
    if mode not in EXECUTION_MODES:
        raise OutcomeSemanticsError(f"unknown execution mode {mode!r}")
    allowed = MODE_ALLOWS_OUTCOME[mode]
    if outcome.source_kind in allowed:
        return
    raise OutcomeSemanticsError(
        f"{mode} may not compute on a {outcome.source_kind} outcome. "
        + ("A sealed acceptance run measures an instrument on worlds whose truth is known; "
           "pointing it at real returns spends the seal on a question it was not registered to "
           "answer." if mode == SEALED_ACCEPTANCE else
           "A historical run computed on a composition world would produce a perfectly formed "
           "HISTORICAL_RESEARCH artifact describing nothing but noise."))


def assert_row_alignment(outcome: OutcomeVector, expected_alignment: str, n_rows: int) -> None:
    """Same length is not the same rows. Alignment is checked, never assumed."""
    if outcome.row_alignment_hash != expected_alignment:
        raise OutcomeSemanticsError(
            f"outcome {outcome.outcome_id} is aligned to {outcome.row_alignment_hash} and the "
            f"population is {expected_alignment}. Two vectors of equal length can index "
            f"different rows, and nothing downstream would notice.")
    if int(np.asarray(outcome.values).size) != n_rows:
        raise OutcomeSemanticsError(
            f"outcome has {np.asarray(outcome.values).size} values for {n_rows} rows")


# ── what is computed ────────────────────────────────────────────────────────
@dataclass(frozen=True)
class V2RunSpec:
    """WHAT. Nothing here may vary between two runs of the same question."""
    estimand_version: str
    search_space_manifest_hash: str
    support_policy_hash: str
    null_family: str
    decision_policy_hash: str
    bootstrap_policy_hash: str
    outcome_definition: str          # the DEFINITION, not the values

    @property
    def spec_hash(self) -> str:
        return _h({"e": self.estimand_version, "m": self.search_space_manifest_hash,
                   "s": self.support_policy_hash, "n": self.null_family,
                   "d": self.decision_policy_hash, "b": self.bootstrap_policy_hash,
                   "o": self.outcome_definition})


# ── under what conditions ───────────────────────────────────────────────────
@dataclass(frozen=True)
class RNGPolicy:
    policy_id: str
    derivation_scheme: str


@dataclass(frozen=True)
class RNGMaterial:
    """Where the randomness comes from, and — the part that matters — where it came FROM.

    `provenance` is lineage, not a value. Two runs may legitimately land on the same integer;
    that is coincidence. A run that took sealed material and transformed it is a breach even
    though no integer matches, which is why the check reads this field and not the numbers.
    """
    namespace: str
    provenance: str                  # e.g. "freeze_commit:abc123" | "derived:spec+snapshot"
    frozen_seeds: tuple = field(default_factory=tuple)
    keyed_root: str = ""

    @property
    def is_sealed_lineage(self) -> bool:
        p = self.provenance.lower()
        return "sealed" in p or "freeze_commit" in p or self.namespace.startswith("sealed")

    @property
    def material_hash(self) -> str:
        return _h({"ns": self.namespace, "prov": self.provenance,
                   "seeds": list(self.frozen_seeds), "root": self.keyed_root})


SEALED_RNG = RNGPolicy("sealed_acceptance_rng_v1", "seeds derived from the freeze commit")
RESEARCH_RNG = RNGPolicy(
    "historical_research_rng_v1",
    "keyed root over (engine namespace, spec_hash, snapshot, claim, replicate) — never the "
    "session, the run id, the rerun number or the clock")


@dataclass(frozen=True)
class ExecutionContext:
    """UNDER WHAT. It carries no statistical decision; every field here is execution semantics."""
    execution_mode: str
    data_snapshot_id: str
    code_hash: str
    rng_policy: RNGPolicy
    rng_material: RNGMaterial
    execution_id: str

    def __post_init__(self):
        if self.execution_mode not in EXECUTION_MODES:
            raise SpecIdentityError(f"unknown execution mode {self.execution_mode!r}")
        if self.execution_mode == HISTORICAL_RESEARCH and self.rng_material.is_sealed_lineage:
            raise SealedRNGReuseError(
                f"execution {self.execution_id} runs as HISTORICAL_RESEARCH with RNG material "
                f"of sealed lineage ({self.rng_material.provenance!r}). Sealed randomness "
                f"belongs to the one acceptance it was derived for; reusing it makes an "
                f"interactive session look like a sealed run, and 'the old seeds are already "
                f"reproducible' is exactly the argument that would retire the second policy.")
        if self.execution_mode == SEALED_ACCEPTANCE and not self.rng_material.is_sealed_lineage:
            raise SealedRNGReuseError(
                f"a SEALED_ACCEPTANCE run was given RNG material of lineage "
                f"{self.rng_material.provenance!r}, which is not sealed. The acceptance is only "
                f"an acceptance if its randomness could not have been chosen after looking.")

    @property
    def rng_provenance_hash(self) -> str:
        return _h({"policy": self.rng_policy.policy_id,
                   "material": self.rng_material.material_hash})


def research_rng_material(spec: V2RunSpec, snapshot_id: str) -> RNGMaterial:
    """Deterministic from the QUESTION and the DATA, and from nothing that a rerun changes.

    Not the session, not the run id, not the rerun number, not the clock. If any of those
    entered, a technical re-run would return a different interval and `revisit` — which the
    ledger charges nothing for — would quietly become a way to shop for Monte-Carlo noise.
    """
    root = _h({"engine": "combolab_v2", "spec": spec.spec_hash, "snapshot": snapshot_id})
    return RNGMaterial(namespace="combolab_v2_research", provenance="derived:spec+snapshot",
                       keyed_root=root)


# ── arm computability: geometry only ────────────────────────────────────────
def arm_computability_mask(treated_rows: np.ndarray, comparator_rows: np.ndarray,
                           row_dates: np.ndarray, drawn_dates: np.ndarray) -> bool:
    """Does this resample retain both arms? Note the signature: there is no `y`.

    Whether a stratum survives a resample is a fact about membership and drawn dates. If the
    answer differed between a synthetic and a real outcome, the defect would not be "real
    returns are awkward" — it would be eligibility depending on the outcome, and that is a far
    more serious thing to discover. The function cannot express that dependence.
    """
    keep = np.isin(row_dates, drawn_dates)
    return bool(np.asarray(treated_rows)[keep].any() and
                np.asarray(comparator_rows)[keep].any())


# ── what came out ───────────────────────────────────────────────────────────
@dataclass(frozen=True)
class CellComputation:
    """What was measured. No policy here."""
    theta_hex: str
    interval_hex: tuple
    bootstrap_summary: dict
    support_identity: str
    eligibility: dict


@dataclass(frozen=True)
class CellDecision:
    """What the measurement was called. No estimate here."""
    stages: tuple
    final_verdict: str
    decision_policy_hash: str


@dataclass(frozen=True)
class CellResult:
    """Identity, computation and decision, kept apart for the same reason as everywhere else.

    Merging them is how "k = 7" stopped answering whether seven effects were looked at or one
    effect under seven rules. The same split, one level down.
    """
    cell_identity: str
    evidence_claim_hash: str
    decision_spec_hash: str
    computation: CellComputation
    decision: CellDecision


@dataclass(frozen=True)
class EngineResultArtifact:
    """Server-only, and NOT a SearchRunArtifact.

    No ranking here. ComboLab v2 has no registered production ranking policy for historical
    cells — the sealed acceptance ranked planted needles inside a capability experiment — and
    inventing one to fill a table would break the first rule of this extraction. Ranking arrives
    when a frozen policy for it is found or registered, not before.
    """
    engine_version: str
    execution_mode: str
    spec_hash: str
    input_outcome_hash: str
    data_snapshot_id: str
    registered_search_space_hash: str
    executed_search_space_hash: str
    executed_cell_order_hash: str
    estimand_version: str
    support_policy_hash: str
    null_family: str
    decision_policy_hash: str
    bootstrap_policy_hash: str
    rng_policy_id: str
    rng_provenance_hash: str
    cell_results: tuple = field(default_factory=tuple)
    deferred_claim_families: dict = field(default_factory=dict)
    numerical_integrity_ref: str = ""

    def __post_init__(self):
        if self.registered_search_space_hash != self.executed_search_space_hash:
            raise SpecIdentityError(
                f"registered space {self.registered_search_space_hash} and executed space "
                f"{self.executed_search_space_hash} differ. Equal COUNTS would not have caught "
                f"this: 31 == 31 says nothing about which 31.")

    @property
    def computation_hash(self) -> str:
        return _h({"cells": [(c.cell_identity, c.computation.theta_hex,
                              c.computation.support_identity, c.decision.final_verdict)
                             for c in self.cell_results]})

    @property
    def artifact_hash(self) -> str:
        return _h({"comp": self.computation_hash, "spec": self.spec_hash,
                   "outcome": self.input_outcome_hash, "mode": self.execution_mode,
                   "rng": self.rng_provenance_hash, "snap": self.data_snapshot_id})

    def legacy_projection(self) -> dict:
        """The new artifact seen through the old result's schema. A test helper, not a feature.

        The new artifact is richer — provenance fields exist now that did not before — so
        demanding byte-identical output would force the new object to be shaped by the old one.
        Projecting instead keeps the claim clean: the artifact grew, and its projection onto the
        previously proven semantics is identical.
        """
        return {c.cell_identity: {"theta_hex": c.computation.theta_hex,
                                  "ci_low_hex": c.computation.interval_hex[0],
                                  "ci_high_hex": c.computation.interval_hex[1],
                                  "verdict": c.decision.final_verdict,
                                  "support_hash": c.computation.support_identity}
                for c in self.cell_results}
