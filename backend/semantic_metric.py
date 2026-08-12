"""The object the UI renders instead of a number.

The backend already refuses to compare two correctly-computed numbers that describe different
probabilistic experiments — `sampling_target.py` raises on it. On a screen there is no such
protection: two figures side by side read as one metric with two values, and a day of contract
work is undone by a layout decision.

So the first code of Analytic Studio is not a page. It is this.

WHY THE CORE IS SMALL. A thirty-five-field contract gets filled halfway and then routed around,
and within a month it is a DTO with nulls where the meaning used to be. `Estimate` in
studio_verdict works because it has nine required fields and every one is enforced at
construction. Eight here, same rule: absent core → raise, never a default.

WHY TWO STATUS AXES. `VALID/INVALID` is about the experiment; `BUILD/NULL/UNRESOLVED/...` is
about the conclusion. Merged into one field, the state that matters most cannot be expressed:

    integrity  = INVALID
    conclusion = BUILD

which must render as "BUILD NOT INTERPRETABLE", never as a green BUILD. That is the
INTEGRITY-outranks-CAPABILITY rule, moved from a protocol document into a type.

WHY EXTENSIONS ARE NOT `None`. "We do not know the cluster unit" and "a deterministic quantity
has no cluster unit" are different facts, and `None` erases the difference. Extensions carry
Known / Unknown(reason) / NotApplicable(reason), and the renderer shows the reason.

WHY CONDITIONING IS STRUCTURED. On one ACHR bar, `RSI 60 ±5` gives 3,940 observations and
`±1` gives 108. The tolerance was chosen by the analyst and appeared nowhere except a caption.
`RSI ±1` and `RSI ±5` are two different research decisions, not two renderings of one query, so
each ConditioningSpec carries its own hash.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from sampling_target import SamplingTarget

# ── the four kinds of number, kept apart because they look identical ─────────
DETERMINISTIC = "DETERMINISTIC"   # a property of a construction: θ = 0 exactly, n = 31
DESCRIPTIVE = "DESCRIPTIVE"       # what was observed: median return, prevalence
INFERENTIAL = "INFERENTIAL"       # an estimate with uncertainty: θ̂ = +0.0038, a CI, an FWER
DECISION = "DECISION"             # a verdict: BUILD, NULL, DEFERRED
SEMANTIC_TYPES = (DETERMINISTIC, DESCRIPTIVE, INFERENTIAL, DECISION)

VALID, INVALID = "VALID", "INVALID"
NONE_, NULL_, UNRESOLVED, BUILD, REJECT, DEFERRED = (
    "NONE", "NULL", "UNRESOLVED", "BUILD", "REJECT", "DEFERRED")
CONCLUSIONS = (NONE_, NULL_, UNRESOLVED, BUILD, REJECT, DEFERRED)


class SemanticContractError(AssertionError):
    """A number was constructed without the meaning required to display it."""


class ComparisonSemanticsError(AssertionError):
    """Two metrics were combined arithmetically when only co-display is permitted."""


# ── absent values are states, not holes ──────────────────────────────────────
@dataclass(frozen=True)
class Known:
    value: object

    def __str__(self):
        return str(self.value)


@dataclass(frozen=True)
class Unknown:
    reason: str

    def __str__(self):
        return f"unknown — {self.reason}"


@dataclass(frozen=True)
class NotApplicable:
    reason: str

    def __str__(self):
        return f"n/a — {self.reason}"


def _opt(x, who: str):
    if isinstance(x, (Known, Unknown, NotApplicable)):
        return x
    raise SemanticContractError(
        f"{who} must be Known / Unknown(reason) / NotApplicable(reason), not {type(x).__name__}. "
        f"'we do not know' and 'this quantity does not have one' are different facts and None "
        f"erases the difference.")


# ── conditioning, with its tolerance as data ─────────────────────────────────
@dataclass(frozen=True)
class ConditioningSpec:
    """One condition, including the tolerance the analyst chose.

    `RSI 45 ±1` and `RSI 45 ±5` are different research decisions — on one real bar they select
    108 rows and 3,940 — so they hash differently and can never silently pass for each other.
    """
    feature: str
    operator: str                 # EQUALS | WITHIN | BETWEEN | IN | IS
    center: object = None
    tolerance: object = None
    unit: str = ""

    def __post_init__(self):
        if not self.feature or not self.operator:
            raise SemanticContractError("a condition needs a feature and an operator")
        if self.operator == "WITHIN" and self.tolerance is None:
            raise SemanticContractError(
                f"{self.feature}: WITHIN without a tolerance is a caption, not a condition — "
                f"the width is the choice that moves n")

    @property
    def hash(self) -> str:
        return hashlib.sha256(json.dumps(
            [self.feature, self.operator, self.center, self.tolerance, self.unit],
            sort_keys=True, default=str).encode()).hexdigest()[:12]

    def __str__(self):
        if self.operator == "WITHIN":
            return f"{self.feature} {self.center} ±{self.tolerance}{self.unit}"
        if self.operator == "EQUALS":
            return f"{self.feature} = {self.center}"
        return f"{self.feature} {self.operator} {self.center}"


@dataclass(frozen=True)
class Provenance:
    experiment_id: str
    spec_hash: str
    code_hash: str
    data_version: str

    def __post_init__(self):
        for f in ("experiment_id", "spec_hash", "code_hash", "data_version"):
            if not getattr(self, f):
                raise SemanticContractError(f"provenance.{f} is empty — provenance is not optional")


# ── the object ───────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class SemanticMetric:
    value: float
    semantic_type: str
    estimand: str
    sampling_target: SamplingTarget
    conditioning: tuple
    provenance: Provenance
    integrity_status: str = VALID
    conclusion_status: str = NONE_
    units: str = ""
    uncertainty: object = field(default_factory=lambda: NotApplicable("not set"))
    population: object = field(default_factory=lambda: NotApplicable("not set"))
    label: str = ""

    def __post_init__(self):
        if self.semantic_type not in SEMANTIC_TYPES:
            raise SemanticContractError(
                f"semantic_type {self.semantic_type!r} is not one of {SEMANTIC_TYPES}. A "
                f"construction property and a statistical estimate look identical on screen; "
                f"this field is what tells them apart.")
        if not self.estimand:
            raise SemanticContractError("estimand is empty — a value without one is a decoration")
        if not isinstance(self.sampling_target, SamplingTarget):
            raise SemanticContractError(
                "sampling_target must be a SamplingTarget. Which probabilistic experiment a "
                "number belongs to is the thing the whole layer exists to keep straight.")
        if not isinstance(self.provenance, Provenance):
            raise SemanticContractError("provenance is required")
        if self.integrity_status not in (VALID, INVALID):
            raise SemanticContractError("integrity_status must be VALID or INVALID")
        if self.conclusion_status not in CONCLUSIONS:
            raise SemanticContractError(f"conclusion_status must be one of {CONCLUSIONS}")
        if self.semantic_type == DETERMINISTIC and not isinstance(
                self.uncertainty, NotApplicable):
            raise SemanticContractError(
                "a DETERMINISTIC quantity may not carry uncertainty — if it has an interval it "
                "is an estimate and belongs to INFERENTIAL")
        if self.semantic_type == INFERENTIAL and isinstance(self.uncertainty, NotApplicable):
            raise SemanticContractError(
                "an INFERENTIAL quantity must say something about its uncertainty, even if that "
                "is Unknown(reason). A point estimate presented bare is how R6 passed 67-75% of "
                "the time on noise.")
        _opt(self.uncertainty, "uncertainty")
        _opt(self.population, "population")

    # ── display ─────────────────────────────────────────────────────────────
    @property
    def renderable_conclusion(self) -> str:
        """INTEGRITY outranks CAPABILITY, enforced here rather than remembered."""
        if self.integrity_status == INVALID and self.conclusion_status != NONE_:
            return f"{self.conclusion_status} — NOT INTERPRETABLE (integrity INVALID)"
        return self.conclusion_status

    @property
    def conditioning_hash(self) -> str:
        return hashlib.sha256("|".join(c.hash for c in self.conditioning).encode()).hexdigest()[:12]

    def inspect(self) -> dict:
        """Everything the drawer shows. No field is computed here that a producer should own."""
        return {
            "value": self.value, "units": self.units, "label": self.label,
            "semantic_type": self.semantic_type, "estimand": self.estimand,
            "sampling_target": str(self.sampling_target),
            "conditioning": [str(c) for c in self.conditioning],
            "conditioning_hash": self.conditioning_hash,
            "uncertainty": str(self.uncertainty), "population": str(self.population),
            "integrity": self.integrity_status, "conclusion": self.renderable_conclusion,
            "provenance": {"experiment": self.provenance.experiment_id,
                           "spec": self.provenance.spec_hash,
                           "code": self.provenance.code_hash,
                           "data": self.provenance.data_version},
        }

    def __str__(self):
        v = f"{self.value:.4g}{self.units}"
        return f"{self.label or self.estimand}: {v} [{self.semantic_type}]"


# ── the guard ────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ComparisonResult:
    comparable: bool
    reason_code: str
    detail: str
    left: str = ""
    right: str = ""


def can_compare(a: SemanticMetric, b: SemanticMetric) -> ComparisonResult:
    """May these two be combined ARITHMETICALLY — difference, ratio, mean, shared axis?

    Co-display is a different question and is always allowed: putting G1's 0.065 beside G2's
    0.685 is exactly how the reader learns that one null model is calibrated and the other is
    not. What is forbidden is subtracting them, averaging them, calling one 10.5× the other, or
    drawing them on one bar chart labelled FWER — because they have different denominators and
    different null models.

    The decision lives here and not in a renderer. A renderer executes it.
    """
    if a.integrity_status == INVALID or b.integrity_status == INVALID:
        return ComparisonResult(False, "INTEGRITY_INVALID",
                                "an experiment that was not the registered experiment has no "
                                "operating characteristics to compare")
    if a.semantic_type != b.semantic_type:
        return ComparisonResult(False, "SEMANTIC_TYPE_MISMATCH",
                                f"{a.semantic_type} against {b.semantic_type} — a construction "
                                f"property and an estimate are not two values of one quantity",
                                a.semantic_type, b.semantic_type)
    if a.estimand != b.estimand:
        return ComparisonResult(False, "ESTIMAND_MISMATCH",
                                "different estimands answer different questions",
                                a.estimand, b.estimand)
    if a.units != b.units:
        return ComparisonResult(False, "UNIT_MISMATCH", "different units", a.units, b.units)
    from sampling_target import compatible
    ok, why = compatible(a.sampling_target, b.sampling_target)
    if not ok:
        return ComparisonResult(False, "SAMPLING_TARGET_MISMATCH", why,
                                str(a.sampling_target), str(b.sampling_target))
    if a.conditioning_hash != b.conditioning_hash:
        return ComparisonResult(False, "CONDITIONING_MISMATCH",
                                "different conditioning, including tolerances — the width of a "
                                "band is a research decision, not a caption",
                                a.conditioning_hash, b.conditioning_hash)
    return ComparisonResult(True, "OK", "same experiment, same question, same conditioning")


def assert_comparable(a: SemanticMetric, b: SemanticMetric) -> None:
    r = can_compare(a, b)
    if not r.comparable:
        raise ComparisonSemanticsError(
            f"{r.reason_code}: {r.detail}"
            + (f"\n  left  {r.left}\n  right {r.right}" if r.left or r.right else ""))


def difference(a: SemanticMetric, b: SemanticMetric) -> float:
    assert_comparable(a, b)
    return a.value - b.value


def ratio(a: SemanticMetric, b: SemanticMetric) -> float:
    assert_comparable(a, b)
    return a.value / b.value


def co_display(a: SemanticMetric, b: SemanticMetric) -> dict:
    """Side by side is allowed. What travels with it is the boundary."""
    r = can_compare(a, b)
    return {"left": a.inspect(), "right": b.inspect(), "comparable": r.comparable,
            "boundary": None if r.comparable else
            f"{r.reason_code} — do not combine or directly compare: {r.detail}"}
