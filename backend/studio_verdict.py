"""verdict_v2 — one decision pipeline, and gates that cannot decide from a bare number.

Two frozen control samples say the same thing about v1, in two independent branches:

    return_v1   true +0.60pp   estimator resolved it 95.8%   verdict passed  0.0%   cause L2a
    risk_v1     true +12pp     estimator resolved it  100%   verdict passed  0.0%   cause R2

Not an unlucky constant twice. The magnitude thresholds were written independently of the
economic policy AND of what the estimator can resolve, so the engine could see an effect and
forbid itself from calling it one. R2 was worse than L2a because RR ≥ 1.5 moves with the
baseline: 0.30pp of required effect at p₀ = 0.59%, 17.1pp at p₀ = 34.21%, for the same claim.

A third defect turned up while measuring the second: R6 read

    R6 = (r_diff >= -0.25)

which is a point estimate compared to a margin — not a test of anything. It passed 67-75% of
the time when nothing had been done to returns at all, deciding on the noise of a date split.
That is the same sin as the ±0.3 interval I typed by hand two hours earlier: a statistical
decision taken from a number that does not carry the information the decision needs. So this
module adds the fourth contract layer.

    data contract          can the data be trusted
    computation invariants is the arithmetic what it claims
    report provenance      did anything print a statistic no estimator produced
    DECISION PROVENANCE    may this gate decide from what it was handed

Not every gate needs an interval. `n_eff >= 80`, `oos_reserved`, `no_lookahead` are
deterministic facts and a scalar is the right input. The rule is narrower and exact:

    A gate making an INFERENCE CLAIM about a population parameter must take an
    uncertainty-bearing Estimate. Handed a float, it raises.

Four layers, asked in order, and each can answer UNRESOLVED separately — which is why the
final status carries a blocking_layer instead of one overloaded word:

    EVIDENCE        does the effect exist
    MATERIALITY     is it large enough to act on, by a policy fixed in advance
    VALIDITY        stability, independence, OOS, multiplicity — governance keeps its veto
    NON-INFERIORITY what the action costs on the other side

RETURN and RISK differ only in the estimand — ΔReturn against ΔTEL — and share every word of
the decision language.

ACCEPTANCE CRITERION, written before the code and unchanged since: no result may end in
"evidence YES, materiality YES, every validity gate PASS, rejected on magnitude". Magnitude IS
materiality here, so the state is structurally unreachable rather than merely discouraged.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import numpy as np

PASS, FAIL, UNRESOLVED = "PASS", "FAIL", "UNRESOLVED"
EVIDENCE, MATERIALITY, VALIDITY, NON_INFERIORITY = (
    "EVIDENCE", "MATERIALITY", "VALIDITY", "NON_INFERIORITY")


class DecisionContractError(AssertionError):
    """A gate was asked to decide from something that cannot support the decision."""


# ── the object every inference gate must be handed ───────────────────────────
@dataclass(frozen=True)
class Estimate:
    """An effect, its uncertainty, and where both came from.

    Every field is required. The ones that look like bookkeeping are not: `estimand` stops a
    median lift being compared against a tail-loss margin, `cluster_unit` records what the
    interval treated as independent, and `n_eff` against `n_raw` is the difference between
    45,146 trades and 7,142 facts.
    """
    estimate: float
    ci_low: float
    ci_high: float
    level: float
    estimand: str
    method: str
    cluster_unit: str
    n_raw: int
    n_eff: int

    def __post_init__(self):
        if not (self.ci_low <= self.estimate <= self.ci_high):
            raise DecisionContractError(
                f"{self.estimand}: estimate {self.estimate:+.3f} lies outside its own "
                f"interval [{self.ci_low:+.3f},{self.ci_high:+.3f}]")
        if not 0.5 < self.level < 1.0:
            raise DecisionContractError(f"confidence level {self.level} is not a level")
        for f in ("estimand", "method", "cluster_unit"):
            if not getattr(self, f):
                raise DecisionContractError(f"{f} is empty — provenance is not optional")

    @property
    def width(self) -> float:
        return self.ci_high - self.ci_low

    def __str__(self):
        return (f"{self.estimate:+.3f} [{self.ci_low:+.3f},{self.ci_high:+.3f}] "
                f"({self.level:.0%}, {self.method}, by {self.cluster_unit}, "
                f"n {self.n_raw:,}→{self.n_eff:,})")


def _require(obj, who: str) -> Estimate:
    if isinstance(obj, Estimate):
        return obj
    raise DecisionContractError(
        f"{who} was handed {type(obj).__name__} {obj!r}. This gate makes an inference claim "
        f"about a population parameter and cannot decide from a point value — R6 did exactly "
        f"that and passed 67-75% of the time on pure noise. Pass an Estimate.")


# ── named RNG substreams ─────────────────────────────────────────────────────
class Substreams:
    """One generator per named component, derived from a single seed.

    Adding a new stochastic step must not shift the draws of the existing ones, or a paired
    v1-vs-v2 comparison at "the same seed" silently stops being paired. Deriving each stream
    from hash(seed, name) makes the sequences independent of one another and of the order in
    which they are created.
    """

    def __init__(self, seed: int):
        self.seed = int(seed)
        self._cache: dict = {}

    def __call__(self, name: str) -> np.random.Generator:
        if name not in self._cache:
            h = hashlib.sha256(f"{self.seed}:{name}".encode()).digest()
            self._cache[name] = np.random.default_rng(int.from_bytes(h[:8], "big"))
        return self._cache[name]


# ── the four layers ──────────────────────────────────────────────────────────
def evidence(est) -> tuple[str, str]:
    """Does the effect exist at all? The interval either excludes zero or it does not."""
    e = _require(est, "evidence()")
    if e.ci_low > 0 or e.ci_high < 0:
        return PASS, f"interval excludes zero: {e}"
    return UNRESOLVED, f"interval contains zero: {e}"


def materiality(est, delta_star: float, *, direction: str = "positive") -> tuple[str, str]:
    """Is it big enough to act on — by an equivalence test, not a threshold on a point.

    PASS        the whole interval is beyond δ* on the useful side
    FAIL        the whole interval lies inside [−δ*, +δ*]: an effect of the size that would
                have interested us is EXCLUDED. This is the only honest NULL.
    UNRESOLVED  the interval straddles the boundary — the data cannot tell a meaningful
                effect from a negligible one, which is a different statement from "no effect"

    Replacing `est >= 1.0` with this is the whole of v2's magnitude logic. There is no second
    threshold anywhere, so a result cannot be rejected on size except here.
    """
    e = _require(est, "materiality()")
    if delta_star <= 0:
        raise DecisionContractError("delta_star must be positive and fixed in advance")
    inside = (-delta_star <= e.ci_low) and (e.ci_high <= delta_star)
    if inside:
        return FAIL, (f"equivalence established: interval inside ±{delta_star:.2f} — an "
                      f"effect worth acting on is excluded {e}")
    if direction == "positive" and e.ci_low > delta_star:
        return PASS, f"entirely beyond +{delta_star:.2f}: {e}"
    if direction == "negative" and e.ci_high < -delta_star:
        return PASS, f"entirely beyond −{delta_star:.2f}: {e}"
    if direction == "either" and (e.ci_low > delta_star or e.ci_high < -delta_star):
        return PASS, f"entirely beyond ±{delta_star:.2f}: {e}"
    return UNRESOLVED, (f"interval straddles ±{delta_star:.2f} — cannot separate meaningful "
                        f"from negligible: {e}")


def validity(**gates: bool) -> tuple[str, str]:
    """Deterministic facts about the study. Governance keeps its veto here, and should.

    Instability, concentration in one cluster, a contaminated OOS window, an undeclared search
    — all legitimate grounds to refuse a real, material effect. What v2 removes is only the
    right to refuse it for being 0.55 instead of 1.0.
    """
    bad = [k for k, v in gates.items() if v is False]
    unknown = [k for k, v in gates.items() if v is None]
    if bad:
        return FAIL, "failed: " + ", ".join(bad)
    if unknown:
        return UNRESOLVED, "not assessed: " + ", ".join(unknown)
    return PASS, "all validity gates pass"


def non_inferiority(cost_est, epsilon: float) -> tuple[str, str]:
    """What the action costs on the other side — one-sided, and honest about not knowing.

    `cost_est` is the effect on the quantity we are willing to give up (a veto's cost in
    return). PASS requires the lower bound to clear −ε: not "we failed to prove harm", but
    "we can exclude harm larger than the margin". Those are different sentences and v1
    conflated them.
    """
    e = _require(cost_est, "non_inferiority()")
    if epsilon <= 0:
        raise DecisionContractError("epsilon must be positive and fixed in advance")
    if e.ci_low > -epsilon:
        return PASS, f"harm beyond −{epsilon:.2f} excluded: {e}"
    if e.ci_high < -epsilon:
        return FAIL, f"harm beyond −{epsilon:.2f} established: {e}"
    return UNRESOLVED, (f"cannot exclude harm beyond −{epsilon:.2f}: {e}")


# ── the decision ─────────────────────────────────────────────────────────────
@dataclass
class Decision:
    status: str
    blocking_layer: str | None
    branch: str
    layers: dict = field(default_factory=dict)

    def report(self):
        print(f"  {'layer':<16s} {'result':<11s} detail", flush=True)
        for k, (r, why) in self.layers.items():
            print(f"  {k:<16s} {r:<11s} {why}", flush=True)
        tail = f" · blocked at {self.blocking_layer}" if self.blocking_layer else ""
        print(f"\n  FINAL: {self.status}{tail}", flush=True)


def decide(*, branch: str, effect, delta_star: float, cost=None, epsilon: float | None = None,
           direction: str = "positive", **validity_gates) -> Decision:
    """Evidence → Materiality → Validity → Non-inferiority → Decision.

    RETURN and RISK differ only in what `effect` measures: ΔReturn for one, ΔTEL for the
    other. Every word after that is shared, which is the point — two verdict functions with
    two sets of thresholds is how the same defect got written twice.
    """
    L = {}
    L[EVIDENCE] = evidence(effect)
    L[MATERIALITY] = materiality(effect, delta_star, direction=direction)
    L[VALIDITY] = validity(**validity_gates)
    if cost is not None:
        if epsilon is None:
            raise DecisionContractError("a cost estimate needs its own margin, declared "
                                        "separately from delta_star — they are different "
                                        "policies")
        L[NON_INFERIORITY] = non_inferiority(cost, epsilon)

    # Validity first: a real, material effect measured on a broken study is still refused,
    # and that refusal is legitimate.
    if L[VALIDITY][0] == FAIL:
        return Decision("REJECT", VALIDITY, branch, L)
    if L[MATERIALITY][0] == FAIL:
        return Decision("NULL", None, branch, L)         # equivalence: the honest null
    if L[EVIDENCE][0] == UNRESOLVED and L[MATERIALITY][0] == UNRESOLVED:
        return Decision("UNRESOLVED", EVIDENCE, branch, L)
    if L[MATERIALITY][0] == UNRESOLVED:
        return Decision("UNRESOLVED", MATERIALITY, branch, L)
    if L[VALIDITY][0] == UNRESOLVED:
        return Decision("UNRESOLVED", VALIDITY, branch, L)
    if NON_INFERIORITY in L:
        if L[NON_INFERIORITY][0] == FAIL:
            return Decision("REJECT", NON_INFERIORITY, branch, L)
        if L[NON_INFERIORITY][0] == UNRESOLVED:
            return Decision("UNRESOLVED", NON_INFERIORITY, branch, L)
    return Decision("BUILD" if branch == "return" else "VETO", None, branch, L)
