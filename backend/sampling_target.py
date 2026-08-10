"""The fifth contract layer: what probabilistic experiment is this number about?

On 2026-08-10 the engine's own control samples reported coverage of 99.7% against a nominal
95%, and that was read as evidence of an over-wide bootstrap. It was not. Four measurements at
sampling fractions 0.30 / 0.50 / 0.70 / 0.90 recovered a pre-registered prediction,

    SE_boot / SD_MC  ≈  1 / √(1 − f)        observed 1.265 / 1.508 / 1.786 / 3.086
                                            predicted 1.195 / 1.414 / 1.826 / 3.162
                                            r = +0.9987, mean obs/pred = 1.02

across a 2.6× span. The Monte Carlo redrew 70% of the dates from a FIXED set of 1,284 without
replacement — finite-population sampling, whose variance carries the (1 − f) correction — while
the bootstrap answered repeated-sampling uncertainty under the empirical cluster-resampling
model. Both numbers were computed correctly. Neither was the ground truth for the other.

That is a new failure class, and it is not statistical:

    data contract          can the data be trusted
    computation invariants is the arithmetic what it claims
    report provenance      did anything print a statistic no estimator produced
    decision provenance    may this gate decide from what it was handed
    SAMPLING SEMANTICS     do these two numbers describe the same experiment

`bootstrap_ci_clustered` was never wrong. The calibration comparison was. So the rule this
module enforces is narrow and exactly the shape of what happened:

    A calibration metric — coverage, size, power — may only be computed when the interval's
    sampling target and the replication generator's sampling target are COMPATIBLE. Handed an
    incompatible pair, it raises and prints the expected discrepancy rather than the metric.

Descriptive metrics are not restricted. A conditional detection rate makes no claim about
calibration; it is a property of the replication generator alone, and it is legitimate and
useful. What it may not do is call itself `power`.

What the cluster bootstrap is NOT, stated here so nothing downstream overclaims it: resampling
observed clusters cannot produce an unseen regime, a structural break, a new microstructure, or
a crisis of a type the window did not contain. "What other five years would give" belongs to
FROZEN_FORWARD and to nothing else in this file.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

__all__ = ["SamplingTarget", "Measurement", "SamplingSemanticsError",
           "finite_population_subsample", "empirical_cluster_resampling", "frozen_forward",
           "synthetic_dgp", "structured_permutation_null", "fpc_ratio", "compatible", "calibration_metric",
           "descriptive_metric"]


class SamplingSemanticsError(AssertionError):
    """Two correctly computed numbers were about different probabilistic experiments."""


# ── the targets ──────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class SamplingTarget:
    """The experiment a number is about — not how it was computed, but over what it varies.

    `kind` is the probabilistic question. `unit` is what is treated as exchangeable.
    `conditioned_on` names the fixed thing, and it is required for the conditional kinds
    because a conditional rate without its condition is the exact statement that caused this
    module to exist.
    """
    kind: str
    unit: str
    conditioned_on: str = ""
    fraction: float | None = None

    def __post_init__(self):
        if self.kind in ("finite_population_subsample",) and not self.conditioned_on:
            raise SamplingSemanticsError(
                f"{self.kind} is conditional by construction and must name what it is "
                f"conditioned on — a conditional rate quoted without its condition is how "
                f"99.7% coverage got read as a broken interval")
        if self.kind == "finite_population_subsample" and not (0 < (self.fraction or 0) < 1):
            raise SamplingSemanticsError("finite_population_subsample needs its fraction f")

    def __str__(self):
        bits = [self.kind, f"by {self.unit}"]
        if self.fraction is not None:
            bits.append(f"f={self.fraction:.2f}")
        if self.conditioned_on:
            bits.append(f"| {self.conditioned_on}")
        return " · ".join(bits)


def finite_population_subsample(fraction: float, conditioned_on: str,
                                unit: str = "trading_date") -> SamplingTarget:
    """Redrawing a fraction of clusters from ONE realised history. Variance carries (1 − f)."""
    return SamplingTarget("finite_population_subsample", unit, conditioned_on, fraction)


def empirical_cluster_resampling(unit: str = "trading_date") -> SamplingTarget:
    """What the cluster bootstrap answers: reshuffling the clusters that occurred.

    Not the future, and not an unseen regime. The observed clusters are assumed exchangeable
    and representative; that assumption is the whole content of the interval.
    """
    return SamplingTarget("empirical_cluster_resampling", unit)


def frozen_forward(since: str, unit: str = "trading_date") -> SamplingTarget:
    """Genuinely unseen data. The only target entitled to speak about other five years."""
    return SamplingTarget("frozen_forward", unit, conditioned_on=f"frozen at {since}")


def structured_permutation_null(generator_id: str, unit: str = "trading_date") -> SamplingTarget:
    """A null built by permuting real outcomes inside preregistered strata.

    Every rate measured against one of these is conditional on the generator, and the generator
    is a modelling choice: `marginal_date_v1` and `conditional_date_setup_price_v1` can return
    materially different false-positive rates from the same engine and the same data, because
    they encode different null hypotheses. Reporting either as "the search FPR" would repeat, a
    third time, the mistake this module was written for.
    """
    return SamplingTarget("structured_permutation_null", unit, conditioned_on=generator_id)


def synthetic_dgp(name: str, unit: str = "trading_date") -> SamplingTarget:
    """A generator we wrote. Calibration against it tests the CODE, never the market."""
    return SamplingTarget("synthetic_dgp", unit, conditioned_on=name)


# ── the rule ─────────────────────────────────────────────────────────────────
def fpc_ratio(fraction: float) -> float:
    """SE_unconditional / SD_finite-population = 1/√(1 − f). Measured to 2% over f ∈ [.3,.9]."""
    if not 0 < fraction < 1:
        raise SamplingSemanticsError("fraction must lie in (0, 1)")
    return 1.0 / math.sqrt(1.0 - fraction)


def compatible(interval: SamplingTarget, replication: SamplingTarget) -> tuple[bool, str]:
    """May a calibration metric be computed from this pair?"""
    if interval.unit != replication.unit:
        return False, (f"exchangeable unit differs: interval treats {interval.unit!r} as "
                       f"independent, replications vary {replication.unit!r}")
    if interval.kind == replication.kind:
        if interval.kind in ("synthetic_dgp", "structured_permutation_null") and \
                interval.conditioned_on != replication.conditioned_on:
            return False, (f"two different generators: {interval.conditioned_on!r} vs "
                           f"{replication.conditioned_on!r} — a rate under one null model says "
                           f"nothing about the other")
        return True, "same sampling target"
    pair = {interval.kind, replication.kind}
    if pair == {"empirical_cluster_resampling", "finite_population_subsample"}:
        fp = interval if interval.kind == "finite_population_subsample" else replication
        r = fpc_ratio(fp.fraction)
        return False, (
            f"THE 2026-08-10 COMPARISON. An empirical-cluster-resampling interval scored "
            f"against finite-population subsample scatter at f={fp.fraction:.2f}. These differ "
            f"by the finite-population correction: the interval is ~{r:.2f}× wider than the "
            f"scatter BY CONSTRUCTION, so coverage is inflated and the metric measures the "
            f"mismatch, not the estimator. Both numbers are individually correct.")
    if "frozen_forward" in pair:
        return False, ("frozen-forward evidence cannot be calibrated against resampled or "
                       "subsampled history — that is the one comparison the forward window "
                       "exists to avoid")
    return False, f"incompatible targets: {interval.kind} vs {replication.kind}"


@dataclass(frozen=True)
class Measurement:
    """A number that carries the experiment it is about. Printing it prints the semantics."""
    metric: str
    value: float
    target: SamplingTarget
    n_replications: int
    calibration_claim: bool

    def __str__(self):
        tag = "calibration" if self.calibration_claim else "descriptive"
        return (f"{self.metric} = {self.value:.4f}  [{tag}; {self.target}; "
                f"{self.n_replications:,} reps]")


CALIBRATION_METRICS = {"coverage", "size", "power", "type_i_error", "interval_width_ratio"}


def calibration_metric(name: str, value: float, *, interval: SamplingTarget,
                       replication: SamplingTarget, n_replications: int) -> Measurement:
    """Coverage / size / power. Refuses an incompatible pair instead of returning a number.

    The refusal carries the expected discrepancy, because "these are incomparable" is much less
    useful than "these differ by 1.83× and here is why".
    """
    if name not in CALIBRATION_METRICS:
        raise SamplingSemanticsError(
            f"{name!r} is not a calibration metric. If it describes one experiment rather than "
            f"validating an estimator against another, use descriptive_metric().")
    ok, why = compatible(interval, replication)
    if not ok:
        raise SamplingSemanticsError(f"cannot compute {name}: {why}")
    return Measurement(name, float(value), interval, int(n_replications), True)


def descriptive_metric(name: str, value: float, *, target: SamplingTarget,
                       n_replications: int) -> Measurement:
    """A property of one experiment, making no claim about another.

    `conditional_detection_rate` and `conditional_acceptance_rate` live here. They are the
    honest names for what the shared-history harness measures, and they are genuinely useful —
    what they may not do is answer to `power`.
    """
    if name in CALIBRATION_METRICS:
        raise SamplingSemanticsError(
            f"{name!r} is a calibration metric and needs both an interval target and a "
            f"replication target. The shared-history harness measures "
            f"conditional_detection_rate, not power — they differ by exactly the confusion "
            f"this module exists to prevent.")
    return Measurement(name, float(value), target, int(n_replications), False)
