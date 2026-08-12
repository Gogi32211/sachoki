"""The three prohibitions, as code that refuses rather than as a paragraph in a spec.

Each one is a place where a statistical decision can enter disguised as an engineering fix, and
each looks entirely reasonable at the moment someone reaches for it:

    `dropna()` inside the estimator          "there were a couple of NaNs"
    draw more replicates until 2,000 good    "some resamples failed, so we topped up"
    skip a resample that lost an arm         "it can't be computed, so we skipped it"

All three are changes to the estimand or to the sampling mechanism, all three are invisible in
the output, and all three arrive the first time a real outcome vector is used. The synthetic
worlds the instrument was accepted on never provoked any of them.

WHAT IS COUNTED, ALWAYS. `requested == valid + rejected`, per cell, reconciled before anything
is published. An accounting that does not reconcile means replicates went somewhere unrecorded,
and the direction of that error is always flattering: the ones that vanish are the awkward ones.

DEGENERATE IS NOT INVALID, and keeping them apart is what stops this gate from rejecting correct
extreme cases. A zero-width interval computed from resamples that genuinely agree is an answer.
A non-finite or inverted interval is a failure. Only the second one is a defect.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

import numpy as np

import real_y_qualification_spec as SPEC

DEGENERATE_BUT_VALID = "DEGENERATE_BUT_VALID"
NUMERICALLY_INVALID = "NUMERICALLY_INVALID"
WELL_POSED = "WELL_POSED"


class OutcomeIntegrityError(RuntimeError):
    """The outcome vector violates the registered admissibility policy."""


class BootstrapPolicyError(RuntimeError):
    """The sampling mechanism departed from the one that was registered."""


# ── admissibility, checked once, before anything is estimated ───────────────
def assert_outcomes_admissible(y: np.ndarray, used_idx: np.ndarray, *, where: str) -> None:
    """Non-finite values in rows the engine USES are an error, never something to drop.

    The registered population already removed non-finite outcomes at load. Anything left inside
    the frozen strata is a contradiction between the population and the data, and resolving it
    here — by dropping rows — would silently redefine whose returns the estimand is about.
    """
    vals = np.asarray(y)[np.asarray(used_idx)]
    bad = ~np.isfinite(vals)
    if bad.any():
        n = int(bad.sum())
        raise OutcomeIntegrityError(
            f"{n} of {vals.size} outcomes used by {where} are not finite. The registered "
            f"population dropped non-finite outcomes at load, so these are a contradiction "
            f"between the population and the data — not something for the estimator to clean up. "
            f"Dropping them here would change the population the estimand is about, after the "
            f"estimand was frozen.")


# ── one replicate ───────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ReplicateOutcome:
    """Valid, or rejected with a frozen reason. There is no third option and no retry."""
    index: int
    valid: bool
    value: float = float("nan")
    reason: str = ""

    def __post_init__(self):
        if not self.valid and self.reason not in SPEC.REASON_CODES:
            raise BootstrapPolicyError(
                f"a replicate was rejected for {self.reason!r}, which is not a registered reason "
                f"code. Every rejection names one of {SPEC.REASON_CODES}, so a new failure mode "
                f"cannot be absorbed as an unlabelled skip.")


def classify_replicate(index: int, value, *, arms_present: bool, strata_nonempty: bool
                       ) -> ReplicateOutcome:
    """The only place a replicate is judged. No caller invents its own rule."""
    if not strata_nonempty:
        return ReplicateOutcome(index, False, reason="EMPTY_STRATUM")
    if not arms_present:
        # The case real-y is most likely to surface first: full support has both arms, and a
        # date-clustered resample can still lose one. It is refused as a labelled rejection
        # rather than skipped, because a skip is a change to the sampling mechanism.
        return ReplicateOutcome(index, False, reason="ARM_LOST_IN_RESAMPLE")
    v = float(value)
    if not np.isfinite(v):
        return ReplicateOutcome(index, False, reason="NONFINITE_STATISTIC")
    return ReplicateOutcome(index, True, value=v)


# ── one cell ────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class CellIntegrity:
    cell: str
    reps_requested: int
    reps_valid: int
    reps_rejected: int
    rejection_reasons: dict = field(default_factory=dict)
    theta_finite: bool = False
    interval: tuple = ()
    degeneracy: str = WELL_POSED
    publishable: bool = False
    uncomputable_reason: str = ""

    def reconciles(self) -> bool:
        return self.reps_valid + self.reps_rejected == self.reps_requested

    def as_dict(self) -> dict:
        d = asdict(self)
        d["interval"] = list(self.interval)
        return d


def assess_cell(cell: str, theta: float, replicates, *, requested: int) -> CellIntegrity:
    """Judge one cell under the registered policy, and publish only if the policy allows.

    `replicates` is the FULL requested set, valid and rejected alike. Handing this function only
    the good ones is the adaptive-retry defect wearing a different shape, so the count is
    reconciled here rather than trusted.
    """
    reps = list(replicates)
    if len(reps) != requested:
        raise BootstrapPolicyError(
            f"{cell}: {len(reps)} replicate outcomes for {requested} requested. Replicates are "
            f"requested once; a shortfall means some were dropped before they were counted, and "
            f"a surplus means more were drawn until enough succeeded. Either way the sampling "
            f"mechanism is no longer the registered one.")
    valid = [r for r in reps if r.valid]
    rejected = [r for r in reps if not r.valid]
    reasons: dict = {}
    for r in rejected:
        reasons[r.reason] = reasons.get(r.reason, 0) + 1

    theta_finite = bool(np.isfinite(theta))
    if not theta_finite:
        return CellIntegrity(cell, requested, len(valid), len(rejected), reasons,
                             theta_finite=False, degeneracy=NUMERICALLY_INVALID,
                             publishable=False, uncomputable_reason="NONFINITE_STATISTIC")

    if len(valid) < SPEC.BOOTSTRAP_VALID_FLOOR * requested:
        return CellIntegrity(cell, requested, len(valid), len(rejected), reasons,
                             theta_finite=True, publishable=False,
                             uncomputable_reason="BELOW_VALID_FLOOR")

    vals = np.array([r.value for r in valid], dtype=float)
    lo, hi = (float(np.percentile(vals, 2.5)), float(np.percentile(vals, 97.5)))

    if not (np.isfinite(lo) and np.isfinite(hi)) or hi < lo:
        return CellIntegrity(cell, requested, len(valid), len(rejected), reasons,
                             theta_finite=True, interval=(lo, hi),
                             degeneracy=NUMERICALLY_INVALID, publishable=False,
                             uncomputable_reason="NONFINITE_STATISTIC")

    # A zero-width interval from resamples that genuinely agree is an answer, not a failure.
    degeneracy = DEGENERATE_BUT_VALID if hi == lo else WELL_POSED
    return CellIntegrity(cell, requested, len(valid), len(rejected), reasons,
                         theta_finite=True, interval=(lo, hi), degeneracy=degeneracy,
                         publishable=True)


# ── the report the gate reads ───────────────────────────────────────────────
@dataclass(frozen=True)
class RealYNumericalIntegrityReport:
    spec_version: str
    spec_hash: str
    outcome_rows_total: int
    outcome_rows_used: int
    nonfinite_y: int
    cells_requested: int
    cells: tuple = field(default_factory=tuple)

    @property
    def cells_theta_finite(self) -> int:
        return sum(1 for c in self.cells if c.theta_finite)

    @property
    def cells_publishable(self) -> int:
        return sum(1 for c in self.cells if c.publishable)

    @property
    def cells_degenerate_but_valid(self) -> int:
        return sum(1 for c in self.cells if c.degeneracy == DEGENERATE_BUT_VALID)

    @property
    def cells_numerically_invalid(self) -> int:
        return sum(1 for c in self.cells if c.degeneracy == NUMERICALLY_INVALID)

    @property
    def cells_uncomputable(self) -> int:
        return sum(1 for c in self.cells if not c.publishable)

    def failures(self) -> list:
        """Every registered criterion that did not hold. Empty is the only pass."""
        out = []
        if self.cells_requested != len(self.cells):
            out.append(f"{len(self.cells)} cells addressed of {self.cells_requested} registered")
        if self.nonfinite_y:
            out.append(f"{self.nonfinite_y} non-finite outcomes among those used")
        for c in self.cells:
            if not c.reconciles():
                out.append(f"{c.cell}: {c.reps_valid} + {c.reps_rejected} != {c.reps_requested}")
            if not c.publishable and not c.uncomputable_reason:
                out.append(f"{c.cell}: uncomputable with no reason code")
            if c.publishable and c.interval and c.interval[0] > c.interval[1]:
                out.append(f"{c.cell}: inverted interval published")
        return out

    def as_dict(self) -> dict:
        return {"spec_version": self.spec_version, "spec_hash": self.spec_hash,
                "outcome_rows_total": self.outcome_rows_total,
                "outcome_rows_used": self.outcome_rows_used, "nonfinite_y": self.nonfinite_y,
                "cells_requested": self.cells_requested,
                "cells_theta_finite": self.cells_theta_finite,
                "cells_publishable": self.cells_publishable,
                "cells_degenerate_but_valid": self.cells_degenerate_but_valid,
                "cells_numerically_invalid": self.cells_numerically_invalid,
                "cells_uncomputable": self.cells_uncomputable,
                "failures": self.failures(),
                "cells": [c.as_dict() for c in self.cells]}


def build_report(*, rows_total: int, rows_used: int, nonfinite: int, requested_cells: int,
                 cells) -> RealYNumericalIntegrityReport:
    return RealYNumericalIntegrityReport(
        spec_version=SPEC.SPEC_VERSION, spec_hash=SPEC.spec_hash(),
        outcome_rows_total=rows_total, outcome_rows_used=rows_used, nonfinite_y=nonfinite,
        cells_requested=requested_cells, cells=tuple(cells))
