"""REAL-Y QUALIFICATION v1 — registered before the first bootstrap on real outcomes.

This file is written and frozen BEFORE any real-y interval is computed, and that order is the
whole point. Criteria chosen after seeing results are not criteria.

WHAT THIS GATE ASKS, and it is narrow on purpose:

    does the frozen computation contract survive the real outcome vector without acquiring new
    rules along the way?

It does not ask whether the historical results are interesting. Three statements are separate,
and the first does not imply the others:

    θ(real-y) computes for 31/31            already shown by V2_CORE_ORACLE.json
    bootstrap(real-y) runs under the frozen policy      NOT yet shown
    inference(real-y) is interpretable                  NOT yet shown

WHERE THE DANGER ACTUALLY IS. The sealed acceptance ran on `Y = μ_setup + γ_date + ε`, a dense
and well-behaved geometry where every stratum is populated in every resample. Real returns are
the first outcome vector with real numerical geometry, and this is exactly where NaN, degenerate
resamples and "let's just skip that case" appear — new statistical decisions wearing the clothes
of engineering fixes. This gate exists to make them impossible to take silently.

THE THREE PROHIBITIONS

    no silent row drops       the registered population already dropped non-finite outcomes at
                              load. A non-finite value inside the strata the engine USES is an
                              OutcomeIntegrityError, never a `dropna()` inside the estimator —
                              that would change the estimand's population mid-computation.

    no adaptive retries       replicates are requested once. Drawing more until 2,000 good ones
                              accumulate changes the sampling mechanism into one nobody
                              registered, and the change is invisible in the output.

    no new degeneracy rule    a stratum that loses an arm in a resample is handled by the frozen
                              uncomputable-stratum contract or the cell is UNCOMPUTABLE. It is
                              not skipped because real-y was the first thing to surface it.

DEGENERATE IS NOT INVALID. A zero-width interval can be a correct answer — if the outcome really
is identical across the relevant resamples, that is what the data says. Conflating it with a
numerical failure would make the gate reject correct extreme cases for looking unusual. Design
degeneracy (eligibility) and outcome degeneracy are different things and stay different.
"""
from __future__ import annotations

import hashlib
import json

SPEC_VERSION = "real_y_qualification_v1"

# ── the registered criteria ─────────────────────────────────────────────────
CRITERIA = {
    "cells_addressed": "all 31 registered OPPORTUNITY_LEVEL classes are attempted",
    "nonfinite_required_outcomes": "zero non-finite values among the outcomes actually used",
    "silent_row_drops": "zero — the engine drops nothing the registered population kept",
    "adaptive_bootstrap_retries": "zero — replicates are requested once",
    "bootstrap_accounting_reconciles": "valid + rejected == requested, per cell",
    "published_intervals_finite_and_ordered": "every published ci_low <= ci_high, both finite",
    "uncomputable_carries_reason": "every uncomputable result names a frozen reason code",
    "deterministic_rerun": "same spec + snapshot + RNG policy → bit-identical output",
    "day_level_untouched": "the 6 DAY_LEVEL claims are not addressed at all",
}

# Stated so nobody can add them later and call the gate stricter. A qualification that depended
# on the answer being pleasant would be an acceptance test for the data, not for the instrument.
NOT_CRITERIA = (
    "at least N cells reach BUILD",
    "θ is positive anywhere",
    "any interval excludes zero",
    "the results resemble the v1 findings",
    "the results are interesting",
)

# ── the frozen policy the run must obey ─────────────────────────────────────
BOOTSTRAP_REPS_REQUESTED = 2000

# Every requested replicate must be computable for an interval to be published. Registered at
# 1.0 because there is no basis for any other number: a partial-completion threshold chosen now
# would be an unregistered statistical decision, and chosen later would be one taken after
# seeing which cells it saves. If this proves too strict in practice, relaxing it is a POLICY
# CHANGE to be registered BEFORE the next run — never an adjustment after reading this one.
BOOTSTRAP_VALID_FLOOR = 1.0

REASON_CODES = (
    "ARM_LOST_IN_RESAMPLE",        # a stratum lost treated or comparator rows in a resample
    "NONFINITE_STATISTIC",         # θ evaluated to NaN or ±inf
    "EMPTY_STRATUM",               # a frozen stratum had no rows to draw from
    "BELOW_VALID_FLOOR",           # too few computable replicates to publish under the policy
)

DEGENERACY_CLASSES = (
    "DEGENERATE_BUT_VALID",        # zero-width or identical resamples, computed correctly
    "NUMERICALLY_INVALID",         # non-finite or inverted interval
)

# Failed qualification runs are kept. If real-y surfaces a defect, the fix does not erase the
# fact that a first application happened; that run is part of the qualification history and its
# evidence keeps the role it was produced under.
FAILED_RUNS_ARE_RETAINED = True


def spec_hash() -> str:
    blob = json.dumps({"v": SPEC_VERSION, "criteria": CRITERIA, "not": list(NOT_CRITERIA),
                       "reps": BOOTSTRAP_REPS_REQUESTED, "floor": BOOTSTRAP_VALID_FLOOR,
                       "reasons": list(REASON_CODES), "degeneracy": list(DEGENERACY_CLASSES)},
                      sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


if __name__ == "__main__":
    print(f"{SPEC_VERSION}  spec_hash {spec_hash()}")
    for k, v in CRITERIA.items():
        print(f"  {k:<40s} {v}")
    print("\n  explicitly NOT criteria:")
    for n in NOT_CRITERIA:
        print(f"    {n}")
