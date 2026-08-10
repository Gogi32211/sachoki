"""ComboLab v2 — the incremental estimand, frozen before any estimator exists.

v1 is a closed generation, not a version being rewritten. It works as specified, its search FWER
is calibrated under its own marginal null (0.055 vs nominal 0.05), and temporal integrity was
checked and found a real contract gap that has since been closed. What null B showed is a
limitation of the QUESTION, not of the code:

    P(any promotion | Cell→Y destroyed within Date × BaseSetup) = 0.525

The marginal claim is substantially susceptible to setup composition. So v2 changes the
scientific question rather than patching v1.

    v1   median(Y | Cell) − median(Y | ¬Cell)
    v2   what does Cell add once BaseSetup is already known

WHAT CARRIES OVER AND WHAT DOES NOT. Infrastructure states how we measure; operating
characteristics are properties of the measured object, and the object changed.

    carried    data contracts · PIT/temporal contracts · decision provenance
               sampling semantics · ledger and k accounting · sealed-development protocol
               RNG substreams · INVALID semantics
    NOT        estimator sensitivity · search δ₅₀ · chance-band critical value
               needle recall · FWER calibration · promotion rates

═══ THE ESTIMAND ════════════════════════════════════════════════════════════

Four ways to say "conditional on BaseSetup" answer four near-but-different questions:
within-setup comparison then aggregate; matched control inside the setup; setup fixed effects
or residualised outcome; hierarchical partial pooling. The first is chosen and the reasons are
recorded, because a choice not written down becomes a degree of freedom later.

    Δ_cs = median(Y | Cell_c = 1, S = s) − median(Y | Cell_c = 0, S = s)
    θ_c  = Σ_{s ∈ E_c} w_cs · Δ_cs

Distribution-free, units preserved in pp so v1 and v2 are directly comparable at the same δ, no
model introduced, and it works with the existing date-clustered bootstrap unchanged.
Residualising would compare deviations rather than levels; partial pooling would introduce the
model this project has deliberately avoided. Both are kept as later robustness diagnostics, not
as second primary targets.

WEIGHTS ARE A SCIENTIFIC CHOICE, NOT A COMPUTATION DETAIL.

    w_cs = n_cell,cs / Σ_{r ∈ E_c} n_cell,cr        ← PRIMARY: treated-opportunity weights
    w_cs = 1 / |E_c|                                 ← a DIFFERENT question, robustness only

The first asks what the context adds across the opportunities this cell actually contains; the
second asks whether the effect is typical across setups. Weighting by full setup size is
rejected outright: a vast complement would then set the weight of an effect for a cell that has
almost no observations there.

EXACTNESS, and it is why this estimand makes a clean positive control. Weights are frozen before
outcomes and normalised to 1, and adding δ to every treated row inside every eligible stratum
gives median(Y + δ) = median(Y) + δ, hence Δ_cs → Δ_cs + δ and

    θ_c^inj = θ_c + δ           exactly — not approximately, not on average.

═══ SUPPORT, MEASURED BEFORE FREEZING ══════════════════════════════════════

2,169 candidate cell × setup strata (46 × 58 families). Median support per stratum: n_cell 108,
dates_cell 71. Eligibility needs more than a row count — a stratum can hold 300 observations
sitting on four dates, which is the trap the clustered bootstrap exists for. 20.8% of candidate
strata have a single date carrying over 20% of the cell arm.

    n_min      100    both arms
    D_min       25    distinct dates, both arms
    conc_max   0.20   largest single date's share of either arm

    → 985 strata · 97.7% of cell rows retained · all 46 cells selectable
      setups per cell: min 7, median 22, max 32 · lowest cell coverage 82.1%

E_c and w_cs are computed from X and membership ONLY and frozen there. Not "bootstrap, see which
strata look stable today, aggregate the survivors" — that would let the support set be chosen by
the outcome.

CELLS HAVE DIFFERENT SUPPORT POPULATIONS, and that is new in v2. θ_c for a cell eligible in 7
setups and one eligible in 32 are both correct incremental effects about DIFFERENT target
populations. Forcing all 46 onto a common setup list would gut the space, so instead every claim
must publish its own target population, and a minimum coverage is registered so a narrow cell
cannot win the ranking on a tiny idiosyncratic slice while the reader sees only "+4.2pp".

═══ SYNTHETIC WORLDS ════════════════════════════════════════════════════════

NEGATIVE — composition only. Between-setup medians differ strongly and Cell = 1 is
over-represented in the strong setups, while inside every setup δ_incremental = 0.

    v1 marginal    MAY see an association — by construction, and it is entitled to
    v2 incremental MUST return θ ≈ 0

This is the regression test on the very motive for v2: can the new estimand refuse to credit the
context with an effect that belongs to composition? A plain δ = 0 harness does not test that.

POSITIVE — composition plus a known incremental effect. The same composition structure, plus δ
added to Cell observations INSIDE each eligible setup. v2 must recover δ without also claiming
the composition it sits on. Adding δ to every row of a planted cell without composition would
not test this: a cell disproportionately made of strong setups moves the marginal estimator too.

═══ δ GRID ══════════════════════════════════════════════════════════════════

    DELTA_GRID = (0.00, 0.60, 1.50, 3.00, 6.00)      identical to v1

Kept identical for direct generation-to-generation comparison at the same δ, which is itself the
interesting quantity. It is NOT derived from the support counts: stratum sizes say whether the
estimator is feasible, not what median difference it can resolve, and turning n into a
pseudo-MDE would reinstate exactly the heuristic this project spent the day removing.

PREREGISTERED EXPECTATION — DIRECTION UNKNOWN. It is tempting to write that conditioning removes
information so v2 must be less sensitive. That is wrong, and it would furnish a ready-made
excuse for any degradation. Var(Y) = Var(E[Y|Setup]) + E[Var(Y|Setup)]: stratification reduces
within-cell support AND removes the between-setup component from the comparison. Either can
dominate. v2's search sensitivity is an empirical quantity and is not inferred from v1's.
"""
from __future__ import annotations

import hashlib
import json

ESTIMAND = "stratified_within_setup_median_difference_pp"
STRATUM = "BaseSetup (family)"
PRIMARY_WEIGHTS = "treated_opportunity: w_cs = n_cell,cs / sum_r n_cell,cr"
ROBUSTNESS_WEIGHTS = ("equal_per_setup: w_cs = 1/|E_c| — a different question, "
                      "reported as a diagnostic and never as a second primary target")

ELIGIBILITY = dict(n_min=100, dates_min=25, max_single_date_share=0.20,
                   applies_to="both arms of every stratum")
SUPPORT_FLOOR = dict(min_coverage_of_cell_opportunities=0.50, min_eligible_setups=5)
# Observed at freeze: lowest cell coverage 82.1%, minimum 7 setups. The floor binds nothing
# today and is registered so that it binds if the data changes underneath.

REQUIRED_PER_CLAIM = ("eligible_setups", "eligible_cell_opportunities", "eligible_dates",
                      "support_fraction")

DELTA_GRID = (0.00, 0.60, 1.50, 3.00, 6.00)
EXPECTATION = "DIRECTION UNKNOWN — v2 sensitivity is not inferred from v1"

WORLDS = {
    "composition_only_negative": dict(
        between_setup_effect="strong", cell_over_represented_in_strong_setups=True,
        incremental_delta=0.0,
        v1_expected="MAY show association — entitled to, by construction",
        v2_required="θ ≈ 0"),
    "composition_plus_incremental_positive": dict(
        between_setup_effect="strong", cell_over_represented_in_strong_setups=True,
        incremental_delta="δ from DELTA_GRID, added to Cell rows INSIDE each eligible stratum",
        v2_required="recover δ without claiming the composition it sits on"),
}

LADDER = ("incremental estimator control", "incremental decision control", "incremental needle",
          "incremental structured null", "sealed acceptance")


def digest() -> str:
    return hashlib.sha256(json.dumps({
        "estimand": ESTIMAND, "stratum": STRATUM, "weights": PRIMARY_WEIGHTS,
        "eligibility": ELIGIBILITY, "support_floor": SUPPORT_FLOOR,
        "delta_grid": list(DELTA_GRID), "worlds": WORLDS, "ladder": list(LADDER),
        "expectation": EXPECTATION,
    }, sort_keys=True).encode()).hexdigest()


if __name__ == "__main__":
    print(f"COMBOLAB v2 SPEC DIGEST  {digest()}")
    print(f"  estimand   {ESTIMAND}")
    print(f"  stratum    {STRATUM}")
    print(f"  weights    {PRIMARY_WEIGHTS}")
    print(f"  eligible   n≥{ELIGIBILITY['n_min']} · dates≥{ELIGIBILITY['dates_min']} · "
          f"top-date share ≤{ELIGIBILITY['max_single_date_share']}")
    print(f"  δ grid     {DELTA_GRID}")
    print(f"  expect     {EXPECTATION}")
