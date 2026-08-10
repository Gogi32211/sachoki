"""Structured-null placebo — frozen before the runner exists, as the needle test was.

ComboLab passed SEARCH VALIDATION: with a hidden needle among 46 claims it finds one of ~3pp
about half the time and one of 0.60pp almost never. The opposite question is still open — does
it stay quiet when the world contains nothing findable?

δ = 0 in the needle test cannot answer it. That world is real data with no ADDED effect, so it
keeps whatever structure the market has, and `unrelated_fp = 1.000` there reflects a genuine
cell (rsi<35&rs at +3.84 against a band of +3.17). It carries
`semantic_status = CONTAMINATED_BY_REAL_STRUCTURE` and is not a false-positive rate.

WHAT COMBOLAB ACTUALLY CLAIMS, read off the frozen spec rather than chosen now:

    CONTROL  = complement_within_base_population
    ESTIMAND = median_return_difference_pp

There is no adjustment for setup family or liquidity anywhere in it. Price enters only as the
$21-89 base restriction. So the claim is MARGINAL: this cell's median beats its complement.
That single fact decides what each null world below means, and getting it backwards would
produce a correct result and label it a defect.

═══ NULL A — MARGINAL ═══════════════════════════════════════════════════════

    H₀ᴬ : Y ⊥ Cell | Date

Outcomes permuted within trading date, between unique economic opportunities. This IS the
false-discovery test for the claim ComboLab makes: under H₀ᴬ nothing is findable, so every
promotion is an error.

It is also, deliberately, the same model ComboLab's own chance band implements — which is why
A alone would be a weak test. Matching the outer generator to the inner model makes FWER come
out near nominal almost by construction, and what that verifies is the Monte-Carlo
implementation, not the adequacy of the model. Independent RNG streams do not fix this; they
prevent coupled STREAMS, not coincident MODELS.

═══ NULL B — CONDITIONAL ════════════════════════════════════════════════════

    H₀ᴮ¹ : Y ⊥ Cell | Date, BaseSetup
    H₀ᴮ² : Y ⊥ Cell | Date, BaseSetup, PriceBucket

Outcomes permuted within strata that preserve the nuisance structure the marginal band erases.

    PROMOTIONS UNDER B ARE NOT FALSE DISCOVERIES for ComboLab as specified.

Under H₀ᴮ a marginal association can genuinely survive, because Cell correlates with BaseSetup
and PriceBucket. Such a promotion is a true marginal effect fully explained by nuisance — which
is exactly what a marginal claim is entitled to report. B therefore measures a DECOMPOSITION,
not an error rate: what share of ComboLab's promotions survives once the structure it ignores
is held fixed. A low survival rate would say the 46-cell marginal space mostly repackages
setups, and that ComboLab v2 should carry an incremental estimand. That is a design finding,
not a bug report.

B1 and B2 are registered separately and BOTH are run. Looking at B1 and then deciding liquidity
should have been included would be a post-hoc change of the null, so the choice is made here.

═══ RARE STRATA ═════════════════════════════════════════════════════════════

Measured before freezing, because a conditional null that cannot permute is an identity
transform wearing a null's name:

    A   date                     1,304 strata   median  201   rows in strata <4:  0.0%
    B1  date × family           24,836 strata   median    3   rows in strata <4:  7.1%
    B2  date × family × price   51,226 strata   median    2   rows in strata <4: 17.0%
                                                             rows in strata <8: 32.3%

A median stratum of 2 permutes to the identity half the time. Left unaddressed, B2 would return
a comfortable FWER because the world was barely shuffled.

Rule: hierarchical fallback, never silent freezing. A stratum below MIN_PERMUTABLE falls back
one conditioning level, and again if still too small. The fraction of rows permuted at each
level is a MANDATORY reported diagnostic — a B2 run that permuted 60% of its rows at date level
is a different experiment from one that permuted 95% at target level, and the number says which
one happened.

═══ METRICS — three, because "promoted" hides where the tax is ══════════════

    FWER_band    P(any cell clears the chance band)          ≈ α if calibrated
    FWER_search  P(any cell is promoted)                     ≤ FWER_band
    FWER_final   P(any cell reaches a positive verdict)      ≤ FWER_search

Reading only the last leaves band-too-strict and bootstrap-cuts-half indistinguishable. The
band is the 95th percentile of the max, so FWER_band ≈ 0.05 is the calibration expectation;
the later gates can only filter further, so the other two are bounded above, not centred on α.

Tolerance is registered as an interval BEFORE the run — a demand for ≤ 5.000% would be false
precision given finite permutations and a bootstrap in the path.

═══ WHAT MAY NOT INFORM THESE CRITERIA ══════════════════════════════════════

The δ = 0 needle results are now known: baseline promotions on real data are frequent. That is
information about the market, not a calibration target. Acceptance criteria here are registered
independently of how uncomfortable those numbers were.
"""
from __future__ import annotations

import hashlib
import json

MIN_PERMUTABLE = 4          # below this a stratum falls back a level; 4! = 24 arrangements
PRICE_BUCKETS = ((21, 35), (35, 55), (55, 89))
N_REPLICATIONS = 200
ALPHA = 0.05
# 200 replications: SE of a rate near 0.05 is 1.5pp, so the interval below is what the design
# can resolve, not a target anyone should hit exactly.
TOLERANCE_BAND = (0.02, 0.10)

GENERATORS = {
    "marginal_date_v1": dict(
        h0="Y ⊥ Cell | Date",
        strata=("date",),
        fallback=(),
        purpose="false-discovery test for the marginal claim ComboLab makes; also the model "
                "its own chance band implements, so A alone tests implementation",
        promotions_are="FALSE DISCOVERIES"),
    "conditional_date_setup_v1": dict(
        h0="Y ⊥ Cell | Date, BaseSetup",
        strata=("date", "family"),
        fallback=(("date",),),
        purpose="how much of ComboLab's promotion survives holding setup family fixed",
        promotions_are="TRUE MARGINAL EFFECTS EXPLAINED BY NUISANCE — not errors"),
    "conditional_date_setup_price_v1": dict(
        h0="Y ⊥ Cell | Date, BaseSetup, PriceBucket",
        strata=("date", "family", "price_bucket"),
        fallback=(("date", "family"), ("date",)),
        purpose="same, additionally holding the price bucket fixed",
        promotions_are="TRUE MARGINAL EFFECTS EXPLAINED BY NUISANCE — not errors"),
}

RNG_STREAMS = ("outer_null_world", "inner_chance_band", "inner_bootstrap")
# Invariant, tested rather than promised: changing the outer replication count must not alter
# inner results for replication ids that already existed. Each stream is derived from
# (generator_id, replication_id), never from execution order.

REQUIRED_DIAGNOSTICS = (
    "pct_rows_permuted_at_target_level",
    "pct_rows_permuted_after_fallback",
    "pct_rows_left_fixed",
    "median_permutable_stratum_size",
)

METRICS = ("FWER_band", "FWER_search", "FWER_final", "E_n_promoted",
           "p_zero", "p_one", "p_two_plus", "max_promoted")

SAMPLING_TARGET_KIND = "structured_permutation_null"
# Named so that sampling_target.py cannot let any of this be reported as a universal search FPR.
# The honest ledger name is: search family-wise false-positive rate under the preregistered
# structured-null generator <generator_id>.


def digest() -> str:
    return hashlib.sha256(json.dumps({
        "generators": GENERATORS, "min_permutable": MIN_PERMUTABLE,
        "price_buckets": PRICE_BUCKETS, "n_replications": N_REPLICATIONS,
        "alpha": ALPHA, "tolerance": list(TOLERANCE_BAND), "metrics": list(METRICS),
        "diagnostics": list(REQUIRED_DIAGNOSTICS), "streams": list(RNG_STREAMS),
    }, sort_keys=True).encode()).hexdigest()


if __name__ == "__main__":
    print(f"PLACEBO SPEC DIGEST  {digest()}")
    for gid, g in GENERATORS.items():
        print(f"\n  {gid}")
        print(f"    H0        {g['h0']}")
        print(f"    strata    {' × '.join(g['strata'])}"
              + (f"  → fallback {' → '.join('×'.join(f) for f in g['fallback'])}"
                 if g["fallback"] else ""))
        print(f"    promotions are {g['promotions_are']}")
