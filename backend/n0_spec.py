"""N0 — the incremental structured null, with TWO generators because one cannot serve both.

S1 found that two classes never accept even at δ = 6 while ranking first and passing the
known-location branch. The cause was measured, not guessed: `sig_macro_vix_up` is constant
within every date — 0 of 1,304 dates carry both values, because it is a property of the market
day. Permuting outcomes within (date × family) therefore cannot break its association with Y:
membership is constant inside the block, so the permuted θ equals the observed θ, the band
absorbs the planted effect, and the class cannot clear its own barrier.

That is not a search failure. It is a defect in the null's construction, and it is the same
lesson for the third time in this project:

    A NULL MUST ACTUALLY BREAK THE ASSOCIATION UNDER TEST.

Three repairs were possible. Changing the block so day-features shuffle would destroy the date
clustering the block exists to preserve. Documenting the degeneracy would leave two claims
permanently untested. So instead each feature is tested by the null that CAN break it, and the
two nulls are registered separately — the same principle as A and B in the v1 placebo, where one
null was never asked to answer for every hypothesis.

═══ ROUTING RULE, declared as a property of the data ════════════════════════

    A feature is DATE-LEVEL if it takes a single value within every trading date.

Measured, not chosen: `sig_macro_vix_up` qualifies (max 1 distinct value per date);
`sig_rs_intact`, `sig_conso`, `sig_lead_in_lag`, `sig_h1_dr` do not (88-99.8% of dates carry
both), and neither do `sig_rsi_14` (up to 349 distinct values per date) or `sig_adx_regime` (5).
A class routes to G2 if ANY feature in its definition is date-level. Six of the 37 do.

The rule is a property of the data and is evaluated before any outcome is touched, so a class
cannot be moved between generators after seeing how it performed.

═══ G1 · WITHIN-STRATUM OUTCOME PERMUTATION ════════════════════════════════

    H₀ : Y ⊥ Cell | Date, BaseSetup

Outcomes permuted inside (date × family). This is the null v1's placebo established and the one
the chance band implements; it is correct for the 31 classes whose features vary within a date.

═══ G2 · DATE-LEVEL LABEL PERMUTATION ══════════════════════════════════════

    H₀ : Y ⊥ DayProperty

The day-property's VALUE is permuted across dates; outcomes are never touched. This is the
"permute the labels" form, and it is exact here for the reason it was NOT exact in v1: a
date-level feature HAS a single label per date, so there is something to exchange. v1's cells
overlapped and no single label existed, which is why outcomes were permuted there instead.

Preserved: every outcome exactly as observed, the within-date correlation structure, family
composition, and the number of day-property days. Destroyed: the link between the market-day
property and returns — which is the whole and only content of H₀ᴳ².

Membership is RECOMPUTED under the permuted labels, and that is deliberate: for these classes
membership is exactly what carries the tested association, so freezing it would freeze the thing
being nulled. For G1 the opposite holds and membership stays frozen. The two generators differ
in what they hold fixed because they null different objects.

═══ WHAT MAY NOT BE COMPARED ══════════════════════════════════════════════

    FWER under G1  and  FWER under G2  are rates under different null models.

`sampling_target.structured_permutation_null(generator_id)` refuses the comparison. Reporting
one figure for "the incremental FWER" by pooling them would be the fourth repetition of the
mistake this architecture was built to prevent.
"""
from __future__ import annotations

import hashlib
import json

N_REPLICATIONS = 200
ALPHA = 0.05
TOLERANCE_BAND = (0.02, 0.10)

DATE_LEVEL_RULE = "a feature is date-level iff it takes exactly one value within every date"
DATE_LEVEL_FEATURES = ("sig_macro_vix_up",)          # measured: 0 of 1,304 dates carry both
N_CLASSES_G1, N_CLASSES_G2 = 31, 6

GENERATORS = {
    "within_stratum_outcome_v1": dict(
        h0="Y ⊥ Cell | Date, BaseSetup",
        permutes="outcomes inside (date × family)",
        membership="FROZEN",
        applies_to="classes with no date-level feature",
        n_classes=N_CLASSES_G1),
    "date_level_label_v1": dict(
        h0="Y ⊥ DayProperty",
        permutes="the day-property's value across dates; outcomes untouched",
        membership="RECOMPUTED under the permuted labels — for these classes membership IS "
                   "the association under test, so freezing it would freeze the null",
        applies_to="classes containing any date-level feature",
        n_classes=N_CLASSES_G2),
}

METRICS = ("FWER_band", "FWER_search", "FWER_final", "E_n_promoted",
           "p_zero", "p_one", "p_two_plus", "max_promoted")
STRUCTURAL_INEQUALITY = "FWER_final <= FWER_search <= FWER_band"

RNG_STREAMS = ("outer_null_world", "inner_chance_band", "inner_bootstrap")

REPORTED_SEPARATELY = True
# One number for "the incremental FWER" would pool two different null models. It is not computed.


def digest() -> str:
    return hashlib.sha256(json.dumps({
        "generators": GENERATORS, "rule": DATE_LEVEL_RULE,
        "date_level_features": list(DATE_LEVEL_FEATURES),
        "n_replications": N_REPLICATIONS, "alpha": ALPHA,
        "tolerance": list(TOLERANCE_BAND), "metrics": list(METRICS),
        "inequality": STRUCTURAL_INEQUALITY, "streams": list(RNG_STREAMS),
        "reported_separately": REPORTED_SEPARATELY,
    }, sort_keys=True).encode()).hexdigest()


if __name__ == "__main__":
    print(f"N0 SPEC DIGEST  {digest()}")
    print(f"  rule   {DATE_LEVEL_RULE}")
    print(f"  daily  {DATE_LEVEL_FEATURES}")
    for gid, g in GENERATORS.items():
        print(f"\n  {gid}   ({g['n_classes']} classes)")
        print(f"    H0          {g['h0']}")
        print(f"    permutes    {g['permutes']}")
        print(f"    membership  {g['membership']}")
