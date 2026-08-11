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

    H₀ : Y ⊥ DayProperty, with the property's own temporal geometry preserved

The daily label sequence is CIRCULARLY SHIFTED by a random offset; outcomes are never touched.
This is the "permute the labels" form, exact here for the reason it was NOT exact in v1: a
date-level feature HAS a single label per date, so there is something to exchange. v1's cells
overlapped and no single label existed, which is why outcomes were permuted there instead.

An iid shuffle across dates was considered and REJECTED, on a measurement rather than a
preference — the flag is far from exchangeable across days (lag-1 +0.387, runs 63% longer than
iid, yearly prevalence 11.6-22.6%). Scattering vix-up days one at a time builds an easier world
than the one we trade in. A circular shift preserves runs, autocorrelation and prevalence
exactly and breaks only the alignment between the property and the returns of those days.

Preserved: every outcome as observed, the within-date correlation structure, family composition,
the number of day-property days AND their clustering. Destroyed: the link between the market-day
property and returns — the whole and only content of H₀ᴳ².

THE DESIGN IS RECOMPUTED INSIDE THE NULL. G2 permutes the PREDICTOR, so membership moves — and
eligibility, strata and weights all follow from membership. Freezing the weights while the
membership moves would produce a hybrid: null-world membership carrying real-world weights, an
object with no scientific meaning. So the whole deterministic X → design mapping is part of the
test statistic. What does NOT move is the POLICY: n_min, dates_min and the concentration cap are
the same frozen numbers in every world, only their consequences differ.

G1 needs none of this because X is fixed there and the estimand is literally unchanged. The
asymmetry is real and is carried through to the end rather than stopped at membership.

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
    "date_level_label_circular_v1": dict(
        h0="Y ⊥ DayProperty, with the day-property's own temporal geometry preserved",
        permutes="the daily label sequence is CIRCULARLY SHIFTED by a random offset; "
                 "outcomes untouched",
        membership="RECOMPUTED, and so are eligibility, strata and weights — variant A below",
        applies_to="classes containing any date-level feature",
        n_classes=N_CLASSES_G2),
}

# ── why circular and not iid, measured rather than chosen ────────────────────
# An iid shuffle of the daily labels preserves prevalence and destroys everything else. The flag
# is nowhere near exchangeable across days:
#
#     prevalence            16.1% of 1,304 days
#     lag-1 autocorrelation +0.387
#     vix-up runs           mean 1.94 vs 1.19 expected under iid   (+63%)
#     ordinary runs         mean 10.04 vs 6.21 expected            (+62%)
#     yearly prevalence     11.6% … 22.6%
#
# Volatility clusters; that is the best-known fact about it. A null that scatters vix-up days
# one at a time across the calendar is an easier world than the one we live in, and a rate
# measured against it would flatter the engine.
#
# A circular shift of the label sequence preserves runs, autocorrelation and prevalence EXACTLY
# and breaks only the alignment between the day-property and the returns of those days — which
# is the whole content of H₀. It draws from 1,304 distinct offsets, ample for 200 replications.
#
# `date_level_label_iid_v1` is REJECTED, and recorded as rejected rather than omitted, because
# the two generators answer different probabilistic experiments and a later reader must be able
# to see that the choice was made deliberately.
# WHAT THE CIRCULAR SHIFT DOES NOT PRESERVE, stated so it cannot later be over-read. The shift
# moves whole blocks along the calendar, so a run of vix-up days from 2022 can land in 2025 and
# the yearly prevalence travels with the sequence. Preserved: global prevalence, the cyclic run
# structure, the lag geometry of the flag itself. NOT preserved: calendar-era composition. So the
# hypothesis tested is
#
#     H₀ : Y ⊥ DayProperty            with the property's own serial structure held
#
# and NOT the stronger
#
#     H₀ : Y ⊥ DayProperty | Year, Regime
#
# That is a deliberate scope, not an oversight, and the runner prints all four facts every run.
CIRCULAR_PRESERVES = {"global_prevalence": True, "cyclic_run_structure": True,
                      "flag_lag_geometry": True, "calendar_year_prevalence": False,
                      "wrap_around_used": True}

# No date-level claim may vanish from the denominator: in every G2 world,
#     evaluated + ineligible + uncomputable == 6
G2_ACCOUNTING_IDENTITY = "evaluated + ineligible + uncomputable == N_CLASSES_G2"
# and each world records a design_hash over (membership, strata, weights), so a real-world weight
# reused by accident is caught by provenance and not only by a test.
G2_RECORDS_DESIGN_HASH = True

REJECTED_GENERATORS = {
    "date_level_label_iid_v1": "destroys the flag's own clustering; lag-1 +0.387 and runs 63% "
                               "longer than iid make that a materially easier world",
}

# ── variant A: the design is recomputed inside the null, thresholds are not ──
# G1 permutes Y with X fixed, so E_c and w_cs stay frozen and the estimand is literally
# unchanged. G2 permutes the PREDICTOR, so membership changes — and then eligibility, strata and
# weights all follow from it. Freezing the weights while the membership moves would produce a
# hybrid object, null-world membership carrying real-world weights, with no scientific meaning.
#
# So the whole deterministic X → design mapping is part of the test statistic and is recomputed:
#
#     permuted DayProperty → membership → eligible strata → opportunity weights → statistic
#
# What is NOT recomputed is the POLICY. n_min = 100, dates_min = 25 and concentration ≤ 0.20 are
# the same frozen numbers in every null world; only their consequences move.
DESIGN_RECOMPUTED_IN_G2 = ("membership", "eligible_strata", "opportunity_weights")
POLICY_FROZEN_IN_G2 = ("n_min=100", "dates_min=25", "max_single_date_share=0.20")

# A permutation can push a claim out of eligibility. That must be visible, not silently absent.
REQUIRED_G2_DIAGNOSTICS = ("selectable_classes_per_world", "support_coverage_per_world",
                           "worlds_where_claim_uncomputable")

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
        "rejected_generators": REJECTED_GENERATORS,
        "design_recomputed_in_g2": list(DESIGN_RECOMPUTED_IN_G2),
        "policy_frozen_in_g2": list(POLICY_FROZEN_IN_G2),
        "g2_diagnostics": list(REQUIRED_G2_DIAGNOSTICS),
        "circular_preserves": CIRCULAR_PRESERVES,
        "g2_accounting": G2_ACCOUNTING_IDENTITY,
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
