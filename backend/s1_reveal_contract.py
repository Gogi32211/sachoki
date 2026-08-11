"""Commitments made BEFORE the S1-31 reveal. Nothing imports this; git is the point.

`s1_spec.py` was briefly edited while the S1-31 run was in flight. The run was unaffected —
Python had already imported the module, and the added constants were declarative and read by no
code path — but the file on disk stopped matching the artifact the experiment used:

    run used      s1 digest d4a9bb1e72228200
    disk became   811eb797e98568c1

That is exactly the drift this whole layer exists to prevent, so `s1_spec.py` was reverted to the
version the run imported, and the commitments live here instead. They are timestamped in git
before any result is visible, which is the only property that matters.

── THE THIRD LINK IS COMBINED, AND ITS NAME SAYS SO ─────────────────────────

The runner records `search_promoted` as one flag, so the decomposition available from this
generation is:

    D = P(KnownLocationAccept)
    R = P(Rank ≤ 5 | KnownLocationAccept)
    S = P(SearchScreenPass | Rank ≤ 5, KnownLocationAccept)
    F = P(FinalSearchAccept | SearchScreenPass, Rank ≤ 5, KnownLocationAccept)

    SearchScreenPass := θ > chance_band AND CI_low > 0
"""
SEARCH_SCREEN_COMPONENTS = ("chance_band", "bootstrap_evidence")
COMPONENTS_SEPARATELY_OBSERVED = False
# P(θ > band) and P(CI_low > 0 | θ > band) cannot be separated under this freeze. `S` is
# therefore NOT a band tax, and must never be described as one. Splitting them is a recorder
# change and belongs to the next generation, not to a retrofit of this run.

NEXT_RECORDER_FIELDS = ("rank_pass", "band_pass", "bootstrap_evidence_pass",
                        "search_promoted", "final_accept")

# ── read the structural implications before any sensitivity number ───────────
IMPLICATIONS = (
    "FinalSearchAccept ⇒ SearchScreenPass",
    "SearchScreenPass ⇒ Rank ≤ 5",
    "FinalSearchAccept ⇒ KnownLocationAccept",
)
# The last is the strongest: the search path adds constraints to the same Estimate and the same
# verdict, so a single (Known = 0, Final = 1) is not an interesting statistical case. It is a
# violation of the paired architecture, and sensitivity is not discussed until it is resolved.

# ── the primary quantity, fixed before the reveal ────────────────────────────
PRIMARY = "Retention(δ) = P(FinalSearchAccept = 1 | KnownLocationAccept = 1)"
PAIRED_TAX = "Tax(δ) = P(Known = 1, Final = 0)"
TAX_LOCALISED_TO = ("ranking loss: P(Rank > 5 | Known = 1)",
                    "post-ranking screen loss: P(SearchScreenPass = 0 | Rank ≤ 5, Known = 1)")

NO_NEW_PRIMARY_ENDPOINT_AFTER_REVEAL = True
FROZEN_AFTER_REVEAL = ("delta_grid", "rank_cutoff_top_k", "n_acceptance_seeds",
                       "acceptable_recall_threshold", "primary_aggregation")
# Diagnostics may be studied without limit. Keeping every existing metric untouched while
# noticing a prettier one in the diagnostics and naming THAT the primary for sealed is the
# loophole the frozen list alone leaves open; it is closed here.

# ── three diagnoses, not two ─────────────────────────────────────────────────
SCENARIOS = {
    "A": "Known low → the limit is decision precision / materiality; search has little to save",
    "B": "Known high, Rank ≤ 5 low → ranking competition is the tax",
    "C": "Known high, Rank ≤ 5 high, ScreenPass low → the post-ranking statistical screen is the "
         "tax (band vs bootstrap inseparable in this generation)",
}

# Runtime contention with other jobs slowed this run. That is operational, not experimental:
# the RNG is keyed on (world, δ, class, rep, stream) and never on scheduling, so a changed
# wall-clock must not later be read as changed experimental conditions.
RUNTIME_VARIATION_IS_NOT_EXPERIMENTAL = True
