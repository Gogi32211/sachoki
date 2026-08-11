"""S1 — the incremental needle, frozen before the runner.

D1 established what the decision engine can do when the claim is named in advance. S1 asks the
one remaining question of the generation: what does it cost to have to FIND the claim instead.

═══ WHY THE LOCATION IS BALANCED, NOT RANDOM ════════════════════════════════

v1 drew the needle uniformly. With 37 classes and 120 replications that gives ≈ 3.2 landings per
class per δ, and class-conditional recall would read `0/3`, `2/4`, `4/4` — noise wearing the
shape of a measurement. D1 makes this fatal rather than merely weak: at δ = 0.60 acceptance runs
from 0% to 80% ACROSS CLASSES AT THE SAME TRUE EFFECT, and no class is accepted always. A
uniformly-drawn aggregate recall would therefore be a mixture of easy and hard claims in
whatever proportion the RNG produced, not a property of the instrument.

So every class receives exactly `M_PER_CLASS` injections at every δ. The ORDER is still drawn
from the `needle_location` substream, so nothing about which class comes when is predictable
from the engine's side; only exposure is controlled.

    aggregate recall = (1/37) Σ_c recall_c        equal weight per claim-class

That is a scientific choice and it is frozen here. Equal weight per class answers "for a typical
selectable claim". Weighting by opportunities would answer "for a typical opportunity" — a
different question, and reporting one while meaning the other is the error this project has now
made in three different guises.

═══ PAIRED ATTRIBUTION — THE POINT OF S1 ═══════════════════════════════════

The tempting way to price search is to correlate D1's per-class acceptance with S1's per-class
recall. That is an observational association between two harnesses. Everything needed for a
paired comparison already exists inside a single S1 replication: the planted class has an
estimate, a CI, and a full verdict regardless of whether the search found it.

    KNOWN_LOCATION_ACCEPT   would this claim pass, had it been registered in advance
    RANK_5                  did the search rank it into the top 5
    SEARCH_PROMOTED         did it survive the chance band
    FINAL_ACCEPT            did it reach a positive verdict

Same world, same class, same δ, same bootstrap draw, same estimate and interval. The ONLY
difference is the presence of the search layer, so

    P(SEARCH_PROMOTED | KNOWN_LOCATION_ACCEPT)

is the price of discovery with nothing else varying. The chain that follows is the real
decomposition:

    resolvable at a known location → can search rank it → can it survive multiplicity → verdict

D1 does not become redundant; its role changes. It characterises each class in advance
(decision sensitivity, support fraction, eligible setups, CI width), and S1 can then ask which
of those pre-measured properties explains search heterogeneity. That is a DIAGNOSTIC, chosen in
advance and short — correlation of D1 acceptance with S1 recall, and search tax against eligible
setups, support coverage and CI width. Not a regression kitchen sink assembled after seeing the
results.

═══ TRUTH-AWARE RANKING ════════════════════════════════════════════════════

In v1 a promoted neighbour of the needle was classified. In v2 it has a NUMBER: the synthetic
world is fully known, so θ_j^true is computable for all 37 classes after injection, and subsets,
supersets and negations of the planted class acquire real non-zero truths (a negation gets
exactly −δ, a subset nearly +δ).

So planted-class rank alone is an incomplete verdict on the search. If the planted class has
θ^true = 3.0 and one of its subsets has θ^true = 2.9, a subset at rank 1 is not an error worth
punishing. Recorded alongside:

    rank_of_planted_class
    rank_of_max_true_effect_class
    regret = max_j θ_j^true − θ_selected^true

Planted-class recall stays — the needle really was planted there and it is the preregistered
target — but regret is what says whether the search chose badly or merely chose a near-twin.

═══ δ = 0 ═════════════════════════════════════════════════════════════════

No recall and no rank: there is no needle. Unlike real-H δ = 0 in v1 — which was contaminated by
real structure and could not be read as a false-positive rate — the synthetic composition world
has a KNOWN incremental null, so a false-promotion rate here is meaningful.

It is still NOT the same quantity as N0's structured null, and `sampling_target` keeps them
apart: this one is conditional on `synthetic_dgp(incremental_composition_generator_v1)`, N0's on
its own registered permutation generator. Two null models, two numbers, no comparison.
"""
from __future__ import annotations

import hashlib
import json

M_PER_CLASS = 5              # injections per class per δ → 37 × 5 = 185 replications per level
N_CLASSES = 37
DELTA_GRID = (0.00, 0.60, 1.50, 3.00, 6.00)          # unchanged from v1 and D1
TOP_K = 5
N_PERM = 120                 # permutation band, parallelised as in v1
N_BOOT = 200

AGGREGATION = "equal_weight_per_claim_class"
LOCATION_DESIGN = ("balanced exposure: every class receives exactly M_PER_CLASS injections at "
                   "every δ; the ORDER comes from the needle_location substream")

PER_RUN = ("known_location_accept", "planted_rank", "rank_le_3", "rank_le_5",
           "search_promoted", "final_accept", "rank_of_max_true_effect", "regret")
PER_CLASS = ("recall_curve", "known_location_acceptance", "search_tax")
AGGREGATE = ("median", "p10", "p90", "equal_class_rate")

DIAGNOSTICS = ("corr(D1_acceptance_c, S1_search_recall_c)",
               "search_tax_c = known_location_accept_c − search_promoted_c",
               "search_tax vs eligible_setups · support_fraction · ci_width")
# Diagnostics, not acceptance criteria. Chosen now so they cannot be chosen later.

RNG_STREAMS = ("needle_location", "needle_effect", "search_chanceband", "search_bootstrap")

SAMPLING_TARGET = "synthetic_dgp(incremental_composition_generator_v1)"
# Kept distinct from N0's structured-null generator. Two null models, two numbers, and
# sampling_target refuses the comparison between them.


def digest() -> str:
    return hashlib.sha256(json.dumps({
        "m_per_class": M_PER_CLASS, "n_classes": N_CLASSES, "delta_grid": list(DELTA_GRID),
        "top_k": TOP_K, "n_perm": N_PERM, "n_boot": N_BOOT, "aggregation": AGGREGATION,
        "location": LOCATION_DESIGN, "per_run": list(PER_RUN), "per_class": list(PER_CLASS),
        "diagnostics": list(DIAGNOSTICS), "streams": list(RNG_STREAMS),
        "sampling_target": SAMPLING_TARGET,
    }, sort_keys=True).encode()).hexdigest()


if __name__ == "__main__":
    print(f"S1 SPEC DIGEST  {digest()}")
    print(f"  replications  {N_CLASSES} classes × {M_PER_CLASS} × {len(DELTA_GRID)} δ = "
          f"{N_CLASSES * M_PER_CLASS * len(DELTA_GRID)}")
    print(f"  aggregation   {AGGREGATION}")
    print(f"  paired        known_location_accept measured in the SAME replication as "
          f"search_promoted")
