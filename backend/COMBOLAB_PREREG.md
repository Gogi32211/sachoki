# ComboLab needle test — preregistration, frozen 2026-08-10

**SPEC DIGEST** `fc70130c7d9faaec7a61d9b476b8d35167fb07f30f3c216f6ced015c79b73c10`
**SEED COMMITMENT** `90bfafc7021d958d…`
*Amended once, still before any ComboLab code — see "Amendment, same day" at the end.*

Written **before ComboLab exists**. Normally a positive control is added after the algorithm
works and its behaviour is explained backwards. The order here is inverted, and that inversion
is the point:

```
claim about the required capability → frozen test → implementation → sealed evaluation
```

## The question

Not "can the statistic recover a known δ" — that is answered (`harness_power`, `harness_v1_v2`).
Not "can governance classify a known δ" — that is answered (`verdict_v2`, `Δ_gov`).

> **If a real effect is hidden among N competing candidates, can the search layer bring it to
> the top without knowing where it is?**

This is the last large positive control missing between the validated estimator/verdict engine
and the first real study (`dilution_yoy`).

## Where this sits

```
ESTIMATOR VALIDATION        can the statistic recover a known δ                    ✅
DECISION VALIDATION         can governance classify a known δ                      ✅
SEARCH VALIDATION           can ComboLab find a hidden δ among N candidates        ← this
FALSE-DISCOVERY VALIDATION  does search stay quiet under a structured null         next
TEMPORAL-INTEGRITY VALID.   can forbidden timing manufacture apparent information  after
```

Kept separate on purpose. The recurring failure of the last three days is one test answering for
a property it never measured.

## SEARCH SPACE

`combolab_spec.py` · **k = 46, derived from the manifest, never typed.**

| family | claims |
|---|---|
| flag_single | 10 (5 flags + 5 negations) |
| flag_pair | 10 |
| flag_triple | 4 |
| rsi_band | 4 |
| adx_regime | 5 |
| flag_rs_cond | 4 |
| rsi_rs_cond | 3 |
| rsi_conso_cond | 3 |
| adx_rs_cond | 3 |

One estimand × one horizon × one control definition, so **each cell contributes exactly one
selectable claim**:

```
estimand   median_return_difference_pp
horizon    path_sim_exit  (the table's own `ret`)
control    complement within the base population
base       price $21-89 · deduplicated by dup_group · finite ret and sig_close
```

**On `k`.** k counts *distinct claims the search was allowed to inspect or select*. A claim that
passes `evidence → materiality → stability → OOS → DSR` is **one** hypothesis with several
acceptance conditions, not five trials. Had the search been free to pick the best of several
horizons or exit rules, each would have been a separate claim. Correcting the earlier
undercounting must not overshoot into inflating multiplicity.

`assert_search_space()` raises `SearchSpaceContractError` if the implementation ever produces a
47th selectable result. Stronger than documentation.

## OVERLAP

Cells overlap by construction — singles, negations, pairs, triples and conditioned variants
share rows. A needle planted in `cell_17` legitimately leaks into any cell sharing its
observations, and promoting such a neighbour is **not** a false discovery; it is the geometry of
the space.

Frozen before injection, and every promotion is classified.

`E[j,i] = |Cj ∩ Ci| / |Cj|`, asymmetric — and the orientation is the substance. Because the injection adds δ to
**every** row of the needle cell, `E[j, needle]` is not a similarity between rules: it is
literally the share of cell *j*'s observations that carry the injected outcome.

```
TRUE_NEEDLE          the planted cell itself
OVERLAP_AFFECTED     exposure ≥ τ = 0.20 — i.e. ≥20% of the PROMOTED cell's rows carry injection
UNRELATED_PROMOTION  ← the only class counted as a false discovery
```

Both the metric and τ are inside the `SPEC DIGEST`, so "cell_29 came up next to the needle, its
overlap is 14%, let us call that contaminated" is not available after the fact.

## INJECTION

**Membership is frozen before injection.** The needle must not change which opportunity falls in
which cell, eligibility, setup membership, universe, or any feature that defines the search
space — otherwise ComboLab could "find" the needle because the injection reshaped the population
it was searching.

```
real data → freeze 46-cell membership → RNG picks hidden cell
          → shadow-outcome injection → ComboLab sees ordinary research objects
```

`delta_grid = [0.00, 0.60, 1.50, 3.00, 6.00]` pp, median return difference.

| δ | provenance |
|---|---|
| 0.00 | false-discovery control — **no needle exists** |
| 0.60 | `near_estimator_MDE` — frozen ref `engine_return_v1`: estimator 95.8%, MATERIALITY 26% |
| 1.50 | `above_estimator_MDE` — frozen ref `harness_power`: MDE@80% ≤ 1.20pp (pooled arm) |
| 3.00 | preregistered upper sensitivity probe |
| 6.00 | preregistered upper sensitivity probe |

**No scaling claim.** 3.00 and 6.00 are *not* derived from √46 or from any known search-layer
MDE. Cells overlap, date mass is wildly unequal, 59 families coexist — none of the assumptions
behind such scaling hold. **The search-layer MDE is unknown, and measuring it is the deliverable.**
δ is not re-chosen after a disappointing run; the grid is frozen here.

### δ = 0 has no needle

Recall and `true_cell_rank` are **undefined** at δ = 0 and will not be reported. A pseudo-location
may be assigned for code symmetry but must never be called the true cell — a pseudo-needle
landing at rank 1 by chance would manufacture a meaningless "recall under null". At δ = 0 the
only outcomes are `conditional_false_promotion_rate`, `unrelated_false_promotions`, and the best
null statistic.

## SELECTION

```
ranking statistic   per-cell median return difference vs complement
top-K rule          K = 5, preregistered
chance band         block permutation within date (studio_gates.chance_band)
multiplicity        DSR / FDR over declared k = 46
promotion rule      survives chance band AND multiplicity policy
final verdict       studio_verdict.decide (v2)
```

## OUTCOMES — three definitions of "found", never one

```
RANK_FOUND              true cell in the preregistered top-5
STATISTICALLY_PROMOTED  true cell survives chance band + multiplicity
FINAL_ACCEPTED          true cell receives a positive final verdict
```

A cell can rank 1st, be promoted, and still end at `REJECT:VALIDITY`. That is a **legitimate
governance refusal** and must not be recorded as a search failure — the same attribution
discipline that separated L2 from the interval and the estimator from the gates.

Reported: `conditional_rank_recall`, `conditional_search_recall`,
`conditional_final_acceptance`, `unrelated_false_promotions`, `overlap_affected_promotions`, and
the **rank distribution** — `P(rank=1)`, `P(≤3)`, `P(≤5)`, median, p90. Not the average: a median
rank of 2 is compatible with 70% at rank 1 and 30% at rank 35, and for a search engine those are
entirely different machines.

## SAMPLING SEMANTICS

```
kind             finite_population_subsample
conditioned_on   realized_history_2021_2026
unit             trading_date
```

Every outcome is conditional on this history. `sampling_target.py` refuses to let any of them be
reported as `power`. The claim this test can support is:

> Within the realised historical composition and the declared search space, ComboLab can surface
> a pre-hidden effect of this type and size after the declared multiplicity policy.

Not: *ComboLab has 95% power in future markets.*

## RNG

Four named substreams — `needle_location`, `needle_effect`, `search`, `bootstrap` — so that
adding a diagnostic later does not move where the needle landed.

## TEST PARTITION — the seal

```
smoke        3 seeds     visible; does the code run at all
development  40 seeds    visible; debug the search architecture as often as needed
acceptance   120 seeds   DO NOT EXIST YET — derived from the freeze commit hash
```

**A plaintext seed list is not a seal.** The first version of this document listed the 120
acceptance seeds openly. That commits to *which code* the acceptance set saw, but leaves
`freeze A → open acceptance → see numbers → change ComboLab → freeze B` available; the ledger
records the violation, but the test is already contaminated. A secret file beside the repo is no
better, because I can read it.

So the acceptance seeds are derived from something that **cannot exist until the implementation
is frozen** — the git hash of the freeze commit itself:

```
acceptance_seeds = sha256("combolab-acceptance:" + freeze_commit_sha) → default_rng → 120 ints
```

That hash is determined by the frozen code and is unknowable while the code is still being
written, including to me. What is committed now is the **derivation rule**; the seeds materialise
at freeze and are verifiable afterwards by anyone holding the commit. Calling
`acceptance_seeds("")` raises rather than returning anything.

If the frozen acceptance replications are run repeatedly while the search logic is being changed
in response, the acceptance set becomes a development set, and `P(pass)` starts measuring the
developer's ability to tune ComboLab against a known test. After ten iterations that number
means nothing.

**Mechanically enforced**, not remembered: the acceptance runner refuses to start without an
`IMPLEMENTATION_FROZEN` marker naming the git hash of `combo_lab.py`, and writes that hash into
its results. Editing the engine after seeing acceptance numbers leaves a mismatch visible in the
ledger. This is the frozen-OOS principle turned on our own research infrastructure.

## Order of work

```
1  preregistration freeze          ← this document, committed before any ComboLab code
2  claim manifest + overlap        ← combolab_spec.py, k derived = 46
3  ComboLab implementation         debugged against SMOKE and DEVELOPMENT only
4  implementation freeze           IMPLEMENTATION_FROZEN + git hash
5  sealed acceptance evaluation    opened once
```

Only after this does the first real study — `dilution_yoy` — become meaningful. Until then we
can say the statistic measures an effect when shown the right sample; we cannot yet say the
search machinery finds one whose location took no part in its development.


## Amendment, same day — before any implementation

Two leaks were closed after the first freeze and before a line of `combo_lab.py` was written,
which is why this is an amendment and not a violation:

1. **The acceptance seeds were plaintext.** Replaced by derivation from the freeze commit hash
   (above). `SEED_MANIFEST_SHA` → `SEED_COMMITMENT`.
2. **`overlap` renamed to `exposure`,** with the metric stated as the literal fraction of a
   promoted cell's rows carrying the injected outcome rather than a similarity between rules.
   `τ = 0.20` was already inside the digest; the metric definition now is too.

`SPEC DIGEST` moved `c020cc5c…` → `fc70130c…`. No further amendment is permitted: from here the
next commits are `combo_lab.py`, debugged against **smoke and development only**.

## No new acceptance metrics after implementation

Anything additional the implementation makes visible is kept as an *exploratory diagnostic*.
The acceptance evaluation answers only what is fixed above: `conditional_rank_recall`,
`conditional_search_recall`, `conditional_final_acceptance`, `P(rank=1)`, `P(rank≤3)`,
`P(rank≤5)`, median and p90 rank, `unrelated_false_promotions`, `overlap_affected_promotions`.
