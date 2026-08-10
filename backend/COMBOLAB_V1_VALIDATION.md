# ComboLab v1 (marginal) — generation record

**Engine** `6f7e5ee496551ab2bdb10dfa3170ed0e9efa6c1f`, sealed and untouched since freeze.
**Claim** marginal: `median(cell) − median(complement)` over the $21-89 base population.

```
├─ estimator validation        PASS   harness_power, harness_v1_v2
├─ decision validation         PASS   verdict_v2, Δ_gov
├─ sealed search validation    PASS   needle, 120 sealed seeds
├─ structured null A           PASS   FWER 0.055 vs nominal 0.05
├─ nuisance decomposition B    DESIGN FINDING — not a defect
└─ temporal integrity          FOUND A CONTRACT GAP, then closed it
```

## Temporal integrity — recorded as what it was, not as a score

```
discovered failure   daily-resolution same-day ambiguity lets an outcome record
                     appear contemporaneously accessible
observed live leak   NONE in the currently registered real sources — earnings and
                     insider are keyed on filing dates, nothing on date_out
contract gap         YES
remediation          temporal_contract.py, regression-locked 9/9
```

`8/9` is not a permanent verdict; it is the state before the gap was closed. `attach()` behaves
exactly as contracted and the boundary probes prove it separately: `available = decision` is
visible, `available = decision + 1d` is not. The exposure came from the resolution of the data.

**The fix is not `assert date_out > date_in`.** Inside the day the entry really may have preceded
the exit. What the data lacks is not validity but ORDER, so the 253 rows stay and only their
misuse becomes impossible:

```
date_out <  date_in                    corrupt        FATAL
date_out == date_in at day resolution  ORDER UNKNOWN  SAME_DAY_ORDER_AMBIGUOUS
```

The general rule, of which `date_out` is one instance: **a system may not infer an ordering its
data does not contain.** `available <= decision` is insufficient once both sides are dates;
what is required is `available < decision`, or a known intraday phase ordering on both sides.

Strongest form, and the one that guards the scenario the barrier surfaced — someone registering
a source keyed on the exit tomorrow: an outcome-derived field (`date_out`, `ret`, `mae`, `mfe`,
`stop_hit`, …) may never anchor or feed a FEATURE source at any resolution, while remaining
exactly right in label space.

**What SEC licenses, stated narrowly.** Proven: a record is unreachable before its filing date.
Not proven: that it was readable before the open on that date. Stored as
`temporal_resolution = DAY, same_day_ordering = UNKNOWN`, which stops `2024-05-15` becoming the
fictitious timestamp `2024-05-15 00:00:00`.

And the quantified cost of the anchor the fundamentals work was built to avoid: under
`period_end`, **34,143 of 40,000 sampled opportunities contaminated (85.4%), median lead 395
days, p90 400, max 400**. That argument now has a number.

This is the best available outcome for a final barrier. It did not merely pass — it found a hole
that had not yet done damage, before a source existed that could exploit it.

The barrier is being finished **on the frozen v1** before anything is rebuilt. Changing the
estimand now would leave no generation of this system fully characterised: estimator, decision,
search and false-discovery would all have been measured on the marginal claim, and only the
temporal test on its successor.

## What each stage measured

| stage | result |
|---|---|
| estimator | resolves δ = 0.60pp 95.8% of the time when pointed at the cell |
| decision | `Δ_gov` +26pp at δ = 0.60 after the L2a/R2 defects were removed |
| search | δ ≈ 3pp found and promoted ~half the time; δ = 0.60pp promoted 3.3% |
| null A | FWER_band = FWER_search = FWER_final = 0.055, inside `[0.02, 0.10]` |
| null B | promotion in 52.5% of worlds with cell-specific information removed |

**The bottleneck has one address, confirmed twice independently.** In the needle test
`final_accept == search_recall` at every δ; under null A all three FWERs are equal at 0.055.
Neither the bootstrap nor governance removes anything beyond the multiplicity band.

## Corrections to the commit message of 66389d3

Two claims in that message went past what was measured. The commit is kept as written and
corrected here rather than amended, so the overstatement stays in the record with its date.

**1. "about half of what the search finds is true as stated and useless as guidance."**
Overstated. What was measured is

```
P(any promotion | B1) = 0.525
```

— in 52.5% of worlds where the Cell→Y association *within* `Date × BaseSetup` has been
destroyed, the marginal search still promotes something. That is a **promotion propensity in a
world without cell-specific information**. It is not an estimate of `P(a real finding is
nuisance-explained)`, which would require taking the actual real-H promotions and testing their
incremental effect against `BaseSetup`. That was not done.

The defensible ledger sentence:

> The current marginal search has a high promotion propensity (52.5%) in worlds where
> cell-specific information beyond `BaseSetup` has been removed. This indicates substantial
> susceptibility of marginal discoveries to setup composition, and motivates an incremental v2.

Which is already more than enough to justify the redesign.

**2. "The nuisance is setup family, not price."**
Not established. At n = 200, `SE ≈ 3.5pp`; the B1 − B2 gap is 2pp, well inside Monte-Carlo
noise, and the two generators differ in permutation geometry and fallback profile besides. What
was measured is that **adding `PriceBucket` to the registered conditional generator produced no
appreciable change in promotion survival in this experiment**. Consistent with the structure
running mainly through `BaseSetup`; it identifies neither cause nor excludes price.

## The limitation, stated precisely

Null A validated the *implementation* — its generator is the same model the chance band
implements, so a calibrated result there was close to guaranteed and its value is in confirming
the Monte-Carlo machinery. Null B was built precisely because A could not test the model, and it
found that the **marginal question itself is scientifically insufficient**, not that the code is
wrong.

That is a limitation of the question v1 asks, not a bug requiring a patch. Limitations of a
question are fixed by a new generation, after the old one has been fully measured.

## v2, when it opens

The scientific question changes from marginal association to incremental information:

```
v1   median(Y | Cell) − median(Y | ¬Cell)
v2   incremental effect of Cell conditional on BaseSetup      ≈  Edge(A+B) − Edge(A)
```

A new estimand is a new measuring instrument. `studio_verdict` is shared and parts of decision
validation may carry over, but **search sensitivity does not transfer** — v1's needle curve says
nothing about a v2 whose control, geometry and multiplicity differ. v2 therefore needs its own:

```
preregistration → incremental estimator positive control → needle search control
               → structured null appropriate to the incremental claim → sealed acceptance
```
