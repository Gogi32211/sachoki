# Harness semantics — ledger record, 2026-08-10

## Verdict

`bootstrap_ci_clustered` is **exonerated relative to the tested mechanism**. The prior evidence
of over-coverage is **withdrawn as invalid**.

The narrow form of the claim matters and is deliberate: the interval is justified *with respect
to the finite-population correction mechanism and the empirical cluster-resampling setting*.
It is **not** "the bootstrap is calibrated" in general. True coverage against independent market
realisations has never been measured and cannot be measured from one five-year window.

## What happened

The engine's positive controls reported coverage of 99.7% against a nominal 95%, and
`SE_boot / SD_MC = 1.63×`. Read as an over-wide interval, this was the standing explanation for
RETURN blocking 72/100 at MATERIALITY and RISK blocking 96/100 at NON_INFERIORITY.

Five diagnostics were run against a frozen `inference_v1`. Nothing in the estimator was changed
at any point.

| # | hypothesis | result |
|---|---|---|
| D | arms receive different cluster weights | **closed** — 1,284 shared dates × 25 replications, 0 elementwise mismatches |
| E | the weighted median is a coarse step function | **refuted** — unique/B 99.8%, largest atom 0.2%, one endpoint step = 0.2% of the interval |
| 4 | per-estimand miscalibration (median vs proportion) | **not reproduced** — 1.63× and 1.59×, the same on both |
| F | a few dates carry the statistic | **real but overstated 5×** — see below |
| **FPC** | **the ratio is the harness design, not the bootstrap** | **confirmed** |

### The decisive experiment

Prediction registered **before** the run: if the Monte Carlo draws a fraction `f` of clusters
without replacement from one fixed history, its variance carries `(1 − f)` while the bootstrap
answers the unconditional question, so

```
SE_boot / SD_MC  ≈  1 / √(1 − f)
```

| f | n dates | SD_MC | SE_boot | observed | predicted | obs/pred |
|---|---|---|---|---|---|---|
| 0.30 | 385 | 0.2260 | 0.2859 | 1.265× | 1.195× | 1.06 |
| 0.50 | 642 | 0.1488 | 0.2243 | 1.508× | 1.414× | 1.07 |
| 0.70 | 898 | 0.1050 | 0.1876 | 1.786× | 1.826× | 0.98 |
| 0.90 | 1,155 | 0.0534 | 0.1649 | 3.086× | 3.162× | 0.98 |

`r = +0.9987 · mean obs/pred = 1.02` over a 2.6× span. This is a mechanism that predicted the
shape of the curve in advance, not a story fitted to the data afterwards.

## The defect, and where it is recorded

```
inference_v1            UNCHANGED — no fix was applied and none is indicated

harness_semantics_v1    INVALID CALIBRATION COMPARISON
                        finite-population Monte-Carlo variance was used as ground
                        truth for empirical-cluster bootstrap uncertainty

harness_semantics_v2    SUBSAMPLE STABILITY  ≠  INFERENCE CALIBRATION
```

Three numbers were involved and **all three were computed correctly**: `SD_MC`, `SE_boot`, and
the FPC. The error was one level above arithmetic — two correct numbers describing different
probabilistic experiments were compared as if they described the same one. That is a new
failure class for this project, and it is now the fifth contract layer.

| layer | question |
|---|---|
| data contract | can the data be trusted |
| computation invariants | is the arithmetic what it claims |
| report provenance | did anything print a statistic no estimator produced |
| decision provenance | may this gate decide from what it was handed |
| **sampling semantics** | **do these two numbers describe the same experiment** |

Enforced by `sampling_target.py`; regression in `test_sampling_semantics.py` (9/9).
`calibration_metric()` refuses an incompatible pair and reports the expected discrepancy;
`descriptive_metric()` refuses to let a conditional rate answer to the name `power`.

## Renamings required

The shared-history harness measures real and useful things under the wrong names. 120
replications are not 120 new five-year histories; they are 120 subsets of one realised history.

| was | is |
|---|---|
| `EstimatorPower` | `ConditionalDetectionRate` |
| `EngineFPR` | `ConditionalAcceptanceRate` (at δ = 0) |
| `coverage` (shared-history) | **not reportable** — no compatible replication target exists |

`Δ_gov` survives intact but is conditional: `Δ_gov|H = P(V2|H) − P(V1|H)`. The paired design
makes the comparison clean *within* H, so the difference is attributable to governance, and the
mechanical defects in `L2a` / `R2` are additionally confirmed by Shapley. But `+26pp` is +26pp
on the compositions of this history, not an expected +26pp on a new five years.

## What each instrument may claim

```
CLUSTER BOOTSTRAP     repeated-sampling uncertainty under the empirical
                      cluster-resampling model — reshuffling regimes that occurred
SUBSAMPLE HARNESS     finite-population sensitivity, conditional on the realised clusters
SYNTHETIC DGP         behaviour of the code relative to a generator we wrote
FROZEN FORWARD/OOS    the only instrument entitled to speak about other five years
```

Resampling observed clusters cannot produce an unseen regime, a structural break, a new
microstructure, or a crisis of a type the window did not contain.

## Consequence for R6

`UNRESOLVED : NON_INFERIORITY` at 96/100 is **not** a bottleneck awaiting a fix. With the
bootstrap exonerated, the honest statement is:

> The risk benefit is detectable within the historical composition, but the available market
> clusters are not sufficient to exclude a return cost worse than −0.25pp at the uncertainty
> level intended for transfer beyond these dates.

This is the behaviour the architecture was built for: a system that can answer neither "no
effect" nor "broken gate" but *I can see the effect and cannot prove what the action costs.*

## Corrections issued during the investigation

- **Step-function median** — my own proposed mechanism. Measured, refuted. unique/B 99.8%.
- **"top-5 dates carry two thirds of the interval" (0.665)** — **withdrawn**. The numerator was
  a *sum of separate* leave-one-out shifts, and those are not additive; a CI half-width is not a
  variance decomposition. Joint removal gives a stress ratio of **0.12** at k=5, five times
  smaller. Influence is *signed*: removing the single most influential date moves the median
  −0.046, removing the top five moves it −0.033, because they push in opposite directions and
  partly cancel. Ranking by `|influence|` does not optimise joint perturbation — a reusable
  lesson for stress testing. At k=25 (11.6% of rows) the stress ratio reaches 0.88, which is a
  real adversarial-composition sensitivity even though it is not a variance explanation.
- **`diag2`'s printed verdict** ("worth fixing") — a hardcoded `> 1.20` threshold written before
  the FPC arithmetic existed. It compared 1.75 against 1, when the design demanded 1.83. Not a
  conclusion.
- **`diag4`'s wording** ("what OTHER five years would give") — overclaimed. Corrected in source.
- **"power remains valid"** — overstated. Detection rate is conditional on H.
- **`n_eff = 1,478`** — should read *iid-row-equivalent effective sample size* for this
  estimand and design. There are only **898** date clusters in that sample; 1,478 measures
  precision relative to a fictional row-iid model, not a count of independent market facts.

## What is NOT settled

Even fully exonerated, the interval's width is conditional on 1,284 dates being a fair sample of
market regimes. Five years containing one 2022 is not five draws of a 2022. Genuine coverage
against independent market realisations remains unmeasured, and no amount of resampling this
window will measure it.

## Files

`inference_diag.py` · `inference_diag2.py` · `inference_diag3.py` · `inference_diag4.py` ·
`inference_diag5.py` — diagnosis only, estimator untouched
`sampling_target.py` · `test_sampling_semantics.py` — the contract and its regression
