# RANK_V1 · A — short-horizon OOS diagnostic · FROZEN 2026-09-13

## VERDICT: **FLAT** (`mtm_20`) · **no verdict** (`mtm_25`, sample too small)

**This is not a validation of Rank V1 and cannot rehabilitate the deployed artifact.** Different
horizon, six sessions, and a table whose own training labels are contaminated. The verdict
vocabulary was fixed in advance at three words — positive / flat / inverted — precisely so this
could not be read as more than it is.

## Frozen spec

| | |
|---|---|
| model | `data/rank_v1_model.json` **as deployed**, via `rank_v1.expected_edge` — **no refit** |
| rows | `sig_date > 2026-08-04` only — everything the artifact never saw |
| target | `mtm_20` and `mtm_25`, **reported separately, never pooled** |
| resolved | only rows where that horizon is genuinely resolved (the `072ff0b` contract) |
| edge | `mtm_N − same-day median` over distinct (ticker, day), days with ≥ 20 |
| unit | one row per (ticker, sig_date); prediction = MAX over its fires, as `rank_map` does |

## `mtm_20`

> ⚠️ **2,717 ticker-days · 6 sessions · 2026-08-05…08-12 · 1,831 tickers**

| bucket | n | mean | median | win % |
|---|---:|---:|---:|---:|
| 1 (lowest predicted) | 546 | +1.106 | +0.229 | 51.1 |
| 2 | 542 | +0.409 | −0.026 | 49.4 |
| 3 | 543 | +0.726 | −0.007 | 49.7 |
| 4 | 542 | +0.326 | +0.116 | 50.9 |
| 5 (highest predicted) | 544 | +0.338 | −0.269 | 48.7 |

```
Q5−Q1        −0.768 pp · rising steps 2/4 · slope −0.162
per-day Spearman  mean −0.0204 · median −0.0306 · positive 2/6 days
bootstrap by date  Q5−Q1      −0.735  90% CI [−2.32, +0.60]  P(≤0) 0.804
                   top decile +1.194  90% CI [+0.14, +2.34]  P(≤0) 0.033
liquidity strata   <p25 −2.26 · p25-50 −0.44 · p50-75 −1.21 · >p75 +0.45
```

**FLAT.** The 90 % CI on Q5−Q1 spans zero comfortably. No ordering, no inversion.

The one metric that excludes zero is the top decile (+1.19). It disagrees in sign with both Q5−Q1
and the Spearman, on six days. **Nothing is taken from it.** Six sessions is the floor of what a
diagnostic can even look at; "flat" here means "this sample shows nothing", not "no ordering
exists".

## `mtm_25` — no verdict

410 rows over **1 session**. Twenty-five bars past 2026-08-04 has barely been reached by anything.
Not pooled with `mtm_20`, not reported as a result.

## What this does not do

The deployed table is fitted through 2026-08-04, so the censored June–July cohort (~38k fires whose
`mtm_60` was a carried-forward short-horizon mark) is inside its training data. A positive A would
not have repaired that and a flat A does not add to it. Only a **refit on clean labels** addresses
the contamination — separate work, not this.

## Next

**B** is the real test: deployed artifact × fully-resolved `mtm_60`, `sig_date > 2026-08-04`, no
refit. That cohort cannot mature before ≈ 2026-10-30. Infrastructure and spec can be prepared;
results cannot.

Artifacts: `scratchpad/rank_a_short.py` · input `data/opportunities.parquet` (rebuilt 2026-09-13,
censor-aware) · prior snapshot `data/opportunities_through_2026-08-04.parquet`
