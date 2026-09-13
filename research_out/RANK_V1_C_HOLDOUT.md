# RANK_V1 · C — historical holdout refit · CORRECTED 2026-09-13

> ## ⚠️ CORRECTION — read this before the body
>
> **The first version of this report concluded "historically weak ordering with a sharp recent
> breakdown, worst in liquid names." That conclusion was wrong.** The breakdown was an artefact of
> a target-generation defect, not a market regime change.
>
> `add_mtm.py` carried the last available mark forward unconditionally, so a trade with only 14
> bars of data left still received a number in `mtm_60`. Censoring by signal month on the
> 2026-08-09 build: 03 **0.0 %** · 04 **0.0 %** · 05 **17.9 %** · 06 **90.7 %** · 07 **96.6 %**.
> In July, `mtm_60` was in truth a 14-bar return — 99.9 % of those rows had
> `mtm_60 == mtm_50 == mtm_40`.
>
> The monthly results tracked the censoring almost exactly — April, the only uncensored month in
> the test window, was flat (−0.09), while the heavily censored months produced −5.33 and −6.04.
> The single walk-forward fold that inverted, `2026-04-07..2026-07-07`, had **23 bars to the data
> edge instead of 60**.
>
> Fixed in `072ff0b` with ten hermetic tests. **What follows is the corrected reading.** The
> original numbers are kept below, labelled, rather than deleted — the error is part of the
> record.
>
> **Corrected result — 13 uncensored folds only:** Q5−Q1 positive **10/13**, median **+1.95 pp**,
> sign-test **p = 0.092** · per-day Spearman positive **11/13**, median **+0.0267**, **p = 0.022**
> · top-decile positive **13/13**.
>
> **Status: weak but consistent out-of-sample ordering. Not strong enough for a production-edge
> claim, and no longer evidence of a breakdown.**
>
> ⚠️ **The deployed artifact is separately compromised.** It is fitted through 2026-08-04, so the
> censored June–July cohort (~38k fires) is inside its training data. Historical validation of the
> METHOD can be clean; the deployed TABLE is a contaminated fit. Waiting for B does not repair
> that — it only measures it.

**Status (corrected): weak but consistent out-of-sample ordering; NOT validated.** The
"breakdown" reported in the first version was target censoring — see the correction above. Nothing below may be re-tuned on these numbers — in
particular, no threshold, family, liquidity or regime rule may be fitted to the failing fold.

## The question

Not "which primitive predicts a breakout" — that was V3-A, closed NULL. This asks whether the
ranking the app already serves **orders its own candidates**: on a given day, is the top-ranked
fire actually better than the bottom-ranked one?

## Why a refit, not the served artifact

`data/rank_v1_model.json` records `fitted_on.rows = 1,138,881`, `through = 2026-08-04`. The
opportunity table has **exactly** 1,138,881 rows with a resolved `mtm_60`, the last on
**2026-08-04**. The deployed table was therefore fitted on 100 % of the resolved outcomes that
exist — it has no out-of-sample data at all. The first genuinely unseen cohort (fires from
2026-08-05) cannot resolve a 60-bar outcome until ≈ 2026-10-30. So C refits the SAME METHOD on a
strict historical cut. **It tests the method, not the deployed table.**

## Frozen spec (verbatim from `rank_family.py`, nothing tuned)

| | |
|---|---|
| state | `family, rsi_band, conso, rs, px_band` |
| fallback | `cell → (family, rsi_band) → family → train mean` |
| shrinkage | `K_SHRINK = 60` |
| target | `mtm_60` (sacred path-sim); `edge = mtm_60 − same-day median`, days with ≥ 20 |
| unit | one row per `(ticker, sig_date)`; prediction = MAX over that day's fires, exactly as `rank_v1.rank_map` keeps the best family |
| train | `sig_date <= 2026-03-31` — 1,067,745 fires, 1,216 days |
| test | `sig_date >= 2026-04-01` — 71,136 fires, 86 days, ends 2026-08-04 |

## Sanity check that the sign is real

Same code, same model, same orientation:

| | Q5−Q1 | Pearson | Spearman |
|---|---:|---:|---:|
| TRAIN (in-sample) | **+4.158** | +0.0897 | +0.0582 |
| TEST (out-of-sample) | **−3.491** | −0.0598 | −0.0669 |

In-sample is monotone 1→5 and positive in **every** liquidity quartile (+3.44 … +4.76). The
inversion is not an orientation bug.

## ⚠️ The single-cut result was misleading — the walk-forward corrected it

*(and the walk-forward's own last fold then turned out to be censored — see the correction at the top)*

A single holdout said "inverted". Fourteen chronological folds (expanding train, 63-session tests)
say otherwise:

| fold | Q5−Q1 | | fold | Q5−Q1 |
|---|---:|---|---|---:|
| 2022-12→2023-03 | −0.41 | | 2024-12→2025-04 | +3.04 |
| 2023-03→2023-06 | −0.68 | | 2025-04→2025-07 | +0.51 |
| 2023-06→2023-09 | +2.67 | | 2025-07→2025-10 | +2.55 |
| 2023-09→2023-12 | +0.03 | | 2025-10→2026-01 | +3.80 |
| 2023-12→2024-04 | +2.69 | | 2026-01→2026-04 | **+5.57** best |
| 2024-04→2024-07 | +1.63 | | 2026-04→2026-07 | **−4.14** worst |
| 2024-07→2024-09 | +1.95 | | | |
| 2024-10→2024-12 | −0.34 | | | |

* Q5−Q1 positive **10/14**, median **+1.79**, sign-test p = **0.180**
* per-day Spearman positive **11/14**, median **+0.022**, p = 0.057
* top-decile positive **13/14**

**The one badly failing fold is the most recent, and it is the window the single cut happened to
use.** Second-worst fold is −0.68; the last is −4.14, **6.1× worse**. Consecutive quarters went
+5.57 → −4.14.

## ~~The breakdown scales with liquidity~~ — WITHDRAWN

**This table was computed on the censored window and does not support a conclusion.** The liquidity gradient below was measured where 53-97 % of the target was a carried-forward short-horizon mark. It is kept only to show what the defect looked like.

### (withdrawn) original table

| stratum | median $vol | TRAIN | TEST 04→08 |
|---|---:|---:|---:|
| `<p25` | $8 M | +4.42 | **+1.66** |
| `p25-50` | $35 M | +4.02 | −2.83 |
| `p50-75` | $112 M | +3.44 | −5.27 |
| `>p75` | $458 M | +4.76 | **−6.82** |

It breaks hardest exactly where one would actually trade. Not a fallback artefact — **99.9 % of
test rows resolve at full `cell` depth**; `fam`/`global` together are 0.1 %. Not concentration —
top-5 ticker share 0.44 % (1.87 % within `rank_pct >= 90`).

Monthly in the failing window: 04 −0.09 · 05 −5.33 · 06 −6.04 · 07 −2.49 · 08(2d) −0.95.

## Verdict against the pre-registered gate

| criterion | |
|---|---|
| monotonic ordering | ⚠️ weak — median Spearman +0.022 |
| top-vs-bottom practically meaningful | ⚠️ +1.79 pp median, sd 2.40 |
| CI not pinned to noise | ❌ sign test **p = 0.180** |
| survives liquidity/concentration | ❌ worst in the most liquid quartile |
| same direction in >1 sub-window | ✅ 10/14 |
| fallback rows not creating an illusion | ✅ 99.9 % full cell |

**Rank V1 cannot be relied on as a production edge today** — but for a different reason than the
first version gave. On clean folds the ordering is real and positive (10/13, median +1.95 pp) yet
weak: median per-day Spearman +0.027, Q5−Q1 sign-test p = 0.092. That is not enough for a
production-edge claim. The "liquid-universe inversion" is WITHDRAWN — it was censoring.

⚠️ The deployed artifact is fitted **through 2026-08-04**, so the censored June-July cohort
(~38k fires whose `mtm_60` was a carried-forward short-horizon mark) sits inside its training
data. It is on screen now. This is a defect in the artifact itself, not only in how it was
evaluated.

## Artifacts

`research_out/rank_c_walkforward.csv` · `rank_c_test_rows.csv` · `rank_c_quintiles.csv` ·
`rank_c_deciles.csv` · script `scratchpad/rank_c_holdout.py` ·
input snapshot `data/opportunities_through_2026-08-04.parquet` (the exact table the served model
was fitted on, preserved before the rebuild)
