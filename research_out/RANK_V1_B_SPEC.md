# RANK_V1 · B — frozen head-to-head OOS spec · SEALED 2026-09-13

**Nothing here may be changed after the first outcome is read.** Written before any B data exists:
the evaluation cohort is 0.30 % resolved today.

## ⚠️ What B is, and what it is not

If B is used to pick between the two artifacts and the winner is then deployed, **B is a
model-selection holdout, not a final independent validation.** That is acceptable — but the status
must be named correctly. A production-edge claim for whichever table wins needs a *further*,
untouched out-of-sample window after the selection.

B is also not a rescue: [[project-rank-v1-validation]] already put the method at weak-but-consistent
(10/13 folds, +1.95 pp, sign-test p = 0.092). B decides *which table*, not *whether ranking works*.

## Candidates — frozen now, neither may be refitted or edited

| | OLD (deployed) | CLEAN (challenger) |
|---|---|---|
| file | `data/rank_v1_model.json` | `data/rank_v1_model_CLEAN.json` |
| sha256 | `332a0dd77b127895a3640981513b78cc…` | `4c248a0bc2cd631bdf50eb3e462cd3d6…` |
| bytes | 138,235 | 136,626 |
| fitted rows | 1,138,881 | 1,087,180 |
| through | 2026-08-04 | 2026-06-15 |
| labels | **includes the censored June–July cohort** | fully-resolved `mtm_60` only |
| cells / p1 / fam | 2,086 / 233 / 72 | 2,057 / 233 / 73 |
| global `g` | 1.2686 | 1.2848 |

OLD stays in production throughout. CLEAN is a frozen challenger on disk and is not wired in.

## Evaluation cohort

* `sig_date > 2026-08-04` only — everything neither table could have seen as a *resolved* label.
* Resolved rows only, decided by the **resolution contract, never by a calendar date**:
  `mtm_60` is usable iff `mtm_exit_bar <= 60` **or** `bars_priced >= 60` (`072ff0b`).
* **OLD and CLEAN are scored on exactly the same ticker-day universe.** Any row unusable for one
  is dropped for both.
* Today: 23,609 fires exist in the window, **70 resolved (0.30 %)**. First fire 2026-08-05 matures
  ≈ **2026-10-29**; half the cohort ≈ **2026-11-18**. Run B when the cohort is materially resolved,
  not on the first day it is technically possible.

## Production semantics — unchanged

One row per `(ticker, sig_date)`; the ticker's prediction is the **max over its fires**, as
`rank_v1.rank_map` keeps the best family. Same-day cross-sectional percentile. Ties, missing values
and the `cell → p1 → fam → global` fallback exactly as production resolves them. Days with ≥ 20
candidates. **No liquidity, regime or family filtering anywhere in the primary comparison.**

## Primary — each artifact separately

Q5−Q1 · quintile monotonicity · per-day Spearman · top-decile vs pool · top-1 / top-3 / top-5 vs
the same-day pool · bootstrap CI **by date** · liquidity strata · fallback depth · ticker
concentration.

## Primary — CLEAN − OLD, paired

Both score the same days and the same candidates, so the paired difference is far more informative
than two independent CIs:

* daily Spearman difference
* top-decile realized-edge difference
* top-k realized-edge difference
* share of days where CLEAN > OLD
* **paired bootstrap by date**

## Decision rule — frozen, no single-metric winners

CLEAN becomes a production-replacement candidate **only if all of these hold**:

1. overall ordering is no worse than OLD;
2. the paired top-decile / top-k comparison favours CLEAN;
3. the result does not collapse under liquidity strata;
4. one or two outlier days do not drive the difference;
5. realized outcome actually improves — ranking turnover alone is not an argument.

**Tie / indeterminate → OLD stays in production.** CLEAN keeps its correctness advantage and B's
OOS simply continues to accumulate. A cleaner fit is not by itself a reason to deploy.

**OLD clearly better → a real finding**, not an embarrassment: removing the contamination fixed
correctness but cost predictive ordering. CLEAN would then *not* be deployed merely for being
cleaner.

## Explicitly not done

No artifact is refitted. CLEAN is not swapped in. No threshold is changed on B's outcome. **A's six
sessions play no part in this decision** — `RANK_V1_A_SHORT_OOS.md` was flat on a sample at the
floor of interpretability, and it is not evidence here.
