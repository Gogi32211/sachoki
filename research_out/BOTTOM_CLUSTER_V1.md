# BOTTOM_CLUSTER_V1 — buying the ▽△ bottom cluster (decomposition + forward registration)

**Date** 2026-09-28 · user OK ("ki") · plan frozen to `botclu_plan.txt`.
**Disclosure.** The rule was already SEEN in both windows as a NATURE_SHIFT_V1 comparator: 2021-23 +1.13 [+0.90, +1.35], 2024-26 +0.30 [+0.07, +0.55]. Nothing on 2021-05…2026-09-25 can confirm it.
- **Part A** below is descriptive, with no verdict.
- **Part B** is the only clean test, and it runs forward.

## Rule (frozen verbatim)
- **Bottom mark on t:** 🔻💪, 🔻 or 🌀.
- **At least one other bottom-mark bar in t-4..t-1.**
- **Filters:** close ≥ $5, $vol ≥ $5M.
- **Entry and exit:** entry open[t+1], book ATR×12 trail, maxh 60.
- **Universe:** all 5,739 US tickers in the DB.

## Part A — decomposition on seen data (same-day Δ vs control, pp, day-clustered)

| slice | 2021-23 | 2024-26 | reading |
|---|---|---|---|
| CLUSTER (the rule) | +1.13 [+0.90, +1.36] | +0.30 [+0.06, +0.54] | positive, shrinking |
| SINGLE bottom (no other in t-4..t-1) | +0.72 [+0.22, +1.21] | +0.69 [+0.10, +1.30] | **clustering does not add consistently**: better in 2021-23, worse in 2024-26 |
| **cluster · 🔻💪 on t** | **+1.62 [+1.07, +2.14]** | **+0.98 [+0.53, +1.43]** | **carries the effect in both windows** |
| cluster · 🔻 on t | +0.86 | +0.29 [−0.01, +0.60] | fades |
| cluster · 🌀 on t | +1.18 | +0.13 [−0.22, +0.45] | fades to 0 |
| cluster with an EDGE fire | +1.42 | +0.14 | unstable |
| cluster, no EDGE fire | +0.29 | +0.50 | unstable (reversed) |
| cluster · RSI ≤ 35 | +0.82 | **−0.80 [−1.30, −0.27]** | **the knife law again**: oversold clusters turn negative |
| cluster · RSI 35-70 | +1.13 [+0.91, +1.36] | +0.36 [+0.13, +0.59] | the stable part |
| cluster · RSI ≥ 70 | n 206 | n 614 | too few |

**Reading.** What survives both windows is the **🔻💪 precise bottom (reversal + relative strength)** inside a cluster, in the RSI mid band.
- This agrees with two standing book laws:
  - 🏆RS is the universal rescuer;
  - RSI < 35 is a knife.
- The other bottom marks (🔻, 🌀) and the clustering itself fade out of sample.

## Part B — forward confirmation (registered, not yet read)
- **Evaluator.** `backend/bottom_cluster_forward.py`, which reads `anatomy_signals.parquet` directly with the same predicates as the catalog keys.
  - Seen-window check on 2024: B1 +0.47 (research +0.48) and B2 +1.50 (research +1.73). The B2 gap comes from the ATR warm-up of a shorter load window.
- **Rules.**
  - **B1** = the frozen CLUSTER rule.
  - **B2** = CLUSTER with 🔻💪 on t. This is **AMENDMENT_1**, declared now, before any forward data, from the Part A decomposition, so the forward k = 2.
- **Entries.** date_in ≥ **2026-09-29**. Only trades with a full 60-session horizon inside the data are scored.
- **Read ONCE on or after 2027-03-01.** The script refuses earlier.
  - PASS per rule requires a same-day Δ > 0 with day-clustered CI lo > 0.
  - No re-tuning in between.

Artifacts (session scratchpad `v4hist/`): `botclu_plan.txt`, `botclu.py`, `botclu.log`, `botclu_result.json`. Repo: `backend/bottom_cluster_forward.py`. No UI or score change.
