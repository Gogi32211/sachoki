# VE_RELBD_V1 — ▼rel + BDV▼ on the same bar: does a reversal follow?

**Date** 2026-09-29 · user: "roca ori signali emtxveva ert barze xSirad amis mere reversalia da ramdenad martalia" · plan frozen to `velbd_plan.txt` before any outcome.
**Universe** all 5,739 US tickers, eligible bars (≥$5, ≥$5M), 2021-05…2026-09. MINE 2021-23 / VERIFY 2024-26.

## Definitional finding
**▼rel ∧ BD-any ≡ ▼rel ∧ BDV▼.** The two hypotheses were identical to the bar.
- ▼rel requires volume > SMA20, and BDV is exactly "BD with volume > SMA20".
- So any ▼rel on a breakdown bar is automatically BDV▼. The effective k is 1.

## Results (H1 = ▼rel ∧ BDV▼, n 5,453 / 6,548)
Expected rates are adjusted for location (close−low10)/ATR deciles × ATR% deciles × bars-since-10-bar-low.

| estimand | MINE | VERIFY | verdict |
|---|---|---|---|
| REV (low holds 10b + ≥3 ATR up in 20b) | 9.0% vs exp 9.1% → lift 0.99 [0.84, 1.13] | 11.8% vs 11.3% → 1.04 [0.92, 1.16] | NULL |
| CLEAN (+3 ATR before −1.5 ATR) | 1.02 [0.92, 1.12] | 1.09 [1.01, 1.16] | NULL (MINE fails) |
| PIVOT (lowest low of t±10, hindsight) | 8.7% vs 9.3% → 0.94 | 10.1% vs 10.7% → 0.94 | NULL |
| return, same-day Δ vs control (ATR×12 trail) | +0.63 [−0.39, +1.74] | −0.88 [−1.74, +0.01] | NULL; years 3+/3− |

**Comparators.** None of these separates from a location-matched bar either:

| comparator | lifts | return Δ |
|---|---|---|
| BDV▼ without ▼rel | 0.98-1.07 | −0.03 / +0.11 |
| BD-any without ▼rel | 0.97-1.02 | — |
| ▼rel without BD | 0.95-1.07 | +0.47 / +0.10 |

**Price buckets.** No bucket has CI lo > 0.

## Why it LOOKS like "often a reversal"
- **Raw pivot rate.** The pair marks the exact 21-bar pivot low on 8.7-10.1% of occurrences, vs 3.2% on an average bar, i.e. about 3× more often. But any bar breaking down to a fresh low on volume has the same rate (expected 9.3-10.7%). The pair adds nothing beyond "a breakdown on a new low".
- **Forward reversal rate.** REV follows only 9-12% of the time, *below* the 19.9% base rate. This is a breakdown bar; most keep falling.
- **Memorable cases.** The bars that did turn are the ones remembered on the chart.

**Verdict: NULL.** Nothing is built, and there is no memory VETO or BUILD. VOL_ECHO long studies were already NULL (LONG_V1 0/274); this agrees.
Artifacts (scratchpad `v4hist/`): `velbd_plan.txt`, `velbd.py`, `velbd.log`.
