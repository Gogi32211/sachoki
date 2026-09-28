# TZ_REVERSAL_V1 — T10/T11/Z10/Z11 (and repeats) as reversal warnings; Z4/Z6 after the turn

**Date** 2026-09-28 · user's observation from the ▽△ bottom zone · plan frozen to `tzrev_plan.txt` before any outcome · k = 3.
**Verdict: 3/3 FAIL in MINE.**
- Every code is ≈ 1.0 on a strictly forward reversal label.
- In 2024-26 most of them are **worse** entries than the day's other bars.

## Setup
- **Context: the bottom zone.** t is within 0-10 bars after a 10-bar low. All 5,739 DB tickers.
- **Labels, strictly forward from t** (the lesson of TZL_BOTTOM_SEQ_V1 — no window reaching back over t):
  - **REV:** the recent bottom holds for 10 bars AND close rises ≥ 3 ATR within 20 bars.
  - **CLEAN:** +3 ATR before −1.5 ATR.
  - **RET:** ATR×12 trail, same-day Δ vs control.
- **Adjustment.** Location × ATR% × bars-since-the-low bucket (0 / 1-3 / 4-10).

## Results (adjusted REV lift [CI] · same-day return Δ pp)

| | MINE 2021-23 | VERIFY 2024-26 |
|---|---|---|
| H1 any of T10/T11/Z10/Z11 | 1.03 [0.94, 1.13] · −0.01 | 0.97 [0.91, 1.02] · **−0.59** |
| H2 ≥ 2 of them in 5 bars (repeat) | 1.12 [1.02, 1.23] (2021 < 1) · +0.18 | 0.98 [0.92, 1.03] · −0.48 |
| H3 Z4/Z6 after the turn | 1.03 [0.91, 1.15] · −0.23 | 0.99 [0.92, 1.06] · **−1.17** |
| T10 | 1.04 · −1.03 | 0.95 · **−1.37** |
| T11 | 1.09 · +0.26 | 1.01 · −0.78 |
| Z10 | 1.02 · **−0.99** | 0.95 · **−1.47** |
| Z11 | 0.99 · **−1.20** | 0.99 · −1.20 |
| Z4 | 1.01 · +0.27 | 1.00 · **−1.29** |
| Z6 | 1.02 · +0.23 | 1.00 · **−1.50** |

**Power check (positive controls, same framework).** Known trade edges read only 1.0-1.17 on this strict forward label, so the framework is conservative:

| edge | MINE | VERIFY |
|---|---|---|
| 🕐DR | 1.05 | **1.17** [1.04, 1.29] |
| 🔻💪 | 1.01 | 1.09 [1.02, 1.15] |
| ATM | 1.12 | 1.11 |

The TZ codes still sit at or below the level of an ordinary context bar. Their same-day returns, which need no label, are negative.

## Reading
- **These states describe the bottom zone; they do not predict the reversal.**
  - T10/T11/Z10/Z11 and their repetitions appear inside the zone about as often at bottoms that hold as at bottoms that break.
  - Z4/Z6 after the low do not mark the start of the rise.
- **As entries they are worse than ordinary bars in 2024-26** (Z10, Z11, Z4, Z6, T10 −1.0…−1.5 pp).
  - This agrees with the book's older note "Z11 = abort" (Z11L12).
  - The Z-states inside a bottom zone are the market still pressing.
- **H2's weak MINE signal** (repetition, 1.12) is below the pre-registered bar and vanishes in VERIFY.

Artifacts (session scratchpad `v4hist/`): `tzrev_plan.txt`, `tzrev.py`, `tzrev.log`, `tzrev_posctrl.py`, `tzrev_posctrl.log`. No UI, score or memory change.
