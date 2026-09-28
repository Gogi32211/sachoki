# HI_STATE_ALT_V1 — repetition / alternation of Z10, Z11, T10, T11, T12 in the bottom zone

**Date** 2026-09-28 · user correction: "not an exact sequence — the alternation or repetition of these bars" · plan frozen to `hialt_plan.txt` before any outcome · k = 2.
**Verdict:**
- **H1 repetition:** MINE PASS → **VERIFY FAIL**.
- **H2 alternation:** MINE FAIL.

The pattern worked in 2021-23, mainly in 2022, and did not carry into 2024-26.

## Setup
- **Set.** S = {Z10, Z11, T10, T11, T12}, counted within t-6..t.
- **Bottom zone.** As in T_CLUSTER_BOTTOM_V1.
- **Labels.** Strictly forward REV and CLEAN, plus same-day RET.
- **Adjustment.** Location × ATR% × bars-since-low.

## Results (adjusted REV lift [CI] · same-day Δ)

| | MINE 2021-23 | VERIFY 2024-26 |
|---|---|---|
| **H1** ≥ 3 S-bars in 7 | **1.22 [1.05, 1.42]** · 2021 1.02 / 2022 1.45 / 2023 1.09 · +0.23 | **0.97 [0.89, 1.06]** · −0.50 |
| **H2** ≥ 3 S-bars with ≥ 2 T↔Z switches (n 455 / 706) | 1.19 [0.88, 1.54] · −0.22 | 1.08 [0.89, 1.28] · −1.09 |
| ≥ 3 S-bars, exactly 1 switch (descriptive) | 1.40 [1.20, 1.59] · +1.08 | 1.06 [0.96, 1.16] · CLEAN 1.11 [1.03, 1.18] · −0.68 |
| ≥ 3 S-bars, no switch | 1.04 | 0.88 |
| ≥ 4 S-bars in 7 | 1.00 | 1.03 · same-day −3.02 |
| bottom zone overall | 1.00 · −0.30 | 1.00 · −0.65 |

**Frequency at true pivots (hindsight, descriptive).**
- H1 appears on bars within ±3 of a true 21-bar pivot at ×0.95 of its overall rate.
- H2 appears there at ×0.97.
- So neither is more common around real bottoms.

## Reading
- **Repetition of these states in the bottom zone was a genuine reversal tell in 2021-23:** lift 1.22, 1.45 in 2022, with one-switch clusters at 1.40.
- **It did not survive 2024-26** (0.97). The best slice decays to 1.06, and same-day returns are negative.
- **The pattern looks like a bear-market (2022) behaviour:** indecisive high-numbered T/Z states churning at a low before a real turn. It is not a general law.
- **Worth keeping as a forward watch item, not a signal.** If the market turns bearish again it may matter. It should not be used without a fresh confirmation.

Artifacts (session scratchpad `v4hist/`): `hialt_plan.txt`, `hialt.py`, `hialt.log`. No UI, score or memory change.
