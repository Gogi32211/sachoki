# WYC_AXIS_V1 — is the ⚛ MARKUP↑ / MKDN↓ axis general, and is it more than trend?

**Date** 2026-09-30 · user-approved ("ki"). Plan frozen to `wyc_plan.txt` before any outcome.
**Disclosure.** The axis was discovered on this history in 7 cells (T2 T6 Z1 T2G | Z2 Z1G Z2G). History can only test GENERALIZATION; clean confirmation needs forward data.
**Contrast.** Inside each T/Z state: bars in ⚛ MARKUP vs ⚛ MKDN. Other phases (ACC/DIST-TR, SPRING, UTAD, SOS) are excluded. Book per-bar long return, paired within strata, day-clustered.

## ⚠️ What the axis actually is (read from `backend/studio/bar_physics.py`, causal, no lookahead)
- `MARKUP = EMA50 > EMA200` (and not ATR-compressed, not SPRING / UTAD / SOS).
- `MKDN = EMA50 < EMA200` (same exclusions).
- **So ⚛ MARKUP / MKDN is the golden-cross / death-cross REGIME**, not a Wyckoff structure. The name overstates it.

## VERDICT: TEST 1 PASS · TEST 2 PASS (as pre-registered) → the golden-cross regime is a general T/Z modifier. Two thirds of it is ordinary trend location; about one third is specific to the EMA50 / EMA200 cross.

**TEST 1 — generalization to the 17 UNSELECTED states** (day × ATR% strata)
- **VERIFY Δ > 0 in 16 of 17 states.** The only negative is Z10 (−2.36, CI wide). Z12 (descriptive only) is also negative.
- **Pooled, strata × state:** MINE **+0.71 [0.39, 1.04]**, VERIFY **+1.69 [1.39, 2.00]**.
- **Years:** 2021 −0.17 · 2022 +0.02 · 2023 +1.88 · 2024 +1.85 · 2025 +2.31 · 2026 +0.15.

**TEST 2 — trend control** (strata add close > EMA50 × close > EMA200)
- **Pooled:** MINE **+0.80 [0.51, 1.12]**, VERIFY **+0.50 [0.10, 0.86]**.
- **$21-89 VERIFY:** +1.05 [0.56, 1.53].
- **Years:** 2021 −0.17 · 2022 +0.43 · 2023 +1.68 · 2024 +1.09 · 2025 +0.72 · **2026 −1.13**.
- **Overlap with location:** 56 % of MARKUP bars are above both EMAs; 52 % of MKDN bars are below both.

**Per-state VERIFY Δ (unselected)**

| state | Δ | state | Δ |
|---|---|---|---|
| T1 | +1.45 | Z3 | +2.00 |
| T1G | +0.70 | Z4 | +1.28 |
| T3 | +0.99 | Z5 | +0.81 |
| T4 | +1.25 | Z6 | +1.28 |
| T5 | +1.73 | Z7 | +1.47 |
| T9 | +1.70 | Z9 | +1.16 |
| T10 | +2.37 | Z10 | −2.36 |
| T11 | +1.24 | Z11 | +0.30 |
| T12 | +1.29 | | |

- **Selected (descriptive):** T2 +1.71 · T6 +2.23 · Z1 +2.04 · T2G +1.66 · Z2 +2.23 · Z1G +2.05 · Z2G +1.95.
- **$21-89 VERIFY:** positive in 16 of 17 unselected states (+0.8…+3.4).

## Reading
1. **A T/Z long in a golden-cross name beats the same T/Z in a death-cross name on the same day and at the same volatility.** It is worth about +1.7 pp in 2024-26 and holds in almost every state.
2. **Most of that is plain trend location** (price above / below the EMAs): +1.69 → +0.50 once location is fixed. The cross itself keeps a small, still-significant increment.
3. **It is regime-dependent.** It was ≈ 0 in 2021-22, strong in 2023-25, and the trend-controlled part is negative so far in 2026. This matches RS (RS_DECOMP_V1: negative in 2022). Both are "trend / strength" measures that pay in trending markets.
4. **Relation to known laws.** It is consistent with 🏆RS as a quality filter and with the ⛔ sub-200-rally suppressor. It does not contradict "pullback wins": a pullback inside a golden-cross name is exactly the favoured case.

## Not done / open
- **Overlap with 🏆RS is not measured.** RS is relative to SPY, the cross is absolute; both are trend-type. The next check is whether the cross adds beyond RS, or RS beyond the cross.
- **Forward confirmation is not registered.** It is the only clean test.

No build, no memory entry. Artifacts (scratchpad `v4hist/`): `wyc_plan.txt`, `wyc.py`, `wyc.log`, `wyc_result.json`.
