# T9_RS_V1 — the 🔻💪+T9 chip re-analysed and run through the RS decomposition

**Date** 2026-09-29 · user: "es signali gamianalize tavidan da rs shic gaatare". Plan frozen to `t9rs_plan.txt` before any outcome.
**Status.** 🔻💪+T9 is forward-registered as B3, which must beat 🔻💪 alone at the ≥ 2027-03-01 read. This is a re-analysis of **seen** data; the forward rule is unchanged.
**Method.** Day × ATR% matched book return (22 bins), day-clustered. PASS rule as in RS_DECOMP_V1. Primary k = 5.

**Counts**
- T9 bars: 105,323.
- 🔻💪 on T9: 8.9 %.
- 🏆rs on T9: 25.0 %.
- T9∧🔻💪 per year: 2022 2,541 · 2023 1,949 · 2024 1,688 · 2025 2,016 · 2026 1,182.

## VERDICT: 0 / 5. The chip is mostly RS. **T9 adds nothing to 🔻💪; in 2024-26 it is slightly worse** than 🔻💪 on other T/Z states.

| test | MINE | VERIFY | years | $21-89 V | verdict |
|---|---|---|---|---|---|
| A0 T9∧🔻💪 vs other T9 (the chip) | +0.63 [−0.96, 2.22] | +0.98 [−0.37, 2.20] | 5/5 + (2022-26) | +0.14 | FAIL (CIs cross 0) |
| C1 🔻💪 vs 🏆rs-without-bottom, inside T9 | +0.93 | −0.21 | 3+ / 2− | −1.82 | FAIL: the bottom adds nothing over RS |
| C2 🏆rs vs no RS, inside T9 | +0.22 | **+1.69 [0.74, 2.71]** | 2022 −1.68, then 4/4 + | +1.85 | FAIL (MINE ≈ 0) |
| C3 🕐DR vs 🏆rs-without-🕐DR, inside T9 | +0.68 | +2.22 [−0.24, 4.81] | 3+ / 1− / 1 ≈ 0 | −0.48 | FAIL |
| **D T9 vs other T/Z, inside 🔻💪** | +0.64 | **−1.06 [−2.32, 0.12]** | 2022 +2.64, then **4/4 −** | **−2.09** | FAIL, **negative since 2023** |

**Descriptive: C4, 🔻 without RS inside T9.** +1.01 / +0.08, fading.

## Reading
1. **The chip's claimed advantage was mostly volatility plus RS.**
   - TZ_X_BOTTOM_V1 (not ATR-matched) had +1.65 pp VERIFY and "+0.7 above 🔻💪 alone".
   - Once ATR is matched, the chip vs other T9 is +0.98 (CI crosses 0).
   - The bottom structure over RS is ≈ 0.
2. **The B3 premise fails on seen data.** T9 was supposed to add to 🔻💪 (test D). It does the opposite since 2023: −1.06 VERIFY, 4/4 years negative, $21-89 −2.09.
   - Expectation for the 2027-03 forward read: B3 fails its anchor condition. The registration is kept, with no re-tuning.
3. **What remains true.** RS inside T9 is worth +1.7 pp from 2023 on and was negative in 2022, the same regime dependence as T1/T3/T6.

## Chip hint — needs a correction (user decision, not changed)
- **Current hint:** "+2.62 / +1.65 pp … +0.7 pp above 🔻💪 alone".
- **ATR-matched truth:** +0.63 / +0.98 vs other T9 (not significant), and T9 is −1.06 vs other 🔻💪 bars in 2024-26.

No build, no memory entry. Artifacts (scratchpad `v4hist/`): `t9rs_plan.txt`, `t9rs.py`, `t9rs.log`, `t9rs_result.json`.
