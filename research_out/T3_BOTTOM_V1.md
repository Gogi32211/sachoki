# T3_BOTTOM_V1 — does a bottom mark on the same bar improve T1 / T3? (T4 / T6 descriptive)

**Date** 2026-09-29 · user: "t1 da t3 ukve T3_BOTTOM_V1 amiti gaanalize". Plan frozen to `tbottom_plan.txt` before any outcome.
**Primary k = 6:** state {T1, T3} × mark {🕐DR, 🔻💪, 🔻}. Contrast = state∧mark vs state∧¬mark, paired within day × ATR% (22 bins), book per-bar return.
**PASS** requires:
- the same sign in MINE and VERIFY, with CI excluding 0 in both;
- ≥ 4/6 years the same sign;
- worst year ≥ −2 pp against;
- $21-89 the same sign.

**⚠️ Coverage.** 🔻💪 needs 120 bars of RS history and 🕐DR needs the 1h DB, which starts 2021-07. So 2021 is empty or a handful of fires.
- **MINE is effectively 2022-23 only**, which means less power there.
- The T1 + 🕐DR "2021 +64.18" is one trade (the known artefact noted in `edge_replay.py`). Read 2022-26.

## VERDICT: 1 / 6 PASS — **T3 + 🔻💪**. Three more are positive in every year 2022-26 but miss the MINE CI.

| state + mark | share | MINE Δ | VERIFY Δ | 2022-26 years | $21-89 V | REV lift V (with / without) | verdict |
|---|---|---|---|---|---|---|---|
| **T3 + 🔻💪** | 11.1 % | **+1.57 [0.24, 2.89]** | **+1.89 [0.36, 3.34]** | **5/5 +** | +2.99 | 1.18 / 1.02 | **PASS** |
| T3 + 🕐DR | 3.7 % | +1.35 [−0.52, 3.29] | **+3.34 [1.16, 5.45]** | 5/5 + | +3.08 | **1.29** / 1.03 | FAIL (MINE CI) |
| T1 + 🕐DR | 2.5 % | +1.07 [−0.91, 3.19] | **+3.43 [1.34, 5.52]** | 4/5 + (2026 −0.40) | +2.81 | **1.32** / 1.01 | FAIL (MINE CI) |
| T1 + 🔻💪 | 9.9 % | +1.07 [−0.32, 2.61] | **+2.49 [1.17, 3.77]** | 5/5 + | +2.64 | 1.21 / 1.00 | FAIL (MINE CI) |
| T1 + 🔻 (no RS) | 32.7 % | +0.32 | −0.48 | 2+ / 3− | −1.03 | 0.99 / 1.03 | FAIL |
| T3 + 🔻 (no RS) | 38.5 % | +0.94 | **−1.61** [−3.71, 0.08] | 2+ / 3− | −2.13 | 1.01 / 1.05 | FAIL, turns negative |

**Descriptive: the same marks inside T4 / T6**

| state + mark | MINE | VERIFY | 2022-26 years |
|---|---|---|---|
| **T6 + 🕐DR** | +3.68 | **+8.13** | 5/5 + (would pass) |
| T6 + 🔻💪 | +0.50 | +2.61 [1.17, 4.09] | 5/5 + |
| T6 + 🔻 | +1.64 | **−2.32** [−3.84, −0.86] | flips |
| T4 + 🕐DR | +0.65 | +1.87 | 3+ / 2− |
| T4 + 🔻💪 | +1.38 | +0.13 | ≈ 0 |
| T4 + 🔻 | +1.37 | +0.07 | ≈ 0 |

## Reading
1. **The RS half decides.**
   - 🔻💪 (bottom + RS) and 🕐DR (1H reclaim + RS + a daily edge) are positive in every year 2022-26 on T1, T3 and T6.
   - 🔻 without RS is ≈ 0 or turns negative in 2024-26 (T3 −1.61, T6 −2.32).
   - This is the book's 🏆RS law again: a bottom without relative strength is a knife.
2. **The marks also raise the forward reversal likelihood, but only in VERIFY.**
   - REV lift with 🕐DR is 1.29-1.32 on T1/T3; with 🔻💪 it is 1.18-1.21.
   - In MINE (2022-23) the lift is ≈ 1.
3. **T4 does not benefit.** Only T4 among the four states does not gain from any of the marks.
4. **Open decomposition (NOT tested).** Is the gain just 🏆RS, or does the bottom structure add to it? The next test would be T3 + 🔻💪 vs T3 + 🏆rs without 🔻.

No build, no memory entry. Artifacts (scratchpad `v4hist/`): `tbottom_plan.txt`, `tbottom.py`, `tbottom.log`, `tbottom_result.json`.
