# L_PATH_V1 — the intraday path inside L34 / L46 bars

**Date** 2026-09-24/25 · plan approved by the user before running ("gaushvi") · registered k = 8.
**Verdict: NULL — 0 of 8 cells pass MINE.** The answer is the same under the registered estimand and under AMENDMENT_1.

## Registry (k = 8)
There are four L × colour groups: L34 green, L34 red, L46 green, L46 red. Each group gets two cells:
- **conflict** — the path contradicts the colour (green with high first, red with low first).
- **close-hour extreme** — the close-side extreme is set in the last hour (green: high at or after 15:00; red: low at or after 15:00).

## Setup
- **Signal.** Studio 15m, S&P 500, full 26-bar sessions. The session OHLC built from 15m matches the 1D store on 99.4 % of sessions, and colour agrees on 99.0 %.
- **Outcome.** 1D store, entry open[D+1], exit close[D+20].
- **Windows.** MINE 2021-2023 · VERIFY 2024-2026.
- **Rows.** 246,691 L34/L46 bars.

| group | MINE | VERIFY |
|---|---|---|
| L34 green | 23,969 | 26,109 |
| L34 red | 7,599 | 9,015 |
| L46 green | 12,787 | 12,964 |
| L46 red | 75,738 | 78,510 |

## Registered estimand (excess vs same-day median of the same L×colour group)
The deciding test gave all four conflict-cell medians = **+0.00** and stopped.

Audit: groups are dense (median 18-157 bars per day), but 0.4-3.9 % of rows are exactly 0, because the bar is its own group's median. An atom of that size can pin a cell median to 0 and hide effects up to about ±0.3 pp, the same size as the 0.25 stop threshold. The stop was therefore **not trustworthy**, and the estimand was amended rather than accepting the NULL.

## AMENDMENT_1 (declared after the audit; same 8 cells, same gates, no new cells)
- Excess vs the same-day median of **all same-colour S&P bars**. This is the dense control from PATH_ORDER_V1, with no self-reference; only 0.18 % of rows are exactly 0.
- Each cell is compared with the **rest of its own L × colour group**. The difference Δ carries a day-clustered bootstrap CI.

MINE 2021-2023, Δ in median 20-day excess (pp):

| cell | n | Δ | CI | 2021 / 2022 / 2023 | gate |
|---|---|---|---|---|---|
| L34 green conflict | 3,115 | −0.09 | [−0.39, +0.20] | +0.40 / −0.17 / −0.26 | fail |
| L34 green close-hour high | 8,726 | −0.00 | [−0.20, +0.20] | −0.11 / +0.09 / −0.02 | fail |
| **L34 red conflict** | 1,671 | **+0.37** | [−0.08, +0.71] | −0.48 / +0.55 / +0.55 | fail: CI ∋ 0, |Δ| < 0.5, 2/3 yrs |
| L34 red close-hour low | 1,699 | −0.02 | [−0.38, +0.35] | −0.26 / −0.23 / +0.21 | fail |
| **L46 green conflict** | 2,999 | **−0.31** | [−0.74, −0.00] | −0.31 / −0.28 / −0.35 | fail: |Δ| < 0.5 |
| L46 green close-hour high | 3,264 | +0.14 | [−0.17, +0.46] | +0.23 / +0.05 / +0.22 | fail |
| L46 red conflict | 5,750 | −0.01 | [−0.22, +0.19] | +0.33 / +0.01 / −0.17 | fail |
| L46 red close-hour low | 31,142 | +0.03 | [−0.06, +0.17] | +0.25 / +0.02 / 0.00 | fail |

The two cells that crossed the 0.25 continue-threshold both fail the 0.5 pp gate. **MINE survivors: 0/8.**

## VERIFY (disclosed deviation)
The amendment script printed VERIFY for all 8 cells, not only survivors. No cell had passed MINE, so nothing was selected on VERIFY. The numbers only confirm the NULL:
- **L46 green conflict** went from −0.31 to −0.07, CI [−0.58, +0.21].
- **L34 red conflict** went from +0.37 to −0.12, a sign flip.
- **Every VERIFY CI includes 0.**

## Reading
- Inside L34/L46, the path is mostly the L-line itself: L34 lows sit in the first hour 65 % of the time, and L46 highs sit in the first hour 77 % of the time.
- The minority paths that contradict the colour, and a close at the extreme, add nothing that survives.
- **L46 green with high first** (rise, fall to the low, recover to close green) leaned consistently negative in 2021-23 (all three years about −0.3). It halved to −0.07 in 2024-26. That is a hint too small for the registered gate. It is **not** a veto and not a candidate. Retesting it would be a new family.
- This is consistent with the book: L34 is a 1D-native state that does not echo intraday, and L46 dissonance variants failed their gates. The intraday path is the eighth intraday-shape family to come back NULL.

Artifacts: session scratchpad `path/lpath.py`, `lpath.log`, `audit.py`, `amend1.py`, `amend1.log`, `amend1_result.json`. No UI, score, or memory change.
