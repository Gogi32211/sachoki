# VOL_ECHO_LONG_2326 — the long study restricted to 2023-2026

**Date** 2026-09-25 · plan approved by the user before running ("kai gaakete") · new family; the DSR family counts the prior 274 cells, 561 in all.
**Verdict: NULL — 0 of 286 cells pass MINE.** Restricting the window to 2023-2026 does not change the answer from VOL_ECHO_LONG_V1.

## Setup
The method is the same as VOL_ECHO_LONG_V1 except for the window and the thresholds:
- **Window.** 2023-01-03 → 2026-08-26, ≥ $8, 2,802,414 bars, 5,694 tickers.
- **Split.** MINE 2023-2024 · VERIFY 2025-2026. VERIFY had never been opened in any earlier cell, so it remains a clean OOS window.
- **Registry.** Cells need MINE n ≥ 200, ≥ 80 distinct days, and VERIFY n ≥ 200. The plan said k = 287. The frozen registry has **286** because right-edge rows without a 20-day outcome were dropped before freezing. It was frozen before outcomes, to `long_2326_registry.json`.

## Deciding test (TZ·L split vs label-shuffle placebo, MINE, 200×)
The split was real in 3 of 8 signals, each only slightly above the placebo p95:

| signal | observed spread | placebo p95 |
|---|---|---|
| VE | 0.454 | 0.400 |
| Q | 0.490 | 0.433 |
| BO▲ | 0.511 | 0.434 |

Combo cells continued.

## MINE 2023-2024
**Signal-level median 20-day excess, pp:**

| VE | Q | ▲ Rel | ▼ Rel | BO▲ | BD▼ | BOV▲ | BDV▼ |
|---|---|---|---|---|---|---|---|
| −0.14 | −0.35 | −0.05 | 0.00 | +0.16 | −0.07 | +0.21 | +0.05 |

Only **1 of 286** cells reached +1.0 pp: **BD▼ × Z5·L12**.
- n 480, 225 days. Median **+1.02** [+0.19, +1.89]. 2023 +1.92, 2024 +0.39.
- It beats the rest of BD▼ by +1.11 [+0.34, +1.76].
- **DSR 0.003 → fail.**
- This is the same cell that topped V1. Both studies contain 2023, so the two results are **not** independent evidence. The cell's 2024 figure (+0.39) is well below the bar.

**MINE survivors: 0.** VERIFY was not opened and path-sim was not run.

## Descriptive (not gates; VERIFY shown here only at signal level)
- **BDV▼ drifts positive:** MINE +0.05, VERIFY +0.34, $21-89 VERIFY +0.46. This is the reversal lean the user expected, still under the bar.
- **▼ Release under $8** is +0.57 / +0.86. That is the lottery zone, excluded by design.
- **BOV▲ under $8** is −0.22 / −0.47. Volume breakouts in cheap names fade, the same as in V1.

## Reading
- **Two windows, two splits, one answer.** VOL_ECHO signals and their TZ·L flavours sit at the market median over the next 20 days.
- **The one recurring cell (BD▼ × Z5·L12) is the most likely thing to test next** if the user wants to continue. It would need its own pre-registered single-cell family. Its clean data would be 2025-2026 only, which has never been opened for it.

Artifacts: session scratchpad `vecho/long_2326.py`, `long_2326.log`, `long_2326_registry.json`, `long_2326_result.json`. No UI, score, or memory change.
