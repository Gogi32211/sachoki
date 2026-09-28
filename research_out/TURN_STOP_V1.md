# TURN_STOP_V1 — does a wider stop rescue TURN·58?

**Date** 2026-09-27 · plan approved by the user ("kargi") · frozen to `turnstop_plan.txt` before any outcome · k = 4.
**Verdict: NULL. The TURN·58 family is closed.** No stop in the grid passes, and all four are negative against ordinary lows with the same stop. VERIFY was not opened.

## Setup
- **Cell.** TURN·58 ≥ 20 on 10-bar lows (S&P 500), MINE 2021-23.
- **Exit.** Identical to TURN_EXIT_V1 except for the stop: target +3 ATR, 20 bars.
- **Stop grid.** Declared in advance as low − {1.0, 1.5, 2.0, 3.0} ATR.
- **Control.** All 10-bar lows with the same stop on the same entry day.

| stop | control mean · win | TURN ≥ 20 mean · win | Δ same-day (pp) | 2021 / 2022 / 2023 |
|---|---|---|---|---|
| low − 1.0 ATR | −0.32 % · 34 % | −0.01 % · 42 % | −0.36 [−0.69, +0.00] | −0.49 / −0.19 / −0.46 |
| low − 1.5 ATR | −0.30 % · 41 % | −0.02 % · 47 % | −0.35 [−0.72, −0.01] | −0.56 / −0.15 / −0.46 |
| low − 2.0 ATR | −0.23 % · 46 % | −0.02 % · 49 % | −0.49 [−0.86, −0.12] | −0.88 / −0.31 / −0.50 |
| low − 3.0 ATR | −0.12 % · 50 % | +0.10 % · 52 % | −0.50 [−0.89, −0.10] | −1.05 / −0.11 / −0.59 |

The earlier 0.5 ATR stop (TURN_EXIT_V1) gave −0.34.

## Reading
- **A wider stop raises the win rate for everyone**, because more lows survive to +3 ATR. It does not change the ranking. High-count lows lag ordinary lows *on the same day* at every stop, by about 0.35-0.5 pp.
- **The pooled mean looks better while the same-day comparison is worse.** Pooled, TURN ≥ 20 averages higher than all lows (e.g. +0.10 % vs −0.12 %). That pattern is consistent with TURN·58 flagging *market-wide* turning days, when many names bounce at once, rather than the better name on a given day.
  - This reading comes from the table and was not tested.
- **Family closed.** TURN_SET_V1 (identification: replicated), TURN_EXIT_V1 (turn bracket: NULL) and TURN_STOP_V1 (stop grid: NULL) together say the same thing. TURN·58 describes how likely a low is to turn; it does not pick a better trade. The Superchart row stays descriptive; nothing else is built on it.

Artifacts (session scratchpad `v4hist/`): `turnstop_plan.txt`, `turnstop.py`, `turnstop.log`. No UI, score or memory change.
