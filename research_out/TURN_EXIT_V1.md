# TURN_EXIT_V1 — does TURN·58 pay with a turn-shaped exit?

**Date** 2026-09-27 · plan approved by the user ("gog gamotove da gaagrzele kvleva") · frozen to `turnexit_plan.txt` before any outcome · k = 3.
**Verdict: NULL — stopped at the deciding test.** With the exit the turn label itself describes (+3 ATR target, stop under the low), high-count lows win *more often* but earn *less* than other 10-bar lows. VERIFY was not opened.

## Setup
- **Candidates.** 10-bar lows in S&P 500, MINE 2021-23.
- **TURN·58.** The count of the frozen 58 keys in t-2..t (TURN_SET_V1).
- **Exit.**
  - Entry open[t+1] + 0.15 %.
  - Target = entry + 3·ATR14.
  - Stop = low[t] − 0.5·ATR14. A gap through fills at the open; a bar that touches both is scored stop first.
  - Time exit at close[t+20]; 0.15 % exit slip; 5-bar cooldown.
- **Control.** All candidates on the same entry day with the same exit.

## MINE result

| cell | trades | mean · win | Δ vs all 10-bar lows (pp) | by year |
|---|---|---|---|---|
| control, all lows | 26,802 | −0.30 % · 25 % | — | |
| TURN·58 ≥ 10 | 19,228 | −0.22 % · 29 % | −0.01 [−0.12, +0.10] | +0.03 / −0.11 / +0.08 |
| **TURN·58 ≥ 20** | 6,243 | −0.08 % · 34 % | **−0.34 [−0.63, −0.05]** | −0.43 / −0.33 / −0.30 |
| TURN·58 ≥ 26 | 1,963 | −0.25 % · 36 % | −0.33 [−0.79, +0.16] | +0.75 / −0.58 / −0.57 |

The deciding test needed ≥ 20 to reach +0.5 pp. It is at −0.34, negative in all three years.

## Reading
- **The count does what TURN_SET_V1 found.** The win rate climbs with it (25 % → 29 % → 34 % → 36 %), because more of these lows really turn and reach +3 ATR.
- **It still earns less than an ordinary low.** The losers cost more. High-count lows come with larger, more active bars, so the entry sits further above a stop placed under the low.
  - That is a hypothesis from this table, not a tested one.
- **Two exits have now been tried on the same count.** A trend trail (TURN_SET_V1) gave ≈ 0. A turn-shaped bracket (this study) gave ≤ 0. **TURN·58 describes turn likelihood; it does not select better trades.** The Superchart row stays as a descriptive gauge only.
- **Search accounting.** This family has used TURN_SET_V1 (4 VERIFY cells + a plateau look) and TURN_EXIT_V1 (3 MINE cells). Any further TURN·58 exit or stop variant would be a forked path on the same labels and is not recommended.

Artifacts (session scratchpad `v4hist/`): `turnexit_plan.txt`, `turnexit.py`, `turnexit_mine.log`, `turnexit_mine.json`. No UI, score or memory change.
