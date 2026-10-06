# TZQ_CENTRE_V1: does the Q body-centre split matter inside the same T/Z state? (2026-10-03)

## Set-up

- **Plan:** approved ("ki"). Script `backend/research_tzq_centre.py`, raw output `TZQ_CENTRE_V1.json`.
- **Contrasts:** k = 9, fixed in advance; the "rest" group is the other centre.

  | states | group g | vs |
  |---|---|---|
  | T4, T6, Z4, Z6 | Q3 (engulf, centre ↑) | Q6 |
  | T9, T10, Z9, Z10 | Q4 (inside, centre ↑) | Q5 |
  | Z11 | Q1R (fully above) | Q2R |

- **Return:** book per-bar return (entry open[t+1], trail clip(12·ATR%, 15, 60), maxh 60, 15 bps).
  - Vectorised here.
  - Checked against `bottom_cluster_forward._bar_returns` on a random sample of 3,000 bars: identical.
- **10-bar win:** `close[t+10] / open[t+1] − 1 − 30 bps > 0`.
- **Pairing:** g vs rest within day × ATR%-bin (frozen cut points), n-weighted per day, day bootstrap.
- **Universe:** close ≥ $5, 20-day dollar volume ≥ $5M.

## Result: 9 / 9 NULL

Δ = centre ↑ (Z11: fully above) minus the other variant, in pp, with 95 % CI.

| state | n g / rest | return 2021-23 | return 2024-26 | $21-89 | win 2021-23 | win 2024-26 |
|---|---|---|---|---|---|---|
| T4 | 83k / 33k | −0.02 [−1.09, +1.05] | +0.67 [−0.34, +1.67] | −0.28 | +0.49 | −1.16 |
| T6 | 39k / 14k | −0.64 [−1.83, +0.54] | −0.64 [−1.89, +0.62] | −0.91 | +2.32 | +1.93 |
| Z4 | 37k / 86k | −0.76 [−1.92, +0.36] | +0.38 [−0.38, +1.16] | −0.11 | +0.22 | +1.18 |
| Z6 | 18k / 47k | +1.11 [−0.34, +2.66] | −0.47 [−2.33, +1.02] | −0.27 | +1.02 | −1.55 |
| T9 | 36k / 91k | −0.18 [−1.36, +1.02] | +0.43 [−0.41, +1.26] | −0.29 | −0.99 | −1.06 |
| T10 | 41k / 14k | −0.13 [−1.69, +1.42] | −0.19 [−1.58, +1.25] | +0.33 | +0.17 | +1.62 |
| Z9 | 85k / 31k | −0.94 [−2.13, +0.16] | +0.55 [−0.95, +2.35] | −0.80 | +1.34 | +1.80 |
| Z10 | 15k / 44k | +0.56 [−0.70, +1.84] | +2.75 [−0.34, +7.48] | −0.17 | +1.44 | +0.89 |
| Z11 | 29k / 20k | −0.77 [−2.19, +0.71] | +0.59 [−1.25, +2.28] | +0.24 | **−4.47 [−7.75, −1.33]** | +1.67 |

- **Return:** no CI excludes 0 in either window.
- **Win:** only one cell has a CI that excludes 0, Z11 win 2021-23 (Q1R worse). It reverses sign in 2024-26 (+1.67), so it does not replicate. One significant cell out of 36 (9 states × 2 windows × return/win) is what chance alone would produce.
- **Per year:** no state keeps the same sign for at least 4/6 years with the worst year ≥ −2.

## Conclusion

- Inside a T/Z state, whether the body centre sits above or below the previous bar's centre does not change the outcome. Together with TZ_Q_MAP_V1, the Q code adds no information beyond T/Z.
- This is consistent with the T/Z laws: the state itself plus context (RS, golden cross, RSI ≤ 35, 🕐DR) carries everything, and the body geometry is already inside the T/Z code.
- **Nothing to build.** The Q tab stays a viewing tool. Its potentially useful lines are the V token, TRUE-gap, R·C·H and Wyckoff, and none of these was tested here.

## Sensitivity: maxh 21 instead of 60 (user request, run after seeing the 60-bar result)

This run brings the total to k = 18 (9 × 2 horizons). Raw output is in `TZQ_CENTRE_V1_h21.json` (`python research_tzq_centre.py 21`). Everything else is identical.

| state | return 2021-23 | return 2024-26 | $21-89 | years |
|---|---|---|---|---|
| T4 | −0.00 [−0.58, +0.57] | **+0.89 [+0.20, +1.62]** | +0.06 | −1.1 +0.4 +0.2 +1.5 +0.8 +0.2 |
| T6 | +0.23 [−0.54, +1.01] | +0.14 [−0.60, +0.83] | −0.41 | |
| Z4 | −0.35 [−1.24, +0.41] | +0.17 [−0.34, +0.68] | −0.05 | |
| Z6 | +0.47 [−0.27, +1.23] | −0.12 [−0.86, +0.56] | −0.14 | |
| T9 | −0.14 [−0.68, +0.39] | +0.07 [−0.42, +0.59] | −0.01 | |
| T10 | −0.07 [−0.94, +0.83] | −0.47 [−1.30, +0.36] | −0.30 | |
| Z9 | **−0.90 [−1.93, −0.00]** | +0.54 [−0.24, +1.52] | −0.33 | sign flips |
| Z10 | +0.72 [−0.01, +1.55] | +0.30 [−0.46, +1.11] | +0.01 | +0.1 +1.4 +0.6 −0.0 +0.8 −0.0 |
| Z11 | −0.51 [−1.36, +0.35] | +0.57 [−0.29, +1.50] | −0.37 | |

- **9 / 9 NULL again.**
- T4 is significant only in 2024-26; in 2021-23 it is exactly 0.
- Z9 is significant only in 2021-23 and flips sign in 2024-26.
- Z10 comes closest (Q4R, centre ↑ inside a Z10, positive in both windows), but neither CI excludes 0 and $21-89 is ≈ 0. With k = 18 it is not a candidate.
- Shortening the horizon only narrows the CIs; it does not create an effect. The conclusion stands.

## Sensitivity: maxh 13 (user request), total k = 27

Raw output: `TZQ_CENTRE_V1_h13.json`.

| state | return 2021-23 | return 2024-26 | $21-89 | years |
|---|---|---|---|---|
| T4 | +0.03 [−0.44, +0.53] | +0.46 [−0.10, +1.02] | −0.00 | −1.0 +0.6 +0.1 +1.1 +0.5 −0.5 |
| T6 | +0.28 [−0.34, +0.93] | +0.11 [−0.47, +0.67] | −0.24 | −0.6 +0.7 +0.4 −0.1 +0.0 +0.5 |
| Z4 | +0.01 [−0.44, +0.47] | +0.07 [−0.34, +0.46] | +0.08 | −0.4 +0.4 −0.2 −0.1 +0.4 −0.2 |
| Z6 | +0.15 [−0.60, +0.82] | −0.08 [−0.65, +0.47] | +0.03 | +1.5 +0.1 −0.6 −0.1 −0.1 +0.0 |
| T9 | −0.08 [−0.50, +0.34] | +0.31 [−0.11, +0.77] | +0.01 | −0.7 +0.2 +0.0 −0.0 +0.6 +0.4 |
| T10 | +0.13 [−0.74, +0.96] | −0.16 [−0.83, +0.53] | +0.18 | +1.3 −0.4 −0.0 +0.9 −0.5 −1.0 |
| Z9 | −0.61 [−1.35, +0.02] | +0.16 [−0.45, +0.75] | −0.26 | +0.1 −0.6 −1.1 +0.1 +0.9 −0.7 |
| Z10 | +0.12 [−0.49, +0.73] | −0.25 [−0.87, +0.41] | −0.10 | −0.5 +0.5 +0.2 −0.6 +0.1 −0.2 |
| Z11 | **−0.80 [−1.42, −0.17]** | +0.49 [−0.19, +1.20] | −0.08 | −1.4 −1.0 −0.3 +1.6 −0.1 −0.1 |

- **9 / 9 NULL.** The only cell with a CI that excludes 0 is Z11 in 2021-23, and it flips sign in 2024-26.
- Z10, the "closest" state at 21 bars, falls to ≈ 0 here (+0.12 / −0.25). The near-miss was noise, not a horizon-specific effect.
- Across three horizons (60 / 21 / 13), k = 27, the centre split has zero replicated cells.
