# Z2G_BOTTOM_V1 — is a 10-bar low that prints Z2G a better bottom?

**Date** 2026-09-28 · user: "ki z2g shevamowmot" · plan frozen to `z2g_plan.txt` before any outcome · k = 1.
**Verdict: FAIL.** Z2G is simply the most common state of a low bar: 31 % of all 10-bar lows. That is why it dominates true pivots (×1.96). It does not make the low hold more often, and in 2024-26 it is a worse entry.

## Results (10-bar-low bars; REV = the bottom holds 10 bars and +3 ATR within 20; adjusted for location × ATR%)

| | MINE: REV lift · mean · same-day Δ | VERIFY: REV lift · mean · same-day Δ |
|---|---|---|
| **Z2G low** | 1.04 [0.87, 1.23] · −0.30 % · −0.28 | 0.98 [0.87, 1.08] · +2.80 % · **−1.77** |
| Z2G + L46 | 1.05 · +0.07 % · −0.25 | 0.98 · +2.97 % · −1.59 |
| Z2G + L5 | 1.03 · −0.43 % · +0.17 | 0.98 · +2.30 % · **−2.72** |
| Z2G low, enter on the next-bar T | mean −0.13 % · −0.28 | mean +2.93 % · −1.47 |
| **Z2G + 🔻💪 on the same bar** | 1.05 · **+2.86 %** · **+2.60 [+1.01, +4.05]** | 0.99 · **+4.96 %** · **+2.15 [+0.73, +3.62]** |
| other 10-bar lows | 0.99 · −1.03 % · −0.14 | 1.01 · +2.48 % · −0.96 |
| Z2G − other lows, pooled mean return | +0.73 [−0.25, +1.70] | +0.31 [−0.78, +1.59] |

## Reading
- **Z2G describes the low bar; it does not select good lows.** Its REV lift is ≈ 1.0 in both windows, and its same-day return is below that of other lows in 2024-26.
- **The only positive slice is Z2G together with 🔻💪:** +2.6 / +2.15 pp same-day, CI > 0 in both windows.
  - 🔻💪 carries this on its own (+1.0…+1.8 in DEPARTURE_V1 and BOTTOM_CLUSTER_V1).
  - The Z2G overlay *may* add about 0.5-1 pp. That is a post-hoc slice (n ≈ 3.5k per window) and would need its own sealed or forward test before use.

Artifacts (session scratchpad `v4hist/`): `z2g_plan.txt`, `z2g.py`, `z2g.log`. No UI, score or memory change.
