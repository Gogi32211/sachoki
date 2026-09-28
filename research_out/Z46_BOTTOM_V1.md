# Z46_BOTTOM_V1 — are Z4 / Z6 "often the bottom" itself?

**Date** 2026-09-28 · user: "Z4/Z6 are often the bottom" · plan frozen to `z46bottom_plan.txt` before any outcome · k = 1.
**Verdict: FAIL.**
- Z4/Z6 are *not* over-represented at true pivot bars.
- A 10-bar low that is Z4/Z6 holds no better than other lows.
- In 2024-26 it is a worse entry.

## Q1 — which T/Z states sit on true pivot bars (descriptive, hindsight)
A true pivot bar is the low of t-10..t+10, among eligible bars of all 5,739 tickers.

| state | share of pivot bars | share of all bars | × |
|---|---|---|---|
| **Z2G** | **21.9 %** | 11.2 % | **1.96** |
| Z2 | 10.3 % | 7.8 % | 1.32 |
| Z6 | 2.3 % | 2.4 % | 0.96 |
| Z10 | 1.8 % | 2.1 % | 0.85 |
| Z1 | 3.3 % | 4.1 % | 0.80 |
| **Z4** | 3.2 % | 4.5 % | **0.71** |
| T1 | 2.4 % | 4.6 % | 0.51 |
| Z11 | 0.5 % | 1.8 % | 0.29 |
| Z9 | 0.8 % | 4.3 % | 0.19 |
| Z5 | 0.3 % | 3.8 % | 0.08 |
| T2G | 0.6 % | 11.7 % | 0.05 |

**The typical bottom bar is Z2G** (one pivot in five), then Z2. Z4/Z6 appear at pivots at their normal rate or below.

## Q2 — a 10-bar low that is Z4/Z6 vs other 10-bar lows (forward labels, adjusted for location × ATR%)

| | MINE: REV lift · same-day | VERIFY: REV lift · same-day |
|---|---|---|
| Z4/Z6 on the low bar | 0.95 [0.76, 1.18] · −0.23 | 1.07 [0.90, 1.27] · **−2.23** |
| other 10-bar lows | 1.00 · −0.17 | 1.00 · −1.02 |
| Z4/Z6 low, then enter on the next-bar T | same-day −0.55 | same-day −1.40 |

## Reading
- **Why Z4/Z6 look like bottoms on the chart:** they appear in the sell-off *around* lows. But the bar that actually becomes the pivot is usually Z2G or Z2.
- **As an entry, a Z4/Z6 low is no better than any other low.** In 2024-26 it is about 1.2 pp worse.

Artifacts (session scratchpad `v4hist/`): `z46bottom_plan.txt`, `z46bottom.py`, `z46bottom.log`. No UI, score or memory change.
