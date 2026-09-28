# NATURE_SHIFT_V1 — does the ▽△ row's change of nature (bottom → 🔺) mark a better entry?

**Date** 2026-09-28 · user hypothesis: "the exact order may not matter, but the nature of the signals changes and that is important" · plan frozen to `natshift_plan.txt` before any outcome · k = 1.
**Verdict: FAIL (NULL).**
- The first 🔺 after a bottom cluster does not beat the day's other bars: MINE +0.32 [−0.28, +0.93], VERIFY −0.02.
- No variant in the grid replicates.
- **Buying the bottom cluster itself** (a descriptive comparator) is the best of the three in both windows.

## Setup
- **Universe.** All 5,739 US tickers in the DB, eligible bars (close ≥ $5, $vol ≥ $5M).
- **SHIFT at t.**
  - 🔺 on t;
  - ≥ 2 bars with a bottom mark (🔻💪, 🔻 or 🌀) in t-5..t-1;
  - no 🔺 in t-5..t-1. So this is the *first* continuation after a bottom cluster.
- **Comparators.**
  - **BOTTOM:** a bottom mark on t with another bottom-mark bar in t-4..t-1, i.e. buying the cluster itself.
  - **PLAIN 🔺:** 🔺 with no bottom mark in the prior 5 bars.
  - **CONTROL:** stride-10 control grid.
- **Exit.** Book ATR×12 trail, maxh 60, entry open[t+1]. Same-day Δ vs CONTROL, day-clustered.

## Results

| rule | MINE 2021-23: n · mean · same-day Δ [CI] · years | VERIFY 2024-26: n · mean · same-day Δ [CI] · years |
|---|---|---|
| **SHIFT** (≥2 bottoms → first 🔺) | 17,417 · −1.04 % · **+0.32 [−0.28, +0.93]** · +0.98/+0.81/−0.48 | 22,358 · +1.90 % · **−0.02 [−0.58, +0.51]** · +0.64/−0.40/−0.38 |
| BOTTOM (buy the cluster) | 106,319 · +0.37 % · **+1.13 [+0.90, +1.35]** · 3/3 + | 136,792 · +2.70 % · **+0.30 [+0.07, +0.55]** · 3/3 + |
| PLAIN 🔺 | 51,506 · −2.39 % · +0.38 [−0.05, +0.83] | 72,166 · +2.50 % · +0.23 [−0.18, +0.65] |
| control | −1.80 % | +2.04 % |

**Sensitivity grid (descriptive):** ≥ 1/2/3 bottom bars × a window of 5/10.

| window | MINE Δ | VERIFY Δ |
|---|---|---|
| 5 | +0.32 … +0.66 | −0.37 … +0.18 |
| 10 | +0.59 … +0.73 | +0.16 … +0.31 |

No VERIFY CI is above 0.

## Reading
- **The change of nature is real on the chart, but it arrives after the move has started.** By the first 🔺, the rebound that the bottom cluster announced is already in the price. Entering there earns what an ordinary bar that day earns.
- **Buying the weakness beats waiting for confirmation.** The bottom-cluster entry is positive in both windows and in all 6 years.
  - Its edge shrinks from +1.13 to +0.30, so it is small.
  - It was a comparator, not the sealed hypothesis, so it is **not** a finding.
  - It is consistent with the book's standing laws: "PULLBACK beats strength-chase" and "confirmation costs".
- This extends ROWSEQ_V1, where the "bottom → 🔺" transitions were not selected as turn-zone features either.

Artifacts (session scratchpad `v4hist/`): `natshift_plan.txt`, `natshift.py`, `natshift.log`, `natshift_result.json`. No UI, score or memory change.
