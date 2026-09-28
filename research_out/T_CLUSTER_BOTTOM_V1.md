# T_CLUSTER_BOTTOM_V1 — repeated T states in the bottom zone (T5 / T12 / T10 …) as a reversal sign

**Date** 2026-09-28 · user's chart observation (02-27…03-09: T5 → T12 → T1G → T10 → T4 at the low) · plan frozen to `tcluster_plan.txt` before any outcome · k = 2.
**Verdict: 2/2 FAIL.** The observation is **true in raw counts** and **fully explained** by where price sits and how recently the low was printed.

## Setup
- **Bottom zone.** A 10-bar low within t-10..t, and price still within 1.5 ATR of it.
- **Scale.** 452k bars in MINE and 539k in VERIFY, over all 5,739 DB tickers.
- **Labels (forward only).**
  - REV: the bottom holds 10 bars and close rises +3 ATR within 20.
  - CLEAN: as in DEPARTURE_V1.
  - Same-day RET Δ.
- **Adjustment.** Location × ATR% × bars-since-low.

## Results

| T bars in the last 5 (bottom zone) | MINE raw REV → adjusted lift · same-day | VERIFY raw REV → adjusted lift · same-day |
|---|---|---|
| 0 | 8.1 % → 1.01 · −1.17 | 9.7 % → 0.99 · −2.99 |
| 1 | 10.6 % → 1.02 · −0.09 | 12.8 % → 1.02 · −1.27 |
| 2 | 12.8 % → 1.00 · −0.11 | 15.4 % → 1.01 · −0.51 |
| 3 | 14.7 % → 0.98 · −0.21 | 17.5 % → 1.00 · −0.12 |
| 4 | 16.4 % → 0.98 · −0.57 | 17.4 % → **0.90** · +0.10 |
| 5 | 18.4 % → 1.01 · −1.97 | 15.9 % → **0.77** · −2.01 |
| **H1** ≥ 3 T bars of 5 | 15.0 % → 0.98 [0.90, 1.08] | 17.4 % → 0.97 [0.91, 1.03] |
| **H2** ≥ 2 of T5/T10/T12 in 7 | 13.8 % → 1.07 [0.94, 1.20] | 16.1 % → 1.04 [0.95, 1.13] |

## Reading
- **The eye is right about frequency.** More T bars at the bottom means more reversals in raw terms: 8 % → 18 %.
- **The T states add no information of their own.** T states appear *because* price is already lifting off the low. The same bottoms at the same distance from the low, the same volatility and the same time since the low reverse just as often without them. Adjusted lift is ≈ 1.0 at every count, and 4-5 T bars is below 1 in VERIFY.
- **This is the recurring lesson of the day.** What the chart shows at a bottom is mostly *where price is*. The signals re-describe it.
- **Same-day returns rise with the T count in VERIFY** (−2.99 at zero T bars, ≈ 0 at 3-4) but **not in MINE**. That is not a stable effect.

Artifacts (session scratchpad `v4hist/`): `tcluster_plan.txt`, `tcluster.py`, `tcluster.log`. No UI, score or memory change.
