# DEPARTURE_V1 — can the chart tell when the move off the low is "real" (no fall back into the range)?

**Date** 2026-09-28 · user goal: "buy at the bottom, but once the real move has started and the risk of falling back into the range is gone". The hypotheses come from the AMD 2026-03-25…04-02 exit from accumulation. Plan frozen to `departure_plan.txt` before any outcome · k = 5.
**Verdict: 5/5 FAIL in MINE. VERIFY was not the deciding test.** No structural confirmation moves the "clean departure" probability by more than about ±2 pp once price location and ATR% are removed.

## Setup
- **Universe.** All 5,739 US tickers in the DB. Context = bars within 10 bars after a 10-bar low.
- **CLEAN target.** From open[t+1], +3 ATR is reached **before** price falls 1.5 ATR below the entry, within 20 bars, checked bar by bar with the stop checked first.
  - Base rate: 24.5 % in 2021-23, 27.6 % in 2024-26.
- **Lift.** Observed − expected, where the expected rate comes from 10 location deciles × 10 ATR% deciles. Day-clustered CI.

## Results (lift in pp · same-day return Δ vs control, ATR×12 trail)

| hypothesis (AMD analogue) | MINE 2021-23 | VERIFY 2024-26 (descriptive) |
|---|---|---|
| H1 EMA whipsaw reclaim (D in t-5..t-1 → close back over EMA20+50) | +1.43 [−0.46, +3.54] · same-day −0.60 | +1.36 [−0.27, +3.00] · −0.33 |
| H2 🔻💪 on a **green** bar | −0.39 · same-day **+1.59** | +2.35 · **+1.03** |
| H2 🔻💪 on a **red/doji** bar | +1.31 · same-day **+1.81** | +2.92 · **+0.99** |
| → H2 difference green − red | **−1.70 [−3.70, +0.45]** | −0.56 [−2.39, +1.12] |
| H3 BD▼/BDV▼ → next bar green | +0.08 · −0.69 | −0.29 · −0.06 |
| (twin) L46 → L34 | +0.06 · +0.07 | +0.59 · −0.78 |
| H4 ⚛ E★ released ·D → E★ released ·U | **−6.36 [−9.17, −3.48]** · −0.72 | −0.18 · −1.17 |
| (twin) ⚛ regime ·D → ·U | +0.15 · −0.64 | +1.09 · −0.58 |
| H5 bullish outside bar closing over the prior high | −0.78 · **−1.01** | +0.57 · **−1.20** |
| FIRSTGREEN (the first green bar after the low) | −0.44 · −0.53 | −0.21 · −1.04 |
| LOWBAR (buy the 10-bar low itself) | 0.00 · −0.12 | +0.64 · −0.96 |

- **Median adverse excursion** after entry is **−1.5 to −1.6 ATR for every group**: signal, comparator or plain context.

**Scores among context bars, VERIFY (descriptive):**

| score | Spearman with CLEAN | CLEAN by quintile, Q1 → Q5 |
|---|---|---|
| V4 | +0.024 | 26.0 → 29.3 % |
| TURN·58 | +0.004 | flat |
| EDGES any | +0.017 | 26.7 → 28.4 % |

## Reading
- **"The move has started" does not buy safety.** Every confirmation pattern visible in AMD leaves the chance of reaching +3 ATR before −1.5 ATR where an ordinary bar at the same location and volatility has it:
  - the EMA reclaim;
  - the green bar after BD▼;
  - L46→L34;
  - the ⚛ energy flip;
  - the bullish outside bar;
  - the first green bar.
- **The dip after entry is volatility, not a failed signal.** The typical post-entry drawdown is ~1.5 ATR regardless of the signal. A stop tighter than about 2 ATR is hit by most entries, confirmed or not. This is the same lesson as TURN_STOP_V1: a wider stop raises everyone's win rate but does not separate good entries from bad ones.
- **Confirmation bars cost money.** The strength-chase law holds again: the outside bull bar (−1.0/−1.2), FIRSTGREEN (−0.5/−1.0) and the EMA reclaim (−0.6/−0.3) all earn **less** than the day's other bars.
- **🔻💪 is the one consistently positive entry** (same-day +1.0…+1.8 in both windows), and the candle colour makes **no** difference; red/doji is if anything slightly better. It does not improve the clean-departure odds either. Its edge is in the average outcome, not in avoiding the dip.
- **Scores do not identify clean departures.** V4 has a faint monotone tilt (+3 pp from Q1 to Q5), too small to use.

## Implication for the user's goal
The chart cannot, with these signals, tell a turn that will not retest from one that will. The practical answers are:
- **Position and stop design:** size for a ~1.5-2 ATR adverse move and use the book ATR×12 trail.
- **Entry choice among already-validated edges:** 🔻💪, 🕐DR and its pairs, which pay on average and accept the dip.

Waiting for confirmation does not remove the retest risk. It removes part of the move.

Artifacts (session scratchpad `v4hist/`): `departure_plan.txt`, `departure.py`, `departure.log`. No UI, score or memory change.
