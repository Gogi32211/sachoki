# VOL_ECHO_LONG_V1 — do the 260925_VOL_ECHO signals (and their TZ·L sub-types) rise afterwards?

**Date** 2026-09-25 · plan approved by the user before running ("gidastureb") · long side only.
**Verdict: NULL — 0 of 274 cells pass MINE.** No signal and no TZ·L sub-type is followed by above-normal 20-day return.

## Setup
- **Signals.** VE, Q, ▲ Release, ▼ Release, BO▲, BD▼, BOV▲, BDV▼ from the exact Python port of the Pine script at its defaults. The port was checked against TradingView on RGTI Oct 2024. SPK was excluded because it is known only after the echo (lookahead). SPIKE was excluded because it fires on 23 % of bars (context).
- **Data.** 5,339,859 daily bars, 5,695 tickers, 2021-05 → 2026-08. Of these, 3,978,146 are ≥ $8.
- **Outcome.** Entry open[D+1], exit close[D+20]. Excess is measured against the same-day median of the same price bucket ($8-21 / $21-89 / $89+). CIs are day-clustered.
- **Windows.** MINE 2021-2023 · VERIFY 2024-2026. Bars under $8 are descriptive only.
- **Registry k = 274.** It holds 8 signal-level cells and 266 signal × TZ·L cells (MINE n ≥ 300, ≥ 100 days, VERIFY n ≥ 300). It was frozen to `long_v1_registry.json` before any outcome was computed.
  - The plan said 276. The frozen census ran after dropping right-edge rows with no 20-day outcome, which removed 2 marginal combos. The DSR family uses 274.

## Deciding test — TZ·L split vs label-shuffle placebo (MINE, 200×)
The spread of combo medians beat the placebo 95th percentile in 2 of 8 signals, and only barely:

| signal | observed spread | placebo p95 |
|---|---|---|
| VE | 0.375 | 0.351 |
| BD▼ | 0.513 | 0.416 |

The other six were within noise. Per the plan the combo cells continued.

## MINE results
**Signal-level median 20-day excess (≥ $8), pp:**

| VE | Q | ▲ Rel | ▼ Rel | BO▲ | BD▼ | BOV▲ | BDV▼ |
|---|---|---|---|---|---|---|---|
| −0.00 | −0.28 | −0.04 | +0.05 | +0.20 | +0.04 | +0.16 | +0.20 |

Only 2 of 274 cells reached the +1.0 pp median bar, and both fail DSR:
- **BD▼ × Z5·L12.** n 542, 268 days. Median **+1.33** [+0.80, +2.05]. All three years positive (+1.03 / +1.22 / +1.92). It beats the rest of BD▼ by +1.31 [+0.72, +2.04]. **DSR 0.038.**
- **BOV▲ × T6·L3.** n 358. Median +1.18 [−0.04, +2.08]. 2021 was negative. **DSR 0.005.**

Against a 274-cell search, a +1.3 pp cell in about 540 trades is what the best of many coin flips looks like. **MINE survivors: 0.** VERIFY was not opened for any cell, and path-sim was not run.

## Descriptive (not gates)
- **BDV▼ leans slightly positive everywhere** (MINE +0.20, VERIFY +0.20, $21-89 +0.24 / +0.27). This is the reversal direction the user expected, but it is about a fifth of the size the bar requires.
- **▼ Release under $8** is +0.55 in MINE and +0.76 in VERIFY. That is the lottery zone, excluded from gates by design (feedback: <$8 = lottery).
- **BOV▲ under $8** is negative in both windows (−0.23 / −0.45). Volume breakouts in cheap names fade.

## Reading
- **The script is a good description of what happened, not a forecast.** Echo, quiet bars, release and breakout, in any TZ·L flavour, leave the next 20 days at the market median.
- **Negative signals do not reverse strongly either.** BDV▼ and ▼ Release lean the right way but stay within ±0.3 pp.
- **BD▼ × Z5·L12 is a hint, not a candidate.**
  - It is a red bar that opened below the echo zone after a gap-up relative to the previous bar, on declining volume.
  - Re-testing it would be a new family with its own k, and VERIFY 2024-26 is still unopened for it.
- **The short-side study** (next, separate plan) starts from this table. No signal-level cell is meaningfully negative except Q (−0.28), so the prior for a short edge is low. That matches the book's closed-short conclusion.

Artifacts: session scratchpad `vecho/long_v1.py`, `long_v1.log`, `long_v1_registry.json`, `long_v1_result.json`. No UI, score, or memory change.
