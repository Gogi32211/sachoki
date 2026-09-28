# PATH_ORDER_V1 — does the intraday path of a daily bar add information beyond its colour?

**Date** 2026-09-24 · plan approved by the user before running ("gidastureb") · registered k = 20 (2 conflict cells + 18 timing-grid cells).
**Verdict: NULL — stopped at the deciding test.** The 18-cell timing grid was never run, so no outcome from it was seen.

## Setup
- **Signal source.** Studio 15m store, S&P 500, regular session. 765,592 full 26-bar sessions; 12,604 partial sessions dropped. High and low slots are the first 15m bar that printed the session extreme.
- **Data contract.** The 15m-built session OHLC matches the 1D store within 0.5 % on 99.4 % of sessions. Colour agrees on 99.0 %. Only colour-agreeing non-doji sessions were kept, leaving 742,207 analysis rows across 614 tickers, 2021-07-02 → 2026-08-25.
- **Outcome.** 1D store, entry open[D+1], exit close[D+20]. There is no close-entry lookahead and censored rows are dropped.
- **Control.** Each bar is measured against the same-day median of bars of the **same colour**. The path agrees with colour 88 % of the time, so the plain universe is the wrong competitor.
- **Windows.** MINE 2021-2023 · VERIFY 2024-2026 (VERIFY untouched).
- **Massive 1m was not used.** That programme stays at Y_EXPOSED = 0.

## Census (descriptive, no outcome)

| | L first | H first | same 15m bar |
|---|---|---|---|
| green | 333,045 (88 %) | 45,171 (12 %) | 7,915 |
| red | 43,215 (12 %) | 324,218 (88 %) | 8,393 |

The extreme is set in the first 15 minutes on 34-35 % of sessions and in the last 15 minutes on about 11 %.

## Deciding test (MINE, 20-day excess vs same-colour same-day median; stop if both |Δ| < 0.25 pp)

| cell | n | days | median | day-clustered CI | 2021 / 2022 / 2023 |
|---|---|---|---|---|---|
| C1 GREEN · H first (intraday shakeout, recovered) | 20,982 | 620 | **−0.09** | [−0.23, +0.00] | −0.09 / −0.18 / −0.02 |
| C2 RED · L first (intraday rally, sold) | 19,247 | 623 | **+0.03** | [−0.08, +0.17] | +0.04 / +0.10 / −0.01 |

Both are below 0.25 pp, so the study **stopped → NULL**. Both also lean the wrong way for the registered hypotheses: the "recovered shakeout" is slightly worse than an ordinary green day, and the "sold rally" slightly better than an ordinary red day.

## Reading
- The order in which a day printed its high and low is, 88 % of the time, just the candle colour again.
- The 12 % where the path contradicts the colour carries no forward information at 20 days.
- This joins the intraday-volume family (OVD, EFFORT_BALANCE, BREADTH_TRANSITION): the shape of *how* a daily bar was built, seen from inside the day, has not added anything the daily candle plus context does not already say.
- The timing grid (first hour / mid / last hour for high and low) remains **unevaluated**. The deciding test was registered to save it. Reopening it would be a new family with its own k.

Artifacts: session scratchpad `path/study.py`, `study.log`, `result.json`. No UI, score, or memory change.
