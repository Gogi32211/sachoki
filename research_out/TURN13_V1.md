# TURN13_V1 — a 13-bar window, V4, and many-signals-on-one-bar as turn identifiers

**Date** 2026-09-27 · user request ("aviRoT 13 bari … ert barze bevri signalis tanxvedra … V4") · plan frozen to `turn13_plan.txt` before outcomes · k = 8.
**Verdict: STOP at the deciding test.** Every 13-bar measure identifies turns *worse* than the existing 3-bar TURN·58. VERIFY was not run.

## The AMD case that motivated it (descriptive)
AMD's March 2026 base (low 2026-03-30 at 196) went on to 278 by 2026-04-16.
- **Signal activity** peaked 3-5 bars *before* the low: 03-25 → 03-27 had TURN·58(3) of 23-26, V4 140, and rare keys such as 🎬SVC, HB15, ATM, G3+L46 and E★.
- **The low itself** (03-30) read only 13.
- **V4** also peaked on the turn bar 03-31 (130, T9 L34 FRI34 BL BEST★) and on the breakout 04-01/02.

That is why a longer window looked worth testing.

## Test — NASDAQ 10-bar lows, MINE 2021-23 (163,251 candidates, base 8.0 %)
Coverage-matched to the 3-bar baseline:

| measure (t-12..t) | at ≈ 11.6 % coverage: lift | Δ vs 3-bar | at ≈ 3.1 % coverage: lift | Δ vs 3-bar |
|---|---|---|---|---|
| **baseline TURN·58, 3 bars** | **1.47** | — | **1.56** | — |
| A. TURN·58 over 13 bars | 1.34 | −0.14 | 1.46 | −0.10 |
| B. max V4 over 13 bars | 1.20 | −0.27 | 1.11 | −0.45 |
| C. sum of V4 over 13 bars | 1.19 | −0.28 | 1.20 | −0.36 |
| D. bars with an unusually high signal count (≥ ticker's own 90th pct) | 1.06 | −0.42 | 1.04 | −0.52 |

## Reading
- **Recency wins.** Signals from 4-13 bars before the low dilute the picture. Across 163k NASDAQ lows, what fires in the last 3 bars carries the most turn information. AMD's "activity 3-5 bars early" is one path among many, not the typical one.
- **V4 does carry turn information,** at a lift of about 1.1-1.2, but it is weaker than the curated 58-key count. V4 counts *activity*, and activity is also high at lows that keep falling.
- **"Many signals on one bar"** (measure D), relative to the ticker's own norm, is almost uninformative on its own (lift 1.04-1.06).
- **Taken together with the earlier studies,** the best turn identifier in this app remains the 3-bar TURN·58 (NASDAQ out of sample: 1.42 / 1.56). Widening the window, weighting by V4, or counting busy bars does not improve it.

Artifacts (session scratchpad `v4hist/`): `turn13_plan.txt`, `turn13.py`, `turn13.log`, `turn13_mine.json`. No UI, score or memory change.
