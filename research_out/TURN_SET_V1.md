# TURN_SET_V1 — which combinations of V4 signals mark a turn?

**Date** 2026-09-27 · plan approved by the user ("ki momwons") · frozen to `turnset_plan.txt` before any outcome.
**Verdict: split.**
- **Turn identification replicates.** Bars carrying many enriched signals turn more often than other 10-bar lows (lift 1.3-1.5 in VERIFY, every threshold tested).
- **It does not reliably become a better trade.** The pre-registered rule (≥ 32 of 58) passed its trade gate. The neighbouring thresholds did not, so that pass is a **peak, not a plateau**.
- **No build is recommended.**

## Setup
- **Candidate.** A 10-bar low: 67,049 in MINE 2021-23, 64,072 in VERIFY 2024-26.
- **Turn.** No lower low in the next 10 bars AND a close ≥ 3 ATR higher within 20 bars. The base rate is 10.5 % in MINE and 12.3 % in VERIFY.
- **Features.** 433 catalog keys (duplicates collapsed); a key is "on" if it fired on any of t-2..t.
- **Lift.** P(turn | pattern) / P(turn | any candidate).
- **Trade check.** Book path-sim (ATR×12 trail, maxh 60), entry open[t+1], compared with all candidates on the same entry day.

## MINE findings
1. **The "always present" set is a trap.** The keys most common at turns (85-99 % of turns: wick-D, L5/PS, Z, L4, NE-E, ...) have lift ≈ 1.0. They are equally common at lows that keep falling.
2. **Enrichment is additive.** Among 58 enriched keys, the more fire together the higher the lift: ≥ 10 → 1.24 · ≥ 18 → 1.49 · ≥ 26 → 1.62 · ≥ 32 → 1.69.
3. **Exact itemsets reach lift 2.5-2.6.** Examples: d_flip_bull + ZRT + ATM/G3, or spring + FBO + flip + ZRT. 7,276 itemsets were scanned, so their MINE lifts are search-inflated.

## VERIFY — the 4 pre-registered candidates (k = 4)

| candidate | n / days | turn lift (CI) | trade Δ vs other lows (CI) | years | gate |
|---|---|---|---|---|---|
| set1 flip + ATM + G3gap + ZRT | 351 / 181 | 1.32 [0.97, 1.71] | +1.79 [−1.35, +5.55] | −0.3 / +5.2 / −0.2 | fail |
| set2 G3 + flip + ZRT | 347 / 178 | 1.08 [0.78, 1.46] | +1.15 [−2.45, +5.33] | −0.8 / +4.8 / −1.2 | fail |
| set3 spring + FBO + flip + ZRT | 282 / 162 | **1.67 [1.24, 2.27]** | +1.45 [−3.48, +8.57] | +1.2 / +3.2 / −0.8 | fail (trade) |
| **≥ 32 of 58** | 330 / 190 | 1.35 [1.02, 1.70] | **+2.66 [+0.28, +5.45]** | +2.8 / +2.8 / +2.3 | **PASS** |

Control, all VERIFY 10-bar lows: +3.55 % mean per trade.

## Plateau check (descriptive, run after the verdict, not a selection)

| rule | n | turn lift | trade Δ (CI) |
|---|---|---|---|
| ≥ 20 | 8,729 | 1.34 | −0.26 [−1.40, +1.08] |
| ≥ 24 | 4,117 | 1.37 | −0.35 [−1.86, +1.73] |
| ≥ 28 | 1,419 | 1.48 | +0.32 [−0.96, +1.71] |
| ≥ 30 | 733 | 1.41 | +0.79 [−0.96, +2.56] |
| **≥ 32** | 330 | 1.35 | **+2.66 [+0.08, +5.43]** |
| ≥ 34 | 130 | 1.25 | +0.65 [−2.21, +3.60] |

## Reading
- **The user's intuition is partly right.** There *is* a recurring combination. It is not a fixed set that is "always there", but a **count**: the more enriched signals fire in the 3 bars, the likelier the low is a real turn. That holds out of sample at every threshold (lift 1.3-1.5), and set3 (spring + FBO + flip + ZRT) replicates at 1.67.
- **Identifying the turn is not the same as earning more from it.** Higher-count lows turn more often, yet their trailing-exit trades earn about the same as any other 10-bar low (Δ ≈ 0 for ≥ 20…30). Only ≥ 32 clears the trade gate, and ≥ 30 / ≥ 34 do not, so that pass sits on a single point.
- **Recommendation.**
  - Do not build a buy rule from this.
  - The count is a reasonable *descriptive* "turn-likelihood" gauge. Displaying it (e.g. "18/58") would be honest; scoring it would not.
  - A follow-up would need its own plan. The obvious one is a count-based turn gauge paired with an exit designed for turns (the 3-ATR target the label uses) rather than a trend-following trail.

Artifacts (session scratchpad `v4hist/`): `turnset_plan.txt`, `turnset.py`, `turnset_mine.log`, `turnset_mine.json`, `turnset_verify.py`, `turnset_verify.log`, `turnset_plateau.py`, `turnset_plateau.log`. No UI, score or memory change.
