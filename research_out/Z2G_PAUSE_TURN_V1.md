# Z2G_PAUSE_TURN_V1 — Z2G drop → pause (T9/T10/T11/Z9/Z10/Z11) → T1/T2G turn from the bottom

**Date** 2026-09-30 · the user's own pattern (from LTRX 2026-09), user-approved plan. Frozen to `zpt_plan.txt` before any outcome.
**Universe.** All US tickers, 2021-05…2026-09. Per-bar book return, paired within day × ATR% (22) × trigger state, day-clustered.

**Definitions**
- **H1 strict:** Z2G, then ONLY pause bars (1-8), then T1 / T2G.
- **H2 loose:** first T1 / T2G within 12 bars of the Z2G, with ≥ 2 pause bars in between and no new Z2G.
- **Both:** the lowest low of [Z2G … trigger] is a 10-bar low.

**Events.** H1 4,834 · H2 22,494 · comparator H0 (Z2G → T1/T2G with no pause) 76,678. For scale, there are 375,994 T1 / T2G bars in total.

## VERDICT: NULL (0 / 2)

| rule | vs other T1 / T2G, MINE | VERIFY | years | $21-89 V | REV lift, event / other | verdict |
|---|---|---|---|---|---|---|
| H1 strict | +0.34 [−1.99, 2.51] | −1.28 [−3.22, 0.67] | 3+ / 3− (2025 −3.67) | −1.03 | 1.01-1.04 / 0.99-1.02 | FAIL |
| H2 loose | +0.00 [−1.09, 0.99] | +0.63 [−0.32, 1.66] | 5+ / 1− | +0.93 | 0.99-1.03 / 0.99-1.02 | FAIL |
| (H0 no pause) | +0.51 | −0.03 | 5+ / 1− | +0.14 | — | — |

**vs ALL bars (descriptive):** H1 +1.04 / −1.53 · H2 +0.03 / −0.04.

**Price buckets (VERIFY)**

| rule | $5-21 | $21-89 | >$89 |
|---|---|---|---|
| H1 | −2.62 | −1.03 | +0.75 |
| H2 | −0.39 | +0.93 | +0.16 |

No bucket is significant.

## Reading
- A T1 or T2G that turns up after a Z2G drop and a pause is **not** better than any other T1 / T2G on the same day at the same volatility. It does not raise the reversal likelihood either (lift ≈ 1.0).
- The pause itself adds nothing: H2 with pause ≈ H0 without pause ≈ 0.
- LTRX in September 2026 is a real instance of the sequence, but it is one of many that did not outperform. This matches every path study so far (T1…T12, Z1…Z2G: 0 of 63): **the sequence into a T/Z bar does not carry information about the trade.** The known modifiers (golden cross, 🕐DR, RS in 2024+) do.

No build, no memory entry. Artifacts (scratchpad `v4hist/`): `zpt_plan.txt`, `zpt.py`, `zpt.log`.
