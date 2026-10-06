# TZQ_DIV_V1: T/Z-vs-Q divergence before a reinforced bull bar (2026-10-04)

## Set-up

- **Origin:** the user's RGTI / AMD / SNDK screenshots. Plan approved ("ki").
- **Script:** `backend/research_tzq_divergence.py`, raw output `TZQ_DIV_V1.json`.
- **Bar polarity:** T/Z sign × Q direction (Q1-Q4 up, Q5-Q8 down, Q0 neutral).

  | code | meaning | screenshot example |
  |---|---|---|
  | TU | reinforced bull | T2G, T1, T4·Q3G |
  | TD | bull divergence | T5, T3, T9·Q5G |
  | ZU | bear divergence / absorption | Z9·Q4R, Z5/Z11, Z4·Q3R |
  | ZD | reinforced bear | |

- **Population:** bar 0 = TU (949,487 liquid bars).
- **Return:** book (60 bars, trail 12·ATR%, 15 bps).
- **Pairing:** day × ATR%-bin × **bar-0 T/Z state**, so a T2G is compared with a T2G.

## Result

| # | contrast | n g / rest | raw return | 2021-23 | 2024-26 | $21-89 | years | PASS |
|---|---|---|---|---|---|---|---|---|
| H1 | ≥ 1 divergence (TD/ZU) in −3..−1 vs none | 655k / 294k | 0.56 / 0.14 | +0.23 [−0.18, +0.59] | **+0.42 [+0.13, +0.70]** | +0.16 | +0.4 +0.1 +0.3 +0.3 +0.3 +0.8 | ✗ |
| H2 | exactly TD → ZD → TU → TU (AMD-2 / SNDK-1) | 11k / 938k | **1.00 / 0.42** | −0.34 [−1.61, +0.99] | −0.39 [−1.97, +1.29] | −1.07 | −1.2 +0.5 −0.6 −0.1 −0.2 −1.2 | ✗ |
| H3 | ≥ 1 ZU in −3..−1 vs none | 396k / 294k | 0.70 / 0.14 | +0.23 [−0.22, +0.69] | **+0.47 [+0.14, +0.83]** | +0.41 | −0.3 +0.2 +0.6 +0.4 +0.3 +0.8 | ✗ |

The secondary 10-bar win rate is also not significant in either window.

## Reading

1. **Divergence is not rare.** 69 % of all TU bars have a TD or ZU bar among the 3 bars before them, so "there was a divergence" is nearly the normal state and not a filter.
2. **H1 / H3 lean small and positive** (≈ +0.2…+0.5 pp per trade; H1 positive in 6 / 6 years). They are significant only in 2024-26 and not in 2021-23, so they fail PASS and Bonferroni. Read the other way round, the same fact says the 31 % of TU bars with three reinforced bars in a row (no pause, no absorption) are slightly worse. That is the known "chasing strength loses" law (`project_entry_timing`) at a small size.
3. **The screenshots' exact pattern (H2) has a strong raw return (1.00 vs 0.42 %) but is NEGATIVE once matched to the same day.** Its good raw number comes from the days it fires on (strong market days), not from the pattern. This is the same trap as the 67 % raw win in Q3_SEQ_V1.

## Conclusion

- No build.
- The pattern visible in the screenshots is real as a shape, but it does not beat the same T/Z bar on the same day.
- What made those charts rise was the market or sector day plus the breakout bar itself. The breakout bar is part of the pattern, so seeing "the rise" partly means seeing the outcome inside the pattern.
