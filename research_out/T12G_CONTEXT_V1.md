# T12G_CONTEXT_V1: when are T1G and T2G good? (2026-10-04)

## Set-up

- **Plan:** approved ("kki"). Script `backend/research_t12g_context.py`, raw output `T12G_CONTEXT_V1.json`.
- **Factors:** 10 book factors, fixed in advance, each measured **inside** T1G and inside T2G. k = 20.
- **Comparison:** group vs rest of the same signal, same day × ATR%-bin.
- **Estimands:** book return (60 bars, trail 12·ATR%, 15 bps) and +10 % within 20 bars before −7 %.
- **Universe:** liquid. T1G has 101k bars, T2G 327k.
- **Definitions:** GC = EMA50 > EMA200; RS = close/SPY above the ratio's EMA200; both need about 200 bars, so 2021 is mostly missing for F1 / F2.

## Result: 0 / 18 strict PASS; F10 not estimable

F10 (season) cannot be estimated as registered: Dec-Mar and Apr-Nov never share a day, so a same-day contrast is impossible. This is a design error.

Book return Δ in pp, with 95 % CI:

| signal · factor | 2021-23 | 2024-26 | $21-89 | years | reading |
|---|---|---|---|---|---|
| **T1G · F1 GC vs DC** | **+1.96 [+0.52, +3.69]** | **+1.24 [+0.19, +2.31]** | +2.37 | −0.6 +3.7 +1.1 +2.2 −0.2 | both windows significant; fails only "≥ 4 positive years" (3 / 5) |
| T2G · F1 GC vs DC | +0.42 | **+1.48 [+0.71, +2.26]** | +0.92 | −1.4 +1.9 +1.8 +2.0 −0.1 | 2024+ only |
| T1G · F2 RS (inside GC) | +0.69 | **+2.32 [+0.66, +4.15]** | +0.25 | −3.2 +1.4 +1.2 +4.5 +0.5 | 2024+ only (known) |
| T2G · F2 RS (inside GC) | −0.06 | **+1.04 [+0.40, +1.76]** | +1.28 | −2.1 +0.5 +0.5 +2.0 +0.2 | 2024+ only (known) |
| T1G · F5 volume ≥ 1.5× median | −0.74 | **−1.60 [−2.89, −0.35]** | −0.78 | **6 / 6 negative** | near-PASS negative: high volume hurts T1G |
| T2G · F4 RSI ≥ 70 | −0.93 [−1.91, +0.01] | **−0.91 [−1.88, −0.03]** | +0.23 | **6 / 6 negative** | near-PASS negative: overbought T2G is worse |
| T2G · F9 10-bar return ≤ −5 % vs ≥ +5 % | **+2.08 [+0.69, +3.47]** | +0.96 | −0.18 | **6 / 6 positive** | near-PASS positive: T2G after a fall beats T2G after a run |
| T2G · F8 ≤ +2 % over EMA20 vs > +6 % | **+1.28** | −0.03 | −0.03 | +2.6 +1.6 +0.2 +0.2 −0.7 +0.8 | 2021-23 only |
| T1G / T2G · F3 RSI ≤ 35 | ns | ns (hit −3.9 / **−5.4**) | − | mixed | knife law, visible in the +10 % hit rate |
| G3 vs G1 gap (F6), weak vs strong close (F7) | ns | ns | | mixed | no effect |

## Combined card (DESCRIPTIVE, in-sample, built from the near-passes above)

**Good T1G / T2G** = golden cross ∧ RS ∧ volume < 1.5× median ∧ 35 < RSI < 70 ∧ 10-bar return ≤ 0 (bought on a dip, not after a run). That is 6 % of the bars (26k of 428k).

| | n | raw book | vs same signal, 2021-23 | 2024-26 | years |
|---|---|---|---|---|---|
| T1G + T2G | 26,109 | 3.57 % vs 0.28 % | +1.53 [+0.13, +3.02] | +0.64 [−0.47, +1.65] | 2022 +0.6 · 2023 +2.1 · 2024 +1.1 · 2025 +0.2 · 2026 +0.6 |

The +10 % hit rate barely moves (28.3 % vs 26.2 %): the card improves the average return, not the odds of a +10 % burst.

## Reading

1. **Context direction is consistent with the book.** Golden cross ↑, RS ↑ (2024+), high volume on the up bar ↓, overbought ↓, buying a pullback beats buying after a run, RSI ≤ 35 is a knife for the +10 % odds.
2. **None passes the strict rule alone.** Each factor is about 1-2 pp and only one window is significant.
3. **The combined card is positive in every year with data**, but small and in-sample. Its 2024-26 CI includes 0.
4. **Raw returns are again misleading.** The card's 3.57 % raw is mostly the market regime; the honest number is +0.6 … +1.5 pp versus T1G / T2G bars on the same day.

## Live (2026-10-02)

82 liquid T1G / T2G bars meet the card. The list is in `T12G_CONTEXT_V1_good_live.csv` and all 671 are in `T12G_CONTEXT_V1_live.csv`. This is a watchlist, not trades.

## F10 re-estimated (user chose option 3)

**Why the change.** The registered F10 (Dec-Mar vs Apr-Nov within a day) is impossible: the two seasons never share a day. The script is `backend/research_t12g_season.py` and the output `T12G_SEASON_V1.json`.

**Estimand used instead (k = 2).**
- For each day, the signal's edge = T1G (or T2G) book return minus all **other** liquid bars of the same day × ATR%-bin.
- Δ = mean daily edge on Dec-Mar days − mean daily edge on Apr-Nov days.
- The CI comes from a bootstrap over days within each season.

| signal | 2021-23 Δ | 2024-26 Δ | $21-89 | years | daily edge Dec-Mar / Apr-Nov |
|---|---|---|---|---|---|
| T1G | **+1.68 [+0.49, +2.96]** | +0.84 [−0.26, +1.94] | **+1.18** | −1.2 +2.8 +1.4 +1.1 +0.5 +1.5 | W1 +1.18 / −0.50 · W2 −0.29 / −1.13 |
| T2G | **+1.48 [+0.63, +2.39]** | −0.06 [−0.82, +0.74] | +0.80 | −0.1 +1.9 +1.6 +0.9 −0.8 +0.0 | W1 +0.37 / −1.11 · W2 −0.43 / −0.37 |

- The +10 % hit rate is significantly higher in Dec-Mar in 2024-26 for both signals: T1G +2.64, T2G +1.62.
- **No PASS.** Neither signal is significant in both windows.

### Reading

1. **The direction is the opposite of the 🗓️ season gate**, but there is no contradiction:
   - the season gate is about **absolute** returns, which are worse for every setup in Dec-Mar (a market effect);
   - here, **relative to the same day's other stocks**, T1G / T2G hold up as well as or better than the rest in Dec-Mar.
2. **More important by-product:** relative to the same day's other bars, T1G / T2G are slightly negative for most of the year (Apr-Nov −0.4 … −1.1 pp). A gap-up strength bar, on average, does a little worse than its peers that day. This is the "chasing strength loses" law seen directly.
3. **Practical reading:**
   - season is not a T1G / T2G filter;
   - what helps is the context card above: golden cross, RS, dip not run-up, normal volume, RSI below 70.
