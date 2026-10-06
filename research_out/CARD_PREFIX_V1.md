# CARD_PREFIX_V1: inside the "good T1G / T2G card", does the 2-bar pattern before the signal matter? (2026-10-05)

## Set-up

- **Plan:** approved ("კი ორივე"). Script `backend/research_card_prefix.py`, raw output `CARD_PREFIX_V1.json`.
- **Population:** T1G / T2G with golden cross ∧ RS ∧ volume < 1.5× median ∧ 35 < RSI < 70 ∧ 10-bar return ≤ 0, liquid.
  - 24,017 bars over 2022-26 (2021 is EMA200 warm-up).
  - By year: 3,677 / 4,773 / 6,087 / 5,633 / 3,847.
- **Comparison:** group vs the rest **inside the card**, same day × ATR%-bin × signal.
- **Estimand:** book return (60 bars, trail 12·ATR%).
- **Windows:** 2022-23 / 2024-26.

## Part A: 5 pre-fixed prefix patterns, 0 / 5 PASS

| pattern (bars −2, −1) | n g / rest | 2022-23 | 2024-26 | $21-89 | years 22-26 |
|---|---|---|---|---|---|
| P1 two red (Z, Z) | 3,930 / 20,087 | −2.93 [−6.68, +0.44] | +3.31 [−2.70, +9.96] | −0.88 | −4.5 −2.0 +0.9 +4.8 +5.0 |
| P2 red L34 in prefix | 1,010 / 23,007 | +1.45 [−1.76, +4.55] | −2.39 [−6.06, +1.15] | −0.70 | +1.6 +1.4 −1.0 −3.3 −3.4 |
| P3 effort-down (L has 6) | 10,715 / 13,302 | −1.42 [−3.31, +0.38] | +0.98 [−1.47, +3.50] | +0.39 | −3.7 −0.1 +0.4 +2.0 +0.2 |
| P4 quiet (L only 1 / 2 / 5) | 6,165 / 17,852 | +0.51 [−1.52, +2.58] | +0.96 [−2.17, +4.28] | −1.00 | +1.2 +0.1 +2.3 +1.8 −2.4 |
| P5 inside pause at −1 | 4,481 / 19,536 | +0.45 [−2.00, +2.81] | −0.37 [−3.19, +2.56] | −0.62 | −0.2 +0.8 +0.9 −2.8 +1.4 |

- No CI excludes 0.
- P1 and P2 flip sign between the windows.
- The +10 % hit rate shows nothing either.

## Part B: search over all 2-bar T/Z prefixes

| step | count |
|---|---|
| cells with n ≥ 100 in 2022-23 (the card is sparse) | 10 |
| MINE pass | 1 |

The one MINE pass, `Z1 > T9`:

| | value |
|---|---|
| MINE (2022-23) | n 113, +7.13 [+0.97, +14.10] |
| VERIFY (2024-26) | +1.17 [−4.33, +6.75] |
| years | +7.6 +6.7 −6.9 +7.1 +4.1 |
| $21-89 | ≈ 0 |

It does not replicate. **0 PASS.**

## Reading and power

- **Power is limited.** Inside the card the same-day pairs are few, so the CIs are ±3-7 pp wide. This result rules out a **large** prefix effect (about 4 pp or more). It cannot rule out a small one.
- **Consistent with everything before:**
  - once the context is right (golden cross, RS, dip, normal volume, RSI 35-70), the 2 bars before the T1G / T2G do not change what it pays;
  - the same holds for the path into any T/Z bar (ALL_TZ 0 / 63).

## Conclusion

The sequence question is closed for T1G / T2G. The context is the card, and the prefix adds nothing measurable.

**Live 2026-10-02:** 82 card bars, each with its prefix and P-flags, in `CARD_PREFIX_V1_live.csv`. Display only.
