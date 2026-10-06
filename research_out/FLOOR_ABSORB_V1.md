# FLOOR_ABSORB_V1: the SBUX / AMD floor-absorption turn as a TZL grammar (2026-10-04)

## Set-up

- **Plan:** approved ("ki"). Script `backend/research_floor_absorb.py`, raw output `FLOOR_ABSORB_V1.json`.
- **Exemplars:** SBUX 2026-06-09 and AMD 2026-04-01. The two examples have an identical TZL core: Z4 → T9·L34 → T1G / T2G.
- **Contrasts:** k = 3.

  | variant | rule |
  |---|---|
  | V-A core | Z4 → T9·L34 → (0-1 bar) → T1G / T2G |
  | V-B core + floor | V-A, plus the floor tested twice and held |
  | V-C grammar | effort-down Z (L has 5 or 6) → green non-breakout T with L34 at the 10-bar floor → ≤ 2 bars → T1 / T1G / T2 / T2G, floor held |

- **Sanity check, AMENDMENT_1 and a fix:**
  - **AMENDMENT_1:** the registered sanity check caught V-B missing SBUX, whose double test was the Z4 / T9 lows themselves. So V-B was amended **before any outcome was computed**: the floor test also passes when the T9 low is within 0.5·ATR of the Z4 low.
  - **Side effect:** V-B is now almost identical to V-A (5,047 vs 5,249 bars).
  - **Strata fix:** the side comparison "vs all bars" first ran with T/Z in its strata by mistake. It was fixed, and the main comparison (vs same T/Z) was correct both times.
- **Comparison:** bar 0 vs the same day × ATR%-bin × **same T/Z** (other strength bars). Liquid universe.
- **Estimands:** book return (60 bars, trail 12·ATR%, 15 bps) and +10 % within 20 bars before −7 %.

## Result: 3 / 3 NULL

| variant | n (days) | raw book return | vs same T/Z, 2021-23 | vs same T/Z, 2024-26 | $21-89 | years |
|---|---|---|---|---|---|---|
| V-A | 2,966 (789) | **2.16 %** | −0.82 [−2.54, +0.82] | +1.60 [−0.61, +3.75] | −0.07 | −0.6 −1.6 −0.3 +0.4 **+3.6** +0.2 |
| V-B | 2,899 (779) | 2.12 % | −0.69 [−2.30, +0.89] | +1.55 [−0.64, +3.85] | −0.10 | +0.6 −2.4 −0.1 +0.3 **+3.6** −0.0 |
| V-C | 55,162 (1,317) | 0.49 % | +0.53 [−0.15, +1.19] | −0.18 [−0.75, +0.38] | +0.14 | +0.9 −0.3 +1.1 +0.1 −1.1 +1.0 |

- **Against all bars of the same day × ATR:** V-A gives −1.08 then +1.97; V-C gives −0.10 then −0.47. None is significant.
- **The +10 % hit rate:** about 27 % for all three variants, which is the same as an ordinary bar.

## Reading

1. **The raw number is the trap again.** V-A's raw 2.16 % is three times a normal bar's, but against the same day and the same T1G / T2G bars it is −0.8 in 2021-23 and +1.6 (not significant) in 2024-26. The positive side rests almost entirely on 2025 (+3.6).
2. **The core codes add nothing to the breakout bar.** Z4 → T9·L34 before a T1G / T2G gives that bar no repeatable edge.
3. **The generalised grammar (V-C) is common (55k events) and flat.**
4. **SBUX and AMD** are two of the roughly 27 % of cases that reached the target. The pattern is a real shape, but not a predictive one.

## Conclusion

No build. What made SBUX and AMD rise was the market day plus each name's own story. The floor-absorption TZL grammar, on its own, does not separate them from the T1G / T2G bars that failed.
