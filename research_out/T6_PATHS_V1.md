# T6_PATHS_V1 — does the path into T6 matter, and which signals strengthen T6 inside a path?

**Date** 2026-09-29 · plan user-approved ("ki") and frozen to `t6paths_plan.txt` before any outcome.
**Data** 44,479 T6 bars, all US tickers, 2021-05…2026-09. MINE 2021-23 / VERIFY 2024-26.
**Estimand** Contrasts are inside T6, matched on day × ATR% (22 bins), on the book per-bar return. REV is shown per side, adjusted for location × ATR% × bars-since-low.

## VERDICT: NULL (Part A 0/3 · Part B 0/20). The path into T6 does not change what T6 does.

### Part A (k = 3), return Δ in pp

| contrast | share | MINE | VERIFY | years | REV lift, group / rest | verdict |
|---|---|---|---|---|---|---|
| A1 TURN (t-2 red) vs CONT (t-2 green) | 48.7 % | +0.91 [−0.62, 2.56] | −0.53 [−1.89, 0.85] | 5+ / 1− (2025 −2.35) | 1.04 / 1.00 | FAIL |
| A2 swallowed T9/T5/T10/T12 vs other | 45.1 % | −0.60 | −0.17 | 2+ / 4− | 1.00 / 1.04 | FAIL |
| A3 covers ≥ 2 bodies vs 1 | 25.4 % | −0.23 | +0.63 | 4+ / 2− | 1.01 / 1.02 | FAIL |

**Reading.** A turn from red and a continuation of a green run pay the same. So do swallowing a small inside bar vs a T2G, and swallowing one body vs several. Every REV lift is 1.00-1.04.

### Part B — signals on any of t-2..t, within each path
- **MINE k = 650 cells.** The number with |t| ≥ 1.96 was 22 (TURN) and 20 (CONT), against about 17 and 16 by chance. There is almost no signal to select.
- **VERIFY k = 20. PASS 0.**
- **Nearest misses** (not passes, recorded only):
  - **CONT + 🔺 continuation (markup):** +2.29 [0.40, 4.28] VERIFY, 6/6 years. It fails the $21-89 check (−0.17).
  - **CONT + ★V:** +1.46 [−0.26, 3.13], 6/6 years.
  - **CONT + K0 ↓:** −1.76 [−4.02, 0.30], 6/6 years.
- The TURN picks (▲4H REV, M5, K1U) all reversed or vanished.

**Context.** The one booster for T6 found so far is **🕐DR** (TZ_BOOSTERS_V1 AMENDMENT_1): +3.68 MINE / +8.13 VERIFY, day × ATR matched. It was too rare (≈ 2 % of T6) to reach n ≥ 200 within a single path here.

Artifacts (scratchpad `v4hist/`): `t6paths_plan.txt`, `t6paths.py`, `t6paths.log`, `t6paths_B.json`. No build, no memory entry.
