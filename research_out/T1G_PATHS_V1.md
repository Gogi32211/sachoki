# T1G_PATHS_V1 — does the path into T1G matter, and which signals strengthen T1G inside a path?

**Date** 2026-09-29 · plan user-approved ("ki") and frozen to `t1gpaths_plan.txt` before any outcome. The design is identical to T6_PATHS_V1.
**Data** 82,164 T1G bars, all US tickers, 2021-05…2026-09. MINE 2021-23 / VERIFY 2024-26.
**Estimand** Contrasts are inside T1G, matched on day × ATR% (22 bins), on the book per-bar return. REV is shown per side, adjusted for location × ATR% × bars-since-low.

## VERDICT: NULL (Part A 0/3 · Part B 0/19)

### Part A (k = 3), return Δ in pp

| contrast | share | MINE | VERIFY | years | REV lift, group / rest | verdict |
|---|---|---|---|---|---|---|
| A1 PAUSE (t-2 green) vs REDRUN (t-2 red) | 49.8 % | +1.15 [0.04, 2.32] | +0.11 [−1.25, 1.43] | 5+ / 1− (2026 −3.03) | 1.03-1.04 / 1.00-1.01 | FAIL (VERIFY ≈ 0) |
| A2 t-1 Z5/Z9/Z10/Z11 vs other | 41.3 % | +0.15 | −0.02 | 4+ / 2− | 1.01-1.02 / 1.02-1.03 | FAIL |
| A3 FULL gap (open > t-1 high) vs partial | 34.0 % | +0.01 [−1.19, 1.14] | **−1.35 [−2.60, −0.11]** | 2021 +2.57, then **2022-26 all negative** | 1.02-1.08 / 1.00-1.01 | FAIL (MINE ≈ 0) |

**Observations** (not passes, recorded only):
- **Full gap (A3).** A T1G that gaps over the whole t-1 range has been worse than a partial gap in every year since 2022. This fits the book pattern that chasing strength loses. It is a hypothesis for a fresh sealed test, not a finding.
- **Pause (A1).** A pause in an up-move (green t-2) was better in 2021-23 but ≈ 0 in 2024-26.

### Part B — signals on any of t-2..t, within each path
- **MINE k = 707 cells.**
  - PAUSE: |t| ≥ 1.96 in 24 cells, against about 18 by chance.
  - REDRUN: 17, against about 17 by chance.
  - In short: noise.
- **VERIFY k = 19. PASS 0.**
- **Nearest misses:**
  - REDRUN + BEST★: +1.96 [−0.72, 4.92], 6/6 years.
  - REDRUN + ⛔veto ↓: −1.57 [−3.85, 0.50], 6/6 years.

**Context.** As with T6, the path into T1G (what it jumps over, how it got there, gap type) does not change what T1G pays. Of the boosters found so far for T1G, one fails and one holds (TZ_BOOSTERS_V1 AMENDMENT_1, ATR-matched):
- **260308 ↓:** −3.41 pp VERIFY. This one passed.
- **MARKUP⚛ ↑:** +0.80 pp VERIFY. This one did not pass.

Artifacts (scratchpad `v4hist/`): `t1gpaths_plan.txt`, `t1gpaths.py`, `t1gpaths.log`, `t1gpaths_B.json`. No build, no memory entry.
