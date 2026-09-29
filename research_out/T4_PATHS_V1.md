# T4_PATHS_V1 — does the path into T4 matter, and which signals strengthen T4 inside a path?

**Date** 2026-09-29 · plan user-approved ("ki") and frozen to `t4paths_plan.txt` before any outcome. The design is identical to T6/T1G_PATHS_V1.
**Data** 98,032 T4 bars, all US tickers, 2021-05…2026-09. MINE 2021-23 / VERIFY 2024-26. Contrasts are day × ATR% matched.

## VERDICT: NULL for the path (Part A 0/3). Part B 1/19, a single pass at roughly chance level (≈ 0.5-1 expected).

### Part A (k = 3), return Δ in pp

| contrast | share | MINE | VERIFY | years | verdict |
|---|---|---|---|---|---|
| A1 REDRUN (t-2 red) vs PAUSE (t-2 green) | 49.4 % | +0.01 | −0.52 | 3+ / 3− | FAIL |
| A2 swallowed Z5/Z9/Z10/Z11 vs other | 34.1 % | +0.41 | −0.40 | 3+ / 3− | FAIL |
| A3 covers ≥ 2 bodies vs 1 | 33.0 % | −0.92 [−1.93, 0.16] | +0.06 | 1+ / 5− | FAIL |

REV lift is 0.95-1.05 on every side.

### Part B — signals on any of t-2..t, within each path
- **MINE k = 741 cells.**
  - REDRUN: 42 cells with |t| ≥ 1.96, against about 18 by chance. There was structure in MINE, but none of it replicated.
  - PAUSE: 19, against about 19 by chance.
- **VERIFY k = 19. PASS 1.**
  - **PAUSE + VE (VOL ECHO echo) ↓:** −2.07 [−3.80, −0.49], 6/6 years, $21-89 −0.90.
  - With 19 tests, one pass is about what chance gives. Treat it as a WATCH item for the veto side, not a finding.
- **Nearest miss:** REDRUN + M1·σ3 ↓ −6.42 [−13.86, 0.76], 6/6 years, n 261.

**Context.** The path into T4 does not change what T4 pays, the same as for T6 and T1G. Findings for T4 from TZ_BOOSTERS_V1 AMENDMENT_1 (ATR-matched):
- **P2 ↓:** −1.39, PASS.
- **MR+ / M0 / V×5 / M6 ↓:** extreme-volume family (V1 audit).

Artifacts (scratchpad `v4hist/`): `t4paths_plan.txt`, `t4paths.py`, `t4paths.log`, `t4paths_B.json`. No build, no memory entry.
