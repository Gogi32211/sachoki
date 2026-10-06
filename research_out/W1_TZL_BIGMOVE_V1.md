# W1_TZL_BIGMOVE_V1: weekly 3/4-bar TZL sequences before a +20 % move (2026-10-04)

## Set-up

- **Plan:** approved ("ki"). Script `backend/research_w1_tzl_bigmove.py`, raw output `W1_TZL_BIGMOVE_V1.json`.
- **Data:** 1W DB, 2021-06 … 2026-09; 558,848 liquid weekly bars (close ≥ $5, ~$5M/day).
- **Tokens:** bars −3..−1 carry the T/Z state; bar 0 carries T/Z + L.
- **Outcome:** +20 % (high) within 13 weeks before a −15 % stop.
  - Entry at next week's open.
  - Bar by bar: open gap first, then stop before target.
  - Censored if the horizon runs past the data.
  - Secondary outcome: +40 % within 26 weeks.
- **Lift:** against the same week × weekly ATR%-bin (cuts frozen from MINE), week-clustered.
- **Base hit rates:**

  | outcome | MINE | VERIFY |
  |---|---|---|
  | +20 % | 24.7 % | 29.6 % |
  | +40 % | 11.8 % | 17.5 % |

## Search accounting

| | MINE cells (n ≥ 100, ≥ 30 weeks) | MINE pass | VERIFY positive | mean lift MINE → VERIFY | PASS |
|---|---|---|---|---|---|
| 3-bar | 587 | 11 | 3 / 11 | +10.0 → **−3.0** | 0 |
| 4-bar | 77 | 1 | 1 / 1 | +18.4 → +13.7 | **1** |

- Total k = 664 MINE cells.
- The 3-bar candidates are pure search noise: they regressed fully, and below zero.

## The survivor: `T2G > T2G > Z3 > Z2G·L46` (weekly)

Two gap-up weeks, then a weak week (Z3), then a gap-down week fully below the prior body with rising volume and a down close (Z2G, L46). In words: the first sharp shakeout week after a strong 2-week run.

| | MINE 2021-23 | VERIFY 2024-26 |
|---|---|---|
| n (weeks) | 107 (31) | 111 (33) |
| +20 % hit | 31.8 % | **45.9 %** |
| lift vs same week × ATR | +18.4 (lo +3.6) | **+13.7 (lo +0.9)** |
| +40 % / 26 w lift | | +10.4 |

- **Bonferroni:** k_verify = 1, so it passes.
- **By year:** 2021 +1.2, 2022 +19.1, 2023 +26.1, 2024 −7.6, 2025 +26.9, 2026 +21.5. That is 5 / 6 positive.
- **$21-89 zone:** +15.4.

## Audit (record-size lift, so audited before reading)

- **Concentration:** events sit in 72 distinct weeks; the top 3 weeks hold 37 % of them (2023-12-31: 44, 2024-09-01: 30, 2022-06-12: 11).
- **Leave-one-week-out (VERIFY):** the lift stays between +12.9 and +14.6 whichever big week is dropped.
- **But the CI is fragile:**
  - dropping 2024-05-19 gives lo −0.2;
  - dropping the top two weeks gives +13.3, lo −0.2.
- The effect size is stable. The significance rests on about 33 weeks and is borderline.
- The lift is measured against other stocks in the **same week**, so a market-wide rebound does not explain it.

## Verdict

**SEARCH SURVIVOR, fragile. A forward candidate, not a BUILD.**

- **Why it is worth keeping:** it is mechanically plausible. It belongs to the same family as the book's capitulation and washout edges: buy absorbed weakness after strength.
- **Why it is not yet a build:** it is 1 of 664, and its significance hangs on about 33 weeks.

**Last week (2026-09-27):** no liquid ticker shows the pattern.
