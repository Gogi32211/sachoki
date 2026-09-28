# SPRING_LPS_V1 — Wyckoff spring → thrust → dry test (LPS) → entry

**Date** 2026-09-27 · user approved the plan ("gaakete ab") · plan frozen to `spring_lps_plan.txt` before any outcome · k = 1.
**Verdict: FAIL (NULL).** The confidence interval crosses zero, and 2021 is negative. The rule is not re-tuned.

## Where the hypothesis came from
Eight hand-picked winners:
- AMD 2026-03
- RGTI 2024-11 and 2025-08
- OKLO 2025-04 and 2025-08
- RKLB 2025-04, 2025-11 and 2026-04

All eight followed the same visual skeleton:
1. A spring or break below the zone (🌀, BD▼ or BDV▼).
2. A turn bar with HILO↑ and R2X on the same bar.
3. A thrust.
4. A dry-volume higher-low test (Q∧R, ⛔dry, ATM+HB15).
5. A liftoff bar, then E★ and 🔺.

Seven of the eight fall in 2025-26, so **2021-23 is the clean primary window**.

## Rule (frozen)
- **Spring.** (🌀 or BD▼ or BDV▼) at a 10-bar low or on 📍flr.
- **Thrust.** T1, T1G or T2G within 6 bars, with a gain of at least 5 % and volume at least 1.3× average.
- **Trigger.** Within 3-12 bars after the thrust, (HILO↑ ∧ R2X) or (ATM ∧ HB15) fires, and the test before it meets all three conditions:
  - mean volume ≤ 0.8×;
  - the spring low holds;
  - price closes below the thrust close at least once.
- **Entry and exit.** Entry at open[t+1]. Book ATR×12 trail, maxh 60.
- **Universe.** NASDAQ plus the Russell 2000 names not in NASDAQ: 5,706 tickers, 5.07M ticker-days. Close ≥ $5 and 20-day mean dollar volume ≥ $5M.
- **Control.** All eligible bars on the stride-10 `control_keys.phase` grid, compared on the same entry day. Δ is day-clustered.

## Results

| window | rule | trades | mean · win | reach +50 % MFE | same-day Δ (95 % CI) | by year |
|---|---|---|---|---|---|---|
| **2021-23** | **LPS** | 390 | −0.18 % · 49 % | 12.6 % | **+1.85 [−1.95, +5.85]** | −4.51 / +1.64 / +4.81 |
| 2021-23 | spring only | 59,738 | +0.05 % · 49 % | 5.1 % | +0.59 [+0.18, +1.01] | +0.11 / +1.22 / +0.19 |
| 2021-23 | thrust (no test) | 4,858 | −0.59 % · 48 % | 9.9 % | −3.15 [−4.48, −1.81] | all negative |
| 2021-23 | HILO↑ ∧ R2X anywhere | 89,095 | −1.36 % · 47 % | 5.1 % | −0.66 [−1.12, −0.20] | |
| 2021-23 | control | 103,891 | −1.84 % · 45 % | 4.7 % | — | |
| 2024-26 | LPS | 443 | +3.48 % · 46 % | 17.4 % | +0.19 [−4.52, +5.36] | −1.23 / +0.68 / +1.10 |
| 2024-26 | spring only | 75,183 | +2.87 % | 7.7 % | −0.37 [−0.74, −0.01] | |

**LPS by price bucket, 2021-23:**

| bucket | Δ (pp) |
|---|---|
| $5-21 | +3.69 |
| $21-89 | +0.34 |
| >$89 | −5.13 |

**By trigger type:**
- HILO∧R2X: 378 trades, Δ +1.33 in 2021-23 and −0.10 in 2024-26.
- ATM∧HB15: 12 and 13 trades, Δ +12.1 and +6.6. The sample is too small to read.

## Reading
- **The sequence does not pick a better trade.** Its point estimate is positive but noisy, and the ~400 trades split across ~180 days give a CI about ±4 pp wide.
- **LPS names reach +50 % MFE 2.5-2.7× more often than the control** (12.6 % vs 4.7 %; 17.4 % vs 7.6 %). They also stop out more, so the mean is no better. The rule selects *volatile names in motion*, not better entries. MFE is descriptive here, not a return (see feedback-pathsim-not-mfe-proxy).
- **Stand-alone lessons:**
  - Buying the **thrust bar itself** is clearly bad (−3.15 pp, every year negative). This matches the "strength-chase loses" law.
  - HILO↑∧R2X alone is mildly negative.
  - Spring-only is +0.59 in 2021-23 but −0.37 in 2024-26, so it does not replicate.
- ⚠️ **Coverage defect, found after the run.** The frozen rule caught **1 of the 8** motivating cases (OKLO 2025-04). The others failed on:
  - the thrust T-type set (AMD's turn bar was T9, RGTI's was T4);
  - the ≤ 0.8× mean test volume (RGTI 2025 had 1.1-1.6× days inside the test).

  So this test measured a *stricter cousin* of the visual pattern, not the pattern itself. The census step ("does the rule reproduce the examples?") should have run before sealing. The result stands as frozen and nothing is re-tuned on it.

## If continued (needs user OK)
A V2 would calibrate the definition on the 8 case dates **only**, targeting ≥ 6/8 captured, before any outcome is seen. It would then freeze and test on 2021-23, which none of the cases touch. With k = 2 for the family, the search burden gets disclosed.

Artifacts (session scratchpad `v4hist/`): `spring_lps_plan.txt`, `spring_lps.py`, `spring_lps.log`, `spring_lps_trades.csv`, `build_rows_r2x.py`, `fired_r2x.ndjson` (Russell-only 1,714 tickers), `fired_oklo.ndjson`, `case_dump.py`, `cases5.txt`. No UI, score or memory change.
