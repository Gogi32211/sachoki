# SPRING_LPS_V2 — the definition re-calibrated to capture the user's cases

**Date** 2026-09-27 · user approved ("ki") · plan frozen to `spring_lps_v2_plan.txt` before any outcome · family k = 2 (V1 FAIL + V2).
**Verdict: FAIL (NULL). The SPRING→THRUST→LPS family is closed.**

## Calibration (cases only, no outcomes)
- **Search.** 432 variants were scored only on whether they capture the 8 hand-picked cases. A capture means an entry within start−7 … start+25 bars at a close ≤ 1.30 × the case base.
- **Best capture.** 6/8. The two misses are RGTI 2024-11 and RKLB 2026-04.
- **Choice.** Among the 6/8 variants, the one with the fewest changes from V1. Four parameters were loosened:
  - the spring set widened to 🌀, BD▼, BDV▼, L46VX, 🔻💪, 🧊CF, QZC, 🎯3 and WSH🔎;
  - the thrust became any T bar at +5 % from the spring close, with no volume requirement;
  - mean test volume ≤ 1.2×;
  - the trigger (HILO↑∧R2X or ATM∧HB15) stayed unchanged.
- **Census.** 10,924 eligible events on 560 days in 2021-23, 28× more than V1, so the test has power.

## Results (same universe, exit and control as V1)

| window | rule | trades | mean | reach +50 % MFE | same-day Δ (95 % CI) | by year |
|---|---|---|---|---|---|---|
| **2021-23** | **LPS V2** | 10,837 | −0.91 % | 5.6 % (control 4.7 %) | **+0.37 [−0.64, +1.45]** | −2.42 / +0.48 / +1.59 |
| 2024-26 | LPS V2 | 13,430 | +3.52 % | 9.2 % (control 7.6 %) | +0.89 [−0.06, +1.87] | +0.60 / +0.35 / +2.01 |
| 2021-23 | spring only (S2) | 78,641 | +0.03 % | | +0.68 [+0.37, +1.01] | 3/3 years, all buckets + |
| 2024-26 | spring only (S2) | 100,286 | | | −0.02 [−0.35, +0.31] | does not replicate |
| 2021-23 | V1 thrust bar | 6,134 | | | −2.33 [−3.57, −1.08] | chasing the thrust loses again |

**LPS V2 by price bucket, 2021-23:**

| bucket | Δ (pp) |
|---|---|
| $5-21 | +1.25 |
| $21-89 | −0.30 |
| >$89 | +0.33 |

## Post-hoc check (descriptive, NOT a test)
The ATM∧HB15 trigger branch looked strong inside LPS, so it was compared with ATM∧HB15 anywhere:

| | 2021-23 Δ | 2024-26 Δ |
|---|---|---|
| ATM∧HB15 anywhere | +2.05 [+0.90, +3.23] (3,042 trades) | +1.71 [+0.42, +3.22] |
| ATM∧HB15 inside LPS | +2.44 [+0.07, +4.97] (401 trades) | +4.18 [+1.52, +7.16] |

- **In the clean window** the LPS context adds only +0.4 pp, and the CIs overlap almost fully.
- **The larger gap in 2024-26** sits in the window the cases were chosen from.
- **What survives** is the already-built ATM and HB15 edges, which work on their own in both windows. The context does not add to them.

## Reading
- **The visual Wyckoff skeleton does not pick better trades,** once it is written as a rule loose enough to capture the user's examples.
- **Looser or tighter, the story is the same.** The strict version (V1, 390 trades) was noisy; the loose version (V2, 10.8k trades) is ≈ 0. The pattern is common, and most instances do not become a +100 % run.
- **What the 8 winners share is a known edge set** (ATM, HB15, 🔻💪, QZC, 🎯3, spring) firing in a name that then ran. The sequence around those edges adds nothing measurable.
- **Consistent lessons:** buying the thrust or ignition bar loses in every window (strength-chase law), and HILO↑∧R2X alone is mildly negative (−0.6 pp) in both windows.

Artifacts (session scratchpad `v4hist/`): `spring_lps_v2_plan.txt`, `lps_prep.py`, `lps_frame.parquet`, `lps_census.py`, `lps_census.pkl`, `lps_count.py`, `lps_v2_mask.npy`, `spring_lps_v2.py`, `spring_lps_v2.log`, `spring_lps_v2_trades.csv`. No UI, score or memory change.
