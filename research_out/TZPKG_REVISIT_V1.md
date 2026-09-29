# TZPKG_REVISIT_V1 — the _TZ ANALYTICS 5YR package re-analysed from scratch (Stage 1: atomic claims)

**Date** 2026-09-28 · user: "set the old audit verdict aside; knowledge has grown — re-analyse" · plan frozen to `tzpkg1_plan.txt` before any outcome · k = 15.

**What changed vs. the package's own method:**
- **Entry.** Next open instead of the signal close.
- **Exit.** Book ATR×12 trail path-sim instead of a fixed 10-day median.
- **Estimator.** Same-day within-family contrast (e.g. T bars with a property vs T bars without it, *on the same day*), day-clustered.
- **Pass bar.** Both windows, 4/6 years and 2/3 price buckets required.
- **Universe.** All 5,739 US tickers.

## Stage 1 results (same-day within-family contrast, pp per trade)

| # | package claim | expected | MINE 2021-23 | VERIFY 2024-26 | years | verdict |
|---|---|---|---|---|---|---|
| H1 | close=O (weak close) | + | +0.43 [−0.17, +1.07] | **−0.77** [−1.33, −0.24] | 4/6 | ✗ flips |
| H2 | EO (escape + weak close) | + | **+0.97** [+0.40, +1.67] | +0.43 [−0.35, +1.15] | 5/6 | ✗ decays |
| H3 | gap G3 vs no gap | + | **+0.66** [+0.24, +1.11] | +0.22 [−0.35, +0.82] | 5/6 | ✗ decays |
| H4 | wick D | + | +0.21 | +0.01 | 5/6 | ✗ |
| **H5** | **pen R** (lower wick into the body, low not broken) | − | **−0.56** [−0.86, −0.26] | **−0.56** [−0.89, −0.23] | **6/6** | **✓** |
| **H6** | **range V/C without a gap** | − | **−2.25** [−2.65, −1.84] | **−2.43** [−2.86, −2.05] | **6/6** | **✓** |
| **H7** | **body M better than X** | + | **+0.29** [+0.01, +0.59] | **+0.28** [+0.01, +0.60] | **6/6** | **✓ (small)** |
| H8 | R2L (RSI2 oversold) | + | +0.31 | **−1.25** [−1.92, −0.58] | 2/6 | ✗ flips |
| H9 | PB + R2L | + | **+1.65** [+0.73, +2.58] | +0.15 | 4/6 | ✗ decays |
| H10 | PS + R2H | − | −0.46 [−0.94, 0.00] | −0.42 [−0.89, +0.07] | 5/6 | ✗ (borderline) |
| **H11** | **volume VB** | − | **−3.04** [−3.86, −2.23] | **−3.33** [−4.22, −2.42] | **6/6** | **✓** |
| **H12** | **previous bar wyc_phase ACC_TR** | − | **−9.32** [−10.4, −8.1] | **−13.94** [−15.5, −12.4] | **6/6** | **✓** |
| H13 | Atomic: close=O + gap G2/G3 | + | **+1.09** [+0.54, +1.70] | +0.27 [−0.40, +1.02] | 5/6 | ✗ decays |
| H14 | Z family: close=A | + | −0.07 | +0.10 | 2/6 | ✗ |
| H15 | Z bar beats T bar | + | −0.06 | −0.32 | 1/6 | ✗ |

**PASS 5/15. Every robust pass is an AVOID rule:**
- VB volume, −3 pp;
- a volatile or climactic range with no gap, −2.3 pp;
- an ACC_TR previous bar, −9 to −14 pp;
- pen R, −0.6 pp.

The one positive pass is **body M over X** (+0.3 pp).

## Reading
- **The package's AVOID list is its most durable part.** VB, V/C-without-gap and ACC_TR all hold in both windows, every year and every price bucket, and they are large.
- **Its BUY claims were real in 2021-23 but decayed.** close=O, EO, G3, PB+R2L and Atomic are positive with CI > 0 in MINE, but not significant in 2024-26. close=O and R2L even flip negative. The package used 2021-26 in-sample, which is where the 2021-23 strength came from.
- **The Atomic edge (T + close=O + gap) is still positive in 2024-26** (+0.27), just not significant on this same-day contrast. The app's ⚡ATM uses a stricter definition (price band etc.), so this is not a verdict on the built edge.
- **"Z beats T" does not hold on a same-day basis.**
- **⚠️ ACC_TR is the strongest negative state in the whole stack.** It shows up twice: `wyc_phase` ACC_TR here, and ⚛ `phys_wyc` ACC-TR in PHYS_LINES_V1. Both mean "downtrend + compression", not accumulation.
  - `ultra_score.py` `_REGIME_BONUS` gives **ACC_TR_CONTEXT +4 points**. This rewards the state the data says to avoid. Needs the user's decision; nothing was changed.

## Next stages (not run)
- **1b.** Sign replication of the 5 passers on the 4H / 1H DBs.
- **2.** The composite / sequence databases against a same-size random-set placebo.
- **3.** Z7 (raw) as its own study.

Artifacts (session scratchpad `v4hist/`): `tzpkg1_plan.txt`, `tzpkg1.py`, `tzpkg1.log`, `tzpkg1_result.csv`. No UI, score or memory change.

## Stage 1b — intraday sign replication of the 5 passers (plan `tzpkg1b_plan.txt`)
- **Setup.** T bars on studio_4h / studio_1h. Outcome fwd10 / fwd20 **bars** from the next open. Same-day within-family contrast.

| rule | 4H MINE · VERIFY (fwd10b) | 1H MINE · VERIFY (fwd10b) |
|---|---|---|
| VB volume (−) | +0.01 · **+0.26** | +0.05 · +0.11 (wrong sign) |
| V/C no gap (−) | +0.02 · −0.03 | −0.01 · +0.01 |
| prev ACC_TR (−) | −0.10 · **−0.58** (fwd20b −1.67) | +0.09 · −0.24 |
| pen R (−) | +0.04 · −0.01 | +0.02 · +0.02 |
| body M vs X (+) | **+0.11** · +0.03 | **+0.08 · +0.03 → replicates** |

**Reading.** The 1D rules are **daily-horizon effects** (the 1D test holds a trade for up to 60 sessions). They do not appear over 10-20 intraday bars. Only body M > X, which is tiny, and the ACC_TR direction on 4H carry over.

## Stage 2 — the composite and sequence databases vs placebo (plan `tzpkg2_plan.txt`)
- **Unit test.** The composite or sequence vs bars of the same T/Z signal on the same day, per-bar ATR×12 trade.
- **Pass.** CI > 0 in **both** windows.

| set | units tested (≥ 300 bars) | pass | all units pass rate | placebo 95th pct | p |
|---|---|---|---|---|---|
| **A · 61 STABLE composites** | 24 | **2 (8.3 %)** | 0.8 % of 380 | 4.2 % | **0.011** |
| B · GOOD & STABLE sequences | 56 (of 1,159 listed) | 0 | 0 % of 780 | 0 % | 1.0 |

**Passing composites (MINE → VERIFY, pp over the same signal on the same day):**

| composite | MINE | VERIFY | in the package? |
|---|---|---|---|
| **T5L46ED** | +1.90 | +1.58 | yes |
| **Z2GL46ED** | +1.07 | +0.70 | yes |
| Z9L46NUR | +1.76 | +1.50 | no (not in the package list) |

- **The STABLE composite label carries real information.** 8 % of those units pass against 0.8 % of all composites (p 0.011).
- **All three survivors are L46 composites,** the absorption line, which fits the book's "absorbed weakness" law. Two of them also carry the E(D) suffix.
- **The sequence database yields nothing.** No sequence of any status passes on both windows. Caveat: our sequence key uses consecutive bars; if the package skipped signal-less bars, some keys differ.

## Stage 3 — Z7 (plan `z7_plan.txt`) → **FAIL, and strongly NEGATIVE**
- **Sample.** 20,537 eligible Z7 bars, 18,901 trades. Book ATR×12 trail vs the same-day control.

| | MINE 2021-23 | VERIFY 2024-26 |
|---|---|---|
| mean · win · PF | −2.80 % · 44 % · 0.71 | +0.63 % · 49 % · 1.08 |
| **same-day Δ vs control** | **−1.34** [−1.86, −0.80] · −0.97 / −1.74 / −1.15 | **−1.54** [−2.12, −1.01] · −1.39 / −2.44 / −0.53 |
| Z7 vs other Z bars, same day | **−1.41** [−1.94, −0.91] | **−1.36** [−1.92, −0.84] |
| price buckets | −0.3 / −1.0 / **−5.8** | −1.1 / −1.3 / **−4.8** |
| Z7L5ED (package composite) | +0.28 [−2.94, +3.91] | −2.20 [−5.00, +0.83] |

**Reading.**
- The earlier "raw Z7 +3.76 pp" lead does not survive next-open entry, the book exit and a same-day control.
- **Z7 is a consistent loser.** It is negative in all 6 years, in all 3 price buckets (worst above $89), and against both the market and other Z states.
- The test was pre-registered for the positive direction, so this is a **veto candidate**, not a sealed veto.

## Forward registration + Ultra chips (AMENDMENT_3 of BOTTOM_CLUSTER_V1)
- **Rules B5 T5L46ED, B6 Z2GL46ED, B7 Z9L46NUR** were added to `backend/bottom_cluster_forward.py`.
  - The PASS test is the research claim itself: a same-day **paired** difference vs the same T/Z state without the composite, CI > 0.
  - Read once on or after 2027-03-01.
  - Seen-window check on 2024 (underpowered, no verdict): paired +0.41 / +0.73 / +1.37.
- **Ultra chips** `T5L46ED`, `Z2GL46ED` and `Z9L46NUR` are in the "EDGE today" section. `tz_wlnbb_l_signal` and `tz_wlnbb_full_suffix` were added to the cache keep-list so the chips survive a reload.
- **Z rows need Direction = all.**

---
## ⚠️ CORRECTION 2026-09-29 — the T/Z-increment claims were not ATR-matched
The book exit (trail = 12·ATR%) makes returns volatility-dependent, and the contrasts here did not match ATR%. Re-checked within day × ATR% (22 bins):
- **🔻💪+T9:** vs other T9 +0.63 / +0.98 pp, not significant. Inside 🔻💪, T9 is −1.06 pp in 2024-26. The gain is RS. (T9_RS_V1)
- **🕐DR+Z2G:** 🕐DR lifts Z2G +2.52 / +2.42. Z2G adds nothing to 🕐DR (+1.65 / −0.86).
- **T5L46ED / Z2GL46ED / Z9L46NUR:** +0.77/+0.16 · +0.20/−0.35 · +0.56/+0.59, all ≈ 0. (TZ_BOOSTERS_V1 AMENDMENT_1)

Ultra chip hints and the `bottom_cluster_forward.py` docstring now carry these numbers. The forward rules are not changed.
