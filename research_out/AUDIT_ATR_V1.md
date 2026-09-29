# AUDIT_ATR_V1 — every displayed return number re-checked against a volatility-matched control

**Date** 2026-09-29 · user: "ki auditi gavaketot da gavasworot yvela ricxvi" + "zvelis gverdit axali gaakete". Plan frozen to `audit_atr_plan.txt` before any audit outcome.
**Why.** The book exit trail is clip(12·ATR%, 15, 60)%, so a trade's return depends on the name's volatility. None of the controls matched ATR%.
- In within-T/Z contrasts this produced 3-10× inflated effects (TZ_BOOSTERS_V1 audit, T9_RS_V1).
- This audit checks everything else that is displayed.

## VERDICT
1. **Edge book (120 setups): ROBUST.** ATR matching does not deflate the book; on average it raises it slightly.
   - **78 of 81** significant setups stay significant; **16 more become significant**.
   - Mean shift is +0.30 pp (median +0.18); the median of the day-edge medians shifts +0.01.
   - Edge fires sit at ATR% 2.6-5.0 (median 3.2), close to the control's centre, unlike the extreme-ATR T/Z contrasts.
2. **Hints: several numbers were wrong and are corrected.** The **🕐DR family holds**. **EDGE ⟲ROW was wrong** (pooled, not like-for-like). The **QR_REL veto no longer reproduces**. Several TOP·58 same-day numbers shrink to ≈ 0.

## Part 1 — edge book
**Method**
- New field `day_med_edge_atr`, **shown beside** `day edge` in Replay and Research. The old column is untouched.
- **Control:** every 10th bar ≥ $21, per-ticker sha256 phase.
- **Cells:** entry-day × ATR% quintile of the signal bar. Quintile cuts are 2.16 / 2.70 / 3.37 / 4.54 %. A cell needs ≥ 8 control trades.
- **Estimand:** each trade minus its cell median → day median → median over days.
- **Audit frame:** full history (72 months, $3M), 5,226 tickers. The table is in `research_out/AUDIT_ATR_V1_edges.csv`, with raw and matched med / mean / CI, MINE / VERIFY and years for every setup.

**Classes (pre-registered: matched mean vs the sign of the raw day-median)**

| class | count |
|---|---|
| ✅ same sign, CI excludes 0 | 94 |
| ⚠️ same sign, CI crosses 0 | 18 |
| ❌ sign flips or \|Δ\| < 0.10 | 8 |
| no fires (H1-bottom, H1-bottom🌀SC) | 2 |

**The 8 ❌**
- **Five were already ≈ 0 raw and stay ≈ 0:** RTB-Base, RTB-Base🧊CONSO, P55, Spring🌀SC, Washout🧊CONSO.
- **Two are mismatches between the raw median and mean:** Engulf-Abs🏆RS (raw med −0.67, mean +1.47) and ND→SC→L46🕐 (med +1.76, mean −0.14).
- **One is a real flip:** **Washout.** Raw −0.69 [−1.33, −0.06] → matched **+0.64** [−0.00, +1.24]. Washout fires on volatile names (ATR% med 4.0) and was being penalised by an average-volatility control.

**Lost significance (3):** Washout (see above; it turns positive), Atomic🌀SC (+0.52 [−0.04, 1.06]), z1gt36·15mZ (n 45).

**Gained significance (16):** D+L1, G3-gap, Spring, Zone-Retest, QZ-Capit🔑, G3+RL, G3→G3→RL, NS→SC, Washout🔎iv, Washout💥vol, G3-Abs🕯️mid, 🎬StopVol-Confirm, 🎬StopVol-Deep, 👑Z1G-CROWN, gem1_capbounce·🏆RS, z1gcrown·🔑KEY.

**Reading.** Many reversal and washout setups fire on above-median-volatility names. In 2021-26 the same-day high-volatility control did worse than the average control, so the raw column **understated** them.

## Part 2 — displayed hint numbers
**Method.** Per-bar book return vs all eligible bars the same day (raw), and vs the same day × ATR% bin (22 bins; matched).
- For the two VOL ECHO vetoes: the original estimand (path-sim with 5-bar cooldown vs a stride-10 control), raw and ATR-quintile matched.
- **Check:** the raw VERIFY column reproduces every published TOP·58 value exactly, so the only change is the matching.

| item | published | ATR-matched VERIFY | ATR-matched all years | class |
|---|---|---|---|---|
| TOP 🕐DR | +2.39 | +2.38 | +1.86 [1.57, 2.18] | ✅ |
| TOP FLP↑+🕐DR | +3.21 | +3.24 | +2.42 | ✅ |
| TOP gG3+🕐DR | +3.00 | +2.65 | +1.50 | ✅ |
| ◆🕐DR pair chip | +3.00 / +3.21 | +2.86 | +2.10 | ✅ |
| TOP G3 | +0.35 | +0.38 | +0.55 | ✅ |
| TOP gG3+FBO↑ | +0.56 | +0.44 | +0.98 | ✅ |
| TOP V+FBO↑ | −0.71 | −0.85 | −0.39 | ✅ |
| TOP SVS+FBO↑ | −1.34 | −1.44 | −0.70 | ✅ |
| TOP M4·σ6 | +0.43 | +0.04 | +0.24 | ✅ (all years), ≈ 0 in 2024-26 |
| TOP 🎯3 | +0.11 | −0.08 | +0.25 | ✅ (all years), ≈ 0 in 2024-26 |
| TOP G3+FBO↑ | +0.10 | −0.10 | +0.70 | ✅ (all years), ≈ 0 in 2024-26 |
| TOP FBO↑ | −0.66 | −0.70 | −0.16 | ⚠️ |
| TOP ZRT | −0.33 | −0.57 | −0.10 | ⚠️ |
| TOP RTV+gG3 | −1.17 | −0.99 | −0.21 | ⚠️ |
| TOP gG3+ZRT | +0.28 | −0.05 | +0.30 | ⚠️ |
| TOP RTV | −0.87 | −0.55 | +0.12 | ❌ |
| TOP gG3 | −0.36 | −0.08 | +0.29 | ❌ |
| TOP HILO↑ | −0.28 | −0.30 | −0.08 | ❌ |
| TOP FBO↑+P | +0.07 | +0.05 | −0.08 | ❌ |
| TOP HILO↑+gG3 | −0.96 | −0.43 | −0.01 | ❌ |
| TOP SVS | −0.00 | −0.05 | −0.23 | — (claim ≈ 0) |
| **EDGE ⟲ROW** (edge in zone vs edge outside) | +1.69 / +0.97 | **−0.77** [−1.14, −0.42] | −0.36 | ❌ The old number was a POOLED average (zone fires fell on better market days). Like-for-like, zone edges are worse. |
| **⛔QR▲ QR_REL veto** | −1.82 / −1.40 | +0.20 | **−0.01** [−0.99, 0.95] | ❌ Not reproduced on current data; raw is −0.01 too, so ATR is not the cause. |
| Q∧R + RSI > 50 | −0.88 | +0.38 | −0.35 [−0.96, 0.32] | ⚠️ Holds in 2021-23 (−1.18), gone in 2024-26. |

**Out of scope** (stated in the plan):
- turn-zone identification lifts (already ATR%-adjusted);
- 4H-lead +0.84 (a same-bar pair);
- absolute path-sim stats (PF / win / %);
- CISD 5-bar forward;
- SeqRules +0.40;
- JournalBench.

## What changed in the app
- **`backend/edge_replay.py`:**
  - `_pathsim` trades carry `atrp` (additive);
  - new `day_med_edge_atr` / `day_win_edge_atr` / `n_days_atr` (additive; `day_med_edge` unchanged);
  - `MIN_CTRL_ATR = 8`.
  - Tests: `test_edge_control_dephased.py` and `test_pending_replay.py` pass.
- **`EdgeReplayPanel.jsx` / `ResearchPanel.jsx`:** new **"day edge ATR"** column beside "day edge".
- **Ultra hints:**
  - 20 TOP·58 items, ◆🕐DR, EDGE ⟲ROW, ⟲ROW≥2, Q∧R, ⛔QR▲;
  - plus the 5 T/Z chips corrected earlier today.
- **Superchart:** the ⛔QR▲ tooltip, VOL ECHO footer and TOP·58 tooltip label.
- **`topPairs.js`:** `sd` = ATR-matched VERIFY.

Artifacts (scratchpad `v4hist/`): `audit_atr_plan.txt`, `audit_edges.py/.log/.json`, `audit_hints.py/.log/.json`, `audit_veto.py/.log`. No memory entry (user OK needed).
