# TZL_BOTTOM_SEQ_V1 — multi-bar T/Z·L sequences at bottoms, plus the recent-low re-audit it triggered

**Date** 2026-09-28 · user: "TZL bar sequences, bottom sequences of several bars" · plan frozen to `tzlseq_plan.txt` before any outcome · k = 30.
**Verdict:**
- **As pre-registered, 27/30 PASS.**
- **After a post-hoc audit, 3/27 survive, so the result is effectively NULL.**
- **The same audit overturns the turn-zone lifts of ⟲ROW, TOP·58 and TURN·58.** See the second half.

## Setup
- **Tokens.** Each bar is T/Z state · L-code, e.g. `T2G·L3` or `Z2G·L46`; 187 distinct tokens.
- **Sequences.** Runs of 2, 3 and 4 bars ending at t.
- **Search.** 545,878 sequences evaluated on MINE 2021-23; 116 passed the filters:
  - support ≥ 300 bars and ≥ 100 days;
  - adjusted lift ≥ 1.5;
  - at least +0.2 above the sequence's last token alone.
- **VERIFY.** The top 30 went to VERIFY once.
- **Target and adjustment.** Target a (turn zone), with location × ATR% as in ROWSEQ_V1.

## As pre-registered
27/30 passed, with VERIFY adjusted lifts of 1.5-3.8. The typical winner opens with a low bar and closes on two strong green T bars:
- `Z2G·L46 → T1G·L3 → T2G·L12` (3.38);
- `T9·L12 → T2G·L3 → T2G·L3` (3.76).

## Audit: the target leaks the recent low
- The turn-zone label contains the pivot at t-3..t+3, so part of it is **already in the past at t**.
- VERIFY bars with a 10-bar low in t-3..t turn **22.2 %** of the time. All other bars turn **2.0 %** of the time.
- Location × ATR% does not remove this. The sequences above almost all begin at a low 1-3 bars earlier.

**Re-scored with a third stratum** (10-bar low in t-3..t, yes/no):
- **3 of the 27 keep lift ≥ 1.3 with CI lo > 1:**
  - `T9·L12 → T2G·L3 → T2G·L3`: 1.41 [1.09, 1.74], same-day +2.41;
  - `Z5·L3 → T1G·L3`: 1.50 [1.09, 1.90], same-day **−2.99**;
  - `T9·L12 → T2G·L3 → T2G·L12`: 1.37 [1.11, 1.64], same-day **−2.60**.
- The other 24 fall to 0.7-1.3.
- Three survivors from a post-hoc test at k = 27, two of them with strongly negative same-day returns, is not a finding.

## Re-audit of the published turn-zone gauges (VERIFY 2024-26; location × ATR% → + recent-low stratum)

| gauge | published | with recent-low | gauge | published | with recent-low |
|---|---|---|---|---|---|
| ⟲ROW FLY● | 1.63 | **1.00** | TOP 🕐DR | 1.96 | **1.21** [1.12, 1.29] |
| ⟲ROW GR● | 1.60 | 1.11 | TOP FBO↑ | 1.70 | 1.01 |
| ⟲ROW MTF● | 1.52 | 1.04 | TOP RTV | 1.65 | 0.97 |
| ⟲ROW ⚛● | 1.45 | 0.98 | TOP 🎯3 | 1.50 | 1.11 |
| ⟲ROW VOL7● | 1.43 | **1.11** [1.04, 1.18] | TOP M4·σ6 | 1.37 | 1.11 |
| ⟲ROW PV● / BRK● / OVD● / Δ● | 1.40-1.43 | 0.99-1.06 | TOP gG3+🕐DR | 2.69 | **1.33** [1.11, 1.53] |
| ⟲ROW ROW●≥2 | 1.60 | 1.04 | TOP FLP↑+🕐DR | 2.36 | **1.20** [1.10, 1.30] |
| ⟲ROW ◆V∧M | 1.87 | **1.15** [1.06, 1.26] | other 8 TOP pairs | 1.97-2.39 | 0.99-1.06 |
| TURN·58 ≥ 15 / 20 / 26 (on 10-bar lows) | 1.33 / 1.42 / 1.56* | **1.00 / 0.98 / 0.95** | | | |

\* The TURN·58 lifts were measured against an average 10-bar low with its own turn label, without the location × ATR% adjustment.

## What this changes and what it does not
- **Changes.** The "turn-zone identification" story behind ⟲ROW, TOP·58 and TURN·58 was mostly *"price made a low recently and closed off it"*: location plus volatility plus recency.
  - Beyond that, only the 🕐DR family (1.2-1.33), ◆V∧M (1.15) and a few single features (VOL7, M4·σ6, 🎯3 at ~1.1) keep a small, real increment.
  - The UI tooltips quote the old lifts and are overstated.
- **Does not change.** Every **return-based** result, because none of them uses the turn-zone label:
  - 🕐DR same-day +2.37 / +2.35;
  - EDGE_IN_TURNZONE_V1 (edges in a ⟲ROW≥2 zone earn more per trade, a timing effect);
  - BOTTOM_CLUSTER, NATURE_SHIFT, DEPARTURE.
- **Lesson.** A label that spans t-3..t+3 must be controlled for what is already known at t, including the recent low, not only location and volatility.

Artifacts (session scratchpad `v4hist/`): `tzlseq_plan.txt`, `tzlseq.py`, `tzlseq.log`, `tzlseq_result.json`, `tzlseq_audit.py`, `tzlseq_audit.log`, `reaudit_recentlow.py`, `reaudit_recentlow.log`.
