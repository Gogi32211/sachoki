# TURN58_TOP_V1 — the best single signals and pairs among the 58 turn keys

**Date** 2026-09-28 · user request ("sauketeso 10 an signalebis wyvilebi") · plan frozen to `turn58top_plan.txt` before any outcome.
**Verdict: 20/20 PASS**, all 10 singles and all 10 pairs, as a **turn-zone identifier**.
- The ranking of the 58 replicates: Spearman MINE vs VERIFY is **0.93**.
- **Trade value is limited to one family:** only the 🕐DR-based items show a positive same-day return.

## Setup
- **Data.** NASDAQ plus Russell-only names, 2.26M eligible bars (close ≥ $5, $vol ≥ $5M).
- **Target.** ROWSEQ target a, the turn zone: a 21-bar pivot within ±3 bars, then +3 ATR within 20 bars, with the pivot holding.
- **Feature.** The key fired on any of t-2..t, as in TURN·58.
- **Adjusted lift.** Observed rate ÷ the rate expected from its stratum, using 10 location deciles × 10 ATR% deciles. This removes "price is near its low" and "volatile stock".
- **Selection (MINE 2021-23).** 1,711 cells searched:
  - the top 10 singles, each with support ≥ 2,000 bars;
  - the top 10 pairs, each with support ≥ 1,000 bars. A pair counts only if its adjusted lift is ≥ 0.10 above **both** of its members; 577 of 1,653 pairs qualify.
- **VERIFY 2024-26, once, k = 20.** PASS requires all of:
  - adjusted lift ≥ 1.2;
  - day-clustered CI lo > 1;
  - lift > 1 in ≥ 2 of 3 price buckets;
  - for pairs, a VERIFY lift above both members.

## Top-10 singles

| # | signal | MINE adj | VERIFY adj [95 % CI] | raw | same-day return Δ (pp) |
|---|---|---|---|---|---|
| 1 | **🕐DR 1H-confirmed bottom** | 1.66 | **1.96** [1.82, 2.11] | 2.57 | **+2.34** [+1.81, +2.85] |
| 2 | FBO↑ | 1.59 | 1.70 [1.59, 1.81] | 2.40 | −0.67 |
| 3 | RTV | 1.56 | 1.65 [1.54, 1.75] | 2.06 | −0.88 |
| 4 | gG3 (⚛ true gap G3) | 1.48 | 1.49 [1.35, 1.64] | 1.25 | −0.37 |
| 5 | 🎯3 Cluster-Bottom | 1.45 | 1.50 [1.36, 1.62] | 1.94 | +0.08 |
| 6 | ZRT Zone-Retest | 1.43 | 1.39 [1.31, 1.48] | 1.75 | −0.36 |
| 7 | HILO↑ | 1.40 | 1.41 [1.33, 1.49] | 1.76 | −0.28 |
| 8 | M4·σ6 | 1.35 | 1.37 [1.28, 1.46] | 1.36 | **+0.43** [+0.18, +0.66] |
| 9 | SVS | 1.33 | 1.37 [1.29, 1.46] | 0.96 | +0.00 |
| 10 | G3 | 1.33 | 1.28 [1.18, 1.40] | 1.17 | **+0.36** [+0.11, +0.62] |

## Top-10 pairs
Each pair beats both of its members in both windows.

| pair | MINE adj (members) | VERIFY adj [95 % CI] (members) | same-day return Δ (pp) |
|---|---|---|---|
| **gG3 + 🕐DR** | 2.26 (1.48 / 1.66) | **2.69** [2.30, 3.00] (1.49 / 1.96) | **+2.97** [+1.82, +4.18] |
| RTV + gG3 | 2.23 | 2.39 [2.15, 2.61] | −1.14 |
| **FLP↑ + 🕐DR** | 2.03 (1.24 / 1.66) | **2.37** [2.17, 2.58] (1.26 / 1.96) | **+3.30** [+2.45, +4.17] |
| FBO↑ + ANY P | 2.06 | 2.36 [2.20, 2.53] | +0.08 |
| gG3 + FBO↑ | 2.26 | 2.29 [2.06, 2.52] | +0.62 [−0.31, +1.47] |
| V + FBO↑ | 2.05 | 2.19 [2.01, 2.37] | −0.74 |
| SVS + FBO↑ | 2.05 | 2.15 [1.97, 2.31] | −1.35 |
| HILO↑ + gG3 | 2.18 | 2.08 [1.91, 2.26] | −0.94 |
| gG3 + ZRT | 2.19 | 2.05 [1.87, 2.23] | +0.34 |
| G3 + FBO↑ | 2.09 | 1.98 [1.77, 2.17] | +0.14 |

## Reading
- **The information concentrates in a few signals.**
  - 🕐DR, FBO↑, RTV, gG3, 🎯3, ZRT and HILO↑ carry most of the turn information in the 58-key set.
  - The bottom of the list adds nothing once location is removed: plain L34, the A/I combo marks, E0 and T9 sit at ≈ 1.0.
- **Pairs clearly add.** A pair of two top signals reaches an adjusted lift of 2.0-2.7, versus 1.4-2.0 for its members.
  - **gG3 (true gap) is the best partner.** It appears in 5 of the 10 pairs.
  - **FBO↑ is second,** in 5 pairs.
- **Identification ≠ trade, with one exception.**
  - Most items have a same-day return ≈ 0 or negative: FBO↑, RTV, SVS and V pairs are −0.7…−1.4. They mark turn zones in names that do not out-earn the day's other bars.
  - **The 🕐DR family is the exception:** 🕐DR alone +2.34, FLP↑+🕐DR +3.30, gG3+🕐DR +2.97 pp, all with CI > 0.
  - This matches 🕐DR being an already-built, trade-validated edge (the 1H-confirmed bottom), not a new discovery. Its pairs are *candidate* refinements of that edge. They have not been path-sim-tested as setups, and that needs its own pre-registered study.
- **The same-day return is descriptive here and was not a pass criterion.** Treat the positive numbers as leads, not results.

Artifacts (session scratchpad `v4hist/`): `turn58top_plan.txt`, `turn58top.py`, `turn58top.log`, `turn58top_result.json`. No UI, score or memory change.

## Re-run on every US ticker in the app DB (same frozen plan, user request "all us stocks")
- **What was added.** The 33 S&P-only names that were in neither the NASDAQ nor the Russell sets:
  - live names: WEC, WELL, WMB, YUM, ZBH, XYL, WAT, MMC, HOLX, …
  - delisted names: ATVI, PXD, DFS, …
- **Coverage.** This makes **5,739 tickers**, all non-index tickers in `studio_analytics.duckdb`, and 2.29M eligible bars.
- **Result.** It is unchanged: the same 10 singles and the same 10 pairs, **20/20 PASS**, Spearman 0.929.
  - 🕐DR: 1.96 [1.82, 2.11], same-day +2.39.
  - gG3+🕐DR: 2.69, same-day +3.00.
  - FLP↑+🕐DR: 2.36, same-day +3.21.
- **Scope note.** US stocks outside the app DB (NYSE names in no tracked universe) carry no signal history, so they cannot be tested without first ingesting them into the nightly pipeline.

Artifacts: `build_rows_sp33.py`, `fired_sp33.ndjson`, `lps_prep_all.py`, `rowseq_build_all.py`, `turn58top_all.py`, `turn58top_all.log`, `turn58top_all_result.json`.

## BUILT — Superchart TOP·58 row + Ultra chips (2026-09-28, user "damimate eseni oriveshi")
- **Library.** `frontend/src/lib/topPairs.js`, frozen from this study: the 10 singles and 10 pairs, each on if it fired in t-2..t.
- **Superchart.** The TOP·58 row sits under ⟲ROW, 1d only. Pairs are shown first, then singles.
  - The 🕐DR pairs get the brighter chip, because they are the only items with a positive same-day return.
  - Tooltips carry the lift, the same-day Δ and the "descriptive, not path-sim-tested" caveat.
- **Ultra.** The "TOP·58" chips:
  - `◆🕐DR pair` and `TOP pair`;
  - the 10 pair chips;
  - the 10 `·3b` single chips (3-bar window, unlike the same-day catalog chips);
  - CSV columns `top58` and `top_dr_pair`.
  - They are fed by the same nightly `turn_rowseq_build.py`, which now also emits `top_s_*` / `top_p_*`.
- **Parity.**
  - JS vs an independent Python implementation: 3,184/3,184 bars.
  - Nightly store vs the research matrix: 99.994 % over 81,217 rows.
  - AAPL Superchart vs Ultra store: 15/15 sessions.
  - Ultra filter on S&P: 317 → 27 (TOP pair) → 7 (◆🕐DR pair).
