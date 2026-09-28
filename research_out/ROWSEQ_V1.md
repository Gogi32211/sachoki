# ROWSEQ_V1 — per-row turn sequences, row states and confluence

**Date** 2026-09-28 · user's design ("tviteul zolze … Semobrunebis tanmimdevroba … mere tanxvedrebi"), targets a + b chosen by the user, ⚛ row added by the user.
**Plan** frozen to `rowseq_plan.txt` before any outcome.
- **Universe.** NASDAQ plus Russell-only names: 5,706 tickers, 2.26M eligible bars (close ≥ $5, $vol ≥ $5M).
- **Windows.** MINE 2021-23 (1.04M bars), VERIFY 2024-26 (1.22M bars).
- **Search burden.** k = 44 row tests + 4 confluence tests.

## Verdict
| target | as pre-registered | after the location × volatility audit |
|---|---|---|
| **a — turn zone** (base 9.0 %) | 18/22 rows PASS; COUNT PASS; PAIR PASS | **Real but smaller.** Rows carry turn information beyond price location (adjusted lift 1.12-1.63). COUNT adds nothing over the best row. Only the VOL7∧MTF pair adds, and it is rare (0.4 %). |
| **b — big move** (≥ +30 % under ATR×12 trail, base 7.9 %) | 3/22 rows PASS; COUNT PASS | **NULL.** All of it is volatility. After adjustment every row is ≤ 1.0, and VOL7 (0.74) and VOLEV (0.75) are significantly below 1. |
| c — same-day return | report only | ≈ 0 or negative for almost every rule. Identification does not become a better trade. |

## Audit
This step is post-hoc, disclosed, and **decisive**.

**Why it was needed.** The raw lifts were suspiciously large (1.4-2.2). A plain price benchmark explains most of that:
- **Target a.** "Close within the lowest 5.5 % of distance above the 10-bar low" alone gives **lift 1.95** on target a.
- **Target b.** "Top 5.5 % ATR%" alone gives **lift 2.01** on target b.

**What was done.** Every rule was re-scored against the rate expected from its stratum: 10 location deciles × 10 ATR% deciles, fixed on MINE, evaluated in VERIFY, with a day-clustered CI.

### Target a — adjusted lift, VERIFY

| row | raw | adjusted [95 % CI] |
|---|---|---|
| FLY (✦fresh ABCD/CD/BD/AD @t-0/1) | 1.58 | **1.63 [1.50, 1.77]** |
| GR (G3 / V gaps, G1→G3 …) | 1.38 | **1.60 [1.41, 1.78]** |
| MTF (▲4H / △1H REV, repeated, 1H→4H order) | 2.21 | **1.52 [1.42, 1.62]** |
| PHYS ⚛ (R·D, K1D, S3D, ★/★★, gG2/3, SPRING⚛) | 1.97 | 1.45 [1.33, 1.61] |
| VOL7 · PV · BREAK · OVD · DELTA | 1.42-2.04 | 1.40-1.43 |
| ECHO · VOLEV · ANAT · WICK · WYCKOFF | 1.30-1.96 | 1.24-1.32 |
| LVX · LINE5 · SHAPE · PD · L · GOG · TZ | 1.21-1.98 | 1.12-1.22 |
| **COUNT ≥ 11 passing rows** | 2.36 | 1.51 [1.39, 1.64]: equal to the best row, **no confluence gain** |
| **PAIR VOL7∧MTF** (both confirmed) | 2.70 | **1.88 [1.72, 2.06]**: coverage 0.4 %, same-day return −0.20 [−1.41, +0.96] |

The ranking changes a lot once location is removed:
- **Rows that were mostly "near the low":** T/Z (1.92 → 1.12), WICK and LINE5.
- **Rows that carry information of their own:** FLY, GR gaps, MTF REV triggers, volume (VOL7, VOLEV, DELTA) and PV.

### Target b — adjusted lift, VERIFY

| row | raw | adjusted |
|---|---|---|
| OVD | 1.20 | 0.91 |
| VOL7 | 1.16 | **0.74** |
| PHYS ⚛ | 1.20 | 0.86 |
| COUNT ≥ 3 | 1.37 | 0.85 |
| every other row with a CONF tier | 1.04-1.13 | 0.90-1.03 |

- **Seven rows found no feature at all in MINE** for target b: LBAL, LVX, DELTA, WICK, PD, FLY and GR.
- **Signals do not tell which bar precedes a +30 % run,** beyond "this is a volatile stock".
- **Heavy-volume states predict fewer big moves** than the stock's volatility implies.

## Reading
1. **The user's idea works for identification.** Each row's own 5-bar sequence identifies turn zones better than chance, even after removing the obvious "price is near its low" effect. Order and repetition inside a row matter; for MTF, for example, a 1H trigger followed by a 4H trigger is selected, as is repetition.
2. **Confluence of many rows does not add** beyond the best single row once location is controlled. The rows read the same turn state; this is the same saturation seen in NASDAQ_TURN_RESELECT.
3. **One specific pair does add:** VOL7∧MTF. A volume-structure state and an intraday REV trigger together give adjusted lift 1.88. It fires on 0.4 % of bars, and it still does not give a better same-day return.
4. **Big-move prediction is NULL.** Every "big move" lift is volatility. This is consistent with SPRING_LPS (MFE ≥ 50 % was 2.5× more frequent, but so were stop-outs).
5. **The turn-identification ≠ money gap persists.** Same-day return Δ for the passing rules is about 0. Knowing a bar is likely in a turn zone does not make it a better purchase than the day's other bars.

## Possible use (needs user OK; no change made)
- **A descriptive Superchart row** showing per-row turn tier (0 / early / confirmed) for the audited-strong rows (FLY, GR, MTF, PHYS, VOL7, PV, BREAK, OVD, DELTA), plus a VOL7∧MTF marker. It would be labelled "turn-zone identification, not a buy signal".
- **Not a score and not a ranking input.** Target b and target c both say there is no selection value.

Artifacts (session scratchpad `v4hist/`):

| kind | files |
|---|---|
| plan | `rowseq_plan.txt` |
| build | `rowseq_build.py`, `rowseq_X.npz` (5.07M × 672 keys incl. 45 raw ⚛ tokens), `rowseq_keys.json`, `rowseq_frame.parquet` |
| run | `rowseq_run.py`, `rowseq_run.log`, `rowseq_rows.csv`, `rowseq_features.json`, `rowseq_tiers.npz` |
| audit | `rowseq_audit.py`, `rowseq_audit.log` |

## BUILT — Superchart ⟲ROW display row (2026-09-28, user "ki gaakete")
- **What it is.** `frontend/src/lib/rowSeq.js` plus the frozen `rowSeqSpec.json`: 9 rows, with their features and MINE thresholds exported from the sealed study.
  - Rows shown: FLY, GR, MTF, ⚛, VOL7, PV, BRK, OVD, Δ.
  - Chips: `ROW●` = CONFIRMED, `ROW○` = EARLY, `◆V∧M` = VOL7 and MTF both confirmed.
  - Neutral amber tones; 1d only, placed under TURN·58.
  - The tooltips carry the adjusted lifts and the "identification only, not a buy signal" caveat.
- **Parity.**
  - JS `rowSeqTiers` vs research tiers: **28,512 / 28,512** cells, on 8 tickers × 400 bars of history.
  - Live :8080 AAPL vs research: **1,812 / 1,818 (99.67 %)** over 202 bars. The residue is 2 FLY cells (✦fresh 15-bar-absence window), 3 Δ and 1 MTF.
- **Live-data fixes made for parity** (`backend/main.py`, `api_bar_signals` 1d studio-DB merge):
  1. `bf_sell` was `None` on this endpoint. It is now filled from `bars` only when absent. Its V4 weight is 0, so the score is unchanged.
  2. The live WLNBB BO↓/BX↓/BE↓ fire on **identical dates** and disagree with the nightly `bars` values (AAPL: 10-13 of 300 bars each way). The existing chips keep the live values, untouched. The DB copies ride along as `_db_bo_dn` / `_db_bx_dn` / `_db_be_dn`, and only ⟲ROW reads them.

    This brought BRK from 36 mismatched bars to 0. The live defect itself is **not fixed** and is flagged as a separate task.
- **Not added to the CSV or to Ultra.** V4 and TURN·58 are not in the Superchart CSV either, because the CSV runs on the raw history bars without the catalog merge.

**Follow-up (same day, user OK "ki").** The BO↓/BX↓/BE↓ defect was an alias bug, not a compute bug.
- **Cause.** `studio/ultra_db_scan._UI_KEY_TO_DB_COL` mapped `bo_dn`, `bx_dn` and `be_dn` to `sig_vbo_dn`. Ultra's `_row_to_dict` did not pass the real `bars` columns through, so **Ultra showed VBO↓ in all three chips too**. The Superchart endpoint mirrored the same table.
- **Fix.** Ultra now passes `bo_dn`, `bx_dn` and `be_dn` through, and the table maps them to `sig_bo_dn`, `sig_bx_dn` and `sig_be_dn`.
- **Verification.**
  - Live endpoint vs `bars`: 840/840 (AAPL, AMD, RKLB × 3 fields × 280 post-warm-up bars).
  - Ultra `_row_to_dict` vs `bars`: 2,808/2,808.

## BUILT — Ultra filter chips (2026-09-28, user request)
- **Nightly build.** `backend/turn_rowseq_build.py`, a step in `update_all.sh` after VOL ECHO, taking about 7.5 minutes. It computes the last 15 sessions per ticker for 5,675 tickers.
  - It uses the Superchart's OWN JS: an esbuild bundle of the catalog plus `lib/turnCount.js` and `lib/rowSeq.js`, via `backend/turn_rowseq/`.
  - Output goes to `data/turn_rowseq_signals.parquet`, which `studio/turn_rowseq_store.py` reads and `_enrich_turn_rowseq` applies in `run_ultra_db_scan`.
- **Chips** (Advanced Filters → "TURN · ⟲ROW"):
  - TURN≥20, TURN≥26 and TURN≥30 (only on a 10-bar-low candidate);
  - ◆V∧M, ROW●≥2 and ROW●≥3;
  - per row, `X●` (confirmed) and `X○+` (early or confirmed).
  - CSV columns: turn58_n, rs_text, rs_nconf, rs_pair.
- **4H/1H REV.** These are read from studio_4h/studio_1h with the live Superchart/Ultra rule. `data/mtf_rev_signals.parquet` is a research reconstruction that is NOT rebuilt nightly; it stops at 2026-09-18.
- **Parity.**
  - ⟲ROW vs research tiers: 99.95 % of 80,992 overlapping rows × 9 rows.
  - TURN·58 count vs research: 99.41 %, and 100 % on candidates.
  - AAPL Ultra store vs the live Superchart: 10/10 sessions.
  - Ultra filter: 317 → 53 (ROW●≥2) → 28 (+MTF●).
- **Not in Preview mode.** The hybrid live-bar scan (`preview_scan.py`) reads no nightly store, the same as VOL ECHO, PV and the rest.
