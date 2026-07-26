# Sachoki — Session Handoff (2026-05-31) → signals research + scoring rebuild + DB-chart codes

> Paste into a new chat:
> `წაიკითხე ეს ფაილი და გააგრძელე იქედან: /Users/sachoki/Desktop/sachoki-desktop/SESSION_NOTES_260531_SIGNALS_SCORING.md`
>
> Continues from `SESSION_NOTES_260530_DB_REBUILD.md` (that DB-rebuild task is **DONE**).
> User writes Georgian in Latin transliteration; reply same, technical terms + signal codes in English.

---

## 0. CURRENT STATE (verified at handoff)
- App = FastAPI (`backend/`, uvicorn :8080, serves built React from `backend/static/`) + DuckDB at
  `/Users/sachoki/Downloads/studio_analytics.duckdb` (8,235,167 rows, 5228 tickers, → 2026-05-29).
- **ONE clean uvicorn running.** Branch `feat/ultra-parquet-inmem`. All this session's work is **committed**
  (30c858a → 15a55d2). Pre-existing uncommitted files (ARCHITECTURE.md, main.py, turbo_engine.py, etc.)
  are from BEFORE this session — leave them unless asked.
- Frontend dev: `cd frontend && npm run dev` (Vite :5173, proxies /api→:8080). After source edits the user
  views **production** at :8080, so **rebuild + sync**: `npm run build && rsync -a --delete dist/ ../backend/static/`
  (backend/static is gitignored — only commit source). Then user hard-reloads (Cmd+Shift+R).
- Restart server (the OLD uvicorn often ignores SIGTERM and lingers in state R → needs `kill -9`; no DB write
  is in progress normally so -9 is safe): kill all `uvicorn main:app` PIDs, wait port free, start one, confirm
  exactly ONE. Readiness: `curl -s --retry 40 --retry-delay 1 --retry-connrefused --max-time 3 http://127.0.0.1:8080/api/studio/stats`.

---

## 1. THE BIG RESEARCH FINDING (drives everything below)
Rigorous OOS analysis (train <2025-06-01, test ≥2025-06-01, nasdaq + sp500 cross-check) proved:
- **turbo_score / beta_score / ultra_score are ANTI-predictive.** Higher score → LOWER forward breakout rate
  (turbo OOS decile monotonicity **r = −0.60**) and ~2× LOWER MFE. They reward "many bullish signals fired" =
  already extended. corr(turbo,beta)=0.76 → the "3 independent scores" are redundant.
- **Breakouts are preceded by WEAKNESS, not strength:** of 235 boolean signals, only ~5 survive OOS; the best
  are `sig_bias_dn`(1.29x), `rsi_le_35`(1.28x), `pb_stop_cause`(1.32x), `wyc_in_tr`(1.23x); bearish Z2/Z2G beat
  bullish T-codes. **Penalties:** `sig_conso`(0.66x, z=−105 — the turbo "backbone"!), `para_*`, `price_gt_200`,
  `sig_bias_up`(0.35x). Volume spikes (vol_5x/10x/20x) predict EXPLOSIVE pops (1.6-1.9x) but not hold.
- **LOOK-AHEAD TRAP:** `is_pivot_low/high_*`, `next_pivot_*`, swing fields need future bars — they top raw-lift
  charts but are leakage. Excluded everywhere.
- **No money-printer exists.** Exhaustive realized backtests (entry next open, target/stop/time, no pyramiding,
  cost): best is **L46EBO on liquid names PF 1.14** (marginal). Everything else ≈ breakeven-to-negative after
  0.5% cost. Edges are real but marginal (PF~1.06-1.14); only survive with low cost + asymmetric exit. **The
  value is AVOIDANCE (don't buy extended/high-score) + watchlist bias, not a mechanical system.**

---

## 2. WHAT WAS BUILT THIS SESSION (all committed)

### A. DB divergence rebuild (finished the 260530 handoff) — `30c858a`
- `incremental_delta.refetch_from` param (overwrite stale trailing bars; the daily `date>last_date` filter
  skipped them). Ran full re-fetch over 5228 tickers → ALL-PAIR price divergence 2686→**0**, signal div→0,
  100% enriched. `backend/refetch_divergent.py` is the driver (overwrite-on-success, zero data-loss).

### B. backtest-expert skill → Studio analytics
- `backend/studio/eval_sequence.py` (`a1f1429`): scores a seq_lab sequence with the skill's gates
  (significance / Bonferroni / expectancy / cost). `seq_lab` now returns `n_candidates` + per-row
  `avg_win/avg_loss/dd_p5`. CLI: `python -m studio.eval_sequence --universe nasdaq --rank 0 --cost 0.5`.
- **Seq Lab UI verdict chip** (`b7f4065`): `/api/studio/seq-lab?evaluate=true&cost=X` annotates each row with
  Deploy/Refine/Abandon (`eval_sequence.annotate_seq_lab`). Finding: 4-bar T/Z color seqs barely beat baseline
  → cost gate forces Abandon (no tradeable edge).

### C. prebreak_v2 — the new data-derived score — `91ab727` + `3f20708`
- `backend/prebreak_v2.py`: logistic model (numpy, no sklearn) fit on 2.31M nasdaq bars, baked weights.
  Predicts BREAKOUT = `mfe_20d>=20 AND fwd_10d>=0`. **OOS validated: nasdaq top-decile lift 1.47x, monotonicity
  r=+0.94; sp500 cross-check 1.84x (NOT overfit).** Reward=accumulation/volume/weakness; biggest penalty
  `sig_conso −0.84`. Output 0..~45 (calibrated prob ×100) + band: **WATCH<15 / BUY 15-27 / HOT>27** (HOT =
  highest breakout% but NEGATIVE mean fwd = lottery/overbought — the user's "mid=buy, extreme=overbought"
  intuition, derived from data). `apply_prebreak_v2()` = vectorised SQL UPDATE; backfilled all 8.2M;
  recomputed per-universe after enrich in incremental_delta. Cols `prebreak_v2` (SMALLINT) + `prebreak_v2_band`.
- **Wired ALONGSIDE legacy (not replacing):** Exit Hunter "v2" column (`3f20708`), Ultra screener sortable "V2"
  column after BETA (`1f3a4e5`/`4f82a93`), Superchart per-bar "v2" row (`0860558`). e.g. AAPL Score 50 but
  V2=9 WATCH (extended); v2 disagrees exactly where legacy chases strength.

### D. Exit Hunter fixes
- Dedupe to one row per ticker by universe priority (`e670b88`) — was 105 rows / 58 tickers (multi-universe dupes).
- Dropped redundant "Lead" column (`4247ffe`) — aes_leading == aes_score in 100% of 8.2M bars (enricher
  `lift_close` collapses leading→main). aes_leading still in DB (harmless), just hidden in UI.

### E. DB Chart — full per-bar codes (TradingView-style) — `2b01487`+`683d068`+`1a9f34e`+`b37ed0b`+`5e802c4`+`68da9d5`
- `frontend/src/components/DbCandleChart.jsx`: custom HTML overlay positioned via lightweight-charts coordinate
  API (markers are single-line; overlay stacks the full code on every bar). 6 lines, all WHITE, 12px, no arrows,
  hover-tooltip removed. Lines: **1** TZ+L · **2** suffix · **3** body/wick · **4** gap/range · **5** line5
  (VIX/PSAR/RSI2) · **6** vol_bucket (W/L/N/B/VB). Bars with no T/Z (≈4%) still labelled from the L line. "codes"
  toggle. Re-positions on pan/zoom/resize.

### F. Volume bucket made first-class — `5e802c4` + `009ee72` + `15a55d2`
- **What the codes mean** (confirmed from `wlnbb_engine.py` + `enricher.py`): bar COLOR (W/L/N/B/VB)=volume
  MAGNITUDE vs 20-bar BB; code "L"-digit (L1-L6)=volume DIRECTION×price; Line4 gap/range = PRICE gap+range in
  ATR (V=Volatile wide-bar, N=normal, C=contracted — NOT volume); Line5 = VIX-Fix/PSAR/RSI2.
- DB Chart 6th line = vol_bucket (above).
- **Exact Sequence search line7 = volume** (`5e802c4`): `query_exact_sequence` matches `vol_bucket` (short `vb`,
  wildcard `*`); bar dict gains `vol`, builder has a "volume" input per bar + "L7 — volume" toggle. Main
  composite untouched. Verified Z2+VB=7118 vs Z2+W=19867.
- **`bars.composite_vol`** column (`009ee72`): `composite·vol_bucket` e.g. `Z9L25NRI·N` / `Z2L46NBO·VB`. SQL in
  `studio/composite_vol.py`, backfilled 8.2M (0.3s, 304k empty), recomputed in incremental, returned by
  `/api/studio/bars/{ticker}`. Search it directly: `composite_vol LIKE 'Z9L25NRI%N'` (== exact-seq fields, both 954).
- **Ultra Advanced-Filters volume chips fixed** (`15a55d2`): VB/B/N chips read `tz_wlnbb_volume_bucket` (Live
  only) → silently dead in DB-instant; now read `(tz_wlnbb_volume_bucket || vol_bucket)` and **added W & L**.

### G. Misc UI: Superchart bigger fonts+wider cols (`4c2db6b`); sidebar 232→168px (fits "Pump Research").

---

## 3. KEY CODES REFERENCE (the 6-line / composite system)
- composite (Sequence-Builder match) = `base + L + suffix` = `(t_sig|z_sig) || l_sig || composite_full_suffix`.
- **Line1 TZ**: T1-T12 (bull) / Z1-Z12,Z7(doji),Z8 (bear) — priority-engine candle types.
- **Line1 L (L1-L6)**: vol↑/↓ × close (L3=vol↑+close↑, L5=vol↓+close↓…).
- **Line2 suffix** = NE(N inside / E beyond prev range) + wick(U/D/B) + pen(P/R/H) + close(A/O/I).
- **Line3 body/wick** = body(X big≥1.5×/S/M small≤0.5×) + wick(J doji/TB/BB/F flat).
- **Line4 gap/range** = gap(G1<0.2ATR/G2/G3>0.5ATR) + range(V>1.5ATR wide / N / C<0.5ATR narrow).
- **Line5** = VX(WVF VIX-fix) / VR(VIX range) / PB(PSAR bull) / PS(PSAR bear) / R2H,R2L,R2X,R2D (RSI2 state).
- **vol_bucket** (bar colour, Line6): W vol<lower / L <mid / N <upper / B <upper+mid / VB ≥upper+mid (20-bar,1σ).
- ⚠️ Same letters collide: "N" = vol bucket AND range-class; "L"/"B" = vol bucket AND code letters. Different meanings.

---

## 4. NEW SEQUENCE SIGNALS (260519 Pine) — user is iterating these
User has TradingView Pine indicators `260519_SEQ_TESTER` (BUY) / `_S` (SELL) that match the `composite` string
against up to 15 fields (`*`=any, `^`=starts, `$`=ends → translates to SQL LIKE). **Their hand-picked patterns
were FALSIFIED OOS** (BUY T-based → no edge; SELL Z-based → INVERTED, precede bounces UP). **Data-mined
replacements** (OOS-stable, precede +7-40% within 10d): `Z2GL5NDO`(1.9x), `Z2L5NDO`, `T4L34NBI`(fwd+3.4),
`T5L25NHO`(fwd+2.2), `Z3L25NPI`(n=11k), `L46EBO` (only one net-positive in realized backtest, PF 1.06-1.14
liquid). Realized examples (verify on TV): EUDA 2026-05-06 +40/+37%, AIP 2025-11-20 +40/+39%; but RETO −18% close
(MFE real, must exit at peak). The big MOVES are in volatile microcaps where COST eats the edge.

---

## 5. POSSIBLE NEXT STEPS (only if user asks)
1. **TradingView Pine**: add vol_bucket to the Pine `composite` (so volume is searchable in TV too) — give a
   snippet that APPENDS it (don't break existing `$` ends-with patterns).
2. **Bake the gates into Playbook** (like Seq Lab) — Playbook already has expectancy/PF/time-split; add Bonferroni
   + cost + a verdict chip. (Seq Lab done; Playbook + realised seq_backtest panel still TODO.)
3. **Refine L46EBO**: liquid-only universe + trailing-stop + walk-forward to see if PF 1.14 → 1.3 (incremental, not
   a printer). Or pursue the **AVOIDANCE** angle (v2 WATCH/extended = don't buy) as defensive value.
4. **prebreak_v2 v3**: per-universe refit, or an explosive-pop variant weighted on volume spikes; add a UI
   verdict/explanation. Currently nasdaq-trained (generalises to sp500).
5. Combined-input search box (type `Z9L25NRI·N` / `Z9%·VB` directly vs composite_vol) instead of 7 separate fields.

---

## 6. KEY FILES / OPS
- DB: `/Users/sachoki/Downloads/studio_analytics.duckdb` (backups `.bak_260530`, `.crashbak` — verified, can delete;
  user said keep). New cols: `prebreak_v2`, `prebreak_v2_band`, `composite_vol`.
- Scoring: `backend/prebreak_v2.py` (baked weights + `apply_*`/SQL), `studio/composite_vol.py`,
  `studio/eval_sequence.py`, `canonical_scoring_engine.py`/`turbo_engine.py` (legacy turbo, **anti-predictive**),
  `ultra_score.py`/`beta_engine.py` (legacy), `wlnbb_engine.py`+`studio/enricher.py` (all the bar codes).
- Search: `studio/signal_stats.py::query_exact_sequence` (line1-7), endpoint `/api/studio/exact-sequence`.
- Frontend: `DbCandleChart.jsx`, `ScannerDataGrid.jsx` (Ultra V2 col), `UltraScanPanel.jsx` (vol chips, SIG_GROUPS),
  `StudioPanel.jsx` (Exact Sequence builder = `ExactSequenceTab`, Seq Lab = `SeqLabTab`, Exit Hunter = `ExitHunterTab`).
- The backtest-expert skill lives at `~/.claude/skills/backtest-expert/` (eval, 57 tests).
- Massive = correct settled data (=TradingView). yfinance OFF (thread-unsafe, hung dashboard). VIX disabled.
- After ANY backend edit needing exclusive DB write (backfill): stop server first. Tests: `cd backend &&
  .venv/bin/python -m pytest ../tests -q` (13 pre-existing baseline failures unrelated).

## 7. STYLE / CONTEXT
- User values getting it DONE + honesty over hype; has been burned by overfit "mirages" → ALWAYS validate OOS,
  significance + cost gates, flag look-ahead. Don't oversell marginal edges as money-printers.
- Reply Georgian-Latin; signal codes stay Latin (T2G, Z4, L46, VB…).
