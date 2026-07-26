# Sachoki — Session Handoff (2026-06-01) → unified chart + Ultra filter cleanup + Seq Lab modes + Wyckoff 260529

> Paste into a new chat:
> `წაიკითხე ეს ფაილი და გააგრძელე იქედან: /Users/sachoki/Desktop/sachoki-desktop/SESSION_NOTES_260601_CHARTS_FILTERS_WYCKOFF.md`
>
> Continues from `SESSION_NOTES_260531_SIGNALS_SCORING.md`.
> User writes Georgian in Latin transliteration; reply same, technical terms + signal codes in English.

---

## 0. CURRENT STATE (verified at handoff)
- App = FastAPI (`backend/`, uvicorn :8080, serves built React from `backend/static/`) + DuckDB at
  `/Users/sachoki/Downloads/studio_analytics.duckdb` (8,235,167 rows, 5228 tickers, → 2026-05-29).
- **ONE clean uvicorn running.** Branch `feat/ultra-parquet-inmem`. All this session's work **committed**
  (`bf31664` → `384e8bc`, 19 commits).
- Frontend: after source edits → `cd frontend && npm run build && rsync -a --delete dist/ ../backend/static/`
  (backend/static gitignored — commit source only). User views production at :8080 (Cmd+Shift+R).
- Restart server (old uvicorn lingers → needs `kill -9` AND `lsof -ti :8080 | xargs kill -9`): kill, wait port
  free, start ONE, confirm. Readiness: `curl -s --retry 40 --retry-delay 1 --retry-connrefused --max-time 3
  http://127.0.0.1:8080/api/studio/stats`.
- ⚠️ Backend edits needing exclusive DB write (backfill) → **stop server first** (DuckDB single-writer; with a
  RW connection open no other process can even open the file).

---

## 1. ⚠️ DB STATUS — was the whole base re-scanned? **NO — only Wyckoff was backfilled.**
- This session did a **targeted Wyckoff backfill** (`backfill_wyckoff.py`): computed ONLY the new
  `w2_*` / `wt_*` columns across all 8.23M rows (~3.5 min) and UPDATE-d just those. **Everything else
  (TZ / L / suffix / body-wick / gap-range / line5 / prebreak / etc.) was NOT recomputed** — it was already
  correct from prior enrichment.
- The `wlnbb_engine.py` L-logic fix (commit `1f0ae4a`) was to the **LIVE** engine (`/api/signals`, hover
  popups, live ultra scan) only. The **DB enricher** uses `analyzers/tz_wlnbb/signal_logic.py`, which was
  already faithful to Pine — so the DB's L-codes did NOT need a re-enrich.
- **A true full re-enrich was NOT run.** If desired it's a separate ~2-3h job: `enrich_universe(uni)` per
  universe (nasdaq/sp500/russell2k) — server stopped — or the UI "Update DB" button. It would recompute every
  column (idempotent for unchanged ones) and is only needed if the enricher logic itself changed (it didn't,
  beyond wiring Wyckoff which the backfill already covered).

---

## 2. WHAT WAS BUILT (all committed)

### A. Unified chart component — `bf31664`
- New `frontend/src/components/CodeCandleChart.jsx` replaces BOTH old `DbCandleChart.jsx` + `CandleChart.jsx`
  (deleted). **Hybrid source:** `tf==='1d'` → Studio DB (`studioBars`) + full white 6-line code overlay;
  intraday/weekly → live `/api/signals`. Falls back to live if a 1d ticker isn't in the DB.
- Used everywhere: global top chart (App.jsx), Ultra+Turbo hover popups (`bare` mode), DB Chart tab,
  Superchart (embedded above the matrix; global chart hidden on superchart tab via `NO_CHART_TABS`).
- Props: `showToolbar / showBarSelector / showFooter / showSector / interactive / bare / codes / onChartReady`.
- Fix `de8e7b9`: live feed now renders **white codes** (TZ·L·vol) instead of arrow markers — white everywhere,
  no arrows. (Full 6-line suffix/body/gap/line5 are DB-only; live shows TZ+L+vol.)

### B. Chart/screener UX
- `88a9cfe`: hover preview popup → `codes={false}` (clean candles, no labels).
- `41f3d5f`: row click in screener = select ticker only (top chart switches); **no more inline expand panel**.
- `c497b27` → `c52055a`: clicking a row shows a **pinned strip above the chart** = an *exact* one-row
  `ScannerDataGrid` (`pinned` mode, header hidden) — identical columns/colors to the grid row, so signals +
  chart are visible together. Grid meta passed up via `onSelectTicker(ticker, row, meta)`.

### C. B1–B11 retired from DISPLAY — `5bb78e8`
- Per user: B-signals never show as chips anywhere (ScannerDataGrid `collectSignals`, Turbo filter+badges,
  PersonalWatchlist, Superchart matrix row, SignalCorrel, Studio stats, HowItWorks docs). **Backend compute
  kept** (feeds CA/CD/CW combos). CSV export columns kept (data).

### D. Seq Lab — new modes + a real bug fix (`backend/studio/seq_lab.py` + StudioPanel SeqLabTab)
- `c95451d` **BUG FIX**: dedupe by `(ticker,date)` — a ticker lives in several universe rows (AAPL in
  sp500+nasdaq+russell2k); without the dedup, `universe='all'` produced impossible repeated-bar sequences
  (e.g. `T5L5·L|T5L5·L|T5L5·L`) and inflated baseline. Now `QUALIFY ROW_NUMBER() OVER (PARTITION BY
  ticker,date ORDER BY universe)=1` in baseline + CTE. **TZ logic note** (user-taught): T2/T2G/T6/T10/T11/T12
  (require prev-bull) CAN legitimately repeat; T1/T1G/T3/T4/T5/T9 (require prev-bear) CANNOT — verified the
  deduped data matches exactly.
- New MODES (dropdown): `lsig` (L1-L6 / l_sig codes), `vol` (W/L/N/B/VB buckets), `combo` (TZ+L+·vol e.g.
  `Z9L25·L`), `swing` (HL/LL/HH/LH pivots), `wyckoff` (SC/AR/ST/SPR/SOS/JAC/LPS) — see §F.
- `d0cdb53` **leak-free `confirm_lag`** for swing mode: enter N bars AFTER the pivot confirms (Williams 3-3 =
  known 3 bars late), measured on a FIXED horizon. **Critical look-ahead lesson:** `swing` mode + `swing→pivot`
  horizon = double leakage → 97% win MIRAGE (swing_type uses future bars; next-pivot return is the move
  itself). Honest version (confirm_lag=3, +10d): `LL>HH>LH` ≈ 46% win, median −0.4% → **no edge** (it's a
  bearish/avoid structure). Bullish edge ends in **HL** (e.g. `LH|HL|HL|HL` ~70% win / +5% fwd10d, Deploy).

### E. Ultra Advanced-Filters — bar-line order + fix misaligned/dead L chips
- `d8c31eb`: reordered the bar-code chip block to read top-to-bottom like the bar, with group labels
  (`L1 L code` → `L2 suffix` → `L3 body·wick` → `L4 gap·range` → `L5 vix·psar·rsi2` → `L6 vol`). Filled the
  gaps: pen **P/R/H** + close **A/O/I** (line2, via newly-exposed `tz_wlnbb_full_suffix` in `ultra_db_scan`),
  **PS** + **R2D** (line5).
- `480ab85`: L2 suffix as the full 11-letter alphabet **N·E·U·D·B·P·R·H·A·O·I** (wick shown as letters U/D/B,
  not WK↑/↓; split old "NE" chip into N + E). DB-verified: those are exactly the suffix letters.
- `1f0ae4a` **L-logic + L-filter fix** (two real bugs the user caught vs the 260523 Pine):
  1. `wlnbb_engine.py` (LIVE) used a NAIVE adapted volume (`v>v[1]`); restored Pine's bucket-aware
     `volUpAdapted = bucketUp OR (sameBucket AND raw-up)`, fixed `L64` (missing `close<open`), and BB stdev →
     population (ddof=0). vol_bucket now agrees 249/250 with DB. (DB engine was already correct.)
  2. Ultra L-filter chips matched the wrong vocabulary. DB `l_sig` = ascending-digit code (**L12, L46, L25,
     L34, L3, L5, L2, L4** are the only values). So old `L43/L64/L22/L1L2/FRI34` chips matched **ZERO** rows;
     `L12/L46/L25` (the 3 most common) had no chip. Now L1-L6 = digit-PRESENCE, combos = exact `L34`/`L46`.
- `8cffceb`: per user, L group = L1-L6 + **L34**(=L3+L4) + **L46**(=L4+L6) only (dropped L12/L25 chips).
- `fe321da`: dropped the two genuinely dead legacy chips **L64** (no DB column) + **L2L4** (`sig_l2l4` always 0).

### F. Wyckoff 260529 — TWO engines, full DB integration — `da4597b` + `384e8bc`
- User shared TWO Pine v6 scripts; built BOTH:
  - **`backend/wyckoff_v2_engine.py`** (`w2_*`): "V2 Soft" Accumulation **state machine**
    SC→AR→ST→Spring→SOS/JAC→LPS + EVR + `w2_tr_quality` (SC-anchored cycle; `w2_accum`/`w2_break`/`w2_state`).
  - **`backend/wyckoff_trig_engine.py`** (`wt_*`): **structure triggers** (Spring/LPS/SOS/EVR) on a valid TR —
    faithful port of `WyckoffTradingAgent core/wyckoff_v2_structure.py` (the python source is NOT in the repo).
    `wt_valid_tr`, `wt_quality`, `wt_support`, `wt_resistance`.
  - Both **leak-free** (confirmed pivots, fire on the confirmation bar).
- Wired into `studio/enricher.py` (`_compute_wyckoff_structure` + ENRICH_COLUMNS); columns added in
  `studio/db.py ensure_schema`; **backfilled all 8.23M rows** (`backfill_wyckoff.py`).
- Surfaced in all 3 UIs (`384e8bc`): **Ultra filters** (groups "Wyckoff cycle" ACC/BRK/SC/AR/ST/SPR/SOS/JAC/
  LPS/EVR + "Wyckoff trig" TR/tSPR/tSOS/tLPS/tEVR; `ultra_db_scan` passes w2/wt through; cache → `v4.0`),
  **Seq Lab** `mode=wyckoff`, **DB Chart** `❖<stage>` overlay line (studio bars return `wyc_stage`/`wt_stage`).
- DB rates (deduped 4.97M): w2 SC 0.69% / Spring 0.22% / SOS 0.35% / JAC 0.34% / LPS 0.09% / accum 44.8%;
  wt valid_TR 49.3% / Spring 0.71% / SOS 0.18% / LPS 12.8% / EVR 1.9%. Seq Lab: `SOS|JAC|SC` win 57% / +2.7%
  → Refine.
- **Wyckoff landscape (3 implementations):** `260225` = `wyckoff_engine.py` (older state machine, **UNWIRED**,
  not in DB); `260523` = the macro `wyc_phase`/`wyc_spring`/`wyc_sos`/`wyc_in_tr`/`wyc_sow` in the DB
  (wyc_spring 0.04% + wyc_sos = AD-FRESH-based, near-vestigial; **wyc_sow = 0 → dead**); `260529` = the two new
  engines above (the good ones). The new w2/wt do NOT overlap the old wyc_* (zero same-bar overlap measured).

---

## 3. DEAD / KNOWN-ISSUE SIGNALS (audited this session)
- **Removed** (truly dead, 0 matches anywhere): B-chips from display; L64, L2L4 (legacy L group); L43/L64/L22/
  L1L2/FRI34 (old `_wl_*` string chips).
- **Still present, FLAGGED dead — not yet removed:** `wyc_sow` (the Ultra "WYC+: SOW" chip) — `wyc_sow`=0 across
  the whole DB. Candidate for removal.
- **NOT dead — live-only (work in `Live (slow)` mode, absent in DB-instant):** RGTI (LL/UP/↑↑/↑↑↑/ORG/GRN/GC),
  GOG (gog_sig/g2p/g3p/g3l/g2c/g3c/★GOG+), SMX, A/SM/N/MX, CTX (LDS/LDC/LDP/LRC/LRP/WRC/SQB/BCT), RS/RS+, CISD
  wicks (X1/X2/X3/X1G/X2G), 2809 (ATR↑/BB↑/UM/RTV), TZ→2/W/Z12, yf. These have no Studio-DB column (computed by
  `turbo_engine.py`/`gog_engine.py` in the live path). NOT removed. **Open offer:** dim/hide live-only chips
  when Source=DB-instant so the user isn't confused (NOT done).

---

## 4. KEY FILES / OPS
- Charts: `frontend/src/components/CodeCandleChart.jsx` (the one unified chart), `ScannerDataGrid.jsx`
  (`pinned` single-row mode + `collectSignals` export), `UltraScanPanel.jsx` / `TurboScanPanel.jsx` (popups +
  `SIG_GROUPS` filters, cache `_CACHE_VERSION='260523_v4.0'`), `App.jsx` (`selectedRow`/`selectedMeta` strip).
- Wyckoff: `backend/wyckoff_v2_engine.py`, `backend/wyckoff_trig_engine.py`, `backend/backfill_wyckoff.py`
  (driver — run with server stopped). Older unwired: `backend/wyckoff_engine.py` (260225).
- Engines/DB: `wlnbb_engine.py` (LIVE L-logic — now Pine-faithful), `analyzers/tz_wlnbb/signal_logic.py` (the
  DB enrichment engine — authoritative, already correct), `studio/enricher.py` (`enrich_ticker_df` +
  `ENRICH_COLUMNS` + `_compute_wyckoff_structure`), `studio/db.py` (`ensure_schema` column lists),
  `studio/ultra_db_scan.py` (`_PASSTHROUGH_SIGNAL_COLS` + float emit + `tz_wlnbb_full_suffix`),
  `studio/seq_lab.py` (modes color/signal/lsig/vol/combo/swing/wyckoff + `confirm_lag`), `studio_api.py`
  (`/bars/{ticker}` returns `wyc_stage`/`wt_stage`; `/seq-lab` has `confirm_lag`).
- New DB columns added this session: `w2_sc/ar/st/spring/sos/jac/lps/evr/accum/break/state/tr_quality`,
  `wt_valid_tr/sos/spring/lps/evr/quality/support/resistance`, plus `tz_wlnbb_full_suffix` is scan-only (alias).
- Tests: `cd backend && .venv/bin/python -m pytest ../tests -q` (pre-existing baseline failures unrelated).

---

## 5. POSSIBLE NEXT STEPS (only if user asks)
1. **Remove the dead `wyc_sow` chip** (Ultra "WYC+: SOW") — 0 everywhere.
2. **Dim/hide live-only chips in DB-instant mode** (RGTI/GOG/SMX/CTX/RS/CISD/2809…) so DB-instant shows only
   functional filters; restore in Live mode.
3. **Wyckoff research:** Seq Lab `mode=wyckoff` surfaced `SOS|JAC|SC` (Refine, +2.7%). Validate OOS / realised
   backtest before trusting. Compare w2 (cycle) vs wt (trigger) Spring/SOS forward edge.
4. **Full DB re-enrich** if the enricher logic ever changes (not needed now) — or wire `wyckoff_engine.py`
   (260225) too if a third view is wanted.
5. Optionally surface w2/wt in the SelectedTickerBar strip / `collectSignals` Signals column (currently filters
   + DB-chart marker only).

---

## 6. STYLE / CONTEXT
- User values getting it DONE + honesty over hype; burned by overfit "mirages" → ALWAYS validate OOS,
  significance + cost gates, FLAG LOOK-AHEAD (swing-pivot/next-pivot leakage was a live example this session).
- The user catches real bugs (naive adapted volume, dead chips, mislabeled SOS, l_sig-vs-chip mismatch) —
  verify claims against the DB before agreeing/building. "discover, don't impose."
- Reply Georgian-Latin; signal codes stay Latin (T2G, Z4, L46, SC, SPR, SOS, JAC, LPS…).
