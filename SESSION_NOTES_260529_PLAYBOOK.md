# Sachoki — Session Handoff (2026-05-29) → build the "Playbook" tab

> Paste into a new chat:
> `წაიკითხე ეს ფაილი და გააგრძელე იქედან: /Users/sachoki/Desktop/sachoki-desktop/SESSION_NOTES_260529_PLAYBOOK.md`
>
> **Main task for the new chat: build the "Playbook" Studio tab** (spec in §3).
> Everything else here is context so you don't repeat this session's mistakes.

---

## ✅ UPDATE 2026-05-30 — Playbook DONE (added by the build session)
The Playbook tab is **built, deployed, and live-verified**. russell2k recovery finished
(all 3 universes @ 2026-05-29, 100% enriched — OKLO 514/514); the 7.27GB `.bak` was deleted
after verification.

**Result (honest):** on the realised-backtest gate, **only buy-the-dip survives across all
universes** — `bottom_rsi_dip_markup` (RSI≤35 in MARKUP): sp500 PF 1.15 / nasdaq 1.08 /
russell2k 1.12, plus `bottom_rsi_range` on sp500. The other 7 candidates are rejected
(marginal expectancy, fails one time-half, or too rare on liquid names) — exactly the §3
"handful of modest edges" expectation. Watchlist/bias overlay, not a money-printer.

**New files:** `backend/studio/playbook_config.py` (role taxonomy + 9 setups),
`backend/studio/playbook.py` (engine — one DB scan, gate, live tickers). `seq_backtest.py`
now exposes `_run_on_df`/`_metrics` (parity-safe refactor) + **representative-ticker sampling**
when the `max_trades` cap bites (no more alphabetical bias; result carries a `truncated` flag).
Endpoint `GET /api/studio/playbook`; `api.js studioPlaybook`; `PlaybookTab` in StudioPanel.
Tests: `tests/test_playbook.py` (4) + a sampling regression in `tests/test_seq_backtest.py`.
Whitelist gained `sig_bias_dn, d_blast_bear_grn, wvf_spike, bf_buy, bf_sell`.

(Original handoff below — §3 was the task, now complete.)

---

## 0. TL;DR — where things stand
- App = FastAPI backend (`backend/`, uvicorn :8080, serves built React from `backend/static/`) + DuckDB analytics DB.
- This session: audited the app, fixed 7 priorities, built 3 new Studio tabs (Seq Lab, Realized Backtest, DB Chart), hit + recovered a data-loss incident, compacted the DB 7.3GB→1.8GB.
- **DB is at 2026-05-29 for sp500+nasdaq (100% enriched). russell2k recovery still RUNNING** (re-fetching 05-23..05-29 from API; was ~26% at handoff time). Let it finish; it only ADDS missing bars.
- Next: **the Playbook tab** — turn ~80 signals into a small, validated, regime-gated set of tradeable setups.

---

## 1. ⚠️ CRITICAL OPERATIONAL RULES (learned the hard way this session)
1. **NEVER `kill -9` the uvicorn process.** DuckDB write transactions (enrich/import/backfill/incremental) can roll back / corrupt on SIGKILL. Use graceful `kill <pid>` (SIGTERM) and WAIT for clean exit. (kill -9 + repeated restarts contributed to today's mess.)
2. **DuckDB is single-file, single-process for writes.** One process can't mix read-only + read-write connections → "Can't open a connection ... different configuration". Reads during a long write fail. `get_stats()` now catches this and returns `{"updating": true}` gracefully.
3. **Do NOT click Overview → "Start Import"** unless you mean a full rebuild. Bulk CSVs only go to ~05-22; importing them DELETEs the universe and **wipes newer incremental bars + all enrichment** (this is exactly what broke things today). An **import-guard** now refuses a CSV import that would regress the max date (pass `force=true` to override).
4. **Restarting kills any running background task** (incremental/edge scan). Check `/api/studio/incremental-update/status` (running?) before restarting.
5. **DuckDB CHECKPOINT does NOT shrink the file.** To compact, rewrite to a new file (copy tables, verify row counts, swap, keep `.bak`). Done once today → 1.8GB.

### How to restart the server safely
```bash
cd /Users/sachoki/Desktop/sachoki-desktop/backend
PID=$(ps aux | grep "uvicorn main:app" | grep -v grep | awk '{print $2}' | head -1)
kill $PID            # SIGTERM, NOT -9
# wait until `ps -p $PID` is gone, then:
nohup .venv/bin/uvicorn main:app --host 127.0.0.1 --port 8080 > /tmp/uvicorn.log 2>&1 &
```
Frontend: `cd frontend && npm run build && cp -r dist/* ../backend/static/` (no restart needed for FE-only changes; hard-refresh browser).
Tests: `cd backend && .venv/bin/python -m pytest ../tests -q` (pytest now installed + pinned).

---

## 2. THE DATABASE (read DB_SCHEMA_GUIDE_260529.txt for full detail)
- File: `/Users/sachoki/Downloads/studio_analytics.duckdb` (~1.8GB compact). Backup: `…/studio_analytics.duckdb.bak` (7.27GB — delete once russell recovery verified, to reclaim disk).
- Table `bars`: ~8.1M rows, 363 cols, 3 universes (sp500/nasdaq/russell2k), 2021-05-26 → 2026-05-29.
- **A ticker can be in >1 universe** (e.g. RGTI in nasdaq AND russell2k) → `WHERE ticker=?` returns DUPLICATE (ticker,date) rows. The bars endpoint now dedupes via `QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY universe)=1`. **Audit other `WHERE ticker=?` queries for this.**
- Forward-return cols (`fwd_*`, `mfe_*`, `mae_*`, `hit_*`, `drop_*`) are OUTCOME labels — never use as inputs (lookahead).
- Enrichment cols (`composite_full_suffix`, `bar_line5`, `bar_body_wick`, `bar_gap_range`, suffixes) computed for ALL bars by `enrich_universe`. TZ (`t_sig`/`z_sig`) only on bars matching a T1-T12/Z1-Z12 pattern (~4% of bars have NO TZ — that's normal, not a bug).
- Connect read-only: `duckdb.connect(path, read_only=True)`.

---

## 3. ⭐ MAIN TASK — build the "Playbook" Studio tab
**Goal:** turn ~80 signals (mostly noise/redundant individually) into a SMALL set of regime-gated, **backtest-validated** tradeable setups. NOT a mega-score of all 80 (that overfits).

### Design — a FUNNEL, not a score
```
REGIME GATE → SETUP (validated confluence) → TRIGGER (timing) →
  → BACKTEST GATE (expectancy>0 & profit_factor>1 & holds in BOTH time-halves) →
  → RANK → today's live tickers that currently match
```

### Signal role taxonomy (classify the 80 — put in a config, e.g. `backend/studio/playbook_config.py`)
- **🚪 REGIME GATE** (when to look, not entry): `wyc_phase` (MARKUP/MKDN/ACC_TR/DIST_TR), `rtb_phase`, `price_gt_200/price_gt_50`, `final_regime`, `pb_macro_penalty`.
- **🟢 BOTTOM SETUP** (validated edge at lows): `rsi_le_35` + `wyc_in_tr` / `d_spring` + `d_absorb_bull` + `pb_stop_cause`.
- **🔴 TOP SETUP** (validated edge at highs): `sig_vol_20x` / `sig_vol_10x` + `d_blast_bear_grn` / `d_absorb_bear` + `d_upthrust` + `wvf_spike`.
- **⏱️ TRIGGER** (timing): TZ sequences (`T9→Z1G→T1G` is time-stable), `bf_buy`/`bf_sell`, fresh reversal bar.
- **✓ CONFIRMATION** (diminishing returns — 2-3 enough, 6 = overfit): confluence count.
- **📊 CONTEXT-ONLY** (describe the bar, don't predict): suffix, body/wick, gap/range, L-family (L = volume, robust).
- **🗑️ DROP / NOISE**: most of F1-F11 / B1-B11, redundant Δ-variants, GOG/CTX (not even computed in DB), RGTI/SMX (disabled).

### Hard rule
**No setup enters the Playbook until it passes the Realized Backtest** (`studio.seq_backtest.backtest`): expectancy>0 AND profit_factor>1 AND positive in BOTH first/second half AND enough trades. The backtest engine already exists (see §4).

### Suggested build steps (for the new chat)
1. `playbook_config.py` — the role map above as a Python dict (gate/bottom/top/trigger/context/drop).
2. Backend endpoint `/api/studio/playbook` — for each predefined setup: run `seq_backtest.backtest`, keep only those passing the gate, attach today's matching tickers (regime-gated).
3. Frontend `PlaybookTab` in `StudioPanel.jsx` (follow the `SeqLabTab`/`DbChartTab` pattern; tab id e.g. `'playbook'`, add to `SUBTABS` + render switch). Show: each validated setup with its rule (entry/stop/target), realized stats (win/PF/expectancy/maxDD, both halves), and live tickers matching now.
4. Add regression tests (in-memory DuckDB, like `tests/test_seq_lab.py`).
5. Build FE, deploy to static, graceful restart, verify live.

### Honest expectation (tell the user)
The realistic output is **~5-10 modest edges** used as a **watchlist/bias overlay**, not a mechanical money-printer. This session's realized backtests showed even the best signals are marginal (PF ~1.15) or rare. The Playbook's value = discipline (regime gate + validation), not magic.

---

## 4. WHAT WAS BUILT THIS SESSION (new tools — all live)
New backend modules (`backend/studio/`):
- `seq_lab.py` — TZ Sequence Lab: rank N-bar T/Z sequences by forward outcome vs baseline. Endpoint `GET /api/studio/seq-lab`. SQL-safe (whitelist + `_q` escape). NaN→None guarded.
- `seq_backtest.py` — **Realized backtest** (entry next-open, target/stop/time exit, no pyramiding, time-split, PF/maxDD). Endpoint `GET /api/studio/seq-backtest`. **This is the "judge" for the Playbook.**
- `backfill_fwd.py` — fills trailing NULL forward-return labels (additive, IS-NULL guard). Endpoint `POST /api/studio/backfill-forward`. Auto-runs after incremental.
New frontend: `frontend/src/components/DbCandleChart.jsx` (DB-sourced candles + 6-line hover tooltip + multi-universe dedup). Tabs added to `StudioPanel.jsx`: **🧬 Seq Lab**, **🕯️ DB Chart** (+ Seq Lab has a Realized Backtest sub-panel).
api.js: `studioSeqLab`, `studioSeqBacktest`, `studioBars`.

---

## 5. VALIDATED EMPIRICAL FINDINGS (what actually has edge — use for the Playbook)
Method: pivots (`swing_type_3`), outcome `fwd_swing_ret_3` (% to next pivot) + `fwd_1d/5d`, baseline-relative lift. Reports: `REVERSAL_*_260528.txt`.

**Bottoms (LL pivot → up), lift over baseline:**
- `pb_stop_cause` +4.3 · `sig_bias_dn` +3.9 · `wyc_in_tr` +2.7 · `d_spring` +2.1 · `rsi_le_35` +1.5 · delta-absorption-on-red bars.
- Confluence `rsi≤35 + wyc_in_tr + d_spring` → win20 48-52% BUT **very rare on liquid names** (realized backtest: only ~2 sp500 trades). Mostly fires on illiquid russell.

**Tops (HH pivot → down):**
- `sig_vol_20x` −19.5 (big%69) · `vol_10x` −12.6 · `sig_sc` −8.4 · `d_blast_bear_grn` −8.3 · `wvf_spike` −5.7 · `d_upthrust` −6.0.
- Confluence `vol_20x + d_blast_bear_grn`/`absorb_bear` → win20 ~80%.

**Sequences (fixed-day):** baseline next-day win ~48%. Most "top by win%" sequences are MIRAGES (selection bias / 2nd-half-only). **Time-split is the filter.** The one that held BOTH halves: `T9|Z1G|T1G` ≈ 57% win, +0.6%/day, median +0.43% (real but modest).

**Realized backtest reality (entry/stop/target):** raw win% collapses. e.g. `rsi≤35 in MARKUP` (+8/−4/15b): 8141 trades, win 42.5%, median −4% (stop), PF 1.15 (marginal). `d_spring` nasdaq: PF 0.87 (loses). → **idealized "to-next-pivot" returns ≠ tradeable.** Validate everything.

**Direction-key gotcha:** pivot HH%/HL% are INVERSELY related to near-term return (high HH% = exhaustion/topping, high HL% = bullish base). Trust f5med/win, not the green/amber colors.

---

## 6. THE 7 AUDIT FIXES (done + tested this session)
1. **SQL injection** — `signal_stats.py` whitelists universe + escapes values (`_safe_universe`, `_q`); incremental DELETE + seq_lab/seq_backtest guarded.
2. **Tests** — pytest pinned in `requirements.txt` + `pytest.ini` + ~24 new regression tests. Baseline 11 pre-existing failures (stale version assertions); my changes added 0 new.
3. **Silent `except`** — `api_bar_signals` engine block → `compute_all_signals()` orchestrator that LOGS which engine fails (no more silent blanks).
4. **Legacy `incremental.py`** — decoupled from live path (import removed from `studio_api`, deprecation note added); `incremental_delta.py` is the only live path.
5. **`api_bar_signals` decomposition** — extracted `compute_all_signals(df, ticker, tf)` orchestrator (parity-safe pure move). Full main.py→routers split deferred.
6. **Frontend bundle** — `React.lazy` + `Suspense` for ~20 panels + vite `manualChunks` (react/charts). Main chunk 1.14MB→240KB.
7. **fwd backfill** — `backfill_fwd.py` (§4).

Plus: `get_stats` graceful "updating" status, multi-universe bars dedup, **import-guard** (regression protection), DB compaction.

---

## 7. KEY FILES MAP
- `backend/main.py` — FastAPI app, `api_bar_signals` + `compute_all_signals`, APScheduler (incremental 17:00 ET sp500+nasdaq).
- `backend/studio_api.py` — all `/api/studio/*` routes (import, incremental, edge-scan, seq-lab, seq-backtest, backfill, bars, exact-sequence).
- `backend/studio/db.py` — `get_conn`, schema, `get_stats` (graceful).
- `backend/studio/signal_stats.py` — `query_exact_sequence` (Sequence Builder; has optional `conn` param), SQL-safety helpers.
- `backend/studio/enricher.py` — `enrich_universe` (ALL bars, idempotent) + `_compute_pine_engines`.
- `backend/studio/incremental_delta.py` — daily delta append (the live incremental).
- `backend/studio/importer.py` — bulk CSV import (now with regression guard + `force`).
- `backend/studio/seq_lab.py`, `seq_backtest.py`, `backfill_fwd.py` — new (§4).
- `frontend/src/components/StudioPanel.jsx` — Studio tabs (add Playbook here).
- `frontend/src/components/DbCandleChart.jsx` — DB chart.
- Docs: `DB_SCHEMA_GUIDE_260529.txt`, `CHART_VS_DB_DIVERGENCE_FULL_260529.txt`, `FILTER_DIVERGENCE_AUDIT_260529.md`, `REVERSAL_*_260528.txt`.

---

## 8. OPEN TASKS / TODO
1. ✅ **DONE — Build the Playbook tab** (§3). See the 2026-05-30 update block at the top.
2. ✅ **DONE — russell2k recovery finished & verified** (all 3 universes @ 2026-05-29, 100% enriched).
3. ✅ **DONE — `.bak` deleted** (7.27GB reclaimed) after russell verification.
4. Audit other `WHERE ticker=?` queries for the multi-universe dedup issue (§2).
5. Clean up scratch scripts: `backend/_reversal_*.py` (8 ad-hoc analysis scripts — safe to delete, outputs saved as `REVERSAL_*.txt`).
6. Nothing is git-committed this session (user hasn't asked). Branch: `feat/ultra-parquet-inmem`. Many `M` + `??` files. Consider committing once russell done.
7. Optional: import-guard live test; main.py→routers split (#5 remainder); a11y; turbo/ultra panel dedup.

---

## 9. STYLE / CONTEXT
- User writes Georgian in Latin transliteration; signal codes stay Latin (TZ, PS-R2X, etc.). Reply in Georgian-Latin, technical terms in English.
- User VALUES honesty over hype — call out overfit/lookahead/marginal-edge plainly (they caught me over-claiming early; honesty built trust).
- App version v4.8.116 · Pine TZ_WLNBB 260523 v3.5.
