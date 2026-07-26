# Sachoki — Session Handoff (2026-05-30) → fix the DuckDB ONCE, correctly

> Paste into a new chat:
> `წაიკითხე ეს ფაილი და გააგრძელე იქედან: /Users/sachoki/Desktop/sachoki-desktop/SESSION_NOTES_260530_DB_REBUILD.md`
>
> **MAIN TASK:** make EVERY bar in `studio_analytics.duckdb` correct AND consistent across
> universes — once and for all — so that the daily incremental adding new bars from Monday
> builds on clean data. The user has **explicitly approved a full 5–6 hour re-fetch**; they
> prefer one long correct run over more piecemeal debugging. Do it RIGHT, test first, NEVER
> lose data.

---

## 0. TL;DR — current state (verified at handoff)
- App = FastAPI (`backend/`, uvicorn :8080, serves built React from `backend/static/`) + DuckDB analytics DB at `/Users/sachoki/Downloads/studio_analytics.duckdb` (~2.9 GB, 8,235,038 rows, 3 universes, → 2026-05-29).
- **The DB is currently WHOLE but DIVERGENT** (restored from backup after a failed re-fetch). 8605 rows on 2026-05-29. ONE clean uvicorn is running.
- **The problem to fix:** a ticker in >1 universe (e.g. CYCU in nasdaq + russell2k) can have DIFFERENT data on the SAME date because each universe was fetched separately at a different time. Divergence is **only on the last bar(s)** (05-29); history is identical across universes. **2684 tickers are divergent on 05-29.**
- The forward-looking fix is already in place (`_bar_cache`, below) — new bars won't diverge. **What remains: re-fetch the existing stale last bars so they're correct + consistent.**

---

## 1. THE DATA PROBLEM (concrete, with proof)
The LIVE Massive source is CORRECT (= the user's TradingView). The STORED DB last-bars are stale/inconsistent:

| ticker | DB nasdaq | DB russell2k | LIVE `/api/signals` = TRUTH |
|---|---|---|---|
| CYCU | $0.99 **T2G** (up) | $0.92 Z4 | **$0.87 / Z4** (−9.23%, TradingView-confirmed) |
| LTRN | $3.42 **Z9** | $3.52 **T2** (opposite!) | $3.59 |
| LULU | $131.34 **T2** | $130.83 **Z9** (opposite!) | $131.18 |

**Impact (this is the nuance the user cares about):**
- 🟢 **Historical / backtest / edge-research: NOT affected.** Last bar = 0.03% of 8.2M bars, and the last bar has **no forward-return labels yet** (no future) → it is never used as a backtest trade. Playbook gate, sequence mining, edge validation are clean.
- 🔴 **TODAY's live signals (Ultra scan, Playbook live-watchlist): WRONG** for these 2684 tickers — CYCU shows a bullish "dip-buy" (T2G) when it actually closed −9% (Z4). The chart is correct (live source); the **DB-based scan/Playbook are wrong** for these names. This is why it must be fixed.

**Find divergent tickers / verify "done" (returns 0 when fixed):**
```sql
WITH d AS (SELECT ticker, universe, close FROM bars WHERE date='2026-05-29')
SELECT DISTINCT n.ticker
FROM d n JOIN d r ON n.ticker=r.ticker AND n.universe='nasdaq' AND r.universe='russell2k'
WHERE ABS(COALESCE(n.close,0)-COALESCE(r.close,0)) > 0.001;   -- 2684 now → target 0
```

---

## 2. FIXES ALREADY IN PLACE (do NOT redo — all UNCOMMITTED)
1. **`backend/studio/incremental_delta.py`**
   - `_bar_cache` — fetches each ticker ONCE per run and reuses the SAME bars for every universe → identical bars across universes → **no future divergence**. (This is the permanent forward fix.)
   - `only_tickers: Optional[set]` param on `incremental_delta_refresh(...)` — restricts the run to an explicit ticker set ∩ that universe's DB tickers (so it can target a subset / the FULL DB list instead of the curated canonical list).
2. **`backend/studio/db.py`** — `UNIVERSE_PRIORITY_SQL` constant (`sp500 > nasdaq > russell2k`).
3. **`backend/studio_api.py`** — `/api/studio/bars/{ticker}` dedup now uses `ORDER BY {UNIVERSE_PRIORITY_SQL}` (was alphabetical, which wrongly ranked nasdaq above sp500).
4. **VIX disabled** (`backend/data.py` `^`-index guard in `fetch_ohlcv`; `backend/dashboard_routes.py` pulse + risk_alerts) — this fixed a dashboard hang (a ^VIX fetch loop that pegged a core at 100% CPU). Keep it off.
5. **`backend/studio/playbook*.py` + Playbook tab** — committed earlier (`git 350bbeb`).
6. **`~/.claude/skills/backtest-expert/`** — enhanced with significance / multiple-testing (Bonferroni) / cost gates (`--baseline-win`, `--num-strategies-tested`, `--cost-per-trade-pct`); 57 tests pass. Done.

---

## 3. WHAT FAILED THIS SESSION — DO NOT REPEAT
1. **Canonical incremental ≠ DB tickers.** `studio.incremental_delta._get_universe_tickers()` → `scanner.get_universe_tickers()` returns a CURATED list (sp500 612, nasdaq ~700, russell2k ~419 — **1557 total unique**). The DB has **5094** tickers on 05-29. **CYCU and ~3998 others are NOT canonical.** So the plain daily incremental can never fix them. **→ For the rebuild, use the FULL DB ticker list, not the canonical one** (pass it via `only_tickers`, or build a DB-ticker list per universe).
2. **DELETE-then-refetch lost data.** I deleted the 2684 divergent rows for 05-29, then ran the incremental to re-fetch — but the re-fetch re-inserted only ~52 of them (the rest came back empty / were dropped by a filter), leaving ~5368 rows MISSING. Restored from backup. **→ NEVER pre-delete before a bulk re-fetch. Use overwrite-on-success only.**
3. **The re-fetch returned empty for ~2600 tickers even though `/api/signals/<tk>` returns 05-29 fine RIGHT NOW.** So the data IS available — the failure was a bug/throttle, NOT missing data. **FIRST diagnose this** (likely the `incremental_delta` keep-filter: `if bdate <= last_date or bdate > today_str: continue` — check `_date.today()` value; or Massive throttling under bulk). Do not launch a bulk run until a 20-ticker test re-inserts 05-29 correctly.
4. **Zombie/multiple uvicorns held the DuckDB write lock** → `500 IO Error: Could not set lock ... Conflicting lock held in PID …`. Always ensure exactly ONE uvicorn; kill rogues (a stuck zombie not mid-write may need `-9`).

---

## 4. THE PLAN FOR THE NEW CHAT
Goal: every ticker's recent bars correct + identical across its universes, so Monday's incremental extends clean data. (History is already consistent — only recent bars need fixing — but a fuller rebuild is fine if you prefer; user accepts 5–6 h.)

**Step 1 — diagnose the re-fetch-empty bug (CRITICAL, ~10 min).**
Run `incremental_delta_refresh` (or `api_bar_signals` directly) on ~20 divergent tickers and confirm the 05-29 bar is actually KEPT and inserted. Inspect the keep-filter + `_date.today()`/`today_str`. Fix whatever drops it. Verify those 20 tickers become consistent ($value/signal identical in nasdaq & russell). Only proceed when this works.

**Step 2 — build the de-risked full re-fetch.**
- Ticker source = the **FULL DB ticker list per universe** (`SELECT DISTINCT ticker FROM bars WHERE universe=?`), union ≈ 5094.
- Fetch each ticker ONCE (the `_bar_cache` already does this) and write its bars to **ALL** its universes (confirm `incremental_delta` fans out per-universe via the universe loop + `only_tickers`; CYCU must be processed under both nasdaq and russell2k).
- **Overwrite-on-success only — NO pre-delete.** For each ticker: if the fetch returns a valid recent bar, DELETE-then-INSERT just that `(universe, ticker, date)`; if empty/error, LEAVE the existing row. → **data loss is physically impossible.** Worst case: some tickers stay divergent and you re-run for them.
- Re-fetch enough trailing days (e.g. last ~10) to cover the divergence window safely.
- Add a small delay + retry-on-empty if Step 1 shows Massive throttles under bulk.
- `enrich_after=True` (recomputes rsi_14/atr_14/avg_vol_20d for the corrected bars).
- Rate: ~0.4–0.7 tickers/sec → 5094 ≈ **2–3.5 h fetch** + ~15 min enrich. Run as a **background** task; monitor `/tmp/studio_incremental_delta_progress.json` and `running` via `/api/studio/incremental-update/status` (or the script's stdout if standalone).
- (Alternative, if the user wants a true from-scratch rebuild: wipe → import the three `*_signals_5y.csv` (to ~05-22) → full incremental to today → enrich. More thorough but the CSVs are stale to ~05-22 and the import is destructive — only with backups verified and `force=true`.)

**Step 3 — verify (definition of done, §6). Step 4 — commit.**

---

## 5. SAFETY / OPERATIONAL RULES (CRITICAL)
- **Backups (keep until verified):** `/Users/sachoki/Downloads/studio_analytics.duckdb.bak_260530` (2.9 GB, the current divergent-but-complete state) and `…/studio_analytics.duckdb.crashbak`. To restore: stop server → `ATTACH '<bak>' AS bak (READ_ONLY); DELETE FROM bars WHERE date>='2026-05-29'; INSERT INTO bars SELECT * FROM bak.bars WHERE date>='2026-05-29'; DETACH bak; CHECKPOINT;`
- **NEVER pre-delete bars before a bulk re-fetch.** Overwrite-on-success only.
- **DuckDB is single-writer.** Stop the server for exclusive standalone writes; ensure exactly ONE uvicorn afterwards. A separate read-write process while the server holds the file → "Conflicting lock".
- **Never `kill -9` uvicorn mid-DB-write** (corruption; there is NO 7 GB .bak anymore — only the two above). A stuck zombie holding the lock with no active write may need `-9`.
- **Test on ≤20 tickers and verify before any full run.** This session's pain came from launching bulk ops before validating.
- Restart safely (graceful SIGTERM, then detached):
  ```bash
  cd /Users/sachoki/Desktop/sachoki-desktop/backend
  for P in $(ps aux|grep "uvicorn main:app"|grep -v grep|awk '{print $2}'); do kill "$P"; done   # SIGTERM, wait to exit
  nohup .venv/bin/uvicorn main:app --host 127.0.0.1 --port 8080 > /tmp/uvicorn.log 2>&1 & disown
  # readiness without foreground sleep:
  curl -s --retry 40 --retry-delay 1 --retry-connrefused --max-time 3 http://127.0.0.1:8080/api/studio/stats >/dev/null && echo READY
  ```
- Tests: `cd backend && .venv/bin/python -m pytest ../tests -q` (13 pre-existing failures in tz_wlnbb/ultra/260523 are unrelated baseline).

---

## 6. DEFINITION OF DONE
- All 3 universes @ the latest trading date, 100% enriched.
- **Divergence query (§1) returns 0.**
- CYCU = ~$0.87 / **Z4** in nasdaq AND russell2k, matching `/api/signals/CYCU` + TradingView. Spot-check LTRN, LULU, AAOI consistent across universes.
- Chart (live) == scan (DB) for those tickers.
- ONE clean uvicorn; dashboard + Ultra + Playbook load fine.
- Then **commit** the uncommitted fixes (§2: incremental_delta, db.py, studio_api.py, data.py, dashboard_routes.py) on branch `feat/ultra-parquet-inmem`.

---

## 7. KEY FILES / PATHS / ENV
- DB: `/Users/sachoki/Downloads/studio_analytics.duckdb` · backups `.bak_260530`, `.crashbak`.
- Bulk CSVs (~/Downloads): `sp500_signals_5y.csv` 558MB, `nasdaq_signals_5y.csv` 2.3GB, `russell2k_signals_5y.csv` 3.2GB.
- `.env` (backend): `MASSIVE_API_KEY=…`, `ALLOW_YFINANCE_FALLBACK=1`.
- `backend/studio/incremental_delta.py` (`_bar_cache`, `only_tickers`, keep-filter), `studio/db.py`, `studio/enricher.py` (`enrich_universe`), `scanner.get_universe_tickers` (the curated list — the trap), `main.api_bar_signals` (the fetch+signals path used by the incremental and `/api/signals`).
- Progress file (incremental): `/tmp/studio_incremental_delta_progress.json`.
- Massive returns CORRECT settled data (= TradingView). yfinance fallback stays OFF for data fetches (thread-unsafe; caused the dashboard hang).

---

## 8. STYLE / CONTEXT
- User writes Georgian in Latin transliteration; reply in Georgian-Latin, technical terms in English. Signal codes stay Latin (TZ, T2G, Z4, L34…).
- **User values getting it DONE over endless debugging** — they explicitly accept a 5–6 h correct run because piecemeal searching burned more time/tokens. Don't rabbit-hole; test small, then commit to the long correct run and monitor.
- Honesty over hype; call out data problems plainly.
- TradingView truth for sanity: CYCU 2026-05-29 = O 0.9802 / H 1.02 / L 0.85 / **C 0.8714 (−9.23%)** / Vol 13.64M / signal **Z4**.

---

## 9. SECONDARY THREAD — the `backtest-expert` skill (done this session; extend if asked)
A Claude Code skill at **`~/.claude/skills/backtest-expert/`** (`SKILL.md` + `references/{methodology,failed_tests}.md` + `scripts/evaluate_backtest.py` + `scripts/tests/`). It scores a backtest across 5 dimensions (Sample Size, Expectancy, Risk Mgmt, Robustness, Exec Realism → 0–100 → Deploy/Refine/Abandon) + red flags. Purpose: enforce "robustness > profit" discipline on the user's signal/sequence validation — it directly fights the overfit/selection-bias "mirages" the user keeps getting burned by.

**Done this session (enhancement — all tested, 57/57 pass):** the plain scorecard couldn't tell a mirage from a real edge (it scored an overfit `90% win, n=42` and a real `42.5% win, n=8144` almost the same). Added the missing statistics, scipy-free (`math.erfc`):
- `--baseline-win <pct>` → one-sided proportion **z-test** of win rate vs baseline (small-n noise check).
- `--num-strategies-tested <N>` → **Bonferroni** correction (`p × N`) — THE guard against "the best of many scanned sequences" selection bias.
- `--cost-per-trade-pct <pct>` → flags edges **below realistic round-trip cost** (net-negative).
- Any FATAL gate forces the verdict to **Abandon** regardless of the score; output shows p-value / Bonferroni-p / net-edge.
- **Caveat (in SKILL.md):** don't use `--baseline-win` for **payoff-asymmetric** strategies (target ≠ stop, e.g. +8/−4) — there the win rate can sit below baseline while expectancy is positive; rely on the expectancy + cost gates instead.
- Demonstrated: the user's `TZZT 90% n=42` (edge 0.31%/day < cost) and a `55% n=45 / 300 scanned` both now → **Abandon** (were "Refine").

**Files were edited in place (not git-tracked in this repo — they live under `~/.claude/skills/`).** Run/test:
```bash
python3 ~/.claude/skills/backtest-expert/scripts/evaluate_backtest.py --help
/Users/sachoki/Desktop/sachoki-desktop/backend/.venv/bin/python -m pytest ~/.claude/skills/backtest-expert/scripts/tests/ -q   # 57 passed
```

**Possible NEXT steps (only if the user asks — this is secondary to the DB rebuild):**
1. ✅ **DONE (commit a1f1429)** — **SQL→skill wrapper** `backend/studio/eval_sequence.py`: `evaluate_sequence(...)` + CLI (`python -m studio.eval_sequence --universe nasdaq --rank 0 --cost 0.5`). Pulls baseline + #candidates from `seq_lab` (now returns `n_candidates` + per-row `avg_win`/`avg_loss`), computes the chosen seq's stats, feeds `evaluate_backtest.evaluate()`. Drawdown = abs(5th-pctile trade) (robust vs delisting −99% bars; raw worst surfaced). Finding: nasdaq 4-bar **color** T/Z seqs barely beat baseline (TZTZ 47.3% vs 46.6%) → cost gate forces **Abandon** (net −0.47%/trade) → no tradeable edge. NEXT to extend: run it over **signal-mode** seqs + Playbook setups (more candidates → real Bonferroni), and wire a verdict badge into the Studio UI (step #2).
2. **Bake the gates into the engines** — ✅ **PARTIAL (commit b7f4065)**: the **Seq Lab tab** now shows a per-row Deploy/Refine/Abandon **verdict chip** (`/api/studio/seq-lab?evaluate=true&cost=X` → `eval_sequence.annotate_seq_lab`; `seq_lab` returns per-row `dd_p5`/`worst`; UI `VerdictChip` in `StudioPanel.jsx`). Verified in-browser. **STILL TODO:** same treatment for **Playbook** (`studio.playbook` already has expectancy/PF/time-split gates and KNOWS its candidate count → add Bonferroni + cost-gate + chip there) and the realised `seq_backtest` sub-panel.
3. **Expectancy-significance test** (t-test / bootstrap CI on per-trade returns) for payoff-asymmetric setups, complementing the win-rate test.
4. **Real walk-forward** — currently `--years-tested` is just a number; compute actual out-of-sample (train/validate) from the trade series.
5. Exact binomial (vs the current normal approximation) for very small n.
