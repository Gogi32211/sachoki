# Session Resume — Zones work (HV-Zones, Gann-Zones, chart history overlays)

Last updated: 2026-06-06. Continue from here.

## What this session built (chronological, all COMMITTED & DEPLOYED)

The work centred on **volume/structure zones** and **chart UX**. Server runs via
`backend/.venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8080` (built
React in `backend/static/`, rebuild with `cd frontend && npm run build && rsync -a --delete dist/ ../backend/static/`).

### 1. HV-Zones (high-volume re-test zones)
- `backend/ai_journal/zone_retest.py` — core. A "trigger" = a daily bar with
  `volume >= vol_min * avg_vol_20d`. Zone = that bar's `[low, high]`. An
  "active re-test" = price LEFT the zone (closed outside) and is now back inside.
- Window: `TRIGGER_LOOKBACK_MIN=8`, `TRIGGER_LOOKBACK_MAX=90` days (min was 20, user lowered to 8).
- **Bullish AND bearish triggers** both create zones (user: "мы не скипаем bearish").
  `direction` field = 'bull'|'bear'. Exit condition generalised to "close NOT BETWEEN bounds".
- `scan()` returns ONE row per (ticker, zone) — multi-zone per ticker, with `n_zones`.
- `history_for_ticker(ticker, vol_min=5, from_date=None, limit=500)` — ALL historical
  HV-spikes (no re-test/classification), for the chart grey overlay. Accepts `from_date`
  to scope to chart's visible range.
- Endpoints (in `backend/main.py`): `/api/zone-retest/{tickers,zones/<tk>,scan}`,
  `/api/hv-zones/history/<tk>?vol_min=&from_date=&limit=`.
- Frontend: `HVZonesPanel.jsx` (sidebar lists all zones per ticker indented, tier
  chips x2-5/x5-10/x10+, rel filters, Dir column bull/bear).

### 2. Gann-Zones (highest-high / lowest-low bar zones)
- `backend/ai_journal/gann_zones.py` — TOP zone = `[low,high]` of highest-high bar in
  lookback; BOTTOM zone = lowest-low bar. `DEFAULT_LOOKBACK=90`.
- `history_pivots(ticker, pivot=5, from_date=None, limit=500)` — pivot highs/lows
  (bar whose high=max over [-pivot,+pivot] window). Confirmed swings; last `pivot`
  days can't confirm.
- Endpoints: `/api/gann-zones/{tickers,zones/<tk>,scan}`, `/api/gann-zones/history/<tk>?pivot=&from_date=&limit=`.
- Frontend: `GannZonesPanel.jsx` (lookback 30/60/90/120/180d, TOP/BOT toggles).
- Ultra filter chip 📐 Gann in `UltraScanPanel.jsx`.

### 3. CodeCandleChart.jsx — chart UX (the big one)
- **Per-zone colors**: `frontend/src/utils/zoneColors.js` — 10-color palette by zone
  index. Active zone lines + trigger-bar markers + trigger CANDLE (color/borderColor/
  wickColor) + sidebar Z# badges all share the color.
- **zoneSource prop**: 'hv' (cyan active) | 'gann' (amber active) — drives which
  active-zone endpoint to fetch and badge.
- **History overlays** (two independent pickers in toolbar):
  - 📊 ×2/×5/×10 = HV history → **grey dashed** lines (lineStyle 2)
  - 📐 ±5/±10/±20 = Gann pivots → **grey dotted** lines (lineStyle 1)
  - **Confluence** (same price level in both, rounded 2dp >$5 / 4dp <$5) → **white SOLID** (lineStyle 0, width 2)
  - Single combined useEffect fetches both in parallel, buckets by rounded price.
  - Scoped to chart's first visible bar via `from_date`.
- **Grid removed** (`grid.{vert,horz}Lines.visible=false`).
- **Fullscreen** ⛶ button → fixed-inset overlay, chart fills viewport + 380px
  `FullscreenSidePanel` (OHLC, active zones, history summary, recent 15 bars Δ%).
  Parent can inject controls via `sidePanelExtras` prop (GannZonesPanel passes
  lookback + TOP/BOT toggles). Forced resize: applyOptions(w,h)+fitContent triple-shot
  (rAF+60ms+200ms) on fullscreen toggle.
- `bare=true` charts (Turbo/Ultra hover thumbnails) skip all toolbar/buttons.

## Latest commits (newest last)
acc3689 multi-zone sidebar · 15e4ff7 per-zone colors · d7998c6 colored trigger candles
· 6540d3c lookback 8d · 192c39d bearish zones · ee0eb59 history overlay · 2977b61
3-tier history+scoping · 7555c86 no-grid+lime+fullscreen · 94ad418 fullscreen
settings · 38953e3 fullscreen flex · b8e5aaf independent pickers · 7e88c41 grey+white
confluence · 5d91167 line styles (HV dashed / Gann dotted)

## Key domain facts (carry forward)
- avg_vol_20d is in `bars` table; vol_mult = volume/avg_vol_20d.
- Liquid mega-caps rarely spike ×10 (AAPL ×5=3 ever, SOFI/MSFT/NVDA/TSLA ×10=0).
  When user asks "why no zone here", check: (a) vol_mult tier vs selected button,
  (b) 8-90d window membership, (c) re-test condition (did price leave & return).
- lightweight-charts v4.1.6: lineStyle 0 Solid / 1 Dotted / 2 Dashed / 3 LargeDashed
  / 4 SparseDotted. NO native dash-dot — Gann uses Dotted as closest to user's "-.-.-".

## Open / possible next
- Gann dotted (lineStyle 1) is the approximation for "-.-.-." — user may want
  3 LargeDashed or 4 SparseDotted instead (offered, not yet chosen).
- The "Preview scan" plan (parsed-roaming-sedgewick.md) is a SEPARATE older task —
  hybrid DB+live-today-bar scan. NOT part of zones work; status unknown, verify if revisited.

## Hard constraints (NEVER violate)
- yfinance must NEVER be used (ALLOW_YFINANCE_FALLBACK=0, hangs). Massive API only.
- DuckDB single-writer: server OFF during studio_analytics.duckdb writes; never run a
  separate journal-writer process while server up. get_journal_conn always read_write.
- ANTHROPIC_API_KEY/MASSIVE_API_KEY live in backend/.env (gitignored — NEVER commit).
- Never modify turbo_score. Paper-trading only.
- User writes Georgian in Latin transliteration + Russian. Be token-efficient.
- Always: rebuild frontend + rsync + restart uvicorn after frontend changes; commit
  with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## Session 2026-06-07 additions (chart context + zone analytics)

All deployed to backend/static on :8080 (launchd now serves it — see
project_backend_launchd memory: needs Full Disk Access for /bin/bash + uv python
because Desktop is TCC-protected).

1. **Dedup fix** — `ultra_db_scan.py:491` `PARTITION BY ticker` (+universe priority
   sp500>nasdaq) so dual-listed tickers (LNT/MDLZ/NWSA) show once, not twice.

2. **VB/W volume-class overlays + filter** (`vol_class.py`, `CodeCandleChart.jsx`,
   `UltraScanPanel.jsx`): chart lines on high/low of VB (red solid) / W (grey
   sparse-dotted) bars; toolbar picker; Ultra filter chips next to Gann. Data from
   `bars.vol_bucket`. Endpoints `/api/vol-class/{history,tickers}`.
   - **last-N limiter** (`🕒 last [N]`) caps each overlay (HV/Gann/VB/W) to most
     recent N occurrences. Frontend `.slice(-N)`.

3. **Fullscreen fix** (`CodeCandleChart.jsx:~746`): `inner` now always wrapped in a
   STABLE div (`display:contents` non-fs, `fixed inset-0 flex` fs) so the chart
   canvas never remounts on fullscreen toggle (was blanking).

4. **Insider buys (SEC Form 4) ★ markers** — `edgar.py` already had Form-4 ingest;
   added:
   - `marks_for_ticker` → per-date buys with `txs` breakdown (who/role/shares/$).
   - **On-demand per-ticker**: `fetch_ticker_form4` pulls ONE ticker's last-year
     Form 4 via SEC submissions API (data.sec.gov/submissions/CIK.json), caches in
     `insider_ticker_cache` (TTL 24h). Endpoint `/api/journal/insider/marks/{tk}?ensure=1`.
     First view ~2-15s (⏳), then instant. THIS replaced the universe backfill for charts.
   - Chart: gold ★N marker on buy bars; fullscreen side panel lists who/role/shares/$.
   - Parallel loader: `_WORKERS=5` + global rate-limiter `_MIN_INTERVAL=0.11` (~9 req/s,
     under SEC 10/s). Resumable universe backfill via `insider_ingest_log` (kept for
     the screener cluster list; STOPPED for charts). Daily scheduler `insider_daily @ 18:30 ET`.
   - SEC User-Agent still placeholder (`AIJ_SEC_UA` env) — set real contact if hammering.

5. **Zone Edge analytics** (`zone_events.py`, `ZoneEdgePanel.jsx`, tab `zoneedge`):
   labels every HV-zone interaction as `exit_up`/`exit_down`/`retest` (window funcs),
   measures forward edge vs baseline + which bar-CONTEXT lifts it. Endpoint
   `/api/zone-events/report?vol_min=&horizon=&first_only=&min_n=`.
   - **Finding**: raw events have NEGATIVE edge (exit_up −0.34%, retest −0.26% vs
     baseline +0.94%). Edge is in CONTEXT: retest + `wyc_spring` = +0.49%/win 50%;
     retest + `vol_bucket=VB` = −0.53%/win 35% (climax trap).
   - Reuses `bars` fwd_*/mfe/mae + curated context cols (`_BOOL_CTX`/`_CAT_CTX`).
   - **Next**: extend to Gann zones (`zone_kind`), OOS time-split, feed best
     event×context into `signal_stats`/qlib, optional on-chart event markers.

### Commit note
Working tree had ~70 pre-existing unrelated dirty files (reversal research, session
notes, other engine tweaks). This session committed ONLY its own isolated files;
the rest left untouched. backend/static is gitignored (deploy via rsync, not git).
