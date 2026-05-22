# Sachoki Screener — Architecture & Signal Reference

> Version 4.7.82 · API v2.9 · TZ_WLNBB Pine 260521 (line3/line4/line5 + ATR-relative gap)
> Build marker format: `<TZ_WLNBB_VERSION>__sha-<git_short>__built-<UTC_TIMESTAMP>`

---

## Table of Contents

1. [Overview](#overview)
2. [Directory Structure](#directory-structure)
3. [Backend Architecture](#backend-architecture)
4. [Signal Types Reference](#signal-types-reference)
5. [Scoring & Turbo Engine](#scoring--turbo-engine)
6. [ULTRA Score v2](#ultra-score-v2)
7. [Sequences Engine](#sequences-engine)
8. [BETA Score Engine](#beta-score-engine)
9. [TZ Intelligence Statistical Layer v2.8](#tz-intelligence-statistical-layer-v28)
10. [Paper Portfolio](#paper-portfolio)
11. [Chart Observations](#chart-observations)
12. [API Endpoints](#api-endpoints)
13. [Frontend Tabs](#frontend-tabs)
14. [Analyzer Modules](#analyzer-modules)
15. [Pivot Swing Character Analytics Engine](#pivot-swing-character-analytics-engine)
16. [Build Marker & Artifact Audit](#build-marker--artifact-audit)
17. [Deployment](#deployment)
18. [Test Suite](#test-suite)

---

## Overview

Sachoki is a real-time multi-timeframe stock screener built on FastAPI + React. It aggregates signals from a dozen independent engines (T/Z candle logic, L-signal volume patterns, GOG priority scoring, VABS volume absorption, Wyckoff phase detection, and more) into a single unified **TURBO_SCORE** (0–100) and a calibrated **ULTRA_SCORE** (0–100, replay-derived). The UI exposes 18 analysis tabs covering scanning, prediction, correlation, backtesting, sector rotation, and sequence analysis.

**Tech stack:**
- Backend: Python 3.11 · FastAPI 0.111 · APScheduler · yfinance / Polygon.io
- Frontend: React 18 · Vite 5 · Tailwind 3 · lightweight-charts 4
- Storage: SQLite (local) · PostgreSQL (production) · Redis (optional cache)
- Deploy: Docker (multi-stage) · Railway (railway.toml)

---

## Directory Structure

```
sachoki/
├── backend/                     # FastAPI application
│   ├── main.py                  # App entry point, all API routes
│   ├── signal_engine.py         # T/Z signal computation
│   ├── wlnbb_engine.py          # L-signal / WLNBB engine
│   ├── gog_engine.py            # GOG priority engine
│   ├── vabs_engine.py           # Volume absorption signals
│   ├── combo_engine.py          # B-signal combo patterns
│   ├── turbo_engine.py          # Turbo multi-engine scoring
│   ├── ultra_engine.py          # ULTRA two-stage scan engine
│   ├── ultra_orchestrator.py    # ULTRA Stage 1+2 orchestrator (lazy enrichment)
│   ├── ultra_score.py           # Shared ULTRA Score formula (no lookahead)
│   ├── ultra_signal_parser.py   # Compact label parser for live + Stock Stat rows
│   ├── sequence_engine.py       # Universe-wide N-bar T/Z sequence analyzer
│   ├── beta_engine.py           # BETA Score v2.1 (exchange-calibrated)
│   ├── paper_portfolio_api.py   # Paper portfolio router (/portfolio/*)
│   ├── paper_portfolio_migration.py  # Startup migration for portfolio tables
│   ├── daily_scanner_runner.py  # Daily 5pm ET scanner → portfolio entry workflow
│   ├── chart_obs_api_v2.py      # Chart Observations router (/obs/*) for K-signal tagging
│   ├── chart_obs_migration.py   # Startup migration for chart_observations table
│   ├── scanner.py               # Scan orchestrator + universe management
│   ├── profile_playbook.py      # Multi-timeframe profile analysis
│   ├── replay_engine.py         # Backtest / replay engine + ULTRA analytics
│   ├── rtb_engine.py            # Range / Trend / Breakout
│   ├── tpsl_engine.py           # Take profit / stop loss
│   ├── br_engine.py             # Bollinger Range breakouts
│   ├── sector_engine.py         # Sector rotation + RRG
│   ├── canonical_scoring_engine.py  # Canonical score computation
│   ├── predictor.py             # T/Z prediction
│   ├── wyckoff_engine.py        # Wyckoff phase detection
│   ├── para_engine.py           # Parabolic SAR patterns
│   ├── fly_engine.py            # Flyby / breakaway patterns
│   ├── power_engine.py          # Price-action power analysis
│   ├── f_engine.py              # Wyckoff F-strength patterns
│   ├── data.py                  # yfinance OHLCV fetching
│   ├── data_polygon.py          # Polygon.io data provider
│   ├── indicators.py            # RSI, CCI, ATR, normalization
│   ├── db.py                    # SQLite / PostgreSQL helpers
│   ├── analyzers/
│   │   ├── rare_reversal/miner.py       # Rare reversal pattern miner
│   │   ├── pullback_miner/miner.py      # Pullback pattern miner
│   │   ├── pivot_swing/                 # Pivot Swing Character Analytics Engine
│   │   │   ├── pivot_detector.py        # Confirmed pivot HIGH/LOW detection
│   │   │   ├── swing_builder.py         # Alternating LOW→HIGH / HIGH→LOW swings
│   │   │   ├── pivot_analytics.py       # 17-file aggregated output pipeline
│   │   │   └── runner.py                # CLI entry point
│   │   └── tz_wlnbb/
│   │       ├── signal_extraction.py     # Vectorised T/Z + line3/4/5 + ATR + PSAR
│   │       ├── signal_logic.py          # Per-bar Pine-equivalent priority engine
│   │       ├── stock_stat.py            # Emits stock_stat_tz_wlnbb_*.csv
│   │       ├── replay.py                # Replay ZIP builder (embeds pivot_swing/)
│   │       ├── build_marker.py          # Version + git_sha + UTC build marker
│   │       ├── config.py                # TZ_WLNBB_VERSION, Z_PRIORITY (no Z8)
│   │       └── schemas.py
│   ├── ultra_scan_routes.py         # Stub router (avoids ImportError on startup)
│   └── tz_intelligence/
│       ├── classifier.py            # ABR + matrix classification
│       ├── abr_classifier.py        # ABR rule matching
│       ├── scanner.py               # TZ Intel scan orchestrator
│       ├── matrix_loader.py         # Master matrix CSV loader
│       ├── stat_engine.py           # Statistical quality labels (STRONG/GOOD/AVERAGE/WEAK/REJECT)
│       ├── final_normalizer.py      # Final action normalizer v2.8 (GO/WATCH_HIGH/WATCH/…)
│       ├── whitelist_builder.py     # Whitelist/blacklist CSV generator from stock_stat
│       └── ABR_rule_database.csv
├── frontend/
│   └── src/
│       ├── App.jsx              # Main shell, tab routing, global state
│       ├── api.js               # API client utilities
│       ├── turboCache.js        # Client-side turbo result cache
│       └── components/          # 24 React panel components
├── tests/                       # Pytest test suite (663 tests)
├── tz_intelligence_package/     # TZ signal intelligence data & guides
├── TURBO_SCORE_REFERENCE.md     # Turbo score family details
├── Dockerfile                   # Multi-stage build (Node 20 → Python 3.11)
├── requirements.txt
├── Procfile                     # Railway / Heroku process definition
└── railway.toml                 # Railway deployment config
```

---

## Backend Architecture

### Request Flow

```
Browser → React (Vite) ──HTTP──► FastAPI (main.py)
                                    │
                        ┌───────────┼────────────────────────┐
                        ▼           ▼                        ▼
                  signal_engine  turbo_engine          ultra_orchestrator
                  wlnbb_engine   gog_engine             └─ ultra_score.py
                  vabs_engine    combo_engine            sequence_engine
                  replay_engine  sector_engine
                        │           │
                        └───────────┘
                                    │
                              pandas DataFrames
                                    │
                         yfinance / Polygon.io OHLCV
```

### Scheduled Scans (APScheduler)

Turbo and combined scans run automatically at **09:30, 12:30, 15:30 ET** on weekdays. Results are cached in-memory and served instantly.

### Universe Definitions

| Key | Description | Size |
|-----|-------------|------|
| `sp500` | S&P 500 large-caps | ~500 |
| `nasdaq` | NASDAQ stocks | ~4,000 |
| `russell2k` | Russell 2K small-caps | ~2,000 |
| `all_us` | All US equities | ~8,000 |
| `split` | Reverse-split window (D-7 → D+90) | dynamic |

---

## Signal Types Reference

### T/Z Signals — Bullish (T) and Bearish (Z)

T/Z signals classify each price bar based on its open/close relationship to the prior bar. They are the foundation of all scoring.

#### Bullish T Signals

| Signal | ID | Description |
|--------|----|-------------|
| T1G | 1 | First bullish gap — bull bar opening above prior close after bear bar |
| T1 | 2 | Standard bullish — bull bar opening above prior close |
| T2G | 3 | Continuation gap — bull bar after bull bar, gap up |
| T2 | 4 | Continuation — bull bar after bull bar (standard) |
| T3 | 5 | Lower-open bull — opens below prior open, closes above prior open |
| T4 | 6 | Full engulf — bull bar engulfs entire prior bar (highest priority) |
| T5 | 7 | Weak bull — opens below prior open, closes below prior close |
| T6 | 8 | Engulf bull — bull bar engulfs prior bull bar |
| T9 | 9 | Inside bull — bull bar fully inside prior bar |
| T10 | 10 | Inside continuation — bull bar inside prior bull bar |
| T11 | 11 | Mid-close bull — closes between prior open and close |
| T12 | 12 | Lower-open continuation — bull bar after bull bar, lower open |

#### Bearish Z Signals

| Signal | ID | Description |
|--------|----|-------------|
| Z1G | 13 | First bearish gap — gap down after bull bar |
| Z1 | 14 | Standard bearish — bear bar below prior close |
| Z2G | 15 | Continuation gap — bear bar after bear bar, gap down |
| Z2 | 16 | Continuation — bear bar after bear bar |
| Z3 | 17 | Higher-open bear — opens above prior open, closes below prior open |
| Z4 | 18 | Full engulf — bear bar engulfs entire prior bar (highest priority) |
| Z5 | 19 | Weak bear — opens above prior open, closes above prior close |
| Z6 | 20 | Engulf bear — bear bar engulfs prior bear bar |
| Z7 | 21 | Doji — open equals close |
| Z9 | 22 | Inside bear — bear bar inside prior bar |
| Z10 | 23 | Inside continuation — bear bar inside prior bear bar |
| Z11 | 24 | Mid-close bear — closes between prior open and close |
| Z12 | 25 | Higher-open continuation — bear bar after bull bar, higher open |

**Signal ID 0** = NONE (neutral bar).

---

### L-Signals — Volume × Price Classification

Computed by `wlnbb_engine.py`.

#### Base L-Signals

| Signal | Condition |
|--------|-----------|
| L1 | Volume ↓, Close ↑ — bullish absorption |
| L2 | Volume ↓, No new low — support on low volume |
| L3 | Volume ↑, Close ↑ — demand |
| L4 | Volume ↑, No new high — supply appearing |
| L5 | Volume ↓, Close ↓ — distribution on low volume |
| L6 | Volume ↑, Close ↓ — selling pressure |

#### L-Combo & WLNBB Overlay Signals

| Signal | Condition | Meaning |
|--------|-----------|---------|
| L34 | L3 ∧ L4 ∧ close ≥ open | Volume surge, no breakout — coiling |
| FRI34 | BLUE ∧ L34 | Premium-quality coiling bar |
| BLUE | Vol Z-score ≥ 1.1 ∧ RSI range ≤ 5.0 | High volume, flat RSI (controlled) |
| UI | BLUE ≥ 2× in last 10 bars | Sustained premium accumulation |
| CCI_READY | CCI in [−110, −50], rising | CCI softening before reversal |
| PRE_PUMP | VSA absorption ≥ 2 bars | Pump precursor signature |
| FUCHSIA_RH | RSI at 50-bar high ∧ volume down | Overbought divergence |
| FUCHSIA_RL | RSI at 50-bar low ∧ volume down | Oversold with drying volume |

---

### VABS Signals — Volume Absorption & Breakout

Computed by `vabs_engine.py`.

| Signal | Description |
|--------|-------------|
| ABS | Absorption spike — volume bucket jumps ≥ 2 levels |
| CLIMB | Volume climb — 3 consecutive rising bucket bars |
| LOAD | Load signature — accumulation combination |
| NS | Narrow Space — narrow spread + low volume + down close |
| SQ | Squeeze — high volume + narrow spread |
| VBO_UP | Volume Breakout Up — closes above 5–10 bar high |
| BC | Breakout Climax — wide spread + high volume + good close |
| SC | Selling Climax — wide spread + high volume + bad close |

---

### Profile Categories

Computed by `profile_playbook.py`.

| Category | Description |
|----------|-------------|
| SWEET_SPOT | `sweet_spot_active=true` and `late_warning=false` — optimal entry zone |
| BUILDING | Pattern building toward breakout |
| WATCH | On watchlist — no immediate signal |
| LATE | Late-stage — risk/reward no longer favorable |

---

## Scoring & Turbo Engine

The **TURBO_SCORE** (0–100) is a weighted aggregate computed by `turbo_engine.py`.

### Score Component Families (capped)

| Family | Cap | Source |
|--------|-----|--------|
| Backbone (conso_2809 + tz_bull chain) | 18 | signal_engine |
| Volume / Accumulation (VABS, Wyckoff) | 22 | vabs_engine |
| Breakout / Expansion | 18 | combo_engine |
| Combo buy patterns | 14 | combo_engine |
| Trend (T/Z, WLNBB, CCI) | 17 | signal_engine / wlnbb_engine |
| Delta / Order-flow | 12 | delta_engine |
| EMA cross series | 10 | turbo_engine |
| G-signals | 10 | gog_engine |
| Confluence bonuses | 18 | turbo_engine |
| Context (Wick, PARA, FLY) | uncapped ~18 | fly_engine / para_engine |

See `TURBO_SCORE_REFERENCE.md` for the full per-signal weight table.

> **Hard rule:** ULTRA Score calibration never modifies Turbo score, Turbo category logic, or live Turbo behavior.

---

## ULTRA Score v2

`backend/ultra_score.py` is the single source of truth for the ULTRA Score formula. Both the live ULTRA orchestrator and historical Stock Stat / Replay use it identically — **no lookahead** (never reads `ret_*d / mfe_* / mae_*`).

### Score Components

| Component | Cap | Description |
|-----------|-----|-------------|
| A. Breakout / Trigger | 35 | BUY_2809 (+20), ROCKET (+20), BB↑ (+15), BX↑ (+12), EB↑/BE↑/BO↑ (+10) |
| B. Setup / Accumulation | 25 | ABS (+10), VA/SVS/STR (+8), CLB (+7), LD (+6), L34/FRI34 (+6), TZ→3 (+10) |
| C. Confirmation / Quality | 25 | RS+ (+8), PF score tiers (+3/+6/+9/+12), SWEET_SPOT (+10), BUILDING (+6) |
| D. Context | −20..+20 | TZ Intel role, Pullback tier, Rare tier, ABR category |
| E. Penalties | negative | REJECT (−10), SHORT_WATCH (−8), WATCH+low_PF (−4), ISOLATED (−5) |
| F. Combination bonuses | additive | MOMENTUM_A, REVERSAL_GROWTH, TRANSITION_A, PULLBACK_ENTRY, L34_TRIGGER |
| G. Regime bonus (v2) | additive | FINAL_REGIME bonus (see table below) |

### Regime Bonus (v2, replay-derived)

| FINAL_REGIME | Bonus | Reason label |
|---|---|---|
| ACTIONABLE_SETUP | +12 | REGIME:ACTIONABLE |
| SHAKEOUT_ABSORB | +10 | REGIME:SHAKEOUT |
| CLEAN_ENTRY | +8 | REGIME:CLEAN |
| REBOUND_SQUEEZE | +5 | REGIME:REBOUND_SQUEEZE |
| RISK_REBOUND | +3 | REGIME:RISK_REBOUND |
| BEARISH_PHASE / BEARISH_CONTEXT | 0 | `BEARISH_CONTEXT_WARN` flag (warning only) |

### Bands v2 (replay-derived calibration)

Historical evidence from SP500 1D replay:

| Score | Band v2 | Priority | Replay data |
|-------|---------|----------|-------------|
| 90–100 | **A+** | HIGH_PRIORITY | avg 10D +2.36%, win 62.1%, fail 8.6% |
| 80–89 | A | WATCH_A | median 10D 0.00%, win 48.3% |
| 65–79 | B | STRONG_WATCH | — |
| 50–64 | C | CONTEXT_WATCH | — |
| <50 | D | LOW | — |

> The old `ultra_score_band` (A/B/C/D at 80/65/50) is kept for backward compatibility. UI and CSV prefer `ultra_score_band_v2` + `ultra_score_priority`.

### Confluence Caps (v2)

| Condition | Cap | Override |
|-----------|-----|---------|
| MOMENTUM_A + no strong regime | ≤ 89 | if ≥2 of {setup present, PF≥12, SWEET_SPOT} |
| SETUP_ONLY (no breakout) | ≤ 49 | if PF≥12 + strong regime |
| BREAKOUT_ONLY (no setup) | ≤ 59 | if PF≥12 + strong regime |
| L34/FRI34 alone | +2 max | +5 with breakout, +7 +PF, +10 +PF +regime |
| change_pct ≥ 25 + no strong regime | −4 light penalty | `EXTENDED_PENALTY_LIGHT` flag |

Strong regime = ACTIONABLE_SETUP, SHAKEOUT_ABSORB, or CLEAN_ENTRY.

### ULTRA Score Output Fields

| Field | Type | Description |
|-------|------|-------------|
| `ultra_score` | int 0..100 | Final clamped score |
| `ultra_score_band` | str | Legacy A/B/C/D |
| `ultra_score_band_v2` | str | A+/A/B/C/D (replay-calibrated) |
| `ultra_score_priority` | str | HIGH_PRIORITY / WATCH_A / STRONG_WATCH / CONTEXT_WATCH / LOW |
| `ultra_score_reasons` | list[str] | Deduped signal labels, max 12 |
| `ultra_score_flags` | list[str] | Combo flags (MOMENTUM_A, SETUP_ONLY, …) |
| `ultra_score_raw_before_penalty` | int | Pre-penalty raw sum |
| `ultra_score_penalty_total` | int | Absolute penalty |
| `ultra_score_regime_bonus` | int | Points added by FINAL_REGIME |
| `ultra_score_caps_applied` | list[str] | Which caps fired |
| `ultra_score_cap_reason` | str | Pipe-separated cap rationale |

All fields are written to the Stock Stat CSV and exposed in the live ULTRA scan JSON.

### ULTRA Two-Stage Orchestrator

`ultra_orchestrator.py` runs a two-stage scan to avoid OOM on large universes:

1. **Stage 1 (Turbo-only)** — fast scan of all tickers, produce `ultra_score` from Turbo fields alone. Results served immediately.
2. **Stage 2 (Lazy enrichment)** — background enrichment of top-N tickers with TZ/WLNBB, TZ Intel, Pullback, and Rare Reversal data. Score recomputed after each batch. UI live-updates.

### Replay Analytics — ULTRA Score

After Stock Stat + Replay, the engine produces:

| File | Description |
|------|-------------|
| `replay_ultra_score_band_summary.csv` | Legacy A/B/C/D band metrics |
| `replay_ultra_score_band_v2_summary.csv` | v2 A+/A/B/C/D band metrics |
| `replay_ultra_score_priority_summary.csv` | Priority label metrics |
| `replay_ultra_score_bucket_summary.csv` | Fine-grained 0–20 / 21–40 / … / 90–100 buckets |
| `replay_ultra_combo_perf.csv` | Per-combo-group (MOMENTUM_A, SETUP_ONLY, …) metrics |
| `replay_ultra_score_events.csv` | Top-N individual events |
| `replay_ultra_false_positives.csv` | Band A events with negative 5D returns |
| `replay_ultra_missed_winners.csv` | Sub-65 events with large 10D gains |

All metrics: count, avg/median 1D/3D/5D/10D returns, win rates, hit +5%/+10%, fail rates, MFE.

---

## Sequences Engine

`backend/sequence_engine.py` scans the full universe for recurring N-bar T/Z signal sequences and aggregates their multi-horizon forward-return statistics.

### How It Works

1. For each ticker, load Stock Stat CSV (TZ/WLNBB or Bulk Signal format).
2. Walk every bar; emit `(sequence_key, type)` events with per-horizon returns.
3. Aggregate events by sequence key → compute win rate, avg return, median return for 1D/3D/5D/9D.
4. Score = `win_rate_1d × log1p(count)` (balanced: high win rate + enough events).
5. Rank across universe; expose breadth (ticker_count / total_tickers).

### Multi-Horizon Returns

Returns derived from `close` (close-to-close). If CSV already has `ret_Nd`, that value is preserved; otherwise:

```
ret_Nd = (close[i+n] / close[i] - 1) × 100
```

Horizons: **1D, 3D, 5D, 9D**. A horizon is `None` when fewer than `n` bars remain.

### Sequence Result Columns

| Column | Description |
|--------|-------------|
| `sequence` | N-bar T/Z key, e.g. `T4→Z3→T2` |
| `type_seq` | BULL / BEAR |
| `count` | Total events (1D horizon) |
| `wins` | Events with ret_1d > 0 |
| `win_rate` | wins / count (1D) |
| `ticker_count` | Distinct tickers that showed this sequence |
| `score` | win_rate × log1p(count) |
| `win_rate_3d/5d/9d` | Win rates at other horizons |
| `avg_ret_1d/3d/5d/9d` | Average forward returns |
| `med_ret_1d/3d/5d/9d` | Median forward returns |
| `count_3d/5d/9d` | Events with sufficient forward bars |

### Sort Options

`score` (default), `win_rate`, `win_rate_3d`, `win_rate_5d`, `win_rate_9d`, `avg_ret_1d`, `avg_ret_3d`, `avg_ret_5d`, `avg_ret_9d`, `count`, `ticker_count`, `breadth`.

---

## BETA Score Engine

`backend/beta_engine.py` — BETA Score v2.1 (calibrated 2026-05-10 from NQ1+NQ2 = 478,909 rows and SP500 = 88,934 rows of replay data).

### Components

| Field | Range | Meaning |
|-------|-------|---------|
| `beta_score` | 0–100 | Display value (non-linear transform of `beta_raw`) |
| `beta_raw` | int | Pre-transform raw value |
| `beta_setup` | 0–60 | Structural quality component |
| `beta_momentum` | −5–50 | Momentum / regime component |
| `beta_zone` | string | Categorical zone label |

### Exchange-Specific Calibration

| Exchange | setup × | momentum × | excess × |
|----------|--------|-----------|---------|
| NASDAQ   | 1.40   | 0.30      | 0.85    |
| SP500    | 1.00   | 1.50      | 0.55    |

Regime multipliers also differ per exchange — `ROCKET_WATCH` NQ=1.2/SP=0.7, `ACTIONABLE_SETUP` SP=1.2, `REBOUND_SQUEEZE` NQ=1.1.

### Gates

- **P89 boost** — ×1.1 when an EMA89 cross-up aligns with WATCH/BUY/OPTIMAL.
- **D89 downgrade** — BUILDING → NEUTRAL when an EMA89 drop is active.

BETA is wired into TURBO/ULTRA scan rows, the SuperChart matrix (BETA Score row), and Replay Analytics.

---

## TZ Intelligence Statistical Layer v2.8

Added in v2.8. A statistical quality gate and final-action normalizer layered on top of the existing ABR classifier. Consumes per-signal SP500 1D replay data to upgrade raw ABR classifications into actionable tiers.

### Modules

| Module | Description |
|--------|-------------|
| `tz_intelligence/stat_engine.py` | Statistical threshold functions; maps (count, median_10d, fail_rate) → quality status |
| `tz_intelligence/final_normalizer.py` | v2.8 normalizer; combines composite lookup, seq4 lookup, ABR role, volume, suffix, and static lists into a single `final_action` |
| `tz_intelligence/whitelist_builder.py` | Reads a `stock_stat_tz_wlnbb_*.csv` and writes 8 output CSV files |
| `tz_intelligence/classifier.py` | Extended to propagate `matched_n` (sample count from master matrix rule) |
| `tz_intelligence/scanner.py` | Calls `normalize_final_action(r)` on every result row |

### Statistical Quality Labels (`stat_engine.py`)

| Status | Criteria |
|--------|----------|
| `STRONG` | median_10d ≥ 1.0%, fail_rate ≤ 20%, n ≥ 50 |
| `GOOD` | median_10d ≥ 0.5%, fail_rate ≤ 25%, n ≥ 50 |
| `AVERAGE` | median_10d ≥ 0.0%, fail_rate ≤ 30%, n ≥ 30 |
| `WEAK` | median_10d < −0.25% or fail_rate ≥ 35% (below AVERAGE thresholds) |
| `REJECT` | Extreme: median_10d deeply negative or fail_rate ≥ 35% |
| `LOW_SAMPLE` | n < 20 (insufficient data) |

Sample confidence labels: `HIGH` (n ≥ 100), `USABLE` (n ≥ 50), `DIRECTIONAL` (n ≥ 20), `LOW` (n < 20).

### Final Action Tiers (`final_normalizer.py`)

| Tier | Meaning | Color |
|------|---------|-------|
| `GO` | All gates pass; STRONG/GOOD composite + correct ABR role | Green |
| `WATCH_HIGH` | One soft cap triggered (AVERAGE stat or WEAK blacklist) | Emerald |
| `WATCH` | One or more hard blocks triggered (see below) | Yellow |
| `SHORT_WATCH` | Bearish/short context | Orange |
| `REJECT` | Hard statistical reject; static blacklist match | Red |

**GO-eligible ABR roles:** `BULL_A`, `PULLBACK_GO`, `PULLBACK_READY_A`

**Hard blocks → WATCH:**
- Volume bucket in `VB_FAIL` set
- ABR conflict flag (`abr_conflict_flag=True`)
- Composite stat = WEAK or REJECT (lookup)
- Composite stat = LOW_SAMPLE
- Composite × seq4 stat = REJECT
- REJECT-level blacklist hit (composite or seq4)
- Static hardcoded REJECT composite match (`_STATIC_REJECT_COMPOSITES`, 29 entries)
- EUR suffix without STRONG whitelist
- Legacy blacklist match

**Soft caps → WATCH_HIGH (not WATCH):**
- `matched_status = AVERAGE` (requires composite GOOD/STRONG)
- WEAK-level blacklist hit (composite or seq4)
- Composite × seq4 stat = WEAK

### CSV Lookup Files

`whitelist_builder.py` generates 8 files from a `stock_stat_tz_wlnbb_*.csv`:

| File | Rows (SP500/1D example) | Description |
|------|------------------------|-------------|
| `composite_whitelist.csv` | 129 | STRONG/GOOD composites |
| `composite_blacklist.csv` | 88 | WEAK/REJECT composites |
| `seq4_whitelist.csv` | 59 | STRONG/GOOD 4-bar signal sequences |
| `seq4_blacklist.csv` | 1105 | WEAK/REJECT 4-bar sequences |
| `composite_seq4_whitelist.csv` | 1 | STRONG/GOOD (composite, seq4) pairs |
| `composite_seq4_blacklist.csv` | 197 | WEAK/REJECT (composite, seq4) pairs |
| `composite_seq4_stats.csv` | 86395 | ALL observed (composite, seq4) pairs (n ≥ 1) |
| `aio_suffix_performance.csv` | 435 | A/I/O close-suffix comparison per base composite |

Files are searched in order: `./`, `/tmp/whitelists`, `/tmp`. The normalizer lazy-loads them on first call and exposes a `reload_lookups()` function.

### seq4 Definition

The 4-bar signal sequence is derived from 3 prior bars + current bar:
```
seq4 = "prev3_signal|prev2_signal|prev1_signal|current_signal"
```
Signals are resolved in priority order: `t_signal` → `z_signal` → `l_signal` → `—`.

### Suffix System (A/I/O)

The composite label has a 4-part suffix:
```
[ne_suffix][wick_suffix][penetration_suffix][close_suffix]
```
where `close_suffix ∈ {A, I, O}` represents the close position within the bar (Above midpoint / In midpoint zone / On/below low).

The `_VALID_SUFFIX_RE` regex was fixed in v2.8 to accept all A/I/O close suffix variants:
```
^[NE][UDB]?[PRH]?[AIO]?$
```

### Volume Bucket (WLNBB)

Volume is classified into 5 buckets per bar relative to 20-bar rolling stats:

| Bucket | Description |
|--------|-------------|
| `VB` | Very Bullish volume |
| `B` | Bullish volume |
| `N` | Neutral volume |
| `L` | Low volume |
| `W` | Weak / depressed volume |

`volume_bucket` is included in the composite label suffix and in the suffix-stats slice key.

### Diagnostic Columns

Every `tz-intelligence/scan` result row now includes:

| Column | Description |
|--------|-------------|
| `final_action` | GO / WATCH_HIGH / WATCH / SHORT_WATCH / REJECT |
| `final_action_reason` | Pipe-separated list of gate decisions |
| `stat_composite_status` | STRONG/GOOD/AVERAGE/WEAK/REJECT/LOW_SAMPLE/UNKNOWN |
| `stat_seq4_status` | Same for the seq4 lookup |
| `stat_comp_seq4_status` | Same for the (composite, seq4) pair |
| `stat_volume_status` | Volume quality from suffix stats |
| `composite_lookup_status_used` | Which of the 3 composite lookup tables was used |
| `seq4_lookup_status_used` | Which seq4 lookup table was used |
| `suffix_lookup_status_used` | Which suffix/AIO lookup was used |
| `volume_lookup_status_used` | Which volume lookup was used |
| `static_reject_match` | bool — matched `_STATIC_REJECT_COMPOSITES` |
| `matched_n` | Sample count from the master matrix rule |
| `matched_status` | ABR role quality from master matrix |
| `abr_conflict_flag` | bool — ABR role conflicted with T/Z direction |

### Replay ZIP Integration

When `POST /api/tz-wlnbb/replay` completes, `generate_replay_zip()` automatically:
1. Calls `build_whitelists(stat_path)` to produce the 8 lookup CSVs.
2. Embeds all 8 CSVs into the download ZIP.
3. Persists `composite_seq4_stats.csv` to disk so the running normalizer can reload it.
4. Calls `reload_lookups()` to hot-swap the lookup tables without a service restart.

### Static Fallback Blacklist

`_STATIC_REJECT_COMPOSITES` (29 hardcoded composites) provides a runtime safety net when no CSV files are present. Derived from SP500 1D replay data with n ≥ 50 and status = REJECT.

---

## Paper Portfolio

A paper-trading layer that consumes top-tier ULTRA picks, simulates entries at next-day open, tracks open positions, and reports realised returns.

### Tables (auto-created via `paper_portfolio_migration.py`)

- `paper_portfolio` — one row per entry: `ticker`, `signal_date`, `entry_price`, `current_price`, `realized_return_p`, `status` (PENDING / OPEN / CLOSED), `tier`, `score`, plus daily OHLC tracking.

### Workflow (driven by `daily_scanner_runner.py`)

1. **5pm ET** — read the day's ULTRA CSV, filter TIER 1 + TIER 2 → `POST /api/portfolio/scan-and-add` (or `/entry`).
2. **Next morning** — actual opens posted via `POST /api/portfolio/entry-price` (PENDING → OPEN).
3. **Each evening** — daily OHLC posted via `POST /api/portfolio/daily-prices` then `POST /api/portfolio/daily-check` evaluates take-profit / stop-loss / holding-period rules and closes qualifying rows.

The frontend `PortfolioPanel` exposes Pending / Open / Closed tabs and a server-side **Scan & Add** action that requires no CSV upload.

---

## Chart Observations

A manual K-signal tagging layer (`backend/chart_obs_api_v2.py`) for retrospective annotation and calibration of T/Z + L + sequence setups.

### Flow

1. User enters **ticker + observation date** in the UI.
2. `GET /api/obs/prefill` looks up the row in the `stock_stat` table, auto-fills T/Z signals, sequence label, turbo/beta/ultra scores, sweet-spot flag, RTB phase, prior 3 bars, and an entry-price suggestion.
3. User confirms / annotates and adds the discretionary fields:
   - `k_signal_match` (K1..K11 or NONE), `k_fired` (bool)
   - `entry_quality` (PERFECT / GOOD / OK / BAD)
   - free-text `notes`
4. `POST /api/obs/save` upserts on `(obs_date, ticker, t_signal)` into the `chart_observations` table.

### Result Tracking

`POST /api/obs/sync-results` joins `chart_observations` to `paper_portfolio` on `(ticker, signal_date)` for closed trades, back-filling `result_5d`, `result_10d`, and `result_outcome` (WIN / LOSS / NEUTRAL).

### Stats & Recent Endpoints

`GET /api/obs/stats?days=N` returns win-rate and avg-10d aggregated by `(t_signal, sequence_label, k_signal_match)`. `GET /api/obs/recent?limit=N` returns the most recent observations for review.

> **Requires** the `stock_stat` table to be populated in the backing DB (CSV import on Railway Postgres). If missing, `/obs/prefill` returns `503` with a clear "data not loaded" message instead of a raw Postgres error.

---

## API Endpoints

All endpoints prefixed `/api/`. Backend serves on port **8080**.

### Health & Config

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Service status, version |
| GET | `/api/settings` | Load persisted settings |
| POST | `/api/settings` | Save settings |

### Ticker Data

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/ticker-info/{ticker}` | Name, sector, industry |
| POST | `/api/ticker-info-batch` | Batch info (up to 200 tickers) |
| GET | `/api/signals/{ticker}` | T/Z signals |
| GET | `/api/wlnbb/{ticker}` | WLNBB L-signals |
| GET | `/api/bar_signals/{ticker}` | Per-bar full signal breakdown |
| GET | `/api/watchlist` | Real-time scan for comma-separated tickers |

### Prediction & Stats

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/predict/{ticker}` | T/Z prediction + TZ matrix |
| GET | `/api/pooled-predict/{ticker}` | Prediction using pooled stats |
| POST | `/api/pooled-stats/build` | Build pooled stats (background) |
| GET | `/api/signal-stats/{ticker}` | Per-signal win% and return stats |
| GET | `/api/tz-l-stats/{ticker}` | T/Z × L matrix + SPY/QQQ benchmarks |
| GET | `/api/signal-correlation` | Signal co-occurrence correlation |

### Scanning

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/scan/results` | T/Z scanner results |
| POST | `/api/scan/trigger` | Start T/Z scan |
| GET | `/api/combined-scan` | Multi-engine aggregated results |
| GET | `/api/turbo-scan` | Turbo scan results (ranked) |
| POST | `/api/turbo-scan/trigger` | Start turbo scan |
| GET | `/api/turbo-analyze/{ticker}` | Deep turbo breakdown |

### ULTRA Scan

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/ultra-scan/results` | ULTRA scan results (stage-aware, paginated) |
| POST | `/api/ultra-scan/trigger` | Start ULTRA two-stage scan |
| GET | `/api/ultra-scan/status` | Scan phase + enrichment progress |

### Sequences

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/sequence-scan/trigger` | Start sequence scan (background) |
| GET | `/api/sequence-scan/status` | Scan progress |
| GET | `/api/sequence-scan/results` | Ranked sequence results (paginated, sortable) |

### Sectors

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/sectors/overview` | All-sector returns + strength |
| GET | `/api/sectors/rrg` | Relative Rotation Graph data |
| GET | `/api/sectors/heatmap` | Heatmap by metric |
| GET | `/api/sectors/{etf}` | Single sector ETF detail |

### Replay / Backtest

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/replay/run` | Run backtest (tf, universe) |
| GET | `/api/replay/reports` | List result reports |
| GET | `/api/replay/report/{name}` | Get report (paginated) |
| GET | `/api/replay/export/{name}` | Export report as CSV |
| GET | `/api/replay/export-all` | Export all reports as ZIP |

### Stock Stat / Bulk Signal CSV

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/stock-stat/trigger` | Generate bulk signal CSV for universe |
| GET | `/api/stock-stat/status` | Generation progress |
| GET | `/api/stock-stat/download` | Download generated CSV |

### TZ/WLNBB Analyzer

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tz-wlnbb/scan` | TZ × WLNBB scan results |
| POST | `/api/tz-wlnbb/generate-stock-stat` | Generate per-stock stat CSV (260521 Pine version with `build_marker` column) |
| GET | `/api/tz-wlnbb/status` | Generation progress |
| POST | `/api/tz-wlnbb/replay` | Run TZ/WLNBB replay (embeds whitelists + Pivot Swing pivot_swing/ files + BUILD_MARKER.txt) |
| POST | `/api/tz-wlnbb/build-whitelists` | Build whitelist/blacklist CSVs from a stock_stat CSV path |
| GET | `/api/tz-wlnbb/replay-perf` | Replay perf rankings (kind = body_wick / gap_range / line5) |
| GET | `/api/tz-wlnbb/download/{filename}` | Download a stock_stat or replay CSV |

### Pivot Swing Character Analytics

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/pivot-swing/run` | Run pivot swing engine (aggregated multi-ticker) |
| GET | `/api/pivot-swing/status` | Current run progress |
| GET | `/api/pivot-swing/results` | List of generated output files |
| GET | `/api/pivot-swing/download/{filename}` | Download a pivot swing artifact |

### Build Verification & Artifact Audit

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/code-version` | Active code's `build_marker`, `tz_wlnbb_version`, Z_PRIORITY, and fingerprint of the deployed final_normalizer / scanner / pivot_swing modules |
| GET | `/api/artifact-audit` | On-disk audit: stock_stat versions, `scan_as_of_date`, `stale_dropped_count`, `WATCH_HIGH:GATES_PASS` count, `bad_gates_pass_count`, `pivot_output_file_count`, `zip_build_marker` vs `active_code_build_marker` + a `checks{}` block + `all_checks_pass` bool |
| POST | `/api/regenerate-and-audit` | One-click chain: generate-stock-stat → replay ZIP (with pivot_swing) → artifact-audit |
| GET | `/api/regenerate-and-audit/status` | Poll for chain progress + final audit result |

### Specialized Miners & Intelligence

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/rare-reversal/scan` | Rare reversal pattern scan |
| GET | `/api/pullback-miner/scan` | Pullback pattern scan |
| GET | `/api/pullback-miner/report` | Pullback pattern report |
| GET | `/api/tz-intelligence/scan` | ABR classification scan |

### Paper Portfolio

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/portfolio/scan-and-add` | Server-side scan → push TIER 1/2 picks |
| POST | `/api/portfolio/entry` | Add a single ticker (PENDING) |
| POST | `/api/portfolio/entry-price` | Set actual entry price (PENDING → OPEN) |
| POST | `/api/portfolio/daily-prices` | Bulk daily OHLC update |
| POST | `/api/portfolio/daily-check` | Evaluate exit rules; close qualifying rows |
| GET | `/api/portfolio/open` | Currently open positions |
| GET | `/api/portfolio/stats` | Aggregate performance metrics |
| GET | `/api/portfolio/export` | CSV export |
| GET | `/api/portfolio/` | Full portfolio listing |

### Chart Observations

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/obs/prefill?ticker=&obs_date=` | Auto-fill observation form from `stock_stat` |
| POST | `/api/obs/save` | Upsert observation with K-signal + notes |
| POST | `/api/obs/sync-results` | Backfill result_5d/10d from `paper_portfolio` |
| GET | `/api/obs/stats?days=N` | Win-rate / avg-10d grouped by signal + K-match |
| GET | `/api/obs/recent?limit=N` | Recent observations |

---

## Frontend Tabs

All tabs defined in `App.jsx`. **20 tabs total.**

### ⚡ TURBO (`TurboScanPanel.jsx`)

High-speed multi-engine scan ranked by TURBO_SCORE. Always-mounted.

Filters: universe / tf / direction / score-band / signal / profile / volume / sector / RTB phase / lookback.

### 🧬 ULTRA (`UltraScanPanel.jsx`)

Two-stage ULTRA scan ranked by ULTRA Score. Always-mounted.

- Stage 1 scores appear immediately after fast Turbo-only pass.
- Stage 2 enriches top-N with TZ/WLNBB + TZ Intel + Pullback + Rare Reversal in background.
- ULTRA Score column shows numeric + v2 band (A+/A/B/C/D). 90+ highlighted with stronger glow than 80–89.
- Tooltip: `ULTRA 92 (A+/HIGH_PRIORITY) · BUY_2809 MOMO+CAT REGIME:ACTIONABLE`
- CSV export carries all `ultra_score_*` fields (legacy + v2 calibration).

### ⭐ Watchlist (`PersonalWatchlistPanel.jsx`)

Personal watchlist. Current signals and scores per ticker. Add/remove support.

### Combined Scan (`CombinedScanPanel.jsx`)

Aggregated results from all engines, tabbed by signal family.

### Predictor (`PredictorPanel.jsx`)

Statistical T/Z signal predictor. Next-bar probability matrix, L-signal prediction, benchmark vs SPY/QQQ.

### T/Z Scanner (`ScannerPanel.jsx`)

Traditional T/Z scan. Filter by signal type, timeframe, min score.

### T/Z × L Stats (`TZLStatsPanel.jsx`)

T/Z × L correlation matrix for a single ticker.

### 📊 Corr (`SignalCorrelPanel.jsx`)

Universe-wide signal co-occurrence correlation heatmap.

### 📋 Superchart (`SuperchartPanel.jsx`)

Dense multi-row candle view with overlaid signal data (T/Z, L, B-signals, GOG, F-signals, Fly, RTB, TPSL).

### 🌐 Sectors (`SectorAnalysisPanel.jsx`)

Sector rotation: overview, RRG, heatmap, macro analysis.

### 🔍 Analyze (`TickerAnalysisPanel.jsx`)

Deep single-ticker analysis across all engine families.

### 🔬 Replay (`ReplayPanel.jsx`)

Backtest results viewer. Includes ULTRA Score analytics sections:

| Section | Key |
|---------|-----|
| ULTRA Score Bands | Legacy A/B/C/D with aggregate return metrics |
| ULTRA Score Bands v2 | A+/A/B/C/D (replay-calibrated) |
| ULTRA Score Priority | HIGH_PRIORITY..LOW aggregate metrics |
| ULTRA Score Buckets | Fine-grained 0–100 bucket breakdown |
| ULTRA Combos | Per-combo-group (MOMENTUM_A, SETUP_ONLY, …) performance |
| ULTRA Examples | Top events with forward returns |
| ULTRA False Positives | Band A losses analysis |
| ULTRA Missed Winners | Sub-65 large-gain events |

### 📡 TZ/WLNBB (`TZWLNBBPanel.jsx`)

TZ × WLNBB scanner. Controls Stock Stat file generation and TZ/WLNBB replay.

### 🧠 TZ Intel (`TZIntelligencePanel.jsx`)

ABR (Activation / Breaking / Retest) pattern scanner using `tz_intelligence/`. In v2.8, the table includes a `Final` column showing GO/WATCH_HIGH/WATCH/SHORT_WATCH/REJECT with color coding, plus StatComp, CompSeq4, Sample, and Reason columns. CSV export includes all 18+ diagnostic fields.

**Color coding:**

| Tier | Color |
|------|-------|
| GO | green-400 |
| WATCH_HIGH | emerald-300 |
| WATCH | yellow-300 |
| SHORT_WATCH | orange-400 |
| REJECT | red-400 |

### 🔄 Rare Reversal (`RareReversalPanel.jsx`)

4–6 bar T/Z rare reversal sequence miner with tier badges, completion progress, and CSV export.

### 🔢 Sequences (`SequenceScanPanel.jsx`)

Universe-wide N-bar T/Z sequence analyzer.

- Universe, timeframe, sequence length (2–6 bars), type (BULL/BEAR/ALL).
- Multi-horizon stats: Win 1D/3D/5D/9D · Avg 1D/3D/5D/9D · Med 1D.
- Sort by any horizon win rate or return.
- Breadth column shows how many tickers exhibited the sequence.
- CSV export with all 20 horizon columns.

### 💼 Portfolio (`PortfolioPanel.jsx`)

Paper-trading dashboard. Pending / Open / Closed tabs with Set-Entry-Prices UI and a server-side **Scan & Add** button (consumes the day's ULTRA results — no CSV upload required). Shows realized return per row and aggregate stats.

### 📈 Chart Obs (`ChartObsPanel.jsx`)

Chart Observation form for K-signal tagging. Enter ticker + date → system prefills T/Z signals, sequence, scores, prior bars from `stock_stat`; user confirms K-signal match (K1..K11), entry quality, and notes. Backed by `/api/obs/*`.

### How It Works (`HowItWorksPanel.jsx`)

Educational reference for signals and scoring.

### ⚙ Admin (`AdminPanel.jsx`)

Operational controls: scan history, manual triggers, pooled stats rebuild, stock-stat generation.

---

## Analyzer Modules

### ULTRA Signal Parser (`backend/ultra_signal_parser.py`)

Normalises two row shapes for the ULTRA Score formula:
- **Live ULTRA rows** — flat boolean keys: `row['buy_2809']=1`
- **Stock Stat rows** — compact label columns: `row['combo']=['BUY_2809','ROCKET']`

Emits a canonical dict of parsed signal flags consumed by `ultra_score.py`.

### TZ/WLNBB Analyzer (`backend/analyzers/tz_wlnbb/`)

Generates per-stock stat CSV for the Pullback Miner, Sequence Engine, and Pivot Swing Engine. Computes T/Z + L sequences with forward returns (ret_1d, ret_5d, ret_10d), MFE, MAE.

**Pine 260521 features (current production version):**
- **line3 — Body + Wick shape**: `bar_body_wick` column (e.g. `XS`, `MTB`, `SJ`)
  - body class: `X` (≥1.5× prev body), `S` (default), `M` (≤0.5× prev body)
  - wick class: `J` (doji), `TB` (heavy upper), `BB` (heavy lower), `F` (flat both)
- **line4 — Gap + Range (ATR-relative)**: `bar_gap_range` column (e.g. `G2-V`, `G1`, `C`)
  - gap class: `G1` (|gap| < 0.2×ATR), `G2` (< 0.5×ATR), `G3` (else)
  - range class: `V` (range > 1.5×ATR), `N` (default), `C` (range < 0.5×ATR)
- **line5 — VIX-Fix / PSAR / RSI2**: `bar_line5` column (e.g. `VX-PB-R2X`)
  - WVF: `VX` (spike), `VR` (range high)
  - PSAR: `PB` (bull), `PS` (bear) — `ta.sar(0.02, 0.02, 0.2)`
  - RSI(2): `R2X` (oversold reclaim), `R2D` (overbought drop), `R2L`, `R2H`
- **ATR**: Wilder smoothing `tr.ewm(alpha=1/14, adjust=False).mean()`
- **Z8 explicitly excluded** from Z_PRIORITY (not a real signal in this system)
- Active version constant: `TZ_WLNBB_VERSION = "260521_TZ_F_WLNBB_CMB_python_v2_line345"`

**Key files:**
- `config.py` — version, T_PRIORITY, Z_PRIORITY (13 entries, no Z8), suffix/L definitions
- `signal_logic.py` — per-bar T/Z compute (Pine-equivalent priority engine)
- `signal_extraction.py` — vectorised compute over a DataFrame; includes `_compute_psar`, `compute_line5`, `compute_atr_wilder`
- `stock_stat.py` — emits `stock_stat_tz_wlnbb_*.csv` with `tz_wlnbb_version` + `build_marker` columns
- `replay.py` — generates `replay_tz_wlnbb_*.zip` with 70+ analytics CSVs, the 17 pivot_swing/ files, BUILD_MARKER.txt at ZIP root, and config_snapshot.json
- `build_marker.py` — produces `<version>__sha-<git>__built-<UTC>` marker at import time

### Rare Reversal Miner (`backend/analyzers/rare_reversal/miner.py`)

4–6 bar T/Z reversal sequences matched against the master matrix. Evidence tiers: CONFIRMED_RARE, READY, FORMING, ANECDOTAL, WATCH.

### Pullback Pattern Miner (`backend/analyzers/pullback_miner/miner.py`)

Pullback entry patterns within T/Z + L sequences.

Evidence tiers:
- `CONFIRMED_PULLBACK` — ≥2 events, median 10d > 0, win ≥ 50%, fail ≤ 35%
- `ANECDOTAL_PULLBACK` — 1 event with positive return
- `NO_DATA` — no stat data
- `REJECT` — data exists but below thresholds

### TZ Intelligence (`backend/tz_intelligence/`)

ABR classifier using `ABR_rule_database.csv`. Classifies bars as Activation / Breaking / Retest using the master matrix. Also provides the `tz_intel_role` field read by ULTRA Score's D-component.

In v2.8, the TZ Intelligence layer gained a full statistical normalization pipeline: `stat_engine.py` labels statistical quality, `final_normalizer.py` merges all signals into a 5-tier final action, and `whitelist_builder.py` builds the lookup tables from replay data. See [TZ Intelligence Statistical Layer v2.8](#tz-intelligence-statistical-layer-v28) for details.

---

## Pivot Swing Character Analytics Engine

**Package:** `backend/analyzers/pivot_swing/`

A self-contained analytics engine that discovers signal behavior at confirmed swing pivots. **Does not modify** signal_logic.py, signal_extraction.py, WLNBB L1–L6 logic, or candle-pattern logic — purely read-only consumer of the stock_stat CSV pipeline.

### Modules

| File | Purpose |
|------|---------|
| `pivot_detector.py` | Confirmed pivot HIGH/LOW detection. `pivot_left=3`, `pivot_right=3` by default. `confirmed_at_index = pivot_idx + pivot_right` — pivot is not known until that many bars close. Zero lookahead leakage. |
| `swing_builder.py` | Alternating LOW→HIGH / HIGH→LOW swing segments. `min_swing_return_pct=3.0`, `min_swing_bars=2`. Same-direction duplicates collapse to the more extreme pivot. |
| `pivot_analytics.py` | Aggregated analytics pipeline. Accepts a list of stock_stat CSV paths and emits a single output set covering all tickers. |
| `runner.py` | CLI entry point (`python -m analyzers.pivot_swing.runner --csv-dir <dir> --out <out>`) |

### Pivot zone window

For every confirmed pivot, the engine extracts every signal at offsets `−5..+5` from the pivot price bar. Each record is tagged:
- `LIVE_SAFE` if `offset ≤ 0` (bar happened before/at the pivot extreme)
- `RESEARCH_ONLY` if `offset > 0` (uses bars after the pivot — must NOT be used in live trading rules)

Statistical role discovery is **agnostic**: T is not assumed bullish, Z is not assumed bearish. Roles are derived from observed pivot side at offset=0: `BULLISH_REVERSAL` (≥65% at LOW), `BEARISH_REVERSAL` (≥65% at HIGH), `NEUTRAL`.

### Confidence tiers

| Tier | Min count |
|------|-----------|
| HIGH | ≥ 100 |
| MEDIUM | ≥ 40 |
| LOW | ≥ 15 |
| RESEARCH_ONLY | < 15 |

### Output files (17 total, all aggregated across input tickers)

| File | Description |
|------|-------------|
| `pivot_swing_summary.csv` | One row per ticker: bars, pivot_lows, pivot_highs, swings, up/down split, avg swing return %, avg swing bars |
| `pivot_low_single_signal_stats.csv` | Per (offset, signal_field, signal_value) at LOW pivots: count + avg forward returns + confidence_tier + lookahead_safe |
| `pivot_high_single_signal_stats.csv` | Same shape, at HIGH pivots |
| `pivot_low_sequence_2bar_stats.csv` … `pivot_low_sequence_6bar_stats.csv` | Sequence patterns of length 2..6 at LOW pivots |
| `pivot_high_sequence_2bar_stats.csv` … `pivot_high_sequence_6bar_stats.csv` | Same at HIGH pivots |
| `pivot_zone_offset_stats.csv` | Aggregate counts + returns per (pivot_type, offset) bucket |
| `pivot_role_map.csv` | Per signal value: counts at LOW vs HIGH, discovered_role (BULLISH_REVERSAL / BEARISH_REVERSAL / NEUTRAL / RESEARCH_ONLY) |
| `pivot_scanner_rules_proposal.md` | Live-safe candidate rules (offset ≤ 0) for incorporation into the live scanner — recommended modules listed |
| `pivot_engine_audit_report.md` | Run parameters, per-ticker audit, version distribution, lookahead-safety policy, scope guarantees |

### Embedding in the replay ZIP

`replay.py::_embed_pivot_swing_in_zip()` builds a DataFrame from the `rows` arg, writes per-ticker temp CSVs, calls `run_pivot_analytics(csv_paths=…)`, and copies the 17 outputs into the replay ZIP under `pivot_swing/`. Embedding is non-fatal — a failure logs a warning and the ZIP is still produced.

---

## Build Marker & Artifact Audit

Every artifact emitted by the backend carries a **build marker** that uniquely identifies the code version that produced it:

```
260521_TZ_F_WLNBB_CMB_python_v2_line345__sha-<git_short_sha>__built-<UTC_TIMESTAMP>
```

Generated at import time by `backend/analyzers/tz_wlnbb/build_marker.py` from:
- `TZ_WLNBB_VERSION` constant in `config.py`
- short git SHA (via `git rev-parse --short HEAD` or `.git/HEAD` fallback)
- UTC timestamp at process boot

### Where the marker lands

| Artifact | Location of marker |
|----------|--------------------|
| Replay ZIP | `BUILD_MARKER.txt` at ZIP root (first line is the marker, body is `BUILD_INFO` JSON) |
| `replay_tz_wlnbb_metadata.json` | `build_marker` + `build_info` keys |
| `tz_wlnbb_config_snapshot.json` | `TZ_WLNBB_ANALYZER_VERSION` (matches the version portion) |
| `stock_stat_tz_wlnbb_*.csv` | new `build_marker` column on every row |
| `/api/tz-intelligence/scan` response | top-level `build_marker` + per-row `build_marker` |
| `/api/code-version` response | `build_marker` + `build_info` dict |

### Verifying deployed code matches uploaded artifacts

```
1. GET /api/code-version
   → note the build_marker value
2. Open the uploaded replay ZIP, read BUILD_MARKER.txt
3. If they match → artifact came from the deployed code
   If they differ → artifact is stale, rebuild required
```

`/api/artifact-audit` does this comparison automatically and returns:
- `active_code_build_marker`
- `zip_build_marker`
- `zip_built_with_active_code` (bool, must be `true` for a clean run)

### Bug-class guardrails

The deployed code carries two complementary fixes for the **`WATCH_HIGH:GATES_PASS` data bug**:

1. **Normalizer-level fix** (`final_normalizer.py:470–516`): `final_reason = GATES_PASS` is reserved strictly for rows where both `volume_gate_status == PASS` and `abr_gate_status == PASS`. Any modifier-promoted `WATCH_HIGH` row keeps its downgrade reasons.

2. **Post-condition repair** (`final_normalizer.py:516–535`): if any path emits `GATES_PASS` / `WATCH_HIGH:GATES_PASS` / `GO:GATES_PASS` while gates failed, the row is rewritten in-place to include the actual gate-failure reasons. Makes the bug impossible to ship.

3. **Defensive post-scan repair** (`main.py:/api/tz-intelligence/scan`): even if the on-disk CSV contains pre-fix rows, the scan endpoint re-checks every result before returning and rewrites stale `GATES_PASS` strings. `debug.post_scan_repair_count` reports how many rows were repaired.

### Latest-mode stale-row filter

`run_intelligence_scan()` accepts `max_stale_trading_days=2` (default). In `latest` mode it:
1. Computes `scan_as_of_date` as the most recent bar date across all tickers in the stock_stat CSV
2. For each ticker, computes `_count_trading_days_between(latest_date, scan_as_of_date)` (weekend-skip aware)
3. If > `max_stale_trading_days`, drops the ticker (or for `split` universe `require_all_tickers` mode, emits a `STALE_DATA` row)

`scan_as_of_date` and `stale_dropped` are surfaced in the response `debug` block.

---

## Deployment

### Railway

```toml
# railway.toml
[build]
builder = "DOCKERFILE"
dockerfilePath = "Dockerfile"

[deploy]
startCommand = "uvicorn backend.main:app --host 0.0.0.0 --port $PORT"
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `PORT` | HTTP port (default: 8080) |
| `DATABASE_URL` | PostgreSQL connection string |
| `POLYGON_API_KEY` | Polygon.io market data key |
| `MASSIVE_API_KEY` | Massive API key (all_us universe) |
| `ANTHROPIC_API_KEY` | Claude/Anthropic API key (backend only — never exposed to frontend) |
| `CLAUDE_MODEL` | Claude model ID override (e.g. `claude-sonnet-4-6`) |
| `USE_PG` | Set to `1` to use PostgreSQL instead of SQLite |

> **Security note:** `ANTHROPIC_API_KEY` must never appear in React code, Vite env vars (public prefix), localStorage, or any network payload visible in browser devtools. Backend uses it only via Python `os.environ`. The dashboard must remain functional via deterministic fallback if the key is absent or the Claude API is unavailable.

---

## Test Suite

Located in `tests/`. Run with `pytest tests/ -q`. **663 tests, all passing.**

| File | Focus | Tests |
|------|-------|-------|
| `test_ultra_score.py` | ULTRA Score: no-lookahead, band/priority v2, regime bonus, confluence caps, replay summaries | 32 |
| `test_ultra_engine.py` | ULTRA two-stage orchestrator | — |
| `test_ultra_signal_parser.py` | Compact label parser — live + Stock Stat shapes | — |
| `test_sequence_engine.py` | Sequence scanner: multi-horizon returns, state machine | 19 |
| `test_tz_wlnbb.py` | Signal extraction, replay, stock-stat generation | — |
| `test_tz_intelligence.py` | ABR classifier, pattern detection, matrix loading | — |
| `test_pullback_miner.py` | Pullback pattern mining | — |
| `test_rare_reversal.py` | Rare reversal mining | — |
| `test_profile_playbook.py` | Multi-timeframe profile analysis | — |
| `test_split_universe.py` | Universe definitions | — |

---

## Key Statistics

| Metric | Value |
|--------|-------|
| Version | 4.4.674 · API v2.8 |
| Backend modules | 33+ |
| Frontend components | 24 |
| API endpoints | 87+ |
| T/Z signal IDs | 26 |
| L-signal variants | 12 base + 8 combos + 10 WLNBB overlays |
| WLNBB volume buckets | 5 (VB/B/N/L/W) |
| TZ Intel final action tiers | 5 (GO/WATCH_HIGH/WATCH/SHORT_WATCH/REJECT) |
| TZ Intel diagnostic columns | 18+ |
| Static reject composites | 29 (hardcoded fallback) |
| Whitelist CSVs generated | 8 per replay run |
| Test count | 663 |
| Tabs | 20 |
| Scheduled scans/day | 3 (09:30, 12:30, 15:30 ET) |
