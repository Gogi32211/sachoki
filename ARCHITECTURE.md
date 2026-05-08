# Sachoki Screener — Architecture & Signal Reference

> Version 4.4.458 · API v2.1

---

## Table of Contents

1. [Overview](#overview)
2. [Directory Structure](#directory-structure)
3. [Backend Architecture](#backend-architecture)
4. [Signal Types Reference](#signal-types-reference)
5. [Scoring & Turbo Engine](#scoring--turbo-engine)
6. [API Endpoints](#api-endpoints)
7. [Frontend Tabs](#frontend-tabs)
8. [Analyzer Modules](#analyzer-modules)
9. [Deployment](#deployment)
10. [Test Suite](#test-suite)

---

## Overview

Sachoki is a real-time multi-timeframe stock screener built on FastAPI + React. It aggregates signals from a dozen independent engines (T/Z candle logic, L-signal volume patterns, GOG priority scoring, VABS volume absorption, Wyckoff phase detection, and more) into a single unified **TURBO_SCORE** (0–100). The UI exposes 16 analysis tabs covering scanning, prediction, correlation, backtesting, and sector rotation.

**Tech stack:**
- Backend: Python 3.11 · FastAPI 0.111 · APScheduler · yfinance / Polygon.io
- Frontend: React 18 · Vite 5 · Tailwind 3 · lightweight-charts 4
- Storage: SQLite (local) · PostgreSQL (production) · Redis (optional cache)
- Deploy: Docker (multi-stage) · Railway / Heroku (Procfile)

---

## Directory Structure

```
sachoki/
├── backend/                     # FastAPI application
│   ├── main.py                  # App entry point, all API routes (2,487 LOC)
│   ├── signal_engine.py         # T/Z signal computation (412 LOC)
│   ├── wlnbb_engine.py          # L-signal / WLNBB engine (300 LOC)
│   ├── gog_engine.py            # GOG priority engine (956 LOC)
│   ├── vabs_engine.py           # Volume absorption signals (229 LOC)
│   ├── combo_engine.py          # B-signal combo patterns (300 LOC)
│   ├── turbo_engine.py          # Turbo multi-engine scoring (2,127 LOC)
│   ├── scanner.py               # Scan orchestrator + universe management (1,337 LOC)
│   ├── profile_playbook.py      # Multi-timeframe profile analysis (896 LOC)
│   ├── replay_engine.py         # Backtest / replay engine (2,227 LOC)
│   ├── rtb_engine.py            # Range / Trend / Breakout (844 LOC)
│   ├── tpsl_engine.py           # Take profit / stop loss (2,046 LOC)
│   ├── br_engine.py             # Bollinger Range breakouts (596 LOC)
│   ├── sector_engine.py         # Sector rotation + RRG (531 LOC)
│   ├── canonical_scoring_engine.py  # Canonical score computation (391 LOC)
│   ├── predictor.py             # T/Z prediction (309 LOC)
│   ├── wyckoff_engine.py        # Wyckoff phase detection (345 LOC)
│   ├── para_engine.py           # Parabolic SAR patterns (329 LOC)
│   ├── fly_engine.py            # Flyby / breakaway patterns (260 LOC)
│   ├── power_engine.py          # Price-action power analysis (347 LOC)
│   ├── f_engine.py              # Wyckoff F-strength patterns (300 LOC)
│   ├── data.py                  # yfinance OHLCV fetching (110 LOC)
│   ├── data_polygon.py          # Polygon.io data provider (265 LOC)
│   ├── indicators.py            # RSI, CCI, ATR, normalization (234 LOC)
│   ├── db.py                    # SQLite / PostgreSQL helpers (184 LOC)
│   ├── analyzers/
│   │   ├── rare_reversal/
│   │   │   └── miner.py         # Rare reversal pattern miner (731 LOC)
│   │   ├── pullback_miner/
│   │   │   └── miner.py         # Pullback pattern miner (992 LOC)
│   │   └── tz_wlnbb/
│   │       ├── signal_extraction.py  # Signal filter + extraction (177 LOC)
│   │       ├── signal_logic.py       # T/Z + WLNBB firing rules (533 LOC)
│   │       ├── stock_stat.py         # Per-stock stat builder (693 LOC)
│   │       ├── replay.py             # TZ/WLNBB backtest replay (3,242 LOC)
│   │       ├── config.py             # Module configuration (76 LOC)
│   │       └── schemas.py            # Pydantic schemas (20 LOC)
│   └── tz_intelligence/
│       ├── classifier.py         # ABR/pattern classifier (1,647 LOC)
│       ├── abr_classifier.py     # Activation/Breaking/Retest rules (534 LOC)
│       ├── scanner.py            # TZ Intelligence scanner (573 LOC)
│       ├── matrix_loader.py      # Signal intelligence matrix loader (330 LOC)
│       └── ABR_rule_database.csv # ABR rule definitions
├── frontend/
│   └── src/
│       ├── App.jsx              # Main shell, tab routing, global state
│       ├── api.js               # API client utilities
│       ├── turboCache.js        # Client-side turbo result cache
│       └── components/          # 20 React panel components (see Tab Reference)
├── tests/                       # Pytest test suite (~8,000 LOC)
├── tz_intelligence_package/     # TZ signal intelligence data & guides
├── signal_engine.py             # Shared signal engine (root)
├── Dockerfile                   # Multi-stage build (Node 20 → Python 3.11)
├── requirements.txt             # Root-level numpy/pandas
├── Procfile                     # Railway / Heroku process definition
└── railway.toml                 # Railway deployment config
```

---

## Backend Architecture

### Request Flow

```
Browser → React (Vite) ──HTTP──► FastAPI (main.py)
                                    │
                        ┌───────────┼───────────────────┐
                        ▼           ▼                   ▼
                  signal_engine  turbo_engine      scanner.py
                  wlnbb_engine   gog_engine        replay_engine
                  vabs_engine    combo_engine      sector_engine
                        │           │                   │
                        └───────────┴───────────────────┘
                                    │
                              pandas DataFrames
                                    │
                         yfinance / Polygon.io OHLCV
```

### Scheduled Scans (APScheduler)

Turbo and combined scans run automatically at **09:30, 12:30, 15:30 ET** on weekdays. Results are cached in SQLite and served instantly to the frontend without re-computing on each request.

### Universe Definitions

| Key | Description | Size |
|-----|-------------|------|
| `sp500` | S&P 500 large-caps | ~500 |
| `nasdaq` | NASDAQ stocks | ~4,000 |
| `russell2k` | Russell 2K small-caps | ~2,000 |
| `all_us` | All US equities (Massive API) | ~8,000 |
| `split` | Reverse-split window (D-7 → D+90) | dynamic |

---

## Signal Types Reference

### T/Z Signals — Bullish (T) and Bearish (Z)

T/Z signals classify each price bar based on its open/close relationship to the prior bar. They are the foundation of all other scoring.

**Signal Priority Codes:** Each bar gets a `BC` (bull code) and `ZC` (bear code). Higher code = stronger signal.

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
| T9 | 9 | Inside bull — bull bar fully inside prior bar (no wick overlap) |
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
| Z7 | 21 | Doji — open equals close, no other signal qualifies |
| Z9 | 22 | Inside bear — bear bar inside prior bar |
| Z10 | 23 | Inside continuation — bear bar inside prior bear bar |
| Z11 | 24 | Mid-close bear — closes between prior open and close |
| Z12 | 25 | Higher-open continuation — bear bar after bull bar, higher open |

**Signal ID 0** = NONE (neutral bar).

---

### L-Signals — Volume × Price Classification

L-signals classify each bar by the relationship between volume direction and price direction. They are computed by `wlnbb_engine.py`.

#### Base L-Signals

| Signal | Condition |
|--------|-----------|
| L1 | Volume ↓, Close ↑ — bullish absorption (effort down, result up) |
| L2 | Volume ↓, No new low — support holding on low volume |
| L3 | Volume ↑, Close ↑ — demand (effort up, result up) |
| L4 | Volume ↑, No new high — supply appearing (effort up, result stalls) |
| L5 | Volume ↓, Close ↓ — distribution on low volume |
| L6 | Volume ↑, Close ↓ — selling pressure (effort up, result down) |

#### L-Combo Signals

| Signal | Condition | Interpretation |
|--------|-----------|----------------|
| L34 | L3 ∧ L4 ∧ close ≥ open | Volume surge, no breakout, candle bullish — coiling |
| L43 | L6 ∧ L4 ∧ close > open | Volume up, supply absorbed, close positive |
| L64 | L6 ∧ L4 | Volume up, no new high — supply pressure |
| L22 | L3 ∧ L4 ∧ close < open | Volume surge, no breakout, candle bearish — distribution |
| L1L2 | L1 ∧ L2 | Double absorption — strong support |
| L2L5 | L2 ∧ L5 | Low-volume down — weak selling |
| L555 | L5 × 3 consecutive | Three-bar distribution sequence |

#### WLNBB Indicator Signals

These overlay WLNBB (Volume Bollinger Bands) data on top of L-signals:

| Signal | Condition | Meaning |
|--------|-----------|---------|
| BLUE | Vol Z-score ≥ 1.1 ∧ RSI range ≤ 5.0 | Premium quality bar — high volume, flat RSI (controlled) |
| FRI34 | BLUE ∧ L34 | BLUE with coiling combo |
| FRI43 | BLUE ∧ L43 | BLUE with absorbed supply combo |
| FRI64 | BLUE ∧ L64 | BLUE with supply pressure combo |
| UI | BLUE appears ≥ 2× in last 10 bars | Unnamed Indicator — sustained premium accumulation |
| CCI_READY | CCI in [−110, −50], rising, range tight | CCI softening before reversal |
| PRE_PUMP | VSA absorption ≥ 2 bars, 6-bar cooldown | Pump precursor signature |
| CCI_BLUE_TURN | BLUE ∧ CCI crossing zero | Recovery + volume confirmation |
| FUCHSIA_RH | RSI at 50-bar high ∧ volume down | Overbought divergence |
| FUCHSIA_RL | RSI at 50-bar low ∧ volume down | Oversold with drying volume |

---

### VABS Signals — Volume Absorption & Breakout

Computed by `vabs_engine.py`. Based on spread (candle range), volume bucket, close location value (CLV), and bar-to-bar changes.

| Signal | Description |
|--------|-------------|
| ABS | Absorption spike — volume bucket jumps ≥ 2 levels in one bar |
| CLIMB | Volume climb — 3 consecutive bars of rising bucket level |
| LOAD | Load signature — specific volume + spread + CLV combination indicating accumulation |
| NS | Narrow Space — narrow spread + low volume + down close + good close position |
| ND | Narrow Down — narrow spread + low volume + up direction + close low in bar |
| BC | Breakout Climax — wide spread + high volume + up + good close |
| SC | Selling Climax — wide spread + high volume + down + bad close |
| SQ | Squeeze — high volume + narrow spread (supply/demand standoff) |
| VBO_UP | Volume Breakout Up — closes above 5–10 bar high on strong volume |
| VBO_DN | Volume Breakout Down — closes below 5–10 bar low on strong volume |

---

### GOG Signals — Goldrush Priority Engine

Computed by `gog_engine.py`. GOG scores are additive and stack with T/Z signals. GOG_TIER is the highest active tier.

| Signal | Points | Condition |
|--------|--------|-----------|
| GOG1 | 50 | Top-tier Goldrush pattern (volume + T/Z alignment) |
| GOG2 | 46 | Strong Goldrush pattern |
| GOG3 | 42 | Moderate Goldrush pattern |
| G1P / G2P / G3P | GOG + premium | GOG tier + recent volume premium bar |
| G1L / G2L / G3L | GOG + load | GOG tier + recent VSA load bar |
| G1C / G2C / G3C | GOG + context | GOG tier + completion context |

**SETUP tokens** (space-separated in bar data): `A` (Aggressive), `SM` (Smart Money), `N` (Normal), `MX` (Mixed)

**CONTEXT tokens**: `LD` (Load), `LDS` (Load Strong), `LDC` (Load Complete), `LDP` (Load Premium), `LRC` (Load Recovery C), `LRP` (Load Recovery P), `WRC` (Wrap Recovery C), `F8C` (F8 Context), `SQB` (Squeeze Breakout), `BCT` (BC Turn), `SVS` (SVS Pattern)

---

### B-Signals — Combo Buy Patterns

Computed by `combo_engine.py`. B-signals identify 2–6 bar sequences of T/Z priority codes that historically precede bullish moves.

| Signal | Pattern | Description |
|--------|---------|-------------|
| B1 | T5[1] → T2 or Z11[2] → T3/T1/T1G | Basic momentum combo |
| B2 | (any T)[2] → T4 | Any two bulls → engulfing |
| B3–B11 | Various multi-bar sequences | Increasingly specific combinations |

---

### F-Signals — Wyckoff Force Patterns

Computed by `f_engine.py`. Track Wyckoff force/strength progression:

| Signal | Description |
|--------|-------------|
| F1 | Preliminary Support |
| F2 | Selling Climax (VSA SC) |
| F3 | Automatic Rally (first bounce off SC) |
| F4 | Secondary Test |
| F5–F8 | Accumulation phases (Springs, Tests, SOS) |
| F10 | Last Point of Support |
| PREP | Pre-markup preparation bar |
| PARA+ | Parabolic up extension |
| RTEST | Retest of breakout level |

---

### RTB Phases

Computed by `rtb_engine.py`. Each ticker gets an RTB phase code:

| Phase | Description |
|-------|-------------|
| A | Accumulation / Range bottom |
| B | Breakout from range |
| C | Continuation / trending |
| D | Distribution top |
| 0 | Unclassified |

---

### Profile Categories

Computed by `profile_playbook.py`. Multi-timeframe playbook classification:

| Category | Description |
|----------|-------------|
| SWEET_SPOT | `sweet_spot_active=true` and `late_warning=false` — optimal entry zone |
| BUILDING | Pattern building toward breakout — not yet ready |
| WATCH | On watchlist for future setup — no immediate signal |
| LATE | Late-stage — risk/reward no longer favorable |

---

## Scoring & Turbo Engine

The **TURBO_SCORE** (0–100) is a weighted aggregate across all signal engines, computed by `turbo_engine.py`.

### Score Component Weights (approximate)

| Engine | Weight | Source |
|--------|--------|--------|
| T/Z signal tier | High | signal_engine |
| GOG score | High | gog_engine |
| VABS score | Medium | vabs_engine |
| WLNBB L-signal | Medium | wlnbb_engine |
| RTB phase | Medium | rtb_engine |
| TPSL (target quality) | Low-medium | tpsl_engine |
| Wyckoff phase | Low | wyckoff_engine |
| Power | Low | power_engine |
| Bollinger Range | Low | br_engine |

### Score Bands (UI filter)

| Band | Range |
|------|-------|
| All | 0–100 (no filter) |
| 0–20 | Weak / noise |
| 21–40 | Low quality |
| 41–60 | Moderate |
| 61–80 | Good |
| 81–100 | Top tier |

### Canonical Score

A separate `signal_score` / `canonical_score` (from `canonical_scoring_engine.py`) provides a rules-based alternative to the ML-weighted turbo score. It is displayed as the `GOG_SCORE` in bar detail views. Profile score (`profile_score`) is **additive context only** and does not replace the canonical score.

---

## API Endpoints

All endpoints are prefixed `/api/`. Backend serves on port **8080**.

### Health & Config

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Service status, version |
| GET | `/api/config` | Global configuration |
| GET | `/api/settings` | Load persisted settings |
| POST | `/api/settings` | Save settings |

### Ticker Data

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/ticker-info/{ticker}` | Name, sector, industry (cached) |
| POST | `/api/ticker-info-batch` | Batch info for up to 200 tickers |
| GET | `/api/signals/{ticker}` | T/Z signals (tf, bars=150) |
| GET | `/api/wlnbb/{ticker}` | WLNBB L-signals + BLUE/CCI_READY |
| GET | `/api/bar_signals/{ticker}` | Per-bar full signal breakdown |
| GET | `/api/watchlist` | Real-time scan for comma-separated tickers |

### Prediction & Stats

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/predict/{ticker}` | T/Z prediction + TZ matrix + L-prediction |
| GET | `/api/l-predict/{ticker}` | L-signal prediction only |
| GET | `/api/pooled-predict/{ticker}` | Prediction using universe pooled stats |
| POST | `/api/pooled-stats/build` | Build pooled stats (background task) |
| GET | `/api/signal-stats/{ticker}` | Per-signal win% and return stats |
| GET | `/api/tz-l-stats/{ticker}` | T/Z × L matrix + SPY/QQQ benchmarks |
| GET | `/api/signal-correlation` | Signal co-occurrence correlation matrix |

### Scanning

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/scan/results` | T/Z scanner results |
| POST | `/api/scan/trigger` | Start background T/Z scan |
| GET | `/api/combined-scan` | Multi-engine aggregated results |
| GET | `/api/turbo-scan` | Turbo scan results (ranked by TURBO_SCORE) |
| POST | `/api/turbo-scan/trigger` | Start turbo scan |
| GET | `/api/turbo-analyze/{ticker}` | Deep turbo breakdown for single ticker |
| GET | `/api/combo-scan` | B-signal combo scan results |
| POST | `/api/combo-scan/trigger` | Start combo scan |
| GET | `/api/br-scan` | Bollinger Range scan |
| GET | `/api/power-scan` | Power engine scan |
| GET | `/api/pump-combos` | Pump combo results |

### Sectors

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/sectors/overview` | All-sector returns + strength |
| GET | `/api/sectors/rrg` | Relative Rotation Graph data (trail=12) |
| GET | `/api/sectors/heatmap` | Heatmap (metric: return_1d, volatility, …) |
| GET | `/api/sectors/macro` | Macro sector analysis |
| GET | `/api/sectors/{etf}` | Single sector ETF detail |

### Replay / Backtest

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/replay/run` | Run backtest (tf, universe) |
| GET | `/api/replay/reports` | List result reports |
| GET | `/api/replay/report/{name}` | Get report (paginated, page_size=500) |
| GET | `/api/replay/export/{name}` | Export report as CSV |

### TZ/WLNBB Analyzer

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tz-wlnbb/scan` | TZ × WLNBB scan results |
| POST | `/api/tz-wlnbb/generate-stock-stat` | Generate per-stock stat CSV |
| GET | `/api/tz-wlnbb/status` | Generation progress |
| POST | `/api/tz-wlnbb/replay` | Run TZ/WLNBB replay |
| GET | `/api/tz-wlnbb/download/{filename}` | Download stock-stat CSV |

### Specialized Miners

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/rare-reversal/scan` | Rare reversal pattern scan |
| GET | `/api/pullback-miner/scan` | Pullback pattern scan |
| GET | `/api/pullback-miner/report` | Pullback pattern report (writes CSV) |

### TZ Intelligence

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tz-intelligence/scan` | ABR classification scan |

### Admin

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/admin/scan-history` | Scan history log |
| POST | `/api/admin/scan-start` | Manual scan trigger |
| POST | `/api/stock-stat/trigger` | Generate stock-stat files |

---

## Frontend Tabs

All tabs are defined in `App.jsx` and correspond to panel components in `frontend/src/components/`.

### ⚡ TURBO (`TurboScanPanel.jsx`)

High-speed multi-engine scan ranked by TURBO_SCORE. Features:
- **Universe selector**: sp500, nasdaq, russell2k, all_us, split
- **Timeframe**: 1d, 4h, 1h, 30m, 15m
- **Direction filter**: ALL / BULL / BEAR
- **Score bands** (multi-select): All · 0–20 · 21–40 · 41–60 · 61–80 · 81–100
- **Signal filters** (AND logic): any combination of GOG, VABS, Wyckoff, Combo, L-sig, RTB, Fly, RSI signals
- **Profile filters**: ⭐ Sweet Spot · ↑ Building · 👁 Watch
- **Volume filter**: min/max avg daily volume
- **Sector filter**: by ETF sector code (XLC, XLY, …)
- **RTB phase filter**: A / B / C / D
- **Lookback**: signal must have fired within N bars
- **Export**: descriptive filename encoding all active filters
- Always-mounted (scan results survive tab switches)

### ⭐ Watchlist (`PersonalWatchlistPanel.jsx`)

Personal watchlist of saved tickers. Shows current signals and scores per ticker. Supports add/remove.

### Combined Scan (`CombinedScanPanel.jsx`)

Aggregated scan results from all engines. Tabbed by signal family. Uses `effectiveScoreCol` to switch between turbo/canonical scores.

### Predictor (`PredictorPanel.jsx`)

Statistical T/Z signal predictor for a single ticker. Displays:
- Next-bar signal probability matrix
- Historical T/Z sequence analysis
- L-signal prediction (next WLNBB signal)
- Benchmark comparison vs SPY/QQQ

### T/Z Scanner (`ScannerPanel.jsx`)

Traditional T/Z signal scan across the universe. Filter by signal type, timeframe, min score. Results table with signal badges.

### T/Z × L Stats (`TZLStatsPanel.jsx`)

Correlation matrix between T/Z signals and L-signals for a single ticker. Shows win rates, return stats, and benchmark comparisons.

### 📊 Corr (`SignalCorrelPanel.jsx`)

Universe-wide signal co-occurrence correlation heatmap. Identifies which signals tend to fire together.

### 📋 Superchart (`SuperchartPanel.jsx`)

Dense multi-row candle view with overlaid signal data. Rows include:
- OHLCV candles
- T/Z signal per bar
- L-signal per bar
- B-signal combos
- GOG tier + SETUP + CONTEXT tokens
- F-signals (Wyckoff force)
- Fly patterns
- RTB phase
- TPSL targets

The Superchart drives the global CandleChart — selecting a ticker/tf here updates the main chart.

### 🌐 Sectors (`SectorAnalysisPanel.jsx`)

Sector rotation analysis:
- **Overview**: returns, strength per sector ETF
- **RRG** (Relative Rotation Graph): momentum + relative strength scatter
- **Heatmap**: configurable metric heatmap
- **Macro**: cross-sector macro analysis

### 🔍 Analyze (`TickerAnalysisPanel.jsx`)

Deep single-ticker analysis across all engine families. Breakdown by engine type with signal timeline. Can add result to watchlist.

### 🔬 Replay (`ReplayPanel.jsx`)

Backtest results viewer. Select universe + timeframe, view per-signal win rates, return distributions, and equity curves. Paginated CSV export.

### 📡 TZ/WLNBB (`TZWLNBBPanel.jsx`)

TZ × WLNBB cross-signal scanner. Also controls:
- Stock-stat file generation (feeds Pullback Miner and Rare Reversal Miner)
- TZ/WLNBB-specific replay/backtest

### 🧠 TZ Intel (`TZIntelligencePanel.jsx`)

ABR (Activation / Breaking / Retest) pattern scanner powered by `tz_intelligence/`. Classifies each bar into ABR phase using a rule database. Shows pattern confidence and historical stats.

### 🔄 Rare Reversal (`RareReversalPanel.jsx`)

Mines rare 4–6 bar T/Z reversal sequences. Extends standard SEQ4 patterns left by 1–2 bars (ext5, ext6). Anchored to master matrix data. Shows:
- Tier badges (RARE / UNCOMMON / COMMON)
- Completion progress bar
- Expandable pattern detail with bottom metrics
- CSV export

### How It Works (`HowItWorksPanel.jsx`)

Educational reference: signal definitions, scoring explanation, engine descriptions.

### ⚙ Admin (`AdminPanel.jsx`)

Operational controls:
- Scan history log
- Manual scan trigger
- Pooled stats rebuild
- Stock-stat generation

---

## Analyzer Modules

### TZ/WLNBB Analyzer (`backend/analyzers/tz_wlnbb/`)

Generates per-stock statistical files (`stock_stat`) used by the Pullback Miner and other modules. For each ticker:
1. Loads OHLCV + signals
2. Computes T/Z + L-signal sequences with timestamps
3. Calculates forward returns (`ret_1d`, `ret_5d`, `ret_10d`), MFE, MAE
4. Saves compressed CSV for fast downstream lookup

### Rare Reversal Miner (`backend/analyzers/rare_reversal/miner.py`)

Identifies rare 4–6 bar T/Z reversal sequences. Key concepts:
- **base4_key**: 4-bar T/Z sequence (e.g. `Z4|T2|Z3|T1`)
- **extended5_key**: 5-bar sequence (extends base4 left by 1)
- **extended6_key**: 6-bar sequence (extends base4 left by 2)
- Matches against master matrix (`TZ_SIGNAL_INTELLIGENCE_master_matrix_seed.csv`)
- **Bottom metrics**: sequence low offset, 20-bar low test, return from low
- Entry: `run_rare_reversal_scan(universe, tf, min_price, max_price, limit)`

### Pullback Pattern Miner (`backend/analyzers/pullback_miner/miner.py`)

Mines pullback entry patterns within T/Z + L-signal sequences. Key constants:

```python
_READY_SIGNALS      = {"Z5","Z9","Z3","Z4","Z6","Z1G","Z2G"}   # pullback setup
_CONFIRMING_SIGNALS = {"T1","T2","T2G","T3","T9"}               # confirmation
_GO_SIGNALS         = {"T4","T5","T6","T11","T12"}              # entry trigger
```

TZL key format uses `+` separator: `Z5|T2|Z3|T1+L34`

Evidence tiers:
- `CONFIRMED_PULLBACK` — ≥2 events, median 10d return > 0, win rate ≥ 50%, fail rate ≤ 35%
- `ANECDOTAL_PULLBACK` — 1 event with positive return
- `NO_DATA` — no stat data available
- `REJECT` — data exists but doesn't meet thresholds

### TZ Intelligence (`backend/tz_intelligence/`)

ABR classifier using a rule database (`ABR_rule_database.csv`). Classifies each bar as:
- **Activation** — signal fires for the first time in a sequence
- **Breaking** — signal breaks through a key level
- **Retest** — signal retests a prior level

The matrix loader reads `TZ_SIGNAL_INTELLIGENCE_master_matrix_seed.csv` and provides lookup by `(universe, pattern)` key.

---

## Deployment

### Docker (Production)

```dockerfile
# Stage 1: Build frontend
FROM node:20 as frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Python runtime
FROM python:3.11-slim
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install -r requirements.txt
COPY backend/ ./backend/
COPY --from=frontend-builder /app/frontend/dist ./static
EXPOSE 8080
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `PORT` | HTTP port (default: 8080) |
| `DB_PATH` | SQLite database path |
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `POLYGON_API_KEY` | Polygon.io market data key |
| `MASSIVE_API_KEY` | Massive API key (for all_us universe) |

---

## Test Suite

Located in `tests/`. Run with `pytest tests/`.

| File | LOC | Coverage |
|------|-----|----------|
| `test_tz_wlnbb.py` | 1,989 | Signal extraction, replay, stock-stat generation |
| `test_tz_intelligence.py` | 3,746 | ABR classifier, pattern detection, matrix loading |
| `test_pullback_miner.py` | 900 | Pullback pattern mining (96 tests) |
| `test_rare_reversal.py` | 544 | Rare reversal mining (45 tests) |
| `test_profile_playbook.py` | 1,250 | Multi-timeframe profile analysis |
| `test_split_universe.py` | 1,020 | Universe definitions |
| `test_signal_engine.py` (root) | 690 | T/Z signal computation |
| `test_signals.py` (root) | 280 | Signal edge cases (doji, engulf, inside bars) |

---

## Key Statistics

| Metric | Value |
|--------|-------|
| Backend LOC | ~21,000 |
| Frontend components | 20 |
| API endpoints | 60+ |
| T/Z signal IDs | 26 (T1–T12, Z1–Z12, Z7 doji) |
| L-signal variants | 12 base + 8 combos + 10 WLNBB overlays |
| GOG/VABS variants | 20+ |
| Analyzer modules | 3 (TZ/WLNBB, Rare Reversal, Pullback Miner) |
| Scheduled scans/day | 3 (09:30, 12:30, 15:30 ET) |
| Test files | 8 |
