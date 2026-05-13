"""
signal_replay_migration.py — Creates Signal Replay / Research engine tables.

Called from main.py lifespan. Safe to call multiple times (IF NOT EXISTS).

Tables:
  signal_replay_runs        — one row per replay/statistics run
  replay_signal_events      — one row per signal event (symbol × date × signal)
  replay_signal_outcomes    — one row per event × horizon
  replay_signal_statistics  — aggregated per-signal stats

Phase 2 will add: replay_pattern_statistics, replay_filter_impact_statistics.
"""
from __future__ import annotations
import logging
from db import get_db, USE_PG

log = logging.getLogger(__name__)

_DDL_PG = """
CREATE TABLE IF NOT EXISTS signal_replay_runs (
    id                  SERIAL PRIMARY KEY,
    status              VARCHAR(20) NOT NULL DEFAULT 'running',
    mode                VARCHAR(20) NOT NULL,
    universe            VARCHAR(20) NOT NULL,
    timeframe           VARCHAR(8)  NOT NULL DEFAULT '1d',
    as_of_date          DATE,
    start_date          DATE,
    end_date            DATE,
    event_scope         VARCHAR(40) DEFAULT 'all_signals',
    min_price           NUMERIC(10,4),
    min_volume          BIGINT,
    min_dollar_volume   NUMERIC(18,2),
    benchmark_symbol    VARCHAR(10) DEFAULT 'QQQ',
    total_days          INTEGER DEFAULT 0,
    days_completed      INTEGER DEFAULT 0,
    total_symbols       INTEGER DEFAULT 0,
    symbols_completed   INTEGER DEFAULT 0,
    total_events        INTEGER DEFAULT 0,
    total_outcomes      INTEGER DEFAULT 0,
    total_statistics_rows INTEGER DEFAULT 0,
    settings_json       TEXT,
    error_message       TEXT,
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_srr_status     ON signal_replay_runs(status);
CREATE INDEX IF NOT EXISTS idx_srr_created_at ON signal_replay_runs(created_at);

CREATE TABLE IF NOT EXISTS replay_signal_events (
    id                      BIGSERIAL PRIMARY KEY,
    replay_run_id           INTEGER NOT NULL,
    scan_date               DATE NOT NULL,
    symbol                  VARCHAR(20) NOT NULL,
    universe                VARCHAR(20),
    timeframe               VARCHAR(8) DEFAULT '1d',
    open                    NUMERIC(14,4),
    high                    NUMERIC(14,4),
    low                     NUMERIC(14,4),
    close                   NUMERIC(14,4),
    volume                  BIGINT,
    dollar_volume           NUMERIC(18,2),
    event_signal            VARCHAR(40) NOT NULL,
    event_signal_family     VARCHAR(20),
    event_signal_type       VARCHAR(40),
    event_direction         VARCHAR(12),
    final_signal            VARCHAR(40),
    raw_signal              VARCHAR(40),
    tz_signal               VARCHAR(40),
    prev1_signal            VARCHAR(40),
    prev2_signal            VARCHAR(40),
    prev3_signal            VARCHAR(40),
    prev4_signal            VARCHAR(40),
    prev5_signal            VARCHAR(40),
    prev6_signal            VARCHAR(40),
    prev7_signal            VARCHAR(40),
    prev8_signal            VARCHAR(40),
    prev9_signal            VARCHAR(40),
    prev10_signal           VARCHAR(40),
    sequence_2bar           VARCHAR(80),
    sequence_3bar           VARCHAR(120),
    sequence_4bar           VARCHAR(160),
    sequence_5bar           VARCHAR(200),
    sequence_7bar           VARCHAR(280),
    sequence_10bar          VARCHAR(400),
    signals_last_3d_json    TEXT,
    signals_last_5d_json    TEXT,
    signals_last_10d_json   TEXT,
    t_signals_last_3d_count INTEGER DEFAULT 0,
    t_signals_last_5d_count INTEGER DEFAULT 0,
    t_signals_last_10d_count INTEGER DEFAULT 0,
    z_signals_last_3d_count INTEGER DEFAULT 0,
    z_signals_last_5d_count INTEGER DEFAULT 0,
    z_signals_last_10d_count INTEGER DEFAULT 0,
    last_t_signal           VARCHAR(40),
    last_z_signal           VARCHAR(40),
    days_since_last_t       INTEGER,
    days_since_last_z       INTEGER,
    had_t_last_3d           BOOLEAN DEFAULT FALSE,
    had_z_last_3d           BOOLEAN DEFAULT FALSE,
    had_t_last_5d           BOOLEAN DEFAULT FALSE,
    had_z_last_5d           BOOLEAN DEFAULT FALSE,
    had_wlnbb_l_last_5d     BOOLEAN DEFAULT FALSE,
    had_volume_burst_last_5d BOOLEAN DEFAULT FALSE,
    had_ema50_reclaim_last_5d BOOLEAN DEFAULT FALSE,
    had_pullback_before_signal BOOLEAN DEFAULT FALSE,
    wlnbb_bucket            VARCHAR(20),
    l_signal                VARCHAR(40),
    volume_bucket           VARCHAR(20),
    wick_suffix             VARCHAR(20),
    candle_color            VARCHAR(8),
    body_pct                NUMERIC(8,4),
    upper_wick_pct          NUMERIC(8,4),
    lower_wick_pct          NUMERIC(8,4),
    range_pct               NUMERIC(8,4),
    gap_pct                 NUMERIC(8,4),
    ema20                   NUMERIC(14,4),
    ema50                   NUMERIC(14,4),
    ema89                   NUMERIC(14,4),
    ema200                  NUMERIC(14,4),
    ema20_state             VARCHAR(20),
    ema50_state             VARCHAR(20),
    ema89_state             VARCHAR(20),
    ema200_state            VARCHAR(20),
    ema_reclaim_20          BOOLEAN DEFAULT FALSE,
    ema_reclaim_50          BOOLEAN DEFAULT FALSE,
    ema_reclaim_89          BOOLEAN DEFAULT FALSE,
    ema_reclaim_200         BOOLEAN DEFAULT FALSE,
    price_above_ema20       BOOLEAN DEFAULT FALSE,
    price_above_ema50       BOOLEAN DEFAULT FALSE,
    price_above_ema89       BOOLEAN DEFAULT FALSE,
    price_above_ema200      BOOLEAN DEFAULT FALSE,
    price_pos_4bar          NUMERIC(6,4),
    price_pos_10bar         NUMERIC(6,4),
    price_pos_20bar         NUMERIC(6,4),
    price_pos_50bar         NUMERIC(6,4),
    price_pos_4bar_bucket   VARCHAR(20),
    price_pos_10bar_bucket  VARCHAR(20),
    price_pos_20bar_bucket  VARCHAR(20),
    price_pos_50bar_bucket  VARCHAR(20),
    atr_pct                 NUMERIC(8,4),
    volatility_bucket       VARCHAR(20),
    relative_volume         NUMERIC(8,4),
    relative_volume_bucket  VARCHAR(20),
    dollar_volume_bucket    VARCHAR(20),
    liquidity_bucket        VARCHAR(20),
    abr_category            VARCHAR(20),
    abr_med10d              NUMERIC(10,4),
    abr_fail10d             NUMERIC(10,4),
    abr_prev1_quality       VARCHAR(20),
    abr_prev2_quality       VARCHAR(20),
    abr_sequence_key        VARCHAR(80),
    role                    VARCHAR(40),
    matched_status          VARCHAR(40),
    score                   NUMERIC(10,4),
    score_bucket            VARCHAR(20),
    market_regime           VARCHAR(40),
    sector                  VARCHAR(40),
    industry                VARCHAR(60),
    event_snapshot_json     TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rse_run         ON replay_signal_events(replay_run_id);
CREATE INDEX IF NOT EXISTS idx_rse_run_signal  ON replay_signal_events(replay_run_id, event_signal);
CREATE INDEX IF NOT EXISTS idx_rse_run_family  ON replay_signal_events(replay_run_id, event_signal_family);
CREATE INDEX IF NOT EXISTS idx_rse_run_symbol  ON replay_signal_events(replay_run_id, symbol);
CREATE INDEX IF NOT EXISTS idx_rse_run_date    ON replay_signal_events(replay_run_id, scan_date);
CREATE INDEX IF NOT EXISTS idx_rse_seq4        ON replay_signal_events(replay_run_id, sequence_4bar);

CREATE TABLE IF NOT EXISTS replay_signal_outcomes (
    id                  BIGSERIAL PRIMARY KEY,
    replay_run_id       INTEGER NOT NULL,
    signal_event_id     BIGINT NOT NULL,
    symbol              VARCHAR(20),
    scan_date           DATE,
    horizon             VARCHAR(8) NOT NULL,
    entry_price         NUMERIC(14,4),
    exit_price          NUMERIC(14,4),
    return_pct          NUMERIC(10,4),
    max_high            NUMERIC(14,4),
    max_gain_pct        NUMERIC(10,4),
    max_gain_day        INTEGER,
    max_gain_date       DATE,
    min_low             NUMERIC(14,4),
    max_drawdown_pct    NUMERIC(10,4),
    max_drawdown_day    INTEGER,
    max_drawdown_date   DATE,
    hit_5pct            BOOLEAN DEFAULT FALSE,
    hit_10pct           BOOLEAN DEFAULT FALSE,
    hit_20pct           BOOLEAN DEFAULT FALSE,
    hit_50pct           BOOLEAN DEFAULT FALSE,
    hit_100pct          BOOLEAN DEFAULT FALSE,
    days_to_5pct        INTEGER,
    days_to_10pct       INTEGER,
    days_to_20pct       INTEGER,
    days_to_50pct       INTEGER,
    days_to_100pct      INTEGER,
    fail_5pct           BOOLEAN DEFAULT FALSE,
    fail_10pct          BOOLEAN DEFAULT FALSE,
    days_to_fail_5pct   INTEGER,
    days_to_fail_10pct  INTEGER,
    alpha_vs_spy        NUMERIC(10,4),
    alpha_vs_qqq        NUMERIC(10,4),
    outcome_label       VARCHAR(20),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rso_run     ON replay_signal_outcomes(replay_run_id);
CREATE INDEX IF NOT EXISTS idx_rso_event   ON replay_signal_outcomes(signal_event_id);
CREATE INDEX IF NOT EXISTS idx_rso_horizon ON replay_signal_outcomes(replay_run_id, horizon);

CREATE TABLE IF NOT EXISTS replay_signal_statistics (
    id                  BIGSERIAL PRIMARY KEY,
    replay_run_id       INTEGER NOT NULL,
    stat_key            VARCHAR(160) NOT NULL,
    stat_type           VARCHAR(40),
    event_signal        VARCHAR(40),
    event_signal_family VARCHAR(20),
    event_signal_type   VARCHAR(40),
    event_direction     VARCHAR(12),
    horizon             VARCHAR(8) NOT NULL,
    sample_size         INTEGER DEFAULT 0,
    avg_return          NUMERIC(10,4),
    median_return       NUMERIC(10,4),
    p25_return          NUMERIC(10,4),
    p75_return          NUMERIC(10,4),
    win_rate            NUMERIC(8,4),
    loss_rate           NUMERIC(8,4),
    hit_5pct_rate       NUMERIC(8,4),
    hit_10pct_rate      NUMERIC(8,4),
    hit_20pct_rate      NUMERIC(8,4),
    hit_50pct_rate      NUMERIC(8,4),
    fail_5pct_rate      NUMERIC(8,4),
    fail_10pct_rate     NUMERIC(8,4),
    avg_max_gain        NUMERIC(10,4),
    median_max_gain     NUMERIC(10,4),
    avg_max_drawdown    NUMERIC(10,4),
    median_max_drawdown NUMERIC(10,4),
    expectancy          NUMERIC(10,4),
    risk_reward_ratio   NUMERIC(10,4),
    alpha_vs_spy_avg    NUMERIC(10,4),
    alpha_vs_qqq_avg    NUMERIC(10,4),
    confidence_score    NUMERIC(6,4),
    confidence_label    VARCHAR(20),
    verdict             VARCHAR(30),
    recommendation      TEXT,
    examples_json       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rss_run        ON replay_signal_statistics(replay_run_id);
CREATE INDEX IF NOT EXISTS idx_rss_run_horiz  ON replay_signal_statistics(replay_run_id, horizon);
CREATE INDEX IF NOT EXISTS idx_rss_run_type   ON replay_signal_statistics(replay_run_id, stat_type)
"""

_DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS signal_replay_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    status              TEXT NOT NULL DEFAULT 'running',
    mode                TEXT NOT NULL,
    universe            TEXT NOT NULL,
    timeframe           TEXT NOT NULL DEFAULT '1d',
    as_of_date          TEXT,
    start_date          TEXT,
    end_date            TEXT,
    event_scope         TEXT DEFAULT 'all_signals',
    min_price           REAL,
    min_volume          INTEGER,
    min_dollar_volume   REAL,
    benchmark_symbol    TEXT DEFAULT 'QQQ',
    total_days          INTEGER DEFAULT 0,
    days_completed      INTEGER DEFAULT 0,
    total_symbols       INTEGER DEFAULT 0,
    symbols_completed   INTEGER DEFAULT 0,
    total_events        INTEGER DEFAULT 0,
    total_outcomes      INTEGER DEFAULT 0,
    total_statistics_rows INTEGER DEFAULT 0,
    settings_json       TEXT,
    error_message       TEXT,
    started_at          TEXT DEFAULT (datetime('now')),
    finished_at         TEXT,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_srr_status     ON signal_replay_runs(status);
CREATE INDEX IF NOT EXISTS idx_srr_created_at ON signal_replay_runs(created_at);

CREATE TABLE IF NOT EXISTS replay_signal_events (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    replay_run_id           INTEGER NOT NULL,
    scan_date               TEXT NOT NULL,
    symbol                  TEXT NOT NULL,
    universe                TEXT,
    timeframe               TEXT DEFAULT '1d',
    open                    REAL, high REAL, low REAL, close REAL,
    volume                  INTEGER, dollar_volume REAL,
    event_signal            TEXT NOT NULL,
    event_signal_family     TEXT,
    event_signal_type       TEXT,
    event_direction         TEXT,
    final_signal            TEXT,
    raw_signal              TEXT,
    tz_signal               TEXT,
    prev1_signal TEXT, prev2_signal TEXT, prev3_signal TEXT,
    prev4_signal TEXT, prev5_signal TEXT, prev6_signal TEXT,
    prev7_signal TEXT, prev8_signal TEXT, prev9_signal TEXT, prev10_signal TEXT,
    sequence_2bar TEXT, sequence_3bar TEXT, sequence_4bar TEXT,
    sequence_5bar TEXT, sequence_7bar TEXT, sequence_10bar TEXT,
    signals_last_3d_json TEXT, signals_last_5d_json TEXT, signals_last_10d_json TEXT,
    t_signals_last_3d_count INTEGER DEFAULT 0,
    t_signals_last_5d_count INTEGER DEFAULT 0,
    t_signals_last_10d_count INTEGER DEFAULT 0,
    z_signals_last_3d_count INTEGER DEFAULT 0,
    z_signals_last_5d_count INTEGER DEFAULT 0,
    z_signals_last_10d_count INTEGER DEFAULT 0,
    last_t_signal TEXT, last_z_signal TEXT,
    days_since_last_t INTEGER, days_since_last_z INTEGER,
    had_t_last_3d INTEGER DEFAULT 0, had_z_last_3d INTEGER DEFAULT 0,
    had_t_last_5d INTEGER DEFAULT 0, had_z_last_5d INTEGER DEFAULT 0,
    had_wlnbb_l_last_5d INTEGER DEFAULT 0,
    had_volume_burst_last_5d INTEGER DEFAULT 0,
    had_ema50_reclaim_last_5d INTEGER DEFAULT 0,
    had_pullback_before_signal INTEGER DEFAULT 0,
    wlnbb_bucket TEXT, l_signal TEXT, volume_bucket TEXT,
    wick_suffix TEXT, candle_color TEXT,
    body_pct REAL, upper_wick_pct REAL, lower_wick_pct REAL,
    range_pct REAL, gap_pct REAL,
    ema20 REAL, ema50 REAL, ema89 REAL, ema200 REAL,
    ema20_state TEXT, ema50_state TEXT, ema89_state TEXT, ema200_state TEXT,
    ema_reclaim_20 INTEGER DEFAULT 0, ema_reclaim_50 INTEGER DEFAULT 0,
    ema_reclaim_89 INTEGER DEFAULT 0, ema_reclaim_200 INTEGER DEFAULT 0,
    price_above_ema20 INTEGER DEFAULT 0, price_above_ema50 INTEGER DEFAULT 0,
    price_above_ema89 INTEGER DEFAULT 0, price_above_ema200 INTEGER DEFAULT 0,
    price_pos_4bar REAL, price_pos_10bar REAL,
    price_pos_20bar REAL, price_pos_50bar REAL,
    price_pos_4bar_bucket TEXT, price_pos_10bar_bucket TEXT,
    price_pos_20bar_bucket TEXT, price_pos_50bar_bucket TEXT,
    atr_pct REAL, volatility_bucket TEXT,
    relative_volume REAL, relative_volume_bucket TEXT,
    dollar_volume_bucket TEXT, liquidity_bucket TEXT,
    abr_category TEXT, abr_med10d REAL, abr_fail10d REAL,
    abr_prev1_quality TEXT, abr_prev2_quality TEXT, abr_sequence_key TEXT,
    role TEXT, matched_status TEXT, score REAL, score_bucket TEXT,
    market_regime TEXT, sector TEXT, industry TEXT,
    event_snapshot_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_rse_run         ON replay_signal_events(replay_run_id);
CREATE INDEX IF NOT EXISTS idx_rse_run_signal  ON replay_signal_events(replay_run_id, event_signal);
CREATE INDEX IF NOT EXISTS idx_rse_run_family  ON replay_signal_events(replay_run_id, event_signal_family);
CREATE INDEX IF NOT EXISTS idx_rse_run_symbol  ON replay_signal_events(replay_run_id, symbol);
CREATE INDEX IF NOT EXISTS idx_rse_run_date    ON replay_signal_events(replay_run_id, scan_date);
CREATE INDEX IF NOT EXISTS idx_rse_seq4        ON replay_signal_events(replay_run_id, sequence_4bar);

CREATE TABLE IF NOT EXISTS replay_signal_outcomes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    replay_run_id       INTEGER NOT NULL,
    signal_event_id     INTEGER NOT NULL,
    symbol              TEXT,
    scan_date           TEXT,
    horizon             TEXT NOT NULL,
    entry_price         REAL, exit_price REAL, return_pct REAL,
    max_high            REAL, max_gain_pct REAL,
    max_gain_day        INTEGER, max_gain_date TEXT,
    min_low             REAL, max_drawdown_pct REAL,
    max_drawdown_day    INTEGER, max_drawdown_date TEXT,
    hit_5pct INTEGER DEFAULT 0, hit_10pct INTEGER DEFAULT 0,
    hit_20pct INTEGER DEFAULT 0, hit_50pct INTEGER DEFAULT 0, hit_100pct INTEGER DEFAULT 0,
    days_to_5pct INTEGER, days_to_10pct INTEGER, days_to_20pct INTEGER,
    days_to_50pct INTEGER, days_to_100pct INTEGER,
    fail_5pct INTEGER DEFAULT 0, fail_10pct INTEGER DEFAULT 0,
    days_to_fail_5pct INTEGER, days_to_fail_10pct INTEGER,
    alpha_vs_spy REAL, alpha_vs_qqq REAL,
    outcome_label TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_rso_run     ON replay_signal_outcomes(replay_run_id);
CREATE INDEX IF NOT EXISTS idx_rso_event   ON replay_signal_outcomes(signal_event_id);
CREATE INDEX IF NOT EXISTS idx_rso_horizon ON replay_signal_outcomes(replay_run_id, horizon);

CREATE TABLE IF NOT EXISTS replay_signal_statistics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    replay_run_id       INTEGER NOT NULL,
    stat_key            TEXT NOT NULL,
    stat_type           TEXT,
    event_signal        TEXT,
    event_signal_family TEXT,
    event_signal_type   TEXT,
    event_direction     TEXT,
    horizon             TEXT NOT NULL,
    sample_size         INTEGER DEFAULT 0,
    avg_return REAL, median_return REAL, p25_return REAL, p75_return REAL,
    win_rate REAL, loss_rate REAL,
    hit_5pct_rate REAL, hit_10pct_rate REAL, hit_20pct_rate REAL, hit_50pct_rate REAL,
    fail_5pct_rate REAL, fail_10pct_rate REAL,
    avg_max_gain REAL, median_max_gain REAL,
    avg_max_drawdown REAL, median_max_drawdown REAL,
    expectancy REAL, risk_reward_ratio REAL,
    alpha_vs_spy_avg REAL, alpha_vs_qqq_avg REAL,
    confidence_score REAL, confidence_label TEXT,
    verdict TEXT, recommendation TEXT,
    examples_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_rss_run        ON replay_signal_statistics(replay_run_id);
CREATE INDEX IF NOT EXISTS idx_rss_run_horiz  ON replay_signal_statistics(replay_run_id, horizon);
CREATE INDEX IF NOT EXISTS idx_rss_run_type   ON replay_signal_statistics(replay_run_id, stat_type)
"""


def ensure_signal_replay_tables() -> None:
    """Create Signal Replay tables if they don't exist."""
    ddl = _DDL_PG if USE_PG else _DDL_SQLITE
    try:
        with get_db() as db:
            db.executescript(ddl)
            db.commit()
        log.info("signal_replay tables ready")
    except Exception as exc:
        log.error("signal_replay migration failed: %s", exc)
        raise
