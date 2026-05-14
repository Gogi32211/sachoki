"""
ultra_pump_migration.py — Creates ULTRA Pump Research metadata tables.

Hybrid storage: Postgres holds the lightweight `ultra_pump_runs` row + reuses
`replay_artifacts` for artifact registry. Heavy parquet data lives on disk
under REPLAY_STORAGE_DIR/run_XXXXXX.
"""
from __future__ import annotations
import logging
from db import get_db, USE_PG

log = logging.getLogger(__name__)

_DDL_PG = """
CREATE TABLE IF NOT EXISTS ultra_pump_runs (
    id                              SERIAL PRIMARY KEY,
    status                          VARCHAR(20) NOT NULL DEFAULT 'running',
    universe                        VARCHAR(20) NOT NULL DEFAULT 'all_us',
    mode                            VARCHAR(20) NOT NULL DEFAULT 'date_range',
    pump_target                     VARCHAR(20) NOT NULL DEFAULT 'X2_TO_X4',
    start_date                      DATE,
    end_date                        DATE,
    pump_horizon                    INTEGER NOT NULL DEFAULT 60,
    pre_pump_window_bars            INTEGER NOT NULL DEFAULT 14,
    scanner_detection_window_bars   INTEGER NOT NULL DEFAULT 14,
    detection_reference             VARCHAR(40) NOT NULL DEFAULT 'before_first_x2_else_before_peak',
    lookback_bars                   INTEGER NOT NULL DEFAULT 500,
    split_impact_window_days        INTEGER NOT NULL DEFAULT 30,
    research_mode                   VARCHAR(20) DEFAULT 'standard',
    event_scope                     VARCHAR(40) DEFAULT 'pumps_only',
    min_price                       NUMERIC(10,4),
    min_volume                      BIGINT,
    min_dollar_volume               NUMERIC(18,2),
    benchmark_symbol                VARCHAR(10) DEFAULT 'QQQ',
    settings_json                   TEXT,
    summary_json                    TEXT,
    artifact_manifest_json          TEXT,
    error_message                   TEXT,
    storage_mode                    VARCHAR(20) DEFAULT 'parquet',
    total_episodes                  INTEGER DEFAULT 0,
    total_caught                    INTEGER DEFAULT 0,
    total_missed                    INTEGER DEFAULT 0,
    symbols_total                   INTEGER DEFAULT 0,
    symbols_completed               INTEGER DEFAULT 0,
    started_at                      TIMESTAMPTZ DEFAULT NOW(),
    finished_at                     TIMESTAMPTZ,
    created_at                      TIMESTAMPTZ DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_upr_status     ON ultra_pump_runs(status);
CREATE INDEX IF NOT EXISTS idx_upr_created_at ON ultra_pump_runs(created_at);
"""

_DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS ultra_pump_runs (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    status                          TEXT NOT NULL DEFAULT 'running',
    universe                        TEXT NOT NULL DEFAULT 'all_us',
    mode                            TEXT NOT NULL DEFAULT 'date_range',
    pump_target                     TEXT NOT NULL DEFAULT 'X2_TO_X4',
    start_date                      TEXT,
    end_date                        TEXT,
    pump_horizon                    INTEGER NOT NULL DEFAULT 60,
    pre_pump_window_bars            INTEGER NOT NULL DEFAULT 14,
    scanner_detection_window_bars   INTEGER NOT NULL DEFAULT 14,
    detection_reference             TEXT NOT NULL DEFAULT 'before_first_x2_else_before_peak',
    lookback_bars                   INTEGER NOT NULL DEFAULT 500,
    split_impact_window_days        INTEGER NOT NULL DEFAULT 30,
    research_mode                   TEXT DEFAULT 'standard',
    event_scope                     TEXT DEFAULT 'pumps_only',
    min_price                       REAL,
    min_volume                      INTEGER,
    min_dollar_volume               REAL,
    benchmark_symbol                TEXT DEFAULT 'QQQ',
    settings_json                   TEXT,
    summary_json                    TEXT,
    artifact_manifest_json          TEXT,
    error_message                   TEXT,
    storage_mode                    TEXT DEFAULT 'parquet',
    total_episodes                  INTEGER DEFAULT 0,
    total_caught                    INTEGER DEFAULT 0,
    total_missed                    INTEGER DEFAULT 0,
    symbols_total                   INTEGER DEFAULT 0,
    symbols_completed               INTEGER DEFAULT 0,
    started_at                      TEXT DEFAULT (datetime('now')),
    finished_at                     TEXT,
    created_at                      TEXT DEFAULT (datetime('now')),
    updated_at                      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_upr_status     ON ultra_pump_runs(status);
CREATE INDEX IF NOT EXISTS idx_upr_created_at ON ultra_pump_runs(created_at);
"""


def ensure_ultra_pump_tables() -> None:
    """Create ULTRA Pump Research tables if missing. Safe to call repeatedly."""
    ddl = _DDL_PG if USE_PG else _DDL_SQLITE
    try:
        with get_db() as db:
            db.executescript(ddl)
            db.commit()
        log.info("ultra_pump tables ready")
    except Exception as exc:
        log.error("ultra_pump migration failed: %s", exc)
        raise
