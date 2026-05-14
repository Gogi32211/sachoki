"""
ultra_scan_migration.py — SQLite/PostgreSQL tables for persisting completed
Ultra Scan runs and their candidates.

Kept separate from ultra_pump_migration.py (research runs) to isolate concerns.
Tables:
  ultra_scan_runs       — one row per completed/stage1_done scan
  ultra_scan_candidates — full row_json per ticker per scan run
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def ensure_ultra_scan_tables() -> None:
    """Create ultra_scan_runs and ultra_scan_candidates if they don't exist."""
    from db import get_db, USE_PG

    with get_db() as conn:
        if USE_PG:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS ultra_scan_runs (
                    id          BIGSERIAL PRIMARY KEY,
                    universe    TEXT NOT NULL DEFAULT 'sp500',
                    tf          TEXT NOT NULL DEFAULT '1d',
                    nasdaq_batch TEXT NOT NULL DEFAULT '',
                    status      TEXT NOT NULL DEFAULT 'completed',
                    phase       TEXT,
                    total_candidates INTEGER DEFAULT 0,
                    finished_at TIMESTAMPTZ,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_usr_univ_tf_created
                    ON ultra_scan_runs(universe, tf, created_at DESC);

                CREATE TABLE IF NOT EXISTS ultra_scan_candidates (
                    id               BIGSERIAL PRIMARY KEY,
                    run_id           BIGINT NOT NULL REFERENCES ultra_scan_runs(id) ON DELETE CASCADE,
                    ticker           TEXT NOT NULL,
                    ultra_score      DOUBLE PRECISION,
                    ultra_score_band TEXT,
                    profile          TEXT,
                    last_price       DOUBLE PRECISION,
                    change_pct       DOUBLE PRECISION,
                    volume           DOUBLE PRECISION,
                    vol_bucket       TEXT,
                    abr              TEXT,
                    ema_ok           BOOLEAN DEFAULT FALSE,
                    bull_score       DOUBLE PRECISION,
                    scanned_at       TEXT,
                    row_json         TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_usc_run_id ON ultra_scan_candidates(run_id);
                CREATE INDEX IF NOT EXISTS idx_usc_ticker  ON ultra_scan_candidates(ticker)
            """)
        else:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS ultra_scan_runs (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    universe         TEXT NOT NULL DEFAULT 'sp500',
                    tf               TEXT NOT NULL DEFAULT '1d',
                    nasdaq_batch     TEXT NOT NULL DEFAULT '',
                    status           TEXT NOT NULL DEFAULT 'completed',
                    phase            TEXT,
                    total_candidates INTEGER DEFAULT 0,
                    finished_at      TEXT,
                    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now'))
                );
                CREATE INDEX IF NOT EXISTS idx_usr_univ_tf_created
                    ON ultra_scan_runs(universe, tf, created_at);

                CREATE TABLE IF NOT EXISTS ultra_scan_candidates (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id           INTEGER NOT NULL REFERENCES ultra_scan_runs(id) ON DELETE CASCADE,
                    ticker           TEXT NOT NULL,
                    ultra_score      REAL,
                    ultra_score_band TEXT,
                    profile          TEXT,
                    last_price       REAL,
                    change_pct       REAL,
                    volume           REAL,
                    vol_bucket       TEXT,
                    abr              TEXT,
                    ema_ok           INTEGER DEFAULT 0,
                    bull_score       REAL,
                    scanned_at       TEXT,
                    row_json         TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_usc_run_id ON ultra_scan_candidates(run_id);
                CREATE INDEX IF NOT EXISTS idx_usc_ticker  ON ultra_scan_candidates(ticker)
            """)
        conn.commit()
    log.info("ultra_scan_migration: tables ready")
