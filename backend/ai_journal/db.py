"""
ai_journal/db.py — the journal's own DuckDB (separate file from the analytics DB).

Why separate: studio_analytics.duckdb is single-writer; the journal writes during
sessions and would deadlock against the app / nightly refresh. Its own file avoids
the lock contention entirely (learned the hard way).

The analytics DB (bars + fwd/HH metrics) is read-only ground truth; the journal DB
holds decisions, positions, the three memory tiers, calibration and shadow state.
"""
from __future__ import annotations

import os
import duckdb

# Journal DB lives next to the analytics DB in the user's data dir.
JOURNAL_DB_PATH = os.environ.get(
    "AI_JOURNAL_DB_PATH",
    os.path.expanduser("~/Downloads/ai_journal.duckdb"),
)
# Read-only source of truth (bars + fwd_*/next_pivot_is_hh_* metrics).
ANALYTICS_DB_PATH = os.environ.get(
    "STUDIO_DB_PATH",
    os.path.expanduser("~/Downloads/studio_analytics.duckdb"),
)


def get_journal_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(JOURNAL_DB_PATH, read_only=read_only)


def get_analytics_conn() -> duckdb.DuckDBPyConnection:
    """Always read-only — we never write to the analytics DB from the journal."""
    return duckdb.connect(ANALYTICS_DB_PATH, read_only=True)


_SCHEMA = """
-- ── Account / bankroll ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS journal_state (
    id            INTEGER PRIMARY KEY DEFAULT 1,
    capital       DOUBLE,
    start_capital DOUBLE,
    updated_at    TIMESTAMP,
    config_json   VARCHAR
);

-- ── Positions (open + closed) — ground-truth P&L ────────────────────────────
CREATE TABLE IF NOT EXISTS journal_position (
    id             BIGINT PRIMARY KEY,
    ticker         VARCHAR,
    universe       VARCHAR,
    decision_date  DATE,        -- the date the decision was made on (as-of bar)
    opened_at      TIMESTAMP,
    action         VARCHAR,     -- BUY / WATCH
    conviction     INTEGER,     -- 0..100
    fingerprint    VARCHAR,     -- shared key across the 3 memory tiers
    entry_px       DOUBLE,
    size_pct       DOUBLE,      -- % of capital at entry
    shares         DOUBLE,
    stop_px        DOUBLE,
    target_px      DOUBLE,
    horizon_days   INTEGER,
    status         VARCHAR,     -- PENDING_OPEN / OPEN / CLOSED
    entry_mode     VARCHAR,     -- AT_DECISION (in-session) / NEXT_OPEN (decided while closed)
    decided_session VARCHAR,    -- 'open' / 'closed' at decision time
    decided_at     TIMESTAMP,
    atr_at_decision DOUBLE,     -- ATR snapshot → stop/target computed at fill for NEXT_OPEN
    filled_date    DATE,        -- the session-open date a NEXT_OPEN was filled on
    closed_at      TIMESTAMP,
    exit_date      DATE,
    exit_px        DOUBLE,
    exit_reason    VARCHAR,     -- stop / target / time / manual
    pnl            DOUBLE,
    pnl_pct        DOUBLE,
    verdict        VARCHAR,     -- WIN / LOSS / FLAT / PENDING
    thesis         VARCHAR,
    evidence_json  VARCHAR
);

-- ── Tier 1: HARD STATS — forward outcomes per signal/predicate (code-computed) ─
CREATE TABLE IF NOT EXISTS signal_outcomes (
    predicate      VARCHAR,     -- e.g. 'vol_20x', 'phase_D', 'v3_ge25_and_vol20'
    predicate_sql  VARCHAR,
    category       VARCHAR,
    as_of_date     DATE,
    n              BIGINT,
    rate_pct       DOUBLE,      -- % of analysis universe firing
    fwd3_med       DOUBLE,
    fwd5_med       DOUBLE,
    fwd10_med      DOUBLE,
    win5           DOUBLE,      -- P(fwd_5d > 0)
    big5           DOUBLE,      -- P(fwd_5d >= 5)
    hh5            DOUBLE,      -- P(next_pivot_is_hh_5)
    base_win5      DOUBLE,
    base_big5      DOUBLE,
    base_hh5       DOUBLE,
    lift_big5      DOUBLE,      -- big5 / base_big5
    lift_hh5       DOUBLE,      -- (hh5 - base_hh5) in pp also stored separately
    hh5_edge_pp    DOUBLE,      -- hh5 - base_hh5 (percentage points)
    updated_at     TIMESTAMP,
    PRIMARY KEY (predicate, as_of_date)
);

-- ── Tier 2: PATTERN MEMORY — own closed-trade stats per fingerprint (as-of) ───
CREATE TABLE IF NOT EXISTS pattern_memory (
    fingerprint  VARCHAR,
    as_of_date   DATE,
    n_trades     BIGINT,
    win_rate     DOUBLE,
    avg_ret      DOUBLE,
    last_seen    DATE,
    PRIMARY KEY (fingerprint, as_of_date)
);

-- ── Tier 3: LLM LESSONS (narrative) — gated by Tier-1/2 evidence ─────────────
CREATE TABLE IF NOT EXISTS trade_lesson (
    id                 BIGINT PRIMARY KEY,
    created_at         TIMESTAMP,
    scope_fingerprint  VARCHAR,
    what_worked        VARCHAR,
    what_failed        VARCHAR,
    lesson             VARCHAR,
    tags               VARCHAR,
    confidence         DOUBLE,
    status             VARCHAR,   -- provisional / active / retired
    evidence_n         BIGINT,
    evidence_lift      DOUBLE,
    source_position_ids VARCHAR,
    revalidated_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signal_blacklist (
    pattern     VARCHAR,
    reason      VARCHAR,
    created_at  TIMESTAMP,
    ttl_days    INTEGER,
    source      VARCHAR     -- llm / rule / human
);

CREATE TABLE IF NOT EXISTS calibration (
    bucket          VARCHAR PRIMARY KEY,  -- e.g. '70-79'
    conviction_lo   INTEGER,
    conviction_hi   INTEGER,
    n               BIGINT,
    realized_win_rate DOUBLE,
    updated_at      TIMESTAMP
);

-- ── Shadow: "what if we'd taken the excluded slice" ──────────────────────────
CREATE TABLE IF NOT EXISTS shadow_position (
    id             BIGINT PRIMARY KEY,
    ticker         VARCHAR,
    decision_date  DATE,
    fingerprint    VARCHAR,
    entry_px       DOUBLE,
    exit_px        DOUBLE,
    pnl_pct        DOUBLE,
    verdict        VARCHAR,
    reason_excluded VARCHAR
);

-- ── Ticker metadata (sector / industry / market-cap) — from Massive reference ─
CREATE TABLE IF NOT EXISTS ticker_meta (
    ticker       VARCHAR PRIMARY KEY,
    name         VARCHAR,
    sector       VARCHAR,
    industry     VARCHAR,      -- sic_description
    sic_code     INTEGER,
    market_cap   DOUBLE,
    employees    INTEGER,
    mcap_bucket  VARCHAR,      -- mega / large / mid / small / micro
    updated_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS journal_session_log (
    ts              TIMESTAMP,
    candidates_n    INTEGER,
    decisions_json  VARCHAR,
    capital_before  DOUBLE,
    capital_after   DOUBLE,
    notes           VARCHAR
);
"""


def ensure_schema() -> None:
    conn = get_journal_conn(read_only=False)
    try:
        conn.execute(_SCHEMA)
        # Migrations for pre-existing journal.duckdb files (idempotent).
        for col, typ in [("entry_mode", "VARCHAR"), ("decided_session", "VARCHAR"),
                         ("decided_at", "TIMESTAMP"), ("atr_at_decision", "DOUBLE"),
                         ("filled_date", "DATE"), ("sector", "VARCHAR"),
                         ("mcap_bucket", "VARCHAR")]:
            conn.execute(f"ALTER TABLE journal_position ADD COLUMN IF NOT EXISTS {col} {typ}")
        # Seed account once.
        row = conn.execute("SELECT count(*) FROM journal_state").fetchone()[0]
        if row == 0:
            conn.execute(
                "INSERT INTO journal_state (id, capital, start_capital, updated_at, config_json) "
                "VALUES (1, 10000, 10000, current_timestamp, '{}')"
            )
        conn.commit()
    finally:
        conn.close()


def next_id(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    return int(conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}").fetchone()[0])


if __name__ == "__main__":
    ensure_schema()
    print(f"ai_journal schema ensured at {JOURNAL_DB_PATH}")
