"""
studio/db.py — DuckDB connection and schema management for Analytic Studio.

Single source of truth for:
- DB path
- Schema creation (CREATE TABLE IF NOT EXISTS)
- get_conn() helper used by all studio modules
"""

from __future__ import annotations

import os
import time
import logging
import duckdb

log = logging.getLogger(__name__)

# When a ticker lives in >1 universe (e.g. RGTI / CYCU in nasdaq AND russell2k),
# the same date can carry DIFFERENT bars because each universe is fetched on its
# own schedule (sp500+nasdaq daily via the scheduler; russell2k via one-off
# recovery), so the latest bar can diverge between them. To keep the app
# internally consistent, every cross-universe single-ticker read must pick ONE
# canonical universe by this priority: sp500 > nasdaq > russell2k > other. Embed
# as `ORDER BY {UNIVERSE_PRIORITY_SQL}` in a ROW_NUMBER()/QUALIFY dedup. (It's a
# constant — no user input — so interpolation is safe.)
UNIVERSE_PRIORITY_SQL = (
    "CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 "
    "WHEN 'russell2k' THEN 3 ELSE 9 END"
)


def _is_busy_error(exc: Exception) -> bool:
    """True if a DuckDB error is just a concurrent read/write access conflict
    (a write — enrich/import/backfill — is in progress in this process) rather
    than a real failure. DuckDB forbids mixing read-only and read-write
    connections to one file at the same time."""
    m = str(exc).lower()
    return ("different configuration" in m
            or "already open" in m
            or "conflicting lock" in m
            or "could not set lock" in m
            or ("connection error" in m and "database" in m))

# ── DB path ────────────────────────────────────────────────────────────────────
_DEFAULT_DB = os.path.join(
    os.path.expanduser("~"), "Downloads", "studio_analytics.duckdb"
)
STUDIO_DB_PATH: str = os.environ.get("STUDIO_DB_PATH", _DEFAULT_DB)


def get_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a new DuckDB connection to the studio database."""
    return duckdb.connect(STUDIO_DB_PATH, read_only=read_only)


# ── Schema ─────────────────────────────────────────────────────────────────────
_SCHEMA_SQL = """
-- ─────────────────────────────────────────────────────────
-- bars: one row per ticker × date, holds ALL signal columns
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bars (
    id          INTEGER,
    ticker      VARCHAR NOT NULL,
    date        DATE    NOT NULL,
    universe    VARCHAR,          -- 'sp500' | 'nasdaq' | 'russell2k'

    -- OHLCV
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,

    -- Forward returns (%) — computed from close, stored here permanently
    fwd_1d      DOUBLE,
    fwd_3d      DOUBLE,
    fwd_5d      DOUBLE,
    fwd_10d     DOUBLE,
    fwd_20d     DOUBLE,
    fwd_30d     DOUBLE,
    fwd_60d     DOUBLE,
    fwd_90d     DOUBLE,

    -- MFE: max(high[i:i+N]) / close[i] - 1  (best exit within window)
    mfe_5d      DOUBLE,
    mfe_10d     DOUBLE,
    mfe_20d     DOUBLE,
    mfe_30d     DOUBLE,
    mfe_60d     DOUBLE,

    -- MAE: min(low[i:i+N]) / close[i] - 1   (worst drawdown within window)
    mae_5d      DOUBLE,
    mae_10d     DOUBLE,
    mae_20d     DOUBLE,
    mae_30d     DOUBLE,

    -- Event flag booleans (pre-computed for fast filtering)
    hit_5pct_5d    BOOLEAN,
    hit_10pct_5d   BOOLEAN,
    hit_20pct_5d   BOOLEAN,
    hit_30pct_10d  BOOLEAN,
    hit_50pct_20d  BOOLEAN,
    hit_2x_60d     BOOLEAN,
    hit_3x_90d     BOOLEAN,
    drop_10pct_5d  BOOLEAN,
    drop_20pct_10d BOOLEAN,
    drop_30pct_20d BOOLEAN,

    -- ── Scoring ───────────────────────────────────────────
    turbo_score         DOUBLE,
    turbo_score_n3      DOUBLE,
    turbo_score_n5      DOUBLE,
    turbo_score_n10     DOUBLE,
    vol_bucket          VARCHAR,
    rtb_phase           VARCHAR,
    rtb_total           DOUBLE,
    beta_score          DOUBLE,
    beta_zone           VARCHAR,
    gog_tier            VARCHAR,
    gog_score           DOUBLE,
    ultra_score         DOUBLE,
    final_bull_score    DOUBLE,
    final_regime        VARCHAR,

    -- ── T/Z/L top-level labels ────────────────────────────
    t_sig               VARCHAR,   -- from 'T' column
    z_sig               VARCHAR,   -- from 'Z' column
    l_sig               VARCHAR,   -- from 'L' column
    fly_sig             VARCHAR,   -- from 'FLY' column
    g_sig               VARCHAR,   -- from 'G' column
    b_sig               VARCHAR,   -- from 'B' column
    combo_sig           VARCHAR,   -- from 'Combo' column
    ult_sig             VARCHAR,   -- from 'ULT' column
    vol_sig             VARCHAR,   -- from 'VOL' column
    all_signals_text    VARCHAR,   -- from 'ALL_SIGNALS'

    -- ── Boolean signal columns (all SIG_* + extras) ──────
    -- Volume / VABS
    sig_best            SMALLINT,
    sig_strong          SMALLINT,
    sig_abs             SMALLINT,
    sig_clm             SMALLINT,
    sig_ns_vabs         SMALLINT,
    sig_nd_vabs         SMALLINT,
    sig_sc              SMALLINT,
    sig_bc              SMALLINT,
    sig_best_up         SMALLINT,
    sig_fbo_up          SMALLINT,
    sig_eb_up           SMALLINT,
    sig_3up             SMALLINT,
    sig_fbo_dn          SMALLINT,
    sig_eb_dn           SMALLINT,
    sig_vbo_dn          SMALLINT,

    -- L / WLNBB
    sig_fri34           SMALLINT,
    sig_fri43           SMALLINT,
    sig_fri64           SMALLINT,
    sig_l555            SMALLINT,
    sig_l2l4            SMALLINT,
    sig_blue            SMALLINT,
    sig_cci             SMALLINT,
    sig_cci0r           SMALLINT,
    sig_ccib            SMALLINT,
    sig_rl              SMALLINT,
    sig_rh              SMALLINT,
    sig_pp              SMALLINT,
    sig_l_any           SMALLINT,
    sig_be_any          SMALLINT,
    l34                 SMALLINT,   -- raw L34 column
    l43                 SMALLINT,
    l22                 SMALLINT,
    be_up               SMALLINT,
    bo_up               SMALLINT,
    bx_up               SMALLINT,
    vbo_up              SMALLINT,

    -- GOG
    sig_g1              SMALLINT,
    sig_g2              SMALLINT,
    sig_g4              SMALLINT,
    sig_g6              SMALLINT,
    sig_g11             SMALLINT,
    sig_gog_plus        SMALLINT,
    g1p                 SMALLINT,
    g2p                 SMALLINT,
    g3p                 SMALLINT,
    g1l                 SMALLINT,
    g2l                 SMALLINT,
    g1c                 SMALLINT,
    g2c                 SMALLINT,
    g3c                 SMALLINT,

    -- B signals
    sig_b1              SMALLINT,
    sig_b2              SMALLINT,
    sig_b3              SMALLINT,
    sig_b4              SMALLINT,
    sig_b5              SMALLINT,
    sig_b6              SMALLINT,
    sig_b7              SMALLINT,
    sig_b8              SMALLINT,
    sig_b9              SMALLINT,
    sig_b10             SMALLINT,
    sig_b11             SMALLINT,
    sig_any_b           SMALLINT,

    -- F signals
    sig_f1              SMALLINT,
    sig_f2              SMALLINT,
    sig_f3              SMALLINT,
    sig_f4              SMALLINT,
    sig_f5              SMALLINT,
    sig_f6              SMALLINT,
    sig_f7              SMALLINT,
    sig_f8              SMALLINT,
    sig_f9              SMALLINT,
    sig_f10             SMALLINT,
    sig_f11             SMALLINT,
    sig_any_f           SMALLINT,

    -- FLY
    sig_fly_abcd        SMALLINT,
    sig_fly_cd          SMALLINT,
    sig_fly_bd          SMALLINT,
    sig_fly_ad          SMALLINT,

    -- WICK
    sig_wk_up           SMALLINT,
    sig_wk_dn           SMALLINT,
    sig_x1              SMALLINT,
    sig_x2              SMALLINT,
    sig_x1g             SMALLINT,
    sig_x3              SMALLINT,

    -- TZ state
    sig_tz              SMALLINT,
    sig_t               SMALLINT,
    sig_z               SMALLINT,
    sig_tz3             SMALLINT,
    sig_tz2             SMALLINT,
    sig_tz_flip         SMALLINT,
    sig_bias_up         SMALLINT,
    sig_bias_dn         SMALLINT,

    -- Individual T signals (derived from t_sig)
    sig_t1g             SMALLINT,
    sig_t2g             SMALLINT,
    sig_t1              SMALLINT,
    sig_t2              SMALLINT,
    sig_t3              SMALLINT,
    sig_t4              SMALLINT,
    sig_t5              SMALLINT,
    sig_t6              SMALLINT,
    sig_t7              SMALLINT,
    sig_t8              SMALLINT,
    sig_t9              SMALLINT,
    sig_t10             SMALLINT,
    sig_t11             SMALLINT,
    sig_t12             SMALLINT,

    -- Individual Z signals (derived from z_sig)
    sig_z1g             SMALLINT,
    sig_z2g             SMALLINT,
    sig_z1              SMALLINT,
    sig_z2              SMALLINT,
    sig_z3              SMALLINT,
    sig_z4              SMALLINT,
    sig_z5              SMALLINT,
    sig_z6              SMALLINT,
    sig_z7              SMALLINT,
    sig_z8              SMALLINT,
    sig_z9              SMALLINT,
    sig_z10             SMALLINT,
    sig_z11             SMALLINT,
    sig_z12             SMALLINT,

    -- EMA cross (PREUP / PREDN)
    sig_p2              SMALLINT,
    sig_p3              SMALLINT,
    sig_p50             SMALLINT,
    sig_p55             SMALLINT,
    sig_p66             SMALLINT,
    sig_p89             SMALLINT,
    sig_any_p           SMALLINT,
    sig_d2              SMALLINT,
    sig_d3              SMALLINT,
    sig_d50             SMALLINT,
    sig_d55             SMALLINT,
    sig_d66             SMALLINT,
    sig_d89             SMALLINT,
    sig_any_d           SMALLINT,

    -- Combo / Momentum
    sig_buy             SMALLINT,
    sig_3g              SMALLINT,
    sig_conso           SMALLINT,
    sig_svs             SMALLINT,
    sig_cd              SMALLINT,
    sig_ca              SMALLINT,
    sig_cw              SMALLINT,
    sig_seq_bcont       SMALLINT,
    sig_ns_delta        SMALLINT,
    sig_nd_delta        SMALLINT,
    rocket              SMALLINT,
    hilo_buy            SMALLINT,
    three_g             SMALLINT,
    svs                 SMALLINT,
    sq                  SMALLINT,
    load                SMALLINT,
    f8                  SMALLINT,

    -- Volume
    sig_va              SMALLINT,
    sig_vol_5x          SMALLINT,
    sig_vol_10x         SMALLINT,
    sig_vol_20x         SMALLINT,

    -- EMA price position
    price_gt_20         SMALLINT,
    price_gt_50         SMALLINT,
    price_gt_89         SMALLINT,
    price_gt_200        SMALLINT,
    price_lt_20         SMALLINT,
    price_lt_50         SMALLINT,
    price_lt_89         SMALLINT,
    price_lt_200        SMALLINT,
    rsi_le_35           SMALLINT,
    rsi_ge_70           SMALLINT,

    -- Delta
    sig_flp_up          SMALLINT,
    sig_org_up          SMALLINT,
    sig_dd_up_red       SMALLINT,
    sig_d_up_red        SMALLINT,
    sig_d_dn_green      SMALLINT,
    sig_dd_dn_green     SMALLINT,

    -- CISD
    sig_cisd_cplus      SMALLINT,
    sig_cisd_cplus_minus SMALLINT,
    sig_cisd_cplus_mm   SMALLINT,

    -- PARA
    sig_para_prep       SMALLINT,
    sig_para_start      SMALLINT,
    sig_para_plus       SMALLINT,
    sig_para_retest     SMALLINT,

    -- Meta / Not-extended
    sig_not_ext         SMALLINT,
    already_extended_flag SMALLINT,

    -- 260523 / Wyckoff / AD
    ad_fresh            SMALLINT,
    ad_cluster          SMALLINT,
    wyc_phase           VARCHAR,
    wyc_spring          SMALLINT,
    wyc_sos             SMALLINT,
    wyc_in_tr           SMALLINT,
    wyc_sow             SMALLINT,

    -- Prebreak
    prebreak_score      DOUBLE,
    prebreak_prime      SMALLINT,
    prebreak_ready      SMALLINT,
    prebreak_watch      SMALLINT,
    pb_lvbo             SMALLINT,
    pb_wvf_confirm      SMALLINT,
    pb_stop_cause       SMALLINT,
    pb_macro_penalty    SMALLINT,

    -- Swing
    swing_type          VARCHAR,

    PRIMARY KEY (ticker, date, universe)
);

-- ─────────────────────────────────────────────────────────
-- events: labelled outcome events
-- ─────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS events_id_seq START 1;
CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY DEFAULT nextval('events_id_seq'),
    ticker       VARCHAR  NOT NULL,
    event_date   DATE     NOT NULL,
    event_type   VARCHAR  NOT NULL,   -- 'BULL_2X_60D', 'FALSE_POS', 'MISS', custom
    close_price  DOUBLE,
    mfe_60d      DOUBLE,
    fwd_30d      DOUBLE,
    fwd_60d      DOUBLE,
    universe     VARCHAR,
    turbo_at_event DOUBLE,
    tags         VARCHAR,             -- JSON array as string
    notes        VARCHAR,
    created_at   TIMESTAMP DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_ticker ON events(ticker);

-- ─────────────────────────────────────────────────────────
-- bar_descriptions: generated text per bar
-- ─────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS mp_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS bt_id_seq START 1;
CREATE TABLE IF NOT EXISTS bar_descriptions (
    ticker        VARCHAR NOT NULL,
    date          DATE    NOT NULL,
    bar_desc      VARCHAR,   -- short ~200 char description
    pre_narrative VARCHAR,   -- 20-bar pre-move narrative (generated on demand)
    PRIMARY KEY (ticker, date)
);

-- ─────────────────────────────────────────────────────────
-- custom_scores: user-defined scoring weights
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS custom_scores (
    score_id     VARCHAR PRIMARY KEY,
    name         VARCHAR,
    weights      VARCHAR,   -- JSON
    hard_filters VARCHAR,   -- JSON
    threshold    INTEGER DEFAULT 45,
    created_at   TIMESTAMP DEFAULT now()
);

-- ─────────────────────────────────────────────────────────
-- backtest_results
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS backtest_results (
    id           INTEGER PRIMARY KEY DEFAULT nextval('bt_id_seq'),
    score_id     VARCHAR,
    event_type   VARCHAR,
    date_from    DATE,
    date_to      DATE,
    universe     VARCHAR,
    precision_   DOUBLE,
    recall_      DOUBLE,
    f1_          DOUBLE,
    avg_fwd_20d  DOUBLE,
    avg_fwd_60d  DOUBLE,
    fp_rate      DOUBLE,
    missed_count INTEGER,
    caught_count INTEGER,
    result_json  VARCHAR,   -- full JSON blob
    created_at   TIMESTAMP DEFAULT now()
);

-- ─────────────────────────────────────────────────────────
-- mined_patterns: results of pattern mining
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mined_patterns (
    id              INTEGER PRIMARY KEY DEFAULT nextval('mp_id_seq'),
    run_id          VARCHAR,   -- UUID per mining run
    event_type      VARCHAR,
    pre_window      INTEGER,
    pattern_type    VARCHAR,   -- 'single' | 'combo_2' | 'combo_3' | 'sequence'
    signals         VARCHAR,   -- JSON array: ["L34", "ad_cluster"]
    freq_in_events  DOUBLE,
    base_freq       DOUBLE,
    lift            DOUBLE,
    avg_fwd_30d     DOUBLE,
    avg_fwd_60d     DOUBLE,
    n_events        INTEGER,
    created_at      TIMESTAMP DEFAULT now()
);

-- ─────────────────────────────────────────────────────────
-- import_log: track what was imported and when
-- ─────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS import_log_id_seq START 1;
CREATE TABLE IF NOT EXISTS import_log (
    id          INTEGER PRIMARY KEY DEFAULT nextval('import_log_id_seq'),
    universe    VARCHAR,
    csv_path    VARCHAR,
    rows_imported INTEGER,
    tickers_imported INTEGER,
    date_from   DATE,
    date_to     DATE,
    duration_sec DOUBLE,
    status      VARCHAR DEFAULT 'ok',
    error_msg   VARCHAR,
    imported_at TIMESTAMP DEFAULT now()
);
"""


def ensure_schema() -> None:
    """Create all tables if they don't exist. Safe to call multiple times."""
    conn = get_conn()
    try:
        # DuckDB doesn't have executescript — split on semicolons and run each statement
        statements = [s.strip() for s in _SCHEMA_SQL.split(";") if s.strip()]
        for stmt in statements:
            try:
                conn.execute(stmt)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    log.debug("Schema stmt skipped: %s", e)
        conn.commit()
    finally:
        conn.close()
    # Migration: add individual T/Z columns to existing bars tables
    _TZ_MIGRATION_COLS = [
        "sig_t1g","sig_t2g","sig_t1","sig_t2","sig_t3","sig_t4",
        "sig_t5","sig_t6","sig_t7","sig_t8","sig_t9","sig_t10","sig_t11","sig_t12",
        "sig_z1g","sig_z2g","sig_z1","sig_z2","sig_z3","sig_z4",
        "sig_z5","sig_z6","sig_z7","sig_z8","sig_z9","sig_z10","sig_z11","sig_z12",
    ]
    # Migration: bar shape / suffix / line5 / Williams pivots (3-3 + 5-5) / HL-HH outcomes
    _ENRICHMENT_VARCHAR_COLS = [
        # Line 2 suffix components + combined
        "ne_suffix", "wick_suffix", "penetration_suffix", "close_suffix",
        "full_suffix", "composite_full_suffix",
        # Line 3 / 4 / 5 combined codes
        "bar_body_wick", "bar_gap_range", "bar_gap_class", "bar_range_class",
        "bar_line5", "rsi2_state",
        # Williams pivots — directional swing type (HH/LH/HL/LL)
        "swing_type_3", "swing_type_5",
        # ULTRA-from-DB extras
        "sector",                 # ticker sector (from cache or YF)
        "profile_category",       # SWEET_SPOT / BUILDING / WATCH / ""
        # Wyckoff Accumulation Exit (260525 v2 — Breakout Hunter)
        "acc_exit_class",         # BO_NOW / BO_1 / BO_2_3 / BO_4_5 / BO_LATE / NOT_ACC / DIST_EXIT
        "aes_stage",              # human-readable stage label
        # PreBreakout v2 (data-derived OOS-validated score) — WATCH/BUY/HOT
        "prebreak_v2_band",
        # composite + volume bucket combined string (e.g. Z2L46NBO·VB)
        "composite_vol",
        # Enrichment metadata
        "enrich_version",
    ]
    _ENRICHMENT_SMALLINT_COLS = [
        # Line 5 bool flags (parsed from bar_line5)
        "wvf_spike", "vix_range", "psar_bull", "psar_bear",
        # Williams 3-3 pivots
        "is_pivot_high_3", "is_pivot_low_3",
        "next_pivot_is_hl_3", "next_pivot_is_hh_3",
        # Williams 5-5 pivots (major)
        "is_pivot_high_5", "is_pivot_low_5",
        "next_pivot_is_hl_5", "next_pivot_is_hh_5",
        # Individual L digit flags (1..6) — derived from l_signal string
        "sig_l1", "sig_l2", "sig_l3", "sig_l4", "sig_l5", "sig_l6",
        # ULTRA-from-DB extras
        "tz_bull",                 # 1 if t_sig present (bullish TZ active)
        "sweet_spot_active",       # composite (turbo>=60 + bull regime + L+W)
        "late_warning",            # turbo dropping recently
        # PreBreakout v2 (data-derived OOS-validated breakout-probability ×100)
        "prebreak_v2",
        # Wyckoff V2 Soft state machine (260529): SC→AR→ST→Spring→SOS/JAC→LPS
        "w2_sc", "w2_ar", "w2_st", "w2_spring", "w2_sos", "w2_jac", "w2_lps",
        "w2_evr", "w2_accum", "w2_break", "w2_state",
        # Wyckoff structure triggers (260529_WYCK_TRIG / WyckoffTradingAgent)
        "wt_valid_tr", "wt_sos", "wt_spring", "wt_lps", "wt_evr",
        # PREBREAK extra sub-signals (260515 v6.0 port) — pb_lvbo already in CREATE
        "pb_pp_rtv", "pb_fly_cd_c", "pb_follow_confirm",
    ]
    _ENRICHMENT_DOUBLE_COLS = [
        # ATR (used by gap/range + downstream)
        "atr_14",
        # ULTRA-from-DB extras
        "avg_vol_20d",            # 20-bar rolling volume average
        "profile_score",          # composite profile score (0-100)
        "rsi_14",                 # Wilder RSI(14)
        "cci_20",                 # CCI(20) SMA
        "change_pct",             # day's % change (close vs prev_close)
        # Wyckoff V2 quality / structure levels
        "w2_tr_quality", "wt_quality", "wt_support", "wt_resistance",
        # Wyckoff ACC Exit (Breakout Hunter)
        "acc_exit_in_n",          # bars until ACC_TR → MARKUP transition (NULL if N/A)
        "aes_score",              # 0-100 composite lift-weighted score
        "aes_leading",            # 0-100 leading-only score (companion metric)
        "aes_trend_5d",           # change over 5 bars (rising = building setup)
        # Williams 3-3 swing returns / pct to next HL/HH
        "fwd_swing_ret_3", "fwd_swing_bars_3", "swing_ret_from_prev_3",
        "pct_to_next_hl_3", "pct_to_next_hh_3",
        "bars_to_next_hl_3", "bars_to_next_hh_3",
        # Williams 5-5 (major)
        "fwd_swing_ret_5", "fwd_swing_bars_5", "swing_ret_from_prev_5",
        "pct_to_next_hl_5", "pct_to_next_hh_5",
        "bars_to_next_hl_5", "bars_to_next_hh_5",
    ]
    conn = get_conn()
    try:
        existing = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()
        for col in _TZ_MIGRATION_COLS:
            if col not in existing:
                try:
                    conn.execute(f"ALTER TABLE bars ADD COLUMN {col} SMALLINT DEFAULT 0")
                except Exception:
                    pass
        for col in _ENRICHMENT_VARCHAR_COLS:
            if col not in existing:
                try:
                    conn.execute(f"ALTER TABLE bars ADD COLUMN {col} VARCHAR")
                except Exception:
                    pass
        for col in _ENRICHMENT_SMALLINT_COLS:
            if col not in existing:
                try:
                    conn.execute(f"ALTER TABLE bars ADD COLUMN {col} SMALLINT")
                except Exception:
                    pass
        for col in _ENRICHMENT_DOUBLE_COLS:
            if col not in existing:
                try:
                    conn.execute(f"ALTER TABLE bars ADD COLUMN {col} DOUBLE")
                except Exception:
                    pass
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()
    log.info("Studio DB schema ensured at %s", STUDIO_DB_PATH)


def get_stats(retries: int = 3) -> dict:
    """Return high-level stats about what's in the DB.

    If a write (enrich / import / incremental / backfill) is in progress in this
    process, a read-only connection briefly can't open — instead of surfacing a
    scary 'Connection Error' we retry a few times, then report a clean 'updating'
    status the UI can render calmly."""
    last_exc = None
    for attempt in range(retries):
        try:
            with get_conn(read_only=True) as conn:
                rows = conn.execute("SELECT COUNT(*) FROM bars").fetchone()[0]
                tickers = conn.execute("SELECT COUNT(DISTINCT ticker) FROM bars").fetchone()[0]
                universes = conn.execute(
                    "SELECT universe, COUNT(*) as n FROM bars GROUP BY universe"
                ).fetchall()
                date_range = conn.execute(
                    "SELECT MIN(date), MAX(date) FROM bars"
                ).fetchone()
                events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            return {
                "rows": rows,
                "tickers": tickers,
                "universes": {u: n for u, n in universes},
                "date_from": str(date_range[0]) if date_range[0] else None,
                "date_to": str(date_range[1]) if date_range[1] else None,
                "events": events,
                "db_path": STUDIO_DB_PATH,
            }
        except Exception as exc:
            last_exc = exc
            if _is_busy_error(exc):
                if attempt < retries - 1:
                    time.sleep(0.4)
                    continue
                # a long write (e.g. enrich) is running — report calmly, not as an error
                return {
                    "updating": True,
                    "message": "Database is being updated (enrich / import in progress). "
                               "Stats will be available again when it finishes.",
                    "db_path": STUDIO_DB_PATH,
                }
            raise
    raise last_exc
