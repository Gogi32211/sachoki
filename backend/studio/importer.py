"""
studio/importer.py — Import bulk_export CSVs into DuckDB with full forward returns.

Steps:
1. Read CSV (SP500 / NASDAQ / Russell2k)
2. Sort by ticker + date
3. Compute fwd_1/3/5/10/20/30/60/90d from close prices
4. Compute MFE (max favorable excursion) and MAE (max adverse excursion)
5. Set event flag booleans
6. Map all signal columns to DB column names
7. UPSERT into DuckDB bars table

Progress is written to /tmp/studio_import_progress.json so the API
can poll it without blocking.
"""

from __future__ import annotations

import os
import json
import time
import logging
import warnings
from typing import Optional

import numpy as np
import pandas as pd

from studio.db import get_conn, ensure_schema, STUDIO_DB_PATH

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

PROGRESS_FILE = "/tmp/studio_import_progress.json"

# ── CSV column → DB column mapping ────────────────────────────────────────────
# Keys = CSV column names (case-insensitive, uppercased in CSV)
# Values = DB column names (lowercase)
# Only need to list columns that differ; everything else is lowercased directly.
_COL_MAP: dict[str, str] = {
    "T":                    "t_sig",
    "Z":                    "z_sig",
    "L":                    "l_sig",
    "F":                    "f_sig_label",   # skip — not in DB
    "FLY":                  "fly_sig",
    "G":                    "g_sig",
    "B":                    "b_sig",
    "Combo":                "combo_sig",
    "ULT":                  "ult_sig",
    "VOL":                  "vol_sig",
    "ALL_SIGNALS":          "all_signals_text",
    "FINAL_BULL_SCORE":     "final_bull_score",
    "FINAL_REGIME":         "final_regime",
    "GOG_TIER":             "gog_tier",
    "GOG_SCORE":            "gog_score",
    "BETA_SCORE":           "beta_score",
    "BETA_ZONE":            "beta_zone",
    "ALREADY_EXTENDED_FLAG":"already_extended_flag",
    "L88":                  "sig_l88",
    "SIG_260308":           "sig_260308",
    # SIG_* → sig_*
    **{f"SIG_{x}": f"sig_{x.lower()}" for x in [
        "BEST","STRONG","ABS","CLM","NS_VABS","ND_VABS","SC","BC",
        "BEST_UP","FBO_UP","EB_UP","3UP","FBO_DN","EB_DN","VBO_DN",
        "FRI34","FRI43","FRI64","L555","L2L4","BLUE","CCI","CCI0R","CCIB",
        "BO_DN","BX_DN","BE_DN","RL","RH","PP",
        "G1","G2","G4","G6","G11","GOG_PLUS",
        "B1","B2","B3","B4","B5","B6","B7","B8","B9","B10","B11","ANY_B",
        "F1","F2","F3","F4","F5","F6","F7","F8","F9","F10","F11","ANY_F",
        "FLY_ABCD","FLY_CD","FLY_BD","FLY_AD",
        "WK_UP","WK_DN","X1","X2","X1G","X3",
        "TZ","T","Z","TZ3","TZ2","TZ_FLIP","BIAS_UP","BIAS_DN",
        "P2","P3","P50","P55","P66","P89","ANY_P",
        "D2","D3","D50","D55","D66","D89","ANY_D",
        "BUY","3G","CONSO","SVS","CD","CA","CW","SEQ_BCONT","NS_DELTA","ND_DELTA",
        "VA","VOL_5X","VOL_10X","VOL_20X",
        "FLP_UP","ORG_UP","DD_UP_RED","D_UP_RED","D_DN_GREEN","DD_DN_GREEN",
        "CISD_CPLUS","CISD_CPLUS_MINUS","CISD_CPLUS_MM",
        "PARA_PREP","PARA_START","PARA_PLUS","PARA_RETEST",
        "NOT_EXT","BE_ANY","L_ANY",
        "4BF_DN",
    ]},
    # Raw signal columns
    "ROCKET":       "rocket",
    "HILO_BUY":     "hilo_buy",
    "THREE_G":      "three_g",
    "SVS":          "svs",
    "SQ":           "sq",
    "LOAD":         "load",
    "F8":           "f8",
    "BE_UP":        "be_up",
    "BO_UP":        "bo_up",
    "BX_UP":        "bx_up",
    "VBO_UP":       "vbo_up",
    "L34":          "l34",
    "L43":          "l43",
    "L22":          "l22",
    "G1P":          "g1p",
    "G2P":          "g2p",
    "G3P":          "g3p",
    "G1L":          "g1l",
    "G2L":          "g2l",
    "G1C":          "g1c",
    "G2C":          "g2c",
    "G3C":          "g3c",
    "PRICE_GT_20":  "price_gt_20",
    "PRICE_GT_50":  "price_gt_50",
    "PRICE_GT_89":  "price_gt_89",
    "PRICE_GT_200": "price_gt_200",
    "PRICE_LT_20":  "price_lt_20",
    "PRICE_LT_50":  "price_lt_50",
    "PRICE_LT_89":  "price_lt_89",
    "PRICE_LT_200": "price_lt_200",
    "RSI_LE_35":    "rsi_le_35",
    "RSI_GE_70":    "rsi_ge_70",
    "TURBO_SCORE":  "turbo_score",
    "TURBO_SCORE_N3": "turbo_score_n3",
    "TURBO_SCORE_N5": "turbo_score_n5",
    "TURBO_SCORE_N10": "turbo_score_n10",
    "VOL_BUCKET":   "vol_bucket",
    "RTB_PHASE":    "rtb_phase",
    "RTB_TOTAL":    "rtb_total",
    "SWING_TYPE":   "swing_type",
    "WYC_PHASE":    "wyc_phase",
    "WYC_SPRING":   "wyc_spring",
    "WYC_SOS":      "wyc_sos",
    "WYC_IN_TR":    "wyc_in_tr",
    "WYC_SOW":      "wyc_sow",
    "AD_FRESH":     "ad_fresh",
    "AD_CLUSTER":   "ad_cluster",
    "PREBREAK_SCORE":  "prebreak_score",
    "PREBREAK_PRIME":  "prebreak_prime",
    "PREBREAK_READY":  "prebreak_ready",
    "PREBREAK_WATCH":  "prebreak_watch",
    "PB_LVBO":         "pb_lvbo",
    "PB_WVF_CONFIRM":  "pb_wvf_confirm",
    "PB_STOP_CAUSE":   "pb_stop_cause",
    "PB_MACRO_PENALTY":"pb_macro_penalty",
}

# DB columns that exist — anything not in this set gets dropped before INSERT
_DB_BOOL_COLS = {
    "sig_best","sig_strong","sig_abs","sig_clm","sig_ns_vabs","sig_nd_vabs",
    "sig_sc","sig_bc","sig_best_up","sig_fbo_up","sig_eb_up","sig_3up",
    "sig_fbo_dn","sig_eb_dn","sig_vbo_dn","sig_fri34","sig_fri43","sig_fri64",
    "sig_l555","sig_l2l4","sig_blue","sig_cci","sig_cci0r","sig_ccib",
    "sig_rl","sig_rh","sig_pp","sig_l_any","sig_be_any","sig_g1","sig_g2",
    "sig_g4","sig_g6","sig_g11","sig_gog_plus","sig_b1","sig_b2","sig_b3",
    "sig_b4","sig_b5","sig_b6","sig_b7","sig_b8","sig_b9","sig_b10","sig_b11",
    "sig_any_b","sig_f1","sig_f2","sig_f3","sig_f4","sig_f5","sig_f6","sig_f7",
    "sig_f8","sig_f9","sig_f10","sig_f11","sig_any_f","sig_fly_abcd","sig_fly_cd",
    "sig_fly_bd","sig_fly_ad","sig_wk_up","sig_wk_dn","sig_x1","sig_x2",
    "sig_x1g","sig_x3","sig_tz","sig_t","sig_z","sig_tz3","sig_tz2",
    "sig_tz_flip","sig_bias_up","sig_bias_dn",
    # Individual T signals
    "sig_t1g","sig_t2g","sig_t1","sig_t2","sig_t3","sig_t4","sig_t5","sig_t6",
    "sig_t7","sig_t8","sig_t9","sig_t10","sig_t11","sig_t12",
    # Individual Z signals
    "sig_z1g","sig_z2g","sig_z1","sig_z2","sig_z3","sig_z4","sig_z5","sig_z6",
    "sig_z7","sig_z8","sig_z9","sig_z10","sig_z11","sig_z12",
    "sig_p2","sig_p3","sig_p50",
    "sig_p55","sig_p66","sig_p89","sig_any_p","sig_d2","sig_d3","sig_d50",
    "sig_d55","sig_d66","sig_d89","sig_any_d","sig_buy","sig_3g","sig_conso",
    "sig_svs","sig_cd","sig_ca","sig_cw","sig_seq_bcont","sig_ns_delta",
    "sig_nd_delta","sig_va","sig_vol_5x","sig_vol_10x","sig_vol_20x",
    "sig_flp_up","sig_org_up","sig_dd_up_red","sig_d_up_red","sig_d_dn_green",
    "sig_dd_dn_green","sig_cisd_cplus","sig_cisd_cplus_minus","sig_cisd_cplus_mm",
    "sig_cisd_plus_struct","sig_cisd_minus_struct","sig_cisd_seq","sig_cisd_mpm",
    "sig_para_prep","sig_para_start","sig_para_plus","sig_para_retest",
    "sig_not_ext","already_extended_flag","l34","l43","l22","be_up","bo_up",
    "bx_up","vbo_up","g1p","g2p","g3p","g1l","g2l","g1c","g2c","g3c",
    "rocket","hilo_buy","three_g","svs","sq","load","f8","price_gt_20",
    "price_gt_50","price_gt_89","price_gt_200","price_lt_20","price_lt_50",
    "price_lt_89","price_lt_200","rsi_le_35","rsi_ge_70","ad_fresh","ad_cluster",
    "wyc_spring","wyc_sos","wyc_in_tr","wyc_sow","prebreak_prime","prebreak_ready",
    "prebreak_watch","pb_lvbo","pb_wvf_confirm","pb_stop_cause","pb_macro_penalty",
}


def _write_progress(done: int, total: int, stage: str, errors: int = 0,
                    started_at: float | None = None, extra: dict | None = None) -> None:
    elapsed = time.time() - (started_at or time.time())
    pct = round(done / total * 100, 1) if total else 0
    eta = round(elapsed / done * (total - done)) if done > 0 else None
    payload = {
        "done": done, "total": total, "pct": pct, "stage": stage,
        "errors": errors, "elapsed_seconds": round(elapsed, 1),
        "eta_seconds": eta,
    }
    if extra:
        payload.update(extra)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(payload, f)


def _compute_rolling(df: pd.DataFrame, n: int, col: str, agg: str) -> pd.Series:
    """
    Compute rolling window stat over the NEXT n bars per ticker.
    agg: 'max_high' | 'min_low' | 'close_shift'
    """
    if agg == "close_shift":
        return df.groupby("ticker")["close"].transform(
            lambda x: (x.shift(-n) / x - 1) * 100
        )
    elif agg == "max_high":
        return df.groupby("ticker")["high"].transform(
            lambda x: x.shift(-1).rolling(n, min_periods=1).max().shift(-(n - 1)) / df["close"] - 1
        ) * 100
    elif agg == "min_low":
        return df.groupby("ticker")["low"].transform(
            lambda x: x.shift(-1).rolling(n, min_periods=1).min().shift(-(n - 1)) / df["close"] - 1
        ) * 100
    raise ValueError(f"Unknown agg: {agg}")


def _compute_mfe(group: pd.DataFrame, n: int) -> pd.Series:
    """max(high[i+1 .. i+n]) / close[i] - 1 for each bar i."""
    highs = group["high"].values
    closes = group["close"].values
    result = np.full(len(group), np.nan)
    for i in range(len(group) - 1):
        end = min(i + n + 1, len(group))
        result[i] = (highs[i + 1 : end].max() / closes[i] - 1) * 100 if end > i + 1 else np.nan
    return pd.Series(result, index=group.index)


def _compute_mae(group: pd.DataFrame, n: int) -> pd.Series:
    """min(low[i+1 .. i+n]) / close[i] - 1 for each bar i."""
    lows = group["low"].values
    closes = group["close"].values
    result = np.full(len(group), np.nan)
    for i in range(len(group) - 1):
        end = min(i + n + 1, len(group))
        result[i] = (lows[i + 1 : end].min() / closes[i] - 1) * 100 if end > i + 1 else np.nan
    return pd.Series(result, index=group.index)


def _add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Add fwd_*d, mfe_*d, mae_*d, and hit_*/drop_* columns."""
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    log.info("  Computing forward close returns...")
    for n, col in [(1,"fwd_1d"),(3,"fwd_3d"),(5,"fwd_5d"),(10,"fwd_10d"),
                   (20,"fwd_20d"),(30,"fwd_30d"),(60,"fwd_60d"),(90,"fwd_90d")]:
        df[col] = df.groupby("ticker")["close"].transform(
            lambda x, _n=n: (x.shift(-_n) / x - 1) * 100
        )

    log.info("  Computing MFE / MAE (this takes ~2 min for 870K rows)...")
    mfe_windows = [(5,"mfe_5d"),(10,"mfe_10d"),(20,"mfe_20d"),(30,"mfe_30d"),(60,"mfe_60d")]
    mae_windows = [(5,"mae_5d"),(10,"mae_10d"),(20,"mae_20d"),(30,"mae_30d")]

    for n, col in mfe_windows:
        df[col] = (
            df.groupby("ticker", group_keys=False)
              .apply(lambda g, _n=n: _compute_mfe(g, _n))
              .values
        )

    for n, col in mae_windows:
        df[col] = (
            df.groupby("ticker", group_keys=False)
              .apply(lambda g, _n=n: _compute_mae(g, _n))
              .values
        )

    log.info("  Computing event flag booleans...")
    df["hit_5pct_5d"]    = (df["mfe_5d"]  >= 5.0).astype("boolean")
    df["hit_10pct_5d"]   = (df["mfe_5d"]  >= 10.0).astype("boolean")
    df["hit_20pct_5d"]   = (df["mfe_5d"]  >= 20.0).astype("boolean")
    df["hit_30pct_10d"]  = (df["mfe_10d"] >= 30.0).astype("boolean")
    df["hit_50pct_20d"]  = (df["mfe_20d"] >= 50.0).astype("boolean")
    df["hit_2x_60d"]     = (df["mfe_60d"] >= 100.0).astype("boolean")
    df["hit_3x_90d"]     = df.get("mfe_90d", pd.Series(np.nan, index=df.index)) >= 200.0
    df["drop_10pct_5d"]  = (df["mae_5d"]  <= -10.0).astype("boolean")
    df["drop_20pct_10d"] = (df["mae_10d"] <= -20.0).astype("boolean")
    df["drop_30pct_20d"] = (df["mae_20d"] <= -30.0).astype("boolean")

    return df


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename CSV columns to DB column names."""
    rename = {}
    for csv_col, db_col in _COL_MAP.items():
        if csv_col in df.columns:
            rename[csv_col] = db_col
    df = df.rename(columns=rename)

    # Also lowercase any remaining uppercase columns
    remaining = {c: c.lower() for c in df.columns if c != c.lower() and c.lower() not in df.columns}
    df = df.rename(columns=remaining)

    # L_CHART override (260525): if CSV had the new chart-format L code,
    # use it as the canonical `l_sig` value. With the v2 patch (main.py:api_bar_signals),
    # L_CHART is already in ascending-digit chart format ("L46" not "L64"), no
    # normalization needed.
    if "l_chart" in df.columns:
        df["l_sig"] = df["l_chart"].fillna("").astype(str)
        log.info("L_CHART column detected — overriding l_sig with chart-format L code")

    return df


def _clean_bool_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce all boolean signal columns to 0/1 integers."""
    for col in _DB_BOOL_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int8")
    return df


def _keep_db_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that don't exist in the DB schema."""
    from studio.db import _SCHEMA_SQL
    import re
    # Extract column names from schema
    db_cols = set(re.findall(r"^\s{4}(\w+)\s+", _SCHEMA_SQL, re.MULTILINE))
    db_cols.discard("PRIMARY"); db_cols.discard("CREATE"); db_cols.discard("UNIQUE")
    db_cols.discard("id")  # auto
    keep = [c for c in df.columns if c in db_cols]
    return df[keep]


def import_csv(
    csv_path: str,
    universe: str,
    started_at: float | None = None,
    total_files: int = 1,
    file_idx: int = 0,
    force: bool = False,
) -> dict:
    """
    Import one bulk_export CSV into DuckDB bars table.
    Returns dict with row/ticker counts.
    """
    started_at = started_at or time.time()
    log.info("Importing %s (universe=%s)...", csv_path, universe)

    # ── 1. Read CSV ────────────────────────────────────────────────────────────
    _write_progress(0, 100, f"reading CSV: {os.path.basename(csv_path)}", started_at=started_at)
    df = pd.read_csv(csv_path, low_memory=False)
    n_raw = len(df)
    log.info("  Read %d rows, %d tickers", n_raw, df["ticker"].nunique())

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["ticker", "date", "close"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["high"]  = pd.to_numeric(df["high"],  errors="coerce")
    df["low"]   = pd.to_numeric(df["low"],   errors="coerce")
    df["open"]  = pd.to_numeric(df["open"],  errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["close", "high", "low"])
    df["universe"] = universe

    _write_progress(10, 100, "computing forward returns", started_at=started_at)

    # ── 2. Forward returns + MFE/MAE ──────────────────────────────────────────
    df = _add_forward_returns(df)

    _write_progress(50, 100, "renaming columns + cleaning", started_at=started_at)

    # ── 3. Column mapping ─────────────────────────────────────────────────────
    df = _rename_columns(df)
    df = _clean_bool_cols(df)

    # ── 3b. Derive individual T/Z boolean columns from t_sig / z_sig ─────────
    _T_NAMES = ["T1G","T2G","T1","T2","T3","T4","T5","T6","T7","T8","T9","T10","T11","T12"]
    _Z_NAMES = ["Z1G","Z2G","Z1","Z2","Z3","Z4","Z5","Z6","Z7","Z8","Z9","Z10","Z11","Z12"]
    if "t_sig" in df.columns:
        for n in _T_NAMES:
            df[f"sig_{n.lower()}"] = (df["t_sig"] == n).astype("int8")
    if "z_sig" in df.columns:
        for n in _Z_NAMES:
            df[f"sig_{n.lower()}"] = (df["z_sig"] == n).astype("int8")

    # ── 4. Assign sequential IDs ──────────────────────────────────────────────
    df = df.reset_index(drop=True)
    df["id"] = df.index + 1 + file_idx * 10_000_000

    # ── 5. Keep only DB columns ───────────────────────────────────────────────
    # Simplified: keep columns that are in _COL_MAP values + known DB cols
    _all_db = (
        {"id","ticker","date","universe","open","high","low","close","volume",
         "fwd_1d","fwd_3d","fwd_5d","fwd_10d","fwd_20d","fwd_30d","fwd_60d","fwd_90d",
         "mfe_5d","mfe_10d","mfe_20d","mfe_30d","mfe_60d",
         "mae_5d","mae_10d","mae_20d","mae_30d",
         "hit_5pct_5d","hit_10pct_5d","hit_20pct_5d","hit_30pct_10d","hit_50pct_20d",
         "hit_2x_60d","hit_3x_90d","drop_10pct_5d","drop_20pct_10d","drop_30pct_20d",
         "turbo_score","turbo_score_n3","turbo_score_n5","turbo_score_n10",
         "vol_bucket","rtb_phase","rtb_total","beta_score","beta_zone",
         "gog_tier","gog_score","final_bull_score","final_regime",
         "t_sig","z_sig","l_sig","fly_sig","g_sig","b_sig","combo_sig",
         "ult_sig","vol_sig","all_signals_text","swing_type",
         "wyc_phase","wyc_spring","wyc_sos","wyc_in_tr","wyc_sow",
         "ad_fresh","ad_cluster",
         "prebreak_score","prebreak_prime","prebreak_ready","prebreak_watch",
         "pb_lvbo","pb_wvf_confirm","pb_stop_cause","pb_macro_penalty",
         }
        | _DB_BOOL_COLS
    )
    df = df[[c for c in df.columns if c in _all_db]]

    _write_progress(60, 100, "writing to DuckDB", started_at=started_at)

    # ── 6. Deduplicate on ticker+date (keep last occurrence) ──────────────────
    before_dedup = len(df)
    df = df.sort_values(["ticker", "date"]).drop_duplicates(
        subset=["ticker", "date"], keep="last"
    ).reset_index(drop=True)
    if len(df) < before_dedup:
        log.info("  Removed %d duplicate ticker+date rows", before_dedup - len(df))

    # Reassign IDs after dedup
    df["id"] = df.index + 1 + file_idx * 10_000_000

    # ── 7. UPSERT into DuckDB ─────────────────────────────────────────────────
    ensure_schema()
    conn = get_conn()
    try:
        # ── REGRESSION GUARD ──────────────────────────────────────────────────
        # A bulk CSV import DELETEs the whole universe then reloads from the CSV,
        # which also wipes enrichment columns (not in the CSV). If the live DB is
        # already NEWER than the CSV (e.g. daily incremental advanced it), this
        # import would silently DESTROY newer bars + all enrichment. Refuse unless
        # force=True. (This is the exact incident that wiped 05-23..05-28 once.)
        csv_max = str(df["date"].max())[:10]
        existing_max = conn.execute(
            "SELECT MAX(date) FROM bars WHERE universe = ?", [universe]
        ).fetchone()[0]
        if existing_max is not None and str(existing_max)[:10] > csv_max and not force:
            raise ValueError(
                f"Refusing CSV import for '{universe}': the DB already has data through "
                f"{str(existing_max)[:10]} but this CSV only goes to {csv_max}. Importing "
                f"would DELETE the newer bars and wipe enrichment. Re-export a current CSV, "
                f"or pass force=true to override (full rebuild — you'll need to re-enrich)."
            )
        # Delete existing rows for this universe first (clean reimport)
        conn.execute("DELETE FROM bars WHERE universe = ?", [universe])
        # Insert via DuckDB's fast DataFrame ingestion
        conn.register("_import_df", df)
        cols = ", ".join(df.columns)
        conn.execute(f"INSERT INTO bars ({cols}) SELECT {cols} FROM _import_df")
        conn.commit()
    finally:
        conn.close()

    n_imported = len(df)
    n_tickers = df["ticker"].nunique()
    log.info("  Inserted %d rows (%d tickers) for universe=%s", n_imported, n_tickers, universe)

    _write_progress(100, 100, "done", started_at=started_at,
                    extra={"rows": n_imported, "tickers": n_tickers, "universe": universe})

    return {
        "universe": universe,
        "csv_path": csv_path,
        "rows_raw": n_raw,
        "rows_imported": n_imported,
        "tickers_imported": n_tickers,
        "date_from": str(df["date"].min()),
        "date_to":   str(df["date"].max()),
        "duration_sec": round(time.time() - started_at, 1),
    }


# ── Default CSV paths (one-time DB-rebuild import seeds → <project>/data/seeds) ──
from studio.paths import seed_path as _seed
UNIVERSE_CSV_MAP: dict[str, str] = {
    "sp500":     _seed("sp500_signals_5y.csv"),
    "nasdaq":    _seed("nasdaq_signals_5y.csv"),
    "russell2k": _seed("russell2k_signals_5y.csv"),
}


def import_all(universes: list[str] | None = None, force: bool = False) -> list[dict]:
    """Import multiple universes sequentially. Returns list of import results.

    force=False (default) refuses to import a CSV that is OLDER than the live DB
    for that universe (prevents wiping newer incremental bars + enrichment)."""
    universes = universes or ["sp500", "nasdaq"]
    started_at = time.time()
    results = []
    for idx, univ in enumerate(universes):
        csv_path = UNIVERSE_CSV_MAP.get(univ)
        if not csv_path or not os.path.exists(csv_path):
            log.warning("CSV not found for universe=%s at %s", univ, csv_path)
            results.append({"universe": univ, "error": "csv not found", "csv_path": csv_path})
            continue
        try:
            r = import_csv(csv_path, univ, started_at=started_at,
                           total_files=len(universes), file_idx=idx, force=force)
            results.append(r)
            # Log to import_log table
            conn = get_conn()
            try:
                conn.execute("""
                    INSERT INTO import_log
                      (universe, csv_path, rows_imported, tickers_imported,
                       date_from, date_to, duration_sec, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'ok')
                """, [
                    univ, csv_path, r["rows_imported"], r["tickers_imported"],
                    r["date_from"], r["date_to"], r["duration_sec"],
                ])
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            log.exception("Import failed for %s: %s", univ, exc)
            results.append({"universe": univ, "error": str(exc)})
    return results
