"""
turbo_engine.py — TURBO combined scan engine.

Runs ALL signal engines per ticker and produces a single turbo_score (0-100)
covering: VABS, Wyckoff, Combo (2809), T/Z patterns, WLNBB (L/FRI/BLUE),
Wick.

turbo_score tiers:
  55+  Fire   — multiple strong signals aligning
  40+  Strong — solid multi-engine confirmation
  25+  Bull   — base bullish setup
  <25  Weak   — sparse signals
"""
from __future__ import annotations

import gc
import os
import logging
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import json
import numpy as np
import pandas as pd

from db import get_db, USE_PG, pk_col

from indicators    import rsi as _rsi_ind, cci as _cci_ind
from signal_engine import compute_signals, compute_b_signals, compute_g_signals
from wlnbb_engine  import compute_wlnbb
from combo_engine  import compute_combo, compute_tz_state
from wick_engine   import compute_wick, compute_wick_x
from vabs_engine   import compute_vabs
from ultra_engine  import compute_260308_l88, compute_ultra_v2
from delta_engine   import compute_delta

log = logging.getLogger(__name__)

# ── Progress ──────────────────────────────────────────────────────────────────
_turbo_state: dict = {
    "running": False,
    "done": 0,
    "total": 0,
    "found": 0,
    "failed": 0,
    "fetched_from_massive": 0,
    "started_at": 0,
    "completed_at": None,
    "universe": None,
    "interval": None,
    "error": None,
    "tfs_total": 1,
    "tfs_done": 0,
}

_SCAN_TIMEOUT = 30 * 60  # 30 minutes max before auto-reset

# All main timeframes covered by a single scan
ALL_SCAN_TFS = ['1wk', '1d', '4h', '1h']


def get_turbo_progress() -> dict:
    state = dict(_turbo_state)
    now = time.time()
    # auto-reset if stuck longer than timeout
    if state["running"] and now - state.get("started_at", 0) > _SCAN_TIMEOUT:
        _turbo_state["running"] = False
        state["running"] = False
    # compute elapsed
    if state.get("started_at"):
        state["elapsed"] = round(now - state["started_at"], 1) if state["running"] else (
            round((state.get("completed_at") or now) - state["started_at"], 1)
        )
    else:
        state["elapsed"] = 0
    # estimate remaining
    done = state.get("done", 0)
    total = state.get("total", 0)
    elapsed = state["elapsed"]
    if state["running"] and done > 5 and total > done:
        rate = done / elapsed if elapsed > 0 else 0
        state["eta"] = round((total - done) / rate, 0) if rate > 0 else None
    else:
        state["eta"] = None
    return state


# ── T/Z signal weights ────────────────────────────────────────────────────────
_TZ_W = {
    "T4": 7, "T6": 7,
    "T1G": 5, "T2G": 5,
    "T1": 4, "T2": 4,
    "T9": 3, "T10": 3,
    "T3": 2, "T11": 2, "T5": 1,
}

# ── DB columns ────────────────────────────────────────────────────────────────
_TURBO_COLS = [
    "turbo_score",
    # VABS
    "best_sig", "strong_sig", "vbo_up", "vbo_dn",
    "abs_sig", "climb_sig", "load_sig",
    # Wyckoff (VABS legacy)
    "ns", "nd", "sc", "bc", "sq",
    # Combo / 2809
    "buy_2809", "rocket", "sig3g", "rtv",
    "hilo_buy", "hilo_sell", "atr_brk", "bb_brk",
    "bias_up", "bias_down", "cons_atr",
    "um_2809", "svs_2809", "conso_2809",
    # B signals (260321) — B1–B11, no RSI filter
    "b1", "b2", "b3", "b4", "b5",
    "b6", "b7", "b8", "b9", "b10", "b11",
    # G signals (260410) — armed by Z10/Z11/Z12, no RSI filter
    "g1", "g2", "g4", "g6", "g11",
    # seqBContLite (260412) — continuation sequence T/Z patterns
    "seq_bcont",
    # VA — ATR Volume Confirm crossover (260402_COMBO_OSC)
    "va",
    # TZ state + confluences + transition signals (260412)
    "tz_state", "ca", "cd", "cw",
    "tz_bull_flip", "tz_attempt",
    # W signals — Bear Dominance→Weakening transition (260414)
    "tz_weak_bull", "tz_weak_bear",
    # T/Z
    "tz_sig", "tz_bull",
    # WLNBB
    "fri34", "fri43", "fri64",
    "l34", "l43", "l64", "l22", "l555", "only_l2l4",
    "blue", "cci_ready", "cci_0_retest", "cci_blue_turn",
    "bo_up", "bo_dn", "bx_up", "bx_dn",
    "be_up", "be_dn",
    "fuchsia_rh", "fuchsia_rl",
    "pre_pump",
    # Wick (3112_2C legacy confirm)
    "wick_bull", "wick_bear",
    # Wick X signals (260402_WICK)
    "x2g_wick", "x2_wick", "x1g_wick", "x1_wick", "x3_wick",
    # meta
    "vol_bucket",
    # Delta / order-flow (260403 V2)
    "d_strong_bull", "d_strong_bear",
    "d_absorb_bull", "d_absorb_bear",
    "d_div_bull",    "d_div_bear",
    "d_cd_bull",     "d_cd_bear",
    "d_surge_bull",  "d_surge_bear",
    "d_blast_bull",  "d_blast_bear",
    "d_vd_div_bull", "d_vd_div_bear",
    "d_spring",      "d_upthrust",
    "d_flip_bull",   "d_flip_bear",   "d_orange_bull",
    "d_blast_bull_red", "d_blast_bear_grn",
    "d_surge_bull_red", "d_surge_bear_grn",
    # RSI / CCI
    "rsi", "cci",
    # 260308 + L88
    "sig_260308", "sig_l88",
    # ULTRA v2
    "eb_bull", "eb_bear",
    "fbo_bull", "fbo_bear",
    "bf_buy", "bf_sell",
    "ultra_3up", "ultra_3dn",
    "best_long", "best_short",
    # RS / Relative Strength vs SPY+IWM
    "rs", "rs_strong",
    # PREUP (EMA cross ↑)
    "preup66", "preup55", "preup89", "preup3", "preup2", "preup50",
    # PREDN (EMA drop ↓)
    "predn66", "predn55", "predn89", "predn3", "predn2", "predn50",
    # N=3, N=5 and N=10 scores (client-side N= switching without rescan)
    "turbo_score_n3",
    "turbo_score_n5",
    "turbo_score_n10",
    # Signal ages JSON {"signal_key": bars_since_last_fire, ...}
    "sig_ages",
    # Data source: "polygon" | "yfinance" (shown as badge in UI)
    "data_source",
    # Average daily volume (20-bar SMA) — used for volume filter
    "avg_vol",
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def _db():
    return get_db()


def _col_def(c: str) -> str:
    _TEXT = {"tz_sig", "vol_bucket", "sig_ages", "data_source"}
    _REAL = {"turbo_score", "turbo_score_n3", "turbo_score_n5", "turbo_score_n10", "rsi", "cci", "avg_vol"}
    typ     = "TEXT"    if c in _TEXT else "REAL" if c in _REAL else "INTEGER"
    default = "''"      if c in _TEXT else "0"
    return f"    {c}  {typ}  DEFAULT {default},"


def _init_db() -> None:
    con = get_db()
    cols_sql = "\n".join(_col_def(c) for c in _TURBO_COLS)
    _pk = pk_col()
    con.executescript(f"""
        CREATE TABLE IF NOT EXISTS turbo_scan_runs (
            id           {_pk},
            tf           TEXT    DEFAULT '1d',
            universe     TEXT    DEFAULT 'sp500',
            started_at   TEXT,
            completed_at TEXT,
            result_count INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS turbo_scan_results (
            id         {_pk},
            scan_id    INTEGER,
            ticker     TEXT NOT NULL,
{cols_sql}
            last_price REAL,
            change_pct REAL,
            scanned_at TEXT
        );
    """)
    con.commit()
    # migration: add any missing columns
    _TEXT_COLS = {"tz_sig", "vol_bucket", "sig_ages", "data_source"}
    _REAL_COLS = {"turbo_score", "turbo_score_n3", "turbo_score_n5", "turbo_score_n10",
                  "rsi", "cci", "avg_vol"}
    try:
        existing = con.table_columns("turbo_scan_results")
        for col in _TURBO_COLS:
            if col not in existing:
                typ     = "TEXT" if col in _TEXT_COLS else "REAL" if col in _REAL_COLS else "INTEGER"
                default = "''"   if col in _TEXT_COLS else "0"
                # Use IF NOT EXISTS on PG to avoid race-condition failures across workers
                if_not = "IF NOT EXISTS " if USE_PG else ""
                con.execute(
                    f"ALTER TABLE turbo_scan_results ADD COLUMN {if_not}{col} {typ} DEFAULT {default}"
                )
        run_cols = con.table_columns("turbo_scan_runs")
        if "tf" not in run_cols:
            con.execute("ALTER TABLE turbo_scan_runs ADD COLUMN tf TEXT DEFAULT '1d'")
        if "universe" not in run_cols:
            con.execute("ALTER TABLE turbo_scan_runs ADD COLUMN universe TEXT DEFAULT 'sp500'")
        con.commit()
    except Exception as _mig_exc:
        log.warning("_init_db migration warning (non-fatal): %s", _mig_exc)
        try:
            if USE_PG:
                con._pg.rollback()
        except Exception:
            pass
    finally:
        con.close()


# ── Score calculator ──────────────────────────────────────────────────────────
def _calc_turbo_score(r: dict) -> float:
    """
    Statistics-based scoring v2 (derived from 593-ticker, 1209-pair co-occurrence analysis).
    Core backbone: conso_2809 (79% freq) → tz_bull (65%) → bf_buy (43%).
    Rarer signals score higher; redundant subsets don't double-count.
      Backbone      cap 18  — conso_2809, tz_bull, chain bonus
      Volume/accum  cap 22  — VABS atomic, Wyckoff, 260308/L88, svs_2809
      Breakout      cap 18  — ULTRA v2, BO/BX (rare→+5), RS
      Combo/trend   cap 14  — Combo signals
      L-structure   cap 13  — T/Z, WLNBB; tz_bull_flip de-duped vs bf_buy
      Delta         cap 12  — Order-flow
      EMA cross     cap 8   — preup series
    Context (Wick) uncapped (max ~18).
    """
    has_conso   = bool(r.get("conso_2809"))
    has_tz_bull = bool(r.get("tz_bull"))
    has_bf_buy  = bool(r.get("bf_buy"))

    # ── Backbone / setup chain (cap 18) ───────────────────────────────────
    # Weights inverse-proportional to frequency: conso 79%→4, tz_bull 65%→6
    # Chain bonus rewards co-occurrence of the full 3-signal backbone
    bkb = 0.0
    if has_conso:   bkb += 4
    if has_tz_bull: bkb += 6
    if has_conso and has_tz_bull and has_bf_buy:
        bkb += 8   # full bullish chain — statistically most predictive combo
    elif has_conso and has_tz_bull:
        bkb += 3   # partial chain (setup without entry confirmation)
    s = min(bkb, 18)

    # ── Volume / accumulation family (cap 22) ─────────────────────────────
    vol = 0.0
    if r.get("abs_sig"):   vol += 5
    if r.get("climb_sig"): vol += 4
    if r.get("load_sig"):  vol += 4   # strong_sig ⊂ load_sig (100%) — load_sig is superset
    if r.get("vbo_up"):    vol += 6
    if r.get("ns"):        vol += 5
    if r.get("sq"):        vol += 4
    if r.get("sc"):        vol += 2
    if r.get("svs_2809"):  vol += 3   # volume expansion within conso_2809 setup
    if r.get("um_2809"):   vol += 3   # NASDAQ: 67% A with tz_bull, 49% with bf_buy
    if r.get("sig_l88"):        vol += 5
    elif r.get("sig_260308"):   vol += 3
    if r.get("va"):             vol += 3
    s += min(vol, 22)

    # ── Breakout / expansion family (cap 18) ──────────────────────────────
    # bf_buy: 43% freq → raised +4→+6 (medium rarity, strong entry signal)
    # bo_up:  14% freq → raised +3→+5 (rare → higher information value)
    brk = 0.0
    if has_bf_buy:          brk += 6
    if r.get("fbo_bull"):   brk += 5
    if r.get("eb_bull"):    brk += 4
    if r.get("ultra_3up"):  brk += 4
    if r.get("bo_up") or r.get("bx_up"): brk += 5
    if r.get("rs_strong"):  brk += 5
    elif r.get("rs"):       brk += 3
    s += min(brk, 18)

    # ── Combo / momentum family (cap 14) ──────────────────────────────────
    combo = 0.0
    if r.get("rocket"):
        combo += 12
    elif r.get("buy_2809"):
        combo += 8
    if r.get("sig3g"):    combo += 4
    if r.get("rtv"):      combo += 3
    if r.get("hilo_buy"): combo += 4   # NASDAQ: 93% A with conso_2809, raised from +2
    if r.get("atr_brk") or r.get("bb_brk"): combo += 2
    if r.get("cd"):   combo += 5
    elif r.get("ca"): combo += 3
    elif r.get("cw"): combo += 2
    if r.get("seq_bcont"): combo += 3
    s += min(combo, 14)

    # ── L-structure / trend family (cap 13) ───────────────────────────────
    trend = 0.0
    trend += _TZ_W.get(r.get("tz_sig", ""), 0)
    # tz_bull_flip: 100%A/98%C triple (FLP↑+4BF+T/Z↑) — strong pattern, raised +1→+3 with bf_buy
    if r.get("tz_bull_flip"):
        trend += 3 if has_bf_buy else 4
    elif r.get("tz_attempt"):
        trend += 2
    if r.get("fri34"):
        trend += 6
    elif r.get("fri43"):
        trend += 4
    if r.get("l34") and not r.get("fri34"): trend += 3
    if r.get("blue"):      trend += 2
    if r.get("cci_ready"): trend += 2
    s += min(trend, 13)

    # ── Delta / order-flow family (cap 12) ───────────────────────────────
    dlt = 0.0
    if r.get("d_blast_bull"):        dlt += 6
    elif r.get("d_surge_bull"):      dlt += 4
    if r.get("d_strong_bull"):       dlt += 5
    if r.get("d_absorb_bull"):       dlt += 4
    if r.get("d_spring"):            dlt += 6
    elif r.get("d_div_bull"):        dlt += 3
    if r.get("d_vd_div_bull"):       dlt += 3
    elif r.get("d_cd_bull"):         dlt += 2
    s += min(dlt, 12)

    # ── EMA cross family (cap 8) ──────────────────────────────────────────
    ema_x = 0.0
    if r.get("preup66"):   ema_x += 8
    elif r.get("preup55"): ema_x += 6
    elif r.get("preup89"): ema_x += 4
    s += min(ema_x, 8)

    # ── Context / confirmation (uncapped, max ~18) ────────────────────────
    if r.get("x2g_wick"):      s += 5
    elif r.get("x2_wick"):     s += 4
    elif r.get("x1g_wick"):    s += 4
    elif r.get("x1_wick"):     s += 3
    elif r.get("x3_wick"):     s += 2
    if r.get("wick_bull"):     s += 5   # 94% C-anchor in WK↑+4BF+T/Z↑ triple, raised +3→+5

    return round(min(100.0, s), 1)


def _sig_age(frame: "pd.DataFrame | None", col: str, max_age: int = 30) -> int:
    """Bars since signal last fired (0 = current bar). Returns max_age if never."""
    if frame is None or col not in frame.columns:
        return max_age
    vals = frame[col].values.astype(bool)
    for i in range(len(vals) - 1, -1, -1):
        if vals[i]:
            return len(vals) - 1 - i
    return max_age


# ── Per-ticker worker ─────────────────────────────────────────────────────────
def _scan_turbo_ticker(
    ticker: str,
    interval: str,
    min_price: float = 0.0,
    max_price: float = 1e9,
    spy_chg: float | None = None,
    iwm_chg: float | None = None,
    partial_day: bool = False,
    min_volume: float = 0,
) -> dict | None:
    try:
        from data_polygon import fetch_bars, polygon_available

        if interval in ("1wk", "1w"):
            days = 400   # need 50+ weekly bars
        elif interval == "1d":
            days = 180
        else:
            days = 90    # 4h, 1h, 30m, 15m

        # ── Fetch OHLCV — Polygon first, yfinance fallback ─────────────────
        df = None
        _data_source = "polygon"
        if polygon_available():
            try:
                df = fetch_bars(ticker, interval=interval, days=days)
            except Exception as exc:
                log.debug("Polygon skip %s: %s — falling back to yfinance", ticker, exc)

        if df is None or df.empty:
            _data_source = "yfinance"
            import yfinance as yf
            period = "5y" if interval in ("1wk", "1w") else "180d" if interval == "1d" else "60d"
            raw = yf.Ticker(ticker).history(period=period, interval=interval, auto_adjust=True)
            if raw is None or raw.empty:
                return None
            raw.columns = [str(c).lower() for c in raw.columns]
            df = raw[["open", "high", "low", "close", "volume"]].dropna()

        if len(df) < 50:
            return None

        # Drop today's bar ONLY while US market is still open.
        # partial_day=True skips this — intentionally keeps the in-progress bar
        # so signals can be computed on the partial daily candle (intraday preview).
        if interval in ("1d", "1wk", "1w") and not partial_day:
            last_dt = df.index[-1]
            if hasattr(last_dt, "date"):
                last_dt = last_dt.date()
            import pytz as _pytz
            _et  = _pytz.timezone("America/New_York")
            now_utc = datetime.now(timezone.utc)
            now_et  = now_utc.astimezone(_et)
            today   = now_utc.date()
            mins_et = now_et.hour * 60 + now_et.minute
            # Mon–Fri, before 16:15 ET → market may still be open → bar incomplete
            market_open = (now_et.weekday() < 5) and (mins_et < 16 * 60 + 15)
            if last_dt == today and market_open:
                df = df.iloc[:-1]
        if len(df) < 40:
            return None

        row: dict = {"ticker": ticker, "data_source": _data_source}

        # ── Price / change ─────────────────────────────────────────────────
        price  = float(df["close"].iloc[-1])
        prev_p = float(df["close"].iloc[-2]) if len(df) >= 2 else price
        row["last_price"] = round(price, 2)
        row["change_pct"] = round((price - prev_p) / prev_p * 100, 2) if prev_p else 0.0

        # ── Price range filter ─────────────────────────────────────────────
        if price < min_price or price > max_price:
            return None

        # ── Average volume filter (20-bar SMA) ────────────────────────────
        avg_vol = float(df["volume"].rolling(20, min_periods=5).mean().iloc[-1])
        row["avg_vol"] = round(avg_vol, 0)
        if min_volume > 0 and avg_vol < min_volume:
            return None

        # Helper: extract bool signal from last N bars (always N=1; ages stored separately)
        def _sig(frame, col, n=1):
            if col not in frame.columns:
                return 0
            return int(bool(frame.iloc[-1][col]))

        # ── T/Z signals ────────────────────────────────────────────────────
        sig_df  = compute_signals(df)
        last_s  = sig_df.iloc[-1]
        tz_bull = bool(last_s.get("is_bull", False))
        tz_name = str(last_s.get("sig_name", "")) if tz_bull else ""
        row["tz_bull"] = int(tz_bull)
        row["tz_sig"]  = tz_name

        # ── WLNBB (L signals, FRI, BLUE, BO, BX, BE, CCI) ─────────────────
        wlnbb    = compute_wlnbb(df)
        last_w   = wlnbb.iloc[-1]
        row["fri34"]         = _sig(wlnbb, "FRI34")
        row["fri43"]         = _sig(wlnbb, "FRI43")
        row["fri64"]         = _sig(wlnbb, "FRI64")
        row["l34"]           = _sig(wlnbb, "L34")
        row["l43"]           = _sig(wlnbb, "L43")
        row["l64"]           = _sig(wlnbb, "L64")
        row["l22"]           = _sig(wlnbb, "L22")
        row["l555"]          = _sig(wlnbb, "L555")
        row["only_l2l4"]     = _sig(wlnbb, "ONLY_L2L4")
        row["blue"]          = _sig(wlnbb, "BLUE")
        row["cci_ready"]     = _sig(wlnbb, "CCI_READY")
        row["cci_0_retest"]  = _sig(wlnbb, "CCI_0_RETEST_OK")
        row["cci_blue_turn"] = _sig(wlnbb, "CCI_BLUE_TURN")
        row["bo_up"]         = _sig(wlnbb, "BO_UP")
        row["bo_dn"]         = _sig(wlnbb, "BO_DN")
        row["bx_up"]         = _sig(wlnbb, "BX_UP")
        row["bx_dn"]         = _sig(wlnbb, "BX_DN")
        row["be_up"]         = _sig(wlnbb, "BE_UP")
        row["be_dn"]         = _sig(wlnbb, "BE_DN")
        row["fuchsia_rh"]    = _sig(wlnbb, "FUCHSIA_RH")
        row["fuchsia_rl"]    = _sig(wlnbb, "FUCHSIA_RL")
        row["pre_pump"]      = _sig(wlnbb, "PRE_PUMP")
        bkt = str(last_w.get("vol_bucket", ""))
        row["vol_bucket"] = bkt
        row["l_combo"] = str(last_w.get("l_combo", "")) or ""

        # ── Combo (2809, CONS, Bias, HILO, RTV, 3G, ROCKET, BRK, PREUP/DN) ─
        combo   = compute_combo(df)
        row["buy_2809"]  = _sig(combo, "buy_2809")
        row["rocket"]    = _sig(combo, "rocket")
        row["sig3g"]     = _sig(combo, "sig3g")
        row["rtv"]       = _sig(combo, "rtv")
        row["hilo_buy"]  = _sig(combo, "hilo_buy")
        row["hilo_sell"] = _sig(combo, "hilo_sell")
        row["atr_brk"]   = _sig(combo, "atr_brk")
        row["bb_brk"]    = _sig(combo, "bb_brk")
        row["bias_up"]   = _sig(combo, "bias_up")
        row["bias_down"] = _sig(combo, "bias_down")
        row["cons_atr"]  = _sig(combo, "cons_atr")
        # 2809 phase labels (UM/SVS/CONSO)
        row["um_2809"]    = _sig(combo, "um_2809")
        row["svs_2809"]   = _sig(combo, "svs_2809")
        row["conso_2809"] = _sig(combo, "conso_2809")
        # PREUP — EMA cross ↑ (strongest = P66)
        row["preup66"]   = _sig(combo, "preup66")
        row["preup55"]   = _sig(combo, "preup55")
        row["preup89"]   = _sig(combo, "preup89")
        row["preup3"]    = _sig(combo, "preup3")
        row["preup2"]    = _sig(combo, "preup2")
        row["preup50"]   = _sig(combo, "preup50")
        # PREDN — EMA drop ↓ (strongest = D66)
        row["predn66"]   = _sig(combo, "predn66")
        row["predn55"]   = _sig(combo, "predn55")
        row["predn89"]   = _sig(combo, "predn89")
        row["predn3"]    = _sig(combo, "predn3")
        row["predn2"]    = _sig(combo, "predn2")
        row["predn50"]   = _sig(combo, "predn50")

        # ── B signals (260321) — B1–B11, no RSI filter ───────────────────
        b_sigs = compute_b_signals(df)
        for _b in range(1, 12):
            row[f"b{_b}"] = _sig(b_sigs, f"b{_b}")
        # ── G signals (260410) — armed by Z10/Z11/Z12, no RSI filter ─────
        g_sigs = compute_g_signals(df)
        row["g1"]  = _sig(g_sigs, "g1")
        row["g2"]  = _sig(g_sigs, "g2")
        row["g4"]  = _sig(g_sigs, "g4")
        row["g6"]  = _sig(g_sigs, "g6")
        row["g11"] = _sig(g_sigs, "g11")
        # ── seqBContLite (260412) — continuation-lite sequence ────────────
        try:
            _sig_obj  = compute_signals(df)
            _bc_ser   = _sig_obj["bc"].fillna(0).astype(int)
            _bc_c  = int(_bc_ser.iloc[-1]) if len(_bc_ser) > 0 else 0
            _bc_p1 = int(_bc_ser.iloc[-2]) if len(_bc_ser) > 1 else 0
            _bc_p2 = int(_bc_ser.iloc[-3]) if len(_bc_ser) > 2 else 0
            row["seq_bcont"] = int(
                (_bc_p2 in {5, 3, 6, 4, 7} and _bc_c == 1) or   # T1/T1G/T2/T2G/T9 @-2 → T4
                (_bc_p1 in {9, 10, 11}      and _bc_c in {1, 2}) or  # T3/T11/T5 @-1 → T4/T6
                (_bc_p1 in {1, 4, 9}        and _bc_c == 2)          # T4/T2G/T3 @-1 → T6
            )
        except Exception:
            row["seq_bcont"] = 0
        # ── VA — ATR Volume Confirm (260402_COMBO_OSC) ────────────────────
        # volConfirmATR = ta.crossover(volume / ta.sma(volume, 20), 2.0)
        try:
            _avg_vol  = df["volume"].rolling(20, min_periods=1).mean()
            _vr       = df["volume"] / _avg_vol.replace(0, np.nan)
            _vr_now   = float(_vr.iloc[-1])  if not pd.isna(_vr.iloc[-1])  else 0.0
            _vr_prev  = float(_vr.iloc[-2])  if len(_vr) > 1 and not pd.isna(_vr.iloc[-2]) else 0.0
            row["va"] = int(_vr_now > 2.0 and _vr_prev <= 2.0)
        except Exception:
            row["va"] = 0
        # ── TZ state machine + CA/CD/CW + transition signals (260412) ─────
        tz_st = compute_tz_state(df)
        row["tz_state"] = int(tz_st.iloc[-1]) if len(tz_st) else 0
        _any_b = any(row.get(f"b{_b}", 0) for _b in range(1, 12))
        _last_st = row["tz_state"]
        row["ca"] = int(_any_b and _last_st == 2)  # Bull Attempt + B
        row["cd"] = int(_any_b and _last_st == 3)  # Bull Dom + B
        row["cw"] = int(_any_b and _last_st == 1)  # Bear Weakening + B
        # TZ transition: bullFlip=state just became 3, attempt=state just became 2
        if len(tz_st) >= 2:
            _tz_prev = int(tz_st.iloc[-2])
            _tz_curr = int(tz_st.iloc[-1])
            row["tz_bull_flip"] = int(_tz_curr == 3 and _tz_prev != 3)
            row["tz_attempt"]   = int(_tz_curr == 2 and _tz_prev != 2)
        else:
            row["tz_bull_flip"] = 0
            row["tz_attempt"]   = 0
        # W signals: earlyWeakening = tzState just moved from 0 (Bear Dom) → 1 (Bear Weak)
        if len(tz_st) >= 2:
            _ew = int(tz_st.iloc[-1]) == 1 and int(tz_st.iloc[-2]) == 0
            _bar_bull = df["close"].iloc[-1] > df["open"].iloc[-1]
            row["tz_weak_bull"] = int(_ew and _bar_bull)
            row["tz_weak_bear"] = int(_ew and not _bar_bull)
            # Full-series W signals for sig_ages
            _tz_st_shifted = tz_st.shift(1, fill_value=0).astype(int)
            _early_weak_ser = (tz_st.astype(int) == 1) & (_tz_st_shifted == 0)
            _bull_bar_ser   = df["close"] > df["open"]
            _tz_weak_df = pd.DataFrame({
                "tz_weak_bull": _early_weak_ser & _bull_bar_ser,
                "tz_weak_bear": _early_weak_ser & ~_bull_bar_ser,
            }, index=df.index)
        else:
            row["tz_weak_bull"] = 0
            row["tz_weak_bear"] = 0

        # ── VABS (ABS, CLIMB, LOAD, Wyckoff, BEST, STRONG, VBO) ───────────
        vabs    = compute_vabs(df)
        row["abs_sig"]    = _sig(vabs, "abs_sig")
        row["climb_sig"]  = _sig(vabs, "climb_sig")
        row["load_sig"]   = _sig(vabs, "load_sig")
        row["ns"]         = _sig(vabs, "ns")
        row["nd"]         = _sig(vabs, "nd")
        row["sc"]         = _sig(vabs, "sc")
        row["bc"]         = _sig(vabs, "bc")
        row["sq"]         = _sig(vabs, "sq")
        row["best_sig"]   = _sig(vabs, "best_sig")
        row["strong_sig"] = _sig(vabs, "strong_sig")
        row["vbo_up"]     = _sig(vabs, "vbo_up")
        row["vbo_dn"]     = _sig(vabs, "vbo_dn")

        wx = ddf = u308 = uv2 = _tz_weak_df = None   # pre-initialise for age computation

        # ── Wick (3112_2C legacy) ─────────────────────────────────────────
        wick   = compute_wick(df)
        row["wick_bull"] = _sig(wick, "WICK_BULL_CONFIRM")
        row["wick_bear"] = _sig(wick, "WICK_BEAR_CONFIRM")

        # ── Wick X (260402_WICK) ──────────────────────────────────────────
        try:
            wx = compute_wick_x(df)
            row["x2g_wick"] = _sig(wx, "x2g_wick")
            row["x2_wick"]  = _sig(wx, "x2_wick")
            row["x1g_wick"] = _sig(wx, "x1g_wick")
            row["x1_wick"]  = _sig(wx, "x1_wick")
            row["x3_wick"]  = _sig(wx, "x3_wick")
        except Exception:
            row["x2g_wick"] = row["x2_wick"] = row["x1g_wick"] = row["x1_wick"] = row["x3_wick"] = 0

        # ── Delta / order-flow (260403 V2) ────────────────────────────────
        try:
            ddf = compute_delta(df)
            for col in ("strong_bull","strong_bear","absorb_bull","absorb_bear",
                        "div_bull","div_bear","cd_bull","cd_bear",
                        "surge_bull","surge_bear","blast_bull","blast_bear",
                        "vd_div_bull","vd_div_bear","spring","upthrust",
                        "flip_bull","flip_bear","orange_bull",
                        "blast_bull_red","blast_bear_grn",
                        "surge_bull_red","surge_bear_grn"):
                row[f"d_{col}"] = _sig(ddf, col)
        except Exception:
            for col in ("strong_bull","strong_bear","absorb_bull","absorb_bear",
                        "div_bull","div_bear","cd_bull","cd_bear",
                        "surge_bull","surge_bear","blast_bull","blast_bear",
                        "vd_div_bull","vd_div_bear","spring","upthrust",
                        "flip_bull","flip_bear","orange_bull",
                        "blast_bull_red","blast_bear_grn",
                        "surge_bull_red","surge_bear_grn"):
                row[f"d_{col}"] = 0

        # ── RSI(14) ────────────────────────────────────────────────────────
        try:
            row["rsi"] = round(float(_rsi_ind(df["close"], 14).iloc[-1]), 1)
        except Exception:
            row["rsi"] = 0.0

        # ── CCI(20) ────────────────────────────────────────────────────────
        try:
            row["cci"] = round(float(_cci_ind(df["high"], df["low"], df["close"], 20).iloc[-1]), 1)
        except Exception:
            row["cci"] = 0.0

        # ── 260308 + L88 ───────────────────────────────────────────────────
        try:
            u308   = compute_260308_l88(df)
            last_u = u308.iloc[-1]
            row["sig_260308"] = int(bool(last_u.get("sig_260308", False)))
            row["sig_l88"]    = int(bool(last_u.get("sig_l88",    False)))
        except Exception:
            row["sig_260308"] = 0
            row["sig_l88"]    = 0

        # ── ULTRA v2 ───────────────────────────────────────────────────────
        try:
            uv2 = compute_ultra_v2(df)
            row["eb_bull"]    = _sig(uv2, "eb_bull")
            row["eb_bear"]    = _sig(uv2, "eb_bear")
            row["fbo_bull"]   = _sig(uv2, "fbo_bull")
            row["fbo_bear"]   = _sig(uv2, "fbo_bear")
            row["bf_buy"]     = _sig(uv2, "bf_buy")
            row["bf_sell"]    = _sig(uv2, "bf_sell")
            row["ultra_3up"]  = _sig(uv2, "ultra_3up")
            row["ultra_3dn"]  = _sig(uv2, "ultra_3dn")
            row["best_long"]  = _sig(uv2, "best_long")
            row["best_short"] = _sig(uv2, "best_short")
        except Exception:
            for k in ("eb_bull","eb_bear","fbo_bull","fbo_bear","bf_buy","bf_sell",
                      "ultra_3up","ultra_3dn","best_long","best_short"):
                row[k] = 0

        # ── RS / Relative Strength vs SPY + IWM ───────────────────────────
        # RS: ticker up ≥ 0.5% while SPY AND IWM both down ≥ 0.3%
        # RS+: RS AND high volume (bucket B or VB)
        try:
            if spy_chg is not None and iwm_chg is not None:
                rs_cond = (
                    row["change_pct"] >= 0.5
                    and spy_chg <= -0.3
                    and iwm_chg <= -0.3
                )
                row["rs"] = int(rs_cond)
                row["rs_strong"] = int(rs_cond and bkt in ("B", "VB"))
            else:
                row["rs"] = 0
                row["rs_strong"] = 0
        except Exception:
            row["rs"] = 0
            row["rs_strong"] = 0

        # ── TURBO SCORE ────────────────────────────────────────────────────
        row["turbo_score"] = _calc_turbo_score(row)

        # ── Signal ages + N=5 / N=10 scores (for client N= switching) ──────
        def _sa(frame, col):
            return _sig_age(frame, col)

        def _sn(frame, col, n):
            if frame is None or col not in frame.columns:
                return 0
            return int(frame[col].iloc[-n:].any())

        row["sig_ages"] = json.dumps({
            # WLNBB
            "fri34":         _sa(wlnbb, "FRI34"),
            "fri43":         _sa(wlnbb, "FRI43"),
            "fri64":         _sa(wlnbb, "FRI64"),
            "l34":           _sa(wlnbb, "L34"),
            "l43":           _sa(wlnbb, "L43"),
            "l64":           _sa(wlnbb, "L64"),
            "l22":           _sa(wlnbb, "L22"),
            "l555":          _sa(wlnbb, "L555"),
            "only_l2l4":     _sa(wlnbb, "ONLY_L2L4"),
            "blue":          _sa(wlnbb, "BLUE"),
            "cci_ready":     _sa(wlnbb, "CCI_READY"),
            "cci_0_retest":  _sa(wlnbb, "CCI_0_RETEST_OK"),
            "cci_blue_turn": _sa(wlnbb, "CCI_BLUE_TURN"),
            "bo_up":         _sa(wlnbb, "BO_UP"),
            "bo_dn":         _sa(wlnbb, "BO_DN"),
            "bx_up":         _sa(wlnbb, "BX_UP"),
            "bx_dn":         _sa(wlnbb, "BX_DN"),
            "be_up":         _sa(wlnbb, "BE_UP"),
            "be_dn":         _sa(wlnbb, "BE_DN"),
            "fuchsia_rh":    _sa(wlnbb, "FUCHSIA_RH"),
            "fuchsia_rl":    _sa(wlnbb, "FUCHSIA_RL"),
            "pre_pump":      _sa(wlnbb, "PRE_PUMP"),
            # Combo
            "buy_2809":   _sa(combo, "buy_2809"),
            "rocket":     _sa(combo, "rocket"),
            "sig3g":      _sa(combo, "sig3g"),
            "rtv":        _sa(combo, "rtv"),
            "hilo_buy":   _sa(combo, "hilo_buy"),
            "hilo_sell":  _sa(combo, "hilo_sell"),
            "atr_brk":    _sa(combo, "atr_brk"),
            "bb_brk":     _sa(combo, "bb_brk"),
            "bias_up":    _sa(combo, "bias_up"),
            "bias_down":  _sa(combo, "bias_down"),
            "cons_atr":   _sa(combo, "cons_atr"),
            "um_2809":    _sa(combo, "um_2809"),
            "svs_2809":   _sa(combo, "svs_2809"),
            "conso_2809": _sa(combo, "conso_2809"),
            "preup66":    _sa(combo, "preup66"),
            "preup55":    _sa(combo, "preup55"),
            "preup89":    _sa(combo, "preup89"),
            "preup3":     _sa(combo, "preup3"),
            "preup2":     _sa(combo, "preup2"),
            "preup50":    _sa(combo, "preup50"),
            "predn66":    _sa(combo, "predn66"),
            "predn55":    _sa(combo, "predn55"),
            "predn89":    _sa(combo, "predn89"),
            "predn3":     _sa(combo, "predn3"),
            "predn2":     _sa(combo, "predn2"),
            "predn50":    _sa(combo, "predn50"),
            # B signals
            **{f"b{i}": _sa(b_sigs, f"b{i}") for i in range(1, 12)},
            # G signals
            "g1":  _sa(g_sigs, "g1"),
            "g2":  _sa(g_sigs, "g2"),
            "g4":  _sa(g_sigs, "g4"),
            "g6":  _sa(g_sigs, "g6"),
            "g11": _sa(g_sigs, "g11"),
            # seqBContLite + VA (stored as 0/1, age = 0 if fired today)
            "seq_bcont": 0 if row.get("seq_bcont") else 999,
            "va":        0 if row.get("va")        else 999,
            # VABS
            "abs_sig":    _sa(vabs, "abs_sig"),
            "climb_sig":  _sa(vabs, "climb_sig"),
            "load_sig":   _sa(vabs, "load_sig"),
            "ns":         _sa(vabs, "ns"),
            "nd":         _sa(vabs, "nd"),
            "sc":         _sa(vabs, "sc"),
            "bc":         _sa(vabs, "bc"),
            "sq":         _sa(vabs, "sq"),
            "best_sig":   _sa(vabs, "best_sig"),
            "strong_sig": _sa(vabs, "strong_sig"),
            "vbo_up":     _sa(vabs, "vbo_up"),
            "vbo_dn":     _sa(vabs, "vbo_dn"),
            # Wick
            "wick_bull": _sa(wick, "WICK_BULL_CONFIRM"),
            "wick_bear": _sa(wick, "WICK_BEAR_CONFIRM"),
            # Wick X
            "x2g_wick": _sa(wx, "x2g_wick"),
            "x2_wick":  _sa(wx, "x2_wick"),
            "x1g_wick": _sa(wx, "x1g_wick"),
            "x1_wick":  _sa(wx, "x1_wick"),
            "x3_wick":  _sa(wx, "x3_wick"),
            # 260308 / L88
            "sig_260308": _sa(u308, "sig_260308"),
            "sig_l88":    _sa(u308, "sig_l88"),
            # Ultra v2
            "eb_bull":    _sa(uv2, "eb_bull"),
            "eb_bear":    _sa(uv2, "eb_bear"),
            "fbo_bull":   _sa(uv2, "fbo_bull"),
            "fbo_bear":   _sa(uv2, "fbo_bear"),
            "bf_buy":     _sa(uv2, "bf_buy"),
            "bf_sell":    _sa(uv2, "bf_sell"),
            "ultra_3up":  _sa(uv2, "ultra_3up"),
            "ultra_3dn":  _sa(uv2, "ultra_3dn"),
            "best_long":  _sa(uv2, "best_long"),
            "best_short": _sa(uv2, "best_short"),
            # Delta
            "d_strong_bull": _sa(ddf, "strong_bull"),
            "d_strong_bear": _sa(ddf, "strong_bear"),
            "d_absorb_bull": _sa(ddf, "absorb_bull"),
            "d_absorb_bear": _sa(ddf, "absorb_bear"),
            "d_div_bull":    _sa(ddf, "div_bull"),
            "d_div_bear":    _sa(ddf, "div_bear"),
            "d_cd_bull":     _sa(ddf, "cd_bull"),
            "d_cd_bear":     _sa(ddf, "cd_bear"),
            "d_surge_bull":  _sa(ddf, "surge_bull"),
            "d_surge_bear":  _sa(ddf, "surge_bear"),
            "d_blast_bull":  _sa(ddf, "blast_bull"),
            "d_blast_bear":  _sa(ddf, "blast_bear"),
            "d_vd_div_bull": _sa(ddf, "vd_div_bull"),
            "d_vd_div_bear": _sa(ddf, "vd_div_bear"),
            "d_spring":      _sa(ddf, "spring"),
            "d_upthrust":    _sa(ddf, "upthrust"),
            "d_flip_bull":       _sa(ddf, "flip_bull"),
            "d_flip_bear":       _sa(ddf, "flip_bear"),
            "d_orange_bull":     _sa(ddf, "orange_bull"),
            "d_blast_bull_red":  _sa(ddf, "blast_bull_red"),
            "d_blast_bear_grn":  _sa(ddf, "blast_bear_grn"),
            "d_surge_bull_red":  _sa(ddf, "surge_bull_red"),
            "d_surge_bear_grn":  _sa(ddf, "surge_bear_grn"),
            # W signals
            "tz_weak_bull":  _sa(_tz_weak_df, "tz_weak_bull"),
            "tz_weak_bear":  _sa(_tz_weak_df, "tz_weak_bear"),
        }, separators=(',', ':'))

        # N=3, N=5 and N=10 turbo scores
        for _n, _key in ((3, "turbo_score_n3"), (5, "turbo_score_n5"), (10, "turbo_score_n10")):
            _r = {
                # Volume / accum
                "abs_sig":    _sn(vabs,  "abs_sig",    _n),
                "climb_sig":  _sn(vabs,  "climb_sig",  _n),
                "load_sig":   _sn(vabs,  "load_sig",   _n),
                "vbo_up":     _sn(vabs,  "vbo_up",     _n),
                "ns":         _sn(vabs,  "ns",          _n),
                "sq":         _sn(vabs,  "sq",          _n),
                "sc":         _sn(vabs,  "sc",          _n),
                "sig_l88":    _sn(u308,  "sig_l88",    _n),
                "sig_260308": _sn(u308,  "sig_260308", _n),
                # Breakout
                "fbo_bull":     _sn(uv2,   "fbo_bull",   _n),
                "eb_bull":      _sn(uv2,   "eb_bull",    _n),
                "bf_buy":       _sn(uv2,   "bf_buy",     _n),
                "ultra_3up":    _sn(uv2,   "ultra_3up",  _n),
                "bo_up":        _sn(wlnbb, "BO_UP",      _n),
                "bx_up":        _sn(wlnbb, "BX_UP",      _n),
                "rs_strong":    row.get("rs_strong", 0),
                "rs":           row.get("rs", 0),
                # Combo
                "rocket":   _sn(combo, "rocket",   _n),
                "buy_2809": _sn(combo, "buy_2809", _n),
                "sig3g":    _sn(combo, "sig3g",    _n),
                "rtv":      _sn(combo, "rtv",      _n),
                "hilo_buy": _sn(combo, "hilo_buy", _n),
                "atr_brk":  _sn(combo, "atr_brk",  _n),
                "bb_brk":   _sn(combo, "bb_brk",   _n),
                "cd":  row.get("cd", 0),
                "ca":  row.get("ca", 0),
                "cw":  row.get("cw", 0),
                # L-structure
                "tz_sig":    tz_name,
                "fri34":     _sn(wlnbb, "FRI34",     _n),
                "fri43":     _sn(wlnbb, "FRI43",     _n),
                "l34":       _sn(wlnbb, "L34",       _n),
                "blue":      _sn(wlnbb, "BLUE",      _n),
                "cci_ready": _sn(wlnbb, "CCI_READY", _n),
                # Delta
                "d_blast_bull":  _sn(ddf, "blast_bull",  _n),
                "d_surge_bull":  _sn(ddf, "surge_bull",  _n),
                "d_strong_bull": _sn(ddf, "strong_bull", _n),
                "d_absorb_bull": _sn(ddf, "absorb_bull", _n),
                "d_spring":      _sn(ddf, "spring",      _n),
                "d_div_bull":    _sn(ddf, "div_bull",    _n),
                "d_vd_div_bull": _sn(ddf, "vd_div_bull", _n),
                "d_cd_bull":     _sn(ddf, "cd_bull",     _n),
                "d_flip_bull":   _sn(ddf, "flip_bull",   _n),
                "d_orange_bull":    _sn(ddf, "orange_bull",    _n),
                "d_blast_bull_red": _sn(ddf, "blast_bull_red", _n),
                "d_surge_bull_red": _sn(ddf, "surge_bull_red", _n),
                # EMA cross
                "preup66": _sn(combo, "preup66", _n),
                "preup55": _sn(combo, "preup55", _n),
                "preup89": _sn(combo, "preup89", _n),
                "preup3":  _sn(combo, "preup3",  _n),
                "preup2":  _sn(combo, "preup2",  _n),
                "preup50": _sn(combo, "preup50", _n),
                # Context (wick)
                "x2g_wick": _sn(wx,   "x2g_wick",          _n),
                "x2_wick":  _sn(wx,   "x2_wick",           _n),
                "x1g_wick": _sn(wx,   "x1g_wick",          _n),
                "x1_wick":  _sn(wx,   "x1_wick",           _n),
                "x3_wick":  _sn(wx,   "x3_wick",           _n),
                "wick_bull": _sn(wick, "WICK_BULL_CONFIRM", _n),
            }
            row[_key] = _calc_turbo_score(_r)

        return row

    except Exception as exc:
        log.debug("Turbo skip %s: %s", ticker, exc)
        return None


# ── Scan runner ───────────────────────────────────────────────────────────────
def run_turbo_scan(
    interval: str = "1d",
    universe: str = "sp500",
    workers: int = 4,
    lookback_n: int = 5,
    partial_day: bool = False,
    min_volume: float = 0,
    _keep_running: bool = False,  # internal: skip running=False at end (used by all-TF wrapper)
) -> int:
    from scanner import get_universe_tickers, UNIVERSE_CONFIGS
    global _turbo_state

    _init_db()
    cfg = UNIVERSE_CONFIGS.get(universe, UNIVERSE_CONFIGS["sp500"])
    min_price = float(cfg["min_price"])
    max_price = float(cfg["max_price"])

    _turbo_state.update({
        "running": True, "done": 0, "total": 0, "found": 0, "failed": 0,
        "fetched_from_massive": 0, "universe": universe, "interval": interval,
        "partial_day": partial_day,
        "started_at": time.time(), "completed_at": None, "error": None,
    })
    try:
        tickers = get_universe_tickers(universe)
    except Exception as exc:
        _turbo_state.update({"running": False, "error": str(exc)})
        log.error("Failed to fetch tickers for universe=%s: %s", universe, exc)
        return 0

    # ── Filter: keep only plain US common-stock primary-listing tickers ────
    # Removes "CFLT B" (space=secondary class), "BF.B" (dot=preferred), etc.
    from data_polygon import _is_valid_stock_ticker
    tickers = [t for t in tickers if _is_valid_stock_ticker(t)]

    # ── Fetch SPY + IWM once for RS computation ────────────────────────────
    spy_chg: float | None = None
    iwm_chg: float | None = None
    try:
        from data_polygon import fetch_bars, polygon_available
        days = 5
        for _sym, _attr in (("SPY", "spy_chg"), ("IWM", "iwm_chg")):
            _df = None
            if polygon_available():
                try:
                    _df = fetch_bars(_sym, interval=interval, days=days)
                except Exception:
                    pass
            if _df is None or _df.empty:
                import yfinance as yf
                _raw = yf.Ticker(_sym).history(period="5d", interval=interval, auto_adjust=True)
                if _raw is not None and not _raw.empty:
                    _raw.columns = [str(c).lower() for c in _raw.columns]
                    _df = _raw[["close"]].dropna()
            if _df is not None and len(_df) >= 2:
                _chg = (_df["close"].iloc[-1] - _df["close"].iloc[-2]) / _df["close"].iloc[-2] * 100
                if _attr == "spy_chg":
                    spy_chg = round(float(_chg), 3)
                else:
                    iwm_chg = round(float(_chg), 3)
    except Exception as exc:
        log.debug("Could not fetch SPY/IWM for RS: %s", exc)

    _turbo_state["total"] = len(tickers)
    _turbo_state["fetched_from_massive"] = len(tickers)
    now_iso = datetime.now(timezone.utc).isoformat()

    con = _db()
    _ret = " RETURNING id" if USE_PG else ""
    scan_id = con.execute(
        f"INSERT INTO turbo_scan_runs (tf, universe, started_at) VALUES (?, ?, ?){_ret}",
        (interval, universe, now_iso),
    ).lastrowid
    con.commit()
    con.close()

    cols = ["scan_id", "ticker", "last_price", "change_pct", "scanned_at"] + _TURBO_COLS
    ph   = ", ".join(f":{c}" for c in cols)
    found = 0
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    _scan_turbo_ticker, t, interval, min_price, max_price,
                    spy_chg, iwm_chg, partial_day, min_volume
                ): t
                for t in tickers
            }
            batch: list[dict] = []
            for fut in as_completed(futures):
                _turbo_state["done"] += 1
                row = fut.result()
                if row:
                    row["scan_id"]    = scan_id
                    row["scanned_at"] = now_iso
                    batch.append(row)
                    found += 1
                    _turbo_state["found"] += 1
                else:
                    _turbo_state["failed"] += 1
                # flush every 50 results so progress survives restart
                if len(batch) >= 50:
                    con = _db()
                    con.executemany(f"INSERT INTO turbo_scan_results ({', '.join(cols)}) VALUES ({ph})", batch)
                    con.commit()
                    con.close()
                    batch.clear()
                    gc.collect()  # release DataFrame memory between batches
            # flush remaining
            if batch:
                con = _db()
                con.executemany(f"INSERT INTO turbo_scan_results ({', '.join(cols)}) VALUES ({ph})", batch)
                con.commit()
                con.close()

        # mark completed
        con = _db()
        con.execute(
            "UPDATE turbo_scan_runs SET completed_at=?, result_count=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), found, scan_id),
        )
        # Keep last 3 completed runs per tf+universe combo (only touch this tf+universe)
        con.execute("""
            DELETE FROM turbo_scan_results WHERE scan_id IN (
                SELECT id FROM turbo_scan_runs WHERE tf=? AND universe=?
            ) AND scan_id NOT IN (
                SELECT id FROM turbo_scan_runs
                WHERE tf=? AND universe=? AND completed_at IS NOT NULL
                ORDER BY id DESC LIMIT 3
            )
        """, (interval, universe, interval, universe))
        con.commit()
        con.close()
    finally:
        if not _keep_running:
            _turbo_state["running"] = False
            _turbo_state["completed_at"] = time.time()

    log.info("Turbo scan done: %d/%d tickers, tf=%s universe=%s", found, len(tickers), interval, universe)
    return found


# ── Multi-TF scan ─────────────────────────────────────────────────────────────

def run_turbo_scan_all_tfs(
    universe: str = "sp500",
    workers: int = 4,
    lookback_n: int = 5,
    partial_day: bool = False,
    min_volume: float = 0,
) -> dict:
    """
    Run turbo scan for all main timeframes (1wk, 1d, 4h, 1h) in sequence.
    One call covers all TFs so switching the TF display button needs no rescan.
    """
    global _turbo_state
    _turbo_state.update({"tfs_total": len(ALL_SCAN_TFS), "tfs_done": 0})
    results: dict = {}
    try:
        for i, tf in enumerate(ALL_SCAN_TFS):
            _turbo_state["tfs_done"] = i
            is_last = (i == len(ALL_SCAN_TFS) - 1)
            # _keep_running=True suppresses the running=False that run_turbo_scan
            # normally sets in its finally block — avoids the race where the 2-second
            # frontend poll catches running=False between consecutive TF scans.
            found = run_turbo_scan(
                tf, universe, workers, lookback_n, partial_day, min_volume,
                _keep_running=not is_last,
            )
            results[tf] = found
    finally:
        _turbo_state["running"] = False
        _turbo_state["completed_at"] = time.time()
        _turbo_state["tfs_done"] = len(ALL_SCAN_TFS)
    log.info("Multi-TF turbo scan done: %s, universe=%s", results, universe)
    return results


# ── Query ─────────────────────────────────────────────────────────────────────
_QUERY_COLS = (
    "ticker, turbo_score, vol_bucket, tz_sig, tz_bull, "
    "best_sig, strong_sig, vbo_up, vbo_dn, abs_sig, climb_sig, load_sig, "
    "ns, nd, sc, bc, sq, "
    "buy_2809, rocket, sig3g, rtv, hilo_buy, hilo_sell, atr_brk, bb_brk, "
    "bias_up, bias_down, cons_atr, "
    "fri34, fri43, fri64, l34, l43, l64, l22, l555, only_l2l4, "
    "blue, cci_ready, cci_0_retest, cci_blue_turn, "
    "bo_up, bo_dn, bx_up, bx_dn, be_up, be_dn, "
    "fuchsia_rh, fuchsia_rl, pre_pump, "
    "wick_bull, wick_bear, "
    "rsi, cci, "
    "d_strong_bull, d_absorb_bull, d_div_bull, d_cd_bull, d_surge_bull, d_blast_bull, "
    "d_strong_bear, d_absorb_bear, d_div_bear, d_cd_bear, d_surge_bear, d_blast_bear, "
    "sig_260308, sig_l88, "
    "eb_bull, eb_bear, fbo_bull, fbo_bear, bf_buy, bf_sell, "
    "ultra_3up, ultra_3dn, best_long, best_short, "
    "last_price, change_pct, scanned_at"
)
_QUERY_KEYS = [c.strip() for c in _QUERY_COLS.split(",")]


def get_turbo_results(
    limit: int = 10000,
    min_score: float = 0,
    direction: str = "bull",
    tf: str = "1d",
    universe: str = "sp500",
    price_min: float = 0,
    price_max: float = 1e9,
    rsi_min: float = 0,
    rsi_max: float = 100,
    cci_min: float = -9999,
    cci_max: float = 9999,
    vol_min: float = 0,
    vol_max: float = 0,
) -> list[dict]:
    _init_db()
    con = _db()
    try:
        row = con.execute(
            "SELECT id FROM turbo_scan_runs WHERE tf=? AND universe=? ORDER BY id DESC LIMIT 1",
            (tf, universe),
        ).fetchone()
        if not row:
            return []
        scan_id = row["id"]

        where  = "scan_id = ? AND turbo_score >= ?"
        params: list = [scan_id, min_score]

        if direction == "bull":
            where += " AND tz_bull = 1"
        elif direction == "bear":
            where += " AND tz_bull = 0"

        if price_min > 0:
            where += " AND last_price >= ?"; params.append(price_min)
        if price_max < 1e8:
            where += " AND last_price <= ?"; params.append(price_max)
        if rsi_min > 0:
            where += " AND rsi >= ?"; params.append(rsi_min)
        if rsi_max < 100:
            where += " AND rsi <= ?"; params.append(rsi_max)
        if cci_min > -9999:
            where += " AND cci >= ?"; params.append(cci_min)
        if cci_max < 9999:
            where += " AND cci <= ?"; params.append(cci_max)
        if vol_min > 0:
            where += " AND avg_vol >= ?"; params.append(vol_min)
        if vol_max > 0:
            where += " AND avg_vol < ?";  params.append(vol_max)

        rows = con.execute(
            f"SELECT * FROM turbo_scan_results WHERE {where} "
            f"ORDER BY turbo_score DESC LIMIT ?",
            params + [limit],
        ).fetchall()
        # Sanitize NaN/Inf floats so JSON serialization never fails
        clean = []
        for row in rows:
            clean.append({
                k: (0.0 if isinstance(v, float) and (v != v or v == float('inf') or v == float('-inf')) else v)
                for k, v in row.items()
            })
        return clean
    finally:
        con.close()


def get_last_turbo_scan_time(tf: str = "1d", universe: str = "sp500") -> str | None:
    _init_db()
    con = _db()
    try:
        row = con.execute(
            "SELECT completed_at FROM turbo_scan_runs WHERE tf=? AND universe=? ORDER BY id DESC LIMIT 1",
            (tf, universe),
        ).fetchone()
        return row["completed_at"] if row else None
    finally:
        con.close()
