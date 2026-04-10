"""
turbo_engine.py — TURBO combined scan engine.

Runs ALL signal engines per ticker and produces a single turbo_score (0-100)
covering: VABS, Wyckoff, Combo (2809), T/Z patterns, WLNBB (L/FRI/BLUE),
Wick, CISD, and BR% readiness.

turbo_score tiers:
  55+  Fire   — multiple strong signals aligning
  40+  Strong — solid multi-engine confirmation
  25+  Bull   — base bullish setup
  <25  Weak   — sparse signals
"""
from __future__ import annotations

import os
import sqlite3
import logging
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

from signal_engine import compute_signals, compute_b_signals
from wlnbb_engine  import compute_wlnbb
from combo_engine  import compute_combo, compute_tz_state
from wick_engine   import compute_wick
from cisd_engine   import compute_cisd
from vabs_engine   import compute_vabs
from br_engine     import compute_br
from ultra_engine  import compute_260308_l88, compute_ultra_v2
from delta_engine   import compute_delta
from wyckoff_engine import compute_wyckoff_accum, compute_wyckoff_dist

log = logging.getLogger(__name__)
DB_PATH = os.environ.get("DB_PATH", "/tmp/scanner.db")

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
}

_SCAN_TIMEOUT = 30 * 60  # 30 minutes max before auto-reset


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
    # Wyckoff Accumulation (260225)
    "wyk_sc", "wyk_ar", "wyk_st", "wyk_spring", "wyk_sos", "wyk_lps",
    "wyk_accum", "wyk_markup",
    # Wyckoff Distribution (260225)
    "wyk_bc", "wyk_ard", "wyk_std", "wyk_utad", "wyk_sow", "wyk_lpsy",
    "wyk_dist", "wyk_markdown",
    # Combo / 2809
    "buy_2809", "rocket", "sig3g", "rtv",
    "hilo_buy", "hilo_sell", "atr_brk", "bb_brk",
    "bias_up", "bias_down", "cons_atr",
    "um_2809", "svs_2809", "conso_2809",
    # B signals (260410/260321)
    "b1", "b2", "b3", "b4", "b5",
    "b6", "b7", "b8", "b9", "b10", "b11",
    # TZ state + confluences
    "tz_state", "ca", "cd", "cw",
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
    # Wick
    "wick_bull", "wick_bear",
    # CISD
    "cisd_ppm", "cisd_seq",
    # BR
    "br_score",
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
    "preup66", "preup55", "preup89",
    # PREDN (EMA drop ↓)
    "predn66", "predn55", "predn89", "predn3", "predn2", "predn50",
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def _db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def _col_def(c: str) -> str:
    _TEXT = {"tz_sig", "vol_bucket"}
    _REAL = {"turbo_score", "br_score", "rsi", "cci"}
    typ     = "TEXT"    if c in _TEXT else "REAL" if c in _REAL else "INTEGER"
    default = "''"      if c in _TEXT else "0"
    return f"    {c}  {typ}  DEFAULT {default},"


def _init_db() -> None:
    con = _db()
    cols_sql = "\n".join(_col_def(c) for c in _TURBO_COLS)
    con.executescript(f"""
        CREATE TABLE IF NOT EXISTS turbo_scan_runs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            tf           TEXT    DEFAULT '1d',
            universe     TEXT    DEFAULT 'sp500',
            started_at   TEXT,
            completed_at TEXT,
            result_count INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS turbo_scan_results (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id    INTEGER,
            ticker     TEXT NOT NULL,
{cols_sql}
            last_price REAL,
            change_pct REAL,
            scanned_at TEXT
        );
    """)
    # migration: add any missing columns
    existing = {r[1] for r in con.execute("PRAGMA table_info(turbo_scan_results)").fetchall()}
    for col in _TURBO_COLS:
        if col not in existing:
            typ = "TEXT" if col in ("tz_sig", "vol_bucket") else "REAL" if col in ("turbo_score", "br_score", "rsi", "cci") else "INTEGER"
            default = "''" if col in ("tz_sig", "vol_bucket") else "0"
            con.execute(f"ALTER TABLE turbo_scan_results ADD COLUMN {col} {typ} DEFAULT {default}")
    run_cols = {r[1] for r in con.execute("PRAGMA table_info(turbo_scan_runs)").fetchall()}
    if "tf" not in run_cols:
        con.execute("ALTER TABLE turbo_scan_runs ADD COLUMN tf TEXT DEFAULT '1d'")
    if "universe" not in run_cols:
        con.execute("ALTER TABLE turbo_scan_runs ADD COLUMN universe TEXT DEFAULT 'sp500'")
    con.commit()
    con.close()


# ── Score calculator ──────────────────────────────────────────────────────────
def _calc_turbo_score(r: dict) -> float:
    """
    Score grouped into 4 capped families — RAW signals only, no composites.
      Volume/accum  cap 22  — VABS atomic (abs/climb/load), Wyckoff, 260308/L88
      Breakout      cap 15  — ULTRA v2 atomic (fbo/eb/bf), BO/BX, RS
      Combo/trend   cap 14  — Combo signals
      L-structure   cap 13  — T/Z, WLNBB
    Context (Wick, CISD, BR%) uncapped (max ~14).
    """
    # ── Volume / accumulation family (cap 22) ─────────────────────────────
    # Only atomic VABS signals — no best_sig / strong_sig composites
    vol = 0.0
    if r.get("abs_sig"):   vol += 5
    if r.get("climb_sig"): vol += 4
    if r.get("load_sig"):  vol += 4
    if r.get("vbo_up"):    vol += 6
    if r.get("ns"):        vol += 5
    if r.get("sq"):        vol += 4
    if r.get("sc"):        vol += 2
    if r.get("sig_l88"):        vol += 5
    elif r.get("sig_260308"):   vol += 3
    # Wyckoff Accumulation (260225) — additive, higher priority phase = more points
    if r.get("wyk_spring"):     vol += 7   # optimal entry — bear trap + absorption
    elif r.get("wyk_lps"):      vol += 5   # buy-the-dip after SOS breakout
    elif r.get("wyk_sos"):      vol += 5   # SOS/JAC breakout confirmation
    elif r.get("wyk_markup"):   vol += 3   # in markup/breakout phase (context)
    elif r.get("wyk_accum"):    vol += 2   # accumulation in progress (context)
    s = min(vol, 22)

    # ── Breakout / expansion family (cap 15) ──────────────────────────────
    # Only atomic ULTRA signals — no best_long composite
    brk = 0.0
    if r.get("fbo_bull"):  brk += 5
    if r.get("eb_bull"):   brk += 4
    if r.get("bf_buy"):    brk += 4
    if r.get("ultra_3up"): brk += 4
    if r.get("bo_up") or r.get("bx_up"): brk += 3
    if r.get("rs_strong"): brk += 5
    elif r.get("rs"):      brk += 3
    # Wyckoff Distribution is a bearish context — subtract from bull score
    if r.get("wyk_sow"):      brk -= 4   # SOW: confirmed breakdown
    elif r.get("wyk_markdown"):brk -= 3  # in markdown phase
    elif r.get("wyk_dist"):    brk -= 1  # distribution in progress
    s += min(brk, 15)

    # ── Combo / momentum family (cap 14) ──────────────────────────────────
    combo = 0.0
    if r.get("rocket"):
        combo += 12
    elif r.get("buy_2809"):
        combo += 8
    if r.get("sig3g"):    combo += 4
    if r.get("rtv"):      combo += 3
    if r.get("hilo_buy"): combo += 2
    if r.get("atr_brk") or r.get("bb_brk"): combo += 2
    # B-signal confluences (CA=Bull Attempt+B, CD=Bull Dom+B, CW=Bear Weak+B)
    if r.get("cd"):   combo += 5
    elif r.get("ca"): combo += 3
    elif r.get("cw"): combo += 2
    s += min(combo, 14)

    # ── L-structure / trend family (cap 13) ───────────────────────────────
    trend = 0.0
    trend += _TZ_W.get(r.get("tz_sig", ""), 0)
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
    if r.get("d_spring"):            dlt += 6   # Wyckoff Spring (bear trap + absorption)
    elif r.get("d_div_bull"):        dlt += 3
    if r.get("d_vd_div_bull"):       dlt += 3   # vol↓ delta↑ (no supply)
    elif r.get("d_cd_bull"):         dlt += 2
    s += min(dlt, 12)

    # ── EMA cross family (cap 8) ──────────────────────────────────────────
    ema_x = 0.0
    if r.get("preup66"):   ema_x += 8   # crossed EMA200 + another  (very strong)
    elif r.get("preup55"): ema_x += 6   # crossed EMA89 + another
    elif r.get("preup89"): ema_x += 4   # crossed EMA89 alone
    s += min(ema_x, 8)

    # ── Context / confirmation (uncapped, max ~14) ────────────────────────
    if r.get("wick_bull"): s += 3
    if r.get("cisd_ppm"):   s += 2
    elif r.get("cisd_seq"): s += 1
    s += min(float(r.get("br_score") or 0) * 0.1, 8)

    return round(min(100.0, s), 1)


# ── Per-ticker worker ─────────────────────────────────────────────────────────
def _scan_turbo_ticker(
    ticker: str,
    interval: str,
    min_price: float = 0.0,
    max_price: float = 1e9,
    lookback_n: int = 1,
    spy_chg: float | None = None,
    iwm_chg: float | None = None,
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
        if polygon_available():
            try:
                df = fetch_bars(ticker, interval=interval, days=days)
            except Exception as exc:
                log.debug("Polygon skip %s: %s — falling back to yfinance", ticker, exc)

        if df is None or df.empty:
            import yfinance as yf
            period = "180d" if interval in ("1d", "1wk") else "60d"
            raw = yf.Ticker(ticker).history(period=period, interval=interval, auto_adjust=True)
            if raw is None or raw.empty:
                return None
            raw.columns = [str(c).lower() for c in raw.columns]
            df = raw[["open", "high", "low", "close", "volume"]].dropna()

        if len(df) < 50:
            return None

        # Drop today's bar ONLY while US market is still open.
        # Use ET timezone so DST is handled automatically:
        # EDT (Mar-Nov): market closes 16:00 ET = 20:00 UTC
        # EST (Nov-Mar): market closes 16:00 ET = 21:00 UTC
        # Buffer: keep bar after 16:15 ET to allow API finalization.
        if interval in ("1d", "1wk", "1w"):
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

        row: dict = {"ticker": ticker}

        # ── Price / change ─────────────────────────────────────────────────
        price  = float(df["close"].iloc[-1])
        prev_p = float(df["close"].iloc[-2]) if len(df) >= 2 else price
        row["last_price"] = round(price, 2)
        row["change_pct"] = round((price - prev_p) / prev_p * 100, 2) if prev_p else 0.0

        # ── Price range filter ─────────────────────────────────────────────
        if price < min_price or price > max_price:
            return None

        # Helper: extract bool signal from last N bars
        def _sig(frame, col, n=lookback_n):
            if col not in frame.columns:
                return 0
            if n <= 1:
                return int(bool(frame.iloc[-1].get(col, False)))
            return int(bool(frame.tail(n)[col].any()))

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
        # preup3/2/50 already in existing _TURBO_COLS as part of combo
        # PREDN — EMA drop ↓ (strongest = D66)
        row["predn66"]   = _sig(combo, "predn66")
        row["predn55"]   = _sig(combo, "predn55")
        row["predn89"]   = _sig(combo, "predn89")
        row["predn3"]    = _sig(combo, "predn3")
        row["predn2"]    = _sig(combo, "predn2")
        row["predn50"]   = _sig(combo, "predn50")

        # ── B signals (260410/260321) ──────────────────────────────────────
        b_sigs = compute_b_signals(df)
        for _b in range(1, 12):
            row[f"b{_b}"] = _sig(b_sigs, f"b{_b}")
        # ── TZ state machine + CA/CD/CW ────────────────────────────────────
        tz_st = compute_tz_state(df)
        row["tz_state"] = int(tz_st.iloc[-1]) if len(tz_st) else 0
        _any_b = any(row.get(f"b{_b}", 0) for _b in range(1, 12))
        _last_st = row["tz_state"]
        row["ca"] = int(_any_b and _last_st == 2)  # Bull Attempt + B
        row["cd"] = int(_any_b and _last_st == 3)  # Bull Dom + B
        row["cw"] = int(_any_b and _last_st == 1)  # Bear Weakening + B

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

        # ── Wyckoff Accumulation (260225) ─────────────────────────────────
        try:
            wya = compute_wyckoff_accum(df)
            for _c in ("wyk_sc","wyk_ar","wyk_st","wyk_spring","wyk_sos","wyk_lps",
                       "wyk_accum","wyk_markup"):
                row[_c] = _sig(wya, _c)
        except Exception:
            for _c in ("wyk_sc","wyk_ar","wyk_st","wyk_spring","wyk_sos","wyk_lps",
                       "wyk_accum","wyk_markup"):
                row[_c] = 0

        # ── Wyckoff Distribution (260225) ──────────────────────────────────
        try:
            wyd = compute_wyckoff_dist(df)
            for _c in ("wyk_bc","wyk_ard","wyk_std","wyk_utad","wyk_sow","wyk_lpsy",
                       "wyk_dist","wyk_markdown"):
                row[_c] = _sig(wyd, _c)
        except Exception:
            for _c in ("wyk_bc","wyk_ard","wyk_std","wyk_utad","wyk_sow","wyk_lpsy",
                       "wyk_dist","wyk_markdown"):
                row[_c] = 0

        # ── Wick ───────────────────────────────────────────────────────────
        wick   = compute_wick(df)
        row["wick_bull"] = _sig(wick, "WICK_BULL_CONFIRM")
        row["wick_bear"] = _sig(wick, "WICK_BEAR_CONFIRM")

        # ── CISD ───────────────────────────────────────────────────────────
        cisd   = compute_cisd(df)
        row["cisd_ppm"]  = _sig(cisd, "CISD_PPM")
        row["cisd_seq"]  = _sig(cisd, "CISD_SEQ")

        # ── BR% readiness score ────────────────────────────────────────────
        try:
            br_df = compute_br(df)
            row["br_score"] = round(float(br_df["br_score"].iloc[-1]), 1)
        except Exception:
            row["br_score"] = 0.0

        # ── Delta / order-flow (260403 V2) ────────────────────────────────
        try:
            ddf = compute_delta(df)
            for col in ("strong_bull","strong_bear","absorb_bull","absorb_bear",
                        "div_bull","div_bear","cd_bull","cd_bear",
                        "surge_bull","surge_bear","blast_bull","blast_bear",
                        "vd_div_bull","vd_div_bear","spring","upthrust"):
                row[f"d_{col}"] = _sig(ddf, col)
        except Exception:
            for col in ("strong_bull","strong_bear","absorb_bull","absorb_bear",
                        "div_bull","div_bear","cd_bull","cd_bear",
                        "surge_bull","surge_bear","blast_bull","blast_bear",
                        "vd_div_bull","vd_div_bear","spring","upthrust"):
                row[f"d_{col}"] = 0

        # ── RSI(14) ────────────────────────────────────────────────────────
        try:
            delta = df["close"].diff()
            gain  = delta.where(delta > 0, 0.0).ewm(alpha=1 / 14, adjust=False).mean()
            loss  = (-delta.where(delta < 0, 0.0)).ewm(alpha=1 / 14, adjust=False).mean()
            rs    = gain / loss.replace(0, np.nan)
            rsi_s = 100 - (100 / (1 + rs))
            row["rsi"] = round(float(rsi_s.iloc[-1]), 1)
        except Exception:
            row["rsi"] = 0.0

        # ── CCI(20) ────────────────────────────────────────────────────────
        try:
            tp     = (df["high"] + df["low"] + df["close"]) / 3
            tp_ma  = tp.rolling(20).mean()
            mad    = tp.rolling(20).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
            cci_s  = (tp - tp_ma) / (0.015 * mad.replace(0, np.nan))
            row["cci"] = round(float(cci_s.iloc[-1]), 1)
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

        return row

    except Exception as exc:
        log.debug("Turbo skip %s: %s", ticker, exc)
        return None


# ── Scan runner ───────────────────────────────────────────────────────────────
def run_turbo_scan(
    interval: str = "1d",
    universe: str = "sp500",
    workers: int = 8,
    lookback_n: int = 5,
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
        "started_at": time.time(), "completed_at": None, "error": None,
    })
    try:
        tickers = get_universe_tickers(universe)
    except Exception as exc:
        _turbo_state.update({"running": False, "error": str(exc)})
        log.error("Failed to fetch tickers for universe=%s: %s", universe, exc)
        return 0

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
    scan_id = con.execute(
        "INSERT INTO turbo_scan_runs (tf, universe, started_at) VALUES (?, ?, ?)",
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
                    lookback_n, spy_chg, iwm_chg
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
        _turbo_state["running"] = False
        _turbo_state["completed_at"] = time.time()

    log.info("Turbo scan done: %d/%d tickers, tf=%s universe=%s", found, len(tickers), interval, universe)
    return found


# ── Query ─────────────────────────────────────────────────────────────────────
_QUERY_COLS = (
    "ticker, turbo_score, br_score, vol_bucket, tz_sig, tz_bull, "
    "best_sig, strong_sig, vbo_up, vbo_dn, abs_sig, climb_sig, load_sig, "
    "ns, nd, sc, bc, sq, "
    "buy_2809, rocket, sig3g, rtv, hilo_buy, hilo_sell, atr_brk, bb_brk, "
    "bias_up, bias_down, cons_atr, "
    "fri34, fri43, fri64, l34, l43, l64, l22, l555, only_l2l4, "
    "blue, cci_ready, cci_0_retest, cci_blue_turn, "
    "bo_up, bo_dn, bx_up, bx_dn, be_up, be_dn, "
    "fuchsia_rh, fuchsia_rl, pre_pump, "
    "wick_bull, wick_bear, cisd_ppm, cisd_seq, "
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
        scan_id = row[0]

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

        rows = con.execute(
            f"SELECT {_QUERY_COLS} FROM turbo_scan_results WHERE {where} "
            f"ORDER BY turbo_score DESC LIMIT ?",
            params + [limit],
        ).fetchall()
        return [dict(zip(_QUERY_KEYS, r)) for r in rows]
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
        return row[0] if row else None
    finally:
        con.close()
