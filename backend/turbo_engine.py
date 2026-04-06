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

from signal_engine import compute_signals
from wlnbb_engine  import compute_wlnbb
from combo_engine  import compute_combo
from wick_engine   import compute_wick
from cisd_engine   import compute_cisd
from vabs_engine   import compute_vabs
from br_engine     import compute_br
from ultra_engine  import compute_260308_l88, compute_ultra_v2
from delta_engine  import compute_delta

log = logging.getLogger(__name__)
DB_PATH = os.environ.get("DB_PATH", "/tmp/scanner.db")

# ── Progress ──────────────────────────────────────────────────────────────────
_turbo_state: dict = {"running": False, "done": 0, "total": 0, "found": 0, "started_at": 0}

_SCAN_TIMEOUT = 30 * 60  # 30 minutes max before auto-reset


def get_turbo_progress() -> dict:
    state = dict(_turbo_state)
    # auto-reset if stuck longer than timeout
    if state["running"] and time.time() - state.get("started_at", 0) > _SCAN_TIMEOUT:
        _turbo_state["running"] = False
        state["running"] = False
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
    # Wyckoff
    "ns", "nd", "sc", "bc", "sq",
    # Combo
    "buy_2809", "rocket", "sig3g", "rtv",
    "hilo_buy", "hilo_sell", "atr_brk", "bb_brk",
    "bias_up", "bias_down", "cons_atr",
    # T/Z
    "tz_sig", "tz_bull",
    # WLNBB
    "fri34", "fri43", "l34", "l43", "l64", "l22",
    "blue", "cci_ready", "bo_up", "bx_up", "fuchsia_rl",
    # Wick
    "wick_bull", "wick_bear",
    # CISD
    "cisd_ppm", "cisd_seq",
    # BR
    "br_score",
    # meta
    "vol_bucket",
    # Delta / order-flow (260403)
    "d_strong_bull", "d_strong_bear",
    "d_absorb_bull", "d_absorb_bear",
    "d_div_bull",    "d_div_bear",
    "d_cd_bull",     "d_cd_bear",
    "d_surge_bull",  "d_surge_bear",
    "d_blast_bull",  "d_blast_bear",
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
    Score grouped into 4 capped families to prevent double-counting:
      Volume/accum  cap 22  — VABS, Wyckoff, 260308/L88
      Breakout      cap 15  — ULTRA v2, BO/BX
      Combo/trend   cap 14  — Combo signals
      L-structure   cap 13  — T/Z, WLNBB
    Context (Wick, CISD, BR%) uncapped (max ~14).
    Grand max ~78; Fire tier at ≥65 requires alignment across families.
    """
    # ── Volume / accumulation family (cap 22) ─────────────────────────────
    vol = 0.0
    if r.get("best_sig"):
        vol += 15
    elif r.get("strong_sig"):
        vol += 9
    if r.get("vbo_up"):
        vol += 6
    if not r.get("best_sig") and not r.get("strong_sig"):
        vol_cnt = int(r.get("abs_sig", 0)) + int(r.get("climb_sig", 0)) + int(r.get("load_sig", 0))
        vol += min(vol_cnt * 3, 7)
    if r.get("ns"):   vol += 4
    if r.get("sq"):   vol += 4
    if r.get("sc"):   vol += 2
    if r.get("sig_l88"):       vol += 5
    elif r.get("sig_260308"):  vol += 3
    s = min(vol, 22)

    # ── Breakout / expansion family (cap 15) ──────────────────────────────
    brk = 0.0
    if r.get("best_long"):
        brk += 8
    else:
        if r.get("fbo_bull"): brk += 4
        if r.get("eb_bull"):  brk += 4
        if r.get("bf_buy"):   brk += 3
    if r.get("ultra_3up"):             brk += 4
    if r.get("bo_up") or r.get("bx_up"): brk += 3
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

    # ── Delta / order-flow family (cap 10) ───────────────────────────────
    dlt = 0.0
    if r.get("d_blast_bull"):        dlt += 6
    elif r.get("d_surge_bull"):      dlt += 4
    if r.get("d_strong_bull"):       dlt += 5
    if r.get("d_absorb_bull"):       dlt += 4
    if r.get("d_div_bull"):          dlt += 3
    elif r.get("d_cd_bull"):         dlt += 2
    s += min(dlt, 10)

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
) -> dict | None:
    try:
        from data_polygon import fetch_bars, polygon_available

        days = 180 if interval in ("1d", "1wk") else 60

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

        # Drop today's incomplete bar for daily/weekly
        if interval in ("1d", "1wk"):
            last_dt = df.index[-1]
            if hasattr(last_dt, "date"):
                last_dt = last_dt.date()
            if last_dt == datetime.now(timezone.utc).date():
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

        # ── T/Z signals ────────────────────────────────────────────────────
        sig_df  = compute_signals(df)
        last_s  = sig_df.iloc[-1]
        tz_bull = bool(last_s.get("is_bull", False))
        tz_name = str(last_s.get("sig_name", "")) if tz_bull else ""
        row["tz_bull"] = int(tz_bull)
        row["tz_sig"]  = tz_name

        # ── WLNBB (L signals, FRI, BLUE, BO, CCI) ─────────────────────────
        wlnbb    = compute_wlnbb(df)
        last_w   = wlnbb.iloc[-1]
        row["fri34"]     = int(bool(last_w.get("FRI34",      False)))
        row["fri43"]     = int(bool(last_w.get("FRI43",      False) if "FRI43" in last_w.index else False))
        row["l34"]       = int(bool(last_w.get("L34",        False)))
        row["l43"]       = int(bool(last_w.get("L43",        False)))
        row["l64"]       = int(bool(last_w.get("L64",        False)))
        row["l22"]       = int(bool(last_w.get("L22",        False)))
        row["blue"]      = int(bool(last_w.get("BLUE",       False)))
        row["cci_ready"] = int(bool(last_w.get("CCI_READY",  False)))
        row["bo_up"]     = int(bool(last_w.get("BO_UP",      False)))
        row["bx_up"]     = int(bool(last_w.get("BX_UP",      False)))
        row["fuchsia_rl"]= int(bool(last_w.get("FUCHSIA_RL", False)))
        bkt = str(last_w.get("vol_bucket", ""))
        row["vol_bucket"] = bkt

        # ── Combo (2809, CONS, Bias, HILO, RTV, 3G, ROCKET, BRK) ─────────
        combo   = compute_combo(df)
        last_c  = combo.iloc[-1]
        row["buy_2809"]  = int(bool(last_c.get("buy_2809",  False)))
        row["rocket"]    = int(bool(last_c.get("rocket",    False)))
        row["sig3g"]     = int(bool(last_c.get("sig3g",     False)))
        row["rtv"]       = int(bool(last_c.get("rtv",       False)))
        row["hilo_buy"]  = int(bool(last_c.get("hilo_buy",  False)))
        row["hilo_sell"] = int(bool(last_c.get("hilo_sell", False)))
        row["atr_brk"]   = int(bool(last_c.get("atr_brk",  False)))
        row["bb_brk"]    = int(bool(last_c.get("bb_brk",   False)))
        row["bias_up"]   = int(bool(last_c.get("bias_up",  False)))
        row["bias_down"] = int(bool(last_c.get("bias_down",False)))
        row["cons_atr"]  = int(bool(last_c.get("cons_atr", False)))

        # ── VABS (ABS, CLIMB, LOAD, Wyckoff, BEST, STRONG, VBO) ───────────
        vabs    = compute_vabs(df)
        last_v  = vabs.iloc[-1]
        row["abs_sig"]   = int(bool(last_v.get("abs_sig",   False)))
        row["climb_sig"] = int(bool(last_v.get("climb_sig", False)))
        row["load_sig"]  = int(bool(last_v.get("load_sig",  False)))
        row["ns"]        = int(bool(last_v.get("ns",        False)))
        row["nd"]        = int(bool(last_v.get("nd",        False)))
        row["sc"]        = int(bool(last_v.get("sc",        False)))
        row["bc"]        = int(bool(last_v.get("bc",        False)))
        row["sq"]        = int(bool(last_v.get("sq",        False)))
        row["best_sig"]  = int(bool(last_v.get("best_sig",  False)))
        row["strong_sig"]= int(bool(last_v.get("strong_sig",False)))
        row["vbo_up"]    = int(bool(last_v.get("vbo_up",    False)))
        row["vbo_dn"]    = int(bool(last_v.get("vbo_dn",    False)))

        # ── Wick ───────────────────────────────────────────────────────────
        wick   = compute_wick(df)
        last_wk= wick.iloc[-1]
        row["wick_bull"] = int(bool(last_wk.get("WICK_BULL_CONFIRM", False)))
        row["wick_bear"] = int(bool(last_wk.get("WICK_BEAR_CONFIRM", False)))

        # ── CISD ───────────────────────────────────────────────────────────
        cisd   = compute_cisd(df)
        last_ci= cisd.iloc[-1]
        row["cisd_ppm"]  = int(bool(last_ci.get("CISD_PPM", False)))
        row["cisd_seq"]  = int(bool(last_ci.get("CISD_SEQ", False)))

        # ── BR% readiness score ────────────────────────────────────────────
        try:
            br_df = compute_br(df)
            row["br_score"] = round(float(br_df["br_score"].iloc[-1]), 1)
        except Exception:
            row["br_score"] = 0.0

        # ── Delta / order-flow (260403) ────────────────────────────────────
        try:
            ddf    = compute_delta(df)
            last_d = ddf.iloc[-1]
            for col in ("strong_bull","strong_bear","absorb_bull","absorb_bear",
                        "div_bull","div_bear","cd_bull","cd_bear",
                        "surge_bull","surge_bear","blast_bull","blast_bear"):
                row[f"d_{col}"] = int(bool(last_d.get(col, False)))
        except Exception:
            for col in ("strong_bull","strong_bear","absorb_bull","absorb_bear",
                        "div_bull","div_bear","cd_bull","cd_bear",
                        "surge_bull","surge_bear","blast_bull","blast_bear"):
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
            uv2    = compute_ultra_v2(df)
            last_u = uv2.iloc[-1]
            row["eb_bull"]    = int(bool(last_u.get("eb_bull",    False)))
            row["eb_bear"]    = int(bool(last_u.get("eb_bear",    False)))
            row["fbo_bull"]   = int(bool(last_u.get("fbo_bull",   False)))
            row["fbo_bear"]   = int(bool(last_u.get("fbo_bear",   False)))
            row["bf_buy"]     = int(bool(last_u.get("bf_buy",     False)))
            row["bf_sell"]    = int(bool(last_u.get("bf_sell",    False)))
            row["ultra_3up"]  = int(bool(last_u.get("ultra_3up",  False)))
            row["ultra_3dn"]  = int(bool(last_u.get("ultra_3dn",  False)))
            row["best_long"]  = int(bool(last_u.get("best_long",  False)))
            row["best_short"] = int(bool(last_u.get("best_short", False)))
        except Exception:
            for k in ("eb_bull","eb_bear","fbo_bull","fbo_bear","bf_buy","bf_sell",
                      "ultra_3up","ultra_3dn","best_long","best_short"):
                row[k] = 0

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
) -> int:
    from scanner import get_universe_tickers, UNIVERSE_CONFIGS
    global _turbo_state

    _init_db()
    cfg = UNIVERSE_CONFIGS.get(universe, UNIVERSE_CONFIGS["sp500"])
    min_price = float(cfg["min_price"])
    max_price = float(cfg["max_price"])

    tickers = get_universe_tickers(universe)
    _turbo_state.update({"running": True, "done": 0, "total": len(tickers), "found": 0,
                         "universe": universe, "started_at": time.time()})
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
                pool.submit(_scan_turbo_ticker, t, interval, min_price, max_price): t
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

    log.info("Turbo scan done: %d/%d tickers, tf=%s universe=%s", len(results), len(tickers), interval, universe)
    return len(results)


# ── Query ─────────────────────────────────────────────────────────────────────
_QUERY_COLS = (
    "ticker, turbo_score, br_score, vol_bucket, tz_sig, tz_bull, "
    "best_sig, strong_sig, vbo_up, vbo_dn, abs_sig, climb_sig, load_sig, "
    "ns, nd, sc, bc, sq, "
    "buy_2809, rocket, sig3g, rtv, hilo_buy, hilo_sell, atr_brk, bb_brk, "
    "bias_up, bias_down, cons_atr, "
    "fri34, fri43, l34, l43, l64, l22, blue, cci_ready, bo_up, bx_up, fuchsia_rl, "
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
    limit: int = 500,
    min_score: float = 0,
    direction: str = "bull",  # bull | bear | all
    tf: str = "1d",
    universe: str = "sp500",
) -> list[dict]:
    _init_db()
    con = _db()
    try:
        row = con.execute(
            "SELECT id FROM turbo_scan_runs WHERE tf=? AND universe=? ORDER BY id DESC LIMIT 1",
            (tf, universe),
        ).fetchone()
        if not row:
            return []   # no scan yet for this tf+universe — don't leak another universe's data
        scan_id = row[0]

        where  = "scan_id = ? AND turbo_score >= ?"
        params: list = [scan_id, min_score]

        if direction == "bull":
            where += " AND tz_bull = 1"
        elif direction == "bear":
            where += " AND tz_bull = 0"

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
