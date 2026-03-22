"""
scanner.py — scan ticker universe for T/Z + WLNBB signals with combined scoring.
Saves results to SQLite. Scheduled at 09:30, 12:00, 15:30 EST.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

from signal_engine import compute_signals, SIG_NAMES
from wlnbb_engine import compute_wlnbb, score_last_bar, l_signal_label

log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "/tmp/scanner.db")

# ── Scan progress state (shared across threads) ───────────────────────────────
_scan_state: dict = {
    "running": False,
    "done": 0,
    "total": 0,
    "found": 0,
    "interval": "",
}

# ── Ticker universe ───────────────────────────────────────────────────────────

_FALLBACK = [
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B","JPM","JNJ",
    "V","PG","UNH","HD","MA","ABBV","MRK","LLY","CVX","PEP","KO","AVGO",
    "BAC","COST","MCD","WMT","TMO","CSCO","ACN","CRM","ABT","LIN","DHR",
    "NEE","TXN","VZ","ADBE","PM","WFC","BMY","CMCSA","RTX","NFLX","INTC",
    "AMGN","HON","QCOM","AMD","UPS","CAT","GS","LOW","SBUX","ELV","DE",
    "SPGI","BLK","AXP","LMT","SYK","GILD","MS","CVS","MDLZ","PLD","ISRG",
    "ZTS","ADI","BKNG","TJX","C","REGN","MO","SO","DUK","USB","PNC",
    "VRTX","CL","ITW","CI","EOG","SLB","EMR","AON","APD","ICE","MCO",
    "FIS","NSC","TGT","FISV","EW","GD","DXCM","FDX","HUM","WM","FCX",
    "OXY","MPC","PSA","MRNA","KLAC","LRCX","SNPS","CDNS","MCHP","AMAT",
    "MU","PANW","CRWD","SNOW","PLTR","SQ","SHOP","UBER","LYFT","ABNB",
    "COIN","RBLX","HOOD","SOFI","AFRM","UPST","RIVN","LCID","NIO",
    "BABA","JD","PDD","BIDU","DDOG","NET","ZS","OKTA","TWLO","MDB",
    "ESTC","HUBS","GTLB","U","DOCN","CFLT","IOT","TOST","ASAN","BILL",
    "DUOL","APPN","AI","PATH","BRZE","SEMR","SPT","MNDY","WIX",
    "F","GM","TM","HMC","RACE","XPEV","LI",
    "DAL","UAL","AAL","LUV","ALK","JBLU",
    "CCL","RCL","NCLH","MGM","WYNN","LVS","CZR","PENN","DKNG",
    "DIS","PARA","WBD","ROKU","SPOT","TTWO","EA",
    "XOM","COP","HAL","BKR","MRO","DVN","FANG","PXD",
    "GLD","SLV","NEM","AEM","WPM","FNV","RGLD",
    "SPY","QQQ","IWM","DIA","EEM","XLF","XLE","XLK","XLV",
]


MAX_TICKERS = 200  # cap to keep scans fast


def get_tickers() -> list[str]:
    """Try Wikipedia S&P 500; fall back to hardcoded list. Capped at MAX_TICKERS."""
    try:
        tables = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            attrs={"id": "constituents"},
        )
        sp500 = tables[0]["Symbol"].tolist()
        tickers = [t.replace(".", "-") for t in sp500]
    except Exception:
        tickers = list(dict.fromkeys(_FALLBACK))  # deduplicated
    return tickers[:MAX_TICKERS]


# ── DB schema ─────────────────────────────────────────────────────────────────

def _init_db() -> None:
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS scan_results (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id      INTEGER DEFAULT 0,
            ticker       TEXT NOT NULL,
            sig_id       INTEGER,
            sig_name     TEXT,
            pattern_3bar TEXT,
            l_signal     TEXT DEFAULT '',
            bull_score   INTEGER DEFAULT 0,
            bear_score   INTEGER DEFAULT 0,
            last_price   REAL DEFAULT 0,
            volume       INTEGER DEFAULT 0,
            change_pct   REAL DEFAULT 0,
            interval     TEXT,
            scanned_at   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scan ON scan_results(interval, scanned_at DESC);

        CREATE TABLE IF NOT EXISTS scan_runs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            interval     TEXT,
            started_at   TEXT,
            completed_at TEXT,
            result_count INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS watchlist (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker    TEXT UNIQUE NOT NULL,
            added_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS pump_combos (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            combo         TEXT NOT NULL,
            count         INTEGER,
            avg_gain_pct  REAL,
            max_gain_pct  REAL,
            win_rate      REAL,
            threshold     REAL DEFAULT 2.0,
            window        INTEGER DEFAULT 20,
            combo_len     INTEGER DEFAULT 3,
            created_at    TEXT
        );
    """)
    # Migrate: add columns if they don't exist (for older DBs)
    existing = {
        row[1]
        for row in con.execute("PRAGMA table_info(scan_results)").fetchall()
    }
    for col, defn in [
        ("scan_id",     "INTEGER DEFAULT 0"),
        ("l_signal",    "TEXT DEFAULT ''"),
        ("bull_score",  "INTEGER DEFAULT 0"),
        ("bear_score",  "INTEGER DEFAULT 0"),
        ("last_price",  "REAL DEFAULT 0"),
        ("volume",      "INTEGER DEFAULT 0"),
        ("change_pct",  "REAL DEFAULT 0"),
    ]:
        if col not in existing:
            con.execute(f"ALTER TABLE scan_results ADD COLUMN {col} {defn}")
    con.commit()
    con.close()


# ── Per-ticker processing ─────────────────────────────────────────────────────

def _scan_ticker(ticker: str, interval: str) -> dict | None:
    try:
        raw = yf.Ticker(ticker).history(
            period="90d", interval=interval, auto_adjust=True
        )
        if raw is None or raw.empty or len(raw) < 5:
            return None

        raw.columns = [str(c).lower() for c in raw.columns]
        needed = ["open", "high", "low", "close"]
        df = raw[needed + (["volume"] if "volume" in raw.columns else [])].dropna()
        if len(df) < 5:
            return None

        sigs = compute_signals(df)
        last_sig = int(sigs["sig_id"].iloc[-1])

        # WLNBB
        try:
            wlnbb = compute_wlnbb(df)
            bull_score, bear_score = score_last_bar(last_sig, wlnbb)
            l_sig = l_signal_label(wlnbb.iloc[-1])
        except Exception:
            bull_score, bear_score, l_sig = 0, 0, ""

        # Only keep bars where T/Z fired OR combined score is meaningful
        if last_sig == 0 and bull_score < 2 and bear_score < 2:
            return None

        last_row = df.iloc[-1]
        prev_row = df.iloc[-2] if len(df) > 1 else last_row
        last_price = float(last_row["close"])
        prev_price = float(prev_row["close"])
        change_pct = round(
            (last_price - prev_price) / prev_price * 100, 2
        ) if prev_price else 0.0
        volume = int(last_row.get("volume", 0)) if "volume" in df.columns else 0

        pat = " → ".join(sigs["sig_name"].tail(3).tolist())

        # Extra WLNBB fields for display
        vol_bucket = candle_dir = l_combo = ""
        try:
            last_w = wlnbb.iloc[-1]
            vol_bucket = str(last_w.get("vol_bucket", ""))
            candle_dir = str(last_w.get("candle_dir", ""))
            l_combo    = str(last_w.get("l_combo", "NONE"))
        except Exception:
            pass

        return {
            "ticker":       ticker,
            "sig_id":       last_sig,
            "sig_name":     SIG_NAMES.get(last_sig, "NONE"),
            "pattern_3bar": pat,
            "l_signal":     l_sig,
            "bull_score":   bull_score,
            "bear_score":   bear_score,
            "last_price":   round(last_price, 2),
            "volume":       volume,
            "change_pct":   change_pct,
            "interval":     interval,
            "vol_bucket":   vol_bucket,
            "candle_dir":   candle_dir,
            "l_combo":      l_combo,
        }
    except Exception as exc:
        log.debug("Scanner skip %s: %s", ticker, exc)
        return None


# ── Main scan ─────────────────────────────────────────────────────────────────

def get_scan_progress() -> dict:
    """Return a copy of the current scan progress state."""
    return dict(_scan_state)


def run_scan(interval: str = "1d", workers: int = 8) -> int:
    """
    Scan all tickers. Save results to SQLite incrementally.
    Returns count of results saved.
    """
    _init_db()
    tickers = get_tickers()
    now_iso = datetime.now(timezone.utc).isoformat()

    _scan_state.update({"running": True, "done": 0, "total": len(tickers),
                        "found": 0, "interval": interval})

    con = sqlite3.connect(DB_PATH)
    cur = con.execute(
        "INSERT INTO scan_runs (interval, started_at) VALUES (?,?)",
        (interval, now_iso),
    )
    scan_id = cur.lastrowid
    con.commit()
    con.close()

    results = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_scan_ticker, t, interval): t for t in tickers}
        for fut in as_completed(futures):
            _scan_state["done"] += 1
            row = fut.result()
            if row is None:
                continue
            row["scan_id"]   = scan_id
            row["scanned_at"] = now_iso
            results.append(row)
            _scan_state["found"] = len(results)

            # Write incrementally every 20 results
            if len(results) % 20 == 0:
                _flush(results[-20:])

    # Final flush for remainder
    remainder = results[-(len(results) % 20) or len(results):]
    if remainder:
        _flush(remainder)

    # Update scan_run record
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "UPDATE scan_runs SET completed_at=?, result_count=? WHERE id=?",
        (datetime.now(timezone.utc).isoformat(), len(results), scan_id),
    )
    # Remove old results for this interval (keep last 2 scan_ids)
    con.execute("""
        DELETE FROM scan_results
        WHERE interval=? AND scan_id NOT IN (
            SELECT id FROM scan_runs
            WHERE interval=?
            ORDER BY id DESC LIMIT 2
        )
    """, (interval, interval))
    con.commit()
    con.close()

    _scan_state["running"] = False
    log.info("Scan %d complete: %d results", scan_id, len(results))
    return len(results)


def _flush(rows: list[dict]) -> None:
    if not rows:
        return
    con = sqlite3.connect(DB_PATH)
    con.executemany(
        "INSERT INTO scan_results "
        "(scan_id,ticker,sig_id,sig_name,pattern_3bar,l_signal,"
        " bull_score,bear_score,last_price,volume,change_pct,interval,scanned_at) "
        "VALUES (:scan_id,:ticker,:sig_id,:sig_name,:pattern_3bar,:l_signal,"
        " :bull_score,:bear_score,:last_price,:volume,:change_pct,:interval,:scanned_at)",
        rows,
    )
    con.commit()
    con.close()


# ── Query helpers ─────────────────────────────────────────────────────────────

def get_results(
    interval: str = "1d",
    limit: int = 50,
    min_bull: int = 0,
    min_bear: int = 0,
    tab: str = "all",
) -> list[dict]:
    """Return latest scan results. tab: all | bull | bear | strong | fire"""
    _init_db()
    con = sqlite3.connect(DB_PATH)

    # Filter by last scan_id for this interval
    last_run = con.execute(
        "SELECT MAX(id) FROM scan_runs WHERE interval=?", (interval,)
    ).fetchone()[0]
    if last_run is None:
        con.close()
        return []

    filters = ["scan_id=?"]
    params: list = [last_run]

    if tab == "bull":
        filters.append("bull_score >= 4")
    elif tab == "bear":
        filters.append("bear_score >= 3")
    elif tab == "strong":
        filters.append("bull_score >= 6")
    elif tab == "fire":
        filters.append("bull_score >= 8")

    if min_bull > 0:
        filters.append(f"bull_score >= {int(min_bull)}")
    if min_bear > 0:
        filters.append(f"bear_score >= {int(min_bear)}")

    where = " AND ".join(filters)
    rows = con.execute(
        f"SELECT ticker,sig_id,sig_name,pattern_3bar,l_signal,"
        f"bull_score,bear_score,last_price,volume,change_pct,scanned_at "
        f"FROM scan_results WHERE {where} "
        f"ORDER BY bull_score DESC, sig_id DESC LIMIT ?",
        (*params, limit),
    ).fetchall()
    con.close()

    return [
        {
            "ticker":       r[0],
            "sig_id":       r[1],
            "sig_name":     r[2],
            "pattern_3bar": r[3],
            "l_signal":     r[4],
            "bull_score":   r[5],
            "bear_score":   r[6],
            "last_price":   r[7],
            "volume":       r[8],
            "change_pct":   r[9],
            "scanned_at":   r[10],
            # These fields are not stored in the DB yet; front-end shows them when live
            "vol_bucket":   "",
            "candle_dir":   "",
        }
        for r in rows
    ]


def get_last_scan_time(interval: str = "1d") -> str | None:
    _init_db()
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        "SELECT completed_at FROM scan_runs WHERE interval=? "
        "ORDER BY id DESC LIMIT 1",
        (interval,),
    ).fetchone()
    con.close()
    return row[0] if row else None


# ── Watchlist persistence ──────────────────────────────────────────────────────

def save_watchlist(tickers: list[str]) -> None:
    _init_db()
    now = datetime.now(timezone.utc).isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM watchlist")
    con.executemany(
        "INSERT OR REPLACE INTO watchlist (ticker, added_at) VALUES (?, ?)",
        [(t.upper().strip(), now) for t in tickers if t.strip()],
    )
    con.commit()
    con.close()


def load_watchlist() -> list[str]:
    _init_db()
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT ticker FROM watchlist ORDER BY added_at"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


# ── Settings persistence ──────────────────────────────────────────────────────

def save_settings(settings: dict) -> None:
    _init_db()
    con = sqlite3.connect(DB_PATH)
    con.executemany(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        [(k, str(v)) for k, v in settings.items()],
    )
    con.commit()
    con.close()


def load_settings() -> dict:
    _init_db()
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT key, value FROM settings").fetchall()
    con.close()
    return {r[0]: r[1] for r in rows}
