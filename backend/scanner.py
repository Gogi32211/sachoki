"""
scanner.py — scan ticker universe for active T/Z signals.
Saves results to SQLite. Scheduled at 09:30, 12:00, 15:30 EST.
"""
from __future__ import annotations
import os, sqlite3, logging
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

from signal_engine import compute_signals, ok3, SIG_NAMES

log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "/tmp/scanner.db")

# ---------------------------------------------------------------------------
# Default ticker universe (~200 liquid US names as fallback)
# ---------------------------------------------------------------------------
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
    "COIN","RBLX","HOOD","SOFI","AFRM","UPST","OPEN","RIVN","LCID","NIO",
    "BABA","JD","PDD","BIDU","DDOG","NET","ZS","OKTA","TWLO","MDB",
    "ESTC","HUBS","GTLB","U","DOCN","CFLT","IOT","TOST","ASAN","BILL",
    "DUOL","APPN","AI","PATH","BRZE","SEMR","SPT","MNDY","WIX","SAMSF",
    "F","GM","STLA","TM","HMC","RACE","HOG","TSLA","XPEV","LI",
    "DAL","UAL","AAL","LUV","ALK","JBLU","SAVE","HA","SKYW","MESA",
    "CCL","RCL","NCLH","MGM","WYNN","LVS","CZR","PENN","DKNG","MLCO",
    "DIS","PARA","WBD","NFLX","ROKU","SPOT","TTWO","EA","ATVI","ZNGA",
    "XOM","CVX","COP","SLB","HAL","BKR","MRO","DVN","FANG","PXD",
    "GLD","SLV","GDX","GDXJ","NEM","AEM","WPM","FNV","RGLD","PAAS",
    "SPY","QQQ","IWM","DIA","EEM","GDX","XLF","XLE","XLK","XLV",
]


def get_tickers() -> list[str]:
    """Try Wikipedia S&P 500; fall back to hardcoded list."""
    try:
        tables = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            attrs={"id": "constituents"},
        )
        sp500 = tables[0]["Symbol"].tolist()
        return [t.replace(".", "-") for t in sp500]
    except Exception:
        return _FALLBACK


def _init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS scan_results (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            sig_id      INTEGER,
            sig_name    TEXT,
            pattern_3bar TEXT,
            interval    TEXT,
            scanned_at  TEXT
        )
    """)
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_interval_time "
        "ON scan_results(interval, scanned_at DESC)"
    )
    con.commit()
    con.close()


def run_scan(interval: str = "1d") -> int:
    """
    Fetch OHLCV for every ticker, compute signals, save active ones to DB.
    Returns count of results saved.
    """
    _init_db()
    tickers = get_tickers()
    results = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for ticker in tickers:
        try:
            raw = yf.download(
                ticker, period="90d", interval=interval,
                progress=False, auto_adjust=True, threads=False,
            )
            if raw is None or raw.empty or len(raw) < 5:
                continue

            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw.columns = [str(c).lower() for c in raw.columns]

            df = raw[["open", "high", "low", "close"]].dropna()
            if len(df) < 5:
                continue

            sigs = compute_signals(df)
            last_sig = int(sigs["sig_id"].iloc[-1])
            if last_sig == 0:
                continue

            pat = " → ".join(sigs["sig_name"].tail(3).tolist())
            results.append({
                "ticker": ticker,
                "sig_id": last_sig,
                "sig_name": SIG_NAMES.get(last_sig, "NONE"),
                "pattern_3bar": pat,
                "interval": interval,
                "scanned_at": now_iso,
            })
        except Exception as exc:
            log.debug("Scanner skip %s: %s", ticker, exc)
            continue

    if results:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "DELETE FROM scan_results WHERE interval = ?", (interval,)
        )
        con.executemany(
            "INSERT INTO scan_results "
            "(ticker,sig_id,sig_name,pattern_3bar,interval,scanned_at) "
            "VALUES (:ticker,:sig_id,:sig_name,:pattern_3bar,:interval,:scanned_at)",
            results,
        )
        con.commit()
        con.close()

    log.info("Scan complete: %d active signals found", len(results))
    return len(results)


def get_results(interval: str = "1d", limit: int = 50) -> list[dict]:
    """Return latest scan results from DB."""
    _init_db()
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT ticker,sig_id,sig_name,pattern_3bar,scanned_at "
        "FROM scan_results WHERE interval=? "
        "ORDER BY scanned_at DESC LIMIT ?",
        (interval, limit),
    ).fetchall()
    con.close()
    return [
        {"ticker": r[0], "sig_id": r[1], "sig_name": r[2],
         "pattern_3bar": r[3], "scanned_at": r[4]}
        for r in rows
    ]
