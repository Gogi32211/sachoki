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
from combo_engine import compute_combo, last_n_active, active_signal_labels
from sq_engine   import compute_sq
from wick_engine import compute_wick
from cisd_engine import compute_cisd

# ── Combo extra boolean columns ───────────────────────────────────────────────
_COMBO_L_COLS = [
    # WLNBB L signals
    "l34", "l43", "l64", "l22",
    "cci_ready", "blue", "fri34", "pre_pump", "bo_up", "bx_up",
    # WLNBB FUCHSIA RH/RL (from 260315)
    "fuchsia_rh", "fuchsia_rl",
    # 260312 VSA signals
    "sq", "ns", "nd", "sig3_up", "sig3_dn",
    # 3112_2C wick reversal
    "wick_bull", "wick_bear",
    # 250115 CISD sequences
    "cisd_seq", "cisd_ppm", "cisd_mpm", "cisd_pmm",
]

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
    # Mega-cap / S&P 500 core
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
    # Tech / Growth
    "MU","PANW","CRWD","SNOW","PLTR","SQ","SHOP","UBER","LYFT","ABNB",
    "COIN","RBLX","HOOD","SOFI","AFRM","UPST","RIVN","LCID","NIO",
    "BABA","JD","PDD","BIDU","DDOG","NET","ZS","OKTA","TWLO","MDB",
    "ESTC","HUBS","GTLB","U","DOCN","CFLT","IOT","TOST","ASAN","BILL",
    "DUOL","APPN","AI","PATH","BRZE","SEMR","SPT","MNDY","WIX",
    # Auto / Transport
    "F","GM","TM","HMC","RACE","XPEV","LI",
    "DAL","UAL","AAL","LUV","ALK","JBLU",
    # Leisure / Gaming
    "CCL","RCL","NCLH","MGM","WYNN","LVS","CZR","PENN","DKNG",
    "DIS","PARA","WBD","ROKU","SPOT","TTWO","EA","ATVI","NTES","BILI",
    # Energy
    "XOM","COP","HAL","BKR","MRO","DVN","FANG","PXD","VLO","PSX","HES",
    "APA","NOV","RIG","WTI","CTRA","MTDR",
    # Metals / Mining
    "GLD","SLV","NEM","AEM","WPM","FNV","RGLD","GOLD","KGC","AGI",
    # ETFs
    "SPY","QQQ","IWM","DIA","EEM","XLF","XLE","XLK","XLV","XBI","ARKK",
    # S&P 500 mid-tier
    "MMM","AOS","ABT","AIG","ARE","AFL","ALB","ALGN","ALLE","LNT",
    "AEE","AEP","AXP","AMT","AWK","AMP","ADM","APTV","ACGL","ADI",
    "APH","AIZ","T","ATO","AZO","AVB","AVY","AXON","BKR","BALL",
    "BAX","BDX","BBY","BIO","TECH","BIIB","BXP","BSX","BA","BWA",
    "BR","BF-B","BLDR","BRO","CHRW","CDNS","CZR","CPT","CPB","COF",
    "CAH","KMX","CCL","CARR","CTLT","CAT","CBOE","CBRE","CDW","CE",
    "COR","CNC","CNP","CF","CHTR","CME","SCHW","LNG","CVX","CMG",
    "CB","CHD","CI","CINF","CTAS","CSCO","C","CFG","CLX","CMI",
    "CMS","KO","CTSH","CL","CMCSA","CAG","COP","ED","STZ","CEG",
    "COO","CPT","CPRT","GLW","CTRA","CSGP","COST","CTRA","CCI","CSX",
    "CMI","CVS","DHI","DHR","DRI","DVA","DAY","DE","DAL","XRAY",
    "DVN","DXCM","FANG","DLR","DFS","DG","DLTR","D","DPZ","ODFL",
    "DOV","DOW","DHI","DTE","DUK","DRE","DD","EMN","ETN","EBAY",
    "ECL","EIX","EW","EA","ELV","LLY","EMR","ENPH","ETR","EOG",
    "EPAM","EQT","EFX","EQIX","EQR","ESS","EL","EG","ES","RE",
    "EXC","EXPE","EXPD","EXR","XOM","FFIV","FDS","FICO","FAST",
    "FRT","FDX","FIS","FITB","FSLR","FE","FLT","FMC","F","FTNT",
    "FTV","FOXA","FOX","BEN","FCX","GRMN","IT","GEHC","GEN","GNRC",
    "GIS","GL","GPC","GWW","HAL","HIG","HAS","HCA","PEAK","HSIC",
    "HES","HPE","HLT","HOLX","HD","HON","HRL","HST","HWM","HPQ",
    "HUBB","HUM","HBAN","HII","IBM","IEX","IDXX","ITW","ILMN","INCY",
    "IR","PODD","INTC","ICE","IFF","IP","IPG","INTU","ISRG","IVZ",
    "INVH","IQV","IRM","JBHT","JBL","JKHY","J","JNJ","JCI","JPM",
    "JNPR","K","KVUE","KDP","KEY","KEYS","KMB","KIM","KMI","KHC",
    "KR","LHX","LH","LRCX","LW","LVS","LDOS","LEN","LNC","LIN",
    "LYV","LKQ","LMT","L","LOW","LULU","LYB","MTB","MRO","MPC",
    "MKTX","MAR","MMC","MLM","MAS","MA","MTCH","MKC","MCD","MCK",
    "MDT","MRK","META","MET","MTD","MGM","MCHP","MU","MSFT","MAA",
    "MRNA","MHK","MOH","TAP","MDLZ","MPWR","MNST","MCO","MS","MSI",
    "MSCI","NDAQ","NTAP","NFLX","NWL","NEM","NWSA","NWS","NEE","NKE",
    "NI","NDSN","NSC","NTRS","NOC","NCLH","NRG","NUE","NVDA","NVR",
    "NXPI","ORLY","OXY","ODFL","OMC","ON","OKE","ORCL","OTIS","PCAR",
    "PKG","PANW","PH","PAYX","PAYC","PYPL","PNR","PEP","PFE","PCG",
    "PM","PSX","PNW","PXD","PNC","POOL","PPG","PPL","PFG","PG",
    "PGR","PLD","PRU","PEG","PTC","PSA","PHM","QRVO","PWR","QCOM",
    "DGX","RL","RJF","RTX","O","REG","REGN","RF","RSG","RMD",
    "RVTY","ROK","ROL","ROP","ROST","RCL","SPGI","CRM","SBAC","SLB",
    "STX","SEE","SRE","NOW","SHW","SPG","SWKS","SJM","SNA","SOLV",
    "SO","LUV","SWK","SBUX","STT","STLD","STE","SYK","SMCI","SYF",
    "SNPS","SYY","TMUS","TROW","TTWO","TPR","TGT","TEL","TDY","TFX",
    "TER","TSLA","TXN","TXT","TMO","TJX","TSCO","TT","TDG","TRV",
    "TRMB","TFC","TYL","USB","UDR","ULTA","UNP","UAL","UPS","URI",
    "UNH","UHS","VLO","VTR","VRSN","VRSK","VZ","VRTX","VTRS","VICI",
    "V","VMC","WRB","GWW","WAB","WBA","WMT","WBD","WM","WAT",
    "WEC","WFC","WELL","WST","WDC","WRK","WY","WHR","WMB","WTW",
    "WYNN","XEL","XYL","YUM","ZBRA","ZBH","ZION","ZTS",
]


MAX_TICKERS = 700  # expanded ticker universe


def get_tickers() -> list[str]:
    """Combine Wikipedia S&P 500 + fallback list, deduplicated. Capped at MAX_TICKERS."""
    sp500 = []
    for url in [
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
    ]:
        try:
            if "wikipedia" in url:
                t = pd.read_html(url, attrs={"id": "constituents"})[0]["Symbol"].tolist()
            else:
                t = pd.read_csv(url)["Symbol"].tolist()
            sp500.extend([x.replace(".", "-") for x in t])
            if len(sp500) >= 500:
                break
        except Exception:
            pass
    combined = list(dict.fromkeys(sp500 + _FALLBACK))
    return combined[:MAX_TICKERS]


# ── DB schema ─────────────────────────────────────────────────────────────────

def _db() -> sqlite3.Connection:
    """Open a DB connection with WAL mode and a generous lock timeout."""
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def _init_db() -> None:
    con = _db()
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

        CREATE TABLE IF NOT EXISTS combo_scan_results (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id    INTEGER DEFAULT 0,
            ticker     TEXT NOT NULL,
            signals    TEXT DEFAULT '',
            buy_2809   INTEGER DEFAULT 0,
            um_2809    INTEGER DEFAULT 0,
            svs_2809   INTEGER DEFAULT 0,
            conso_2809 INTEGER DEFAULT 0,
            cons_atr   INTEGER DEFAULT 0,
            bias_up    INTEGER DEFAULT 0,
            bias_down  INTEGER DEFAULT 0,
            atr_brk    INTEGER DEFAULT 0,
            bb_brk     INTEGER DEFAULT 0,
            hilo_buy   INTEGER DEFAULT 0,
            hilo_sell  INTEGER DEFAULT 0,
            rtv        INTEGER DEFAULT 0,
            preup3     INTEGER DEFAULT 0,
            preup2     INTEGER DEFAULT 0,
            preup50    INTEGER DEFAULT 0,
            preup89    INTEGER DEFAULT 0,
            sig3g      INTEGER DEFAULT 0,
            rocket     INTEGER DEFAULT 0,
            tz_sig     TEXT DEFAULT '',
            l34        INTEGER DEFAULT 0,
            l43        INTEGER DEFAULT 0,
            l64        INTEGER DEFAULT 0,
            l22        INTEGER DEFAULT 0,
            cci_ready  INTEGER DEFAULT 0,
            blue       INTEGER DEFAULT 0,
            fri34      INTEGER DEFAULT 0,
            pre_pump   INTEGER DEFAULT 0,
            bo_up      INTEGER DEFAULT 0,
            bx_up      INTEGER DEFAULT 0,
            fuchsia_rh INTEGER DEFAULT 0,
            fuchsia_rl INTEGER DEFAULT 0,
            sq         INTEGER DEFAULT 0,
            ns         INTEGER DEFAULT 0,
            nd         INTEGER DEFAULT 0,
            sig3_up    INTEGER DEFAULT 0,
            sig3_dn    INTEGER DEFAULT 0,
            wick_bull  INTEGER DEFAULT 0,
            wick_bear  INTEGER DEFAULT 0,
            cisd_seq   INTEGER DEFAULT 0,
            cisd_ppm   INTEGER DEFAULT 0,
            cisd_mpm   INTEGER DEFAULT 0,
            cisd_pmm   INTEGER DEFAULT 0,
            last_price REAL DEFAULT 0,
            volume     INTEGER DEFAULT 0,
            change_pct REAL DEFAULT 0,
            scanned_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_combo_scan
            ON combo_scan_results(scanned_at DESC);

        CREATE TABLE IF NOT EXISTS combo_scan_runs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at   TEXT,
            completed_at TEXT,
            result_count INTEGER DEFAULT 0,
            n_bars       INTEGER DEFAULT 3
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

    # Migrate combo_scan_results: add all extra columns if missing
    existing_combo = {
        row[1]
        for row in con.execute("PRAGMA table_info(combo_scan_results)").fetchall()
    }
    for col, defn in [
        ("tz_sig",    "TEXT DEFAULT ''"),
        ("l34",       "INTEGER DEFAULT 0"),
        ("l43",       "INTEGER DEFAULT 0"),
        ("l64",       "INTEGER DEFAULT 0"),
        ("l22",       "INTEGER DEFAULT 0"),
        ("cci_ready", "INTEGER DEFAULT 0"),
        ("blue",      "INTEGER DEFAULT 0"),
        ("fri34",     "INTEGER DEFAULT 0"),
        ("pre_pump",  "INTEGER DEFAULT 0"),
        ("bo_up",     "INTEGER DEFAULT 0"),
        ("bx_up",     "INTEGER DEFAULT 0"),
        ("fuchsia_rh","INTEGER DEFAULT 0"),
        ("fuchsia_rl","INTEGER DEFAULT 0"),
        ("sq",        "INTEGER DEFAULT 0"),
        ("ns",        "INTEGER DEFAULT 0"),
        ("nd",        "INTEGER DEFAULT 0"),
        ("sig3_up",   "INTEGER DEFAULT 0"),
        ("sig3_dn",   "INTEGER DEFAULT 0"),
        ("wick_bull", "INTEGER DEFAULT 0"),
        ("wick_bear", "INTEGER DEFAULT 0"),
        ("cisd_seq",  "INTEGER DEFAULT 0"),
        ("cisd_ppm",  "INTEGER DEFAULT 0"),
        ("cisd_mpm",  "INTEGER DEFAULT 0"),
        ("cisd_pmm",  "INTEGER DEFAULT 0"),
    ]:
        if col not in existing_combo:
            con.execute(f"ALTER TABLE combo_scan_results ADD COLUMN {col} {defn}")

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

    con = _db()
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
    con = _db()
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
    con = _db()
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
    con = _db()

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
    con = _db()
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
    con = _db()
    con.execute("DELETE FROM watchlist")
    con.executemany(
        "INSERT OR REPLACE INTO watchlist (ticker, added_at) VALUES (?, ?)",
        [(t.upper().strip(), now) for t in tickers if t.strip()],
    )
    con.commit()
    con.close()


def load_watchlist() -> list[str]:
    _init_db()
    con = _db()
    rows = con.execute(
        "SELECT ticker FROM watchlist ORDER BY added_at"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


# ── Settings persistence ──────────────────────────────────────────────────────

def save_settings(settings: dict) -> None:
    _init_db()
    con = _db()
    con.executemany(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        [(k, str(v)) for k, v in settings.items()],
    )
    con.commit()
    con.close()


def load_settings() -> dict:
    _init_db()
    con = _db()
    rows = con.execute("SELECT key, value FROM settings").fetchall()
    con.close()
    return {r[0]: r[1] for r in rows}


# ── Combo scan (260323) ────────────────────────────────────────────────────────

_combo_state: dict = {
    "running": False,
    "done": 0,
    "total": 0,
    "found": 0,
}

_COMBO_BOOL_COLS = [
    "buy_2809", "um_2809", "svs_2809", "conso_2809",
    "cons_atr", "bias_up", "bias_down",
    "atr_brk", "bb_brk",
    "hilo_buy", "hilo_sell", "rtv",
    "preup3", "preup2", "preup50", "preup89",
    "sig3g", "rocket",
]


def get_combo_scan_progress() -> dict:
    return dict(_combo_state)


def _scan_combo_ticker(ticker: str, interval: str, n_bars: int = 3) -> dict | None:
    """Compute 260323 combo signals for the last bar (and last n_bars) of a ticker."""
    try:
        raw = yf.Ticker(ticker).history(
            period="90d", interval=interval, auto_adjust=True
        )
        if raw is None or raw.empty or len(raw) < 20:
            return None

        raw.columns = [str(c).lower() for c in raw.columns]
        needed = ["open", "high", "low", "close"]
        df = raw[needed + (["volume"] if "volume" in raw.columns else [])].dropna()
        if len(df) < 20:
            return None

        combo = compute_combo(df)
        active = last_n_active(combo, n_bars)

        # Skip tickers with no active signals
        if not any(active.values()):
            return None

        last  = df.iloc[-1]
        prev  = df.iloc[-2] if len(df) > 1 else last
        price = float(last["close"])
        prev_p = float(prev["close"])
        chg   = round((price - prev_p) / prev_p * 100, 2) if prev_p else 0.0
        vol   = int(last.get("volume", 0)) if "volume" in df.columns else 0

        # ── T/Z Signal (last bar) ─────────────────────────────────────────
        tz_sig = ""
        try:
            sigs     = compute_signals(df)
            last_sig = sigs.iloc[-1]
            if bool(last_sig["is_bull"]):
                tz_sig = str(last_sig["sig_name"])
        except Exception:
            pass

        # ── WLNBB L + FUCHSIA signals (last bar) ─────────────────────────
        l_flags: dict = {col: 0 for col in _COMBO_L_COLS}
        try:
            wlnbb  = compute_wlnbb(df)
            last_w = wlnbb.iloc[-1]
            l_flags.update({
                "l34":       int(bool(last_w.get("L34",       False))),
                "l43":       int(bool(last_w.get("L43",       False))),
                "l64":       int(bool(last_w.get("L64",       False))),
                "l22":       int(bool(last_w.get("L22",       False))),
                "cci_ready": int(bool(last_w.get("CCI_READY", False))),
                "blue":      int(bool(last_w.get("BLUE",      False))),
                "fri34":     int(bool(last_w.get("FRI34",     False))),
                "pre_pump":  int(bool(last_w.get("PRE_PUMP",  False))),
                "bo_up":     int(bool(last_w.get("BO_UP",     False))),
                "bx_up":     int(bool(last_w.get("BX_UP",     False))),
                "fuchsia_rh":int(bool(last_w.get("FUCHSIA_RH",False))),
                "fuchsia_rl":int(bool(last_w.get("FUCHSIA_RL",False))),
            })
        except Exception:
            pass

        # ── 260312 VSA signals (last bar) ─────────────────────────────────
        try:
            sq_df  = compute_sq(df)
            last_s = sq_df.iloc[-1]
            l_flags.update({
                "sq":      int(bool(last_s.get("SQ",      False))),
                "ns":      int(bool(last_s.get("NS",      False))),
                "nd":      int(bool(last_s.get("ND",      False))),
                "sig3_up": int(bool(last_s.get("SIG3_UP", False))),
                "sig3_dn": int(bool(last_s.get("SIG3_DN", False))),
            })
        except Exception:
            pass

        # ── 3112_2C wick signals (last bar) ──────────────────────────────
        try:
            wick_df = compute_wick(df)
            last_wk = wick_df.iloc[-1]
            l_flags.update({
                "wick_bull": int(bool(last_wk.get("WICK_BULL_CONFIRM", False))),
                "wick_bear": int(bool(last_wk.get("WICK_BEAR_CONFIRM", False))),
            })
        except Exception:
            pass

        # ── 250115 CISD sequences (last bar only) ─────────────────────────
        try:
            cisd_df  = compute_cisd(df)
            last_c   = cisd_df.iloc[-1]
            l_flags.update({
                "cisd_seq": int(bool(last_c["CISD_SEQ"])),
                "cisd_ppm": int(bool(last_c["CISD_PPM"])),
                "cisd_mpm": int(bool(last_c["CISD_MPM"])),
                "cisd_pmm": int(bool(last_c["CISD_PMM"])),
            })
        except Exception:
            pass

        return {
            "ticker":     ticker,
            "signals":    ",".join(active_signal_labels(active)),
            "tz_sig":     tz_sig,
            "last_price": round(price, 2),
            "volume":     vol,
            "change_pct": chg,
            **{col: int(active.get(col, False)) for col in _COMBO_BOOL_COLS},
            **l_flags,
        }
    except Exception as exc:
        log.debug("Combo skip %s: %s", ticker, exc)
        return None


def run_combo_scan(interval: str = "1d", n_bars: int = 3, workers: int = 8) -> int:
    """Scan all tickers for 260323 combo signals. Saves results to SQLite."""
    _init_db()
    tickers  = get_tickers()
    now_iso  = datetime.now(timezone.utc).isoformat()

    _combo_state.update({"running": True, "done": 0,
                         "total": len(tickers), "found": 0})

    con = _db()
    cur = con.execute(
        "INSERT INTO combo_scan_runs (started_at, n_bars) VALUES (?,?)",
        (now_iso, n_bars),
    )
    scan_id = cur.lastrowid
    con.commit()
    con.close()

    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_scan_combo_ticker, t, interval, n_bars): t
            for t in tickers
        }
        for fut in as_completed(futures):
            _combo_state["done"] += 1
            row = fut.result()
            if row is None:
                continue
            row["scan_id"]    = scan_id
            row["scanned_at"] = now_iso
            results.append(row)
            _combo_state["found"] = len(results)

            if len(results) % 20 == 0:
                _flush_combo(results[-20:])

    remainder = results[-(len(results) % 20) or len(results):]
    if remainder:
        _flush_combo(remainder)

    con = _db()
    con.execute(
        "UPDATE combo_scan_runs SET completed_at=?, result_count=? WHERE id=?",
        (datetime.now(timezone.utc).isoformat(), len(results), scan_id),
    )
    # Keep only last 2 scan runs
    con.execute("""
        DELETE FROM combo_scan_results
        WHERE scan_id NOT IN (
            SELECT id FROM combo_scan_runs ORDER BY id DESC LIMIT 2
        )
    """)
    con.commit()
    con.close()

    _combo_state["running"] = False
    log.info("Combo scan %d done: %d results", scan_id, len(results))
    return len(results)


def _flush_combo(rows: list[dict]) -> None:
    if not rows:
        return
    cols      = (["scan_id", "ticker", "signals", "tz_sig", "last_price", "volume",
                  "change_pct", "scanned_at"]
                 + _COMBO_BOOL_COLS + _COMBO_L_COLS)
    placeholders = ", ".join(f":{c}" for c in cols)
    col_names    = ", ".join(cols)
    con = _db()
    con.executemany(
        f"INSERT INTO combo_scan_results ({col_names}) VALUES ({placeholders})",
        rows,
    )
    con.commit()
    con.close()


def get_combo_results(
    signal_filter: str = "all",
    limit: int = 200,
) -> list[dict]:
    """Return latest combo scan results, optionally filtered by signal column."""
    _init_db()
    con = _db()

    last_run = con.execute(
        "SELECT MAX(id) FROM combo_scan_runs"
    ).fetchone()[0]
    if last_run is None:
        con.close()
        return []

    where = "scan_id=?"
    params: list = [last_run]
    if signal_filter != "all" and signal_filter in _COMBO_BOOL_COLS:
        where += f" AND {signal_filter}=1"

    cols = (["ticker", "signals", "tz_sig", "last_price", "volume", "change_pct",
             "scanned_at"]
            + _COMBO_BOOL_COLS + _COMBO_L_COLS)
    col_str = ", ".join(cols)

    rows = con.execute(
        f"SELECT {col_str} FROM combo_scan_results "
        f"WHERE {where} ORDER BY scanned_at DESC LIMIT ?",
        (*params, limit),
    ).fetchall()
    con.close()

    return [dict(zip(cols, r)) for r in rows]


def get_last_combo_scan_time() -> str | None:
    _init_db()
    con = _db()
    row = con.execute(
        "SELECT completed_at FROM combo_scan_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    con.close()
    return row[0] if row else None
