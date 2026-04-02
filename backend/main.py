"""
main.py — FastAPI app + APScheduler + all API routes.
"""
from __future__ import annotations
import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from data import fetch_ohlcv
from signal_engine import compute_signals
from wlnbb_engine import compute_wlnbb, score_last_bar, score_bars, l_signal_label
from predictor import predict_next
from l_sequence_predictor import predict_l_next
from stats_engine import compute_tz_l_matrix
from scanner import (
    run_scan, get_results, get_last_scan_time,
    get_scan_progress,
    save_watchlist, load_watchlist,
    save_settings, load_settings,
    run_combo_scan, get_combo_results, get_last_combo_scan_time,
    get_combo_scan_progress,
)
from combo_engine import compute_combo, last_n_active, COMBO_LABELS
from pump_finder import find_pump_combos, save_pump_combos, get_pump_combos

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


# ── Lifespan (scheduler) ──────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = None
    try:
        scheduler = BackgroundScheduler(timezone="America/New_York")
        scheduler.add_job(
            lambda: run_scan("1d"),
            CronTrigger(hour="9,12,15", minute="30"),
            id="daily_scan",
            replace_existing=True,
        )
        scheduler.start()
        log.info("Scheduler started")
    except Exception as exc:
        log.warning("Scheduler failed to start: %s", exc)

    yield

    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="TZ Signal Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Utilities ─────────────────────────────────────────────────────────────────

def _normalise_date(idx) -> list[str]:
    """Convert a DataFrame index to clean YYYY-MM-DD strings."""
    try:
        return list(idx.strftime("%Y-%m-%d"))
    except AttributeError:
        return [str(v)[:10] for v in idx]


def _df_to_records(df) -> list[dict]:
    """Convert DataFrame to JSON-safe list of dicts with a 'date' column."""
    dates = _normalise_date(df.index)
    df = df.copy()
    df.index = dates
    df.index.name = "date"
    records = df.reset_index()
    first = records.columns[0]
    if first != "date":
        records = records.rename(columns={first: "date"})
    records["date"] = records["date"].astype(str).str[:10]
    for col in ["sig_id", "bc", "zc"]:
        if col in records.columns:
            records[col] = records[col].astype(int)
    for col in ["is_bull", "is_bear"]:
        if col in records.columns:
            records[col] = records[col].astype(bool)
    # Convert bool columns to plain bool (handles numpy bool_)
    bool_cols = [c for c in records.columns
                 if records[c].dtype == object and c not in ("date", "sig_name", "l_combo", "vol_bucket", "candle_dir")]
    for col in records.columns:
        if col in ("date", "sig_name", "l_combo", "vol_bucket", "candle_dir"):
            continue
        try:
            if str(records[col].dtype) == "bool":
                records[col] = records[col].astype(bool)
        except Exception:
            pass
    return records.to_dict(orient="records")


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "service": "tz-signal-dashboard", "version": "2.1"}


# ── Signals (now includes WLNBB columns + per-bar scores) ────────────────────

@app.get("/api/signals/{ticker}")
def api_signals(ticker: str, tf: str = "1d", bars: int = 150):
    """OHLCV + T/Z signal columns + WLNBB overlays for a single ticker."""
    try:
        df    = fetch_ohlcv(ticker, interval=tf, bars=bars)
        sigs  = compute_signals(df)
        wlnbb = compute_wlnbb(df)
        scores = score_bars(sigs["sig_id"], wlnbb)
        out   = df.join(sigs).join(wlnbb).join(scores)
        return _df_to_records(out)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── WLNBB ─────────────────────────────────────────────────────────────────────

@app.get("/api/wlnbb/{ticker}")
def api_wlnbb(ticker: str, tf: str = "1d", bars: int = 150):
    """OHLCV + WLNBB L-signal columns for a single ticker."""
    try:
        df    = fetch_ohlcv(ticker, interval=tf, bars=bars)
        wlnbb = compute_wlnbb(df)
        out   = df.join(wlnbb)
        return _df_to_records(out)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Watchlist ─────────────────────────────────────────────────────────────────

@app.get("/api/watchlist")
def api_watchlist(
    tickers: str = Query(..., description="Comma-separated tickers"),
    tf: str = "1d",
):
    """Current signal + price for a list of tickers."""
    result = []
    for raw in tickers.split(","):
        ticker = raw.strip().upper()
        if not ticker:
            continue
        try:
            df   = fetch_ohlcv(ticker, interval=tf, bars=10)
            sigs = compute_signals(df)
            last = df.iloc[-1]
            prev = df.iloc[-2] if len(df) > 1 else last
            pct  = round(
                (float(last["close"]) - float(prev["close"]))
                / float(prev["close"]) * 100, 2
            )
            sig = sigs.iloc[-1]

            # WLNBB quick score + extra fields
            try:
                wlnbb = compute_wlnbb(df)
                bull_score, bear_score = score_last_bar(int(sig["sig_id"]), wlnbb)
                last_w = wlnbb.iloc[-1]
                l_sig       = l_signal_label(last_w)
                vol_bucket  = str(last_w.get("vol_bucket", ""))
                candle_dir  = str(last_w.get("candle_dir", ""))
                l_combo     = str(last_w.get("l_combo", "NONE"))
                blue        = bool(last_w.get("BLUE", False))
                cci_ready   = bool(last_w.get("CCI_READY", False))
                pre_pump    = bool(last_w.get("PRE_PUMP", False))
            except Exception:
                bull_score, bear_score = 0, 0
                l_sig = vol_bucket = candle_dir = ""
                l_combo = "NONE"
                blue = cci_ready = pre_pump = False

            result.append({
                "ticker":      ticker,
                "price":       round(float(last["close"]), 2),
                "change_pct":  pct,
                "sig_id":      int(sig["sig_id"]),
                "sig_name":    str(sig["sig_name"]),
                "is_bull":     bool(sig["is_bull"]),
                "is_bear":     bool(sig["is_bear"]),
                "bull_score":  bull_score,
                "bear_score":  bear_score,
                "l_signal":    l_sig,
                "vol_bucket":  vol_bucket,
                "candle_dir":  candle_dir,
                "l_combo":     l_combo,
                "blue":        blue,
                "cci_ready":   cci_ready,
                "pre_pump":    pre_pump,
            })
        except Exception as exc:
            result.append({"ticker": ticker, "error": str(exc)})
    return result


@app.get("/api/watchlist/saved")
def api_watchlist_saved():
    return {"tickers": load_watchlist()}


@app.post("/api/watchlist/save")
def api_watchlist_save(body: dict):
    tickers = body.get("tickers", [])
    save_watchlist(tickers)
    return {"status": "ok", "count": len(tickers)}


# ── Predict (T/Z + L-combo, all 4 predictors) ────────────────────────────────

@app.get("/api/predict/{ticker}")
def api_predict(ticker: str, tf: str = "1d"):
    """3-bar and 2-bar next-signal prediction for T/Z signals and L combos."""
    try:
        df    = fetch_ohlcv(ticker, interval=tf, bars=5000)
        sigs  = compute_signals(df)
        full  = df.join(sigs)
        tz    = predict_next(full)          # {"tz_3bar": ..., "tz_2bar": ...}

        wlnbb = compute_wlnbb(df)
        full_w = full.join(wlnbb)
        l_preds = predict_l_next(full_w)   # {"l_3bar": ..., "l_2bar": ...}

        return {**tz, **l_preds}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── L-Predict (dedicated endpoint) ───────────────────────────────────────────

@app.get("/api/l-predict/{ticker}")
def api_l_predict(ticker: str, tf: str = "1d"):
    """L-combo 2-bar and 3-bar sequence predictors."""
    try:
        df    = fetch_ohlcv(ticker, interval=tf, bars=5000)
        wlnbb = compute_wlnbb(df)
        return predict_l_next(wlnbb)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── T/Z × L Stats ─────────────────────────────────────────────────────────────

@app.get("/api/tz-l-stats/{ticker}")
def api_tz_l_stats(ticker: str, tf: str = "1d"):
    """25 × 12 T/Z signal × L-column co-occurrence matrix."""
    try:
        df    = fetch_ohlcv(ticker, interval=tf, bars=5000)
        sigs  = compute_signals(df)
        wlnbb = compute_wlnbb(df)
        combined = sigs.join(wlnbb)
        return {"matrix": compute_tz_l_matrix(combined)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Scanner ───────────────────────────────────────────────────────────────────

@app.get("/api/scan/results")
def api_scan_results(
    tf: str = "1d",
    limit: int = 100,
    tab: str = "all",
    min_score: int = 0,
):
    """Latest scan results from DB. tab: all | bull | bear | strong | fire"""
    try:
        results   = get_results(interval=tf, limit=limit, min_bull=min_score, tab=tab)
        last_time = get_last_scan_time(tf)
        return {"results": results, "last_scan": last_time}
    except Exception as exc:
        log.exception("scan/results error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/scan/trigger")
def api_scan_trigger(background_tasks: BackgroundTasks, tf: str = "1d"):
    background_tasks.add_task(run_scan, tf)
    return {"status": "scan started"}


@app.get("/api/scan/status")
def api_scan_status():
    """Current scan progress: running, done, total, found."""
    return get_scan_progress()


# ── Combined scan ─────────────────────────────────────────────────────────────

@app.get("/api/combined-scan")
def api_combined_scan(
    tf: str = "1d",
    min_score: int = Query(4, ge=0, le=10),
    tab: str = "bull",
    limit: int = 100,
):
    """Tickers with combined score >= threshold, sorted by score desc."""
    try:
        results   = get_results(interval=tf, limit=limit, min_bull=min_score, tab=tab)
        last_time = get_last_scan_time(tf)
        return {"results": results, "last_scan": last_time}
    except Exception as exc:
        log.exception("combined-scan error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Pump combos ───────────────────────────────────────────────────────────────

@app.get("/api/pump-combos")
def api_pump_combos(
    threshold: float = 2.0,
    window: int = 20,
    combo_len: int = 3,
    limit: int = 50,
):
    combos = get_pump_combos(
        threshold=threshold, window=window, combo_len=combo_len, limit=limit
    )
    return {"combos": combos, "count": len(combos)}


@app.post("/api/pump-combos/trigger")
def api_pump_trigger(
    background_tasks: BackgroundTasks,
    threshold: float = 2.0,
    window: int = 20,
    combo_len: int = 3,
):
    from scanner import get_tickers

    def _run():
        tickers = get_tickers()
        df = find_pump_combos(
            tickers,
            pump_threshold=threshold,
            pump_window=window,
            combo_len=combo_len,
        )
        save_pump_combos(df, threshold=threshold, window=window, combo_len=combo_len)
        log.info("Pump combo mining done: %d combos", len(df))

    background_tasks.add_task(_run)
    return {"status": "started", "estimated_minutes": 15}


# ── 260323 Combo scan ─────────────────────────────────────────────────────────

@app.get("/api/combo-scan")
def api_combo_scan(
    signal: str = "all",
    limit: int = 200,
):
    """Latest 260323 combo scan results. signal: all | buy_2809 | rocket | ..."""
    try:
        results   = get_combo_results(signal_filter=signal, limit=limit)
        last_time = get_last_combo_scan_time()
        return {"results": results, "last_scan": last_time}
    except Exception as exc:
        log.exception("combo-scan error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/combo-scan/trigger")
def api_combo_scan_trigger(
    background_tasks: BackgroundTasks,
    tf: str = "1d",
    n_bars: int = 3,
):
    background_tasks.add_task(run_combo_scan, tf, n_bars)
    return {"status": "combo scan started"}


@app.get("/api/combo-scan/status")
def api_combo_scan_status():
    return get_combo_scan_progress()


@app.get("/api/combo-scan/debug/{ticker}")
def api_combo_scan_debug(ticker: str, tf: str = "1d", rows: int = 7, n_bars: int = 3):
    """
    Show last `rows` bars of combo signals for a ticker.
    Helps diagnose which bars triggered which signals.
    """
    import yfinance as yf
    try:
        df = yf.Ticker(ticker.upper()).history(period="90d", interval=tf, auto_adjust=True)
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data for {ticker}")
        df.columns = [c.lower() for c in df.columns]
        combo = compute_combo(df)
        active = last_n_active(combo, n_bars)

        tail = combo.tail(rows)
        signal_cols = list(COMBO_LABELS.keys())

        bar_rows = []
        for date, row in tail.iterrows():
            fired = [COMBO_LABELS[c] for c in signal_cols if row.get(c, False)]
            bar_rows.append({
                "date":    str(date.date()) if hasattr(date, "date") else str(date),
                "signals": fired,
            })

        active_labels = [COMBO_LABELS[k] for k, v in active.items() if v]

        return {
            "ticker":       ticker.upper(),
            "n_bars":       n_bars,
            "active":       active_labels,
            "bars":         bar_rows,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Power Scan (260323 + T/Z + WLNBB confluence) ──────────────────────────────

@app.get("/api/power-scan")
def api_power_scan(limit: int = 200):
    from power_engine import get_power_results, get_last_power_scan_time
    results   = get_power_results(limit=limit)
    last_time = get_last_power_scan_time()
    return {"results": results, "last_scan": last_time}


@app.post("/api/power-scan/trigger")
def api_power_scan_trigger(
    background_tasks: BackgroundTasks,
    tf: str = "1d",
    n_bars: int = 3,
):
    from power_engine import run_power_scan
    background_tasks.add_task(run_power_scan, tf, n_bars)
    return {"status": "power scan started"}


@app.get("/api/power-scan/status")
def api_power_scan_status():
    from power_engine import get_power_scan_progress
    return get_power_scan_progress()


# ── TURBO Scan (unified all-engine scan) ─────────────────────────────────────

@app.get("/api/turbo-scan")
def api_turbo_scan(
    limit: int = 500,
    min_score: float = 0,
    direction: str = "bull",
    tf: str = "1d",
    universe: str = "sp500",
):
    from turbo_engine import get_turbo_results, get_last_turbo_scan_time
    results   = get_turbo_results(limit=limit, min_score=min_score, direction=direction,
                                  tf=tf, universe=universe)
    last_time = get_last_turbo_scan_time(tf=tf, universe=universe)
    return {"results": results, "last_scan": last_time}


@app.post("/api/turbo-scan/trigger")
def api_turbo_scan_trigger(
    background_tasks: BackgroundTasks,
    tf: str = "1d",
    universe: str = "sp500",
):
    from turbo_engine import run_turbo_scan
    background_tasks.add_task(run_turbo_scan, tf, universe)
    return {"status": "turbo scan started", "tf": tf, "universe": universe}


@app.get("/api/turbo-scan/status")
def api_turbo_scan_status():
    from turbo_engine import get_turbo_progress
    return get_turbo_progress()


# ── BR Scan (260328 Break Readiness) ──────────────────────────────────────────

@app.get("/api/br-scan")
def api_br_scan(limit: int = 300, min_br: float = 0, entry: str = "all", tf: str = "1d"):
    from br_engine import get_br_results, get_last_br_scan_time
    results   = get_br_results(limit=limit, min_br=min_br, entry_filter=entry, tf=tf)
    last_time = get_last_br_scan_time(tf=tf)
    return {"results": results, "last_scan": last_time}


@app.post("/api/br-scan/trigger")
def api_br_scan_trigger(background_tasks: BackgroundTasks, tf: str = "1d"):
    from br_engine import run_br_scan
    background_tasks.add_task(run_br_scan, tf)
    return {"status": "br scan started"}


@app.get("/api/br-scan/status")
def api_br_scan_status():
    from br_engine import get_br_scan_progress
    return get_br_scan_progress()


# ── Settings ──────────────────────────────────────────────────────────────────

@app.get("/api/settings")
def api_get_settings():
    return load_settings()


@app.post("/api/settings")
def api_save_settings(body: dict):
    save_settings(body)
    return {"status": "ok"}


# ── Serve React SPA (must be last) ────────────────────────────────────────────

_static = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_static):
    app.mount("/", StaticFiles(directory=_static, html=True), name="static")
