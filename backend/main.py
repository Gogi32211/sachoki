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
from wlnbb_engine import compute_wlnbb
from predictor import predict_next
from scanner import (
    run_scan, get_results, get_last_scan_time,
    save_watchlist, load_watchlist,
    save_settings, load_settings,
)
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
    # Convert any bool columns (WLNBB)
    for col in records.columns:
        if records[col].dtype == object:
            continue
        try:
            if records[col].dtype == bool:
                records[col] = records[col].astype(bool)
        except Exception:
            pass
    return records.to_dict(orient="records")


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "service": "tz-signal-dashboard", "version": "1.0"}


# ── Signals ───────────────────────────────────────────────────────────────────

@app.get("/api/signals/{ticker}")
def api_signals(ticker: str, tf: str = "1d", bars: int = 150):
    """OHLCV + T/Z signal columns for a single ticker."""
    try:
        df   = fetch_ohlcv(ticker, interval=tf, bars=bars)
        sigs = compute_signals(df)
        out  = df.join(sigs)
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

            # WLNBB quick score
            try:
                wlnbb = compute_wlnbb(df)
                from wlnbb_engine import score_last_bar, l_signal_label
                bull_score, bear_score = score_last_bar(int(sig["sig_id"]), wlnbb)
                l_sig = l_signal_label(wlnbb.iloc[-1])
            except Exception:
                bull_score, bear_score, l_sig = 0, 0, ""

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


# ── Predict ───────────────────────────────────────────────────────────────────

@app.get("/api/predict/{ticker}")
def api_predict(ticker: str, tf: str = "1d"):
    """3-bar and 2-bar next-signal prediction."""
    try:
        df   = fetch_ohlcv(ticker, interval=tf, bars=5000)
        sigs = compute_signals(df)
        full = df.join(sigs)
        return predict_next(full)
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
    results = get_results(interval=tf, limit=limit, min_bull=min_score, tab=tab)
    last_time = get_last_scan_time(tf)
    return {"results": results, "last_scan": last_time}


@app.post("/api/scan/trigger")
def api_scan_trigger(background_tasks: BackgroundTasks, tf: str = "1d"):
    background_tasks.add_task(run_scan, tf)
    return {"status": "scan started", "estimated_seconds": 120}


# ── Combined scan ─────────────────────────────────────────────────────────────

@app.get("/api/combined-scan")
def api_combined_scan(
    tf: str = "1d",
    min_score: int = Query(4, ge=0, le=10),
    tab: str = "bull",
    limit: int = 100,
):
    """Tickers with combined score >= threshold, sorted by score desc."""
    results = get_results(interval=tf, limit=limit, min_bull=min_score, tab=tab)
    last_time = get_last_scan_time(tf)
    return {"results": results, "last_scan": last_time}


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
