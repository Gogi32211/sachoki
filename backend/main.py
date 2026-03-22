"""
main.py — FastAPI app + APScheduler + all API routes.
"""
from __future__ import annotations
import os, logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware

from data import fetch_ohlcv
from signal_engine import compute_signals
from predictor import predict_next
from scanner import run_scan, get_results

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

_scheduler = BackgroundScheduler(timezone="America/New_York")
_scheduler.add_job(
    lambda: run_scan("1d"),
    CronTrigger(hour="9,12,15", minute="30"),
    id="daily_scan",
    replace_existing=True,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _scheduler.start()
    log.info("Scheduler started")
    yield
    _scheduler.shutdown(wait=False)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="TZ Signal Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "service": "tz-signal-dashboard"}


@app.get("/api/signals/{ticker}")
def api_signals(ticker: str, tf: str = "1d", bars: int = 100):
    """OHLCV + signal columns for a single ticker."""
    try:
        df = fetch_ohlcv(ticker, interval=tf, bars=bars)
        sigs = compute_signals(df)
        out = df.join(sigs)
        out.index = out.index.astype(str)
        records = out.reset_index().rename(columns={"index": "date"})
        # Ensure JSON-serialisable types
        for col in ["sig_id", "bc", "zc"]:
            if col in records.columns:
                records[col] = records[col].astype(int)
        return records.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/predict/{ticker}")
def api_predict(ticker: str, tf: str = "1d"):
    """3-bar and 2-bar next-signal prediction."""
    try:
        df = fetch_ohlcv(ticker, interval=tf, bars=5000)
        sigs = compute_signals(df)
        full = df.join(sigs)
        return predict_next(full)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/scan/results")
def api_scan_results(tf: str = "1d", limit: int = 50):
    """Latest scanner results from DB."""
    return get_results(interval=tf, limit=limit)


@app.post("/api/scan/trigger")
def api_scan_trigger(background_tasks: BackgroundTasks, tf: str = "1d"):
    """Kick off a background scan immediately."""
    background_tasks.add_task(run_scan, tf)
    return {"status": "scan started", "estimated_seconds": 120}


@app.get("/api/watchlist")
def api_watchlist(
    tickers: str = Query(..., description="Comma-separated tickers"),
    tf: str = "1d",
):
    """Current signal + price for a list of tickers."""
    result = []
    for raw_ticker in tickers.split(","):
        ticker = raw_ticker.strip().upper()
        if not ticker:
            continue
        try:
            df = fetch_ohlcv(ticker, interval=tf, bars=5)
            sigs = compute_signals(df)
            last = df.iloc[-1]
            prev = df.iloc[-2] if len(df) > 1 else last
            change_pct = round(
                (float(last["close"]) - float(prev["close"])) / float(prev["close"]) * 100, 2
            )
            sig = sigs.iloc[-1]
            result.append({
                "ticker": ticker,
                "price": round(float(last["close"]), 2),
                "change_pct": change_pct,
                "sig_id": int(sig["sig_id"]),
                "sig_name": str(sig["sig_name"]),
                "is_bull": bool(sig["is_bull"]),
                "is_bear": bool(sig["is_bear"]),
            })
        except Exception as exc:
            result.append({"ticker": ticker, "error": str(exc)})
    return result
