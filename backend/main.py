"""
main.py — FastAPI app + APScheduler + all API routes.
"""
from __future__ import annotations
import os
import logging
import concurrent.futures
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = None
    try:
        scheduler = BackgroundScheduler(timezone="America/New_York")
        def _scheduled_scan():
            if not get_scan_progress().get("running"):
                run_scan("1d")

        scheduler.add_job(
            _scheduled_scan,
            CronTrigger(hour="9,12,15", minute="30"),
            id="daily_scan",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        log.info("Scheduler started")
    except Exception as exc:
        log.warning("Scheduler failed to start: %s", exc)

    yield

    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="TZ Signal Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _normalise_date(idx) -> list[str]:
    try:
        return list(idx.strftime("%Y-%m-%d"))
    except AttributeError:
        return [str(v)[:10] for v in idx]


def _df_to_records(df) -> list[dict]:
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


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "tz-signal-dashboard", "version": "2.1"}


_ticker_info_cache: dict = {}

@app.get("/api/ticker-info/{ticker}")
def api_ticker_info(ticker: str):
    t = ticker.upper()
    if t in _ticker_info_cache:
        return _ticker_info_cache[t]
    try:
        import yfinance as yf
        info = yf.Ticker(t).info or {}
        result = {
            "ticker":  t,
            "name":    info.get("longName") or info.get("shortName") or t,
            "sector":  info.get("sector") or "",
            "industry": info.get("industry") or "",
        }
    except Exception:
        result = {"ticker": t, "name": t, "sector": "", "industry": ""}
    _ticker_info_cache[t] = result
    return result


@app.post("/api/ticker-info-batch")
def api_ticker_info_batch(body: dict):
    """Batch sector/name lookup for up to 200 tickers. Returns {ticker: {sector,...}}."""
    tickers = [str(t).upper() for t in (body.get("tickers") or [])[:200]]
    result: dict = {}

    need_fetch: list[str] = []
    for t in tickers:
        if t in _ticker_info_cache:
            result[t] = _ticker_info_cache[t]
        else:
            need_fetch.append(t)

    if need_fetch:
        def _fetch_one(t: str):
            try:
                import yfinance as yf
                info = yf.Ticker(t).info or {}
                r = {
                    "ticker": t,
                    "name":   info.get("longName") or info.get("shortName") or t,
                    "sector": info.get("sector") or "",
                    "industry": info.get("industry") or "",
                }
            except Exception:
                r = {"ticker": t, "name": t, "sector": "", "industry": ""}
            _ticker_info_cache[t] = r
            return t, r

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(need_fetch)))
        futures = {pool.submit(_fetch_one, t): t for t in need_fetch}
        done, _ = concurrent.futures.wait(futures, timeout=15)
        pool.shutdown(wait=False)

        for fut in done:
            try:
                t, r = fut.result()
                result[t] = r
            except Exception:
                pass

        for t in need_fetch:
            if t not in result:
                result[t] = {"ticker": t, "name": t, "sector": "", "industry": ""}

    return result


@app.get("/api/signals/{ticker}")
def api_signals(ticker: str, tf: str = "1d", bars: int = 150):
    try:
        df    = fetch_ohlcv(ticker, interval=tf, bars=bars)
        sigs  = compute_signals(df)
        wlnbb = compute_wlnbb(df)
        scores = score_bars(sigs["sig_id"], wlnbb)
        out   = df.join(sigs).join(wlnbb).join(scores)
        return _df_to_records(out)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/wlnbb/{ticker}")
def api_wlnbb(ticker: str, tf: str = "1d", bars: int = 150):
    try:
        df    = fetch_ohlcv(ticker, interval=tf, bars=bars)
        wlnbb = compute_wlnbb(df)
        out   = df.join(wlnbb)
        return _df_to_records(out)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/watchlist")
def api_watchlist(
    tickers: str = Query(..., description="Comma-separated tickers"),
    tf: str = "1d",
):
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


@app.get("/api/predict/{ticker}")
def api_predict(ticker: str, tf: str = "1d"):
    try:
        from predictor import compute_tz_stats, compute_tz_matrix
        df    = fetch_ohlcv(ticker, interval=tf, bars=5000)
        sigs  = compute_signals(df)
        full  = df.join(sigs)
        tz    = predict_next(full)
        tz_stats  = compute_tz_stats(full)
        tz_matrix = compute_tz_matrix(full)

        wlnbb = compute_wlnbb(df)
        full_w = full.join(wlnbb)
        l_preds = predict_l_next(full_w)

        return {**tz, **l_preds, "tz_stats": tz_stats, "tz_matrix": tz_matrix}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/l-predict/{ticker}")
def api_l_predict(ticker: str, tf: str = "1d"):
    try:
        df    = fetch_ohlcv(ticker, interval=tf, bars=5000)
        wlnbb = compute_wlnbb(df)
        return predict_l_next(wlnbb)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/pooled-predict/{ticker}")
def api_pooled_predict(ticker: str, tf: str = "1d", universe: str = "sp500"):
    try:
        from pooled_stats import get_pooled_predict, get_pooled_tz_freq, get_pooled_tz_matrix
        df    = fetch_ohlcv(ticker, interval=tf, bars=5000)
        sigs  = compute_signals(df)
        wlnbb = compute_wlnbb(df)

        sig_ids  = sigs["sig_id"].to_numpy()
        l_combos = wlnbb["l_combo"].values

        sig_seq_3 = tuple(int(s) for s in sig_ids[-3:])
        sig_seq_2 = tuple(int(s) for s in sig_ids[-2:])
        l_seq_3   = tuple(str(l) for l in l_combos[-3:])
        l_seq_2   = tuple(str(l) for l in l_combos[-2:])

        result = get_pooled_predict(sig_seq_3, sig_seq_2, l_seq_3, l_seq_2,
                                    universe=universe, interval=tf)
        result["bench_tz_stats"]  = get_pooled_tz_freq(universe, tf)
        result["bench_tz_matrix"] = get_pooled_tz_matrix(universe, tf)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/pooled-stats/build")
def api_pooled_stats_build(
    background_tasks: BackgroundTasks,
    universe: str = "sp500",
    interval: str = "1d",
    max_tickers: int = 2000,
):
    from pooled_stats import build_pooled_stats, get_pooled_state
    if get_pooled_state().get("running"):
        raise HTTPException(status_code=409, detail="Pooled stats build already running")
    background_tasks.add_task(build_pooled_stats, universe, interval, 6, max_tickers)
    return {"ok": True, "universe": universe, "interval": interval}


@app.get("/api/pooled-stats/status")
def api_pooled_stats_status(universe: str = "sp500", interval: str = "1d"):
    from pooled_stats import get_pooled_status, get_pooled_state
    data  = get_pooled_status(universe, interval)
    state = get_pooled_state()
    return {"data": data, "job": state}


@app.get("/api/tz-l-stats/{ticker}")
def api_tz_l_stats(ticker: str, tf: str = "1d"):
    try:
        def _matrix(sym):
            d = fetch_ohlcv(sym, interval=tf, bars=5000)
            return compute_tz_l_matrix(compute_signals(d).join(compute_wlnbb(d)))

        matrix = _matrix(ticker.upper())
        try:    bench_spy = _matrix("SPY")
        except: bench_spy = None
        try:    bench_qqq = _matrix("QQQ")
        except: bench_qqq = None

        return {"matrix": matrix, "bench_spy": bench_spy, "bench_qqq": bench_qqq}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/scan/results")
def api_scan_results(
    tf: str = "1d",
    limit: int = 100,
    tab: str = "all",
    min_score: int = 0,
):
    try:
        results   = get_results(interval=tf, limit=limit, min_bull=min_score, tab=tab)
        last_time = get_last_scan_time(tf)
        return {"results": results, "last_scan": last_time}
    except Exception as exc:
        log.exception("scan/results error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/scan/trigger")
def api_scan_trigger(background_tasks: BackgroundTasks, tf: str = "1d"):
    if get_scan_progress().get("running"):
        raise HTTPException(status_code=409, detail="Scan already running")
    background_tasks.add_task(run_scan, tf)
    return {"status": "scan started"}


@app.get("/api/scan/status")
def api_scan_status():
    return get_scan_progress()


@app.get("/api/combined-scan")
def api_combined_scan(
    tf: str = "1d",
    min_score: int = Query(4, ge=0, le=10),
    tab: str = "bull",
    limit: int = 100,
):
    try:
        results   = get_results(interval=tf, limit=limit, min_bull=min_score, tab=tab)
        last_time = get_last_scan_time(tf)
        return {"results": results, "last_scan": last_time}
    except Exception as exc:
        log.exception("combined-scan error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


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


@app.get("/api/combo-scan")
def api_combo_scan(
    signal: str = "all",
    limit: int = 200,
):
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
    if get_combo_scan_progress().get("running"):
        raise HTTPException(status_code=409, detail="Combo scan already running")
    background_tasks.add_task(run_combo_scan, tf, n_bars)
    return {"status": "combo scan started"}


@app.get("/api/combo-scan/status")
def api_combo_scan_status():
    return get_combo_scan_progress()


@app.get("/api/combo-scan/debug/{ticker}")
def api_combo_scan_debug(ticker: str, tf: str = "1d", rows: int = 7, n_bars: int = 3):
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
    from power_engine import run_power_scan, get_power_scan_progress
    if get_power_scan_progress().get("running"):
        raise HTTPException(status_code=409, detail="Power scan already running")
    background_tasks.add_task(run_power_scan, tf, n_bars)
    return {"status": "power scan started"}


@app.get("/api/power-scan/status")
def api_power_scan_status():
    from power_engine import get_power_scan_progress
    return get_power_scan_progress()


@app.get("/api/turbo-scan")
def api_turbo_scan(
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
):
    try:
        from turbo_engine import get_turbo_results, get_last_turbo_scan_time
        results   = get_turbo_results(limit=limit, min_score=min_score, direction=direction,
                                      tf=tf, universe=universe,
                                      price_min=price_min, price_max=price_max,
                                      rsi_min=rsi_min, rsi_max=rsi_max,
                                      cci_min=cci_min, cci_max=cci_max,
                                      vol_min=vol_min, vol_max=vol_max)
        last_time = get_last_turbo_scan_time(tf=tf, universe=universe)
        return {"results": results, "last_scan": last_time}
    except Exception as exc:
        log.exception("turbo-scan error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/turbo-scan/trigger")
def api_turbo_scan_trigger(
    background_tasks: BackgroundTasks,
    tf: str = "1d",
    universe: str = "sp500",
    lookback_n: int = 5,
    partial_day: bool = False,
    min_volume: float = 0,
    min_store_score: float = 5,
):
    from turbo_engine import run_turbo_scan, get_turbo_progress
    if get_turbo_progress().get("running"):
        raise HTTPException(status_code=409, detail="Scan already running")
    background_tasks.add_task(run_turbo_scan, tf, universe, 8, lookback_n, partial_day, min_volume, False, min_store_score)
    return {"status": "turbo scan started", "tf": tf, "universe": universe, "lookback_n": lookback_n, "partial_day": partial_day, "min_volume": min_volume, "min_store_score": min_store_score}


@app.get("/api/turbo-scan/status")
def api_turbo_scan_status():
    from turbo_engine import get_turbo_progress
    return get_turbo_progress()


@app.post("/api/turbo-scan/reset")
def api_turbo_scan_reset():
    import time as _t
    from turbo_engine import _turbo_state
    _turbo_state["running"] = False
    _turbo_state["completed_at"] = _t.time()
    _turbo_state["error"] = "Manually stopped"
    return {"status": "stopped"}


@app.get("/api/br-scan")
def api_br_scan(limit: int = 300, min_br: float = 0, entry: str = "all", tf: str = "1d"):
    from br_engine import get_br_results, get_last_br_scan_time
    results   = get_br_results(limit=limit, min_br=min_br, entry_filter=entry, tf=tf)
    last_time = get_last_br_scan_time(tf=tf)
    return {"results": results, "last_scan": last_time}


@app.post("/api/br-scan/trigger")
def api_br_scan_trigger(background_tasks: BackgroundTasks, tf: str = "1d"):
    from br_engine import run_br_scan, get_br_scan_progress
    if get_br_scan_progress().get("running"):
        raise HTTPException(status_code=409, detail="BR scan already running")
    background_tasks.add_task(run_br_scan, tf)
    return {"status": "br scan started"}


@app.get("/api/br-scan/status")
def api_br_scan_status():
    from br_engine import get_br_scan_progress
    return get_br_scan_progress()


@app.get("/api/settings")
def api_get_settings():
    return load_settings()


@app.post("/api/settings")
def api_save_settings(body: dict):
    save_settings(body)
    return {"status": "ok"}


@app.get("/api/config")
def api_config():
    from data_polygon import polygon_available
    return {"massive_api_ready": polygon_available()}


@app.get("/api/turbo-analyze/{ticker}")
def api_turbo_analyze(ticker: str, tf: str = "1d"):
    from turbo_engine import _scan_turbo_ticker
    result = _scan_turbo_ticker(ticker.upper(), tf)
    if result is None:
        raise HTTPException(status_code=404, detail="Not Found")
    return result


@app.get("/api/signal-stats/{ticker}")
def api_signal_stats(
    ticker: str,
    tf: str = "1d",
    signals: str = "",
    combo: bool = False,
    min_n: int = 3,
):
    from signal_stats_engine import run_signal_stats, SIGNAL_LABELS
    sig_list = [s.strip() for s in signals.split(",") if s.strip()]
    if not sig_list:
        sig_list = list(SIGNAL_LABELS.keys())
    return run_signal_stats(ticker.upper(), tf, sig_list, combo=combo, min_n=min_n)


# ── Pooled signal stats (SP500 aggregate) ─────────────────────────────────────
_SS_POOLED: dict = {}  # key: f"{universe}_{tf}"

# ── Stock Stat scan state ─────────────────────────────────────────────────────
_stock_stat_state: dict = {
    "running": False, "done": 0, "total": 0,
    "error": None, "output_path": None, "output_size": 0,
    "tf": None, "universe": None, "elapsed": 0.0,
    "validation": None,
}


def _ss_pooled_worker(universe: str, tf: str, signals: list, max_tickers: int = 500):
    import threading
    key = f"{universe}_{tf}"
    _SS_POOLED[key] = {"status": "running", "done": 0, "total": 0, "results": {}, "error": None}
    try:
        from scanner import get_universe_tickers
        from signal_stats_engine import run_signal_stats, SIGNAL_LABELS
        tickers = get_universe_tickers(universe)[:max_tickers]
        _SS_POOLED[key]["total"] = len(tickers)
        if not signals:
            signals = list(SIGNAL_LABELS.keys())

        # Aggregation: signal -> list of per-ticker stat dicts
        agg: dict = {s: [] for s in signals}

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _one(t):
            try:
                return run_signal_stats(t, tf, signals, combo=False, min_n=1)
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=6) as ex:
            futs = {ex.submit(_one, t): t for t in tickers}
            for fut in as_completed(futs):
                res = fut.result()
                if res and "results" in res:
                    for s, st in res["results"].items():
                        if s in agg and st.get("n", 0) > 0:
                            agg[s].append(st)
                _SS_POOLED[key]["done"] += 1

        # Weighted-average aggregation
        pooled = {}
        for sig, stats_list in agg.items():
            if not stats_list:
                continue
            total_n = sum(s["n"] for s in stats_list)
            if total_n < 5:
                continue
            def _wavg(field):
                return sum(s.get(field, 0) * s["n"] for s in stats_list) / total_n
            pooled[sig] = {
                "n":          total_n,
                "tickers":    len(stats_list),
                "bull_rate":  round(_wavg("bull_rate"), 3),
                "avg_1bar":   round(_wavg("avg_1bar"), 2),
                "avg_3bar":   round(_wavg("avg_3bar"), 2),
                "avg_5bar":   round(_wavg("avg_5bar"), 2),
                "mae_3":      round(_wavg("mae_3"), 2),
                "false_rate": round(_wavg("false_rate"), 3),
            }

        _SS_POOLED[key].update({
            "status":   "done",
            "results":  pooled,
            "labels":   {k: SIGNAL_LABELS.get(k, k) for k in pooled},
            "universe": universe,
            "tf":       tf,
        })
    except Exception as exc:
        _SS_POOLED[key].update({"status": "error", "error": str(exc)})


@app.post("/api/signal-stats/pooled/build")
def api_ss_pooled_build(
    background_tasks: BackgroundTasks,
    tf: str = "1d",
    universe: str = "sp500",
    signals: str = "",
    max_tickers: int = 500,
):
    key = f"{universe}_{tf}"
    if _SS_POOLED.get(key, {}).get("status") == "running":
        raise HTTPException(status_code=409, detail="Build already running")
    sig_list = [s.strip() for s in signals.split(",") if s.strip()]
    background_tasks.add_task(_ss_pooled_worker, universe, tf, sig_list, max_tickers)
    return {"ok": True, "universe": universe, "tf": tf}


@app.get("/api/signal-stats/pooled/status")
def api_ss_pooled_status(tf: str = "1d", universe: str = "sp500"):
    key = f"{universe}_{tf}"
    return _SS_POOLED.get(key, {"status": "idle"})


@app.get("/api/signal-correlation")
def api_signal_correlation(tf: str = "1d", universe: str = "sp500", min_pct: int = 15):
    from turbo_engine import get_turbo_results, _TURBO_COLS, _init_db, _db
    import numpy as np

    # Try requested universe first; fall back to latest scan for this tf regardless of universe
    rows = get_turbo_results(limit=5000, min_score=0, direction="all", tf=tf, universe=universe)
    if not rows:
        _init_db()
        con = _db()
        try:
            row = con.execute(
                "SELECT id, universe FROM turbo_scan_runs WHERE tf=? ORDER BY id DESC LIMIT 1", (tf,)
            ).fetchone()
        finally:
            con.close()
        if row:
            rows = get_turbo_results(limit=5000, min_score=0, direction="all", tf=tf, universe=row["universe"])

    if not rows:
        return {"n_tickers": 0, "signal_counts": {}, "pairs": []}

    # Boolean signal columns only
    bool_cols = [c for c in _TURBO_COLS if c not in {
        "turbo_score", "turbo_score_n3", "turbo_score_n5", "turbo_score_n10",
        "rsi", "cci", "avg_vol", "tz_sig", "vol_bucket", "sig_ages", "data_source", "tz_state",
        "any_f",  # derived aggregate (any F1-F11) — always redundant with individual F signals
    }]

    n = len(rows)
    mat = {c: np.array([int(bool(r.get(c, 0))) for r in rows], dtype=np.int8) for c in bool_cols}
    counts = {c: int(mat[c].sum()) for c in bool_cols}

    pairs = []
    cols_with_signals = [c for c in bool_cols if counts[c] > 0]
    for i, a in enumerate(cols_with_signals):
        for b in cols_with_signals[i+1:]:
            both = int((mat[a] & mat[b]).sum())
            if both == 0:
                continue
            ca, cb = counts[a], counts[b]
            pct_a = round(both / ca * 100) if ca else 0
            pct_b = round(both / cb * 100) if cb else 0
            max_pct = max(pct_a, pct_b)
            if max_pct >= min_pct:
                pairs.append({"sig_a": a, "sig_b": b, "both": both,
                               "a_count": ca, "b_count": cb,
                               "pct_a": pct_a, "pct_b": pct_b, "max_pct": max_pct})

    pairs.sort(key=lambda x: -x["max_pct"])

    # Compute top-C signal for each A-B pair (ABC chain)
    for p in pairs:
        ab_mask = mat[p["sig_a"]] & mat[p["sig_b"]]
        ab_n = p["both"]
        best_c, best_pct = None, 0
        for c in cols_with_signals:
            if c == p["sig_a"] or c == p["sig_b"]:
                continue
            cnt = int((ab_mask & mat[c]).sum())
            if cnt > 0:
                pct = round(cnt / ab_n * 100)
                if pct > best_pct:
                    best_pct, best_c = pct, c
        p["top_c"] = best_c
        p["pct_c"] = best_pct

    return {"n_tickers": n, "signal_counts": counts, "pairs": pairs}


@app.get("/api/admin/scan-history")
def api_admin_scan_history():
    from turbo_engine import _db, _init_db
    _init_db()
    con = _db()
    try:
        rows = con.execute("""
            SELECT id, tf, universe, started_at, completed_at, result_count
            FROM turbo_scan_runs ORDER BY id DESC LIMIT 20
        """).fetchall()
        return [{"id": r["id"], "tf": r["tf"], "universe": r["universe"],
                 "started_at": r["started_at"], "completed_at": r["completed_at"],
                 "result_count": r["result_count"]}
                for r in rows]
    finally:
        con.close()


@app.post("/api/admin/scan-start")
def api_admin_scan_start(background_tasks: BackgroundTasks, tf: str = "1d", universe: str = "sp500", min_store_score: float = 5):
    from turbo_engine import run_turbo_scan, get_turbo_progress
    if get_turbo_progress().get("running"):
        raise HTTPException(status_code=409, detail="Scan already running")
    background_tasks.add_task(run_turbo_scan, tf, universe, 8, 5, False, 0, False, min_store_score)
    return {"ok": True, "tf": tf, "universe": universe, "min_store_score": min_store_score}


@app.get("/api/bar_signals/{ticker}")
def api_bar_signals(ticker: str, tf: str = "1d", bars: int = 150):
    """Per-bar signal matrix for SuperChart view."""
    import pandas as pd
    import numpy as np
    from signal_engine import compute_g_signals, compute_b_signals
    from f_engine import compute_f_signals
    from fly_engine import compute_fly_series
    from vabs_engine import compute_vabs
    from wick_engine import compute_wick
    from ultra_engine import compute_260308_l88, compute_ultra_v2
    from turbo_engine import _calc_turbo_score
    from combo_engine import compute_tz_state

    try:
        df = fetch_ohlcv(ticker, interval=tf, bars=bars)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    _EDF = pd.DataFrame()

    try:
        sig_df = compute_signals(df)
    except Exception:
        sig_df = _EDF
    try:
        wlnbb = compute_wlnbb(df)
    except Exception:
        wlnbb = _EDF
    try:
        f_sigs = compute_f_signals(df)
    except Exception:
        f_sigs = _EDF
    try:
        fly_sigs = compute_fly_series(df)
    except Exception:
        fly_sigs = _EDF
    try:
        g_sigs = compute_g_signals(df)
    except Exception:
        g_sigs = _EDF
    try:
        b_sigs = compute_b_signals(df)
    except Exception:
        b_sigs = _EDF
    try:
        combo_df = compute_combo(df)
    except Exception:
        combo_df = _EDF
    try:
        vabs = compute_vabs(df)
    except Exception:
        vabs = _EDF
    try:
        wick = compute_wick(df)
    except Exception:
        wick = _EDF
    try:
        ultra260 = compute_260308_l88(df)
    except Exception:
        ultra260 = _EDF
    try:
        ultraV2 = compute_ultra_v2(df)
    except Exception:
        ultraV2 = _EDF
    try:
        tz_state_ser = compute_tz_state(df)
    except Exception:
        tz_state_ser = pd.Series(0, index=df.index, dtype=np.int8)
    tz_state_prev = tz_state_ser.shift(1, fill_value=0).astype(int)

    # seq_bcont vectorized from bc column
    try:
        _bc = sig_df["bc"].fillna(0).astype(int) if not sig_df.empty else pd.Series(0, index=df.index)
        _bc_p1 = _bc.shift(1, fill_value=0).astype(int)
        _bc_p2 = _bc.shift(2, fill_value=0).astype(int)
        seq_bcont_ser = (
            (_bc_p2.isin([5, 3, 6, 4, 7]) & (_bc == 1)) |
            (_bc_p1.isin([9, 10, 11])      & (_bc.isin([1, 2]))) |
            (_bc_p1.isin([1, 4, 9])        & (_bc == 2))
        ).astype(int)
    except Exception:
        seq_bcont_ser = pd.Series(0, index=df.index)

    # VA — volume ATR crossover (vol/sma20 crosses above 2.0)
    try:
        _avg20 = df["volume"].rolling(20, min_periods=1).mean()
        _vr    = (df["volume"] / _avg20.replace(0, np.nan)).fillna(0)
        va_ser = ((_vr > 2.0) & (_vr.shift(1, fill_value=0) <= 2.0)).astype(int)
    except Exception:
        va_ser = pd.Series(0, index=df.index)

    # Vol spike ratio (current bar vs previous bar)
    vol_prev  = df["volume"].shift(1)
    vol_ratio = (df["volume"] / vol_prev.replace(0, np.nan)).fillna(0)

    isIntraday = tf in ("4h", "1h", "30m", "15m")

    # RTB v4 — per-bar sequential state
    try:
        from rtb_engine import calc_rtb_v4 as _rtb_v4
        _rtb_ok = True
    except Exception:
        _rtb_ok = False
    _rtb_prev_phase    = "0"
    _rtb_prev_age      = 0
    _rtb_soft_streak   = 0
    _rtb_pending_phase = ""
    _rtb_pending_count = 0
    _rtb_history: list = []   # chronological sig_rows (oldest first)

    # ── GOG Priority Engine (260501 FULL + F8) — vectorized precomputation ──────
    try:
        from gog_engine import compute_gog_signals as _cgog, compute_forward_stats as _cfwd
        _gog_df = _cgog(df, wlnbb, sig_df, f_sigs, vabs, ultra260, ultraV2, combo_df)
        _fwd_df = _cfwd(df, _gog_df)
    except Exception:
        _gog_df = pd.DataFrame(index=df.index)
        _fwd_df = pd.DataFrame(index=df.index)

    def _gv(col, i, default=0):
        if col not in _gog_df.columns: return default
        v = _gog_df[col].iloc[i]
        return v if v is not None and not (isinstance(v, float) and np.isnan(v)) else default

    def _fv(col, i, default=None):
        if col not in _fwd_df.columns: return default
        v = _fwd_df[col].iloc[i]
        return None if (v is None or (isinstance(v, float) and np.isnan(v))) else v

    result = []

    for i in range(len(df)):
        row = df.iloc[i]
        ts  = df.index[i]
        date_val = int(ts.timestamp()) if isIntraday else str(ts)[:10]

        def _b(frame, col):
            if frame is None or frame.empty or col not in frame.columns:
                return False
            return bool(frame.iloc[i][col])

        # T/Z signal name
        tz = ""
        if not sig_df.empty and "sig_id" in sig_df.columns:
            if int(sig_df.iloc[i]["sig_id"]) > 0:
                tz = str(sig_df.iloc[i].get("sig_name", ""))

        # L / FRI / BLUE / BO / BX / BE / RL / RH / CCI signals
        l_map = [
            ("L34", "L34"), ("L43", "L43"), ("L64", "L64"), ("L22", "L22"),
            ("L555", "L555"), ("ONLY_L2L4", "L2L4"),
            ("FRI34", "FRI34"), ("FRI43", "FRI43"), ("FRI64", "FRI64"),
            ("BLUE", "BL"), ("CCI_READY", "CCI"),
            ("CCI_0_RETEST_OK", "CCI0R"), ("CCI_BLUE_TURN", "CCIB"),
            ("BO_UP", "BO↑"), ("BO_DN", "BO↓"),
            ("BX_UP", "BX↑"), ("BX_DN", "BX↓"),
            ("BE_UP", "BE↑"), ("BE_DN", "BE↓"),
            ("FUCHSIA_RL", "RL"), ("FUCHSIA_RH", "RH"), ("PRE_PUMP", "PP"),
        ]
        l_list = [lbl for col, lbl in l_map if _b(wlnbb, col)]

        # F signals
        f_list = [f"F{n}" for n in range(1, 12) if _b(f_sigs, f"f{n}")]

        # FLY — show strongest only
        fly_list = []
        for col, lbl in [("fly_abcd", "FLY"), ("fly_cd", "FLY-CD"),
                          ("fly_bd", "FLY-BD"), ("fly_ad", "FLY-AD")]:
            if _b(fly_sigs, col):
                fly_list.append(lbl)
                break

        # G signals
        g_list = [f"G{n}" for n in [1, 2, 4, 6, 11] if _b(g_sigs, f"g{n}")]

        # B signals
        b_list = [f"B{n}" for n in range(1, 12) if _b(b_sigs, f"b{n}")]

        # Combo signals
        combo_map = [
            ("rocket", "ROCKET"), ("buy_2809", "BUY"), ("sig3g", "3G"),
            ("bb_brk", "BB↑"), ("atr_brk", "ATR↑"), ("rtv", "RTV"),
            ("preup3", "P3"), ("preup2", "P2"), ("preup50", "P50"), ("preup89", "P89"),
            ("hilo_buy", "HILO↑"), ("hilo_sell", "HILO↓"),
            ("bias_up", "↑BIAS"), ("bias_down", "↓BIAS"),
            ("cons_atr", "CONS"), ("um_2809", "UM"), ("svs_2809", "SVS"),
            ("conso_2809", "CONSO"),
        ]
        combo_list = [lbl for col, lbl in combo_map if _b(combo_df, col)]

        # Vol spike
        vr = float(vol_ratio.iloc[i])
        vol_list = []
        if vr >= 20: vol_list.append("20×")
        elif vr >= 10: vol_list.append("10×")
        elif vr >= 5:  vol_list.append("5×")

        # VABS signals
        vabs_map = [
            ("best_sig", "BEST★"), ("strong_sig", "STRONG"),
            ("vbo_up", "VBO↑"), ("vbo_dn", "VBO↓"),
            ("ns", "NS"), ("nd", "ND"), ("sc", "SC"), ("bc", "BC"),
            ("sq", "SQ"), ("abs_sig", "ABS"), ("climb_sig", "CLM"), ("load_sig", "LOAD"),
        ]
        vabs_list = [lbl for col, lbl in vabs_map if _b(vabs, col)]

        # Wick signals
        wick_map = [
            ("WICK_BULL_CONFIRM", "WC↑"), ("WICK_BEAR_CONFIRM", "WC↓"),
            ("WICK_BULL_PATTERN", "WP↑"), ("WICK_BEAR_PATTERN", "WP↓"),
        ]
        wick_list = [lbl for col, lbl in wick_map if _b(wick, col)]

        # ULTRA v2 signals
        ultra_map = [
            ("best_long", "BEST↑"), ("fbo_bull", "FBO↑"), ("fbo_bear", "FBO↓"),
            ("eb_bull", "EB↑"), ("eb_bear", "EB↓"),
            ("bf_buy", "4BF"), ("bf_sell", "4BF↓"),
            ("ultra_3up", "3↑"),
        ]
        ultra_list = [lbl for col, lbl in ultra_map if _b(ultraV2, col)]
        if _b(ultra260, "sig_l88"):       ultra_list.append("L88")
        elif _b(ultra260, "sig_260308"):  ultra_list.append("260308")

        # Turbo score per bar — same formula as Turbo scanner
        tz_s = ""
        if not sig_df.empty and "sig_id" in sig_df.columns:
            tz_s = str(sig_df.iloc[i].get("sig_name", ""))
        is_bull_bar = not sig_df.empty and bool(sig_df.iloc[i].get("is_bull", False))
        sig_row = {
            # Backbone
            "conso_2809":  _b(combo_df, "conso_2809"),
            "tz_bull":     is_bull_bar,
            "bf_buy":      _b(ultraV2, "bf_buy"),
            # Volume / accum
            "abs_sig":     _b(vabs, "abs_sig"),
            "climb_sig":   _b(vabs, "climb_sig"),
            "load_sig":    _b(vabs, "load_sig"),
            "vbo_up":      _b(vabs, "vbo_up"),
            "ns":          _b(vabs, "ns"),
            "sq":          _b(vabs, "sq"),
            "sc":          _b(vabs, "sc"),
            "svs_2809":    _b(combo_df, "svs_2809"),
            "um_2809":     _b(combo_df, "um_2809"),
            "sig_l88":     _b(ultra260, "sig_l88"),
            "sig_260308":  _b(ultra260, "sig_260308"),
            # Breakout
            "fbo_bull":    _b(ultraV2, "fbo_bull"),
            "eb_bull":     _b(ultraV2, "eb_bull"),
            "ultra_3up":   _b(ultraV2, "ultra_3up"),
            "bo_up":       _b(wlnbb, "BO_UP"),
            "bx_up":       _b(wlnbb, "BX_UP"),
            "be_up":       _b(wlnbb, "BE_UP"),
            # Combo / momentum (+ stateful: cd/ca/cw/seq_bcont/va)
            "rocket":      _b(combo_df, "rocket"),
            "buy_2809":    _b(combo_df, "buy_2809"),
            "sig3g":       _b(combo_df, "sig3g"),
            "rtv":         _b(combo_df, "rtv"),
            "hilo_buy":    _b(combo_df, "hilo_buy"),
            "atr_brk":     _b(combo_df, "atr_brk"),
            "bb_brk":      _b(combo_df, "bb_brk"),
            "seq_bcont":   bool(seq_bcont_ser.iloc[i]),
            "va":          bool(va_ser.iloc[i]),
            # cd/ca/cw from tz_state + any B signal
            "cd": bool(int(tz_state_ser.iloc[i]) == 3 and
                       any(_b(b_sigs, f"b{n}") for n in range(1, 12))),
            "ca": bool(int(tz_state_ser.iloc[i]) == 2 and
                       any(_b(b_sigs, f"b{n}") for n in range(1, 12))),
            "cw": bool(int(tz_state_ser.iloc[i]) == 1 and
                       any(_b(b_sigs, f"b{n}") for n in range(1, 12))),
            # L-structure / trend
            "tz_sig":        tz_s,
            "tz_bull_flip":  bool(int(tz_state_ser.iloc[i]) == 3 and
                                  int(tz_state_prev.iloc[i]) != 3),
            "tz_attempt":    bool(int(tz_state_ser.iloc[i]) == 2 and
                                  int(tz_state_prev.iloc[i]) != 2),
            "tz_weak_bull":  bool(int(tz_state_ser.iloc[i]) == 1 and
                                  int(tz_state_prev.iloc[i]) == 0 and
                                  float(df["close"].iloc[i]) > float(df["open"].iloc[i])),
            "fri34":       _b(wlnbb, "FRI34"),
            "fri43":       _b(wlnbb, "FRI43"),
            "l34":         _b(wlnbb, "L34"),
            "l43":         _b(wlnbb, "L43"),
            "blue":        _b(wlnbb, "BLUE"),
            "cci_ready":   _b(wlnbb, "CCI_READY"),
            "fuchsia_rl":  _b(wlnbb, "FUCHSIA_RL"),
            # EMA cross / preup
            "preup89":     _b(combo_df, "preup89"),
            "preup3":      _b(combo_df, "preup3"),
            "preup2":      _b(combo_df, "preup2"),
            # G signals
            "g1":  _b(g_sigs, "g1"),
            "g2":  _b(g_sigs, "g2"),
            "g4":  _b(g_sigs, "g4"),
            "g6":  _b(g_sigs, "g6"),
            "g11": _b(g_sigs, "g11"),
            # Wick context
            "x2g_wick":  _b(wick, "x2g_wick"),
            "x2_wick":   _b(wick, "x2_wick"),
            "x1g_wick":  _b(wick, "x1g_wick"),
            "x1_wick":   _b(wick, "x1_wick"),
            "x3_wick":   _b(wick, "x3_wick"),
            "wick_bull": _b(wick, "WICK_BULL_CONFIRM"),
            # FLY context
            "fly_abcd": _b(fly_sigs, "fly_abcd"),
            "fly_cd":   _b(fly_sigs, "fly_cd"),
            "fly_bd":   _b(fly_sigs, "fly_bd"),
            "fly_ad":   _b(fly_sigs, "fly_ad"),
            # Vol spike context
            "vol_spike_10x": float(vol_ratio.iloc[i]) >= 10,
            # Composite setup signals
            "smx_sig":  bool(_gv("SM",  i)),
            "akan_sig": bool(_gv("A",   i)),
            "nnn_sig":  bool(_gv("N",   i)),
            "mx_sig":   bool(_gv("MX",  i)),
            "gog_sig":  bool(_gv("GOG_SCORE", i, 0) > 0),
        }
        # GOG per-bar data from engine
        gog_tier_val  = str(_gv("GOG_TIER",  i, ""))
        gog_score_val = float(_gv("GOG_SCORE", i, 0.0))
        setup_list    = str(_gv("SETUP",   i, "")).split() if _gv("SETUP", i, "") else []
        context_list  = str(_gv("CONTEXT", i, "")).split() if _gv("CONTEXT", i, "") else []
        turbo_score_val = _calc_turbo_score(sig_row)

        vol_bkt = ""
        if not wlnbb.empty and "vol_bucket" in wlnbb.columns:
            vol_bkt = str(wlnbb.iloc[i]["vol_bucket"])

        # RTB v4 per-bar
        rtb_phase_val      = ""
        rtb_total_val      = 0.0
        rtb_transition_val = ""
        rtb_build_val      = 0.0
        rtb_turn_val       = 0.0
        rtb_ready_val      = 0.0
        rtb_late_val       = 0.0
        rtb_bonus3_val     = 0.0
        dbg_context_ready_val        = False
        dbg_t4_ctx_val               = False
        dbg_t6_ctx_val               = False
        dbg_t4t6_activation_plus_val = False
        dbg_launch_cluster_count_val = 0
        dbg_pending_phase_val        = ""
        dbg_pending_phase_count_val  = 0
        if _rtb_ok:
            try:
                _sr = dict(sig_row,
                           close=float(row["close"]),
                           open=float(row["open"]),
                           high=float(row["high"]),
                           vol_bucket=vol_bkt)
                # history: most-recent-first (history[0] = 1 bar ago)
                _hist = list(reversed(_rtb_history[-5:]))
                _res  = _rtb_v4(_sr, _hist, _rtb_prev_phase, _rtb_prev_age,
                                _rtb_soft_streak, _rtb_pending_phase, _rtb_pending_count)
                rtb_phase_val      = _res["rtb_phase"]
                rtb_total_val      = round(float(_res["rtb_total"]), 1)
                rtb_transition_val = _res["rtb_transition"]
                rtb_build_val      = round(float(_res["rtb_build"]),  1)
                rtb_turn_val       = round(float(_res["rtb_turn"]),   1)
                rtb_ready_val      = round(float(_res["rtb_ready"]),  1)
                rtb_late_val       = round(float(_res["rtb_late"]),   1)
                rtb_bonus3_val     = round(float(_res["rtb_bonus3"]), 1)
                dbg_context_ready_val        = bool(_res["dbg_context_ready"])
                dbg_t4_ctx_val               = bool(_res["dbg_t4_ctx"])
                dbg_t6_ctx_val               = bool(_res["dbg_t6_ctx"])
                dbg_t4t6_activation_plus_val = bool(_res["dbg_t4t6_activation_plus"])
                dbg_launch_cluster_count_val = int(_res["dbg_launch_cluster_count"])
                dbg_pending_phase_val        = _res["dbg_pending_phase"]
                dbg_pending_phase_count_val  = int(_res["dbg_pending_phase_count"])
                _rtb_prev_phase    = rtb_phase_val
                _rtb_prev_age      = _res["rtb_phase_age"]
                _rtb_soft_streak   = _res["_soft_streak"]
                _rtb_pending_phase = _res["_pending_phase"]
                _rtb_pending_count = _res["_pending_phase_count"]
                _rtb_history.append(_sr)
            except Exception:
                pass

        result.append({
            "date":       date_val,
            "open":       float(row["open"]),
            "high":       float(row["high"]),
            "low":        float(row["low"]),
            "close":      float(row["close"]),
            "volume":     float(row["volume"]),
            "vol_bucket": vol_bkt,
            "tz":        tz,
            "l":         l_list,
            "f":         f_list,
            "fly":       fly_list,
            "g":         g_list,
            "b":         b_list,
            "combo":     combo_list,
            "vol":       vol_list,
            "vabs":      vabs_list,
            "wick":      wick_list,
            "setup":     setup_list,
            "context":   context_list,
            "gog_tier":  gog_tier_val,
            "gog_score": gog_score_val,
            # GOG boosted tiers (bool)
            "g1p":  int(_gv("G1P",i)), "g2p": int(_gv("G2P",i)), "g3p": int(_gv("G3P",i)),
            "g1l":  int(_gv("G1L",i)), "g2l": int(_gv("G2L",i)), "g3l": int(_gv("G3L",i)),
            "g1c":  int(_gv("G1C",i)), "g2c": int(_gv("G2C",i)), "g3c": int(_gv("G3C",i)),
            "gog1": int(_gv("GOG1",i)),"gog2":int(_gv("GOG2",i)),"gog3":int(_gv("GOG3",i)),
            # Raw supporting signals
            "raw_load":   int(_gv("LOAD",i)),       "raw_sq":     int(_gv("SQ",i)),
            "raw_w":      int(_gv("W",i)),           "raw_f8":     int(_gv("F8",i)),
            "raw_vbo_up": int(_gv("VBO_UP",i)),      "raw_be_up":  int(_gv("BE_UP",i)),
            "raw_bo_up":  int(_gv("BO_UP",i)),       "raw_bx_up":  int(_gv("BX_UP",i)),
            "raw_t10":    int(_gv("T10",i)),          "raw_t11":    int(_gv("T11",i)),
            "raw_t12":    int(_gv("T12",i)),          "raw_z10":    int(_gv("Z10",i)),
            "raw_z11":    int(_gv("Z11",i)),          "raw_z12":    int(_gv("Z12",i)),
            "raw_l34":    int(_gv("L34",i)),          "raw_l43":    int(_gv("L43",i)),
            "raw_l64":    int(_gv("L64",i)),          "raw_l22":    int(_gv("L22",i)),
            "raw_z4":     int(_gv("Z4",i)),           "raw_z6":     int(_gv("Z6",i)),
            "raw_z9":     int(_gv("Z9",i)),
            "raw_f3":     int(_gv("F3",i)),           "raw_f4":     int(_gv("F4",i)),
            "raw_f6":     int(_gv("F6",i)),           "raw_f11":    int(_gv("F11",i)),
            "raw_bf4":    int(_gv("BF4",i)),
            "raw_sig260308": int(_gv("SIG_260308",i)),
            "raw_l88":    int(_gv("L88",i)),          "raw_um":     int(_gv("UM",i)),
            "raw_svs_raw":int(_gv("SVS_RAW",i)),      "raw_cons":   int(_gv("CONS",i)),
            "raw_buy_here":   int(_gv("BUY_HERE",i)),
            "raw_atr_brk":    int(_gv("ATR_BREAKOUT",i)),
            "raw_bb_brk":     int(_gv("BOLL_BREAKOUT",i)),
            "raw_hilo_buy":   int(_gv("HILO_BUY",i)),
            "raw_rtv":    int(_gv("RTV",i)),
            "raw_three_g":int(_gv("THREE_G",i)),      "raw_rocket": int(_gv("ROCKET",i)),
            "all_signals": str(_gv("ALL_SIGNALS", i, "")),
            # Diagnostics
            "already_extended": int(_gv("already_extended_flag",i)),
            "pct_change_3d":    round(float(_gv("pct_change_3d",i,0)), 2),
            "pct_change_5d":    round(float(_gv("pct_change_5d",i,0)), 2),
            "pct_change_10d":   round(float(_gv("pct_change_10d",i,0)), 2),
            "pct_from_20d_high":  round(float(_gv("pct_from_20d_high",i,0)), 2),
            "pct_from_20d_low":   round(float(_gv("pct_from_20d_low",i,0)), 2),
            "distance_to_20d_high_pct": round(float(_gv("distance_to_20d_high_pct",i,0)), 2),
            "volume_ratio_20d": round(float(_gv("volume_ratio_20d",i,0)), 2),
            "dollar_volume":    round(float(_gv("dollar_volume",i,0)), 0),
            "gap_pct":          round(float(_gv("gap_pct",i,0)), 2),
            # Forward stats (note: gog_engine suffixes are '5d'/'10d')
            "fwd_close_1d":   _fv("fwd_close_1d",i),
            "fwd_close_3d":   _fv("fwd_close_3d",i),
            "fwd_close_5d":   _fv("fwd_close_5d",i),
            "fwd_close_10d":  _fv("fwd_close_10d",i),
            "max_high_5d_pct":  _fv("max_high_5d_pct",i),
            "max_high_10d_pct": _fv("max_high_10d_pct",i),
            "hit_5pct_5d":    int(_fv("hit_5pct_5d",i)  or 0),
            "hit_5pct_10d":   int(_fv("hit_5pct_10d",i) or 0),
            "hit_10pct_5d":   int(_fv("hit_10pct_5d",i)  or 0),
            "hit_10pct_10d":  int(_fv("hit_10pct_10d",i) or 0),
            "vbo_within_5":   int(_fv("vbo_within_5d",i)  or 0),
            "vbo_within_10":  int(_fv("vbo_within_10d",i) or 0),
            "bars_to_next_vbo": _fv("bars_to_next_vbo",i),
            "gog_within_5":   int(_fv("gog_within_5d",i)  or 0),
            "gog_within_10":  int(_fv("gog_within_10d",i) or 0),
            "bars_to_next_gog": _fv("bars_to_next_gog",i),
            "ret_to_next_vbo_close": _fv("ret_to_next_vbo_close",i),
            "ret_to_next_vbo_high":  _fv("ret_to_next_vbo_high",i),
            "ret_to_next_gog_close": _fv("ret_to_next_gog_close",i),
            "ret_to_next_gog_high":  _fv("ret_to_next_gog_high",i),
            "ultra":          ultra_list,
            "turbo_score":    turbo_score_val,
            "rtb_phase":      rtb_phase_val,
            "rtb_total":      rtb_total_val,
            "rtb_transition": rtb_transition_val,
            "rtb_build":      rtb_build_val,
            "rtb_turn":       rtb_turn_val,
            "rtb_ready":      rtb_ready_val,
            "rtb_late":       rtb_late_val,
            "rtb_bonus3":     rtb_bonus3_val,
            "dbg_context_ready":        dbg_context_ready_val,
            "dbg_t4_ctx":               dbg_t4_ctx_val,
            "dbg_t6_ctx":               dbg_t6_ctx_val,
            "dbg_t4t6_activation_plus": dbg_t4t6_activation_plus_val,
            "dbg_launch_cluster_count": dbg_launch_cluster_count_val,
            "dbg_pending_phase":        dbg_pending_phase_val,
            "dbg_pending_phase_count":  dbg_pending_phase_count_val,
        })

    return result


# ── Stock Stat — bulk per-bar signal CSV for entire universe ──────────────────

def run_stock_stat(tf: str = "1d", universe: str = "sp500", bars: int = 60):
    import csv, time, collections
    from scanner import get_universe_tickers

    t0 = time.time()
    _stock_stat_state.update(
        running=True, done=0, total=0, error=None,
        output_path=None, output_size=0, tf=tf, universe=universe, elapsed=0.0,
        validation=None,
    )
    _PREUP = {"P2", "P3", "P50", "P89"}

    # ── GOG tier priority for the CSV (user-specified order) ──────────────────
    _GOG_PRIO_KEYS = [
        ("G1P","g1p"), ("G1C","g1c"), ("G1L","g1l"), ("GOG1","gog1"),
        ("G2P","g2p"), ("G2C","g2c"), ("G2L","g2l"), ("GOG2","gog2"),
        ("G3P","g3p"), ("G3C","g3c"), ("G3L","g3l"), ("GOG3","gog3"),
    ]
    # GOG base scores for SIGNAL_SCORE (separate from gog_engine GOG_SCORE)
    _GOG_BASE_SCORES = {
        "G1P":100,"G1C":88,"G1L":82,"GOG1":70,
        "G2P":62, "G2C":55,"G2L":45,"GOG2":35,
        "G3P":60, "G3C":52,"G3L":48,"GOG3":42,
    }

    def _signal_score(b):
        """Compute SIGNAL_SCORE, RESEARCH_SCORE, SIGNAL_BUCKET, REGIME for a bar dict."""
        setup = b.get("setup", [])
        if isinstance(setup, str):
            setup = setup.split()
        setup_set = set(setup)

        ctx = b.get("context", [])
        if isinstance(ctx, str):
            ctx = ctx.split()
        ctx_set = set(ctx)

        # 7.1 GOG Base — new priority/scores (separate from engine GOG_SCORE)
        gog_tier_s = ""
        for t, k in _GOG_PRIO_KEYS:
            if b.get(k):
                gog_tier_s = t
                break
        gog_base = _GOG_BASE_SCORES.get(gog_tier_s, 0)

        # 7.2 Premium Context
        ldp = 1 if "LDP" in ctx_set else 0
        lrp = 1 if "LRP" in ctx_set else 0
        prem_raw = 22 * ldp + 26 * lrp
        premium_score = min(prem_raw, 35) if (ldp and lrp) else prem_raw

        # 7.3 Load Family Context (max of LD/LDS/LDC; LDP already in premium)
        load_score = max(
            8  if "LD"  in ctx_set else 0,
            11 if "LDS" in ctx_set else 0,
            16 if "LDC" in ctx_set else 0,
        )

        # 7.4 L-Reclaim (skip LRC if LRP already scored in premium)
        lrc = 1 if "LRC" in ctx_set else 0
        l_reclaim_score = 0 if lrp else 12 * lrc

        # 7.5 Compression Context
        wrc = 1 if "WRC" in ctx_set else 0
        f8c = 1 if "F8C" in ctx_set else 0
        comp_raw = 10 * wrc + 12 * f8c
        comp_score = min(comp_raw, 18) if (wrc and f8c) else comp_raw

        # 7.6 SQ/BCT/SVS
        sqb = 1 if "SQB" in ctx_set else 0
        bct = 1 if "BCT" in ctx_set else 0
        svs = 1 if "SVS" in ctx_set else 0
        sq_bct_score = (18 + 6 * svs) if bct else (14 * sqb + 6 * svs)

        # 7.7 Base Setup Family
        ha = 1 if "A"  in setup_set else 0
        hs = 1 if "SM" in setup_set else 0
        hn = 1 if "N"  in setup_set else 0
        hm = 1 if "MX" in setup_set else 0
        if not gog_base:
            pts = 10*ha + 10*hs + 4*hn + 5*hm
            if ha and hs and hn and hm: pts += 12
            elif (ha or hs) and (hn or hm): pts += 8
            setup_score = pts
        else:
            setup_score = min(8, 10*ha + 10*hs + 4*hn + 5*hm)

        # 7.8 Raw Supporting Signal Score (capped at 25)
        def _rb(k): return 1 if b.get(k) else 0
        raw = (
              6*_rb("raw_load")   + 5*_rb("raw_sq")      + 3*_rb("raw_w")        + 5*_rb("raw_f8")
            + 4*_rb("raw_l34")   + 3*_rb("raw_l43")     + 3*_rb("raw_l64")      + 5*_rb("raw_l22")
            + 8*_rb("raw_vbo_up")+ 5*_rb("raw_bo_up")   + 6*_rb("raw_be_up")    + 4*_rb("raw_bx_up")
            + 5*_rb("raw_f3")    + 4*_rb("raw_f4")      + 6*_rb("raw_f6")       + 4*_rb("raw_f11")
            + 2*_rb("raw_t10")   + 2*_rb("raw_t11")     + 2*_rb("raw_t12")
            + 4*_rb("raw_z10")   + 4*_rb("raw_z11")     + 3*_rb("raw_z12")
            + 3*_rb("raw_z4")    + 3*_rb("raw_z6")      + 3*_rb("raw_z9")
            + 5*_rb("raw_bf4")   + 7*_rb("raw_sig260308")+ 8*_rb("raw_l88")     + 4*_rb("raw_um")
            + 5*_rb("raw_svs_raw")+ 8*_rb("raw_buy_here")+ 7*_rb("raw_atr_brk") + 7*_rb("raw_bb_brk")
            + 4*_rb("raw_hilo_buy")+ 3*_rb("raw_rtv")   + 6*_rb("raw_three_g")  + 8*_rb("raw_rocket")
        )
        raw_support_score = min(raw, 25)

        # 7.9 Research Forward Score (future data — separate column only)
        btv = b.get("bars_to_next_vbo")
        btg = b.get("bars_to_next_gog")
        vbo_w5  = btv is not None and 1 <= btv <= 5
        vbo_w10 = btv is not None and 1 <= btv <= 10
        gog_w5  = btg is not None and 1 <= btg <= 5
        gog_w10 = btg is not None and 1 <= btg <= 10
        fwd_pts = 0
        if vbo_w5:    fwd_pts += 10
        elif vbo_w10: fwd_pts += 6
        if gog_w5:    fwd_pts += 12
        elif gog_w10: fwd_pts += 8

        # 7.10 Risk Penalty
        ext = int(bool(b.get("already_extended")))
        risk_penalty = 15 * ext

        # 7.11 Final
        total = (gog_base + premium_score + load_score + l_reclaim_score
                 + comp_score + sq_bct_score + setup_score + raw_support_score
                 - risk_penalty)
        sig_score = max(0, min(160, int(round(total))))
        research_score = sig_score + fwd_pts

        # Regime
        if ext and gog_tier_s in ("G1P","G1C","G1L"):
            regime = "PARABOLIC_GOG"
        elif not ext and gog_tier_s:
            regime = "CLEAN_GOG"
        elif not gog_tier_s and (premium_score or load_score or comp_score):
            regime = "WATCH_CONTEXT"
        else:
            regime = ""

        # 8. Bucket
        if sig_score >= 120:   bucket = "ELITE"
        elif sig_score >= 100: bucket = "A_PLUS"
        elif sig_score >= 80:  bucket = "A"
        elif sig_score >= 60:  bucket = "B"
        elif sig_score >= 40:  bucket = "WATCH"
        elif sig_score >= 20:  bucket = "LOW_WATCH"
        else:                  bucket = "NO_SIGNAL"
        if ext and sig_score >= 80:
            bucket += "_EXTENDED"

        return sig_score, research_score, bucket, regime, gog_tier_s, vbo_w5, vbo_w10, gog_w5, gog_w10

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _j(lst): return " ".join(lst) if lst else ""
    def _ctx(b, tok): return 1 if tok in b.get("context", []) else 0
    def _fmt(v): return "" if v is None else v

    try:
        tickers = get_universe_tickers(universe)
        _stock_stat_state["total"] = len(tickers)

        os.makedirs("stock_stat_output", exist_ok=True)
        out_path = f"stock_stat_output/stock_stat_{universe}_{tf}.csv"

        headers = [
            "ticker", "date", "open", "high", "low", "close", "volume",
            "vol_bucket", "turbo_score",
            "rtb_phase", "rtb_total", "rtb_transition",
            "rtb_build", "rtb_turn", "rtb_ready", "rtb_late", "rtb_bonus3",
            "dbg_context_ready", "dbg_t4_ctx", "dbg_t6_ctx", "dbg_t4t6_activation_plus",
            "dbg_launch_cluster_count", "dbg_pending_phase", "dbg_pending_phase_count",
            "Z", "T", "L", "F", "FLY", "G", "B", "Combo", "ULT", "VOL", "VABS", "WICK",
            # ── Text Summary ──────────────────────────────────────────────────
            "SETUP", "CONTEXT", "GOG_TIER", "ALL_SIGNALS",
            # ── Scoring ───────────────────────────────────────────────────────
            "GOG_SCORE",
            "SIGNAL_SCORE", "SIGNAL_BUCKET", "RESEARCH_SCORE", "REGIME",
            # ── Base Setup Booleans ───────────────────────────────────────────
            "A", "SM", "N", "MX",
            # ── Raw GOG ──────────────────────────────────────────────────────
            "GOG1", "GOG2", "GOG3",
            # ── Boosted GOG ──────────────────────────────────────────────────
            "G1P", "G2P", "G3P", "G1L", "G2L", "G3L", "G1C", "G2C", "G3C",
            # ── Context Signals ───────────────────────────────────────────────
            "LD", "LDS", "LDC", "LDP", "LRC", "LRP", "WRC", "F8C", "SQB", "BCT", "SVS",
            # ── Raw / Supporting ──────────────────────────────────────────────
            "LOAD", "SQ", "W", "F8",
            "L34", "L43", "L64", "L22",
            "VBO_UP", "BO_UP", "BE_UP", "BX_UP",
            "T10", "T11", "T12",
            "Z10", "Z11", "Z12", "Z4", "Z6", "Z9",
            "F3", "F4", "F6", "F11",
            "4BF", "SIG_260308", "L88", "UM", "SVS_RAW", "CONS",
            "BUY_HERE", "ATR_BREAKOUT", "BOLL_BREAKOUT", "HILO_BUY",
            "RTV", "THREE_G", "ROCKET",
            # ── Diagnostics ───────────────────────────────────────────────────
            "ALREADY_EXTENDED",
            "PCT_CHANGE_3D", "PCT_CHANGE_5D", "PCT_CHANGE_10D",
            "PCT_FROM_20D_HIGH", "PCT_FROM_20D_LOW",
            "DISTANCE_TO_20D_HIGH_PCT",
            "VOLUME_RATIO_20D", "DOLLAR_VOLUME", "GAP_PCT",
            # ── Forward Returns ───────────────────────────────────────────────
            "RET_1D", "RET_3D", "RET_5D", "RET_10D",
            "MAX_RET_5D", "MAX_RET_10D",
            "HIT_5D_5PCT", "HIT_5D_10PCT", "HIT_10D_5PCT", "HIT_10D_10PCT",
            # ── Next Event ────────────────────────────────────────────────────
            "BARS_TO_VBO", "BARS_TO_GOG",
            "VBO_W5", "VBO_W10", "GOG_W5", "GOG_W10",
            "RET_TO_NEXT_VBO_CLOSE", "RET_TO_NEXT_VBO_HIGH",
            "RET_TO_NEXT_GOG_CLOSE", "RET_TO_NEXT_GOG_HIGH",
        ]

        # Validation accumulators
        _val_rows       = 0
        _val_tickers    = set()
        _val_tier_cnt   = collections.Counter()
        _val_bucket_cnt = collections.Counter()
        _val_top20      = []   # (sig_score, ticker, date, gog_tier)
        _val_setup_mismatch = 0
        _val_window_mismatch = 0

        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            wr = csv.writer(fh)
            wr.writerow(headers)

            for idx, ticker in enumerate(tickers):
                try:
                    bd = api_bar_signals(ticker, tf, bars)
                    for b in bd:
                        tz = b.get("tz", "")

                        # ── Setup tokens (exact token match, not substring) ──
                        setup_lst = b.get("setup", [])
                        if isinstance(setup_lst, str):
                            setup_lst = setup_lst.split()
                        setup_set = set(setup_lst)
                        setup_str = " ".join(setup_lst)

                        # ── Context tokens ──────────────────────────────────
                        ctx_lst = b.get("context", [])
                        if isinstance(ctx_lst, str):
                            ctx_lst = ctx_lst.split()
                        ctx_set = set(ctx_lst)
                        ctx_str = " ".join(ctx_lst)

                        # ── A/SM/N/MX — derive from setup tokens ────────────
                        col_A  = 1 if "A"  in setup_set else 0
                        col_SM = 1 if "SM" in setup_set else 0
                        col_N  = 1 if "N"  in setup_set else 0
                        col_MX = 1 if "MX" in setup_set else 0

                        # ── GOG_TIER with user priority order ───────────────
                        gog_tier_csv = ""
                        for t_name, t_key in _GOG_PRIO_KEYS:
                            if b.get(t_key):
                                gog_tier_csv = t_name
                                break

                        # ── ALL_SIGNALS text ────────────────────────────────
                        all_sig_parts = [p for p in [setup_str, gog_tier_csv, ctx_str] if p]
                        all_signals_str = b.get("all_signals","") or " ".join(all_sig_parts)

                        # ── BARS_TO_VBO / GOG ───────────────────────────────
                        btv = b.get("bars_to_next_vbo")
                        btg = b.get("bars_to_next_gog")

                        # ── VBO_W5/W10/GOG_W5/W10 from bars_to (correct) ───
                        vbo_w5  = 1 if (btv is not None and 0 <= btv <= 5)  else 0
                        vbo_w10 = 1 if (btv is not None and 0 <= btv <= 10) else 0
                        gog_w5  = 1 if (btg is not None and 0 <= btg <= 5)  else 0
                        gog_w10 = 1 if (btg is not None and 0 <= btg <= 10) else 0

                        # ── SIGNAL_SCORE ────────────────────────────────────
                        (sig_score, research_score, bucket, regime,
                         _gts, _vw5, _vw10, _gw5, _gw10) = _signal_score(b)

                        # ── Validation checks ───────────────────────────────
                        _val_rows += 1
                        _val_tickers.add(ticker)
                        _val_tier_cnt[gog_tier_csv or "NONE"] += 1
                        _val_bucket_cnt[bucket] += 1
                        _val_top20.append((sig_score, ticker, b.get("date",""), gog_tier_csv))
                        if len(_val_top20) > 20:
                            _val_top20.sort(reverse=True)
                            _val_top20 = _val_top20[:20]

                        # Setup mismatch: boolean vs token
                        bool_a  = 1 if b.get("akan_sig") else 0
                        bool_sm = 1 if b.get("smx_sig")  else 0
                        bool_n  = 1 if b.get("nnn_sig")  else 0
                        bool_mx = 1 if b.get("mx_sig")   else 0
                        if (bool_a!=col_A or bool_sm!=col_SM or
                                bool_n!=col_N or bool_mx!=col_MX):
                            _val_setup_mismatch += 1

                        # VBO/GOG window mismatch: boolean vs bars_to
                        vbo_w10_check = b.get("vbo_within_10", 0)
                        gog_w10_check = b.get("gog_within_10", 0)
                        if vbo_w10_check != vbo_w10 or gog_w10_check != gog_w10:
                            _val_window_mismatch += 1

                        wr.writerow([
                            ticker,
                            b.get("date", ""),
                            round(b.get("open", 0), 4),
                            round(b.get("high", 0), 4),
                            round(b.get("low",  0), 4),
                            round(b.get("close",0), 4),
                            round(b.get("volume",0), 0),
                            b.get("vol_bucket", ""),
                            b.get("turbo_score", 0),
                            b.get("rtb_phase", ""),
                            b.get("rtb_total", 0),
                            b.get("rtb_transition", ""),
                            b.get("rtb_build", 0),
                            b.get("rtb_turn",  0),
                            b.get("rtb_ready", 0),
                            b.get("rtb_late",  0),
                            b.get("rtb_bonus3",0),
                            1 if b.get("dbg_context_ready") else 0,
                            1 if b.get("dbg_t4_ctx")        else 0,
                            1 if b.get("dbg_t6_ctx")        else 0,
                            1 if b.get("dbg_t4t6_activation_plus") else 0,
                            b.get("dbg_launch_cluster_count", 0),
                            b.get("dbg_pending_phase", ""),
                            b.get("dbg_pending_phase_count", 0),
                            tz if tz.startswith("Z") else "",
                            tz if tz.startswith("T") else "",
                            _j(b.get("l", [])),
                            _j(b.get("f", [])),
                            _j(b.get("fly", [])),
                            _j(b.get("g", [])),
                            _j(b.get("b", [])),
                            _j([s for s in b.get("combo", []) if s not in _PREUP]),
                            _j(b.get("ultra", [])),
                            _j(b.get("vol", [])),
                            _j(b.get("vabs", [])),
                            _j(b.get("wick", [])),
                            # ── Text Summary ──────────────────────────────
                            setup_str,
                            ctx_str,
                            gog_tier_csv,
                            all_signals_str,
                            # ── Scoring ───────────────────────────────────
                            b.get("gog_score", 0),
                            sig_score,
                            bucket,
                            research_score,
                            regime,
                            # ── Base Setup Booleans ───────────────────────
                            col_A, col_SM, col_N, col_MX,
                            # ── Raw GOG ───────────────────────────────────
                            b.get("gog1",0), b.get("gog2",0), b.get("gog3",0),
                            # ── Boosted GOG ───────────────────────────────
                            b.get("g1p",0), b.get("g2p",0), b.get("g3p",0),
                            b.get("g1l",0), b.get("g2l",0), b.get("g3l",0),
                            b.get("g1c",0), b.get("g2c",0), b.get("g3c",0),
                            # ── Context Signals ───────────────────────────
                            1 if "LD"  in ctx_set else 0,
                            1 if "LDS" in ctx_set else 0,
                            1 if "LDC" in ctx_set else 0,
                            1 if "LDP" in ctx_set else 0,
                            1 if "LRC" in ctx_set else 0,
                            1 if "LRP" in ctx_set else 0,
                            1 if "WRC" in ctx_set else 0,
                            1 if "F8C" in ctx_set else 0,
                            1 if "SQB" in ctx_set else 0,
                            1 if "BCT" in ctx_set else 0,
                            1 if "SVS" in ctx_set else 0,
                            # ── Raw / Supporting ──────────────────────────
                            b.get("raw_load",0),   b.get("raw_sq",0),
                            b.get("raw_w",0),       b.get("raw_f8",0),
                            b.get("raw_l34",0),     b.get("raw_l43",0),
                            b.get("raw_l64",0),     b.get("raw_l22",0),
                            b.get("raw_vbo_up",0),  b.get("raw_bo_up",0),
                            b.get("raw_be_up",0),   b.get("raw_bx_up",0),
                            b.get("raw_t10",0),     b.get("raw_t11",0),
                            b.get("raw_t12",0),
                            b.get("raw_z10",0),     b.get("raw_z11",0),
                            b.get("raw_z12",0),     b.get("raw_z4",0),
                            b.get("raw_z6",0),      b.get("raw_z9",0),
                            b.get("raw_f3",0),      b.get("raw_f4",0),
                            b.get("raw_f6",0),      b.get("raw_f11",0),
                            b.get("raw_bf4",0),     b.get("raw_sig260308",0),
                            b.get("raw_l88",0),     b.get("raw_um",0),
                            b.get("raw_svs_raw",0), b.get("raw_cons",0),
                            b.get("raw_buy_here",0),b.get("raw_atr_brk",0),
                            b.get("raw_bb_brk",0),  b.get("raw_hilo_buy",0),
                            b.get("raw_rtv",0),     b.get("raw_three_g",0),
                            b.get("raw_rocket",0),
                            # ── Diagnostics ───────────────────────────────
                            b.get("already_extended", 0),
                            _fmt(b.get("pct_change_3d","")),
                            _fmt(b.get("pct_change_5d","")),
                            _fmt(b.get("pct_change_10d","")),
                            _fmt(b.get("pct_from_20d_high","")),
                            _fmt(b.get("pct_from_20d_low","")),
                            _fmt(b.get("distance_to_20d_high_pct","")),
                            _fmt(b.get("volume_ratio_20d","")),
                            _fmt(b.get("dollar_volume","")),
                            _fmt(b.get("gap_pct","")),
                            # ── Forward Returns ───────────────────────────
                            _fmt(b.get("fwd_close_1d")),
                            _fmt(b.get("fwd_close_3d")),
                            _fmt(b.get("fwd_close_5d")),
                            _fmt(b.get("fwd_close_10d")),
                            _fmt(b.get("max_high_5d_pct")),
                            _fmt(b.get("max_high_10d_pct")),
                            b.get("hit_5pct_5d",0),    # HIT_5D_5PCT
                            b.get("hit_10pct_5d",0),   # HIT_5D_10PCT
                            b.get("hit_5pct_10d",0),   # HIT_10D_5PCT
                            b.get("hit_10pct_10d",0),  # HIT_10D_10PCT
                            # ── Next Event ────────────────────────────────
                            _fmt(btv),
                            _fmt(btg),
                            vbo_w5, vbo_w10,
                            gog_w5, gog_w10,
                            _fmt(b.get("ret_to_next_vbo_close")),
                            _fmt(b.get("ret_to_next_vbo_high")),
                            _fmt(b.get("ret_to_next_gog_close")),
                            _fmt(b.get("ret_to_next_gog_high")),
                        ])
                except Exception:
                    pass
                _stock_stat_state["done"] = idx + 1
                _stock_stat_state["elapsed"] = round(time.time() - t0, 1)

        fsize = os.path.getsize(out_path)

        # ── Validation Summary ────────────────────────────────────────────────
        _val_top20.sort(reverse=True)
        validation = {
            "total_rows":         _val_rows,
            "ticker_count":       len(_val_tickers),
            "by_gog_tier":        dict(_val_tier_cnt.most_common()),
            "by_signal_bucket":   dict(_val_bucket_cnt.most_common()),
            "top20_by_score": [
                {"score": s, "ticker": t, "date": d, "gog_tier": g}
                for s, t, d, g in _val_top20
            ],
            "setup_bool_token_mismatches": _val_setup_mismatch,
            "window_flag_mismatches":      _val_window_mismatch,
        }

        _stock_stat_state.update(
            running=False, output_path=out_path,
            output_size=fsize, elapsed=round(time.time() - t0, 1),
            validation=validation,
        )
    except Exception as e:
        _stock_stat_state.update(
            running=False, error=str(e),
            elapsed=round(time.time() - t0, 1)
        )


@app.post("/api/stock-stat/trigger")
def api_stock_stat_trigger(
    background_tasks: BackgroundTasks,
    tf: str = "1d", universe: str = "sp500", bars: int = 60,
):
    if _stock_stat_state["running"]:
        raise HTTPException(400, "Stock Stat scan already running")
    background_tasks.add_task(run_stock_stat, tf, universe, bars)
    return {"ok": True}


@app.get("/api/stock-stat/status")
def api_stock_stat_status():
    return _stock_stat_state


@app.get("/api/stock-stat/download")
def api_stock_stat_download():
    path = _stock_stat_state.get("output_path")
    if not path or not os.path.exists(path):
        raise HTTPException(404, "No output file — run a scan first")
    return FileResponse(path, media_type="text/csv", filename=os.path.basename(path))


# ── Sector Analysis ────────────────────────────────────────────────────────────
from sector_engine import (
    get_sector_overview,
    get_sector_detail,
    get_sector_rrg,
    get_sector_heatmap,
    get_macro_matrix,
)


def _sector_err(exc: Exception) -> dict:
    """Stable error envelope so sector endpoints never return raw 500 text."""
    return {
        "ok": False,
        "last_updated": round(__import__("time").time()),
        "data": None,
        "errors": [str(exc)],
    }


# Primary routes — /api/sectors/ (plural)
@app.get("/api/sectors/overview")
def api_sectors_overview():
    try:
        return get_sector_overview()
    except Exception as exc:
        return _sector_err(exc)


@app.get("/api/sectors/rrg")
def api_sectors_rrg(trail: int = 12):
    try:
        return get_sector_rrg(trail=trail)
    except Exception as exc:
        return _sector_err(exc)


@app.get("/api/sectors/heatmap")
def api_sectors_heatmap(metric: str = "return_1d"):
    try:
        return get_sector_heatmap(metric)
    except Exception as exc:
        return _sector_err(exc)


@app.get("/api/sectors/macro")
def api_sectors_macro():
    try:
        return get_macro_matrix()
    except Exception as exc:
        return _sector_err(exc)


# Must be registered AFTER the fixed-path routes above to avoid shadowing them
@app.get("/api/sectors/{etf}")
def api_sectors_detail(etf: str):
    try:
        return get_sector_detail(etf)
    except Exception as exc:
        return _sector_err(exc)


# Backward-compatible aliases — /api/sector/ (singular, kept for any existing callers)
@app.get("/api/sector/overview")
def api_sector_overview_alias():
    return api_sectors_overview()


@app.get("/api/sector/rrg")
def api_sector_rrg_alias(trail: int = 12):
    return api_sectors_rrg(trail=trail)


@app.get("/api/sector/heatmap")
def api_sector_heatmap_alias(metric: str = "return_1d"):
    return api_sectors_heatmap(metric=metric)


@app.get("/api/sector/detail/{ticker}")
def api_sector_detail_alias(ticker: str):
    return api_sectors_detail(ticker)


_static = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_static):
    app.mount("/", StaticFiles(directory=_static, html=True), name="static")