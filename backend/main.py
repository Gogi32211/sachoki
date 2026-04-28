"""
main.py — FastAPI app + APScheduler + all API routes.
"""
from __future__ import annotations
import os
import logging
import concurrent.futures
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
):
    from turbo_engine import run_turbo_scan, get_turbo_progress
    if get_turbo_progress().get("running"):
        raise HTTPException(status_code=409, detail="Scan already running")
    background_tasks.add_task(run_turbo_scan, tf, universe, 8, lookback_n, partial_day, min_volume)
    return {"status": "turbo scan started", "tf": tf, "universe": universe, "lookback_n": lookback_n, "partial_day": partial_day, "min_volume": min_volume}


@app.get("/api/turbo-scan/status")
def api_turbo_scan_status():
    from turbo_engine import get_turbo_progress
    return get_turbo_progress()


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
    signals: str = "tz_bull,best_sig,vbo_up",
    combo: bool = False,
    min_n: int = 5,
):
    from signal_stats_engine import run_signal_stats
    sig_list = [s.strip() for s in signals.split(",") if s.strip()]
    return run_signal_stats(ticker.upper(), tf, sig_list, combo=combo, min_n=min_n)


# ── Pooled signal stats (SP500 aggregate) ─────────────────────────────────────
_SS_POOLED: dict = {}  # key: f"{universe}_{tf}"


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
def api_admin_scan_start(background_tasks: BackgroundTasks, tf: str = "1d", universe: str = "sp500"):
    from turbo_engine import run_turbo_scan, get_turbo_progress
    if get_turbo_progress().get("running"):
        raise HTTPException(status_code=409, detail="Scan already running")
    background_tasks.add_task(run_turbo_scan, tf, universe)
    return {"ok": True, "tf": tf, "universe": universe}


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
        }
        turbo_score_val = _calc_turbo_score(sig_row)

        vol_bkt = ""
        if not wlnbb.empty and "vol_bucket" in wlnbb.columns:
            vol_bkt = str(wlnbb.iloc[i]["vol_bucket"])

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
            "ultra":       ultra_list,
            "turbo_score": turbo_score_val,
        })

    return result


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