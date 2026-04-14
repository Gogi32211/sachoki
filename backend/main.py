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

    # Auto-build pooled stats for sp500 1d on startup if not present
    try:
        from pooled_stats import get_pooled_status, build_pooled_stats, get_pooled_state
        status = get_pooled_status("sp500", "1d")
        if not status.get("available") and not get_pooled_state().get("running"):
            import threading
            log.info("Pooled stats not found — auto-building sp500 1d in background")
            threading.Thread(
                target=build_pooled_stats,
                args=("sp500", "1d", 3, 2000),  # 3 workers (was 6) to limit RAM
                daemon=True,
            ).start()
    except Exception as exc:
        log.warning("Auto-build pooled stats failed: %s", exc)

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


@app.get("/api/config")
def api_config():
    from data_polygon import polygon_available
    return {"massive_api_ready": polygon_available()}


# ── Utilities ─────────────────────────────────────────────────────────────────

def _normalise_date(idx) -> list[str]:
    """Convert a DataFrame index to clean YYYY-MM-DD strings."""
    try:
        return list(idx.strftime("%Y-%m-%d"))
    except AttributeError:
        return [str(v)[:10] for v in idx]


def _df_to_records(df) -> list[dict]:
    """Convert DataFrame to JSON-safe list of dicts with a 'date' column."""
    idx = df.index
    # Detect intraday: DatetimeIndex where any bar has a non-midnight time
    is_intraday = False
    try:
        is_intraday = hasattr(idx, 'hour') and bool(idx.minute.any() or idx.hour.any())
    except Exception:
        pass

    if is_intraday:
        # Preserve full "YYYY-MM-DD HH:MM:SS" — strip tz so JSON is clean
        try:
            tz_naive = idx.tz_localize(None) if idx.tz is None else idx.tz_convert('UTC').tz_localize(None)
            dates = list(tz_naive.strftime('%Y-%m-%d %H:%M:%S'))
        except Exception:
            dates = [str(v)[:19].replace('T', ' ') for v in idx]
    else:
        dates = _normalise_date(idx)

    df = df.copy()
    df.index = dates
    df.index.name = "date"
    records = df.reset_index()
    first = records.columns[0]
    if first != "date":
        records = records.rename(columns={first: "date"})
    if not is_intraday:
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


@app.get("/api/pooled-predict/{ticker}")
def api_pooled_predict(ticker: str, tf: str = "1d", universe: str = "sp500"):
    """Query pooled cross-universe stats for the ticker's current T/Z + L sequences."""
    try:
        from pooled_stats import get_pooled_predict, get_pooled_status
        from signal_engine import compute_signals
        from wlnbb_engine import compute_wlnbb

        status = get_pooled_status(universe, tf)
        if not status.get("available"):
            return {"error": "not_built", "status": status,
                    "tz_3bar": {"total_matches": 0, "top_outcomes": []},
                    "tz_2bar": {"total_matches": 0, "top_outcomes": []},
                    "l_3bar":  {"total_matches": 0, "top_outcomes": []},
                    "l_2bar":  {"total_matches": 0, "top_outcomes": []}}

        df    = fetch_ohlcv(ticker, interval=tf, bars=200)
        sigs  = compute_signals(df)
        wlnbb = compute_wlnbb(df)

        sig_ids  = sigs["sig_id"].values
        l_combos = wlnbb["l_combo"].values

        sig3 = tuple(int(s) for s in sig_ids[-3:])
        sig2 = tuple(int(s) for s in sig_ids[-2:])
        l3   = tuple(str(s) for s in l_combos[-3:])
        l2   = tuple(str(s) for s in l_combos[-2:])

        return get_pooled_predict(sig3, sig2, l3, l2, universe, tf)
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
        raise HTTPException(status_code=409, detail="Build already running")
    background_tasks.add_task(build_pooled_stats, universe, interval, 6, max_tickers)
    return {"status": "started", "universe": universe, "interval": interval, "max_tickers": max_tickers}


@app.get("/api/pooled-stats/status")
def api_pooled_stats_status(universe: str = "sp500", interval: str = "1d"):
    from pooled_stats import get_pooled_state, get_pooled_status
    return {
        "job":  get_pooled_state(),
        "data": get_pooled_status(universe, interval),
    }


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
):
    from turbo_engine import get_turbo_results, get_last_turbo_scan_time
    results   = get_turbo_results(limit=limit, min_score=min_score, direction=direction,
                                  tf=tf, universe=universe,
                                  price_min=price_min, price_max=price_max,
                                  rsi_min=rsi_min, rsi_max=rsi_max,
                                  cci_min=cci_min, cci_max=cci_max)
    last_time = get_last_turbo_scan_time(tf=tf, universe=universe)
    return {"results": results, "last_scan": last_time}


@app.post("/api/turbo-scan/trigger")
def api_turbo_scan_trigger(
    background_tasks: BackgroundTasks,
    tf: str = "1d",
    universe: str = "sp500",
    lookback_n: int = 5,
    partial_day: bool = False,
):
    from turbo_engine import run_turbo_scan, get_turbo_progress
    if get_turbo_progress().get("running"):
        raise HTTPException(status_code=409, detail="Scan already running")
    background_tasks.add_task(run_turbo_scan, tf, universe, 8, lookback_n, partial_day)
    return {"status": "turbo scan started", "tf": tf, "universe": universe, "lookback_n": lookback_n, "partial_day": partial_day}


@app.get("/api/turbo-scan/status")
def api_turbo_scan_status():
    from turbo_engine import get_turbo_progress
    return get_turbo_progress()


@app.post("/api/turbo-scan/reset")
def api_turbo_scan_reset():
    from turbo_engine import _turbo_state
    _turbo_state["running"] = False
    return {"ok": True, "message": "Scan state reset"}


# ── Signal correlation matrix ─────────────────────────────────────────────────
_CORR_SIGS = [
    "best_sig", "vbo_up", "ns", "abs_sig", "load_sig",
    "wyk_spring", "wyk_sos", "wyk_lps",
    "d_spring", "d_strong_bull", "d_blast_bull", "d_absorb_bull",
    "rocket", "buy_2809", "sig_l88",
    "fri34", "fri43", "l34", "preup66", "preup55",
    "rs_strong", "tz_bull_flip", "tz_attempt",
    "g1", "g2", "b10", "b1", "va", "seq_bcont",
]

@app.get("/api/signal-correlation")
def api_signal_correlation(tf: str = "1d", universe: str = "sp500", min_pct: float = 15.0):
    """
    Compute pairwise signal co-occurrence from the latest turbo scan results.
    Uses get_turbo_results (limit=10000, no score/direction filter) so it
    reads data exactly the same way as the TURBO tab.
    """
    from turbo_engine import get_turbo_results, _TURBO_COLS as _tc
    rows = get_turbo_results(tf=tf, universe=universe, limit=10000, min_score=0, direction="all")
    n = len(rows)
    if n == 0:
        return {"pairs": [], "signal_counts": {}, "n_tickers": 0}

    valid = [s for s in _CORR_SIGS if s in _tc]
    counts = {s: sum(1 for r in rows if r.get(s)) for s in valid}

    pairs = []
    for i, a in enumerate(valid):
        for b in valid[i+1:]:
            if counts[a] == 0 or counts[b] == 0:
                continue
            both = sum(1 for r in rows if r.get(a) and r.get(b))
            if both == 0:
                continue
            pct_a = round(both / counts[a] * 100)
            pct_b = round(both / counts[b] * 100)
            max_pct = max(pct_a, pct_b)
            if max_pct >= min_pct:
                pairs.append({
                    "sig_a": a, "sig_b": b,
                    "both": both,
                    "a_count": counts[a], "b_count": counts[b],
                    "pct_a": pct_a, "pct_b": pct_b,
                    "max_pct": max_pct,
                })

    pairs.sort(key=lambda x: -x["max_pct"])
    return {
        "pairs": pairs[:60],
        "signal_counts": {s: counts[s] for s in valid},
        "n_tickers": n,
    }


# ── Single-ticker Turbo analysis ──────────────────────────────────────────────
@app.get("/api/turbo-analyze/{ticker}")
def api_turbo_analyze(ticker: str, tf: str = "1d"):
    """
    Run the full Turbo signal engine on a single ticker and return the same
    row format as the TURBO scan — identical scoring, all signals, all badges.
    """
    from turbo_engine import _scan_turbo_ticker, _calc_turbo_score
    ticker = ticker.upper().strip()
    row = _scan_turbo_ticker(ticker, interval=tf)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Could not fetch data for {ticker}")
    row["turbo_score"] = _calc_turbo_score(row)
    return row


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
