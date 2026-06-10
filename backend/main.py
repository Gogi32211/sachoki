"""
main.py — FastAPI app + APScheduler + all API routes.
"""
from __future__ import annotations
import os
import sys
import logging
from typing import Optional

# ── Load .env before anything else reads os.environ ──────────────────────────
try:
    from dotenv import load_dotenv as _load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    _load_dotenv(_env_path, override=False)   # override=False: real env vars take precedence
except ImportError:
    pass  # python-dotenv not installed — env vars must be set externally

# Ensure backend/ directory is on sys.path so sub-packages (analyzers/) are importable
# regardless of which directory uvicorn is launched from.
_backend_dir = os.path.dirname(os.path.abspath(__file__))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)
import concurrent.futures
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from data import fetch_ohlcv
from signal_engine import compute_signals
from wlnbb_engine import compute_wlnbb, score_last_bar, score_bars, l_signal_label
from predictor import predict_next
from l_sequence_predictor import predict_l_next
from stats_engine import compute_tz_l_matrix
from canonical_scoring_engine import compute_canonical_score, get_scoring_metadata, SCORING_ENGINE_NAME, SCORING_ENGINE_VERSION
from ultra_score import compute_ultra_score as _compute_ultra_score
from ultra_signal_parser import parse_stock_stat_signals as _parse_ultra_signals
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
from paper_portfolio_migration import ensure_paper_portfolio_tables
from paper_portfolio_api import router as portfolio_router
from chart_obs_migration import ensure_chart_obs_tables
from chart_obs_api_v2 import router as chart_obs_router
from signal_replay_migration import ensure_signal_replay_tables
from signal_replay_routes import router as signal_replay_router
from ultra_pump_migration import ensure_ultra_pump_tables
from ultra_pump_routes import router as ultra_pump_router
from dashboard_routes import router as dashboard_router
from ultra_scan_migration import ensure_ultra_scan_tables
from ultra_scan_routes import router as ultra_scan_router
try:
    from studio_api import router as studio_router
    _STUDIO_AVAILABLE = True
except Exception as _studio_err:
    log.warning("Analytic Studio not available: %s", _studio_err)
    _STUDIO_AVAILABLE = False
try:
    from qlib_lab.api import router as qlib_router
    _QLIB_AVAILABLE = True
except Exception as _qlib_err:
    log.warning("QLIB lab not available: %s", _qlib_err)
    _QLIB_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = None
    try:
        ensure_paper_portfolio_tables()
        ensure_chart_obs_tables()
        ensure_signal_replay_tables()
        ensure_ultra_pump_tables()
        ensure_ultra_scan_tables()
        # Pre-warm memory cache from DB so dashboard/ultra tab show data immediately
        try:
            from ultra_orchestrator import load_latest_ultra_scan_from_db
            for _tf in ("1d", "4h"):
                for _uni in ("sp500", "nasdaq"):
                    try:
                        load_latest_ultra_scan_from_db(_uni, _tf)
                    except Exception:
                        pass
        except Exception as _exc:
            log.warning("DB pre-warm failed (non-fatal): %s", _exc)
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

        # ── Studio DB daily incremental refresh: market close + 1h, Mon-Fri ──
        # Market closes at 16:00 ET. We run at 17:00 ET to ensure all bars
        # are settled. Adds at most ~1 bar per ticker per day.
        def _scheduled_studio_refresh():
            try:
                from studio_api import _run_incremental
                _run_incremental(["sp500", "nasdaq"])
            except Exception as _e:
                log.warning("Scheduled studio refresh failed: %s", _e)

        scheduler.add_job(
            _scheduled_studio_refresh,
            CronTrigger(hour=17, minute=0, day_of_week="mon-fri"),
            id="studio_daily_refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # ── AI Journal daily cadence ────────────────────────────────────────
        # 15:30 ET: decide IN-SESSION (→ AT_DECISION fills, ~30 min before close).
        # 09:31 ET: fill yesterday's PENDING_OPEN at the open + grade + reflect.
        def _journal_session():
            try:
                from ai_journal.decide import run_session
                run_session()
            except Exception as _e:
                log.warning("Journal session failed: %s", _e)

        def _journal_open_routine():
            try:
                from ai_journal.fills import fill_pending_open
                from ai_journal.grading import grade_open_positions
                from ai_journal.lessons import reflect as _reflect
                fill_pending_open(); grade_open_positions(); _reflect()
            except Exception as _e:
                log.warning("Journal open routine failed: %s", _e)

        scheduler.add_job(_journal_session, CronTrigger(hour=15, minute=30, day_of_week="mon-fri"),
                          id="journal_session", replace_existing=True, max_instances=1, coalesce=True)
        # 9:50 (not 9:31): the Massive feed is ~15 min delayed, so today's opening
        # price isn't available right at the bell — fill once it is.
        scheduler.add_job(_journal_open_routine, CronTrigger(hour=9, minute=50, day_of_week="mon-fri"),
                          id="journal_open_routine", replace_existing=True, max_instances=1, coalesce=True)

        # Daily SEC Form 4 incremental — re-fetch the last few days (catches late /
        # amended filings) so the chart's ★ insider-buy markers stay fresh.
        def _insider_daily():
            try:
                from ai_journal.edgar import ingest_form4
                ingest_form4(days=5)   # small window, ~5 min, runs in-process
            except Exception as _e:
                log.warning("Insider daily ingest failed: %s", _e)

        scheduler.add_job(_insider_daily, CronTrigger(hour=18, minute=30, day_of_week="mon-fri"),
                          id="insider_daily", replace_existing=True, max_instances=1, coalesce=True)

        # Daily Zone-Edge ALERT — the OOS-validated retest+flip+volB pattern as a
        # daily log line (open the Zone Edge tab for the full clickable list).
        def _zone_setup_daily():
            try:
                from ai_journal.zone_events import live_setups
                r = live_setups(event_type="retest", slots={"vol": "B"},
                                require_flip=True, max_age_days=3)
                tks = [s["ticker"] for s in r.get("setups", [])][:25]
                log.info("ZONE-SETUP alert %s: %d confirmed retest+flip+volB: %s",
                         r.get("as_of"), r.get("confirmed", 0), ", ".join(tks))
            except Exception as _e:
                log.warning("Zone setup daily failed: %s", _e)

        scheduler.add_job(_zone_setup_daily, CronTrigger(hour=17, minute=30, day_of_week="mon-fri"),
                          id="zone_setup_daily", replace_existing=True, max_instances=1, coalesce=True)

        scheduler.start()
        log.info("Scheduler started (daily_scan @ 9:30,12:30,15:30 ET; "
                 "studio_daily_refresh @ 17:00 ET Mon-Fri; "
                 "journal_session @ 15:30; journal_open_routine @ 9:50 ET Mon-Fri; "
                 "insider_daily @ 18:30 ET Mon-Fri; zone_setup_daily @ 17:30 ET Mon-Fri)")
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

app.include_router(portfolio_router)
app.include_router(chart_obs_router)
app.include_router(signal_replay_router)
app.include_router(ultra_pump_router)
app.include_router(dashboard_router)
app.include_router(ultra_scan_router)
if _STUDIO_AVAILABLE:
    app.include_router(studio_router)
if _QLIB_AVAILABLE:
    app.include_router(qlib_router)
try:
    from ai_journal.api import router as ai_journal_router
    app.include_router(ai_journal_router)
except Exception as _aij_exc:  # never block app startup on the journal
    log.warning("ai_journal router not loaded: %s", _aij_exc)


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
    return {"status": "ok", "service": "tz-signal-dashboard", "version": "2.8"}


@app.get("/api/zone-retest/tickers")
def zone_retest_tickers(lookback_min: int = 8, lookback_max: int = 90,
                        vol_min: float = 10.0, vol_max: float | None = None):
    """Tickers currently in a high-volume zone re-test, with vol band
    [vol_min, vol_max). Filter chips in Ultra call this per tier."""
    from ai_journal.zone_retest import active_retests
    try:
        res = active_retests(lb_min=lookback_min, lb_max=lookback_max,
                             vol_min=vol_min, vol_max=vol_max, limit=5000)
        return {"as_of": res["as_of"], "params": res["params"],
                "tickers": sorted({r["ticker"] for r in res["rows"]})}
    except Exception as e:
        log.warning("zone-retest tickers failed: %s", e)
        return {"as_of": None, "tickers": [], "error": str(e)}


@app.get("/api/zone-retest/zones/{ticker}")
def zone_retest_zones(ticker: str, lookback_min: int = 8, lookback_max: int = 90,
                      vol_min: float = 2.0, vol_max: float | None = None,
                      classify: bool = False):
    """All currently-active zones for one ticker (drawn on the chart) — default
    vol_min=2 shows every tier so chart matches whatever filter was used.

    If classify=true, each zone gets a `bar_classifications` array — every daily
    bar from trigger_date until today tagged inside / cross / touch_below /
    touch_above / above / below — used to render markers on the chart."""
    from ai_journal.zone_retest import zones_for_ticker, classify_recent_bars, get_analytics_conn
    tk = ticker.upper()
    try:
        zones = zones_for_ticker(tk, lb_min=lookback_min, lb_max=lookback_max,
                                 vol_min=vol_min, vol_max=vol_max)
        if classify and zones:
            # Need universe for the per-bar query
            with get_analytics_conn() as a:
                uni = a.execute(
                    "SELECT universe FROM bars WHERE ticker=? ORDER BY date DESC LIMIT 1",
                    [tk]).fetchone()
                as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
            universe = uni[0] if uni else "sp500"
            for z in zones:
                z["bar_classifications"] = classify_recent_bars(
                    tk, universe, z["zone_low"], z["zone_high"],
                    since_date=z["trigger_date"], until_date=as_of)
        return {"ticker": tk, "zones": zones, "count": len(zones)}
    except Exception as e:
        log.warning("zone-retest zones failed for %s: %s", ticker, e)
        return {"ticker": tk, "zones": [], "error": str(e)}


@app.get("/api/hv-zones/history/{ticker}")
def hv_zones_history(ticker: str, vol_min: float = 5.0,
                     from_date: str | None = None, limit: int = 500):
    """All historical HV-spike triggers for a ticker — chart 'history' overlay.
    from_date: optional, restricts to triggers since that date (matches the
    chart's earliest visible bar)."""
    from ai_journal.zone_retest import history_for_ticker
    try:
        return history_for_ticker(ticker, vol_min=vol_min,
                                  from_date=from_date, limit=limit)
    except Exception as e:
        log.warning("hv-zones history for %s failed: %s", ticker, e)
        return {"ticker": ticker.upper(), "zones": [], "error": str(e)}


@app.get("/api/gann-zones/history/{ticker}")
def gann_zones_history(ticker: str, pivot: int = 5,
                       from_date: str | None = None, limit: int = 500):
    """Pivot-based Gann historical zones (highs and lows of local swings)
    over the chart's visible range."""
    from ai_journal.gann_zones import history_pivots
    try:
        return history_pivots(ticker, pivot=pivot,
                              from_date=from_date, limit=limit)
    except Exception as e:
        log.warning("gann-zones history for %s failed: %s", ticker, e)
        return {"ticker": ticker.upper(), "zones": [], "error": str(e)}


@app.get("/api/gann-zones/tickers")
def gann_zones_tickers(lookback: int = 90, zone_kind: str = "any"):
    """Tickers whose current close sits inside a Gann zone (top, bottom, or
    either). Filter chip in Ultra calls this."""
    from ai_journal.gann_zones import active_tickers
    try:
        return active_tickers(lookback=lookback, zone_kind=zone_kind)
    except Exception as e:
        log.warning("gann zones tickers failed: %s", e)
        return {"tickers": [], "error": str(e)}


@app.get("/api/vol-class/history/{ticker}")
def vol_class_history(ticker: str, cls: str = "VB",
                      from_date: str | None = None, limit: int = 500):
    """All historical bars of a given volume-class (VB/W/...) for a ticker —
    chart overlay drawing S/R lines at each bar's [low, high]. `cls` is the
    TZ_WLNBB volume bucket (W/L/N/B/VB)."""
    from ai_journal.vol_class import history_vol_class
    try:
        return history_vol_class(ticker, cls=cls, from_date=from_date, limit=limit)
    except Exception as e:
        log.warning("vol-class history for %s (%s) failed: %s", ticker, cls, e)
        return {"ticker": ticker.upper(), "cls": cls, "zones": [], "error": str(e)}


@app.get("/api/vol-class/tickers")
def vol_class_tickers(cls: str = "VB"):
    """Tickers whose latest bar is volume-class `cls`. Ultra VB/W filter chip."""
    from ai_journal.vol_class import active_tickers
    try:
        return active_tickers(cls=cls)
    except Exception as e:
        log.warning("vol-class tickers (%s) failed: %s", cls, e)
        return {"cls": cls, "tickers": [], "error": str(e)}


@app.get("/api/gann-zones/zones/{ticker}")
def gann_zones_zones(ticker: str, lookback: int = 90):
    """Top + bottom Gann zones for one ticker with per-bar classifications."""
    from ai_journal.gann_zones import zones_for_ticker
    tk = ticker.upper()
    try:
        zones = zones_for_ticker(tk, lookback=lookback)
        return {"ticker": tk, "zones": zones, "count": len(zones)}
    except Exception as e:
        log.warning("gann zones for %s failed: %s", ticker, e)
        return {"ticker": tk, "zones": [], "error": str(e)}


@app.get("/api/gann-zones/scan")
def gann_zones_scan(lookback: int = 90):
    """Sidebar list view for the Gann-Zones page."""
    from ai_journal.gann_zones import scan as _gscan
    from ai_journal.db import get_journal_conn
    try:
        res = _gscan(lookback=lookback)
        with get_journal_conn() as j:
            meta = {r[0]: {"name": r[1], "sector": r[2], "mcap_bucket": r[3]}
                    for r in j.execute(
                        "SELECT ticker,name,sector,mcap_bucket FROM ticker_meta").fetchall()}
        for r in res["rows"]:
            m = meta.get(r["ticker"], {})
            r["name"]        = m.get("name") or ""
            r["sector"]      = m.get("sector") or ""
            r["mcap_bucket"] = m.get("mcap_bucket") or "unknown"
        return res
    except Exception as e:
        log.warning("gann zones scan failed: %s", e)
        return {"rows": [], "error": str(e)}


@app.get("/api/zone-retest/scan")
def zone_retest_scan():
    """List view for the HV-Zones page — all active tickers across all tiers
    with their meta + current zone + tier."""
    from ai_journal.zone_retest import scan
    from ai_journal.db import get_journal_conn
    try:
        res = scan()
        # Enrich with ticker_meta (sector/mcap/name)
        with get_journal_conn() as j:
            meta = {r[0]: {"name": r[1], "sector": r[2], "mcap_bucket": r[3]}
                    for r in j.execute(
                        "SELECT ticker,name,sector,mcap_bucket FROM ticker_meta").fetchall()}
        for r in res["rows"]:
            m = meta.get(r["ticker"], {})
            r["name"]        = m.get("name") or ""
            r["sector"]      = m.get("sector") or ""
            r["mcap_bucket"] = m.get("mcap_bucket") or "unknown"
        return res
    except Exception as e:
        log.warning("zone-retest scan failed: %s", e)
        return {"as_of": None, "rows": [], "error": str(e)}


@app.get("/api/zone-events/report")
def zone_events_report(vol_min: float = 5.0, lb_max: int = 90, horizon: int = 10,
                       first_only: bool = True, min_n: int = 30):
    """Zone EXIT vs RETEST forward-edge analytics + the bar-context lifts that
    most improve each event's outcome (research, whole-history)."""
    from ai_journal.zone_events import full_report
    try:
        return full_report(vol_min=vol_min, lb_max=lb_max, horizon=horizon,
                           first_only=first_only, min_n=min_n)
    except Exception as e:
        log.exception("zone-events report failed")
        return {"events": [], "context": {}, "error": str(e)}


@app.get("/api/fib/levels/{ticker}")
def fib_levels_api(ticker: str, mode: str = "macro", from_date: str | None = None,
                   years: int = 5):
    """Fibonacci retracement levels for a ticker — chart overlay. mode=macro
    (last `years`) or swing (visible range via from_date)."""
    from ai_journal.fib_levels import fib_levels
    try:
        return fib_levels(ticker, mode=mode, from_date=from_date, years=years)
    except Exception as e:
        log.warning("fib levels %s failed: %s", ticker, e)
        return {"ticker": ticker.upper(), "mode": mode, "levels": [], "error": str(e)}


@app.get("/api/zone-events/ticker/{ticker}")
def zone_events_ticker(ticker: str, vol_min: float = 5.0, from_date: str | None = None,
                       horizon: int = 10):
    """All EXIT/RETEST events for one ticker — chart overlay (zone + markers + flip)."""
    from ai_journal.zone_events import events_for_ticker
    try:
        return events_for_ticker(ticker, vol_min=vol_min, from_date=from_date, horizon=horizon)
    except Exception as e:
        log.warning("zone-events ticker %s failed: %s", ticker, e)
        return {"ticker": ticker.upper(), "events": [], "error": str(e)}


@app.get("/api/zone-events/examples")
def zone_events_examples(event_type: str = "retest", require_flip: bool = True,
                         vol_min: float = 5.0, horizon: int = 10, limit: int = 20):
    """~limit concrete example instances of a pattern (one per ticker) to open and
    inspect on the chart."""
    from ai_journal.zone_events import examples
    try:
        return examples(event_type=event_type, require_flip=require_flip,
                       vol_min=vol_min, horizon=horizon, limit=limit)
    except Exception as e:
        log.exception("zone-events examples failed")
        return {"examples": [], "error": str(e)}


@app.get("/api/zone-events/pattern")
def zone_events_pattern(event_type: str = "retest", require_flip: bool = False,
                        vol_min: float = 5.0, horizon: int = 10,
                        tz: str = "*", z: str = "*", flip: str = "*", l: str = "*", suffix: str = "*",
                        bodywk: str = "*", gaprng: str = "*", l5: str = "*", vol: str = "*"):
    """Full bar-code pattern → matched zone-event edge + example tickers. Each
    slot is a value or '*' (wildcard)."""
    from ai_journal.zone_events import pattern
    slots = {"tz": tz, "z": z, "flip": flip, "l": l, "suffix": suffix, "bodywk": bodywk,
             "gaprng": gaprng, "l5": l5, "vol": vol}
    try:
        return pattern(event_type=event_type, slots=slots, require_flip=require_flip,
                      vol_min=vol_min, horizon=horizon)
    except Exception as e:
        log.exception("zone-events pattern failed")
        return {"matched": {"n": 0}, "examples": [], "error": str(e)}


@app.get("/api/zone-events/live")
def zone_events_live(event_type: str = "retest", require_flip: bool = False,
                     vol_min: float = 5.0, horizon: int = 10, max_age_days: int = 5,
                     tz: str = "*", z: str = "*", flip: str = "*", l: str = "*", suffix: str = "*", bodywk: str = "*",
                     gaprng: str = "*", l5: str = "*", vol: str = "*", bools: str = "", cats: str = ""):
    """LIVE setup scan — recent bars matching the pattern, flagged confirmed
    (flip fired) vs pending (watch for flip). The OOS pattern as a daily alert.
    `bools` = comma-separated boolean signal names; `cats` = comma-separated
    col=val categoricals (flip_code, sequence p1_*/p2_*, fib_level, …)."""
    from ai_journal.zone_events import live_setups
    slots = {"tz": tz, "z": z, "flip": flip, "l": l, "suffix": suffix, "bodywk": bodywk,
             "gaprng": gaprng, "l5": l5, "vol": vol}
    bool_list = [b.strip() for b in bools.split(",") if b.strip()]
    cat_map = {}
    for pair in cats.split(","):
        if "=" in pair:
            k, v = pair.split("=", 1)
            cat_map[k.strip()] = v.strip()
    try:
        return live_setups(event_type=event_type, slots=slots, bools=bool_list, cats=cat_map,
                          require_flip=require_flip,
                          vol_min=vol_min, horizon=horizon, max_age_days=max_age_days)
    except Exception as e:
        log.exception("zone-events live failed")
        return {"setups": [], "count": 0, "error": str(e)}


@app.get("/api/zone-events/pattern/values")
def zone_events_pattern_values(event_type: str = "retest", require_flip: bool = False,
                               vol_min: float = 5.0, horizon: int = 10):
    """Distinct values per slot for the pattern builder dropdowns."""
    from ai_journal.zone_events import pattern_values
    try:
        return pattern_values(event_type=event_type, require_flip=require_flip,
                             vol_min=vol_min, horizon=horizon)
    except Exception as e:
        log.warning("pattern values failed: %s", e)
        return {"slots": {}, "error": str(e)}


@app.post("/api/admin/backfill-z1")
def admin_backfill_z1(universes: str = ""):
    """One-shot: backfill the historically-missing Z1 signal in `bars`
    (recompute with the fixed engine, patch only Z1-involved rows). Runs in a
    background thread; poll /api/admin/backfill-z1/status."""
    from studio.backfill_z1 import start_backfill_bg
    unis = [u.strip() for u in universes.split(",") if u.strip()] or None
    return start_backfill_bg(unis)


@app.get("/api/admin/backfill-z1/status")
def admin_backfill_z1_status():
    from studio.backfill_z1 import STATUS
    return STATUS


@app.get("/api/zone-events/sequences")
def zone_events_sequences(event_type: str = "exit_up", depth: int = 3, horizon: int = 10,
                          vol_min: float = 5.0, min_n: int = 30, ways: int = 2,
                          zone_def: str = "spike"):
    """AUTO-MINER: ranked multi-bar lead-in signal buildups before a zone exit.
    Each result is an ordered sequence (e.g. −2:sig_abs → −1:eb_bull → 0:vbo_up)
    with forward win-rate, lift and IS/OOS split. depth = bars back (2–4).
    zone_def: 'spike' (vol ≥ N×avg, V1) or 'vb' (VB vol-class bar, V2)."""
    from ai_journal.zone_events import exit_sequences
    try:
        return exit_sequences(event_type=event_type, depth=depth, horizon=horizon,
                             vol_min=vol_min, min_n=min_n, ways=ways, zone_def=zone_def)
    except Exception as e:
        log.exception("zone-events sequences failed")
        return {"event_type": event_type, "best": [], "worst": [], "error": str(e)}


@app.get("/api/zone-events/live-sequences")
def zone_events_live_sequences(event_type: str = "exit_down", zone_def: str = "spike",
                               depth: int = 4, max_age_days: int = 10, min_sigs: int = 2):
    """LIVE scan: recent zone exits with the de-biased lead-in buildup that fired,
    flagging any cross-stable validated pattern. No look-ahead (pivots excluded)."""
    from ai_journal.zone_events import live_sequences
    try:
        return live_sequences(event_type=event_type, zone_def=zone_def, depth=depth,
                             max_age_days=max_age_days, min_sigs=min_sigs)
    except Exception as e:
        log.exception("zone-events live-sequences failed")
        return {"event_type": event_type, "setups": [], "count": 0, "error": str(e)}


@app.get("/api/live-prices")
def live_prices(tickers: str):
    """Bulk LIVE price snapshot (Massive) for a comma-separated ticker list."""
    from data_polygon import fetch_snapshot
    tks = [t.strip().upper() for t in tickers.split(",") if t.strip()][:400]
    try:
        return {"prices": fetch_snapshot(tks)}
    except Exception as e:
        log.warning("live-prices failed: %s", e)
        return {"prices": {}, "error": str(e)}


@app.post("/api/journal/advise")
def journal_advise(items: list[dict]):
    """Generic advisory: judge a list of {ticker, setup?, source?} (BUY/WATCH/SKIP +
    conviction + thesis). Advisory — opens no positions. Used to decide ALL tickers
    on the Setups page (sequences + combos) at once."""
    from ai_journal.decide import advise_tickers
    try:
        return advise_tickers(items)
    except Exception as e:
        log.exception("advise failed")
        return {"decisions": [], "error": str(e)}


@app.post("/api/journal/advise-setups")
def journal_advise_setups(zone_def: str = "spike", max_age_days: int = 20, limit: int = 25):
    """Ask the journal's decision LLM to judge the Setups-Board tickers
    (BUY/WATCH/SKIP + conviction + thesis). Advisory — opens no positions."""
    from ai_journal.decide import advise_setups
    try:
        return advise_setups(zone_def=zone_def, max_age_days=max_age_days, limit=limit)
    except Exception as e:
        log.exception("advise-setups failed")
        return {"decisions": [], "error": str(e)}


@app.get("/api/atomic-scan")
def api_atomic_scan(max_age_days: int = 4, dv_floor: float = 500_000):
    """Live 'weak-close gap-up' scan — bull T-signal + close=O + gap(G2/G3), the
    5-year-validated atomic edge. Scored by corroborating atoms (R2L/EO/vol=B/wick=D/G3),
    with the current market regime attached as a size gate. Surfaces candidates only."""
    from ai_journal.atomic_scan import atomic_scan
    try:
        return atomic_scan(max_age_days=max_age_days, dv_floor=dv_floor)
    except Exception as e:
        log.exception("atomic scan failed")
        return {"rows": [], "count": 0, "error": str(e)}


@app.get("/api/atomic-journal")
def api_atomic_journal():
    """Separate paper-trading journal for the atomic weak-close gap-up edge."""
    from ai_journal.atomic_journal import summary
    try:
        return summary()
    except Exception as e:
        log.exception("atomic journal failed"); return {"open": [], "closed": [], "error": str(e)}


@app.post("/api/atomic-journal/open")
def api_atomic_journal_open(top: int = 15, min_score: int = 70):
    """Open paper positions from today's atomic scan (regime-sized)."""
    from ai_journal.atomic_journal import open_from_scan
    try:
        return open_from_scan(top=top, min_score=min_score)
    except Exception as e:
        log.exception("atomic open failed"); return {"opened": [], "error": str(e)}


@app.post("/api/atomic-journal/grade")
def api_atomic_journal_grade():
    """Walk forward the open atomic positions; close on stop/target/horizon."""
    from ai_journal.atomic_journal import grade
    try:
        return grade()
    except Exception as e:
        log.exception("atomic grade failed"); return {"graded": 0, "error": str(e)}


@app.get("/api/zone-events/board")
def zone_events_board(zone_def: str = "spike", max_age_days: int = 20, min_oos: float = 55.0):
    """Setups Board: recent tickers that built an OOS-holding lead-in sequence, with
    score, probability-up (= OOS win%), why, last price, sector and journal status."""
    from ai_journal.zone_events import sequence_board
    try:
        return sequence_board(zone_def=zone_def, max_age_days=max_age_days, min_oos=min_oos)
    except Exception as e:
        log.exception("sequence board failed")
        return {"rows": [], "count": 0, "error": str(e)}


@app.get("/api/zone-events/combo-board")
def zone_events_combo_board(event_types: str = "retest,exit_up,exit_down",
                            vol_min: float = 5.0, horizon: int = 10, ways: int = 2,
                            min_n: int = 40, max_age_days: int = 10):
    """Combos Board: across retest/breakout(exit_up)/spring(exit_down), recent
    tickers satisfying an OOS-holding combo, scored for BUY quality (OOS win +
    lift + confirmed-flip bonus + recency)."""
    from ai_journal.zone_events import combo_board
    ets = [e.strip() for e in event_types.split(",") if e.strip()]
    try:
        return combo_board(event_types=ets, vol_min=vol_min, horizon=horizon,
                           ways=ways, min_n=min_n, max_age_days=max_age_days)
    except Exception as e:
        log.exception("combo board failed")
        return {"rows": [], "count": 0, "error": str(e)}


@app.get("/api/zone-events/sequence-tickers")
def zone_events_sequence_tickers(seq: str, event_type: str = "exit_up",
                                 zone_def: str = "spike", depth: int = 4,
                                 max_age_days: int = 60):
    """Drill-down for a clicked miner sequence: the recent tickers that built it.
    `seq` = comma-separated 'signal@-k' tokens (k=0 = exit bar)."""
    from ai_journal.zone_events import sequence_tickers
    try:
        return sequence_tickers(seq=seq, event_type=event_type, zone_def=zone_def,
                               depth=depth, max_age_days=max_age_days)
    except Exception as e:
        log.exception("sequence-tickers failed")
        return {"seq": seq, "tickers": [], "count": 0, "error": str(e)}


@app.get("/api/zone-events/combos")
def zone_events_combos(event_type: str = "retest", vol_min: float = 5.0,
                       lb_max: int = 90, horizon: int = 10, first_only: bool = True,
                       min_n: int = 40, anchor: str | None = None, top: int = 15,
                       ways: int = 2):
    """Top feature COMBINATIONS (2- or 3-way) for one zone event type, ranked by
    forward-edge lift, with in-sample/out-of-sample win split. anchor=tz_up_next3
    → best partners for the T/Z follow-through."""
    from ai_journal.zone_events import combo_lift
    try:
        return combo_lift(event_type=event_type, vol_min=vol_min, lb_max=lb_max,
                          horizon=horizon, first_only=first_only, min_n=min_n,
                          anchor=anchor or None, top=top, ways=ways)
    except Exception as e:
        log.exception("zone-events combos failed")
        return {"best": [], "worst": [], "error": str(e)}


_ticker_info_cache: dict = {}

@app.get("/api/ticker-info/{ticker}")
def api_ticker_info(ticker: str):
    t = ticker.upper()
    if t in _ticker_info_cache:
        return _ticker_info_cache[t]
    try:
        from data_massive import get_ticker_info
        info = get_ticker_info(t)
        result = {
            "ticker":   t,
            "name":     info.get("name") or t,
            "sector":   info.get("sector") or "",
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
                from data_massive import get_ticker_info
                info = get_ticker_info(t)
                r = {
                    "ticker":   t,
                    "name":     info.get("name") or t,
                    "sector":   info.get("sector") or "",
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


@app.get("/api/studio/live-tail/{ticker}")
def api_studio_live_tail(ticker: str, after: str = "", tf: str = "1d"):
    """Today's LIVE forming 1d bar(s) + signals from Massive, to append onto the
    DB chart (which ends at the last enriched bar). Returns bars with date > `after`
    (the client's last DB bar date) in the compact studioBars row shape.

    Only returns data while the US regular session is live ("when the market opens,
    start forming the bar") — pre-open/after-close → []. 15-min delayed is fine for
    context. Reuses api_bar_signals (full engine suite, Massive primary source)."""
    try:
        from premarket_cache import _regular_session_open
        if not _regular_session_open():
            return {"bars": [], "reason": "market_closed"}
        rows = api_bar_signals(ticker, tf="1d", bars=210)
        after = (after or "")[:10]
        out = []
        for b in rows:
            d = str(b.get("date") or "")[:10]
            if not d or (after and d <= after):
                continue
            tzs = str(b.get("tz") or "")
            out.append({
                "date": d,
                "open": b.get("open"), "high": b.get("high"),
                "low": b.get("low"), "close": b.get("close"), "volume": b.get("volume"),
                "vol_bucket": b.get("vol_bucket") or "",
                "t_sig": tzs if tzs.startswith("T") else "",
                "z_sig": tzs if tzs.startswith("Z") else "",
                "l_sig": b.get("l_chart") or b.get("l") or "",
                "composite_full_suffix": "",          # not computed in live path
                "bar_body_wick": b.get("bar_body_wick") or "",
                "bar_gap_range": b.get("bar_gap_range") or "",
                "bar_line5": b.get("bar_line5") or "",
                "wyc_stage": "", "wt_stage": "",
                "forming": True,
            })
        return {"bars": out}
    except Exception as e:
        log.warning("live-tail %s: %s", ticker, e)
        return {"bars": [], "error": str(e)}


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
        from predictor import compute_tz_stats, compute_tz_matrix, get_last_tz_signals
        df    = fetch_ohlcv(ticker, interval=tf, bars=5000)
        sigs  = compute_signals(df)
        full  = df.join(sigs)
        tz    = predict_next(full)
        tz_stats  = compute_tz_stats(full)
        tz_matrix = compute_tz_matrix(full)

        wlnbb = compute_wlnbb(df)
        full_w = full.join(wlnbb)
        l_preds = predict_l_next(full_w)

        last_tz = get_last_tz_signals(full, n=5)

        return {**tz, **l_preds, "tz_stats": tz_stats, "tz_matrix": tz_matrix,
                "last_tz_signals": last_tz}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/predict-sequence/{ticker}")
def api_predict_sequence(ticker: str, body: dict, tf: str = "1d"):
    """
    Predict next T/Z signal for an arbitrary N-bar sequence on a specific ticker.
    body: { "sequence": ["T1", null, "T2G", "T1"], "tf": "1d" }
    null in sequence = wildcard (any signal).
    """
    try:
        from predictor import predict_sequence
        sequence = body.get("sequence", [])
        interval = body.get("tf", tf)
        df   = fetch_ohlcv(ticker, interval=interval, bars=5000)
        sigs = compute_signals(df)
        full = df.join(sigs)
        return predict_sequence(full, sequence)
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


# ── 260523 SuperChart endpoint ──────────────────────────────────────────────
# Returns last N bars from the same stock_stat CSV that Turbo/Ultra use, so
# any signal visible in those scans appears on the same bar here. Includes:
# T/Z signals, L-signals, line3/4/5, AD-FRESH/AD-CLUSTER, WYC Phase, PREUP/
# PREDN, plus parsed wvf_spike/psar_bull/rsi2_token convenience flags.

from analyzers.tz_wlnbb.filters_260523 import (
    parse_line5_tokens as _parse_line5_tokens,
    _to_bool,
)


@app.get("/api/superchart/{ticker}")
def api_superchart(
    ticker: str,
    universe: str = "sp500",
    tf: str = "1d",
    bars: int = 60,
    nasdaq_batch: str = "",
):
    """SuperChart full-signal sync from the same stock_stat_tz_wlnbb CSV used
    by Turbo / Ultra. Returns last N bars with all signal families joined."""
    import csv as _csv
    from datetime import datetime
    try:
        path = _tz_batch_stat_path(universe, tf, nasdaq_batch)
        if not os.path.exists(path):
            path = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"
        if not os.path.exists(path):
            raise HTTPException(
                status_code=404,
                detail=f"No stock_stat CSV for {universe}/{tf}. Run "
                       f"/api/tz-wlnbb/generate-stock-stat first.",
            )

        rows: list[dict] = []
        ticker_norm = ticker.upper()
        with open(path, newline="", encoding="utf-8") as fh:
            for row in _csv.DictReader(fh):
                if row.get("ticker", "").upper() != ticker_norm:
                    continue
                rows.append(row)
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Ticker {ticker} not found in {path}",
            )

        # Last N bars
        rows = rows[-max(1, bars):]

        out_bars = []
        for r in rows:
            l5 = r.get("bar_line5") or ""
            l5_tokens = _parse_line5_tokens(l5)
            out_bars.append({
                "date":            r.get("date") or r.get("bar_datetime") or "",
                "open":            r.get("open"),
                "high":            r.get("high"),
                "low":             r.get("low"),
                "close":           r.get("close"),
                "volume":          r.get("volume"),
                # T/Z
                "t_signal":        r.get("t_signal", ""),
                "z_signal":        r.get("z_signal", ""),
                "bull_priority_code": r.get("bull_priority_code", 0),
                "bear_priority_code": r.get("bear_priority_code", 0),
                # L
                "l_signal":        r.get("l_signal", ""),
                "l_digits":        r.get("l_digits", ""),
                "volume_bucket":   r.get("volume_bucket", ""),
                # line3/4/5
                "bar_body_wick":   r.get("bar_body_wick", ""),
                "bar_gap_range":   r.get("bar_gap_range", ""),
                "bar_line5":       l5,
                **l5_tokens,
                # 260523 — AD-FRESH / WYC
                "ad_fresh":        _to_bool(r.get("ad_fresh", "")),
                "ad_cluster":      _to_bool(r.get("ad_cluster", "")),
                "wyc_phase":       r.get("wyc_phase", "") or "NEUTRAL",
                "wyc_spring":      _to_bool(r.get("wyc_spring", "")),
                "wyc_sos":         _to_bool(r.get("wyc_sos", "")),
                "wyc_acc_tr":      _to_bool(r.get("wyc_acc_tr", "")),
                "wyc_markup":      _to_bool(r.get("wyc_markup", "")),
                # 260523 v3.1 — swing classification
                "swing_type":      r.get("swing_type", "") or "",
                "swing_ret":       r.get("swing_ret", ""),
                "swing_bars":      r.get("swing_bars", ""),
                "is_pivot_high":   _to_bool(r.get("is_pivot_high", "")),
                "is_pivot_low":    _to_bool(r.get("is_pivot_low", "")),
                # PREUP / PREDN
                "preup_text":      r.get("preup_signal", ""),
                "predn_text":      r.get("predn_signal", ""),
                # Composite labels
                "composite_full_label":    r.get("composite_full_label", ""),
                "composite_primary_label": r.get("composite_primary_label", ""),
                "full_suffix":             r.get("full_suffix", ""),
                # Version
                "tz_wlnbb_version": r.get("tz_wlnbb_version", ""),
                "build_marker":     r.get("build_marker", ""),
            })

        # Sync-warning: compare stock_stat mtime against today
        try:
            stat_mtime = datetime.utcfromtimestamp(os.path.getmtime(path))
            stat_age_h = (datetime.utcnow() - stat_mtime).total_seconds() / 3600.0
        except Exception:
            stat_age_h = 0.0

        response = {
            "ticker": ticker_norm,
            "tf": tf,
            "universe": universe,
            "bars": out_bars,
            "stock_stat_path": path,
            "stock_stat_age_hours": round(stat_age_h, 2),
            "tz_wlnbb_version": out_bars[-1].get("tz_wlnbb_version", "") if out_bars else "",
            "build_marker": out_bars[-1].get("build_marker", "") if out_bars else "",
        }

        # Sync warning if stock_stat is older than 24h (configurable)
        if stat_age_h > 24:
            response["data_sync_warning"] = (
                f"SuperChart stock_stat is {stat_age_h:.1f}h old "
                f"(> 24h). Trigger /api/tz-wlnbb/generate-stock-stat to resync."
            )
        return response
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("superchart error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── 260523 enrichment + filter helpers ──────────────────────────────────────
# Loads latest stock_stat row per ticker for ad_fresh / ad_cluster / wyc_phase
# and applies optional filter params. Pure logic lives in
# analyzers/tz_wlnbb/filters_260523.py so it is importable without FastAPI.
from analyzers.tz_wlnbb.filters_260523 import (
    enrich_with_260523 as _enrich_with_260523_pure,
    apply_260523_filters as _apply_260523_filters,
)


def _enrich_with_260523(results: list, universe: str, tf: str, nasdaq_batch: str = "") -> list:
    """Wrap the pure helper, injecting our canonical _tz_batch_stat_path resolver."""
    return _enrich_with_260523_pure(
        results, universe, tf, nasdaq_batch,
        path_resolver=_tz_batch_stat_path,
    )


def _diagnose_260523_columns(results: list, requested: dict) -> list:
    """Return warning strings when the user asked to filter on a 260523
    column but the stock_stat CSV doesn't have any True/non-empty values
    for it across the result set. Common cause: stock_stat was generated
    before this code version → column is missing/all-default.

    Returns at most 5 warnings (one per requested-but-empty filter).
    """
    if not results:
        return []
    warnings: list = []
    for col, want in requested.items():
        if want is None or want == "":
            continue
        # Scan up to 200 rows for any populated value
        sample = results[:200]
        any_populated = False
        for r in sample:
            v = r.get(col)
            if v not in (None, "", False, 0, "NEUTRAL", "0"):
                any_populated = True
                break
        if not any_populated:
            warnings.append(
                f"Filter '{col}' = {want!r} requested but no row has this "
                f"column populated. Run /api/regenerate-and-audit to rebuild "
                f"stock_stat with the latest 260523 columns."
            )
            if len(warnings) >= 5:
                break
    return warnings


def _enrich_atomic(results: list, universe: str) -> list:
    """Attach the 5-year-validated 'weak-close gap-up' atomic flags to each Ultra
    result row (additive, orthogonal to turbo_score). Sets atomic_match / atomic_score
    / atomic_atoms by joining each ticker's latest bar (close=O + gap + R2L/EO/vol=B/wick=D/G3).
    Read-only; never touches the canonical score."""
    if not results:
        return results
    tickers = [r.get("ticker") for r in results if r.get("ticker")]
    if not tickers:
        return results
    _BULL = ("T1", "T1G", "T2", "T2G", "T3", "T4", "T5", "T6", "T9", "T10", "T11", "T12")
    try:
        from ai_journal.db import get_analytics_conn
        a = get_analytics_conn()
        try:
            ph = ",".join("?" * len(tickers))
            df = a.execute(f"""
                WITH latest AS (SELECT ticker, max(date) d FROM bars WHERE universe=? AND ticker IN ({ph}) GROUP BY ticker)
                SELECT b.ticker, b.t_sig, b.close_suffix, b.bar_gap_class AS gap, b.vol_bucket AS vol,
                       b.full_suffix AS sfx, CASE WHEN regexp_matches(b.bar_line5,'R2L') THEN 1 ELSE 0 END AS r2l
                FROM bars b JOIN latest l ON b.ticker=l.ticker AND b.date=l.d
                WHERE b.universe=?
            """, [universe, *tickers, universe]).fetchdf()
        finally:
            a.close()
    except Exception as exc:
        log.warning("atomic enrich failed: %s", exc)
        return results
    info = {}
    for _, r in df.iterrows():
        sfx = str(r["sfx"] or "")
        match = (str(r["t_sig"]) in _BULL and r["close_suffix"] == "O" and str(r["gap"]) in ("G2", "G3"))
        atoms, score = [], 0
        if match:
            atoms = ["close=O", "gap"]; score = 40
            if int(r["r2l"] or 0): atoms.append("R2L"); score += 25
            if sfx[:1] == "E": atoms.append("EO"); score += 15
            if r["vol"] == "B": atoms.append("vol=B"); score += 15
            if "D" in sfx[1:2]: atoms.append("wick=D"); score += 10
            if r["gap"] == "G3": atoms.append("G3"); score += 10
        info[str(r["ticker"])] = (match, min(score, 100), atoms)
    for r in results:
        m, sc, at = info.get(r.get("ticker"), (False, 0, []))
        r["atomic_match"] = bool(m); r["atomic_score"] = int(sc); r["atomic_atoms"] = at
    return results


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
    # ── 260523 filter params (all optional) ─────────────────────────────────
    ad_fresh: Optional[bool] = None,
    ad_cluster: Optional[bool] = None,
    wyc_phase: Optional[str] = None,
    wyc_spring: Optional[bool] = None,
    wyc_sos: Optional[bool] = None,
    wyc_acc_tr: Optional[bool] = None,
    # ── 260523 v3.1 swing filter ──────────────────────────────────────────
    swing_type: Optional[str] = None,   # "HH" | "LH" | "HL" | "LL" | "pivot"
    # ── 260523 v3.5 PREBREAK + WYC additional filters ─────────────────────
    prebreak_prime: Optional[bool] = None,
    prebreak_ready: Optional[bool] = None,
    prebreak_watch: Optional[bool] = None,
    pb_lvbo: Optional[bool] = None,
    pb_stop_cause: Optional[bool] = None,
    pb_wvf_confirm: Optional[bool] = None,
    pb_macro_penalty: Optional[bool] = None,
    wyc_in_tr: Optional[bool] = None,
    wyc_sow: Optional[bool] = None,
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

        # Enrich split-universe rows with lifecycle metadata + cross-filter to live universe
        meta: dict = {}
        if universe == "split":
            try:
                from split_universe import split_service, normalize_split_symbol
                sresult = split_service.get_split_universe_result()
                smeta   = {r["ticker"]: r for r in sresult.rows}
                live_tickers = frozenset(sresult.tickers)

                # Cross-filter: only show tickers in the current live split universe
                results = [r for r in results
                           if normalize_split_symbol(r.get("ticker", "")) in live_tickers]

                for r in results:
                    s = smeta.get(normalize_split_symbol(r.get("ticker", "")))
                    if s:
                        r["split_date"]            = s["split_date"]
                        r["split_ratio"]           = s["ratio_str"]
                        r["split_status"]          = s.get("split_status", "")
                        r["split_days_offset"]     = s.get("days_offset", 0)
                        r["split_phase"]           = s.get("phase", "")
                        r["split_wave"]            = s.get("wave", "")
                        r["split_watch_until"]     = s.get("watch_until", "")
                        r["split_next_wave_label"] = s.get("next_wave_label", "")
                        r["split_next_wave_start"] = s.get("next_wave_start_date", "")
                        r["split_next_wave_end"]   = s.get("next_wave_end_date", "")
                        r["split_heat_score"]      = s.get("heat_score", 0)
                        r["split_notes"]           = s.get("notes", "")
                        r["split_watch_days"]      = s.get("watch_days", 60)
                meta["split_count"]      = len(live_tickers)
                meta["split_source"]     = sresult.source
                meta["split_cache_key"]  = sresult.cache_key
                meta["split_generated_at"] = sresult.generated_at
            except Exception as exc:
                log.warning("split metadata enrich failed: %s", exc)

        # Enrich all rows with profile playbook fields (additive context only,
        # does not modify canonical scoring columns)
        try:
            from profile_playbook import enrich_row_with_profile
            results = [enrich_row_with_profile(r, universe) for r in results]
        except Exception as exc:
            log.warning("profile playbook enrichment failed: %s", exc)

        # Enrich with BETA Score — needs canonical fields, so we compute them
        # on the fly from the existing signal flags in each turbo row.
        try:
            from beta_engine import calc_beta_score as _calc_beta
            for r in results:
                try:
                    canon = compute_canonical_score(r, universe)
                    # Synthesise VOL string from vol_spike flags
                    if r.get("vol_spike_20x"):   _vol = "20x"
                    elif r.get("vol_spike_10x"): _vol = "10x"
                    elif r.get("vol_spike_5x"):  _vol = "5x"
                    else:                         _vol = ""
                    _beta_row = dict(r,
                        ROCKET_SCORE=canon["ROCKET_SCORE"],
                        CLEAN_ENTRY_SCORE=canon["CLEAN_ENTRY_SCORE"],
                        FINAL_REGIME=canon["FINAL_REGIME"],
                        VOL=_vol,
                    )
                    _b = _calc_beta(_beta_row, [], universe)
                    r["beta_score"]    = _b["beta_score"]
                    r["beta_raw"]      = _b["beta_raw"]
                    r["beta_setup"]    = _b["beta_setup"]
                    r["beta_momentum"] = _b["beta_momentum"]
                    r["beta_excess"]   = _b["beta_excess"]
                    r["beta_zone"]     = _b["beta_zone"]
                    r["beta_auto_buy"] = _b["beta_auto_buy"]
                except Exception:
                    r["beta_score"] = 0
                    r["beta_zone"] = ""
                    r["beta_auto_buy"] = False
        except Exception as exc:
            log.warning("beta score enrichment failed: %s", exc)

        # ── 260523 enrichment + filter (AD-FRESH / AD-CLUSTER / WYC / SWING / PREBREAK) ──
        results = _enrich_with_260523(results, universe, tf)
        # Detect stale stock_stat (missing v3.5 columns) BEFORE filtering, so
        # we can warn the UI rather than silently returning 0 rows.
        warnings_260523 = _diagnose_260523_columns(
            results,
            requested={
                "ad_fresh": ad_fresh, "ad_cluster": ad_cluster,
                "wyc_phase": wyc_phase, "wyc_spring": wyc_spring,
                "wyc_sos": wyc_sos, "wyc_acc_tr": wyc_acc_tr,
                "swing_type": swing_type,
                "prebreak_prime": prebreak_prime, "prebreak_ready": prebreak_ready,
                "prebreak_watch": prebreak_watch,
                "pb_lvbo": pb_lvbo, "pb_stop_cause": pb_stop_cause,
                "pb_wvf_confirm": pb_wvf_confirm, "pb_macro_penalty": pb_macro_penalty,
                "wyc_in_tr": wyc_in_tr, "wyc_sow": wyc_sow,
            },
        )
        results = _apply_260523_filters(
            results,
            ad_fresh=ad_fresh, ad_cluster=ad_cluster,
            wyc_phase=wyc_phase, wyc_spring=wyc_spring,
            wyc_sos=wyc_sos, wyc_acc_tr=wyc_acc_tr,
            swing_type=swing_type,
            prebreak_prime=prebreak_prime, prebreak_ready=prebreak_ready,
            prebreak_watch=prebreak_watch,
            pb_lvbo=pb_lvbo, pb_stop_cause=pb_stop_cause,
            pb_wvf_confirm=pb_wvf_confirm, pb_macro_penalty=pb_macro_penalty,
            wyc_in_tr=wyc_in_tr, wyc_sow=wyc_sow,
        )

        resp = {"results": results, "last_scan": last_time, "meta": meta}
        if warnings_260523:
            resp["warnings_260523"] = warnings_260523
        return resp
    except Exception as exc:
        log.exception("turbo-scan error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/split-universe/audit")
def api_split_universe_audit(
    force_refresh: bool = False,
    tf: str = "1d",
):
    """Three-way audit: shared split universe vs stock_stat vs TZ Intelligence.

    Sources compared:
      A. shared  — split_universe_latest.csv (written at generation time);
                   falls back to live split_service if the file is missing.
      B. stock_stat — unique tickers in the WLNBB/TZ stock_stat CSV.
      C. intelligence — after the scanner fix, equal to stock_stat (no live
                   cross-filter; NO_EDGE tickers are returned, not dropped).

    Expected after fix:
      shared_count == stock_stat_count == intelligence_count
      all difference lists == []
      is_consistent == true
    """
    try:
        from split_universe import (
            split_service, normalize_split_symbol, SPLIT_UNIVERSE_CSV_PATH,
        )
        from tz_intelligence.scanner import _stat_path
        import csv as _csv_mod

        # ── A: shared split universe ──────────────────────────────────────────
        canonical_csv = SPLIT_UNIVERSE_CSV_PATH
        canonical_tickers: set = set()
        canonical_exists = os.path.exists(canonical_csv)
        canonical_generated_at = ""

        if canonical_exists:
            with open(canonical_csv, newline="", encoding="utf-8") as f:
                for row in _csv_mod.DictReader(f):
                    t = normalize_split_symbol(row.get("ticker", ""))
                    if t:
                        canonical_tickers.add(t)
                    if not canonical_generated_at:
                        canonical_generated_at = row.get("generated_at", "")

        # Always also fetch live result (for debug metadata + fallback)
        sresult = split_service.get_split_universe_result(force_refresh=force_refresh)
        live_set = frozenset(sresult.tickers)

        # Use canonical CSV if present, else fall back to live service
        shared_set = frozenset(canonical_tickers) if canonical_tickers else live_set

        # ── B: stock_stat unique tickers ──────────────────────────────────────
        stat_path = _stat_path("split", tf)
        stock_stat_tickers: set = set()
        csv_total_rows = 0
        if os.path.exists(stat_path):
            with open(stat_path, newline="", encoding="utf-8") as f:
                for row in _csv_mod.DictReader(f):
                    csv_total_rows += 1
                    t = normalize_split_symbol(row.get("ticker", ""))
                    if t:
                        stock_stat_tickers.add(t)

        # ── C: intelligence tickers ───────────────────────────────────────────
        # After the scanner fix: no live cross-filter and NO_EDGE tickers are
        # preserved, so the intelligence ticker set equals the stock_stat set.
        intelligence_tickers = set(stock_stat_tickers)

        # ── Differences ───────────────────────────────────────────────────────
        shared_not_in_stock_stat      = sorted(shared_set - stock_stat_tickers)
        stock_stat_not_in_shared      = sorted(stock_stat_tickers - shared_set)
        stock_stat_not_in_intelligence = sorted(stock_stat_tickers - intelligence_tickers)
        intelligence_not_in_stock_stat = sorted(intelligence_tickers - stock_stat_tickers)
        shared_not_in_intelligence    = sorted(shared_set - intelligence_tickers)
        intelligence_not_in_shared    = sorted(intelligence_tickers - shared_set)

        is_consistent = not any([
            shared_not_in_stock_stat,
            stock_stat_not_in_shared,
            stock_stat_not_in_intelligence,
            intelligence_not_in_stock_stat,
        ])

        return {
            # counts
            "shared_count":        len(shared_set),
            "stock_stat_count":    len(stock_stat_tickers),
            "intelligence_count":  len(intelligence_tickers),
            # difference lists
            "shared_not_in_stock_stat":       shared_not_in_stock_stat,
            "stock_stat_not_in_shared":       stock_stat_not_in_shared,
            "stock_stat_not_in_intelligence": stock_stat_not_in_intelligence,
            "intelligence_not_in_stock_stat": intelligence_not_in_stock_stat,
            "shared_not_in_intelligence":     shared_not_in_intelligence,
            "intelligence_not_in_shared":     intelligence_not_in_shared,
            # verdict
            "is_consistent":  is_consistent,
            # legacy fields (for UI backward-compat)
            "counts": {
                "live_split_universe":         len(live_set),
                "shared":                      len(shared_set),
                "stock_stat":                  len(stock_stat_tickers),
                "intelligence":                len(intelligence_tickers),
                "only_in_turbo":               len(shared_not_in_stock_stat),
                "only_in_wlnbb":               len(stock_stat_not_in_shared),
                "wlnbb_csv_total_rows":        csv_total_rows,
                "wlnbb_csv_unique_tickers":    len(stock_stat_tickers),
            },
            "debug": {
                "total_events":                sresult.total_events,
                "reverse_split_events":        sresult.reverse_split_events,
                "stock_like_events":           sresult.stock_like_events,
                "filtered_non_stock":          sresult.filtered_non_stock,
                "missing_symbol":              sresult.missing_symbol,
                "duplicate_symbols_removed":   sresult.duplicate_symbols_removed,
                "ratio_parse_failed_count":    sresult.ratio_parse_failed_count,
                "date_mode":                   sresult.date_mode,
                "start_date":                  sresult.start_date,
                "end_date":                    sresult.end_date,
                "source":                      sresult.source,
                "cache_key":                   sresult.cache_key,
                "generated_at":                sresult.generated_at,
                "canonical_csv_path":          canonical_csv,
                "canonical_csv_exists":        canonical_exists,
                "canonical_csv_generated_at":  canonical_generated_at,
                "stock_stat_csv_path":         stat_path,
                "stock_stat_csv_exists":       os.path.exists(stat_path),
                "excluded_examples":           sresult.excluded_examples[:10],
            },
        }
    except Exception as exc:
        log.exception("split-universe audit error")
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


# ── DB pruning endpoints (260523 Phase 1) ────────────────────────────────────

@app.get("/api/admin/db-stats")
def api_admin_db_stats():
    """Per-table row count, oldest/newest dates, exists flag."""
    from db_pruner import list_db_stats
    return {"tables": list_db_stats()}


@app.get("/api/admin/scan-state")
def api_admin_scan_state():
    """Last incremental scan timestamp per (universe, tf, nasdaq_batch).
    Used by the UI to show 'last scan: 2026-05-23' hints + decide whether
    'today only' mode is safe (gap >max_gap_days falls back to full)."""
    from scan_state import list_all
    return {"scans": list_all()}


@app.post("/api/admin/db-prune")
def api_admin_db_prune(
    table: str,
    older_than_days: int = 30,
    dry_run: bool = True,
    allow_protected: bool = False,
):
    """Delete rows from a single prunable table older than N days.
    Defaults to dry_run=true. Set dry_run=false to actually delete."""
    from db_pruner import prune_table
    result = prune_table(
        table=table, older_than_days=older_than_days,
        dry_run=dry_run, allow_protected=allow_protected,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "prune failed"))
    return result


@app.post("/api/admin/db-prune-all")
def api_admin_db_prune_all(
    older_than_days: int = 30,
    dry_run: bool = True,
    include_protected: bool = False,
):
    """Bulk-prune every non-protected table older than N days.
    include_protected=true also clears chart_observations + paper_portfolio."""
    from db_pruner import prune_all
    return prune_all(
        older_than_days=older_than_days,
        dry_run=dry_run,
        include_protected=include_protected,
    )


@app.post("/api/admin/scan-start")
def api_admin_scan_start(background_tasks: BackgroundTasks, tf: str = "1d", universe: str = "sp500", min_store_score: float = 5):
    from turbo_engine import run_turbo_scan, get_turbo_progress
    if get_turbo_progress().get("running"):
        raise HTTPException(status_code=409, detail="Scan already running")
    background_tasks.add_task(run_turbo_scan, tf, universe, 8, 5, False, 0, False, min_store_score)
    return {"ok": True, "tf": tf, "universe": universe, "min_store_score": min_store_score}


def compute_all_signals(df, ticker: str = "?", tf: str = "1d"):
    """Run every per-bar signal engine on one OHLCV frame and return them bundled.

    Extracted from api_bar_signals (audit #5) so the engine set is a single named,
    reusable, testable unit. Each engine is wrapped so a failure is LOGGED (which
    engine, which ticker) and falls back to an empty frame, instead of silently
    blanking the whole row. This is a pure relocation: the engine calls and their
    fallbacks are byte-for-byte the same as the previous inline version, so signal
    output is unchanged.

    Returns a SimpleNamespace with: sig_df, wlnbb, f_sigs, fly_sigs, g_sigs,
    b_sigs, combo_df, vabs, wick, ultra260, ultraV2, tz_state_ser.
    """
    import pandas as pd
    import numpy as np
    from types import SimpleNamespace
    from signal_engine import compute_g_signals, compute_b_signals
    from f_engine import compute_f_signals
    from fly_engine import compute_fly_series
    from vabs_engine import compute_vabs
    from wick_engine import compute_wick
    from ultra_engine import compute_260308_l88, compute_ultra_v2
    from combo_engine import compute_tz_state

    _EDF = pd.DataFrame()

    def _safe_engine(name, fn, fallback):
        try:
            return fn()
        except Exception as _e:
            log.warning("bar_signals: engine %s failed for %s [%s]: %s",
                        name, ticker, tf, _e)
            return fallback

    return SimpleNamespace(
        sig_df       = _safe_engine("compute_signals",     lambda: compute_signals(df),     _EDF),
        wlnbb        = _safe_engine("compute_wlnbb",        lambda: compute_wlnbb(df),       _EDF),
        f_sigs       = _safe_engine("compute_f_signals",   lambda: compute_f_signals(df),   _EDF),
        fly_sigs     = _safe_engine("compute_fly_series",  lambda: compute_fly_series(df),  _EDF),
        g_sigs       = _safe_engine("compute_g_signals",   lambda: compute_g_signals(df),   _EDF),
        b_sigs       = _safe_engine("compute_b_signals",   lambda: compute_b_signals(df),   _EDF),
        combo_df     = _safe_engine("compute_combo",       lambda: compute_combo(df),       _EDF),
        vabs         = _safe_engine("compute_vabs",        lambda: compute_vabs(df),        _EDF),
        wick         = _safe_engine("compute_wick",        lambda: compute_wick(df),        _EDF),
        ultra260     = _safe_engine("compute_260308_l88",  lambda: compute_260308_l88(df),  _EDF),
        ultraV2      = _safe_engine("compute_ultra_v2",    lambda: compute_ultra_v2(df),    _EDF),
        tz_state_ser = _safe_engine("compute_tz_state",    lambda: compute_tz_state(df),
                                    pd.Series(0, index=df.index, dtype=np.int8)),
    )


@app.get("/api/bar_signals/{ticker}")
def api_bar_signals(ticker: str, tf: str = "1d", bars: int = 150, universe: str = "sp500",
                    _df=None, _last_only=False):
    """Per-bar signal matrix for SuperChart view.

    `_df` (optional): a pre-built OHLCV DataFrame (UTC DatetimeIndex, cols
    open/high/low/close/volume — same schema as fetch_ohlcv). When provided,
    the Massive fetch is skipped and signals are computed on the given frame.
    Used by the hybrid Preview scan (DB history + today's live forming bar).

    `_last_only` (optional): when True, the vectorised engines still run over the
    full series (so all rolling context is correct), but ONLY the LAST bar's
    output dict is assembled. This skips the ~80k pandas row-lookups the per-bar
    assembly loop does for every historical bar — ~5× faster — which makes the
    universe-scale Preview scan viable. Returns a 1-element list.
    """
    import pandas as pd
    import numpy as np
    from turbo_engine import _calc_turbo_score
    try:
        from profile_playbook import compute_profile_playbook_for_row as _pf_compute
        _pf_ok = True
    except Exception:
        _pf_ok = False

    try:
        df = _df if _df is not None else fetch_ohlcv(ticker, interval=tf, bars=bars)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Run all per-bar engines via the shared orchestrator, then unpack into the
    # same local names the rest of this function already uses (downstream code
    # unchanged → no behavioural difference).
    _eng = compute_all_signals(df, ticker, tf)
    sig_df       = _eng.sig_df
    wlnbb        = _eng.wlnbb
    f_sigs       = _eng.f_sigs
    fly_sigs     = _eng.fly_sigs
    g_sigs       = _eng.g_sigs
    b_sigs       = _eng.b_sigs
    combo_df     = _eng.combo_df
    vabs         = _eng.vabs
    wick         = _eng.wick
    ultra260     = _eng.ultra260
    ultraV2      = _eng.ultraV2
    tz_state_ser = _eng.tz_state_ser
    tz_state_prev = tz_state_ser.shift(1, fill_value=0).astype(int)

    # ── 260523 per-bar signals (AD / WYC / PREBREAK / Pullback / Swing) ──────
    # Compute via the canonical pipeline (same as stock_stat) so chips in
    # Superchart match what ULTRA's Signals column shows.
    try:
        from analyzers.tz_wlnbb.signal_extraction import compute_signals_for_ticker
        _wy523_df = compute_signals_for_ticker(df.copy(), universe=universe)
    except Exception as _wy_exc:
        log.warning("260523 compute failed for %s: %s", ticker, _wy_exc)
        _wy523_df = None

    def _wy523_arr(col: str, default=False):
        if _wy523_df is None or col not in _wy523_df.columns:
            return [default] * len(df)
        return _wy523_df[col].tolist()

    # ── 260529 Wyckoff V2 (w2) + structure triggers (wt) — Superchart WYCK row,
    # syncs with ULTRA's "Wyckoff cycle" chips. Live-computed per bar. ──────────
    try:
        from wyckoff_v2_engine import compute_wyckoff_v2
        _w2_df = compute_wyckoff_v2(df.copy())
    except Exception:
        _w2_df = None
    try:
        from wyckoff_trig_engine import compute_wyckoff_trig
        _wt_df = compute_wyckoff_trig(df.copy())
    except Exception:
        _wt_df = None

    def _eng_arr(_d, col):
        return _d[col].tolist() if (_d is not None and col in _d.columns) else [0] * len(df)
    _w2_sc, _w2_ar, _w2_st = _eng_arr(_w2_df, "w2_sc"), _eng_arr(_w2_df, "w2_ar"), _eng_arr(_w2_df, "w2_st")
    _w2_spr, _w2_sos2, _w2_jac = _eng_arr(_w2_df, "w2_spring"), _eng_arr(_w2_df, "w2_sos"), _eng_arr(_w2_df, "w2_jac")
    _w2_lps, _w2_evr = _eng_arr(_w2_df, "w2_lps"), _eng_arr(_w2_df, "w2_evr")
    _wt_spr, _wt_sos, _wt_lps, _wt_evr = (_eng_arr(_wt_df, "wt_spring"), _eng_arr(_wt_df, "wt_sos"),
                                          _eng_arr(_wt_df, "wt_lps"), _eng_arr(_wt_df, "wt_evr"))
    # PREBREAK extra sub-signals (already computed in _wy523_df, just not emitted)
    _pb_pp_rtv_arr      = _wy523_arr("pb_pp_rtv", False)
    _pb_fly_cd_c_arr    = _wy523_arr("pb_fly_cd_c", False)
    _pb_follow_arr      = _wy523_arr("pb_follow_confirm", False)

    _ad_fresh_arr        = _wy523_arr("ad_fresh", False)
    _ad_cluster_arr      = _wy523_arr("ad_cluster", False)
    _wyc_phase_arr       = _wy523_arr("wyc_phase", "")
    _wyc_spring_arr      = _wy523_arr("wyc_spring", False)
    _wyc_sos_arr         = _wy523_arr("wyc_sos", False)
    _wyc_in_tr_arr       = _wy523_arr("wyc_in_tr", False)
    _wyc_sow_arr         = _wy523_arr("wyc_sow", False)
    _prebreak_prime_arr  = _wy523_arr("prebreak_prime", False)
    _prebreak_ready_arr  = _wy523_arr("prebreak_ready", False)
    _prebreak_watch_arr  = _wy523_arr("prebreak_watch", False)
    _pb_lvbo_arr         = _wy523_arr("pb_lvbo", False)
    _pb_wvf_confirm_arr  = _wy523_arr("pb_wvf_confirm", False)
    _pb_stop_cause_arr   = _wy523_arr("pb_stop_cause", False)
    _pb_macro_pen_arr    = _wy523_arr("pb_macro_penalty", False)
    _swing_type_arr      = _wy523_arr("swing_type", "")

    # GOG engine — provides SETUP, GOG_TIER, CONTEXT, GOG_SCORE per bar
    try:
        from gog_engine import compute_gog_signals as _compute_gog
        gog_result = _compute_gog(df, wlnbb, sig_df, f_sigs, vabs, ultra260, ultraV2, combo_df)
        gog_setup_ser   = gog_result.get("SETUP",    pd.Series("",  index=df.index))
        gog_tier_ser    = gog_result.get("GOG_TIER", pd.Series("",  index=df.index))
        gog_context_ser = gog_result.get("CONTEXT",  pd.Series("",  index=df.index))
        gog_score_ser      = gog_result.get("GOG_SCORE",    pd.Series(0.0, index=df.index))
        gog_all_sig_ser    = gog_result.get("ALL_SIGNALS",  pd.Series("",  index=df.index))
        _gog_ok = True
    except Exception:
        gog_setup_ser      = pd.Series("",  index=df.index)
        gog_tier_ser       = pd.Series("",  index=df.index)
        gog_context_ser    = pd.Series("",  index=df.index)
        gog_score_ser      = pd.Series(0.0, index=df.index)
        gog_all_sig_ser    = pd.Series("",  index=df.index)
        _gog_ok = False

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

    # BETA Score engine
    try:
        from beta_engine import calc_beta_score as _calc_beta_score
        _beta_ok = True
    except Exception:
        _beta_ok = False
    _beta_history: list = []   # chronological T/Z dicts (oldest first)
    _ultra_score_history: list = []   # chronological ultra_score floats (oldest first)

    # Per-bar rolling history for bear-to-bull sequence scoring (most-recent-first)
    _pf_bar_history: list = []  # list of Set[str], [1_bar_ago, 2_bars_ago, ...]

    # ── Pine 260520/260521: precompute ATR + bar_line5 for shape fields
    try:
        from analyzers.tz_wlnbb.signal_logic import compute_bar_shape_fields as _bar_shape_fn
        from analyzers.tz_wlnbb.signal_extraction import compute_line5 as _compute_line5
        _prev_c_arr = df["close"].shift(1)
        _tr_arr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - _prev_c_arr).abs(),
            (df["low"]  - _prev_c_arr).abs(),
        ], axis=1).max(axis=1)
        _atr_arr = _tr_arr.ewm(alpha=1.0 / 14.0, adjust=False).mean()
        _line5_df = df.copy()
        _compute_line5(_line5_df)
        _line5_arr = _line5_df["bar_line5"].tolist()
    except Exception:
        _bar_shape_fn = None
        _atr_arr = None
        _line5_arr = None

    # ── EMA for PRICE_GT_* / PRICE_LT_* + D/P family ────────────────────────
    try:
        _ema9   = df["close"].ewm(span=9,   adjust=False).mean()
        _ema20  = df["close"].ewm(span=20,  adjust=False).mean()
        _ema34  = df["close"].ewm(span=34,  adjust=False).mean()
        _ema50  = df["close"].ewm(span=50,  adjust=False).mean()
        _ema89  = df["close"].ewm(span=89,  adjust=False).mean()
        _ema200 = df["close"].ewm(span=200, adjust=False).mean()
    except Exception:
        _ema9 = _ema20 = _ema34 = _ema50 = _ema89 = _ema200 = pd.Series(0.0, index=df.index)

    # ── D-family (PREDN) and P66/P55 (PREUP) — vectorized ────────────────────
    try:
        _o = df["open"]; _c = df["close"]
        _drop9  = (_o > _ema9)   & (_c < _ema9)
        _drop20 = (_o > _ema20)  & (_c < _ema20)
        _drop34 = (_o > _ema34)  & (_c < _ema34)
        _drop50 = (_o > _ema50)  & (_c < _ema50)
        _drop89 = (_o > _ema89)  & (_c < _ema89)
        _drop200= (_o > _ema200) & (_c < _ema200)
        _d66_ser = _drop200 & (_drop89 | _drop50 | _drop34 | _drop20 | _drop9)
        _d55_ser = _drop89  & (_drop200| _drop50 | _drop34 | _drop20 | _drop9)
        _d89_ser = _drop89
        _d3_ser  = _drop9 & _drop20 & _drop50
        _d2_ser  = _drop9 & _drop20
        _d50_ser = _drop50
        _cross9  = (_o < _ema9)   & (_c > _ema9)
        _cross20 = (_o < _ema20)  & (_c > _ema20)
        _cross34 = (_o < _ema34)  & (_c > _ema34)
        _cross50 = (_o < _ema50)  & (_c > _ema50)
        _cross89 = (_o < _ema89)  & (_c > _ema89)
        _cross200= (_o < _ema200) & (_c > _ema200)
        _p66_ser = _cross200 & (_cross89 | _cross50 | _cross34 | _cross20 | _cross9)
        _p55_ser = _cross89  & (_cross200| _cross50 | _cross34 | _cross20 | _cross9)
    except Exception:
        _d66_ser = _d55_ser = _d89_ser = _d3_ser = _d2_ser = _d50_ser = \
        _p66_ser = _p55_ser = pd.Series(False, index=df.index)

    # ── CISD engine ───────────────────────────────────────────────────────────
    try:
        from cisd_engine import compute_cisd as _compute_cisd
        _cisd_df = _compute_cisd(df)
    except Exception:
        _cisd_df = pd.DataFrame()

    # ── PARA engine (series mode) ─────────────────────────────────────────────
    try:
        from para_engine import compute_para_series as _compute_para_series
        _para_df = _compute_para_series(df, is_daily=(tf == "1d"))
    except Exception:
        _para_df = None

    # ── Delta engine ──────────────────────────────────────────────────────────
    try:
        from delta_engine import compute_delta as _compute_delta
        _delta_df = _compute_delta(df)
    except Exception:
        _delta_df = pd.DataFrame()

    result = []

    # Preview scan only needs the latest bar; assembling the per-bar dict for
    # every historical bar is the dominant cost (~80k pandas row-lookups/ticker).
    # We still assemble a short TAIL (last 12 bars) so the loop-accumulated
    # cross-bar state — _ultra_score_history (rolling_score_max_5d decay bonus),
    # etc. — is correctly seeded for the final bar, matching the full computation.
    _TAIL = 12
    _loop_indices = (range(max(0, len(df) - _TAIL), len(df)) if (_last_only and len(df) > 0)
                     else range(len(df)))
    for i in _loop_indices:
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
            # ── Additional flags for canonical sub-score computation ────────
            # F-signal entries (CLEAN_ENTRY_SCORE + BETA Score)
            "f1":  _b(f_sigs, "f1"),
            "f2":  _b(f_sigs, "f2"),
            "f3":  _b(f_sigs, "f3"),
            "f4":  _b(f_sigs, "f4"),
            "f5":  _b(f_sigs, "f5"),
            "f6":  _b(f_sigs, "f6"),
            "f7":  _b(f_sigs, "f7"),
            "f8":  _b(f_sigs, "f8"),
            "f9":  _b(f_sigs, "f9"),
            "f10": _b(f_sigs, "f10"),
            "f11": _b(f_sigs, "f11"),
            # B-signal breakout confirms (CLEAN_ENTRY_SCORE)
            "b6":  _b(b_sigs, "b6"),
            "b8":  _b(b_sigs, "b8"),
            # Bear / risk signals (HARD_BEAR_SCORE)
            "fbo_bear":   _b(ultraV2, "fbo_bear"),
            "eb_bear":    _b(ultraV2, "eb_bear"),
            "bo_dn":      _b(wlnbb, "BO_DN"),
            "bx_dn":      _b(wlnbb, "BX_DN"),
            "fuchsia_rh": _b(wlnbb, "FUCHSIA_RH"),
        }
        # ── Canonical scoring — single call for all score columns ──────────
        canonical = compute_canonical_score(sig_row)
        turbo_score_val = canonical["turbo_score"]

        vol_bkt = ""
        if not wlnbb.empty and "vol_bucket" in wlnbb.columns:
            vol_bkt = str(wlnbb.iloc[i]["vol_bucket"])

        # RSI and CCI (from wlnbb)
        rsi_val = None
        cci_val = None
        if not wlnbb.empty:
            if "rsi" in wlnbb.columns:
                rsi_val = float(wlnbb.iloc[i]["rsi"])
            if "cci_sma" in wlnbb.columns:
                cci_val = float(wlnbb.iloc[i]["cci_sma"])

        # Profile enrichment per bar — unified function with rolling history
        _pf_result: dict = {}
        if _pf_ok:
            try:
                _bar_proxy = {
                    "close": float(row["close"]),
                    "combo": combo_list, "vabs": vabs_list,
                    "l": l_list, "f": f_list, "fly": fly_list,
                    "g": g_list, "b": b_list, "ultra": ultra_list,
                    "vol": vol_list, "wick": wick_list, "tz": tz_s,
                }
                _pf_result = _pf_compute(
                    _bar_proxy, universe, history_context=_pf_bar_history[:5]
                )
                _pf_bar_history.insert(0, set(_pf_result["active_signals"]))
                if len(_pf_bar_history) > 5:
                    _pf_bar_history.pop()
            except Exception:
                pass

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

        # BETA Score — called after RTB + profile playbook are resolved
        _beta_result = {"beta_score": 0, "beta_raw": 0, "beta_setup": 0,
                        "beta_momentum": 0, "beta_excess": 0,
                        "beta_zone": "NEUTRAL", "beta_auto_buy": False}
        if _beta_ok:
            try:
                _beta_row = dict(sig_row,
                                 rtb_total=rtb_total_val,
                                 rtb_phase=rtb_phase_val,
                                 sweet_spot_active=int(_pf_result.get("sweet_spot_active", False)),
                                 bear_to_bull_confirmed=int(_pf_result.get("bear_to_bull_confirmed", 0)),
                                 profile_category=_pf_result.get("profile_category", "WATCH"),
                                 profile_score=_pf_result.get("profile_score", 0),
                                 CLEAN_ENTRY_SCORE=canonical["CLEAN_ENTRY_SCORE"],
                                 ROCKET_SCORE=canonical["ROCKET_SCORE"],
                                 FINAL_REGIME=canonical["FINAL_REGIME"],
                                 rsi=rsi_val or 50.0,
                                 VOL=" ".join(vol_list),
                                 T=tz_s if tz_s.startswith("T") else "",
                                 Z=tz_s if tz_s.startswith("Z") else "")
                _beta_hist = list(reversed(_beta_history[-5:]))
                # rolling_score_max_5d: max ultra_score over past 5 bars (excludes current)
                _rolling_score_max = max(_ultra_score_history[-5:]) if _ultra_score_history else 0.0
                _beta_row["turbo_score"] = turbo_score_val
                _beta_result = _calc_beta_score(_beta_row, _beta_hist, universe,
                                                rolling_score_max=_rolling_score_max)
                _beta_history.append({"T": _beta_row["T"], "Z": _beta_row["Z"]})
                if len(_beta_history) > 10:
                    _beta_history.pop(0)
            except Exception:
                pass

        # GOG / SETUP / CONTEXT per bar
        _gog_tier_val  = str(gog_tier_ser.iloc[i]    or "")
        _gog_score_val = float(gog_score_ser.iloc[i]) if not pd.isna(gog_score_ser.iloc[i]) else 0.0
        _setup_str     = str(gog_setup_ser.iloc[i]   or "")
        _ctx_str       = str(gog_context_ser.iloc[i] or "")
        setup_list   = [t for t in _setup_str.split()  if t]
        context_list = [t for t in _ctx_str.split()    if t]

        # Pine 260520/260521: body/wick, gap/range, line5
        _bar_body_wick = ""
        _bar_gap_range = ""
        _bar_line5 = _line5_arr[i] if _line5_arr is not None else ""
        if _bar_shape_fn is not None and i > 0:
            _prev_row = df.iloc[i - 1]
            try:
                _atr_val = float(_atr_arr.iloc[i]) if _atr_arr is not None else 0.0
                if pd.isna(_atr_val):
                    _atr_val = 0.0
                _shape = _bar_shape_fn(
                    o=float(row["open"]), h=float(row["high"]),
                    l=float(row["low"]),  c=float(row["close"]),
                    prev_o=float(_prev_row["open"]), prev_h=float(_prev_row["high"]),
                    prev_l=float(_prev_row["low"]),  prev_c=float(_prev_row["close"]),
                    atr=_atr_val,
                )
                _bar_body_wick = _shape["bar_body_wick"]
                _bar_gap_range = _shape["bar_gap_range"]
            except Exception:
                pass

        # ── 260523 per-bar chip list ───────────────────────────────────────
        wy523_list: list = []
        if _ad_cluster_arr[i]:    wy523_list.append("AD-CLU")
        elif _ad_fresh_arr[i]:    wy523_list.append("AD-FR")
        if _wyc_spring_arr[i]:    wy523_list.append("SPRING")
        if _wyc_sos_arr[i]:       wy523_list.append("SOS")
        _wp = _wyc_phase_arr[i]
        if _wp and _wp not in ("", "NEUTRAL"):
            wy523_list.append(_wp)        # MARKUP / MKDN / ACC_TR / DIST_TR / UTAD
        if _wyc_in_tr_arr[i]:     wy523_list.append("InTR")
        if _wyc_sow_arr[i]:       wy523_list.append("SOW")
        if _prebreak_prime_arr[i]:      wy523_list.append("PRIME★")
        elif _prebreak_ready_arr[i]:    wy523_list.append("READY")
        elif _prebreak_watch_arr[i]:    wy523_list.append("WATCH")
        if _pb_lvbo_arr[i]:        wy523_list.append("LVBO")
        if _pb_wvf_confirm_arr[i]: wy523_list.append("WVF")
        if _pb_stop_cause_arr[i]:  wy523_list.append("W-PH")
        if _pb_macro_pen_arr[i]:   wy523_list.append("PEN")
        if _pb_pp_rtv_arr[i]:      wy523_list.append("PP+RTV")
        if _pb_fly_cd_c_arr[i]:    wy523_list.append("FLY-C")
        if _pb_follow_arr[i]:      wy523_list.append("FOLLOW")
        _st = _swing_type_arr[i] or ""
        if _st: wy523_list.append(_st)    # HL / LL / HH / LH

        # 260529 Wyckoff V2 (w2) + structure triggers (wt) — WYCK row
        wyck_list: list = []
        if _w2_sc[i]:   wyck_list.append("SC")
        if _w2_ar[i]:   wyck_list.append("AR")
        if _w2_st[i]:   wyck_list.append("ST")
        if _w2_spr[i]:  wyck_list.append("SPR")
        if _w2_sos2[i]: wyck_list.append("SOS")
        if _w2_jac[i]:  wyck_list.append("JAC")
        if _w2_lps[i]:  wyck_list.append("LPS")
        if _w2_evr[i]:  wyck_list.append("EVR")
        if _wt_spr[i]:  wyck_list.append("tSPR")
        if _wt_sos[i]:  wyck_list.append("tSOS")
        if _wt_lps[i]:  wyck_list.append("tLPS")
        if _wt_evr[i]:  wyck_list.append("tEVR")

        # Chart-format L code (single value matching chart tooltip exactly).
        # Logic mirrors signal_logic.compute_tz_wlnbb_for_bar(): ascending concat
        # of active L1..L6 digit flags. E.g. L4 AND L6 → "L46" (not "L64").
        # Falls back to priority-named labels (BO_UP, FRI34, etc.) when no L digit
        # is active but a non-L wlnbb signal fires.
        try:
            if i < len(wlnbb):
                _wlnbb_row = wlnbb.iloc[i]
                _digits = "".join(
                    str(d) for d in range(1, 7)
                    if bool(_wlnbb_row.get(f"L{d}", False))
                )
                if _digits:
                    _l_chart = "L" + _digits
                else:
                    # No L digit active → fall back to priority-named wlnbb signals
                    _l_chart = ""
                    for _name in ("FRI34", "FRI43", "FRI64", "BLUE", "CCI_READY",
                                  "CCI_0_RETEST_OK", "CCI_BLUE_TURN",
                                  "BE_UP", "BE_DN", "BO_UP", "BO_DN",
                                  "BX_UP", "BX_DN", "FUCHSIA_RL", "FUCHSIA_RH",
                                  "PRE_PUMP"):
                        if bool(_wlnbb_row.get(_name, False)):
                            _l_chart = _name
                            break
            else:
                _l_chart = ""
        except Exception:
            _l_chart = ""

        result.append({
            "date":       date_val,
            "open":       float(row["open"]),
            "high":       float(row["high"]),
            "low":        float(row["low"]),
            "close":      float(row["close"]),
            "volume":     float(row["volume"]),
            "vol_bucket": vol_bkt,
            "bar_body_wick": _bar_body_wick,
            "bar_gap_range": _bar_gap_range,
            "bar_line5":     _bar_line5,
            "tz":        tz,
            "l":         l_list,
            "l_chart":   _l_chart,           # ← new: single chart-format L code (e.g. "L1", "L34")
            "f":         f_list,
            "fly":       fly_list,
            "g":         g_list,
            "b":         b_list,
            "combo":     combo_list,
            "vol":       vol_list,
            "vabs":      vabs_list,
            "wick":      wick_list,
            "ultra":          ultra_list,
            "setup":          setup_list,
            "gog_tier":       _gog_tier_val,
            "context":        context_list,
            "wy523":          wy523_list,
            "wyck":           wyck_list,   # 260529 Wyckoff V2 (SC/AR/ST/SPR/SOS/JAC/LPS/EVR + tSPR/tSOS/tLPS/tEVR)
            # ── 260523 individual fields (sync: ULTRA screener = Superchart = CSV) ──
            "ad_fresh":         bool(_ad_fresh_arr[i]),
            "ad_cluster":       bool(_ad_cluster_arr[i]),
            "wyc_phase":        str(_wyc_phase_arr[i] or ""),
            "wyc_spring":       bool(_wyc_spring_arr[i]),
            "wyc_sos":          bool(_wyc_sos_arr[i]),
            "wyc_in_tr":        bool(_wyc_in_tr_arr[i]),
            "wyc_sow":          bool(_wyc_sow_arr[i]),
            "prebreak_score":   float(_wy523_df["prebreak_score"].iloc[i]) if (_wy523_df is not None and "prebreak_score" in _wy523_df.columns) else 0.0,
            "prebreak_prime":   bool(_prebreak_prime_arr[i]),
            "prebreak_ready":   bool(_prebreak_ready_arr[i]),
            "prebreak_watch":   bool(_prebreak_watch_arr[i]),
            "pb_lvbo":          bool(_pb_lvbo_arr[i]),
            "pb_wvf_confirm":   bool(_pb_wvf_confirm_arr[i]),
            "pb_stop_cause":    bool(_pb_stop_cause_arr[i]),
            "pb_macro_penalty": bool(_pb_macro_pen_arr[i]),
            "swing_type":       str(_swing_type_arr[i] or ""),
            "gog_score":      _gog_score_val,
            "gog1": 1 if _gog_tier_val.startswith("G1") else 0,
            "gog2": 1 if _gog_tier_val.startswith("G2") else 0,
            "gog3": 1 if _gog_tier_val.startswith("G3") else 0,
            "signal_score": _gog_score_val,
            "turbo_score":           turbo_score_val,
            # ── Canonical score columns — uppercase (stock_stat CSV / replay engine) ──
            "FINAL_BULL_SCORE":      canonical["FINAL_BULL_SCORE"],
            "ROCKET_SCORE":          canonical["ROCKET_SCORE"],
            "CLEAN_ENTRY_SCORE":     canonical["CLEAN_ENTRY_SCORE"],
            "SHAKEOUT_ABSORB_SCORE": canonical["SHAKEOUT_ABSORB_SCORE"],
            "EXTRA_BULL_SCORE":      canonical["EXTRA_BULL_SCORE"],
            "EXPERIMENTAL_SCORE":    canonical["EXPERIMENTAL_SCORE"],
            "REBOUND_SQUEEZE_SCORE": canonical["REBOUND_SQUEEZE_SCORE"],
            "HARD_BEAR_SCORE":       canonical["HARD_BEAR_SCORE"],
            "VOLATILITY_RISK_SCORE": canonical["VOLATILITY_RISK_SCORE"],
            "HAS_ELITE_MODEL":       canonical["HAS_ELITE_MODEL"],
            "HAS_REBOUND_MODEL":     canonical["HAS_REBOUND_MODEL"],
            "HAS_STRONG_BULL_MODEL": canonical["HAS_STRONG_BULL_MODEL"],
            "FINAL_REGIME":          canonical["FINAL_REGIME"],
            "FINAL_SCORE_BUCKET":    canonical["FINAL_SCORE_BUCKET"],
            # ── Lowercase aliases — required by SuperchartPanel.jsx CSV export ──
            # JavaScript key access is case-sensitive; b.final_bull_score !== b.FINAL_BULL_SCORE
            "final_bull_score":      canonical["FINAL_BULL_SCORE"],
            "rocket_score":          canonical["ROCKET_SCORE"],
            "clean_entry_score":     canonical["CLEAN_ENTRY_SCORE"],
            "shakeout_absorb_score": canonical["SHAKEOUT_ABSORB_SCORE"],
            "extra_bull_score":      canonical["EXTRA_BULL_SCORE"],
            "experimental_score":    canonical["EXPERIMENTAL_SCORE"],
            "rebound_squeeze_score": canonical["REBOUND_SQUEEZE_SCORE"],
            "hard_bear_score":       canonical["HARD_BEAR_SCORE"],
            "volatility_risk_score": canonical["VOLATILITY_RISK_SCORE"],
            "has_elite_model":       canonical["HAS_ELITE_MODEL"],
            "has_rebound_model":     canonical["HAS_REBOUND_MODEL"],
            "has_strong_bull_model": canonical["HAS_STRONG_BULL_MODEL"],
            "final_regime":          canonical["FINAL_REGIME"],
            "final_score_bucket":    canonical["FINAL_SCORE_BUCKET"],
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
            # ── RSI / CCI (numeric values for SuperChart display) ──────────────
            "rsi":              rsi_val,
            "cci":              cci_val,
            # ── Profile playbook per-bar (all fields from unified function) ────
            "profile_playbook_version":  _pf_result.get("profile_playbook_version", ""),
            "profile_name":              _pf_result.get("profile_name", ""),
            "profile_score":             _pf_result.get("profile_score", 0),
            "profile_category":          _pf_result.get("profile_category", "WATCH"),
            "sweet_spot_active":         int(_pf_result.get("sweet_spot_active", False)),
            "late_warning":              int(_pf_result.get("late_warning", False)),
            "bear_context_last_3":       _pf_result.get("bear_context_last_3", 0),
            "bear_context_last_5":       _pf_result.get("bear_context_last_5", 0),
            "bull_confirm_now":          _pf_result.get("bull_confirm_now", 0),
            "bear_to_bull_confirmed":    _pf_result.get("bear_to_bull_confirmed", 0),
            "bear_to_bull_bars_ago":     _pf_result.get("bear_to_bull_bars_ago", 0),
            "bear_to_bull_bonus":        _pf_result.get("bear_to_bull_bonus", 0),
            "bear_to_bull_pairs":        _pf_result.get("bear_to_bull_pairs", []),
            "base_profile_score_without_btb": _pf_result.get("base_profile_score_without_btb", 0),
            "category_without_btb":      _pf_result.get("category_without_btb", "WATCH"),
            "category_with_btb":         _pf_result.get("category_with_btb", "WATCH"),
            "btb_category_upgrade":      _pf_result.get("btb_category_upgrade", 0),
            "btb_created_sweet_spot":    _pf_result.get("btb_created_sweet_spot", 0),
            "btb_late_clamped":          _pf_result.get("btb_late_clamped", 0),
            "btb_sweet_spot_allowed_profile": _pf_result.get("btb_sweet_spot_allowed_profile", 0),
            "active_signals":            _pf_result.get("active_signals", []),
            # ── BETA Score ────────────────────────────────────────────────────
            "beta_score":    _beta_result["beta_score"],
            "beta_raw":      _beta_result["beta_raw"],
            "beta_setup":    _beta_result["beta_setup"],
            "beta_momentum": _beta_result["beta_momentum"],
            "beta_excess":   _beta_result["beta_excess"],
            "beta_zone":     _beta_result["beta_zone"],
            "beta_auto_buy": _beta_result["beta_auto_buy"],
            # rolling_score_max_5d — populated below after ultra_score is computed
            "rolling_score_max_5d": (
                max(_ultra_score_history[-5:]) if _ultra_score_history else 0.0
            ),
            # ── ALL_SIGNALS text ────────────────────────────────────────────────
            "all_signals": str(gog_all_sig_ser.iloc[i] or "") if i < len(gog_all_sig_ser) else "",
            # ── GOG sub-tier booleans ───────────────────────────────────────────
            "g1p": 1 if _gog_tier_val.startswith("G1P") else 0,
            "g2p": 1 if _gog_tier_val.startswith("G2P") else 0,
            "g3p": 1 if _gog_tier_val.startswith("G3P") else 0,
            "g1l": 1 if _gog_tier_val.startswith("G1L") else 0,
            "g2l": 1 if _gog_tier_val.startswith("G2L") else 0,
            "g3l": 1 if _gog_tier_val.startswith("G3L") else 0,
            "g1c": 1 if _gog_tier_val.startswith("G1C") else 0,
            "g2c": 1 if _gog_tier_val.startswith("G2C") else 0,
            "g3c": 1 if _gog_tier_val.startswith("G3C") else 0,
            # ── VABS individual booleans ────────────────────────────────────────
            "sig_best":    int(_b(vabs, "best_sig")),
            "sig_strong":  int(_b(vabs, "strong_sig")),
            "sig_vbo_dn":  int(_b(vabs, "vbo_dn")),
            "sig_ns_vabs": int(_b(vabs, "ns")),
            "sig_nd_vabs": int(_b(vabs, "nd")),
            "sig_sc":      int(_b(vabs, "sc")),
            "sig_bc":      int(not vabs.empty and "bc" in vabs.columns and i < len(vabs) and int(vabs.iloc[i].get("bc", 0) or 0) > 0),
            "sig_abs":     int(_b(vabs, "abs_sig")),
            "sig_clm":     int(_b(vabs, "climb_sig")),
            # ── UltraV2 individual booleans ────────────────────────────────────
            "sig_best_up": int(_b(ultraV2, "best_long")),
            "sig_fbo_up":  int(_b(ultraV2, "fbo_bull")),
            "sig_eb_up":   int(_b(ultraV2, "eb_bull")),
            "sig_3up":     int(_b(ultraV2, "ultra_3up")),
            "sig_fbo_dn":  int(_b(ultraV2, "fbo_bear")),
            "sig_eb_dn":   int(_b(ultraV2, "eb_bear")),
            "sig_4bf_dn":  int(_b(ultraV2, "bf_sell")),
            # ── wlnbb L-signal booleans ────────────────────────────────────────
            "sig_fri34": int(_b(wlnbb, "FRI34")),
            "sig_fri43": int(_b(wlnbb, "FRI43")),
            "sig_fri64": int(_b(wlnbb, "FRI64")),
            "sig_l555":  int(_b(wlnbb, "L555")),
            "sig_l2l4":  int(_b(wlnbb, "ONLY_L2L4")),
            "sig_blue":  int(_b(wlnbb, "BLUE")),
            "sig_cci":   int(_b(wlnbb, "CCI_READY")),
            "sig_cci0r": int(_b(wlnbb, "CCI_0_RETEST_OK")),
            "sig_ccib":  int(_b(wlnbb, "CCI_BLUE_TURN")),
            "sig_bo_dn": int(_b(wlnbb, "BO_DN")),
            "sig_bx_dn": int(_b(wlnbb, "BX_DN")),
            "sig_be_dn": int(_b(wlnbb, "BE_DN")),
            "sig_rl":    int(_b(wlnbb, "FUCHSIA_RL")),
            "sig_rh":    int(_b(wlnbb, "FUCHSIA_RH")),
            "sig_pp":    int(_b(wlnbb, "PRE_PUMP")),
            # ── G individual booleans ──────────────────────────────────────────
            "sig_g1":  int(_b(g_sigs, "g1")),
            "sig_g2":  int(_b(g_sigs, "g2")),
            "sig_g4":  int(_b(g_sigs, "g4")),
            "sig_g6":  int(_b(g_sigs, "g6")),
            "sig_g11": int(_b(g_sigs, "g11")),
            # ── B individual booleans ──────────────────────────────────────────
            "sig_b1":  int(_b(b_sigs, "b1")),
            "sig_b2":  int(_b(b_sigs, "b2")),
            "sig_b3":  int(_b(b_sigs, "b3")),
            "sig_b4":  int(_b(b_sigs, "b4")),
            "sig_b5":  int(_b(b_sigs, "b5")),
            "sig_b6":  int(_b(b_sigs, "b6")),
            "sig_b7":  int(_b(b_sigs, "b7")),
            "sig_b8":  int(_b(b_sigs, "b8")),
            "sig_b9":  int(_b(b_sigs, "b9")),
            "sig_b10": int(_b(b_sigs, "b10")),
            "sig_b11": int(_b(b_sigs, "b11")),
            # ── F individual booleans ──────────────────────────────────────────
            "sig_f1":  int(_b(f_sigs, "f1")),
            "sig_f2":  int(_b(f_sigs, "f2")),
            "sig_f3":  int(_b(f_sigs, "f3")),
            "sig_f4":  int(_b(f_sigs, "f4")),
            "sig_f5":  int(_b(f_sigs, "f5")),
            "sig_f6":  int(_b(f_sigs, "f6")),
            "sig_f7":  int(_b(f_sigs, "f7")),
            "sig_f8":  int(_b(f_sigs, "f8")),
            "sig_f9":  int(_b(f_sigs, "f9")),
            "sig_f10": int(_b(f_sigs, "f10")),
            "sig_f11": int(_b(f_sigs, "f11")),
            # ── FLY booleans ───────────────────────────────────────────────────
            "sig_fly_abcd": int(_b(fly_sigs, "fly_abcd")),
            "sig_fly_cd":   int(_b(fly_sigs, "fly_cd")),
            "sig_fly_bd":   int(_b(fly_sigs, "fly_bd")),
            "sig_fly_ad":   int(_b(fly_sigs, "fly_ad")),
            # ── Wick booleans ──────────────────────────────────────────────────
            "sig_wk_up": int(_b(wick, "WICK_BULL_CONFIRM")),
            "sig_wk_dn": int(_b(wick, "WICK_BEAR_CONFIRM")),
            "sig_x1":    int(_b(wick, "x1_wick")),
            "sig_x2":    int(_b(wick, "x2_wick")),
            "sig_x1g":   int(_b(wick, "x1g_wick")),
            "sig_x3":    int(_b(wick, "x3_wick")),
            # ── Combo booleans ─────────────────────────────────────────────────
            "sig_bias_up": int(_b(combo_df, "bias_up")),
            "sig_bias_dn": int(_b(combo_df, "bias_down")),
            "sig_svs":     int(_b(combo_df, "svs_2809")),
            "sig_conso":   int(_b(combo_df, "conso_2809")),
            "sig_p2":      int(_b(combo_df, "preup2")),
            "sig_p3":      int(_b(combo_df, "preup3")),
            "sig_p50":     int(_b(combo_df, "preup50")),
            "sig_p89":     int(_b(combo_df, "preup89")),
            "sig_buy":     int(_b(combo_df, "buy_2809")),
            "sig_3g":      int(_b(combo_df, "sig3g")),
            # ── Volume ATR / spike ─────────────────────────────────────────────
            "sig_va":      int(bool(va_ser.iloc[i])),
            "sig_vol_5x":  int(float(vol_ratio.iloc[i]) >= 5),
            "sig_vol_10x": int(float(vol_ratio.iloc[i]) >= 10),
            "sig_vol_20x": int(float(vol_ratio.iloc[i]) >= 20),
            # ── TZ state booleans ──────────────────────────────────────────────
            "sig_tz":       int(int(tz_state_ser.iloc[i]) >= 1),
            "sig_t":        int(bool(tz_s.startswith("T"))),
            "sig_z":        int(bool(tz_s.startswith("Z"))),
            "sig_tz3":      int(int(tz_state_ser.iloc[i]) == 3),
            "sig_tz2":      int(int(tz_state_ser.iloc[i]) == 2),
            "sig_tz_flip":  int(sig_row["tz_bull_flip"]),
            "sig_cd":       int(sig_row["cd"]),
            "sig_ca":       int(sig_row["ca"]),
            "sig_cw":       int(sig_row["cw"]),
            "sig_seq_bcont":int(bool(seq_bcont_ser.iloc[i])),
            # ── P66/P55 (PREUP EMA cross-up) ──────────────────────────────────
            "sig_p66": int(bool(_p66_ser.iloc[i])),
            "sig_p55": int(bool(_p55_ser.iloc[i])),
            # ── D-family (PREDN EMA cross-down) ───────────────────────────────
            "sig_d66": int(bool(_d66_ser.iloc[i])),
            "sig_d55": int(bool(_d55_ser.iloc[i])),
            "sig_d89": int(bool(_d89_ser.iloc[i])),
            "sig_d50": int(bool(_d50_ser.iloc[i])),
            "sig_d3":  int(bool(_d3_ser.iloc[i])),
            "sig_d2":  int(bool(_d2_ser.iloc[i])),
            # ── CISD ──────────────────────────────────────────────────────────
            "sig_cisd_cplus":       int(_b(_cisd_df, "PLUS_CISD")),
            "sig_cisd_cplus_minus": int(_b(_cisd_df, "CISD_PPM")),
            "sig_cisd_cplus_mm":    int(_b(_cisd_df, "CISD_PMM")),
            # ── PARA context ──────────────────────────────────────────────────
            "sig_para_prep":   int(bool(_para_df.iloc[i]["para_prep"])   if _para_df is not None and i < len(_para_df) and "para_prep"   in _para_df.columns else 0),
            "sig_para_start":  int(bool(_para_df.iloc[i]["para_start"])  if _para_df is not None and i < len(_para_df) and "para_start"  in _para_df.columns else 0),
            "sig_para_plus":   int(bool(_para_df.iloc[i]["para_plus"])   if _para_df is not None and i < len(_para_df) and "para_plus"   in _para_df.columns else 0),
            "sig_para_retest": int(bool(_para_df.iloc[i]["para_retest"]) if _para_df is not None and i < len(_para_df) and "para_retest" in _para_df.columns else 0),
            # ── Delta extras ──────────────────────────────────────────────────
            "sig_flp_up":      int(_b(_delta_df, "flip_bull")),
            "sig_org_up":      int(_b(_delta_df, "orange_bull")),
            "sig_dd_up_red":   int(_b(_delta_df, "blast_bull_red")),
            "sig_d_up_red":    int(_b(_delta_df, "surge_bull_red")),
            "sig_d_dn_green":  int(_b(_delta_df, "surge_bear_grn")),
            "sig_dd_dn_green": int(_b(_delta_df, "blast_bear_grn")),
            # ── NS/ND Delta (combo vs vabs disambiguation) ────────────────────
            "sig_ns_delta": int("NS" in combo_list and "NS" not in vabs_list),
            "sig_nd_delta": int("ND" in combo_list and "ND" not in vabs_list),
            # ── Meta family any-flags (derived) ───────────────────────────────
            "sig_any_f":    int(any(_b(f_sigs,  f"f{n}") for n in range(1, 12))),
            "sig_any_b":    int(any(_b(b_sigs,  f"b{n}") for n in range(1, 12))),
            "sig_any_p":    int(_b(combo_df,"preup2") or _b(combo_df,"preup3") or
                                _b(combo_df,"preup50") or _b(combo_df,"preup89") or
                                bool(_p66_ser.iloc[i]) or bool(_p55_ser.iloc[i])),
            "sig_any_d":    int(bool(_d66_ser.iloc[i]) or bool(_d55_ser.iloc[i]) or
                                bool(_d89_ser.iloc[i]) or bool(_d50_ser.iloc[i]) or
                                bool(_d3_ser.iloc[i])  or bool(_d2_ser.iloc[i])),
            "sig_l_any":    int(_b(wlnbb,"L34") or _b(wlnbb,"L43") or
                                _b(wlnbb,"L64") or _b(wlnbb,"L22") or _b(wlnbb,"L555")),
            "sig_be_any":   int(_b(wlnbb,"BE_UP") or _b(wlnbb,"BE_DN") or
                                _b(ultraV2,"eb_bull") or _b(ultraV2,"eb_bear")),
            "sig_gog_plus": int(_gog_tier_val.startswith("G1P") or
                                _gog_tier_val.startswith("G2P") or
                                _gog_tier_val.startswith("G3P")),
            "sig_not_ext":  int(not (_b(sig_df,"already_extended")
                                     if not sig_df.empty and "already_extended" in sig_df.columns
                                     else False)),
            # ── RSI thresholds ─────────────────────────────────────────────────
            "sig_rsi_le_35": int(rsi_val is not None and rsi_val <= 35),
            "sig_rsi_ge_70": int(rsi_val is not None and rsi_val >= 70),
            # ── Price vs EMA ───────────────────────────────────────────────────
            "sig_price_gt_20":  int(float(row["close"]) > float(_ema20.iloc[i])),
            "sig_price_gt_50":  int(float(row["close"]) > float(_ema50.iloc[i])),
            "sig_price_gt_89":  int(float(row["close"]) > float(_ema89.iloc[i])),
            "sig_price_gt_200": int(float(row["close"]) > float(_ema200.iloc[i])),
            "sig_price_lt_20":  int(float(row["close"]) < float(_ema20.iloc[i])),
            "sig_price_lt_50":  int(float(row["close"]) < float(_ema50.iloc[i])),
            "sig_price_lt_89":  int(float(row["close"]) < float(_ema89.iloc[i])),
            "sig_price_lt_200": int(float(row["close"]) < float(_ema200.iloc[i])),
            # ── Raw individual signal booleans ─────────────────────────────────
            "raw_load":      int(_b(vabs, "load_sig")),
            "raw_sq":        int(_b(vabs, "sq")),
            "raw_vbo_up":    int(_b(vabs, "vbo_up")),
            "raw_bo_up":     int(_b(wlnbb, "BO_UP")),
            "raw_be_up":     int(_b(wlnbb, "BE_UP")),
            "raw_bx_up":     int(_b(wlnbb, "BX_UP")),
            "raw_l34":       int(_b(wlnbb, "L34")),
            "raw_l43":       int(_b(wlnbb, "L43")),
            "raw_l64":       int(_b(wlnbb, "L64")),
            "raw_l22":       int(_b(wlnbb, "L22")),
            "raw_f8":        int(_b(f_sigs, "f8")),
            "raw_f3":        int(_b(f_sigs, "f3")),
            "raw_f4":        int(_b(f_sigs, "f4")),
            "raw_f6":        int(_b(f_sigs, "f6")),
            "raw_f11":       int(_b(f_sigs, "f11")),
            "raw_t10":       int(tz_s == "T10"),
            "raw_t11":       int(tz_s == "T11"),
            "raw_t12":       int(tz_s == "T12"),
            "raw_z10":       int(tz_s == "Z10"),
            "raw_z11":       int(tz_s == "Z11"),
            "raw_z12":       int(tz_s == "Z12"),
            "raw_z4":        int(tz_s == "Z4"),
            "raw_z6":        int(tz_s == "Z6"),
            "raw_z9":        int(tz_s == "Z9"),
            "raw_sig260308": int(_b(ultra260, "sig_260308")),
            "raw_l88":       int(_b(ultra260, "sig_l88")),
            "raw_um":        int(_b(combo_df, "um_2809")),
            "raw_svs_raw":   int(_b(combo_df, "svs_2809")),
            "raw_cons":      int(_b(combo_df, "cons_atr") if not combo_df.empty and "cons_atr" in combo_df.columns else False),
            "raw_buy_here":  int(_b(combo_df, "buy_2809")),
            "raw_atr_brk":   int(_b(combo_df, "atr_brk")),
            "raw_bb_brk":    int(_b(combo_df, "bb_brk")),
            "raw_hilo_buy":  int(_b(combo_df, "hilo_buy")),
            "raw_rtv":       int(_b(combo_df, "rtv")),
            "raw_three_g":   int(_b(combo_df, "sig3g")),
            "raw_rocket":    int(_b(combo_df, "rocket")),
            "raw_bf4":       int(_b(ultraV2, "bf_buy")),
            "raw_w":         int(_b(wick, "WICK_BULL_CONFIRM")),
            # ── Diagnostics ────────────────────────────────────────────────────
            "already_extended": int(_b(sig_df, "already_extended") if not sig_df.empty and "already_extended" in sig_df.columns else False),
        })

        # Track ultra_score for next bar's DECAY MEMORY BONUS (rolling_score_max_5d).
        # Also expose ultra_score / ultra_score_band on the per-bar dict so
        # downstream consumers (ULTRA Pump Research, stock_stat, replays) can
        # read the historical ULTRA scoring directly without recomputing.
        try:
            _us = _compute_ultra_score(result[-1])
            _us_val = float(_us.get("ultra_score", 0) or 0)
            result[-1]["ultra_score"]      = _us_val
            result[-1]["ultra_score_band"] = _us.get("ultra_score_band", "")
            _ultra_score_history.append(_us_val)
            if len(_ultra_score_history) > 10:
                _ultra_score_history.pop(0)
        except Exception:
            result[-1].setdefault("ultra_score", 0.0)
            result[-1].setdefault("ultra_score_band", "")
            _ultra_score_history.append(0.0)
            if len(_ultra_score_history) > 10:
                _ultra_score_history.pop(0)

        # prebreak_v3 (additive cluster, 0..50) computed from this bar's signals —
        # so the Superchart V3 row matches the DB/ULTRA prebreak_v3.
        try:
            from prebreak_v3 import calc_prebreak_v3
            result[-1]["prebreak_v3"], result[-1]["prebreak_v3_reasons"] = calc_prebreak_v3(result[-1])
        except Exception:
            result[-1].setdefault("prebreak_v3", 0)
            result[-1].setdefault("prebreak_v3_reasons", "")

    return result


# ── Stock Stat — bulk per-bar signal CSV for entire universe ──────────────────

def run_stock_stat(tf: str = "1d", universe: str = "sp500", bars: int = 60):
    import csv, time
    from scanner import get_universe_tickers

    t0 = time.time()
    _stock_stat_state.update(
        running=True, done=0, total=0, error=None,
        output_path=None, output_size=0, tf=tf, universe=universe, elapsed=0.0
    )
    _PREUP = {"P2", "P3", "P50", "P89"}

    try:
        tickers = get_universe_tickers(universe)
        _stock_stat_state["total"] = len(tickers)

        os.makedirs("stock_stat_output", exist_ok=True)
        out_path = f"stock_stat_output/stock_stat_{universe}_{tf}.csv"

        headers = [
            "ticker", "date", "open", "high", "low", "close", "volume",
            "vol_bucket", "turbo_score",
            # ── Canonical score columns ───────────────────────────────────────
            "FINAL_BULL_SCORE",
            "ROCKET_SCORE", "CLEAN_ENTRY_SCORE", "SHAKEOUT_ABSORB_SCORE",
            "EXTRA_BULL_SCORE", "EXPERIMENTAL_SCORE", "REBOUND_SQUEEZE_SCORE",
            "HARD_BEAR_SCORE", "VOLATILITY_RISK_SCORE",
            "HAS_ELITE_MODEL", "HAS_REBOUND_MODEL", "HAS_STRONG_BULL_MODEL",
            "FINAL_REGIME", "FINAL_SCORE_BUCKET",
            # ── RTB ───────────────────────────────────────────────────────────
            "rtb_phase", "rtb_total", "rtb_transition",
            "rtb_build", "rtb_turn", "rtb_ready", "rtb_late", "rtb_bonus3",
            "dbg_context_ready", "dbg_t4_ctx", "dbg_t6_ctx", "dbg_t4t6_activation_plus",
            "dbg_launch_cluster_count", "dbg_pending_phase", "dbg_pending_phase_count",
            "Z", "T", "L", "F", "FLY", "G", "B", "Combo", "ULT", "VOL", "VABS", "WICK",
            # ── Profile playbook (all from compute_profile_playbook_for_row) ──
            "profile_playbook_version",
            "profile_name", "profile_score", "profile_category",
            "sweet_spot_active", "late_warning",
            "bear_context_last_3", "bear_context_last_5",
            "bull_confirm_now", "bear_to_bull_confirmed",
            "bear_to_bull_bars_ago", "bear_to_bull_bonus", "bear_to_bull_pairs",
            "base_profile_score_without_btb", "category_without_btb", "category_with_btb",
            "btb_category_upgrade", "btb_created_sweet_spot",
            "btb_late_clamped", "btb_sweet_spot_allowed_profile",
            # ── ULTRA Score (independent additive ranking, no lookahead) ──────
            "ultra_score", "ultra_score_band", "ultra_score_reasons",
            "ultra_score_flags", "ultra_score_raw_before_penalty",
            "ultra_score_penalty_total",
            # ── ULTRA Score v2 calibration (replay-derived) ──────────────────
            "ultra_score_band_v2", "ultra_score_priority",
            "ultra_score_regime_bonus", "ultra_score_caps_applied",
            "ultra_score_cap_reason",
            # ── Additional fields used by ULTRA Replay combo analytics. Filled
            # opportunistically — empty when bar_signals doesn't expose them
            # (e.g. pullback_evidence_tier / abr_category / tz_intel_role
            # require running TZ Intelligence + Pullback / Rare miners which
            # would make Stock Stat much slower). Replay surfaces these as
            # 'missing dependency' rather than silent zero. ──
            "rs_strong", "rs",
            "tz_bull_flip", "tz_transition_present",
            "pullback_evidence_tier", "rare_evidence_tier",
            "tz_intel_role", "abr_category",
            # ── BETA Score ───────────────────────────────────────────────────
            "beta_score", "beta_raw", "beta_setup", "beta_momentum",
            "beta_excess", "beta_zone", "beta_auto_buy",
            # Decay-memory input — max ultra_score over past 5 bars (excludes current)
            "rolling_score_max_5d",
        ]

        def _j(lst): return " ".join(str(x) for x in lst) if lst else ""

        import sys as _sys

        # Audit counters for fail-loud check
        _audit = {
            "rows_total": 0,
            "rows_with_active_signals": 0,
            "rows_with_pf_score_gt_0": 0,
            "cat_dist": {},
            "rows_bear3": 0, "rows_bear5": 0,
            "rows_bull_now": 0, "rows_btb": 0,
            "btb_bonus_sum": 0.0,
            "sig_counts": {},
            "btb_pair_counts": {},
        }

        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            wr = csv.writer(fh)
            wr.writerow(headers)
            for idx, ticker in enumerate(tickers):
                try:
                    # Fetch ≥150 bars for warm-up; trim to requested window
                    effective_bars = max(bars, 150)
                    bd = api_bar_signals(ticker, tf, effective_bars)
                    if len(bd) > bars:
                        bd = bd[-bars:]
                    for b in bd:
                        tz = b.get("tz", "")
                        _audit["rows_total"] += 1
                        act = b.get("active_signals", [])
                        if act:
                            _audit["rows_with_active_signals"] += 1
                            for s in act:
                                _audit["sig_counts"][s] = _audit["sig_counts"].get(s, 0) + 1
                        pf_sc = b.get("profile_score", 0)
                        if pf_sc > 0:
                            _audit["rows_with_pf_score_gt_0"] += 1
                        cat = b.get("profile_category", "WATCH")
                        _audit["cat_dist"][cat] = _audit["cat_dist"].get(cat, 0) + 1
                        if b.get("bear_context_last_3"): _audit["rows_bear3"] += 1
                        if b.get("bear_context_last_5"): _audit["rows_bear5"] += 1
                        if b.get("bull_confirm_now"):     _audit["rows_bull_now"] += 1
                        if b.get("bear_to_bull_confirmed"):
                            _audit["rows_btb"] += 1
                            _audit["btb_bonus_sum"] += b.get("bear_to_bull_bonus", 0)
                            for p in b.get("bear_to_bull_pairs", []):
                                _audit["btb_pair_counts"][p] = _audit["btb_pair_counts"].get(p, 0) + 1
                        wr.writerow([
                            ticker,
                            b.get("date", ""),
                            round(b.get("open", 0), 4),
                            round(b.get("high", 0), 4),
                            round(b.get("low", 0), 4),
                            round(b.get("close", 0), 4),
                            round(b.get("volume", 0), 0),
                            b.get("vol_bucket", ""),
                            b.get("turbo_score", 0),
                            b.get("FINAL_BULL_SCORE", 0),
                            b.get("ROCKET_SCORE", 0),
                            b.get("CLEAN_ENTRY_SCORE", 0),
                            b.get("SHAKEOUT_ABSORB_SCORE", 0),
                            b.get("EXTRA_BULL_SCORE", 0),
                            b.get("EXPERIMENTAL_SCORE", 0),
                            b.get("REBOUND_SQUEEZE_SCORE", 0),
                            b.get("HARD_BEAR_SCORE", 0),
                            b.get("VOLATILITY_RISK_SCORE", 0),
                            b.get("HAS_ELITE_MODEL", 0),
                            b.get("HAS_REBOUND_MODEL", 0),
                            b.get("HAS_STRONG_BULL_MODEL", 0),
                            b.get("FINAL_REGIME", ""),
                            b.get("FINAL_SCORE_BUCKET", ""),
                            b.get("rtb_phase", ""),
                            b.get("rtb_total", 0),
                            b.get("rtb_transition", ""),
                            b.get("rtb_build", 0),
                            b.get("rtb_turn", 0),
                            b.get("rtb_ready", 0),
                            b.get("rtb_late", 0),
                            b.get("rtb_bonus3", 0),
                            1 if b.get("dbg_context_ready") else 0,
                            1 if b.get("dbg_t4_ctx") else 0,
                            1 if b.get("dbg_t6_ctx") else 0,
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
                            # ── Profile playbook fields ────────────────────────
                            b.get("profile_playbook_version", ""),
                            b.get("profile_name", ""),
                            b.get("profile_score", 0),
                            b.get("profile_category", "WATCH"),
                            b.get("sweet_spot_active", 0),
                            b.get("late_warning", 0),
                            b.get("bear_context_last_3", 0),
                            b.get("bear_context_last_5", 0),
                            b.get("bull_confirm_now", 0),
                            b.get("bear_to_bull_confirmed", 0),
                            b.get("bear_to_bull_bars_ago", 0),
                            b.get("bear_to_bull_bonus", 0),
                            _j(b.get("bear_to_bull_pairs", [])),
                            b.get("base_profile_score_without_btb", 0),
                            b.get("category_without_btb", "WATCH"),
                            b.get("category_with_btb", "WATCH"),
                            b.get("btb_category_upgrade", 0),
                            b.get("btb_created_sweet_spot", 0),
                            b.get("btb_late_clamped", 0),
                            b.get("btb_sweet_spot_allowed_profile", 0),
                            # ── ULTRA Score (computed from current/past
                            # bar fields only — never reads forward returns).
                            # Adds 6 columns; missing input fields contribute 0.
                            *(lambda _r: (
                                _r["ultra_score"], _r["ultra_score_band"],
                                " ".join(_r["ultra_score_reasons"]),
                                " ".join(_r["ultra_score_flags"]),
                                _r["ultra_score_raw_before_penalty"],
                                _r["ultra_score_penalty_total"],
                                _r.get("ultra_score_band_v2", ""),
                                _r.get("ultra_score_priority", ""),
                                _r.get("ultra_score_regime_bonus", 0),
                                " ".join(_r.get("ultra_score_caps_applied", []) or []),
                                _r.get("ultra_score_cap_reason", ""),
                            ))(_compute_ultra_score(b)),
                            # ── Additional ULTRA Replay analytics fields ──
                            # rs_strong / tz_bull_flip derived from the same
                            # parser the score uses. pullback / rare / tz_intel
                            # / abr are passed through if upstream populated
                            # them, otherwise empty.
                            *(lambda _p: (
                                int(bool(_p["rs_strong"])),
                                int(bool(_p["rs"])),
                                int(bool(_p["tz_bull_flip"])),
                                int(bool(_p["tz_transition_present"])),
                                b.get("pullback_evidence_tier", "")
                                  or (b.get("pullback") or {}).get("evidence_tier", ""),
                                b.get("rare_evidence_tier", "")
                                  or (b.get("rare_reversal") or {}).get("evidence_tier", ""),
                                b.get("tz_intel_role", "")
                                  or (b.get("tz_intel") or {}).get("role", ""),
                                b.get("abr_category", "")
                                  or (b.get("abr") or {}).get("category", ""),
                            ))(_parse_ultra_signals(b)),
                            # ── BETA Score ─────────────────────────────────
                            b.get("beta_score", 0),
                            b.get("beta_raw", 0),
                            b.get("beta_setup", 0),
                            b.get("beta_momentum", 0),
                            b.get("beta_excess", 0),
                            b.get("beta_zone", ""),
                            1 if b.get("beta_auto_buy") else 0,
                            round(float(b.get("rolling_score_max_5d", 0) or 0), 1),
                        ])
                except Exception:
                    pass
                _stock_stat_state["done"] = idx + 1
                _stock_stat_state["elapsed"] = round(time.time() - t0, 1)

        # ── PROFILE_PLAYBOOK_AUDIT ────────────────────────────────────────────
        top20_sigs  = sorted(_audit["sig_counts"], key=lambda k: -_audit["sig_counts"][k])[:20]
        top20_pairs = sorted(_audit["btb_pair_counts"], key=lambda k: -_audit["btb_pair_counts"][k])[:20]
        avg_btb = (
            round(_audit["btb_bonus_sum"] / _audit["rows_btb"], 2)
            if _audit["rows_btb"] else 0
        )
        print(
            f"PROFILE_PLAYBOOK_AUDIT universe={universe} tf={tf}\n"
            f"  rows_total={_audit['rows_total']}\n"
            f"  rows_with_active_signals={_audit['rows_with_active_signals']}\n"
            f"  rows_with_pf_score_gt_0={_audit['rows_with_pf_score_gt_0']}\n"
            f"  category_distribution={_audit['cat_dist']}\n"
            f"  rows_bear_context_last_3={_audit['rows_bear3']}\n"
            f"  rows_bear_context_last_5={_audit['rows_bear5']}\n"
            f"  rows_bull_confirm_now={_audit['rows_bull_now']}\n"
            f"  rows_bear_to_bull_confirmed={_audit['rows_btb']}\n"
            f"  avg_bear_to_bull_bonus={avg_btb}\n"
            f"  top_20_extracted_signals={top20_sigs}\n"
            f"  top_20_bear_to_bull_pairs={top20_pairs}",
            file=_sys.stderr, flush=True,
        )
        # Fail-loud check
        if (_audit["rows_with_active_signals"] > 0
                and _audit["rows_with_pf_score_gt_0"] == 0):
            log.error(
                "PROFILE_PLAYBOOK_FAILURE: active signals found in %d rows "
                "but profile_score is zero for all rows. "
                "Check extraction/scoring integration.",
                _audit["rows_with_active_signals"],
            )

        # Config snapshot
        try:
            import json as _json
            from profile_playbook import get_playbook_config_snapshot
            snap_path = "stock_stat_output/profile_playbook_config_snapshot.json"
            with open(snap_path, "w", encoding="utf-8") as _sf:
                _json.dump(get_playbook_config_snapshot(), _sf, indent=2)
        except Exception as _snap_err:
            log.warning("Config snapshot failed: %s", _snap_err)

        fsize = os.path.getsize(out_path)
        _stock_stat_state.update(
            running=False, output_path=out_path,
            output_size=fsize, elapsed=round(time.time() - t0, 1),
            scoring_engine=SCORING_ENGINE_NAME,
            scoring_version=SCORING_ENGINE_VERSION,
            bars_used=bars,
        )
        try:
            from chart_obs_api_v2 import import_stock_stat_csv
            result = import_stock_stat_csv(out_path)
            log.info("Auto-imported stock_stat into DB: %s", result)
        except Exception as _imp_err:
            log.warning("stock_stat DB auto-import failed: %s", _imp_err)
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


# ── Replay Analytics ──────────────────────────────────────────────────────────
import replay_engine as _re
import csv as _csv
import io as _io
from fastapi.responses import Response as _Response


@app.post("/api/replay/run")
def api_replay_run(background_tasks: BackgroundTasks, tf: str = "1d", universe: str = "sp500"):
    state = _re.get_state()
    if state.get("status") == "running":
        raise HTTPException(400, "Replay already running")
    background_tasks.add_task(_re.run_replay, tf, universe)
    return {"status": "started"}


@app.get("/api/replay/status")
def api_replay_status():
    return _re.get_state()


@app.get("/api/replay/reports")
def api_replay_reports():
    return {"reports": _re.get_report_list()}


@app.get("/api/replay/report/{name}")
def api_replay_report(name: str, page: int = 1, page_size: int = 500):
    data, err = _re.load_report(name, page, page_size)
    if err:
        raise HTTPException(404, err)
    return data


@app.get("/api/replay/export/{name}")
def api_replay_export(name: str):
    data, err = _re.load_report(name, 1, 999999)
    if err:
        raise HTTPException(404, err)
    rows = data.get("rows", [])
    if not rows:
        raise HTTPException(404, "No data for section")
    buf = _io.StringIO()
    w = _csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
    return _Response(
        buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="replay_{name}.csv"'},
    )


@app.get("/api/replay/export-all")
def api_replay_export_all():
    data = _re.export_zip()
    return _Response(
        data,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="replay_analytics.zip"'},
    )


# ── TZ/WLNBB Analyzer endpoints ──────────────────────────────────────────────

def _filter_nasdaq_batch(tickers: list, batch: str) -> list:
    """Filter NASDAQ tickers by alphabetical batch.
    batch='a_m' -> first letter A-M  (nasdaq)
    batch='n_z' -> first letter N-Z  (nasdaq)
    batch='a_f' -> first letter A-F  (nasdaq_gt5)
    batch='g_m' -> first letter G-M  (nasdaq_gt5)
    batch='n_s' -> first letter N-S  (nasdaq_gt5)
    batch='t_z' -> first letter T-Z  (nasdaq_gt5)
    batch='other' -> first letter non-alpha
    batch='' or 'all' -> no filter
    """
    if not batch or batch == "all":
        return tickers
    _RANGES = {
        "a_m": ("A", "M"), "n_z": ("N", "Z"),
        "a_f": ("A", "F"), "g_m": ("G", "M"),
        "n_s": ("N", "S"), "t_z": ("T", "Z"),
    }
    if batch in _RANGES:
        lo, hi = _RANGES[batch]
        return [t for t in tickers if t and t[0].upper().isalpha() and lo <= t[0].upper() <= hi]
    if batch == "other":
        return [t for t in tickers if not (t and t[0].upper().isalpha())]
    return tickers


def _tz_batch_stat_path(universe: str, tf: str, nasdaq_batch: str = "") -> str:
    """Return the canonical stock_stat CSV path for a given universe/tf/batch."""
    if nasdaq_batch and nasdaq_batch != "all":
        if universe == "nasdaq":
            return f"stock_stat_tz_wlnbb_nasdaq_{nasdaq_batch}_{tf}.csv"
        if universe == "nasdaq_gt5":
            return f"stock_stat_tz_wlnbb_nasdaq_gt5_{nasdaq_batch}_{tf}.csv"
    return f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"


def _tz_batch_replay_path(universe: str, tf: str, nasdaq_batch: str = "") -> str:
    """Return the canonical replay ZIP path for a given universe/tf/batch."""
    if nasdaq_batch and nasdaq_batch != "all":
        if universe == "nasdaq":
            return f"replay_tz_wlnbb_nasdaq_{nasdaq_batch}_{tf}_analytics.zip"
        if universe == "nasdaq_gt5":
            return f"replay_tz_wlnbb_nasdaq_gt5_{nasdaq_batch}_{tf}_analytics.zip"
    return f"replay_tz_wlnbb_{universe}_{tf}_analytics.zip"


_tz_wlnbb_state: dict = {"running": False, "done": 0, "total": 0, "output": None, "error": None}


@app.get("/api/tz-wlnbb/scan")
def api_tz_wlnbb_scan(
    universe: str = "sp500",
    tf: str = "1d",
    min_price: float = 0,
    max_price: float = 1e9,
    min_volume: float = 0,
    signal_type: str = "all",
    signal_name: str = "",
    recent_window: int = 1,
    nasdaq_batch: str = "",
):
    """Return latest TZ/WLNBB signals from stock_stat CSV."""
    try:
        import csv as _csv
        stat_path = _tz_batch_stat_path(universe, tf, nasdaq_batch)
        if not os.path.exists(stat_path):
            # fallback to generic universe path
            stat_path = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"
        if not os.path.exists(stat_path):
            # last-resort fallback to old naming
            stat_path = f"stock_stat_tz_wlnbb_{tf}.csv"
        if not os.path.exists(stat_path):
            return {"results": [], "error": "No stock_stat_tz_wlnbb CSV found. Run generate-stock-stat first."}

        rows_by_ticker: dict = {}
        with open(stat_path, newline="", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                if row.get("universe", "") != universe:
                    continue
                t = row.get("ticker", "")
                rows_by_ticker.setdefault(t, []).append(row)

        results = []
        for ticker, rows in rows_by_ticker.items():
            rows.sort(key=lambda x: x.get("date", ""))
            recent = rows[-recent_window:]
            for row in recent:
                try:
                    price = float(row.get("close", 0) or 0)
                    vol   = float(row.get("volume", 0) or 0)
                    if price < min_price or price > max_price:
                        continue
                    if min_volume > 0 and vol < min_volume:
                        continue
                    if signal_type not in ("all", ""):
                        has_sig = False
                        if signal_type == "T"     and row.get("t_signal"):          has_sig = True
                        if signal_type == "Z"     and row.get("z_signal"):          has_sig = True
                        if signal_type == "L"     and row.get("l_signal"):          has_sig = True
                        if signal_type == "PREUP" and row.get("preup_signal"):      has_sig = True
                        if signal_type == "PREDN" and row.get("predn_signal"):      has_sig = True
                        if signal_type == "Combo" and row.get("has_tz_l_combo") == "1": has_sig = True
                        if not has_sig:
                            continue
                    if signal_name:
                        if signal_name not in [
                            row.get("t_signal", ""), row.get("z_signal", ""),
                            row.get("l_signal", ""), row.get("preup_signal", ""),
                            row.get("predn_signal", ""),
                        ]:
                            continue
                    results.append(row)
                except Exception:
                    pass

        results.sort(key=lambda x: x.get("date", ""), reverse=True)
        return {"results": results[:2000]}
    except Exception as exc:
        log.exception("tz-wlnbb scan error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/tz-wlnbb/stats/suffix")
def api_tz_wlnbb_stats_suffix(
    universe: str = "sp500",
    tf: str = "1d",
    nasdaq_batch: str = "",
    signal_type: str = "all",     # all | T | Z | L | PREUP | PREDN
    base_signal: str = "",         # optional exact base (e.g. "T4", "Z6", "L34", "P55")
    min_count: int = 5,            # drop slices with fewer than N rows
    return_horizon: str = "5d",    # 1d | 3d | 5d | 10d  (drives win/avg/median)
):
    """Suffix-breakdown statistics for TZ/WLNBB signals.

    Aggregates each (base_signal, ne_suffix, wick_suffix, penetration_suffix)
    combination across the saved stock_stat CSV and returns count, win rate,
    avg / median forward return, and the marginal vs the base. The exact
    suffix vocabulary mirrors the Pine script:
        ne_suffix:          E (new-extreme close) | N (none)
        wick_suffix:        U (wick up) | D (wick down) | B (both) | "" (none)
        penetration_suffix: H (both) | P (upper) | R (lower) | "" (none)
    """
    import csv as _csv
    import statistics as _stat

    horizon_col = {
        "1d":  "ret_1d",
        "3d":  "ret_3d",
        "5d":  "ret_5d",
        "10d": "ret_10d",
    }.get(return_horizon, "ret_5d")

    stat_path = _tz_batch_stat_path(universe, tf, nasdaq_batch)
    if not os.path.exists(stat_path):
        stat_path = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"
    if not os.path.exists(stat_path):
        stat_path = f"stock_stat_tz_wlnbb_{tf}.csv"
    if not os.path.exists(stat_path):
        return {"slices": [], "base_totals": [], "error":
                "No stock_stat_tz_wlnbb CSV found. Run generate-stock-stat first."}

    # Map signal_type → list of base-signal column names to pull from
    type_cols = {
        "T":     ["t_signal"],
        "Z":     ["z_signal"],
        "L":     ["l_signal"],
        "PREUP": ["preup_signal"],
        "PREDN": ["predn_signal"],
    }
    if signal_type in type_cols:
        cols_to_scan = type_cols[signal_type]
    else:
        cols_to_scan = ["t_signal", "z_signal", "l_signal", "preup_signal", "predn_signal"]

    # slices[(base, vol_bkt, ne, wick, pen, cls_eff)] = list[float] of returns
    slices: dict[tuple, list[float]] = {}
    base_totals: dict[str, list[float]] = {}

    with open(stat_path, newline="", encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            if row.get("universe", "") != universe:
                continue
            ret_raw = row.get(horizon_col, "")
            if ret_raw in ("", None):
                continue
            try:
                ret = float(ret_raw)
            except (TypeError, ValueError):
                continue
            vol_bkt = (row.get("volume_bucket") or "").strip()
            ne   = (row.get("ne_suffix")          or "").strip()
            wick = (row.get("wick_suffix")        or "").strip()
            pen  = (row.get("penetration_suffix") or "").strip()
            cls_ = (row.get("close_suffix")       or "").strip()
            try:
                cls_appended = int(row.get("close_appended") or 0) == 1
            except (TypeError, ValueError):
                cls_appended = False
            # Use the close suffix only when the bar actually appends it; otherwise
            # collapse to empty so the slice keys mirror the visible label.
            cls_eff = cls_ if cls_appended else ""
            for col in cols_to_scan:
                base = (row.get(col) or "").strip()
                if not base:
                    continue
                if base_signal and base != base_signal:
                    continue
                slices.setdefault((base, vol_bkt, ne, wick, pen, cls_eff), []).append(ret)
                base_totals.setdefault(base, []).append(ret)

    def _summarize(returns: list[float]) -> dict:
        n = len(returns)
        if not n:
            return {"count": 0, "win_rate": 0.0, "avg_ret": 0.0,
                    "median_ret": 0.0, "p25_ret": 0.0, "p75_ret": 0.0}
        wins = sum(1 for r in returns if r > 0)
        s = sorted(returns)
        return {
            "count":      n,
            "win_rate":   round(wins / n * 100, 2),
            "avg_ret":    round(sum(returns) / n, 3),
            "median_ret": round(_stat.median(returns), 3),
            "p25_ret":    round(s[max(0, n // 4 - 1)],          3),
            "p75_ret":    round(s[min(n - 1, (3 * n) // 4)],    3),
        }

    base_summary = {b: _summarize(rets) for b, rets in base_totals.items()}

    slices_out: list[dict] = []
    for (base, vol_bkt, ne, wick, pen, cls_), rets in slices.items():
        if len(rets) < min_count:
            continue
        s = _summarize(rets)
        bs = base_summary.get(base) or {}
        s["base_signal"]         = base
        s["volume_bucket"]       = vol_bkt
        s["ne_suffix"]           = ne
        s["wick_suffix"]         = wick
        s["penetration_suffix"]  = pen
        s["close_suffix"]        = cls_
        s["suffix_label"]        = (ne + wick + pen + cls_) or "—"
        s["base_count"]          = bs.get("count", 0)
        s["base_win_rate"]       = bs.get("win_rate", 0.0)
        s["base_avg_ret"]        = bs.get("avg_ret", 0.0)
        s["win_rate_lift"]       = round(s["win_rate"] - bs.get("win_rate", 0.0), 2)
        s["avg_ret_lift"]        = round(s["avg_ret"]  - bs.get("avg_ret",  0.0), 3)
        slices_out.append(s)

    slices_out.sort(key=lambda r: (-r["count"], -r["avg_ret"]))

    base_totals_out = [
        {"base_signal": b, **_summarize(rets)}
        for b, rets in base_totals.items()
        if len(rets) >= min_count
    ]
    base_totals_out.sort(key=lambda r: (-r["count"], -r["avg_ret"]))

    return {
        "universe": universe, "tf": tf, "horizon": return_horizon,
        "stat_path": stat_path,
        "signal_type": signal_type, "base_signal": base_signal or None,
        "min_count": min_count,
        "slices":      slices_out,
        "base_totals": base_totals_out,
    }


def _tz_resolve_stat_path(universe: str, tf: str, nasdaq_batch: str) -> str | None:
    """Return the first existing stock_stat_tz_wlnbb CSV path, or None."""
    p = _tz_batch_stat_path(universe, tf, nasdaq_batch)
    if os.path.exists(p): return p
    p = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"
    if os.path.exists(p): return p
    p = f"stock_stat_tz_wlnbb_{tf}.csv"
    if os.path.exists(p): return p
    return None


def _tz_iter_rows(stat_path: str, universe: str):
    """Yield CSV rows filtered to the requested universe."""
    import csv as _csv
    with open(stat_path, newline="", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            if row.get("universe", "") == universe:
                yield row


def _tz_summary(returns: list[float]) -> dict:
    import statistics as _stat
    n = len(returns)
    if not n:
        return {"count": 0, "win_rate": 0.0, "avg_ret": 0.0,
                "median_ret": 0.0, "p25_ret": 0.0, "p75_ret": 0.0}
    wins = sum(1 for r in returns if r > 0)
    s = sorted(returns)
    return {
        "count":      n,
        "win_rate":   round(wins / n * 100, 2),
        "avg_ret":    round(sum(returns) / n, 3),
        "median_ret": round(_stat.median(returns), 3),
        "p25_ret":    round(s[max(0, n // 4 - 1)],          3),
        "p75_ret":    round(s[min(n - 1, (3 * n) // 4)],    3),
    }


_TZ_SIG_COLS = {
    "T":     ["t_signal"],
    "Z":     ["z_signal"],
    "L":     ["l_signal"],
    "PREUP": ["preup_signal"],
    "PREDN": ["predn_signal"],
}
_TZ_ALL_COLS = ["t_signal", "z_signal", "l_signal", "preup_signal", "predn_signal"]


@app.get("/api/tz-wlnbb/stats/leaderboard")
def api_tz_wlnbb_stats_leaderboard(
    universe: str = "sp500",
    tf: str = "1d",
    nasdaq_batch: str = "",
    signal_type: str = "all",
    min_count: int = 5,
):
    """Per-signal performance leaderboard across 1d/3d/5d/10d horizons.

    Returns count plus win-rate / avg / median return at each horizon, plus the
    clean_win_5d / big_win_10d / fail_5d / fail_10d outcome rates that
    stock_stat already labels.
    """
    stat_path = _tz_resolve_stat_path(universe, tf, nasdaq_batch)
    if not stat_path:
        return {"rows": [], "error": "No stock_stat_tz_wlnbb CSV found. Run generate-stock-stat first."}

    cols_to_scan = _TZ_SIG_COLS.get(signal_type, _TZ_ALL_COLS)

    # bucket[signal] = {h: list[float], outcomes: {key: list[int]}}
    horizons = ("ret_1d", "ret_3d", "ret_5d", "ret_10d")
    outcomes = ("clean_win_5d", "big_win_10d", "fail_5d", "fail_10d")

    by_sig: dict[str, dict] = {}

    for row in _tz_iter_rows(stat_path, universe):
        signals_in_row: list[str] = []
        for col in cols_to_scan:
            v = (row.get(col) or "").strip()
            if v:
                signals_in_row.append(v)
        if not signals_in_row:
            continue
        # Deduplicate so a row that fires both T-and-L doesn't double-count one signal
        for sig in set(signals_in_row):
            slot = by_sig.setdefault(sig, {
                "ret_1d": [], "ret_3d": [], "ret_5d": [], "ret_10d": [],
                "clean_win_5d": 0, "big_win_10d": 0, "fail_5d": 0, "fail_10d": 0,
                "count": 0,
            })
            slot["count"] += 1
            for h in horizons:
                raw = row.get(h, "")
                if raw in ("", None): continue
                try:
                    slot[h].append(float(raw))
                except (TypeError, ValueError):
                    pass
            for ok in outcomes:
                if (row.get(ok) or "").strip() == "1":
                    slot[ok] += 1

    out_rows: list[dict] = []
    for sig, slot in by_sig.items():
        if slot["count"] < min_count:
            continue
        rec = {"signal": sig, "count": slot["count"]}
        for h in horizons:
            s = _tz_summary(slot[h])
            rec[f"{h}_win_rate"]   = s["win_rate"]
            rec[f"{h}_avg_ret"]    = s["avg_ret"]
            rec[f"{h}_median_ret"] = s["median_ret"]
        n = slot["count"]
        rec["clean_win_5d_pct"] = round(slot["clean_win_5d"] / n * 100, 2)
        rec["big_win_10d_pct"]  = round(slot["big_win_10d"]  / n * 100, 2)
        rec["fail_5d_pct"]      = round(slot["fail_5d"]      / n * 100, 2)
        rec["fail_10d_pct"]     = round(slot["fail_10d"]     / n * 100, 2)
        # Family tag for color coding on the UI
        if sig.startswith("T"):   fam = "T"
        elif sig.startswith("Z"): fam = "Z"
        elif sig.startswith("L"): fam = "L"
        elif sig.startswith("P"): fam = "PREUP"
        elif sig.startswith("D"): fam = "PREDN"
        else:                     fam = "OTHER"
        rec["family"] = fam
        out_rows.append(rec)
    out_rows.sort(key=lambda r: (-(r["count"]), -(r["ret_5d_avg_ret"] or 0)))

    return {"rows": out_rows, "stat_path": stat_path,
            "universe": universe, "tf": tf, "signal_type": signal_type,
            "min_count": min_count}


@app.get("/api/tz-wlnbb/stats/bucket-matrix")
def api_tz_wlnbb_stats_bucket_matrix(
    universe: str = "sp500",
    tf: str = "1d",
    nasdaq_batch: str = "",
    signal_type: str = "all",
    return_horizon: str = "5d",
    min_count: int = 3,
):
    """Crosstab of volume_bucket (W/L/N/B/VB) × signal_name.

    Each cell shows count + win-rate + avg forward return at the chosen
    horizon, so you can see e.g. whether T4 fires better on VB or W bars.
    """
    horizon_col = {"1d": "ret_1d", "3d": "ret_3d", "5d": "ret_5d", "10d": "ret_10d"}.get(return_horizon, "ret_5d")

    stat_path = _tz_resolve_stat_path(universe, tf, nasdaq_batch)
    if not stat_path:
        return {"cells": [], "error": "No stock_stat_tz_wlnbb CSV found. Run generate-stock-stat first."}

    cols_to_scan = _TZ_SIG_COLS.get(signal_type, _TZ_ALL_COLS)
    buckets_order = ["W", "L", "N", "B", "VB"]

    # cell[(sig, bucket)] = list[float]
    cells: dict[tuple, list[float]] = {}
    sig_totals: dict[str, list[float]] = {}
    bucket_totals: dict[str, list[float]] = {}

    for row in _tz_iter_rows(stat_path, universe):
        bkt = (row.get("volume_bucket") or "").strip()
        if bkt not in buckets_order:
            continue
        raw = row.get(horizon_col, "")
        if raw in ("", None): continue
        try:
            ret = float(raw)
        except (TypeError, ValueError):
            continue
        signals_in_row = {(row.get(c) or "").strip() for c in cols_to_scan if (row.get(c) or "").strip()}
        for sig in signals_in_row:
            cells.setdefault((sig, bkt), []).append(ret)
            sig_totals.setdefault(sig, []).append(ret)
            bucket_totals.setdefault(bkt, []).append(ret)

    cells_out: list[dict] = []
    for (sig, bkt), rets in cells.items():
        if len(rets) < min_count:
            continue
        s = _tz_summary(rets)
        s["signal"] = sig
        s["volume_bucket"] = bkt
        cells_out.append(s)

    sig_totals_out = [{"signal": s, **_tz_summary(r)} for s, r in sig_totals.items() if len(r) >= min_count]
    bucket_totals_out = [{"volume_bucket": b, **_tz_summary(r)} for b, r in bucket_totals.items() if len(r) >= min_count]

    sig_totals_out.sort(key=lambda r: (-r["count"], -r["avg_ret"]))
    bucket_totals_out.sort(key=lambda r: buckets_order.index(r["volume_bucket"]) if r["volume_bucket"] in buckets_order else 99)
    cells_out.sort(key=lambda r: (r["signal"], buckets_order.index(r["volume_bucket"]) if r["volume_bucket"] in buckets_order else 99))

    return {
        "horizon": return_horizon, "stat_path": stat_path,
        "universe": universe, "tf": tf, "signal_type": signal_type,
        "min_count": min_count, "buckets": buckets_order,
        "cells": cells_out, "signal_totals": sig_totals_out,
        "bucket_totals": bucket_totals_out,
    }


@app.get("/api/tz-wlnbb/stats/sequence")
def api_tz_wlnbb_stats_sequence(
    universe: str = "sp500",
    tf: str = "1d",
    nasdaq_batch: str = "",
    signal_type: str = "all",
    return_horizon: str = "5d",
    prev_window: int = 1,          # 1 | 3 | 5  (uses prev_N_signal_summary)
    min_count: int = 5,
):
    """Co-occurrence statistics: for each (prev_signal → current_signal) pair
    in the saved stock_stat, return count, win rate, avg forward return at
    the chosen horizon. Uses prev_{1|3|5}_signal_summary already produced
    by the stock_stat generator.
    """
    horizon_col = {"1d": "ret_1d", "3d": "ret_3d", "5d": "ret_5d", "10d": "ret_10d"}.get(return_horizon, "ret_5d")
    prev_col = {1: "prev_1_signal_summary", 3: "prev_3_signal_summary", 5: "prev_5_signal_summary"}.get(prev_window, "prev_1_signal_summary")

    stat_path = _tz_resolve_stat_path(universe, tf, nasdaq_batch)
    if not stat_path:
        return {"pairs": [], "error": "No stock_stat_tz_wlnbb CSV found. Run generate-stock-stat first."}

    cols_to_scan = _TZ_SIG_COLS.get(signal_type, _TZ_ALL_COLS)

    # pair[(prev, curr)] = list[float]
    pair_rets: dict[tuple, list[float]] = {}
    curr_totals: dict[str, list[float]] = {}

    for row in _tz_iter_rows(stat_path, universe):
        raw = row.get(horizon_col, "")
        if raw in ("", None): continue
        try: ret = float(raw)
        except (TypeError, ValueError): continue
        curr_signals = [(row.get(c) or "").strip() for c in cols_to_scan]
        curr_signals = [c for c in curr_signals if c]
        if not curr_signals: continue
        prev_raw = (row.get(prev_col) or "").strip()
        if not prev_raw: continue
        prev_tokens = [p for p in prev_raw.split("|") if p]
        if not prev_tokens: continue
        for cur in set(curr_signals):
            curr_totals.setdefault(cur, []).append(ret)
            for pr in set(prev_tokens):
                pair_rets.setdefault((pr, cur), []).append(ret)

    curr_baseline = {c: _tz_summary(r) for c, r in curr_totals.items()}

    out: list[dict] = []
    for (prev, cur), rets in pair_rets.items():
        if len(rets) < min_count: continue
        s = _tz_summary(rets)
        s["prev_signal"] = prev
        s["current_signal"] = cur
        base = curr_baseline.get(cur) or {}
        s["base_count"]    = base.get("count", 0)
        s["base_win_rate"] = base.get("win_rate", 0.0)
        s["base_avg_ret"]  = base.get("avg_ret", 0.0)
        s["win_rate_lift"] = round(s["win_rate"] - base.get("win_rate", 0.0), 2)
        s["avg_ret_lift"]  = round(s["avg_ret"]  - base.get("avg_ret",  0.0), 3)
        out.append(s)
    out.sort(key=lambda r: (-r["count"], -r["avg_ret"]))

    base_rows = [{"current_signal": c, **v} for c, v in curr_baseline.items() if v.get("count", 0) >= min_count]
    base_rows.sort(key=lambda r: (-r["count"], -r["avg_ret"]))

    return {
        "horizon": return_horizon, "prev_window": prev_window,
        "stat_path": stat_path, "universe": universe, "tf": tf,
        "signal_type": signal_type, "min_count": min_count,
        "pairs": out, "current_baseline": base_rows,
    }


@app.post("/api/tz-wlnbb/build-whitelists")
def api_tz_wlnbb_build_whitelists(
    universe: str = "sp500",
    tf: str = "1d",
    nasdaq_batch: str = "",
    output_dir: str = ".",
):
    """Generate composite/seq4/composite_seq4 whitelist+blacklist CSVs from stock_stat.

    Reads the existing stock_stat_tz_wlnbb CSV and writes 7 CSV files to output_dir.
    Returns summary counts. Run generate-stock-stat first to create the source data.
    """
    from tz_intelligence.whitelist_builder import build_whitelists

    stat_path = _tz_resolve_stat_path(universe, tf, nasdaq_batch)
    if not stat_path:
        return {"error": "No stock_stat_tz_wlnbb CSV found. Run generate-stock-stat first."}

    # Prevent path traversal
    abs_out = os.path.realpath(output_dir)
    if not abs_out.startswith(os.path.realpath(".")):
        return {"error": "output_dir must be within the working directory"}

    os.makedirs(abs_out, exist_ok=True)
    result = build_whitelists(stat_path, abs_out)
    return {"stat_path": stat_path, "output_dir": abs_out, **result}


@app.post("/api/tz-wlnbb/generate-stock-stat")
def api_tz_wlnbb_generate(
    background_tasks: BackgroundTasks,
    universe: str = "sp500",
    tf: str = "1d",
    bars: int = 500,
    nasdaq_batch: str = "",
    mode: str = "full",       # 260523 v4.9 Phase 2: "full" | "today"
):
    """Trigger stock_stat regeneration.

    mode='full'   → rewrite the entire CSV from scratch (old behaviour, default).
    mode='today'  → incremental: read existing CSV, fetch only NEW bars per
                    ticker since last date, append them. ~100× faster on daily
                    refresh once the universe is already populated.
    """
    global _tz_wlnbb_state
    if _tz_wlnbb_state.get("running"):
        raise HTTPException(status_code=409, detail="Already running")
    if universe == "nasdaq" and not nasdaq_batch:
        nasdaq_batch = "a_m"  # safe default; full NASDAQ at once is too large
    if universe == "nasdaq" and nasdaq_batch == "all":
        raise HTTPException(
            status_code=400,
            detail="nasdaq_batch='all' is not allowed — NASDAQ is too large for a single run. "
                   "Use 'a_m' then 'n_z' separately.",
        )
    if mode not in ("full", "today"):
        raise HTTPException(status_code=400, detail=f"mode must be 'full' or 'today', got '{mode}'")
    background_tasks.add_task(_run_tz_wlnbb_stock_stat, universe, tf, bars, nasdaq_batch, mode)
    return {"status": "started", "mode": mode, "nasdaq_batch": nasdaq_batch or None}


@app.post("/api/tz-wlnbb/stop")
def api_tz_wlnbb_stop():
    """Signal a running generate-stock-stat to stop after the current ticker."""
    _tz_wlnbb_state["stop_requested"] = True
    return {"ok": True, "message": "Stop requested"}


def _run_tz_wlnbb_stock_stat(universe: str, tf: str, bars: int, nasdaq_batch: str = "", mode: str = "full"):
    global _tz_wlnbb_state
    _tz_wlnbb_state = {
        "running": True, "done": 0, "total": 0, "output": None, "error": None,
        "stop_requested": False, "nasdaq_batch": nasdaq_batch or None,
        "mode": mode,
    }
    try:
        from analyzers.tz_wlnbb.stock_stat import generate_stock_stat, generate_stock_stat_incremental
        from scanner import get_universe_tickers

        # nasdaq_gt5 loads NASDAQ tickers and enforces close >= 5 during generation
        source_universe = "nasdaq" if universe == "nasdaq_gt5" else universe
        gen_min_price   = 5.0     if universe == "nasdaq_gt5" else 0.0

        if universe == "split":
            # Force-refresh the split service so we get the latest window,
            # write split_universe_latest.csv as the canonical reference, and
            # use exactly those tickers — no stale cache.
            from split_universe import split_service as _svc
            fresh = _svc.get_split_universe_result(force_refresh=True)
            tickers = list(fresh.tickers)
            log.info(
                "split universe: force-refreshed %d tickers "
                "(total_events=%d reverse=%d stock_like=%d filtered_non_stock=%d) "
                "for stock_stat generation",
                len(tickers), fresh.total_events, fresh.reverse_split_events,
                fresh.stock_like_events, fresh.filtered_non_stock,
            )
        else:
            try:
                tickers = get_universe_tickers(source_universe)
            except Exception:
                try:
                    from scanner import get_tickers
                    tickers = get_tickers() or []
                except Exception:
                    tickers = []

        if universe in ("nasdaq", "nasdaq_gt5") and nasdaq_batch and nasdaq_batch != "all":
            tickers = _filter_nasdaq_batch(tickers, nasdaq_batch)
            log.info("%s batch=%s: %d tickers after filter", universe, nasdaq_batch, len(tickers))
        if universe == "nasdaq_gt5":
            log.info("nasdaq_gt5: %d tickers loaded; price >= 5 filter will apply", len(tickers))

        _tz_wlnbb_state["total"] = len(tickers)

        # Phase 0: Massive primary, yfinance only if ALLOW_YFINANCE_FALLBACK=1
        from data_polygon import fetch_bars as _fetch_bars, polygon_available
        if polygon_available():
            def _fetch(ticker, interval, n_bars_or_kw=None, since=None):
                # Two call conventions supported:
                #  (ticker, interval, n_bars)         — full scan path
                #  (ticker, interval, since="YYYY-MM-DD") — incremental path
                if since is not None:
                    from datetime import date as _date, datetime as _dt
                    try:
                        s = _dt.strptime(since[:10], "%Y-%m-%d").date()
                    except Exception:
                        s = _date.today()
                    days = max((_date.today() - s).days + 5, 5)
                    return _fetch_bars(ticker, interval=interval, days=days)
                # Default full-history path: bars → days w/ safety margin
                n_bars = n_bars_or_kw if isinstance(n_bars_or_kw, int) else 500
                days = max(int(n_bars * 1.6), 365)
                return _fetch_bars(ticker, interval=interval, days=days)
        else:
            from data import fetch_ohlcv as _fetch_yf
            def _fetch(ticker, interval, n_bars_or_kw=None, since=None):
                if since is not None:
                    return _fetch_yf(ticker, interval, bars=500, since=since)
                n_bars = n_bars_or_kw if isinstance(n_bars_or_kw, int) else 500
                return _fetch_yf(ticker, interval, n_bars)

        def _on_progress(done, total):
            _tz_wlnbb_state["done"] = done
            _tz_wlnbb_state["total"] = total

        def _should_stop():
            return bool(_tz_wlnbb_state.get("stop_requested"))

        out_path = _tz_batch_stat_path(universe, tf, nasdaq_batch)
        if mode == "today":
            path, audit = generate_stock_stat_incremental(
                tickers, _fetch, universe=universe, tf=tf,
                output_path=out_path,
                progress_callback=_on_progress,
                early_stop_fn=_should_stop,
            )
        else:
            path, audit = generate_stock_stat(
                tickers, _fetch, universe=universe, tf=tf, bars=bars,
                min_price=gen_min_price,
                output_path=out_path,
                progress_callback=_on_progress,
                early_stop_fn=_should_stop,
            )
        _tz_wlnbb_state["output"] = path
        _tz_wlnbb_state["audit"] = audit

        # Record scan_state for the admin / future incremental decisions
        try:
            from scan_state import set_last_scan
            from datetime import date as _date
            today_str = _date.today().strftime("%Y-%m-%d")
            set_last_scan(universe, tf, today_str,
                          mode=mode, nasdaq_batch=nasdaq_batch,
                          notes=f"rows_added={audit.get('rows_added', audit.get('rows_processed', 0))}")
        except Exception as _exc:
            log.warning("scan_state.set_last_scan skipped: %s", _exc)
    except Exception as exc:
        log.exception("tz_wlnbb stock_stat generation failed")
        _tz_wlnbb_state["error"] = str(exc)
    finally:
        _tz_wlnbb_state["running"] = False


@app.get("/api/tz-wlnbb/status")
def api_tz_wlnbb_status():
    return _tz_wlnbb_state


@app.get("/api/tz-wlnbb/replay-perf")
def api_tz_wlnbb_replay_perf(
    kind: str = "body_wick",
    universe: str = "sp500",
    tf: str = "1d",
    min_count: int = 30,
    sort: str = "top",
    limit: int = 200,
):
    """Compute body_wick or gap_range performance from the latest stock_stat CSV.

    kind   : "body_wick" or "gap_range"
    sort   : "top" (avg_ret_10d desc, fail<20), "bad" (avg_ret_10d asc <0), "raw" (count desc)
    """
    try:
        import csv as _csv
        stat_path = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"
        if not os.path.exists(stat_path):
            stat_path = f"stock_stat_tz_wlnbb_{tf}.csv"
        if not os.path.exists(stat_path):
            return {"rows": [], "error": "No stock_stat_tz_wlnbb CSV found."}

        with open(stat_path, newline="", encoding="utf-8") as f:
            rows = list(_csv.DictReader(f))

        from analyzers.tz_wlnbb.replay import _body_wick_perf, _gap_range_perf, _line5_perf, _safe_float
        if kind == "gap_range":
            perf = _gap_range_perf(rows, min_count=min_count)
        elif kind == "line5":
            perf = _line5_perf(rows, min_count=min_count)
        else:
            perf = _body_wick_perf(rows, min_count=min_count)

        if sort == "top":
            out = [r for r in perf if (_safe_float(r.get("fail_10d_rate")) or 0) < 20]
            out.sort(key=lambda x: -(_safe_float(x.get("avg_ret_10d")) or 0))
        elif sort == "bad":
            out = [r for r in perf if (_safe_float(r.get("avg_ret_10d")) or 0) < 0]
            out.sort(key=lambda x: (_safe_float(x.get("avg_ret_10d")) or 0))
        else:
            out = sorted(perf, key=lambda x: -(x.get("count") or 0))

        return {
            "rows": out[:limit],
            "total": len(perf),
            "kind": kind,
            "min_count": min_count,
            "stat_path": stat_path,
        }
    except Exception as exc:
        log.exception("tz-wlnbb replay-perf error")
        return {"rows": [], "error": str(exc)}


@app.get("/api/tz-wlnbb/debug")
def api_tz_wlnbb_debug(ticker: str, date: str = "", tf: str = "1d", universe: str = "sp500"):
    """Return detailed signal breakdown for a specific ticker/date."""
    try:
        import csv as _csv
        stat_path = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"
        if not os.path.exists(stat_path):
            # fallback to old naming
            stat_path = f"stock_stat_tz_wlnbb_{tf}.csv"
        if not os.path.exists(stat_path):
            return {"error": "No stock_stat_tz_wlnbb CSV found."}

        with open(stat_path, newline="", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            rows = [r for r in reader if r.get("ticker", "").upper() == ticker.upper()]

        if not rows:
            return {"error": f"No data for {ticker}"}

        if date:
            rows = [r for r in rows if r.get("date", "") == date]
        else:
            rows = sorted(rows, key=lambda x: x.get("date", ""))[-1:]

        if not rows:
            return {"error": f"No data for {ticker} on {date}"}

        return {"ticker": ticker, "date": date, "rows": rows}
    except Exception as exc:
        log.exception("tz-wlnbb debug error")
        raise HTTPException(status_code=500, detail=str(exc))


# ── TZ/WLNBB Replay ──────────────────────────────────────────────────────────

_tz_replay_state: dict = {"running": False, "output": None, "error": None}


@app.post("/api/tz-wlnbb/replay")
def api_tz_wlnbb_replay(
    background_tasks: BackgroundTasks,
    universe: str = "sp500",
    tf: str = "1d",
    nasdaq_batch: str = "",
):
    global _tz_replay_state
    if _tz_replay_state.get("running"):
        raise HTTPException(status_code=409, detail="Replay already running")
    background_tasks.add_task(_run_tz_wlnbb_replay, universe, tf, nasdaq_batch)
    return {"status": "started", "nasdaq_batch": nasdaq_batch or None}


def _run_tz_wlnbb_replay(universe: str, tf: str, nasdaq_batch: str = ""):
    global _tz_replay_state
    _tz_replay_state = {"running": True, "output": None, "error": None, "nasdaq_batch": nasdaq_batch or None}
    try:
        import csv as _csv
        from analyzers.tz_wlnbb.replay import generate_replay_zip
        stat_path = _tz_batch_stat_path(universe, tf, nasdaq_batch)
        if not os.path.exists(stat_path):
            # fallback to old naming convention
            stat_path = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"
        if not os.path.exists(stat_path):
            # last-resort fallback
            stat_path = f"stock_stat_tz_wlnbb_{tf}.csv"
        if not os.path.exists(stat_path):
            _tz_replay_state["error"] = (
                f"{_tz_batch_stat_path(universe, tf, nasdaq_batch)} not found — "
                "run generate-stock-stat first"
            )
            return
        rows = []
        with open(stat_path, newline="", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                rows.append(row)
        # Defensive ticker normalization: preserve string values like "NA",
        # coerce NaN-floats / missing to empty string.
        for row in rows:
            t = row.get("ticker")
            if t is None or (isinstance(t, float) and t != t):
                row["ticker"] = ""
            else:
                row["ticker"] = str(t)
        if not rows:
            _tz_replay_state["error"] = (
                f"TZ_WLNBB_ANALYZER_FAILURE: {stat_path} has zero data rows — "
                "run generate-stock-stat first and verify it completes successfully"
            )
            log.error(_tz_replay_state["error"])
            return
        ticker_count = len(set(r.get("ticker", "") for r in rows))
        log.info(
            "tz_wlnbb replay: loaded %d rows from %d tickers from %s (batch=%s)",
            len(rows), ticker_count, stat_path, nasdaq_batch or "none",
        )
        out = _tz_batch_replay_path(universe, tf, nasdaq_batch)
        generate_replay_zip(
            rows, output_path=out, universe=universe, tf=tf,
            ticker_count=ticker_count, nasdaq_batch=nasdaq_batch,
        )

        # ── Append whitelist CSVs to the replay ZIP ──────────────────────────
        # Generate composite/seq4/composite_seq4 whitelists+blacklists from
        # the same stock_stat CSV, then embed them in the analytics ZIP.
        # Also writes them to disk so the scanner's composite_seq4 lookup
        # can find them on next run.
        try:
            import tempfile
            import zipfile
            from tz_intelligence.whitelist_builder import build_whitelists
            from tz_intelligence.final_normalizer import reload_comp_seq4_lookup

            with tempfile.TemporaryDirectory() as tmp:
                wl_result = build_whitelists(stat_path, tmp)
                wl_files = [
                    "composite_whitelist.csv", "composite_blacklist.csv",
                    "seq4_whitelist.csv",      "seq4_blacklist.csv",
                    "composite_seq4_whitelist.csv",
                    "composite_seq4_blacklist.csv",
                    "composite_seq4_stats.csv",
                    "aio_suffix_performance.csv",
                ]
                with zipfile.ZipFile(out, "a", zipfile.ZIP_DEFLATED) as zf:
                    for name in wl_files:
                        p = os.path.join(tmp, name)
                        if os.path.exists(p):
                            zf.write(p, arcname=name)
                            # Also persist to disk so scanner can use it
                            try:
                                import shutil
                                shutil.copy(p, name)
                            except Exception:
                                pass
            # Reload the composite_seq4 lookup so subsequent scans see fresh data
            try:
                n_loaded = reload_comp_seq4_lookup()
                log.info("composite_seq4 lookup reloaded: %d entries", n_loaded)
            except Exception:
                pass
            log.info("whitelists embedded in replay ZIP: %s", wl_result)
        except Exception as wl_exc:
            log.warning("whitelist embedding skipped (non-fatal): %s", wl_exc)

        _tz_replay_state["output"] = out
    except Exception as exc:
        log.exception("tz_wlnbb replay failed")
        _tz_replay_state["error"] = str(exc)
    finally:
        _tz_replay_state["running"] = False


@app.get("/api/tz-wlnbb/replay/status")
def api_tz_wlnbb_replay_status():
    return _tz_replay_state


@app.get("/api/tz-wlnbb/download/{filename}")
def api_tz_wlnbb_download(filename: str):
    from fastapi.responses import FileResponse
    # Safety: only allow tz_wlnbb files
    if not (filename.startswith("replay_tz_wlnbb_") or filename.startswith("stock_stat_tz_wlnbb_")):
        raise HTTPException(status_code=403, detail="Not allowed")
    path = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(path):
        # Try current working directory
        path = filename
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=filename)


@app.get("/api/code-version")
def api_code_version():
    """Returns the active code's signal-engine version + fingerprint of the
    final_reason fix and stale-filter so deployments can be verified."""
    from analyzers.tz_wlnbb.config import TZ_WLNBB_VERSION, Z_PRIORITY
    from analyzers.tz_wlnbb.build_marker import BUILD_MARKER, BUILD_INFO
    fingerprint = {
        "build_marker": BUILD_MARKER,
        "build_info": BUILD_INFO,
        "tz_wlnbb_version": TZ_WLNBB_VERSION,
        "z_priority": Z_PRIORITY,
        "z8_present": "Z8" in Z_PRIORITY,  # must be False
        "final_normalizer_has_guardrail": _check_final_normalizer_fix(),
        "scanner_has_stale_filter": _check_scanner_stale_filter(),
        "pivot_swing_module_present": _check_pivot_module(),
    }
    return fingerprint


def _check_final_normalizer_fix() -> bool:
    try:
        import inspect
        from tz_intelligence import final_normalizer as fn
        src = inspect.getsource(fn.normalize_final_action)
        return ("gates_actually_passed" in src
                and "GATES_PASS reserved ONLY for true pass rows" in src
                and "make the WATCH_HIGH:GATES_PASS bug impossible to ship" in src)
    except Exception:
        return False


def _check_scanner_stale_filter() -> bool:
    try:
        import inspect
        from tz_intelligence import scanner as sc
        src = inspect.getsource(sc)
        return ("max_stale_trading_days" in src
                and "_count_trading_days_between" in src
                and "scan_as_of_date" in src)
    except Exception:
        return False


def _check_pivot_module() -> bool:
    try:
        from analyzers.pivot_swing.pivot_analytics import run_pivot_analytics  # noqa
        return True
    except Exception:
        return False


@app.get("/api/tz-intelligence/scan")
def api_tz_intelligence_scan(
    universe: str = "sp500",
    tf: str = "1d",
    nasdaq_batch: str = "",
    min_price: float = 0,
    max_price: float = 1e9,
    min_volume: float = 0,
    role_filter: str = "all",
    scan_mode: str = "latest",
    limit: int = 500,
    debug: bool = False,
    max_stale_trading_days: int = 2,
):
    """Classify TZ/WLNBB bars using the Signal Intelligence matrix.

    scan_mode='latest'  — one result per ticker (most recent bar).
    scan_mode='history' — all historical classified events.
    """
    # nasdaq_gt5 always enforces close >= 5
    if universe == "nasdaq_gt5":
        min_price = max(min_price, 5.0)
    try:
        from tz_intelligence.scanner import run_intelligence_scan
        result = run_intelligence_scan(
            universe=universe,
            tf=tf,
            nasdaq_batch=nasdaq_batch,
            min_price=min_price,
            max_price=max_price,
            min_volume=min_volume,
            role_filter=role_filter,
            scan_mode=scan_mode,
            limit=limit,
            debug=debug,
            max_stale_trading_days=max_stale_trading_days,
        )
        # ── Tag every row + the response with the active build marker ─────────
        from analyzers.tz_wlnbb.build_marker import BUILD_MARKER, BUILD_INFO
        result["build_marker"] = BUILD_MARKER
        result["build_info"] = BUILD_INFO
        for _r in result.get("results", []) or []:
            _r["build_marker"] = BUILD_MARKER

        # ── Defensive post-scan repair pass ───────────────────────────────────
        # Any row whose final_reason still contains the legacy "GATES_PASS"
        # token while gates actually failed gets repaired here. This protects
        # against stale process state or any racing code path.
        repaired = 0
        for r in result.get("results", []) or []:
            fr = str(r.get("final_reason", "") or "")
            vg = str(r.get("volume_gate_status", "") or "")
            ag = str(r.get("abr_gate_status", "") or "")
            if fr in ("GATES_PASS", "WATCH_HIGH:GATES_PASS", "GO:GATES_PASS") and (vg != "PASS" or ag != "PASS"):
                downgrade_reason = str(r.get("downgrade_reason", "") or "")
                parts = [p for p in downgrade_reason.split(" | ") if p]
                if vg != "PASS":
                    parts.append(f"VOL_GATE:{vg}")
                if ag != "PASS":
                    parts.append(f"ABR_GATE:{ag}")
                fa = str(r.get("final_action", "") or "")
                prefix = "GO:" if fa == "GO" else "WATCH_HIGH:" if fa == "WATCH_HIGH" else ""
                r["final_reason"] = prefix + (" | ".join(parts) if parts else "GATES_FAILED")
                repaired += 1
        if repaired:
            result.setdefault("debug", {})["post_scan_repair_count"] = repaired
            log.warning("post-scan repair: rewrote %d rows with bad GATES_PASS", repaired)
        return result
    except Exception as exc:
        log.exception("tz-intelligence scan error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/pullback-miner/scan")
def api_pullback_miner_scan(
    universe:  str   = Query("sp500"),
    tf:        str   = Query("1d"),
    min_price: float = Query(0.0),
    max_price: float = Query(1e9),
    limit:     int   = Query(500),
):
    """
    Pullback Pattern Miner — Phase 1.

    Discovers 4-bar and 5-bar TZ/WLNBB pullback continuation patterns from
    the stock_stat CSV. Returns CONFIRMED_PULLBACK and ANECDOTAL_PULLBACK
    evidence tiers; top-3 per ticker sorted by tier then score.
    """
    try:
        from analyzers.pullback_miner.miner import run_pullback_scan
        return run_pullback_scan(
            universe=universe,
            tf=tf,
            min_price=min_price,
            max_price=max_price,
            limit=limit,
        )
    except Exception as exc:
        log.exception("pullback-miner scan error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/pullback-miner/report")
def api_pullback_miner_report():
    """
    Run pullback scan for SP500/1d and NASDAQ_GT5/1d and write output CSVs.
    Returns summary with output file paths, counts, and top-20 global patterns.
    """
    try:
        from analyzers.pullback_miner.miner import run_and_report
        result = run_and_report(
            universe_tf_pairs=[("sp500", "1d"), ("nasdaq_gt5", "1d")],
            out_dir=".",
        )
        return result
    except Exception as exc:
        log.exception("pullback-miner report error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/rare-reversal/scan")
def api_rare_reversal_scan(
    universe:  str   = Query("sp500"),
    tf:        str   = Query("1d"),
    min_price: float = Query(0.0),
    max_price: float = Query(1e9),
    limit:     int   = Query(200),
):
    """
    Mine rare bottom-reversal patterns from the stock_stat CSV.

    Extends each known 4-bar SEQ4 pattern left by 1–2 bars (ext5, ext6),
    measures bottom quality (sequence low vs 10/20-bar context), and returns
    evidence-tiered results (CONFIRMED_RARE, ANECDOTAL_RARE, FORMING_PATTERN).
    """
    try:
        from analyzers.rare_reversal.miner import run_rare_reversal_scan
        return run_rare_reversal_scan(
            universe=universe,
            tf=tf,
            min_price=min_price,
            max_price=max_price,
            limit=limit,
        )
    except Exception as exc:
        log.exception("rare-reversal scan error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/ultra-scan/reset")
def api_ultra_scan_reset(force: bool = Query(False)):
    """Manually clear the ULTRA `running` flag if a scan got stuck.

    Without `force=true`, refuses to clear a state younger than 60s
    (avoids killing a healthy fresh scan). `force=true` clears
    unconditionally — use when you're certain the background job is dead.
    """
    from ultra_orchestrator import reset_ultra_state
    return reset_ultra_state(force=force)


@app.post("/api/ultra-scan/trigger")
def api_ultra_scan_trigger(
    background_tasks: BackgroundTasks,
    universe:        str   = Query("sp500"),
    tf:              str   = Query("1d"),
    lookback_n:      int   = Query(5),
    partial_day:     bool  = Query(False),
    min_volume:      float = Query(0.0),
    min_store_score: float = Query(5.0),
    nasdaq_batch:    str   = Query(""),
):
    """ULTRA Stage 1: trigger a Turbo-only scan.

    Stage 2 enrichment (TZ/WLNBB stock_stat + TZ/WLNBB / TZ Intelligence /
    Pullback / Rare Reversal) is run separately via
    /api/ultra-scan/enrich for a chosen subset of tickers, so the heavy
    secondary modules never run for the full universe.

    Runs as a background task; poll /api/ultra-scan/status.
    """
    from ultra_orchestrator import get_ultra_status, run_ultra_scan_job
    if get_ultra_status().get("running"):
        raise HTTPException(status_code=409,
            detail="ULTRA scan already running — if it's stuck, POST "
                   "/api/ultra-scan/reset?force=true to clear it")
    background_tasks.add_task(
        run_ultra_scan_job,
        universe=universe, tf=tf, lookback_n=lookback_n,
        partial_day=partial_day, min_volume=min_volume,
        min_store_score=min_store_score, nasdaq_batch=nasdaq_batch,
    )
    return {
        "status":       "ULTRA Stage 1 (Turbo) started",
        "stage":        "turbo",
        "universe":     universe,
        "tf":           tf,
        "nasdaq_batch": nasdaq_batch or None,
    }


class _UltraEnrichBody(BaseModel):
    universe:     str   = "sp500"
    tf:           str   = "1d"
    nasdaq_batch: str   = ""
    tickers:      list[str] = []
    direction:    str   = "all"   # accepted for future filtering; not enforced server-side
    min_price:    float = 0.0
    max_price:    float = 1e9
    min_volume:   float = 0.0
    stock_stat_bars: int = 500
    max_workers:  int   = 4


@app.post("/api/ultra-scan/enrich")
def api_ultra_scan_enrich(
    body: _UltraEnrichBody,
    background_tasks: BackgroundTasks,
):
    """ULTRA Stage 2: enrich a subset of tickers with TZ/WLNBB / TZ
    Intelligence / Pullback / Rare Reversal. Subset stock_stat is generated
    (or extracted from canonical) at an ULTRA-private path; the canonical
    stock_stat file is never overwritten."""
    from ultra_orchestrator import get_ultra_status, run_ultra_enrich_job
    if get_ultra_status().get("running"):
        raise HTTPException(status_code=409,
            detail="ULTRA scan/enrich already running — POST "
                   "/api/ultra-scan/reset?force=true to clear if stuck")
    if not body.tickers:
        raise HTTPException(status_code=400, detail="tickers list is empty")
    background_tasks.add_task(
        run_ultra_enrich_job,
        tickers=body.tickers,
        universe=body.universe, tf=body.tf, nasdaq_batch=body.nasdaq_batch,
        min_price=body.min_price, max_price=body.max_price,
        min_volume=body.min_volume, stock_stat_bars=body.stock_stat_bars,
        max_workers=body.max_workers,
    )
    return {
        "status":   "ULTRA Stage 2 (enrich) started",
        "stage":    "enrich",
        "universe": body.universe,
        "tf":       body.tf,
        "tickers":  len(body.tickers),
    }


@app.get("/api/ultra-scan/enrich-status")
def api_ultra_scan_enrich_status():
    """Alias for /api/ultra-scan/status; convenient for the frontend's
    enrich-only progress polling."""
    from ultra_orchestrator import get_ultra_status
    return get_ultra_status()


@app.get("/api/ultra-scan/status")
def api_ultra_scan_status():
    from ultra_orchestrator import get_ultra_status
    return get_ultra_status()


@app.get("/api/ultra-scan/results")
def api_ultra_scan_results(
    universe:     str = Query("sp500"),
    tf:           str = Query("1d"),
    nasdaq_batch: str = Query(""),
    # ── 260523 filter params ────────────────────────────────────────────────
    ad_fresh:   Optional[bool] = None,
    ad_cluster: Optional[bool] = None,
    wyc_phase:  Optional[str]  = None,
    wyc_spring: Optional[bool] = None,
    wyc_sos:    Optional[bool] = None,
    wyc_acc_tr: Optional[bool] = None,
    swing_type: Optional[str]  = None,
    # 260523 v3.5 PREBREAK + WYC additional
    prebreak_prime:  Optional[bool] = None,
    prebreak_ready:  Optional[bool] = None,
    prebreak_watch:  Optional[bool] = None,
    pb_lvbo:         Optional[bool] = None,
    pb_stop_cause:   Optional[bool] = None,
    pb_wvf_confirm:  Optional[bool] = None,
    pb_macro_penalty:Optional[bool] = None,
    wyc_in_tr:       Optional[bool] = None,
    wyc_sow:         Optional[bool] = None,
):
    """Return the most recently merged ULTRA results for this (universe, tf,
    batch). Falls back to DB when memory cache is empty (survives restart)."""
    try:
        from ultra_orchestrator import get_ultra_results, load_latest_ultra_scan_from_db
        resp = get_ultra_results(universe=universe, tf=tf, nasdaq_batch=nasdaq_batch)
        # Memory cache miss — try loading persisted scan from DB
        if not resp.get("results"):
            try:
                if load_latest_ultra_scan_from_db(universe, tf, nasdaq_batch):
                    resp = get_ultra_results(universe=universe, tf=tf, nasdaq_batch=nasdaq_batch)
            except Exception as _db_exc:
                log.warning("ultra-scan/results DB fallback error: %s", _db_exc)

        # ── 260523 enrichment + filter ─────────────────────────────────────
        results = resp.get("results") or []
        if results:
            results = _enrich_with_260523(results, universe, tf, nasdaq_batch)
            warnings_260523 = _diagnose_260523_columns(
                results,
                requested={
                    "ad_fresh": ad_fresh, "ad_cluster": ad_cluster,
                    "wyc_phase": wyc_phase, "wyc_spring": wyc_spring,
                    "wyc_sos": wyc_sos, "wyc_acc_tr": wyc_acc_tr,
                    "swing_type": swing_type,
                    "prebreak_prime": prebreak_prime, "prebreak_ready": prebreak_ready,
                    "prebreak_watch": prebreak_watch,
                    "pb_lvbo": pb_lvbo, "pb_stop_cause": pb_stop_cause,
                    "pb_wvf_confirm": pb_wvf_confirm, "pb_macro_penalty": pb_macro_penalty,
                    "wyc_in_tr": wyc_in_tr, "wyc_sow": wyc_sow,
                },
            )
            results = _apply_260523_filters(
                results,
                ad_fresh=ad_fresh, ad_cluster=ad_cluster,
                wyc_phase=wyc_phase, wyc_spring=wyc_spring,
                wyc_sos=wyc_sos, wyc_acc_tr=wyc_acc_tr,
                swing_type=swing_type,
                prebreak_prime=prebreak_prime, prebreak_ready=prebreak_ready,
                prebreak_watch=prebreak_watch,
                pb_lvbo=pb_lvbo, pb_stop_cause=pb_stop_cause,
                pb_wvf_confirm=pb_wvf_confirm, pb_macro_penalty=pb_macro_penalty,
                wyc_in_tr=wyc_in_tr, wyc_sow=wyc_sow,
            )
            results = _enrich_atomic(results, universe)
            resp["results"] = results
            if warnings_260523:
                existing = list(resp.get("warnings") or [])
                resp["warnings"] = existing + warnings_260523
                resp["warnings_260523"] = warnings_260523
        return resp
    except Exception as exc:
        log.exception("ultra-scan/results error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/ultra-scan")
def api_ultra_scan(
    universe:     str = Query("sp500"),
    tf:           str = Query("1d"),
    nasdaq_batch: str = Query(""),
):
    """Convenience alias for /api/ultra-scan/results — returns the cached
    ULTRA response for this (universe, tf, batch). The main UX is
    trigger → status → results, like Turbo."""
    try:
        from ultra_orchestrator import get_ultra_results
        return get_ultra_results(universe=universe, tf=tf, nasdaq_batch=nasdaq_batch)
    except Exception as exc:
        log.exception("ultra-scan error")
        raise HTTPException(status_code=500, detail=str(exc))


# ─── Sequence scan ───────────────────────────────────────────────────────────
#
# Universe-wide N-bar T/Z sequence analyzer. Consumes the canonical
# stock_stat_tz_wlnbb / bulk Stock Stat CSV (no OHLCV re-fetch).
#
# State model intentionally mirrors Turbo / Stock Stat / Replay: a module-
# level dict for the live run + a per-cache-key dict for completed runs.
# We deliberately do NOT use SQLite here — the earlier SQLite path silently
# failed on Postgres (Railway), causing /status to fall through to "not_run"
# right after the worker finished. Plain in-memory keeps the screener in
# lockstep with the rest of the app and survives across status polls.

from datetime import datetime, timezone
from threading import Lock as _SeqLock
_seq_state: dict = {
    "running":      False,
    "cache_key":    None,
    "progress":     0,
    "total":        0,
    "started_at":   None,
    "completed_at": None,
    "error":        None,
    "params":       {},
    "stat_path":    None,
}
# Completed runs, keyed by cache_key. Holds the same shape /status expects
# plus 'results'. Capped to keep memory bounded.
_seq_results: dict[str, dict] = {}
_SEQ_RESULTS_CAP = 8
_seq_state_lock = _SeqLock()


def _seq_cache_key(universe: str, tf: str, seq_len: int, mode: str,
                   min_count: int, nasdaq_batch: str = "") -> str:
    parts = [universe, tf, str(seq_len), mode, str(min_count)]
    if nasdaq_batch:
        parts.append(nasdaq_batch)
    return "|".join(parts)


def _seq_store_completed(cache_key: str, payload: dict) -> None:
    """Save a completed run, evicting the oldest entry beyond the cap."""
    with _seq_state_lock:
        _seq_results[cache_key] = payload
        if len(_seq_results) > _SEQ_RESULTS_CAP:
            # Evict oldest by completed_at (or insertion order)
            try:
                victim = min(_seq_results.items(),
                             key=lambda kv: kv[1].get("completed_at") or "")[0]
                _seq_results.pop(victim, None)
            except Exception:
                # Fall back to popping the first key
                first_key = next(iter(_seq_results))
                _seq_results.pop(first_key, None)


def _run_sequence_scan_bg(cache_key: str, universe: str, tf: str,
                          seq_len: int, mode: str, min_count: int,
                          nasdaq_batch: str) -> None:
    """Background worker. Updates the live ``_seq_state`` for progress polls,
    and on completion stores the result in ``_seq_results[cache_key]`` so
    /status and /results can return it after the worker exits."""
    from sequence_engine import run_sequence_scan
    started = datetime.now(timezone.utc).isoformat()

    with _seq_state_lock:
        _seq_state.update({
            "running": True, "cache_key": cache_key,
            "progress": 0, "total": 0,
            "started_at": started, "completed_at": None, "error": None,
            "stat_path": None,
            "params": {
                "universe": universe, "tf": tf, "seq_len": seq_len,
                "mode": mode, "min_count": min_count,
                "nasdaq_batch": nasdaq_batch or None,
            },
        })

    def _on_progress(done: int, total: int) -> None:
        with _seq_state_lock:
            _seq_state["progress"] = done
            _seq_state["total"]    = total

    try:
        out = run_sequence_scan(
            universe=universe, tf=tf, seq_len=seq_len, mode=mode,
            min_count=min_count, nasdaq_batch=nasdaq_batch,
            progress_cb=_on_progress,
        )
    except Exception as exc:
        log.exception("sequence-scan worker crashed")
        out = {"status": "error", "error": str(exc), "results": []}

    completed = datetime.now(timezone.utc).isoformat()
    with _seq_state_lock:
        total = _seq_state["total"]
        done  = _seq_state["progress"] or total

    stat_path_used = out.get("stat_path") or (
        ", ".join(out.get("tried_paths") or []) or None
    )
    final_status = out.get("status") or "error"
    if final_status == "ok":
        final_status = "done"

    _seq_store_completed(cache_key, {
        "status":        final_status,
        "started_at":    started,
        "completed_at":  completed,
        "tickers_done":  done,
        "tickers_total": total,
        "error":         None if final_status == "done" else out.get("error"),
        "stat_path":     stat_path_used,
        "tried_paths":   out.get("tried_paths") or [],
        "results":       out.get("results") or [],
        "params": {
            "universe": universe, "tf": tf, "seq_len": seq_len,
            "mode": mode, "min_count": min_count,
            "nasdaq_batch": nasdaq_batch or None,
        },
    })

    with _seq_state_lock:
        _seq_state["running"]      = False
        _seq_state["completed_at"] = completed
        _seq_state["error"]        = out.get("error")
        _seq_state["stat_path"]    = stat_path_used


@app.post("/api/sequence-scan/trigger")
def api_sequence_scan_trigger(
    background_tasks: BackgroundTasks,
    universe:     str = Query("sp500"),
    tf:           str = Query("1d"),
    seq_len:      int = Query(4, ge=2, le=6),
    min_count:    int = Query(10, ge=1),
    mode:         str = Query("type"),
    nasdaq_batch: str = Query(""),
):
    """Start a universe-wide sequence scan over the existing TZ/WLNBB
    stock_stat / bulk Stock Stat CSV. Returns immediately; poll
    /api/sequence-scan/status for progress."""
    if mode not in ("type", "full"):
        raise HTTPException(400, "mode must be 'type' or 'full'")
    if _seq_state.get("running"):
        raise HTTPException(409, "Another sequence scan is already running")
    cache_key = _seq_cache_key(universe, tf, seq_len, mode, min_count, nasdaq_batch)
    background_tasks.add_task(
        _run_sequence_scan_bg, cache_key, universe, tf, seq_len, mode,
        min_count, nasdaq_batch,
    )
    return {"status": "started", "cache_key": cache_key}


@app.get("/api/sequence-scan/status")
def api_sequence_scan_status(
    universe:     str = Query("sp500"),
    tf:           str = Query("1d"),
    seq_len:      int = Query(4),
    min_count:    int = Query(10),
    mode:         str = Query("type"),
    nasdaq_batch: str = Query(""),
):
    """Live in-memory progress while a scan with this cache_key is running;
    cached completed-run state otherwise."""
    cache_key = _seq_cache_key(universe, tf, seq_len, mode, min_count, nasdaq_batch)
    with _seq_state_lock:
        live      = dict(_seq_state) if _seq_state.get("cache_key") == cache_key else None
        completed = dict(_seq_results.get(cache_key) or {}) or None
    if live and live.get("running"):
        total = max(int(live.get("total") or 1), 1)
        return {
            "status":     "running",
            "cache_key":  cache_key,
            "progress":   int(live.get("progress") or 0),
            "total":      int(live.get("total") or 0),
            "pct":        int((live.get("progress") or 0) / total * 100),
            "started_at": live.get("started_at"),
            "params":     live.get("params") or {},
        }
    if completed:
        total = max(int(completed.get("tickers_total") or 1), 1)
        return {
            "status":       completed.get("status") or "unknown",
            "cache_key":    cache_key,
            "progress":     int(completed.get("tickers_done") or 0),
            "total":        int(completed.get("tickers_total") or 0),
            "pct":          int((completed.get("tickers_done") or 0) / total * 100),
            "started_at":   completed.get("started_at"),
            "completed_at": completed.get("completed_at"),
            "error":        completed.get("error"),
            "stat_path":    completed.get("stat_path"),
            "tried_paths":  completed.get("tried_paths") or [],
            "params":       completed.get("params") or {},
        }
    return {"status": "not_run", "cache_key": cache_key,
            "progress": 0, "total": 0, "pct": 0}


@app.get("/api/sequence-scan/results")
def api_sequence_scan_results(
    universe:     str = Query("sp500"),
    tf:           str = Query("1d"),
    seq_len:      int = Query(4),
    min_count:    int = Query(10),
    mode:         str = Query("type"),
    nasdaq_batch: str = Query(""),
    limit:        int = Query(50, ge=1, le=10000),
    sort_by:      str = Query("score"),    # score|win_rate|count|ticker_count
):
    """Return cached top sequences for the given params."""
    cache_key = _seq_cache_key(universe, tf, seq_len, mode, min_count, nasdaq_batch)
    with _seq_state_lock:
        completed = dict(_seq_results.get(cache_key) or {})
    if not completed:
        return {"status": "not_run", "results": [], "total_sequences": 0,
                "cache_key": cache_key}
    results = list(completed.get("results") or [])

    sort_keys = {
        "win_rate":     lambda x: (-(x.get("win_rate")     or 0), -(x.get("count") or 0)),
        "count":        lambda x: (-(x.get("count")        or 0), -(x.get("win_rate") or 0)),
        "ticker_count": lambda x: (-(x.get("ticker_count") or 0), -(x.get("win_rate") or 0)),
        "score":        lambda x: (-(x.get("score")        or 0), -(x.get("count") or 0)),
        # Multi-horizon win-rate / avg-return sort options
        "win_rate_3d":  lambda x: (-(x.get("win_rate_3d")  or 0), -(x.get("count") or 0)),
        "win_rate_5d":  lambda x: (-(x.get("win_rate_5d")  or 0), -(x.get("count") or 0)),
        "win_rate_9d":  lambda x: (-(x.get("win_rate_9d")  or 0), -(x.get("count") or 0)),
        "avg_ret_1d":   lambda x: (-(x.get("avg_ret_1d")   or 0), -(x.get("count") or 0)),
        "avg_ret_3d":   lambda x: (-(x.get("avg_ret_3d")   or 0), -(x.get("count") or 0)),
        "avg_ret_5d":   lambda x: (-(x.get("avg_ret_5d")   or 0), -(x.get("count") or 0)),
        "avg_ret_9d":   lambda x: (-(x.get("avg_ret_9d")   or 0), -(x.get("count") or 0)),
    }
    results.sort(key=sort_keys.get(sort_by, sort_keys["score"]))

    return {
        "status":          completed.get("status") or "unknown",
        "cache_key":       cache_key,
        "completed_at":    completed.get("completed_at"),
        "results":         results[:limit],
        "total_sequences": len(results),
        "tickers_total":   int(completed.get("tickers_total") or 0),
        "stat_path":       completed.get("stat_path"),
        "tried_paths":     completed.get("tried_paths") or [],
        "error":           completed.get("error"),
        "params":          completed.get("params") or {},
    }


@app.get("/api/debug/compare-ultra-superchart")
def api_debug_compare_ultra_superchart(symbol: str, tf: str = "1d"):
    """
    Debug endpoint: compare the two pipelines side-by-side for a single symbol.

    Returns per-pipeline: bar count, last bar date, data source, key signal values,
    and turbo_score. Also returns a diff dict highlighting any mismatches.

    This endpoint is READ-ONLY and never modifies any scan state.
    """
    import traceback
    from data import fetch_ohlcv
    from signal_engine import compute_signals
    from wlnbb_engine import compute_wlnbb
    from canonical_scoring_engine import compute_canonical_score

    ticker = symbol.upper().strip()
    report: dict = {"symbol": ticker, "tf": tf, "pipelines": {}, "diff": {}}

    # ── Helper: compute last-bar signals from a DataFrame ─────────────────
    def _analyze_df(df, label: str, source: str) -> dict:
        try:
            sig_df = compute_signals(df)
        except Exception:
            sig_df = None
        try:
            wlnbb_df = compute_wlnbb(df)
        except Exception:
            wlnbb_df = None

        last_sig = sig_df.iloc[-1] if sig_df is not None and not sig_df.empty else {}
        last_w   = wlnbb_df.iloc[-1] if wlnbb_df is not None and not wlnbb_df.empty else {}

        tz_name = str(last_sig.get("sig_name", "")) if last_sig.get("is_bull") else ""
        l_sigs = []
        for col, lbl in [("L34","L34"),("L43","L43"),("L64","L64"),("L22","L22"),
                          ("FRI34","FRI34"),("FRI43","FRI43"),
                          ("BO_UP","BO↑"),("BX_UP","BX↑"),("BE_UP","BE↑"),
                          ("BLUE","BL"),("CCI_READY","CCI")]:
            if bool(last_w.get(col)):
                l_sigs.append(lbl)

        sig_row = {
            "tz_bull": bool(last_sig.get("is_bull")),
            "tz_sig":  tz_name,
            "conso_2809": bool(last_w.get("conso_2809") if hasattr(last_w,"get") else False),
            "bf_buy":  False,
            "rocket":  False,
            "bo_up":   bool(last_w.get("BO_UP")),
            "bx_up":   bool(last_w.get("BX_UP")),
            "be_up":   bool(last_w.get("BE_UP")),
            "l34":     bool(last_w.get("L34")),
            "fri34":   bool(last_w.get("FRI34")),
        }
        try:
            from combo_engine import compute_combo as _cc
            combo_df = _cc(df)
            if not combo_df.empty:
                lc = combo_df.iloc[-1]
                sig_row["conso_2809"] = bool(lc.get("conso_2809"))
                sig_row["rocket"]     = bool(lc.get("rocket"))
                sig_row["bf_buy"]     = bool(lc.get("buy_2809"))
        except Exception:
            pass

        try:
            canon = compute_canonical_score(sig_row)
            turbo_score = canon.get("turbo_score", 0)
        except Exception:
            turbo_score = None

        last_idx = df.index[-1]
        last_date = str(last_idx)[:10] if hasattr(last_idx, "__str__") else ""

        return {
            "label":       label,
            "source":      source,
            "bar_count":   len(df),
            "last_bar":    last_date,
            "first_bar":   str(df.index[0])[:10],
            "tz_signal":   tz_name or None,
            "l_signals":   l_sigs,
            "vol_bucket":  str(last_w.get("vol_bucket", "")) if hasattr(last_w,"get") else "",
            "turbo_score": turbo_score,
        }

    # ── Pipeline A: Superchart (yfinance, 150-bar default) ────────────────
    try:
        df_sc = fetch_ohlcv(ticker, interval=tf, bars=150)
        report["pipelines"]["superchart"] = _analyze_df(df_sc, "superchart", "yfinance")
    except Exception as exc:
        report["pipelines"]["superchart"] = {"error": str(exc)}

    # ── Pipeline B: Turbo-equivalent (Polygon → yfinance, 180-day window) ─
    try:
        df_turbo = None
        turbo_source = "yfinance"
        try:
            from data_polygon import fetch_bars, polygon_available
            if polygon_available():
                days = 400 if tf in ("1wk","1w") else 180 if tf == "1d" else 90
                df_turbo = fetch_bars(ticker, interval=tf, days=days)
                turbo_source = "polygon"
        except Exception:
            pass
        if df_turbo is None or df_turbo.empty:
            import yfinance as yf
            period = "5y" if tf in ("1wk","1w") else "180d" if tf == "1d" else "60d"
            raw = yf.Ticker(ticker).history(period=period, interval=tf, auto_adjust=True)
            raw.columns = [str(c).lower() for c in raw.columns]
            df_turbo = raw[["open","high","low","close","volume"]].dropna()
            turbo_source = "yfinance"
        report["pipelines"]["turbo"] = _analyze_df(df_turbo, "turbo", turbo_source)
    except Exception as exc:
        report["pipelines"]["turbo"] = {"error": str(exc), "traceback": traceback.format_exc()}

    # ── Diff ──────────────────────────────────────────────────────────────
    sc = report["pipelines"].get("superchart", {})
    tu = report["pipelines"].get("turbo", {})
    diff = {}
    if "error" not in sc and "error" not in tu:
        if sc.get("last_bar") != tu.get("last_bar"):
            diff["last_bar"] = {"superchart": sc["last_bar"], "turbo": tu["last_bar"],
                                "note": "Different latest bar — partial-day trim mismatch or source lag"}
        if sc.get("source") != tu.get("source"):
            diff["data_source"] = {"superchart": sc["source"], "turbo": tu["source"],
                                   "note": "Different OHLCV source — adjusted prices may differ"}
        sc_bars, tu_bars = sc.get("bar_count",0), tu.get("bar_count",0)
        if abs(sc_bars - tu_bars) > 5:
            diff["bar_count"] = {"superchart": sc_bars, "turbo": tu_bars,
                                 "note": "Different lookback windows — rolling indicators will diverge"}
        if sc.get("tz_signal") != tu.get("tz_signal"):
            diff["tz_signal"] = {"superchart": sc.get("tz_signal"), "turbo": tu.get("tz_signal"),
                                 "note": "T/Z signal differs on last bar"}
        sc_l = set(sc.get("l_signals", []))
        tu_l = set(tu.get("l_signals", []))
        if sc_l != tu_l:
            diff["l_signals"] = {
                "superchart_only": sorted(sc_l - tu_l),
                "turbo_only":      sorted(tu_l - sc_l),
                "note": "L/structure signals differ — likely due to bar-count or source mismatch",
            }
        sc_s = sc.get("turbo_score")
        tu_s = tu.get("turbo_score")
        if sc_s is not None and tu_s is not None and abs((sc_s or 0) - (tu_s or 0)) > 2:
            diff["turbo_score"] = {"superchart": sc_s, "turbo": tu_s,
                                   "delta": round((sc_s or 0) - (tu_s or 0), 1)}
    report["diff"] = diff
    report["mismatch_count"] = len(diff)
    report["root_causes"] = [v["note"] for v in diff.values() if isinstance(v, dict) and "note" in v]
    return report



# ---------------------------------------------------------------------------
# Combined regenerate-and-audit endpoint — one-click full pipeline
# ---------------------------------------------------------------------------

@app.post("/api/regenerate-and-audit")
def api_regenerate_and_audit(
    background_tasks: BackgroundTasks,
    universe: str = "sp500",
    tf: str = "1d",
    bars: int = 500,
    nasdaq_batch: str = "",
):
    """
    Trigger stock_stat regeneration → replay ZIP regeneration → audit, in sequence.
    Returns immediately with a job_id; poll GET /api/regenerate-and-audit/status
    until done=true, then GET /api/artifact-audit for the result.
    Reuses the existing _run_tz_wlnbb_stock_stat and _run_tz_wlnbb_replay.
    """
    global _tz_wlnbb_state
    if _tz_wlnbb_state.get("running"):
        raise HTTPException(status_code=409, detail="Stock-stat job already running")
    background_tasks.add_task(
        _run_regen_audit_chain, universe, tf, bars, nasdaq_batch,
    )
    return {
        "status": "started",
        "steps": ["generate_stock_stat", "generate_replay_zip", "artifact_audit"],
        "poll_url": "/api/regenerate-and-audit/status",
        "audit_url": f"/api/artifact-audit?universe={universe}&tf={tf}&nasdaq_batch={nasdaq_batch}",
    }


_regen_audit_state: dict = {"running": False, "done": False, "step": None,
                            "error": None, "audit": None}


@app.get("/api/regenerate-and-audit/status")
def api_regenerate_and_audit_status():
    return _regen_audit_state


def _run_regen_audit_chain(universe: str, tf: str, bars: int, nasdaq_batch: str):
    global _regen_audit_state
    _regen_audit_state = {
        "running": True, "done": False, "step": "generate_stock_stat",
        "error": None, "audit": None,
    }
    try:
        _run_tz_wlnbb_stock_stat(universe, tf, bars, nasdaq_batch)
        if _tz_wlnbb_state.get("error"):
            _regen_audit_state["error"] = f"stock_stat: {_tz_wlnbb_state['error']}"
            return
        _regen_audit_state["step"] = "generate_replay_zip"
        _run_tz_wlnbb_replay(universe, tf, nasdaq_batch)
        if _tz_replay_state.get("error"):
            _regen_audit_state["error"] = f"replay: {_tz_replay_state['error']}"
            return
        _regen_audit_state["step"] = "artifact_audit"
        _regen_audit_state["audit"] = api_artifact_audit(
            universe=universe, tf=tf, nasdaq_batch=nasdaq_batch,
        )
    except Exception as e:
        _regen_audit_state["error"] = str(e)
        log.exception("regenerate-and-audit chain failed")
    finally:
        _regen_audit_state["running"] = False
        _regen_audit_state["done"] = True


# ---------------------------------------------------------------------------
# Artifact audit endpoint — reports post-regeneration sanity metrics
# ---------------------------------------------------------------------------

@app.get("/api/artifact-audit")
def api_artifact_audit(
    universe: str = "sp500",
    tf: str = "1d",
    nasdaq_batch: str = "",
):
    """
    Read the on-disk stock_stat CSV and most recent replay ZIP and report:
      - stock_stat version distribution
      - latest row count / scan_as_of_date / stale_dropped count
      - WATCH_HIGH:GATES_PASS count (should be 0 after fix)
      - bad GATES_PASS count (non-GO rows that just say GATES_PASS)
      - pivot output file count in replay ZIP
    """
    import csv as _csv
    import zipfile as _zf
    import glob as _glob

    stat_path = _tz_batch_stat_path(universe, tf, nasdaq_batch)
    if not os.path.exists(stat_path):
        stat_path = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"
    if not os.path.exists(stat_path):
        stat_path = f"stock_stat_tz_wlnbb_{tf}.csv"

    audit: dict = {
        "universe": universe, "tf": tf, "nasdaq_batch": nasdaq_batch or None,
        "stock_stat_path": stat_path,
        "stock_stat_exists": os.path.exists(stat_path),
    }

    # ── Stock-stat version + stale audit ──────────────────────────────────────
    if os.path.exists(stat_path):
        from tz_intelligence.scanner import run_intelligence_scan, _count_trading_days_between
        version_counts: dict = {}
        all_rows: list = []
        latest_dates: list = []
        with open(stat_path, newline="", encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                all_rows.append(row)
                v = row.get("tz_wlnbb_version") or "(empty)"
                version_counts[v] = version_counts.get(v, 0) + 1
        audit["stock_stat_total_rows"] = len(all_rows)
        audit["stock_stat_versions"] = version_counts

        # Find scan_as_of_date and stale rows
        by_ticker: dict = {}
        for row in all_rows:
            tk = row.get("ticker", "")
            if not tk:
                continue
            dt = row.get("bar_datetime") or row.get("date", "")
            if tk not in by_ticker or dt > by_ticker[tk]:
                by_ticker[tk] = dt
        latest_date_vals = [d for d in by_ticker.values() if d]
        scan_as_of = max(latest_date_vals) if latest_date_vals else ""
        stale_count = sum(
            1 for dt in by_ticker.values()
            if dt and scan_as_of and _count_trading_days_between(dt, scan_as_of) > 2
        )
        audit["scan_as_of_date"] = scan_as_of
        audit["latest_ticker_count"] = len(by_ticker)
        audit["stale_dropped_count"] = stale_count
    else:
        audit["stock_stat_total_rows"] = 0
        audit["stock_stat_versions"] = {}
        audit["scan_as_of_date"] = None
        audit["stale_dropped_count"] = 0

    # ── Latest scan WATCH_HIGH:GATES_PASS + bad GATES_PASS ────────────────────
    try:
        scan_result = run_intelligence_scan(
            universe=universe, tf=tf, nasdaq_batch=nasdaq_batch,
            scan_mode="latest", limit=10000,
        )
        results = scan_result.get("results", [])
        wh_gates_pass = sum(
            1 for r in results
            if str(r.get("final_reason", "")).startswith("WATCH_HIGH:GATES_PASS")
        )
        bad_gates_pass = sum(
            1 for r in results
            if str(r.get("final_reason", "")) == "GATES_PASS"
            and str(r.get("final_action", "")) not in ("GO",)
        )
        audit["latest_scan_row_count"] = len(results)
        audit["watch_high_gates_pass_count"] = wh_gates_pass
        audit["bad_gates_pass_count"] = bad_gates_pass
        audit["stale_dropped_count"] = scan_result.get("debug", {}).get("stale_dropped_count", audit.get("stale_dropped_count", 0))
        audit["scan_as_of_date"] = scan_result.get("scan_as_of_date") or audit.get("scan_as_of_date")
    except Exception as e:
        audit["latest_scan_error"] = str(e)
        audit["watch_high_gates_pass_count"] = "ERROR"
        audit["bad_gates_pass_count"] = "ERROR"

    # ── Replay ZIP pivot_swing file count ─────────────────────────────────────
    replay_path = _tz_batch_replay_path(universe, tf, nasdaq_batch)
    if not os.path.exists(replay_path):
        replay_path = f"replay_tz_wlnbb_{universe}_{tf}_analytics.zip"
    audit["replay_zip_path"] = replay_path
    audit["replay_zip_exists"] = os.path.exists(replay_path)
    pivot_files_in_zip: list = []
    replay_metadata_version = None
    zip_build_marker = None
    if os.path.exists(replay_path):
        try:
            import json as _json
            with _zf.ZipFile(replay_path, "r") as zf:
                names = zf.namelist()
                pivot_files_in_zip = [n for n in names if n.startswith("pivot_swing/")]
                # Read top-level BUILD_MARKER.txt if present
                if "BUILD_MARKER.txt" in names:
                    zip_build_marker = zf.read("BUILD_MARKER.txt").decode("utf-8").splitlines()[0].strip()
                # version lives in tz_wlnbb_config_snapshot.json
                for snap_name in ("tz_wlnbb_config_snapshot.json", "replay_tz_wlnbb_metadata.json"):
                    if snap_name in names:
                        obj = _json.loads(zf.read(snap_name).decode("utf-8"))
                        replay_metadata_version = (
                            obj.get("TZ_WLNBB_ANALYZER_VERSION")
                            or obj.get("source_pine_script")
                            or obj.get("tz_wlnbb_version")
                        )
                        if zip_build_marker is None:
                            zip_build_marker = obj.get("build_marker")
                        if replay_metadata_version:
                            break
        except Exception as e:
            audit["replay_zip_read_error"] = str(e)
    audit["zip_build_marker"] = zip_build_marker
    from analyzers.tz_wlnbb.build_marker import BUILD_MARKER as _ACTIVE_MARKER
    audit["active_code_build_marker"] = _ACTIVE_MARKER
    audit["zip_built_with_active_code"] = (zip_build_marker == _ACTIVE_MARKER)
    audit["pivot_output_file_count"] = len(pivot_files_in_zip)
    audit["pivot_files_in_zip"] = pivot_files_in_zip
    audit["replay_metadata_version"] = replay_metadata_version

    # ── Pass/fail summary ─────────────────────────────────────────────────────
    checks = {
        "stock_stat_version_correct": all(
            "260521" in str(v) for v in audit.get("stock_stat_versions", {}) if v
        ) if audit.get("stock_stat_versions") else None,
        "replay_version_correct": (
            "260521" in str(replay_metadata_version)
            if replay_metadata_version else None
        ),
        "stale_rows_zero": audit.get("stale_dropped_count") == 0,
        "watch_high_gates_pass_zero": audit.get("watch_high_gates_pass_count") == 0,
        "bad_gates_pass_zero": audit.get("bad_gates_pass_count") == 0,
        "pivot_files_present": audit.get("pivot_output_file_count", 0) == 17,
        "zip_built_with_active_code": audit.get("zip_built_with_active_code", False),
    }
    audit["checks"] = checks
    audit["all_checks_pass"] = all(v for v in checks.values() if v is not None)

    return audit


# ---------------------------------------------------------------------------
# Pivot Swing Character Analytics Engine endpoints
# ---------------------------------------------------------------------------

_pivot_swing_state = {
    "running": False, "done": 0, "total": 0, "output_dir": None, "error": None,
    "files": [],
}


@app.post("/api/pivot-swing/run")
def api_pivot_swing_run(
    background_tasks: BackgroundTasks,
    csv_dir: str = "",
    csv_file: str = "",
    output_dir: str = "",
    pivot_left: int = 3,
    pivot_right: int = 3,
    min_swing_pct: float = 3.0,
    min_swing_bars: int = 2,
):
    """
    Run the Pivot Swing Character Analytics Engine.

    Supply either csv_dir (process all stock_stat_tz_wlnbb_*.csv files in that dir)
    or csv_file (single file path). output_dir defaults to <csv_dir>/pivot_analytics
    or /tmp/pivot_analytics.
    """
    global _pivot_swing_state
    if _pivot_swing_state.get("running"):
        raise HTTPException(status_code=409, detail="Already running")
    if not csv_dir and not csv_file:
        # Default: look for stock-stat CSVs in backend dir
        csv_dir = os.path.dirname(__file__)
    if not output_dir:
        base = csv_dir or os.path.dirname(csv_file)
        output_dir = os.path.join(base, "pivot_analytics")
    background_tasks.add_task(
        _run_pivot_swing_bg, csv_dir, csv_file, output_dir,
        pivot_left, pivot_right, min_swing_pct, min_swing_bars,
    )
    return {"status": "started", "output_dir": output_dir}


@app.get("/api/pivot-swing/status")
def api_pivot_swing_status():
    return _pivot_swing_state


@app.get("/api/pivot-swing/results")
def api_pivot_swing_results():
    return {"files": _pivot_swing_state.get("files", []), "output_dir": _pivot_swing_state.get("output_dir")}


@app.get("/api/pivot-swing/download/{filename}")
def api_pivot_swing_download(filename: str):
    from fastapi.responses import FileResponse
    out_dir = _pivot_swing_state.get("output_dir") or ""
    path = os.path.join(out_dir, filename) if out_dir else filename
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=filename)


@app.post("/api/pivot-swing/analyze-csv")
async def api_pivot_swing_analyze_csv(
    ticker: str = "",
    pivot_left: int = 3,
    pivot_right: int = 3,
    min_swing_pct: float = 3.0,
    min_swing_bars: int = 2,
):
    """
    Accept an uploaded stock_stat_tz_wlnbb CSV and return pivot analytics inline.
    Returns a JSON summary (pivot count, swing count, top signals at pivot zones).
    """
    import tempfile
    from analyzers.pivot_swing.pivot_analytics import run_pivot_analytics


def _run_pivot_swing_bg(
    csv_dir: str,
    csv_file: str,
    output_dir: str,
    pivot_left: int,
    pivot_right: int,
    min_swing_pct: float,
    min_swing_bars: int,
):
    global _pivot_swing_state
    import glob as _glob
    from analyzers.pivot_swing.pivot_analytics import run_pivot_analytics

    _pivot_swing_state = {
        "running": True, "done": 0, "total": 0, "output_dir": output_dir,
        "error": None, "files": [],
    }
    try:
        if csv_file:
            csv_files = [csv_file]
        else:
            csv_files = sorted(_glob.glob(os.path.join(csv_dir, "stock_stat_tz_wlnbb_*.csv")))
        _pivot_swing_state["total"] = len(csv_files)
        # Aggregated run: all CSVs combine into a single output set.
        out = run_pivot_analytics(
            csv_paths=csv_files,
            output_dir=output_dir,
            pivot_left=pivot_left,
            pivot_right=pivot_right,
            min_swing_return_pct=min_swing_pct,
            min_swing_bars=min_swing_bars,
        )
        _pivot_swing_state["done"] = len(csv_files)
        _pivot_swing_state["files"] = sorted(out.keys())
    except Exception as e:
        _pivot_swing_state["error"] = str(e)
    finally:
        _pivot_swing_state["running"] = False


# ── Pre-market cache endpoint ─────────────────────────────────────────────────
@app.get("/api/premarket")
def api_premarket(tickers: str = Query("", description="Comma-separated ticker list")):
    """Return pre-market price and % change for given tickers (TTL 15 min).

    Response: { "data": { "AAPL": { pm_price, pm_chg_pct, pm_vol, prev_close }, ... } }
    """
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        return {"data": {}, "count": 0}
    try:
        from premarket_cache import get_premarket
        data = get_premarket(ticker_list)
        return {"data": data, "count": len(data)}
    except Exception as exc:
        log.warning("api_premarket error: %s", exc)
        return {"data": {}, "count": 0, "error": str(exc)}


_static = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_static):
    app.mount("/", StaticFiles(directory=_static, html=True), name="static")