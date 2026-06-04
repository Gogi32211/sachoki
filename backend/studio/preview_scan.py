"""
preview_scan.py — hybrid "Preview" scan: DB history + today's live forming bar.

For each ticker in the selected universe(s):
  1. Pull the last `warmup_bars` CLOSED daily bars from the DuckDB DB (one query).
  2. Append TODAY's still-forming daily bar fetched LIVE from the Massive snapshot.
  3. Recompute the full signal suite on the combined series via api_bar_signals
     (the SAME engine that builds the DB), using the _df-injection + _last_only
     fast path (engines run over the full series; only the latest bar's dict is
     assembled → ~8× faster, identical output).
  4. Map today's bar through the SAME chain the DB scan uses
     (api_bar_signals dict → _bar_to_db_row → _row_to_dict) so the emitted row
     shape is byte-for-byte identical to a DB-instant scan row.

Parallelised with a ProcessPoolExecutor — api_bar_signals is CPU/GIL-bound, so
threads don't help; processes give true multi-core speedup (~36s for S&P500 on
a 10-core box vs ~13min single-threaded).

Use case: act on TODAY's signals shortly before market close, before the
premarket gap. If the regular session is NOT open, falls back to the plain DB
scan (run_ultra_db_scan) tagged preview_session="closed".

Endpoint: POST /api/studio/ultra-preview  (backend/studio_api.py)
"""
from __future__ import annotations

import os
import sys
import time
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone

import pandas as pd

log = logging.getLogger(__name__)

_BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Signals whose N=lookback (evHit) filter reads r.<col>_age directly. For the
# preview we set age 0 when the signal fires on today's bar. Mirrors the local
# set in ultra_db_scan.run_ultra_db_scan.
_EMIT_AGE_FIELDS = {
    "ad_fresh", "ad_cluster",
    "prebreak_prime", "prebreak_ready", "prebreak_watch",
    "pb_lvbo", "pb_stop_cause", "pb_wvf_confirm", "pb_macro_penalty",
    "pb_pp_rtv", "pb_fly_cd_c", "pb_follow_confirm", "seq_l34_eb",
    "wyc_spring", "wyc_sos", "wyc_in_tr", "wyc_sow",
    "swing_type",
}


def _et_today_ts():
    """Today's date as a UTC-midnight Timestamp (matches fetch_ohlcv's UTC index)."""
    try:
        from zoneinfo import ZoneInfo
        now_et = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        now_et = datetime.now(timezone.utc)
    return pd.Timestamp(now_et.date(), tz="UTC")


# ── Worker process globals + functions (module-level → picklable) ────────────
_MP_DB_COLS: set | None = None


def _mp_init(backend_dir: str, db_cols: set):
    """ProcessPool worker initializer — warm the heavy `main` import once per
    worker and stash db_cols so each task doesn't re-pickle the 392-col set."""
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    global _MP_DB_COLS
    _MP_DB_COLS = db_cols
    import main  # noqa: F401 — warm import (defines api_bar_signals + all engines)


def _build_df(recs, today_bar, today_iso):
    """Assemble the hybrid OHLCV df (DB history + today's forming bar) matching
    fetch_ohlcv's schema: UTC DatetimeIndex, cols open/high/low/close/volume."""
    df = pd.DataFrame(recs)
    df.index = pd.to_datetime(df.pop("date"), utc=True)
    df = df[["open", "high", "low", "close", "volume"]]
    df = df[~df.index.duplicated(keep="last")].sort_index()
    ts = pd.Timestamp(today_iso)
    df.loc[ts, ["open", "high", "low", "close", "volume"]] = [
        today_bar["open"], today_bar["high"], today_bar["low"],
        today_bar["close"], today_bar["volume"],
    ]
    return df.sort_index()


def _scan_one_mp(payload):
    """Worker: hybrid df → recompute (last-only) → DB-shaped row for today."""
    if _BACKEND_DIR not in sys.path:
        sys.path.insert(0, _BACKEND_DIR)
    try:
        from main import api_bar_signals
        from studio.incremental_delta import _bar_to_db_row

        tk, universe, recs, today_bar, today_iso = payload
        if not recs or len(recs) < 2:
            return None
        df = _build_df(recs, today_bar, today_iso)
        bars = api_bar_signals(tk, tf="1d", universe=universe, _df=df, _last_only=True)
        if not bars:
            return None
        b = bars[-1]
        row = _bar_to_db_row(tk, b, universe, _MP_DB_COLS)
        # bulk_export HEADERS don't carry prebreak_v3 → inject from the bar dict.
        row["prebreak_v3"]         = int(b.get("prebreak_v3") or 0)
        row["prebreak_v3_reasons"] = str(b.get("prebreak_v3_reasons") or "")
        # rsi/cci live in the bar dict as 'rsi'/'cci'; DB cols are rsi_14/cci_20.
        if b.get("rsi") is not None:
            row["rsi_14"] = b.get("rsi")
        if b.get("cci") is not None:
            row["cci_20"] = b.get("cci")
        row["ticker"]   = tk
        row["universe"] = universe
        return row
    except Exception as e:  # never break the pool — drop the ticker
        return {"_preview_error": f"{payload[0] if payload else '?'}: {e}"}


def run_preview_scan(
    universes:    list[str] | None = None,
    min_price:    float | None     = None,
    min_volume:   int   | None     = None,
    age_lookback: int               = 20,
    warmup_bars:  int               = 260,
) -> dict:
    """Hybrid preview scan. Returns the same response shape as run_ultra_db_scan
    with data_source='studio_preview' (or falls back to the DB scan off-hours)."""
    from studio.db import get_conn
    from studio.ultra_db_scan import _row_to_dict, run_ultra_db_scan
    from premarket_cache import get_today_bars

    universes = universes or ["sp500", "nasdaq"]
    started = time.time()

    # ── Batch DB history (one query) ─────────────────────────────────────────
    conn = get_conn(read_only=True)
    try:
        db_cols = set(conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())
        placeholders = ",".join("?" * len(universes))
        hist_all = conn.execute(f"""
            WITH ranked AS (
              SELECT ticker, universe, date, open, high, low, close, volume,
                     ROW_NUMBER() OVER (PARTITION BY ticker, universe ORDER BY date DESC) AS rn
              FROM bars
              WHERE universe IN ({placeholders})
            )
            SELECT ticker, universe, date, open, high, low, close, volume
            FROM ranked WHERE rn <= ?
            ORDER BY ticker, universe, date
        """, [*universes, warmup_bars]).fetchdf()
    finally:
        conn.close()

    all_tickers = sorted(hist_all["ticker"].unique().tolist())
    today_bars = get_today_bars(all_tickers)

    # No live bars (session closed / weekend) → fall back to the DB scan.
    if not today_bars:
        res = run_ultra_db_scan(universes, min_price=min_price, min_volume=min_volume,
                                age_signals=None, age_lookback=age_lookback)
        res["data_source"]     = "studio_preview"
        res["preview_session"] = "closed"
        res["preview_note"]    = "Market closed — showing latest closed DB bars (no live forming bar)."
        return res

    today_iso = _et_today_ts().isoformat()

    # ── Build per-ticker payloads (picklable: date → str records) ────────────
    payloads = []
    for tk, g in hist_all.groupby("ticker"):
        tb = today_bars.get(tk.upper())
        if not tb or g.empty:
            continue
        universe = str(g["universe"].iloc[0])
        sub = g[["date", "open", "high", "low", "close", "volume"]].copy()
        sub["date"] = sub["date"].astype(str)
        payloads.append((tk, universe, sub.to_dict("records"), tb, today_iso))

    # ── Parallel recompute (true multi-core via ProcessPool) ─────────────────
    rows: list[dict] = []
    errors = 0
    workers = max(2, min(10, (os.cpu_count() or 4)))
    try:
        with ProcessPoolExecutor(max_workers=workers, initializer=_mp_init,
                                 initargs=(_BACKEND_DIR, db_cols)) as ex:
            for r in ex.map(_scan_one_mp, payloads, chunksize=4):
                if not r:
                    continue
                if "_preview_error" in r:
                    errors += 1
                    log.warning("preview_scan worker: %s", r["_preview_error"])
                    continue
                rows.append(r)
    except Exception as e:
        # Pool blew up (rare) → sequential in-process fallback so the user still
        # gets a result. Slower but correct.
        log.warning("preview_scan ProcessPool failed (%s) — sequential fallback", e)
        _mp_init(_BACKEND_DIR, db_cols)
        for p in payloads:
            r = _scan_one_mp(p)
            if r and "_preview_error" not in r:
                rows.append(r)

    # ── Emit via the SAME _row_to_dict the DB scan uses ──────────────────────
    results = []
    if rows:
        latest = pd.DataFrame(rows)
        if min_price is not None and "close" in latest.columns:
            latest = latest[pd.to_numeric(latest["close"], errors="coerce") >= min_price]
        if min_volume is not None and "avg_vol_20d" in latest.columns:
            latest = latest[pd.to_numeric(latest["avg_vol_20d"], errors="coerce") >= min_volume]
        today_date = today_iso[:10]
        for _, row in latest.iterrows():
            d = _row_to_dict(row)
            d["scan_date"] = today_date
            d["_preview"]  = True
            for col in _EMIT_AGE_FIELDS:        # today's firing signals → age 0
                if d.get(col):
                    d[f"{col}_age"] = 0
            results.append(d)

    duration = time.time() - started
    log.info("preview_scan: %d rows (%d payloads, %d errors, %d workers) in %.1fs for %s",
             len(results), len(payloads), errors, workers, duration, universes)
    return {
        "results":         results,
        "running":         False,
        "stage":           "done",
        "progress_pct":    100,
        "elapsed_seconds": round(duration, 2),
        "data_source":     "studio_preview",
        "preview_session": "open",
        "universes":       universes,
        "row_count":       len(results),
        "live_bar_count":  len(payloads),
        "errors":          errors,
        "min_price":       min_price,
        "min_volume":      min_volume,
        "scanned_at":      pd.Timestamp.utcnow().isoformat(),
    }
