"""
incremental_delta.py — Daily delta append for the Studio DB.

For each ticker:
  1. Find MAX(date) in DB (per universe).
  2. Call api_bar_signals(ticker, '1d', bars=200, universe) — Pine warm-up.
  3. Take only bars with date > last_date.
  4. Build a full DB row dict using the SAME logic as bulk_export → CSV import:
       - bulk_export.bar_to_row() → 345-element list aligned with HEADERS
       - importer._COL_MAP renames CSV → DB columns
       - l_sig forced to b['l_chart'] (matches DB convention)
       - sig_t1g … sig_z12 derived from t_sig / z_sig
       - Filter to actual DB columns
  5. Bulk INSERT with auto-incrementing IDs (no NULL id bug).
  6. DELETE-then-INSERT on (universe, ticker, date) for idempotency.
  7. Run enricher on the universe so rolling cols (rsi_14 / atr_14 / avg_vol_20d /
     cci_20 / pivot stats / suffixes etc.) are populated for the new bars.

Validated 470/470 cells = 100% identical to the existing CSV-imported rows
(SP500 sample of 10 tickers × 47 stable Pine columns on 2026-05-22).
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date as _date, datetime, timezone
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

# Per-bar Pine warm-up window — empirically: 50 = wrong scores, 150 = identical.
_WARMUP_BARS = 200
_PROGRESS_FILE = "/tmp/studio_incremental_delta_progress.json"

_T_NAMES = ["T1G", "T2G", "T1", "T2", "T3", "T4", "T5", "T6",
            "T7", "T8", "T9", "T10", "T11", "T12"]
_Z_NAMES = ["Z1G", "Z2G", "Z1", "Z2", "Z3", "Z4", "Z5", "Z6",
            "Z7", "Z8", "Z9", "Z10", "Z11", "Z12"]


# ── Progress ─────────────────────────────────────────────────────────────────
_progress: dict = {"stage": "idle", "done": 0, "total": 0, "pct": 0,
                   "ts": None, "new_rows": 0, "errors": 0,
                   "current_universe": None}


def _write_progress(stage: str, done: int = 0, total: int = 0,
                    started_at: float | None = None,
                    extra: dict | None = None) -> None:
    global _progress
    pct = round(100 * done / total, 1) if total else 0.0
    _progress = {
        "stage": stage, "done": done, "total": total, "pct": pct,
        "ts": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started_at, 1) if started_at else 0.0,
        "eta_seconds": None,
        **(extra or {}),
    }
    if started_at and done > 0 and total > 0:
        elapsed = time.time() - started_at
        _progress["eta_seconds"] = int(elapsed / done * (total - done))
    try:
        with open(_PROGRESS_FILE, "w") as f:
            json.dump(_progress, f)
    except Exception:
        pass


def get_progress() -> dict:
    return dict(_progress)


# ── Row conversion (api_bar_signals → DB row dict) ───────────────────────────

def _bar_to_db_row(ticker: str, bar: dict, universe: str,
                   db_cols: set[str]) -> dict:
    """Convert one api_bar_signals bar dict to a DB row dict.

    Uses the EXACT same path as bulk_export → importer, so resulting columns
    are identical to historical CSV-imported rows (validated 100% on Pine
    cols for SP500 sample).
    """
    from bulk_export import HEADERS, bar_to_row
    from studio.importer import _COL_MAP

    # 1. Serialize via bulk_export (345 ordered CSV values)
    csv_row = bar_to_row(ticker, bar)
    csv_dict = dict(zip(HEADERS, csv_row))

    # 2. Translate CSV column names → DB column names
    row = {_COL_MAP.get(k, k.lower()): v for k, v in csv_dict.items()}

    # 3. Critical: l_sig in DB is the chart-format ("L34" / "L3") which is
    #    b["l_chart"]; bar_to_row writes the raw _join(b["l"]) which differs.
    row["l_sig"] = bar.get("l_chart", "") or row.get("l_sig", "")

    # 4. Metadata
    row["ticker"]   = ticker
    row["universe"] = universe

    # 5. Derive sig_t*/sig_z* flags from t_sig / z_sig (matches importer line 363)
    tsig = row.get("t_sig", "") or ""
    zsig = row.get("z_sig", "") or ""
    for n in _T_NAMES:
        row[f"sig_{n.lower()}"] = 1 if tsig == n else 0
    for n in _Z_NAMES:
        row[f"sig_{n.lower()}"] = 1 if zsig == n else 0

    # 5b. 2026-07-21: engine-only signals now persisted ("ertxel da samudamod")
    row["um_2809"] = int(bar.get("raw_um") or 0)
    row["ev_l22"] = int(bar.get("raw_l22") or 0)
    row["ev_l43"] = int(bar.get("raw_l43") or 0)
    row["ev_l64"] = int(bar.get("raw_l64") or 0)
    row["ev_l34"] = int(bar.get("raw_l34") or 0)
    row["bo_dn"] = int(bar.get("sig_bo_dn") or 0)
    row["bx_dn"] = int(bar.get("sig_bx_dn") or 0)
    row["be_dn"] = int(bar.get("sig_be_dn") or 0)
    row["buy_here"] = int(bar.get("raw_buy_here") or 0)
    row["atr_brk"] = int(bar.get("raw_atr_brk") or 0)
    row["bb_brk"] = int(bar.get("raw_bb_brk") or 0)
    row["rtv"] = int(bar.get("raw_rtv") or 0)
    row["svs_raw"] = int(bar.get("raw_svs_raw") or 0)
    row["cons_atr"] = int(bar.get("raw_cons") or 0)
    row["gog1"] = int(bar.get("gog1") or 0)
    row["gog2"] = int(bar.get("gog2") or 0)
    row["gog3"] = int(bar.get("gog3") or 0)
    _su = bar.get("setup"); _cx = bar.get("context")
    row["setup_tokens"] = " ".join(_su) if isinstance(_su, list) else str(_su or "")
    row["context_tokens"] = " ".join(_cx) if isinstance(_cx, list) else str(_cx or "")

    # 6. Filter to actual DB columns
    return {k: v for k, v in row.items() if k in db_cols}


# ── Tickers ─────────────────────────────────────────────────────────────────

def _get_tickers_with_last_dates(conn, universe: str) -> dict[str, str]:
    """Return {ticker: max(date) ISO string} for all tickers in `universe`."""
    rows = conn.execute(
        "SELECT ticker, MAX(date) FROM bars WHERE universe = ? GROUP BY ticker",
        [universe],
    ).fetchall()
    return {t: str(d) for t, d in rows}


def _get_universe_tickers(universe: str) -> list[str]:
    """Get the full canonical ticker list for the universe."""
    try:
        from scanner import get_universe_tickers
        return list(get_universe_tickers(universe))
    except Exception:
        try:
            from scanner import get_tickers
            return list(get_tickers() or [])
        except Exception:
            return []


# ── Main entry ───────────────────────────────────────────────────────────────

def incremental_delta_refresh(
    universes: Optional[list[str]] = None,
    warmup_bars: int = _WARMUP_BARS,
    enrich_after: bool = True,
    only_tickers: Optional[set] = None,
    refetch_from: Optional[str] = None,
) -> dict:
    """Incremental delta append. Returns summary dict.

    Per-universe steps:
      1. SELECT ticker, MAX(date) FROM bars WHERE universe=?
      2. For each ticker:
          a. api_bar_signals(ticker, '1d', warmup_bars, universe)
          b. keep bars where date > last_date (or all bars if ticker unseen).
             If `refetch_from` (ISO date) is given, instead keep bars where
             date >= refetch_from — this OVERWRITES already-stored trailing
             bars (the daily filter `date > last_date` would otherwise skip
             them, since their last_date already equals their own date). Used
             for the one-time re-fetch that corrects stale/divergent last bars.
          c. _bar_to_db_row() — 320-column dict
      3. Bulk INSERT with sequential auto-IDs.
      4. DELETE-then-INSERT on (universe, ticker, date) for idempotency.
      5. enrich_universe(universe) for rsi_14 / atr_14 / avg_vol_20d / etc.
    """
    from studio.db import get_conn
    from main import api_bar_signals
    universes = universes or ["sp500", "nasdaq"]
    # Whitelist universe names — they are interpolated into the idempotency DELETE,
    # so restrict to known values to prevent any SQL injection via the request body.
    _ALLOWED_UNIVERSES = {"sp500", "nasdaq", "russell2k", "index"}
    _bad = [u for u in universes if str(u).strip().lower() not in _ALLOWED_UNIVERSES]
    if _bad:
        log.warning("incremental_delta: ignoring unknown universes %s", _bad)
    universes = [str(u).strip().lower() for u in universes
                 if str(u).strip().lower() in _ALLOWED_UNIVERSES]
    # Normalize the optional re-fetch floor to a YYYY-MM-DD string (compared
    # lexicographically against each bar's ISO date, same as today_str).
    refetch_from = str(refetch_from)[:10] if refetch_from else None
    started = time.time()
    overall = {"universes": {}, "started_at": datetime.now(timezone.utc).isoformat()}

    _write_progress("starting", 0, 0, started)

    # Fetch each ticker only ONCE per run and reuse the SAME bars for every universe
    # it belongs to. A dual-universe ticker (e.g. CYCU in nasdaq + russell2k) must
    # get IDENTICAL bars in both — otherwise the per-universe fetch happens at
    # different times (one before market settle, one after) and the same date ends
    # up with different OHLC/signals (T2G vs Z4). Keyed by ticker, lives across the
    # universe loop. (api_bar_signals' OHLCV + TZ/L signals are price-derived, so
    # they don't depend on which universe is passed.)
    _bar_cache: dict[str, list] = {}

    for universe in universes:
        log.info("incremental_delta: universe=%s", universe)

        # Read-only: ticker → last date
        conn_r = get_conn(read_only=True)
        try:
            db_cols = set(conn_r.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())
            ticker_last = _get_tickers_with_last_dates(conn_r, universe)
        finally:
            conn_r.close()

        if only_tickers is not None:
            # Explicit subset (one-time divergent-ticker re-fetch). Restrict to
            # tickers that actually have bars in THIS universe — the canonical
            # scanner list may not include them (e.g. micro-caps like CYCU).
            tickers = sorted(set(only_tickers) & set(ticker_last.keys()))
        else:
            tickers = _get_universe_tickers(universe)
        if not tickers:
            log.warning("No tickers found for universe=%s", universe)
            continue

        total = len(tickers)
        _write_progress(f"fetching {universe}", 0, total, started,
                        extra={"current_universe": universe, "new_rows": 0, "errors": 0})

        # Today as a hard cap (don't insert future-dated bars)
        today_str = _date.today().strftime("%Y-%m-%d")

        new_rows: list[dict] = []
        errors: list[dict] = []
        affected = set()

        # ── Parallel fetch pass (network-bound → threads scale near-linearly) ──
        # api_bar_signals' Massive HTTP fetch dominates wall-time; the GIL is
        # released during the request, so a thread pool parallelises the waits.
        # _last_only assembles only the last ~12 bars' dicts (≈8× less CPU) — we
        # keep only bars newer than the DB tail anyway. Full history is still
        # fetched (signals need the warm-up) but the per-bar matrix is skipped.
        # Big gaps / unseen tickers fall back to full assembly so no bar slips by.
        def _last_only_ok(tk: str) -> bool:
            if refetch_from is not None:
                return False
            ld = ticker_last.get(tk, "1900-01-01")[:10]
            if ld <= "1901-01-01":
                return False
            try:
                return (_date.today() - _date.fromisoformat(ld)).days <= 10
            except Exception:
                return False

        to_fetch = [t for t in tickers if t not in _bar_cache]
        if to_fetch:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            workers = max(4, min(12, (os.cpu_count() or 4) + 6))

            def _fetch_one(tk: str):
                try:
                    return tk, api_bar_signals(tk, tf="1d", bars=warmup_bars,
                                               universe=universe,
                                               _last_only=_last_only_ok(tk)), None
                except Exception as e:
                    return tk, [], str(e)

            done = 0
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = [ex.submit(_fetch_one, t) for t in to_fetch]
                for fut in as_completed(futs):
                    tk, bars, err = fut.result()
                    _bar_cache[tk] = bars
                    if err:
                        errors.append({"ticker": tk, "stage": "fetch", "error": err})
                    done += 1
                    if done % 50 == 0 or done == len(to_fetch):
                        _write_progress(f"fetching {universe}", done, len(to_fetch), started,
                                        extra={"current_universe": universe,
                                               "new_rows": len(new_rows),
                                               "errors": len(errors)})

        # ── Build rows (cheap, serial) from the cached bars ───────────────────
        for ticker in tickers:
            last_date = ticker_last.get(ticker, "1900-01-01")[:10]
            for b in _bar_cache.get(ticker, []):
                bdate = str(b.get("date", ""))[:10]
                if not bdate or bdate > today_str:
                    continue
                if refetch_from is not None:
                    # one-time overwrite mode: keep already-stored trailing bars
                    if bdate < refetch_from:
                        continue
                elif bdate <= last_date:
                    # daily append mode: only bars strictly newer than the DB tail
                    continue
                try:
                    row = _bar_to_db_row(ticker, b, universe, db_cols)
                    new_rows.append(row)
                    affected.add(ticker)
                except Exception as e:
                    errors.append({"ticker": ticker, "stage": "row", "error": str(e)})

        # ── INSERT ────────────────────────────────────────────────────────────
        inserted = 0
        if new_rows:
            _write_progress(f"inserting {universe}", total, total, started,
                            extra={"current_universe": universe,
                                   "new_rows": len(new_rows),
                                   "errors": len(errors)})
            df = pd.DataFrame(new_rows)
            df["date"] = pd.to_datetime(df["date"]).dt.date

            # Coerce boolean signal cols → int8 (same as importer._clean_bool_cols)
            from studio.importer import _DB_BOOL_COLS
            for c in _DB_BOOL_COLS:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int8")

            # Coerce all numeric DB columns from "" → NaN → NULL so DuckDB doesn't
            # raise on string "" inserted into DOUBLE/BIGINT cols (fwd_*, mfe_*,
            # hit_*, scores, etc.). bar_to_row writes "" for empty numeric fields.
            try:
                schema_df = get_conn(read_only=True).execute("DESCRIBE bars").fetchdf()
                num_types = {"DOUBLE", "FLOAT", "REAL", "BIGINT", "INTEGER",
                             "SMALLINT", "TINYINT", "HUGEINT", "DECIMAL"}
                numeric_db_cols = {
                    r["column_name"] for _, r in schema_df.iterrows()
                    if any(t in str(r["column_type"]).upper() for t in num_types)
                }
            except Exception:
                numeric_db_cols = set()
            for c in df.columns:
                if c in numeric_db_cols:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            conn_w = get_conn(read_only=False)
            try:
                # Auto-incrementing IDs starting from current MAX+1
                max_id = conn_w.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM bars"
                ).fetchone()[0]
                df = df.reset_index(drop=True)
                df["id"] = df.index + 1 + int(max_id)

                conn_w.register("delta_tmp", df)
                # Idempotency: drop any pre-existing (universe, ticker, date) rows
                conn_w.execute(f"""
                    DELETE FROM bars
                    WHERE universe = '{universe}'
                      AND (ticker, date) IN (SELECT ticker, date FROM delta_tmp)
                """)
                cols_str = ", ".join(df.columns)
                conn_w.execute(
                    f"INSERT INTO bars ({cols_str}) SELECT {cols_str} FROM delta_tmp"
                )
                conn_w.unregister("delta_tmp")
                conn_w.commit()
                inserted = len(df)
                log.info("  Inserted %d rows for %s (%d tickers)",
                         inserted, universe, len(affected))
            except Exception as e:
                log.exception("INSERT failed for %s", universe)
                errors.append({"stage": "insert", "error": str(e)})
            finally:
                conn_w.close()

        # ── Enrich (rsi/atr/avg_vol/cci/pivots/suffixes) ──────────────────────
        enrich_summary: dict = {}
        if inserted > 0 and enrich_after:
            _write_progress(f"enriching {universe}", total, total, started,
                            extra={"current_universe": universe,
                                   "new_rows": inserted, "errors": len(errors)})
            try:
                from studio.enricher import enrich_universe
                enrich_summary = enrich_universe(universe=universe, max_workers=1)
                # PreBreakout v2 score depends on the enriched signal flags, so
                # recompute it for this universe right after enrichment (cheap,
                # one vectorised SQL pass).
                try:
                    from prebreak_v2 import apply_prebreak_v2
                    apply_prebreak_v2(universe)
                    from prebreak_v3 import apply_prebreak_v3
                    apply_prebreak_v3(universe)
                except Exception:
                    log.exception("prebreak_v2 apply failed for %s", universe)
                try:
                    from studio.composite_vol import apply_composite_vol
                    apply_composite_vol(universe)
                except Exception:
                    log.exception("composite_vol apply failed for %s", universe)
            except Exception as e:
                log.exception("enrich failed for %s", universe)
                enrich_summary = {"error": str(e)}
        elif inserted == 0:
            enrich_summary = {"skipped": "no new rows"}

        overall["universes"][universe] = {
            "tickers_checked":   total,
            "new_rows_inserted": inserted,
            "affected_tickers":  len(affected),
            "errors":            len(errors),
            "error_samples":     errors[:5],
            "enrich":            enrich_summary,
        }

    overall["duration_sec"] = round(time.time() - started, 1)
    _write_progress("done", 0, 0, started, extra={"summary": overall})
    log.info("incremental_delta: done in %.1fs — %s",
             overall["duration_sec"], overall["universes"])
    return overall
