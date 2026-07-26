"""
update_intraday_db.py — Incremental update for 1H, 4H, and 1W databases.

For each ticker already in the DB:
  1. Find max(date) in DB for that ticker.
  2. Fetch last FETCH_DAYS of bars from MASSIVE API (same pipeline as build_intraday_db).
  3. DELETE bars from DB where date >= (max_date - OVERLAP days) — handles corrections.
  4. INSERT only bars with date > (max_date - OVERLAP).

Tickers NOT yet in the DB are skipped (use build_intraday_db --all for first-time build).

Usage:
    python update_intraday_db.py --tf 1h
    python update_intraday_db.py --tf 4h
    python update_intraday_db.py --tf 1w   (uses build_weekly_db pipeline)
    python update_intraday_db.py --tf 1h --workers 8
    python update_intraday_db.py --tf 1h --tickers AAPL,TSLA  (test subset)
"""
from __future__ import annotations
import argparse, os, sys, time, importlib
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(__file__))

FETCH_DAYS = 15   # bars to fetch per ticker (short warm-up is fine for last 3 days)
OVERLAP    = 3    # re-fetch/re-insert last N days to capture corrections

_TF      = "1h"   # overridden by __main__
_DB      = ""
_WEEKLY  = False

_DB_COLS: set | None = None


def _load_env():
    p = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(p):
        for line in open(p):
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.strip().split("=", 1)
                os.environ.setdefault(k, v.strip().strip('"').strip("'"))


def _init_worker(db_cols, tf, is_weekly):
    """Worker initializer: set globals in BOTH this module AND the builder module."""
    global _DB_COLS, _TF, _WEEKLY
    _DB_COLS = db_cols
    _TF      = tf
    _WEEKLY  = is_weekly
    _load_env()
    if not is_weekly:
        os.environ["INTRADAY_TF"]      = tf
        os.environ["INTRADAY_REGULAR"] = "1"
        import build_intraday_db as bidb
        bidb._DB_COLS  = db_cols
        bidb._TF       = tf
        bidb._REGULAR  = True
        bidb._init_worker(db_cols)   # warms up `import main` once per process
    else:
        import build_weekly_db as bwdb
        bwdb._DB_COLS = db_cols
        bwdb._init_worker(db_cols)


def _build_one_intraday(args):
    tk, universe, days = args
    try:
        import build_intraday_db as bidb
        return bidb._build_one((tk, universe, days))
    except Exception as e:
        return tk, universe, None, str(e)


def _build_one_weekly(args):
    tk, universe, days = args
    try:
        import build_weekly_db as bwdb
        return bwdb._build_one((tk, universe))
    except Exception as e:
        return tk, universe, None, str(e)


def run(db_path: str, tf: str, is_weekly: bool, workers: int,
        tickers_filter: list[str] | None = None):
    import duckdb, pandas as pd

    # ── schema cols ───────────────────────────────────────────────────────────
    con = duckdb.connect(db_path)
    db_cols = set(con.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())

    # ── per-ticker max date ───────────────────────────────────────────────────
    max_dates = dict(con.execute(
        "SELECT ticker, max(date)::VARCHAR FROM bars GROUP BY ticker"
    ).fetchall())
    universe_map = dict(con.execute(
        "SELECT ticker, any_value(universe) FROM bars GROUP BY ticker"
    ).fetchall())

    if tickers_filter:
        max_dates  = {t: max_dates[t]  for t in tickers_filter if t in max_dates}
        universe_map = {t: universe_map[t] for t in tickers_filter if t in universe_map}

    tickers = list(max_dates.keys())
    if not tickers:
        con.close(); print("No tickers in DB — run build_intraday_db first."); return

    print(f"tf={tf}  db={db_path}")
    print(f"tickers in DB: {len(tickers)}  fetch_days={FETCH_DAYS}  overlap={OVERLAP}  workers={workers}")
    t0 = time.time()
    built = ins = errs = skipped = 0

    build_fn = _build_one_weekly if is_weekly else _build_one_intraday
    todo = [(tk, universe_map[tk], FETCH_DAYS) for tk in tickers]

    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_init_worker,
        initargs=(db_cols, tf, is_weekly)
    ) as ex:
        futs = {ex.submit(build_fn, item): item for item in todo}
        for fut in as_completed(futs):
            tk, universe, en, err = fut.result()
            built += 1

            if err or en is None or len(en) == 0:
                if err and "empty" not in str(err).lower():
                    print(f"  ✗ {tk}: {err}")
                skipped += 1
            else:
                old_max = max_dates.get(tk, "2000-01-01")
                # cutoff: re-insert from (old_max - OVERLAP days)
                import pandas as _pd
                en["_dt_key"] = _pd.to_datetime(en["date"]).dt.normalize()
                cutoff = (_pd.to_datetime(old_max) - _pd.Timedelta(days=OVERLAP)).normalize()
                fresh = en[en["_dt_key"] > cutoff].drop(columns=["_dt_key"])

                if fresh.empty:
                    skipped += 1
                else:
                    # DELETE bars in overlap window then INSERT fresh
                    cutoff_str = cutoff.strftime("%Y-%m-%d")
                    try:
                        con.execute(
                            "DELETE FROM bars WHERE ticker=? AND universe=? AND date > ?",
                            [tk, universe, cutoff_str]
                        )
                        # Assign new IDs
                        next_id = (con.execute("SELECT coalesce(max(id),0) FROM bars").fetchone()[0]) + 1
                        fresh = fresh.copy()
                        fresh["id"] = range(next_id, next_id + len(fresh))

                        # Coerce dtypes to match DB
                        import pandas as _pd2
                        schema = con.execute("DESCRIBE bars").fetchdf()
                        num_types = ("DOUBLE", "FLOAT", "INTEGER", "BIGINT", "SMALLINT", "HUGEINT")
                        for _, row in schema.iterrows():
                            c = row["column_name"]
                            if c in fresh.columns and any(t in str(row["column_type"]).upper() for t in num_types):
                                fresh[c] = _pd2.to_numeric(fresh[c], errors="coerce")

                        cols = [c for c in fresh.columns if c in db_cols and c != "id"] + ["id"]
                        cols = [c for c in cols if c in fresh.columns]
                        q = ",".join('"' + c + '"' for c in cols)
                        con.register("tmp_ins", fresh[cols])
                        con.execute(f"INSERT INTO bars ({q}) SELECT {q} FROM tmp_ins")
                        con.unregister("tmp_ins")
                        ins += len(fresh)
                    except Exception as e:
                        print(f"  ✗ {tk} (insert): {e}")
                        errs += 1

            if built % 100 == 0 or built == len(todo):
                el = time.time() - t0
                rate = built / el if el else 0
                eta = (len(todo) - built) / rate / 60 if rate else 0
                print(f"  [{built}/{len(todo)}] +{ins:,} rows | {skipped} skipped | {errs} err | ETA {eta:.0f}min", flush=True)

    con.close()
    elapsed = time.time() - t0
    print(f"\n✅ DONE: +{ins:,} new rows · {built-errs-skipped} updated · {skipped} unchanged · {errs} errors · {elapsed/60:.1f}min")
    print(f"DB: {db_path}")


if __name__ == "__main__":
    _load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf",      default="1h", choices=["1h", "4h", "1w"])
    ap.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 1))
    ap.add_argument("--tickers", default="", help="comma-separated subset for testing")
    a = ap.parse_args()

    _TF      = a.tf
    _WEEKLY  = a.tf == "1w"
    from studio.paths import db_path
    _DB      = db_path("studio_1w.duckdb" if _WEEKLY else a.tf)

    # Set env so child processes get the right config
    if not _WEEKLY:
        os.environ["INTRADAY_TF"]      = a.tf
        os.environ["INTRADAY_REGULAR"] = "1"
        os.environ["INTRADAY_DB"]      = _DB
    os.environ["STUDIO_DB_PATH"] = _DB

    tickers_filter = [t.strip().upper() for t in a.tickers.split(",") if t.strip()] or None
    run(_DB, a.tf, _WEEKLY, a.workers, tickers_filter)
