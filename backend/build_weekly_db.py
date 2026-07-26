"""
build_weekly_db.py — build a weekly analytics DB (1W bars, last 5 years).

Same `bars`-table structure as the 1D studio DB. `date` stays DATE (not TIMESTAMP)
because weekly bars are one-per-week. Every signal/indicator column is identical —
computed by the same pipeline (api_bar_signals + enricher) on weekly OHLCV.

  • Multiprocessing — CPU/GIL-bound pipeline, one process per core.
  • Resumable — tickers already present are skipped (unless --force).

Usage:
    uv run python build_weekly_db.py --tickers AAPL                # test single
    uv run python build_weekly_db.py --universe sp500
    uv run python build_weekly_db.py --all                         # full universe
    uv run python build_weekly_db.py --all --workers 8
Output: ~/Downloads/studio_1w.duckdb  (table `bars`, date=DATE)
"""
from __future__ import annotations
import argparse, os, sys, time

sys.path.insert(0, os.path.dirname(__file__))
from studio.paths import ANALYTICS_DB as ONE_DB, WEEKLY_DB as OUT_DB  # noqa: E402
TF      = "1w"
DAYS    = 1825   # ~5 years

os.environ["STUDIO_DB_PATH"] = OUT_DB

import duckdb, pandas as pd  # noqa: E402

_UNIVERSES = ["sp500", "nasdaq", "russell2k"]
_DB_COLS: set | None = None


# ── env / schema ──────────────────────────────────────────────────────────────
def _load_env():
    p = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(p):
        for line in open(p):
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.strip().split("=", 1)
                os.environ.setdefault(k, v.strip().strip('"').strip("'"))


def ensure_schema(db_path: str) -> set:
    """Create <db> with the 1D `bars` columns — date stays as DATE (not TIMESTAMP)."""
    src = duckdb.connect(ONE_DB, read_only=True)
    cols = src.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name='bars' ORDER BY ordinal_position").fetchall()
    src.close()
    if not cols:
        raise RuntimeError(f"no bars schema in {ONE_DB}")
    con = duckdb.connect(db_path)
    has = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='bars'").fetchone()[0]
    if not has:
        # Keep original types (date stays DATE — one bar per week, no collision)
        defs = [f'"{n}" {t}' for n, t in cols]
        con.execute("CREATE TABLE bars (\n  " + ",\n  ".join(defs) +
                    ",\n  PRIMARY KEY (ticker, date, universe)\n)")
        con.execute("CREATE INDEX idx_bars_tk ON bars(ticker, universe, date)")
        print(f"created bars table ({len(cols)} cols, date=DATE) in {db_path}")
    db_cols = set(r[0] for r in con.execute("DESCRIBE bars").fetchall())
    con.close()
    return db_cols


def universe_tickers(universe: str) -> list[str]:
    src = duckdb.connect(ONE_DB, read_only=True)
    tks = [r[0] for r in src.execute(
        "SELECT DISTINCT ticker FROM bars WHERE universe=? ORDER BY ticker", [universe]).fetchall()]
    src.close()
    return tks


# ── worker ────────────────────────────────────────────────────────────────────
def _init_worker(db_cols: set):
    global _DB_COLS
    _DB_COLS = db_cols
    import main  # warm heavy import once per process


def _build_one(item):
    tk, u = item
    try:
        import main
        from data_polygon import fetch_bars
        from studio.incremental_delta import _bar_to_db_row
        from studio.enricher import enrich_ticker_df

        df = fetch_bars(tk, interval=TF, days=DAYS)
        if df is None or len(df) == 0:
            return (tk, u, None, "empty")

        bars = main.api_bar_signals(tk, tf=TF, bars=len(df), universe=u, _df=df)
        rows = []
        for b in bars:
            try:
                rows.append(_bar_to_db_row(tk, b, u, _DB_COLS))
            except Exception:
                pass
        if not rows:
            return (tk, u, None, "no-rows")

        rd = pd.DataFrame(rows)
        # date comes as ISO string ('2021-06-13') from _bar_to_db_row for daily/weekly bars
        rd["date"] = pd.to_datetime(rd["date"], errors="coerce")
        rd = rd.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

        en = enrich_ticker_df(rd)
        en = en[[c for c in en.columns if c in _DB_COLS]].copy()
        en["ticker"]   = tk
        en["universe"] = u
        return (tk, u, en, None)
    except Exception as e:
        return (tk, u, None, str(e)[:140])


def _coerce(df: pd.DataFrame, con) -> pd.DataFrame:
    from studio.importer import _DB_BOOL_COLS
    for c in _DB_BOOL_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int8")
    schema = con.execute("DESCRIBE bars").fetchdf()
    numt = {"DOUBLE", "FLOAT", "REAL", "BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT", "DECIMAL"}
    for _, r in schema.iterrows():
        c = r["column_name"]
        if c in df.columns and any(t in str(r["column_type"]).upper() for t in numt):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def run(targets, db_path, workers, force):
    from concurrent.futures import ProcessPoolExecutor, as_completed
    db_cols = ensure_schema(db_path)
    con = duckdb.connect(db_path)
    next_id = (con.execute("SELECT coalesce(max(id),0) FROM bars").fetchone()[0]) + 1
    done = set() if force else set(r[0] for r in con.execute("SELECT DISTINCT ticker FROM bars").fetchall())
    todo = [(tk, u) for tk, u in targets if force or tk not in done]
    print(f"tf={TF} days={DAYS} db={db_path}")
    print(f"targets={len(targets)} done={len(targets)-len(todo)} todo={len(todo)} workers={workers}")
    if not todo:
        con.close(); print("nothing to build."); return

    t0 = time.time(); built = ins = errs = 0
    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker, initargs=(db_cols,)) as ex:
        futs = [ex.submit(_build_one, it) for it in todo]
        for fut in as_completed(futs):
            tk, u, en, err = fut.result()
            built += 1
            if err or en is None or len(en) == 0:
                errs += 1
                if err and "empty" not in err:
                    print(f"  ✗ {tk}: {err}")
            else:
                en = en.copy()
                en["id"] = range(next_id, next_id + len(en)); next_id += len(en)
                en = _coerce(en, con)
                cols = [c for c in en.columns if c in db_cols]
                q = ",".join('"' + c + '"' for c in cols)
                con.register("tmp_ins", en[cols])
                con.execute(f"INSERT INTO bars ({q}) SELECT {q} FROM tmp_ins")
                con.unregister("tmp_ins")
                ins += len(en)
            if built % 50 == 0 or built == len(todo):
                el = time.time() - t0; rate = built / el if el else 0
                eta = (len(todo) - built) / rate / 60 if rate else 0
                print(f"  [{built}/{len(todo)}] rows={ins:,} errs={errs} | {rate:.1f} tk/s | ETA {eta:.0f}min",
                      flush=True)
    con.close()
    print(f"\nDONE: {ins:,} rows · {built-errs} tickers · {errs} errors · {(time.time()-t0)/60:.1f}min\nDB: {db_path}")


if __name__ == "__main__":
    _load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers",  default="")
    ap.add_argument("--universe", default="", choices=["", *_UNIVERSES])
    ap.add_argument("--all",      action="store_true")
    ap.add_argument("--workers",  type=int, default=max(2, (os.cpu_count() or 4) - 1))
    ap.add_argument("--force",    action="store_true")
    a = ap.parse_args()

    targets: list[tuple[str, str]] = []
    if a.tickers:
        src = duckdb.connect(ONE_DB, read_only=True)
        for tk in [t.strip().upper() for t in a.tickers.split(",") if t.strip()]:
            row = src.execute("SELECT universe FROM bars WHERE ticker=? LIMIT 1", [tk]).fetchone()
            targets.append((tk, row[0] if row else "sp500"))
        src.close()
    else:
        unis = _UNIVERSES if a.all else ([a.universe] if a.universe else ["sp500"])
        seen: set = set()
        for u in unis:
            for tk in universe_tickers(u):
                if tk not in seen:
                    seen.add(tk); targets.append((tk, u))

    run(targets, OUT_DB, workers=a.workers, force=a.force)
