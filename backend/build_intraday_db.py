"""
build_intraday_db.py — build a SEPARATE intraday analytics DB (1h / 30m).

Same `bars`-table structure as the 1D studio DB, with ONE change: `date` is a
TIMESTAMP (not DATE) so intraday bars don't collide on the PK (ticker,date,universe).
Every signal/indicator column is identical — computed by the SAME pipeline
(api_bar_signals + enricher) on intraday bars.

Defaults (this session's decision): tf=1h, REGULAR hours only (9:30-16:00 ET =
ET start-hours 09:00-15:00 = 7 bars/day), full 1D universe.

  • Multiprocessing — the signal+enrich pipeline is CPU/GIL-bound, so we use
    processes (one per core) not threads.
  • Resumable — tickers already present are skipped (unless --force).

Usage:
    uv run python build_intraday_db.py --tickers AAPL                  # test
    uv run python build_intraday_db.py --universe sp500
    uv run python build_intraday_db.py --all                          # full run
    uv run python build_intraday_db.py --all --tf 1h --workers 9
    uv run python build_intraday_db.py --all --extended               # opt back into ext hours
Output: ~/Downloads/studio_<tf>.duckdb  (table `bars`, date=TIMESTAMP)
"""
from __future__ import annotations
import argparse, os, sys, time

sys.path.insert(0, os.path.dirname(__file__))
from studio.paths import ANALYTICS_DB as ONE_DB, db_path as _dbp  # noqa: E402

# Worker config comes via env so spawned child processes (macOS spawn) inherit it
# at import time — set by __main__ BEFORE the pool is created.
_TF      = os.environ.get("INTRADAY_TF", "1h")
_REGULAR = os.environ.get("INTRADAY_REGULAR", "1") == "1"
_DB      = os.environ.get("INTRADAY_DB", _dbp(_TF))
os.environ["STUDIO_DB_PATH"] = _DB          # point studio.db / enricher at this DB

import duckdb, pandas as pd  # noqa: E402

_UNIVERSES = ["sp500", "nasdaq", "russell2k"]
_DB_COLS: set | None = None   # per-process, set by the pool initializer


# ── env / schema ──────────────────────────────────────────────────────────────
def _load_env():
    p = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(p):
        for line in open(p):
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.strip().split("=", 1)
                os.environ.setdefault(k, v.strip().strip('"').strip("'"))


def ensure_schema(db_path: str) -> set:
    """Create <db> with the 1D `bars` columns, but date as TIMESTAMP."""
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
        defs = [f'"{n}" {"TIMESTAMP" if n=="date" else t}' for n, t in cols]
        con.execute("CREATE TABLE bars (\n  " + ",\n  ".join(defs) +
                    ",\n  PRIMARY KEY (ticker, date, universe)\n)")
        con.execute("CREATE INDEX idx_bars_tk ON bars(ticker, universe, date)")
        print(f"created bars table ({len(cols)} cols, date=TIMESTAMP) in {db_path}")
    db_cols = set(r[0] for r in con.execute("DESCRIBE bars").fetchall())
    con.close()
    return db_cols


def universe_tickers(universe: str) -> list[str]:
    src = duckdb.connect(ONE_DB, read_only=True)
    tks = [r[0] for r in src.execute(
        "SELECT DISTINCT ticker FROM bars WHERE universe=? ORDER BY ticker", [universe]).fetchall()]
    src.close()
    return tks


def liquid_tickers(min_dv_m: float) -> set:
    """Tickers whose median daily dollar-volume (last ~60 1D bars) ≥ $min_dv_m M."""
    src = duckdb.connect(ONE_DB, read_only=True)
    df = src.execute("""
        WITH recent AS (
          SELECT ticker, close*volume AS dv,
                 row_number() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
          FROM bars WHERE close IS NOT NULL AND volume IS NOT NULL)
        SELECT ticker FROM recent WHERE rn<=60 GROUP BY ticker
        HAVING median(dv)/1e6 >= ?""", [min_dv_m]).fetchall()
    src.close()
    return set(r[0] for r in df)


# ── worker (runs in a separate process) ───────────────────────────────────────
def _init_worker(db_cols: set):
    global _DB_COLS
    _DB_COLS = db_cols
    import main  # noqa: warm the heavy import once per process


def _regular_only(df: pd.DataFrame) -> pd.DataFrame:
    """Keep regular US session 30m bars (DST-aware): ET 09:30-15:59 (13 bars/day).
    Massive 30m bars fall on :00/:30 ET, so 09:30 (the session open) lands exactly
    on a boundary — the basis for TV-aligned re-aggregation in resample_session()."""
    et = df.index.tz_convert("America/New_York")
    mins = et.hour * 60 + et.minute
    return df[(mins >= 570) & (mins < 960)]   # 09:30 .. 16:00


def resample_session(df30: pd.DataFrame, tf: str) -> pd.DataFrame:
    """Re-aggregate regular-session 30m bars into SESSION-ANCHORED tf bars so they
    match TradingView (whose intraday bars start at the 09:30 ET session open, NOT
    on the UTC grid that Massive's native 1h/4h aggregates use).
      • 1h → 7 bars/day: 09:30,10:30,11:30,12:30,13:30,14:30,15:30 ET (last = 30m)
      • 4h → 2 bars/day: 09:30-13:30 and 13:30-16:00 ET
    Input/return index is UTC tz-aware; each bar labeled by its FIRST 30m timestamp."""
    d = _regular_only(df30).copy()
    if len(d) == 0:
        return d[["open", "high", "low", "close", "volume"]]
    et = d.index.tz_convert("America/New_York")
    since = (et.hour * 60 + et.minute) - 570          # minutes since 09:30
    width = 60 if tf == "1h" else 240                 # 1h or 4h slot width
    d["_day"] = et.normalize()
    d["_slot"] = (since // width).astype(int)
    g = d.groupby(["_day", "_slot"], sort=True)
    out = g.agg(open=("open", "first"), high=("high", "max"), low=("low", "min"),
                close=("close", "last"), volume=("volume", "sum"))
    first_ts = g.apply(lambda x: x.index.min())       # session-anchored bar-start (UTC)
    out.index = pd.DatetimeIndex(first_ts.values).tz_localize("UTC")
    return out.sort_index()[["open", "high", "low", "close", "volume"]]


def _process(tk, u, df, tf):
    """OHLCV df (UTC index) → signals + enrich → DB-col frame for timeframe `tf`."""
    import main
    from studio.incremental_delta import _bar_to_db_row
    from studio.enricher import enrich_ticker_df
    if df is None or len(df) == 0:
        return None
    bars = main.api_bar_signals(tk, tf=tf, bars=len(df), universe=u, _df=df)
    rows = []
    for b in bars:
        try:
            rows.append(_bar_to_db_row(tk, b, u, _DB_COLS))
        except Exception:
            pass
    if not rows:
        return None
    rd = pd.DataFrame(rows)
    # api_bar_signals emits epoch-seconds for intraday tf but a 'YYYY-MM-DD' string for 1d
    _num = pd.to_numeric(rd["date"], errors="coerce")
    rd["date"] = pd.to_datetime(_num, unit="s") if _num.notna().any() else pd.to_datetime(rd["date"], errors="coerce")
    rd = rd.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    en = enrich_ticker_df(rd)
    en = en[[c for c in en.columns if c in _DB_COLS]].copy()
    en["ticker"] = tk
    en["universe"] = u
    return en


def _build_one(item):
    tk, u, days = item
    try:
        from data_polygon import fetch_bars
        # Always fetch 30m and re-aggregate to the target tf SESSION-ANCHORED (TV-aligned).
        # Massive's native 1h/4h aggregates are UTC-grid-anchored (09:00/08:00 ET starts,
        # bleeding in premarket) and do NOT match TV; 30m bars land on :00/:30 ET so the
        # 09:30 open is a clean boundary we can rebuild sessions from.
        df30 = fetch_bars(tk, interval="30m", days=days)
        if df30 is None or len(df30) == 0:
            return (tk, u, None, "empty")
        if _TF in ("1h", "4h"):
            df = resample_session(df30, _TF)
        elif _REGULAR:
            df = _regular_only(df30)
        else:
            df = df30
        if len(df) == 0:
            return (tk, u, None, "empty-after-regular")
        en = _process(tk, u, df, _TF)
        if en is None:
            return (tk, u, None, "no-rows")
        return (tk, u, en, None)
    except Exception as e:
        return (tk, u, None, str(e)[:140])


def _build_both(item):
    """ONE 30m fetch → session-anchored 1h AND 4h (halves API cost of a full rebuild).
    Returns (tk, u, {"1h": en1h, "4h": en4h}, err)."""
    tk, u, days = item
    try:
        from data_polygon import fetch_bars
        df30 = fetch_bars(tk, interval="30m", days=days)
        if df30 is None or len(df30) == 0:
            return (tk, u, None, "empty")
        out = {}
        for tf in ("1h", "4h"):
            en = _process(tk, u, resample_session(df30, tf), tf)
            if en is not None and len(en):
                out[tf] = en
        if not out:
            return (tk, u, None, "no-rows")
        return (tk, u, out, None)
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


def run(targets, db_path, days, workers, force):
    from concurrent.futures import ProcessPoolExecutor, as_completed
    db_cols = ensure_schema(db_path)
    con = duckdb.connect(db_path)
    next_id = (con.execute("SELECT coalesce(max(id),0) FROM bars").fetchone()[0]) + 1
    done = set() if force else set(r[0] for r in con.execute("SELECT DISTINCT ticker FROM bars").fetchall())
    todo = [(tk, u, days) for tk, u in targets if force or tk not in done]
    print(f"tf={_TF} regular={_REGULAR} db={db_path}")
    print(f"targets={len(targets)} done={len(targets)-len(todo)} todo={len(todo)} workers={workers} days={days}")
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
                print(f"  [{built}/{len(todo)}] rows={ins:,} errs={errs} | {rate:.1f} tk/s | ETA {eta:.0f}min", flush=True)
    con.close()
    print(f"\nDONE: {ins:,} rows · {built-errs} tickers · {errs} errors · {(time.time()-t0)/60:.1f}min\nDB: {db_path}")


def run_dual(targets, days, workers, force):
    """Fetch 30m ONCE per ticker, write session-anchored 1h AND 4h to their two DBs."""
    from concurrent.futures import ProcessPoolExecutor, as_completed
    dbs = {"1h": os.environ.get("DUAL_DB_1H", _dbp("1h")),
           "4h": os.environ.get("DUAL_DB_4H", _dbp("4h"))}
    db_cols = None
    cons, nid = {}, {}
    for tf, path in dbs.items():
        db_cols = ensure_schema(path)          # identical schema both DBs
        cons[tf] = duckdb.connect(path)
        nid[tf] = (cons[tf].execute("SELECT coalesce(max(id),0) FROM bars").fetchone()[0]) + 1
    # resume = ticker present in BOTH DBs
    if force:
        for tf in dbs:
            cons[tf].execute("DELETE FROM bars")
        done = set()
    else:
        d1 = set(r[0] for r in cons["1h"].execute("SELECT DISTINCT ticker FROM bars").fetchall())
        d4 = set(r[0] for r in cons["4h"].execute("SELECT DISTINCT ticker FROM bars").fetchall())
        done = d1 & d4
    todo = [(tk, u, days) for tk, u in targets if tk not in done]
    print(f"DUAL 1h+4h (session-anchored, TV) db1h={dbs['1h']} db4h={dbs['4h']}")
    print(f"targets={len(targets)} done={len(targets)-len(todo)} todo={len(todo)} workers={workers} days={days}")
    if not todo:
        for c in cons.values(): c.close()
        print("nothing to build."); return

    t0 = time.time(); built = ins = errs = 0
    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker, initargs=(db_cols,)) as ex:
        futs = [ex.submit(_build_both, it) for it in todo]
        for fut in as_completed(futs):
            tk, u, out, err = fut.result()
            built += 1
            if err or not out:
                errs += 1
                if err and "empty" not in err:
                    print(f"  ✗ {tk}: {err}")
            else:
                for tf, en in out.items():
                    con = cons[tf]
                    en = en.copy()
                    en["id"] = range(nid[tf], nid[tf] + len(en)); nid[tf] += len(en)
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
                print(f"  [{built}/{len(todo)}] rows={ins:,} errs={errs} | {rate:.1f} tk/s | ETA {eta:.0f}min", flush=True)
    for c in cons.values(): c.close()
    print(f"\nDONE: {ins:,} rows (1h+4h) · {built-errs} tickers · {errs} errors · {(time.time()-t0)/60:.1f}min")


if __name__ == "__main__":
    _load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default="")
    ap.add_argument("--universe", default="", choices=["", *_UNIVERSES])
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--tf", default="1h", choices=["1h", "4h", "30m"])
    ap.add_argument("--dual", action="store_true",
                    help="fetch 30m ONCE, build session-anchored 1h AND 4h together (TV-aligned full rebuild)")
    ap.add_argument("--extended", action="store_true", help="include pre/post-market (default: regular only)")
    ap.add_argument("--days", type=int, default=1900)
    ap.add_argument("--min-dv", type=float, default=0.0,
                    help="liquidity floor: median daily $-volume in $M (0=all). e.g. 5 = $5M/day")
    ap.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 1))
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()

    # propagate config to (spawned) workers via env BEFORE the pool starts
    os.environ["INTRADAY_TF"] = a.tf
    os.environ["INTRADAY_REGULAR"] = "0" if a.extended else "1"
    db_path = _dbp(a.tf)
    os.environ["INTRADAY_DB"] = db_path
    os.environ["STUDIO_DB_PATH"] = db_path
    globals()["_TF"] = a.tf
    globals()["_REGULAR"] = not a.extended
    globals()["_DB"] = db_path

    targets: list[tuple[str, str]] = []
    if a.tickers:
        src = duckdb.connect(ONE_DB, read_only=True)
        for tk in [t.strip().upper() for t in a.tickers.split(",") if t.strip()]:
            row = src.execute("SELECT universe FROM bars WHERE ticker=? LIMIT 1", [tk]).fetchone()
            targets.append((tk, row[0] if row else "sp500"))
        src.close()
    else:
        unis = _UNIVERSES if a.all else ([a.universe] if a.universe else ["sp500"])
        seen = set()
        for u in unis:
            for tk in universe_tickers(u):
                if tk not in seen:
                    seen.add(tk); targets.append((tk, u))

    if a.min_dv > 0:
        keep = liquid_tickers(a.min_dv)
        before = len(targets)
        targets = [(tk, u) for tk, u in targets if tk in keep]
        print(f"liquidity filter dv≥${a.min_dv}M: {before} → {len(targets)} tickers")

    if a.dual:
        run_dual(targets, days=a.days, workers=a.workers, force=a.force)
    else:
        run(targets, db_path, days=a.days, workers=a.workers, force=a.force)
