"""
derive_intraday.py — Phase B of the unified intraday architecture.

Read the LEAN 15m base (studio_15m_base.duckdb) and LOCALLY re-aggregate it into a
session-anchored timeframe (30m / 1h / 2h / 4h), compute the full signal set, and
write ~/Downloads/studio_<tf>.duckdb (same 394-col schema as the 1D studio DB).

No network. Fast, deterministic, re-runnable — tweak signals and rebuild any tf in
minutes without re-fetching. Bars are session-anchored (start at 09:30 ET) so they
match TradingView.

Usage:
    uv run python derive_intraday.py --tf 4h --tickers AAPL,NOVT --out ~/Downloads/studio_4h_TEST.duckdb
    uv run python derive_intraday.py --tf 1h --all --workers 8
    uv run python derive_intraday.py --tf 4h --all --workers 8
Reads:  ~/Downloads/studio_15m_base.duckdb
Writes: --out (default ~/Downloads/studio_<tf>.duckdb)
"""
from __future__ import annotations
import argparse, os, sys, time

sys.path.insert(0, os.path.dirname(__file__))

# import Phase-A/-B shared helpers WITHOUT triggering build_intraday_db's __main__
os.environ.setdefault("INTRADAY_TF", "1h")
import duckdb, pandas as pd  # noqa: E402
import build_intraday_db as B  # noqa: E402  (resample_session, _process, _coerce, ensure_schema)
from studio.paths import BASE_15M_DB as BASE_DB, ANALYTICS_DB as ONE_DB, db_path  # noqa: E402

_SLOT = {"30m": None, "1h": 60, "2h": 120, "4h": 240}   # slot width (min); 30m = pass-through pairs


def _resample(df30_or_15, tf):
    """Session-anchored resample of fine bars → tf. Reuses build_intraday_db.resample_session
    for 1h/4h; generalizes to 2h/30m/1d via slot width."""
    if tf == "15m":
        return df30_or_15[["open", "high", "low", "close", "volume"]]   # base IS 15m — identity
    if tf in ("1h", "4h"):
        return B.resample_session(df30_or_15, tf)
    # generic path (2h, 30m, 1d) — mirror resample_session with a parametric width
    d = df30_or_15.copy()
    if len(d) == 0:
        return d[["open", "high", "low", "close", "volume"]]
    et = d.index.tz_convert("America/New_York")
    since = (et.hour * 60 + et.minute) - 570
    width = {"30m": 30, "2h": 120, "1d": 100000}[tf]   # 1d: one slot = whole session
    d["_day"] = et.normalize()
    d["_slot"] = (since // width).astype(int)
    g = d.groupby(["_day", "_slot"], sort=True)
    out = g.agg(open=("open", "first"), high=("high", "max"), low=("low", "min"),
                close=("close", "last"), volume=("volume", "sum"))
    first_ts = g.apply(lambda x: x.index.min())
    out.index = pd.DatetimeIndex(first_ts.values).tz_localize("UTC")
    return out.sort_index()[["open", "high", "low", "close", "volume"]]


_DB_COLS = None


def _init_worker(db_cols):
    global _DB_COLS
    _DB_COLS = db_cols
    B._DB_COLS = db_cols          # _process reads build_intraday_db._DB_COLS
    import main  # noqa: warm import


# Warmup window for incremental enrich: read this many days of base history
# BEFORE the last enriched bar so every bounded-lookback signal (EMA200 on 15m
# needs ~8 sessions; all others less) is fully converged on the first NEW bar.
# 45 calendar days ≈ 30 sessions ≈ 780 15m bars — generous safety margin.
_INCR_WARMUP_DAYS = 45


def _derive_one(item):
    # item = (ticker, universe, tf) for a full rebuild, or
    #        (ticker, universe, tf, since_iso) for incremental — read only a
    #        warmup window of base history and return ONLY bars after `since`.
    tk, u, tf = item[0], item[1], item[2]
    since = item[3] if len(item) > 3 else None
    try:
        con = duckdb.connect(BASE_DB, read_only=True)
        if since:
            raw = con.execute(
                "SELECT date, open, high, low, close, volume FROM bars "
                "WHERE ticker=? AND universe=? "
                f"AND date >= TIMESTAMP '{since}' - INTERVAL {_INCR_WARMUP_DAYS} DAY "
                "ORDER BY date", [tk, u]).fetchdf()
        else:
            raw = con.execute(
                "SELECT date, open, high, low, close, volume FROM bars "
                "WHERE ticker=? AND universe=? ORDER BY date", [tk, u]).fetchdf()
        con.close()
        if raw is None or len(raw) == 0:
            return (tk, u, None, "no-base")
        raw["date"] = pd.to_datetime(raw["date"]).dt.tz_localize("UTC")   # naive UTC → aware
        fine = raw.set_index("date")[["open", "high", "low", "close", "volume"]]
        bars = _resample(fine, tf)
        if len(bars) == 0:
            return (tk, u, None, "empty-after-resample")
        en = B._process(tk, u, bars, tf)
        if en is None:
            return (tk, u, None, "no-rows")
        if since:
            # keep only genuinely-new bars (warmup rows were only for lookback)
            cutoff = pd.to_datetime(since)
            edates = pd.to_datetime(en["date"]).dt.tz_localize(None) if getattr(
                en["date"].dt, "tz", None) is not None else pd.to_datetime(en["date"])
            en = en[edates > cutoff]
            if len(en) == 0:
                return (tk, u, None, "no-new-bars")
        return (tk, u, en, None)
    except Exception as e:
        return (tk, u, None, str(e)[:160])


def _base_targets():
    con = duckdb.connect(BASE_DB, read_only=True)
    rows = con.execute("SELECT DISTINCT ticker, universe FROM bars ORDER BY ticker").fetchall()
    con.close()
    return [(r[0], r[1]) for r in rows]


def run(tf, out_db, targets, workers, force, incremental=False):
    from concurrent.futures import ProcessPoolExecutor, as_completed
    db_cols = B.ensure_schema(out_db)           # clones 394-col schema, date=TIMESTAMP
    con = duckdb.connect(out_db)
    if force:
        con.execute("DELETE FROM bars")
    next_id = (con.execute("SELECT coalesce(max(id),0) FROM bars").fetchone()[0]) + 1
    if incremental and not force:
        # Nightly top-up: append ONLY bars newer than each ticker's last enriched
        # bar (warmup window handles lookback). Tickers absent from the enriched
        # DB fall through to a full derive.
        maxmap = {r[0]: str(r[1]) for r in con.execute(
            "SELECT ticker, max(date) FROM bars GROUP BY ticker").fetchall()}
        todo = [(tk, u, tf, maxmap[tk]) if tk in maxmap else (tk, u, tf)
                for tk, u in targets]
        print(f"derive {tf} INCREMENTAL ← {BASE_DB}  →  {out_db}")
        print(f"targets={len(targets)} (top-up {sum(1 for t in todo if len(t)>3)} · "
              f"new {sum(1 for t in todo if len(t)==3)}) workers={workers}")
    else:
        done = set() if force else set(r[0] for r in con.execute("SELECT DISTINCT ticker FROM bars").fetchall())
        todo = [(tk, u, tf) for tk, u in targets if tk not in done]
        print(f"derive {tf} ← {BASE_DB}  →  {out_db}")
        print(f"targets={len(targets)} done={len(targets)-len(todo)} todo={len(todo)} workers={workers}")
    if not todo:
        con.close(); print("nothing to derive."); return

    t0 = time.time(); built = ins = errs = 0
    # BOUNDED in-flight (2026-07-03 OOM fix): submitting all N tasks upfront let fast
    # workers pile up enriched frames (~40MB each for 15m) faster than the serial parent
    # could INSERT them → memory blew up ~800 tickers in. Keep only ~2×workers in flight;
    # submit a new task each time one completes → memory stays bounded regardless of N.
    from concurrent.futures import FIRST_COMPLETED, wait as _wait
    cap = max(2, workers * 2)
    it = iter(todo)
    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker, initargs=(db_cols,)) as ex:
        inflight = set()
        for _ in range(cap):
            try:
                inflight.add(ex.submit(_derive_one, next(it)))
            except StopIteration:
                break
        while inflight:
            done, inflight = _wait(inflight, return_when=FIRST_COMPLETED)
            for fut in done:
                tk, u, en, err = fut.result()
                built += 1
                if err or en is None or len(en) == 0:
                    errs += 1
                    if err and "empty" not in err and "no-base" not in err:
                        print(f"  ✗ {tk}: {err}", flush=True)
                else:
                    en = en.copy()
                    en["id"] = range(next_id, next_id + len(en)); next_id += len(en)
                    en = B._coerce(en, con)
                    cols = [c for c in en.columns if c in db_cols]
                    q = ",".join('"' + c + '"' for c in cols)
                    con.register("tmp_ins", en[cols])
                    con.execute(f"INSERT INTO bars ({q}) SELECT {q} FROM tmp_ins")
                    con.unregister("tmp_ins")
                    ins += len(en)
                    del en
                # backfill the pool as each result is consumed
                try:
                    inflight.add(ex.submit(_derive_one, next(it)))
                except StopIteration:
                    pass
                if built % 50 == 0 or built == len(todo):
                    el = time.time() - t0; rate = built / el if el else 0
                    eta = (len(todo) - built) / rate / 60 if rate else 0
                    print(f"  [{built}/{len(todo)}] rows={ins:,} errs={errs} | {rate:.1f} tk/s | ETA {eta:.0f}min", flush=True)
    con.close()
    print(f"\nDONE {tf}: {ins:,} rows · {built-errs} tickers · {errs} errors · {(time.time()-t0)/60:.1f}min\nDB: {out_db}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf", required=True, choices=["15m", "30m", "1h", "2h", "4h", "1d"])
    ap.add_argument("--tickers", default="")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--out", default="")
    ap.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 2))
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--incremental", action="store_true",
                    help="nightly top-up: append only bars newer than each ticker's "
                         "last enriched bar (warmup window handles signal lookback)")
    a = ap.parse_args()

    out_db = os.path.expanduser(a.out) if a.out else db_path(a.tf)
    os.environ["STUDIO_DB_PATH"] = out_db

    if a.tickers:
        want = [t.strip().upper() for t in a.tickers.split(",") if t.strip()]
        allt = {tk: u for tk, u in _base_targets()}
        targets = [(tk, allt.get(tk, "sp500")) for tk in want]
    else:
        targets = _base_targets()

    run(a.tf, out_db, targets, workers=a.workers, force=a.force,
        incremental=a.incremental)
