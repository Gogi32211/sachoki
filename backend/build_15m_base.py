"""
build_15m_base.py — Phase A of the unified intraday architecture.

Fetch 15-minute REGULAR-SESSION OHLCV from Massive and store it LEAN (no signals)
in ~/Downloads/studio_15m_base.duckdb. This single fine-grained base is the ONE
network-heavy step; every intraday timeframe (30m/1h/2h/4h) is later re-aggregated
from it LOCALLY (derive_intraday.py) — session-anchored so it matches TradingView.

Why lean: our DBs are huge because of the 394-col signal enrichment, NOT raw price.
The base stores only OHLCV (8 cols) → a few GB for the whole universe, and signals
are computed per-timeframe downstream.

Why 15m: 15m bars land on :00/:15/:30/:45 ET, so the 09:30 session open is a clean
boundary — 15m aggregates exactly to session-anchored 30m/1h/2h/4h (and 15m itself).

1D / 1W are NOT derived here — they stay native Massive daily/weekly bars so the
entire validated research corpus (seq_rules, edges, price-zones) keeps its calibration.

Usage:
    uv run python build_15m_base.py --tickers AAPL,NOVT          # test
    uv run python build_15m_base.py --all --min-dv 2 --workers 4 # full crawl (gentle)
    uv run python build_15m_base.py --all --workers 4            # resume (skips present)
Output: ~/Downloads/studio_15m_base.duckdb  (table `bars`: ticker,universe,date,OHLCV)
"""
from __future__ import annotations
import argparse, os, sys, time

_UNIVERSES = ["sp500", "nasdaq", "russell2k"]

sys.path.insert(0, os.path.dirname(__file__))
import duckdb, pandas as pd  # noqa: E402
from studio.paths import ANALYTICS_DB as ONE_DB, BASE_15M_DB as BASE_DB  # noqa: E402


def _load_env():
    p = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(p):
        for line in open(p):
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.strip().split("=", 1)
                os.environ.setdefault(k, v.strip().strip('"').strip("'"))


def ensure_schema(db_path: str):
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bars (
          ticker   VARCHAR,
          universe VARCHAR,
          date     TIMESTAMP,        -- naive UTC bar-start (Phase B localizes→ET)
          open     DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE,
          PRIMARY KEY (ticker, date, universe)
        )""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_15m_tk ON bars(ticker, universe, date)")
    con.close()


def universe_tickers(universe: str) -> list[str]:
    src = duckdb.connect(ONE_DB, read_only=True)
    tks = [r[0] for r in src.execute(
        "SELECT DISTINCT ticker FROM bars WHERE universe=? ORDER BY ticker", [universe]).fetchall()]
    src.close()
    return tks


def liquid_tickers(min_dv_m: float) -> set:
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


# ── worker (separate process) ───────────────────────────────────────────────
def _fetch_one(item):
    """Fetch 15m, keep regular session (09:30-16:00 ET, 26 bars/day), lean OHLCV."""
    tk, u, days = item
    try:
        from data_polygon import fetch_bars
        df = fetch_bars(tk, interval="15m", days=days)
        if df is None or len(df) == 0:
            return (tk, u, None, "empty")
        et = df.index.tz_convert("America/New_York")
        mins = et.hour * 60 + et.minute
        df = df[(mins >= 570) & (mins < 960)]          # 09:30 .. 16:00
        if len(df) == 0:
            return (tk, u, None, "empty-after-regular")
        out = df[["open", "high", "low", "close", "volume"]].copy()
        out["date"] = df.index.tz_convert("UTC").tz_localize(None)   # naive UTC
        out["ticker"] = tk
        out["universe"] = u
        return (tk, u, out, None)
    except Exception as e:
        return (tk, u, None, str(e)[:160])


def run(targets, days, workers, force, delta=False):
    from concurrent.futures import ProcessPoolExecutor, as_completed
    ensure_schema(BASE_DB)
    con = duckdb.connect(BASE_DB)
    if force:
        con.execute("DELETE FROM bars")
    # delta mode: re-fetch a short recent window for EVERY ticker (no wipe, no skip);
    # the (ticker,date,universe) PK + INSERT OR IGNORE dedups — only new bars land.
    done = set() if (force or delta) else set(r[0] for r in con.execute("SELECT DISTINCT ticker FROM bars").fetchall())
    todo = [(tk, u, days) for tk, u in targets if tk not in done]
    print(f"15m base → {BASE_DB}")
    print(f"targets={len(targets)} done={len(targets)-len(todo)} todo={len(todo)} workers={workers} days={days}")
    if not todo:
        con.close(); print("nothing to build."); return

    cols = ["ticker", "universe", "date", "open", "high", "low", "close", "volume"]
    t0 = time.time(); built = ins = errs = 0
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_fetch_one, it) for it in todo]
        for fut in as_completed(futs):
            tk, u, out, err = fut.result()
            built += 1
            if err or out is None or len(out) == 0:
                errs += 1
                if err and "empty" not in err:
                    print(f"  ✗ {tk}: {err}", flush=True)
            else:
                con.register("tmp_ins", out[cols])
                con.execute("INSERT OR IGNORE INTO bars SELECT * FROM tmp_ins")
                con.unregister("tmp_ins")
                ins += len(out)
            if built % 25 == 0 or built == len(todo):
                el = time.time() - t0; rate = built / el if el else 0
                eta = (len(todo) - built) / rate / 60 if rate else 0
                print(f"  [{built}/{len(todo)}] rows={ins:,} errs={errs} | {rate:.2f} tk/s | ETA {eta:.0f}min", flush=True)
    con.close()
    print(f"\nDONE: {ins:,} rows · {built-errs} tickers · {errs} errors · {(time.time()-t0)/60:.1f}min\nDB: {BASE_DB}")


if __name__ == "__main__":
    _load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default="")
    ap.add_argument("--universe", default="", choices=["", *_UNIVERSES])
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--days", type=int, default=1900)
    ap.add_argument("--min-dv", type=float, default=0.0, help="median daily $-volume floor in $M")
    ap.add_argument("--workers", type=int, default=4, help="keep low (network-bound, be gentle)")
    ap.add_argument("--force", action="store_true", help="WIPE the table and rebuild from scratch")
    ap.add_argument("--delta", action="store_true",
                    help="incremental: re-fetch --days recent days for ALL tickers, dedup on PK (nightly top-up)")
    a = ap.parse_args()

    targets: list[tuple[str, str]] = []
    if a.delta and not a.tickers:
        # delta top-up: targets = the tickers ALREADY IN the base (never add new
        # tickers with a truncated 4-day history — full builds handle newcomers)
        src = duckdb.connect(BASE_DB, read_only=True)
        targets = [(r[0], r[1]) for r in src.execute(
            "SELECT DISTINCT ticker, universe FROM bars ORDER BY ticker").fetchall()]
        src.close()
    elif a.tickers:
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

    run(targets, days=a.days, workers=a.workers, force=a.force, delta=a.delta)
