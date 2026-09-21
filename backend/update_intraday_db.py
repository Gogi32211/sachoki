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

FETCH_DAYS = 15   # legacy single-TF path only — see FETCH_DAYS_DUAL
# ⚠️ 15 days is NOT enough warm-up for a Wilder-14, whatever the old comment here claimed. The RMA
# is seeded from the first bar of whatever frame it is handed, so the seed must be given room to
# decay. INTRADAY_RSI_WARMUP_PARITY_V1 measured the shortest window reproducing a full-history
# Wilder against the canonical series: 1H needs 30 calendar days, 4H needs 90. At 15 days 4H was
# off by a median of 9.6 RSI points (p95 18.5, max 27.8) and 1H by 0.8.
FETCH_DAYS_DUAL = 90
OVERLAP    = 3    # re-fetch/re-insert last N days to capture corrections

# ── THE OVERLAP INVARIANT ─────────────────────────────────────────────────────────────────────
# The range DELETE removes must be EXACTLY the range INSERT puts back. Until 2026-09-13 it was not:
# the INSERT filter compared a date NORMALISED to midnight (`_dt_key > cutoff`) while the DELETE
# compared the raw TIMESTAMP (`date > 'YYYY-MM-DD'`). A bar at 2026-08-24 13:30 is > 2026-08-24
# 00:00, so it was deleted; its normalised key is NOT > 2026-08-24, so it was never put back.
# The cutoff day was destroyed on every run and silently never restored — the script still printed
# "✅ DONE: +87,000 new rows" and exited 0, so update_all.sh's `|| echo ... failed` never fired.
#
# With OVERLAP = 3 and the Tue-Sat schedule, the cutoff lands on Tue (Tue run), Fri (Wed run) and
# Mon (Sat run); on Thu and Fri runs it lands on Sat/Sun, which are not trading days. That is
# exactly the damage found in the stores: Mon 10/10, Tue 9/10, Fri 8/9 destroyed, Wed and Thu 0/10.
#
# The contract here is a CALENDAR-DAY overlap, not a timestamp overlap, so both sides are now
# date-level INCLUSIVE (`>=`). Pinned by backend/tests/test_intraday_update_overlap.py.
DELETE_PREDICATE = "date >= ?"


def overlap_cutoff(old_max, overlap: int = OVERLAP, fetch_min=None):
    """First calendar day of the re-write window: everything from here on is deleted and re-inserted.

    ⚠️ CLAMPED TO THE FETCH. The delete range is derived from `old_max`, but the restore range is
    bounded by FETCH_DAYS. For a ticker that has stopped updating — delisted, or the vendor stopped
    returning it — `old_max - OVERLAP` can fall BEFORE the fetched window even starts, and then the
    DELETE removes days the fetch cannot give back. Found 2026-09-16 by the gate's own invariant:
    APGE (max_date 2026-09-02) and HLX (2026-09-01) had cutoffs of 08-30 and 08-29 against a fetch
    beginning ~09-01, and lost their 2026-08-31 bars. It is PROGRESSIVE — one more day every night.

    Never delete outside what can be put back: the window starts at the later of the two.
    """
    import pandas as _pd
    cut = (_pd.to_datetime(old_max) - _pd.Timedelta(days=overlap)).normalize()
    if fetch_min is not None:
        cut = max(cut, _pd.to_datetime(fetch_min).normalize())
    return cut

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


def _build_both_intraday(args):
    """ONE 30m fetch -> session-anchored 1h AND 4h. Delegates to build_intraday_db._build_both, the
    same function the 2026 backfill used, so nothing on the fetch side is new code."""
    tk, universe, days = args
    try:
        import build_intraday_db as bidb
        return bidb._build_both((tk, universe, days))
    except Exception as e:
        return (tk, universe, None, str(e)[:140])


def run_dual(workers: int, tickers_filter: list[str] | None = None,
             dbs: dict | None = None, days: int = None):
    """NIGHTLY INTRADAY UPDATE V2 — one 30m vendor fetch per ticker, both timeframes built from it.

    Satisfies both warm-up requirements (1H 30d, 4H 90d) with a single 90-day window, and HALVES
    the vendor call count: 3,203/night instead of 6,406, because the old path ran once per
    timeframe and each run did its own fetch. The overlap semantics are not re-implemented — both
    timeframes go through the same write_ticker as the single-TF path.
    """
    import duckdb
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from studio.paths import db_path
    days = days or FETCH_DAYS_DUAL
    dbs = dbs or {tf: db_path(tf) for tf in ("1h", "4h")}
    cons = {tf: duckdb.connect(dbs[tf]) for tf in dbs}
    db_cols = {tf: set(cons[tf].execute("DESCRIBE bars").fetchdf()["column_name"].tolist()) for tf in dbs}
    max_dates = {tf: dict(cons[tf].execute(
        "SELECT ticker, max(date)::VARCHAR FROM bars GROUP BY ticker").fetchall()) for tf in dbs}
    uni = dict(cons["1h"].execute("SELECT ticker, any_value(universe) FROM bars GROUP BY ticker").fetchall())
    tickers = sorted(set(max_dates["1h"]) & set(max_dates["4h"]))
    if tickers_filter:
        tickers = [t for t in tickers if t in set(tickers_filter)]
    if not tickers:
        for c in cons.values():
            c.close()
        print("No tickers present in BOTH DBs — run build_intraday_db first.")
        return
    print(f"DUAL 1h+4h · one 30m fetch per ticker · fetch_days={days} overlap={OVERLAP}")
    print(f"  db1h={dbs['1h']}\n  db4h={dbs['4h']}")
    print(f"  tickers {len(tickers):,} · workers {workers} · vendor calls {len(tickers):,} "
          f"(single-TF mode would make {2*len(tickers):,})")
    t0 = time.time()
    acc = {tf: dict(deleted=0, reinserted=0, ins=0, lossy=[]) for tf in dbs}
    built = errs = skipped = 0
    rows_enriched = 0
    todo = [(tk, uni.get(tk, "sp500"), days) for tk in tickers]
    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker,
                             initargs=(db_cols["1h"], "1h", False)) as ex:
        futs = {ex.submit(_build_both_intraday, it): it for it in todo}
        for fut in as_completed(futs):
            tk, universe, out, err = fut.result()
            built += 1
            if err or not out:
                errs += 1
                if err and "empty" not in str(err).lower():
                    print(f"  ✗ {tk}: {err}")
            else:
                for tf, en in out.items():
                    if en is None or not len(en) or tf not in cons:
                        continue
                    rows_enriched += len(en)
                    r = write_ticker(cons[tf], tk, universe, en, max_dates[tf], db_cols[tf], acc[tf])
                    if r == "skipped":
                        skipped += 1
                    elif r == "error":
                        errs += 1
            if built % 100 == 0 or built == len(todo):
                el = time.time() - t0
                rate = built / el if el else 0
                print(f"  [{built}/{len(todo)}] 1h +{acc['1h']['ins']:,} · 4h +{acc['4h']['ins']:,}"
                      f" | {errs} err | ETA {(len(todo)-built)/rate/60 if rate else 0:.0f}min", flush=True)
    el = time.time() - t0
    print(f"\n✅ DUAL DONE: 1h +{acc['1h']['ins']:,} · 4h +{acc['4h']['ins']:,} rows · "
          f"{built-errs} tickers · {errs} errors · {el/60:.1f}min")
    print(f"   rows enriched from the fetch {rows_enriched:,} · vendor calls {len(todo):,}")
    for tf in ("1h", "4h"):
        report_invariant(acc[tf], f" [{tf}]")
    for c in cons.values():
        c.close()


def _build_one_weekly(args):
    tk, universe, days = args
    try:
        import build_weekly_db as bwdb
        return bwdb._build_one((tk, universe))
    except Exception as e:
        return tk, universe, None, str(e)


def write_ticker(con, tk, universe, en, max_dates, db_cols, acc) -> str:
    """ONE copy of the overlap semantics, shared by the single-TF path and --dual.

    Two copies of this block would drift, and drift in exactly this block caused the 2026 outage:
    the DELETE range and the INSERT range must stay identical (both date-level inclusive) and the
    cutoff must stay clamped to what the fetch can restore. Counters live in `acc` so the
    per-ticker invariant reads the same on both paths. Returns "ok" | "skipped" | "error".
    """
    old_max = max_dates.get(tk, "2000-01-01")
    # cutoff: re-insert from (old_max - OVERLAP days)
    import pandas as _pd
    en["_dt_key"] = _pd.to_datetime(en["date"]).dt.normalize()
    cutoff = overlap_cutoff(old_max, OVERLAP, fetch_min=en["_dt_key"].min())
    fresh = en[en["_dt_key"] >= cutoff].drop(columns=["_dt_key"])

    if fresh.empty:
        return "skipped"
    else:
        # DELETE bars in overlap window then INSERT fresh
        cutoff_str = cutoff.strftime("%Y-%m-%d")
        try:
            _before = con.execute(
                "SELECT count(*) FROM bars WHERE ticker=? AND universe=?",
                [tk, universe]).fetchone()[0]
            con.execute(
                f"DELETE FROM bars WHERE ticker=? AND universe=? AND {DELETE_PREDICATE}",
                [tk, universe, cutoff_str]
            )
            _del = _before - con.execute(
                "SELECT count(*) FROM bars WHERE ticker=? AND universe=?",
                [tk, universe]).fetchone()[0]
            acc["deleted"] += _del
            # A net difference across 3,200 tickers hides a per-ticker loss inside other
            # tickers' gains — exactly how the APGE/HLX loss stayed invisible on 1h
            # (+6,200) while surfacing on 4h (-2). Judge each ticker on its own.
            if _del > len(fresh):
                acc["lossy"].append((tk, _del, len(fresh)))
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
            acc["ins"] += len(fresh)
            acc["reinserted"] += len(fresh)
        except Exception as e:
            print(f"  ✗ {tk} (insert): {e}")
            return "error"
    return "ok"


def report_invariant(acc, label=""):
    """THE OVERLAP INVARIANT — see the module note above."""
    diff = acc["reinserted"] - acc["deleted"]
    print(f"   overlap invariant{label}: deleted {acc['deleted']:,} · "
          f"re-inserted {acc['reinserted']:,} · key_difference {diff:+,}"
          + ("" if diff >= 0 else "   ⛔ ROWS LOST — sessions were destroyed and not restored"))
    if acc["lossy"]:
        print(f"   ⛔ {len(acc['lossy'])} ticker(s) lost rows — per-ticker, which the net sum hides:")
        for _tk, _d, _r in sorted(acc["lossy"], key=lambda x: x[1] - x[2], reverse=True)[:20]:
            print(f"        {_tk}: deleted {_d:,} · restored {_r:,} · net {_r - _d:+,}")
    else:
        print("   per-ticker check: no ticker lost rows")


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
    acc = dict(deleted=0, reinserted=0, ins=0, lossy=[])   # shared with write_ticker

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
                r = write_ticker(con, tk, universe, en, max_dates, db_cols, acc)
                if r == "skipped":
                    skipped += 1
                elif r == "error":
                    errs += 1

            if built % 100 == 0 or built == len(todo):
                el = time.time() - t0
                rate = built / el if el else 0
                eta = (len(todo) - built) / rate / 60 if rate else 0
                print(f"  [{built}/{len(todo)}] +{acc['ins']:,} rows | {skipped} skipped | {errs} err | ETA {eta:.0f}min", flush=True)

    con.close()
    elapsed = time.time() - t0
    print(f"\n✅ DONE: +{acc['ins']:,} new rows · {built-errs-skipped} updated · {skipped} unchanged · {errs} errors · {elapsed/60:.1f}min")
    # THE OVERLAP INVARIANT. rows_deleted must equal rows_reinserted whenever the vendor returned a
    # complete payload: the DELETE range and the INSERT range are the same calendar window. A
    # negative difference means sessions were destroyed and not restored — the 2026 defect. A
    # positive one means the fetch brought back MORE than was there, which is normal on the first
    # run after a gap. Either way it must never be silently ignored again.
    report_invariant(acc)
    print(f"DB: {db_path}")


if __name__ == "__main__":
    # dbupdate writer path: the canonical data tree is on an external volume, so refuse
    # to start rather than let DuckDB create an empty database on the internal disk.
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose=_os.path.basename(__file__))
    _load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf",      default="1h", choices=["1h", "4h", "1w"])
    ap.add_argument("--dual", action="store_true",
                    help="NIGHTLY V2: one 30m fetch per ticker at FETCH_DAYS_DUAL, build 1h AND 4h")
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
    if a.dual:
        run_dual(a.workers, tickers_filter)
    else:
        run(_DB, a.tf, _WEEKLY, a.workers, tickers_filter)
