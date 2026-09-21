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


def _fetch_stats() -> dict:
    """The vendor counters for the call this worker just made (data_polygon.LAST_FETCH)."""
    try:
        import data_polygon
        return dict(data_polygon.LAST_FETCH)
    except Exception:
        return {}


def _build_both_intraday(args):
    """ONE 30m fetch -> session-anchored 1h AND 4h. Delegates to build_intraday_db._build_both, the
    same function the 2026 backfill used, so nothing on the fetch side is new code.

    Returns (tk, universe, out, err, fetch_stats). LAST_FETCH is per-PROCESS state, so the stats are
    read HERE, in the worker, and carried back explicitly — the parent cannot see a child's module
    globals. Read on the error path too: a fetch that raised is precisely the one whose counters
    decide whether the run is acceptable.
    """
    tk, universe, days = args
    try:
        import build_intraday_db as bidb
        return (*bidb._build_both((tk, universe, days)), _fetch_stats())
    except Exception as e:
        return (tk, universe, None, str(e)[:140], _fetch_stats())


# ── ACCEPTANCE GATE for a --dual run (registered 2026-09-21, before the first live run) ───────
# PRIMARY parity gate is the one WARMUP_PARITY_V1 froze: p95 |Δ| <= 0.1 AND max |Δ| <= 0.5 between
# the stored rsi_14 and a full-history Wilder-14. Stored rsi_14 is a DOUBLE rounded to 1 dp, so
# 0.1 IS the storage quantum — "exact" cannot be asked for below it.
PARITY_GATE_P95 = 0.1
PARITY_GATE_MAX = 0.5
# …compared with a tolerance, because the quantum IS the gate. |round(wilder,1) - stored| on a 4H
# bar one storage unit apart comes out as 0.10000000000000142, and a bare `> 0.1` turned the very
# first rehearsal into NOT PASS over binary floating-point residue. The tolerance is far below the
# quantum, so it cannot hide a real disagreement — the next representable difference is 0.1 away.
PARITY_EPS = 1e-6


def dual_verdict(acc: dict, fetch: dict, parity: dict) -> tuple[bool, list[str]]:
    """PASS / NOT PASS for one --dual run. Pure, so backend/tests can pin every branch.

    A run is NOT PASS if, on EITHER timeframe:
      · key_difference < 0            — the overlap window deleted more than it put back
      · any ticker lost rows          — a net sum hides a per-ticker loss inside other tickers' gains
      · any vendor response was PARTIAL — a short frame is indistinguishable from a complete one
      · the RSI parity sample was not measured, or missed the pre-registered gate
    The last clause is deliberate: an unmeasured check is not a passed check. A missing verification
    that reads as success is how the acceptance baseline went uncommitted for a week.
    """
    bad: list[str] = []
    for tf in sorted(acc):
        a = acc[tf]
        diff = a["reinserted"] - a["deleted"]
        if diff < 0:
            bad.append(f"{tf}: key_difference {diff:+,} — sessions destroyed and not restored")
        if a["lossy"]:
            bad.append(f"{tf}: {len(a['lossy'])} ticker(s) lost rows")
    if fetch.get("partial"):
        bad.append(f"{fetch['partial']} partial vendor response(s) — not written, but the run is incomplete")
    for tf in sorted(parity):
        r = parity[tf]
        if r is None:
            bad.append(f"{tf}: RSI parity NOT TESTED")
        elif r["p95"] > PARITY_GATE_P95 + PARITY_EPS or r["mx"] > PARITY_GATE_MAX + PARITY_EPS:
            bad.append(f"{tf}: RSI parity p95 {r['p95']:.3f} / max {r['mx']:.3f} "
                       f"(gate {PARITY_GATE_P95} / {PARITY_GATE_MAX})")
    return (not bad), bad


def rsi_parity_after_write(con, tickers, n_tickers: int = 25, n_bars: int = 8):
    """Does the rsi_14 we JUST WROTE match a Wilder-14 computed over the ticker's full history?

    This is the check the old 15-day path would have failed every night for three months: the RMA is
    seeded from the first bar of the fetched frame, so too short a window leaves the seed undecayed
    and the stored value wrong — silently, with every row present and every count correct. Coverage
    checks cannot see it; only recomputation can. Sampled on the last `n_bars` bars, which are
    inside the window this run rewrote. Returns None when it could not be measured at all.
    """
    try:
        import numpy as _np, pandas as _pd
        from mtf_rev_build import wilder_rsi14
    except Exception as e:
        print(f"   ⚠ RSI parity: cannot import the reference implementation ({e})")
        return None
    tks = sorted(str(t) for t in tickers)
    if not tks:
        return None
    rng = _np.random.default_rng(20260921)
    pick = [str(t) for t in rng.choice(tks, size=min(n_tickers, len(tks)), replace=False)]
    d: list[float] = []
    for tk in pick:
        try:
            g = con.execute(
                "SELECT date, close, rsi_14 FROM bars WHERE ticker=? ORDER BY date", [tk]).fetchdf()
        except Exception:
            continue
        if len(g) < 200:
            continue
        full = wilder_rsi14(g["close"]).to_numpy()
        st = _pd.to_numeric(g["rsi_14"], errors="coerce").to_numpy()
        for i in range(max(0, len(g) - n_bars), len(g)):
            if _np.isfinite(full[i]) and _np.isfinite(st[i]):
                d.append(round(abs(round(float(full[i]), 1) - float(st[i])), 6))
    if not d:
        return None
    a = _np.asarray(d)
    return dict(n=int(a.size), tickers=len(pick), median=float(_np.median(a)),
                p95=float(_np.percentile(a, 95)), mx=float(a.max()),
                within_quantum=float((a <= 0.1 + 1e-9).mean()))


def run_dual(workers: int, tickers_filter: list[str] | None = None,
             dbs: dict | None = None, days: int = None) -> bool:
    """NIGHTLY INTRADAY UPDATE V2 — one 30m vendor fetch per ticker, both timeframes built from it.

    Satisfies both warm-up requirements (1H 30d, 4H 90d) with a single 90-day window, and HALVES
    the TICKER FETCHES: 3,203/night instead of 6,406, because the old path ran once per timeframe
    and each run did its own fetch. The overlap semantics are not re-implemented — both timeframes
    go through the same write_ticker as the single-TF path.

    ⚠️ HTTP requests do NOT halve. A 30m frame is one cursor page at 15 days and two at 90 (AAPL:
    294 rows vs 1,990), so both paths cost ~2 requests per ticker; bytes rise ~3.4x. Read "halved"
    on the FETCH line, never on the request count.

    Returns True only on a PASS run (see dual_verdict). The caller exits non-zero otherwise, so a
    bad night is visible in the nightly log instead of ending in "✅ DONE".
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
        return False
    print(f"DUAL 1h+4h · one 30m fetch per ticker · fetch_days={days} overlap={OVERLAP}")
    print(f"  db1h={dbs['1h']}\n  db4h={dbs['4h']}")
    print(f"  tickers {len(tickers):,} · workers {workers} · ticker fetches {len(tickers):,} "
          f"(single-TF mode would make {2*len(tickers):,})")
    t0 = time.time()
    acc = {tf: dict(deleted=0, reinserted=0, ins=0, lossy=[]) for tf in dbs}
    built = errs = skipped = 0
    rows_enriched = 0
    # vendor-side telemetry, aggregated from each worker's own data_polygon.LAST_FETCH
    fetch = dict(requested=len(tickers), succeeded=0, failed=0, partial=0, vendor_errors=0,
                 calls=0, rows=0, bytes=0, retries=0, http_429=0,
                 partial_tickers=[], failed_tickers=[])
    written = {tf: set() for tf in dbs}
    todo = [(tk, uni.get(tk, "sp500"), days) for tk in tickers]
    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker,
                             initargs=(db_cols["1h"], "1h", False)) as ex:
        futs = {ex.submit(_build_both_intraday, it): it for it in todo}
        for fut in as_completed(futs):
            tk, universe, out, err, st = fut.result()
            built += 1
            for k in ("calls", "rows", "bytes", "retries", "http_429"):
                fetch[k] += int(st.get(k) or 0)
            if st.get("error"):
                fetch["vendor_errors"] += 1
            if err or not out:
                errs += 1
                fetch["failed"] += 1
                fetch["failed_tickers"].append((tk, str(err)[:70]))
                if err and "empty" not in str(err).lower():
                    print(f"  ✗ {tk}: {err}")
            elif st.get("partial"):
                # FAIL-CLOSED. A partial frame is short, and a short frame inside the overlap window
                # means the DELETE removes days the INSERT cannot put back — the exact mechanism of
                # the 2026 outage. Rows already in the store are better than rows half-replaced, so
                # the ticker is left alone this run and the whole run is marked NOT PASS.
                fetch["partial"] += 1
                fetch["partial_tickers"].append(tk)
                skipped += 1
                print(f"  ⛔ {tk}: PARTIAL vendor response — NOT written ({st.get('error')})")
            else:
                fetch["succeeded"] += 1
                for tf, en in out.items():
                    if en is None or not len(en) or tf not in cons:
                        continue
                    rows_enriched += len(en)
                    r = write_ticker(cons[tf], tk, universe, en, max_dates[tf], db_cols[tf], acc[tf])
                    if r == "skipped":
                        skipped += 1
                    elif r == "error":
                        errs += 1
                    else:
                        written[tf].add(tk)
            if built % 100 == 0 or built == len(todo):
                el = time.time() - t0
                rate = built / el if el else 0
                print(f"  [{built}/{len(todo)}] 1h +{acc['1h']['ins']:,} · 4h +{acc['4h']['ins']:,}"
                      f" | {errs} err | ETA {(len(todo)-built)/rate/60 if rate else 0:.0f}min", flush=True)
    el = time.time() - t0
    print(f"\n✅ DUAL DONE: 1h +{acc['1h']['ins']:,} · 4h +{acc['4h']['ins']:,} rows · "
          f"{built-errs} tickers · {errs} errors · {el/60:.1f}min")

    # ── FIRST-RUN TELEMETRY ───────────────────────────────────────────────────────────────────
    # Registered before the first live run, and printed on every run since: a nightly job that
    # reports only "+N rows" cannot be audited. Vendor calls HALVING against the old path is the
    # sanity check that --dual really replaced both single-TF invocations rather than joining them.
    print("\n" + "─" * 92)
    print("NIGHTLY INTRADAY V2 · RUN TELEMETRY")
    print("─" * 92)
    print(f"   runtime                     {el/60:.1f} min ({el:.0f}s)")
    print(f"   tickers requested           {fetch['requested']:,}")
    print(f"   tickers succeeded           {fetch['succeeded']:,}")
    print(f"   tickers failed              {fetch['failed']:,}")
    print(f"   partial responses           {fetch['partial']:,}"
          + ("   ⛔ NOT WRITTEN" if fetch["partial"] else ""))
    print(f"   vendor errors               {fetch['vendor_errors']:,} "
          f"(retries {fetch['retries']:,} · HTTP 429 {fetch['http_429']:,})")
    _tk_fetches = fetch["succeeded"] + fetch["failed"]
    print(f"   total vendor calls          {fetch['calls']:,} HTTP requests")
    print(f"   ticker fetches              {_tk_fetches:,} "
          f"(single-TF path would make {2*_tk_fetches:,} — THIS is the number that halves)")
    print(f"   ⚠ HTTP requests do NOT halve: a 30m frame is 1 cursor page at 15d and 2 at 90d,")
    print(f"     so both paths cost ~2 requests per ticker. ~{2*_tk_fetches:,} requests is EXPECTED,")
    print(f"     not a regression; bytes rise ~3.4x as the warm-up's price.")
    print(f"   total rows fetched          {fetch['rows']:,} raw 30m bars")
    print(f"   total bytes fetched         {fetch['bytes']:,} bytes ({fetch['bytes']/1e6:.1f} MB)")
    print(f"   rows enriched from fetch    {rows_enriched:,}")
    print(f"   1H rows inserted            {acc['1h']['ins']:,}")
    print(f"   4H rows inserted            {acc['4h']['ins']:,}")
    if fetch["partial_tickers"]:
        print(f"   partial tickers: {', '.join(fetch['partial_tickers'][:20])}")
    if fetch["failed_tickers"]:
        print("   failed tickers (first 10): "
              + "; ".join(f"{t} — {e}" for t, e in fetch["failed_tickers"][:10]))
    for tf in sorted(acc):
        report_invariant(acc[tf], f" [{tf}]")

    # ── RSI PARITY AFTER WRITE ────────────────────────────────────────────────────────────────
    parity = {}
    print("\n   RSI parity after write (stored rsi_14 vs full-history Wilder-14, |Δ|):")
    for tf in sorted(cons):
        r = rsi_parity_after_write(cons[tf], written[tf])
        parity[tf] = r
        if r is None:
            print(f"     {tf}   ⚠ NOT TESTED — no measurable sample")
        else:
            print(f"     {tf}   n={r['n']:,} over {r['tickers']} tickers · median {r['median']:.3f}"
                  f" · p95 {r['p95']:.3f} · max {r['mx']:.3f}"
                  f" · within one storage unit {r['within_quantum']*100:.1f}%")

    ok, bad = dual_verdict(acc, fetch, parity)
    print("\n" + "═" * 92)
    if ok:
        print("VERDICT: PASS — key_difference >= 0 on both TFs, no ticker lost rows, "
              "no partial responses, RSI parity within gate")
    else:
        print("VERDICT: NOT PASS")
        for b in bad:
            print(f"   ⛔ {b}")
    print("═" * 92)
    for c in cons.values():
        c.close()
    return ok


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
        # Exit 2 on NOT PASS. A nightly that writes a damaged store and still exits 0 is how the
        # overlap bug survived three months — update_all.sh's `|| echo ... failed` never fired.
        ok = run_dual(a.workers, tickers_filter)
        sys.exit(0 if ok else 2)
    else:
        run(_DB, a.tf, _WEEKLY, a.workers, tickers_filter)
