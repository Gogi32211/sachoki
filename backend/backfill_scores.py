"""backfill_scores.py — store ALL scores historically in the bars table (2026-07-18).

WHY: ultra_score existed as a column but was 0% filled (only computed live), and
ultra_score_v3 / buy_score were never stored — so no historical analysis could use them
and per-ticker CSV exports lacked them. User: every score must be historical + in CSVs.

HOW: computes the scores through studio.ultra_db_scan._row_to_dict — the SAME function
the DB-instant Ultra screener uses — so historical values are semantically identical to
what the live screener shows. Two phases so the app stays up during the heavy part:

  compute  read-only scan of bars (safe while the backend runs) → scores.parquet
           NOTE: ultra_score_v3 is stored as the CORE score (oversold+price+earners).
           The live 🏆RS/🎯cluster/🎋TLS axes are a TODAY-snapshot — injecting them into
           historical bars would be lookahead, so the axes map is stubbed to empty here.
  apply    exclusive UPDATE from the parquet (backend must be STOPPED, takes ~1-2 min):
             launchctl bootout gui/$UID/com.sachoki.backend
             python backfill_scores.py apply
             launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.sachoki.backend.plist

  incremental  compute+apply for the last N days only — called nightly in-process from
           the studio post-swap hook (the backend owns the write there), keeps new bars
           filled going forward.

Usage:  python backfill_scores.py compute [--days N]     (omit --days = full history)
        python backfill_scores.py apply
"""
from __future__ import annotations
import os, sys, time, types, logging

log = logging.getLogger("backfill_scores")
_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "data", "studio_analytics.duckdb")
_PARQ = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "score_backfill.parquet")


def _stub_axes():
    """Force ultra_score_v3 to its CORE (no live RS/cluster/TLS axes — lookahead in history)."""
    m = types.ModuleType("ultra_orchestrator")
    m._v3_axes_map = lambda: {}
    sys.modules["ultra_orchestrator"] = m


def compute(days: int | None = None, out_path: str = _PARQ) -> int:
    import duckdb, pandas as pd
    _stub_axes()
    from studio.ultra_db_scan import _row_to_dict
    con = duckdb.connect(_DB, read_only=True)
    cols = {r[0] for r in con.execute("DESCRIBE bars").fetchall()}
    where = "" if days is None else \
        f"WHERE date >= (SELECT max(date) FROM bars) - INTERVAL {int(days)} DAY"
    tks = [r[0] for r in con.execute(f"SELECT DISTINCT ticker FROM bars {where} ORDER BY ticker").fetchall()]
    total = con.execute(f"SELECT count(*) FROM bars {where}").fetchone()[0]
    log.info("compute: %s tickers, %s rows, window=%s", len(tks), total, days or "ALL")
    parts, done, t0 = [], 0, time.time()
    CH = 120
    for ci in range(0, len(tks), CH):
        chunk = tks[ci:ci + CH]
        ph = ",".join("?" * len(chunk))
        df = con.execute(
            f"SELECT * FROM bars {where} {'AND' if where else 'WHERE'} ticker IN ({ph}) "
            f"ORDER BY ticker, date", chunk).fetchdf()
        recs = []
        for _, row in df.iterrows():
            try:
                out = _row_to_dict(row)
                recs.append((row["ticker"], str(row["date"])[:10], row["universe"],
                             out.get("ultra_score"), out.get("ultra_score_v3"), out.get("buy_score")))
            except Exception:
                recs.append((row["ticker"], str(row["date"])[:10], row["universe"], None, None, None))
        parts.append(pd.DataFrame(recs, columns=["ticker", "date", "universe",
                                                 "ultra_score", "ultra_score_v3", "buy_score"]))
        done += len(df)
        if (ci // CH) % 5 == 0:
            el = time.time() - t0
            log.info("  %s/%s rows (%.0f%%) · %.0f rows/s · eta %.0f min",
                     done, total, done / max(total, 1) * 100, done / max(el, 1),
                     (total - done) / max(done / max(el, 1), 1) / 60)
    res = pd.concat(parts, ignore_index=True)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    res.to_parquet(out_path, index=False)
    log.info("compute done: %s rows → %s (%.0f min)", len(res), out_path, (time.time() - t0) / 60)
    con.close()
    return len(res)


def apply(parquet: str = _PARQ) -> int:
    """EXCLUSIVE write — the backend must be stopped (or call in-process post-swap)."""
    import duckdb
    con = duckdb.connect(_DB)
    try:
        for col, typ in (("ultra_score", "DOUBLE"), ("ultra_score_v3", "INTEGER"), ("buy_score", "DOUBLE")):
            try:
                con.execute(f"ALTER TABLE bars ADD COLUMN {col} {typ}")
            except Exception:
                pass                                    # exists
        n = con.execute(f"""
            UPDATE bars SET
                ultra_score    = s.ultra_score,
                ultra_score_v3 = s.ultra_score_v3,
                buy_score      = s.buy_score
            FROM read_parquet('{parquet}') s
            WHERE bars.ticker = s.ticker
              AND CAST(bars.date AS DATE) = CAST(s.date AS DATE)
              AND bars.universe = s.universe
        """).fetchone()
        log.info("apply done: %s", n)
        return int(n[0]) if n else 0
    finally:
        con.close()


def run_incremental(days: int = 6) -> int:
    """Nightly: compute+apply the last N days in-process (post-swap hook — the backend is
    the DB owner there, so the write is safe)."""
    tmp = _PARQ.replace(".parquet", "_incr.parquet")
    n = compute(days=days, out_path=tmp)
    if n:
        apply(parquet=tmp)
    try:
        os.remove(tmp)
    except OSError:
        pass
    return n


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    mode = sys.argv[1] if len(sys.argv) > 1 else "compute"
    days = None
    if "--days" in sys.argv:
        days = int(sys.argv[sys.argv.index("--days") + 1])
    if mode == "compute":
        compute(days=days)
    elif mode == "apply":
        apply()
    elif mode == "incremental":
        run_incremental(days or 6)
    else:
        print(__doc__)
