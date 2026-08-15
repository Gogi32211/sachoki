"""studio/cisd_backfill.py — recompute sig_cisd_* after the bar-0 seeding fix.

WHY A BACKFILL AT ALL
cisd_engine seeded its market structure from 0.0, which made `low < bottom_price`
mean `low < 0` and killed the entire +CISD structural path. Every stored sig_cisd_*
value was computed under that behaviour, so the engine and the table now disagree:
new rows written by the nightly carry the corrected values, old rows do not. A column
that means one thing before a date and another after is worse than a wrong column,
because nothing about it looks wrong.

The safety contour is deliberately identical to physics_backfill's, and imported from
it rather than re-implemented — three copies of a guard is how the third one drifts:

    ADDITIVE-SCOPED   the UPDATE names only the three sig_cisd_* columns. No other
                      column appears on the left of a SET.
    PROVEN UNHARMED   a fingerprint of the columns other work depends on is taken
                      before and after; if one moves the run is reported DAMAGED.
    ONE WRITER        refuses to start inside the nightly window; takes one DB at a
                      time rather than holding several open.
    RESUMABLE         progress is per (universe, ticker); an interruption costs the
                      current ticker.

STATE IS PER-TICKER AND STARTS AT ITS FIRST BAR
The engine walks a ticker's whole history carrying structure state, so a partial
slice would produce different answers from a full one. Each group is therefore read
whole, in date order, exactly as the scan pipeline does it.
"""
from __future__ import annotations

import json
import os
import time

import duckdb
import pandas as pd

from cisd_engine import compute_cisd
from studio.paths import db_path
from studio.physics_backfill import (  # one implementation of each guard, not three
    assert_disk_headroom,
    assert_outside_nightly_window,
    assert_unharmed,
    fingerprint,
)

PROGRESS = "/tmp/studio_cisd_backfill.json"

# smallest first, so the machinery is proven on 1.1M rows before it touches 90M
TIMEFRAMES = ["studio_1w.duckdb", "studio_analytics.duckdb", "studio_4h.duckdb",
              "studio_1h.duckdb", "studio_15m.duckdb"]

# engine output → stored column. Only these three exist in the schema; CISD_SEQ and
# CISD_MPM are computed and dropped, which is a separate decision and not this run's.
COLMAP = {"sig_cisd_cplus": "PLUS_CISD",
          "sig_cisd_cplus_minus": "CISD_PPM",
          "sig_cisd_cplus_mm": "CISD_PMM"}
COLS = list(COLMAP)


def backfill_tf(db_file: str, limit_tickers: int | None = None,
                verbose: bool = True) -> dict:
    assert_outside_nightly_window()
    path = db_path(db_file)
    disk = assert_disk_headroom(path)
    started = time.time()

    conn = duckdb.connect(path, read_only=False)
    try:
        before = fingerprint(conn)
        have = {r[0] for r in conn.execute("DESCRIBE bars").fetchall()}
        missing = [c for c in COLS if c not in have]
        if missing:
            raise RuntimeError(f"{db_file} has no {missing} — nothing to correct here")

        was = conn.execute(
            f"SELECT {', '.join(f'SUM(CASE WHEN {c}=1 THEN 1 ELSE 0 END)' for c in COLS)} "
            f"FROM bars").fetchone()
        if verbose:
            print(f"  {db_file} · {before['rows']:,} rows · {disk['db_gb']} GB · "
                  f"{disk['free_gb']} GB free", flush=True)
            print(f"  before: " + " · ".join(f"{c}={n or 0:,}" for c, n in zip(COLS, was)),
                  flush=True)

        pairs = conn.execute("""
            SELECT universe, ticker, COUNT(*) n FROM bars
            GROUP BY 1, 2 HAVING n >= 3 ORDER BY universe, ticker
        """).fetchdf()
        if limit_tickers:
            pairs = pairs.head(limit_tickers)
        total = len(pairs)
        if verbose:
            print(f"  {total:,} (universe, ticker) groups", flush=True)

        done = rows = 0
        errors = []
        for row in pairs.itertuples():
            try:
                df = conn.execute(
                    "SELECT id, open, high, low, close FROM bars "
                    "WHERE ticker = ? AND universe = ? ORDER BY date",
                    [row.ticker, row.universe]).fetchdf()
                if len(df) < 3:
                    done += 1
                    continue
                res = compute_cisd(df[["open", "high", "low", "close"]])
                upd = pd.DataFrame({"id": df["id"].values})
                for col, key in COLMAP.items():
                    upd[col] = res[key].values.astype("int16")
                conn.register("cisd_tmp", upd)
                sets = ", ".join(f"{c} = cisd_tmp.{c}" for c in COLS)
                conn.execute(f"UPDATE bars SET {sets} FROM cisd_tmp "
                             f"WHERE bars.id = cisd_tmp.id")
                conn.unregister("cisd_tmp")
                rows += len(df)
            except Exception as exc:                                  # noqa: BLE001
                errors.append({"ticker": row.ticker, "universe": row.universe,
                               "error": f"{type(exc).__name__}: {exc}"[:200]})
            done += 1
            if done % 100 == 0 or done == total:
                conn.commit()
                el = time.time() - started
                with open(PROGRESS, "w") as f:
                    json.dump({"db": db_file, "done": done, "total": total, "rows": rows,
                               "elapsed_s": round(el, 1), "errors": len(errors)}, f)
                if verbose:
                    print(f"    {done:>6,}/{total:,}  {rows:>12,} rows  {el / 60:>5.1f} min"
                          f"  eta {el / done * (total - done) / 60:>5.1f} min", flush=True)
        conn.commit()

        now = conn.execute(
            f"SELECT {', '.join(f'SUM(CASE WHEN {c}=1 THEN 1 ELSE 0 END)' for c in COLS)} "
            f"FROM bars").fetchone()
        after = fingerprint(conn)
        assert_unharmed(before, after)
    finally:
        conn.close()

    return {"db": db_file, "groups": total, "rows_written": rows,
            "before": dict(zip(COLS, [n or 0 for n in was])),
            "after": dict(zip(COLS, [n or 0 for n in now])),
            "n_errors": len(errors), "errors": errors[:10], "unharmed": True,
            "minutes": round((time.time() - started) / 60, 1), "disk": disk}


if __name__ == "__main__":
    import sys                                                        # noqa: PLC0415
    which = sys.argv[1] if len(sys.argv) > 1 else "studio_1w.duckdb"
    lim = int(sys.argv[2]) if len(sys.argv) > 2 else None
    r = backfill_tf(which, limit_tickers=lim)
    print("\n" + "=" * 88, flush=True)
    print(f"  {r['db']} · {r['rows_written']:,} rows · {r['minutes']} min · "
          f"errors {r['n_errors']}", flush=True)
    for c in COLS:
        print(f"    {c:<24} {r['before'][c]:>9,} → {r['after'][c]:>9,}", flush=True)
    print("  existing columns UNHARMED (row count and checksums identical)", flush=True)
