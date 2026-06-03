"""
backfill_prebreak_extra.py — populate the PREBREAK extra sub-signals across the DB.

Computes ONLY pb_pp_rtv / pb_fly_cd_c / pb_lvbo (fixed) / pb_follow_confirm per
(ticker, universe) via studio.enricher._compute_prebreak_extra and UPDATEs just
those 4 columns. Pure OHLC math (no network / no VIX). Much faster than a full
enrich_universe. Run with the uvicorn server STOPPED (exclusive DuckDB write).

  python -m backfill_prebreak_extra               # all universes
  python -m backfill_prebreak_extra nasdaq sp500  # specific
"""
from __future__ import annotations
import sys, time, warnings
warnings.filterwarnings("ignore")

import pandas as pd
from studio.db import get_conn, ensure_schema
from studio.enricher import _compute_prebreak_extra

_COLS = ["pb_pp_rtv", "pb_fly_cd_c", "pb_lvbo", "pb_follow_confirm"]


def backfill(universes: list[str]) -> None:
    ensure_schema()  # make sure the 3 new columns exist (ALTER ADD)
    started = time.time()
    conn = get_conn(read_only=False)
    try:
        if not universes:
            universes = [r[0] for r in conn.execute(
                "SELECT DISTINCT universe FROM bars ORDER BY universe").fetchall()]
        print(f"universes: {universes}", flush=True)
        set_clause = ", ".join(f"{c}=u.{c}" for c in _COLS)
        grand = 0
        for uni in universes:
            tickers = [r[0] for r in conn.execute(
                "SELECT DISTINCT ticker FROM bars WHERE universe=? ORDER BY ticker", [uni]).fetchall()]
            n = len(tickers); rows = 0; t0 = time.time()
            print(f"[{uni}] {n} tickers", flush=True)
            for i, tk in enumerate(tickers, 1):
                try:
                    df = conn.execute(
                        "SELECT id, date, open, high, low, close, l_sig, t_sig, "
                        "       is_pivot_high_3, is_pivot_low_3, prebreak_prime "
                        "FROM bars WHERE ticker=? AND universe=? ORDER BY date",
                        [tk, uni]).fetchdf()
                    if len(df) < 2:
                        continue
                    df = _compute_prebreak_extra(df)
                    upd = df[["id"] + _COLS].copy()
                    conn.register("u", upd)
                    conn.execute(f"UPDATE bars SET {set_clause} FROM u WHERE bars.id=u.id")
                    conn.unregister("u")
                    rows += len(df)
                except Exception as e:
                    print(f"  ! {tk}: {type(e).__name__}: {e}", flush=True)
                if i % 200 == 0:
                    conn.commit()
                    el = time.time() - t0
                    print(f"  [{uni}] {i}/{n}  rows={rows:,}  {el:.0f}s", flush=True)
            conn.commit()
            grand += rows
            print(f"[{uni}] done — {rows:,} rows, {time.time()-t0:.0f}s", flush=True)
        print(f"ALL DONE — {grand:,} rows in {time.time()-started:.0f}s", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    backfill([a.lower() for a in sys.argv[1:]])
