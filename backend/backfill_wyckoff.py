"""
backfill_wyckoff.py — populate the 260529 Wyckoff columns across the whole DB.

Computes ONLY the two Wyckoff engines per (ticker, universe) and UPDATEs the
w2_* / wt_* columns. Much faster than a full enrich_universe (no other engines
re-run). Run with the uvicorn server STOPPED (exclusive DuckDB write lock).

  python -m backfill_wyckoff               # all universes
  python -m backfill_wyckoff nasdaq sp500  # specific
"""
from __future__ import annotations
import sys, time, warnings
warnings.filterwarnings("ignore")

import pandas as pd
from studio.db import get_conn, ensure_schema
from wyckoff_v2_engine import compute_wyckoff_v2
from wyckoff_trig_engine import compute_wyckoff_trig

_INT_COLS = ["w2_sc","w2_ar","w2_st","w2_spring","w2_sos","w2_jac","w2_lps",
             "w2_evr","w2_accum","w2_break","w2_state",
             "wt_valid_tr","wt_sos","wt_spring","wt_lps","wt_evr"]
_DBL_COLS = ["w2_tr_quality","wt_quality","wt_support","wt_resistance"]
_ALL = _INT_COLS + _DBL_COLS


def _compute(df: pd.DataFrame) -> pd.DataFrame:
    w2 = compute_wyckoff_v2(df)
    wt = compute_wyckoff_trig(df)
    out = pd.DataFrame({"id": df["id"].values})
    for c in ["w2_sc","w2_ar","w2_st","w2_spring","w2_sos","w2_jac","w2_lps",
              "w2_evr","w2_accum","w2_break","w2_state","w2_tr_quality"]:
        out[c] = w2[c].values
    for c in ["wt_valid_tr","wt_sos","wt_spring","wt_lps","wt_evr",
              "wt_quality","wt_support","wt_resistance"]:
        out[c] = wt[c].values
    return out


def backfill(universes: list[str]) -> None:
    ensure_schema()  # make sure the w2_*/wt_* columns exist
    started = time.time()
    conn = get_conn(read_only=False)
    try:
        if not universes:
            universes = [r[0] for r in conn.execute(
                "SELECT DISTINCT universe FROM bars ORDER BY universe").fetchall()]
        print(f"universes: {universes}", flush=True)
        set_clause = ", ".join(f"{c}=u.{c}" for c in _ALL)
        grand_rows = 0
        for uni in universes:
            tickers = [r[0] for r in conn.execute(
                "SELECT DISTINCT ticker FROM bars WHERE universe=? ORDER BY ticker", [uni]).fetchall()]
            n = len(tickers); rows = 0; t0 = time.time()
            print(f"[{uni}] {n} tickers", flush=True)
            for i, tk in enumerate(tickers, 1):
                try:
                    df = conn.execute(
                        "SELECT id,open,high,low,close,volume FROM bars "
                        "WHERE ticker=? AND universe=? ORDER BY date", [tk, uni]).fetchdf()
                    if len(df) < 30:
                        continue
                    upd = _compute(df)
                    conn.register("u", upd)
                    conn.execute(f"UPDATE bars SET {set_clause} FROM u WHERE bars.id=u.id")
                    conn.unregister("u")
                    rows += len(df)
                except Exception as e:
                    print(f"  ! {tk}: {type(e).__name__}: {e}", flush=True)
                if i % 200 == 0:
                    conn.commit()
                    el = time.time() - t0
                    print(f"  [{uni}] {i}/{n}  rows={rows:,}  {el:.0f}s  ({i/el:.1f} tk/s)", flush=True)
            conn.commit()
            grand_rows += rows
            print(f"[{uni}] done — {rows:,} rows, {time.time()-t0:.0f}s", flush=True)
        print(f"ALL DONE — {grand_rows:,} rows in {time.time()-started:.0f}s", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    backfill([a.lower() for a in sys.argv[1:]])
