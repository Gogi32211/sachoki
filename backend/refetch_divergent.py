"""
refetch_divergent.py — one-time de-risked re-fetch of stale/divergent trailing bars.

Context: a ticker living in >1 universe (e.g. CYCU in nasdaq + russell2k) can carry
DIFFERENT data on the SAME date because each universe was fetched at a different time
(one before settle, one after). Divergence is confined to the most-recent bar(s).
The live Massive source (== TradingView) is correct; the stored DB last-bars are stale.

This script calls incremental_delta_refresh in OVERWRITE mode (refetch_from), which:
  • fetches each ticker ONCE per run and reuses the SAME bars for every universe it
    belongs to (the _bar_cache) → identical bars across universes → divergence gone;
  • DELETE-then-INSERT only the (universe, ticker, date) rows it actually re-fetched,
    and ONLY when the fetch returned data → overwrite-on-success, NO pre-delete →
    data loss is physically impossible (an empty/failed fetch leaves the old row).

Run with the uvicorn server STOPPED (DuckDB is single-writer).

Usage:
  # 20-ticker test (no enrich, fast), verify cross-universe consistency:
  .venv/bin/python refetch_divergent.py --test --refetch-from 2026-05-29 --verify
  # full run over ALL DB tickers, with enrich:
  .venv/bin/python refetch_divergent.py --full --refetch-from 2026-05-29 --enrich --verify
"""
from __future__ import annotations

import argparse
import sys
import time

import duckdb
from studio.db import STUDIO_DB_PATH

UNIVERSES = ["sp500", "nasdaq", "russell2k"]
DIV_DATE = "2026-05-29"


def _all_db_tickers() -> set[str]:
    c = duckdb.connect(STUDIO_DB_PATH, read_only=True)
    try:
        return {r[0] for r in c.execute("SELECT DISTINCT ticker FROM bars").fetchall()}
    finally:
        c.close()


def _divergent_tickers(limit: int | None = None) -> list[str]:
    """Tickers whose nasdaq vs russell2k close differ on DIV_DATE."""
    c = duckdb.connect(STUDIO_DB_PATH, read_only=True)
    try:
        rows = c.execute(f"""
            WITH d AS (SELECT ticker, universe, close FROM bars WHERE date='{DIV_DATE}')
            SELECT DISTINCT n.ticker
            FROM d n JOIN d r
              ON n.ticker=r.ticker AND n.universe='nasdaq' AND r.universe='russell2k'
            WHERE ABS(COALESCE(n.close,0)-COALESCE(r.close,0)) > 0.001
            ORDER BY n.ticker
        """).fetchall()
    finally:
        c.close()
    out = [r[0] for r in rows]
    return out[:limit] if limit else out


def _verify(tickers: list[str]) -> None:
    """Print nasdaq vs russell2k close/t/z for each ticker on DIV_DATE."""
    c = duckdb.connect(STUDIO_DB_PATH, read_only=True)
    try:
        bad = 0
        for tk in tickers:
            rows = c.execute(
                "SELECT universe, close, t_sig, z_sig FROM bars "
                "WHERE ticker=? AND date=? AND universe IN ('nasdaq','russell2k') "
                "ORDER BY universe", [tk, DIV_DATE]).fetchall()
            d = {u: (cl, t, z) for u, cl, t, z in rows}
            n = d.get("nasdaq"); r = d.get("russell2k")
            if n and r:
                ok = abs((n[0] or 0) - (r[0] or 0)) <= 0.001
                bad += 0 if ok else 1
                flag = "OK " if ok else "DIVERGENT"
                print(f"  {tk:8s} {flag}  nasdaq={n}  russell2k={r}")
            else:
                present = ",".join(d.keys()) or "none"
                print(f"  {tk:8s} (only in: {present})  {d}")
        print(f"\n  cross-universe divergent among checked: {bad}/{len(tickers)}")
    finally:
        c.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="run on ~20 divergent tickers")
    ap.add_argument("--full", action="store_true", help="run on ALL DB tickers")
    ap.add_argument("--refetch-from", default=DIV_DATE, help="ISO date floor to overwrite")
    ap.add_argument("--enrich", action="store_true", help="enrich_after=True")
    ap.add_argument("--verify", action="store_true", help="print cross-universe check after")
    ap.add_argument("--n", type=int, default=20, help="test ticker count")
    args = ap.parse_args()

    if not (args.test or args.full):
        print("specify --test or --full"); return 2

    from studio.incremental_delta import incremental_delta_refresh

    if args.full:
        tickers = _all_db_tickers()
        print(f"FULL run: {len(tickers)} DB tickers, refetch_from={args.refetch_from}, "
              f"enrich={args.enrich}")
    else:
        tickers = set(_divergent_tickers(limit=args.n))
        # always include the canonical sanity names if divergent
        for must in ("CYCU", "LTRN", "LULU", "AAOI"):
            tickers.add(must)
        print(f"TEST run: {len(tickers)} tickers {sorted(tickers)}, "
              f"refetch_from={args.refetch_from}, enrich={args.enrich}")

    t0 = time.time()
    summary = incremental_delta_refresh(
        universes=UNIVERSES,
        enrich_after=args.enrich,
        only_tickers=tickers,
        refetch_from=args.refetch_from,
    )
    dt = time.time() - t0
    print(f"\n=== done in {dt:.1f}s ===")
    for u, info in summary.get("universes", {}).items():
        print(f"  {u}: checked={info['tickers_checked']} "
              f"inserted={info['new_rows_inserted']} "
              f"affected={info['affected_tickers']} errors={info['errors']}")
        if info.get("error_samples"):
            print(f"     err sample: {info['error_samples'][:2]}")

    if args.verify:
        check = sorted(tickers)[:25] if args.full else sorted(tickers)
        print("\n=== verify (nasdaq vs russell2k on %s) ===" % DIV_DATE)
        _verify(check)
        # global divergence count
        remaining = len(_divergent_tickers())
        print(f"\n=== GLOBAL divergent tickers on {DIV_DATE}: {remaining} (target 0) ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
