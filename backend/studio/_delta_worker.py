"""
studio/_delta_worker.py — runs the incremental delta refresh in a SEPARATE PROCESS,
targeting whatever DB `STUDIO_DB_PATH` points at (a staging copy — see incremental_swap.py).

Running the write in its own process against a STAGING file is what lets the live backend
keep serving read-only queries uninterrupted during the daily update (no more DuckDB
"different configuration" / lock conflicts that broke the scanner). The parent atomically
swaps the staging file over the live DB when this worker exits cleanly.

Progress is written to the shared /tmp progress file, so the parent's status endpoint keeps
working across the process boundary.

  python -m studio._delta_worker '["sp500","nasdaq","russell2k"]'
"""
import os, sys, json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Write bars, nothing else. api_bar_signals() decorates every 1d bar with Edge chips, the
# intraday no-volume-event flag and the divergence funnel; each of those reaches through
# ticker_edges → _prep into whole-universe intraday aggregates (15m and 1h DBs, GB-scale)
# that this process then holds forever. Harmless in the backend, fatal here: the worker is
# called once per ticker for ~9,600 tickers and was SIGKILLed by the OS every night from
# 2026-07-28 on. Set BEFORE any backend module is imported.
os.environ["SACHOKI_BARS_ONLY"] = "1"


def main():
    universes = json.loads(sys.argv[1]) if len(sys.argv) > 1 else ["sp500", "nasdaq"]
    tgt = os.environ.get("STUDIO_DB_PATH", "?")
    print(f"[delta_worker] STUDIO_DB_PATH={tgt} universes={universes}", flush=True)

    from studio.db import ensure_schema, get_conn
    from studio.incremental_delta import incremental_delta_refresh
    ensure_schema()
    res = incremental_delta_refresh(universes=universes)
    try:
        from studio.backfill_fwd import backfill_forward_returns
        res["forward_backfill"] = backfill_forward_returns()
    except Exception as e:
        res["forward_backfill"] = {"error": str(e)[:200]}

    # force a checkpoint so the staging .duckdb is self-contained (no lingering .wal)
    # before the parent swaps it over the live DB.
    try:
        c = get_conn(read_only=False)
        c.execute("CHECKPOINT"); c.close()
    except Exception as e:
        print(f"[delta_worker] checkpoint warn: {e}", flush=True)

    print("RESULT_JSON:" + json.dumps(res))


if __name__ == "__main__":
    main()
