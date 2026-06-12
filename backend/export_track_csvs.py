"""
export_track_csvs.py — write the journal REPLAY track records to CSV for DB analysis.

Each row = the signal bar's FULL bars-table row (every DB column — l_sig, rsi_14,
cci_20, all suffixes/signals) JOINED to the trade outcome (entry, exit, pnl, reason,
score, dates). Fully synchronised with the DB so you can load it back into DuckDB and
ask "which feature drives winners?" before changing a pattern.

Usage:   uv run python export_track_csvs.py [months]      (default 12)
Outputs: ~/Downloads/{capit,atomic,ai}_track.csv

Then in DuckDB / Python, e.g.:
    SELECT l_sig, count(*) n, round(avg(pnl),2) avg_pnl,
           round(avg(CASE WHEN pnl>0 THEN 1 ELSE 0 END)*100,1) win
    FROM 'capit_track.csv' GROUP BY l_sig ORDER BY n DESC;
"""
import sys, os
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from ai_journal.db import get_analytics_conn
from ai_journal import capit_journal, atomic_journal, ai_replay

OUT = os.path.expanduser("~/Downloads")
MONTHS = int(sys.argv[1]) if len(sys.argv) > 1 else 12
_OUT_FIRST = ["ticker", "universe", "signal_date", "open_date", "close_date",
              "entry", "exit", "pnl", "reason", "score", "v3", "month"]


def export(name: str, replay_fn) -> None:
    r = replay_fn(months=MONTHS, limit=10_000_000)
    trades = r.get("trades") or []
    if not trades:
        print(f"{name}: no trades"); return
    od = pd.DataFrame(trades)
    od["signal_date"] = pd.to_datetime(od["signal_date"])
    a = get_analytics_conn()
    try:
        tks = tuple(od["ticker"].unique())
        ph = ",".join("?" * len(tks))
        mind = od["signal_date"].min().date().isoformat()
        bars = a.execute(f"SELECT * FROM bars WHERE ticker IN ({ph}) AND date >= DATE '{mind}'",
                         list(tks)).fetchdf()
    finally:
        a.close()
    bars["date"] = pd.to_datetime(bars["date"])
    m = od.merge(bars, left_on=["ticker", "universe", "signal_date"],
                 right_on=["ticker", "universe", "date"], how="left", suffixes=("_trade", ""))
    cols = [c for c in _OUT_FIRST if c in m.columns] + [c for c in m.columns if c not in _OUT_FIRST]
    m = m[cols]
    path = os.path.join(OUT, f"{name}_track.csv")
    m.to_csv(path, index=False)
    matched = int(m["date"].notna().sum()) if "date" in m else 0
    print(f"{name:7}: {len(m):>5} trades | {len(m.columns)} cols | DB-matched {matched} "
          f"| win% {round((m.pnl > 0).mean() * 100, 1)} avg {round(m.pnl.mean(), 2)}% -> {path}")


if __name__ == "__main__":
    print(f"Exporting {MONTHS}-month journal track records to {OUT} …")
    export("capit", capit_journal.replay)
    export("atomic", atomic_journal.replay)
    export("ai", ai_replay.replay)
