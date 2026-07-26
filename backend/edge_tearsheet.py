"""
edge_tearsheet.py — quantstats HTML tearsheet for an Edge setup's PORTFOLIO curve.

Turns edge_replay trade lists (date_in/date_out/ret) into a daily portfolio
return series and renders the full quantstats report (equity curve, drawdown
depth/DURATION, rolling Sharpe, monthly heatmap, VaR/CVaR) — the view our
per-trade stats can't show: how long you sit underwater.

Capital model (honest & simple): equal slots. K = p75 of concurrent open
trades over the window (so the book is realistically "full"); each trade has
weight 1/K and its return is REALIZED on date_out:
    daily_ret(t) = Σ ret_i[date_out_i = t] / K
No compounding across slots, no benchmark (NO yfinance calls — offline only).

Files land in edge_tearsheets/<slug>.html (served by /api/edge-tearsheet).
"""
from __future__ import annotations
import os, re
import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")            # headless — must precede quantstats import
import quantstats as qs

import edge_replay as ER

OUT_DIR = os.path.join(os.path.dirname(__file__), "edge_tearsheets")
os.makedirs(OUT_DIR, exist_ok=True)


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", s).strip("_") or "setup"


def daily_returns(tr: pd.DataFrame) -> tuple[pd.Series, int]:
    """Trade list → daily equal-slot portfolio returns (realized on exit day)."""
    tr = tr.copy()
    tr["din"] = pd.to_datetime(tr["date_in"]); tr["dout"] = pd.to_datetime(tr["date_out"])
    days = pd.date_range(tr["din"].min(), tr["dout"].max(), freq="B")
    # concurrency: open trades per business day → K = p75 (a realistically-full book)
    opens = np.zeros(len(days), int)
    di = {d: i for i, d in enumerate(days)}
    for _, r in tr.iterrows():
        a = di.get(r["din"]); b = di.get(r["dout"])
        if a is None or b is None:
            continue
        opens[a:b + 1] += 1
    K = max(1, int(np.percentile(opens[opens > 0], 75)) if (opens > 0).any() else 1)
    pnl = tr.groupby("dout")["ret"].sum()
    ser = pd.Series(0.0, index=days)
    ser.loc[ser.index.intersection(pnl.index)] = pnl.reindex(ser.index).fillna(0.0)
    return (ser / K).rename("returns"), K


def make_tearsheet(setup: str, months: int = 62, dv_floor: float = 3_000_000,
                   trail: float = 0.25, maxh: int = 60) -> dict:
    grp, as_of = ER._frame(int(months), float(dv_floor))
    match = [s for s in ER.SETUPS if s[0].lower() == setup.lower()]
    if not match:
        return {"error": f"unknown setup '{setup}'", "known": [s[0] for s in ER.SETUPS]}
    name, col = match[0]
    tr = ER._pathsim(grp, col, "trail", 0.10, 0.25, trail, maxh)
    if len(tr) < 20:
        return {"error": f"only {len(tr)} trades — not enough for a tearsheet"}
    rets, K = daily_returns(tr)
    path = os.path.join(OUT_DIR, f"{_slug(name)}_{months}mo.html")
    qs.reports.html(rets, output=path, title=f"{name} · {months}mo · trail{int(trail*100)} · {K} slots",
                    download_filename=os.path.basename(path))
    return {"setup": name, "months": months, "n_trades": int(len(tr)), "slots": K,
            "as_of": as_of, "file": os.path.basename(path), "path": path}


if __name__ == "__main__":
    import sys
    r = make_tearsheet(sys.argv[1] if len(sys.argv) > 1 else "T1-CapBounce",
                       months=int(sys.argv[2]) if len(sys.argv) > 2 else 62)
    print(r)
