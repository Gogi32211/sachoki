"""BOTTOM_CLUSTER_V1 — PART B, the forward confirmation (research_out/BOTTOM_CLUSTER_V1.md).

The rule was SEEN on all history (a NATURE_SHIFT_V1 comparator), so the only clean test is forward.
Frozen 2026-09-28; entries with date_in ≥ 2026-09-29; read ONCE on/after 2027-03-01.

Rules (▽△ anatomy verdict from data/anatomy_signals.parquet — same predicates as the catalog keys
x_anat_rev_rs / x_anat_rev_norm / x_anat_shake in frontend/src/lib/v4ExtraGroups.js):
  bottom(t)  = anat_v == 'rev'  or  anat_v == 'shake'
  B1 CLUSTER = bottom(t) and ≥1 other bottom bar in t-4..t-1                (the frozen rule)
  B2 CL·🔻💪 = B1 and anat_v(t) == 'rev' and anat_rs(t)                      (AMENDMENT_1, declared
               2026-09-28 before any forward data, from the seen-data decomposition; k = 2)
Eligible: close ≥ $5, 20-day mean $volume ≥ $5M. Entry open[t+1], book _pathsim ATR×12 trail, maxh 60,
slip .0015, 5-bar cooldown. Control: every eligible bar on the stride-10 control_keys.phase grid.
Only trades whose full 60-session horizon lies inside the data are scored (right-edge censoring).
PASS per rule: same-day Δ vs control > 0 with day-clustered 95 % CI lo > 0 (days with ≥ 20 control trades).

  python bottom_cluster_forward.py                      → the forward read (refuses before 2027-03-01)
  python bottom_cluster_forward.py --check 2024-01-01 2024-12-31
                                                        → the same code on a SEEN window, to verify
                                                          it reproduces the research (no verdict)
"""
from __future__ import annotations
import os, sys
from datetime import date

import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from edge_replay import _pathsim                       # noqa: E402
from control_keys import phase                         # noqa: E402
from studio.paths import db_path, DATA_DIR             # noqa: E402

FORWARD_START = "2026-09-29"
READ_ON_OR_AFTER = date(2027, 3, 1)
HORIZON = 60


def run(d0: str, d1: str | None) -> dict:
    load_from = (pd.Timestamp(d0) - pd.Timedelta(days=150)).strftime("%Y-%m-%d")    # ATR / $vol / t-4 warm-up
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    bars = con.execute(f"""WITH r AS (SELECT ticker, date, open, high, low, "close", volume,
            row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
            FROM bars WHERE universe <> 'index' AND date >= '{load_from}')
        SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date""").fetchdf()
    bars["date"] = pd.to_datetime(bars["date"]).dt.strftime("%Y-%m-%d")
    an = duckdb.connect().execute(f"SELECT ticker, CAST(date AS VARCHAR) date, v, rs FROM "
                                  f"read_parquet('{DATA_DIR}/anatomy_signals.parquet') WHERE date >= '{load_from}'").fetchdf()
    an["date"] = an["date"].str[:10]
    df = bars.merge(an.drop_duplicates(["ticker", "date"]), on=["ticker", "date"], how="left").reset_index(drop=True)
    g = df.groupby("ticker", sort=False)
    pc = g["close"].shift(1)
    tr = np.maximum(df.high - df.low, np.maximum((df.high - pc).abs(), (df.low - pc).abs()))
    df["atr_14"] = tr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    dv = (df["close"] * df["volume"]).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    el = ((df["close"] >= 5) & (dv >= 5e6) & df["atr_14"].notna()).to_numpy()
    v = df["v"].fillna("").to_numpy(); rs = df["rs"].fillna(False).astype(bool).to_numpy()
    tk = df.ticker.to_numpy(); n = len(df)
    bot = (v == "rev") | (v == "shake")
    prev = np.zeros(n, np.int16)
    for j in range(1, 5):
        s = np.zeros(n, bool); s[j:] = bot[:-j]; s[np.r_[np.zeros(j, bool), tk[j:] != tk[:-j]]] = False; prev += s
    df["B1"] = bot & (prev >= 1) & el
    df["B2"] = df["B1"].to_numpy() & (v == "rev") & rs
    starts = np.r_[0, np.flatnonzero(tk[1:] != tk[:-1]) + 1]; ends = np.r_[starts[1:], n]
    pos = np.arange(n) - np.repeat(starts, ends - starts)
    ph = pd.Series(tk).map(lambda t: phase(t, 10)).to_numpy()
    df["CTRL"] = el & ((pos - ph) % 10 == 0)
    grp = {t: x.reset_index(drop=True) for t, x in df[["ticker", "date", "open", "high", "low", "close", "atr_14",
                                                        "B1", "B2", "CTRL"]].groupby("ticker", sort=False)}
    cal = sorted(df.date.unique())
    last_ok = cal[-(HORIZON + 2)] if len(cal) > HORIZON + 2 else cal[0]     # entry must have 60 sessions after it
    hi = min(d1, last_ok) if d1 else last_ok
    sim = lambda c: _pathsim(grp, c, "trail", 0.10, 0.25, 0.25, HORIZON, slip=0.0015, atr_k=12.0)
    keep = lambda t: t[(t.date_in >= d0) & (t.date_in <= hi)]
    ctrl = keep(sim("CTRL"))
    cd = ctrl.groupby("date_in").ret.agg(["mean", "size"])
    rng = np.random.default_rng(20260928)
    out = {"window": [d0, hi], "control_trades": len(ctrl)}
    for rule in ("B1", "B2"):
        t = keep(sim(rule))
        dm = t.groupby("date_in").ret.mean().to_frame("s").join(cd, how="inner"); dm = dm[dm["size"] >= 20]
        x = ((dm.s - dm["mean"]) * 100).to_numpy()
        if len(x) < 5:
            out[rule] = {"n": len(t), "days": len(x), "note": "too few scored days"}; continue
        lo, hi_ = np.percentile([x[rng.integers(0, len(x), len(x))].mean() for _ in range(3000)], [2.5, 97.5])
        out[rule] = {"n": len(t), "days": len(x), "mean_ret_pct": round(100 * t.ret.mean(), 2),
                     "same_day_delta_pp": round(float(x.mean()), 2), "ci95": [round(lo, 2), round(hi_, 2)],
                     "PASS": bool(x.mean() > 0 and lo > 0)}
    return out


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--check":
        print("SEEN-WINDOW CHECK (no verdict):", run(sys.argv[2], sys.argv[3]))
    elif len(sys.argv) == 1:
        if date.today() < READ_ON_OR_AFTER:
            sys.exit(f"refusing: the forward read is registered for on/after {READ_ON_OR_AFTER} (single read)")
        print("FORWARD READ (BOTTOM_CLUSTER_V1 PART B):", run(FORWARD_START, None))
    else:
        sys.exit(__doc__)
