"""REACCUM RA-A forward confirmation (research_reaccum.py / REACCUM_V1.json, user: "RA.A c gaakete", 2026-10-05).

Seen history (2022-26): RA-A = re-accumulation STATE + edge cluster conf_n >= 4 for the first time — the AMD
2026-03-04 spring entry. n = 99 (94 tickers, 76 days). Book return vs golden-cross bars of the same day x ATR%-bin:
2022-23 +4.31 [+0.66, +8.48], 2024-26 +2.86 [-2.47, +8.66] -> no PASS on history (too few events). The only clean
test is forward data the research never saw.

Frozen 2026-10-05 (the rule below is a COPY of research_reaccum.py, so later edits there cannot move it).
Entries (bar t) dated 2026-10-05 … 2028-07-31.
  interim read  on/after 2027-10-01 : descriptive only (n, raw return, Δ with CI) — NO verdict, nothing changes
  verdict read  ONCE on/after 2028-10-01 (60-session horizon of the last entry has passed)
STATE at t (all): EMA50 > EMA200 · a VABS climax (sig_clm) in [t-60, t-10] · shapectx touches >= 4 ·
  min RSI14 over [t-10, t] > 35 · >= 2 VABS NS bars (sig_ns_vabs) in [t-20, t] ·
  median(volume / 20-bar median volume) over [t-15, t-1] < 1.0
RA-A = STATE ∧ conf_n >= 4 ∧ conf_n(t-1) < 4      (conf_n from data/edge_votes_v1.parquet —
  REBUILD IT FIRST: python edge_votes_build.py, so conf_n covers the forward window)
Estimand (the research one): per-bar book return (entry open[t+1], trail clip(12*ATR%,15,60), maxh 60, 15 bps,
no cooldown), paired vs golden-cross liquid bars of the same day x ATR%-bin (frozen ATR_CUTS), day bootstrap.
PASS = mean Δ > 0 with 95 % CI lo > 0. Forward k = 1. Side read (no verdict): vs all liquid bars.
POWER WARNING (recorded now): ~21 events / year → ~40 by the verdict read; a true +3 pp effect may still not
reach CI lo > 0. A FAIL with n < 40 means "not shown", not "refuted".

  python reaccum_forward.py --interim     -> the interim descriptive read (refuses before 2027-10-01)
  python reaccum_forward.py               -> the verdict read (refuses before 2028-10-01)
  python reaccum_forward.py --check 2024-01-01 2024-12-31   -> same code on a SEEN window (no verdict)
"""
from __future__ import annotations
import os, sys
from datetime import date

import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS, _paired      # noqa: E402
from research_tzq_centre import book_returns_vec          # noqa: E402
from studio.paths import db_path                          # noqa: E402
from studio.db import UNIVERSE_PRIORITY_SQL               # noqa: E402

DATA = os.path.join(HERE, "..", "data")
FORWARD_START, FORWARD_END = "2026-10-05", "2028-07-31"
INTERIM_ON_OR_AFTER = date(2027, 10, 1)
READ_ON_OR_AFTER = date(2028, 10, 1)


def build(d0: str) -> pd.DataFrame:
    load_from = (pd.Timestamp(d0) - pd.Timedelta(days=420)).strftime("%Y-%m-%d")      # EMA200 + windows warm-up
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    df = con.execute(f"""SELECT ticker, CAST(date AS DATE) d, open o, high h, low l, close c, volume v, rsi_14 rsi,
            COALESCE(sig_clm,0) clm, COALESCE(sig_ns_vabs,0) ns
        FROM bars WHERE universe <> 'index' AND date >= '{load_from}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
        ORDER BY ticker, date""").fetchdf()
    sh = pd.read_parquet(os.path.join(DATA, "shapectx_signals.parquet"), columns=["ticker", "date", "touches"])
    sh["d"] = pd.to_datetime(sh.date).dt.date; sh = sh.drop(columns="date").drop_duplicates(["ticker", "d"])
    ev = pd.read_parquet(os.path.join(DATA, "edge_votes_v1.parquet"), columns=["ticker", "session", "conf_n"])
    ev["d"] = pd.to_datetime(ev.session).dt.date; ev = ev.drop(columns="session").drop_duplicates(["ticker", "d"])
    df["d"] = pd.to_datetime(df.d).dt.date
    df = df.merge(sh, on=["ticker", "d"], how="left").merge(ev, on=["ticker", "d"], how="left")
    df = df.sort_values(["ticker", "d"]).reset_index(drop=True)
    g = df.groupby("ticker", sort=False)
    ew = lambda col, span: g[col].transform(lambda s: s.ewm(span=span, adjust=False, min_periods=span).mean())
    df["e50"], df["e200"] = ew("c", 50), ew("c", 200)
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["vmed"] = g.v.transform(lambda s: s.rolling(20, min_periods=20).median())
    df["vr"] = df.v / df.vmed
    df["clm_win"] = g["clm"].transform(lambda s: s.shift(10).rolling(51, min_periods=1).max())
    df["rsi_min"] = g["rsi"].transform(lambda s: s.rolling(11, min_periods=11).min())
    df["ns_cnt"] = g["ns"].transform(lambda s: s.rolling(21, min_periods=21).sum())
    df["vr_med"] = g["vr"].transform(lambda s: s.shift(1).rolling(15, min_periods=15).median())
    df["conf_prev"] = g["conf_n"].shift(1)
    state = ((df.e50 > df.e200) & (df.clm_win >= 1) & (df.touches >= 4) & (df.rsi_min > 35)
             & (df.ns_cnt >= 2) & (df.vr_med < 1.0))
    df["RA_A"] = state & (df.conf_n >= 4) & (df.conf_prev.fillna(0) < 4)
    return df


def run(d0: str, d1: str) -> dict:
    df = build(d0)
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc"); A = df.atr.to_numpy(float)
    ds = df.d.astype(str).to_numpy()
    liq = ((df.c >= 5) & (df.dv20 >= 5e6) & (df.atr > 0) & df.e200.notna()).to_numpy() & (ds >= d0) & (ds <= d1)
    el = np.flatnonzero(liq)
    e = df.iloc[el][["ticker", "d", "c", "atr", "e50", "e200", "RA_A", "conf_n"]].copy()
    e["r"] = book_returns_vec(o, h, l, c, A, tk, el)
    cov = {"conf_n_last_session": str(df.loc[df.conf_n.notna(), "d"].max()), "bars_last_session": str(df.d.max())}
    e = e[e.r.notna()]
    e["k"] = np.digitize(e.atr / e.c, ATR_CUTS); e["d"] = e.d.astype(str)
    rng = np.random.default_rng(20261005)
    P = e[e.RA_A]
    out = {"window": [d0, d1], "coverage": cov, "n": int(len(P)), "days": int(P.d.nunique()), "tickers": int(P.ticker.nunique()),
           "raw_ret_pct": round(float(P.r.mean() * 100), 2) if len(P) else None}
    gc = e[e.e50 > e.e200].assign(g=lambda f: f.RA_A)
    out["vs_GC"] = _paired(gc[["d", "k", "r", "g"]], rng)
    out["vs_all"] = _paired(e.assign(g=e.RA_A)[["d", "k", "r", "g"]], rng)
    out["events"] = P[["ticker", "d", "c"]].astype(str).values.tolist()
    return out


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--check":
        print("SEEN-WINDOW CHECK (no verdict):", run(sys.argv[2], sys.argv[3]))
    elif sys.argv[1:] == ["--interim"]:
        if date.today() < INTERIM_ON_OR_AFTER:
            sys.exit(f"refusing: the interim read is registered for on/after {INTERIM_ON_OR_AFTER}")
        print("INTERIM (descriptive, NO verdict):", run(FORWARD_START, FORWARD_END))
    elif len(sys.argv) == 1:
        if date.today() < READ_ON_OR_AFTER:
            sys.exit(f"refusing: the verdict read is registered for on/after {READ_ON_OR_AFTER} (single read)")
        print("FORWARD VERDICT READ (REACCUM RA-A):", run(FORWARD_START, FORWARD_END))
    else:
        sys.exit(__doc__)
