"""
validate_goga_ema.py — Engulf-Goga net, but ONLY when EMA20>EMA50 AND EMA20>EMA200
(a rising EMA-stack / uptrend filter, user hypothesis 2026-07-07). Does the swallow-net
carry edge inside an uptrend, where it didn't universe-wide? Path-sim trail25/60, per year.
READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        return a.execute("""
            WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,close*volume dv,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=3 AND avg_vol_20d>0)
            SELECT ticker, CAST(date AS VARCHAR)[:10] date, open,high,low,close,rsi_14
            FROM r WHERE rn=1 AND dv>=3000000 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()


def _net(df, LB):
    g = df.groupby("ticker", sort=False)
    L = df["low"].to_numpy(float); H = df["high"].to_numpy(float)
    sg = np.zeros(len(df)); sr = np.zeros(len(df))
    for k in range(1, LB + 1):
        po = g["open"].shift(k).to_numpy(float); pc = g["close"].shift(k).to_numpy(float)
        oi = (po >= L) & (po <= H); ci = (pc >= L) & (pc <= H)
        sw = oi | ci; bull = pc > po; bear = pc < po; v = ~np.isnan(po) & ~np.isnan(pc)
        sg += (sw & bull & v); sr += (sw & bear & v)
    return sr - sg


def main():
    t0 = time.time()
    df = _pull()
    df["yr"] = df["date"].str[:4]
    g = df.groupby("ticker", sort=False)
    for span in (20, 50, 200):
        df[f"e{span}"] = g["close"].transform(lambda s, sp=span: s.ewm(span=sp, adjust=False).mean())
    df["stack"] = (df.e20 > df.e50) & (df.e20 > df.e200)
    df["net21"] = _net(df, 21); df["net34"] = _net(df, 34)
    df["green"] = df["close"] > df["open"]
    r = df["rsi_14"]
    _stpct = df["stack"].mean() * 100
    print(f"rows {len(df):,} · in-stack {_stpct:.0f}% ({time.time()-t0:.0f}s)", flush=True)

    # green bar inside the EMA stack, split by net
    df["st_base"]    = df.green & df["stack"]
    df["st_netpos"]  = df.green & df["stack"] & (df.net34 > 0)
    df["st_netneg"]  = df.green & df["stack"] & (df.net34 <= 0)
    df["st_netbig"]  = df.green & df["stack"] & (df.net34 >= 3)      # strong absorption
    # compare: same net split OUTSIDE the stack
    df["ns_netpos"]  = df.green & ~df["stack"] & (df.net34 > 0)
    df["ns_netneg"]  = df.green & ~df["stack"] & (df.net34 <= 0)
    # stack + RSI context
    df["st_os"]      = df.green & df["stack"] & (r < 45)
    df["st_os_np"]   = df.green & df["stack"] & (r < 45) & (df.net34 > 0)
    df["allbase"]    = df.green

    grp = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}

    def rep(cols, title):
        print(f"\n── {title} ──")
        for name, col in cols:
            s = _stats(name, _pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60))
            if not s or s.get("n", 0) == 0:
                print(f"  {name:22s} n=0"); continue
            py = s["per_year"]
            yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
            print(f"  {name:22s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} "
                  f"win{s['win']:4.1f} pf{str(s['pf']):>4} y{s['pos_years']}/6 | {yr}")

    rep([("all green (baseline)", "allbase"), ("green in-stack", "st_base"),
         ("in-stack & net34>0", "st_netpos"), ("in-stack & net34<=0", "st_netneg"),
         ("in-stack & net34>=3", "st_netbig")],
        "EMA20>50 & EMA20>200 — does net carry edge INSIDE an uptrend?")
    rep([("OUT-stack & net34>0", "ns_netpos"), ("OUT-stack & net34<=0", "ns_netneg")],
        "net split OUTSIDE the stack (contrast)")
    rep([("in-stack & RSI<45", "st_os"), ("in-stack & RSI<45 & net>0", "st_os_np")],
        "stack + oversold + net")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
