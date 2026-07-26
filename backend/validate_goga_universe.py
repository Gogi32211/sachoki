"""
validate_goga_universe.py — the HONEST universe test of the Engulf-Goga idea, with RSI zones.

Question: does the swallow-net (green bar absorbing more RED than green candles over a
lookback) carry forward edge, AND does it add anything ON TOP of the RSI zone? The
hand-picked SNDK/RGTI/AMD zones suggested net>0 precedes big moves, but full-history on
those 2 tickers already reversed it. This settles it universe-wide, per year.

Metric per bar (green bars): net{LB} = swallowed_red - swallowed_green over last LB bars
(swallow = current range covers the prior candle's open and/or close). net_rise = net55-net21
(the "mega-mover" signature: absorption deepens with lookback). Plus swallow-count tot34.

Buckets (all require GREEN bar), path-sim trail25/60, per year:
  RSI zones (baseline): <30 / 30-40 / 40-50 / 50-60 / >60
  within each zone: net34>0 vs net34<=0   → does net ADD over the zone?
  oversold(<40) × net_rise>0 vs <=0        → the mega-mover signature
  oversold(<40) × high-swallow (tot34 top20%)
READ-ONLY. dv>=3M.
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
    sg = np.zeros(len(df)); sr = np.zeros(len(df)); tot = np.zeros(len(df))
    for k in range(1, LB + 1):
        po = g["open"].shift(k).to_numpy(float); pc = g["close"].shift(k).to_numpy(float)
        oi = (po >= L) & (po <= H); ci = (pc >= L) & (pc <= H)
        sw = oi | ci; bull = pc > po; bear = pc < po; v = ~np.isnan(po) & ~np.isnan(pc)
        sg += (sw & bull & v); sr += (sw & bear & v); tot += (sw & v)
    return sr - sg, tot


def main():
    t0 = time.time()
    df = _pull()
    df["yr"] = df["date"].str[:4]
    print(f"rows {len(df):,} ({time.time()-t0:.0f}s) — computing net…", flush=True)
    df["net21"], _ = _net(df, 21)
    df["net34"], df["tot34"] = _net(df, 34)
    df["net55"], _ = _net(df, 55)
    df["net_rise"] = df["net55"] - df["net21"]
    df["green"] = df["close"] > df["open"]
    print(f"computed ({time.time()-t0:.0f}s)", flush=True)

    r = df["rsi_14"]
    tot80 = df["tot34"].quantile(0.80)
    df["z_deep"] = df.green & (r < 30)
    df["z_os"]   = df.green & (r >= 30) & (r < 40)
    df["z_lo"]   = df.green & (r >= 40) & (r < 50)
    df["z_mid"]  = df.green & (r >= 50) & (r < 60)
    df["z_ob"]   = df.green & (r >= 60)
    # net split within oversold(<40) and low-mid(40-50)
    df["os_netpos"]  = df.green & (r < 40) & (df.net34 > 0)
    df["os_netneg"]  = df.green & (r < 40) & (df.net34 <= 0)
    df["lo_netpos"]  = df.green & (r >= 40) & (r < 50) & (df.net34 > 0)
    df["lo_netneg"]  = df.green & (r >= 40) & (r < 50) & (df.net34 <= 0)
    # mega-mover signature: net rises with lookback
    df["os_rise"]    = df.green & (r < 40) & (df.net_rise > 0)
    df["os_norise"]  = df.green & (r < 40) & (df.net_rise <= 0)
    # high-swallow within oversold
    df["os_bigswall"] = df.green & (r < 40) & (df.tot34 >= tot80)

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

    rep([("RSI<30", "z_deep"), ("RSI30-40", "z_os"), ("RSI40-50", "z_lo"),
         ("RSI50-60", "z_mid"), ("RSI>60", "z_ob")], "RSI zones (green bars) — baseline")
    rep([("OS<40 & net34>0", "os_netpos"), ("OS<40 & net34<=0", "os_netneg"),
         ("40-50 & net34>0", "lo_netpos"), ("40-50 & net34<=0", "lo_netneg")],
        "does net ADD within an RSI zone?")
    rep([("OS<40 & net_rise>0", "os_rise"), ("OS<40 & net_rise<=0", "os_norise"),
         ("OS<40 & big-swallow", "os_bigswall")], "mega-mover signature (net rises w/ LB) + high-swallow")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
