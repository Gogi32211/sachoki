"""
validate_goga_lsig.py — extend Engulf-Goga: count how many swallowed prior bars carried an
L46/L34 VSA volume-line (l_sig), and test whether swallowing those adds forward edge.

Per bar (green), over last LB bars: swL = # swallowed prior candles whose l_sig in {L46,L34}
(swallow = current range covers the prior open and/or close). Test vs RSI zones, per year.
Does 'swallowing many L46/L34 volume-events' precede better forward returns? READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
LB = 34


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        return a.execute("""
            WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,coalesce(l_sig,'') l_sig,
                              close*volume dv,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=3 AND avg_vol_20d>0)
            SELECT ticker, CAST(date AS VARCHAR)[:10] date, open,high,low,close,rsi_14,l_sig
            FROM r WHERE rn=1 AND dv>=3000000 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()


def main():
    t0 = time.time()
    df = _pull()
    df["yr"] = df["date"].str[:4]
    df["isL"] = df["l_sig"].isin(["L46", "L34"]).astype(float)
    print(f"rows {len(df):,} · L46/L34 bars {int(df.isL.sum()):,} ({time.time()-t0:.0f}s) — counting swallowed-L…", flush=True)
    g = df.groupby("ticker", sort=False)
    L = df["low"].to_numpy(float); H = df["high"].to_numpy(float)
    swL = np.zeros(len(df)); swAll = np.zeros(len(df))
    for k in range(1, LB + 1):
        po = g["open"].shift(k).to_numpy(float); pc = g["close"].shift(k).to_numpy(float)
        isl = g["isL"].shift(k).to_numpy(float)
        sw = ((po >= L) & (po <= H)) | ((pc >= L) & (pc <= H))
        v = ~np.isnan(po) & ~np.isnan(pc)
        swL += (sw & v & (isl == 1)); swAll += (sw & v)
    df["swL"] = swL; df["swAll"] = swAll
    df["green"] = df["close"] > df["open"]
    print(f"computed ({time.time()-t0:.0f}s) · swL dist p50/p80/p90: "
          f"{df.swL.quantile(.5):.0f}/{df.swL.quantile(.8):.0f}/{df.swL.quantile(.9):.0f}", flush=True)

    r = df["rsi_14"]
    p80 = df.swL.quantile(0.80); p90 = df.swL.quantile(0.90)
    df["g_hiL"]    = df.green & (df.swL >= p80)                 # swallows many L46/L34
    df["g_loL"]    = df.green & (df.swL == 0)                   # swallows none
    df["os_hiL"]   = df.green & (r < 40) & (df.swL >= p80)      # + oversold
    df["os_loL"]   = df.green & (r < 40) & (df.swL == 0)
    df["os_base"]  = df.green & (r < 40)                        # oversold baseline
    df["os_veryL"] = df.green & (r < 40) & (df.swL >= p90)      # top decile L-swallow
    # L-swallow as a FRACTION of all swallowed (dense volume-events in the range)
    frac = df.swL / df.swAll.replace(0, np.nan)
    df["os_Lfrac"] = df.green & (r < 40) & (frac >= 0.5)        # ≥half of swallowed carried L46/L34

    grp = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}

    def rep(cols, title):
        print(f"\n── {title} ──")
        for name, col in cols:
            s = _stats(name, _pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60))
            if not s or s.get("n", 0) == 0:
                print(f"  {name:24s} n=0"); continue
            py = s["per_year"]
            yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
            print(f"  {name:24s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} "
                  f"win{s['win']:4.1f} pf{str(s['pf']):>4} y{s['pos_years']}/6 | {yr}")

    rep([("green & swL>=p80", "g_hiL"), ("green & swL==0", "g_loL")],
        "swallow many L46/L34 vs none (all green)")
    rep([("OS<40 base", "os_base"), ("OS<40 & swL>=p80", "os_hiL"),
         ("OS<40 & swL==0", "os_loL"), ("OS<40 & swL>=p90", "os_veryL"),
         ("OS<40 & L-frac>=0.5", "os_Lfrac")],
        "does swallowing L46/L34 ADD over oversold?")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
