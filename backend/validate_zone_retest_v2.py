"""
validate_zone_retest_v2.py — strengthen the RETEST edge with reclaim + absorption + oversold.

Base RETEST (2nd+ touch of a causal 25-bar support, held, green) beat first-touch (median
-1.80 -> -0.17). Now layer the validated ingredients:
  reclaim   : the bar's LOW pierced BELOW support and CLOSE reclaimed above (a shakeout/spring)
  L-absorb  : range swallows >=1 L46/L34 VSA bar in the last 10 (the Engulf-Abs-Lⁿ insight)
  E-absorb  : range swallows >=1 validated EDGE-signal bar in the last 10
  oversold  : RSI < 40
Path-sim trail25/60, per year. Uses edge_replay._prep for the E_* masks + l_sig. READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _prep, _pathsim, _stats
from edge_echo import pull

YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
TOL = 0.03; FLOORDN = 0.90; PRIORW = 15; ABSW = 10
EDGE_COLS = ["E_l43triple", "E_z11t11", "E_washout", "E_dl1", "E_g3", "E_atomic",
             "E_h1bottom", "E_spring", "E_p55", "E_parabola", "E_atomicR", "E_t1capbounce"]


def _sw_count(df, g, flagcol, W):
    L = df["low"].to_numpy(float); H = df["high"].to_numpy(float)
    cnt = np.zeros(len(df))
    for k in range(1, W + 1):
        po = g["open"].shift(k).to_numpy(float); pc = g["close"].shift(k).to_numpy(float)
        fl = g[flagcol].shift(k).to_numpy(float)
        sw = ((po >= L) & (po <= H)) | ((pc >= L) & (pc <= H))
        cnt += (sw & ~np.isnan(po) & ~np.isnan(pc) & (fl == 1))
    return cnt


def main():
    t0 = time.time()
    df = _prep(pull("1d"))
    df["yr"] = df["date"].astype(str).str[:4]
    g = df.groupby("ticker", sort=False)
    df["ref_low"] = g["low"].transform(lambda s: s.rolling(25, min_periods=15).min().shift(3))
    rl = df["ref_low"]; lo = df["low"]
    df["green"] = df["close"] > df["open"]
    df["touch"] = (lo <= rl * (1 + TOL)) & (lo >= rl * FLOORDN)
    df["entry"] = df["touch"] & (df["close"] >= rl) & df["green"] & rl.notna()
    df["prior_touch"] = g["touch"].transform(
        lambda s: s.astype(float).shift(1).rolling(PRIORW, min_periods=1).sum()).fillna(0)
    df["retest"] = df["entry"] & (df["prior_touch"] >= 1)
    df["reclaim"] = (lo < rl) & (df["close"] > rl) & df["green"] & rl.notna() & (df["prior_touch"] >= 1)
    df["isL"] = df["l"].isin(("L46", "L34")).astype(float)
    df["anyE"] = df[EDGE_COLS].any(axis=1).astype(float)
    df["swL"] = _sw_count(df, g, "isL", ABSW)
    df["swE"] = _sw_count(df, g, "anyE", ABSW)
    r = df["rsi_14"]
    print(f"rows {len(df):,} · retest {int(df.retest.sum()):,} · reclaim {int(df.reclaim.sum()):,} ({time.time()-t0:.0f}s)", flush=True)

    df["m_retest"]   = df["retest"]
    df["m_reclaim"]  = df["reclaim"]
    df["m_rc_os"]    = df["reclaim"] & (r < 40)
    df["m_rc_L"]     = df["reclaim"] & (df["swL"] >= 1)
    df["m_rc_E"]     = df["reclaim"] & (df["swE"] >= 1)
    df["m_rc_os_L"]  = df["reclaim"] & (r < 40) & (df["swL"] >= 1)
    df["m_rc_os_abs"]= df["reclaim"] & (r < 40) & ((df["swL"] >= 1) | (df["swE"] >= 1))
    df["m_rt_os_abs"]= df["retest"]  & (r < 40) & ((df["swL"] >= 1) | (df["swE"] >= 1))

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

    rep([("retest (base)", "m_retest"), ("reclaim (dip+recover)", "m_reclaim"),
         ("reclaim+RSI<40", "m_rc_os"), ("reclaim+L-absorb", "m_rc_L"),
         ("reclaim+E-absorb", "m_rc_E"), ("reclaim+RSI<40+L", "m_rc_os_L"),
         ("reclaim+RSI<40+(L|E)", "m_rc_os_abs"), ("retest+RSI<40+(L|E)", "m_rt_os_abs")],
        "strengthening the retest")

    # 2x-slip on the winner(s)
    print("\n── 2x-slip (30bps) stress ──")
    for name, col in [("reclaim+RSI<40+(L|E)", "m_rc_os_abs"), ("reclaim+RSI<40", "m_rc_os")]:
        s = _stats(name, _pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, slip=0.003))
        if s.get("n"):
            print(f"  {name:24s} 2x m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} y{s['pos_years']}/6")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
