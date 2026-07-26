"""
validate_goga_edge.py — Engulf-Goga v2: combine the working ingredients found so far.

Refined hypothesis (user, 2026-07-07): a GREEN bar over a lookback that
  (a) swallows one or more VALIDATED EDGE-signal bars   (absorption — the Engulf-Abs ingredient)
  (b) swallows FEW/NO L46/L34 high-volume bars          (light overhead supply — new finding)
  (c) is oversold (RSI low)                             (the only universe-robust STATE)
should beat plain Engulf-Abs. Swallow = current bar's RANGE covers the prior candle's open
and/or close (full or partial), over the last LB bars (not just 2 like Engulf-Abs).

Baseline for comparison = the existing E_engulfabs. Path-sim trail25/60, per year, dv≥3M.
READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _prep, _pathsim, _stats
from edge_echo import pull

YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
EDGE_COLS = ["E_l43triple", "E_z11t11", "E_washout", "E_dl1", "E_g3", "E_atomic",
             "E_h1bottom", "E_spring", "E_p55", "E_parabola", "E_atomicR", "E_t1capbounce"]


def _swallow_counts(df, LB, flagcol):
    """# swallowed prior bars (range covers prior open and/or close) carrying flagcol==1, over LB."""
    g = df.groupby("ticker", sort=False)
    L = df["low"].to_numpy(float); H = df["high"].to_numpy(float)
    cnt = np.zeros(len(df))
    for k in range(1, LB + 1):
        po = g["open"].shift(k).to_numpy(float); pc = g["close"].shift(k).to_numpy(float)
        fl = g[flagcol].shift(k).to_numpy(float)
        sw = ((po >= L) & (po <= H)) | ((pc >= L) & (pc <= H))
        v = ~np.isnan(po) & ~np.isnan(pc)
        cnt += (sw & v & (fl == 1))
    return cnt


def main():
    t0 = time.time()
    df = _prep(pull("1d"))
    df["yr"] = df["date"].astype(str).str[:4]
    df["anyE"] = df[EDGE_COLS].any(axis=1).astype(float)
    df["isL"] = df["l"].isin(["L46", "L34"]).astype(float)
    df["green"] = df["close"] > df["open"]
    r = df["rsi_14"]
    print(f"rows {len(df):,} · anyE bars {int(df.anyE.sum()):,} ({time.time()-t0:.0f}s)", flush=True)

    for LB in (13, 21):
        df[f"swE{LB}"] = _swallow_counts(df, LB, "anyE")
        df[f"swL{LB}"] = _swallow_counts(df, LB, "isL")
    print(f"swallow counts done ({time.time()-t0:.0f}s)", flush=True)

    # masks (all green). progression: add EDGE-swallow, then low-L, then oversold.
    df["m_engabs"]  = df["E_engulfabs"]                                       # baseline (existing)
    df["m_swE"]     = df.green & (r < 45) & (df.swE21 >= 1)                   # swallow ≥1 edge / 21
    df["m_swE_lite"]= df.green & (r < 45) & (df.swE21 >= 1) & (df.swL21 <= 1) # + light range
    df["m_swE_os"]  = df.green & (r < 40) & (df.swE21 >= 1) & (df.swL21 <= 1) # + oversold
    df["m_swE2_os"] = df.green & (r < 40) & (df.swE21 >= 2) & (df.swL21 <= 1) # ≥2 edges swallowed
    df["m_swE_os_z"]= df.green & (r < 40) & (df.swE21 >= 1) & (df.swL21 == 0) # zero-L
    df["m_noE_os"]  = df.green & (r < 40) & (df.swE21 == 0) & (df.swL21 <= 1) # control: no edge swallowed

    grp = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}

    def rep(cols):
        for name, col in cols:
            s = _stats(name, _pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60))
            if not s or s.get("n", 0) == 0:
                print(f"  {name:26s} n=0"); continue
            py = s["per_year"]
            yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
            print(f"  {name:26s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} "
                  f"win{s['win']:4.1f} pf{str(s['pf']):>4} y{s['pos_years']}/6 | {yr}")

    print("\n── Engulf-Goga v2 (swallow validated EDGE + light-L + oversold) vs Engulf-Abs ──")
    rep([("Engulf-Abs (baseline)", "m_engabs"),
         ("green·R<45·swE≥1", "m_swE"),
         ("  +light-L(swL≤1)", "m_swE_lite"),
         ("  +oversold(R<40)", "m_swE_os"),
         ("  +swE≥2", "m_swE2_os"),
         ("  +zero-L(swL=0)", "m_swE_os_z"),
         ("control: R<40·noE·light", "m_noE_os")])

    # PLATEAU + STRESS: is Engulf-Abs & heavy-L robust across thresholds and 2x slip?
    ea = df["E_engulfabs"]
    for th in (1, 2, 3, 4):
        df[f"ea_L{th}"] = ea & (df.swL21 >= th)
    grp2 = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}
    print("\n── PLATEAU: Engulf-Abs & swL21 >= threshold ──")
    for th in (1, 2, 3, 4):
        s = _stats(f"L>={th}", _pathsim(grp2, f"ea_L{th}", "trail", 0.10, 0.25, 0.25, 60))
        py = s["per_year"]; yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
        print(f"  Engulf-Abs L>={th:<2} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} win{s['win']:4.1f} pf{str(s['pf']):>4} y{s['pos_years']}/6 | {yr}")
    print("  --- 2x slip (30bps) stress ---")
    for name, col in [("Engulf-Abs all", "m_engabs"), ("Engulf-Abs L>=3", "ea_L3")]:
        s = _stats(name, _pathsim(grp2, col, "trail", 0.10, 0.25, 0.25, 60, slip=0.003))
        print(f"  {name:20s} 2x-slip m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} y{s['pos_years']}/6")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
