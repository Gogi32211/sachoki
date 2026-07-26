"""
validate_engulf_goga.py — NEW descriptor "Engulf-Goga" (user spec 2026-07-07).

For each bar, look back at the last LOOKBACK (20) bars. Using the CURRENT bar's RANGE
[low, high] as the swallower, for every prior candle test whether its OPEN and CLOSE
price levels fall inside the current range:
    full   = BOTH open & close of the prior candle inside current range  (body swallowed)
    part   = EXACTLY ONE of open/close inside                            (open-or-close only)
Classify each prior candle by colour (bull = close>open, bear = close<open) → 4 counts:
    full_bull full_bear part_bull part_bear
Nets (the "difference" the user wants):
    swall_bear = full_bear + part_bear ; swall_bull = full_bull + part_bull
    net = swall_bear - swall_bull   (>0 = the bar swallows more RED than green = absorbed
                                     selling; hypothesis: bullish. Validation decides.)
    full_net = full_bear - full_bull (strong-only)

Then HONEST validation: bucket bars by net (deciles) and by current-bar colour, path-sim
trail25/60 forward per bucket, per year — does the difference carry directional edge?
READ-ONLY. Daily 1D, dv≥3M.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

LOOKBACK = 20
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        return a.execute("""
            WITH r AS (SELECT ticker, date, open, high, low, close, rsi_14, close*volume dv,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=3 AND avg_vol_20d>0)
            SELECT ticker, CAST(date AS VARCHAR)[:10] dstr, open, high, low, close, rsi_14, dv
            FROM r WHERE rn=1 AND dv>=3000000 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()


def compute(df: pd.DataFrame) -> pd.DataFrame:
    """Add Engulf-Goga counts + nets (20-bar lookback). Vectorised over 20 lags."""
    g = df.groupby("ticker", sort=False)
    L = df["low"].to_numpy(float); H = df["high"].to_numpy(float)
    fb = np.zeros(len(df)); fr = np.zeros(len(df))   # full  bull / bear
    pb = np.zeros(len(df)); pr = np.zeros(len(df))   # part  bull / bear
    for k in range(1, LOOKBACK + 1):
        po = g["open"].shift(k).to_numpy(float)
        pc = g["close"].shift(k).to_numpy(float)
        o_in = (po >= L) & (po <= H)
        c_in = (pc >= L) & (pc <= H)
        full = o_in & c_in
        part = o_in ^ c_in
        bull = pc > po; bear = pc < po
        valid = ~np.isnan(po) & ~np.isnan(pc)
        fb += (full & bull & valid); fr += (full & bear & valid)
        pb += (part & bull & valid); pr += (part & bear & valid)
    df = df.copy()
    df["g_full_bull"] = fb; df["g_full_bear"] = fr
    df["g_part_bull"] = pb; df["g_part_bear"] = pr
    df["g_swall_bull"] = fb + pb
    df["g_swall_bear"] = fr + pr
    df["g_net"] = df["g_swall_bear"] - df["g_swall_bull"]
    df["g_full_net"] = fr - fb
    df["bull_bar"] = df["close"] > df["open"]
    return df


def _report(grp, cols, title):
    print(f"\n── {title} — forward path-sim trail25/60 ──")
    for name, col in cols:
        s = _stats(name, _pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60))
        if not s or s.get("n", 0) == 0:
            print(f"  {name:22s} n=0"); continue
        py = s["per_year"]
        yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
        print(f"  {name:22s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} "
              f"win{s['win']:4.1f} pf{str(s['pf']):>4} | {yr}")


def main():
    t0 = time.time()
    df = _pull()
    df["yr"] = df["dstr"].str[:4]
    print(f"rows {len(df):,} ({time.time()-t0:.0f}s) — computing Engulf-Goga…", flush=True)
    df = compute(df)
    print(f"computed ({time.time()-t0:.0f}s)", flush=True)

    # example: last 8 AAPL bars
    ex = df[df.ticker == "AAPL"].tail(8)[["dstr", "open", "close", "g_full_bull", "g_full_bear",
                                          "g_part_bull", "g_part_bear", "g_net"]]
    print("\nexample — AAPL last 8 bars:")
    print(ex.to_string(index=False))

    print("\ndistribution of g_net:", {p: round(float(df.g_net.quantile(p)), 1)
                                       for p in (0.1, 0.25, 0.5, 0.75, 0.9)})

    # precompute all bucket boolean columns on df BEFORE building grp (pathsim reads them)
    df["date"] = df["dstr"]
    p10, p90 = df.g_net.quantile(0.10), df.g_net.quantile(0.90)
    f10, f90 = df.g_full_net.quantile(0.10), df.g_full_net.quantile(0.90)
    df["net_hi"] = df.g_net >= p90            # swallows more RED (absorbed selling)
    df["net_lo"] = df.g_net <= p10            # swallows more GREEN (absorbed buying)
    df["fnet_hi"] = df.g_full_net >= f90
    df["fnet_lo"] = df.g_full_net <= f10
    df["net_hi_grn"] = df.net_hi & df.bull_bar
    df["net_hi_red"] = df.net_hi & ~df.bull_bar
    df["net_lo_grn"] = df.net_lo & df.bull_bar
    df["net_lo_red"] = df.net_lo & ~df.bull_bar
    grp = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}

    _report(grp, [("net_hi (swall RED ≥p90)", "net_hi"),
                  ("net_lo (swall GRN ≤p10)", "net_lo"),
                  ("full_net_hi", "fnet_hi"), ("full_net_lo", "fnet_lo")],
            "g_net directional, ALL bars")
    _report(grp, [("net_hi & bar GREEN", "net_hi_grn"), ("net_hi & bar RED", "net_hi_red"),
                  ("net_lo & bar GREEN", "net_lo_grn"), ("net_lo & bar RED", "net_lo_red")],
            "g_net × current-bar colour")

    # ── RSI conditioning: does absorption-net separate forward returns at RSI extremes? ──
    rsi = df["rsi_14"]
    df["net_hi_os"] = df.net_hi & (rsi < 40)          # swallow RED while oversold → bounce?
    df["net_hi_deep"] = df.net_hi & (rsi < 30)
    df["net_hi_ob"] = df.net_hi & (rsi > 60)          # swallow RED while overbought
    df["net_lo_os"] = df.net_lo & (rsi < 40)          # swallow GREEN while oversold
    df["os_base"] = rsi < 40                          # plain oversold baseline (compare)
    df["net_hi_os_grn"] = df.net_hi & (rsi < 40) & df.bull_bar
    grp = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}  # rebuild w/ new cols
    _report(grp, [("net_hi & RSI<40", "net_hi_os"), ("net_hi & RSI<30", "net_hi_deep"),
                  ("net_hi & RSI>60", "net_hi_ob"), ("net_lo & RSI<40", "net_lo_os"),
                  ("net_hi & RSI<40 & GREEN", "net_hi_os_grn"),
                  ("plain RSI<40 (baseline)", "os_base")],
            "g_net × RSI")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
