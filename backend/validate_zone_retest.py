"""
validate_zone_retest.py — the user's REAL idea, done right: don't buy the FIRST touch of a
support zone (could be a knife), buy the RETEST (2nd+ touch that holds). Temporal, causal.

Support level  = causal rolling 25-bar low (shifted 3) — an established prior low.
Touch          = bar dips to within +3% of support but not >10% below, AND closes back above
                 support (held), AND is green (bullish reaction).
First touch     = touch with NO prior touch in the last 15 bars.
Retest (2nd+)   = touch WITH ≥1 prior touch in the last 15 bars (been here, left, came back).
Test: forward path-sim trail25/60, first vs retest, per year (+ oversold split). Then dump the
actual signal dates for AMD / SNDK / RGTI. READ-ONLY, dv≥3M.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
TOL = 0.03; FLOORDN = 0.90; PRIORW = 15


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


def enrich(df):
    g = df.groupby("ticker", sort=False)
    df["ref_low"] = g["low"].transform(lambda s: s.rolling(25, min_periods=15).min().shift(3))
    lo = df["low"]; rl = df["ref_low"]
    df["touch"] = (lo <= rl * (1 + TOL)) & (lo >= rl * FLOORDN)
    df["held"]  = df["close"] >= rl
    df["green"] = df["close"] > df["open"]
    df["entry"] = df["touch"] & df["held"] & df["green"] & rl.notna()
    # prior touches in the last PRIORW bars (causal: shift 1 so current bar excluded)
    df["prior_touch"] = g["touch"].transform(
        lambda s: s.astype(float).shift(1).rolling(PRIORW, min_periods=1).sum()).fillna(0)
    df["m_first"]  = df["entry"] & (df["prior_touch"] == 0)
    df["m_retest"] = df["entry"] & (df["prior_touch"] >= 1)
    r = df["rsi_14"]
    df["m_retest_os"] = df["m_retest"] & (r < 45)
    df["m_first_os"]  = df["m_first"] & (r < 45)
    df["m_retest_deep"] = df["m_retest"] & (df["prior_touch"] >= 2)   # 3rd+ touch
    return df


def main():
    t0 = time.time()
    df = enrich(_pull())
    df["yr"] = df["date"].str[:4]
    df["fwd20"] = df.groupby("ticker")["close"].transform(lambda s: s.shift(-20) / s - 1) * 100
    print(f"rows {len(df):,} · first {int(df.m_first.sum()):,} · retest {int(df.m_retest.sum()):,} "
          f"({time.time()-t0:.0f}s)", flush=True)
    grp = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}

    def rep(cols, title):
        print(f"\n── {title} ──")
        for name, col in cols:
            s = _stats(name, _pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60))
            if not s or s.get("n", 0) == 0:
                print(f"  {name:20s} n=0"); continue
            py = s["per_year"]
            yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
            print(f"  {name:20s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} "
                  f"win{s['win']:4.1f} pf{str(s['pf']):>4} y{s['pos_years']}/6 | {yr}")

    rep([("FIRST touch", "m_first"), ("RETEST (2nd+)", "m_retest"),
         ("RETEST deep (3rd+)", "m_retest_deep")], "does the RETEST beat the FIRST touch?")
    rep([("FIRST & RSI<45", "m_first_os"), ("RETEST & RSI<45", "m_retest_os")],
        "+ oversold context")

    # actual signal dates on the 3 studied tickers
    print("\n── signals on AMD / SNDK / RGTI ──")
    for tk in ("AMD", "SNDK", "RGTI"):
        d = df[(df.ticker == tk) & df.entry]
        fr = d[d.m_first]; re = d[d.m_retest]
        print(f"\n  {tk}: {len(fr)} first · {len(re)} retest")
        for _, r in re.tail(8).iterrows():
            f20 = "" if pd.isna(r.fwd20) else f"{r.fwd20:+.0f}%"
            print(f"    RETEST {r.date}  close {r.close:7.2f}  supp {r.ref_low:7.2f}  "
                  f"RSI {r.rsi_14:.0f}  priorTouch {int(r.prior_touch)}  fwd20 {f20}")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
