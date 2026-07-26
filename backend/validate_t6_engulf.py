"""
validate_t6_engulf.py — does a T6 that ENGULFS the prior 2-3 bars beat a plain T6?

Hypothesis: a bigger, decisive engulfing (the T6 bar's range/body swallows the last
2-3 bars) is a stronger reversal than a 1-bar engulf. Tested with our engine: path-sim
(stop-first, trail25/60), per-year + 2022, plus the T6-specific context that memory says
is its ONLY edge (project_t6_momentum_dip): buy-the-DIP (close<EMA20, uptrend), trailing,
russell small-cap. READ-ONLY on the 1d DB.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

DVF = 3_000_000
MONTHS = 62
SIG = (sys.argv[1] if len(sys.argv) > 1 else "T6").upper()   # which T-signal to test


def _pull(months, dv):
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>={dv}
                         AND date >= DATE '{as_of}' - INTERVAL {int(months)*31+40} DAY)
            SELECT universe, ticker, date, open, high, low, close, rsi_14, atr_14,
                   coalesce(t_sig,'') t,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1 THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df, as_of
    finally:
        a.close()


def _prep(df):
    g = df.groupby("ticker", sort=False)
    o, c, hi, lo = df["open"], df["close"], df["high"], df["low"]
    bmax = np.maximum(o, c); bmin = np.minimum(o, c)   # body top/bottom
    df["ema20"] = g["close"].transform(lambda s: s.ewm(span=20, adjust=False).mean())
    df["ema50"] = g["close"].transform(lambda s: s.ewm(span=50, adjust=False).mean())
    df["t6"] = df["t"].str.match(r"^T\d") if SIG in ("ANY", "ANYT", "T") else (df["t"] == SIG)
    df["clean"] = df["supp"] == 0
    df["dip"] = (df["close"] < df["ema20"]) & (df["ema20"] > df["ema50"])   # pullback in uptrend
    # engulf prior N bars: current RANGE covers prior N ranges (outside bar) / BODY covers prior N bodies
    for n in (1, 2, 3):
        hmax = g["high"].transform(lambda s: s.shift(1).rolling(n).max())
        lmin = g["low"].transform(lambda s: s.shift(1).rolling(n).min())
        df[f"engR{n}"] = (hi >= hmax) & (lo <= lmin)
        bt = g.apply(lambda x: np.maximum(x["open"], x["close"])).reset_index(level=0, drop=True)
        bb = g.apply(lambda x: np.minimum(x["open"], x["close"])).reset_index(level=0, drop=True)
        btmax = bt.groupby(df["ticker"]).transform(lambda s: s.shift(1).rolling(n).max())
        bbmin = bb.groupby(df["ticker"]).transform(lambda s: s.shift(1).rolling(n).min())
        df[f"engB{n}"] = (bmax >= btmax) & (bmin <= bbmin) & (c > o)   # bullish body engulf
    # per-bar engulf of EXACTLY bar t-k (range & body) → lets us require covering t-2,t-3 but NOT t-1
    bt = np.maximum(o, c); bb = np.minimum(o, c)
    for k in (1, 2, 3):
        df[f"eR{k}"] = (hi >= g["high"].transform(lambda s, k=k: s.shift(k))) & (lo <= g["low"].transform(lambda s, k=k: s.shift(k)))
        _btk = bt.groupby(df["ticker"]).transform(lambda s, k=k: s.shift(k))
        _bbk = bb.groupby(df["ticker"]).transform(lambda s, k=k: s.shift(k))
        df[f"eB{k}"] = (bmax >= _btk) & (bmin <= _bbk) & (c > o)
    df["engR_23n1"] = df["eR2"] & df["eR3"] & ~df["eR1"]   # covers t-2 & t-3 but NOT t-1
    df["engB_23n1"] = df["eB2"] & df["eB3"] & ~df["eB1"]
    return df


def _grp(df, mask):
    d = df.copy(); d["_m"] = mask.values if hasattr(mask, "values") else mask
    return {tk: gg.reset_index(drop=True) for tk, gg in d.groupby("ticker", sort=False)}


def _f(s, ref=None):
    if not s or s.get("n", 0) == 0:
        return "n=0"
    d = f" (Δ{s['mean']-ref:+.2f})" if ref is not None else ""
    return (f"n={s['n']:>5} mean{s['mean']:+5.2f}{d} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} '22={s['per_year'].get('2022',float('nan')):+5.2f}")


def _row(df, mask, lbl, ref=None, **kw):
    return lbl, _stats(lbl, _pathsim(_grp(df, mask), "_m", **kw))


def run():
    print("pulling T6…", flush=True)
    df, as_of = _pull(MONTHS, DVF)
    df = _prep(df)
    base = df["t6"] & df["clean"]
    KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)   # T6 = trailing (memory)
    print(f"as_of {as_of} · path-sim TRAIL25/60 · {SIG} & clean base\n")

    def block(title, submask):
        print(f"── {title} ──")
        b = _stats("base", _pathsim(_grp(df, submask), "_m", **KW))
        bm = b.get("mean", 0)
        print(f"  {'base':22s} {_f(b)}")
        print(f"  {'+engulf3 all (range)':22s} {_f(_stats('x', _pathsim(_grp(df, submask & df['engR3']), '_m', **KW)), bm)}")
        print(f"  {'+eng t2&t3 NOT t1 (rng)':22s} {_f(_stats('x', _pathsim(_grp(df, submask & df['engR_23n1']), '_m', **KW)), bm)}")
        print(f"  {'+eng t2&t3 NOT t1 (body)':22s} {_f(_stats('x', _pathsim(_grp(df, submask & df['engB_23n1']), '_m', **KW)), bm)}")
        print()

    block("ALL universes", base)
    block("+ DIP (close<EMA20, uptrend)", base & df["dip"])
    block("russell2k only", base & (df["universe"] == "russell2k"))
    block("russell2k + DIP", base & (df["universe"] == "russell2k") & df["dip"])
    print(f"as_of {as_of}")


if __name__ == "__main__":
    run()
