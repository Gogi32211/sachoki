"""
validate_t13_zbody.py — T1/T3 bull signal whose BODY ≈ the prior Z (bear) bar's body.
Idea: a balanced 1-to-1 reversal — the bull bar's body roughly matches the immediately-
prior Z bar's body (the bull cleanly offsets the bear). path-sim trail25/60 + per-year +
2022 + universe/price/RSI. READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]


def _pull():
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
                         AND date >= DATE '{as_of}' - INTERVAL {62*31+40} DAY)
            SELECT universe, ticker, date, open, high, low, close, rsi_14, coalesce(vol_bucket,'') vb,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1 THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df, as_of
    finally:
        a.close()


def _grp(df, m):
    d = df.copy(); d["_m"] = m.values if hasattr(m, "values") else m
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _line(lbl, m, df):
    s = _stats("x", _pathsim(_grp(df, m), "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:26s} n=0"
    py = s["per_year"]
    tr = [py[y] for y in TR if y in py]; te = [py[y] for y in TE if y in py]
    tr = sum(tr)/len(tr) if tr else float("nan"); te = sum(te)/len(te) if te else float("nan")
    return (f"  {lbl:26s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    g = df.groupby("ticker", sort=False)
    body = (df["close"] - df["open"]).abs()
    pbody = g.apply(lambda x: (x["close"] - x["open"]).abs()).reset_index(level=0, drop=True).groupby(df["ticker"]).shift(1)
    df["ratio"] = body / pbody.replace(0, np.nan)
    df["t13"] = df["t"].isin(["T1", "T3"])
    df["prevZ"] = g["z"].shift(1).fillna("") != ""
    clean = df["supp"] == 0
    base = df["t13"] & clean
    print(f"as_of {as_of} · trail25/60 · T1/T3 & clean base\n")

    print("── build-up ──")
    print(_line("T1/T3 base", base, df))
    print(_line("+ prevZ (t-1 is a Z)", base & df["prevZ"], df))
    print(_line("+ prevZ & body 0.8-1.25", base & df["prevZ"] & df["ratio"].between(0.8, 1.25), df))
    print(_line("+ prevZ & body 0.7-1.43", base & df["prevZ"] & df["ratio"].between(0.7, 1.43), df))
    print(_line("+ prevZ & body 0.9-1.11", base & df["prevZ"] & df["ratio"].between(0.9, 1.111), df))

    core = base & df["prevZ"] & df["ratio"].between(0.8, 1.25)
    print("\n── prevZ·bodymatch(0.8-1.25) by universe ──")
    for u in ("sp500", "nasdaq", "russell2k"):
        print(_line(u, core & (df["universe"] == u), df))
    print("\n── + price/RSI/vol overlays ──")
    print(_line("≥$21", core & (df["close"] >= 21), df))
    print(_line("≥$21 · RSI<45", core & (df["close"] >= 21) & (df["rsi_14"] < 45), df))
    print(_line("vol=B", core & (df["vb"] == "B"), df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
