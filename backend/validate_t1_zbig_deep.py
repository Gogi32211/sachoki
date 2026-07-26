"""
validate_t1_zbig_deep.py — deepen the star cell: T1 with a prior Z whose body ≫ the T1
body (ratio = T_body/Z_body < 0.5) = a small T1 bounce off a big bear/capitulation Z.
Break down by the L VSA-volume line (l_sig) on the T1 bar AND on the prior Z bar, plus
RSI and vol. path-sim trail25/60 + TRAIN/TEST + 2022. READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
RSIB = [(0, 30, "RSI<30"), (30, 40, "RSI30-40"), (40, 50, "RSI40-50"), (50, 200, "RSI50+")]
SIG = (sys.argv[1] if len(sys.argv) > 1 else "T1").upper()
DIR = (sys.argv[2] if len(sys.argv) > 2 else "Zbig")   # Zbig=ratio<0.5 · Tbig=ratio>2


def _pull():
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
                         AND date >= DATE '{as_of}' - INTERVAL {62*31+40} DAY)
            SELECT universe, ticker, date, open, high, low, close, rsi_14,
                   coalesce(vol_bucket,'') vb, coalesce(l_sig,'') l,
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
        return f"    {lbl:16s} n=0"
    py = s["per_year"]
    tr = [py[y] for y in TR if y in py]; te = [py[y] for y in TE if y in py]
    tr = sum(tr)/len(tr) if tr else float("nan"); te = sum(te)/len(te) if te else float("nan")
    return (f"    {lbl:16s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    g = df.groupby("ticker", sort=False)
    body = (df["close"] - df["open"]).abs()
    df["ratio"] = body / body.groupby(df["ticker"]).shift(1).replace(0, np.nan)
    df["prevZ"] = g["z"].shift(1).fillna("") != ""
    df["prevL"] = g["l"].shift(1).fillna("")            # L on the prior Z bar
    clean = df["supp"] == 0
    dmask = (df["ratio"] < 0.5) if DIR == "Zbig" else (df["ratio"] > 2.0)
    dlbl = "Z≫T (T_body<0.5×Z)" if DIR == "Zbig" else "T≫Z (T_body>2×Z)"
    core = (df["t"] == SIG) & clean & df["prevZ"] & dmask
    print(f"as_of {as_of} · trail25/60 · {SIG} · prevZ · {dlbl}  base n={int(core.sum())}\n")
    print(_line("BASE", core, df))

    print("\n── by L VSA-line on the T1 bar ──")
    for lv in sorted(df.loc[core, "l"].value_counts().index):
        m = core & (df["l"] == lv)
        if int(m.sum()) >= 100:
            print(_line(lv or "(none)", m, df))

    print("\n── by L on the PRIOR Z (capitulation) bar ──")
    for lv in sorted(df.loc[core, "prevL"].value_counts().index):
        m = core & (df["prevL"] == lv)
        if int(m.sum()) >= 100:
            print(_line("prevL=" + (lv or "none"), m, df))

    print("\n── by RSI ──")
    for lo, hi, lbl in RSIB:
        print(_line(lbl, core & df["rsi_14"].between(lo, hi), df))
    print("\n── by vol ──")
    for v in ("B", "N", "W", "L", "VB"):
        print(_line("vol=" + v, core & (df["vb"] == v), df))

    print("\n── candidate combos ──")
    print(_line("L5/L46 · RSI30-50", core & df["l"].isin(["L5", "L46"]) & df["rsi_14"].between(30, 50), df))
    print(_line("RSI30-50 · vol=B", core & df["rsi_14"].between(30, 50) & (df["vb"] == "B"), df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
