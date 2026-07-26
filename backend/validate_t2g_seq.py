"""
validate_t2g_seq.py — backtest the Pine 260519_T2G_SEQ pattern with our engine.
Pattern: current bar = T2G, and within N=15 bars the NEAREST [T2G|Z2G] (=START) has a
FILLER [Z4/Z6/Z9/T9/Z10/Z11/T10/T11/T12] between it and now. Replicated from t_sig/z_sig.
path-sim trail25/60 + per-universe + per-year + 2022 + price/RSI overlays. READ-ONLY.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
FILLER = {"Z4", "Z6", "Z9", "T9", "Z10", "Z11", "T10", "T11", "T12"}
N = 15


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
                   coalesce(vol_bucket,'') vb,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1 THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df, as_of
    finally:
        a.close()


def _seq_fire(is_start, is_filler, is_t2g, n=N):
    m = len(is_start); fire = np.zeros(m, bool)
    for t in range(m):
        if not is_t2g[t]:
            continue
        best = 0
        for i in range(2, n + 1):                 # nearest START at offset 2..N
            if t - i < 0:
                break
            if is_start[t - i]:
                best = i; break
        if best >= 2:
            for j in range(1, best):              # FILLER between START and now
                if is_filler[t - j]:
                    fire[t] = True; break
    return fire


def _grp(df, m):
    d = df.copy(); d["_m"] = m.values if hasattr(m, "values") else m
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _line(lbl, m, df):
    s = _stats("x", _pathsim(_grp(df, m), "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:22s} n=0"
    py = s["per_year"]
    tr = [py[y] for y in TR if y in py]; te = [py[y] for y in TE if y in py]
    tr = sum(tr)/len(tr) if tr else float("nan"); te = sum(te)/len(te) if te else float("nan")
    return (f"  {lbl:22s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    df["is_start"] = (df["t"] == "T2G") | (df["z"] == "Z2G")
    df["is_filler"] = df["t"].isin(FILLER) | df["z"].isin(FILLER)
    df["is_t2g"] = df["t"] == "T2G"
    # per-ticker sequence fire (replicates the Pine nearest-start + filler-between logic)
    fires = np.zeros(len(df), bool)
    for tk, idx in df.groupby("ticker", sort=False).indices.items():
        sub = df.iloc[idx]
        fires[idx] = _seq_fire(sub["is_start"].to_numpy(), sub["is_filler"].to_numpy(), sub["is_t2g"].to_numpy())
    df["seq"] = fires
    clean = df["supp"] == 0
    print(f"as_of {as_of} · T2G total {int(df['is_t2g'].sum())} · T2G_SEQ fires {int(df['seq'].sum())} · trail25/60\n")

    print("── T2G_SEQ vs plain T2G ──")
    print(_line("T2G plain", df["is_t2g"] & clean, df))
    print(_line("T2G_SEQ", df["seq"] & clean, df))
    print("\n── T2G_SEQ by universe ──")
    for u in ("sp500", "nasdaq", "russell2k"):
        print(_line(u, df["seq"] & clean & (df["universe"] == u), df))
    print("\n── T2G_SEQ + price/RSI overlays ──")
    print(_line("≥$21", df["seq"] & clean & (df["close"] >= 21), df))
    print(_line("≥$21 · RSI<45", df["seq"] & clean & (df["close"] >= 21) & (df["rsi_14"] < 45), df))
    print(_line("≥$21 · RSI30-50", df["seq"] & clean & (df["close"] >= 21) & df["rsi_14"].between(30, 50), df))
    print("\n── T2G_SEQ by VOLUME bucket ──")
    for v in ("B", "N", "W", "L", "VB"):
        print(_line(f"vol={v}", df["seq"] & clean & (df["vb"] == v), df))
    print("\n── T2G_SEQ · vol=B + overlays ──")
    b = df["seq"] & clean & (df["vb"] == "B")
    print(_line("vol=B", b, df))
    print(_line("vol=B · ≥$21", b & (df["close"] >= 21), df))
    print(_line("vol=B · ≥$21 · RSI<45", b & (df["close"] >= 21) & (df["rsi_14"] < 45), df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
