"""
validate_lps_retest.py — the WYCKOFF LPS play (user's insight): after a SOS (Sign of
Strength — close breaks ABOVE the range resistance), price often RETESTS down to that old
resistance (now support) = the LPS (Last Point of Support), the classic continuation long.
This is the OPPOSITE of "buy the AR ceiling" (which whipsaws) — here the level flipped to
support after the breakout.

Detect (backward-looking, no lookahead): fresh SOS at bar j (close>res & prev close<=res);
freeze R=res[j]; within K bars, an LPS bar = low dips to R (±band) AND close holds >= R*(1-0.02)
= a higher-low retest of the broken level. Then path-sim which EDGE setups (and a plain bull-T)
firing AT the LPS beat their ungated selves. trail25/60 gap-realistic. 1d. READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _prep, _pathsim, _stats, SETUPS

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
BAND = 0.05          # ±5% retest tolerance to the broken level
KWIN = 20            # LPS must occur within 20 bars of the SOS


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB
    c = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        return c.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14, atr_14,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
                   coalesce(vol_bucket,'') vb, coalesce(bar_gap_class,'') gap,
                   coalesce(close_suffix,'') csfx, coalesce(bar_line5,'') l5,
                   coalesce(w2_spring,0) spring,
                   coalesce(sig_t11,0) t11, coalesce(sig_t12,0) t12, coalesce(sig_eb_up,0) ebu,
                   coalesce(sig_any_d,0) anyd, coalesce(sig_l1,0) l1,
                   coalesce(sig_p55,0) p55, coalesce(sig_para_start,0) para,
                   CASE WHEN sig_l6=1 AND sig_l4=1 AND close>=open THEN 1 ELSE 0 END l43,
                   coalesce(wt_valid_tr,0) vtr, coalesce(wt_support,0) sup, coalesce(wt_resistance,0) res,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        c.close()


def _line(lbl, m, df):
    d2 = df.copy(); d2["_m"] = m.values
    grp = {tk: gg.reset_index(drop=True) for tk, gg in d2.groupby("ticker", sort=False)}
    s = _stats("x", _pathsim(grp, "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:22s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    return (f"  {lbl:22s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022', float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df = _pull().reset_index(drop=True)
    df = _prep(df)
    n = len(df)
    lps = np.zeros(n, bool)

    for tk, g in df.groupby("ticker", sort=False):
        idx = g.index.to_numpy()
        cl = g["close"].to_numpy(float); lo = g["low"].to_numpy(float)
        res = g["res"].to_numpy(float); sup = g["sup"].to_numpy(float); m = len(g)
        R = None; sos_i = -999          # frozen broken level + its bar
        for i in range(1, m):
            # fresh SOS: close breaks above resistance (was at/below prior bar)
            if res[i] > 0 and cl[i] > res[i] and cl[i - 1] <= res[i - 1]:
                R = res[i]; sos_i = i; continue
            if R is None:
                continue
            if i - sos_i > KWIN:         # SOS too old — deactivate
                R = None; continue
            # LPS retest: dipped to the broken level (now support) and held above it
            if lo[i] <= R * (1 + BAND) and cl[i] >= R * (1 - 0.02) and sup[i] < cl[i]:
                lps[idx[i]] = True
    df["lps"] = lps
    print(f"1d · LPS = retest of broken resistance within {KWIN} bars of a SOS (±{int(BAND*100)}%) "
          f"· in-LPS bars {int(lps.sum()):,} · trail25/60 gap-realistic\n")

    # pure Wyckoff entry: any bull-T at the LPS
    _bullT = df["t"].str.match(r"^T\d").fillna(False)
    print("── pure LPS entries ──")
    print(_line("any bull-T @ LPS", df["lps"] & _bullT & (df["supp"] == 0), df))
    print(_line("bull-T @ LPS · RSI<50", df["lps"] & _bullT & (df["supp"] == 0) & (df["rsi_14"] < 50), df))
    print(_line("bull-T @ LPS · vol=B", df["lps"] & _bullT & (df["supp"] == 0) & (df["vb"] == "B"), df))
    print("\n── EDGE setups: ungated vs @ LPS ──")
    for name, col in SETUPS:
        m_lps = df[col] & df["lps"]
        if int(m_lps.sum()) < 25:
            continue
        print(_line(f"{name}", df[col], df))
        print(_line(f"{name} @ LPS", m_lps, df))
        print()
    print("done.")


if __name__ == "__main__":
    run()
