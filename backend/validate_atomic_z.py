"""
validate_atomic_z.py — the Z-side (bearish) mirror of the atomic edge, SHORT path-sim.
Research claims Z mirrors T: close=A best (vs O for T), ne=E, wick=U bearish. Z = fade
strength. Tested as a SHORT (profit on decline), same rigor: per-year + 2022 + breadth.
Symmetry question: if T (buy weak-close) works in FEAR (risk-off), does Z (short strong-
close) work in EUPHORIA (risk-on)?  READ-ONLY.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _stats
import validate_atomic as VA

SLIP = 0.0015
_ZALL = ("Z1","Z1G","Z2","Z2G","Z3","Z4","Z5","Z6","Z7","Z9","Z10","Z11","Z12")


def _pull(months, dv):
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>={dv}
                         AND date >= DATE '{as_of}' - INTERVAL {int(months)*31+40} DAY)
            SELECT universe, ticker, date, open, high, low, close,
                   coalesce(z_sig,'') z, coalesce(close_suffix,'') csfx, coalesce(ne_suffix,'') ne,
                   coalesce(wick_suffix,'') wick, coalesce(bar_gap_class,'') gap,
                   coalesce(vol_bucket,'') vb,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1 THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df, as_of
    finally:
        a.close()


def _pathsim_short(grp, col, trail=0.25, maxh=60):
    """SHORT: enter next-open, profit on DECLINE, trailing stop above the trough."""
    trades = []
    for tk, g in grp.items():
        if col not in g: continue
        o=g["open"].to_numpy(float); hi=g["high"].to_numpy(float); lo=g["low"].to_numpy(float); cl=g["close"].to_numpy(float)
        ent=g[col].to_numpy(bool); n=len(g); last=-99; yr=g["date"].astype(str).str[:4].to_numpy()
        for i in range(n-1):
            if not ent[i] or i+1>=n or i-last<5: continue
            ep=o[i+1]
            if ep<=0: continue
            last=i; entry=ep*(1-SLIP); ret=None; end=min(i+1+maxh,n); trough=entry
            for j in range(i+1,end):
                trough=min(trough,lo[j]); ts=trough*(1+trail)
                if hi[j]>=ts: ret=(entry-ts)/entry - SLIP; break
            if ret is None: ret=(entry-cl[end-1])/entry - SLIP
            trades.append({"ticker":tk,"ret":ret,"yr":yr[i]})
    return pd.DataFrame(trades)


def _breadth(df, w=20):
    d=df.sort_values(["ticker","date"]).copy()
    d["cw"]=d.groupby("ticker")["close"].shift(w); d["up"]=(d["close"]>d["cw"]).astype(float)
    return d.dropna(subset=["cw"]).groupby("date")["up"].mean()


def _grp(df, mask):
    d=df.copy(); d["_m"]=mask.values
    return {tk:g.reset_index(drop=True) for tk,g in d.groupby("ticker",sort=False)}


def _f(s):
    if not s or s.get("n",0)==0: return "n=0"
    return (f"n={s['n']:>6} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} '22={s['per_year'].get('2022',float('nan')):+5.2f}")


def run():
    print("pulling Z…", flush=True)
    df, as_of = _pull(VA.MONTHS, VA.DVF)
    df["zbase"]=df["z"].isin(_ZALL) & (df["supp"]==0)
    df["br20"]=df["date"].map(_breadth(df))
    b=df["zbase"]
    def st(mask,lbl): return _stats(lbl, _pathsim_short(_grp(df,mask), "_m"))
    print(f"as_of {as_of} · SHORT path-sim trail25 · Z base = any-Z & clean\n")
    print("── Z atomic mirror (short; +=short profit) ──")
    rows=[("Z base","zbase_m"),("+close=A(mirror)",None),("+close=O(sanity)",None),
          ("+gapG2/3",None),("+vol=B",None),("+wick=U",None),("A&gap&B(FULL-Z)",None)]
    masks={
        "Z base": b,
        "+close=A(mirror)": b & (df.csfx=="A"),
        "+close=O(sanity)": b & (df.csfx=="O"),
        "+gapG2/3": b & df.gap.isin(("G2","G3")),
        "+vol=B": b & (df.vb=="B"),
        "+wick=U": b & df.wick.str.contains("U",na=False),
        "A&gap&B(FULL-Z)": b & (df.csfx=="A") & df.gap.isin(("G2","G3")) & (df.vb=="B"),
    }
    for lbl,m in masks.items():
        print(f"  {lbl:18s} {_f(st(m,lbl))}")
    # symmetry: does FULL-Z short work in risk-ON (euphoria)?
    print("\n── SYMMETRY: FULL-Z short by breadth regime (expect risk-ON better for shorts) ──")
    fz=masks["A&gap&B(FULL-Z)"]
    for lbl,m in [("FULL-Z all",fz),("risk-ON(>50%)",fz & (df.br20>=0.5)),("risk-OFF(<50%)",fz & (df.br20<0.5))]:
        print(f"  {lbl:16s} {_f(st(m,lbl))}")
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
