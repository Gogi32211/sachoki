"""t1_prevbar.py — ANALYSIS ONLY. Which PRECEDING bar makes T1/T1G best?
Lags bar-1 / bar-2 context (z_sig, t_sig, vol_bucket, wyc_phase) over each ticker,
then measures forward-10d lift of T1/T1G conditioned on what came before.
Per universe, n>=50, lift vs universe baseline + vs signal-alone. Percent units."""
import duckdb, numpy as np, pandas as pd
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
UNIS=("sp500","nasdaq","russell2k")
con=duckdb.connect(DB,read_only=True)

def baseline(u):
    return con.execute(f"SELECT median(fwd_10d) FROM bars WHERE universe='{u}' AND fwd_10d BETWEEN -90 AND 500").fetchone()[0]

def load(sig):
    # compute bar-1 / bar-2 context via window, then keep only the T1/T1G rows
    df=con.execute(f"""
      WITH w AS (
        SELECT universe,ticker,date,t_sig,fwd_10d,vol_bucket,
          lag(z_sig,1)  OVER p AS p1_z,  lag(t_sig,1) OVER p AS p1_t,
          lag(vol_bucket,1) OVER p AS p1_vol, lag(wyc_phase,1) OVER p AS p1_wyc,
          lag(z_sig,2)  OVER p AS p2_z,
          row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn
        FROM bars
        WINDOW p AS (PARTITION BY universe,ticker ORDER BY date)
      )
      SELECT * FROM w WHERE t_sig='{sig}' AND rn=1 AND fwd_10d BETWEEN -90 AND 500
    """).fetchdf()
    df["fwd_10d"]=pd.to_numeric(df.fwd_10d,errors="coerce")
    return df[np.isfinite(df.fwd_10d)].copy()

def lift_table(d,col,base,sigmed,label,minn=50,top=12):
    out=[]
    vc=d[col].fillna("∅").replace("","∅")
    for val,n in vc.value_counts().items():
        if n<minn: continue
        s=d.loc[vc==val,"fwd_10d"]
        out.append((str(val),len(s),round(float(s.median()),2),round(float(s.median())-base,2),
                    round(float(s.median())-sigmed,2),round(float((s>0).mean()*100),1)))
    out.sort(key=lambda x:x[4],reverse=True)
    print(f"   {label}: {'val':10} {'n':>6} {'med':>6} {'lift_base':>9} {'lift_sig':>8} {'win%':>5}")
    for v,n,m,lb,ls,w in out[:top]:
        print(f"     {v:10} {n:>6} {m:>6} {lb:>9} {ls:>8} {w:>5}")

for sig in ("T1G","T1"):
    print(f"\n############ {sig} — preceding-bar context ############")
    df=load(sig)
    for u in UNIS:
        d=df[df.universe==u]; b=baseline(u); sm=float(d.fwd_10d.median())
        print(f"\n## {sig} · {u}  (n={len(d)}, baseline {round(b,3)}, {sig}-alone {round(sm,2)})")
        lift_table(d,"p1_z",  b,sm,"bar-1 Z-code")
        lift_table(d,"p1_vol",b,sm,"bar-1 vol_bucket",minn=80)
        lift_table(d,"p1_wyc",b,sm,"bar-1 wyc_phase",minn=80)
        lift_table(d,"p2_z",  b,sm,"bar-2 Z-code")
con.close()
