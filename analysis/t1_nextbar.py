"""t1_nextbar.py — ANALYSIS ONLY. After T1/T1G, which NEXT bar (bar+1) is the best
continuation? Causal 2-bar entry: enter on bar+1 CLOSE, measure forward from there
(uses bar+1's own fwd_10d → no lookahead). Conditions on bar+1's t_sig / z_sig /
vol_bucket / direction. Per universe, lift vs baseline. Percent units."""
import duckdb, numpy as np, pandas as pd
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
UNIS=("sp500","nasdaq","russell2k")
con=duckdb.connect(DB,read_only=True)

def baseline(u):
    return con.execute(f"SELECT median(fwd_10d) FROM bars WHERE universe='{u}' AND fwd_10d BETWEEN -90 AND 500").fetchone()[0]

def load(sig):
    df=con.execute(f"""
      WITH w AS (
        SELECT universe,ticker,date,t_sig,
          lead(t_sig,1)     OVER p AS n1_t,
          lead(z_sig,1)     OVER p AS n1_z,
          lead(vol_bucket,1)OVER p AS n1_vol,
          lead(fwd_10d,1)   OVER p AS n1_fwd,
          lead(CASE WHEN close>open THEN 1 ELSE 0 END,1) OVER p AS n1_bull,
          row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn
        FROM bars
        WINDOW p AS (PARTITION BY universe,ticker ORDER BY date)
      )
      SELECT * FROM w WHERE t_sig='{sig}' AND rn=1 AND n1_fwd BETWEEN -90 AND 500
    """).fetchdf()
    df["n1_fwd"]=pd.to_numeric(df.n1_fwd,errors="coerce")
    return df[np.isfinite(df.n1_fwd)].copy()

def tbl(d,col,base,b1med,label,minn=60,top=12):
    out=[]
    vc=d[col].fillna("∅").replace("","∅").astype(str)
    for val,n in vc.value_counts().items():
        if n<minn: continue
        s=d.loc[vc==val,"n1_fwd"]
        out.append((val,len(s),round(float(s.median()),2),round(float(s.median())-base,2),
                    round(float(s.median())-b1med,2),round(float((s>0).mean()*100),1)))
    out.sort(key=lambda x:x[3],reverse=True)
    print(f"   {label}: {'val':9}{'n':>7}{'med':>7}{'lift_base':>10}{'lift_b+1':>9}{'win%':>6}")
    for v,n,m,lb,ls,w in out[:top]:
        print(f"     {v:9}{n:>7}{m:>7}{lb:>10}{ls:>9}{w:>6}")

for sig in ("T1G","T1"):
    print(f"\n############ {sig} — NEXT-bar (bar+1) continuation ############")
    df=load(sig)
    for u in UNIS:
        d=df[df.universe==u]; b=baseline(u); b1=float(d.n1_fwd.median())
        print(f"\n## {sig} · {u}  (n={len(d)}, baseline {round(b,3)}, bar+1-any med {round(b1,2)})")
        # direction first
        bull=d[d.n1_bull==1]; bear=d[d.n1_bull==0]
        print(f"   bar+1 BULL: med {round(float(bull.n1_fwd.median()),2)} (n{len(bull)}, win{round((bull.n1_fwd>0).mean()*100,1)})  |  bar+1 BEAR: med {round(float(bear.n1_fwd.median()),2)} (n{len(bear)}, win{round((bear.n1_fwd>0).mean()*100,1)})")
        tbl(d,"n1_t",b,b1,"bar+1 T-code (continuation)")
        tbl(d,"n1_z",b,b1,"bar+1 Z-code (pullback)")
        tbl(d,"n1_vol",b,b1,"bar+1 vol_bucket",minn=100)
con.close()
