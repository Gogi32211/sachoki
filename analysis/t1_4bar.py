"""t1_4bar.py — ANALYSIS ONLY. 4-bar structure with T1/T1G as the 2nd bar:
[bar-1] -> [T1/T1G] -> [bar+1] -> [bar+2]. Entry at bar+2 CLOSE, forward from there
(uses bar+2's own fwd_10d -> no lookahead). Step 1: which 4th bar (bar+2) is best.
Step 2: build the composite 4-bar recipe, report n shrink + lift at each constraint.
nasdaq+russell2k focus (edge zone) + per-universe. Percent units."""
import duckdb, numpy as np, pandas as pd
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
con=duckdb.connect(DB,read_only=True)

def baseline(u):
    return con.execute(f"SELECT median(fwd_10d) FROM bars WHERE universe='{u}' AND fwd_10d BETWEEN -90 AND 500").fetchone()[0]

# both T1 and T1G as the 2nd bar (user: "2nd bar has T1, T1G")
df=con.execute("""
  WITH w AS (
    SELECT universe,ticker,date,t_sig,vol_bucket,
      lag(z_sig,1) OVER p AS p1_z, lag(vol_bucket,1) OVER p AS p1_vol, lag(wyc_phase,1) OVER p AS p1_wyc,
      lead(z_sig,1) OVER p AS n1_z, lead(t_sig,1) OVER p AS n1_t, lead(vol_bucket,1) OVER p AS n1_vol,
      lead(z_sig,2) OVER p AS n2_z, lead(t_sig,2) OVER p AS n2_t, lead(vol_bucket,2) OVER p AS n2_vol,
      lead(CASE WHEN close>open THEN 1 ELSE 0 END,2) OVER p AS n2_bull,
      lead(fwd_10d,2) OVER p AS n2_fwd,
      row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn
    FROM bars WINDOW p AS (PARTITION BY universe,ticker ORDER BY date)
  )
  SELECT * FROM w WHERE t_sig IN ('T1','T1G') AND rn=1 AND n2_fwd BETWEEN -90 AND 500
""").fetchdf()
df["n2_fwd"]=pd.to_numeric(df.n2_fwd,errors="coerce")
df=df[np.isfinite(df.n2_fwd)].copy()
BASE={u:baseline(u) for u in ("sp500","nasdaq","russell2k")}

def med(s):
    a=pd.to_numeric(s,errors="coerce").dropna(); return float(a.median()) if len(a) else np.nan
def win(s):
    a=pd.to_numeric(s,errors="coerce").dropna(); return round(float((a>0).mean()*100),1) if len(a) else 0

print("=== STEP 1: which 4th bar (bar+2) is best? entry at bar+2 close, nasdaq+r2k ===")
d=df[df.universe.isin(("nasdaq","russell2k"))]
b1=med(d.n2_fwd)
print(f" pop n={len(d)}, T1/T1G+2bars-any med {round(b1,2)}")
for col,lab in (("n2_z","bar+2 Z-code"),("n2_t","bar+2 T-code"),("n2_vol","bar+2 vol")):
    vc=d[col].fillna("∅").replace("","∅").astype(str)
    rows=[]
    for v,n in vc.value_counts().items():
        if n<60: continue
        s=d.loc[vc==v,"n2_fwd"]; rows.append((v,len(s),round(med(s),2),round(med(s)-b1,2),win(s)))
    rows.sort(key=lambda x:x[3],reverse=True)
    print(f"  {lab}: "+"  ".join(f"{v}:{m}(Δ{ls},n{n},w{w})" for v,n,m,ls,w in rows[:7]))

print("\n=== STEP 2: composite 4-bar recipe (progressive constraints), per universe ===")
def cell(sub,u):
    s=sub.n2_fwd; b=BASE[u]
    return f"n={len(s):>5} med={round(med(s),2):>6} lift={round(med(s)-b,2):>6} win={win(s)}"

recipes=[
 ("R0  T1/T1G (entry +2)",                 lambda x: x),
 ("R1  +bar-1=Z1G",                        lambda x: x[x.p1_z=='Z1G']),
 ("R2  +bar+1=Z1G (double-gap)",           lambda x: x[(x.p1_z=='Z1G')&(x.n1_z=='Z1G')]),
 ("R1b +bar+1=Z1G only",                   lambda x: x[x.n1_z=='Z1G']),
 ("R3  bar+1=Z1G & no VB(+1,+2)",          lambda x: x[(x.n1_z=='Z1G')&(x.n1_vol!='VB')&(x.n2_vol!='VB')]),
 ("R4  bar+1=Z1G & bar+2 bull & no VB",    lambda x: x[(x.n1_z=='Z1G')&(x.n2_bull==1)&(x.n1_vol!='VB')&(x.n2_vol!='VB')]),
 ("R5  bar-1!=ACC_TR & bar+1=Z1G & noVB",  lambda x: x[(x.p1_wyc!='ACC_TR')&(x.n1_z=='Z1G')&(x.n1_vol!='VB')&(x.n2_vol!='VB')]),
]
for u in ("nasdaq","russell2k","sp500"):
    du=df[df.universe==u]
    print(f"\n {u} (baseline {round(BASE[u],3)}):")
    for name,fn in recipes:
        sub=fn(du)
        flag="  ⚠n<30" if len(sub)<30 else ""
        print(f"   {name:38} {cell(sub,u)}{flag}")
con.close()
