"""
t1_analysis.py — ANALYSIS ONLY. Detailed T1 / T1G study, engulf-report style.
T1G = gap-up bull reversal off a bear bar; T1 = inside-reclaim bull reversal.
LIFT vs universe baseline, distribution per holding period, regime by year,
vol-bucket + preceding-bar context, T1 vs T1G head-to-head. Percent units.
Run: cd backend && uv run python ../analysis/t1_analysis.py
"""
import duckdb, numpy as np, pandas as pd
DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
UNIS = ("sp500", "nasdaq", "russell2k")

con = duckdb.connect(DB, read_only=True)

def load(sig):
    return con.execute(f"""
        SELECT universe, ticker, date, vol_bucket, t_sig,
               fwd_5d, fwd_10d, fwd_20d, mfe_10d, mae_10d,
               year(date) AS yr
        FROM (SELECT *, row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn FROM bars
              WHERE t_sig = '{sig}') WHERE rn=1
    """).fetchdf()

def baseline(uni):
    return con.execute(f"""
        SELECT median(fwd_5d) m5, median(fwd_10d) m10, median(fwd_20d) m20,
               avg(CASE WHEN fwd_10d>0 THEN 1.0 ELSE 0 END)*100 win10
        FROM bars WHERE universe='{uni}' AND fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
    """).fetchdf().iloc[0]

def clean(df):
    for c in ("fwd_5d","fwd_10d","fwd_20d","mfe_10d","mae_10d"):
        df[c]=pd.to_numeric(df[c],errors="coerce")
    return df[np.isfinite(df.fwd_10d) & df.fwd_10d.between(-90,500)].copy()

def dist(s):
    s=pd.to_numeric(s,errors="coerce").dropna(); s=s[s.between(-90,500)]
    if len(s)<5: return None
    return dict(n=len(s),med=round(float(s.median()),2),mean=round(float(s.clip(-90,500).mean()),2),
                p25=round(float(s.quantile(.25)),2),p75=round(float(s.quantile(.75)),2),
                win=round(float((s>0).mean()*100),1))

T1=clean(load("T1")); T1G=clean(load("T1G"))
print("=== BASELINES (unconditional) ===")
base={}
for u in UNIS:
    b=baseline(u); base[u]=b
    print(f"{u:10} fwd10 median {round(b.m10,3):>7}  win10 {round(b.win10,1)}%")

for name,df in (("T1",T1),("T1G",T1G)):
    print(f"\n===== {name}  (total n={len(df)}) =====")
    for u in UNIS:
        d=df[df.universe==u]
        if len(d)<30: print(f"  {u:10} n={len(d)} (<30, skip)"); continue
        d10=dist(d.fwd_10d)
        lift=round(d10['med']-float(base[u].m10),2)
        mfe=pd.to_numeric(d.mfe_10d,errors="coerce").median(); mae=abs(pd.to_numeric(d.mae_10d,errors="coerce").median())
        rr=round(mfe/mae,2) if mae else None
        print(f"  {u:10} n={d10['n']:>5}  med10 {d10['med']:>6}  LIFT {lift:>6}  win {d10['win']}%  mean {d10['mean']:>6}  p25 {d10['p25']:>6}  RR {rr}")

# holding period (pooled per universe, T1G nasdaq+r2k where signal lives)
print("\n=== HOLDING PERIOD (T1G, per universe) ===")
for u in UNIS:
    d=T1G[T1G.universe==u]
    if len(d)<30: continue
    print(f"  {u:10} 5d {dist(d.fwd_5d)['med'] if dist(d.fwd_5d) else '—'}  10d {dist(d.fwd_10d)['med']}  20d {dist(d.fwd_20d)['med'] if dist(d.fwd_20d) else '—'}")

# regime by year
print("\n=== REGIME by year — fwd_10d median (T1G) ===")
for u in UNIS:
    d=T1G[T1G.universe==u]
    if len(d)<30: continue
    row=[]
    for y in range(2021,2027):
        dy=d[d.yr==y]; m=dist(dy.fwd_10d)
        row.append(f"{y}:{m['med'] if m else 'na'}(n{len(dy)})")
    print(f"  {u:10} "+"  ".join(row))

# vol bucket context
print("\n=== VOL-BUCKET context — fwd_10d median (T1G) ===")
for u in UNIS:
    d=T1G[T1G.universe==u]
    if len(d)<30: continue
    parts=[]
    for vb in ("VB","B","N","L","W"):
        dv=d[d.vol_bucket==vb]; m=dist(dv.fwd_10d)
        if m and m['n']>=30: parts.append(f"{vb}:{m['med']}(n{m['n']},win{m['win']})")
    print(f"  {u:10} "+"  ".join(parts))

# T1 vs T1G head-to-head (pooled small/micro: nasdaq+russell2k)
print("\n=== T1 vs T1G head-to-head (nasdaq+russell2k pooled-report) ===")
for name,df in (("T1",T1),("T1G",T1G)):
    d=df[df.universe.isin(("nasdaq","russell2k"))]; m=dist(d.fwd_10d)
    mfe=pd.to_numeric(d.mfe_10d,errors='coerce').median()
    print(f"  {name:4} n={m['n']:>5} med10 {m['med']:>6} mean {m['mean']:>6} win {m['win']}% p25 {m['p25']:>6} medMFE {round(float(mfe),1)}")
con.close()
print("\ndone")
