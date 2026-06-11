"""volume_optimal_lzones.py — find the OPTIMAL volume for L34/L46 signals across RSI zones.
L34 only requires vol>vol[1] (not by how much). Test vol_ratio = volume/avg_vol_20d in fine
buckets, per RSI zone — is there a Goldilocks level (not too much = VB spike, not too little)?
Cross-check vs vol_bucket (W/L/N/B/VB) — recall VB underperformed B/N. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT universe,date,l_sig,rsi_14,volume,avg_vol_20d,vol_bucket,fwd_10d
  FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
   AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND avg_vol_20d>0""").fetchdf();a.close()
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["vr"]=df.volume/df.avg_vol_20d
VB=[("<0.5",0,.5),("0.5-0.8",.5,.8),("0.8-1.2",.8,1.2),("1.2-1.8",1.2,1.8),("1.8-2.5",1.8,2.5),
    ("2.5-4",2.5,4),("4-7",4,7),("7-15",7,15),(">15",15,1e9)]
ZONES=[("all RSI",0,101),("20-30 (capit)",20,30),("30-40",30,40),("40-50",40,50),
       ("50-60",50,60),("60-70",60,70)]
def cell(s):
    if len(s)<60: return f"{'—':>13}"
    e=s.exc; med=float(e.median()); py=sum(1 for y in range(2021,2027) for sy in [e[s.yr==y]] if len(sy)>=10 and float(sy.median())>0)
    ny=sum(1 for y in range(2021,2027) if len(e[s.yr==y])>=10)
    return f"{med:>+6.2f}({len(s)//1000 if len(s)>=1000 else len(s)}{'k' if len(s)>=1000 else ''})/{py}{'/'}{ny}"
print("cell = median fwd_10d EXCESS (n) /posYr/totYr   ·   vol_ratio = volume / avg_vol_20d")
print("\n### vol_ratio × RSI zone  (L34+L46 pooled, all universes)")
print(f"  {'vol/avg':9}"+"".join(f"{z[0]:>15}" for z in ZONES))
for lab,lo,hi in VB:
    cells=[]
    for zl,zlo,zhi in ZONES:
        s=df[(df.vr>=lo)&(df.vr<hi)&(df.rsi_14>=zlo)&(df.rsi_14<zhi)]
        cells.append(cell(s))
    print(f"  {lab:9}"+"".join(f"{c:>15}" for c in cells))

print("\n### vol_bucket (W/L/N/B/VB) × RSI zone  — confirm VB underperforms")
for lab in ["W","L","N","B","VB"]:
    cells=[]
    for zl,zlo,zhi in ZONES:
        s=df[(df.vol_bucket==lab)&(df.rsi_14>=zlo)&(df.rsi_14<zhi)]
        cells.append(cell(s))
    print(f"  {lab:9}"+"".join(f"{c:>15}" for c in cells))
print("\nlegend: + bullish vs universe. /posYr/totYr = robustness. done")
