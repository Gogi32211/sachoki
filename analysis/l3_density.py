"""l3_density.py — RGTI's markup was L3-heavy (pure demand: vol_up & up_close), NOT L34/L46.
Test L3-density (and broader up-demand density L3|L34|L43|L1) in aggregate — does dense L3 in
the markup zone catch RGTI-type runs, or is it base-rate noise (L3 fires 14% of bars)?
ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,l_sig,close,rsi_14,fwd_10d,sq,load
  FROM bars WHERE fwd_10d IS NOT NULL ORDER BY ticker,universe,date""").fetchdf();a.close()
for c in ["sq","load"]: df[c]=df[c].fillna(0).astype(np.int8)
df["is3"]=(df.l_sig=='L3').astype(np.int8)
df["isUP"]=df.l_sig.isin(['L3','L34','L43','L1','L12']).astype(np.int8)   # up-close demand L-codes
gb=df.groupby(["ticker","universe"],sort=False)
df["d3"]=gb["is3"].transform(lambda s:s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df["dUP"]=gb["isUP"].transform(lambda s:s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df["c10"]=gb["close"].transform(lambda s:s.shift(10))
df=df[df.fwd_10d.between(-90,500)&df.c10.notna()&(df.c10>0)].copy()
df["chg10"]=(df.close/df.c10-1)*100
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
def rep(m,lab):
    s=df[m];n=len(s)
    if n<200:return f"  {lab:34} n={n}"
    e=s.exc;w=int((e>0).sum())
    py=sum(1 for y in range(2021,2027) for sy in [e[s.yr==y]] if len(sy)>=20 and float(sy.median())>0)
    ny=sum(1 for y in range(2021,2027) if len(e[s.yr==y])>=20)
    return f"  {lab:34} {n:>7} {w/n*100:>5.1f} {float(e.median()):>+6.2f} {float(e.clip(-25,25).mean()):>+6.2f} {wl(w,n)*100:>5.1f} {py}/{ny}"
hdr=f"  {'pattern':34} {'n':>7} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
cur3=df.l_sig=='L3'
print("### A) L3-density (trailing-10 count of L3), current bar = L3")
print(hdr)
for k in (0,2,4,6,8):
    print(rep(cur3&(df.d3>=k)&(df.d3<k+2 if k<8 else df.d3>=8), f"  L3 trailing count {k}-{k+1}" if k<8 else "  L3 count >=8"))
print(rep(cur3&(df.d3>=6), "  L3 count >=6"))
print("\n### B) up-demand density (L3|L34|L43|L1|L12) >= K, current = up-code")
print(hdr)
curUP=df.isUP==1
for k in (5,7,9):
    print(rep(curUP&(df.dUP>=k), f"  up-density >= {k}"))
print("\n### C) markup context: L3-dense + RSI 40-65 + price rising")
print(hdr)
MK=(df.l_sig=='L3')&(df.rsi_14>=40)&(df.rsi_14<65)
print(rep(MK&(df.d3>=5), "  L3-dense(>=5) mid-RSI"))
print(rep(MK&(df.d3>=5)&(df.chg10>=0), "  + price >= flat"))
print(rep(MK&(df.d3>=5)&(df.chg10>=10), "  + price rose >=10%"))
print(rep(MK&(df.dUP>=7)&(df.chg10>=0), "  up-density>=7 + price>=flat"))
print(rep(MK&(df.dUP>=7)&(df.chg10>=0)&((df.sq==1)|(df.load==1)), "  up-dense + price-up + sq|load"))
print("\nlegend: medL=median fwd_10d excess. base rate: L3~14% of bars. done")
