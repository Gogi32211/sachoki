"""l_density.py — the user's obs: L34/L46 REPEAT (cluster) before a breakout, sometimes
1-bar gap, sometimes tighter. Test the DENSITY (count of L34/L46 in the trailing window) →
forward edge, and the alternation (L34<->L46 switching = coil). Not just frequency (already
flat) — clustering. Per-year + context. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,l_sig,rsi_14,cci_20,fwd_10d
  FROM bars WHERE fwd_10d IS NOT NULL ORDER BY ticker,universe,date""").fetchdf();a.close()
df["isL"]=df.l_sig.isin(['L34','L46']).astype(np.int8)
df["is34"]=(df.l_sig=='L34').astype(np.int8); df["is46"]=(df.l_sig=='L46').astype(np.int8)
gb=df.groupby(["ticker","universe"],sort=False)
# trailing counts (last W bars, EXCLUDING current) — the cluster density
for W in (5,10):
    df[f"d{W}"]=gb["isL"].transform(lambda s: s.shift(1).rolling(W,min_periods=1).sum()).fillna(0)
# alternation: both L34 AND L46 present in last 10 (coil switching)
df["c34"]=gb["is34"].transform(lambda s: s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df["c46"]=gb["is46"].transform(lambda s: s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df=df[df.fwd_10d.between(-90,500)].copy()
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
def rep(m,lab):
    s=df[m];n=len(s)
    if n<300:return f"  {lab:28} n={n}"
    e=s.exc;w=int((e>0).sum())
    py=sum(1 for y in range(2021,2027) for sy in [e[s.yr==y]] if len(sy)>=20 and float(sy.median())>0)
    ny=sum(1 for y in range(2021,2027) if len(e[s.yr==y])>=20)
    return f"  {lab:28} {n:>8} {w/n*100:>5.1f} {float(e.median()):>+6.2f} {float(e.clip(-25,25).mean()):>+6.2f} {wl(w,n)*100:>5.1f} {py}/{ny}"
hdr=f"  {'pattern':28} {'n':>8} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
print("### A) density: # of L34/L46 in trailing 10 bars (current bar = L34/L46) → fwd")
print(hdr)
cur=df.isL==1
for k in range(0,9):
    print(rep(cur&(df.d10==k), f"  trailing-10 count = {k}"))
print(rep(cur&(df.d10>=6), "  trailing-10 count >= 6"))
print("\n### B) trailing-5 (tighter cluster), current=L34/L46")
print(hdr)
for k in range(0,6):
    print(rep(cur&(df.d5==k), f"  trailing-5 count = {k}"))
print("\n### C) alternation (coil): both L34 & L46 in last 10, current=L34/L46")
print(hdr)
print(rep(cur&(df.c34>=1)&(df.c46>=1), "  L34&L46 both prior (coil)"))
print(rep(cur&(df.c34>=2)&(df.c46>=2), "  >=2 each (tight coil)"))
print(rep(cur&((df.c34==0)|(df.c46==0)), "  only one type prior"))
print("\n### D) density × oversold context (RSI<35) — does clustering matter MORE oversold?")
print(hdr)
os=df.rsi_14<35
print(rep(cur&os&(df.d10<=2), "  oversold + sparse(<=2)"))
print(rep(cur&os&(df.d10>=5), "  oversold + dense(>=5)"))
print(rep(cur&os&(df.c34>=1)&(df.c46>=1), "  oversold + coil(both)"))
print("\nlegend: medL=median fwd_10d excess. done")
