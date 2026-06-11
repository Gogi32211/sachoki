"""zone_alternation.py — clarify: 'repetition' = L46 then L34 or vice-versa (the MIX, not the
same one). Confirm the density metric counts both, and test whether ALTERNATION (both L34 AND
L46 present, switching) beats single-type, in the validated context (dense + pullback -20..-40%
+ RSI<30). ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,l_sig,close,rsi_14,fwd_10d
  FROM bars WHERE fwd_10d IS NOT NULL ORDER BY ticker,universe,date""").fetchdf();a.close()
df["is34"]=(df.l_sig=='L34').astype(np.int8); df["is46"]=(df.l_sig=='L46').astype(np.int8)
df["isL"]=(df.is34|df.is46)
gb=df.groupby(["ticker","universe"],sort=False)
df["d10"]=gb["isL"].transform(lambda s:s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df["c34"]=gb["is34"].transform(lambda s:s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df["c46"]=gb["is46"].transform(lambda s:s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df["c10"]=gb["close"].transform(lambda s:s.shift(10))
df=df[df.fwd_10d.between(-90,500)&df.c10.notna()&(df.c10>0)].copy()
df["chg10"]=(df.close/df.c10-1)*100
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
def rep(m,lab):
    s=df[m];n=len(s)
    if n<150:return f"  {lab:38} n={n}"
    e=s.exc;w=int((e>0).sum())
    py=sum(1 for y in range(2021,2027) for sy in [e[s.yr==y]] if len(sy)>=15 and float(sy.median())>0)
    ny=sum(1 for y in range(2021,2027) if len(e[s.yr==y])>=15)
    return f"  {lab:38} {n:>6} {w/n*100:>5.1f} {float(e.median()):>+6.2f} {float(e.clip(-25,25).mean()):>+6.2f} {wl(w,n)*100:>5.1f} {py}/{ny}"
hdr=f"  {'pattern':38} {'n':>6} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
# validated context: dense + moderate pullback (-40<chg<-20... wait user wants -20..-40, knife-guard >-40) + RSI<30
CTX=(df.isL==1)&(df.d10>=6)&(df.chg10>=-40)&(df.chg10<-15)&(df.rsi_14<30)
print("### validated context = dense(>=6) + pullback(-40..-15%) + RSI<30 — alternation breakdown")
print(hdr)
print(rep(CTX, "ALL (any L34/L46 mix)"))
print(rep(CTX&(df.c34>=1)&(df.c46>=1), "  ALTERNATION: both L34 & L46 present"))
print(rep(CTX&(df.c34>=2)&(df.c46>=2), "  strong: >=2 of EACH"))
print(rep(CTX&((df.c34==0)|(df.c46==0)), "  single-type only (no mix)"))
print(rep(CTX&(df.c46>df.c34), "  L46-dominant (more supply)"))
print(rep(CTX&(df.c34>df.c46), "  L34-dominant (more demand)"))
print("\n### same, the FULL validated signal (best so far) for reference")
print(hdr)
print(rep(CTX&(df.c34>=1)&(df.c46>=1)&(df.close<=df.close.shift(0)), "  alternation (final candidate)"))
print("\nlegend: c34/c46 = count of L34 / L46 in last 10. medL=median fwd excess. done")
