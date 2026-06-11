"""zone_dense_hold.py — the user's refined thesis: dense L34/L46 repetition (accumulation
churn) + price HOLDS (doesn't keep falling) = real accumulation → buy. dense + price FALLING
= falling knife (SMX). Test density × price-hold. Does 'held' beat 'falling' and fix the knife?
ANALYSIS ONLY."""
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
df["isL"]=df.l_sig.isin(['L34','L46']).astype(np.int8)
gb=df.groupby(["ticker","universe"],sort=False)
df["d10"]=gb["isL"].transform(lambda s:s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df["c10"]=gb["close"].transform(lambda s:s.shift(10))           # close 10 bars ago
df["lo10"]=gb["close"].transform(lambda s:s.shift(1).rolling(10,min_periods=1).min())  # trailing low
df=df[df.fwd_10d.between(-90,500) & df.c10.notna() & (df.c10>0)].copy()
df["chg10"]=(df.close/df.c10-1)*100                              # price change over the window
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
def rep(m,lab):
    s=df[m];n=len(s)
    if n<300:return f"  {lab:36} n={n}"
    e=s.exc;w=int((e>0).sum())
    py=sum(1 for y in range(2021,2027) for sy in [e[s.yr==y]] if len(sy)>=20 and float(sy.median())>0)
    ny=sum(1 for y in range(2021,2027) if len(e[s.yr==y])>=20)
    return f"  {lab:36} {n:>7} {w/n*100:>5.1f} {float(e.median()):>+6.2f} {float(e.clip(-25,25).mean()):>+6.2f} {wl(w,n)*100:>5.1f} {py}/{ny}"
hdr=f"  {'pattern':36} {'n':>7} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
DENSE=(df.isL==1)&(df.d10>=6)
print("### dense L34/L46 (d10>=6) × price behaviour over the 10-bar window")
print(hdr)
print(rep(DENSE, "dense (any price)"))
print(rep(DENSE&(df.chg10>=-3), "dense + HELD (chg10 >= -3%)"))
print(rep(DENSE&(df.chg10>=0), "dense + flat/up (chg10 >= 0)"))
print(rep(DENSE&(df.chg10.between(-15,-3)), "dense + mild pullback (-15..-3%)"))
print(rep(DENSE&(df.chg10< -20), "dense + FALLING (chg10 < -20%) = knife"))
print(rep(DENSE&(df.chg10< -40), "dense + CRASHING (chg10 < -40%)"))
print(rep(DENSE&(df.close>df.lo10), "dense + close > trailing-low (holds floor)"))
print("\n### dense + held, by RSI")
print(hdr)
held=DENSE&(df.chg10>=-3)
print(rep(held&(df.rsi_14<35), "  held · oversold RSI<35"))
print(rep(held&(df.rsi_14>=35)&(df.rsi_14<55), "  held · RSI 35-55"))
print(rep(held&(df.rsi_14>=55), "  held · RSI>=55"))
print("\n### the knife contrast (what SMX was): dense + crashing by RSI")
print(hdr)
print(rep(DENSE&(df.chg10< -20)&(df.rsi_14<30), "  dense + crashing + RSI<30 (SMX-type)"))
print("\nlegend: medL=median fwd_10d excess. chg10=price % change over 10 bars. done")
