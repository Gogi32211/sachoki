"""momentum_zone_dense.py — the MOMENTUM/markup counterpart of capitulation Zone-Dense.
Capitulation = dense + price FELL -15..-40% + RSI<30 (+1.39). Momentum = dense L34/L46 churn
+ price RISING/holding + RSI mid-zone (40-65) — the RGTI-Oct-2024 markup. Test price-change
bands, compression(squeeze/load), volume, early-vs-late RSI. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,l_sig,close,rsi_14,fwd_10d,sq,load,sig_best,sig_vol_5x,sig_vol_10x,vbo_up
  FROM bars WHERE fwd_10d IS NOT NULL ORDER BY ticker,universe,date""").fetchdf();a.close()
for c in ["sq","load","sig_best","sig_vol_5x","sig_vol_10x","vbo_up"]: df[c]=df[c].fillna(0).astype(np.int8)
df["isL"]=df.l_sig.isin(['L34','L46']).astype(np.int8)
gb=df.groupby(["ticker","universe"],sort=False)
df["d10"]=gb["isL"].transform(lambda s:s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df["c10"]=gb["close"].transform(lambda s:s.shift(10))
df["r5"]=gb["rsi_14"].transform(lambda s:s.shift(5))            # rsi 5 bars ago (rising?)
df=df[df.fwd_10d.between(-90,500)&df.c10.notna()&(df.c10>0)].copy()
df["chg10"]=(df.close/df.c10-1)*100
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
g=lambda n: df[n]==1
def rep(m,lab):
    s=df[m];n=len(s)
    if n<200:return f"  {lab:36} n={n}"
    e=s.exc;w=int((e>0).sum())
    py=sum(1 for y in range(2021,2027) for sy in [e[s.yr==y]] if len(sy)>=20 and float(sy.median())>0)
    ny=sum(1 for y in range(2021,2027) if len(e[s.yr==y])>=20)
    return f"  {lab:36} {n:>7} {w/n*100:>5.1f} {float(e.median()):>+6.2f} {float(e.clip(-25,25).mean()):>+6.2f} {wl(w,n)*100:>5.1f} {py}/{ny}"
hdr=f"  {'pattern':36} {'n':>7} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
MID=(df.isL==1)&(df.rsi_14>=40)&(df.rsi_14<65)&(df.d10>=6)   # dense churn, mid-RSI momentum zone
print("### dense(>=6) in MID RSI (40-65) × price behaviour over window")
print(hdr)
print(rep(MID, "dense mid-RSI (any price)"))
print(rep(MID&(df.chg10>=20), "  + price ROSE >+20% (strong mom)"))
print(rep(MID&(df.chg10.between(0,20)), "  + price rose 0..+20% (markup)"))
print(rep(MID&(df.chg10.between(-15,0)), "  + price flat (-15..0)"))
print(rep(MID&(df.chg10< -15), "  + price fell <-15% (pullback-in-uptrend)"))
print(rep(MID&(df.rsi_14>df.r5), "  + RSI rising (vs 5 bars ago)"))
print("\n### + compression / volume (the markup add-ons)")
print(hdr)
print(rep(MID&g("sq")&g("load"), "  dense mid + squeeze + load"))
print(rep(MID&g("sq"), "  dense mid + squeeze"))
print(rep(MID&g("load"), "  dense mid + load"))
print(rep(MID&(g("sig_vol_5x")|g("sig_vol_10x")), "  dense mid + vol-climax"))
print(rep(MID&g("vbo_up"), "  dense mid + vbo_up"))
print(rep(MID&g("sig_best"), "  dense mid + best"))
print(rep(MID&g("sq")&g("load")&(df.chg10>=0), "  dense mid + sq+load + price>=flat"))
print("\n### early vs late momentum (RSI sub-band, dense + squeeze+load)")
print(hdr)
SL=MID&g("sq")&g("load")
print(rep(SL&(df.rsi_14<50), "  sq+load · RSI 40-50 (early)"))
print(rep(SL&(df.rsi_14>=50)&(df.rsi_14<60), "  sq+load · RSI 50-60"))
print(rep(SL&(df.rsi_14>=60), "  sq+load · RSI 60-65 (late)"))
print("\nlegend: medL=median fwd_10d excess. done")
