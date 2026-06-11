"""l_rsi_extremes.py — finer RSI buckets (<20, 20-35, 35-65, 65-80, >80) for L34/L46
(+cluster). Does deep-oversold (<20) amplify the bull edge, and does deep-overbought
(>80) finally flip BEARISH (short)? + RSI×CCI extremes. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def _wilson(w,n,z=1.96):
    if n==0: return 0.0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**0.5);return max(0.0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT universe,date,l_sig,fwd_10d,rsi_14,cci_20,load,sq,sig_vol_5x,sig_vol_10x
   FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
     AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND cci_20 IS NOT NULL""").fetchdf(); a.close()
for c in ["load","sq","sig_vol_5x","sig_vol_10x"]: df[c]=df[c].fillna(0).astype(np.int8)
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["rbk"]=pd.cut(df.rsi_14,[-1,20,35,65,80,101],labels=["<20","20-35","35-65","65-80",">80"])
g=lambda n: df[n]==1

def rep(mask,label):
    sub=df[mask]; n=len(sub)
    if n<60: return f"  {label:18} n={n} <60"
    e=sub.exc; w=int((e>0).sum()); med=float(e.median()); m25=float(e.clip(-25,25).mean())
    posy=ny=0
    for y in range(2021,2027):
        sy=e[sub.yr==y]
        if len(sy)>=12: ny+=1; posy+= float(sy.median())>0
    return f"  {label:18} {n:>6} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {_wilson(w,n)*100:>5.1f} {posy}/{ny}"

hdr=f"  {'RSI bucket':18} {'n':>6} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
for anchor,amask in [("L34",df.l_sig=='L34'),("L46",df.l_sig=='L46'),
                     ("L34+LOAD+sq",(df.l_sig=='L34')&g("load")&g("sq")),
                     ("L46+LOAD+sq",(df.l_sig=='L46')&g("load")&g("sq"))]:
    print(f"\n### {anchor} by RSI extreme")
    print(hdr)
    for bk in ["<20","20-35","35-65","65-80",">80"]:
        print(rep(amask & (df.rbk==bk), bk))

print("\n### deep extremes — long (RSI<20) vs short test (RSI>80)")
print(hdr)
print(rep((df.l_sig.isin(['L34','L46']))&(df.rsi_14<20), "L34/46 RSI<20"))
print(rep((df.l_sig.isin(['L34','L46']))&(df.rsi_14<20)&(df.cci_20<-100), "L34/46 RSI<20 CCI<-100"))
print(rep((df.l_sig.isin(['L34','L46']))&(df.rsi_14>80), "L34/46 RSI>80"))
print(rep((df.l_sig.isin(['L34','L46']))&(df.rsi_14>80)&(df.cci_20>100), "L34/46 RSI>80 CCI>100"))
print(rep((df.l_sig.isin(['L34','L46']))&(df.rsi_14>80)&(g("sig_vol_5x")|g("sig_vol_10x")), "L34/46 RSI>80 vol-climax"))
print("\nlegend: medL = median forward excess (+ bull / − bear). >80 negative ⇒ short thesis holds. done")
