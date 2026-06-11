"""l46_rsi_cci.py — (A) L46 + cluster validation (mirror L34); (B) RSI/CCI conditioning:
test the user's thesis — L34/L46(+cluster) are BULLISH when RSI/CCI are LOW (oversold) and
BEARISH (short) when HIGH (overbought). Median forward EXCESS per RSI / CCI bucket, per-year.
ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def _wilson(w,n,z=1.96):
    if n==0: return 0.0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**0.5);return max(0.0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,l_sig,fwd_10d,rsi_14,cci_20,
   load,sq,sig_fri34,sig_best,sig_vol_5x,sig_vol_10x,d_absorb_bull,d_absorb_bear
   FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
     AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND cci_20 IS NOT NULL""").fetchdf(); a.close()
for c in ["load","sq","sig_fri34","sig_best","sig_vol_5x","sig_vol_10x","d_absorb_bull","d_absorb_bear"]:
    df[c]=df[c].fillna(0).astype(np.int8)
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["rbk"]=pd.cut(df.rsi_14,[-1,35,50,65,101],labels=["RSI≤35","35-50","50-65","≥65"])
df["cbk"]=pd.cut(df.cci_20,[-9999,-100,0,100,9999],labels=["CCI≤-100","-100..0","0..100","≥100"])
g=lambda n: df[n]==1

def rep(mask,label):
    sub=df[mask]; n=len(sub)
    if n<80: return f"  {label:30} n={n} <80"
    e=sub.exc; w=int((e>0).sum()); med=float(e.median()); m25=float(e.clip(-25,25).mean())
    posy=ny=0
    for y in range(2021,2027):
        sy=e[sub.yr==y]
        if len(sy)>=15: ny+=1; posy+= float(sy.median())>0
    return f"  {label:30} {n:>6} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {_wilson(w,n)*100:>5.1f} {posy}/{ny}"

hdr=f"  {'pattern':30} {'n':>6} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
print("### (A) L46 + cluster (mirror L34)")
print(hdr)
for lbl,m in [("L46 alone",g(df.l_sig=='L46')&True if False else (df.l_sig=='L46')),
              ("L46 + LOAD",(df.l_sig=='L46')&g("load")),
              ("L46 + squeeze",(df.l_sig=='L46')&g("sq")),
              ("L46 + LOAD + squeeze",(df.l_sig=='L46')&g("load")&g("sq")),
              ("L46 + d_absorb_bull",(df.l_sig=='L46')&g("d_absorb_bull")),
              ("L46 + vol-climax",(df.l_sig=='L46')&(g("sig_vol_5x")|g("sig_vol_10x")))]:
    print(rep(m,lbl))

print("\n### (B) RSI bucket — does LOW=bullish, HIGH=bearish?  (median forward excess)")
for anchor,amask in [("L34",df.l_sig=='L34'),("L46",df.l_sig=='L46'),
                     ("L34+LOAD+sq",(df.l_sig=='L34')&g("load")&g("sq"))]:
    print(f"\n  {anchor}:")
    print(hdr)
    for bk in ["RSI≤35","35-50","50-65","≥65"]:
        print(rep(amask & (df.rbk==bk), f"  {bk}"))

print("\n### (B) CCI bucket — does LOW=bullish, HIGH=bearish?")
for anchor,amask in [("L34",df.l_sig=='L34'),("L46",df.l_sig=='L46'),
                     ("L34+LOAD+sq",(df.l_sig=='L34')&g("load")&g("sq"))]:
    print(f"\n  {anchor}:")
    print(hdr)
    for bk in ["CCI≤-100","-100..0","0..100","≥100"]:
        print(rep(amask & (df.cbk==bk), f"  {bk}"))

print("\n### SHORT thesis: HIGH RSI&CCI + L46/L34 → is forward NEGATIVE (short edge)?")
print(hdr)
hi=(df.rsi_14>=65)&(df.cci_20>=100)
print(rep((df.l_sig=='L46')&hi, "L46 & RSI≥65 & CCI≥100"))
print(rep((df.l_sig=='L34')&hi, "L34 & RSI≥65 & CCI≥100"))
print(rep((df.l_sig.isin(['L34','L46']))&hi&(g("sig_vol_5x")|g("sig_vol_10x")), "L34/46 & hi & vol-climax"))
lo=(df.rsi_14<=35)&(df.cci_20<=-100)
print(rep((df.l_sig=='L34')&lo&g("load"), "L34 & RSI≤35 & CCI≤-100 & LOAD (long)"))
print("\nlegend: medL=median excess (+ bullish / − bearish). +yr=years median>0. done")
