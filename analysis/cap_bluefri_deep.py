"""cap_bluefri_deep.py — DEEP test of the top L-line composite:
(L34|L46) & RSI<20 & CCI<-100 & (BLUE|FRI64). Honest checks: clip25 vs median (tail),
MFE/MAE path, per-ticker concentration (a few pump-names?), tail (>+25 / <-25 / worst),
fwd_20d hold, IS/OOS, per-year, universe split. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def _wilson(w,n,z=1.96):
    if n==0: return 0.0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**0.5);return max(0.0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,l_sig,rsi_14,cci_20,sig_blue,sig_fri64,d_absorb_bear,
   fwd_10d,fwd_20d,mfe_10d,mae_10d
   FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
     AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND cci_20 IS NOT NULL
     AND rsi_14<20 AND cci_20<-100""").fetchdf(); a.close()   # CAP base
for c in ["sig_blue","sig_fri64","d_absorb_bear"]: df[c]=df[c].fillna(0).astype(np.int8)
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
g=lambda n: df[n]==1

def deep(mask,label):
    sub=df[mask]; n=len(sub)
    if n<50: return f"  {label:26} n={n} <50"
    e=sub.exc; w=int((e>0).sum())
    med=float(e.median()); m25=float(e.clip(-25,25).mean())
    med20=float((sub.fwd_20d-sub.universe.map(BMED)).median())
    mfe=float(sub.mfe_10d.median()); mae=float(sub.mae_10d.median())
    big=(sub.fwd_10d>25).mean()*100; dn=(sub.fwd_10d<-25).mean()*100; worst=float(sub.fwd_10d.min())
    nt=sub.ticker.nunique(); top=sub.ticker.value_counts(); topshare=top.iloc[0]/n*100; top5=top.iloc[:5].sum()/n*100
    isv=float(e[~(sub.date.astype(str)>=OOS)].median()); oo=float(e[sub.date.astype(str)>=OOS].median())
    posy=ny=0; yc=[]
    for y in range(2021,2027):
        sy=e[sub.yr==y]
        if len(sy)>=10: ny+=1; posy+= float(sy.median())>0; yc.append(f"{y%100}:{float(sy.median()):+.1f}")
        else: yc.append(f"{y%100}:–")
    print(f"  {label:26} {n:>5} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {med20:>+6.2f} {_wilson(w,n)*100:>5.1f} {isv:>+5.1f}/{oo:>+5.1f} {posy}/{ny}")
    print(f"      └ path MFE{mfe:+.1f}/MAE{mae:.1f} · >+25%:{big:.1f}% <-25%:{dn:.1f}% worst{worst:.0f}% · tickers:{nt} top1:{topshare:.1f}% top5:{top5:.1f}%  yr["+" ".join(yc)+"]")

print(f"CAP base population: n={len(df):,}, tickers={df.ticker.nunique():,}")
print(f"\n  {'composite':26} {'n':>5} {'win%':>5} {'medL':>6} {'m25L':>6} {'f20L':>6} {'wLB':>5} {'IS/OOS':>11} +yr")
deep(pd.Series(True,index=df.index), "CAP base (RSI<20 CCI<-100)")
deep(g("sig_blue"), "CAP + BLUE")
deep(g("sig_fri64"), "CAP + FRI64")
deep(g("sig_blue")|g("sig_fri64"), "CAP + (BLUE|FRI64)")
deep(g("d_absorb_bear"), "CAP + d_absorb_bear")
deep((g("sig_blue")|g("sig_fri64"))&g("d_absorb_bear"), "CAP + coil + absorb")
print("\n### universe split — CAP + (BLUE|FRI64)")
for u in ("sp500","nasdaq","russell2k"):
    deep((g("sig_blue")|g("sig_fri64"))&(df.universe==u), f"  {u}")
print("\nlegend: medL/f20L=median 10d/20d excess. m25L=clip25-mean (tail check). top1/5=ticker concentration. done")
