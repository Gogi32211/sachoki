"""markup_setup.py — build the MARKUP (momentum/continuation) setup from scratch, the
RGTI-Oct-2024 type: L34/L46 in the uptrend zone (RSI 40-70, NOT oversold). 4 variants
(green/red) × RSI sub-zones, optimal volume, coil/breakout add-ons, then deep-validate +
candle profile (bar-driven vs gap chase). ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
SIG=["sig_blue","sig_fri34","bo_up","bx_up","vbo_up","eb_bull","sq","sig_best","load",
     "sig_vol_5x","sig_vol_10x","d_surge_bull","d_strong_bull","hilo_buy","sig_t1","sig_t1g","sig_t2g"]
df=a.execute(f"""SELECT ticker,universe,date,l_sig,open,close,rsi_14,volume,avg_vol_20d,
   fwd_10d,fwd_20d,mfe_10d,mae_10d,lag(close) OVER (PARTITION BY ticker,universe ORDER BY date) pclose,
   {','.join(SIG)}
  FROM bars WHERE l_sig IN ('L34','L46') AND rsi_14>=40 AND rsi_14<70 AND avg_vol_20d>0
    AND fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500""").fetchdf();a.close()
for c in SIG: df[c]=df[c].fillna(0).astype(np.int8)
df=df[df.pclose.notna()&(df.pclose>0)&(df.open>0)].copy()
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["green"]=df.close>df.open; df["vr"]=df.volume/df.avg_vol_20d
df["bar"]=(df.close-df.open)/df.open*100; df["gap"]=(df.open-df.pclose)/df.pclose*100
g=lambda n: df[n]==1
def rep(m,lab,path=False):
    s=df[m];n=len(s)
    if n<60: return f"  {lab:30} n={n}"
    e=s.exc;w=int((e>0).sum());med=float(e.median());m25=float(e.clip(-25,25).mean())
    py=ny=0
    for y in range(2021,2027):
        sy=e[s.yr==y]
        if len(sy)>=10:ny+=1;py+=float(sy.median())>0
    out=f"  {lab:30} {n:>6} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {wl(w,n)*100:>5.1f} {py}/{ny}"
    if path:
        isv=float(e[~(s.date.astype(str)>=OOS)].median());oo=float(e[s.date.astype(str)>=OOS].median())
        out+=f"\n      └ bar{float(s.bar.median()):+.1f}/gap{float(s.gap.median()):+.1f} MFE{float(s.mfe_10d.median()):+.1f}/MAE{float(s.mae_10d.median()):.1f} IS{isv:+.1f}/OOS{oo:+.1f} tk:{s.ticker.nunique()}"
    return out
hdr=f"  {'setup':30} {'n':>6} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
print("### A) 4 variants in markup zone (RSI 40-70)")
print(hdr)
for lab,ls,gr in [("L34↑GREEN","L34",True),("L34↓red","L34",False),("L46↑GREEN","L46",True),("L46↓red","L46",False)]:
    print(rep((df.l_sig==ls)&(df.green==gr),lab))
print("\n### B) L34↑GREEN by volume band (markup wants more than capit?)")
print(hdr)
for vl,lo,hi in [("0.8-1.2",.8,1.2),("1.2-1.8",1.2,1.8),("1.8-2.5",1.8,2.5),("2.5-4",2.5,4),("4-7",4,7),("7-15",7,15)]:
    print(rep(g("sig_t1g")&False|((df.l_sig=='L34')&df.green&(df.vr>=lo)&(df.vr<hi)),f"  vol {vl}"))
print("\n### C) what STACKS on L34↑GREEN markup (RSI 40-70)")
base=df[(df.l_sig=='L34')&df.green]; bmed=float(base.exc.median())
print(f"  baseline {bmed:+.2f}, n={len(base):,}")
res=[]
for c in SIG+["bardriven"]:
    sub=base[base.bar.abs()>base.gap.abs()] if c=="bardriven" else base[base[c]==1]
    if len(sub)<80: continue
    e=sub.exc;py=sum(1 for y in range(2021,2027) for sy in [e[sub.yr==y]] if len(sy)>=10 and float(sy.median())>0)
    res.append((c,len(sub),(e>0).mean()*100,float(e.median()),float(e.median())-bmed,py))
for c,n,wr,m_,fl,py in sorted(res,key=lambda r:-r[4])[:12]:
    print(f"    {c:16} {n:>6} {wr:>5.1f} {m_:>+6.2f} lift{fl:>+6.2f} {py}/6")
print("\n### D) deep — best markup composites")
print(hdr)
print(rep((df.l_sig=='L34')&df.green&(g("sig_vol_10x")|g("sig_vol_5x")), "L34grn + vol-climax(5/10x)", path=True))
print(rep((df.l_sig=='L34')&df.green&(g("sig_blue")|g("sig_fri34")), "L34grn + coil(BLUE/FRI34)", path=True))
print(rep((df.l_sig=='L34')&df.green&(g("bo_up")|g("bx_up")|g("vbo_up")), "L34grn + breakout(BO/BX/VBO)", path=True))
print(rep((df.l_sig=='L34')&df.green&(df.bar.abs()>df.gap.abs())&(g("sig_vol_5x")|g("sig_vol_10x")), "L34grn + bar-driven + vol", path=True))
print("\ndone")
