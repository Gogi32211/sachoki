"""midzone_4050_5060.py — detail on the mid RSI zones 40-50 and 50-60 (the flat 'no
man's land' / second-breakout area). Per universe: 4 variants (L34/L46 × green/red), then
what ADDITIONAL signal lifts the zone. NO CCI in the anchor. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
SIG=["sig_blue","sig_fri34","sig_fri64","bo_up","bx_up","vbo_up","eb_bull","load","sq",
     "sig_best","hilo_buy","d_surge_bull","d_strong_bull","sig_t1","sig_t1g","sig_t2g","sig_vol_5x","sig_vol_10x"]
df=a.execute(f"""SELECT universe,date,l_sig,open,close,rsi_14,cci_20,fwd_10d,{','.join(SIG)}
  FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
   AND l_sig IN ('L34','L46') AND rsi_14>=40 AND rsi_14<60 AND rsi_14 IS NOT NULL""").fetchdf();a.close()
for c in SIG: df[c]=df[c].fillna(0).astype(np.int8)
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["green"]=df.close>df.open
df["zone"]=np.where(df.rsi_14<50,"40-50","50-60")
g=lambda n: df[n]==1
def med(s): return float(s.exc.median()) if len(s) else 0

for zone in ("40-50","50-60"):
    dz=df[df.zone==zone]
    print(f"\n{'='*96}\n### RSI {zone}")
    # A) 4 variants x universe
    print("  4 variants (median excess, n):")
    print(f"  {'universe':10} {'L34↑grn':>14} {'L34↓red':>14} {'L46↑grn':>14} {'L46↓red':>14}")
    for uni in ("nasdaq","sp500","russell2k"):
        du=dz[dz.universe==uni]; cells=[]
        for lsig,grn in [("L34",True),("L34",False),("L46",True),("L46",False)]:
            s=du[(du.l_sig==lsig)&(du.green==grn)]
            cells.append(f"{med(s):>+7.2f}({len(s)//1000}k)" if len(s)>=1000 else (f"{med(s):>+7.2f}({len(s)})" if len(s)>=40 else f"{'—':>11}"))
        print(f"  {uni:10} "+" ".join(f"{c:>14}" for c in cells))
    # B) what lifts the zone (pooled all universes/variants)
    base=dz; bmed=med(base)
    print(f"\n  + signal lift (pooled, baseline {bmed:+.2f}, n={len(base):,}):")
    res=[]
    for c in SIG+["green","cci>100"]:
        if c=="green": m=base[base.green]
        elif c=="cci>100": m=base[base.cci_20>100]
        else: m=base[base[c]==1]
        if len(m)<60: continue
        e=m.exc;w=int((e>0).sum())
        py=ny=0
        for y in range(2021,2027):
            sy=e[m.yr==y]
            if len(sy)>=10:ny+=1;py+=float(sy.median())>0
        res.append((c,len(m),w/len(m)*100,float(e.median()),float(e.median())-bmed,wl(w,len(m))*100,py,ny))
    print(f"    {'signal':16} {'n':>6} {'win%':>5} {'medL':>6} {'lift':>6} {'wLB':>5} +yr")
    for c,n,wr,m_,fl,lb,py,ny in sorted(res,key=lambda r:-r[4])[:12]:
        print(f"    {c:16} {n:>6} {wr:>5.1f} {m_:>+6.2f} {fl:>+6.2f} {lb:>5.1f} {py}/{ny}")
print("\nlegend: median fwd_10d excess vs universe. done")
