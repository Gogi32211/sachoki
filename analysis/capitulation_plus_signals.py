"""capitulation_plus_signals.py — anchor on the two golden oversold contexts on L34/L46
bars, then scan EVERY discriminating signal for ADDITIONAL forward lift. Find what stacks
on top of capitulation. ANALYSIS ONLY.
  CAP    = (L34|L46) & RSI<20 & CCI<-100   (deep capitulation)
  OS2035 = (L34|L46) & RSI 20-35           (oversold sweet spot)"""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def _wilson(w,n,z=1.96):
    if n==0: return 0.0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**0.5);return max(0.0,(c-m)/d)
a=get_analytics_conn(read_only=True)
cols=[r[1] for r in a.execute("PRAGMA table_info('bars')").fetchall()]
import re
EXCL={'sig_tz','sig_conso','sig_z','sig_t','tz_bull','sig_l_any','pb_macro_penalty','sig_tz2','sig_tz3',
      'sig_not_ext','sig_l1','sig_l2','sig_l3','sig_l4','sig_l5','sig_l6'}
EXCL_PREF=('hit_','drop_','price_','next_pivot','is_pivot','sig_fly','sig_any','rsi_')
CAND=[c for c in cols if (c.startswith(('sig_','d_','wyc_','pb_','seq_'))
      or c in ('bo_up','bx_up','vbo_up','eb_bull','fbo_bull','be_up','load','sq','svs','ns','nd','sc',
               'hilo_buy','rocket','va','bf_buy','best_sig','strong_sig','abs_sig','climb_sig','ad_fresh','ad_cluster'))
      and c not in EXCL and not any(c.startswith(p) for p in EXCL_PREF)]
tot=a.execute("SELECT count(*) FROM bars WHERE fwd_10d IS NOT NULL").fetchone()[0]
SIGS=[]
for c in CAND:
    try:
        n=a.execute(f"SELECT count(*) FILTER(WHERE {c}=1) FROM bars WHERE fwd_10d IS NOT NULL").fetchone()[0]
        if 3000<=n<=int(0.25*tot): SIGS.append(c)
    except: pass
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
sel=",".join(SIGS)
df=a.execute(f"""SELECT universe,date,l_sig,fwd_10d,rsi_14,cci_20,{sel} FROM bars
   WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
     AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND cci_20 IS NOT NULL""").fetchdf(); a.close()
for c in SIGS: df[c]=df[c].fillna(0)
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year

def scan(anchor_mask, label):
    base=df[anchor_mask]; bn=len(base); bmed=float(base.exc.median())
    print(f"\n{'='*104}\n### {label}   (n={bn:,}, baseline median-excess {bmed:+.2f}, win {(base.exc>0).mean()*100:.1f}%)")
    print(f"  {'+ signal':22} {'n':>5} {'win%':>5} {'medL':>6} {'fwdLift':>7} {'bigUp%':>6} {'wLB':>5} +yr")
    res=[]
    for c in SIGS:
        sub=base[base[c]==1]; n=len(sub)
        if n<60: continue
        e=sub.exc; w=int((e>0).sum()); med=float(e.median())
        posy=ny=0
        for y in range(2021,2027):
            sy=e[sub.yr==y]
            if len(sy)>=10: ny+=1; posy+= float(sy.median())>0
        res.append((c,n,w/n*100,med,med-bmed,float((sub.fwd_10d>15).mean()*100),_wilson(w,n)*100,posy,ny))
    for c,n,wr,med,fl,bu,lb,py,ny in sorted(res,key=lambda r:-r[4])[:16]:
        print(f"  {c:22} {n:>5} {wr:>5.1f} {med:>+6.2f} {fl:>+7.2f} {bu:>6.1f} {lb:>5.1f} {py}/{ny}")

L=df.l_sig.isin(['L34','L46'])
scan(L & (df.rsi_14<20) & (df.cci_20<-100), "CAP = L34/46 & RSI<20 & CCI<-100")
scan(L & (df.rsi_14>=20) & (df.rsi_14<=35), "OS2035 = L34/46 & RSI 20-35")
print("\nlegend: fwdLift = median-excess(anchor & signal) − anchor baseline. bigUp%=P(fwd_10d>+15%). done")
