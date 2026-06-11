"""cap_greencandle.py — REDO from scratch with the missing condition: the L34/L46 bar
must CLOSE GREEN (close>open). NB: FRI64 is a RED-candle coil → excluded by close>open;
the green coil is BLUE / FRI34. Re-test baseline, capitulation, co-occurrence, deep.
ANALYSIS ONLY."""
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
EP=('hit_','drop_','price_','next_pivot','is_pivot','sig_fly','sig_any','rsi_')
CAND=[c for c in cols if (c.startswith(('sig_','d_','wyc_','pb_','seq_'))
      or c in ('bo_up','bx_up','vbo_up','eb_bull','fbo_bull','be_up','load','sq','svs','ns','nd','sc',
               'hilo_buy','rocket','va','bf_buy','best_sig','strong_sig','abs_sig','climb_sig','ad_fresh','ad_cluster'))
      and c not in EXCL and not any(c.startswith(p) for p in EP)]
tot=a.execute("SELECT count(*) FROM bars WHERE fwd_10d IS NOT NULL").fetchone()[0]
SIGS=[]
for c in CAND:
    try:
        n=a.execute(f"SELECT count(*) FILTER(WHERE {c}=1) FROM bars WHERE fwd_10d IS NOT NULL").fetchone()[0]
        if 3000<=n<=int(0.25*tot): SIGS.append(c)
    except: pass
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
sel=",".join(SIGS)
df=a.execute(f"""SELECT ticker,universe,date,l_sig,open,close,rsi_14,cci_20,fwd_10d,fwd_20d,mfe_10d,mae_10d,{sel}
   FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
     AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND cci_20 IS NOT NULL""").fetchdf(); a.close()
for c in SIGS: df[c]=df[c].fillna(0)
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["GO"]=(df.close>df.open)
g=lambda n: df[n]==1

def st(mask,label,path=False):
    sub=df[mask]; n=len(sub)
    if n<60: return f"  {label:30} n={n} <60"
    e=sub.exc; w=int((e>0).sum()); med=float(e.median()); m25=float(e.clip(-25,25).mean())
    posy=ny=0; yc=[]
    for y in range(2021,2027):
        sy=e[sub.yr==y]
        if len(sy)>=10: ny+=1; posy+=float(sy.median())>0; yc.append(f"{y%100}:{float(sy.median()):+.1f}")
    s=f"  {label:30} {n:>6} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {_wilson(w,n)*100:>5.1f} {posy}/{ny}"
    if path:
        mfe=float(sub.mfe_10d.median()); mae=float(sub.mae_10d.median()); nt=sub.ticker.nunique()
        isv=float(e[~(sub.date.astype(str)>=OOS)].median()); oo=float(e[sub.date.astype(str)>=OOS].median())
        s+=f"\n      └ clip25 above ⇒ not tail · MFE{mfe:+.1f}/MAE{mae:.1f} · IS{isv:+.1f}/OOS{oo:+.1f} · tickers:{nt} · yr["+" ".join(yc)+"]"
    return s

hdr=f"  {'pattern':30} {'n':>6} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
print("### A) does close>open (GREEN) improve the L34/L46 anchor?")
print(hdr)
print(st(df.l_sig.isin(['L34','L46']), "L34/L46 (all)"))
print(st(df.l_sig.isin(['L34','L46'])&df.GO, "L34/L46 & GREEN (c>o)"))
print(st(df.l_sig.isin(['L34','L46'])&~df.GO, "L34/L46 & RED (c<=o)"))
CAP=df.l_sig.isin(['L34','L46'])&(df.rsi_14<20)&(df.cci_20<-100)
print(st(CAP, "CAP (RSI<20 CCI<-100)"))
print(st(CAP&df.GO, "CAP & GREEN"))
print(st(CAP&~df.GO, "CAP & RED"))

print("\n### B) co-occurrence on CAP & GREEN — what coil/signal adds now? (FRI64 is RED, excluded)")
base=df[CAP&df.GO]; bmed=float(base.exc.median()); bn=len(base)
print(f"  anchor CAP&GREEN: n={bn:,}, baseline med {bmed:+.2f}")
print(f"  {'+ signal':22} {'n':>5} {'win%':>5} {'medL':>6} {'fwdLift':>7} +yr")
res=[]
for c in SIGS:
    sub=base[base[c]==1]; n=len(sub)
    if n<60: continue
    e=sub.exc; med=float(e.median())
    posy=ny=0
    for y in range(2021,2027):
        sy=e[sub.yr==y]
        if len(sy)>=8: ny+=1; posy+=float(sy.median())>0
    res.append((c,n,(e>0).mean()*100,med,med-bmed,posy,ny))
for c,n,wr,med,fl,py,ny in sorted(res,key=lambda r:-r[4])[:14]:
    print(f"  {c:22} {n:>5} {wr:>5.1f} {med:>+6.2f} {fl:>+7.2f} {py}/{ny}")

print("\n### C) deep: corrected composite CAP & GREEN & (BLUE|FRI34) vs old (FRI64)")
print(hdr)
print(st(CAP&df.GO&(g("sig_blue")), "CAP&GREEN + BLUE", path=True))
print(st(CAP&df.GO&(g("sig_fri34")), "CAP&GREEN + FRI34", path=True))
print(st(CAP&df.GO&(g("sig_blue")|g("sig_fri34")), "CAP&GREEN + (BLUE|FRI34)", path=True))
print(st(CAP&(g("sig_fri64")), "[old] CAP + FRI64 (mostly RED)", path=True))
print("\nlegend: medL=median forward excess. GO=close>open. done")
