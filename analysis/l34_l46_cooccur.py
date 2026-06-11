"""l34_l46_cooccur.py — START FRESH: which signals co-occur with L34 / L46 and
PREDICT a forward move (the pre-breakout pattern)? Anchor on L34/L46 bars, rank every
discriminating co-signal by forward-return LIFT over the L34/L46 baseline + P(big move).
SMX hint: LOAD/FRI34/ABS/d_flip/squeeze/volume/R2L cluster. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
a=get_analytics_conn(read_only=True)
cols=[r[1] for r in a.execute("PRAGMA table_info('bars')").fetchall()]
# curated discriminating signals (exclude ubiquitous / redundant)
import re
EXCL={'sig_tz','sig_conso','sig_z','sig_t','tz_bull','sig_l_any','pb_macro_penalty',
      'sig_tz2','sig_tz3','sig_not_ext','sig_l1','sig_l2','sig_l3','sig_l4','sig_l5','sig_l6'}
EXCL_PREF=('hit_','drop_','price_','next_pivot','is_pivot','sig_fly','sig_any','rsi_')
CAND=[c for c in cols if (c.startswith(('sig_','d_','wyc_','pb_','seq_'))
      or c in ('bo_up','bx_up','vbo_up','eb_bull','fbo_bull','be_up','load','sq','svs','ns','nd','sc',
               'hilo_buy','rocket','va','bf_buy','best_sig','strong_sig','abs_sig','climb_sig',
               'ad_fresh','ad_cluster'))
      and c not in EXCL and not any(c.startswith(p) for p in EXCL_PREF)]
# base rates, keep discriminating range
tot=a.execute("SELECT count(*) FROM bars WHERE fwd_10d IS NOT NULL").fetchone()[0]
BASE={}
for c in CAND:
    try:
        n=a.execute(f"SELECT count(*) FILTER(WHERE {c}=1) FROM bars WHERE fwd_10d IS NOT NULL").fetchone()[0]
        if 3000<=n<=int(0.22*tot): BASE[c]=n/tot
    except: pass
SIGS=list(BASE);
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
sel=",".join(SIGS)
df=a.execute(f"""SELECT universe,date,l_sig,fwd_10d,{sel} FROM bars
   WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
     AND l_sig IN ('L34','L46')""").fetchdf(); a.close()
for c in SIGS: df[c]=df[c].fillna(0)
df["exc"]=df.fwd_10d-df.universe.map(BMED)
df["yr"]=pd.to_datetime(df.date).dt.year

def scan(anchor, label):
    sub=df[df.l_sig==anchor]; base_med=float(sub.exc.median()); n0=len(sub)
    print(f"\n{'='*100}\n### anchor = {anchor}  (n={n0:,}, baseline median-excess {base_med:+.2f})")
    print(f"  {'co-signal':22} {'n':>6} {'co-freq':>7} {'freqLift':>8} {'fwdLift':>7} {'bigUp%':>6} {'med(L+X)':>8}")
    res=[]
    for c in SIGS:
        m=sub[c]==1; n=int(m.sum())
        if n<150: continue
        cofreq=n/n0; freqlift=cofreq-BASE[c]
        med=float(sub[m].exc.median()); fwdlift=med-base_med
        bigup=float((sub[m].fwd_10d>15).mean()*100)
        res.append((c,n,cofreq,freqlift,fwdlift,bigup,med))
    # rank by forward lift (predictive)
    for c,n,cf,fl,fwl,bu,med in sorted(res,key=lambda r:-r[4])[:18]:
        print(f"  {c:22} {n:>6} {cf*100:>6.1f}% {fl*100:>+7.1f} {fwl:>+7.2f} {bu:>6.1f} {med:>+8.2f}")
    return res

scan("L34","L34")
scan("L46","L46")
print("\nlegend: freqLift = co-occur freq − global base rate (what L34/L46 attracts).")
print("fwdLift = median-excess of (anchor & co-signal) − anchor baseline (does the co-signal ADD a move).")
print("bigUp% = P(fwd_10d > +15%). done")
