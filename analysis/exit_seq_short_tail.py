"""exit_seq_short_tail.py — honesty check on the SHORT edge: clip25 hides squeeze
risk (a +500% mover = -500 on a short, clipped to -25). Report UNCLIPPED short mean,
worst single outcome, and how often the short blows past -25%/-50%. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.zone_events import _seq_sql,_leadin_cols
from ai_journal.db import get_analytics_conn

def load(zone_def,vm=5.0,depth=4):
    a=get_analytics_conn();df=a.execute(_seq_sql(vm,depth,_leadin_cols(),zone_def=zone_def)).fetchdf();a.close()
    df=df.drop_duplicates(["ticker","e_date","et"]);df=df[df.ev_seq==1]
    df["fwd_10d"]=pd.to_numeric(df.fwd_10d,errors="coerce")
    df=df[np.isfinite(df.fwd_10d)&df.fwd_10d.between(-90,500)].copy()
    df["s"]=-df.fwd_10d
    return df
def mask(pop,t):
    m=pd.Series(True,index=pop.index)
    for sg,k in t:
        c=f"e{k}_{sg}";m=m&(pop[c]==1) if c in pop.columns else (m&False)
    return m

# top short candidates from the short pass (big-n, broad coverage)
CAND=[
 ("spike25","exit_up","c=O→V×5→… (n179)",[("close_o",2),("sig_vol_5x",1)]),
 ("spike25","exit_up","r2l→V×5→… (n109)",[("r2l_os",2),("sig_vol_5x",1)]),
 ("vb","exit_up","V×5→V×10 (n124)",[("sig_vol_5x",1),("sig_vol_10x",0)]),
 ("vb","exit_up","c=O→V×5→… (n91)",[("close_o",2),("sig_vol_5x",1)]),
 ("spike","exit_up","c=O→V×5 (n68)",[("close_o",2),("sig_vol_5x",1)]),
 ("vb","exit_down","PSAR→Ab→V10 (n32)",[("psar_bull",2),("d_absorb_bull",1)]),
]
print(f"{'panel/seq':40} {'n':>4} {'win%':>5} {'clip25':>7} {'UNCLIP':>7} {'worst':>7} {'>+25%loss':>9} {'>+50%loss':>9}")
for zone,et,name,toks in CAND:
    pop=load(zone);pop=pop[pop.et==et];sub=pop[mask(pop,toks)];s=sub.s;n=len(s)
    if n<20: print(f"{zone+' '+et+' '+name:40} n={n}<20");continue
    win=round((s>0).mean()*100,1)
    c25=round(float(s.clip(-25,25).mean()),2)
    unc=round(float(s.mean()),2)              # unclipped short pnl
    worst=round(float(s.min()),1)             # worst single short (a big squeeze)
    big25=round((s< -25).mean()*100,1)        # % of trades losing >25% on the short
    big50=round((s< -50).mean()*100,1)
    print(f"{zone+' '+et+' '+name:40} {n:>4} {win:>5} {c25:>+7} {unc:>+7} {worst:>+7} {str(big25)+'%':>9} {str(big50)+'%':>9}")
print("\nNB: short pnl=-fwd_10d. 'worst' = biggest single squeeze against the short.")
print("done")
