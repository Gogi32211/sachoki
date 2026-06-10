"""exit_seq_short.py — test the SHORT side. The panel's exit_down 'win' = price UP
(a spring LONG). Here we measure the ACTUAL short edge: short P&L = -fwd_10d.
Question: are robust SHORT setups easier to find than longs? Base + best sequences,
clip25-mean lift + per-year. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.zone_events import _seq_sql, _leadin_cols, exit_sequences
from ai_journal.db import get_analytics_conn
OOS_FROM = "2024-09-01"
ZONES=[("spike",5.0,"spike>=5x"),("spike25",5.0,"spike 2-5x"),("vb",5.0,"VB class")]

def load(zone_def, vol_min=5.0, depth=4):
    a=get_analytics_conn()
    df=a.execute(_seq_sql(vol_min,depth,_leadin_cols(),zone_def=zone_def)).fetchdf(); a.close()
    df=df.drop_duplicates(["ticker","e_date","et"]); df=df[df.ev_seq==1]
    df["fwd_10d"]=pd.to_numeric(df.fwd_10d,errors="coerce")
    df=df[np.isfinite(df.fwd_10d)&df.fwd_10d.between(-90,500)].copy()
    df["s"]=-df.fwd_10d                     # SHORT pnl
    df["yr"]=pd.to_datetime(df.e_date).dt.year
    return df

def toks(c):
    o=[]
    for p in (c.get("a"),c.get("b"),c.get("c")):
        if p and "@-" in p:
            sg,off=p.split("@-"); o.append((sg,int(off)))
    return o
def mask(pop,t):
    m=pd.Series(True,index=pop.index)
    for sg,k in t:
        cc=f"e{k}_{sg}"; m=m&(pop[cc]==1) if cc in pop.columns else (m&False)
    return m

for zone,vm,zl in ZONES:
    popall=load(zone,vm)
    for et in ["exit_up","exit_down"]:
        pop=popall[popall.et==et]
        if len(pop)<50: continue
        # SHORT base
        b_med=float(pop.s.median()); b_m25=float(pop.s.clip(-25,25).mean())
        bwin=round((pop.s>0).mean()*100,1)
        # mine on the SHORT objective by flipping: exit_sequences ranks by long edge,
        # so to find short edges we just scan the same combos and eval short pnl.
        res=exit_sequences(event_type=et,depth=4,horizon=10,vol_min=vm,min_n=30,
                           top=60,ways=3,first_only=True,zone_def=zone)
        rows=[]
        for c in res.get("best",[])+res.get("worst",[]):
            sub=pop[mask(pop,toks(c))]; s=sub.s; n=len(s)
            if n<30: continue
            m25=float(s.clip(-25,25).mean())
            yrs={y:(round((sub[sub.yr==y].s>0).mean()*100) if (sub.yr==y).sum()>=6 else None) for y in range(2021,2027)}
            posyr=sum(1 for v in yrs.values() if v is not None and v>=50)
            nyr=sum(1 for v in yrs.values() if v is not None)
            rows.append((c.get("sequence"),n,round((s>0).mean()*100,1),
                         round(float(s.median())-b_med,2),round(m25,2),round(m25-b_m25,2),yrs,posyr,nyr))
        rows={tuple(str(x['bar'])+x['signal'] for x in (r[0] or [])):r for r in rows}.values()  # dedup
        rows=sorted(rows,key=lambda r:-r[5])[:10]
        print(f"\n### {zl} · {et}   SHORT base: win {bwin}%  median {b_med:+.2f}  clip25 {b_m25:+.2f}  (n={len(pop)})")
        print(f"  {'sequence':30} {'n':>4} {'win':>5} {'medLift':>7} {'m25':>6} {'m25L':>6} {'+yr':>4}  per-year short-win%")
        for seq,n,win,ml,m25,m25l,yrs,posyr,nyr in rows:
            s=" ".join(f"{x['bar']}:{x['signal']}" for x in (seq or []))
            yrstr=" ".join(f"{y%100}:{v}" for y,v in yrs.items() if v is not None)
            print(f"  {s[:30]:30} {n:>4} {win:>5} {ml:>+7} {m25:>+6} {m25l:>+6} {str(posyr)+'/'+str(nyr):>4}  {yrstr}")
print("\ndone")
