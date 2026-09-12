"""sig_buy 7-cell raw/output census.
raw comes ONLY from the mechanically bound PRE-COOLDOWN predicate
    buy_raw = upmove_ok & break_gate     (combo_engine.py L74)
via compute_combo_uncooled. FORBIDDEN: deriving raw from buy_2809, from any
post-cooldown surface, or by inverting the cooldown."""
import sys, os, json, time, warnings; warnings.filterwarnings('ignore')
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend'); sys.path.insert(0,S)
import duckdb, numpy as np, pandas as pd
from sig_buy_raw import compute_combo_uncooled
RUN='SIGBUY_CENSUS'; J=f'{S}/{RUN}.json'
def atomic(o):
    t=J+'.tmp'
    with open(t,'w') as f: json.dump(o,f); f.flush(); os.fsync(f.fileno())
    os.replace(t,J)
bars=duckdb.connect(S+'/stageA_input.duckdb',read_only=True)
emad=duckdb.connect(S+'/pd_input.duckdb',read_only=True)
canon=duckdb.connect(S+'/CANON69.duckdb',read_only=True)
keys=[r[0] for r in bars.execute("select distinct k from bars order by k").fetchall()]
CELL={}; T={'run':RUN,'done':0,'sec':len(keys),'err':0}; t0=last=time.time()
for kn,k in enumerate(keys):
    try:
        d=bars.execute("select bar_start,coverage_state,open,high,low,close,volume from bars where k=? order by bar_start",[k]).df()
        n=len(d)
        if n<2: T['done']+=1; continue
        ok=(d.coverage_state=='COMPLETE').to_numpy()
        age=np.zeros(n,int); a=0
        for i in range(n): a=a+1 if ok[i] else 0; age[i]=a
        seg=np.cumsum(~ok)
        e=emad.execute("select bar_start,p,st from ema where k=? and p in (9,20,50)",[k]).df()
        es=e.pivot(index='bar_start',columns='p',values='st').reindex(d.bar_start.values)
        eok=(es[9].to_numpy()=='VALID')&(es[20].to_numpy()=='VALID')&(es[50].to_numpy()=='VALID')
        raw=np.zeros(n,bool)
        for _,idx in pd.Series(np.arange(n)).groupby(seg):
            ii=idx.to_numpy(); ii=ii[ok[ii]]
            if len(ii)<2: continue
            cb=compute_combo_uncooled(d.iloc[ii].copy())
            raw[ii]=cb['buy_2809'].fillna(False).to_numpy().astype(bool)   # slot holds buy_raw
        identifiable=ok&(age>=50)&eok
        rawcls=np.where(~identifiable,'raw UNKNOWN',np.where(raw,'raw TRUE','raw FALSE'))
        o=canon.execute("select bar_start,bval,availability from x69 where feature='sig_buy' and k=? order by bar_start",[k]).df()
        o=o.set_index('bar_start').reindex(d.bar_start.values)
        av=o.availability.to_numpy(); bv=o.bval.to_numpy()
        outcls=np.where(av=='AVAILABLE_VALID',np.where(bv==1,'output VALID TRUE','output VALID FALSE'),
                 np.where(pd.Series(av).str.startswith('INITIALIZATION').to_numpy(),'output INIT','output UNAVAILABLE'))
        for r,oc in zip(rawcls,outcls):
            key=f"{r} -> {oc}"; CELL[key]=CELL.get(key,0)+1
    except Exception as ex:
        T['err']+=1; print(f"  ERR {k[:8]}: {str(ex)[:90]}",flush=True)
    T['done']+=1; now=time.time()
    if (kn+1)%25==0 or now-last>=60:
        last=now; print(f"  HB {T['done']}/{T['sec']} el={now-t0:.0f}s",flush=True)
        atomic(dict(T=T,cells=CELL,el=round(now-t0),FINISHED=False))
atomic(dict(T=T,cells=CELL,el=round(time.time()-t0),FINISHED=True))
print("DONE err",T['err'])
