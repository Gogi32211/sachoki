import sys, warnings, json, time; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from wlnbb_engine import compute_wlnbb, _PP_WINDOW, _PP_MIN, _PP_COOL
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
N=_PP_COOL; OLD=N
FALL=("FRI34","FRI43","FRI64","BLUE","CCI_READY","CCI_0_RETEST_OK","CCI_BLUE_TURN",
      "BE_UP","BE_DN","BO_UP","BO_DN","BX_UP","BX_DN","FUCHSIA_RL","FUCHSIA_RH","PRE_PUMP")
def cooldown_states(raw):
    """raw: array of True/False/None (None = unobserved). returns per-bar (output, states_before)"""
    st=set(range(0,N))|{OLD}   # unknown at series start
    outs=[]; snap=[]
    for r in raw:
        snap.append(frozenset(st))
        o=set(); new=set()
        for rr in ([True,False] if r is None else [r]):
            for a in st:
                if rr and a>=OLD: o.add(True);  new.add(0)
                else:             o.add(False); new.add(a)
        outs.append(o.pop() if len(o)==1 else None)
        st={min(a+1,OLD) for a in new}
    return outs, snap
con=duckdb.connect(S+'/stageA_input.duckdb',read_only=True)
keys=[r[0] for r in con.execute("select distinct k from bars order by k").fetchall()]
rows=[]; t0=time.time()
for kn,k in enumerate(keys):
    d=con.execute("select bar_start,session_date,coverage_state,open,high,low,close,volume from bars where k=? order by bar_start",[k]).df()
    n=len(d); ok=(d.coverage_state=='COMPLETE').to_numpy()
    age=np.zeros(n,int); a=0
    for i in range(n):
        a=a+1 if ok[i] else 0; age[i]=a
    seg=np.cumsum(~ok)
    digit=np.zeros(n,bool); vsa_raw=np.full(n,None,dtype=object)
    aL34=np.zeros(n,bool); aL43=np.zeros(n,bool); aSet=np.zeros(n,bool)
    for _,idx in pd.Series(np.arange(n)).groupby(seg):
        ii=idx.to_numpy(); ii=ii[ok[ii]]
        if len(ii)<2: continue
        w=compute_wlnbb(d.iloc[ii].copy())
        dg=np.zeros(len(ii),bool)
        for dd in range(1,7):
            if f"L{dd}" in w.columns: dg |= w[f"L{dd}"].to_numpy().astype(bool)
        digit[ii]=dg
        L34=w['L34'].to_numpy().astype(bool); L22=w['L22'].to_numpy().astype(bool); L43=w['L43'].to_numpy().astype(bool)
        aL34[ii]=np.maximum.accumulate(L34.astype(np.int8)).astype(bool)
        aL43[ii]=np.maximum.accumulate(L43.astype(np.int8)).astype(bool)
        aSet[ii]=np.maximum.accumulate((L34|L22).astype(np.int8)).astype(bool)
        # raw PRE_PUMP predicate on observed bars; segment-local rolling is itself bounded
        if 'vol_zscore' in w.columns or True:
            hits=(w.get('L1',pd.Series(False,index=w.index)).astype(bool)|
                  w.get('L2',pd.Series(False,index=w.index)).astype(bool))
        vs=hits.rolling(_PP_WINDOW,min_periods=1).sum()
        vsa_raw[ii]=(vs>=_PP_MIN).to_numpy()
    # corrected: unobserved bars carry raw=None
    outs,_=cooldown_states(list(vsa_raw))
    pp_determined=np.array([o is not None for o in outs])
    # fallback rows = bounded-eligible, no digit
    fb = ok&(age>=20)&~digit
    for i in np.flatnonzero(fb):
        rows.append(dict(k=k,bar_start=int(d.bar_start.iloc[i]),session=str(d.session_date.iloc[i]),
                         age=int(age[i]), anchors=bool(aL34[i] and aL43[i] and aSet[i]),
                         pp_determined=bool(pp_determined[i]),
                         is_head=bool(seg[i]==seg[np.argmax(ok)])))
    if (kn+1)%100==0: print(f"  {kn+1}/{len(keys)} {time.time()-t0:.0f}s",flush=True)
con.close()
json.dump(rows,open(S+'/lsig_fix.json','w'))
print("DONE fallback rows:",len(rows))
