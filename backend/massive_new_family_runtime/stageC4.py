import sys, warnings, json, time; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from wlnbb_engine import compute_wlnbb
from cisd_engine import compute_cisd
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
FALL=("FRI34","FRI43","FRI64","BLUE","CCI_READY","CCI_0_RETEST_OK","CCI_BLUE_TURN",
      "BE_UP","BE_DN","BO_UP","BO_DN","BX_UP","BX_DN","FUCHSIA_RL","FUCHSIA_RH","PRE_PUMP")
WL=['vol_bucket','l34','l22','l43','bo_up','bx_up','be_up','l_sig']
CI={'sig_cisd_cplus':'PLUS_CISD','sig_cisd_cplus_minus':'CISD_PPM',
    'sig_cisd_minus_struct':'MINUS_STRUCT','sig_cisd_mpm':'CISD_MPM'}
COLS=WL+list(CI)
con=duckdb.connect(S+'/stageA_input.duckdb',read_only=True)
out=duckdb.connect(S+'/stageC4.duckdb'); out.execute("drop table if exists c4")
out.execute("""create table c4(k VARCHAR, bar_start BIGINT, session_date VARCHAR, col VARCHAR,
   bval TINYINT, sval VARCHAR, availability VARCHAR)""")
keys=[r[0] for r in con.execute("select distinct k from bars order by k").fetchall()]
agg={c:{} for c in COLS}; fires={c:0 for c in COLS}
fallback_rows=0; t0=time.time(); T=dict(sec=len(keys),done=0,rows=0)
for kn,k in enumerate(keys):
    d=con.execute("select bar_start,session_date,coverage_state,open,high,low,close,volume from bars where k=? order by bar_start",[k]).df()
    n=len(d); ok=(d.coverage_state=='COMPLETE').to_numpy()
    age=np.zeros(n,int); a=0
    for i in range(n):
        a=a+1 if ok[i] else 0; age[i]=a
    seg=np.cumsum(~ok)
    first_seg = seg[np.argmax(ok)] if ok.any() else -1
    is_head = ok & (seg==first_seg)                     # first contiguous COMPLETE run
    # ---- WLNBB per segment ----
    bval={c:np.zeros(n,dtype=np.int8) for c in ['l34','l22','l43','bo_up','bx_up','be_up']}
    sval={c:np.array(['']*n,dtype=object) for c in ['vol_bucket','l_sig']}
    aL34=np.zeros(n,bool); aL43=np.zeros(n,bool); aSet=np.zeros(n,bool); digit=np.zeros(n,bool)
    for _,idx in pd.Series(np.arange(n)).groupby(seg):
        ii=idx.to_numpy(); ii=ii[ok[ii]]
        if len(ii)<2: continue
        w=compute_wlnbb(d.iloc[ii].copy())
        for c,src in [('l34','L34'),('l22','L22'),('l43','L43'),('bo_up','BO_UP'),('bx_up','BX_UP'),('be_up','BE_UP')]:
            bval[c][ii]=w[src].fillna(False).to_numpy().astype(np.int8)
        sval['vol_bucket'][ii]=w['vol_bucket'].astype(str).to_numpy()
        L34=w['L34'].to_numpy().astype(bool); L22=w['L22'].to_numpy().astype(bool); L43=w['L43'].to_numpy().astype(bool)
        aL34[ii]=np.maximum.accumulate(L34.astype(np.int8)).astype(bool)
        aL43[ii]=np.maximum.accumulate(L43.astype(np.int8)).astype(bool)
        aSet[ii]=np.maximum.accumulate((L34|L22).astype(np.int8)).astype(bool)
        dg=np.zeros(len(ii),bool)
        for dd in range(1,7):
            if f"L{dd}" in w.columns: dg |= w[f"L{dd}"].to_numpy().astype(bool)
        digit[ii]=dg
        # l_sig string
        dgt=np.full(len(ii),'',dtype=object)
        for dd in range(1,7):
            if f"L{dd}" in w.columns:
                arr=w[f"L{dd}"].to_numpy().astype(bool)
                dgt=np.array([x+str(dd) if y else x for x,y in zip(dgt,arr)],dtype=object)
        lab=np.array([('L'+x) if x else '' for x in dgt],dtype=object)
        empty=(dgt=='')
        for nm in FALL:
            if nm in w.columns:
                lab=np.where(w[nm].to_numpy().astype(bool)&empty&(lab==''),nm,lab)
        sval['l_sig'][ii]=lab
    # ---- CISD: first run only ----
    cval={c:np.zeros(n,dtype=np.int8) for c in CI}
    if ok.any():
        hi=np.flatnonzero(is_head)
        if len(hi)>=2:
            r=compute_cisd(d.iloc[hi][['open','high','low','close','volume']].copy())
            for c,src in CI.items(): cval[c][hi]=r[src].fillna(False).to_numpy().astype(np.int8)
    # ---- availability ----
    frames=[]
    for c in COLS:
        if c in ('vol_bucket','l34','l22','l43'):
            v=ok&(age>=20); pend=ok&(age<20)
        elif c=='bo_up': v=ok&(age>=20)&aL34; pend=ok&~(( age>=20)&aL34)
        elif c=='bx_up': v=ok&(age>=20)&aL43; pend=ok&~((age>=20)&aL43)
        elif c=='be_up': v=ok&(age>=20)&aSet&aL43; pend=ok&~((age>=20)&aSet&aL43)
        elif c=='l_sig':
            v=ok&(age>=20)&(digit | (aL34&aL43&aSet)); pend=ok&~((age>=20)&(digit|(aL34&aL43&aSet)))
            fallback_rows+=int((ok&(age>=20)&~digit).sum())
        else:
            v=is_head.copy(); pend=ok&~is_head
        avail=np.where(~ok,'UNAVAILABLE_CURRENT',
                np.where(v,'AVAILABLE_VALID',
                  np.where(is_head,'INITIALIZATION_SENSITIVE_HEAD','INITIALIZATION_SENSITIVE_POST_GAP')))
        for s in np.unique(avail): agg[c][s]=agg[c].get(s,0)+int((avail==s).sum())
        if c in sval:
            sv=np.where(v,sval[c],None); bv=np.full(n,None)
        else:
            src = bval[c] if c in bval else cval[c]
            bv=np.where(v,src,None); sv=np.full(n,None)
            fires[c]+=int((src[v]==1).sum())
        frames.append(pd.DataFrame(dict(k=k,bar_start=d.bar_start.values,session_date=d.session_date.values,
                                        col=c,bval=bv,sval=sv,availability=avail)))
    df_t=pd.concat(frames,ignore_index=True)
    out.register('df_t',df_t); out.execute("insert into c4 select * from df_t"); out.unregister('df_t')
    T['rows']+=len(df_t); T['done']+=1
    if (kn+1)%50==0:
        print(f"  {kn+1}/{len(keys)} {time.time()-t0:.0f}s",flush=True)
        json.dump(dict(T=T,agg=agg,fires=fires,fallback_rows=fallback_rows,el=round(time.time()-t0)),open(S+'/c4.json','w'))
json.dump(dict(T=T,agg=agg,fires=fires,fallback_rows=fallback_rows,el=round(time.time()-t0),FINISHED=True),open(S+'/c4.json','w'))
out.close(); con.close(); print("DONE",T['rows'])
