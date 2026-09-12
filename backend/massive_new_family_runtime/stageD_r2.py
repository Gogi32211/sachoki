# MASSIVE_NEW_FAMILY_STAGE_D_PRODUCTION_V1  ·  run STAGE_D_R2  (fresh lineage)
import sys, os, warnings, json, time; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import duckdb, pandas as pd, numpy as np
from sig_buy_raw import compute_combo_uncooled, cooldown_states
from studio.enricher import _compute_body_wick, _compute_gap_range
RUN='STAGE_D_R2'; OUTJ=f'{S}/{RUN}.json'; TMP=OUTJ+'.tmp'
COLS=['rsi_14','sig_3g','sig_buy','sig_svs','sig_va','bar_gap_range']
def atomic(obj):
    with open(TMP,'w') as f: json.dump(obj,f); f.flush(); os.fsync(f.fileno())
    os.replace(TMP,OUTJ)
bars=duckdb.connect(S+'/stageA_input.duckdb',read_only=True)
emad=duckdb.connect(S+'/pd_input.duckdb',read_only=True)
out=duckdb.connect(f'{S}/{RUN}.duckdb'); out.execute("drop table if exists d6")
out.execute("""create table d6(k VARCHAR, bar_start BIGINT, session_date VARCHAR, col VARCHAR,
                 bval TINYINT, dval DOUBLE, sval VARCHAR, availability VARCHAR)""")
keys=[r[0] for r in bars.execute("select distinct k from bars order by k").fetchall()]
agg={c:{} for c in COLS}; fires={c:0 for c in COLS}
T=dict(run=RUN,sec=len(keys),done=0,rows=0,err=0); t0=last=time.time()
for kn,k in enumerate(keys):
    try:
        d=bars.execute("select bar_start,session_date,coverage_state,open,high,low,close,volume from bars where k=? order by bar_start",[k]).df()
        n=len(d)
        if n<2: T['done']+=1; continue
        ok=(d.coverage_state=='COMPLETE').to_numpy()
        age=np.zeros(n,int); a=0
        for i in range(n): a=a+1 if ok[i] else 0; age[i]=a
        seg=np.cumsum(~ok); is_head=ok&(seg==(seg[np.argmax(ok)] if ok.any() else -1))
        e=emad.execute("select bar_start,p,st from ema where k=? and p in (9,20,50)",[k]).df()
        es=e.pivot(index='bar_start',columns='p',values='st').reindex(d.bar_start.values)
        eok={p:(es[p].to_numpy()=='VALID') for p in (9,20,50)}
        eprev={p:np.r_[False,eok[p][:-1]] for p in (9,20,50)}
        rsi=np.full(n,np.nan); g3=np.zeros(n,np.int8); svs=np.zeros(n,np.int8)
        va=np.zeros(n,np.int8); bgr=np.array(['']*n,dtype=object); rawbuy=np.zeros(n,bool)
        for _,idx in pd.Series(np.arange(n)).groupby(seg):
            ii=idx.to_numpy(); ii=ii[ok[ii]]
            if len(ii)<2: continue
            gg=d.iloc[ii]
            dd=gg.close.diff(); ru=dd.clip(lower=0).ewm(alpha=1/14,adjust=False,min_periods=1).mean()
            rd=(-dd).clip(lower=0).ewm(alpha=1/14,adjust=False,min_periods=1).mean()
            rsi[ii]=(100.0-100.0/(1.0+ru/rd.replace(0,1e-10))).round(1).to_numpy()
            pc=gg.close.shift(1)
            tr=pd.concat([(gg.high-gg.low).abs(),(gg.high-pc).abs(),(gg.low-pc).abs()],axis=1).max(axis=1)
            atr=tr.ewm(alpha=1/14,adjust=False).mean()
            cb=compute_combo_uncooled(gg.copy())          # ONE call: buy slot IS buy_raw
            g3[ii]=cb['sig3g'].fillna(False).to_numpy().astype(np.int8)
            svs[ii]=cb['svs_2809'].fillna(False).to_numpy().astype(np.int8)
            rawbuy[ii]=cb['buy_2809'].fillna(False).to_numpy().astype(bool)
            vr=(gg.volume/gg.volume.rolling(20,min_periods=1).mean().replace(0,np.nan)).fillna(0)
            va[ii]=((vr>2.0)&(vr.shift(1,fill_value=0)<=2.0)).astype(np.int8).to_numpy()
            src=gg.copy(); src['atr_14']=atr.to_numpy()
            bgr[ii]=_compute_gap_range(_compute_body_wick(src))['bar_gap_range'].fillna('').astype(str).to_numpy()
        base=ok&(age>=50)&eok[9]&eok[20]&eok[50]
        # raw is an INPUT to the state engine only where the raw predicate itself is identifiable
        cd=cooldown_states([bool(rawbuy[i]) if base[i] else None for i in range(n)])
        det=np.array([x is not None for x in cd])
        frames=[]
        for c in COLS:
            if   c=='rsi_14':   v=ok&(age>=14)
            elif c=='sig_3g':   v=ok&(age>=51)&eok[9]&eok[20]&eok[50]&eprev[9]&eprev[20]&eprev[50]
            elif c=='sig_buy':  v=base&det
            elif c in ('sig_svs','sig_va'): v=ok&(age>=21)
            else:               v=ok&(age>=14)
            av=np.where(~ok,'UNAVAILABLE_CURRENT',np.where(v,'AVAILABLE_VALID',
                 np.where(is_head,'INITIALIZATION_SENSITIVE_HEAD','INITIALIZATION_SENSITIVE_POST_GAP')))
            if c=='sig_buy': av=np.where(ok&(age<41),'UNAVAILABLE_REQUIRED_HISTORY',av)
            for s in np.unique(av): agg[c][s]=agg[c].get(s,0)+int((av==s).sum())
            bv=np.full(n,None,object); dv=np.full(n,None,object); sv=np.full(n,None,object)
            if   c=='rsi_14':          dv=np.where(v,rsi,None)
            elif c=='bar_gap_range':   sv=np.where(v,bgr,None)
            else:
                arr={'sig_3g':g3,'sig_svs':svs,'sig_va':va,
                     'sig_buy':np.array([1 if x else 0 for x in cd],np.int8)}[c]
                bv=np.where(v,arr,None); fires[c]+=int((arr[v]==1).sum())
            frames.append(pd.DataFrame(dict(k=k,bar_start=d.bar_start.values,session_date=d.session_date.values,
                                            col=c,bval=bv,dval=dv,sval=sv,availability=av)))
        df_t=pd.concat(frames,ignore_index=True)
        out.register('df_t',df_t); out.execute("insert into d6 select * from df_t"); out.unregister('df_t')
        T['rows']+=len(df_t)
    except Exception as ex:
        T['err']+=1; print(f"  ERR {k[:8]}: {str(ex)[:90]}",flush=True)
    T['done']+=1; now=time.time()
    if (kn+1)%25==0 or now-last>=60:
        last=now; print(f"  HB {T['done']}/{T['sec']} rows={T['rows']:,} el={now-t0:.0f}s r/s={T['rows']/max(1e-9,now-t0):,.0f}",flush=True)
        atomic(dict(T=T,agg=agg,fires=fires,el=round(now-t0),FINISHED=False))
atomic(dict(T=T,agg=agg,fires=fires,el=round(time.time()-t0),FINISHED=True))
out.close(); bars.close(); emad.close(); print("DONE rows",T['rows'],"err",T['err'])
