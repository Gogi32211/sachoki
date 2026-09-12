import sys, os, warnings, json, time; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from combo_engine import compute_combo
from studio.enricher import _compute_body_wick, _compute_gap_range
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
OUTJ=S+'/stageD.json'; TMP=S+'/stageD.json.tmp'
COLS=['rsi_14','sig_3g','sig_buy','sig_svs','sig_va','bar_gap_range']
N_CD=6; OLD=N_CD

# ---- authoritative shared cooldown state engine (same one l_sig now uses) ----
def cooldown_availability(raw):
    """raw: list of True/False/None (None = unobserved). Returns list of (output, determined)."""
    st=set(range(0,N_CD))|{OLD}
    out=[]
    for r in raw:
        o=set(); new=set()
        for rr in ([True,False] if r is None else [r]):
            for a in st:
                if rr and a>=OLD: o.add(True);  new.add(0)
                else:             o.add(False); new.add(a)
        out.append(o.pop() if len(o)==1 else None)
        st={min(a+1,OLD) for a in new}
    return out

def atomic_json(obj, path, tmp):
    with open(tmp,'w') as f:
        json.dump(obj,f); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)

bars=duckdb.connect(S+'/stageA_input.duckdb',read_only=True)
emad=duckdb.connect(S+'/pd_input.duckdb',read_only=True)
out=duckdb.connect(S+'/stageD.duckdb'); out.execute("drop table if exists d6")
out.execute("""create table d6(k VARCHAR, bar_start BIGINT, session_date VARCHAR, col VARCHAR,
                 bval TINYINT, dval DOUBLE, sval VARCHAR, availability VARCHAR)""")
keys=[r[0] for r in bars.execute("select distinct k from bars order by k").fetchall()]
agg={c:{} for c in COLS}; fires={c:0 for c in COLS}
T=dict(sec=len(keys),done=0,rows=0,invalid=0); t0=time.time(); last_hb=t0
for kn,k in enumerate(keys):
    try:
        d=bars.execute("select bar_start,session_date,coverage_state,open,high,low,close,volume from bars where k=? order by bar_start",[k]).df()
        n=len(d)
        if n<2: T['done']+=1; continue
        ok=(d.coverage_state=='COMPLETE').to_numpy()
        age=np.zeros(n,int); a=0
        for i in range(n):
            a=a+1 if ok[i] else 0; age[i]=a
        seg=np.cumsum(~ok); first=seg[np.argmax(ok)] if ok.any() else -1
        is_head=ok&(seg==first)
        e=emad.execute("select bar_start,p,st from ema where k=? and p in (9,20,50)",[k]).df()
        es=e.pivot(index='bar_start',columns='p',values='st').reindex(d.bar_start.values)
        ema_ok={p:(es[p].to_numpy()=='VALID') for p in (9,20,50)}
        ema_prev={p:np.r_[False,ema_ok[p][:-1]] for p in (9,20,50)}
        rsi=np.full(n,np.nan); atr=np.full(n,np.nan)
        g3=np.zeros(n,np.int8); svs=np.zeros(n,np.int8); va=np.zeros(n,np.int8)
        bgr=np.array(['']*n,dtype=object); buy_raw=np.full(n,None,dtype=object)
        for _,idx in pd.Series(np.arange(n)).groupby(seg):
            ii=idx.to_numpy(); ii=ii[ok[ii]]
            if len(ii)<2: continue
            gg=d.iloc[ii]
            c_=gg.close; dd=c_.diff(); up=dd.clip(lower=0); dn=(-dd).clip(lower=0)
            ru=up.ewm(alpha=1/14,adjust=False,min_periods=1).mean(); rd=dn.ewm(alpha=1/14,adjust=False,min_periods=1).mean()
            rsi[ii]=(100.0-100.0/(1.0+ru/rd.replace(0,1e-10))).round(1).to_numpy()
            h,l,cc=gg.high,gg.low,gg.close; pc=cc.shift(1)
            tr=pd.concat([(h-l).abs(),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
            atr[ii]=tr.ewm(alpha=1/14,adjust=False).mean().to_numpy()
            cb=compute_combo(gg.copy())
            g3[ii]=cb['sig3g'].fillna(False).to_numpy().astype(np.int8)
            svs[ii]=cb['svs_2809'].fillna(False).to_numpy().astype(np.int8)
            vr=(gg.volume/gg.volume.rolling(20,min_periods=1).mean().replace(0,np.nan)).fillna(0)
            va[ii]=((vr>2.0)&(vr.shift(1,fill_value=0)<=2.0)).astype(np.int8).to_numpy()
            src=gg.copy(); src['atr_14']=atr[ii]
            bg=_compute_gap_range(_compute_body_wick(src))
            bgr[ii]=bg['bar_gap_range'].fillna('').astype(str).to_numpy()
            # RAW buy predicate: neutralise the engine internal cooldown so buy_2809 == buy_raw,
            # then apply the authoritative shared cooldown engine ourselves across the FULL series.
            import combo_engine as _CE
            _save=_CE._apply_cooldown
            try:
                _CE._apply_cooldown = lambda ser, n: ser
                cb_raw=compute_combo(gg.copy())
            finally:
                _CE._apply_cooldown=_save
            buy_raw[ii]=cb_raw['buy_2809'].fillna(False).to_numpy().astype(bool)
        raw_list=[ (None if not ok[i] else bool(buy_raw[i])) for i in range(n) ]
        cd_out=cooldown_availability(raw_list)
        cd_det=np.array([o is not None for o in cd_out])
        frames=[]
        for c in COLS:
            if c=='rsi_14':      v=ok&(age>=14)
            elif c=='sig_3g':    v=ok&(age>=51)&ema_ok[9]&ema_ok[20]&ema_ok[50]&ema_prev[9]&ema_prev[20]&ema_prev[50]
            elif c=='sig_buy':
                base=ok&(age>=50)&ema_ok[9]&ema_ok[20]&ema_ok[50]
                v=base&cd_det
            elif c in ('sig_svs','sig_va'): v=ok&(age>=21)
            else:                v=ok&(age>=14)     # bar_gap_range via Massive ATR14
            avail=np.where(~ok,'UNAVAILABLE_CURRENT',
                    np.where(v,'AVAILABLE_VALID',
                      np.where(is_head,'INITIALIZATION_SENSITIVE_HEAD','INITIALIZATION_SENSITIVE_POST_GAP')))
            if c=='sig_buy':
                hard=ok&(age<41)
                avail=np.where(hard,'UNAVAILABLE_REQUIRED_HISTORY',avail)
            for s in np.unique(avail): agg[c][s]=agg[c].get(s,0)+int((avail==s).sum())
            bv=dv=sv=np.full(n,None)
            if c=='rsi_14':          dv=np.where(v,rsi,None)
            elif c=='bar_gap_range': sv=np.where(v,bgr,None)
            else:
                src_arr={'sig_3g':g3,'sig_svs':svs,'sig_va':va,
                         'sig_buy':np.array([1 if o else 0 for o in cd_out],dtype=np.int8)}[c]
                bv=np.where(v,src_arr,None); fires[c]+=int((src_arr[v]==1).sum())
            frames.append(pd.DataFrame(dict(k=k,bar_start=d.bar_start.values,session_date=d.session_date.values,
                                            col=c,bval=bv,dval=dv,sval=sv,availability=avail)))
        df_t=pd.concat(frames,ignore_index=True)
        out.register('df_t',df_t); out.execute("insert into d6 select * from df_t"); out.unregister('df_t')
        T['rows']+=len(df_t)
    except Exception as ex:
        T['invalid']+=1
        print(f"  ERR {k[:8]}: {str(ex)[:80]}",flush=True)
    T['done']+=1
    now=time.time()
    if (kn+1)%25==0 or now-last_hb>=60:
        last_hb=now
        rate=T['rows']/max(1e-9,now-t0)
        print(f"  HB {T['done']}/{T['sec']} rows={T['rows']:,} ticker={k[:8]} elapsed={now-t0:.0f}s rows/s={rate:,.0f}",flush=True)
        atomic_json(dict(T=T,agg=agg,fires=fires,el=round(now-t0),FINISHED=False),OUTJ,TMP)
atomic_json(dict(T=T,agg=agg,fires=fires,el=round(time.time()-t0),FINISHED=True),OUTJ,TMP)
out.close(); bars.close(); emad.close()
print("DONE rows",T['rows'],"invalid",T['invalid'])
