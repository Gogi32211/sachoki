# PHASE 1 — durable production of the 39 missing columns.  run PHASE1_R1
import sys, os, warnings, json, time; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import duckdb, pandas as pd, numpy as np
import signal_engine as _se
from studio.enricher import _compute_suffixes, _compute_body_wick
from vabs_engine import compute_vabs
from tz_flip_state import step as tz_step, classify as tz_classify, HEAD_SEED, ORDER as TZ_ORDER

RUN='PHASE1_SMOKE'; OUTJ=f'{S}/{RUN}.json'; TMP=OUTJ+'.tmp'
T_COLS=[f'sig_t{i}' for i in [1,2,3,4,5,6,9,10,11,12]]+['sig_t1g','sig_t2g']
Z_COLS=[f'sig_z{i}' for i in [1,2,3,4,5,6,7,9,10,11,12]]+['sig_z1g','sig_z2g']
G_COLS=['sig_g1','sig_g2','sig_g4','sig_g11']
SUF=['ne_suffix','wick_suffix','close_suffix','full_suffix']
VOL=['sig_vol_5x','sig_vol_10x','sig_vol_20x']
COLS=T_COLS+Z_COLS+G_COLS+SUF+VOL+['bar_body_wick','vbo_up','sig_tz_flip']
assert len(COLS)==39, len(COLS)
# DERIVED from source, never hand-written: a hand-written map had T1 and T2G swapped.
NAME2ID={v:kk for kk,v in _se.SIG_NAMES.items() if v!='NONE'}
assert NAME2ID['T1G']==1 and NAME2ID['T1']==2 and NAME2ID['T2G']==3, NAME2ID
assert all(c.replace('sig_','').upper() in NAME2ID for c in T_COLS+Z_COLS)
G_SRC=['g1','g2','g4','g11']   # by NAME; a [:4] slice picked up g6 instead of g11
SETTER_DEPTH={'bull_dom':50,'bear_dom':50,'bull_att':20,'bear_att':20,'bear_weak':7,'bull_weak':7}
VBO_TRIG_DEPTH=22; BREAK=10

def atomic(o):
    with open(TMP,'w') as f: json.dump(o,f); f.flush(); os.fsync(f.fileno())
    os.replace(TMP,OUTJ)

def tz_predicates(df):
    sig=_se.compute_signals(df); bc=sig["bc"].fillna(0).astype(int); zc=sig["zc"].fillna(0).astype(int)
    close,high,low=df["close"],df["high"],df["low"]
    _TW={1:4.,2:4.,3:3.,4:3.,5:2.,6:2.,7:1.,8:1.,9:1.,10:1.,11:.75}
    _ZW={1:4.,2:4.,3:3.,4:3.,5:2.,6:2.,7:1.,8:1.,9:1.,10:1.,11:1.,12:.75,13:1.,14:.25}
    tW=pd.Series([_TW.get(x,0.) for x in bc],index=bc.index); zW=pd.Series([_ZW.get(x,0.) for x in zc],index=zc.index)
    tDF=tW.rolling(3,min_periods=1).sum(); tDS=tW.rolling(7,min_periods=1).sum()
    zDF=zW.rolling(3,min_periods=1).sum(); zDS=zW.rolling(7,min_periods=1).sum()
    tC=(bc>0).astype(float).rolling(3,min_periods=1).sum(); zC=(zc>0).astype(float).rolling(3,min_periods=1).sum()
    sTR=bc.isin([1,2,3,4]).astype(float).rolling(3,min_periods=1).max().astype(bool)
    sZR=zc.isin([1,2,3,4]).astype(float).rolling(3,min_periods=1).max().astype(bool)
    e20=close.ewm(span=20,adjust=False).mean(); e50=close.ewm(span=50,adjust=False).mean()
    sH=high.shift(1).rolling(10,min_periods=1).max(); sL=low.shift(1).rolling(10,min_periods=1).min()
    return {'bull_dom':((tDS>zDS)&(tDF>zDF)&(tC>=2)&sTR&(close>e20)&(close>e50)&(close>sH)).to_numpy(),
            'bear_dom':((zDS>tDS)&(zDF>tDF)&(zC>=2)&sZR&(close<e20)&(close<e50)&(close<sL)).to_numpy(),
            'bull_att':((tDF>zDF)&(tC>=2)&sTR&(close>e20)).to_numpy(),
            'bear_att':((zDF>tDF)&(zC>=2)&sZR&(close<e20)).to_numpy(),
            'bear_weak':((zDS>tDS)&(zDS>=4.)&(tDF>=zDF*.70)&~sZR).to_numpy(),
            'bull_weak':((tDS>zDS)&(tDS>=4.)&(zDF>=tDF*.70)&~sTR).to_numpy()}

def vbo_reach(trig_know, hi, cl, ok):
    """Reachable-state engine. Returns (value_or_None, determined) per bar."""
    Sset={None}; out=[]
    n=len(trig_know)
    for i in range(n):
        k=trig_know[i]; NS=set(); OU=set()
        for st in Sset:
            for t in ([True,False] if k is None else [k]):
                s=i if t else st
                if s is not None and i>s:
                    if i-s<=BREAK:
                        if (not ok[i]) or (i-1<0) or (not ok[i-1]) or (not ok[s]): OU.add(None)
                        else: OU.add(bool(cl[i-1]<=hi[s] and cl[i]>hi[s]))
                    else: OU.add(False); s=None
                else: OU.add(False)
                NS.add(s)
        Sset=NS
        out.append((list(OU)[0] if len(OU)==1 and None not in OU else None, len(OU)==1 and None not in OU))
    return out

bars=duckdb.connect(S+'/stageA_input.duckdb',read_only=True)
out=duckdb.connect(f'{S}/{RUN}.duckdb'); out.execute("drop table if exists p39")
out.execute("""create table p39(k VARCHAR, bar_start BIGINT, session_date VARCHAR, col VARCHAR,
                 bval TINYINT, sval VARCHAR, availability VARCHAR)""")
keys=[r[0] for r in bars.execute("select distinct k from bars order by k limit 2").fetchall()]
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
        sid=np.zeros(n,np.int8); gmat=np.zeros((n,4),np.int8)
        sufm=np.array([['']*4]*n,dtype=object); bwv=np.array(['']*n,dtype=object)
        volm=np.zeros((n,3),np.int8)
        preds={p:np.zeros(n,bool) for p in SETTER_DEPTH}
        trig=np.zeros(n,bool)
        for _,idx in pd.Series(np.arange(n)).groupby(seg):
            ii=idx.to_numpy(); ii=ii[ok[ii]]
            if len(ii)<2: continue
            gg=d.iloc[ii].reset_index(drop=True)
            s=_se.compute_signals(gg.copy())
            # column is 'sig_id' (NOT 'sid'); a silent else-0 fallback zeroed all 25 T/Z columns.
            assert 'sig_id' in s.columns, list(s.columns)
            sid[ii]=s['sig_id'].fillna(0).to_numpy().astype(np.int8)
            g=_se.compute_g_signals(gg.copy())
            assert all(x in g.columns for x in G_SRC), list(g.columns)
            gmat[ii]=g[G_SRC].fillna(False).to_numpy().astype(np.int8)
            sf=_compute_suffixes(gg.copy()); sufm[ii]=sf[SUF].fillna('').astype(str).to_numpy()
            bwv[ii]=_compute_body_wick(gg.copy())['bar_body_wick'].fillna('').astype(str).to_numpy()
            vr=(gg.volume/gg.volume.shift(1).replace(0,np.nan)).fillna(0).to_numpy()
            volm[ii]=np.stack([(vr>=5),(vr>=10),(vr>=20)],axis=1).astype(np.int8)
            P=tz_predicates(gg.copy())
            for p in SETTER_DEPTH: preds[p][ii]=P[p]
            v=compute_vabs(gg.copy())
            trig[ii]=(v['abs_sig'].astype(bool)|v['climb_sig'].astype(bool)|v['load_sig'].astype(bool)).to_numpy()
        # ---- sig_tz_flip reachable-state pass (whole series) ----
        Sset={HEAD_SEED}; tzf=np.zeros(n,np.int8); tzd=np.zeros(n,bool)
        for i in range(n):
            if i==0: tzf[0]=0; tzd[0]=True; continue
            know=[(bool(preds[p][i]) if (ok[i] and age[i]>=SETTER_DEPTH[p]) else None) for p in TZ_ORDER]
            Sset,f=tz_step(Sset,know); cls,val=tz_classify(f)
            tzd[i]=(cls=='AVAILABLE_VALID'); tzf[i]=(val or 0)
        # ---- vbo_up reachable-state pass ----
        tk=[(bool(trig[i]) if (ok[i] and age[i]>=VBO_TRIG_DEPTH) else None) for i in range(n)]
        vres=vbo_reach(tk,d.high.to_numpy(),d.close.to_numpy(),ok)
        vbv=np.array([1 if (r[1] and r[0]) else 0 for r in vres],np.int8)
        vbd=np.array([r[1] for r in vres],bool)
        frames=[]
        for ci,c in enumerate(COLS):
            if c in T_COLS or c in Z_COLS:
                v=ok&(age>=2); nm=c.replace('sig_','').upper(); arr=(sid==NAME2ID[nm]).astype(np.int8); kind='b'
            elif c in G_COLS:
                v=ok&(age>=2); arr=gmat[:,G_COLS.index(c)]; kind='b'
            elif c in SUF:
                v=ok&(age>=2); arr=sufm[:,SUF.index(c)]; kind='s'
            elif c in VOL:
                v=ok&(age>=2); arr=volm[:,VOL.index(c)]; kind='b'
            elif c=='bar_body_wick': v=ok&(age>=2); arr=bwv; kind='s'
            elif c=='vbo_up':       v=ok&(age>=VBO_TRIG_DEPTH)&vbd; arr=vbv; kind='b'
            else:                   v=ok&tzd; arr=tzf; kind='b'
            av=np.where(~ok,'UNAVAILABLE_CURRENT',np.where(v,'AVAILABLE_VALID',
                 np.where(is_head,'INITIALIZATION_SENSITIVE_HEAD','INITIALIZATION_SENSITIVE_POST_GAP')))
            for st in np.unique(av): agg[c][st]=agg[c].get(st,0)+int((av==st).sum())
            bv=np.full(n,None,object); sv=np.full(n,None,object)
            if kind=='b': bv=np.where(v,arr,None); fires[c]+=int((np.asarray(arr)[v]==1).sum())
            else:         sv=np.where(v,arr,None)
            frames.append(pd.DataFrame(dict(k=k,bar_start=d.bar_start.values,session_date=d.session_date.values,
                                            col=c,bval=bv,sval=sv,availability=av)))
        df_t=pd.concat(frames,ignore_index=True)
        out.register('df_t',df_t); out.execute("insert into p39 select * from df_t"); out.unregister('df_t')
        T['rows']+=len(df_t)
    except Exception as ex:
        T['err']+=1; print(f"  ERR {k[:8]}: {type(ex).__name__}: {str(ex)[:110]}",flush=True)
    T['done']+=1; now=time.time()
    if (kn+1)%25==0 or now-last>=60:
        last=now; print(f"  HB {T['done']}/{T['sec']} rows={T['rows']:,} el={now-t0:.0f}s r/s={T['rows']/max(1e-9,now-t0):,.0f}",flush=True)
        atomic(dict(T=T,agg=agg,fires=fires,el=round(now-t0),FINISHED=False))
atomic(dict(T=T,agg=agg,fires=fires,el=round(time.time()-t0),FINISHED=True))
out.close(); bars.close(); print("DONE rows",T['rows'],"err",T['err'])
