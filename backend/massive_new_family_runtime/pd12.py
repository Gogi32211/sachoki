import sys, warnings, json, time; warnings.filterwarnings('ignore')
import duckdb, pandas as pd, numpy as np
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
DEP=json.load(open('/tmp/pd_dep.json'))          # machine-extracted transitive graph
COLS=list(DEP)
bars=duckdb.connect(S+'/stageA_input.duckdb',read_only=True)
emad=duckdb.connect(S+'/pd_input.duckdb',read_only=True)
out=duckdb.connect(S+'/pd12.duckdb'); out.execute("drop table if exists pd")
out.execute("""create table pd(k VARCHAR, bar_start BIGINT, session_date VARCHAR, col VARCHAR,
                               value TINYINT, availability VARCHAR)""")
keys=[r[0] for r in bars.execute("select distinct k from bars order by k").fetchall()]
PER=[9,20,34,50,89,200]
agg={c:{'AVAILABLE_VALID':0,'INITIALIZATION_SENSITIVE':0,'UNAVAILABLE_DEPENDENCY':0,'UNAVAILABLE_CURRENT':0} for c in COLS}
fires={c:0 for c in COLS}; persec={c:[] for c in COLS}
T=dict(sec=len(keys),done=0,rows=0); t0=time.time()
for kn,k in enumerate(keys):
    d=bars.execute("select bar_start,session_date,coverage_state,open,close from bars where k=? order by bar_start",[k]).df()
    e=emad.execute("select bar_start,p,v,st from ema where k=?",[k]).df()
    ev=e.pivot(index='bar_start',columns='p',values='v')
    es=e.pivot(index='bar_start',columns='p',values='st')
    d=d.set_index('bar_start'); ev=ev.reindex(d.index); es=es.reindex(d.index)
    n=len(d); ok=(d.coverage_state=='COMPLETE').to_numpy()
    o=d.open.to_numpy(); c_=d.close.to_numpy()
    cx={p:(o<ev[p].to_numpy())&(c_>ev[p].to_numpy()) for p in PER}
    dx={p:(o>ev[p].to_numpy())&(c_<ev[p].to_numpy()) for p in PER}
    stv={p:es[p].to_numpy() for p in PER}
    p3=cx[9]&cx[20]&cx[50]
    F={'sig_p66':cx[200]&(cx[9]|cx[20]|cx[34]|cx[50]|cx[89]),
       'sig_p55':cx[89]&(cx[9]|cx[20]|cx[34]|cx[50]|cx[200]),
       'sig_p89':cx[89],'sig_p3':p3,'sig_p2':cx[9]&cx[20]&~p3,'sig_p50':cx[50]&~cx[9]&~cx[20],
       'sig_d66':dx[200]&(dx[9]|dx[20]|dx[34]|dx[50]|dx[89]),
       'sig_d55':dx[89]&(dx[9]|dx[20]|dx[34]|dx[50]|dx[200]),
       'sig_d89':dx[89],'sig_d3':dx[9]&dx[20]&dx[50],'sig_d2':dx[9]&dx[20],'sig_d50':dx[50]}
    frames=[]
    for c in COLS:
        need=DEP[c]
        anyun=np.zeros(n,bool); anyinit=np.zeros(n,bool)
        for p in need:
            anyun |= (stv[p]=='UNAVAILABLE')
            anyinit |= (stv[p]=='INITIALIZATION_SENSITIVE')
        av=np.where(~ok,'UNAVAILABLE_CURRENT',
             np.where(anyun,'UNAVAILABLE_DEPENDENCY',
               np.where(anyinit,'INITIALIZATION_SENSITIVE','AVAILABLE_VALID')))
        valid=(av=='AVAILABLE_VALID')
        val=np.where(valid, F[c].astype(np.int8), None)
        for s in agg[c]: agg[c][s]+=int((av==s).sum())
        fires[c]+=int((F[c]&valid).sum()); persec[c].append(float(valid.sum())/n)
        frames.append(pd.DataFrame(dict(k=k,bar_start=d.index.values,session_date=d.session_date.values,
                                        col=c,value=val,availability=av)))
    df_t=pd.concat(frames,ignore_index=True)
    out.register('df_t',df_t); out.execute("insert into pd select * from df_t"); out.unregister('df_t')
    T['rows']+=len(df_t); T['done']+=1
    if (kn+1)%100==0: print(f"  {kn+1}/{len(keys)} {time.time()-t0:.0f}s",flush=True)
json.dump(dict(T=T,agg=agg,fires=fires,elapsed=round(time.time()-t0,1),FINISHED=True,
  persec={c:{'zero':int(sum(1 for x in v if x==0)),'any':int(sum(1 for x in v if x>0)),
             'median':float(np.median(v)),'p10':float(np.percentile(v,10)),'p25':float(np.percentile(v,25)),
             'p75':float(np.percentile(v,75)),'p90':float(np.percentile(v,90))} for c,v in persec.items()}),
  open(S+'/pd12_result.json','w'))
out.close(); bars.close(); emad.close()
print("DONE rows",T['rows'])
