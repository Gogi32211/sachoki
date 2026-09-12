import duckdb, pandas as pd, numpy as np, json, time, sys
BASE='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m_base.duckdb'
ENR='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m.duckdb'
OUT='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/fullstore_result.json'
COLS=['sig_p66','sig_p55','sig_p89','sig_p3','sig_p2','sig_p50',
      'sig_d66','sig_d55','sig_d89','sig_d3','sig_d2','sig_d50']
def calc(o,c):
    E={n:c.ewm(span=n,adjust=False).mean() for n in (9,20,34,50,89,200)}
    cx={n:(o<E[n])&(c>E[n]) for n in E}; dx={n:(o>E[n])&(c<E[n]) for n in E}
    p3=cx[9]&cx[20]&cx[50]
    return {'sig_p66':cx[200]&(cx[9]|cx[20]|cx[34]|cx[50]|cx[89]),
            'sig_p55':cx[89]&(cx[9]|cx[20]|cx[34]|cx[50]|cx[200]),
            'sig_p89':cx[89],'sig_p3':p3,'sig_p2':cx[9]&cx[20]&~p3,'sig_p50':cx[50]&~cx[9]&~cx[20],
            'sig_d66':dx[200]&(dx[9]|dx[20]|dx[34]|dx[50]|dx[89]),
            'sig_d55':dx[89]&(dx[9]|dx[20]|dx[34]|dx[50]|dx[200]),
            'sig_d89':dx[89],'sig_d3':dx[9]&dx[20]&dx[50],'sig_d2':dx[9]&dx[20],'sig_d50':dx[50]}
b=duckdb.connect(BASE,read_only=True); e=duckdb.connect(ENR,read_only=True)
pairs=b.execute("select ticker,universe from bars group by 1,2 order by ticker,universe").df()
T=dict(series=len(pairs),done=0,base=0,mat=0,matched=0,cells=0,rows=0)
percol={c:0 for c in COLS}; bad=[]; t0=time.time()
for k,r in pairs.iterrows():
    tk,uni=r.ticker,r.universe
    try:
        df=b.execute("select date,open,close from bars where ticker=? and universe=? order by date",[tk,uni]).df()
        st=e.execute("select date,"+",".join(COLS)+" from bars where ticker=? and universe=? order by date",[tk,uni]).df()
    except Exception as ex:
        bad.append([tk,uni,'QUERY_ERROR',str(ex)[:80]]); continue
    T['base']+=len(df); T['mat']+=len(st)
    if len(df)==0 or len(st)==0: T['done']+=1; continue
    C=calc(df.open.astype('float64'),df.close.astype('float64'))
    m=pd.DataFrame({'date':df.date.values}).merge(st,on='date',how='inner')
    if len(m)==0: T['done']+=1; continue
    idx=pd.Index(df.date.values).get_indexer(m.date.values)
    T['matched']+=len(m)
    rowbad=np.zeros(len(m),dtype=bool)
    for col in COLS:
        v=C[col].to_numpy().astype('int8')[idx]; s=m[col].astype('int8').to_numpy()
        d=(v!=s); n=int(d.sum())
        if n: percol[col]+=n; rowbad|=d
    nb=int(rowbad.sum())
    if nb:
        T['rows']+=nb
        for j in np.flatnonzero(rowbad)[:50]:
            cs=[c for c in COLS if C[c].to_numpy().astype('int8')[idx][j]!=m[c].astype('int8').to_numpy()[j]]
            bad.append([tk,uni,str(m.date.iloc[j]),cs])
    T['done']+=1
    if T['done']%200==0:
        T['cells']=sum(percol.values())
        json.dump(dict(T=T,percol=percol,bad=bad[:400],elapsed=round(time.time()-t0,1)),open(OUT,'w'))
T['cells']=sum(percol.values())
json.dump(dict(T=T,percol=percol,bad=bad[:400],elapsed=round(time.time()-t0,1),FINISHED=True),open(OUT,'w'))
b.close(); e.close()
print("DONE",T['cells'],T['rows'])
