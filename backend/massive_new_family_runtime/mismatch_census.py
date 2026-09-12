import duckdb, pandas as pd, numpy as np, json
BASE='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m_base.duckdb'
ENR='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m.duckdb'
OUT='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/census.json'
COLS=['sig_p66','sig_p55','sig_p89','sig_p3','sig_p2','sig_p50','sig_d66','sig_d55','sig_d89','sig_d3','sig_d2','sig_d50']
def calc(o,c):
    E={n:c.ewm(span=n,adjust=False).mean() for n in (9,20,34,50,89,200)}
    cx={n:(o<E[n])&(c>E[n]) for n in E}; dx={n:(o>E[n])&(c<E[n]) for n in E}
    p3=cx[9]&cx[20]&cx[50]
    return {'sig_p66':cx[200]&(cx[9]|cx[20]|cx[34]|cx[50]|cx[89]),'sig_p55':cx[89]&(cx[9]|cx[20]|cx[34]|cx[50]|cx[200]),
            'sig_p89':cx[89],'sig_p3':p3,'sig_p2':cx[9]&cx[20]&~p3,'sig_p50':cx[50]&~cx[9]&~cx[20],
            'sig_d66':dx[200]&(dx[9]|dx[20]|dx[34]|dx[50]|dx[89]),'sig_d55':dx[89]&(dx[9]|dx[20]|dx[34]|dx[50]|dx[200]),
            'sig_d89':dx[89],'sig_d3':dx[9]&dx[20]&dx[50],'sig_d2':dx[9]&dx[20],'sig_d50':dx[50]}
b=duckdb.connect(BASE,read_only=True); e=duckdb.connect(ENR,read_only=True)
pairs=b.execute("select ticker,universe from bars group by 1,2 order by ticker,universe").df()
rec=[]
for _,r in pairs.iterrows():
    tk,uni=r.ticker,r.universe
    df=b.execute("select date,open,close from bars where ticker=? and universe=? order by date",[tk,uni]).df()
    st=e.execute("select date,"+",".join(COLS)+" from bars where ticker=? and universe=? order by date",[tk,uni]).df()
    if len(df)==0 or len(st)==0: continue
    C=calc(df.open.astype('float64'),df.close.astype('float64'))
    m=pd.DataFrame({'date':df.date.values}).merge(st,on='date',how='inner')
    if not len(m): continue
    idx=pd.Index(df.date.values).get_indexer(m.date.values)
    for col in COLS:
        v=C[col].to_numpy().astype('int8')[idx]; s=m[col].astype('int8').to_numpy()
        for j in np.flatnonzero(v!=s):
            rec.append(dict(ticker=tk,universe=uni,date=str(m.date.iloc[j]),col=col,calc=int(v[j]),stored=int(s[j]),
                            series_last=str(df.date.iloc[-1])))
json.dump(rec,open(OUT,'w'))
print("total mismatch cells:",len(rec))
b.close(); e.close()
