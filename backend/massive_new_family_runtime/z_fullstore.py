import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np, json, time
from signal_engine import compute_signals
BASE='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m_base.duckdb'
ENR='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m.duckdb'
OUT='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/z_result.json'
ZS=['Z1G','Z1','Z2G','Z2','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11','Z12']
ZC=['sig_'+z.lower() for z in ZS]
b=duckdb.connect(BASE,read_only=True); e=duckdb.connect(ENR,read_only=True)
pairs=b.execute("select ticker,universe from bars group by 1,2 order by ticker,universe").df()
T=dict(series=len(pairs),done=0,matched=0,zsig=0,onehot=0,multi=0); per={c:0 for c in ZC}; rec=[]
t0=time.time()
for _,r in pairs.iterrows():
    try:
        df=b.execute("select date,open,high,low,close from bars where ticker=? and universe=? order by date",[r.ticker,r.universe]).df()
        st=e.execute("select date,z_sig,"+",".join(ZC)+" from bars where ticker=? and universe=? order by date",[r.ticker,r.universe]).df()
        if not len(df) or not len(st): T['done']+=1; continue
        nm=compute_signals(df)['sig_name'].astype(str).to_numpy()
        zn=np.where(np.isin(nm,ZS),nm,'')
        m=pd.DataFrame({'date':df.date.values}).merge(st,on='date',how='inner')
        if not len(m): T['done']+=1; continue
        idx=pd.Index(df.date.values).get_indexer(m.date.values); T['matched']+=len(m)
        sz=m.z_sig.fillna('').astype(str).to_numpy(); calc=zn[idx]
        d=(calc!=sz); n=int(d.sum()); T['zsig']+=n
        for j in np.flatnonzero(d):
            rec.append([r.ticker,r.universe,str(m.date.iloc[j]),calc[j],sz[j]])
        for z,c in zip(ZS,ZC):
            v=(calc==z).astype('int8'); s=m[c].astype('int8').to_numpy()
            k=int((v!=s).sum()); per[c]+=k; T['onehot']+=k
        oh=np.sum([m[c].astype('int8').to_numpy() for c in ZC],axis=0)
        T['multi']+=int((oh>1).sum())
    except Exception as ex:
        rec.append([r.ticker,r.universe,'ERR',str(ex)[:60],''])
    T['done']+=1
    if T['done']%300==0: json.dump(dict(T=T,per=per,rec=rec[:600],elapsed=round(time.time()-t0,1)),open(OUT,'w'))
json.dump(dict(T=T,per=per,rec=rec[:600],elapsed=round(time.time()-t0,1),FINISHED=True),open(OUT,'w'))
print("DONE zsig",T['zsig'],"onehot",T['onehot'])
