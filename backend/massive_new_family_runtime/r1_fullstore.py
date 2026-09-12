import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np, json, time
from combo_engine import compute_combo
BASE='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m_base.duckdb'
ENR='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m.duckdb'
OUT='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/r1_result.json'
MAP={'sig3g':'sig_3g','buy_2809':'sig_buy','svs_2809':'sig_svs'}
b=duckdb.connect(BASE,read_only=True); e=duckdb.connect(ENR,read_only=True)
pairs=b.execute("select ticker,universe from bars group by 1,2 order by ticker,universe").df()
T=dict(series=len(pairs),done=0,matched=0); percol={c:0 for c in MAP.values()}; bad=[]
t0=time.time()
for _,r in pairs.iterrows():
    try:
        df=b.execute("select date,open,high,low,close,volume from bars where ticker=? and universe=? order by date",[r.ticker,r.universe]).df()
        st=e.execute("select date,"+",".join(MAP.values())+" from bars where ticker=? and universe=? order by date",[r.ticker,r.universe]).df()
        if len(df)==0 or len(st)==0: T['done']+=1; continue
        out=compute_combo(df.copy())
        m=pd.DataFrame({'date':df.date.values}).merge(st,on='date',how='inner')
        if not len(m): T['done']+=1; continue
        idx=pd.Index(df.date.values).get_indexer(m.date.values); T['matched']+=len(m)
        for h,s in MAP.items():
            v=out[h].to_numpy().astype('int8')[idx]; sv=m[s].astype('int8').to_numpy()
            d=(v!=sv); n=int(d.sum())
            if n:
                percol[s]+=n
                for j in np.flatnonzero(d)[:20]:
                    bad.append([r.ticker,r.universe,str(m.date.iloc[j]),s,int(v[j]),int(sv[j])])
    except Exception as ex:
        bad.append([r.ticker,r.universe,'ERR',str(ex)[:70],0,0])
    T['done']+=1
    if T['done']%200==0: json.dump(dict(T=T,percol=percol,bad=bad[:500],elapsed=round(time.time()-t0,1)),open(OUT,'w'))
json.dump(dict(T=T,percol=percol,bad=bad[:500],elapsed=round(time.time()-t0,1),FINISHED=True),open(OUT,'w'))
print("DONE",sum(percol.values()))
