import sys, warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np, json, time
from studio.bar_physics import compute as phys_compute
ENR='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m.duckdb'
OUT='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/phys_result.json'
REQ=['phys_ad','phys_c','phys_e','phys_gap_true','phys_h','phys_k','phys_m','phys_r','phys_regime','phys_s','phys_wyc']
IN=['open','high','low','close','volume','atr_14','sig_l3','sig_l4','wvf_spike']
e=duckdb.connect(ENR,read_only=True)
pairs=e.execute("select ticker,universe from bars group by 1,2 order by ticker,universe").df()
T=dict(series=len(pairs),done=0,matched=0); per={c:0 for c in REQ}; rec=[]; t0=time.time()
for _,r in pairs.iterrows():
    try:
        d=e.execute(f"select date,{','.join(IN)},{','.join(REQ)} from bars where ticker=? and universe=? order by date",[r.ticker,r.universe]).df()
        if len(d)<2: T['done']+=1; continue
        out=phys_compute(d[IN].copy()); T['matched']+=len(d)
        for c in REQ:
            a=out[c].to_numpy(); b=d[c].to_numpy()
            if a.dtype.kind in 'fc' or b.dtype.kind in 'fc':
                af=pd.to_numeric(pd.Series(a),errors='coerce'); bf=pd.to_numeric(pd.Series(b),errors='coerce')
                bad=((af-bf).abs()>1e-9) | (af.isna()!=bf.isna())
            else:
                bad=(pd.Series(a).astype(str).fillna('')!=pd.Series(b).astype(str).fillna(''))
            n=int(bad.sum())
            if n:
                per[c]+=n
                for j in np.flatnonzero(bad.to_numpy())[:99999]:
                    rec.append([r.ticker,r.universe,str(d.date.iloc[j]),c,str(a[j]),str(b[j])])
    except Exception as ex:
        rec.append([r.ticker,r.universe,'ERR',str(ex)[:60],'',''])
    T['done']+=1
    if T['done']%300==0: json.dump(dict(T=T,per=per,rec=rec[:5000],elapsed=round(time.time()-t0,1)),open(OUT,'w'))
json.dump(dict(T=T,per=per,rec=rec[:5000],elapsed=round(time.time()-t0,1),FINISHED=True),open(OUT,'w'))
print("DONE",sum(per.values()))
