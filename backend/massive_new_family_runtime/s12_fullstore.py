import sys, warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np, json, time
from analyzers.tz_wlnbb.signal_extraction import compute_line5
from studio.enricher import _compute_suffixes
ENR='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m.duckdb'
OUT='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/s12_result.json'
SUF=['ne_suffix','wick_suffix','close_suffix','full_suffix']
e=duckdb.connect(ENR,read_only=True)
pairs=e.execute("select ticker,universe from bars group by 1,2 order by ticker,universe").df()
T=dict(series=len(pairs),done=0,matched=0); per={c:0 for c in ['bar_line5']+SUF}; rec=[]; t0=time.time()
for _,r in pairs.iterrows():
    try:
        d=e.execute(f"select date,open,high,low,close,volume,bar_line5,{','.join(SUF)} from bars where ticker=? and universe=? order by date",[r.ticker,r.universe]).df()
        if len(d)<2: T['done']+=1; continue
        T['matched']+=len(d)
        src=d[['date','open','high','low','close','volume']].copy()
        l5=compute_line5(src.copy())['bar_line5'].fillna('').astype(str).to_numpy()
        s=d['bar_line5'].fillna('').astype(str).to_numpy()
        bad=(l5!=s); n=int(bad.sum())
        if n:
            per['bar_line5']+=n
            for j in np.flatnonzero(bad)[:99999]: rec.append([r.ticker,r.universe,str(d.date.iloc[j]),'bar_line5',str(l5[j]),str(s[j])])
        sf=_compute_suffixes(src.copy())
        for c in SUF:
            v=sf[c].fillna('').astype(str).to_numpy(); ss=d[c].fillna('').astype(str).to_numpy()
            bad=(v!=ss); n=int(bad.sum())
            if n:
                per[c]+=n
                for j in np.flatnonzero(bad)[:99999]: rec.append([r.ticker,r.universe,str(d.date.iloc[j]),c,str(v[j]),str(ss[j])])
    except Exception as ex:
        rec.append([r.ticker,r.universe,'ERR',str(ex)[:60],'',''])
    T['done']+=1
    if T['done']%400==0: json.dump(dict(T=T,per=per,rec=rec[:8000],elapsed=round(time.time()-t0,1)),open(OUT,'w'))
json.dump(dict(T=T,per=per,rec=rec[:8000],elapsed=round(time.time()-t0,1),FINISHED=True),open(OUT,'w'))
print("DONE",sum(per.values()))
