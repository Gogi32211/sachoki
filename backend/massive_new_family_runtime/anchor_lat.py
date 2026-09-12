import sys, warnings, json, time; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from wlnbb_engine import compute_wlnbb
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
con=duckdb.connect(S+'/stageA_input.duckdb',read_only=True)
keys=[r[0] for r in con.execute("select distinct k from bars order by k").fetchall()]
lat={'L34':{'HEAD':[],'POST_GAP':[]},'L43':{'HEAD':[],'POST_GAP':[]},'SETUP':{'HEAD':[],'POST_GAP':[]}}
never={'L34':0,'L43':0,'SETUP':0}; segs=0; t0=time.time()
for kn,k in enumerate(keys):
    d=con.execute("select coverage_state,open,high,low,close,volume from bars where k=? order by bar_start",[k]).df()
    n=len(d); ok=(d.coverage_state=='COMPLETE').to_numpy()
    seg=np.cumsum(~ok); first=seg[np.argmax(ok)] if ok.any() else -1
    for sid,idx in pd.Series(np.arange(n)).groupby(seg):
        ii=idx.to_numpy(); ii=ii[ok[ii]]
        if len(ii)<20: continue          # only segments that can reach the bounded requirement
        segs+=1
        role='HEAD' if sid==first else 'POST_GAP'
        w=compute_wlnbb(d.iloc[ii].copy())
        L34=w['L34'].to_numpy().astype(bool); L22=w['L22'].to_numpy().astype(bool); L43=w['L43'].to_numpy().astype(bool)
        for nm,arr in [('L34',L34),('L43',L43),('SETUP',L34|L22)]:
            hit=np.flatnonzero(arr)
            if len(hit)==0: never[nm]+=1; continue
            # latency measured from the first bounded-eligible bar (index 19, age==20)
            first_anchor=hit[0]
            lat[nm][role].append(max(0,int(first_anchor)-19))
    if (kn+1)%100==0: print(f"  {kn+1}/{len(keys)} {time.time()-t0:.0f}s",flush=True)
out={}
for nm in lat:
    out[nm]={}
    for role in ('HEAD','POST_GAP'):
        v=np.array(lat[nm][role])
        out[nm][role]={'n':len(v),'median':float(np.median(v)) if len(v) else None,
          'p75':float(np.percentile(v,75)) if len(v) else None,
          'p90':float(np.percentile(v,90)) if len(v) else None,
          'p95':float(np.percentile(v,95)) if len(v) else None}
    out[nm]['never_anchored_segments']=never[nm]
out['eligible_segments']=segs
json.dump(out,open(S+'/anchor_lat.json','w'))
print(json.dumps(out,indent=1))
