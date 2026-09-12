import sys, warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np, time, json
from wlnbb_engine import compute_wlnbb
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
con=duckdb.connect(S+"/stageA_input.duckdb",read_only=True)
keys=[r[0] for r in con.execute("select distinct k from bars order by k").fetchall()]
tot_bounded=0; tot_nodigit=0; sec_with=0; t0=time.time(); done=0
for k in keys:
    g=con.execute("select coverage_state,open,high,low,close,volume from bars where k=? order by bar_start",[k]).df()
    n=len(g); ok=(g.coverage_state=='COMPLETE').to_numpy()
    age=np.zeros(n,int); a=0
    for i in range(n):
        a=a+1 if ok[i] else 0; age[i]=a
    seg=np.cumsum(~ok); bounded=ok&(age>=20)
    if bounded.any():
        nod=0
        for _,idx in pd.Series(np.arange(n)).groupby(seg):
            ii=idx.to_numpy(); ii=ii[ok[ii]]
            if len(ii)<20: continue
            w=compute_wlnbb(g.iloc[ii].copy())
            dg=np.zeros(len(ii),bool)
            for dd in range(1,7):
                if f"L{dd}" in w.columns: dg |= w[f"L{dd}"].to_numpy().astype(bool)
            nod+=int((~dg & bounded[ii]).sum())
        tot_bounded+=int(bounded.sum()); tot_nodigit+=nod
        if nod>0: sec_with+=1
    done+=1
    if done%100==0:
        json.dump(dict(done=done,bounded=tot_bounded,nodigit=tot_nodigit,sec=sec_with,el=round(time.time()-t0)),open(S+'/lsig.json','w'))
json.dump(dict(done=done,bounded=tot_bounded,nodigit=tot_nodigit,sec=sec_with,el=round(time.time()-t0),FINISHED=True),open(S+'/lsig.json','w'))
print("DONE",tot_bounded,tot_nodigit,sec_with)
