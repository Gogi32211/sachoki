import sys, warnings, json, time; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from signal_engine import compute_signals, compute_g_signals
from combo_engine import compute_tz_state
from vabs_engine import compute_vabs
from studio.enricher import _compute_suffixes, _compute_body_wick
DB='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/stageA_input.duckdb'
OUT='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/stageA_result.json'
ZS=['Z1G','Z1','Z2G','Z2','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11','Z12']
ZC=['sig_'+z.lower() for z in ZS]
SUF=['ne_suffix','wick_suffix','close_suffix','full_suffix']; VOL=['sig_vol_5x','sig_vol_10x','sig_vol_20x']
GS=['sig_g1','sig_g2','sig_g4','sig_g11']
HARD=ZC+VOL+SUF+['bar_body_wick']          # need prior bar; age<2 => UNAVAILABLE_HISTORY
WARM={'sig_tz_flip':9,'vbo_up':20}          # converging window; age<depth => INIT_SENSITIVE
COLS=HARD+GS+list(WARM)
FIRE_SID={2,1,6,8}      # T1,T1G,T4,T6 -> the only bars whose g* output depends on state
ARM_SID={23,24,25}      # Z10,Z11,Z12 -> arming triggers
DET_SID=FIRE_SID|ARM_SID  # either determines g_armed for all later bars
con=duckdb.connect(DB,read_only=True); con.execute("SET memory_limit='6GB'")
keys=[r[0] for r in con.execute('select distinct k from bars order by k').fetchall()]
agg={c:{'AVAILABLE_VALID':0,'INIT_SENSITIVE':0,'UNAVAILABLE_CURRENT':0,'UNAVAILABLE_HISTORY':0} for c in COLS}
fires={c:0 for c in COLS}; persec={c:[] for c in COLS}
T=dict(securities=len(keys),done=0,rows=0,invalid=0); t0=time.time()
for k in keys:
    try:
        d=con.execute("select session_date,bar_start,interval_index,coverage_state,open,high,low,close,volume from bars where k=? order by bar_start",[k]).df()
        if len(d)<2: T['done']+=1; continue
        n=len(d); T['rows']+=n
        ok=(d.coverage_state=='COMPLETE').to_numpy()
        age=np.zeros(n,dtype=int); a=0
        for i in range(n):
            a=a+1 if ok[i] else 0; age[i]=a
        segid=np.cumsum(~ok)
        val={c:np.zeros(n,dtype=np.int8) for c in ZC+VOL+GS+list(WARM)}
        armed_ok=np.zeros(n,dtype=bool)     # g*: True from first observed arming trigger in segment
        for _,idx in pd.Series(np.arange(n)).groupby(segid):
            ii=idx.to_numpy(); ii=ii[ok[ii]]
            if len(ii)<2: continue
            g=d.iloc[ii]
            sig=compute_signals(g[['open','high','low','close']])
            nm=sig['sig_name'].astype(str).to_numpy()
            for z,c in zip(ZS,ZC): val[c][ii]=(nm==z).astype(np.int8)
            vr=(g.volume/g.volume.shift(1).replace(0,np.nan)).fillna(0).to_numpy()
            for c,th in zip(VOL,[5,10,20]): val[c][ii]=(vr>=th).astype(np.int8)
            gg=compute_g_signals(g[['open','high','low','close']])
            for c in GS: val[c][ii]=gg[c.replace('sig_','')].to_numpy().astype(np.int8)
            sid_seg=sig['sig_id'].to_numpy().astype(int)
            det=np.isin(sid_seg,list(DET_SID))
            known=np.zeros(len(ii),bool)
            if det.any():
                known[int(np.argmax(det))+1:]=True
            # g* output is VALID when it is trivially False (bar is not a firing signal)
            # OR when g_armed has been determined by an earlier state-forcing event
            armed_ok[ii]=(~np.isin(sid_seg,list(FIRE_SID))) | known
            if len(ii)>=9:
                ts=pd.Series(np.asarray(compute_tz_state(g.copy()))).fillna(0).astype(int)
                val['sig_tz_flip'][ii]=((ts==3)&(ts.shift(1).fillna(0).astype(int)!=3)).astype(np.int8).to_numpy()
            if len(ii)>=20:
                val['vbo_up'][ii]=compute_vabs(g.copy())['vbo_up'].fillna(0).astype(np.int8).to_numpy()
        cur_un=int((~ok).sum())
        for c in COLS:
            if c in HARD:      v=ok&(age>=2); hist=ok&(age<2); ini=np.zeros(n,bool)
            elif c in WARM:    v=ok&(age>=WARM[c]); ini=ok&(age<WARM[c]); hist=np.zeros(n,bool)
            else:              v=ok&armed_ok;  ini=ok&~armed_ok; hist=np.zeros(n,bool)
            agg[c]['AVAILABLE_VALID']+=int(v.sum()); agg[c]['INIT_SENSITIVE']+=int(ini.sum())
            agg[c]['UNAVAILABLE_HISTORY']+=int(hist.sum()); agg[c]['UNAVAILABLE_CURRENT']+=cur_un
            if c in val: fires[c]+=int((val[c][v]==1).sum())
            persec[c].append(float(v.sum())/n)
    except Exception as ex:
        T['invalid']+=1
    T['done']+=1
    if T['done']%10==0: json.dump(dict(T=T,agg=agg,fires=fires,elapsed=round(time.time()-t0,1)),open(OUT,'w'))
json.dump(dict(T=T,agg=agg,fires=fires,elapsed=round(time.time()-t0,1),FINISHED=True,
  persec={c:{'zero_valid':int(sum(1 for x in v if x==0)),'any_valid':int(sum(1 for x in v if x>0)),
             'median':float(np.median(v)) if v else 0.0,'p10':float(np.percentile(v,10)) if v else 0.0,
             'p90':float(np.percentile(v,90)) if v else 0.0} for c,v in persec.items()}),open(OUT,'w'))
print("DONE rows",T['rows'],"invalid",T['invalid'])
