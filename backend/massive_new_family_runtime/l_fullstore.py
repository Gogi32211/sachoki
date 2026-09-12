import sys, warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np, json, time
from wlnbb_engine import compute_wlnbb
BASE='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m_base.duckdb'
ENR='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m.duckdb'
OUT='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/l_census.json'
FALL=("FRI34","FRI43","FRI64","BLUE","CCI_READY","CCI_0_RETEST_OK","CCI_BLUE_TURN",
      "BE_UP","BE_DN","BO_UP","BO_DN","BX_UP","BX_DN","FUCHSIA_RL","FUCHSIA_RH","PRE_PUMP")
BIN={'l22':'L22','l34':'L34','l43':'L43','be_up':'BE_UP','bo_up':'BO_UP','bx_up':'BX_UP'}
def lchart(w,n):
    dig=np.full(n,'',dtype=object)
    for d in range(1,7):
        a=w[f"L{d}"].to_numpy().astype(bool) if f"L{d}" in w.columns else np.zeros(n,bool)
        dig=np.array([x+str(d) if y else x for x,y in zip(dig,a)],dtype=object)
    out=np.array([('L'+x) if x else '' for x in dig],dtype=object)
    empty=(dig=='')
    for k in FALL:
        if k not in w.columns: continue
        a=w[k].to_numpy().astype(bool)&empty&(out=='')
        out=np.where(a,k,out)
    return out
b=duckdb.connect(BASE,read_only=True); e=duckdb.connect(ENR,read_only=True)
pairs=b.execute("select ticker,universe from bars group by 1,2 order by ticker,universe").df()
T=dict(series=len(pairs),done=0,matched=0,lsig=0,vb=0); per={k:0 for k in BIN}; rec=[]
t0=time.time()
for _,r in pairs.iterrows():
    try:
        df=b.execute("select date,open,high,low,close,volume from bars where ticker=? and universe=? order by date",[r.ticker,r.universe]).df()
        st=e.execute("select date,l_sig,vol_bucket,"+",".join(BIN)+" from bars where ticker=? and universe=? order by date",[r.ticker,r.universe]).df()
        if not len(df) or not len(st): T['done']+=1; continue
        w=compute_wlnbb(df.copy())
        v=lchart(w,len(df))
        m=pd.DataFrame({'date':df.date.values}).merge(st,on='date',how='inner')
        if not len(m): T['done']+=1; continue
        idx=pd.Index(df.date.values).get_indexer(m.date.values); T['matched']+=len(m)
        s=m.l_sig.fillna('').astype(str).to_numpy(); cv=v[idx]
        d=(cv!=s); n=int(d.sum()); T['lsig']+=n
        for j in np.flatnonzero(d)[:99999]: rec.append([r.ticker,r.universe,str(m.date.iloc[j]),'l_sig',str(cv[j]),str(s[j])])
        vb=w['vol_bucket'].astype(str).to_numpy()[idx]; sv=m.vol_bucket.fillna('').astype(str).to_numpy()
        dv=(vb!=sv); T['vb']+=int(dv.sum())
        for j in np.flatnonzero(dv)[:99999]: rec.append([r.ticker,r.universe,str(m.date.iloc[j]),'vol_bucket',str(vb[j]),str(sv[j])])
        for s_,h in BIN.items():
            a=w[h].to_numpy().astype('int8')[idx]; b_=m[s_].astype('int8').to_numpy()
            k=int((a!=b_).sum()); per[s_]+=k
            for j in np.flatnonzero(a!=b_)[:99999]: rec.append([r.ticker,r.universe,str(m.date.iloc[j]),s_,int(a[j]),int(b_[j])])
    except Exception as ex:
        rec.append([r.ticker,r.universe,'ERR',str(ex)[:60],'',''])
    T['done']+=1
    if T['done']%300==0: json.dump(dict(T=T,per=per,rec=rec,elapsed=round(time.time()-t0,1)),open(OUT,'w'))
json.dump(dict(T=T,per=per,rec=rec,elapsed=round(time.time()-t0,1),FINISHED=True),open(OUT,'w'))
print("DONE lsig",T['lsig'],"vol_bucket",T['vb'],"bins",sum(per.values()))
