import sys, warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np, json, time
from signal_engine import compute_g_signals
from combo_engine import compute_tz_state
from vabs_engine import compute_vabs
from cisd_engine import compute_cisd
from studio.enricher import _compute_body_wick, _compute_gap_range
BASE='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m_base.duckdb'
ENR='/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m.duckdb'
OUT='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/r16_census.json'
LEAN=['sig_vol_5x','sig_vol_10x','sig_vol_20x','sig_g1','sig_g2','sig_g4','sig_g11','vbo_up','sig_tz_flip']
ENRC=['rsi_14','bar_body_wick','bar_gap_range','sig_cisd_cplus','sig_cisd_cplus_minus','sig_cisd_minus_struct','sig_cisd_mpm']
CIS={'sig_cisd_cplus':'PLUS_CISD','sig_cisd_cplus_minus':'CISD_PPM','sig_cisd_minus_struct':'MINUS_STRUCT','sig_cisd_mpm':'CISD_MPM'}
ALL=LEAN+ENRC
b=duckdb.connect(BASE,read_only=True); e=duckdb.connect(ENR,read_only=True)
pairs=b.execute("select ticker,universe from bars group by 1,2 order by ticker,universe").df()
cmp_rows={c:0 for c in ALL}; mism={c:0 for c in ALL}; rec=[]
T=dict(series=len(pairs),done=0,invalid=0); t0=time.time()
def atr(d):
    h,l,c=d.high,d.low,d.close; pc=c.shift(1)
    tr=pd.concat([(h-l).abs(),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
    return tr.ewm(alpha=1.0/14,adjust=False).mean()
for _,r in pairs.iterrows():
    tk,uni=r.ticker,r.universe
    try:
        bb=b.execute("select date,open,high,low,close,volume from bars where ticker=? and universe=? order by date",[tk,uni]).df()
        en=e.execute(f"select date,open,high,low,close,volume,atr_14,{','.join(ALL)} from bars where ticker=? and universe=? order by date",[tk,uni]).df()
        if len(bb)<3 or len(en)<3: T['done']+=1; continue
        m=pd.DataFrame({'date':bb.date.values}).merge(en,on='date',how='inner')
        if len(m)==0: T['done']+=1; continue
        ib=pd.Index(bb.date.values).get_indexer(m.date.values)
        ie=pd.Index(en.date.values).get_indexer(m.date.values)
        def chk(col,vals,idx,as_str=False):
            v=vals[idx]; s=m[col]
            if as_str:
                v=pd.Series(v).fillna('').astype(str).to_numpy(); s=s.fillna('').astype(str).to_numpy()
                bad=(v!=s)
            elif col=='rsi_14':
                s=s.to_numpy(); bad=(np.abs(np.nan_to_num(v)-np.nan_to_num(s))>1e-9)
            else:
                v=pd.Series(v).fillna(0).astype('int8').to_numpy(); s=s.fillna(0).astype('int8').to_numpy()
                bad=(v!=s)
            cmp_rows[col]+=len(m); n=int(bad.sum()); mism[col]+=n
            if n:
                for j in np.flatnonzero(bad)[:99999]:
                    rec.append([tk,uni,str(m.date.iloc[j]),col,str(v[j]),str(s[j])])
        # LEAN
        vr=(bb.volume/bb.volume.shift(1).replace(0,np.nan)).fillna(0)
        for col,k in zip(['sig_vol_5x','sig_vol_10x','sig_vol_20x'],[5,10,20]):
            chk(col,(vr>=k).astype('int8').to_numpy(),ib)
        g=compute_g_signals(bb[['open','high','low','close']].copy())
        for col in ['sig_g1','sig_g2','sig_g4','sig_g11']:
            chk(col,g[col.replace('sig_','')].to_numpy().astype('int8'),ib)
        va=compute_vabs(bb.copy()); chk('vbo_up',va['vbo_up'].fillna(0).astype('int8').to_numpy(),ib)
        ts=pd.Series(np.asarray(compute_tz_state(bb.copy()))).fillna(0).astype(int)
        chk('sig_tz_flip',((ts==3)&(ts.shift(1).fillna(0).astype(int)!=3)).astype('int8').to_numpy(),ib)
        # ENRICHED
        c_=en.close; d_=c_.diff(); up=d_.clip(lower=0); dn=(-d_).clip(lower=0)
        ru=up.ewm(alpha=1.0/14,adjust=False,min_periods=1).mean(); rd=dn.ewm(alpha=1.0/14,adjust=False,min_periods=1).mean()
        chk('rsi_14',(100.0-100.0/(1.0+ru/rd.replace(0,1e-10))).round(1).to_numpy(),ie)
        src=en.copy()
        out=_compute_gap_range(_compute_body_wick(src))
        chk('bar_body_wick',out['bar_body_wick'].to_numpy(),ie,as_str=True)
        chk('bar_gap_range',out['bar_gap_range'].to_numpy(),ie,as_str=True)
        ci=compute_cisd(en[['open','high','low','close','volume']].copy())
        for sc,pc in CIS.items():
            chk(sc,ci[pc].fillna(0).astype('int8').to_numpy(),ie)
    except Exception as ex:
        T['invalid']+=1; rec.append([tk,uni,'ERR',str(ex)[:70],'',''])
    T['done']+=1
    if T['done']%250==0: json.dump(dict(T=T,cmp=cmp_rows,mism=mism,rec=rec,elapsed=round(time.time()-t0,1)),open(OUT,'w'))
json.dump(dict(T=T,cmp=cmp_rows,mism=mism,rec=rec,elapsed=round(time.time()-t0,1),FINISHED=True),open(OUT,'w'))
print("DONE",sum(mism.values()),"invalid",T['invalid'])
