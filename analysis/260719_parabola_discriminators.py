"""What separates the re-ignition events that DID go parabolic from those that didn't?
Events: the 36k strict re-ignition population (spike→hold→quiet). Label by forward MFE60:
  WINNER  = max high in next 60 bars >= +50% over entry
  LOSER   = MFE60 < +15%   (middle excluded to sharpen contrast)
Features: causal at D, on 1D + 4H/1H/15M day-aggregates (5-day pre-window).
Metric: rank AUC winner-vs-loser, computed SEPARATELY on TRAIN 21-23 and TEST 24-26 —
a real discriminator must hold in BOTH eras. (Prior art warns: winner anatomy is usually
survivor bias — this is the honest test of that.)"""
import numpy as np, pandas as pd, duckdb, sys
print("loading daily...",flush=True)
a=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,avg_vol_20d,
  rsi_14,cci_20,turbo_score,beta_score,prebreak_v2 v2,buy_score,ultra_score us,
  atr_14,close*volume dv,coalesce(sig_conso,0) conso,coalesce(sig_any_p,0) anyp,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D["date"]=D["date"].astype(str).str[:10]
# market breadth per day (share of names above their 20d-ago close)
print("breadth...",flush=True)
D["_up20"]=D.groupby("ticker")["close"].transform(lambda s: s>s.shift(20))
breadth=D.groupby("date")["_up20"].mean()
def day_aggs(db):
    c=duckdb.connect(f'../data/studio_{db}.duckdb',read_only=True)
    df=c.execute("""SELECT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d,
        min(rsi_14) rlo, max(turbo_score) tmax
        FROM bars WHERE close>=5 GROUP BY 1,2""").fetchdf()
    c.close()
    return {(t,d_):(r,tm) for t,d_,r,tm in zip(df.ticker,df.d,df.rlo,df.tmax)}
print("4h aggs...",flush=True); A4=day_aggs('4h'); print(len(A4),flush=True)
print("1h aggs...",flush=True); A1=day_aggs('1h'); print(len(A1),flush=True)
print("15m aggs...",flush=True); A15=day_aggs('15m'); print(len(A15),flush=True)

rows=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<120: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    rs=g.rsi_14.to_numpy(float); cc=g.cci_20.to_numpy(float)
    tb=g.turbo_score.to_numpy(float); v=g.volume.to_numpy(float); av=g.avg_vol_20d.to_numpy(float)
    dv=g.dv.to_numpy(float); ds=g.date.tolist(); atr=g.atr_14.to_numpy(float)
    beta=g.beta_score.to_numpy(float); v2=g.v2.to_numpy(float); bs=g.buy_score.to_numpy(float)
    us=g.us.to_numpy(float); conso=g.conso.to_numpy(float); anyp=g.anyp.to_numpy(float)
    lastD=-99
    for s in np.flatnonzero(tb>=50):
        if s+5>=n or s<60: continue
        for d_ in range(s+4,min(s+10,n-61)):
            if d_-lastD<5: continue
            if not (cl[s+1:d_+1].min()>=cl[s]*0.97 and np.nanmin(rs[s+1:d_+1])>48
                    and np.nanmin(cc[s+1:d_+1])>0): continue
            if not ((v[d_]<=0.8*max(av[d_],1)) and tb[d_]<=10) or dv[d_]<3e6: continue
            entry=o[d_+1]
            if entry<=0: continue
            mfe=hi[d_+1:d_+61].max()/entry-1.0
            wdays=ds[max(0,d_-4):d_+1]
            def agg(A):
                rl=[A[(tk,x)][0] for x in wdays if (tk,x) in A]
                tm=[A[(tk,x)][1] for x in wdays if (tk,x) in A]
                return (min(rl) if rl else np.nan, max(tm) if tm else np.nan)
            r4,t4=agg(A4); r1,t1=agg(A1); r15,t15=agg(A15)
            hi120=hi[max(0,d_-120):d_+1].max()
            lo120=lo[max(0,d_-120):d_+1].min()
            rows.append(dict(tk=tk,d=ds[d_],yr=ds[d_][:4],mfe=mfe,
                px=cl[d_], dv=dv[d_], rsi=rs[d_], cci=cc[d_],
                spike_gain=cl[s]/cl[s-1]-1 if s>0 else np.nan,
                spike_turbo=tb[s], hold_len=d_-s,
                held_gain=cl[d_]/cl[s]-1,
                off_high=cl[d_]/hi120-1,                # distance from 6m high
                range_pos=(cl[d_]-lo120)/max(hi120-lo120,1e-9),
                atr_pct=atr[d_]/cl[d_]*100,
                ret60=cl[d_]/cl[d_-60]-1, ret20=cl[d_]/cl[d_-20]-1,
                beta=beta[d_], v2=v2[d_], buy=bs[d_], ultra_max5=np.nanmax(us[d_-4:d_+1]),
                conso_streak=conso[max(0,d_-9):d_+1].sum(),
                p_in_win=anyp[max(0,d_-9):d_+1].sum(),
                breadth=breadth.get(ds[d_],np.nan)*100,
                rsi4_min=r4, turbo4_max=t4, rsi1_min=r1, turbo1_max=t1,
                rsi15_min=r15, turbo15_max=t15,
                vdry=v[d_]/max(av[d_],1)))
            lastD=d_; break
E=pd.DataFrame(rows)
print(f"\nevents {len(E)}  winners(MFE>=50%) {(E.mfe>=0.5).sum()}  losers(<15%) {(E.mfe<0.15).sum()}",flush=True)
E["lab"]=np.where(E.mfe>=0.5,1,np.where(E.mfe<0.15,0,-1))
E=E[E.lab>=0]
FEATS=[c for c in E.columns if c not in ("tk","d","yr","mfe","lab")]
def auc(x,y):
    m=~(np.isnan(x)); x,y=x[m],y[m]
    if y.sum()<15 or (1-y).sum()<15: return np.nan
    r=pd.Series(x).rank().to_numpy()
    n1=y.sum(); n0=len(y)-n1
    return (r[y==1].sum()-n1*(n1+1)/2)/(n1*n0)
print(f"\n{'feature':res14} ", end="") if False else None
print(f"\n{'feature':16}{'TRAIN AUC':>10}{'TEST AUC':>10}{'win-med':>10}{'lose-med':>10}   (AUC>0.5 = higher→parabola)")
print("-"*72)
res=[]
for f in FEATS:
    tr=E[E.yr.isin(['2021','2022','2023'])]; te=E[E.yr.isin(['2024','2025','2026'])]
    a_tr=auc(tr[f].to_numpy(float),tr.lab.to_numpy()); a_te=auc(te[f].to_numpy(float),te.lab.to_numpy())
    res.append((f,a_tr,a_te,E[E.lab==1][f].median(),E[E.lab==0][f].median()))
res.sort(key=lambda x: -abs(((x[1] or .5)+(x[2] or .5))/2-0.5))
for f,atr_,ate_,mw,ml in res:
    star=' ★' if atr_==atr_ and ate_==ate_ and (atr_-0.5)*(ate_-0.5)>0 and min(abs(atr_-0.5),abs(ate_-0.5))>=0.05 else ''
    print(f"{f:16}{atr_ if atr_==atr_ else float('nan'):>10.3f}{ate_ if ate_==ate_ else float('nan'):>10.3f}{mw:>10.2f}{ml:>10.2f}{star}")

E.to_parquet('/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/5b6f6b5f-eb52-4041-9fed-b0cbcf6a28fc/scratchpad/reignite_events.parquet',index=False)
print("\n=== ATR-CONTROLLED (within top-ATR tercile only — kills the mechanical MFE confound)")
q=E.atr_pct.quantile([1/3,2/3])
hiATR=E[E.atr_pct>=q.iloc[1]]
print(f"top-ATR tercile: n={len(hiATR)}  winners {int((hiATR.lab==1).sum())}")
print(f"{'feature':16}{'TRAIN AUC':>10}{'TEST AUC':>10}")
res2=[]
for f in FEATS:
    if f=='atr_pct': continue
    tr=hiATR[hiATR.yr.isin(['2021','2022','2023'])]; te=hiATR[hiATR.yr.isin(['2024','2025','2026'])]
    a_tr=auc(tr[f].to_numpy(float),tr.lab.to_numpy()); a_te=auc(te[f].to_numpy(float),te.lab.to_numpy())
    res2.append((f,a_tr,a_te))
res2.sort(key=lambda x: -abs(((x[1] or .5)+(x[2] or .5))/2-0.5))
for f,atr_,ate_ in res2[:12]:
    star=' ★' if atr_==atr_ and ate_==ate_ and (atr_-0.5)*(ate_-0.5)>0 and min(abs(atr_-0.5),abs(ate_-0.5))>=0.04 else ''
    print(f"{f:16}{atr_:>10.3f}{ate_:>10.3f}{star}")
