"""RIDE-signature validation. Context = in-run bars (close >= +15% over 20 bars).
Signature ON = fly_sig non-empty AND cci_20>100 AND rsi_14>70.
  A) continuation: fwd10 close-return, ON vs OFF days (within runs), per era.
  B) exit timing: first BREAK after >=3 consecutive ON days → fwd10 after the break
     vs fwd10 on continuing-ON days. If break-days fade, the break is the exit bell."""
import numpy as np, pandas as pd, duckdb
a=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,close,rsi_14,cci_20,
  CASE WHEN coalesce(fly_sig,'')<>'' THEN 1 ELSE 0 END fly,close*volume dv,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D["date"]=D["date"].astype(str).str[:10]
rows=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<60: continue
    cl=g.close.to_numpy(float); rs=g.rsi_14.to_numpy(float); cc=g.cci_20.to_numpy(float)
    fl=g.fly.to_numpy(int); dv=g.dv.to_numpy(float); ds=g.date.tolist()
    run=np.zeros(n,bool); run[20:]=cl[20:]>=cl[:-20]*1.15
    sig=(fl==1)&(cc>100)&(rs>70)
    onstreak=0
    for i in range(20,n-10):
        if not run[i] or dv[i]<3e6: onstreak=sig[i]*max(onstreak,0)+ (1 if sig[i] else 0) if False else (onstreak+1 if sig[i] else 0); continue
        fwd10=cl[i+10]/cl[i]-1
        kind=None
        if sig[i]:
            onstreak+=1
            kind='ON' if onstreak>=1 else None
        else:
            kind='BREAK' if onstreak>=3 else 'OFF'
            onstreak=0
        rows.append((ds[i][:4],kind,onstreak,fwd10))
R=pd.DataFrame(rows,columns=["yr","kind","streak","fwd10"])
print(f"in-run bars: {len(R)}  (ON {sum(R.kind=='ON')} · OFF {sum(R.kind=='OFF')} · BREAK {sum(R.kind=='BREAK')})\n")
def rep(lab,s):
    if len(s)<200: print(f"{lab:12} n={len(s)} too few"); return
    tr=s[s.yr.isin(['2021','2022','2023'])]; te=s[s.yr.isin(['2024','2025','2026'])]
    print(f"{lab:12} n={len(s):7}  fwd10 mean {s.fwd10.mean()*100:+.2f}%  med {s.fwd10.median()*100:+.2f}%  P(fwd10>0) {(s.fwd10>0).mean()*100:.0f}%  | TRAIN {tr.fwd10.mean()*100:+.2f}  TEST {te.fwd10.mean()*100:+.2f}")
rep("ON (ride)",R[R.kind=='ON'])
rep("OFF (no sig)",R[R.kind=='OFF'])
rep("BREAK (≥3→off)",R[R.kind=='BREAK'])
print("\nON by streak length (does a longer ride streak still continue?):")
on=R[R.kind=='ON']
for lo,hi,lab in [(1,3,'1-2'),(3,6,'3-5'),(6,11,'6-10'),(11,999,'11+')]:
    s=on[(on.streak>=lo)&(on.streak<hi)]
    if len(s)>=200: print(f"  streak {lab:5} n={len(s):6}  fwd10 {s.fwd10.mean()*100:+.2f}%  med {s.fwd10.median()*100:+.2f}%")
