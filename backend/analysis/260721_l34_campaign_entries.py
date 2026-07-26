"""(2026-07-21) Three questions in one:
 A) all-red L34-campaign → REV-turn <=3 bars (enter at the turn)
 B) red-L34 TRIPLE baseline (rev & red-L34 & ▲4H, enter next open)
 C) TRIPLE → BO↑ entry gate (wait for a close above the L34 bar body-top <=5 bars) —
    MATCHED against D) immediate entry on the SAME fires (fair comparison)
 E) campaign → BO↑ (user's old observation, no REV required)
Path-sim trail25/-15/60/15bps, cooldown-5, per-year, TR/TE."""
import numpy as np, pandas as pd, duckdb
S_=0.0015
def sim(o,hi,lo,cl,start):
    n=len(cl); e=o[start]*(1+S_)
    if e<=0 or start>=n: return None
    pk=e; hd=e*0.85; end=min(start+60,n)
    for j in range(start,end):
        if o[j]<=hd and j>start: return o[j]/e-1-S_
        if lo[j]<=hd: return -0.15-S_
        pk=max(pk,hi[j]); ts=pk*0.75
        if o[j]<=ts and j>start: return o[j]/e-1-S_
        if lo[j]<=ts: return ts/e-1-S_
    return cl[end-1]/e-1-S_
c4=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_4h.duckdb',read_only=True)
r4=c4.execute("""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d FROM (
  SELECT ticker,date,rsi_14,close,
    MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) cp,
    LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
  FROM bars WHERE close>=5)
 WHERE m5<38 AND rsi_14 BETWEEN 30 AND 55 AND close>cp AND rsi_14>rp""").fetchdf()
c4.close(); C4=set(zip(r4.ticker,r4.d))
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,rsi_14,beta_score,
  coalesce(l_sig,'') l,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D['dv']=D.close*D.volume
res={k:[] for k in ("A camp→REV","B TRIPLE now","C TRIPLE→BO","D TRIPLE match","E camp→BO")}
conv={"C":0,"Cmiss":0,"E":0,"Emiss":0,"A":0,"Amiss":0}
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<60: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    rs=g.rsi_14.to_numpy(float); bt=g.beta_score.to_numpy(float)
    dv=g.dv.to_numpy(float); ds=g.date.astype(str).str[:10].tolist()
    isl=(g.l=='L34').to_numpy(); red=isl&(cl<o)
    m5=pd.Series(rs).rolling(5,min_periods=2).min().shift(1).to_numpy()
    pc=np.concatenate([[np.nan],cl[:-1]]); prs=np.concatenate([[np.nan],rs[:-1]])
    rev=np.zeros(n,bool)
    for i in range(1,n):
        rev[i]=(m5[i]==m5[i]) and m5[i]<38 and 30<=rs[i]<=55 and cl[i]>pc[i] and rs[i]>prs[i] and bt[i]<=13
    idx=np.nonzero(isl)[0]
    last={k:-99 for k in res}
    for i in range(25,n-6):
        if dv[i]<3e6: continue
        # campaign completer (all-red W20 N2 band5%)
        camp=False
        if red[i]:
            prev=[j for j in idx if i-20<=j<i and red[j]]
            cb=prev+[i]
            if len(cb)>=2:
                px=cl[cb]
                camp = px.max()/px.min()-1<=0.05
        if camp:
            # A: REV within 3 bars after completer
            hitA=None
            for k in range(i+1,min(i+4,n)):
                if rev[k]: hitA=k; break
            if hitA is not None and hitA+1<n and hitA-last["A camp→REV"]>=5:
                r=sim(o,hi,lo,cl,hitA+1)
                if r is not None: res["A camp→REV"].append((ds[hitA][:4],r)); last["A camp→REV"]=hitA; conv["A"]+=1
            elif hitA is None: conv["Amiss"]+=1
            # E: BO (close above campaign body-top) within 5 bars
            lvl=max(max(o[j],cl[j]) for j in cb)
            hitE=None
            for k in range(i+1,min(i+6,n)):
                if cl[k]>lvl: hitE=k; break
            if hitE is not None and hitE+1<n and hitE-last["E camp→BO"]>=5:
                r=sim(o,hi,lo,cl,hitE+1)
                if r is not None: res["E camp→BO"].append((ds[hitE][:4],r)); last["E camp→BO"]=hitE; conv["E"]+=1
            elif hitE is None: conv["Emiss"]+=1
        # TRIPLE: rev & red-L34 & ▲4H same bar
        if rev[i] and red[i] and (tk,ds[i]) in C4:
            if i-last["B TRIPLE now"]>=5:
                r=sim(o,hi,lo,cl,i+1)
                if r is not None: res["B TRIPLE now"].append((ds[i][:4],r)); last["B TRIPLE now"]=i
            # BO gate: close above THIS L34 bar's body top within 5 bars
            lvl=max(o[i],cl[i])
            hit=None
            for k in range(i+1,min(i+6,n)):
                if cl[k]>lvl: hit=k; break
            if hit is not None and hit+1<n:
                conv["C"]+=1
                if hit-last["C TRIPLE→BO"]>=5:
                    r=sim(o,hi,lo,cl,hit+1)
                    r2=sim(o,hi,lo,cl,i+1)
                    if r is not None: res["C TRIPLE→BO"].append((ds[hit][:4],r)); last["C TRIPLE→BO"]=hit
                    if r2 is not None: res["D TRIPLE match"].append((ds[i][:4],r2))
            else: conv["Cmiss"]+=1
print(f"კონვერსია: TRIPLE→BO {conv['C']}/{conv['C']+conv['Cmiss']} · camp→REV {conv['A']}/{conv['A']+conv['Amiss']} · camp→BO {conv['E']}/{conv['E']+conv['Emiss']}\n")
for k,T in res.items():
    T=pd.DataFrame(T,columns=["yr","ret"])
    if len(T)<60: print(f"{k:16} n={len(T)} too few"); continue
    yrs=T.groupby('yr').ret.mean()*100
    tr=T[T.yr.isin(('2021','2022','2023'))]; te=T[T.yr.isin(('2024','2025','2026'))]
    trm=100*tr.ret.mean() if len(tr)>=25 else float('nan')
    tem=100*te.ret.mean() if len(te)>=25 else float('nan')
    pfd=-T.ret[T.ret<=0].sum(); pf=T.ret[T.ret>0].sum()/pfd if pfd>0 else float('nan')
    print(f"{k:16} n={len(T):5,} mean {100*T.ret.mean():+.2f}% med {100*T.ret.median():+.2f}% "
          f"win {100*(T.ret>0).mean():.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr | TR {trm:+.2f} TE {tem:+.2f} | "
          +" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items()))
