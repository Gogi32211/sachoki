"""L34 STRENGTH DIFFERENTIATION (2026-07-21): which axes separate strong from weak
L34 bars. Axes: color, volume class, CLV (close location), distance-from-low,
campaign membership, RSI zone, price bucket. Each axis: fwd-20 + path-sim per side,
TR/TE. Then an additive GRADE from surviving axes with a monotonicity ladder."""
import numpy as np, pandas as pd, duckdb
S_=0.0015
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,rsi_14,atr_14,
  coalesce(l_sig,'') l, coalesce(vol_bucket,'') vb,
  lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20,
  min(low) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 25 PRECEDING AND 1 PRECEDING) lo25,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
# per-bar path-sim
o=D.open.to_numpy(float);h=D.high.to_numpy(float);lo=D.low.to_numpy(float);c=D.close.to_numpy(float)
tk=D.ticker.to_numpy(); n=len(D); ps=np.full(n,np.nan)
i=0
while i<n:
    j=i
    while j+1<n and tk[j+1]==tk[i]: j+=1
    for b in range(i,j):
        e=o[b+1]*(1+S_)
        if e<=0: continue
        pk=e; hd=e*0.85; end=min(b+61,j+1); r=None
        for q in range(b+1,end):
            if q>b+1 and o[q]<=hd: r=o[q]/e-1-S_; break
            if lo[q]<=hd: r=-0.15-S_; break
            pk=max(pk,h[q]); ts=pk*0.75
            if q>b+1 and o[q]<=ts: r=o[q]/e-1-S_; break
            if lo[q]<=ts: r=ts/e-1-S_; break
        ps[b]=r if r is not None else c[end-1]/e-1-S_
    i=j+1
D['ps']=ps
D['dv']=D.close*D.volume
D['r20']=D.f20/D.close-1
D['yr']=D.date.astype(str).str[:4]
L=D[(D.l=='L34')&(D.dv>=3e6)&D.r20.notna()&D.ps.notna()].copy()
print(f"L34 liquid bars: {len(L):,}")
L['red']=(L.close<L.open)
L['heavy_vol']=L.vb.isin(('B','VB'))
rng_=(L.high-L.low).replace(0,np.nan)
L['clv']=(L.close-L.low)/rng_
L['clv_hi']=L.clv>=0.6
L['near_low']=(L.close/L.lo25-1)<=0.10
L['rsi_z']=pd.cut(L.rsi_14,[0,40,55,100],labels=['os','mid','hi'])
# campaign: prior red-L34 within 20 bars, close within 5%
L2=D[['ticker','date','l','close','open']].copy()
L2['redl']=((L2.l=='L34')&(L2.close<L2.open))
g=L2.groupby('ticker')
L2['last_rl_close']=g.apply(lambda x: x.close.where(x.redl).shift(1).ffill(limit=None)).reset_index(level=0,drop=True)
L2['last_rl_idx']=g.cumcount()
L2['rl_seen']=g['redl'].transform(lambda s: s.shift(1).rolling(20,min_periods=1).max()).fillna(0)
L=L.merge(L2[['ticker','date','last_rl_close','rl_seen']],on=['ticker','date'],how='left')
L['camp']=(L.rl_seen==1)&((L.close/L.last_rl_close-1).abs()<=0.05)&L.red
def rep(label,m):
    s=L[m]
    if len(s)<300: print(f"  {label:22} n={len(s)} too few"); return None
    tr=s[s.yr.isin(('2021','2022','2023'))]; te=s[s.yr.isin(('2024','2025','2026'))]
    print(f"  {label:22} n={len(s):7,} up {100*(s.r20>0).mean():4.1f}% ps {100*s.ps.mean():+5.2f}% "
          f"med {100*s.ps.median():+5.2f}% | psTR {100*tr.ps.mean():+.2f} psTE {100*te.ps.mean():+.2f}")
    return 100*s.ps.mean()
print("\n══ ღერძები ══")
print("─ ფერი:")
rep("წითელი",L.red); rep("მწვანე",~L.red)
print("─ მოცულობის კლასი:")
rep("B/VB",L.heavy_vol); rep("N/L/W",~L.heavy_vol)
print("─ CLV (დახურვა რენჯში):")
rep("მაღალი (≥0.6)",L.clv_hi); rep("დაბალი (<0.6)",~L.clv_hi)
print("─ დაბალთან სიახლოვე (≤10% 25-bar low):")
rep("ფსკერთან",L.near_low); rep("შორს",~L.near_low)
print("─ კამპანია (წინა წითელი-L34 ±5% 20 ბარში):")
rep("კამპანიაში",L.camp.fillna(False)); rep("სოლო",~L.camp.fillna(False))
print("─ RSI ზონა:")
for z in ('os','mid','hi'): rep(f"RSI {z}",L.rsi_z==z)
print("─ ფასი:")
rep("$5-21",(L.close<21)); rep("$21-89",(L.close>=21)&(L.close<89)); rep("$89+",L.close>=89)
# additive grade from axes that separate: red, near_low, clv_hi, heavy_vol?, camp, rsi os
print("\n══ GRADE ladder (ქულა = red + near_low + clv_hi + camp + RSI<40) ══")
L['grade']=(L.red.astype(int)+L.near_low.astype(int)+L.clv_hi.astype(int)
            +L.camp.fillna(False).astype(int)+(L.rsi_14<40).astype(int))
for k in range(0,6):
    s=L[L.grade==k]
    if len(s)<200: print(f"  grade {k}: n={len(s)} too few"); continue
    tr=s[s.yr.isin(('2021','2022','2023'))]; te=s[s.yr.isin(('2024','2025','2026'))]
    yrs=s.groupby('yr').ps.mean()*100
    print(f"  grade {k}: n={len(s):7,} up {100*(s.r20>0).mean():4.1f}% ps {100*s.ps.mean():+5.2f}% med {100*s.ps.median():+5.2f}% "
          f"{int((yrs>0).sum())}/{len(yrs)}yr+ | TR {100*tr.ps.mean():+.2f} TE {100*te.ps.mean():+.2f}")
