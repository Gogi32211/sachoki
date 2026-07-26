"""BE↓ (be_dn = break-of-heavy-L-body DOWN arrow) forward edge, segmented by L_sig
(2026-07-22, user). Same path-sim (trail25/-15/60/15bps, next-open, LONG entry — so a
POSITIVE ps means the down-break bar was actually a bullish reversal, i.e. a fade of the
breakdown). Also reports the SHORT view (−ps) since a down-break is naturally a bear
signal. dv>=3M, non-index, $5+. Per-year + TRAIN/TEST."""
import duckdb
import numpy as np
import pandas as pd
from collections import defaultdict

c = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
df = c.execute("""
    WITH deduped AS (
        SELECT * FROM bars WHERE close >= 5 AND universe <> 'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY
            CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 WHEN 'russell2k' THEN 3 ELSE 4 END) = 1
    )
    SELECT ticker, date, open, high, low, close, volume,
           coalesce(be_dn,0) bedn, coalesce(l_sig,'') ll
    FROM deduped ORDER BY ticker, date
""").fetchdf()
c.close()
o=df.open.to_numpy(float); h=df.high.to_numpy(float); lo_=df.low.to_numpy(float); cl=df.close.to_numpy(float)
vol=df.volume.to_numpy(float); bedn=df.bedn.to_numpy(); ll=df.ll.to_numpy()
yr=df.date.astype(str).str[:4].to_numpy(); tk=df.ticker.to_numpy(); n=len(df)
idx=defaultdict(list)
for i,t in enumerate(tk): idx[t].append(i)
last=np.empty(n,int)
for t,ii in idx.items():
    for i in ii: last[i]=ii[-1]

def psim(s,tl):
    if s>=tl: return None
    e=o[s+1]*(1+0.0015)
    if e<=0: return None
    pk=e; hd=e*0.85; end=min(s+61,tl+1); r=None
    for q in range(s+1,end):
        if q>s+1 and o[q]<=hd: r=o[q]/e-1-0.0015; break
        if lo_[q]<=hd: r=-0.15-0.0015; break
        pk=max(pk,h[q]); ts=pk*0.75
        if q>s+1 and o[q]<=ts: r=o[q]/e-1-0.0015; break
        if lo_[q]<=ts: r=ts/e-1-0.0015; break
    return r if r is not None else cl[end-1]/e-1-0.0015

mask=(bedn==1)&(cl*vol>=3e6)
ps=np.full(n,np.nan)
for i in np.nonzero(mask)[0]:
    ps[i]=psim(i,last[i]) if i<last[i] else np.nan
R=pd.DataFrame({"ps":ps,"yr":yr,"ll":ll,"o":o,"c":cl})[mask & ~np.isnan(ps)]
print(f"BE↓ bars (dv>=3M, forward data): {len(R)}")
print("(ps = LONG next-open. +ps → down-break was a bullish fade; −ps → breakdown followed through / short works)\n")

def block(sub,label):
    n=len(sub)
    if n<30:
        print(f"  {label:22} n={n:5}  ⚠ n<30"); return
    pm=sub.ps.mean()*100; pmed=sub.ps.median()*100; win=(sub.ps>0).mean()*100
    tr=sub[sub.yr.isin(["2021","2022","2023"])]; te=sub[sub.yr.isin(["2024","2025","2026"])]
    ptr=tr.ps.mean()*100 if len(tr)>=20 else float("nan"); pte=te.ps.mean()*100 if len(te)>=20 else float("nan")
    yp=sum(1 for y in ["2021","2022","2023","2024","2025","2026"] if (sub.yr==y).sum()>=10 and sub[sub.yr==y].ps.mean()>0)
    yt=sum(1 for y in ["2021","2022","2023","2024","2025","2026"] if (sub.yr==y).sum()>=10)
    print(f"  {label:22} n={n:5}  LONG ps {pm:+6.2f}%  med {pmed:+6.2f}%  win {win:4.1f}%  TR{ptr:+5.1f} TE{pte:+5.1f}  {yp}/{yt}yr+ | SHORT ps {-pm:+.2f}%")

print("═══ BASELINE ═══")
block(R, "BE↓ (any L)")
print("\n═══ BE↓ by L_sig on the same bar ═══")
for L in ["L3","L46","L34","L12","L25","L5"]:
    block(R[R.ll==L], f"{L}+BE↓")
block(R[R.ll==""], "(no L)+BE↓")
print("\n═══ L34/L3 RED vs GREEN ═══")
block(R[(R.ll=="L34")&(R.c<R.o)], "L34red+BE↓")
block(R[(R.ll=="L34")&(R.c>=R.o)], "L34green+BE↓")
block(R[(R.ll=="L3")&(R.c<R.o)], "L3red+BE↓")
block(R[(R.ll=="L3")&(R.c>=R.o)], "L3green+BE↓")
