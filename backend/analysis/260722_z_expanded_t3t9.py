"""Expand the anchor Z-set: add Z3 + Z4 to the (Z5|Z11) flagship (2026-07-22, user).
Setup: anchor Z in {…} + wt_evr + red-L34 → bar+1 ∈ {T3,T9} → enter (next-open).
Tests each Z-code SOLO first (does Z3/Z4 carry the same edge as Z5/Z11?), then the
pooled expanded set, with full per-year / TRAIN-TEST / z-control. Path-sim trail25/
-15/60/15bps. dv>=3M, non-index, $5+."""
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
           coalesce(t_sig,'') tt, coalesce(z_sig,'') zz, coalesce(l_sig,'') ll,
           coalesce(wt_evr,0) wt
    FROM deduped ORDER BY ticker, date
""").fetchdf()
c.close()
o=df.open.to_numpy(float); h=df.high.to_numpy(float); lo_=df.low.to_numpy(float); cl=df.close.to_numpy(float)
vol=df.volume.to_numpy(float); tt=df.tt.to_numpy(); zz=df.zz.to_numpy(); ll=df.ll.to_numpy(); wt=df.wt.to_numpy()
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

def collect(zset, confirm={"T3","T9"}):
    recs=[]
    for i in range(n):
        if zz[i] not in zset: continue
        if cl[i]*vol[i] < 3e6: continue
        if not (wt[i]==1 and ll[i]=="L34" and cl[i]<o[i]): continue
        tl=last[i]
        if i+1>tl: continue
        b1 = tt[i+1] if tt[i+1] else zz[i+1]
        if b1 not in confirm: continue
        ps=psim(i+1,tl)
        if ps is None: continue
        recs.append((ps, yr[i+2] if i+2<=tl else yr[i+1], zz[i], cl[i+1]))
    return pd.DataFrame(recs, columns=["ps","yr","az","px"])

def summ(R,label,peryear=False):
    if len(R)==0:
        print(f"  {label:32} n=0"); return
    pm=R.ps.mean()*100; pmed=R.ps.median()*100; win=(R.ps>0).mean()*100
    tr=R[R.yr.isin(["2021","2022","2023"])]; te=R[R.yr.isin(["2024","2025","2026"])]
    ptr=tr.ps.mean()*100 if len(tr) else float("nan"); pte=te.ps.mean()*100 if len(te) else float("nan")
    yp=sum(1 for y in ["2021","2022","2023","2024","2025","2026"] if (R.yr==y).sum()>=5 and R[R.yr==y].ps.mean()>0)
    yt=sum(1 for y in ["2021","2022","2023","2024","2025","2026"] if (R.yr==y).sum()>=5)
    print(f"  {label:32} n={len(R):4}  ps {pm:+6.2f}%  med {pmed:+6.2f}%  win {win:4.1f}%  TR{ptr:+5.1f} TE{pte:+5.1f}  {yp}/{yt}yr+")
    if peryear:
        for y in ["2021","2022","2023","2024","2025","2026"]:
            s=R[R.yr==y]
            if len(s)>=3: print(f"      {y}: n={len(s):3} ps {s.ps.mean()*100:+.2f}% med {s.ps.median()*100:+.2f}% win {(s.ps>0).mean()*100:.0f}%")

print("═══ each Z-code SOLO (anchor Z + wt_evr+L34red → T3/T9) ═══")
for z in ["Z3","Z4","Z5","Z11"]:
    summ(collect({z}), f"{z} solo")

print("\n═══ pooled sets ═══")
summ(collect({"Z5","Z11"}),            "Z5|Z11 (current flagship)", peryear=True)
summ(collect({"Z5","Z11","Z3"}),       "+Z3 → Z3|Z5|Z11")
summ(collect({"Z5","Z11","Z4"}),       "+Z4 → Z4|Z5|Z11")
summ(collect({"Z3","Z4","Z5","Z11"}),  "+Z3+Z4 → Z3|Z4|Z5|Z11", peryear=True)

print("\n═══ z-control on the expanded Z3|Z4|Z5|Z11 set ═══")
# population = all bar+1 entries from these anchors regardless of confirm code
def pool_all(zset):
    ps_list=[]
    for i in range(n):
        if zz[i] not in zset: continue
        if cl[i]*vol[i] < 3e6: continue
        if not (wt[i]==1 and ll[i]=="L34" and cl[i]<o[i]): continue
        tl=last[i]
        if i+1>tl: continue
        b1 = tt[i+1] if tt[i+1] else zz[i+1]
        if b1 in ("",): continue
        ps=psim(i+1,tl)
        if ps is not None: ps_list.append(ps)
    return np.array(ps_list)
exp = collect({"Z3","Z4","Z5","Z11"})
pool = pool_all({"Z3","Z4","Z5","Z11"})
rng=np.random.default_rng(13)
rm=np.array([100*rng.choice(pool,len(exp),replace=False).mean() for _ in range(2000)])
z=(exp.ps.mean()*100-rm.mean())/rm.std()
print(f"  expanded n={len(exp)} ps {exp.ps.mean()*100:+.2f}% vs random(any bar+1) {rm.mean():+.2f}% (sd {rm.std():.2f}) → z={z:+.2f}, top {100*np.mean(rm>=exp.ps.mean()*100):.1f}%")
