"""Cross-TF ECHO test for the (Z5|Z11)+wt_evr+L34red → bar+1∈{T3,T9} flagship
(2026-07-22). Fractal criterion (project_edge_echo_crosstf): a REAL edge echoes on
other timeframes. Runs the IDENTICAL setup on 1H / 4H / 15M bars (fwd = N *bars*,
not days — on 1H ≈ N hours). Path-sim trail25/-15/60bar/15bps, entry = next bar open.
Same dedup logic. Compares each TF's per-year robustness to the 1D flagship."""
import duckdb
import numpy as np
import pandas as pd
from collections import defaultdict

def run_tf(tf, dbpath):
    c = duckdb.connect(dbpath, read_only=True)
    # Step 1: find tickers that HAVE a qualifying anchor (Z5|Z11 + wt_evr + red-L34).
    # Keeps the big pull (15M=89M rows) bounded to only relevant tickers.
    atk = c.execute("""
        SELECT DISTINCT ticker FROM bars
        WHERE close>=5 AND z_sig IN ('Z5','Z11') AND wt_evr=1 AND l_sig='L34' AND close<open
    """).fetchdf()["ticker"].tolist()
    if not atk:
        c.close()
        return pd.DataFrame(columns=["ps","yr"])
    ph = ",".join(f"'{t}'" for t in atk)
    df = c.execute(f"""
        WITH deduped AS (
            SELECT * FROM bars WHERE close >= 5 AND ticker IN ({ph})
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

    recs=[]
    # intraday dv gate scaled down (a single 1H/15m bar's $vol is a fraction of a day's);
    # keep it loose so we don't over-thin — use $500k like other intraday studies.
    DV = 5e5
    for i in range(n):
        if zz[i] not in ("Z5","Z11"): continue
        if cl[i]*vol[i] < DV: continue
        if not (wt[i]==1 and ll[i]=="L34" and cl[i]<o[i]): continue
        tl=last[i]
        if i+1>tl: continue
        b1 = tt[i+1] if tt[i+1] else zz[i+1]
        if b1 not in ("T3","T9"): continue
        ps=psim(i+1,tl)
        if ps is None: continue
        recs.append((ps, yr[i+2] if i+2<=tl else yr[i+1]))
    R=pd.DataFrame(recs,columns=["ps","yr"])
    return R

def summ(R,label):
    if len(R)==0:
        print(f"  {label:16} n=0"); return
    pm=R.ps.mean()*100; pmed=R.ps.median()*100; win=(R.ps>0).mean()*100
    yp=sum(1 for y in ["2021","2022","2023","2024","2025","2026"] if (R.yr==y).sum()>=5 and R[R.yr==y].ps.mean()>0)
    yt=sum(1 for y in ["2021","2022","2023","2024","2025","2026"] if (R.yr==y).sum()>=5)
    tr=R[R.yr.isin(["2021","2022","2023"])]; te=R[R.yr.isin(["2024","2025","2026"])]
    ptr=tr.ps.mean()*100 if len(tr) else float("nan"); pte=te.ps.mean()*100 if len(te) else float("nan")
    print(f"  {label:16} n={len(R):5}  ps {pm:+6.2f}%  med {pmed:+6.2f}%  win {win:4.1f}%  TR{ptr:+5.1f} TE{pte:+5.1f}  {yp}/{yt}yr+")
    for y in ["2021","2022","2023","2024","2025","2026"]:
        sub=R[R.yr==y]
        if len(sub)>=5:
            print(f"      {y}: n={len(sub):4} ps {sub.ps.mean()*100:+.2f}% win {(sub.ps>0).mean()*100:.0f}%")

print("═══ CROSS-TF ECHO: (Z5|Z11)+wt_evr+L34red → {T3,T9}, fwd = N bars ═══")
print("  [1D flagship reference: n=142 ps +5.54% med +2.51% win 57.7% 5/6yr, TR+3.0 TE+7.9]\n")
for tf, dbp in [("4H","../data/studio_4h.duckdb"), ("1H","../data/studio_1h.duckdb"), ("15M","../data/studio_15m.duckdb")]:
    print(f"── {tf} ──", flush=True)
    R = run_tf(tf, dbp)
    summ(R, f"{tf} echo")
    print()
