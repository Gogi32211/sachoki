"""doublegap_backtest.py — ANALYSIS ONLY. Promotion-trail for the double-gap 4-bar:
[Z1G(-1)] -> [T1/T1G(0)] -> [Z1G(+1)] -> ENTER bar+2 OPEN. Path-aware exit (gap fills,
stop-first), $500k entry-$vol floor, glitch screen, OOS time-split, per-year.
Reuses exit_backtest.sim / is_glitch. Percent units, per-universe. No production code."""
import sys, duckdb, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/analysis")
from exit_backtest import sim, is_glitch
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT="/Users/sachoki/Desktop/sachoki-desktop/DOUBLEGAP_BACKTEST.md"
UNIS=("sp500","nasdaq","russell2k")
DVFLOOR=500_000; OOS_FROM="2024-09-01"; DEDUP=5
con=duckdb.connect(DB,read_only=True)

# 1. double-gap signal bars (causal: lag/lead of z_sig)
sig=con.execute("""
  WITH w AS (
    SELECT universe,ticker,date,t_sig,
      lag(z_sig,1) OVER p AS pz, lead(z_sig,1) OVER p AS nz,
      row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn
    FROM bars WINDOW p AS (PARTITION BY universe,ticker ORDER BY date)
  )
  SELECT universe,ticker,date FROM w
  WHERE t_sig IN ('T1','T1G') AND pz='Z1G' AND nz='Z1G' AND rn=1
""").fetchdf()
print("double-gap signal bars:", len(sig), dict(sig.universe.value_counts()))
tickers=sorted(sig.ticker.unique())

# 2. store for those tickers
ph=",".join("?"*len(tickers))
bars=con.execute(f"""SELECT universe,ticker,date,open,high,low,close,volume
  FROM bars WHERE ticker IN ({ph}) ORDER BY universe,ticker,date""",tickers).fetchdf()
con.close()
bars=bars.drop_duplicates(["universe","ticker","date"]).reset_index(drop=True)
bars["dv"]=bars.close*bars.volume
store={}
for (u,t),s in bars.groupby(["universe","ticker"],sort=False):
    s=s.reset_index(drop=True)
    store[(u,t)]=dict(O=s.open.to_numpy(float),H=s.high.to_numpy(float),L=s.low.to_numpy(float),
                      C=s.close.to_numpy(float),DV=s.dv.to_numpy(float),
                      idx={d:k for k,d in enumerate(s.date.to_numpy())})

# 3. build entries (entry index for sim = i+1 -> sim enters at i+2 open)
def entries(sub):
    out=[]
    for r in sub.itertuples():
        rec=store.get((r.universe,r.ticker))
        if rec is None: continue
        i=rec["idx"].get(np.datetime64(r.date))
        if i is None or i+1>=len(rec["C"]): continue
        out.append((r.universe,r.ticker,i+1,r.date))   # sim idx = i+1 -> enter i+2 open
    return out

def run(eps,stop,target,hz,trail=None,floor=DVFLOOR,glitch=True):
    rets=[]; meta=[]; last={}
    for u,t,idx,d in sorted(eps,key=lambda x:(x[1],x[3])):
        if idx-last.get((u,t),-99)<=DEDUP: continue        # episode dedup
        rec=store[(u,t)]
        if glitch and is_glitch(rec,idx,hz): continue
        res=sim(rec,idx,stop,target,hz,trailing=trail,entry_mode="next_open")
        if res is None or not res.get("dv") or res["dv"]<floor: continue
        last[(u,t)]=idx; rets.append(res["r"]); meta.append((u,d))
    return np.array(rets),meta

def stats(r):
    if len(r)==0: return None
    return dict(n=len(r),exp=round(float(r.mean()),2),med=round(float(np.median(r)),2),
                win=round(float((r>0).mean()*100),1),p50=round(float((r>=50).mean()*100),1),
                maxloss=round(float(r.min()),1))

md=["# Double-gap 4-bar — promotion backtest","",
    "_[Z1G(−1)]→[T1/T1G(0)]→[Z1G(+1)]→ შესვლა bar+2 OPEN. gap-aware exit, stop-first, "
    "$500k floor, glitch-screen, episode-dedup. OOS split 2024-09-01. პროცენტი._\n",
    f"_double-gap სიგნალ-ბარები: {len(sig)} ({dict(sig.universe.value_counts())})_\n"]

# grid per universe
md.append("## Exit grid (entry bar+2 open, horizon 10d)\n")
md.append("| universe | config | n | EXPECT | med | win% | P(+50%) | maxloss |\n|---|---|---|---|---|---|---|---|")
CFG=[("s15/t100",15,100,None),("s12/t50",12,50,None),("s20/t100",20,100,None),("trail20",None,None,20)]
for u in UNIS:
    sub=sig[sig.universe==u]; eps=entries(sub)
    for nm,s,tg,tr in CFG:
        st=stats(run(eps,s,tg,10,trail=tr)[0])
        if st: md.append(f"| {u} | {nm} | {st['n']} | **{st['exp']}** | {st['med']} | {st['win']} | {st['p50']} | {st['maxloss']} |")
        else:  md.append(f"| {u} | {nm} | 0 | — | — | — | — | — |")

# IS/OOS + per-year for s15/t100
md.append("\n## OOS split + per-year (s15/t100, entry bar+2 open)\n")
md.append("| universe | IS EXPECT (n) | OOS EXPECT (n) |\n|---|---|---|")
for u in UNIS:
    eps=entries(sig[sig.universe==u]); r,meta=run(eps,15,100,10)
    isr=np.array([x for x,(uu,d) in zip(r,meta) if str(d)<OOS_FROM])
    oor=np.array([x for x,(uu,d) in zip(r,meta) if str(d)>=OOS_FROM])
    si,so=stats(isr),stats(oor)
    md.append(f"| {u} | {si['exp'] if si else '—'} (n{si['n'] if si else 0}) | {so['exp'] if so else '—'} (n{so['n'] if so else 0}) |")

md.append("\n## Per-year (s15/t100, EXPECT (n)) — pooled small/micro nasdaq+r2k\n")
eps=entries(sig[sig.universe.isin(('nasdaq','russell2k'))]); r,meta=run(eps,15,100,10)
md.append("| year | EXPECT | n |\n|---|---|---|")
for y in range(2021,2027):
    ry=np.array([x for x,(uu,d) in zip(r,meta) if pd.Timestamp(d).year==y])
    st=stats(ry)
    md.append(f"| {y} | {st['exp'] if st else '—'} | {st['n'] if st else 0} |")

# close-entry comparison + reference (T1/T1G no double-gap not run here)
open(OUT,"w").write("\n".join(md)+"\n")
print("wrote",OUT)
