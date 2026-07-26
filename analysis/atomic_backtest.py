"""atomic_backtest.py — PART B: path-aware backtest of the atomic bull profile
weak-close gap-up = [T-signal AND close=O AND gap in (G2,G3)] and EO+gap variant.
Entry next-open, gap-aware stop/target, $500k floor, glitch-screen, per-year, per-univ."""
import sys, duckdb, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/analysis")
from exit_backtest import sim, is_glitch
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT="/Users/sachoki/Desktop/sachoki-desktop/ATOMIC_BACKTEST.md"
TS="('T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12')"
UNIS=("sp500","nasdaq","russell2k"); DVFLOOR=500_000; DEDUP=5
con=duckdb.connect(DB,read_only=True); con.execute("PRAGMA threads=6")

def popl(extra):
    return con.execute(f"""SELECT universe,ticker,date FROM bars
      WHERE t_sig IN {TS} AND close_suffix='O' AND bar_gap_class IN ('G2','G3') {extra}
      ORDER BY ticker,date""").fetchdf()
profiles={"weakclose_gap (close=O & gap)":"", "EO_gap (escape & O & gap)":"AND substr(full_suffix,1,1)='E'"}
allp=popl("AND substr(full_suffix,1,1)='E'")  # union superset (EO is subset of weakclose) -> use weakclose tickers
sigs={n:popl(e) for n,e in profiles.items()}
tickers=sorted(set(sigs["weakclose_gap (close=O & gap)"].ticker))
ph=",".join("?"*len(tickers))
bars=con.execute(f"SELECT universe,ticker,date,open,high,low,close,volume FROM bars WHERE ticker IN ({ph}) ORDER BY universe,ticker,date",tickers).fetchdf()
con.close()
bars=bars.drop_duplicates(["universe","ticker","date"]).reset_index(drop=True); bars["dv"]=bars.close*bars.volume
store={}
for (u,t),s in bars.groupby(["universe","ticker"],sort=False):
    s=s.reset_index(drop=True)
    store[(u,t)]=dict(O=s.open.to_numpy(float),H=s.high.to_numpy(float),L=s.low.to_numpy(float),
                      C=s.close.to_numpy(float),DV=s.dv.to_numpy(float),idx={d:k for k,d in enumerate(s.date.to_numpy())})
def run(df,uni,stop,target,hz=10):
    last={}; r=[]; meta=[]
    sub=df[df.universe==uni].sort_values(["ticker","date"])
    for x in sub.itertuples():
        rec=store.get((x.universe,x.ticker))
        if rec is None: continue
        i=rec["idx"].get(np.datetime64(x.date))
        if i is None or i-last.get((x.universe,x.ticker),-99)<=DEDUP: continue
        if is_glitch(rec,i,hz): continue
        res=sim(rec,i,stop,target,hz,trailing=None,entry_mode="next_open")
        if res is None or not res.get("dv") or res["dv"]<DVFLOOR: continue
        last[(x.universe,x.ticker)]=i; r.append(res["r"]); meta.append(pd.Timestamp(x.date).year)
    return np.array(r),meta
def st(r):
    if len(r)<1: return None
    return dict(n=len(r),exp=round(float(r.mean()),2),med=round(float(np.median(r)),2),
                win=round(float((r>0).mean()*100),1),p50=round(float((r>=50).mean()*100),1),ml=round(float(r.min()),1))

md=["# Atomic bull profile — path-aware backtest","",
    "_weak-close gap-up = T-signal AND close=O AND gap∈(G2,G3). Entry next-open, gap-aware "
    "s15/t100, $500k floor, glitch-screen, episode-dedup. Percent units._\n"]
md.append("## Exit grid (entry next-open, horizon 10d)\n| profile · universe | config | n | EXPECT | med | win% | P(+50%) | maxloss |\n|---|---|---|---|---|---|---|---|")
for pname,df in sigs.items():
    for uni in UNIS:
        for cfg,(s,t) in [("s15/t100",(15,100)),("s12/t50",(12,50))]:
            r,_=run(df,uni,s,t); m=st(r)
            if m: md.append(f"| {pname} · {uni} | {cfg} | {m['n']} | **{m['exp']}** | {m['med']} | {m['win']} | {m['p50']} | {m['ml']} |")
md.append("\n## Per-year (weak-close gap-up, s15/t100, EXPECT(n)) — nasdaq+russell2k\n| year | EXPECT | n |\n|---|---|---|")
df=sigs["weakclose_gap (close=O & gap)"]
rr=[];mm=[]
for uni in ('nasdaq','russell2k'):
    a,b=run(df,uni,15,100); rr.append(a); mm+= [(x,y) for x,y in zip(a,b)]
allr=np.array([x for x,_ in mm]); ally=[y for _,y in mm]
for y in range(2021,2027):
    ry=np.array([x for x,yy in zip(allr,ally) if yy==y]); m=st(ry)
    md.append(f"| {y} | {m['exp'] if m else '—'} | {m['n'] if m else 0} |")
open(OUT,"w").write("\n".join(md)+"\n"); print("wrote",OUT)
for line in md: print(line)
