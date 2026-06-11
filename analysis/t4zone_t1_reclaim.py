"""t4zone_t1_reclaim.py — new logic: T4 bar forms a demand zone [low,high]; if price
holds the floor (no bar's low < T4_low) and a T1/T1G fires INSIDE the zone (close in
[T4_low,T4_high]) within W bars → BUY at the trigger. Forward edge vs universe + vs the
raw-T1/T1G baseline, per-year + IS/OOS + path. ANALYSIS ONLY (trigger-centric, no double-count)."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def _wilson(w,n,z=1.96):
    if n==0: return 0.0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**0.5);return max(0.0,(c-m)/d)

a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,high,low,close,
       sig_t4,sig_t1,sig_t1g,fwd_10d,fwd_20d,mfe_10d,mae_10d
   FROM bars ORDER BY ticker,universe,date""").fetchdf(); a.close()
for c in ("sig_t4","sig_t1","sig_t1g"): df[c]=df[c].fillna(0).astype(np.int8)
df["yr"]=pd.to_datetime(df.date).dt.year

def scan(W):
    """trigger-centric: for each T1/T1G bar, look back <=W for a T4 whose zone holds & contains it."""
    hits=[]
    for (tk,uni),g in df.groupby(["ticker","universe"],sort=False):
        hi=g.high.to_numpy(); lo=g.low.to_numpy(); cl=g.close.to_numpy()
        t4=g.sig_t4.to_numpy(); t1=g.sig_t1.to_numpy(); t1g=g.sig_t1g.to_numpy()
        idx=g.index.to_numpy(); n=len(g)
        trig_pos=np.where((t1==1)|(t1g==1))[0]            # only iterate trigger bars (sparse)
        for j in trig_pos:
            if j<1: continue
            cj=cl[j]
            # look back up to W bars for a qualifying T4 anchor
            for i in range(j-1, max(-1, j-1-W), -1):
                if not t4[i]: continue
                zl, zh = lo[i], hi[i]
                if not (zl <= cj <= zh): continue          # trigger inside zone
                # floor held: every bar i+1..j has low >= zl
                if lo[i+1:j+1].min() >= zl:
                    hits.append(idx[j]); break              # first qualifying T4 → count once
    return df.loc[hits]

def stats(sub, base, label):
    sub=sub[sub.fwd_10d.notna() & sub.fwd_10d.between(-90,500)]
    n=len(sub)
    if n<20: return f"  {label:30} n={n} <20"
    e=sub.fwd_10d - sub.universe.map(BMED); w=int((e>0).sum())
    med=float(e.median()); m25=float(e.clip(-25,25).mean()); lb=_wilson(w,n)*100
    e20=(sub.fwd_20d - sub.universe.map(BMED)).median()
    mfe=float(sub.mfe_10d.median()); mae=float(sub.mae_10d.median())
    isv=float(e[~(sub.date.astype(str)>=OOS)].median()); oo=float(e[sub.date.astype(str)>=OOS].median())
    posy=ny=0; yc=[]
    for y in range(2021,2027):
        sy=e[sub.yr==y]
        if len(sy)>=15: v=float(sy.median()); yc.append(f"{y%100}:{v:+.1f}"); ny+=1; posy+=v>0
        else: yc.append(f"{y%100}:–")
    return (f"  {label:30} {n:>6} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {float(e20):>+6.2f} {lb:>5.1f} "
            f"{isv:>+5.1f}/{oo:>+5.1f} {posy}/{ny} MFE{mfe:+.1f}/MAE{mae:.1f}  "+" ".join(yc))

# baselines (excess vs universe)
allt1  = df[(df.sig_t1==1)|(df.sig_t1g==1)]
allt4  = df[df.sig_t4==1]
hdr=(f"  {'pattern':30} {'n':>6} {'win%':>5} {'medL':>6} {'m25L':>6} {'f20L':>6} {'wLB':>5} "
     f"{'IS/OOS':>11} +yr  path · per-year")
print("### baselines")
print(hdr)
print(stats(allt1, None, "ALL T1/T1G (raw)"))
print(stats(allt4, None, "ALL T4 (raw)"))
print("\n### NEW LOGIC: T4-zone held + T1/T1G inside zone → BUY")
print(hdr)
for W in (5,10,15):
    print(stats(scan(W), None, f"T4zone→T1/T1G (W={W})"))
print("\nlegend: medL/f20L=median 10d/20d excess vs universe. path=median MFE/MAE 10d.")
print("done")
