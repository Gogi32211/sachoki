"""l34_cluster_validate.py — PHASE 2: validate the L34 + accumulation-cluster pattern.
The co-occurrence scan found L34 attracts LOAD/FRI34/BLUE/BEST/squeeze/volume. Now test
which COMPOSITE is robust (median + clip25 + per-year + IS/OOS + P(big up)) vs L34-alone,
plus the SMX multi-bar sequence L46(supply)→L34+cluster(demand). ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def _wilson(w,n,z=1.96):
    if n==0: return 0.0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**0.5);return max(0.0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
CL=["load","sig_fri34","sig_blue","sig_best","sig_strong","sq","sig_va","sig_vol_5x","sig_vol_10x","r2l"]
df=a.execute(f"""SELECT ticker,universe,date,l_sig,fwd_10d,
   load,sig_fri34,sig_blue,sig_best,sig_strong,sq,sig_va,sig_vol_5x,sig_vol_10x,
   CASE WHEN bar_line5 LIKE '%R2L%' THEN 1 ELSE 0 END AS r2l
   FROM bars WHERE fwd_10d IS NOT NULL ORDER BY ticker,universe,date""").fetchdf(); a.close()
for c in CL: df[c]=df[c].fillna(0).astype(np.int8)
df=df[df.fwd_10d.between(-90,500)].copy()
df["L34"]=(df.l_sig=="L34").astype(np.int8)
df["L46"]=(df.l_sig=="L46").astype(np.int8)
# L46 present in prior 1-5 bars (per ticker) — the SMX supply→demand sequence
df["L46_prior5"]=(df.groupby(["ticker","universe"])["L46"]
                  .transform(lambda s: s.shift(1).rolling(5,min_periods=1).max())).fillna(0)
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
g=lambda n: df[n]==1

def rep(mask,label):
    sub=df[mask]; n=len(sub)
    if n<100: return f"  {label:34} n={n} <100"
    e=sub.exc; w=int((e>0).sum())
    med=float(e.median()); m25=float(e.clip(-25,25).mean()); lb=_wilson(w,n)*100
    bigup=float((sub.fwd_10d>15).mean()*100)
    isv=float(e[~(sub.date.astype(str)>=OOS)].median()); oo=float(e[sub.date.astype(str)>=OOS].median())
    posy=ny=0; yc=[]
    for y in range(2021,2027):
        sy=e[sub.yr==y]
        if len(sy)>=20: v=float(sy.median()); yc.append(f"{y%100}:{v:+.1f}"); ny+=1; posy+=v>0
        else: yc.append(f"{y%100}:–")
    return f"  {label:34} {n:>6} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {bigup:>5.1f} {lb:>5.1f} {isv:>+5.1f}/{oo:>+5.1f} {posy}/{ny}  "+" ".join(yc)

hdr=f"  {'pattern':34} {'n':>6} {'win%':>5} {'medL':>6} {'m25L':>6} {'big%':>5} {'wLB':>5} {'IS/OOS':>11} +yr  per-year"
print("### L34 + accumulation cluster (vs L34 alone)")
print(hdr)
print(rep(g("L34"), "L34 alone (baseline)"))
print(rep(g("L34")&g("load"), "L34 + LOAD"))
print(rep(g("L34")&g("sig_fri34"), "L34 + FRI34"))
print(rep(g("L34")&g("sig_best"), "L34 + BEST"))
print(rep(g("L34")&g("sq"), "L34 + squeeze"))
print(rep(g("L34")&g("r2l"), "L34 + R2L (oversold)"))
print(rep(g("L34")&(g("sig_vol_5x")|g("sig_vol_10x")), "L34 + vol-climax (V×5/10)"))
print(rep(g("L34")&g("load")&g("sig_fri34"), "L34 + LOAD + FRI34  [SMX core]"))
print(rep(g("L34")&g("load")&g("sq"), "L34 + LOAD + squeeze"))
print(rep(g("L34")&(g("load")|g("sig_fri34")|g("sig_best"))&(g("sig_vol_5x")|g("sig_vol_10x")), "L34 + cluster + vol-climax"))
print("\n### SMX sequence: L46 (supply, prior 5 bars) → L34 + cluster (demand now)")
print(hdr)
print(rep(g("L46_prior5")&g("L34"), "L46→L34"))
print(rep(g("L46_prior5")&g("L34")&g("load"), "L46→L34 + LOAD"))
print(rep(g("L46_prior5")&g("L34")&(g("load")|g("sig_fri34"))&(g("sig_vol_5x")|g("sig_vol_10x")), "L46→L34 + cluster + vol"))
print("\nlegend: medL/m25L=median/clip25 excess. big%=P(fwd_10d>+15%). ✅ if med>0 & 5/6yr & OOS>0. done")
