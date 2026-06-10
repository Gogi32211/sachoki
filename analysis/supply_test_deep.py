"""supply_test_deep.py — deep-validate the one K-signal survivor: Z3→T4→Z9 supply-test.
Variants (target T3/T5/T9/any/none, Z3 vs Z4 vs Z5, with/without the Z3 supply bar),
per-year robustness, universe split, path (MFE/MAE) + tail distribution. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
BULL=["t1","t1g","t2","t2g","t3","t4","t5","t6","t9","t10","t11","t12"]
NEED=["z3","z4","z5","z9","t4","t3","t5","t9"]+BULL
def _wilson(w,n,z=1.96):
    if n==0: return 0.0
    p=w/n; d=1+z*z/n; c=p+z*z/(2*n); m=z*((p*(1-p)/n+z*z/(4*n*n))**0.5); return max(0.0,(c-m)/d)

a=get_analytics_conn()
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
sel=[]
for s in set(NEED):
    for k in range(0,4): sel.append(f"sig_{s} AS {s}_0" if k==0 else f"lag(sig_{s},{k}) OVER w AS {s}_{k}")
df=a.execute(f"""SELECT universe,date,fwd_10d,mfe_10d,mae_10d,{', '.join(sel)}
   FROM bars WINDOW w AS (PARTITION BY ticker,universe ORDER BY date)""").fetchdf(); a.close()
df=df[df.fwd_10d.notna() & df.fwd_10d.between(-90,500)].copy()
for c in df.columns:
    if c not in ("universe","date","fwd_10d","mfe_10d","mae_10d"): df[c]=df[c].fillna(0)
df["yr"]=pd.to_datetime(df.date).dt.year; df["oos"]=df.date.astype(str)>=OOS
df["exc"]=df.fwd_10d-df.universe.map(BMED)
g=lambda n: df[n]==1
anybull0 = np.logical_or.reduce([df[f"{s}_0"]==1 for s in BULL])

def row(mask,label,path=False):
    sub=df[mask]; n=len(sub)
    if n<20: return f"  {label:34} n={n:>5}  <20"
    e=sub.exc; w=int((e>0).sum())
    med=float(e.median()); m25=float(e.clip(-25,25).mean()); lb=_wilson(w,n)*100
    isv=float(sub[~sub.oos].exc.median()); oo=float(sub[sub.oos].exc.median())
    posy=ny=0; yc=[]
    for y in range(2021,2027):
        sy=sub[sub.yr==y]
        if len(sy)>=15: v=float(sy.exc.median()); yc.append(f"{y%100}:{v:+.1f}"); ny+=1; posy+=v>0
        else: yc.append(f"{y%100}:–")
    s=f"  {label:34} {n:>5} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {lb:>5.1f} {isv:>+5.1f}/{oo:>+5.1f} {posy}/{ny}  "+" ".join(yc)
    if path:
        mfe=float(sub.mfe_10d.median()); mae=float(sub.mae_10d.median())
        big=(e>25).mean()*100; dn=(sub.fwd_10d< -25).mean()*100; worst=float(sub.fwd_10d.min())
        s+=f"\n      └ path: MFE+{mfe:.1f} MAE{mae:.1f} · >+25%:{big:.1f}% · <-25%:{dn:.1f}% · worst{worst:.0f}%"
    return s

hdr=f"  {'variant':34} {'n':>5} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} {'IS/OOS':>11} +yr  per-year"
print("### A) target matters? Z3[3]→T4[2]→Z9[1]→ <trigger>[0]")
print(hdr)
seq3 = g("z3_3")&g("t4_2")&g("z9_1")
print(row(seq3 & g("t3_0"), "→ T3 (K3①)", path=True))
print(row(seq3 & g("t5_0"), "→ T5 (K5①)"))
print(row(seq3 & g("t9_0"), "→ T9 (K9⑧)"))
print(row(seq3 & anybull0,  "→ any bull T", path=True))
print(row(g("z3_2")&g("t4_1")&g("z9_0"), "→ none (enter at Z9 bar)", path=True))

print("\n### B) does the Z3 supply bar matter? (vs Z4/Z5 / drop it)")
print(hdr)
print(row(g("z3_3")&g("t4_2")&g("z9_1")&g("t3_0"), "Z3→T4→Z9→T3 (canonical)"))
print(row(g("z4_3")&g("t4_2")&g("z9_1")&g("t3_0"), "Z4→T4→Z9→T3 (engulf, K3②)"))
print(row(g("z5_3")&g("t4_2")&g("z9_1")&g("t3_0"), "Z5→T4→Z9→T3 (upthrust)"))
print(row(g("t4_2")&g("z9_1")&g("t3_0"),           "T4→Z9→T3 (no supply bar)"))
print(row((g("z3_3")|g("z4_3")|g("z5_3"))&g("t4_2")&g("z9_1")&g("t3_0"), "(Z3|Z4|Z5)→T4→Z9→T3 [pooled]", path=True))

print("\n### C) universe split — canonical Z3→T4→Z9→T3")
print(hdr)
for u in ("sp500","nasdaq","russell2k"):
    print(row(seq3 & g("t3_0") & (df.universe==u), f"  {u}"))

print("\nlegend: medL=median excess vs universe (%). MFE/MAE=median max favorable/adverse excursion 10d.")
print("done")
