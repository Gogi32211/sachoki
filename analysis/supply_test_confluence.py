"""supply_test_confluence.py — does adding our validated axes (oversold R2L /
absorption LOAD·FRI34 / volume) to the Z3→T4→Z9→T3 supply-test FIX the 2022 weakness
and generalize beyond russell2k — or just shrink n (redundant)? Context applied at the
trigger bar (entry). per-year + universe + n-retention. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def _wilson(w,n,z=1.96):
    if n==0: return 0.0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**0.5);return max(0.0,(c-m)/d)
a=get_analytics_conn()
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
sig=["z3","z4","z5","t4","z9","t3"]
sel=[]
for s in sig:
    for k in range(0,4): sel.append(f"sig_{s} AS {s}_0" if k==0 else f"lag(sig_{s},{k}) OVER w AS {s}_{k}")
# context at bar 0 (entry/trigger)
ctx="rsi2_state, rsi_le_35, load, sig_fri34, sig_abs, vol_bucket, sig_vol_5x, wyc_phase"
df=a.execute(f"""SELECT universe,date,fwd_10d,{ctx},{', '.join(sel)}
   FROM bars WINDOW w AS (PARTITION BY ticker,universe ORDER BY date)""").fetchdf(); a.close()
df=df[df.fwd_10d.notna() & df.fwd_10d.between(-90,500)].copy()
for c in [f"{s}_{k}" for s in sig for k in range(4)]+["rsi_le_35","load","sig_fri34","sig_abs","sig_vol_5x"]:
    df[c]=df[c].fillna(0)
df["yr"]=pd.to_datetime(df.date).dt.year; df["oos"]=df.date.astype(str)>=OOS
df["exc"]=df.fwd_10d-df.universe.map(BMED)
g=lambda n: df[n]==1
BASE=g("z3_3")&g("t4_2")&g("z9_1")&g("t3_0")               # canonical supply-test
POOL=(g("z3_3")|g("z4_3")|g("z5_3"))&g("t4_2")&g("z9_1")&g("t3_0")
# context axes (at entry bar)
OVS = (df.rsi2_state=="R2L")|(df.rsi_le_35==1)
ABS = (df.load==1)|(df.sig_fri34==1)|(df.sig_abs==1)
VOL = (df.vol_bucket.isin(["B","VB"]))|(df.sig_vol_5x==1)

def row(mask,label):
    sub=df[mask]; n=len(sub)
    if n<15: return f"  {label:38} n={n:>5}  <15"
    e=sub.exc; w=int((e>0).sum()); med=float(e.median()); m25=float(e.clip(-25,25).mean()); lb=_wilson(w,n)*100
    posy=ny=y22=0; yc=[]
    for y in range(2021,2027):
        sy=sub[sub.yr==y]
        if len(sy)>=12:
            v=float(sy.exc.median()); yc.append(f"{y%100}:{v:+.1f}"); ny+=1; posy+=v>0
            if y==2022: y22=v
        else: yc.append(f"{y%100}:–")
    return f"  {label:38} {n:>5} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {lb:>5.1f} {posy}/{ny}  "+" ".join(yc)

hdr=f"  {'variant':38} {'n':>5} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr  per-year median-excess"
print("### Canonical Z3→T4→Z9→T3 + context axis at entry (all universes)")
print(hdr)
print(row(BASE, "base (no context)"))
print(row(BASE&OVS, "+ oversold (R2L|RSI≤35)"))
print(row(BASE&ABS, "+ absorption (LOAD|FRI34|ABS)"))
print(row(BASE&VOL, "+ volume (B|VB|V×5)"))
print(row(BASE&OVS&ABS, "+ oversold & absorption [coherent]"))
print(row(BASE&~OVS, "+ NOT oversold (control)"))

print("\n### Pooled (Z3|Z4|Z5)→T4→Z9→T3 + context (bigger n)")
print(hdr)
print(row(POOL, "pooled base"))
print(row(POOL&OVS, "+ oversold"))
print(row(POOL&ABS, "+ absorption"))
print(row(POOL&OVS&ABS, "+ oversold & absorption"))

print("\n### Does context RESCUE nasdaq+sp500 (large-cap, where base was weak)?")
print(hdr)
LC = df.universe.isin(["nasdaq","sp500"])
print(row(BASE&LC, "base · large-cap"))
print(row(BASE&LC&OVS, "+ oversold · large-cap"))
print(row(POOL&LC&OVS, "pooled + oversold · large-cap"))
print("\nlegend: medL=median excess vs universe. focus: does 2022 turn +, does n survive, large-cap rescue?")
print("done")
