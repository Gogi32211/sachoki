"""zone_breakout.py — the user's full thesis: L34/L46 = LEVELS that build a ZONE; BE/EB/BO =
the bar that CLOSES ABOVE the zone = breakout. Test: does the breakout (bo_up close>L34-high /
be_up engulf / eb_bull) AFTER a dense L34/L46 zone work — or is the breakout just chase (it
failed alone)? Density(zone) × breakout(trigger). ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,l_sig,rsi_14,fwd_10d,
   bo_up,be_up,eb_bull,vbo_up,seq_l34_eb
  FROM bars WHERE fwd_10d IS NOT NULL ORDER BY ticker,universe,date""").fetchdf();a.close()
for c in ["bo_up","be_up","eb_bull","vbo_up","seq_l34_eb"]: df[c]=df[c].fillna(0).astype(np.int8)
df["isL"]=df.l_sig.isin(['L34','L46']).astype(np.int8)
# zone density: L34/L46 count in trailing 10 (excl current)
df["d10"]=df.groupby(["ticker","universe"],sort=False)["isL"].transform(lambda s:s.shift(1).rolling(10,min_periods=1).sum()).fillna(0)
df=df[df.fwd_10d.between(-90,500)].copy()
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
g=lambda n: df[n]==1
brk=g("bo_up")|g("be_up")|g("eb_bull")
def rep(m,lab):
    s=df[m];n=len(s)
    if n<200:return f"  {lab:34} n={n}"
    e=s.exc;w=int((e>0).sum())
    py=sum(1 for y in range(2021,2027) for sy in [e[s.yr==y]] if len(sy)>=20 and float(sy.median())>0)
    ny=sum(1 for y in range(2021,2027) if len(e[s.yr==y])>=20)
    return f"  {lab:34} {n:>7} {w/n*100:>5.1f} {float(e.median()):>+6.2f} {float(e.clip(-25,25).mean()):>+6.2f} {wl(w,n)*100:>5.1f} {py}/{ny}"
hdr=f"  {'pattern':34} {'n':>7} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
print("### A) breakout ALONE (baseline)")
print(hdr)
print(rep(g("bo_up"), "BO↑ (close > L34 high)"))
print(rep(g("be_up"), "BE↑ (breakout-engulf)"))
print(rep(g("eb_bull"), "EB↑ (expansion)"))
print(rep(g("seq_l34_eb"), "seq_l34_eb (L34→EB)"))
print("\n### B) breakout AFTER dense L34/L46 zone (density>=K in last 10)")
print(hdr)
for K in (3,5,7):
    print(rep(brk&(df.d10>=K), f"  breakout · zone d10>={K}"))
print(rep(g("bo_up")&(df.d10>=5), "  BO↑ · zone d10>=5"))
print(rep(g("seq_l34_eb")&(df.d10>=5), "  seq_l34_eb · zone d10>=5"))
print("\n### C) zone-density alone (no breakout) — for comparison")
print(hdr)
print(rep((df.isL==1)&(df.d10>=5), "  L34/L46 · d10>=5 (no breakout)"))
print(rep((df.isL==1)&(df.d10>=7), "  L34/L46 · d10>=7"))
print("\n### D) breakout after zone, by RSI context")
print(hdr)
print(rep(brk&(df.d10>=5)&(df.rsi_14<40), "  zone-breakout · RSI<40"))
print(rep(brk&(df.d10>=5)&(df.rsi_14>=40)&(df.rsi_14<60), "  zone-breakout · RSI 40-60"))
print(rep(brk&(df.d10>=5)&(df.rsi_14>=60), "  zone-breakout · RSI>=60"))
print("\nlegend: medL=median fwd_10d excess. done")
