"""fri34_load_abs_eb.py — 5-yr forward edge of the four SMX-bottom signals:
FRI34 (coiled vol-spike), LOAD (Wyckoff absorption), ABS (vol-bucket jump),
EB-up (expansion thrust), and the ABS->EB-up sequence. Universe-baseline removed
(excess), median LIFT + clip25 + per-year + IS/OOS. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"; a=get_analytics_conn()
base=a.execute("SELECT universe, median(fwd_10d) m FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchdf()
BMED=dict(zip(base.universe,base.m))

# pull each signal's rows + an ABS->EB sequence flag (ABS in prior 1-3 bars, then EB-up now)
df=a.execute(f"""
WITH w AS (
  SELECT universe,ticker,date,fwd_10d,sig_fri34,load,sig_abs,eb_bull,
    MAX(sig_abs) OVER (PARTITION BY ticker,universe ORDER BY date
                       ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS abs_prior3
  FROM bars WHERE fwd_10d IS NOT NULL)
SELECT universe,date,fwd_10d,sig_fri34,load,sig_abs,eb_bull,
       (eb_bull=1 AND abs_prior3=1)::int AS abs_then_eb
FROM w WHERE fwd_10d BETWEEN -90 AND 500
  AND (sig_fri34=1 OR load=1 OR sig_abs=1 OR eb_bull=1)""").fetchdf()
a.close()
df['yr']=pd.to_datetime(df.date).dt.year; df['oos']=df.date.astype(str)>=OOS
df['exc']=df.fwd_10d-df.universe.map(BMED)

# overall universe baseline for reference (excess of full pop = 0 by construction)
print("universe medians:", {k:round(v,3) for k,v in BMED.items()})
print(f"\n{'signal':26} {'n':>8} {'win%':>6} {'medL':>7} {'m25L':>7} {'IS/OOS':>13} {'+yr':>5}  per-year median-excess")
def rep(m,label):
    sub=df[m]
    if len(sub)<50: print(f"  {label:24} n={len(sub)} <50"); return
    e=sub.exc; win=(e>0).mean()*100
    med=float(e.median()); m25=float(e.clip(-25,25).mean())
    isv=float(sub[~sub.oos].exc.median()); oo=float(sub[sub.oos].exc.median())
    posy=ny=0; yrs=[]
    for y in range(2021,2027):
        sy=sub[sub.yr==y]
        if len(sy)>=30:
            v=float(sy.exc.median()); yrs.append(f"{y%100}:{round(v,2)}"); ny+=1; posy+= v>0
        else: yrs.append(f"{y%100}:–")
    print(f"  {label:24} {len(sub):>8} {win:>5.1f} {med:>+7.2f} {m25:>+7.2f} {isv:>+6.2f}/{oo:>+5.2f} {str(posy)+'/'+str(ny):>5}  "+" ".join(yrs))

rep(df.sig_fri34==1, "FRI34 (coiled spike)")
rep(df['load']==1,   "LOAD (Wyckoff absorb)")
rep(df.sig_abs==1,   "ABS (vol-bucket jump)")
rep(df.eb_bull==1,   "EB-up (expansion)")
rep(df.abs_then_eb==1,"ABS->EB-up (seq, <=3 bars)")
# context combos that mattered on SMX
rep((df.eb_bull==1)&(df.sig_abs==1), "EB-up & ABS (same bar)")
rep((df.sig_fri34==1)&(df['load']==1),"FRI34 & LOAD (same bar)")
print("\ndone")
