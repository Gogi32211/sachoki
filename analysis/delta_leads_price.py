"""delta_leads_price.py — does order-flow (delta) LEAD price at turns?
Hypothesis from SMX: d_flip_bull marks bottoms (LONG edge), d_div_bear marks tops
(SHORT edge), strongest in pivot/oversold/overbought context. 5-yr, universe-baseline
removed (excess = fwd_10d - universe static median), median LIFT + clip25 + per-year +
IS/OOS. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
a=get_analytics_conn()

# 1) universe static baseline (median fwd_10d) — the structural drift to remove
base=a.execute("""SELECT universe, median(fwd_10d) m, avg(LEAST(GREATEST(fwd_10d,-25),25)) m25
                  FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe""").fetchdf()
BMED=dict(zip(base.universe,base.m)); BM25=dict(zip(base.universe,base.m25))
print("universe baselines (median / clip25-mean):")
for u in BMED: print(f"  {u:10} med {BMED[u]:+.3f}  m25 {BM25[u]:+.3f}")

# 2) pull rows where any delta-turn signal fires (+ context flags)
cols="universe,date,fwd_10d,d_flip_bull,d_div_bear,d_flip_bear,d_div_bull,is_pivot_low_5,is_pivot_high_5,rsi_le_35,rsi_ge_70,rsi2_state,d_surge_bull,d_surge_bear"
df=a.execute(f"""SELECT {cols} FROM bars
   WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
   AND (d_flip_bull=1 OR d_div_bear=1 OR d_flip_bear=1 OR d_div_bull=1)""").fetchdf()
a.close()
df['yr']=pd.to_datetime(df.date).dt.year
df['oos']=df.date.astype(str)>=OOS
df['exc']=df.fwd_10d - df.universe.map(BMED)          # excess vs universe (the LIFT)
df['exc25']=df.fwd_10d.clip(-25,25) - df.universe.map(BM25)

def rep(sub, label, short=False):
    if len(sub)<50: print(f"  {label:42} n={len(sub)} <50"); return
    e=sub.exc; e25=sub.exc25
    sgn=-1 if short else 1                              # short: profit = -excess
    win=(sgn*sub.fwd_10d>0).mean()*100 if False else (((sgn*e)>0).mean()*100)
    med=sgn*float(e.median()); m25=sgn*float(e25.mean())
    isv=sgn*float(sub[~sub.oos].exc.median()); oo=sgn*float(sub[sub.oos].exc.median())
    yrs=[]
    for y in range(2021,2027):
        sy=sub[sub.yr==y]
        yrs.append(f"{y%100}:{sgn*round(float(sy.exc.median()),2)}" if len(sy)>=30 else f"{y%100}:–")
    posy=sum(1 for y in range(2021,2027) for sy in [sub[sub.yr==y]] if len(sy)>=30 and sgn*sy.exc.median()>0)
    ny=sum(1 for y in range(2021,2027) if (df.yr==y).any() and len(sub[sub.yr==y])>=30)
    print(f"  {label:42} {len(sub):>7} {med:>+7.2f} {m25:>+7.2f} {isv:>+6.2f}/{oo:>+5.2f} {str(posy)+'/'+str(ny):>5}  "+" ".join(yrs))

print(f"\n{'='*130}\n### LONG hypothesis — d_flip_bull marks BOTTOMS (excess LIFT vs universe; +=edge)")
print(f"  {'context':42} {'n':>7} {'medL':>7} {'m25L':>7} {'IS/OOS':>12} {'+yr':>5}  per-year median-excess")
rep(df[df.d_flip_bull==1], "d_flip_bull (all)")
rep(df[(df.d_flip_bull==1)&(df.is_pivot_low_5==1)], "d_flip_bull & pivot_low")
rep(df[(df.d_flip_bull==1)&(df.rsi_le_35==1)], "d_flip_bull & RSI<=35 (oversold)")
rep(df[(df.d_flip_bull==1)&(df.rsi2_state=='R2L')], "d_flip_bull & R2L (RSI2 oversold)")
rep(df[(df.d_flip_bull==1)&(df.d_surge_bull==1)], "d_flip_bull & d_surge_bull")
print("  -- control: d_flip_BEAR (should be ~0 or negative for long)")
rep(df[df.d_flip_bear==1], "d_flip_bear (all)")

print(f"\n### SHORT hypothesis — d_div_bear marks TOPS (short profit = -excess; +=edge)")
print(f"  {'context':42} {'n':>7} {'medL':>7} {'m25L':>7} {'IS/OOS':>12} {'+yr':>5}  per-year (short)")
rep(df[df.d_div_bear==1], "d_div_bear (all)", short=True)
rep(df[(df.d_div_bear==1)&(df.is_pivot_high_5==1)], "d_div_bear & pivot_high", short=True)
rep(df[(df.d_div_bear==1)&(df.rsi_ge_70==1)], "d_div_bear & RSI>=70 (overbought)", short=True)
rep(df[(df.d_div_bear==1)&(df.rsi2_state=='R2H')], "d_div_bear & R2H (RSI2 overbought)", short=True)
rep(df[(df.d_div_bear==1)&(df.d_surge_bear==1)], "d_div_bear & d_surge_bear", short=True)
print("  -- control: d_div_BULL shorted (should be ~0 or negative for short)")
rep(df[df.d_div_bull==1], "d_div_bull (all) — shorted", short=True)
print("\ndone")
