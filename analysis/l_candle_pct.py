"""l_candle_pct.py — for the GOOD L34/L46 signals (optimal RSI/vol ranges), what % move
does the candle itself make? TWO metrics: bar% = (close-open)/open (intraday body) and
total% = (close-prevclose)/prevclose (gap+bar). Also the gap component alone. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
a=get_analytics_conn(read_only=True)
df=a.execute("""SELECT ticker,universe,date,l_sig,open,close,rsi_14,volume,avg_vol_20d,
   lag(close) OVER (PARTITION BY ticker,universe ORDER BY date) AS pclose, fwd_10d
  FROM bars WHERE l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND avg_vol_20d>0""").fetchdf();a.close()
df=df[df.pclose.notna() & (df.pclose>0) & (df.open>0)].copy()
df["vr"]=df.volume/df.avg_vol_20d; df["green"]=df.close>df.open
df["bar_pct"]=(df.close-df.open)/df.open*100          # intraday body
df["gap_pct"]=(df.open-df.pclose)/df.pclose*100       # overnight gap
df["tot_pct"]=(df.close-df.pclose)/df.pclose*100      # gap + bar
def stat(m,label):
    s=df[m]; n=len(s)
    if n<40: return f"  {label:34} n={n}"
    return (f"  {label:34} {n:>6} │ bar%: med{float(s.bar_pct.median()):>+6.2f} mean{float(s.bar_pct.mean()):>+6.2f}"
            f" │ gap%: med{float(s.gap_pct.median()):>+5.2f} │ TOTAL%: med{float(s.tot_pct.median()):>+6.2f} mean{float(s.tot_pct.mean()):>+6.2f}")
V=(df.vr>=1.8)&(df.vr<2.5)            # optimal volume band
print("GOOD signals (optimal vol 1.8-2.5×) — candle's own move:")
print(f"  {'setup':34} {'n':>6} │ bar%=open→close (intraday) │ gap%=overnight │ TOTAL%=gap+bar")
print("\n### CAPITULATION zone (RSI 20-30):")
for lab,ls,gr in [("L34 ↑GREEN","L34",True),("L34 ↓RED","L34",False),("L46 ↑GREEN","L46",True),("L46 ↓RED","L46",False)]:
    stat_m=(df.l_sig==ls)&(df.green==gr)&(df.rsi_14>=20)&(df.rsi_14<30)&V
    print(stat(stat_m,lab))
print("\n### MOMENTUM zone (RSI 50-60):")
for lab,ls,gr in [("L34 ↑GREEN","L34",True),("L34 ↓RED","L34",False),("L46 ↑GREEN","L46",True),("L46 ↓RED","L46",False)]:
    print(stat((df.l_sig==ls)&(df.green==gr)&(df.rsi_14>=50)&(df.rsi_14<60)&V,lab))
print("\n### how candle size GROWS with volume (L46↓RED @ RSI 20-30, by vol band):")
for vl,lo,hi in [("0.8-1.2",.8,1.2),("1.2-1.8",1.2,1.8),("1.8-2.5",1.8,2.5),("2.5-4",2.5,4),("4-7",4,7),("7-15",7,15),(">15",15,1e9)]:
    print(stat((df.l_sig=='L46')&~df.green&(df.rsi_14>=20)&(df.rsi_14<30)&(df.vr>=lo)&(df.vr<hi), f"vol {vl}"))
print("\nlegend: bar% can be negative (RED=down candle). done")
