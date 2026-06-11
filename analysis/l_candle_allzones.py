"""l_candle_allzones.py — full candle profile across ALL RSI zones, 4 variants
(L34/L46 × green/red), at optimal vol 1.8-2.5×. Per cell: bar% (open→close), gap%,
total% (gap+bar), AND forward edge (median excess) — connect candle shape to outcome
across every zone, hunt for surprises. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,l_sig,open,close,rsi_14,volume,avg_vol_20d,fwd_10d,
   lag(close) OVER (PARTITION BY ticker,universe ORDER BY date) AS pclose
  FROM bars WHERE l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND avg_vol_20d>0
    AND fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500""").fetchdf();a.close()
df=df[df.pclose.notna()&(df.pclose>0)&(df.open>0)].copy()
df["vr"]=df.volume/df.avg_vol_20d; df["green"]=df.close>df.open
df["bar_pct"]=(df.close-df.open)/df.open*100
df["gap_pct"]=(df.open-df.pclose)/df.pclose*100
df["tot_pct"]=(df.close-df.pclose)/df.pclose*100
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
V=(df.vr>=1.8)&(df.vr<2.5)
DEC=[("10-20",10,20),("20-30",20,30),("30-40",30,40),("40-50",40,50),
     ("50-60",50,60),("60-70",60,70),("70-80",70,80),("80-90",80,90)]
def cell(m):
    s=df[m]; n=len(s)
    if n<40: return f"  {'·':>4} n={n}"
    py=sum(1 for y in range(2021,2027) for sy in [s.exc[s.yr==y]] if len(sy)>=8 and float(sy.median())>0)
    ny=sum(1 for y in range(2021,2027) if len(s.exc[s.yr==y])>=8)
    return (f"  bar{float(s.bar_pct.median()):>+6.2f} gap{float(s.gap_pct.median()):>+5.2f} "
            f"TOT{float(s.tot_pct.median()):>+6.2f} │ fwd{float(s.exc.median()):>+5.2f}·{py}/{ny} (n{n})")
for lab,ls,gr in [("L34 ↑GREEN","L34",True),("L34 ↓RED","L34",False),("L46 ↑GREEN","L46",True),("L46 ↓RED","L46",False)]:
    print(f"\n{'='*92}\n### {lab}  @ vol 1.8-2.5×   (bar/gap/TOTAL = candle move %; fwd = median excess·posYr)")
    for dl,lo,hi in DEC:
        print(f"  RSI {dl:6}"+cell((df.l_sig==ls)&(df.green==gr)&(df.rsi_14>=lo)&(df.rsi_14<hi)&V))
print("\nlegend: bar%=open→close, gap%=overnight, TOT%=gap+bar. fwd=fwd_10d excess. done")
