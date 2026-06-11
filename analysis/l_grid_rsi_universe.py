"""l_grid_rsi_universe.py — clean matrix: L34/L46 × candle (green close>open / red close<open)
× RSI decile (10-step) × universe (nasdaq/sp500/russell2k). NO CCI. Cell = median forward
EXCESS vs that universe's baseline (n). ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT universe,date,l_sig,open,close,rsi_14,fwd_10d
  FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
   AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL""").fetchdf();a.close()
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["green"]=df.close>df.open
df["dec"]=pd.cut(df.rsi_14,[0,10,20,30,40,50,60,70,80,90,100],right=False,
                 labels=["0-10","10-20","20-30","30-40","40-50","50-60","60-70","70-80","80-90","90+"])
VARS=[("L34↑green",("L34",True)),("L34↓red",("L34",False)),("L46↑green",("L46",True)),("L46↓red",("L46",False))]
DECS=["0-10","10-20","20-30","30-40","40-50","50-60","60-70","70-80","80-90","90+"]
print("cell = median fwd_10d EXCESS vs universe baseline  ·  (n)  ·  baselines:",{k:round(v,2) for k,v in BMED.items()})
for uni in ("nasdaq","sp500","russell2k"):
    du=df[df.universe==uni]
    print(f"\n{'='*112}\n### {uni.upper()}")
    print(f"  {'RSI':6} "+" ".join(f"{v[0]:>16}" for v in VARS))
    for d in DECS:
        cells=[]
        for _,(lsig,grn) in VARS:
            sub=du[(du.l_sig==lsig)&(du.green==grn)&(du.dec==d)]
            if len(sub)>=40:
                med=float(sub.exc.median())
                cells.append(f"{med:>+7.2f}({len(sub)//1000}k)" if len(sub)>=1000 else f"{med:>+7.2f}({len(sub):>4})")
            else:
                cells.append(f"{'—':>16}")
        print(f"  {d:6} "+" ".join(f"{c:>16}" for c in cells))
print("\nlegend: + bullish / − bearish (vs universe). green=close>open, red=close<open. done")
