"""volume_rsi_fullgrid.py — full grid: vol_ratio (volume/avg_vol_20d) × EVERY RSI decile,
L34/L46 pooled. Find the optimal volume PER RSI zone (capitulation wants moderate ~2x;
momentum tolerates higher; >15x catastrophic everywhere). clean median-excess grid +yr.
ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT universe,date,rsi_14,volume,avg_vol_20d,fwd_10d
  FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
   AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND avg_vol_20d>0""").fetchdf();a.close()
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["vr"]=df.volume/df.avg_vol_20d
VB=[("<0.5",0,.5),("0.5-0.8",.5,.8),("0.8-1.2",.8,1.2),("1.2-1.8",1.2,1.8),("1.8-2.5",1.8,2.5),
    ("2.5-4",2.5,4),("4-7",4,7),("7-15",7,15),(">15",15,1e9)]
DEC=[("10-20",10,20),("20-30",20,30),("30-40",30,40),("40-50",40,50),
     ("50-60",50,60),("60-70",60,70),("70-80",70,80),("80-90",80,90)]
def c(s):
    if len(s)<50: return f"{'·':>8}"
    med=float(s.exc.median())
    py=sum(1 for y in range(2021,2027) for sy in [s.exc[s.yr==y]] if len(sy)>=10 and float(sy.median())>0)
    return f"{med:>+5.2f}·{py}"
print("cell = median fwd_10d excess · posYears(of 6)   ·   rows=vol/avg, cols=RSI decile")
print(f"  {'vol/avg':9}"+"".join(f"{d[0]:>9}" for d in DEC))
for vl,vlo,vhi in VB:
    print(f"  {vl:9}"+"".join(f"{c(df[(df.vr>=vlo)&(df.vr<vhi)&(df.rsi_14>=dlo)&(df.rsi_14<dhi)]):>9}" for dl,dlo,dhi in DEC))
# best vol band per RSI zone
print("\n### optimal vol_ratio per RSI zone (highest median excess, n>=300):")
for dl,dlo,dhi in DEC:
    best=None
    for vl,vlo,vhi in VB:
        s=df[(df.vr>=vlo)&(df.vr<vhi)&(df.rsi_14>=dlo)&(df.rsi_14<dhi)]
        if len(s)>=300:
            m=float(s.exc.median())
            if best is None or m>best[1]: best=(vl,m,len(s))
    if best: print(f"  RSI {dl:6} → vol {best[0]:8} (med {best[1]:+.2f}, n={best[2]:,})")
print("\ndone")
