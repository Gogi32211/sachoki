"""volume_rsi_4variants.py — the vol_ratio × RSI grid, now split into the 4 variants:
L34/L46 × green(close>open)/red(close<open). Does optimal volume differ by variant?
cell = median fwd_10d excess vs universe. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT universe,date,l_sig,open,close,rsi_14,volume,avg_vol_20d,fwd_10d
  FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
   AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND avg_vol_20d>0""").fetchdf();a.close()
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["vr"]=df.volume/df.avg_vol_20d; df["green"]=df.close>df.open
VB=[("<0.5",0,.5),("0.5-0.8",.5,.8),("0.8-1.2",.8,1.2),("1.2-1.8",1.2,1.8),("1.8-2.5",1.8,2.5),
    ("2.5-4",2.5,4),("4-7",4,7),("7-15",7,15),(">15",15,1e9)]
DEC=[("20-30",20,30),("30-40",30,40),("40-50",40,50),("50-60",50,60),("60-70",60,70),("70-80",70,80)]
VARIANTS=[("L34 ↑GREEN","L34",True),("L34 ↓RED","L34",False),("L46 ↑GREEN","L46",True),("L46 ↓RED","L46",False)]
def cc(s):
    if len(s)<40: return f"{'·':>7}"
    return f"{float(s.exc.median()):>+6.2f}"
for vlab,lsig,grn in VARIANTS:
    dv=df[(df.l_sig==lsig)&(df.green==grn)]
    print(f"\n{'='*86}\n### {vlab}   (n={len(dv):,})   cell=median excess")
    print(f"  {'vol/avg':9}"+"".join(f"{d[0]:>8}" for d in DEC))
    for vl,vlo,vhi in VB:
        print(f"  {vl:9}"+"".join(f"{cc(dv[(dv.vr>=vlo)&(dv.vr<vhi)&(dv.rsi_14>=dlo)&(dv.rsi_14<dhi)]):>8}" for dl,dlo,dhi in DEC))
    # best cell for this variant
    best=None
    for vl,vlo,vhi in VB:
        for dl,dlo,dhi in DEC:
            s=dv[(dv.vr>=vlo)&(dv.vr<vhi)&(dv.rsi_14>=dlo)&(dv.rsi_14<dhi)]
            if len(s)>=300:
                m=float(s.exc.median())
                if best is None or m>best[0]: best=(m,vl,dl,len(s))
    if best: print(f"  → best: vol {best[1]} × RSI {best[2]}  med {best[0]:+.2f} (n={best[3]:,})")
print("\nlegend: green=close>open, red=close<open. done")
