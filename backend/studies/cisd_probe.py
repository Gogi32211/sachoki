import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from studio.paths import db_path
from cisd_engine import compute_cisd
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
tks=[r[0] for r in c.execute("SELECT DISTINCT ticker FROM bars WHERE universe='sp500' ORDER BY ticker LIMIT 60").fetchall()]
tot={'bars':0,'p':0,'m':0,'both':0,'pp':0,'mm':0,'alt':0}
for t in tks:
    d=c.execute("SELECT open,high,low,close FROM bars WHERE ticker=? AND universe='sp500' ORDER BY date",[t]).fetchdf()
    if len(d)<50: continue
    r=compute_cisd(d)
    p=r.PLUS_CISD.values; m=r.MINUS_CISD.values
    ev=[]
    for i in range(len(p)):
        if p[i] and m[i]: ev.append('B')
        elif p[i]: ev.append('P')
        elif m[i]: ev.append('M')
    tot['bars']+=len(d); tot['p']+=int(p.sum()); tot['m']+=int(m.sum())
    tot['both']+=sum(1 for e in ev if e=='B')
    for i in range(1,len(ev)):
        if ev[i-1]=='P' and ev[i]=='P': tot['pp']+=1
        if ev[i-1]=='M' and ev[i]=='M': tot['mm']+=1
        if {ev[i-1],ev[i]}=={'P','M'}: tot['alt']+=1
print(f"60 tickers · {tot['bars']:,} bars")
print(f"  +CISD events                 {tot['p']:>7,}")
print(f"  -CISD events                 {tot['m']:>7,}")
print(f"  bars carrying BOTH           {tot['both']:>7,}")
print()
print("consecutive event pairs (ignoring bars with no event):")
print(f"  P then P  (what ++- needs)   {tot['pp']:>7,}")
print(f"  M then M  (what +-- needs)   {tot['mm']:>7,}")
print(f"  P↔M alternating              {tot['alt']:>7,}")
