import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, numpy as np
from studio.paths import db_path
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
tks=[r[0] for r in c.execute("SELECT DISTINCT ticker FROM bars WHERE universe='sp500' ORDER BY ticker LIMIT 60").fetchall()]

tot={'bars':0,'bottom_break':0,'top_break':0,'plus_struct':0,'plus_completion':0,
     'minus_struct':0,'minus_completion':0,'min_low':1e9}
for t in tks:
    d=c.execute("SELECT open,high,low,close FROM bars WHERE ticker=? AND universe='sp500' ORDER BY date",[t]).fetchdf()
    if len(d)<50: continue
    o,h,l,cl=[d[x].values.astype(float) for x in ('open','high','low','close')]
    top_price=bottom_price=0.0
    is_bull_pb=is_bear_pb=False; pot_top=pot_bottom=0.0; bull_i=bear_i=0
    bu_active=be_active=False; bu_price=be_price=0.0
    tot['min_low']=min(tot['min_low'], l.min())
    for i in range(1,len(d)):
        pb_bull=cl[i-1]>o[i-1]; pb_bear=cl[i-1]<o[i-1]
        if pb_bull and not is_bear_pb: is_bear_pb=True; pot_top=o[i-1]; bull_i=i-1
        if pb_bear and not is_bull_pb: is_bull_pb=True; pot_bottom=o[i-1]; bear_i=i-1
        if l[i] < bottom_price:                      # ← the branch in question
            tot['bottom_break']+=1; bottom_price=l[i]
            if is_bear_pb and (i-bull_i)!=0: tot['plus_struct']+=1; is_bear_pb=False
        if h[i] > top_price:
            tot['top_break']+=1; top_price=h[i]
            if is_bull_pb and (i-bear_i)!=0: tot['minus_struct']+=1; is_bull_pb=False
    tot['bars']+=len(d)
print(f"60 tickers · {tot['bars']:,} bars · lowest low seen = {tot['min_low']:.2f}")
print()
print(f"  'low < bottomPrice'  fired   {tot['bottom_break']:>7,}   ← +CISD structural path")
print(f"  'high > topPrice'    fired   {tot['top_break']:>7,}   ← -CISD structural path")
print()
print(f"  +CISD from a structure break {tot['plus_struct']:>7,}")
print(f"  -CISD from a structure break {tot['minus_struct']:>7,}")
