import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import duckdb, pandas as pd, numpy as np
from studio.paths import db_path
from analysis_kit import bootstrap_ci_clustered
import edge_replay as ER

c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
q="""WITH b AS (SELECT ticker,date,open,high,low,close,atr_14,high-low AS rng,
                     high-GREATEST(open,close) AS up_w, LEAST(open,close)-low AS dn_w
              FROM bars WHERE universe='sp500' AND high>low AND close>0),
r AS (SELECT *, up_w/rng AS up_s, dn_w/rng AS dn_s FROM b),
p AS (SELECT *, LAG(up_s) OVER w AS p_up_s, LAG(dn_s) OVER w AS p_dn_s
      FROM r WINDOW w AS (PARTITION BY ticker ORDER BY date))
SELECT ticker,date,open,high,low,close,atr_14,up_s,dn_s,p_up_s,p_dn_s FROM p"""
df=c.execute(q).fetchdf(); df['date']=pd.to_datetime(df['date'])
W,O,M=0.50,0.05,3.0
df['sig']=((df.p_up_s>=W)&(df.p_dn_s<=O)&(df.p_up_s>=M*df.p_dn_s)
         &(df.dn_s>=W)&(df.up_s<=O)&(df.dn_s>=M*df.up_s))
df['hammer']=(df.dn_s>=W)&(df.up_s<=O)&(df.dn_s>=M*df.up_s)
rng=np.random.default_rng(7)

def run(frame,col,label):
    grp={tk:g.sort_values('date').reset_index(drop=True) for tk,g in frame.groupby('ticker') if g[col].any()}
    t=ER._pathsim(grp,col,mode="trail",stop=0.0,target=0.0,trail=0.25,maxh=60,atr_k=12)
    if len(t)<10: print(f"  {label:<30} too few"); return
    r100=t.ret*100
    lo,hi=bootstrap_ci_clustered(r100, pd.to_datetime(t.date_in).dt.to_period('M').astype(str), stat="median")
    yr=t.assign(y=pd.to_datetime(t.date_in).dt.year).groupby('y')['ret'].median()*100
    print(f"  {label:<30} n={len(t):>6}  median {r100.median():+6.2f}% [{lo:+.2f},{hi:+.2f}]  "
          f"mean {r100.mean():+6.2f}%  win {(t.ret>0).mean()*100:4.1f}%  yrs+ {(yr>0).sum()}/{len(yr)}  worst {yr.min():+6.2f}%")

for name,lo_,hi_ in (("MINED 2021-2023","2021-01-01","2024-01-01"),("OOS 2024-2026","2024-01-01","2027-01-01")):
    w=df[(df.date>=lo_)&(df.date<hi_)].copy()
    print(f"\n{name}  ·  ATR×12 trail · 60-bar timer · gap-realistic fills")
    run(w,'sig','WICK-FLIP pair')
    run(w,'hammer','bar2 alone (hammer)')
    w['rand']=rng.random(len(w))<(w.sig.sum()/len(w))
    run(w,'rand','random, same entry rate')
