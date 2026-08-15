import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from studio.paths import db_path
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
q="""
WITH b AS (SELECT ticker,date,open,high,low,close,fwd_5d,
             (high-GREATEST(open,close))/(high-low) AS up_s,
             (LEAST(open,close)-low)/(high-low)     AS dn_s,
             LAG((high-GREATEST(open,close))/(high-low)) OVER w AS p_up_s,
             LAG((LEAST(open,close)-low)/(high-low))     OVER w AS p_dn_s
           FROM bars WHERE universe='sp500' AND high>low AND close>0
           WINDOW w AS (PARTITION BY ticker ORDER BY date))
SELECT ticker,date,fwd_5d, close<open AS bar2_red,
       (close-low)/(high-low)*100 AS close_pos
FROM b
WHERE p_up_s>=0.30 AND p_dn_s<=0.20 AND p_up_s>=3*p_dn_s
  AND dn_s>=0.30 AND up_s<=0.20 AND dn_s>=3*up_s AND fwd_5d IS NOT NULL
"""
d=c.execute(q).fetchdf(); d['date']=pd.to_datetime(d['date']); d['yr']=d.date.dt.year
base=c.execute("SELECT MEDIAN(fwd_5d) FROM bars WHERE universe='sp500' AND fwd_5d IS NOT NULL").fetchone()[0]
print(f"baseline (all sp500 bars)   {base:+.3f}%\n")
print(f"{'bar 2':<12}{'n':>7}{'median fwd5':>13}{'close in range':>16}{'yrs+':>7}{'worst':>8}")
for lab, m in (("GREEN", ~d.bar2_red), ("RED", d.bar2_red)):
    s=d[m]; yr=s.groupby('yr')['fwd_5d'].median()
    print(f"{lab:<12}{len(s):>7}{s.fwd_5d.median():>12.3f}%{s.close_pos.mean():>15.1f}%"
          f"{(yr>0).sum():>4}/{len(yr)}{yr.min():>8.2f}")
