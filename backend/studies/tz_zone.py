import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb
from studio.paths import db_path
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
q="""
WITH b AS (SELECT ticker,date,open,high,low,close,t_sig,z_sig,fwd_5d,
     LAG(high) OVER w AS p_high, LAG(low) OVER w AS p_low,
     GREATEST(LAG(open) OVER w, LAG(close) OVER w) AS p_top,
     LEAST(LAG(open) OVER w, LAG(close) OVER w)    AS p_bot
   FROM bars WHERE universe='sp500' AND high>low AND close>0
   WINDOW w AS (PARTITION BY ticker ORDER BY date))
SELECT ticker,date,t_sig,z_sig,fwd_5d,
  CASE WHEN close > p_high              THEN 'ABOVE the wick'
       WHEN close >= p_top              THEN 'IN the wick'
       WHEN close >= p_bot              THEN 'in the body'
       WHEN close >= p_low              THEN 'in the lower wick'
       ELSE 'BELOW everything' END AS zone,
  (p_high - p_top) / NULLIF(p_high - p_low,0) AS wick_frac
FROM b WHERE p_high IS NOT NULL AND p_high>p_low AND fwd_5d IS NOT NULL
"""
df=c.execute(q).fetchdf(); df.to_parquet('/tmp/tz_zone.parquet', index=False)
print('rows',len(df),'| T signals', (df.t_sig.notna()&(df.t_sig!='')).sum())
