import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd
from studio.paths import db_path
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
q="""
WITH b AS (
  SELECT ticker, date, open, high, low, close, volume, atr_14,
         fwd_5d, fwd_10d, fwd_20d,
         high-low AS rng,
         high-GREATEST(open,close) AS up_w,
         LEAST(open,close)-low     AS dn_w,
         ABS(close-open)           AS body
  FROM bars WHERE universe='sp500' AND high>low AND close>0
),
r AS (SELECT *, up_w/rng AS up_s, dn_w/rng AS dn_s, body/rng AS body_s FROM b),
p AS (
  SELECT *,
    LAG(up_s)  OVER w AS p_up_s,  LAG(dn_s) OVER w AS p_dn_s,
    LAG(close) OVER w AS p_close, LAG(low)  OVER w AS p_low,
    LAG(close,5) OVER w AS c5
  FROM r WINDOW w AS (PARTITION BY ticker ORDER BY date)
)
SELECT ticker, date, open, high, low, close, atr_14,
       fwd_5d, fwd_10d, fwd_20d,
       up_s, dn_s, body_s, p_up_s, p_dn_s, p_close, p_low, c5
FROM p WHERE p_up_s IS NOT NULL AND fwd_5d IS NOT NULL
"""
df=c.execute(q).fetchdf()
df.to_parquet('/tmp/wf_pool.parquet', index=False)
print('pool rows', len(df), '| tickers', df.ticker.nunique(),
      '|', df.date.min(), '→', df.date.max())
