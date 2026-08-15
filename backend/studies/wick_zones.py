import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb
from studio.paths import db_path
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
q="""
WITH b AS (
  SELECT ticker,date,open,high,low,close,atr_14,fwd_5d,fwd_10d,
         LAG(high) OVER w AS p_high, LAG(low) OVER w AS p_low,
         GREATEST(LAG(open) OVER w, LAG(close) OVER w) AS p_top,
         LEAST(LAG(open) OVER w, LAG(close) OVER w)    AS p_bot
  FROM bars WHERE universe='sp500' AND high>low AND close>0
  WINDOW w AS (PARTITION BY ticker ORDER BY date)),
s AS (SELECT *, p_high-p_top AS up_wick, p_bot-p_low AS lo_wick,
             p_high-p_low AS p_rng FROM b
      WHERE p_high IS NOT NULL AND p_high>p_low),
f AS (SELECT ticker,date,close,atr_14,fwd_5d,fwd_10d,
        (up_wick>=0.20*p_rng AND open>=p_top AND open<=p_high) AS open_in_up,
        (lo_wick>=0.20*p_rng AND open>=p_low AND open<=p_bot
                            AND close>=p_low AND close<=p_bot) AS lo_held,
        close, p_top, p_high
      FROM s)
SELECT ticker,date,close,atr_14,fwd_5d,fwd_10d,
       (open_in_up AND close>=p_top AND close<=p_high) AS held,
       (open_in_up AND close>p_high)                   AS broke,
       (open_in_up AND close<p_top)                    AS fell,
       lo_held
FROM f
"""
df=c.execute(q).fetchdf()
df.to_parquet('/tmp/wick_zones.parquet', index=False)
print('rows', len(df), '| fires:', {k:int(df[k].sum()) for k in ('held','broke','fell','lo_held')})
