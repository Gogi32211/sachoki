import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb
from studio.paths import db_path
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
q="""
WITH b AS (
  SELECT ticker,date,open,high,low,close,l_sig,fwd_5d,
         LAG(high) OVER w AS p_high, LAG(low) OVER w AS p_low,
         GREATEST(LAG(open) OVER w, LAG(close) OVER w) AS p_top,
         LEAST(LAG(open) OVER w, LAG(close) OVER w)    AS p_bot,
         (high-GREATEST(open,close))/(high-low) AS up_s,
         (LEAST(open,close)-low)/(high-low)     AS dn_s,
         LAG((high-GREATEST(open,close))/(high-low)) OVER w AS p_up_s,
         LAG((LEAST(open,close)-low)/(high-low))     OVER w AS p_dn_s
  FROM bars WHERE universe='sp500' AND high>low AND close>0
  WINDOW w AS (PARTITION BY ticker ORDER BY date)),
s AS (SELECT *, p_high-p_top AS up_wick, p_bot-p_low AS lo_wick, p_high-p_low AS p_rng
      FROM b WHERE p_high IS NOT NULL AND p_high>p_low)
SELECT ticker,date,fwd_5d,l_sig,
  (up_wick>=0.20*p_rng AND open>=p_top AND open<=p_high AND close>=p_top AND close<=p_high) AS held,
  (up_wick>=0.20*p_rng AND open>=p_top AND open<=p_high AND close>p_high)                    AS broke,
  (up_wick>=0.20*p_rng AND open>=p_top AND open<=p_high AND close<p_top)                     AS fell,
  (lo_wick>=0.20*p_rng AND open>=p_low AND open<=p_bot AND close>=p_low AND close<=p_bot)    AS lo_held,
  (p_up_s>=0.30 AND p_dn_s<=0.20 AND p_up_s>=3*p_dn_s
   AND dn_s>=0.30 AND up_s<=0.20 AND dn_s>=3*up_s)                                           AS wickflip
FROM s WHERE fwd_5d IS NOT NULL
"""
df=c.execute(q).fetchdf()
df.to_parquet('/tmp/wz_l.parquet', index=False)
print('rows',len(df))
print({k:int(df[k].sum()) for k in ('held','broke','fell','lo_held','wickflip')})
