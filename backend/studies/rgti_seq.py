import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from studio.paths import db_path
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
q="""
WITH b AS (SELECT ticker,date,close,phys_r,phys_regime,fwd_5d,fwd_20d,
     LAG(phys_r) OVER w r1, LAG(phys_regime) OVER w g1,
     LAG(phys_r,2) OVER w r2, LAG(phys_regime,2) OVER w g2,
     LAG(phys_r,3) OVER w r3, LAG(phys_regime,3) OVER w g3,
     MAX(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 30 FOLLOWING) fwd_max
   FROM bars WHERE universe IN ('sp500','nasdaq','russell2k') AND close>0
   WINDOW w AS (PARTITION BY ticker ORDER BY date))
SELECT COUNT(*) n,
  MEDIAN(fwd_20d) med20,
  AVG(CASE WHEN fwd_max/close-1 >= 0.50 THEN 1.0 ELSE 0 END)*100 pct_50up,
  AVG(CASE WHEN fwd_max/close-1 >= 1.00 THEN 1.0 ELSE 0 END)*100 pct_100up
FROM b WHERE {cond}
"""
base = c.execute(q.format(cond="fwd_20d IS NOT NULL AND fwd_max IS NOT NULL")).fetchone()
seq  = c.execute(q.format(cond="""r1='RA' AND g1='D' AND r2='RA' AND g2='D' AND r3='RA' AND g3='D'
   AND phys_r='RF' AND phys_regime='U' AND fwd_20d IS NOT NULL AND fwd_max IS NOT NULL""")).fetchone()
two  = c.execute(q.format(cond="""r1='RA' AND g1='D' AND r2='RA' AND g2='D'
   AND phys_r='RF' AND phys_regime='U' AND fwd_20d IS NOT NULL AND fwd_max IS NOT NULL""")).fetchone()
flip = c.execute(q.format(cond="""r1='RA' AND g1='D'
   AND phys_r='RF' AND phys_regime='U' AND fwd_20d IS NOT NULL AND fwd_max IS NOT NULL""")).fetchone()
print(f"{'':<34}{'n':>10}{'med fwd20':>12}{'+50% in 30d':>14}{'+100%':>9}")
for lbl,r in (("ALL bars (baseline)",base),("RA·D ×1 → RF·U",flip),
              ("RA·D ×2 → RF·U",two),("RA·D ×3 → RF·U  (the RGTI shape)",seq)):
    print(f"{lbl:<34}{r[0]:>10,}{r[1]:>11.2f}%{r[2]:>13.2f}%{r[3]:>8.2f}%")
