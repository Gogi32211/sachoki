import sys
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from studio.paths import db_path
from analysis_kit import Study
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
q="""WITH b AS (SELECT ticker,date,open,high,low,close,fwd_5d,
        (high-GREATEST(open,close))/(high-low) AS up_s,
        (LEAST(open,close)-low)/(high-low)     AS dn_s,
        LAG((high-GREATEST(open,close))/(high-low)) OVER w AS p_up_s,
        LAG((LEAST(open,close)-low)/(high-low))     OVER w AS p_dn_s
      FROM bars WHERE universe='sp500' AND high>low AND close>0
      WINDOW w AS (PARTITION BY ticker ORDER BY date))
SELECT ticker,date,fwd_5d,
  (p_up_s>=0.30 AND p_dn_s<=0.20 AND p_up_s>=3*p_dn_s
   AND dn_s>=0.30 AND up_s<=0.20 AND dn_s>=3*up_s) AS wf,
  close<open AS red
FROM b WHERE fwd_5d IS NOT NULL"""
df=c.execute(q).fetchdf(); df['date']=pd.to_datetime(df['date'])
# the first bar of every ticker has no predecessor → wf is NULL, not False
df['wf']=df.wf.fillna(False).astype(bool); df['red']=df.red.fillna(False).astype(bool)
OOS=df[df.date>='2024-01-01'].copy()
st=Study("FROZEN wick-flip with a RED second bar, on the reserved window",
         n_trials=3, outcome="fwd_5d", time_col="date", unit="%")
st.describe(OOS,"fwd_5d"); st.baseline(OOS)
cell=st.cell(OOS,"wickflip × bar2 RED [FROZEN]", OOS.wf&OOS.red, requires=["fwd_5d"])
st.cell(OOS,"wickflip alone (the competitor)", OOS.wf.astype(bool), requires=["fwd_5d"])
print(st.verdict(cell, mined_window="2021-05→2023-12", oos_window="2024-01→2026-08"))
