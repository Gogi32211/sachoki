import sys
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import duckdb, pandas as pd, numpy as np
from studio.paths import db_path
from analysis_kit import Study
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
E=['sig_sc','sig_vbo_dn','sig_eb_dn','sig_fri64','sig_rl','sig_clm']
df=c.execute(f"""SELECT date, fwd_5d, sig_cisd_plus_struct AS cis, {', '.join(E)}
  FROM bars WHERE universe='sp500' AND fwd_5d IS NOT NULL""").fetchdf()
df['date']=pd.to_datetime(df.date); df=df.reset_index(drop=True)
st=Study("does +CISD add to an edge, or restate it?", n_trials=20,
         outcome="fwd_5d", time_col="date", unit="%")
st.describe(df,"fwd_5d"); st.baseline(df)
cis=df.cis==1
c_cell=st.cell(df,"+CISD alone", cis, requires=["fwd_5d"])
print()
for e in E:
    m=df[e]==1
    a=st.cell(df,f"{e} alone", m, requires=["fwd_5d"])
    b=st.cell(df,f"  {e} + CISD", m&cis, requires=["fwd_5d"])
    best=max(a.est, c_cell.est)
    print(f"      → vs best component ({'edge' if a.est>c_cell.est else 'CISD'} {best:+.3f}): {b.est-best:+.3f}pp")
