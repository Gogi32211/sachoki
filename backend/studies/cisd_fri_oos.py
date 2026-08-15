import sys
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import duckdb, pandas as pd
from studio.paths import db_path
from analysis_kit import Study
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
df=c.execute("""SELECT date, fwd_5d, sig_cisd_plus_struct AS cis, sig_fri64
  FROM bars WHERE universe='sp500' AND fwd_5d IS NOT NULL""").fetchdf()
df['date']=pd.to_datetime(df.date); df=df.reset_index(drop=True)
OOS=df[df.date>='2024-01-01'].reset_index(drop=True)
st=Study("FROZEN: +CISD x FRI64 on the reserved window", n_trials=4,
         outcome="fwd_5d", time_col="date", unit="%")
st.describe(OOS,"fwd_5d"); st.baseline(OOS)
cis=OOS.cis==1; fri=OOS.sig_fri64==1
a=st.cell(OOS,"+CISD alone", cis, requires=["fwd_5d"])
b=st.cell(OOS,"FRI64 alone", fri, requires=["fwd_5d"])
cc=st.cell(OOS,"+CISD x FRI64  [FROZEN]", cis&fri, requires=["fwd_5d"])
print(f"\n  lift over the better component: {cc.est - max(a.est,b.est):+.3f}pp")
print(st.verdict(cc, mined_window="2021-05→2023-12", oos_window="2024-01→2026-08"))
