import sys
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import duckdb, numpy as np, pandas as pd
from studio.paths import db_path
from analysis_kit import Study
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)

def run(o,h,l,cl):
    n=len(o); top=h[0]; bot=l[0]
    bull_pb=bear_pb=False; pot_top=pot_bot=0.0; bi=ai=0
    plus=np.zeros(n,bool); minus=np.zeros(n,bool)
    for i in range(1,n):
        if cl[i-1]>o[i-1] and not bear_pb: bear_pb=True; pot_top=o[i-1]; bi=i-1
        if cl[i-1]<o[i-1] and not bull_pb: bull_pb=True; pot_bot=o[i-1]; ai=i-1
        if bull_pb:
            if o[i]<pot_bot: pot_bot=o[i]; ai=i
            if cl[i]<o[i] and o[i]>pot_bot: pot_bot=o[i]; ai=i
        if bear_pb:
            if o[i]>pot_top: pot_top=o[i]; bi=i
            if cl[i]>o[i] and o[i]<pot_top: pot_top=o[i]; bi=i
        if l[i]<bot:
            bot=l[i]
            if (bear_pb and (i-bi)!=0) or (cl[i-1]>o[i-1] and cl[i]<o[i]):
                bear_pb=False; plus[i]=True
        if h[i]>top:
            top=h[i]
            if (bull_pb and (i-ai)!=0) or (cl[i-1]<o[i-1] and cl[i]>o[i]):
                bull_pb=False; minus[i]=True
    return plus,minus

frames=[]
for t in [r[0] for r in c.execute("SELECT DISTINCT ticker FROM bars WHERE universe='sp500' ORDER BY ticker").fetchall()]:
    d=c.execute("""SELECT date,open,high,low,close,fwd_5d FROM bars
                   WHERE ticker=? AND universe='sp500' ORDER BY date""",[t]).fetchdf()
    if len(d)<60: continue
    o,h,l,cl=[d[x].values.astype(float) for x in ('open','high','low','close')]
    p,m=run(o,h,l,cl)
    d['plus']=p; d['minus']=m; frames.append(d[['date','fwd_5d','plus','minus']])
df=pd.concat(frames, ignore_index=True); df["date"]=pd.to_datetime(df.date); df=df[df.fwd_5d.notna()].reset_index(drop=True)

OOS=df[df.date>="2024-01-01"].reset_index(drop=True)
st=Study("fixed +CISD on the reserved window", n_trials=3, outcome="fwd_5d",
         time_col="date", unit="%")
st.describe(OOS,"fwd_5d"); st.baseline(OOS)
cc=st.cell(OOS,"+CISD [engine fix, frozen]", OOS.plus, requires=["fwd_5d"])
st.cell(OOS,"-CISD (the other half)", OOS.minus, requires=["fwd_5d"])
print(st.verdict(cc, mined_window="2021-05→2023-12", oos_window="2024-01→2026-08"))
