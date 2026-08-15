import sys
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import pandas as pd, numpy as np
from analysis_kit import Study

df=pd.read_parquet('/tmp/wz_l.parquet'); df['date']=pd.to_datetime(df['date'])
df['L']=df.l_sig.fillna('—')
WF=df.wickflip.astype(bool)
# the ladder, grouped by what it actually encodes: where the close sat in the range
STRONG={'L3','L12','L34'}      # close 62-75% of range, 74-91% green
WEAK  ={'L25','L46','L5'}      # close 25-37% of range,  8-23% green
df['Lstrong']=df.L.isin(STRONG); df['Lweak']=df.L.isin(WEAK)

st=Study("does wick-flip pay when combined with an L code?", n_trials=16,
         outcome="fwd_5d", time_col="date", unit="%")
st.describe(df,"fwd_5d"); st.baseline(df)

print("\n── the two components alone ──")
wf_cell = st.cell(df,"wickflip alone",WF,requires=["fwd_5d"])
for code in ['L12','L46','L3','L25','L5','L34']:
    st.cell(df,f"{code} alone", df.L==code, requires=["fwd_5d"])

print("\n── combined ──")
best={}
for code in ['L12','L46','L3','L25','L5','L34']:
    c=st.cell(df,f"wickflip × {code}", WF&(df.L==code), requires=["fwd_5d"])
    best[code]=c
st.cell(df,"wickflip × STRONG close", WF&df.Lstrong, requires=["fwd_5d"])
st.cell(df,"wickflip × WEAK close",   WF&df.Lweak,   requires=["fwd_5d"])
