import sys
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import pandas as pd, numpy as np
from analysis_kit import Study

df = pd.read_parquet('/tmp/wick_zones.parquet'); df['date']=pd.to_datetime(df['date'])
df = df.sort_values(['ticker','date']).reset_index(drop=True)
SIGS=['held','broke','fell','lo_held']
for s in SIGS:
    g=df.groupby('ticker')[s]
    df[f'{s}_run']=g.transform(lambda x: x.groupby((~x).cumsum()).cumsum())

print("run-length reality check (how many bars survive each threshold):")
for s in SIGS:
    print(f"   {s:<9} run≥1 {int((df[f'{s}_run']>=1).sum()):>6,}   "
          f"run≥2 {int((df[f'{s}_run']>=2).sum()):>5,}   run≥3 {int((df[f'{s}_run']>=3).sum()):>4,}")

st = Study("do wick-zone signals improve when they repeat?", n_trials=8,
           outcome="fwd_5d", time_col="date", unit="%")
st.describe(df,"fwd_5d"); st.baseline(df)
for s in SIGS:
    st.cell(df, f"{s} run>=2", df[f'{s}_run']>=2, requires=["fwd_5d"])
for s in SIGS:
    st.cell(df, f"{s} run>=3", df[f'{s}_run']>=3, requires=["fwd_5d"])
