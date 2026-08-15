import sys
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import pandas as pd, numpy as np
from analysis_kit import Study

df = pd.read_parquet('/tmp/wick_zones.parquet'); df['date']=pd.to_datetime(df['date'])
df = df.sort_values(['ticker','date']).reset_index(drop=True)

SIGS=['held','broke','fell','lo_held']
g = df.groupby('ticker')
for s in SIGS:
    # consecutive run length ending on this bar, and count in the last 5 bars
    grp = df.groupby('ticker')[s]
    df[f'{s}_run'] = grp.transform(lambda x: x.groupby((~x).cumsum()).cumcount() + 1) * df[s]
    df[f'{s}_5'] = grp.transform(lambda x: x.rolling(5, min_periods=1).sum())

st = Study("which wick-zone signal pays, alone and in runs?",
           n_trials=16, outcome="fwd_5d", time_col="date", unit="%")
st.describe(df,"fwd_5d")
st.baseline(df)

print("\n── each signal alone ──")
for s in SIGS:
    st.cell(df, f"{s}", df[s].astype(bool), requires=["fwd_5d"])

print("\n── runs: N consecutive bars carrying it ──")
for s in SIGS:
    st.cell(df, f"{s} run>=2", df[f'{s}_run']>=2, requires=["fwd_5d"])
for s in SIGS:
    st.cell(df, f"{s} run>=3", df[f'{s}_run']>=3, requires=["fwd_5d"])

print("\n── clusters: >=3 of the last 5 bars ──")
for s in SIGS:
    st.cell(df, f"{s} >=3 of last 5", df[f'{s}_5']>=3, requires=["fwd_5d"])
