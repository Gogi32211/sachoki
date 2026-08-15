import sys
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import pandas as pd, numpy as np
from analysis_kit import Study
df=pd.read_parquet('/tmp/tz_zone.parquet'); df['date']=pd.to_datetime(df['date'])
T=df[df.t_sig.notna()&(df.t_sig!='')].copy()
# what the existing script already encodes
T['suffix']=np.where(T.zone.isin(['ABOVE the wick','IN the wick']),'A',
             np.where(T.zone=='in the body','I','O'))

MINE=T[T.date<'2024-01-01']; OOS=T[T.date>='2024-01-01']
st=Study("does splitting TZ's close-suffix by the wick zones add anything?",
         n_trials=10, outcome="fwd_5d", time_col="date", unit="%")
st.describe(MINE,"fwd_5d"); st.baseline(MINE)
print("\n── what the script already has ──")
for s in ['A','I','O']:
    st.cell(MINE,f"suffix {s} (existing)", MINE.suffix==s, requires=["fwd_5d"])
print("\n── what splitting them adds ──")
for z in ['ABOVE the wick','IN the wick','in the lower wick','BELOW everything']:
    st.cell(MINE,f"  → {z}", MINE.zone==z, requires=["fwd_5d"])

print("\n"+"="*88)
print("FROZEN: 'O' split into its two halves, scored once on 2024-2026")
st2=Study("TZ close BELOW the previous low, out of sample", n_trials=3,
          outcome="fwd_5d", time_col="date", unit="%")
st2.describe(OOS,"fwd_5d"); st2.baseline(OOS)
c=st2.cell(OOS,"BELOW everything [FROZEN]", OOS.zone=='BELOW everything', requires=["fwd_5d"])
st2.cell(OOS,"suffix O (the competitor)", OOS.suffix=='O', requires=["fwd_5d"])
print(st2.verdict(c, mined_window="2021-05→2023-12", oos_window="2024-01→2026-08"))
