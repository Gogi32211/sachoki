import sys
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import pandas as pd, numpy as np
from analysis_kit import Study

df = pd.read_parquet('/tmp/wf_pool.parquet'); df['date']=pd.to_datetime(df['date'])

# FROZEN on the mining window: wick >= 50% of range, opposite <= 5%, ratio >= 3.
# Chosen because 50% and 60% agree (a plateau), not because 50% was the peak.
WICK, OPP, MULT = 0.50, 0.05, 3.0
def pair(d):
    b1=(d.p_up_s>=WICK)&(d.p_dn_s<=OPP)&(d.p_up_s>=MULT*d.p_dn_s)
    b2=(d.dn_s  >=WICK)&(d.up_s  <=OPP)&(d.dn_s  >=MULT*d.up_s)
    return b1&b2, b2

OOS = df[df.date>='2024-01-01'].copy()
st = Study("FROZEN Wick-Flip on the reserved window", n_trials=3,
           outcome="fwd_5d", time_col="date", unit="%")
st.describe(OOS,"up_s"); st.describe(OOS,"dn_s")
st.baseline(OOS)
m, b2 = pair(OOS)
c = st.cell(OOS, f"PAIR wick>={WICK:.0%} opp<={OPP:.0%}  [FROZEN]", m, requires=["up_s","dn_s"])
st.cell(OOS, "bar2 ALONE (the real competitor)", b2, requires=["dn_s"])
print(st.verdict(c, mined_window="2021-05→2023-12", oos_window="2024-01→2026-08"))
