import sys
sys.path.insert(0,'/Users/sachoki/.claude/skills/quant-study/scripts')
import pandas as pd, numpy as np
from analysis_kit import Study

df = pd.read_parquet('/tmp/wf_pool.parquet')
df['date'] = pd.to_datetime(df['date'])

# MINING WINDOW ONLY. 2024-2026 is reserved and not touched until the definition is frozen.
MINE = df[df.date < '2024-01-01'].copy()
print(f"mining window {MINE.date.min().date()} → {MINE.date.max().date()}  n={len(MINE):,}")
print(f"reserved OOS  2024-01-01 → {df.date.max().date()}  n={len(df[df.date>='2024-01-01']):,}\n")

st = Study("does the Wick-Flip pair beat a matched baseline?",
           n_trials=16, outcome="fwd_5d", time_col="date", unit="%")

st.describe(MINE, "up_s")
st.describe(MINE, "dn_s")
st.baseline(MINE)

def pair(d, wick, opp, mult=3.0):
    b1 = (d.p_up_s >= wick) & (d.p_dn_s <= opp) & (d.p_up_s >= mult*d.p_dn_s)
    b2 = (d.dn_s   >= wick) & (d.up_s   <= opp) & (d.dn_s   >= mult*d.up_s)
    return b1 & b2, b1, b2

# ── main hypothesis in BANDS, so a plateau is visible instead of a peak ──
for wick, opp in ((0.30,0.15),(0.40,0.10),(0.50,0.05),(0.60,0.05)):
    m,_,_ = pair(MINE, wick, opp)
    st.cell(MINE, f"PAIR wick>={wick:.0%} opp<={opp:.0%}", m, requires=["up_s","dn_s"])

# ── the control that decides whether bar 1 is doing anything ──
m50, b1_50, b2_50 = pair(MINE, 0.50, 0.05)
st.cell(MINE, "bar2 ALONE (hammer, wick>=50%)", b2_50, requires=["dn_s"])
st.cell(MINE, "bar1 ALONE (top-heavy, wick>=50%)", b1_50, requires=["up_s"])
