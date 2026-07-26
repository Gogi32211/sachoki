"""
T-Z-T4 + RSI filter:
  bar[-2]: any T signal
  bar[-1]: any Z signal
  bar[ 0]: T4 signal  →  reference RSI = rsi_14 at T4 bar
  Entry: skip bar+1,+2 → fill bar 3+ when low <= T4 midpoint
         AND rsi_14 at fill bar > rsi_14 at T4 bar
  Stop : T4 low
  Target: midpoint + 3*(midpoint - T4_low)
"""
import duckdb, pandas as pd, numpy as np

DB        = "/Users/sachoki/Downloads/studio_analytics.duckdb"
START_BAR = 3; MAX_FILL = 12; MAX_HOLD = 20; RR = 3.0; TOTAL = MAX_FILL + MAX_HOLD

lead_parts = []
for i in range(1, TOTAL + 1):
    lead_parts.append(f"LEAD(low,   {i}) OVER (PARTITION BY ticker ORDER BY date) AS fl{i}")
    lead_parts.append(f"LEAD(high,  {i}) OVER (PARTITION BY ticker ORDER BY date) AS fh{i}")
    lead_parts.append(f"LEAD(rsi_14,{i}) OVER (PARTITION BY ticker ORDER BY date) AS fr{i}")
leads_sql = ",\n".join(lead_parts)

con = duckdb.connect(DB, read_only=True)
df = con.execute(f"""
WITH deduped AS (
    SELECT * FROM bars
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
),
lagged AS (
    SELECT ticker, date, close, high AS t4_high, low AS t4_low,
           (high+low)/2.0 AS midpoint, universe, rsi_14,
           CAST(sig_t4 AS DOUBLE) AS t4,
           CAST(LAG(sig_z,1) OVER (PARTITION BY ticker ORDER BY date) AS DOUBLE) AS z_lag1,
           CAST(LAG(sig_t,2) OVER (PARTITION BY ticker ORDER BY date) AS DOUBLE) AS t_lag2,
           {leads_sql}
    FROM deduped
    WHERE date >= '2022-01-01' AND close > 0
)
SELECT * FROM lagged WHERE t4 > 0 AND z_lag1 > 0 AND t_lag2 > 0
""").df()
con.close()
print(f"T-Z-T4 signals: {len(df):,}")

n        = len(df)
entry    = df['midpoint'].values
sl       = df['t4_low'].values
risk     = entry - sl
tgt      = entry + RR * risk
rsi_t4   = df['rsi_14'].values

def simulate(rsi_filter=False):
    ou = np.full(n, 'no_fill', dtype=object)
    pu = np.zeros(n)
    for j in range(n):
        m=entry[j]; s=sl[j]; tp=tgt[j]; r=risk[j]; r0=rsi_t4[j]
        if r<=0 or np.isnan(m) or m<=0: continue
        fb=-1
        for i in range(START_BAR, MAX_FILL+1):
            lo = df[f'fl{i}'].iloc[j]
            if np.isnan(lo): break
            if lo <= m:
                if rsi_filter:
                    ri = df[f'fr{i}'].iloc[j]
                    if np.isnan(ri) or ri <= r0:
                        continue          # RSI ≤ T4 RSI → skip this fill bar
                fb = i; break
        if fb == -1: continue
        ou[j] = 'timeout'
        for i in range(fb, TOTAL+1):
            lo=df[f'fl{i}'].iloc[j]; hi=df[f'fh{i}'].iloc[j]
            if np.isnan(lo): break
            if lo<=s: ou[j]='loss'; pu[j]=-(r/m); break
            if hi>=tp: ou[j]='win';  pu[j]=RR*r/m; break
    return ou, pu

ou_base, pu_base = simulate(rsi_filter=False)
ou_filt, pu_filt = simulate(rsi_filter=True)

def show(ou, pu, mask, label, tot=None):
    so=ou[mask]; sp=pu[mask]
    tot = tot if tot is not None else mask.sum()
    nf=(so!='no_fill').sum()
    if nf==0: print(f"  {label}: —"); return
    w=(so=='win').sum(); l=(so=='loss').sum(); t=(so=='timeout').sum()
    aw=sp[so=='win'].mean()     if w else 0
    al=sp[so=='loss'].mean()    if l else 0
    at=sp[so=='timeout'].mean() if t else 0
    exp=(w/nf)*aw+(l/nf)*al+(t/nf)*at
    print(f"  {label:<32} N={nf:>6,}  fill%={nf/tot:>5.1%}"
          f"  Win={w/nf:>6.1%}  Exp={exp:>+7.2%}  W={aw:>+6.1%}  L={al:>+6.1%}")

sep = "=" * 78
all_m = np.ones(n, bool)

print(f"\n{sep}")
print(f"T-Z-T4  vs  T-Z-T4 + RSI_fill > RSI_T4  |  RR=1:{RR:.0f}")
print(sep)
show(ou_base, pu_base, all_m, "T-Z-T4 baseline")
show(ou_filt, pu_filt, all_m, "T-Z-T4 + RSI filter")

print(f"\n{sep}\nUNIVERSE\n{sep}")
for u in ['sp500','nasdaq','russell2k']:
    m = df['universe'].values == u
    print(f"  -- {u.upper()} --")
    show(ou_base, pu_base, m, "  baseline", m.sum())
    show(ou_filt, pu_filt, m, "  +RSI filter", m.sum())

df['year'] = pd.to_datetime(df['date']).dt.year
print(f"\n{sep}\nYEAR BREAKDOWN\n{sep}")
print(f"  {'Year':<6}  {'':30} {'N_fill':>7} {'fill%':>6} {'Win%':>7} {'Expect':>8}")
print("  " + "-" * 62)
for yr in sorted(df['year'].unique()):
    mask = df['year'].values == yr
    tot  = mask.sum()
    print(f"  {yr} baseline  ", end="")
    show(ou_base, pu_base, mask, "", tot)
    print(f"  {yr} +RSI      ", end="")
    show(ou_filt, pu_filt, mask, "", tot)

# RSI bucket analysis — does higher T4 RSI matter?
print(f"\n{sep}\nRSI BUCKET  (T4 bar RSI)  — +RSI filter\n{sep}")
print(f"  {'RSI range':<14} {'N_fill':>8} {'Win%':>7} {'Expect':>8}")
print("  " + "-"*40)
for lo_r, hi_r in [(0,30),(30,40),(40,50),(50,60),(60,70),(70,100)]:
    mask = (rsi_t4 >= lo_r) & (rsi_t4 < hi_r)
    so=ou_filt[mask]; sp=pu_filt[mask]; tot=mask.sum()
    nf=(so!='no_fill').sum()
    if nf<20: continue
    w=(so=='win').sum(); l=(so=='loss').sum(); t=(so=='timeout').sum()
    aw=sp[so=='win'].mean() if w else 0
    al=sp[so=='loss'].mean() if l else 0
    at=sp[so=='timeout'].mean() if t else 0
    exp=(w/nf)*aw+(l/nf)*al+(t/nf)*at
    print(f"  RSI {lo_r:>2}-{hi_r:<3}       {nf:>8,} {w/nf:>7.1%} {exp:>+8.2%}")

print(f"\n{sep}\nRR SENSITIVITY — +RSI filter\n{sep}")
print(f"  {'RR':>5} {'Win%':>7} {'Expect':>9}")
print("  " + "-"*26)
for rr in [1.5, 2.0, 2.5, 3.0, 4.0, 5.0]:
    tgt_r = entry + rr * risk
    ou_r=np.full(n,'no_fill',dtype=object); pu_r=np.zeros(n)
    for j in range(n):
        m_=entry[j]; s_=sl[j]; tp_=tgt_r[j]; r_=risk[j]; r0_=rsi_t4[j]
        if r_<=0 or np.isnan(m_) or m_<=0: continue
        fb_=-1
        for i in range(START_BAR,MAX_FILL+1):
            lo=df[f'fl{i}'].iloc[j]
            if np.isnan(lo): break
            if lo<=m_:
                ri=df[f'fr{i}'].iloc[j]
                if np.isnan(ri) or ri<=r0_: continue
                fb_=i; break
        if fb_==-1: continue
        ou_r[j]='timeout'
        for i in range(fb_,TOTAL+1):
            lo=df[f'fl{i}'].iloc[j]; hi=df[f'fh{i}'].iloc[j]
            if np.isnan(lo): break
            if lo<=s_: ou_r[j]='loss'; pu_r[j]=-(r_/m_); break
            if hi>=tp_: ou_r[j]='win'; pu_r[j]=rr*r_/m_; break
    nf_=(ou_r!='no_fill').sum()
    if nf_==0: continue
    w_=(ou_r=='win').sum(); l_=(ou_r=='loss').sum(); t_=(ou_r=='timeout').sum()
    aw_=pu_r[ou_r=='win'].mean() if w_ else 0
    al_=pu_r[ou_r=='loss'].mean() if l_ else 0
    at_=pu_r[ou_r=='timeout'].mean() if t_ else 0
    exp_=(w_/nf_)*aw_+(l_/nf_)*al_+(t_/nf_)*at_
    print(f"  1:{rr:<4.1f} {w_/nf_:>7.1%} {exp_:>+9.2%}")

print("\nDONE.")
