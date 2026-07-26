"""
t_z_t4_backtest.py
Combo condition:
  bar[-2]: any bullish T signal  (sig_t > 0)
  bar[-1]: any bearish Z signal  (sig_z > 0)
  bar[ 0]: T4 signal             (sig_t4 > 0)

Entry: skip bar+1 and bar+2, fill from bar+3 when low <= T4 midpoint
Stop : T4 low
Target: midpoint + 3*(midpoint - T4_low)  →  1:3 RR
"""
import duckdb, pandas as pd, numpy as np

DB        = "/Users/sachoki/Downloads/studio_analytics.duckdb"
START_BAR = 3
MAX_FILL  = 12
MAX_HOLD  = 20
RR        = 3.0
TOTAL     = MAX_FILL + MAX_HOLD

def lead_cols():
    return ",\n".join([
        f"LEAD(low,{i})  OVER (PARTITION BY ticker ORDER BY date) AS fl{i},\n"
        f"LEAD(high,{i}) OVER (PARTITION BY ticker ORDER BY date) AS fh{i}"
        for i in range(1, TOTAL + 1)
    ])

con = duckdb.connect(DB, read_only=True)
df = con.execute(f"""
WITH deduped AS (
    SELECT * FROM bars
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
),
lagged AS (
    SELECT ticker, date, close, high AS t4_high, low AS t4_low,
           (high+low)/2.0 AS midpoint, universe,
           CAST(sig_t4 AS DOUBLE) AS t4,
           CAST(LAG(sig_z, 1) OVER (PARTITION BY ticker ORDER BY date) AS DOUBLE) AS z_lag1,
           CAST(LAG(sig_t, 2) OVER (PARTITION BY ticker ORDER BY date) AS DOUBLE) AS t_lag2,
           {lead_cols()}
    FROM deduped
    WHERE date >= '2022-01-01' AND close > 0
)
SELECT * FROM lagged
WHERE t4 > 0 AND z_lag1 > 0 AND t_lag2 > 0
""").df()
con.close()

print(f"T-Z-T4 signals: {len(df):,}  tickers={df['ticker'].nunique():,}")

# also load plain T4 count for comparison
con2 = duckdb.connect(DB, read_only=True)
n_t4_total = con2.execute("""
    SELECT COUNT(*) FROM bars
    WHERE CAST(sig_t4 AS DOUBLE)>0 AND date>='2022-01-01' AND close>0
""").fetchone()[0]
con2.close()
print(f"T4 total (2022+): {n_t4_total:,}  →  T-Z-T4 is {len(df)/n_t4_total:.1%} of all T4")

n     = len(df)
entry = df['midpoint'].values
sl    = df['t4_low'].values
risk  = entry - sl
tgt   = entry + RR * risk

outcomes  = np.full(n, 'no_fill', dtype=object)
fill_bars = np.full(n, np.nan)
pnl       = np.zeros(n)

for j in range(n):
    m=entry[j]; s=sl[j]; tp=tgt[j]; r=risk[j]
    if r<=0 or np.isnan(m) or m<=0: continue
    fb=-1
    for i in range(START_BAR, MAX_FILL+1):
        lo=df[f'fl{i}'].iloc[j]
        if np.isnan(lo): break
        if lo<=m: fb=i; break
    if fb==-1: continue
    fill_bars[j]=fb; outcomes[j]='timeout'
    for i in range(fb, TOTAL+1):
        lo=df[f'fl{i}'].iloc[j]; hi=df[f'fh{i}'].iloc[j]
        if np.isnan(lo): break
        if lo<=s: outcomes[j]='loss'; pnl[j]=-(r/m); break
        if hi>=tp: outcomes[j]='win'; pnl[j]=RR*r/m; break


def show(mask, label):
    so=outcomes[mask]; sp=pnl[mask]
    total_sig = mask.sum()
    nf=(so!='no_fill').sum()
    if nf==0: print(f"  {label}: no fills"); return
    w=(so=='win').sum(); l=(so=='loss').sum(); t=(so=='timeout').sum()
    aw=sp[so=='win'].mean()     if w else 0
    al=sp[so=='loss'].mean()    if l else 0
    at=sp[so=='timeout'].mean() if t else 0
    exp=(w/nf)*aw+(l/nf)*al+(t/nf)*at
    print(f"  {label:<24} N={nf:>6,}  fill%={nf/total_sig:>5.1%}"
          f"  Win={w/nf:>6.1%}  Exp={exp:>+7.2%}"
          f"  W={aw:>+6.1%}  L={al:>+6.1%}  TO={at:>+6.1%}")


sep = "=" * 76
print(f"\n{sep}")
print(f"T-Z-T4 COMBO  |  entry=midpoint  skip=bar1-2  fill=bar3-{MAX_FILL}  RR=1:{RR:.0f}")
print(sep)
filled = outcomes != 'no_fill'
print(f"Fill rate: {filled.mean():.1%}  ({filled.sum():,} trades)\n")
show(np.ones(n, bool), "All T-Z-T4")

print(f"\n{sep}")
print("UNIVERSE")
print(sep)
for u in ['sp500','nasdaq','russell2k']:
    show(df['universe'].values==u, u.upper())

print(f"\n{sep}")
print("YEAR")
print(sep)
df['year'] = pd.to_datetime(df['date']).dt.year
print(f"  {'Year':<6} {'N_fill':>7} {'fill%':>6} {'Win%':>7} {'Expect':>8} {'AvgW':>7} {'AvgL':>7}")
print("  "+"-"*52)
for yr in sorted(df['year'].unique()):
    mask=df['year'].values==yr
    so=outcomes[mask]; sp=pnl[mask]
    nf=(so!='no_fill').sum()
    if nf==0: continue
    w=(so=='win').sum(); l=(so=='loss').sum(); t=(so=='timeout').sum()
    aw=sp[so=='win'].mean() if w else 0
    al=sp[so=='loss'].mean() if l else 0
    at=sp[so=='timeout'].mean() if t else 0
    exp=(w/nf)*aw+(l/nf)*al+(t/nf)*at
    print(f"  {yr:<6} {nf:>7,} {nf/mask.sum():>6.1%} {w/nf:>7.1%} {exp:>+8.2%} {aw:>+7.1%} {al:>+7.1%}")

# compare vs plain T4
print(f"\n{sep}")
print("COMPARISON: plain T4 vs T-Z-T4 (same entry logic, 2025-2026)")
print(sep)

con3 = duckdb.connect(DB, read_only=True)
df_t4 = con3.execute(f"""
WITH deduped AS (
    SELECT * FROM bars
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
)
SELECT ticker, date, close, high AS t4_high, low AS t4_low,
       (high+low)/2.0 AS midpoint, universe,
       {lead_cols()}
FROM deduped
WHERE CAST(sig_t4 AS DOUBLE)>0
  AND date >= '2025-01-01' AND close>0
""").df()
con3.close()

n2=len(df_t4); e2=df_t4['midpoint'].values; s2=df_t4['t4_low'].values
r2=e2-s2; tg2=e2+RR*r2
ou2=np.full(n2,'no_fill',dtype=object); pu2=np.zeros(n2)
for j in range(n2):
    m_=e2[j]; ss=s2[j]; tp_=tg2[j]; rr_=r2[j]
    if rr_<=0 or np.isnan(m_) or m_<=0: continue
    fb_=-1
    for i in range(START_BAR, MAX_FILL+1):
        lo=df_t4[f'fl{i}'].iloc[j]
        if np.isnan(lo): break
        if lo<=m_: fb_=i; break
    if fb_==-1: continue
    ou2[j]='timeout'
    for i in range(fb_, TOTAL+1):
        lo=df_t4[f'fl{i}'].iloc[j]; hi=df_t4[f'fh{i}'].iloc[j]
        if np.isnan(lo): break
        if lo<=ss: ou2[j]='loss'; pu2[j]=-(rr_/m_); break
        if hi>=tp_: ou2[j]='win'; pu2[j]=RR*rr_/m_; break

def quick(ou, pu, label, total):
    nf=(ou!='no_fill').sum()
    if nf==0: return
    w=(ou=='win').sum(); l=(ou=='loss').sum(); t=(ou=='timeout').sum()
    aw=pu[ou=='win'].mean() if w else 0
    al=pu[ou=='loss'].mean() if l else 0
    at=pu[ou=='timeout'].mean() if t else 0
    exp=(w/nf)*aw+(l/nf)*al+(t/nf)*at
    print(f"  {label:<24} N={nf:>6,}  fill%={nf/total:>5.1%}  Win={w/nf:>6.1%}  Exp={exp:>+7.2%}")

df_tz25 = df[df['year']>=2025]
n_tz25 = len(df_tz25)
ou_tz25 = outcomes[df['year'].values>=2025]
pu_tz25 = pnl[df['year'].values>=2025]

quick(ou2, pu2, "Plain T4  (2025-26)", n2)
quick(ou_tz25, pu_tz25, "T-Z-T4   (2025-26)", n_tz25)

print(f"\n{sep}")
print("RR SENSITIVITY — T-Z-T4 (2022-2026)")
print(sep)
print(f"  {'RR':>5} {'Win%':>7} {'Expect':>9}")
print("  "+"-"*26)
for rr in [1.5, 2.0, 2.5, 3.0, 4.0, 5.0]:
    tgt_rr=entry+rr*risk
    ou=np.full(n,'no_fill',dtype=object); pu=np.zeros(n)
    for j in range(n):
        m_=entry[j]; s_=sl[j]; tp_=tgt_rr[j]; r_=risk[j]
        if r_<=0 or np.isnan(m_) or m_<=0: continue
        fb_=-1
        for i in range(START_BAR, MAX_FILL+1):
            lo=df[f'fl{i}'].iloc[j]
            if np.isnan(lo): break
            if lo<=m_: fb_=i; break
        if fb_==-1: continue
        ou[j]='timeout'
        for i in range(fb_, TOTAL+1):
            lo=df[f'fl{i}'].iloc[j]; hi=df[f'fh{i}'].iloc[j]
            if np.isnan(lo): break
            if lo<=s_: ou[j]='loss'; pu[j]=-(r_/m_); break
            if hi>=tp_: ou[j]='win'; pu[j]=rr*r_/m_; break
    nf_=(ou!='no_fill').sum()
    if nf_==0: continue
    w_=(ou=='win').sum(); l_=(ou=='loss').sum(); t_=(ou=='timeout').sum()
    aw_=pu[ou=='win'].mean() if w_ else 0
    al_=pu[ou=='loss'].mean() if l_ else 0
    at_=pu[ou=='timeout'].mean() if t_ else 0
    exp_=(w_/nf_)*aw_+(l_/nf_)*al_+(t_/nf_)*at_
    print(f"  1:{rr:<4.1f} {w_/nf_:>7.1%} {exp_:>+9.2%}")

print("\nDONE.")
