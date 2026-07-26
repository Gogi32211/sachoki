"""T-Z-T6 backtest — same logic as T-Z-T4 but on T6 signal."""
import duckdb, pandas as pd, numpy as np

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
START_BAR = 3; MAX_FILL = 12; MAX_HOLD = 20; RR = 3.0; TOTAL = MAX_FILL + MAX_HOLD

leads = ",\n".join([
    f"LEAD(low,{i}) OVER (PARTITION BY ticker ORDER BY date) AS fl{i},"
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
    SELECT ticker, date, close, high AS t6_high, low AS t6_low,
           (high+low)/2.0 AS midpoint, universe,
           CAST(sig_t6 AS DOUBLE) AS t6,
           CAST(LAG(sig_z,1) OVER (PARTITION BY ticker ORDER BY date) AS DOUBLE) AS z_lag1,
           CAST(LAG(sig_t,2) OVER (PARTITION BY ticker ORDER BY date) AS DOUBLE) AS t_lag2,
           {leads}
    FROM deduped
    WHERE date >= '2022-01-01' AND close > 0
)
SELECT * FROM lagged WHERE t6 > 0 AND z_lag1 > 0 AND t_lag2 > 0
""").df()

df_plain = con.execute(f"""
WITH deduped AS (
    SELECT * FROM bars
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
)
SELECT ticker, date, close, high AS t6_high, low AS t6_low,
       (high+low)/2.0 AS midpoint, universe,
       {leads}
FROM deduped
WHERE CAST(sig_t6 AS DOUBLE) > 0 AND date >= '2022-01-01' AND close > 0
""").df()

con.close()
print(f"T-Z-T6: {len(df):,}   Plain T6: {len(df_plain):,}   ratio: {len(df)/len(df_plain):.1%}")


def simulate(data, rr=RR):
    n = len(data)
    entry = data['midpoint'].values
    sl    = data['t6_low'].values
    risk  = entry - sl
    tgt   = entry + rr * risk
    ou    = np.full(n, 'no_fill', dtype=object)
    pu    = np.zeros(n)
    for j in range(n):
        m = entry[j]; s = sl[j]; tp = tgt[j]; r = risk[j]
        if r <= 0 or np.isnan(m) or m <= 0: continue
        fb = -1
        for i in range(START_BAR, MAX_FILL + 1):
            lo = data[f'fl{i}'].iloc[j]
            if np.isnan(lo): break
            if lo <= m: fb = i; break
        if fb == -1: continue
        ou[j] = 'timeout'
        for i in range(fb, TOTAL + 1):
            lo = data[f'fl{i}'].iloc[j]; hi = data[f'fh{i}'].iloc[j]
            if np.isnan(lo): break
            if lo <= s: ou[j] = 'loss'; pu[j] = -(r / m); break
            if hi >= tp: ou[j] = 'win';  pu[j] = rr * r / m; break
    return ou, pu


def show(mask, ou_a, pu_a, tot, label):
    so = ou_a[mask]; sp = pu_a[mask]
    nf = (so != 'no_fill').sum()
    if nf == 0: print(f"  {label}: —"); return
    w = (so=='win').sum(); l = (so=='loss').sum(); t = (so=='timeout').sum()
    aw = sp[so=='win'].mean()     if w else 0
    al = sp[so=='loss'].mean()    if l else 0
    at = sp[so=='timeout'].mean() if t else 0
    exp = (w/nf)*aw + (l/nf)*al + (t/nf)*at
    print(f"  {label:<26} N={nf:>6,}  fill%={nf/tot:>5.1%}  Win={w/nf:>6.1%}  Exp={exp:>+7.2%}  W={aw:>+6.1%}  L={al:>+6.1%}")


ou, pu           = simulate(df)
ou_p, pu_p       = simulate(df_plain)
n                = len(df)
n_p              = len(df_plain)

sep = "=" * 76
print(f"\n{sep}")
print(f"T-Z-T6  |  entry=midpoint  skip bar1-2  fill bar3-{MAX_FILL}  RR=1:{RR:.0f}")
print(sep)
show(np.ones(n,bool), ou, pu, n, "All T-Z-T6")

print(f"\n{sep}\nUNIVERSE\n{sep}")
for u in ['sp500','nasdaq','russell2k']:
    m = df['universe'].values == u
    show(m, ou, pu, m.sum(), u.upper())

df['year'] = pd.to_datetime(df['date']).dt.year
print(f"\n{sep}\nYEAR\n{sep}")
print(f"  {'Year':<6} {'N_fill':>7} {'fill%':>6} {'Win%':>7} {'Expect':>8} {'AvgW':>7} {'AvgL':>7}")
print("  " + "-"*52)
for yr in sorted(df['year'].unique()):
    mask = df['year'].values == yr
    so = ou[mask]; sp = pu[mask]
    nf = (so!='no_fill').sum()
    if nf == 0: continue
    w=(so=='win').sum(); l=(so=='loss').sum(); t=(so=='timeout').sum()
    aw=sp[so=='win'].mean() if w else 0
    al=sp[so=='loss'].mean() if l else 0
    at=sp[so=='timeout'].mean() if t else 0
    exp=(w/nf)*aw+(l/nf)*al+(t/nf)*at
    print(f"  {yr:<6} {nf:>7,} {nf/mask.sum():>6.1%} {w/nf:>7.1%} {exp:>+8.2%} {aw:>+7.1%} {al:>+7.1%}")

print(f"\n{sep}\nSHEDAREBA: Plain T6 vs T-Z-T6 vs T-Z-T4\n{sep}")
show(np.ones(n_p,bool), ou_p, pu_p, n_p, "Plain T6")
show(np.ones(n,bool),   ou,   pu,   n,   "T-Z-T6")
print(f"  {'T-Z-T4 (reference)':<26} N=75,708  fill%=81.3%  Win= 17.5%  Exp= -0.79%  W= +8.9%  L= -2.9%")

print(f"\n{sep}\nRR SENSITIVITY — T-Z-T6\n{sep}")
print(f"  {'RR':>5} {'Win%':>7} {'Expect':>9}")
print("  " + "-"*26)
for rr in [1.5, 2.0, 2.5, 3.0, 4.0, 5.0]:
    ou_r, pu_r = simulate(df, rr)
    nf_ = (ou_r!='no_fill').sum()
    if nf_==0: continue
    w_=(ou_r=='win').sum(); l_=(ou_r=='loss').sum(); t_=(ou_r=='timeout').sum()
    aw_=pu_r[ou_r=='win'].mean() if w_ else 0
    al_=pu_r[ou_r=='loss'].mean() if l_ else 0
    at_=pu_r[ou_r=='timeout'].mean() if t_ else 0
    exp_=(w_/nf_)*aw_+(l_/nf_)*al_+(t_/nf_)*at_
    print(f"  1:{rr:<4.1f} {w_/nf_:>7.1%} {exp_:>+9.2%}")

print("\nDONE.")
