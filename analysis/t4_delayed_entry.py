"""
t4_delayed_entry.py
Strategy:
  Bar 0 : T4 signal
  Bar 1-2: skip (no entry)
  Bar 3+ : if low <= entry_level → fill
  Entry  : T4 midpoint  (high+low)/2
  Stop   : T4 low
  Target : entry + 3 * (entry - stop)  →  1:3 RR
  Max fill search: bar 3..12
  Max hold after fill: 20 bars
"""
import duckdb, pandas as pd, numpy as np

DB        = "/Users/sachoki/Downloads/studio_analytics.duckdb"
SKIP_BARS = 2          # bars 1 and 2 are skipped
START_BAR = SKIP_BARS + 1   # fill search starts at bar 3
MAX_FILL  = 12         # look until bar 12 (so bars 3-12)
MAX_HOLD  = 20
RR        = 3.0
TOTAL     = MAX_FILL + MAX_HOLD

def lead_cols():
    parts = []
    for i in range(1, TOTAL + 1):
        parts.append(f"LEAD(low, {i}) OVER (PARTITION BY ticker ORDER BY date) AS fl{i}")
        parts.append(f"LEAD(high,{i}) OVER (PARTITION BY ticker ORDER BY date) AS fh{i}")
    return ",\n           ".join(parts)

con = duckdb.connect(DB, read_only=True)
df = con.execute(f"""
WITH deduped AS (
    SELECT * FROM bars
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
)
SELECT ticker, date, close, high AS t4_high, low AS t4_low,
       (high + low) / 2.0 AS midpoint,
       universe,
       {lead_cols()}
FROM deduped
WHERE CAST(sig_t4 AS DOUBLE) > 0
  AND date >= '2022-01-01'
  AND close > 0
""").df()
con.close()
print(f"T4 signals: {len(df):,}  tickers={df['ticker'].nunique():,}")

n     = len(df)
entry = df['midpoint'].values
sl    = df['t4_low'].values
risk  = entry - sl
tgt   = entry + RR * risk

outcomes  = np.full(n, 'no_fill', dtype=object)
fill_bars = np.full(n, np.nan)
hold_bars = np.full(n, np.nan)
pnl       = np.zeros(n)

for j in range(n):
    m = entry[j]; s = sl[j]; tp = tgt[j]; r = risk[j]
    if r <= 0 or np.isnan(m) or m <= 0: continue

    # bars 1-2: skip entirely
    # fill search: bars START_BAR .. MAX_FILL
    fill_bar = -1
    for i in range(START_BAR, MAX_FILL + 1):
        lo = df[f'fl{i}'].iloc[j]
        if np.isnan(lo): break
        if lo <= m:
            fill_bar = i
            break

    if fill_bar == -1:
        continue

    fill_bars[j] = fill_bar
    outcomes[j]  = 'timeout'

    # hold: from fill_bar onward (stop-first within each bar)
    for i in range(fill_bar, TOTAL + 1):
        lo = df[f'fl{i}'].iloc[j]
        hi = df[f'fh{i}'].iloc[j]
        if np.isnan(lo): break
        if lo <= s:
            outcomes[j]  = 'loss'
            hold_bars[j] = i - fill_bar
            pnl[j]       = -(r / m)
            break
        if hi >= tp:
            outcomes[j]  = 'win'
            hold_bars[j] = i - fill_bar
            pnl[j]       = RR * r / m
            break


def stats(mask, label):
    so = outcomes[mask]; sp = pnl[mask]
    nf = (so != 'no_fill').sum()
    if nf == 0:
        print(f"  {label}: no fills")
        return
    w = (so=='win').sum(); l = (so=='loss').sum(); t = (so=='timeout').sum()
    aw = sp[so=='win'].mean()     if w else 0
    al = sp[so=='loss'].mean()    if l else 0
    at = sp[so=='timeout'].mean() if t else 0
    exp = (w/nf)*aw + (l/nf)*al + (t/nf)*at
    print(f"  {label:<22} N={nf:>7,}  fill%={nf/mask.sum():>5.1%}"
          f"  Win={w/nf:>6.1%}  Exp={exp:>+7.2%}"
          f"  W={aw:>+6.1%}  L={al:>+6.1%}  TO={at:>+6.1%}")


sep = "=" * 75
print(f"\n{sep}")
print(f"T4 DELAYED ENTRY (skip bar 1-2, fill bar 3-{MAX_FILL})  |  RR=1:{RR:.0f}")
print(sep)
filled = outcomes != 'no_fill'
print(f"Fill rate (bar 3-{MAX_FILL}): {filled.mean():.1%}  ({filled.sum():,} trades)")
print()
all_mask = np.ones(n, dtype=bool)
stats(all_mask, "All T4")

print(f"\n{sep}")
print("UNIVERSE BREAKDOWN")
print(sep)
for u in ['sp500', 'nasdaq', 'russell2k']:
    stats(df['universe'].values == u, u.upper())

print(f"\n{sep}")
print("YEAR BREAKDOWN")
print(sep)
df['year'] = pd.to_datetime(df['date']).dt.year
print(f"  {'Year':<6} {'N_fill':>8} {'fill%':>6} {'Win%':>7} {'Expect':>8} {'AvgW':>7} {'AvgL':>7}")
print("  " + "-" * 55)
for yr in sorted(df['year'].unique()):
    mask = df['year'].values == yr
    so = outcomes[mask]; sp = pnl[mask]
    nf = (so != 'no_fill').sum()
    if nf == 0: continue
    w=(so=='win').sum(); l=(so=='loss').sum(); t=(so=='timeout').sum()
    aw=sp[so=='win'].mean() if w else 0
    al=sp[so=='loss'].mean() if l else 0
    at=sp[so=='timeout'].mean() if t else 0
    exp=(w/nf)*aw+(l/nf)*al+(t/nf)*at
    print(f"  {yr:<6} {nf:>8,} {nf/mask.sum():>6.1%} {w/nf:>7.1%} {exp:>+8.2%} {aw:>+7.1%} {al:>+7.1%}")

print(f"\n{sep}")
print(f"FILL SPEED — რომელ bar-ზე ხდება fill (bar 3+)")
print(sep)
fb = fill_bars[filled]
n_filled = filled.sum()
for d in range(START_BAR, MAX_FILL + 1):
    cnt = int((fb == d).sum())
    if cnt:
        print(f"  bar {d:>2}: {cnt:>6,}  ({cnt/n_filled:.1%})")

print(f"\n{sep}")
print("RR SENSITIVITY")
print(sep)
print(f"  {'RR':>5} {'Win%':>7} {'Expect':>9}")
print("  " + "-" * 26)
for rr in [1.5, 2.0, 2.5, 3.0, 4.0, 5.0]:
    tgt_rr = entry + rr * risk
    ou = np.full(n, 'no_fill', dtype=object)
    pu = np.zeros(n)
    for j in range(n):
        m_=entry[j]; s_=sl[j]; tp_=tgt_rr[j]; r_=risk[j]
        if r_<=0 or np.isnan(m_) or m_<=0: continue
        fb_ = -1
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
