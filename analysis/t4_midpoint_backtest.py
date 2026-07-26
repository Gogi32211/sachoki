"""
t4_midpoint_backtest.py
Strategy:
  1. T4 signal fires on bar N
  2. Wait for price to pull back to T4 bar's midpoint (high+low)/2
  3. Entry at midpoint
  4. Stop: T4 bar low
  5. Target: midpoint + 3 * (midpoint - T4_low)  → 1:3 RR
  6. Path simulation: stop-first within each bar
"""
import duckdb, pandas as pd, numpy as np

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
MAX_FILL = 10
MAX_HOLD = 20
RR       = 3.0

def lead_cols():
    parts = []
    for i in range(1, MAX_FILL + MAX_HOLD + 1):
        parts.append(f"LEAD(low,{i})  OVER (PARTITION BY ticker ORDER BY date) AS fl{i}")
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

n   = len(df)
mid = df['midpoint'].values
sl  = df['t4_low'].values
rsk = mid - sl              # risk per share (half the bar range)
tgt = mid + RR * rsk

outcomes  = np.full(n, 'no_fill', dtype=object)
fill_bars = np.full(n, np.nan)
hold_bars = np.full(n, np.nan)
pnl       = np.zeros(n)

for j in range(n):
    m = mid[j]; s = sl[j]; tp = tgt[j]; r = rsk[j]
    if r <= 0 or np.isnan(m) or m <= 0: continue

    # Phase 1: find fill bar (low <= midpoint)
    fill_bar = -1
    for i in range(1, MAX_FILL + 1):
        lo = df[f'fl{i}'].iloc[j]
        if np.isnan(lo): break
        if lo <= m:
            fill_bar = i
            break

    if fill_bar == -1:
        continue

    fill_bars[j] = fill_bar
    outcomes[j]  = 'timeout'

    # Phase 2: from fill bar onward (same bar can stop or target)
    for i in range(fill_bar, MAX_FILL + MAX_HOLD + 1):
        lo = df[f'fl{i}'].iloc[j]
        hi = df[f'fh{i}'].iloc[j]
        if np.isnan(lo): break
        # stop first
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


def show_stats(mask, label):
    sub_out = outcomes[mask]
    sub_pnl = pnl[mask]
    nf = (sub_out != 'no_fill').sum()
    if nf == 0:
        print(f"  {label}: no fills")
        return
    w = (sub_out == 'win').sum()
    l = (sub_out == 'loss').sum()
    t = (sub_out == 'timeout').sum()
    avg_w  = sub_pnl[sub_out == 'win'].mean()     if w else 0
    avg_l  = sub_pnl[sub_out == 'loss'].mean()    if l else 0
    avg_to = sub_pnl[sub_out == 'timeout'].mean() if t else 0
    exp    = (w/nf)*avg_w + (l/nf)*avg_l + (t/nf)*avg_to
    print(f"  {label:<22} N={nf:>7,}  Win={w/nf:>6.1%}  Exp={exp:>+7.2%}"
          f"  W={avg_w:>+6.1%}  L={avg_l:>+6.1%}  TO={avg_to:>+6.1%}")


sep = "=" * 72
print(f"\n{sep}")
print(f"T4 MIDPOINT PULLBACK  |  RR=1:{RR:.0f}  max_fill={MAX_FILL}d  max_hold={MAX_HOLD}d")
print(sep)
filled = outcomes != 'no_fill'
print(f"Fill rate (midpoint hit ≤{MAX_FILL}d): {filled.mean():.1%}  ({filled.sum():,} trades)")
print()
show_stats(np.ones(n, bool), "All T4")

# ── Universe breakdown ──────────────────────────────────────────────────────
print(f"\n{sep}")
print("UNIVERSE BREAKDOWN")
print(sep)
for univ in ['sp500', 'nasdaq', 'russell2k']:
    mask = df['universe'].values == univ
    show_stats(mask, univ.upper())

# ── Year breakdown ──────────────────────────────────────────────────────────
print(f"\n{sep}")
print("YEAR BREAKDOWN")
print(sep)
df['year'] = pd.to_datetime(df['date']).dt.year
print(f"  {'Year':<6} {'N_fill':>8} {'Win%':>7} {'Expect':>8} {'AvgW':>7} {'AvgL':>7}")
print("  " + "-" * 47)
for yr in sorted(df['year'].unique()):
    mask = df['year'].values == yr
    sub_out = outcomes[mask]; sub_pnl = pnl[mask]
    nf = (sub_out != 'no_fill').sum()
    if nf == 0: continue
    w = (sub_out=='win').sum(); l = (sub_out=='loss').sum(); t = (sub_out=='timeout').sum()
    aw = sub_pnl[sub_out=='win'].mean()     if w else 0
    al = sub_pnl[sub_out=='loss'].mean()    if l else 0
    at = sub_pnl[sub_out=='timeout'].mean() if t else 0
    exp = (w/nf)*aw + (l/nf)*al + (t/nf)*at
    print(f"  {yr:<6} {nf:>8,} {w/nf:>7.1%} {exp:>+8.2%} {aw:>+7.1%} {al:>+7.1%}")

# ── Fill speed ──────────────────────────────────────────────────────────────
print(f"\n{sep}")
print("FILL SPEED (რომელ bar-ზე ბრუნდება midpoint-ზე)")
print(sep)
fb = fill_bars[filled]
cum = 0
for d in range(1, MAX_FILL + 1):
    cnt = int((fb == d).sum())
    cum += cnt
    if cnt:
        print(f"  bar {d:>2}: {cnt:>6,}  ({cnt/filled.sum():.1%} of fills)  cumul={cum/filled.sum():.1%}")

# ── RR sensitivity ──────────────────────────────────────────────────────────
print(f"\n{sep}")
print("RR SENSITIVITY (same fill universe)")
print(sep)
print(f"  {'RR':>5} {'Win%':>7} {'Expect':>9}")
print("  " + "-" * 24)
for rr in [1.5, 2.0, 2.5, 3.0, 4.0, 5.0]:
    tgt_rr = mid + rr * rsk
    out_rr = np.full(n, 'no_fill', dtype=object)
    pnl_rr = np.zeros(n)
    for j in range(n):
        m_ = mid[j]; s_ = sl[j]; tp_ = tgt_rr[j]; r_ = rsk[j]
        if r_ <= 0 or np.isnan(m_) or m_ <= 0: continue
        fill_bar = -1
        for i in range(1, MAX_FILL + 1):
            lo = df[f'fl{i}'].iloc[j]
            if np.isnan(lo): break
            if lo <= m_: fill_bar = i; break
        if fill_bar == -1: continue
        out_rr[j] = 'timeout'
        for i in range(fill_bar, MAX_FILL + MAX_HOLD + 1):
            lo = df[f'fl{i}'].iloc[j]; hi = df[f'fh{i}'].iloc[j]
            if np.isnan(lo): break
            if lo <= s_: out_rr[j]='loss'; pnl_rr[j]=-(r_/m_); break
            if hi >= tp_: out_rr[j]='win'; pnl_rr[j]=rr*r_/m_; break
    nf_ = (out_rr!='no_fill').sum()
    if nf_==0: continue
    w_=( out_rr=='win').sum(); l_=(out_rr=='loss').sum(); t_=(out_rr=='timeout').sum()
    aw_=pnl_rr[out_rr=='win'].mean()  if w_ else 0
    al_=pnl_rr[out_rr=='loss'].mean() if l_ else 0
    at_=pnl_rr[out_rr=='timeout'].mean() if t_ else 0
    exp_=(w_/nf_)*aw_+(l_/nf_)*al_+(t_/nf_)*at_
    print(f"  1:{rr:<4.1f} {w_/nf_:>7.1%} {exp_:>+9.2%}")

print("\nDONE.")
