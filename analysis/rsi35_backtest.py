"""rsi35_backtest.py — T1 / T4 / T6 signals split by RSI regime.

Compares three RSI regimes:
  - RSI < 35 (oversold — "qvemot vpovnot")
  - 35 ≤ RSI < 60 (neutral)
  - RSI ≥ 60 (momentum)

For each signal (T1, T4, T6) × regime × universe:
  path-sim stop-first, entry=next_open, s12/t100, horizon=20d, DV≥500k.

Also breaks down sequence patterns within RSI<35:
  tzt4 (T[-2] + Z[-1] + T4[0]), ttt6 (T[-2] + T[-1] + T6[0]), t1seq (any + any + T1[0])
"""

import sys, duckdb, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/analysis")
from exit_backtest import sim, is_glitch

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT = "/Users/sachoki/Desktop/sachoki-desktop/RSI35_BACKTEST.md"
UNIS = ("sp500", "nasdaq", "russell2k")
DVFLOOR = 500_000
DEDUP = 5
STOP, TARGET, HZ = 12, 100, 20

con = duckdb.connect(DB, read_only=True)
con.execute("PRAGMA threads=6")

# ── fetch all T1/T4/T6 signal bars with RSI and lookback context ──────────────
print("Fetching signal bars …")
df = con.execute("""
    WITH ranked AS (
      SELECT universe, ticker, date,
             sig_t1, sig_t4, sig_t6,
             sig_z, sig_t,
             rsi_14, rsi_le_35,
             composite_full_suffix AS sfx,
             LAG(sig_z, 1)  OVER (PARTITION BY universe,ticker ORDER BY date) AS z_1,
             LAG(sig_t, 1)  OVER (PARTITION BY universe,ticker ORDER BY date) AS t_1,
             LAG(sig_z, 2)  OVER (PARTITION BY universe,ticker ORDER BY date) AS z_2,
             LAG(sig_t, 2)  OVER (PARTITION BY universe,ticker ORDER BY date) AS t_2,
             LAG(sig_t4, 2) OVER (PARTITION BY universe,ticker ORDER BY date) AS t4_2,
             LAG(sig_t3, 2) OVER (PARTITION BY universe,ticker ORDER BY date) AS t3_2,
             LAG(sig_t9, 2) OVER (PARTITION BY universe,ticker ORDER BY date) AS t9_2,
             LAG(sig_t10,2) OVER (PARTITION BY universe,ticker ORDER BY date) AS t10_2,
             LAG(sig_t2, 2) OVER (PARTITION BY universe,ticker ORDER BY date) AS t2_2,
             LAG(sig_t6, 2) OVER (PARTITION BY universe,ticker ORDER BY date) AS t6_2,
             LAG(sig_t1, 2) OVER (PARTITION BY universe,ticker ORDER BY date) AS t1_2,
             LAG(sig_t, 1)  OVER (PARTITION BY universe,ticker ORDER BY date) AS tany_1
      FROM bars
      WHERE (sig_t1>0 OR sig_t4>0 OR sig_t6>0)
        AND rsi_14 IS NOT NULL
        AND universe IN ('sp500','nasdaq','russell2k')
      QUALIFY ROW_NUMBER() OVER (PARTITION BY universe,ticker,date ORDER BY universe) = 1
    )
    SELECT * FROM ranked
    ORDER BY ticker, date
""").fetchdf()
print(f"  signal rows: {len(df):,}")

# ── build bar store for all signal tickers ────────────────────────────────────
tickers = sorted(df.ticker.unique())
ph = ",".join("?" * len(tickers))
bars = con.execute(f"""
    SELECT universe, ticker, date, open, high, low, close, volume
    FROM bars WHERE ticker IN ({ph})
    ORDER BY universe, ticker, date
""", tickers).fetchdf()
con.close()

bars = bars.drop_duplicates(["universe", "ticker", "date"]).reset_index(drop=True)
bars["dv"] = bars.close * bars.volume

store = {}
for (u, t), s in bars.groupby(["universe", "ticker"], sort=False):
    s = s.reset_index(drop=True)
    store[(u, t)] = dict(
        O=s.open.to_numpy(float), H=s.high.to_numpy(float),
        L=s.low.to_numpy(float), C=s.close.to_numpy(float),
        DV=s.dv.to_numpy(float),
        idx={d: k for k, d in enumerate(s.date.to_numpy())},
        dates=s.date.to_numpy(),
    )
print(f"  bar store: {len(store):,} series")


def run_trades(subset):
    """Path-sim all rows in subset. Returns list of trade dicts."""
    trades = []
    last = {}
    for x in subset.sort_values(["ticker", "date"]).itertuples():
        key = (x.universe, x.ticker)
        rec = store.get(key)
        if rec is None:
            continue
        i = rec["idx"].get(np.datetime64(x.date))
        if i is None:
            continue
        if i - last.get(key, -99) <= DEDUP:
            continue
        if is_glitch(rec, i, HZ):
            continue
        res = sim(rec, i, STOP, TARGET, HZ, trailing=None, entry_mode="next_open")
        if res is None or not res.get("dv") or res["dv"] < DVFLOOR:
            continue
        last[key] = i
        trades.append({**res, "year": pd.Timestamp(x.date).year, "uni": x.universe})
    return trades


def st(trades):
    if not trades:
        return None
    r = np.array([t["r"] for t in trades])
    return dict(n=len(r), exp=round(float(r.mean()), 2),
                med=round(float(np.median(r)), 2),
                win=round(float((r > 0).mean() * 100), 1),
                p50=round(float((r >= 50).mean() * 100), 1),
                ml=round(float(r.min()), 1))


def fmt(m):
    if m is None or m["n"] == 0:
        return "—"
    return f"n={m['n']} exp=**{m['exp']}** win={m['win']}% p50={m['p50']}%"


# ── RSI regime filters ────────────────────────────────────────────────────────
def rsi_regime(df_sig, sig_col):
    base = df_sig[df_sig[sig_col] > 0]
    lo = base[base.rsi_14 < 35]
    mid = base[(base.rsi_14 >= 35) & (base.rsi_14 < 60)]
    hi = base[base.rsi_14 >= 60]
    return base, lo, mid, hi


# ── Sequence sub-filters (within RSI<35) ─────────────────────────────────────
def tzt4_seq(df_t4_lo):
    """T[-2] + Z[-1] + T4[0] with RSI<35"""
    m = df_t4_lo.copy()
    has_t_2 = (m.t4_2 > 0) | (m.t3_2 > 0) | (m.t9_2 > 0) | (m.t10_2 > 0) | (m.t2_2 > 0)
    has_z_1 = m.z_1 > 0
    return m[has_t_2 & has_z_1]


def ttt6_seq(df_t6_lo):
    """T[-2] + T[-1] + T6[0] with RSI<35"""
    m = df_t6_lo.copy()
    has_t_2 = (m.t6_2 > 0) | (m.t4_2 > 0) | (m.t3_2 > 0) | (m.t2_2 > 0) | (m.t9_2 > 0) | (m.t10_2 > 0)
    has_t_1 = m.t_1 > 0
    return m[has_t_2 & has_t_1]


def t1seq_sub(df_t1_lo):
    """(Z or T)[-2] + (Z or T)[-1] + T1[0] with RSI<35"""
    m = df_t1_lo.copy()
    has_prev2 = (m.z_2 > 0) | (m.t_2 > 0)
    has_prev1 = (m.z_1 > 0) | (m.t_1 > 0)
    return m[has_prev2 & has_prev1]


# ── Run everything ────────────────────────────────────────────────────────────
md = [
    "# T1 / T4 / T6 — RSI Regime Backtest",
    "",
    f"_Entry=next_open · stop={STOP}% · target={TARGET}% · horizon={HZ}d · DV≥{DVFLOOR//1000}k · stop-first path-sim · all universes_",
    "",
]

REGIMES = ["ALL", "RSI<35", "RSI 35–60", "RSI≥60"]
SIGS = [("T1", "sig_t1"), ("T4", "sig_t4"), ("T6", "sig_t6")]

md.append("## 1. Per-signal × RSI regime (all universes combined)\n")
md.append("| Signal | Regime | n | EXPECT | med | win% | p50% | maxloss |")
md.append("|---|---|---|---|---|---|---|---|")

regime_data = {}  # store for per-year breakdown

for sig_name, sig_col in SIGS:
    base, lo, mid, hi = rsi_regime(df, sig_col)
    regime_data[sig_name] = {"base": base, "lo": lo, "mid": mid, "hi": hi}

    for label, subset in zip(REGIMES, [base, lo, mid, hi]):
        trades = run_trades(subset)
        m = st(trades)
        if m:
            md.append(f"| {sig_name} | {label} | {m['n']} | **{m['exp']}** | {m['med']} | {m['win']} | {m['p50']} | {m['ml']} |")
        else:
            md.append(f"| {sig_name} | {label} | 0 | — | | | | |")
    md.append("|  |  |  |  |  |  |  |  |")  # spacer

# ── Per-universe × RSI<35 ────────────────────────────────────────────────────
md.append("\n## 2. RSI<35 breakdown by universe\n")
md.append("| Signal | Universe | n | EXPECT | med | win% | p50% | maxloss |")
md.append("|---|---|---|---|---|---|---|---|")

for sig_name, sig_col in SIGS:
    lo = regime_data[sig_name]["lo"]
    for uni in UNIS:
        u_lo = lo[lo.universe == uni]
        trades = run_trades(u_lo)
        m = st(trades)
        if m:
            md.append(f"| {sig_name} | {uni} | {m['n']} | **{m['exp']}** | {m['med']} | {m['win']} | {m['p50']} | {m['ml']} |")
        else:
            md.append(f"| {sig_name} | {uni} | 0 | — | | | | |")
    md.append("|  |  |  |  |  |  |  |  |")

# ── Sequence patterns within RSI<35 ─────────────────────────────────────────
md.append("\n## 3. Sequence patterns within RSI<35\n")
md.append("| Pattern | n | EXPECT | med | win% | p50% | maxloss |")
md.append("|---|---|---|---|---|---|---|")

# T-Z-T4 in RSI<35
lo_t4 = regime_data["T4"]["lo"]
tzt4_lo = tzt4_seq(lo_t4)
t_tzt4 = run_trades(tzt4_lo)
m = st(t_tzt4)
if m:
    md.append(f"| T-Z-T4 (RSI<35) | {m['n']} | **{m['exp']}** | {m['med']} | {m['win']} | {m['p50']} | {m['ml']} |")
else:
    md.append("| T-Z-T4 (RSI<35) | 0 | — | | | | |")

# T-Z-T4 in RSI>=60 for comparison
base_t4 = regime_data["T4"]["base"]
hi_t4 = regime_data["T4"]["hi"]
tzt4_hi = tzt4_seq(hi_t4)
m_hi = st(run_trades(tzt4_hi))
if m_hi:
    md.append(f"| T-Z-T4 (RSI≥60) | {m_hi['n']} | **{m_hi['exp']}** | {m_hi['med']} | {m_hi['win']} | {m_hi['p50']} | {m_hi['ml']} |")

md.append("|  |  |  |  |  |  |  |")

# T-T-T6 in RSI<35
lo_t6 = regime_data["T6"]["lo"]
ttt6_lo = ttt6_seq(lo_t6)
t_ttt6 = run_trades(ttt6_lo)
m = st(t_ttt6)
if m:
    md.append(f"| T-T-T6 (RSI<35) | {m['n']} | **{m['exp']}** | {m['med']} | {m['win']} | {m['p50']} | {m['ml']} |")
else:
    md.append("| T-T-T6 (RSI<35) | 0 | — | | | | |")

lo_t6_hi = regime_data["T6"]["hi"]
ttt6_hi = ttt6_seq(lo_t6_hi)
m_hi = st(run_trades(ttt6_hi))
if m_hi:
    md.append(f"| T-T-T6 (RSI≥60) | {m_hi['n']} | **{m_hi['exp']}** | {m_hi['med']} | {m_hi['win']} | {m_hi['p50']} | {m_hi['ml']} |")

md.append("|  |  |  |  |  |  |  |")

# T1-seq in RSI<35
lo_t1 = regime_data["T1"]["lo"]
t1s_lo = t1seq_sub(lo_t1)
t_t1s = run_trades(t1s_lo)
m = st(t_t1s)
if m:
    md.append(f"| T1-seq (RSI<35) | {m['n']} | **{m['exp']}** | {m['med']} | {m['win']} | {m['p50']} | {m['ml']} |")
else:
    md.append("| T1-seq (RSI<35) | 0 | — | | | | |")

hi_t1 = regime_data["T1"]["hi"]
t1s_hi = t1seq_sub(hi_t1)
m_hi = st(run_trades(t1s_hi))
if m_hi:
    md.append(f"| T1-seq (RSI≥60) | {m_hi['n']} | **{m_hi['exp']}** | {m_hi['med']} | {m_hi['win']} | {m_hi['p50']} | {m_hi['ml']} |")

# ── Per-year stability (RSI<35, all signals combined) ────────────────────────
md.append("\n## 4. Per-year stability — RSI<35 (T1+T4+T6 combined)\n")
md.append("| Year | n | EXPECT | win% |")
md.append("|---|---|---|---|")

all_lo_trades = []
for sig_name, sig_col in SIGS:
    all_lo_trades += run_trades(regime_data[sig_name]["lo"])

for yr in range(2021, 2027):
    yr_trades = [t for t in all_lo_trades if t["year"] == yr]
    m = st(yr_trades)
    if m:
        md.append(f"| {yr} | {m['n']} | **{m['exp']}** | {m['win']} |")

# ── T1-seq RSI<35 per-tier ───────────────────────────────────────────────────
md.append("\n## 5. T1-seq RSI<35 — context tier breakdown\n")
md.append("| Tier | Context | n | EXPECT | win% | p50% |")
md.append("|---|---|---|---|---|---|")

for tier_label, z2, t2, z1, t1_flag in [
    ("T1 (ZZ)", True, False, True, False),   # Z[-2] + Z[-1]
    ("T2 (TZ)", False, True, True, False),   # T[-2] + Z[-1]
    ("T3 (ZT)", True, False, False, True),   # Z[-2] + T[-1]
    ("T4 (TT)", False, True, False, True),   # T[-2] + T[-1]
]:
    lo_t1 = regime_data["T1"]["lo"]
    m_rows = lo_t1.copy()
    if z2:
        m_rows = m_rows[m_rows.z_2 > 0]
    elif t2:
        m_rows = m_rows[(m_rows.t_2 > 0) & (m_rows.z_2 == 0)]
    if z1:
        m_rows = m_rows[m_rows.z_1 > 0]
    elif t1_flag:
        m_rows = m_rows[(m_rows.t_1 > 0) & (m_rows.z_1 == 0)]

    trades = run_trades(m_rows)
    m = st(trades)
    if m:
        md.append(f"| {tier_label} | RSI<35 | {m['n']} | **{m['exp']}** | {m['win']} | {m['p50']} |")
    else:
        md.append(f"| {tier_label} | RSI<35 | 0 | — | | |")

# RSI≥60 tier breakdown for comparison
md.append("|  |  |  |  |  |  |")
for tier_label, z2, t2, z1, t1_flag in [
    ("T1 (ZZ)", True, False, True, False),
    ("T2 (TZ)", False, True, True, False),
    ("T3 (ZT)", True, False, False, True),
    ("T4 (TT)", False, True, False, True),
]:
    hi_t1 = regime_data["T1"]["hi"]
    m_rows = hi_t1.copy()
    if z2:
        m_rows = m_rows[m_rows.z_2 > 0]
    elif t2:
        m_rows = m_rows[(m_rows.t_2 > 0) & (m_rows.z_2 == 0)]
    if z1:
        m_rows = m_rows[m_rows.z_1 > 0]
    elif t1_flag:
        m_rows = m_rows[(m_rows.t_1 > 0) & (m_rows.z_1 == 0)]

    trades = run_trades(m_rows)
    m = st(trades)
    if m:
        md.append(f"| {tier_label} | RSI≥60 | {m['n']} | **{m['exp']}** | {m['win']} | {m['p50']} |")
    else:
        md.append(f"| {tier_label} | RSI≥60 | 0 | — | | |")

# ── write out ────────────────────────────────────────────────────────────────
out_text = "\n".join(md) + "\n"
with open(OUT, "w") as f:
    f.write(out_text)
print(f"\nWrote {OUT}")
print()
for line in md:
    print(line)
