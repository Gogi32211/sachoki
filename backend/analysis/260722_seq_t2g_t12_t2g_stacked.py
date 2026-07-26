"""Stacked-filter test on the user's T2G/T12(RSI35-55)/T2G sequence (2026-07-22):
RSI<60 + CCI<100 + px $21-89 + wyc!=MKDN, all at once, vs baseline. Same path-sim
(trail25/-15%/60bar/15bps, next-open) as the booster sweep. Reuses the cached
1,102-match JSON + recomputes path-sim (cheap, ~2s)."""
import json, time
import numpy as np, pandas as pd, duckdb

t0 = time.time(); S_ = 0.0015
MATCHES_JSON = "/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/5b6f6b5f-eb52-4041-9fed-b0cbcf6a28fc/scratchpad/t2g_t12_t2g.json"
with open(MATCHES_JSON) as f:
    matched = json.load(f)["rows"]
tickers = sorted({r["ticker"] for r in matched})
anchor_by_tk = {}
for r in matched:
    anchor_by_tk.setdefault(r["ticker"], set()).add(r["date"])

conn = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
placeholders = ",".join(f"'{t}'" for t in tickers)
df = conn.execute(f"""
    WITH r AS (SELECT ticker, date, open, high, low, close, rsi_14, cci_20,
        coalesce(wyc_phase,'') wp,
        row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
        FROM bars WHERE ticker IN ({placeholders}))
    SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date
""").fetchdf()
conn.close()
print(f"frame {len(df):,} rows ({time.time()-t0:.0f}s)", flush=True)

o = df.open.to_numpy(float); h = df.high.to_numpy(float); lo_ = df.low.to_numpy(float); c = df.close.to_numpy(float)
tk = df.ticker.to_numpy(); dt = df.date.astype(str).to_numpy(); n = len(df)
is_anchor = np.zeros(n, bool)
for i in range(n):
    s = anchor_by_tk.get(tk[i])
    if s and dt[i] in s:
        is_anchor[i] = True

ps = np.full(n, np.nan)
idx_by_tk = {}
for i, t in enumerate(tk):
    idx_by_tk.setdefault(t, []).append(i)
for t, idxs in idx_by_tk.items():
    lo_i, hi_i = idxs[0], idxs[-1]
    for b in range(lo_i, hi_i):
        if not is_anchor[b]:
            continue
        e = o[b + 1] * (1 + S_)
        if e <= 0:
            continue
        pk = e; hd = e * 0.85; end = min(b + 61, hi_i + 1); r = None
        for q in range(b + 1, end):
            if q > b + 1 and o[q] <= hd: r = o[q] / e - 1 - S_; break
            if lo_[q] <= hd: r = -0.15 - S_; break
            pk = max(pk, h[q]); ts = pk * 0.75
            if q > b + 1 and o[q] <= ts: r = o[q] / e - 1 - S_; break
            if lo_[q] <= ts: r = ts / e - 1 - S_; break
        ps[b] = r if r is not None else c[end - 1] / e - 1 - S_

A = df[is_anchor].copy()
A["ps"] = ps[is_anchor]
A = A[A["ps"].notna()].reset_index(drop=True)
A["yr"] = A.date.astype(str).str[:4]

def block(mask, label):
    nn = int(mask.sum())
    if nn == 0:
        print(f"  {label}: n=0"); return
    pm = A.loc[mask, "ps"].mean() * 100
    pmed = A.loc[mask, "ps"].median() * 100
    win = (A.loc[mask, "ps"] > 0).mean() * 100
    print(f"  {label:28} n={nn:4}  ps {pm:+6.2f}%  med {pmed:+6.2f}%  win {win:5.1f}%")

base_mask = np.ones(len(A), bool)
print(f"\n== BASELINE (n={len(A)}) ==")
block(base_mask, "all 1,101")

combo = (A.rsi_14 < 60) & (A.cci_20 < 100) & (A.close >= 21) & (A.close < 89) & (A.wp != "MKDN")
print(f"\n== STACKED: RSI<60 + CCI<100 + px$21-89 + wyc!=MKDN ==")
block(combo, "stacked combo")
print(f"\n  coverage: {combo.sum()}/{len(A)} = {100*combo.mean():.1f}% of baseline matches kept")

print(f"\n== per-year (stacked) ==")
for y in ["2021", "2022", "2023", "2024", "2025", "2026"]:
    block(combo & (A.yr == y), y)

print(f"\n== TRAIN(21-23) vs TEST(24-26), stacked ==")
block(combo & A.yr.isin(["2021", "2022", "2023"]), "TRAIN")
block(combo & A.yr.isin(["2024", "2025", "2026"]), "TEST")

# partial combos for context — which single exclusion matters most
print(f"\n== partial combos (drop one filter at a time) ==")
block((A.cci_20 < 100) & (A.close >= 21) & (A.close < 89) & (A.wp != "MKDN"), "no RSI filter")
block((A.rsi_14 < 60) & (A.close >= 21) & (A.close < 89) & (A.wp != "MKDN"), "no CCI filter")
block((A.rsi_14 < 60) & (A.cci_20 < 100) & (A.wp != "MKDN"), "no price filter")
block((A.rsi_14 < 60) & (A.cci_20 < 100) & (A.close >= 21) & (A.close < 89), "no wyc filter")

print(f"\ndone ({time.time()-t0:.0f}s)")
