"""Same random-basket control, but for the Capit→Atom replay's 627 trades over 12 months.
Random tickers on the SAME signal dates, SAME exit rule (next-open, -15% stop, +100%
target, 20-bar, stop-first, gap-aware). If random matches it, the tab measures the market."""
import sys, json, urllib.request, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn

HOR, STOP, TGT = 20, 0.85, 2.00
d = json.load(urllib.request.urlopen(
    "http://127.0.0.1:8080/api/capit-atom-journal/replay?months=12&min_score=70"))
tr = pd.DataFrame(d["trades"])
print("replay cols:", [c for c in tr.columns][:14])
dcol = "signal_date" if "signal_date" in tr else tr.columns[1]
tr[dcol] = tr[dcol].astype(str).str[:10]
closed = tr[tr["pnl"].notna()] if "pnl" in tr else tr
print(f"trades n={len(tr)}  with pnl={len(closed)}")
p = pd.to_numeric(closed["pnl"], errors="coerce").dropna()
print(f"REPLAY: n={len(p)} win={(p>0).mean()*100:.1f}% mean={p.mean():+.2f}% med={p.median():+.2f}%")
dates = closed[dcol].value_counts().sort_index()
print(f"spread over {len(dates)} distinct signal dates, {dates.max()} max on one day")

a = get_analytics_conn()
lo = min(dates.index)
bars = a.execute(f"""
    WITH r AS (SELECT ticker, date, open, high, low, close, close*volume dv,
        row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
      FROM bars WHERE date >= DATE '{lo}' - INTERVAL 5 DAY AND close > 0)
    SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker, date""").fetchdf()
a.close()
bars["date"] = bars["date"].astype(str).str[:10]
G = {tk: g.reset_index(drop=True) for tk, g in bars.groupby("ticker", sort=False)}

def sim(tk, sd):
    g = G.get(tk)
    if g is None: return None
    fut = g[g["date"] > sd]
    if len(fut) < 2: return None
    e = float(fut.iloc[0]["open"])
    if e <= 0: return None
    st, tg = e*STOP, e*TGT
    w = fut.iloc[1:HOR+1]
    if len(w) < HOR: return None
    for _, b in w.iterrows():
        if b["low"] <= st: return (min(st, b["open"])/e - 1)*100
        if b["high"] >= tg: return (max(tg, b["open"])/e - 1)*100
    return (float(w.iloc[-1]["close"])/e - 1)*100

liq = bars[bars.dv >= 3_000_000].groupby("date")["ticker"].apply(list).to_dict()
rng = np.random.default_rng(11)
wins, means = [], []
for _ in range(200):
    rets = []
    for dt, k in dates.items():
        pool = liq.get(dt, [])
        if not pool: continue
        for tk in rng.choice(pool, min(k, len(pool)), replace=False):
            r = sim(tk, dt)
            if r is not None: rets.append(r)
    if len(rets) < 50: continue
    rets = np.array(rets); wins.append((rets>0).mean()*100); means.append(rets.mean())
wins, means = np.array(wins), np.array(means)
jw, jm = (p>0).mean()*100, p.mean()
print(f"\nRANDOM basket, same {len(dates)} dates & exit rule ({len(wins)} trials):")
print(f"  win  {wins.mean():.1f}% ± {wins.std():.1f}   → replay {jw:.1f}% = {(jw-wins.mean())/wins.std():+.2f}σ")
print(f"  mean {means.mean():+.2f}% ± {means.std():.2f}  → replay {jm:+.2f}% = {(jm-means.mean())/means.std():+.2f}σ")
print(f"  replay beats {(means<jm).mean()*100:.0f}% of random baskets on mean, {(wins<jw).mean()*100:.0f}% on win%")
