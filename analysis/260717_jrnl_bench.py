"""Is the Atomic Jrnl's 73% win / +5.49% mean an EDGE, or just that week's market?

The 52 closed trades all have signal dates in ONE 5-day span (2026-06-09..06-16), so they
are ~1 market event with 52 correlated legs, not 52 independent samples. The only honest
read is a benchmark: replay the journal's EXACT exit rule (next-open entry, -15% stop,
+100% target, 20-bar horizon, stop-first, gap-aware) on RANDOM tickers with the SAME
entry dates and the same liquidity screen. If random wins ~73% too, the journal measured
June, not the edge.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_journal_conn, get_analytics_conn

HOR, STOP, TGT = 20, 0.85, 2.00

j = get_journal_conn()
pos = j.execute("""SELECT ticker, universe, signal_date, entry_px, pnl_pct, exit_reason
                   FROM atomic_position WHERE status='CLOSED'""").fetchdf()
j.close()
pos["signal_date"] = pos["signal_date"].astype(str).str[:10]
dates = pos["signal_date"].value_counts().sort_index()
print("journal trades per signal date:", dates.to_dict())
print(f"journal: n={len(pos)} win={(pos.pnl_pct>0).mean()*100:.1f}% mean={pos.pnl_pct.mean():+.2f}% "
      f"med={pos.pnl_pct.median():+.2f}%")

a = get_analytics_conn()
lo, hi = min(dates.index), "2026-07-17"
bars = a.execute(f"""
    WITH r AS (
      SELECT ticker, universe, date, open, high, low, close, close*volume dv,
             row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
      FROM bars WHERE date >= DATE '{lo}' - INTERVAL 5 DAY AND date <= DATE '{hi}' AND close > 0
    ) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker, date
""").fetchdf()
a.close()
bars["date"] = bars["date"].astype(str).str[:10]
print("bars pulled:", len(bars), "tickers:", bars.ticker.nunique())

G = {tk: g.reset_index(drop=True) for tk, g in bars.groupby("ticker", sort=False)}


def sim(tk, sig_date):
    """The journal's exact rule: enter at the next open after sig_date."""
    g = G.get(tk)
    if g is None:
        return None
    fut = g[g["date"] > sig_date]
    if len(fut) < 2:
        return None
    entry = float(fut.iloc[0]["open"])
    if entry <= 0:
        return None
    stop, tgt = entry * STOP, entry * TGT
    walk = fut.iloc[1:HOR + 1]
    if len(walk) < HOR:
        return None                      # not enough bars → journal would leave it OPEN
    for _, b in walk.iterrows():
        if b["low"] <= stop:             # stop-first (conservative), gap-aware
            return (min(stop, b["open"]) / entry - 1) * 100
        if b["high"] >= tgt:
            return (max(tgt, b["open"]) / entry - 1) * 100
    return (float(walk.iloc[-1]["close"]) / entry - 1) * 100


# ---- 1. reproduce the journal itself (sanity)
rep = [sim(r.ticker, r.signal_date) for r in pos.itertuples()]
rep = np.array([x for x in rep if x is not None])
print(f"\nreproduced journal: n={len(rep)} win={(rep>0).mean()*100:.1f}% mean={rep.mean():+.2f}%")

# ---- 2. RANDOM control: same dates, same trade count per date, same liquidity screen
liq = bars[bars.dv >= 3_000_000].groupby("date")["ticker"].apply(list).to_dict()
rng = np.random.default_rng(7)
wins, means, meds = [], [], []
for trial in range(300):
    rets = []
    for dt, k in dates.items():
        pool = liq.get(dt, [])
        if not pool:
            continue
        for tk in rng.choice(pool, min(k, len(pool)), replace=False):
            r = sim(tk, dt)
            if r is not None:
                rets.append(r)
    if len(rets) < 20:
        continue
    rets = np.array(rets)
    wins.append((rets > 0).mean() * 100); means.append(rets.mean()); meds.append(np.median(rets))
wins, means, meds = np.array(wins), np.array(means), np.array(meds)
print(f"\nRANDOM basket, SAME dates & exit rule ({len(wins)} trials of ~{len(pos)} trades):")
print(f"  win  {wins.mean():.1f}% ± {wins.std():.1f}   (journal 73.1% → {(pos.pnl_pct>0).mean()*100 - wins.mean():+.1f}pp, "
      f"{((pos.pnl_pct>0).mean()*100 - wins.mean())/wins.std():+.2f}σ)")
print(f"  mean {means.mean():+.2f}% ± {means.std():.2f}  (journal +5.49% → {5.49 - means.mean():+.2f}pp, "
      f"{(5.49 - means.mean())/means.std():+.2f}σ)")
print(f"  med  {meds.mean():+.2f}% ± {meds.std():.2f}  (journal +3.17%)")
print(f"  journal beats {(means < 5.49).mean()*100:.1f}% of random baskets on mean, "
      f"{(wins < 73.1).mean()*100:.1f}% on win%")

# ---- 3. what did the market itself do over that exact window?
spy = None
for cand in ("SPY", "IVV", "VOO"):
    if cand in G:
        spy = cand; break
if spy:
    s = [sim(spy, dt) for dt in dates.index]
    s = [x for x in s if x is not None]
    print(f"\n{spy} over the same entries: {np.mean(s):+.2f}% avg (per entry date: "
          + " ".join(f"{d}:{v:+.1f}" for d, v in zip(dates.index, s)) + ")")
