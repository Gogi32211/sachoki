"""Intensity is a strict subset of breadth<50 (24/24). So does it add anything OVER
breadth, or is breadth alone the better instrument (many more days, same information)?
Decisive test: bucket by breadth directly, then ask whether intensity separates WITHIN
a breadth bucket. If it doesn't, it's breadth with a smaller sample = strictly worse."""
import sys
import numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
from ai_journal.atomic_journal import replay
import journal_bench as JB
import edge_replay as ER

cf = pd.DataFrame(replay(months=72, min_score=70, conf_only=True, limit=1_000_000)["trades"])
cf["signal_date"] = cf["signal_date"].astype(str).str[:10]
day_n = cf["signal_date"].value_counts()

grp, as_of = ER._frame(72, 3_000_000)
rows = []
for tk, g in grp.items():
    d = g["date"].astype(str).str[:10]
    r20 = g["close"] / g["close"].shift(20) - 1.0
    rows.append(pd.DataFrame({"date": d, "up": (r20 > 0).to_numpy(), "ok": r20.notna().to_numpy()}))
B = pd.concat(rows, ignore_index=True); B = B[B.ok]
breadth = (B.groupby("date")["up"].mean() * 100).rename("breadth")

a = get_analytics_conn()
bars = a.execute(f"""
    WITH r AS (SELECT ticker, date, open, high, low, close, close*volume dv,
        row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
      FROM bars WHERE close > 0 AND date >= DATE '{as_of}' - INTERVAL 2270 DAY)
    SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker, date""").fetchdf()
a.close()
bars["date"] = bars["date"].astype(str).str[:10]
O = pd.concat([pd.DataFrame({"date": g.reset_index(drop=True)["date"],
                             "dv": g.reset_index(drop=True)["dv"],
                             "ret": JB._outcomes(g.reset_index(drop=True), 0.15, 1.00)})
               for _, g in bars.groupby("ticker", sort=False)], ignore_index=True)
O = O[O["ret"].notna() & (O["dv"] >= 3_000_000)]
O = O.join(breadth, on="date")
O["intens"] = O["date"].map(day_n).fillna(0).astype(int)
O = O.dropna(subset=["breadth"])

print("=" * 88)
print("BREADTH alone — the axis we already have")
print("=" * 88)
print(f"{'breadth':12} {'days':>5} {'n':>9} | {'win':>6} {'mean':>8}")
for lo, hi in [(0,15),(15,25),(25,35),(35,50),(50,65),(65,101)]:
    s = O[(O.breadth >= lo) & (O.breadth < hi)]
    if len(s) < 500: continue
    print(f"  <{hi:3} ≥{lo:<3}   {s.date.nunique():5} {len(s):9} | {(s.ret>0).mean()*100:5.1f}% {s.ret.mean():+7.2f}%")

print("\n" + "=" * 88)
print("WITHIN a breadth bucket — does intensity still separate? (if not → pure rediscovery)")
print("=" * 88)
print(f"{'breadth':12} | {'intensity <10':>28} | {'intensity ≥10':>28} | {'Δ':>8}")
for lo, hi in [(0,25),(25,35),(35,50)]:
    s = O[(O.breadth >= lo) & (O.breadth < hi)]
    a_, b_ = s[s.intens < 10], s[s.intens >= 10]
    if len(a_) < 300 or len(b_) < 300:
        print(f"  {lo}-{hi}: n={len(a_)}/{len(b_)} too few"); continue
    print(f"  {lo:2}-{hi:<3}      | n={len(a_):7} d={a_.date.nunique():3} mean {a_.ret.mean():+6.2f}% "
          f"| n={len(b_):7} d={b_.date.nunique():3} mean {b_.ret.mean():+6.2f}% | {b_.ret.mean()-a_.ret.mean():+6.2f}pp")

print("\n" + "=" * 88)
print("HEAD TO HEAD — deep-breadth days vs high-intensity days (which instrument is better?)")
print("=" * 88)
for lab, m in (("breadth <15%", O[O.breadth < 15]),
               ("breadth <25%", O[O.breadth < 25]),
               ("intensity ≥30", O[O.intens >= 30]),
               ("intensity ≥100", O[O.intens >= 100])):
    if len(m) < 200: continue
    yrs = m.assign(yr=m.date.str[:4]).groupby("yr")["ret"].mean()
    print(f"  {lab:16} days={m.date.nunique():3}  n={len(m):7}  win {(m.ret>0).mean()*100:5.1f}%  "
          f"mean {m.ret.mean():+6.2f}%  yrs+{int((yrs>0).sum())}/{len(yrs)}")
