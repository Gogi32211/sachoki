"""Is capitulation-cluster INTENSITY a real buy-day timer?

The 6yr test's confluence baseline was +1.97% but the unweighted per-day baseline is only
+0.56% — the gap is because that baseline weighted days by trade count, so it leaned on the
few days where MANY confluences fired at once. That says intensity, not the confluence
itself, may be the signal. Test it directly: bucket days by how many fired, and ask what
the WHOLE universe did (= is it a market-bottom timer?) and what OUR stack did on top.
Exit held constant at the journal rule. Everything causal: intensity is same-day, known
at the close before the next-open entry.
"""
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
print("intensity percentiles:", {p: int(np.percentile(day_n, p)) for p in (50, 75, 90, 95, 99)}, flush=True)

grp, as_of = ER._frame(72, 3_000_000)
sel = []
for tk, g in grp.items():
    sel.append(pd.DataFrame({"ticker": tk, "date": g["date"].astype(str).str[:10],
                             "pick": (g["close"].between(21, 89)
                                      & g["rs_intact"].fillna(False).astype(bool)
                                      & (g["conf_n"] >= 3)).to_numpy()}))
S = pd.concat(sel, ignore_index=True)

a = get_analytics_conn()
bars = a.execute(f"""
    WITH r AS (SELECT ticker, date, open, high, low, close, close*volume dv,
        row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
      FROM bars WHERE close > 0 AND date >= DATE '{as_of}' - INTERVAL 2270 DAY)
    SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker, date""").fetchdf()
a.close()
bars["date"] = bars["date"].astype(str).str[:10]
parts = [pd.DataFrame({"ticker": tk, "date": g.reset_index(drop=True)["date"],
                       "dv": g.reset_index(drop=True)["dv"],
                       "ret": JB._outcomes(g.reset_index(drop=True), 0.15, 1.00)})
         for tk, g in bars.groupby("ticker", sort=False)]
O = pd.concat(parts, ignore_index=True)
O = O[O["ret"].notna() & (O["dv"] >= 3_000_000)]
M = O.merge(S, on=["ticker", "date"], how="left")
M["pick"] = M["pick"].fillna(False).astype(bool)
M["intens"] = M["date"].map(day_n).fillna(0).astype(int)
M["yr"] = M["date"].str[:4]

BUCKETS = [(0, 1, "no fire"), (1, 3, "1-2"), (3, 10, "3-9"), (10, 30, "10-29"),
           (30, 100, "30-99"), (100, 10**9, "100+")]
print("\n" + "=" * 96)
print("BUY-DAY TIMER? — what the WHOLE liquid universe did, by that day's capit-cluster intensity")
print("=" * 96)
print(f"{'fires that day':16} {'days':>6} {'n':>9} | {'win':>6} {'mean':>8} | {'our stack n':>11} {'win':>6} {'mean':>8}")
for lo, hi, lab in BUCKETS:
    m = M[(M.intens >= lo) & (M.intens < hi)]
    if len(m) < 500: continue
    p = m[m.pick]
    nd = m["date"].nunique()
    ps = (f"{len(p):11} {(p.ret>0).mean()*100:5.1f}% {p.ret.mean():+7.2f}%" if len(p) > 50
          else f"{len(p):11} {'—':>6} {'—':>8}")
    print(f"{lab:16} {nd:6} {len(m):9} | {(m.ret>0).mean()*100:5.1f}% {m.ret.mean():+7.2f}% | {ps}")

print("\n" + "=" * 96)
print("PER YEAR — universe mean on high-intensity days (30+) vs quiet days (<3)")
print("=" * 96)
for y in sorted(M.yr.unique()):
    my = M[M.yr == y]
    hi_, lo_ = my[my.intens >= 30], my[my.intens < 3]
    if len(hi_) < 200 or len(lo_) < 200:
        print(f"{y}: n={len(hi_)}/{len(lo_)} too few"); continue
    print(f"{y} | 30+ fires: n={len(hi_):7} days={hi_.date.nunique():3} mean {hi_.ret.mean():+6.2f}% "
          f"| quiet: n={len(lo_):7} mean {lo_.ret.mean():+6.2f}% | Δ {hi_.ret.mean()-lo_.ret.mean():+6.2f}pp")
