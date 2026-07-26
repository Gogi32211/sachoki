"""Does atomic_score RANK? The journal buys top-15 by score>=70 — if the score is real,
the mean lift over a random basket on the same dates must RISE with it."""
import sys; sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
import pandas as pd
from ai_journal.atomic_journal import replay
import journal_bench as JB

d = replay(months=72, min_score=0, limit=1_000_000)
tr = pd.DataFrame(d["trades"])
tr["signal_date"] = tr["signal_date"].astype(str).str[:10]
tr["yr"] = tr["signal_date"].str[:4]
print(f"full atomic population 6yr: n={len(tr)}  score range {tr.score.min()}-{tr.score.max()}")
print("score distribution:", tr.score.value_counts().sort_index().to_dict())

print(f"\n{'score':>10} {'n':>6} | {'win':>6} {'rand':>6} {'lift':>7} | {'mean':>7} {'rand':>7} {'LIFT':>7} | {'yrs lift>0':>10}")
print("-" * 84)
bins = [(0,60),(60,70),(70,80),(80,90),(90,101)]
for lo, hi in bins:
    sub = tr[(tr.score >= lo) & (tr.score < hi)]
    if len(sub) < 50: continue
    b = JB.baseline(list(sub.signal_date))
    if not b: continue
    jw, jm = (sub.pnl > 0).mean()*100, sub.pnl.mean()
    ny = 0
    for y in sorted(sub.yr.unique()):
        s2 = sub[sub.yr == y]
        if len(s2) < 20: continue
        b2 = JB.baseline(list(s2.signal_date))
        if b2 and s2.pnl.mean() - b2["mean"] > 0: ny += 1
    print(f"{lo:4}-{hi-1:<5}{len(sub):6} | {jw:5.1f}% {b['win']:5.1f}% {jw-b['win']:+6.1f}pp | "
          f"{jm:+6.2f}% {b['mean']:+6.2f}% {jm-b['mean']:+6.2f}pp | {ny}/6")
