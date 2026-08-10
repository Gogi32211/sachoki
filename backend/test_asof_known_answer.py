"""Both semantics, both checked against the same real restatement."""
import sys; sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
import duckdb, pandas as pd, numpy as np
import fundamentals as F

T, PE = "DJCO", "2023-09-30"
c = duckdb.connect("/Users/sachoki/Desktop/sachoki-desktop/data/fundamentals.duckdb", read_only=True)
rows = c.execute(f"""SELECT cast(filed as DATE) f, val FROM facts WHERE ticker='{T}'
    AND concept='cash' AND period_end=DATE '{PE}' ORDER BY filed""").fetchall()
hist, seen = [], set()
for f, v in rows:
    if str(f) not in seen: seen.add(str(f)); hist.append((str(f)[:10], v))

print(f"A · VINTAGE — what was believed about the period ending {PE}\n")
probe = [(pd.Timestamp(hist[0][0]) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")]
for i,(f,_) in enumerate(hist):
    probe.append(f)
    if i+1 < len(hist):
        probe.append((pd.Timestamp(f) + (pd.Timestamp(hist[i+1][0])-pd.Timestamp(f))/2).strftime("%Y-%m-%d"))
got = F.as_of_period([T]*len(probe), "cash", [PE]*len(probe), probe)
okA = True
for d, g in zip(probe, got):
    exp = None
    for f, v in hist:
        if f <= d: exp = v
    m = (np.isnan(g) and exp is None) or (exp is not None and abs(g-exp) < 1)
    okA &= m
    print(f"    {d}  →  {'NaN' if np.isnan(g) else f'${g/1e6:>9,.2f}M':>12s}   "
          f"expected {'NaN' if exp is None else f'${exp/1e6:,.2f}M':>12s}  {'✅' if m else '🔴'}")

print(f"\nB · FRONTIER — the company's latest reported cash, as known on each date\n")
q = c.execute(f"""SELECT cast(filed as DATE) f, cast(period_end as DATE) pe, val FROM facts
    WHERE ticker='{T}' AND concept='cash' ORDER BY filed, period_end""").fetchall()
best_pe, exp_map = None, {}
for f, pe, v in q:
    if best_pe is None or pe >= best_pe:
        best_pe = pe; exp_map[str(f)[:10]] = v
dates = sorted(exp_map)[-8:]
probeB = []
for d in dates:
    probeB += [d, (pd.Timestamp(d)+pd.Timedelta(days=20)).strftime("%Y-%m-%d")]
gotB = F.as_of([T]*len(probeB), "cash", probeB)
okB = True
for d, g in zip(probeB, gotB):
    exp = None
    for f in sorted(exp_map):
        if f <= d: exp = exp_map[f]
    m = (exp is not None and np.isfinite(g) and abs(g-exp) < 1)
    okB &= m
    print(f"    {d}  →  ${g/1e6:>9,.2f}M   expected ${exp/1e6:>9,.2f}M  {'✅' if m else '🔴'}")
print(f"\n  A vintage  {'✅ PASS' if okA else '🔴 FAIL'}")
print(f"  B frontier {'✅ PASS' if okB else '🔴 FAIL'}")
c.close()
