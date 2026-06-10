"""confluence_count.py — does STACKING weak tilts produce a tradeable edge?
5 semi-independent bullish axes; forward excess (universe-drift removed) by how many
fire. Monotonic? Does any count beat the negative drift enough to trade? per-year+IS/OOS.
ANALYSIS ONLY."""
import sys, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"; a=get_analytics_conn()
# 5 axes from distinct families (each real-time, no pivot lookahead)
AX = {
 "A1_absorb":  "(load=1 OR sig_fri34=1 OR sig_abs=1)",
 "A2_oversold":"(rsi2_state='R2L' OR rsi_le_35=1)",
 "A3_flow":    "(d_flip_bull=1 OR d_surge_bull=1)",
 "A4_thrust":  "(eb_bull=1 OR bo_up=1 OR vbo_up=1)",
 "A5_tz":      "(tz_bull=1)",
}
cnt_expr = " + ".join(f"({e})::int" for e in AX.values())
q=f"""
WITH u AS (SELECT universe, median(fwd_10d) m FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe),
x AS (
  SELECT b.fwd_10d - u.m AS exc, ({cnt_expr}) AS cnt,
         year(b.date) AS yr, (b.date::VARCHAR >= '{OOS}') AS oos
  FROM bars b JOIN u USING(universe)
  WHERE b.fwd_10d IS NOT NULL AND b.fwd_10d BETWEEN -90 AND 500)
SELECT cnt, count(*) n,
       round(100.0*avg((exc>0)::int),1) win,
       round(median(exc),3) medL,
       round(avg(LEAST(GREATEST(exc,-25),25)),3) m25L,
       round(median(exc) FILTER(WHERE NOT oos),3) is_med,
       round(median(exc) FILTER(WHERE oos),3) oos_med
FROM x GROUP BY cnt ORDER BY cnt
"""
df=a.execute(q).fetchdf()
tot=df.n.sum()
print("CONFLUENCE COUNT → forward excess (universe-drift removed). axes: absorb/oversold/flow/thrust/tz")
print(f"{'cnt':>3} {'n':>9} {'%pop':>6} {'win%':>6} {'medL':>7} {'m25L':>7} {'IS':>7} {'OOS':>7}")
for _,r in df.iterrows():
    print(f"{int(r.cnt):>3} {int(r.n):>9} {100*r.n/tot:>5.1f}% {r.win:>6} {r.medL:>+7} {r.m25L:>+7} {r.is_med:>+7} {r.oos_med:>+7}")
# per-year for each count
py=a.execute(f"""
WITH u AS (SELECT universe, median(fwd_10d) m FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe),
x AS (SELECT b.fwd_10d-u.m exc, ({cnt_expr}) cnt, year(b.date) yr
      FROM bars b JOIN u USING(universe)
      WHERE b.fwd_10d IS NOT NULL AND b.fwd_10d BETWEEN -90 AND 500)
SELECT cnt, yr, round(median(exc),2) m, count(*) n FROM x GROUP BY cnt,yr ORDER BY cnt,yr""").fetchdf()
a.close()
print("\nper-year median-excess by count:")
for c in sorted(py.cnt.unique()):
    row=py[py.cnt==c]
    cells=" ".join(f"{int(y)%100}:{float(row[row.yr==y].m.iloc[0]) if len(row[row.yr==y]) and int(row[row.yr==y].n.iloc[0])>=30 else '–'}" for y in range(2021,2027))
    print(f"  cnt{int(c)}: {cells}")
print("\ndone")
