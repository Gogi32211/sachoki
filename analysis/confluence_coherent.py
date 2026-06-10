"""confluence_coherent.py — naive count fails because axes conflict (buy-weakness vs
chase-strength). Test COHERENT stacks: does absorption/flow ADD to the oversold base?
And does the chase stack lose? universe-drift removed, per-year. ANALYSIS ONLY."""
import sys, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"; a=get_analytics_conn()
A1="(load=1 OR sig_fri34=1 OR sig_abs=1)"        # absorption
A2="(rsi2_state='R2L' OR rsi_le_35=1)"           # oversold (buy weakness)
A3="(d_flip_bull=1 OR d_surge_bull=1)"           # order-flow up
A4="(eb_bull=1 OR bo_up=1 OR vbo_up=1)"          # thrust (chase strength)
A5="(tz_bull=1)"
TESTS=[
 ("oversold alone",                 f"{A2}"),
 ("oversold + absorption",          f"{A2} AND {A1}"),
 ("oversold + flow",                f"{A2} AND {A3}"),
 ("oversold + absorb + flow [coherent, no thrust]", f"{A2} AND {A1} AND {A3}"),
 ("oversold + absorb + flow + NOT thrust", f"{A2} AND {A1} AND {A3} AND NOT {A4}"),
 ("oversold THEN thrust (reversal up)", f"{A2} AND {A4}"),
 ("-- chase --",                    None),
 ("thrust alone",                   f"{A4}"),
 ("thrust + tz",                    f"{A4} AND {A5}"),
 ("thrust + flow + tz [chase stack]", f"{A4} AND {A3} AND {A5}"),
 ("thrust + NOT oversold (chase, not reversal)", f"{A4} AND NOT {A2}"),
]
def stats(where):
    q=f"""
    WITH u AS (SELECT universe, median(fwd_10d) m FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe),
    x AS (SELECT b.fwd_10d-u.m exc, year(b.date) yr, (b.date::VARCHAR>='{OOS}') oos
          FROM bars b JOIN u USING(universe)
          WHERE b.fwd_10d IS NOT NULL AND b.fwd_10d BETWEEN -90 AND 500 AND {where})
    SELECT count(*) n, round(median(exc),3) medL, round(avg(LEAST(GREATEST(exc,-25),25)),3) m25L,
           round(median(exc) FILTER(WHERE NOT oos),3) is_m, round(median(exc) FILTER(WHERE oos),3) oo_m
    FROM x"""
    r=a.execute(q).fetchone()
    py=a.execute(f"""WITH u AS (SELECT universe,median(fwd_10d) m FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe),
       x AS (SELECT b.fwd_10d-u.m exc, year(b.date) yr FROM bars b JOIN u USING(universe)
             WHERE b.fwd_10d IS NOT NULL AND b.fwd_10d BETWEEN -90 AND 500 AND {where})
       SELECT yr, round(median(exc),2) m, count(*) n FROM x GROUP BY yr ORDER BY yr""").fetchdf()
    yrs=" ".join(f"{int(y)%100}:{float(py[py.yr==y].m.iloc[0]) if len(py[py.yr==y]) and int(py[py.yr==y].n.iloc[0])>=30 else '–'}" for y in range(2021,2027))
    return r, yrs
print(f"{'stack':48} {'n':>8} {'medL':>7} {'m25L':>7} {'IS/OOS':>14}  per-year")
for label,where in TESTS:
    if where is None: print(f"  {label}"); continue
    r,yrs=stats(where)
    if not r[0] or r[0]<50: print(f"  {label:48} n={r[0]}"); continue
    print(f"  {label:48} {r[0]:>8} {r[1]:>+7} {r[2]:>+7} {str(r[3])+'/'+str(r[4]):>14}  {yrs}")
a.close(); print("\ndone")
