"""rgti_pattern_aggregate.py — RGTI Oct-2024 run was persistently in Wyckoff ACCUMULATION
(w2_accum) with breakout signals (pb_lvbo / sig_abs / eb_bull / vbo_up / bo_up) at each leg.
Test these candidate patterns in AGGREGATE (all 8M bars) — is there a generalizable
regularity, or RGTI survivorship? median excess + per-year + IS/OOS. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
C=["w2_accum","pb_lvbo","sig_abs","eb_bull","vbo_up","bo_up","bx_up","load","sq","sig_best","sig_strong","sig_eb_up"]
df=a.execute(f"""SELECT universe,date,rsi_14,l_sig,fwd_10d,{','.join(C)}
  FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500 AND rsi_14 IS NOT NULL""").fetchdf();a.close()
for c in C: df[c]=df[c].fillna(0).astype(np.int8)
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
g=lambda n: df[n]==1
def rep(m,lab):
    s=df[m];n=len(s)
    if n<200: return f"  {lab:38} n={n}"
    e=s.exc;w=int((e>0).sum());med=float(e.median());m25=float(e.clip(-25,25).mean())
    isv=float(e[~(s.date.astype(str)>=OOS)].median());oo=float(e[s.date.astype(str)>=OOS].median())
    py=ny=0
    for y in range(2021,2027):
        sy=e[s.yr==y]
        if len(sy)>=20:ny+=1;py+=float(sy.median())>0
    return f"  {lab:38} {n:>7} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {wl(w,n)*100:>5.1f} {isv:>+5.1f}/{oo:>+5.1f} {py}/{ny}"
hdr=f"  {'pattern':38} {'n':>7} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} {'IS/OOS':>11} +yr"
print("### individual candidates (all bars)")
print(hdr)
for c in C: print(rep(g(c),c))
MK=(df.rsi_14>=40)&(df.rsi_14<70)
print("\n### in MARKUP zone (RSI 40-70) + combos (the RGTI signature)")
print(hdr)
print(rep(MK&g("w2_accum"), "w2_accum (markup)"))
print(rep(MK&g("w2_accum")&(g("eb_bull")|g("vbo_up")|g("bo_up")), "w2_accum + breakout"))
print(rep(MK&g("w2_accum")&g("pb_lvbo"), "w2_accum + pb_lvbo"))
print(rep(MK&g("sig_abs")&g("eb_bull"), "ABS + EB↑ (validated seq)"))
print(rep(MK&g("pb_lvbo"), "pb_lvbo (markup)"))
print(rep(MK&g("sig_abs")&(g("eb_bull")|g("vbo_up")), "ABS + breakout"))
print(rep(MK&g("w2_accum")&g("sig_abs"), "w2_accum + ABS"))
print(rep(MK&(df.l_sig=='L34')&g("sq")&g("load"), "L34 + squeeze + load"))
print("\nlegend: medL=median fwd_10d excess. done")
