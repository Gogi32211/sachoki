"""gap_flip.py — clean bar%+gap% (drop total) + the GAP-FLIP hypothesis: a move that is
BAR-driven (intraday action, real demand/absorption) vs GAP-driven (overnight chase/FOMO).
Does gap-dominance predict WORSE forward returns? Per RSI zone, green & red. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT ticker,universe,date,l_sig,open,close,rsi_14,volume,avg_vol_20d,fwd_10d,
   lag(close) OVER (PARTITION BY ticker,universe ORDER BY date) AS pclose
  FROM bars WHERE l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND avg_vol_20d>0
    AND fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500""").fetchdf();a.close()
df=df[df.pclose.notna()&(df.pclose>0)&(df.open>0)].copy()
df["vr"]=df.volume/df.avg_vol_20d; df["green"]=df.close>df.open
df["bar"]=(df.close-df.open)/df.open*100
df["gap"]=(df.open-df.pclose)/df.pclose*100
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["gapdom"]=df.gap.abs()>df.bar.abs()   # gap is the bigger component
V=(df.vr>=1.8)&(df.vr<2.5)
DEC=[("20-30",20,30),("30-40",30,40),("40-50",40,50),("50-60",50,60),("60-70",60,70),("70-80",70,80)]
def m(s): return float(s.exc.median()) if len(s)>=40 else None
print("### A) clean bar% + gap% (median) per RSI zone × 4 variants @ vol 1.8-2.5×  (total dropped)")
print(f"  {'variant':12} "+" ".join(f"{d[0]:>13}" for d in DEC)+"   (bar/gap)")
for lab,ls,gr in [("L34↑green","L34",True),("L34↓red","L34",False),("L46↑green","L46",True),("L46↓red","L46",False)]:
    cells=[]
    for dl,lo,hi in DEC:
        s=df[(df.l_sig==ls)&(df.green==gr)&(df.rsi_14>=lo)&(df.rsi_14<hi)&V]
        cells.append(f"{float(s.bar.median()):>+5.1f}/{float(s.gap.median()):>+5.1f}" if len(s)>=40 else f"{'·':>11}")
    print(f"  {lab:12} "+" ".join(f"{c:>13}" for c in cells))

def fwdrow(s,lab):
    n=len(s)
    if n<60: return f"  {lab:30} n={n}"
    e=s.exc;w=int((e>0).sum())
    py=sum(1 for y in range(2021,2027) for sy in [e[s.yr==y]] if len(sy)>=10 and float(sy.median())>0)
    ny=sum(1 for y in range(2021,2027) if len(e[s.yr==y])>=10)
    return f"  {lab:30} {n:>6} {w/n*100:>5.1f} {float(e.median()):>+6.2f} {wl(w,n)*100:>5.1f} {py}/{ny}"
print("\n### B) GAP-FLIP test — forward edge: BAR-driven vs GAP-driven (all vol, L34/L46 pooled)")
print(f"  {'split':30} {'n':>6} {'win%':>5} {'medL':>6} {'wLB':>5} +yr")
print("  -- GREEN up-moves (demand): is bar-driven > gap-driven? --")
GU=df.green
print(fwdrow(df[GU & ~df.gapdom], "green · BAR-driven (|bar|>|gap|)"))
print(fwdrow(df[GU & df.gapdom],  "green · GAP-driven (|gap|>|bar|) = chase"))
print("  -- by RSI zone (green, gap-driven = chase) --")
for dl,lo,hi in DEC:
    print(fwdrow(df[GU & df.gapdom & (df.rsi_14>=lo)&(df.rsi_14<hi)], f"  green gap-driven RSI {dl}"))
print("  -- RED down-moves (flush): bar-driven (intraday flush) vs gap-driven (gap-down panic) --")
RD=~df.green
print(fwdrow(df[RD & ~df.gapdom & (df.rsi_14<35)], "red · BAR-flush · RSI<35"))
print(fwdrow(df[RD & df.gapdom & (df.rsi_14<35)],  "red · GAP-down · RSI<35"))
print("\nlegend: medL=median fwd_10d excess. gapdom=gap is the bigger half of the move. done")
