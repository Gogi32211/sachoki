"""validate_k_signals.py — validate the '290509 K Signals (BUY)' Pine indicator on
OUR 5-yr stock DB (3 universes, 8M bars). Faithfully reconstruct each K from our
T/Z signal columns (lags) + RSI-rising filter + price anchors. Report the HONEST
edge: median excess (universe-drift removed) + clip25 + per-year + IS/OOS + Wilson LB
— not the in-sample NQ annotations. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"

SIGS=["t1","t1g","t2","t2g","t3","t4","t5","t6","t9","t10","t11","t12",
      "z1","z1g","z2","z2g","z3","z4","z5","z6","z9","z10","z11","z12"]
PRICE=["open","close"]
MAXLAG=4

def _wilson(w,n,z=1.96):
    if n==0: return 0.0
    p=w/n; d=1+z*z/n
    c=p+z*z/(2*n); m=z*((p*(1-p)/n+z*z/(4*n*n))**0.5)
    return max(0.0,(c-m)/d)

def load():
    a=get_analytics_conn()
    base=a.execute("SELECT universe,median(fwd_10d) m FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchdf()
    BMED=dict(zip(base.universe,base.m))
    lagsel=[]
    for s in SIGS:
        lagsel.append(f"sig_{s} AS {s}_0")
        for k in range(1,MAXLAG+1):
            lagsel.append(f"lag(sig_{s},{k}) OVER w AS {s}_{k}")
    for p in PRICE:
        for k in range(0,MAXLAG+1):
            lagsel.append(f"lag({p},{k}) OVER w AS {p}_{k}" if k else f"{p} AS {p}_0")
    lagsel.append("rsi_14 AS rsi_0"); lagsel.append("lag(rsi_14,2) OVER w AS rsi_2")
    q=f"""SELECT universe,date,fwd_10d,{', '.join(lagsel)}
          FROM bars WINDOW w AS (PARTITION BY ticker,universe ORDER BY date)"""
    df=a.execute(q).fetchdf(); a.close()
    df=df[df.fwd_10d.notna() & df.fwd_10d.between(-90,500)].copy()
    for c in df.columns:
        if c not in ("universe","date","fwd_10d"): df[c]=df[c].fillna(0)
    df["yr"]=pd.to_datetime(df.date).dt.year
    df["oos"]=df.date.astype(str)>=OOS
    df["exc"]=df.fwd_10d - df.universe.map(BMED)
    return df

def b(df,name): return df[name]==1            # signal lag truthy

def build_K(df):
    g=lambda n: df[n]==1
    rsi = df.rsi_0 > df.rsi_2                   # rsiPass (RSI rising vs 2 bars)
    K={}
    K["K1"]=g("t1_0")&((g("z1g_3")&g("z2g_2")&g("z2_1"))|(g("t2g_4")&g("z1g_3")&g("z2g_2")&g("z2_1"))|
        (g("z5_4")&g("z2g_3")&g("z2g_2")&g("z2_1"))|(g("z5_3")&g("z2g_2")&g("z2g_1"))|(g("z2_3")&g("t9_2")&g("z4_1"))|
        (g("t9_3")&g("t2g_2")&g("z3_1"))|(g("z2g_3")&g("z2g_2")&g("z2_1"))|(g("t4_2")&g("z3_1"))|
        (g("z2g_2")&g("z2_1"))|(g("z2g_1")&(df.close_0>df.open_2))|(g("z6_2")&g("z11_1"))|
        (g("z2g_3")&g("z6_2")&g("z11_1"))|(g("z4_3")&g("z6_2")&g("z11_1"))|(g("z11_2")&g("z11_1"))|
        (g("t3_2")&g("z3_1"))|(g("t9_2")&g("z3_1")))
    K["K1G"]=g("t1g_0")&((g("z1g_2")&g("z10_1"))|(g("t4_2")&g("z3_1"))|(g("z2g_3")&g("t4_2")&g("z3_1"))|
        (g("z2_3")&g("t9_2")&g("z5_1"))|(g("z2g_3")&g("z2g_2")&g("z2_1"))|(g("t3_2")&g("z1g_1"))|
        (g("t2g_2")&g("z9_1"))|(g("t4_3")&g("z9_2")&g("z2_1"))|(g("z3_3")&g("z2_2")&g("z10_1"))|
        (g("z2_2")&g("z10_1"))|g("z10_1")|(g("z11_2")&g("z10_1"))|(g("z2g_3")&g("z11_2")&g("z10_1")))
    K["K2"]=g("t2_0")&((g("z1g_2")&g("t4_1"))|(g("t1_3")&g("z5_2")&g("t1_1"))|(g("t6_3")&g("t2g_2")&g("t2g_1"))|
        (g("t1g_3")&g("t2_2")&g("t2g_1"))|(g("t1_3")&g("t2g_2")&g("t6_1"))|(g("t1g_3")&g("t2g_2")&g("t6_1"))|
        (g("t2g_2")&g("t11_1"))|(g("t2g_2")&g("t12_1"))|g("t1_1")|g("t1g_1"))
    K["K2G"]=g("t2g_0")&((g("t1_2")&g("t6_1"))|(g("t9_3")&g("t6_2")&g("t2g_1"))|(g("t1_3")&g("t2g_2")&g("t11_1"))|
        (g("t3_3")&g("t2g_2")&g("t6_1"))|(g("t9_3")&g("z5_2")&g("t4_1"))|(g("z2g_4")&g("z2g_3")&g("z2_2")&g("t1_1"))|
        (g("t3_3")&g("z4_2")&g("t9_1"))|(g("t2g_3")&g("t6_2")&g("t2g_1"))|g("t1_1")|g("t1g_1")|
        (g("t9_1")&(df.close_0>df.close_3))|(g("t2_1")&(df.close_0>df.close_3))|(g("z10_3")&g("z6_2")&g("t3_1"))|
        (g("z6_2")&g("t3_1"))|(g("z10_2")&g("t3_1"))|(g("z9_3")&g("z2g_2")&g("t4_1"))|g("t4_1")|
        (g("t2g_2")&g("t11_1"))|(g("t2g_2")&g("t10_1")))
    K["K3"]=g("t3_0")&((g("z3_3")&g("t4_2")&g("z9_1"))|(g("z4_3")&g("t4_2")&g("z9_1"))|(g("t6_2")&g("z5_1"))|
        (g("t2_3")&g("z1g_2")&g("z2g_1"))|(g("t6_3")&g("z1g_2")&g("z2g_1"))|(g("t1g_2")&g("z3_1"))|
        (g("z9_2")&g("z11_1"))|g("z11_1"))
    K["K4"]=g("t4_0")&((g("t1g_2")&g("z5_1"))|(g("t9_2")&g("z5_1"))|(g("z4_3")&g("z2_2")&g("z2g_1"))|
        (g("z2_3")&g("t4_2")&g("z3_1"))|(g("t2g_3")&g("t6_2")&g("z9_1"))|(g("z1g_2")&g("z2_1"))|
        (g("t3_2")&g("z3_1"))|(g("t4_2")&g("z9_1"))|(g("t1g_3")&g("z9_2")&g("z2g_1"))|(g("z10_2")&g("z11_1")))
    K["K5"]=g("t5_0")&((g("z3_3")&g("t4_2")&g("z9_1"))|(g("z5_3")&g("t4_2")&g("z9_1"))|(g("t6_3")&g("z1g_2")&g("z2g_1"))|
        (g("t2_3")&g("z1g_2")&g("z2g_1"))|(g("t2g_3")&g("t2g_2")&g("z3_1"))|(g("t2_3")&g("t2g_2")&g("z1_1"))|
        (g("t4_2")&g("z9_1")))
    k6ex=~(g("t4_1")|g("t6_1")|g("t12_1")|g("t2_1"))
    K["K6"]=g("t6_0")&k6ex&((g("z4_3")&g("t9_2")&g("t2g_1"))|(g("z2g_4")&g("z2_3")&g("t1_2")&g("t2g_1"))|
        (g("z2_3")&g("t1_2")&g("t2g_1"))|(g("z2g_3")&g("t1_2")&g("t2g_1"))|(g("t1g_2")&g("t2g_1"))|
        (g("t9_2")&g("t2g_1"))|(g("t1_2")&g("t2g_1"))|(g("t2g_3")&g("t2_2")&g("t2g_1"))|(g("z2_3")&g("t9_2")&g("t2g_1"))|
        (g("z5_3")&g("t1_2")&g("t10_1"))|(g("t1_2")&g("t10_1"))|(g("t2g_3")&g("t11_2")&g("t2g_1"))|
        (g("t2g_3")&g("t10_2")&g("t2g_1")))
    K["K9"]=g("t9_0")&((g("t2g_3")&g("z1g_2")&g("z2g_1"))|(g("t2g_3")&g("z1g_2")&g("z2_1"))|(g("z2_3")&g("t1_2")&g("z4_1"))|
        (g("z5_3")&g("t4_2")&g("z3_1"))|(g("t3_3")&g("t2g_2")&g("z4_1"))|(g("z9_3")&g("z2g_2")&g("z2_1"))|
        (g("t3_3")&g("z4_2")&g("z2_1"))|(g("z3_3")&g("t4_2")&g("z9_1")))
    K["K10"]=g("t4_0")&((g("z5_3")&g("t3_2")&g("z3_1"))|(g("z11_3")&g("z2g_2")&g("z2_1"))|(g("z9_3")&g("t3_2")&g("z9_1"))|
        (g("z2_3")&g("t4_2")&g("z3_1"))|(g("t4_3")&g("z9_2")&g("z11_1")))
    K["K11"]=((g("t2g_0")&g("t1_2")&g("t6_1"))|(g("t2g_0")&g("t9_3")&g("t6_2")&g("t2g_1"))|
        (g("t6_0")&g("t1_1")&~(g("t4_2")|g("t6_2")))|(g("t2g_0")&g("z5_3")&g("z2_2")&g("t9_1"))|
        (g("t2g_0")&g("t4_3")&g("z3_2")&g("t3_1")))
    return {k:(v&rsi) for k,v in K.items()}   # faithful: RSI-rising filter ON

# headline sub-patterns the indicator BRAGS about (test standalone vs NQ annotation)
def subpatterns(df):
    g=lambda n: df[n]==1
    return {
      "K11① T1|T6→T2G (NQ avg10+35%)": g("t2g_0")&g("t1_2")&g("t6_1"),
      "K2 ① Z1G|T4→T2 (NQ avg10+16.8%)": g("t2_0")&g("z1g_2")&g("t4_1"),
      "K2G① T1|T6→T2G (NQ avg10+35%)": g("t2g_0")&g("t1_2")&g("t6_1"),
      "K3 ① Z3|T4|Z9→T3 (fail0%!!)": g("t3_0")&g("z3_3")&g("t4_2")&g("z9_1"),
      "K10① Z5|T3|Z3→T4 (w5 76%)": g("t4_0")&g("z5_3")&g("t3_2")&g("z3_1"),
      "K1 ① Z1G|Z2G|Z2→T1 (w5 76%)": g("t1_0")&g("z1g_3")&g("z2g_2")&g("z2_1"),
    }

def stats(df,mask,label):
    sub=df[mask]; n=len(sub)
    if n<20: return f"  {label:40} n={n:>6}  <20 (skip)"
    e=sub.exc; w=int((e>0).sum())
    med=float(e.median()); m25=float(e.clip(-25,25).mean())
    lb=_wilson(w,n)*100
    isv=float(sub[~sub.oos].exc.median()); oo=float(sub[sub.oos].exc.median())
    posy=ny=0; yc=[]
    for y in range(2021,2027):
        sy=sub[sub.yr==y]
        if len(sy)>=20:
            v=float(sy.exc.median()); yc.append(f"{y%100}:{v:+.1f}"); ny+=1; posy+= v>0
        else: yc.append(f"{y%100}:–")
    flag="✅" if (med>0 and posy>=max(4,ny-1) and oo>0) else ("⚠️" if med>0 else "❌")
    return f"  {label:40} {n:>6} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {lb:>5.1f} {isv:>+5.1f}/{oo:>+5.1f} {posy}/{ny} {flag}  "+" ".join(yc)

if __name__=="__main__":
    print("loading 5-yr T/Z lags…"); df=load()
    print(f"rows={len(df):,}  universe medians removed (excess)\n")
    hdr=f"  {'signal':40} {'n':>6} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} {'IS/OOS':>11} {'+yr':>4}  per-year median-excess"
    print("### FULL K-SIGNALS (as coded: sequences + broad anchors + RSI-rising filter)")
    print(hdr)
    K=build_K(df)
    for k in ["K1","K1G","K2","K2G","K3","K4","K5","K6","K9","K10","K11"]:
        print(stats(df,K[k],k))
    print("\n### HEADLINE SUB-PATTERNS standalone (test the bragged NQ stats on STOCKS)")
    print(hdr)
    for lbl,m in subpatterns(df).items():
        print(stats(df,m,lbl))
    print("\nlegend: medL/m25L = median / clip25-mean forward EXCESS vs universe (%). "
          "wLB=Wilson95 lower bound on win%. ✅ med>0 & 5/6yr & OOS>0.")
    print("done")
