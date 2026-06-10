"""delta_leads_tradeable.py — the pivot-conditioned edge is LOOKAHEAD (Williams
centered pivot needs N future bars). Re-test the SMX 'delta leads price' insight with
ONLY real-time-available context (crash / LOAD / no-supply / run-up). Honest answer:
is delta-flip tradeable without hindsight? ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"; a=get_analytics_conn()
base=a.execute("""SELECT universe, median(fwd_10d) m FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe""").fetchdf()
BMED=dict(zip(base.universe,base.m))
cols=("universe,date,fwd_10d,d_flip_bull,d_div_bear,load,sig_ns_vabs,d_surge_bull,d_surge_bear,"
      "drop_20pct_10d,drop_30pct_20d,hit_2x_60d,hit_50pct_20d,price_lt_20,price_gt_20,rsi_le_35,rsi_ge_70,"
      "sig_fri34,sig_abs,is_pivot_low_5,is_pivot_high_5")
df=a.execute(f"""SELECT {cols} FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
   AND (d_flip_bull=1 OR d_div_bear=1)""").fetchdf(); a.close()
df['yr']=pd.to_datetime(df.date).dt.year; df['oos']=df.date.astype(str)>=OOS
df['exc']=df.fwd_10d-df.universe.map(BMED)

def rep(m,label,short=False):
    sub=df[m]; sgn=-1 if short else 1
    if len(sub)<40: print(f"  {label:46} n={len(sub)} <40"); return
    e=sub.exc; med=sgn*float(e.median()); m25=sgn*float(e.clip(-25,25).mean())
    isv=sgn*float(sub[~sub.oos].exc.median()); oo=sgn*float(sub[sub.oos].exc.median())
    posy=ny=0; yrs=[]
    for y in range(2021,2027):
        sy=sub[sub.yr==y]
        if len(sy)>=20:
            v=sgn*float(sy.exc.median()); yrs.append(f"{y%100}:{round(v,2)}"); ny+=1; posy+= v>0
        else: yrs.append(f"{y%100}:–")
    print(f"  {label:46} {len(sub):>6} {med:>+7.2f} {m25:>+7.2f} {isv:>+6.2f}/{oo:>+5.2f} {str(posy)+'/'+str(ny):>5}  "+" ".join(yrs))

print("### LONG — d_flip_bull, REAL-TIME context only (no pivot lookahead)")
print(f"  {'context':46} {'n':>6} {'medL':>7} {'m25L':>7} {'IS/OOS':>12} {'+yr':>5}  per-year excess")
rep(df.d_flip_bull==1, "d_flip_bull alone")
rep((df.d_flip_bull==1)&(df.drop_20pct_10d==1), "+ crashed -20% in 10d")
rep((df.d_flip_bull==1)&(df.drop_30pct_20d==1), "+ crashed -30% in 20d")
rep((df.d_flip_bull==1)&(df.load==1), "+ LOAD (Wyckoff)")
rep((df.d_flip_bull==1)&(df.sig_ns_vabs==1), "+ no-supply VABS")
rep((df.d_flip_bull==1)&(df.load==1)&(df.d_surge_bull==1), "+ LOAD & d_surge_bull")
rep((df.d_flip_bull==1)&(df.drop_20pct_10d==1)&(df.price_lt_20==1), "+ crash & below 20MA (SMX-like)")
rep((df.d_flip_bull==1)&(df.sig_fri34==1), "+ FRI34 (same bar)")
print("  >> reference (LOOKAHEAD, not tradeable):")
rep((df.d_flip_bull==1)&(df.is_pivot_low_5==1), "d_flip_bull & pivot_low [lookahead]")

print("\n### SHORT — d_div_bear, REAL-TIME context only")
print(f"  {'context':46} {'n':>6} {'medL':>7} {'m25L':>7} {'IS/OOS':>12} {'+yr':>5}  per-year (short)")
rep(df.d_div_bear==1, "d_div_bear alone", short=True)
rep((df.d_div_bear==1)&(df.hit_2x_60d==1), "+ doubled in 60d (run-up)", short=True)
rep((df.d_div_bear==1)&(df.hit_50pct_20d==1), "+ +50% in 20d", short=True)
rep((df.d_div_bear==1)&(df.rsi_ge_70==1), "+ RSI>=70 overbought", short=True)
rep((df.d_div_bear==1)&(df.hit_2x_60d==1)&(df.rsi_ge_70==1), "+ doubled & overbought", short=True)
rep((df.d_div_bear==1)&(df.d_surge_bear==1), "+ d_surge_bear", short=True)
print("  >> reference (LOOKAHEAD, not tradeable):")
rep((df.d_div_bear==1)&(df.is_pivot_high_5==1), "d_div_bear & pivot_high [lookahead]", short=True)
print("\ndone")
