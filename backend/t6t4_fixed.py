"""T6/T4 and the gap, redone on deduplicated bars.

The original run concluded that T6/T4 sit on a large gap-down bounce worth +0.9% at a 57.7%
win rate. On clean data their median overnight gap is 0.000% and that edge is 0.000% — the
gap, the bounce and the effect were all the duplicate rows. This re-runs the two tests that
were supposed to settle it, so the conclusion rests on something real:

  A. T6/T4 against non-token bars in the SAME narrow gap bin — does the token add anything
     once gap depth is equalised
  B. the token with no gap at all (gap >= 0)

Plus the shape across horizons, which separates a one-day bounce from a drift.
"""
import numpy as np
from naked_study import NakedStudy

BINS = [(-0.5,-1.0), (-1.0,-2.0), (-2.0,-4.0), (-4.0,-100.0)]
LBL = ["-0.5..-1%","-1..-2%","-2..-4%","< -4%"]

st = NakedStudy("T6/T4 vs the gap (deduplicated)", n_trials=4, columns=("t_sig","z_sig"),
                horizons=(1,3,10,20), min_price=5.0, min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str).to_numpy()
Z = d["z_sig"].fillna("").astype(str).to_numpy()
tok = np.where((T!="")&(T!="nan"), T, Z)
nxo = d.groupby("ticker", sort=False)["open"].shift(-1).to_numpy()
g = (nxo/d["close"].to_numpy()-1)*100
ok = np.isfinite(g)
t6, t4 = (tok=="T6")&ok, (tok=="T4")&ok
nei = ~(t6|t4)&ok
print(f"\n  median overnight gap — T6 {np.median(g[t6]):+.3f}% · T4 {np.median(g[t4]):+.3f}% "
      f"· all bars {np.median(g[ok]):+.3f}%   (before dedup: -0.63% / -0.65%)", flush=True)
print("\n" + "="*118)
print("  A · SAME GAP SIZE, WITH AND WITHOUT THE TOKEN")
print("="*118)
print(f"  {'gap bin':>10s} {'who':>9s} {'n':>9s} {'gap med':>8s} " +
      " ".join(f"{f'{h}b':>8s}" for h in st.hor) + f" {'vs ctl 10b':>11s}")
for (hi,lo_),lbl in zip(BINS,LBL):
    m = (g<hi)&(g>=lo_)
    ref=None
    for who,msk in (("NEITHER",nei&m),("T6",t6&m),("T4",t4&m)):
        sub = d[msk]
        if len(sub)<300:
            print(f"  {lbl:>10s} {who:>9s} {len(sub):>9,}   (thin)"); continue
        med=[sub[f"r{h}"].median()*100 for h in st.hor]
        if who=="NEITHER": ref=med
        dv = med[st.hor.index(10)]-ref[st.hor.index(10)] if ref else 0.0
        print(f"  {lbl:>10s} {who:>9s} {len(sub):>9,} {np.median(g[msk]):>+8.2f} " +
              " ".join(f"{x:>+8.3f}" for x in med) +
              (f" {dv:>+11.3f}" if who!="NEITHER" else f" {'-':>11s}"))
    print()
print("="*118)
print("  B · THE TOKEN WITH NO GAP  (gap >= 0)")
print("="*118)
print(f"  {'who':>9s} {'n':>9s} " + " ".join(f"{f'{h}b':>8s}" for h in st.hor))
ref=None
for who,msk in (("NEITHER",nei&(g>=0)),("T6",t6&(g>=0)),("T4",t4&(g>=0))):
    sub=d[msk]
    med=[sub[f"r{h}"].median()*100 for h in st.hor]
    if who=="NEITHER": ref=med
    print(f"  {who:>9s} {len(sub):>9,} " + " ".join(f"{x:>+8.3f}" for x in med) +
          ("" if who=="NEITHER" else
           "   token effect " + " ".join(f"{a-b:>+7.3f}" for a,b in zip(med,ref))))
print("\n" + "="*118)
print("  C · WITH A GAP-MATCHED CONTROL (the fix that killed the original claim)")
print("="*118)
st.population(n_boot=250)
for lbl,m in (("T6 (all)",t6),("T4 (all)",t4)):
    st.signal(lbl, m, n_boot=350, on=np.nan_to_num(g))
print("\nDONE", flush=True)
