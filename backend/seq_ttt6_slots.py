"""Is it the PAIR, or just which token sits in each slot?

The 53-pair ranking disagrees with itself: the best cell clears chance by a hair (+0.887 vs a
p95 of +0.837), yet the mined→OOS rank correlation is NEGATIVE (−0.228) — the fine ordering
does not survive. That combination usually means the real structure is lower-dimensional than
the grid used to look for it.

Two things in the table point the same way. Eight of the top ten pairs carry T2, T2G or T12 in
the SECOND slot, and the calmest cells are the repeats (T6→T6→T6 drawdown −1.80 against a
family median of −2.48). So the marginals are worth reading directly: 14 tokens per slot is 28
cells instead of 196, a search small enough to mean something, and each cell keeps thousands of
observations instead of a hundred.

Same conventions: entry at close[i+1], five bars, drawdown as the true path low.
"""
import numpy as np, pandas as pd
from naked_study import NakedStudy

st = NakedStudy("slot marginals for tok1 → tok2 → T6", n_trials=2,
                columns=("t_sig","z_sig"), horizons=(5,), min_price=5.0,
                min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str).to_numpy(); tk = d["ticker"].to_numpy()
c = d["close"].to_numpy(float); lo = d["low"].to_numpy(float)
c1 = np.r_[c[1:], np.nan]; c6 = np.r_[c[6:], np.full(6, np.nan)]
lowroll = pd.Series(lo).rolling(5).min().shift(-5).to_numpy()
s1 = np.r_[tk[:-1]==tk[1:], False]; s6 = np.r_[tk[:-6]==tk[6:], np.zeros(6,bool)]
ent = np.where(s1, c1, np.nan)
ret5 = np.where(s6, c6/ent-1, np.nan)*100
mae5 = (np.where(s6, np.r_[lowroll[1:], np.nan], np.nan)/ent-1)*100
isT = (T!="")&(T!="nan"); p1=np.r_[[""],T[:-1]]; p2=np.r_[["",""],T[:-2]]
base = (T=="T6") & np.r_[False,tk[:-1]==tk[1:]] & np.r_[False,False,tk[:-2]==tk[2:]] \
       & np.r_[False,isT[:-1]] & np.r_[False,False,isT[:-2]] \
       & np.isfinite(ret5) & np.isfinite(mae5)
fam_r, fam_m = np.median(ret5[base]), np.median(mae5[base])
print(f"\n  family n={base.sum():,} · ret5 {fam_r:+.3f} · MAE5 {fam_m:+.2f}\n", flush=True)
SPLIT = np.datetime64("2024-01-01"); yr = d["_dt"].to_numpy()
for slot, arr in (("slot 1 (two bars back)", p2), ("slot 2 (the bar before T6)", p1)):
    print("="*104); print(f"  {slot}"); print("="*104)
    print(f"  {'token':>7s} {'n':>8s} {'ret5':>8s} {'Δfam':>7s} {'win':>7s} | "
          f"{'MAE med':>8s} {'Δfam':>7s} {'>5%':>7s} {'>10%':>7s} | {'mined':>8s} {'OOS':>8s}")
    rows=[]
    for t in sorted(set(T[isT])):
        m = base & (arr==t)
        if m.sum() < 150: continue
        r, a = ret5[m], mae5[m]
        mi, oo = m & (yr<SPLIT), m & (yr>=SPLIT)
        rows.append((t, int(m.sum()), np.median(r), (r>0).mean()*100, np.median(a),
                     (a<-5).mean()*100, (a<-10).mean()*100,
                     np.median(ret5[mi]) if mi.sum()>=80 else np.nan,
                     np.median(ret5[oo]) if oo.sum()>=80 else np.nan))
    for t,n,r,w,a,d5,d10,mi,oo in sorted(rows, key=lambda x:-x[2]):
        print(f"  {t:>7s} {n:>8,} {r:>+8.3f} {r-fam_r:>+7.3f} {w:>6.2f}% | {a:>+8.2f} "
              f"{a-fam_m:>+7.2f} {d5:>6.1f}% {d10:>6.1f}% | {mi:>+8.3f} {oo:>+8.3f}")
    R = pd.DataFrame(rows, columns=["t","n","r","w","a","d5","d10","mi","oo"]).dropna()
    print(f"\n    mined↔OOS rank correlation: {R.mi.corr(R.oo, method='spearman'):+.3f} "
          f"· sign held {int((np.sign(R.mi)==np.sign(R.oo)).sum())}/{len(R)}")
    print(f"    drawdown spread across tokens: {R.a.max()-R.a.min():.2f}pp\n", flush=True)
print("DONE", flush=True)
