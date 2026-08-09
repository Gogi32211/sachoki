"""The other tail of tok1 → tok2 → T6: the biggest five-bar gains, with names and dates.

Same trades, same conventions as the drawdown list (deduplicated bars, calendar-adjacent
sequences, entry at close[i+1]) — only sorted the other way. Two extra columns matter here:
MFE, the highest the position ever got, and MAE, how far it fell BEFORE it paid, because a
+60% that first went −30% is not a trade anyone holds.

Unadjusted corporate actions distort this tail too, just mirrored: a reverse split that the
data never adjusted multiplies the price and reads as a spectacular gain. Same detector — a
single bar past +45% — plus a price implausible for the name's liquidity.
"""
import numpy as np, pandas as pd
from data_contract import sequence_mask
from naked_study import NakedStudy

st = NakedStudy("tok1 → tok2 → T6, the winning tail", n_trials=2,
                columns=("t_sig","z_sig"), horizons=(5,), min_price=5.0,
                min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str); Z = d["z_sig"].fillna("").astype(str)
d["tok"] = np.where(T.ne("")&T.ne("nan"), T, Z)
TT = sorted({t for t in d["tok"].unique() if str(t).startswith("T")})
tk = d["ticker"].to_numpy(); c = d["close"].to_numpy(float)
hi = d["high"].to_numpy(float); lo = d["low"].to_numpy(float)
dates = d["date"].astype(str).str[:10].to_numpy()
c1 = np.r_[c[1:], np.nan]; c6 = np.r_[c[6:], np.full(6, np.nan)]
hiroll = pd.Series(hi).rolling(5).max().shift(-5).to_numpy()
loroll = pd.Series(lo).rolling(5).min().shift(-5).to_numpy()
barmax = pd.Series(np.r_[c[1:]/c[:-1]-1, np.nan]).rolling(5).max().shift(-5).to_numpy()
s1 = np.r_[tk[:-1]==tk[1:], False]; s6 = np.r_[tk[:-6]==tk[6:], np.zeros(6,bool)]
ent = np.where(s1, c1, np.nan)
ret5 = np.where(s6, c6/ent-1, np.nan)*100
mfe5 = (np.where(s6, np.r_[hiroll[1:], np.nan], np.nan)/ent-1)*100
mae5 = (np.where(s6, np.r_[loroll[1:], np.nan], np.nan)/ent-1)*100
bbar = np.r_[barmax[1:], np.nan]*100
base = sequence_mask(d, [list(TT), list(TT), "T6"]) & np.isfinite(ret5) & np.isfinite(mfe5)
tokarr = d["tok"].to_numpy(); p1 = np.r_[[""],tokarr[:-1]]; p2 = np.r_[["",""],tokarr[:-2]]
i = np.where(base)[0]
B = pd.DataFrame(dict(ticker=tk[i], entry_date=dates[np.minimum(i+1,len(d)-1)],
    combo=[f"{a}→{b}→T6" for a,b in zip(p2[i],p1[i])], entry=np.round(ent[i],2),
    ret5=np.round(ret5[i],2), mfe=np.round(mfe5[i],2), mae=np.round(mae5[i],2),
    best_bar=np.round(bbar[i],2)))
B["artifact"] = (B.best_bar > 45) | (B.entry > 150)
B = B.sort_values("ret5", ascending=False)
B.to_csv("seq_ttt6_best.csv", index=False)
C = B[~B.artifact]
print(f"\n  {len(B):,} trades · flagged as corporate actions {int(B.artifact.sum()):,}\n")
print(" ტიკერი    შესვლა         კომბინაცია      $in     ret5      MFE      MAE   საუკ. ბარი")
for _, r in C.head(35).iterrows():
    note = f"   ← ჯერ {r.mae:.0f}%-ით ჩავარდა" if r.mae < -12 else ""
    print(f" {r.ticker:>6s}   {r.entry_date}   {r.combo:>14s}   {r.entry:>7.2f}  "
          f"{r.ret5:>+7.2f}  {r.mfe:>+7.2f}  {r.mae:>+7.2f}    {r.best_bar:>+7.2f}{note}")
print()
up = C[C.ret5 >= 25]; dn_ct = (C.mae <= -25).sum()
print(f"+25%-ზე მეტი მოგება: {len(up):,} ({len(up)/len(C):.2%})   ·   "
      f"−25%-ზე ღრმა ვარდნა: {dn_ct:,} ({dn_ct/len(C):.2%})")
print(f"   → asymmetry: {len(up)/max(dn_ct,1):.2f}× more big winners than big losers")
print(f"\nდიდი მოგებიდან ({len(up)}) ჯერ −10%-ზე ღრმად ჩავარდა: "
      f"{int((up.mae<-10).sum())} ({(up.mae<-10).mean():.1%})")
print(f"MFE მედიანა ყველა სდელკაზე {C.mfe.median():+.2f}% · realised {C.ret5.median():+.2f}%"
      f" → capture {C.ret5.median()/C.mfe.median():.1%}")
b = pd.cut(C.entry,[0,8,21,89,377,1e9],labels=["<$8","$8-21","$21-89","$89-377",">$377"])
print(f"\n+25% მოგების ალბათობა ფასის მიხედვით:")
for k in ["<$8","$8-21","$21-89","$89-377",">$377"]:
    n_all=int((b==k).sum()); n_up=int(((b==k)&(C.ret5>=25)).sum())
    n_dn=int(((b==k)&(C.mae<=-25)).sum())
    if n_all: print(f"   {k:>9s}  +25% → {n_up/n_all:>6.2%}   −25% → {n_dn/n_all:>6.2%}   "
                    f"ratio {n_up/max(n_dn,1):>5.2f}×   (n={n_all:,})")
print("\n  written: seq_ttt6_best.csv")
