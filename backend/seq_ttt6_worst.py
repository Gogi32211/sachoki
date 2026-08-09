"""The actual names and dates behind the drawdown tails of tok1 → tok2 → T6.

A percentile hides what a list shows. −20% at p1 could be forty ordinary bad weeks or four
catastrophes, and only the rows tell you which. So this prints, for every combination, the
trades whose five-bar path low was deepest — ticker, the T6 bar's date, the date we would
have entered, how far it fell and where it finished.

Entry is close[i+1] and the drawdown is min(low[i+2 … i+6])/entry − 1, the same convention as
the pair study, so these rows are the same trades those medians were computed from.

Anything past about −50% in five sessions on a $5+/$3M name is usually a corporate action the
data never adjusted rather than a real loss, so each row carries the single worst bar inside
the window: a −80% day with volume is a crash, a −80% day is otherwise a split.
"""
import numpy as np, pandas as pd
from naked_study import NakedStudy

st = NakedStudy("worst drawdowns behind tok1 → tok2 → T6", n_trials=2,
                columns=("t_sig","z_sig"), horizons=(5,), min_price=5.0,
                min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str).to_numpy(); tk = d["ticker"].to_numpy()
c = d["close"].to_numpy(float); lo = d["low"].to_numpy(float)
dt = pd.to_datetime(d["_dt"]).dt.strftime("%Y-%m-%d").to_numpy()
c1 = np.r_[c[1:], np.nan]; c6 = np.r_[c[6:], np.full(6, np.nan)]
lowroll = pd.Series(lo).rolling(5).min().shift(-5).to_numpy()
s1 = np.r_[tk[:-1]==tk[1:], False]; s6 = np.r_[tk[:-6]==tk[6:], np.zeros(6,bool)]
ent = np.where(s1, c1, np.nan)
ret5 = np.where(s6, c6/ent-1, np.nan)*100
mae5 = (np.where(s6, np.r_[lowroll[1:], np.nan], np.nan)/ent-1)*100
# worst single bar inside the window — separates a crash from an unadjusted split
bar = np.r_[c[1:]/c[:-1]-1, np.nan]
bar = np.where(np.r_[tk[:-1]==tk[1:], False], bar, np.nan)
worstbar = pd.Series(bar).rolling(5).min().shift(-5).to_numpy()*100

isT = (T!="")&(T!="nan"); p1=np.r_[[""],T[:-1]]; p2=np.r_[["",""],T[:-2]]
base = (T=="T6") & np.r_[False,tk[:-1]==tk[1:]] & np.r_[False,False,tk[:-2]==tk[2:]] \
       & np.r_[False,isT[:-1]] & np.r_[False,False,isT[:-2]] \
       & np.isfinite(ret5) & np.isfinite(mae5)
i = np.where(base)[0]
W = pd.DataFrame(dict(ticker=tk[i], t6_date=dt[i], entry_date=dt[i+1],
                      combo=[f"{a}→{b}→T6" for a,b in zip(p2[i], p1[i])],
                      entry=np.round(ent[i],2), mae=np.round(mae5[i],2),
                      ret5=np.round(ret5[i],2), worst_bar=np.round(worstbar[i],2)))
W["flag"] = np.where(W.worst_bar < -45, "⚠ likely unadjusted corporate action", "")
W = W.sort_values("mae")
W.to_csv("seq_ttt6_worst.csv", index=False)

print("\n" + "="*126)
print("  THE 40 DEEPEST FIVE-BAR DRAWDOWNS IN THE WHOLE FAMILY")
print("="*126)
print(f"  {'ticker':>7s} {'T6 bar':>11s} {'entry':>11s} {'combo':>16s} {'$in':>8s} "
      f"{'MAE':>8s} {'ret5':>8s} {'worst bar':>10s}  note")
for _, r in W.head(40).iterrows():
    print(f"  {r.ticker:>7s} {r.t6_date:>11s} {r.entry_date:>11s} {r.combo:>16s} "
          f"{r.entry:>8.2f} {r.mae:>+8.2f} {r.ret5:>+8.2f} {r.worst_bar:>+10.2f}  {r.flag}")

print("\n" + "="*126)
print("  WORST THREE PER COMBINATION  (combinations with n ≥ 100)")
print("="*126)
cnt = W.combo.value_counts()
for combo in [c_ for c_ in cnt.index if cnt[c_] >= 100]:
    g = W[W.combo == combo].head(3)
    med = W[W.combo == combo].mae.median()
    print(f"\n  {combo:18s} n={cnt[combo]:>6,}  median MAE {med:>+6.2f}%")
    for _, r in g.iterrows():
        print(f"      {r.ticker:>7s} {r.entry_date}  ${r.entry:>8.2f}  MAE {r.mae:>+8.2f}%  "
              f"ret5 {r.ret5:>+7.2f}%  worst bar {r.worst_bar:>+7.2f}%  {r.flag}")

print("\n" + "="*126)
print("  HOW MUCH OF THE TAIL IS REAL?")
print("="*126)
for th in (-20, -30, -50):
    s = W[W.mae <= th]
    art = (s.worst_bar < -45).sum()
    print(f"    drawdowns worse than {th}%: {len(s):>5,}  ({len(s)/len(W):.2%})   "
          f"of which likely corporate actions: {art} ({art/max(len(s),1):.1%})")
rep = W[W.flag != ""]
print(f"\n    flagged rows: {len(rep)} · tickers: "
      f"{', '.join(sorted(rep.ticker.unique())[:18])}"
      f"{' …' if rep.ticker.nunique() > 18 else ''}")
clean = W[W.flag == ""]
print(f"\n    excluding flagged rows: median MAE {clean.mae.median():+.2f}% · "
      f"p1 {np.percentile(clean.mae,1):+.2f}% · worst {clean.mae.min():+.2f}% "
      f"({clean.iloc[clean.mae.values.argmin()].ticker} "
      f"{clean.iloc[clean.mae.values.argmin()].entry_date})")
print(f"\n  written: seq_ttt6_worst.csv  ({len(W):,} rows, sorted worst first)")
print("\nDONE", flush=True)
