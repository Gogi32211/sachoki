"""1H personality segmentation of the S&P 500 (user request 2026-08-05).

Per ticker, compute the portrait metrics the 14 hand-studied names separated on:
  drift/vol, regime bases (oversold / mid / fresh-50-cross), green-chain sign,
  panic-Z-chain sign, the momentum signature (gap-pair -> soft red Z3/Z4),
  the value token (Z11 ending), and top-cell year-stability (lottery detector).
Then KMeans over standardized features, k chosen by silhouette from 4..7 — so any
EXTRA segment beyond the four known ones can emerge on its own. Validation: the 12
hand-labeled reference tickers (AMD/NVDA/LLY=🔥, TSLA=⚡, CVX/JPM/KO/CSCO/IBM/UBER=🏦,
RGTI-not-sp500... use in-index refs only) must land coherently.

Output: cluster profiles + members -> h1_sp500_segments.csv
"""
import numpy as np
import pandas as pd
import duckdb, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB

c = duckdb.connect(ANALYTICS_DB, read_only=True)
SP = set(r[0] for r in c.execute("SELECT DISTINCT ticker FROM bars WHERE universe='sp500'").fetchall())
c.close()
print(f"sp500 tickers: {len(SP)}", flush=True)

DF = pd.read_parquet("data/h1_research_frame.parquet",
                     columns=["ticker", "dt", "close", "rsi_14", "t", "z"])
DF = DF[DF["ticker"].isin(SP)]
print(f"frame rows {len(DF):,} · tickers {DF['ticker'].nunique()}", flush=True)

TT = {"T2", "T2G", "T1", "T1G"}
rows = []
for tk, a in DF.groupby("ticker", sort=False):
    if len(a) < 3000:                       # need real history for year-stability
        continue
    a = a.reset_index(drop=True)
    code = np.where(a["t"] != "", a["t"], a["z"])
    isT = (a["t"] != "").to_numpy()
    f35 = (a["close"].shift(-35) / a["close"] - 1) * 100
    yr = a["dt"].str[:4]
    r = a["rsi_14"]
    base = f35.median(); std = f35.std()
    ovs = f35[r < 35].median()
    mid = f35[(r >= 35) & (r < 50)].median()
    cross = f35[(r >= 50) & (r.shift(1).rolling(3).min() < 50)].median()
    c1 = np.roll(code, 1); c2 = np.roll(code, 2)
    t1 = np.roll(isT, 1); t2 = np.roll(isT, 2)
    t1[0] = t2[:2] = False
    green = f35[isT & t1 & t2]
    green3 = green.median() if len(green) >= 30 else np.nan
    panic = f35[(~isT) & ~t1 & ~t2 & (r < 40).to_numpy()]
    panic3 = panic.median() if len(panic) >= 30 else np.nan
    gp = (np.isin(c2, list(TT)) & np.isin(c1, list(TT))
          & (np.char.find(c2.astype(str), "G") + np.char.find(c1.astype(str), "G") >= -1)
          & np.isin(code, ["Z3", "Z4"]))
    gapsoft = f35[gp].median() if gp.sum() >= 15 else np.nan
    z11 = f35[code == "Z11"]
    z11m = z11.median() if len(z11) >= 10 else np.nan
    # lottery detector: median worst-year of the top-5 sequence cells
    seq = pd.Series(c2).astype(str) + ">" + pd.Series(c1).astype(str) + ">" + pd.Series(code)
    d = pd.DataFrame({"s": seq, "f": f35, "y": yr}).dropna()
    g = d.groupby("s")["f"].agg(["size", "median"])
    top = g[g["size"] >= 12].sort_values("median", ascending=False).head(5)
    wt = []
    for s in top.index:
        ym = d[d["s"] == s].groupby("y")["f"].median()
        if len(ym) >= 3:
            wt.append(ym.min())
    wtop = float(np.median(wt)) if wt else np.nan
    rows.append(dict(ticker=tk, n=len(a), base=base, std=std, ovs=ovs, mid=mid,
                     cross=cross, green3=green3, panic3=panic3, gapsoft=gapsoft,
                     z11=z11m, wtop=wtop))

F = pd.DataFrame(rows).set_index("ticker")
print(f"feature table: {len(F)} tickers", flush=True)

# relative-to-base versions (personality = deviation from own drift)
for col in ["ovs", "mid", "cross", "green3", "panic3", "gapsoft", "z11"]:
    F["r_" + col] = F[col] - F["base"]
FEATS = ["base", "std", "r_ovs", "r_mid", "r_cross", "r_green3", "r_panic3",
         "r_gapsoft", "r_z11", "wtop"]
X = F[FEATS].copy()
X = X.fillna(X.median())
Z = (X - X.mean()) / X.std()

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
best = None
for k in range(4, 8):
    km = KMeans(n_clusters=k, n_init=20, random_state=42).fit(Z)
    s = silhouette_score(Z, km.labels_)
    print(f"k={k} silhouette {s:.3f}", flush=True)
    if best is None or s > best[1]:
        best = (k, s, km)
k, s, km = best
F["cluster"] = km.labels_
print(f"\nCHOSEN k={k} (silhouette {s:.3f})", flush=True)

REF = {"AMD": "🔥", "NVDA": "🔥", "LLY": "🔥", "TSLA": "⚡", "UBER": "⚡/🏦",
       "CVX": "🏦", "JPM": "🏦", "KO": "🏦", "CSCO": "🏦", "IBM": "🏦"}
print("\n===== cluster profiles (means) =====", flush=True)
prof = F.groupby("cluster")[FEATS].mean().round(2)
prof["size"] = F.groupby("cluster").size()
print(prof.to_string(), flush=True)
for cl in sorted(F["cluster"].unique()):
    sub = F[F["cluster"] == cl]
    refs = [f"{t}({REF[t]})" for t in sub.index if t in REF]
    print(f"\n— cluster {cl} · {len(sub)} · refs: {', '.join(refs) if refs else '—'}", flush=True)
    print("  members:", ", ".join(list(sub.index[:25])), "..." if len(sub) > 25 else "", flush=True)

F.round(3).to_csv("h1_sp500_segments.csv")
print("\nsaved -> h1_sp500_segments.csv", flush=True)
print("DONE", flush=True)
