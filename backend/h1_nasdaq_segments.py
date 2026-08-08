"""1H personality segmentation of the NASDAQ universe (user request 2026-08-05).

Same feature set as the SP500 run (h1_sp500_segments.py), now over the nasdaq
universe — where the 🎰 lottery temperament (RGTI/RKLB/ASTS-type) actually lives.
KMeans k chosen by silhouette from 4..8. Reference tickers used for validation.
Known caveat: ticker-reuse contamination (META case) — extreme f35 outliers are
clipped at +/-60 for feature computation so single bad splices don't hijack std.

Output: cluster profiles + members -> h1_nasdaq_segments.csv
"""
import numpy as np
import pandas as pd
import duckdb, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB

c = duckdb.connect(ANALYTICS_DB, read_only=True)
NQ = set(r[0] for r in c.execute("SELECT DISTINCT ticker FROM bars WHERE universe='nasdaq'").fetchall())
c.close()
print(f"nasdaq tickers: {len(NQ)}", flush=True)

DF = pd.read_parquet("data/h1_research_frame.parquet",
                     columns=["ticker", "dt", "close", "rsi_14", "t", "z"])
DF = DF[DF["ticker"].isin(NQ)]
print(f"frame rows {len(DF):,} · tickers {DF['ticker'].nunique()}", flush=True)

TT = {"T2", "T2G", "T1", "T1G"}
rows = []
for tk, a in DF.groupby("ticker", sort=False):
    if len(a) < 3000:
        continue
    a = a.reset_index(drop=True)
    code = np.where(a["t"] != "", a["t"], a["z"])
    isT = (a["t"] != "").to_numpy()
    f35 = ((a["close"].shift(-35) / a["close"] - 1) * 100).clip(-60, 60)
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

for col in ["ovs", "mid", "cross", "green3", "panic3", "gapsoft", "z11"]:
    F["r_" + col] = F[col] - F["base"]
FEATS = ["base", "std", "r_ovs", "r_mid", "r_cross", "r_green3", "r_panic3",
         "r_gapsoft", "r_z11", "wtop"]
X = F[FEATS].fillna(F[FEATS].median())
Z = (X - X.mean()) / X.std()

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
best = None
for k in range(4, 9):
    km = KMeans(n_clusters=k, n_init=20, random_state=42).fit(Z)
    s = silhouette_score(Z, km.labels_)
    print(f"k={k} silhouette {s:.3f}", flush=True)
    if best is None or s > best[1]:
        best = (k, s, km)
k, s, km = best
F["cluster"] = km.labels_
print(f"\nCHOSEN k={k} (silhouette {s:.3f})", flush=True)

REF = {"AMD": "🔥", "NVDA": "🔥", "TSLA": "⚡", "CSCO": "🏦", "RGTI": "🎰",
       "RKLB": "🎰", "ASTS": "🎰", "OKLO": "🔥?", "KO": "🏦", "AAPL": "?"}
print("\n===== cluster profiles (means) =====", flush=True)
prof = F.groupby("cluster")[FEATS].mean().round(2)
prof["size"] = F.groupby("cluster").size()
print(prof.to_string(), flush=True)
for cl in sorted(F["cluster"].unique()):
    sub = F[F["cluster"] == cl]
    refs = [f"{t}({REF[t]})" for t in sub.index if t in REF]
    print(f"\n— cluster {cl} · {len(sub)} · refs: {', '.join(refs) if refs else '—'}", flush=True)
    print("  members:", ", ".join(list(sub.index[:22])), "..." if len(sub) > 22 else "", flush=True)

F.round(3).to_csv("h1_nasdaq_segments.csv")
print("\nsaved -> h1_nasdaq_segments.csv", flush=True)
print("DONE", flush=True)
