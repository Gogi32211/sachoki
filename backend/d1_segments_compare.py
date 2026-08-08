"""1D personality segmentation + cross-TF comparison (user request 2026-08-06).

Same feature set as the 1H runs, computed on DAILY bars (dedup by ticker-date — the
multi-universe x3 duplication trap), horizon fwd10d (the book's standard), f10 clipped
+/-60 (ticker-reuse splices). Universes clustered separately like 1H.

Then the question the user actually asked: do tickers STAY in their groups across
timeframes? Clusters on both TFs are auto-NAMED with one rule set applied to cluster
means (lottery: wtop<-5 or deep-negative base w/ max std; panic: max r_ovs>=0.4;
chase: max r_cross>=0.15 with r_ovs<0; majority: the rest/largest), and a per-ticker
1H-segment vs 1D-segment migration matrix is printed per universe.
"""
import numpy as np
import pandas as pd
import duckdb, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB

BASE = os.path.dirname(os.path.abspath(__file__))
TT = {"T2", "T2G", "T1", "T1G"}

c = duckdb.connect(ANALYTICS_DB, read_only=True)
UNI = {u: set(r[0] for r in c.execute(
    "SELECT DISTINCT ticker FROM bars WHERE universe=?", [u]).fetchall())
    for u in ["sp500", "nasdaq", "russell2k"]}
D = c.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt, any_value("close") AS close,
           any_value(rsi_14) AS rsi_14,
           coalesce(any_value(t_sig),'') AS t, coalesce(any_value(z_sig),'') AS z
    FROM bars
    WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000
    GROUP BY ticker, date
    ORDER BY ticker, date
""").fetchdf()
c.close()
print(f"daily dedup frame: {len(D):,} rows · {D['ticker'].nunique():,} tickers", flush=True)

rows = []
for tk, a in D.groupby("ticker", sort=False):
    if len(a) < 750:
        continue
    a = a.reset_index(drop=True)
    code = np.where(a["t"] != "", a["t"], a["z"])
    isT = (a["t"] != "").to_numpy()
    f10 = ((a["close"].shift(-10) / a["close"] - 1) * 100).clip(-60, 60)
    yr = a["dt"].str[:4]
    r = a["rsi_14"]
    base = f10.median(); std = f10.std()
    ovs = f10[r < 35].median()
    mid = f10[(r >= 35) & (r < 50)].median()
    cross = f10[(r >= 50) & (r.shift(1).rolling(3).min() < 50)].median()
    c1 = np.roll(code, 1); c2 = np.roll(code, 2)
    t1 = np.roll(isT, 1); t2 = np.roll(isT, 2)
    t1[0] = t2[:2] = False
    green = f10[isT & t1 & t2]
    green3 = green.median() if len(green) >= 30 else np.nan
    panic = f10[(~isT) & ~t1 & ~t2 & (r < 40).to_numpy()]
    panic3 = panic.median() if len(panic) >= 30 else np.nan
    gp = (np.isin(c2, list(TT)) & np.isin(c1, list(TT))
          & (np.char.find(c2.astype(str), "G") + np.char.find(c1.astype(str), "G") >= -1)
          & np.isin(code, ["Z3", "Z4"]))
    gapsoft = f10[gp].median() if gp.sum() >= 15 else np.nan
    z11 = f10[code == "Z11"]
    z11m = z11.median() if len(z11) >= 10 else np.nan
    seq = pd.Series(c2).astype(str) + ">" + pd.Series(c1).astype(str) + ">" + pd.Series(code)
    d = pd.DataFrame({"s": seq, "f": f10, "y": yr}).dropna()
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
for col in ["ovs", "mid", "cross", "green3", "panic3", "gapsoft", "z11"]:
    F["r_" + col] = F[col] - F["base"]
FEATS = ["base", "std", "r_ovs", "r_mid", "r_cross", "r_green3", "r_panic3",
         "r_gapsoft", "r_z11", "wtop"]
print(f"1D feature table: {len(F)} tickers", flush=True)

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


def name_clusters(prof):
    """one rule set, applied to cluster-mean profiles -> segment names"""
    names = {}
    left = list(prof.index)
    # lottery: deepest wtop if < -5, else most negative base with top-2 std
    lot = prof.loc[left, "wtop"].idxmin()
    if prof.loc[lot, "wtop"] < -5 or (prof.loc[lot, "base"] < -0.5
                                      and prof.loc[lot, "std"] >= prof["std"].median()):
        names[lot] = "🎰lottery"; left.remove(lot)
    if left:
        pan = prof.loc[left, "r_ovs"].idxmax()
        if prof.loc[pan, "r_ovs"] >= 0.4:
            names[pan] = "⚡panic"; left.remove(pan)
    if left:
        ch = prof.loc[left, "r_cross"].idxmax()
        if prof.loc[ch, "r_cross"] >= 0.15 and prof.loc[ch, "r_ovs"] < 0.1:
            names[ch] = "🎲chase"; left.remove(ch)
    for cl in left:
        names[cl] = "🏦majority"
    return names


def run_universe(uni, F, feats):
    sub = F[F.index.isin(UNI[uni])]
    X = sub[feats].fillna(sub[feats].median())
    Z = (X - X.mean()) / X.std()
    best = None
    for k in range(4, 8):
        km = KMeans(n_clusters=k, n_init=20, random_state=42).fit(Z)
        s = silhouette_score(Z, km.labels_)
        if best is None or s > best[1]:
            best = (k, s, km)
    k, s, km = best
    lab = pd.Series(km.labels_, index=sub.index)
    prof = sub.groupby(lab)[feats].mean()
    names = name_clusters(prof)
    seg = lab.map(names)
    print(f"\n===== 1D {uni} — k={k} silhouette {s:.3f} =====", flush=True)
    p2 = prof.round(2); p2["size"] = lab.value_counts()
    p2["name"] = [names[i] for i in p2.index]
    print(p2.to_string(), flush=True)
    return seg


SEG_1D = {}
for u in ["sp500", "nasdaq", "russell2k"]:
    SEG_1D[u] = run_universe(u, F, FEATS)
    SEG_1D[u].rename("seg1d").to_frame().join(F[FEATS]).round(3).to_csv(
        os.path.join(BASE, f"d1_{u}_segments.csv"))

# ── cross-TF migration ───────────────────────────────────────────────────────────
H1FILES = {"sp500": "h1_sp500_segments.csv", "nasdaq": "h1_nasdaq_segments.csv",
           "russell2k": "h1_russell_segments.csv"}
for u, fn in H1FILES.items():
    p = os.path.join(BASE, fn)
    if not os.path.exists(p):
        continue
    H = pd.read_csv(p, index_col=0)
    prof = H.groupby("cluster")[[c for c in FEATS if c in H.columns]].mean()
    names = name_clusters(prof)
    H["seg1h"] = H["cluster"].map(names)
    both = H[["seg1h"]].join(SEG_1D[u].rename("seg1d"), how="inner").dropna()
    agree = (both["seg1h"] == both["seg1d"]).mean() * 100
    print(f"\n===== {u}: 1H vs 1D migration (common tickers {len(both)}) — agreement {agree:.1f}% =====",
          flush=True)
    print(pd.crosstab(both["seg1h"], both["seg1d"]).to_string(), flush=True)
    movers = both[both["seg1h"] != both["seg1d"]]
    refs = [t for t in ["AMD","NVDA","TSLA","LLY","CVX","JPM","KO","CSCO","IBM","UBER",
                        "RGTI","RKLB","ASTS","OKLO","AAPL"] if t in both.index]
    if refs:
        print("  refs:", ", ".join(f"{t} {both.loc[t,'seg1h']}→{both.loc[t,'seg1d']}"
                                   for t in refs), flush=True)
print("\nDONE", flush=True)
