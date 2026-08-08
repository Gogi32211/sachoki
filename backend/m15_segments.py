"""15m personality segmentation — the last TF (user request 2026-08-06).

Memory plan for the 33GB DB: DuckDB COPY streams the floored frame to parquet
(ORDER BY ticker,date so row-groups are ticker-clustered), then features are computed
in ticker batches with parquet filter pushdown. Horizon 130 bars ≈ one trading week
(the 1H run's f35 equivalent). Floor $120k/bar ≈ $3M/day. min_bars 10000 (~1.5yr+).

Ends with the FIVE-TF stability table (15m·1H·4H·1D·1W) per universe.
"""
import numpy as np
import pandas as pd
import duckdb, os, sys, gc
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB, db_path

BASE = os.path.dirname(os.path.abspath(__file__))
TT = {"T2", "T2G", "T1", "T1G"}
PQ = os.path.join(BASE, "data", "m15_seg_frame.parquet")
H = 130

c = duckdb.connect(ANALYTICS_DB, read_only=True)
UNI = {u: set(r[0] for r in c.execute(
    "SELECT DISTINCT ticker FROM bars WHERE universe=?", [u]).fetchall())
    for u in ["sp500", "nasdaq", "russell2k"]}
c.close()

if not os.path.exists(PQ):
    con = duckdb.connect(db_path("studio_15m.duckdb"), read_only=True)
    con.execute("PRAGMA memory_limit='6GB'")
    con.execute(f"""
        COPY (SELECT ticker, CAST(date AS VARCHAR) dt, close, rsi_14,
                     coalesce(t_sig,'') t, coalesce(z_sig,'') z
              FROM bars WHERE close >= 5 AND close*volume >= 120000
              ORDER BY ticker, date)
        TO '{PQ}' (FORMAT PARQUET, ROW_GROUP_SIZE 500000)
    """)
    con.close()
    print("parquet written", flush=True)
import pyarrow.parquet as pq_
tks = pq_.read_table(PQ, columns=["ticker"]).column("ticker").to_pylist()
tks = pd.unique(pd.Series(tks))
print(f"15m frame tickers: {len(tks):,}", flush=True)

rows = []
B = 200
for bi in range(0, len(tks), B):
    batch = list(tks[bi:bi + B])
    D = pd.read_parquet(PQ, filters=[("ticker", "in", batch)])
    for tk, a in D.groupby("ticker", sort=False):
        if len(a) < 10000:
            continue
        a = a.reset_index(drop=True)
        code = np.where(a["t"] != "", a["t"], a["z"])
        isT = (a["t"] != "").to_numpy()
        f = ((a["close"].shift(-H) / a["close"] - 1) * 100).clip(-60, 60)
        yr = a["dt"].str[:4]
        r = a["rsi_14"]
        base = f.median(); std = f.std()
        ovs = f[r < 35].median()
        mid = f[(r >= 35) & (r < 50)].median()
        cross = f[(r >= 50) & (r.shift(1).rolling(3).min() < 50)].median()
        c1 = np.roll(code, 1); c2 = np.roll(code, 2)
        t1 = np.roll(isT, 1); t2 = np.roll(isT, 2)
        t1[0] = t2[:2] = False
        green = f[isT & t1 & t2]
        green3 = green.median() if len(green) >= 30 else np.nan
        panic = f[(~isT) & ~t1 & ~t2 & (r < 40).to_numpy()]
        panic3 = panic.median() if len(panic) >= 30 else np.nan
        gp = (np.isin(c2, list(TT)) & np.isin(c1, list(TT))
              & (np.char.find(c2.astype(str), "G") + np.char.find(c1.astype(str), "G") >= -1)
              & np.isin(code, ["Z3", "Z4"]))
        gapsoft = f[gp].median() if gp.sum() >= 15 else np.nan
        z11 = f[code == "Z11"]
        z11m = z11.median() if len(z11) >= 10 else np.nan
        seq = pd.Series(c2).astype(str) + ">" + pd.Series(c1).astype(str) + ">" + pd.Series(code)
        d = pd.DataFrame({"s": seq, "f": f, "y": yr}).dropna()
        g = d.groupby("s")["f"].agg(["size", "median"])
        top = g[g["size"] >= 12].sort_values("median", ascending=False).head(5)
        wt = []
        for s in top.index:
            ym = d[d["s"] == s].groupby("y")["f"].median()
            if len(ym) >= 3:
                wt.append(ym.min())
        wtop = float(np.median(wt)) if wt else np.nan
        rows.append(dict(ticker=tk, base=base, std=std, ovs=ovs, mid=mid, cross=cross,
                         green3=green3, panic3=panic3, gapsoft=gapsoft, z11=z11m, wtop=wtop))
    del D
    gc.collect()
    if (bi // B) % 5 == 0:
        print(f"  batch {bi//B + 1}/{(len(tks)+B-1)//B} · features {len(rows)}", flush=True)

F = pd.DataFrame(rows).set_index("ticker")
for col in ["ovs", "mid", "cross", "green3", "panic3", "gapsoft", "z11"]:
    F["r_" + col] = F[col] - F["base"]
FEATS = ["base", "std", "r_ovs", "r_mid", "r_cross", "r_green3", "r_panic3",
         "r_gapsoft", "r_z11", "wtop"]
print(f"15m feature table: {len(F)}", flush=True)

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


def name_clusters(prof):
    names = {}
    left = list(prof.index)
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


SEG15 = {}
for u in ["sp500", "nasdaq", "russell2k"]:
    sub = F[F.index.isin(UNI[u])]
    if len(sub) < 100:
        print(f"15m {u}: too few ({len(sub)})", flush=True); continue
    X = sub[FEATS].fillna(sub[FEATS].median()).fillna(0.0)
    Z = ((X - X.mean()) / X.std().replace(0, 1)).fillna(0.0)
    best = None
    for k in range(4, 8):
        km = KMeans(n_clusters=k, n_init=20, random_state=42).fit(Z)
        s = silhouette_score(Z, km.labels_)
        if best is None or s > best[1]:
            best = (k, s, km)
    k, s, km = best
    lab = pd.Series(km.labels_, index=sub.index)
    prof = sub.groupby(lab)[FEATS].mean()
    names = name_clusters(prof)
    SEG15[u] = lab.map(names)
    print(f"\n===== 15m {u} — k={k} silhouette {s:.3f} =====", flush=True)
    p2 = prof.round(2); p2["size"] = lab.value_counts(); p2["name"] = [names[i] for i in p2.index]
    print(p2.to_string(), flush=True)
    SEG15[u].rename("seg").to_frame().join(F[FEATS]).round(3).to_csv(
        os.path.join(BASE, f"m15_{u}_segments.csv"))


def load_named(fn):
    p = os.path.join(BASE, fn)
    if not os.path.exists(p):
        return None
    d = pd.read_csv(p, index_col=0)
    if "seg" in d.columns:
        return d["seg"]
    if "seg1d" in d.columns:
        return d["seg1d"]
    prof = d.groupby("cluster")[[c for c in FEATS if c in d.columns]].mean()
    return d["cluster"].map(name_clusters(prof))


print("\n\n===== 5-TF MEMBERSHIP STABILITY =====", flush=True)
for u in ["sp500", "nasdaq", "russell2k"]:
    cols = {}
    if u in SEG15: cols["15m"] = SEG15[u]
    for tfn, fn in [("1H", f"h1_{u}_segments.csv"), ("4H", f"4h_{u}_segments.csv"),
                    ("1D", f"d1_{u}_segments.csv"), ("1W", f"1w_{u}_segments.csv")]:
        s = load_named(fn)
        if s is not None:
            cols[tfn] = s
    M = pd.DataFrame(cols).dropna()
    if M.empty:
        continue
    same_all = (M.nunique(axis=1) == 1).mean() * 100
    print(f"\n— {u}: common {len(M)} · SAME on ALL {len(M.columns)} TFs: {same_all:.1f}%", flush=True)
    tfs = list(M.columns)
    for i in range(len(tfs)):
        for j in range(i + 1, len(tfs)):
            print(f"   {tfs[i]} vs {tfs[j]}: {(M[tfs[i]] == M[tfs[j]]).mean()*100:.1f}%", flush=True)
    refs = [t for t in ["AMD", "NVDA", "TSLA", "CVX", "JPM", "KO", "CSCO", "IBM",
                        "UBER", "RGTI", "RKLB", "ASTS", "AAPL", "LLY"] if t in M.index]
    for t in refs:
        print(f"   {t}: " + " · ".join(f"{tf}={M.loc[t, tf]}" for tf in tfs), flush=True)
print("\nDONE", flush=True)
