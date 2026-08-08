"""Segment-gate validation with the honest standard (user: "meta gawminde da mere
gaakete danarCeni", 2026-08-06). META is now clean in all DBs.

CLAIM TO TEST (from edge_by_segment.csv, same-window): reversal edges pay on 🏦 tickers
and lose on ⚡/🎰; the momentum trio (CAP/🥪/🪨) prefers 🎲.

CIRCULARITY CONTROL (the gate that decides): segment labels are recomputed on
2021-01-01..2023-12-31 ONLY (frozen in-sample), then edge trades are evaluated on
date_in >= 2024-01-01 ONLY (frozen OOS). If the 🏦-vs-rest split holds per-year in
2024/2025/2026 on labels the trades never saw, the gate is real.
"""
import os, sys
import numpy as np
import pandas as pd
import duckdb
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB
import edge_replay as er

BASE = os.path.dirname(os.path.abspath(__file__))
TT = {"T2", "T2G", "T1", "T1G"}

# ── 1. frozen labels: daily features on 2021-2023 only ──────────────────────────
c = duckdb.connect(ANALYTICS_DB, read_only=True)
D = c.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt, any_value("close") AS close,
           any_value(rsi_14) AS rsi_14,
           coalesce(any_value(t_sig),'') AS t, coalesce(any_value(z_sig),'') AS z
    FROM bars
    WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000
      AND date < '2024-01-01'
    GROUP BY ticker, date ORDER BY ticker, date
""").fetchdf()
c.close()
print(f"IS frame (2021-23): {len(D):,} rows", flush=True)

rows = []
for tk, a in D.groupby("ticker", sort=False):
    if len(a) < 400:
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
    green3 = green.median() if len(green) >= 20 else np.nan
    panic = f10[(~isT) & ~t1 & ~t2 & (r < 40).to_numpy()]
    panic3 = panic.median() if len(panic) >= 20 else np.nan
    gp = (np.isin(c2, list(TT)) & np.isin(c1, list(TT))
          & (np.char.find(c2.astype(str), "G") + np.char.find(c1.astype(str), "G") >= -1)
          & np.isin(code, ["Z3", "Z4"]))
    gapsoft = f10[gp].median() if gp.sum() >= 10 else np.nan
    z11 = f10[code == "Z11"]
    z11m = z11.median() if len(z11) >= 8 else np.nan
    seq = pd.Series(c2).astype(str) + ">" + pd.Series(c1).astype(str) + ">" + pd.Series(code)
    d = pd.DataFrame({"s": seq, "f": f10, "y": yr}).dropna()
    g = d.groupby("s")["f"].agg(["size", "median"])
    top = g[g["size"] >= 10].sort_values("median", ascending=False).head(5)
    wt = []
    for s in top.index:
        ym = d[d["s"] == s].groupby("y")["f"].median()
        if len(ym) >= 2:
            wt.append(ym.min())
    wtop = float(np.median(wt)) if wt else np.nan
    rows.append(dict(ticker=tk, base=base, std=std, ovs=ovs, mid=mid, cross=cross,
                     green3=green3, panic3=panic3, gapsoft=gapsoft, z11=z11m, wtop=wtop))

F = pd.DataFrame(rows).set_index("ticker")
for col in ["ovs", "mid", "cross", "green3", "panic3", "gapsoft", "z11"]:
    F["r_" + col] = F[col] - F["base"]
FEATS = ["base", "std", "r_ovs", "r_mid", "r_cross", "r_green3", "r_panic3",
         "r_gapsoft", "r_z11", "wtop"]
print(f"IS features: {len(F)} tickers", flush=True)

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

X = F[FEATS].fillna(F[FEATS].median()).fillna(0.0)
Z = ((X - X.mean()) / X.std().replace(0, 1)).fillna(0.0)
best = None
for k in range(4, 8):
    km = KMeans(n_clusters=k, n_init=20, random_state=42).fit(Z)
    s = silhouette_score(Z, km.labels_)
    if best is None or s > best[1]:
        best = (k, s, km)
k, s, km = best
lab = pd.Series(km.labels_, index=F.index)
prof = F.groupby(lab)[FEATS].mean()

names = {}
left = list(prof.index)
lot = prof.loc[left, "wtop"].idxmin()
if prof.loc[lot, "wtop"] < -5 or (prof.loc[lot, "base"] < -0.5
                                  and prof.loc[lot, "std"] >= prof["std"].median()):
    names[lot] = "🎰"; left.remove(lot)
if left:
    pan = prof.loc[left, "r_ovs"].idxmax()
    if prof.loc[pan, "r_ovs"] >= 0.4:
        names[pan] = "⚡"; left.remove(pan)
if left:
    ch = prof.loc[left, "r_cross"].idxmax()
    if prof.loc[ch, "r_cross"] >= 0.15 and prof.loc[ch, "r_ovs"] < 0.1:
        names[ch] = "🎲"; left.remove(ch)
for cl in left:
    names[cl] = "🏦"
SEG = lab.map(names)
print(f"IS labels (k={k}, sil {s:.3f}): {SEG.value_counts().to_dict()}", flush=True)
SEG.rename("seg_is2123").to_frame().to_csv(os.path.join(BASE, "seg_frozen_2123.csv"))

# ── 2. OOS edge trades (2024+) sliced by the frozen labels ──────────────────────
grp, as_of = er._frame(60, 3_000_000)
print(f"replay frame as_of {as_of}", flush=True)

REV = [("QZC", "E_qzcapit"), ("WSH", "E_washout"), ("D+L1", "E_dl1"),
       ("G3", "E_g3"), ("ZRT", "E_zoneretest"), ("ATM", "E_atomic"),
       ("SPR", "E_spring"), ("G3A", "E_g3abs")]
MOM = [("CAP", "E_t1capbounce"), ("SAND", "E_t2gsand_rs"), ("GNB", "E_t1gnb_rs")]

def cols_ok(pairs):
    g0 = next(iter(grp.values()))
    out = []
    for n, col in pairs:
        if col in g0:
            out.append((n, col))
        else:
            print(f"  !! column missing: {n} {col}", flush=True)
    return out

REV = cols_ok(REV); MOM = cols_ok(MOM)

def fam_trades(pairs):
    frames = []
    for n, col in pairs:
        tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60)
        tr["edge"] = n
        frames.append(tr)
    T = pd.concat(frames, ignore_index=True)
    T["date_in"] = pd.to_datetime(T["date_in"]).astype(str)
    T = T[T["date_in"] >= "2024-01-01"].copy()
    T["seg"] = T["ticker"].map(SEG).fillna("∅")
    T["y"] = T["date_in"].str[:4]
    return T


def report(T, title):
    print(f"\n===== {title} — OOS 2024-26, frozen 2021-23 labels =====", flush=True)
    print(f"{'seg':4s} {'n':>6s} {'med':>7s} {'win':>5s} "
          f"{'2024':>7s}{'2025':>7s}{'2026':>7s}  pos", flush=True)
    for sgn in ["🏦", "⚡", "🎲", "🎰", "∅"]:
        sub = T[T["seg"] == sgn]
        if len(sub) < 30:
            print(f"{sgn:4s} n={len(sub)} — thin", flush=True)
            continue
        ym = sub.groupby("y")["ret"].median() * 100
        ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in (2024, 2025, 2026))
        print(f"{sgn:4s} {len(sub):>6d} {sub['ret'].median()*100:>+7.2f} "
              f"{(sub['ret']>0).mean()*100:>5.1f} {ys}  {int((ym>0).sum())}/{len(ym)}", flush=True)
    # per-edge 🏦-vs-rest delta
    print("  — per-edge Δ(🏦 − rest), OOS —", flush=True)
    for e, sub in T.groupby("edge"):
        a = sub[sub["seg"] == "🏦"]["ret"].median()
        b = sub[sub["seg"].isin(["⚡", "🎲", "🎰"])]["ret"].median()
        if pd.notna(a) and pd.notna(b):
            print(f"    {e:6s} 🏦 {a*100:+.2f} vs rest {b*100:+.2f}  Δ {100*(a-b):+.2f}", flush=True)


T = fam_trades(REV)
report(T, "REVERSAL family (pooled: " + ",".join(n for n, _ in REV) + ")")
T2 = fam_trades(MOM)
report(T2, "MOMENTUM trio (CAP/SAND/GNB) — does 🎲 lead?")
if len(T2):
    sub = T2[T2["seg"] == "🎲"]
    print(f"\n  MOM on 🎲: n={len(sub)} med {sub['ret'].median()*100 if len(sub) else float('nan'):+.2f}", flush=True)

print("\nDONE", flush=True)
