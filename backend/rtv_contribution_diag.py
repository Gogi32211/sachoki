"""Pre-registered DIAGNOSTICS for RTV_CONTRIBUTION_V1. These do not decide anything (k = 1 sits on
the paired primary); they explain it. Same sealed sample, same sacred outcomes."""
from __future__ import annotations
import os, sys
import numpy as np, pandas as pd, duckdb
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")

R = "/Users/sachoki/MASSIVE_DATA/SCORE_AUDIT_V1/runs/"
DB = "/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb"
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
MIN_DAY, MIN_CELL, TOP_PCT = 20, 5, 0.8

X = pd.read_parquet(os.path.join(R, "SA_20260907T162603Z", "X.parquet"))
T = pd.read_parquet(os.path.join(R, "OUT_20260907T163751Z_AMENDMENT_1", "direct_trades.parquet"))
con = duckdb.connect(DB, read_only=True)
B = con.execute("""WITH r AS (SELECT ticker, date, universe, rtv, rocket, sig_3g, hilo_buy, atr_brk,
                                     sig_cd, sig_ca, sig_cw, sig_seq_bcont, combo_sig,
                                     row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
                              FROM bars)
                   SELECT ticker, date AS session, rtv, rocket, sig_3g, hilo_buy, atr_brk, sig_cd,
                          sig_ca, sig_cw, sig_seq_bcont, combo_sig FROM r WHERE rn = 1""").fetchdf()
for d in (B, X):
    d["session"] = pd.to_datetime(d["session"]).dt.date.astype(str)
X = X.merge(B, on=["ticker", "session"], how="left")
b = lambda c: (pd.to_numeric(X[c], errors="coerce").fillna(0) != 0).to_numpy()
tok = X["combo_sig"].fillna("")
base = (np.where(b("rocket"), 12, np.where(tok.str.contains("BUY", regex=False), 8, 0))
        + np.where(b("sig_3g"), 4, 0) + np.where(b("hilo_buy"), 4, 0) + np.where(b("atr_brk"), 2, 0)
        + np.where(b("sig_cd"), 5, np.where(b("sig_ca"), 3, np.where(b("sig_cw"), 2, 0)))
        + np.where(b("sig_seq_bcont"), 3, 0))
X["delta"] = np.minimum(base + 3, 14) - np.minimum(base, 14)
X["rtv_b"] = b("rtv")
X["score_with"] = X["turbo_score"].astype(float)
X["score_wo"] = X["score_with"] - np.where(X["rtv_b"], X["delta"], 0.0)
for c in ("score_with", "score_wo"):
    X[f"pct_{c}"] = X[c].groupby([X["session"], X["rsi_band"], X["px_band"]]).rank(method="average", pct=True).to_numpy(float)

df = X.merge(T, on=["ticker", "session"], how="inner")
df = df[df["ret"].notna()].reset_index(drop=True)
dn = df.groupby("date_in")["ret"].transform("size").to_numpy()
dm = df.groupby("date_in")["ret"].transform("median").to_numpy()
df["edge_raw"] = np.where(dn >= MIN_DAY, df["ret"].to_numpy(float) - dm, np.nan)
g = df.groupby(["date_in", "rsi_band", "px_band"])["ret"]; n1, m1 = g.transform("size").to_numpy(), g.transform("median").to_numpy()
g2 = df.groupby(["date_in", "rsi_band"])["ret"]; n2, m2 = g2.transform("size").to_numpy(), g2.transform("median").to_numpy()
r = df["ret"].to_numpy(float)
df["edge"] = np.where(n1 >= MIN_CELL, r - m1, np.where(n2 >= MIN_CELL, r - m2, np.nan))
W = lambda d, w: d[(d["date_in"] >= OOS[w][0]) & (d["date_in"] <= OOS[w][1])]

def daystat(d, lab):
    s = d.dropna(subset=["edge"]).groupby("date_in")["edge"].median()
    return (f"  {lab:34s} rows {len(d):>6,}  days {len(s):>4,}  "
            f"median {s.median():+.3f}  days>0 {100*(s>0).mean():5.1f} %" if len(s) else f"  {lab:34s} —")

print("═" * 84); print("D1 · RTV ALONE vs the rest of the eligible universe  (day-clustered)"); print("═" * 84)
for w in ("MINE", "VERIFY"):
    d = W(df, w); print(f" {w}")
    print(daystat(d[d["rtv_b"]], "rtv = 1"))
    print(daystat(d[~d["rtv_b"]], "rtv = 0 (rest of universe)"))

print("\n" + "═" * 84); print("D2 · RTV x RS-intact"); print("═" * 84)
for w in ("MINE", "VERIFY"):
    d = W(df, w); print(f" {w}")
    for rs in (True, False):
        m = d["rs_intact"].astype(bool) == rs
        print(daystat(d[m & d["rtv_b"]], f"rtv=1 · rs_intact={rs}"))

print("\n" + "═" * 84); print("D3 · liquidity strata (dollar volume terciles within window)"); print("═" * 84)
df["dv"] = df["close"].astype(float) * df["volume"].astype(float)
for w in ("MINE", "VERIFY"):
    d = W(df, w).copy(); q = d["dv"].quantile([1/3, 2/3]).to_numpy(); print(f" {w}")
    for lab, m in (("low dv", d.dv <= q[0]), ("mid dv", (d.dv > q[0]) & (d.dv <= q[1])), ("high dv", d.dv > q[1])):
        print(daystat(d[m & d["rtv_b"]], f"rtv=1 · {lab}"))

print("\n" + "═" * 84); print("D4 · ticker concentration among treated rows"); print("═" * 84)
t = df[df["rtv_b"]]
vc = t["ticker"].value_counts()
print(f"  treated rows {len(t):,} across {t['ticker'].nunique():,} tickers")
print(f"  top ticker {vc.index[0]} = {vc.iloc[0]} rows ({100*vc.iloc[0]/len(t):.2f} %) · "
      f"top 10 = {100*vc.head(10).sum()/len(t):.1f} % · top 50 = {100*vc.head(50).sum()/len(t):.1f} %")

print("\n" + "═" * 84); print("D5 · is the paired difference driven by a few days?"); print("═" * 84)
sw  = df[(df["pct_score_with"] >= TOP_PCT) & np.isfinite(df["edge"])].groupby("date_in")["edge"].median()
swo = df[(df["pct_score_wo"]   >= TOP_PCT) & np.isfinite(df["edge"])].groupby("date_in")["edge"].median()
j = pd.concat([sw.rename("a"), swo.rename("b")], axis=1).dropna()
for w in ("MINE", "VERIFY"):
    k = j[(j.index >= OOS[w][0]) & (j.index <= OOS[w][1])]
    d = (k["b"] - k["a"]); nz = d[d != 0].sort_values()
    tot = d.sum()
    top3 = pd.concat([nz.head(3), nz.tail(3)])
    print(f" {w}: total Σdiff {tot:+.3f} over {len(d)} days; "
          f"3 largest +/- contribute {top3.abs().sum()/d.abs().sum()*100:.1f} % of |Σ|")
    print(f"    drop the 3 biggest movers -> mean {d.drop(top3.index).mean():+.4f} "
          f"(full mean {d.mean():+.4f})")
