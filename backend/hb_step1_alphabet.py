"""🏦-universe 1D sequence research — STEP 1: let the DATA pick the alphabet.

No prior knowledge assumed (user: "wina codnas nu gaitvaliswineb"). Before mining any
sequence we measure how much information each descriptor layer actually carries on a
SINGLE bar, inside the 🏦 majority universe only (frozen 2021-23 labels):

    L0  TZ code alone            (~25 states)
    L1  TZ + L-line              (~200 states)
    L2  TZ + L + suffix          (~2400 states)

Metrics per layer (all on fwd10d, cells with n>=100):
  - spread    : p90-p10 of cell medians   → how far apart the cells sit
  - IQR       : robust version of the same
  - wSD       : n-weighted SD of cell medians (dispersion that actually carries volume)
  - coverage  : share of bars living in cells with n>=100 (does the layer survive splitting?)
  - stable    : share of qualifying cells whose sign holds in >=4 of 6 years
The layer with real added dispersion AND surviving coverage/stability becomes the
sequence alphabet in step 2. A layer that only adds spread while coverage collapses is
splitting noise, not adding information.

Also emits: the leading/lagging cells at each layer (both directions, thin cells named).
"""
import os, sys
import numpy as np
import pandas as pd
import duckdb
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB

BASE = os.path.dirname(os.path.abspath(__file__))
MIN_N = 100

SEG = pd.read_csv(os.path.join(BASE, "seg_frozen_2123.csv"), index_col=0)["seg_is2123"]
BANK = set(SEG[SEG == "🏦"].index)
print(f"🏦 universe (frozen 2021-23 labels): {len(BANK)} tickers", flush=True)

con = duckdb.connect(ANALYTICS_DB, read_only=True)
D = con.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt, any_value("close") AS cl,
           coalesce(any_value(t_sig),'')  AS t,
           coalesce(any_value(z_sig),'')  AS z,
           coalesce(any_value(l_sig),'')  AS l,
           coalesce(any_value(full_suffix),'') AS sfx
    FROM bars
    WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000
    GROUP BY ticker, date
    ORDER BY ticker, date
""").fetchdf()
con.close()
D = D[D["ticker"].isin(BANK)].reset_index(drop=True)
print(f"frame: {len(D):,} bars · {D['ticker'].nunique()} tickers "
      f"({D['dt'].min()[:10]} → {D['dt'].max()[:10]})", flush=True)

tk = D["ticker"].to_numpy()
cl = D["cl"].to_numpy(float)
f10 = np.full(len(D), np.nan)
f10[:-10] = cl[10:] / cl[:-10] - 1
bad = np.zeros(len(D), dtype=bool); bad[:-10] = tk[10:] != tk[:-10]
f10[bad] = np.nan
f10 *= 100
D["f10"] = np.clip(f10, -60, 60)
D["yr"] = D["dt"].str[:4]
D["tz"] = np.where(D["t"].to_numpy() != "", D["t"].to_numpy(), D["z"].to_numpy())

# keep only real signal bars (a TZ code exists) — that is the alphabet's domain
D = D[D["tz"] != ""].dropna(subset=["f10"]).reset_index(drop=True)
GLOB = D["f10"].median()
print(f"signal bars with fwd10: {len(D):,} · 🏦 baseline med {GLOB:+.3f}", flush=True)

LAYERS = [
    ("L0 TZ",            lambda d: d["tz"]),
    ("L1 TZ+L",          lambda d: d["tz"] + "|" + d["l"].replace("", "·")),
    ("L2 TZ+L+suffix",   lambda d: d["tz"] + "|" + d["l"].replace("", "·") + "|" + d["sfx"].replace("", "·")),
]

summary = []
for lname, fn in LAYERS:
    D["_k"] = fn(D)
    g = D.groupby("_k")["f10"]
    agg = pd.DataFrame({"n": g.size(), "med": g.median()})
    tot_states = len(agg)
    keep = agg[agg["n"] >= MIN_N]
    cov = keep["n"].sum() / len(D) * 100
    med = keep["med"]
    w = keep["n"] / keep["n"].sum()
    wsd = float(np.sqrt(((med - (med * w).sum()) ** 2 * w).sum()))
    spread = float(med.quantile(0.90) - med.quantile(0.10))
    iqr = float(med.quantile(0.75) - med.quantile(0.25))
    # sign stability of qualifying cells
    stab = []
    sub = D[D["_k"].isin(keep.index)]
    for k, s in sub.groupby("_k"):
        ym = s.groupby("yr")["f10"].median()
        if len(ym) >= 5:
            stab.append(max((ym > 0).sum(), (ym < 0).sum()) / len(ym))
    stable = float(np.mean([x >= 4 / 6 for x in stab]) * 100) if stab else float("nan")
    summary.append(dict(layer=lname, states=tot_states, cells=len(keep), cov=cov,
                        spread=spread, iqr=iqr, wsd=wsd, stable=stable))
    print(f"\n===== {lname} · states {tot_states:,} · cells n>={MIN_N}: {len(keep)} "
          f"· coverage {cov:.1f}% =====", flush=True)
    print(f"  spread(p90-p10) {spread:+.2f} · IQR {iqr:+.2f} · weighted-SD {wsd:.3f} "
          f"· sign-stable cells {stable:.0f}%", flush=True)
    top = keep.sort_values("med", ascending=False).head(8)
    bot = keep.sort_values("med").head(6)
    print("  ტოპ:", flush=True)
    for k, r in top.iterrows():
        ym = D[D["_k"] == k].groupby("yr")["f10"].median()
        print(f"    {k:26s} n={int(r['n']):>6d} med{r['med']:>+7.2f} "
              f"Δ{r['med']-GLOB:>+6.2f} yr{int((ym>0).sum())}/{len(ym)} worst{ym.min():>+6.2f}",
              flush=True)
    print("  ჩამხშობი:", flush=True)
    for k, r in bot.iterrows():
        ym = D[D["_k"] == k].groupby("yr")["f10"].median()
        print(f"    {k:26s} n={int(r['n']):>6d} med{r['med']:>+7.2f} "
              f"Δ{r['med']-GLOB:>+6.2f} yr{int((ym>0).sum())}/{len(ym)} worst{ym.min():>+6.2f}",
              flush=True)

S = pd.DataFrame(summary)
print("\n\n===== LAYER COMPARISON =====", flush=True)
print(S.round(2).to_string(index=False), flush=True)
S.to_csv(os.path.join(BASE, "hb_step1_layers.csv"), index=False)
print("\nDONE", flush=True)
