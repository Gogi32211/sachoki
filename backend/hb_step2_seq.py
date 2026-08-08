"""🏦-universe 1D sequence research — STEP 2: sequence sweep on the DATA-CHOSEN alphabet.

Step-1 permutation control settled the alphabet (obs vs shuffled dispersion):
    L0 TZ         spread ratio 4.02 · real wSD 0.121 · sign-stable 96%
    L1 TZ+L       spread ratio 1.83 · real wSD 0.141 · sign-stable 80%
    L2 TZ+L+sfx   spread ratio 1.30 · real wSD 0.169 · sign-stable 74%
Every layer adds REAL dispersion (variance above shuffle rises 0.121→0.141→0.169) but
with falling efficiency and falling sign-stability. TZ+L is therefore the sequence
alphabet (94 states — 2/3-bar combinations stay populated); the SUFFIX layer, which
still carries ~20% more real dispersion but cannot survive being multiplied into a
sequence, is applied as a POST-FILTER on the finalists instead of being dropped.

Sweep: 2-bar and 3-bar sequences of TZ+L states, n>=100, fwd10d, both directions.
Gates: per-year (>=4/6 + worst >=-2) AND must hold on 2024-26 alone (frozen-OOS vs the
2021-23 segment labels). Suffix post-filter on survivors. No path-sim yet, no builds.
"""
import os, sys, gc
import numpy as np
import pandas as pd
import duckdb
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB

BASE = os.path.dirname(os.path.abspath(__file__))
MIN_N = 100

SEG = pd.read_csv(os.path.join(BASE, "seg_frozen_2123.csv"), index_col=0)["seg_is2123"]
BANK = set(SEG[SEG == "🏦"].index)

con = duckdb.connect(ANALYTICS_DB, read_only=True)
D = con.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt, any_value("close") AS cl,
           coalesce(any_value(t_sig),'')  AS t, coalesce(any_value(z_sig),'')  AS z,
           coalesce(any_value(l_sig),'')  AS l, coalesce(any_value(full_suffix),'') AS sfx
    FROM bars
    WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000
    GROUP BY ticker, date ORDER BY ticker, date
""").fetchdf()
con.close()
D = D[D["ticker"].isin(BANK)].reset_index(drop=True)

tk = D["ticker"].to_numpy(); cl = D["cl"].to_numpy(float)
f = np.full(len(D), np.nan); f[:-10] = cl[10:] / cl[:-10] - 1
bad = np.zeros(len(D), bool); bad[:-10] = tk[10:] != tk[:-10]; f[bad] = np.nan
D["f10"] = np.clip(f * 100, -60, 60)
D["yr"] = D["dt"].str[:4]
tz = np.where(D["t"].to_numpy() != "", D["t"].to_numpy(), D["z"].to_numpy())
lsig = np.where(D["l"].to_numpy() != "", D["l"].to_numpy(), "·")
state = np.where(tz != "", np.char.add(np.char.add(tz.astype(str), "|"), lsig.astype(str)), "")
D["st"] = state
print(f"frame {len(D):,} bars · {D['ticker'].nunique()} 🏦 tickers", flush=True)

vocab = sorted(set(state[state != ""]))
V = {s: i + 1 for i, s in enumerate(vocab)}          # 0 reserved for "no state"
IV = {i: s for s, i in V.items()}
code = np.array([V.get(s, 0) for s in state], dtype=np.int32)
NV = len(vocab) + 1
print(f"alphabet: {len(vocab)} TZ+L states", flush=True)


def shift_back(a, k):
    out = np.zeros(len(a), dtype=np.int64)
    out[k:] = a[:-k]
    bad = np.zeros(len(a), dtype=bool)
    for j in range(1, k + 1):
        b = np.zeros(len(a), dtype=bool); b[j:] = tk[j:] != tk[:-j]
        bad |= b
    out[bad] = 0
    return out


c0 = code.astype(np.int64); c1 = shift_back(c0, 1); c2 = shift_back(c0, 2)
ok = (~np.isnan(D["f10"].to_numpy())) & (c0 > 0)
F = D["f10"].to_numpy(); Y = D["yr"].to_numpy(); SF = D["sfx"].fillna("·").to_numpy()
GLOB = np.nanmedian(F[ok])
print(f"🏦 baseline med {GLOB:+.3f}\n", flush=True)


def sweep(keys, valid, namer, label, top=14, bot=8):
    """keys = integer sequence codes; naming happens only on qualifying cells"""
    m = ok & valid
    d = pd.DataFrame({"k": keys[m], "f": F[m], "y": Y[m]})
    g = d.groupby("k")["f"]
    agg = pd.DataFrame({"n": g.size(), "med": g.median()})
    agg = agg[agg["n"] >= MIN_N]
    rows = []
    for k, r in agg.iterrows():
        sub = d[d["k"] == k]
        ym = sub.groupby("y")["f"].median()
        oos = sub[sub["y"] >= "2024"]
        rows.append(dict(k=k, name=namer(k), n=int(r["n"]), med=r["med"],
                         pos=int((ym > 0).sum()), ny=len(ym), worst=float(ym.min()),
                         oos_n=len(oos), oos_med=oos["f"].median() if len(oos) >= 30 else np.nan))
    R = pd.DataFrame(rows).sort_values("med", ascending=False)
    print(f"\n===== {label} · cells n>={MIN_N}: {len(R)} =====", flush=True)
    print(f"  {'sequence':44s} {'n':>6s} {'med':>7s} {'Δ':>6s} {'yrs':>5s} {'worst':>7s} "
          f"{'OOS24-26':>9s}", flush=True)
    def show(sub, tag=""):
        for _, r in sub.iterrows():
            gate = " ✅" if (r["pos"] >= 4 and r["worst"] >= -2 and r["oos_med"] > GLOB) else ""
            print(f"  {tag}{r['name']:44s} {r['n']:>6d} {r['med']:>+7.2f} {r['med']-GLOB:>+6.2f} "
                  f"{r['pos']}/{r['ny']} {r['worst']:>+7.2f} {r['oos_med']:>+9.2f}{gate}", flush=True)
    show(R.head(top))
    print("  ── ჩამხშობი ──", flush=True)
    show(R.tail(bot).iloc[::-1], "▼ ")
    return R


def name2(k): return f"{IV[k // NV]} → {IV[k % NV]}"
def name3(k): return f"{IV[k // NV**2]} → {IV[(k // NV) % NV]} → {IV[k % NV]}"

K2 = c1 * NV + c0
R2 = sweep(K2, c1 > 0, name2, "2-ბარიანი (TZ+L)")
K3 = c2 * NV**2 + c1 * NV + c0
R3 = sweep(K3, (c1 > 0) & (c2 > 0), name3, "3-ბარიანი (TZ+L)")

# ── suffix post-filter on the survivors ─────────────────────────────────────────
def survivors(R):
    return R[(R["pos"] >= 4) & (R["worst"] >= -2) & (R["oos_med"] > GLOB) & (R["med"] > GLOB + 0.3)]

print("\n\n===== SUFFIX POST-FILTER on gate-passing cells =====", flush=True)
for R, keys, lab in [(R2, K2, "2-bar"), (R3, K3, "3-bar")]:
    S = survivors(R)
    print(f"\n— {lab}: {len(S)} cells passed the gates —", flush=True)
    for _, r in S.head(6).iterrows():
        m = ok & (keys == r["k"])
        d = pd.DataFrame({"s": SF[m], "f": F[m], "y": Y[m]})
        g = d.groupby("s")["f"].agg(["size", "median"])
        g = g[g["size"] >= 30].sort_values("median", ascending=False)
        if len(g) < 2:
            continue
        best, worst = g.index[0], g.index[-1]
        bym = d[d["s"] == best].groupby("y")["f"].median()
        print(f"  {r['name']:44s} base {r['med']:+.2f}", flush=True)
        print(f"      best sfx {best:5s} n={int(g.loc[best,'size']):>5d} "
              f"med{g.loc[best,'median']:>+7.2f} (Δ{g.loc[best,'median']-r['med']:+.2f}) "
              f"yr{int((bym>0).sum())}/{len(bym)}", flush=True)
        print(f"      worst sfx {worst:5s} n={int(g.loc[worst,'size']):>5d} "
              f"med{g.loc[worst,'median']:>+7.2f} (Δ{g.loc[worst,'median']-r['med']:+.2f})", flush=True)

print("\nDONE", flush=True)
