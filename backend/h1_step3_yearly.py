"""1H independent research — STEP 3 (user picked option ა): per-year stability of the
step-2 league tops. Takes the top-50 booster cells and top-15 suppressor cells across
all endings (same keys, same n>=300 floor, fwd35), slices each by calendar year, and
flags the ones that hold in >=4 of 5 full-ish years. 2021 runs from 07-02 and 2026 to
08-04 — both partial, counted but marked. Descriptive still — no path-sim, no builds.
"""
import gc, os
import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, "data", "h1_research_frame.parquet")
H = 35
MIN_N = 300

df = pd.read_parquet(CACHE, columns=["ticker", "dt", "close", "t", "z"])
print(f"frame {len(df):,}", flush=True)

code_str = np.where(df["t"].to_numpy() != "", df["t"].to_numpy(), df["z"].to_numpy())
vocab = sorted(set(code_str.tolist()))
V = {c: i for i, c in enumerate(vocab)}; IV = {i: c for c, i in V.items()}
c0 = np.array([V[c] for c in code_str], dtype=np.int64)
tk = df["ticker"].to_numpy()
yr = df["dt"].str[:4].astype(int).to_numpy()

def shift_back(a, k):
    out = np.full(len(a), -1, dtype=np.int64)
    out[k:] = a[:-k]
    bad = np.zeros(len(a), dtype=bool)
    for j in range(1, k + 1):
        b = np.zeros(len(a), dtype=bool); b[j:] = tk[j:] != tk[:-j]
        bad |= b
    out[bad] = -1
    return out

c1 = shift_back(c0, 1); c2 = shift_back(c0, 2); c3 = shift_back(c0, 3); c4 = shift_back(c0, 4)
cl = df["close"].to_numpy(float)
f = np.full(len(cl), np.nan)
f[:-H] = cl[H:] / cl[:-H] - 1
bad = np.zeros(len(cl), dtype=bool); bad[:-H] = tk[H:] != tk[:-H]
f[bad] = np.nan
f *= 100
del df, code_str, cl
gc.collect()

# keys: pack length + codes into one int (length tag in high bits)
K3 = np.where((c2 >= 0), 1_0000_0000_00 + c2 * 1024 + c1 * 32 + c0, -1)
K4 = np.where((c3 >= 0), 2_0000_0000_00 + c3 * 32768 + c2 * 1024 + c1 * 32 + c0, -1)
K5 = np.where((c4 >= 0), 3_0000_0000_00 + c4 * 1048576 + c3 * 32768 + c2 * 1024 + c1 * 32 + c0, -1)

def kname(k):
    tag = k // 1_0000_0000_00; r = k % 1_0000_0000_00
    parts = []
    for div in (1048576, 32768, 1024, 32, 1):
        parts.append(int(r // div) % 32 if div > 1 else int(r % 32))
    # decode by tag length
    if tag == 1: idx = parts[2:]
    elif tag == 2: idx = parts[1:]
    else: idx = parts
    return "→".join(IV[i] for i in idx)

ok = ~np.isnan(f)
# ── overall league to pick candidates (Δ vs the ENDING's own base) ───────────────
end_base = {}
for e in range(len(vocab)):
    m = ok & (c0 == e)
    if m.sum() >= 2000:
        end_base[e] = np.nanmedian(f[m])

rows = []
for K in (K3, K4, K5):
    m = ok & (K >= 0)
    d = pd.DataFrame({"k": K[m], "f": f[m]})
    g = d.groupby("k")["f"].agg(n="size", med="median")
    g = g[g["n"] >= MIN_N]
    e0 = (g.index % 32).astype(int)
    g["delta"] = g["med"] - pd.Series(e0, index=g.index).map(lambda e: end_base.get(e, np.nan))
    rows.append(g.dropna(subset=["delta"]))
    del d, g
    gc.collect()
LG = pd.concat(rows)
boost = LG.sort_values("delta", ascending=False).head(50)
supp  = LG.sort_values("delta").head(15)
print(f"league cells n≥{MIN_N}: {len(LG):,} · candidates picked", flush=True)

# ── per-year slice for the candidates ────────────────────────────────────────────
def yearly(sel_keys, K):
    out = {}
    kset = set(sel_keys)
    m = ok & (K >= 0) & np.isin(K, list(kset))
    d = pd.DataFrame({"k": K[m], "yr": yr[m], "f": f[m]})
    for k, sub in d.groupby("k"):
        ymed = sub.groupby("yr")["f"].median()
        ywin = sub.groupby("yr")["f"].apply(lambda s: (s > 0).mean() * 100)
        out[k] = (ymed, ywin, len(sub))
    return out

Y = {}
# one pass per K with all candidate keys — wrong-length keys simply won't match
allkeys = list(boost.index) + list(supp.index)
for K in (K3, K4, K5):
    Y.update(yearly(allkeys, K))

YEARS = [2021, 2022, 2023, 2024, 2025, 2026]
def report(sel, title):
    print(f"\n===== {title} =====", flush=True)
    print(f"{'sequence':40s} {'n':>6s} {'med':>7s} {'Δ':>6s} "
          + "".join(f"{y:>7d}" for y in YEARS) + "   pos worst", flush=True)
    keep = []
    for k, r in sel.iterrows():
        ymed, ywin, n = Y.get(k, (None, None, 0))
        if ymed is None:
            continue
        ys = "".join(f"{ymed.get(y, float('nan')):>7.2f}" for y in YEARS)
        pos = int((ymed > 0).sum()); ny = len(ymed)
        worst = ymed.min()
        flag = " ✅" if (pos >= 4 and worst > -0.5) else ""
        if flag: keep.append(kname(k))
        print(f"{kname(k):40s} {int(r['n']):>6d} {r['med']:>+7.2f} {r['delta']:>+6.2f} "
              f"{ys}   {pos}/{ny} {worst:>+6.2f}{flag}", flush=True)
    return keep

stable = report(boost, "TOP-50 BOOSTERS — per-year med35")
report(supp, "TOP-15 SUPPRESSORS — per-year med35")
print(f"\n✅ stable boosters (pos>=4yr & worst>-0.5): {len(stable)}", flush=True)
for s in stable:
    print("   ", s, flush=True)
print("\nDONE", flush=True)
