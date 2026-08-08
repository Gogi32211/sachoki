"""1H independent research — STEP 2 (user spec 2026-08-05): for EVERY T/Z ending code,
league tables of the 3/4/5-bar sequences that END on it, PLUS the entry bar's L-line
and suffix leagues. Both directions reported (boosters AND suppressors). No 1D priors:
horizon 35 h-bars = one 1H-week (session anatomy from step 1), fwd-return descriptive
(path-sim comes later, on finalists only). Global baseline H=35: med +0.15, win 51.3.

Output: full leagues -> h1_step2_leagues.txt ; stdout = T1 example + cross-ending top.
"""
import gc, os, sys
import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, "data", "h1_research_frame.parquet")
OUT = os.path.join(BASE, "h1_step2_leagues.txt")
H1, H2 = 35, 70
MIN_N = 300

df = pd.read_parquet(CACHE, columns=["ticker", "dt", "close", "t", "z", "l", "fsfx"])
print(f"frame {len(df):,}", flush=True)

# ── int-coded language (memory: object cols are deadly at 18M rows) ─────────────
code_str = np.where(df["t"].to_numpy() != "", df["t"].to_numpy(), df["z"].to_numpy())
vocab = sorted(set(code_str.tolist()))
V = {c: i for i, c in enumerate(vocab)}          # ≤ 26 codes
IV = {i: c for c, i in V.items()}
c0 = np.array([V[c] for c in code_str], dtype=np.int32)

tk = df["ticker"].to_numpy()
same1 = np.empty(len(df), dtype=bool); same1[0] = False; same1[1:] = tk[1:] == tk[:-1]

def shift_back(a, k):
    """a shifted k bars back within ticker (invalid -> -1)"""
    out = np.full(len(a), -1, dtype=np.int32)
    out[k:] = a[:-k]
    bad = np.zeros(len(a), dtype=bool)
    for j in range(1, k + 1):
        b = np.zeros(len(a), dtype=bool); b[j:] = tk[j:] != tk[:-j]
        bad |= b
    out[bad] = -1
    return out

c1 = shift_back(c0, 1); c2 = shift_back(c0, 2); c3 = shift_back(c0, 3); c4 = shift_back(c0, 4)

cl = df["close"].to_numpy(float)
def fwd(h):
    out = np.full(len(cl), np.nan)
    out[:-h] = cl[h:] / cl[:-h] - 1
    bad = np.zeros(len(cl), dtype=bool); bad[:-h] = tk[h:] != tk[:-h]
    out[bad] = np.nan
    return out * 100

f35 = fwd(H1); f70 = fwd(H2)
lline = df["l"].to_numpy(); sfx = df["fsfx"].to_numpy()
del df, code_str
gc.collect()
print("shifts + fwd ready", flush=True)

GLOB_MED = np.nanmedian(f35)

def league(mask_rows, keys, names, fh):
    """group masked rows by keys tuple-array -> league lines"""
    d = pd.DataFrame({"k": keys[mask_rows], "f35": f35[mask_rows], "f70": f70[mask_rows]})
    d = d.dropna(subset=["f35"])
    g = d.groupby("k")
    agg = g.agg(n=("f35", "size"), med=("f35", "median"),
                win=("f35", lambda s: (s > 0).mean() * 100), med70=("f70", "median"))
    agg = agg[agg["n"] >= MIN_N].sort_values("med", ascending=False)
    return agg

def fmt(agg, name_fn, base_med, top=15, bottom=5):
    lines = []
    rows = list(agg.iterrows())
    shown = rows[:top] + ([("...", None)] if len(rows) > top + bottom else []) + \
            (rows[-bottom:] if len(rows) > top else [])
    for k, r in shown:
        if r is None:
            lines.append("      ···"); continue
        lines.append(f"      {name_fn(k):34s} n={int(r['n']):>6d}  med{r['med']:>+7.2f}"
                     f"  win{r['win']:>5.1f}  med70{r['med70']:>+7.2f}  Δbase{r['med']-base_med:>+7.2f}")
    return lines

fh = open(OUT, "w")
def emit(s, echo=False):
    fh.write(s + "\n")
    if echo:
        print(s, flush=True)

# key packers
K3 = c2 * 1024 + c1 * 32 + c0          # 3-bar total (2 prefix)
K4 = c3 * 32768 + K3                   # 4-bar
K5 = c4 * 1048576 + K4                 # 5-bar
def n3(k): return f"{IV[k//1024]}→{IV[(k//32)%32]}→{IV[k%32]}"
def n4(k): return f"{IV[k//32768]}→{n3(k%32768)}"
def n5(k): return f"{IV[k//1048576]}→{n4(k%1048576)}"

ENDINGS = [V[c] for c in vocab]
cross_top = []

for e in ENDINGS:
    name = IV[e]
    m0 = (c0 == e) & ~np.isnan(f35)
    if m0.sum() < 2000:
        emit(f"\n##### {name} — base n={int(m0.sum())} (too thin, skipped)")
        continue
    base_med = np.nanmedian(f35[m0])
    base_win = (f35[m0] > 0).mean() * 100
    echo = (name == "T1")
    emit(f"\n##### ENDING {name} — base n={int(m0.sum()):,} med{base_med:+.2f} "
         f"win{base_win:.1f} (global {GLOB_MED:+.2f})", echo)

    for K, nf, lab, valid in [(K3, n3, "3-bar", (c2 >= 0)),
                              (K4, n4, "4-bar", (c3 >= 0)),
                              (K5, n5, "5-bar", (c4 >= 0))]:
        m = m0 & valid
        agg = league(m, K, nf, fh)
        emit(f"    ── {lab} sequences (top15 / bottom5, n≥{MIN_N}) ──", echo)
        for ln in fmt(agg, nf, base_med):
            emit(ln, echo)
        for k, r in agg.head(3).iterrows():
            cross_top.append((r["med"] - base_med, r["med"], int(r["n"]), nf(k), name, lab))
        del agg

    # entry-bar L league
    dl = pd.DataFrame({"k": lline[m0], "f35": f35[m0], "f70": f70[m0]}).dropna(subset=["f35"])
    g = dl.groupby("k").agg(n=("f35", "size"), med=("f35", "median"),
                            win=("f35", lambda s: (s > 0).mean() * 100), med70=("f70", "median"))
    g = g[g["n"] >= MIN_N].sort_values("med", ascending=False)
    emit(f"    ── entry-bar L-line ──", echo)
    for k, r in g.iterrows():
        emit(f"      {k:34s} n={int(r['n']):>6d}  med{r['med']:>+7.2f}  win{r['win']:>5.1f}"
             f"  med70{r['med70']:>+7.2f}  Δbase{r['med']-base_med:>+7.2f}", echo)
    # entry-bar suffix league
    ds = pd.DataFrame({"k": sfx[m0], "f35": f35[m0], "f70": f70[m0]}).dropna(subset=["f35"])
    g = ds.groupby("k").agg(n=("f35", "size"), med=("f35", "median"),
                            win=("f35", lambda s: (s > 0).mean() * 100), med70=("f70", "median"))
    g = g[g["n"] >= MIN_N].sort_values("med", ascending=False)
    emit(f"    ── entry-bar suffix ──", echo)
    for k, r in g.head(10).iterrows():
        emit(f"      {str(k):34s} n={int(r['n']):>6d}  med{r['med']:>+7.2f}  win{r['win']:>5.1f}"
             f"  med70{r['med70']:>+7.2f}  Δbase{r['med']-base_med:>+7.2f}", echo)
    for k, r in g.tail(3).iterrows():
        emit(f"   ▼  {str(k):31s} n={int(r['n']):>6d}  med{r['med']:>+7.2f}  win{r['win']:>5.1f}"
             f"  med70{r['med70']:>+7.2f}  Δbase{r['med']-base_med:>+7.2f}", echo)
    del dl, ds, g
    gc.collect()

emit("\n\n===== CROSS-ENDING TOP (by Δ vs own base, n≥%d) =====" % MIN_N, True)
cross_top.sort(reverse=True)
for dm, med, n, seq, endname, lab in cross_top[:30]:
    emit(f"  {seq:40s} [{lab} → {endname}]  n={n:>6d}  med{med:>+7.2f}  Δ{dm:>+7.2f}", True)

fh.close()
print(f"\nfull leagues -> {OUT}", flush=True)
print("DONE", flush=True)
