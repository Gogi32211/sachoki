"""1H independent research — STEP 4 (user approved): path-sim WITH REAL COSTS on the
31 year-stable cells from step 3.

Semantics = edge_replay._pathsim trail-mode EXACTLY (entry next-open x(1+SLIP), trail
from prior peak, gap-through-trail fills at OPEN, intrabar touch fills at trail level,
-SLIP on exit, 5-bar re-fire spacing, time stop at close), reimplemented to iterate
only fire indices — the original scans every bar per ticker per column, which at
18M x 62 runs is ~1e9 python iterations.

Configs (1H-scaled — the 1D 10/25% trail dwarfs a +1.5% edge):
  A: trail 5%,  maxh 35 (one 1H-week)
  B: trail 8%,  maxh 70
Costs: SLIP 15bps each way (the book's default) + 2x stress (30bps) on survivors.
Baseline: every-200th bar under the same exits.
"""
import gc, os
import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, "data", "h1_research_frame.parquet")
SLIP = 0.0015

SEQS = [  # (name, tuple oldest->entry)
    ("T2G→T2→T1G", ("T2G","T2","T1G")), ("T2→T2G→T1G", ("T2","T2G","T1G")),
    ("Z2G→T5→Z2G", ("Z2G","T5","Z2G")), ("T1G→T2→T4", ("T1G","T2","T4")),
    ("T1→T2→T4", ("T1","T2","T4")), ("Z1G→T2→T2", ("Z1G","T2","T2")),
    ("Z3→Z2G→Z4", ("Z3","Z2G","Z4")), ("T2G→Z5→T2G", ("T2G","Z5","T2G")),
    ("Z2G→T9→T1", ("Z2G","T9","T1")), ("T2G→T1G→T2", ("T2G","T1G","T2")),
    ("Z1→T4→T11", ("Z1","T4","T11")), ("T1G→T2G→T4", ("T1G","T2G","T4")),
    ("Z1→Z2→T2G", ("Z1","Z2","T2G")), ("Z6→Z2→T2G", ("Z6","Z2","T2G")),
    ("Z2→T5→Z2G", ("Z2","T5","Z2G")), ("T2G→T2→T4", ("T2G","T2","T4")),
    ("Z5→Z2G→T2", ("Z5","Z2G","T2")), ("Z2→Z4→Z2", ("Z2","Z4","Z2")),
    ("T3→T2→Z2G", ("T3","T2","Z2G")), ("T1→Z2G→T4", ("T1","Z2G","T4")),
    ("T9→Z4→T3→T6", ("T9","Z4","T3","T6")), ("Z4→Z2G→T5→Z3", ("Z4","Z2G","T5","Z3")),
    ("Z2G→Z2→T3→T12", ("Z2G","Z2","T3","T12")), ("Z2G→Z2G→T9→T12", ("Z2G","Z2G","T9","T12")),
    ("Z5→T9→Z4→Z2G", ("Z5","T9","Z4","Z2G")), ("Z4→Z2G→T5→T2", ("Z4","Z2G","T5","T2")),
    ("T4→Z4→Z2G→T5", ("T4","Z4","Z2G","T5")),
    ("Z11→Z2G→Z2G→Z2→Z2", ("Z11","Z2G","Z2G","Z2","Z2")),
    ("Z4→Z2→Z2→Z2G→T5", ("Z4","Z2","Z2","Z2G","T5")),
    ("T2G→Z3→T9→Z4→T3", ("T2G","Z3","T9","Z4","T3")),
    ("Z2→Z6→Z2→Z2→T9", ("Z2","Z6","Z2","Z2","T9")),
]

df = pd.read_parquet(CACHE, columns=["ticker", "dt", "open", "high", "low", "close", "t", "z"])
print(f"frame {len(df):,}", flush=True)
code_str = np.where(df["t"].to_numpy() != "", df["t"].to_numpy(), df["z"].to_numpy())
vocab = sorted(set(code_str.tolist())); V = {c: i for i, c in enumerate(vocab)}
c0 = np.array([V[c] for c in code_str], dtype=np.int16)
tk = df["ticker"].to_numpy()
yrs = df["dt"].str[:4].astype(np.int16).to_numpy()
O = df["open"].to_numpy(float); Hh = df["high"].to_numpy(float)
L = df["low"].to_numpy(float); C = df["close"].to_numpy(float)
del df, code_str
gc.collect()

def shift_back(a, k):
    out = np.full(len(a), -1, dtype=np.int16)
    out[k:] = a[:-k]
    bad = np.zeros(len(a), dtype=bool)
    for j in range(1, k + 1):
        b = np.zeros(len(a), dtype=bool); b[j:] = tk[j:] != tk[:-j]
        bad |= b
    out[bad] = -1
    return out

sh = {0: c0, 1: shift_back(c0, 1), 2: shift_back(c0, 2),
      3: shift_back(c0, 3), 4: shift_back(c0, 4)}

# ticker boundaries: last index of each ticker run (entry i+1 must stay same ticker)
tk_end = np.empty(len(tk), dtype=np.int64)
change = np.flatnonzero(tk[1:] != tk[:-1])
bounds = np.concatenate([change, [len(tk) - 1]])
start = 0
for b in bounds:
    tk_end[start:b + 1] = b
    start = b + 1

def fires_of(seq):
    ids = [V.get(s, -999) for s in seq]
    if any(i == -999 for i in ids):
        return np.array([], dtype=np.int64)
    m = sh[0] == ids[-1]
    for k, cid in enumerate(reversed(ids[:-1]), start=1):
        m &= sh[k] == cid
    return np.flatnonzero(m)

def pathsim(idx, trail, maxh, slip):
    trades = []
    last_tk = None; last_i = -99
    for i in idx:
        e = tk_end[i]
        if i + 1 > e:
            continue
        t = tk[i]
        if t == last_tk and i - last_i < 5:
            continue
        ep = O[i + 1]
        if not np.isfinite(ep) or ep <= 0:
            continue
        last_tk = t; last_i = i
        entry = ep * (1 + slip); ret = None
        end = min(i + 1 + maxh, e + 1)
        pk = entry; jout = end - 1; mlo = entry
        for j in range(i + 1, end):
            if L[j] < mlo: mlo = L[j]
            ts_prev = pk * (1 - trail)
            if j > i + 1 and O[j] <= ts_prev:
                ret = O[j] / entry - 1 - slip; jout = j; break
            if Hh[j] > pk: pk = Hh[j]
            ts = pk * (1 - trail)
            if L[j] <= ts:
                ret = ts / entry - 1 - slip; jout = j; break
        if ret is None:
            ret = C[end - 1] / entry - 1 - slip
        trades.append((ret, yrs[i], (mlo / entry - 1), jout - i))
    return pd.DataFrame(trades, columns=["ret", "yr", "mae", "hold"])

def stats(tr):
    if len(tr) == 0:
        return None
    ymed = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    return dict(n=len(tr), med=tr["ret"].median() * 100, win=w.mean() * 100,
                pf=(tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf"),
                pos=int((ymed > 0).sum()), ny=len(ymed), worst=float(ymed.min()),
                mae=tr["mae"].median() * 100, hold=tr["hold"].mean())

CFG = [("A trail5%/35b", 0.05, 35), ("B trail8%/70b", 0.08, 70)]
HDR = (f"{'sequence':26s} {'n':>5s} {'med':>7s} {'win':>5s} {'pf':>5s} "
       f"{'pos':>4s} {'worst':>6s} {'MAE':>6s} {'hold':>5s}")

survivors = []
for cname, trail, maxh in CFG:
    print(f"\n===== CONFIG {cname} · slip 15bps =====\n" + HDR, flush=True)
    bidx = np.flatnonzero(np.arange(len(c0)) % 200 == 0)
    b = stats(pathsim(bidx, trail, maxh, SLIP))
    print(f"{'BASELINE (200th bar)':26s} {b['n']:>5d} {b['med']:>+7.2f} {b['win']:>5.1f}"
          f" {b['pf']:>5.2f} {b['pos']}/{b['ny']} {b['worst']:>+6.2f} {b['mae']:>6.2f} {b['hold']:>5.1f}",
          flush=True)
    for name, seq in SEQS:
        idx = fires_of(seq)
        s = stats(pathsim(idx, trail, maxh, SLIP))
        if s is None:
            print(f"{name:26s}  n=0", flush=True); continue
        ok = s["med"] > 0 and s["pf"] >= 1.3 and s["pos"] >= 4 and s["med"] > b["med"]
        if ok and cname.startswith("A"):
            survivors.append((name, seq))
        print(f"{name:26s} {s['n']:>5d} {s['med']:>+7.2f} {s['win']:>5.1f} {s['pf']:>5.2f}"
              f" {s['pos']}/{s['ny']} {s['worst']:>+6.2f} {s['mae']:>6.2f} {s['hold']:>5.1f}"
              f"{'  ✅' if ok else ''}", flush=True)

print(f"\n===== 2x SLIP STRESS (30bps each way) on config-A survivors =====\n" + HDR, flush=True)
for name, seq in survivors:
    s = stats(pathsim(fires_of(seq), 0.05, 35, SLIP * 2))
    print(f"{name:26s} {s['n']:>5d} {s['med']:>+7.2f} {s['win']:>5.1f} {s['pf']:>5.2f}"
          f" {s['pos']}/{s['ny']} {s['worst']:>+6.2f} {s['mae']:>6.2f} {s['hold']:>5.1f}"
          f"{'  ✅' if s['med'] > 0 and s['pos'] >= 4 else '  ❌'}", flush=True)

print("\nDONE", flush=True)
