"""Dried-up Z-absorption chain — the MSFT 2026-07 structure, tested causally.

HYPOTHESIS (user-approved plan, 2026-08-04): >=5 consecutive Z-bars (absorption, no T)
all printing vol_bucket 'L' (volume dried up) = accumulation → the next 10 days pay.

CAUSAL: fires ON the Nth consecutive dry-Z bar; entry next open. No lookahead.
CONTROL that decides it: the same chain INSIDE vs OUTSIDE the earnings window. MSFT's
+14.7% came the morning after its 2026-07-29 report — if the whole effect is the
earnings gap, the pattern owns none of it.

Gates: L1 years>=4/6 + worst>=-2 -> L2 baseline+1pp -> L3 (n, $-bucket, plateau, DSR).
"""
import numpy as np
import pandas as pd
import edge_replay as er
import earnings_feed

grp, as_of = er._frame(60, 3_000_000)
print(f"frame {len(grp)} tickers, as_of {as_of}", flush=True)

# ── earnings map: ticker -> sorted list of report dates ─────────────────────────
try:
    _ed = earnings_feed.load() or {}
except Exception as e:
    _ed = {}
    print("earnings feed unavailable:", e, flush=True)
EARN = {k.upper(): sorted(pd.to_datetime(v)) for k, v in _ed.items() if v}
print(f"earnings feed: {len(EARN)} tickers", flush=True)


def add(g, tk):
    isz = (g["z"] != "") & (g["t"] == "")
    dry = isz & (g["vb"] == "L")
    # run-length of consecutive dry-Z bars ending at each bar
    run = np.zeros(len(g), dtype=int)
    d = dry.to_numpy()
    for i in range(len(g)):
        run[i] = run[i - 1] + 1 if (d[i] and i > 0) else (1 if d[i] else 0)
    g["zrun"] = run
    for n in (3, 4, 5, 6):
        g[f"S_dry{n}"] = (run == n)          # exact-N: fires once per chain, no double count
    g["S_dry5p"] = run >= 5

    # days until the NEXT earnings report (the causal, forward-looking calendar fact —
    # a report date is public in advance, so this is legitimately knowable at the bar)
    dts = EARN.get(tk, [])
    if dts:
        bar = pd.to_datetime(g["date"]).dt.tz_localize(None)
        nxt = np.searchsorted(np.array(dts, dtype="datetime64[ns]"),
                              bar.to_numpy(dtype="datetime64[ns]"), side="left")
        arr = np.array(dts, dtype="datetime64[ns]")
        far = np.full(len(g), 9999)
        ok = nxt < len(arr)
        far[ok] = (arr[nxt[ok]] - bar.to_numpy(dtype="datetime64[ns]")[ok]) / np.timedelta64(1, "D")
        g["d2e"] = far
    else:
        g["d2e"] = 9999
    return g


for tk in list(grp):
    grp[tk] = add(grp[tk], tk)
print("masks ready", flush=True)


def sub(col, extra=None, lo=None, hi=None):
    """materialise a boolean col with optional extra predicate + price bucket"""
    out = {}
    for tk, g in grp.items():
        m = g[col].copy()
        if extra is not None:
            m = m & extra(g)
        if lo is not None:
            m = m & g["close"].between(lo, hi)
        g2 = g.copy(); g2["_M"] = m
        out[tk] = g2
    return out


def run(col, extra=None, lo=None, hi=None):
    gd = sub(col, extra, lo, hi)
    tr = er._pathsim(gd, "_M", "trail", 0.10, 0.25, 0.25, 60)
    if len(tr) == 0:
        return None
    yr = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    return {"n": len(tr), "med": tr["ret"].median() * 100, "win": w.mean() * 100,
            "pf": (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf"),
            "yr": {int(k): round(v, 1) for k, v in yr.items()},
            "pos": int((yr > 0).sum()), "ny": len(yr), "worst": yr.min(),
            "sr": float(tr["ret"].mean() / tr["ret"].std(ddof=1)) if tr["ret"].std(ddof=1) > 0 else 0.0}


def line(label, r):
    if r is None or r["n"] == 0:
        print(f"  {label:34s}   n=0", flush=True); return
    ys = "".join(f"{r['yr'].get(y, float('nan')):>6.1f}" for y in range(2021, 2027))
    print(f"  {label:34s} {r['n']:>6d} {r['med']:>+7.2f} {r['win']:>5.1f} {r['pf']:>5.2f}"
          f" {ys}  {r['pos']}/{r['ny']} {r['worst']:>+6.1f}", flush=True)


HDR = (f"  {'cell':34s} {'n':>6s} {'med':>7s} {'win':>5s} {'pf':>5s} "
       f"{'2021':>6s}{'2022':>6s}{'2023':>6s}{'2024':>6s}{'2025':>6s}{'2026':>6s}  {'pos':>4s} {'worst':>6s}")

# ── 1. run-length plateau (does MORE dry-Z bars pay more?) ─────────────────────
print("\n===== 1. RUN-LENGTH PLATEAU (all prices) =====\n" + HDR, flush=True)
for n in (3, 4, 5, 6):
    line(f"exactly {n} dry-Z bars", run(f"S_dry{n}"))
line("5+ dry-Z bars", run("S_dry5p"))

# baseline: every 10th bar (the book's reference)
gd = {}
for tk, g in grp.items():
    g2 = g.copy(); g2["_M"] = (np.arange(len(g)) % 10 == 0)
    gd[tk] = g2
tr = er._pathsim(gd, "_M", "trail", 0.10, 0.25, 0.25, 60)
print(f"  {'BASELINE every-10th-bar':34s} {len(tr):>6d} {tr['ret'].median()*100:>+7.2f}", flush=True)

# ── 2. THE DECIDING CONTROL: inside vs outside the earnings window ─────────────
print("\n===== 2. EARNINGS SPLIT (5+ dry-Z bars) — the deciding control =====\n" + HDR, flush=True)
line("5+ dry-Z, report within 5d",  run("S_dry5p", lambda g: g["d2e"] <= 5))
line("5+ dry-Z, report within 10d", run("S_dry5p", lambda g: g["d2e"] <= 10))
line("5+ dry-Z, NO report <=10d",   run("S_dry5p", lambda g: g["d2e"] > 10))
line("5+ dry-Z, NO report <=21d",   run("S_dry5p", lambda g: g["d2e"] > 21))

# ── 3. price buckets ──────────────────────────────────────────────────────────
print("\n===== 3. PRICE BUCKETS (5+ dry-Z bars) =====\n" + HDR, flush=True)
for lo, hi, nm in [(5, 8, "$5-8"), (8, 21, "$8-21"), (21, 89, "$21-89"), (89, 377, "$89-377")]:
    line(f"5+ dry-Z {nm}", run("S_dry5p", None, lo, hi))

# ── 4. the MSFT cell exactly: 5+ dry-Z AND a report inside 5d, $89+ ───────────
print("\n===== 4. THE MSFT CELL (5+ dry-Z + report<=5d, by bucket) =====\n" + HDR, flush=True)
for lo, hi, nm in [(8, 21, "$8-21"), (21, 89, "$21-89"), (89, 377, "$89-377")]:
    line(f"MSFT-cell {nm}", run("S_dry5p", lambda g: g["d2e"] <= 5, lo, hi))

print("\nDONE", flush=True)
