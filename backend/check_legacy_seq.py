"""Audit the LEGACY toolbar sequence signals with the current honest standard.

Signals (definitions copied from main.py enrich helpers / Ultra chip logic):
  🎯 T-Z-T4   T[-2] -> Z[-1] -> T4[0], client gate RSI>=60
  🔺 T-T-T6   T[-2] -> T[-1] -> T6[0]  (+tier1: T3/T1/T10 at [-2])
  ⚡ T1-seq    ZZ->T1 / TZ->T1 / ZT->T1
  🟡 T3·35    T3 + RSI<35
  🔵 T9·35    T9 + RSI<35  (+premium: N* suffix)
  🟢 Z1G→EUR  Z1G[-2] -> T1[-1] -> T2G[0] + fsfx EUR + RSI 35-60

Path-sim trail 10/25/25/60 on the canonical frame, per-year medians, $21-89 slice,
complement deltas (same ending without the qualifier), overlap vs the new 🟡 seq family.
"""
import numpy as np
import pandas as pd
import edge_replay as er

grp, as_of = er._frame(60, 3_000_000)
print("frame", len(grp), "as_of", as_of, flush=True)

NEW_COLS = ["E_z1gt4", "E_z9hl", "E_z1gt36", "E_seq20", "E_z1gcrown"]

def addmasks(g):
    t = g["t"].fillna(""); z = g["z"].fillna("")
    t1 = t.shift(1).fillna(""); t2 = t.shift(2).fillna("")
    z1 = z.shift(1).fillna(""); z2 = z.shift(2).fillna("")
    rsi = g["rsi_14"]; fs = g["fsfx"].fillna("")
    g["S_tzt4"]    = (t == "T4") & (z1 != "") & (t2 != "")
    g["S_tzt4_60"] = g["S_tzt4"] & (rsi >= 60)
    g["S_t4_cmp"]  = (t == "T4") & ~g["S_tzt4"]
    g["S_ttt6"]    = (t == "T6") & (t1 != "") & (t2 != "")
    g["S_ttt6_t1"] = g["S_ttt6"] & t2.isin(["T3", "T1", "T10"])
    g["S_t6_cmp"]  = (t == "T6") & ~g["S_ttt6"]
    g["S_zz_t1"]   = (t == "T1") & (z1 != "") & (z2 != "")
    g["S_tz_t1"]   = (t == "T1") & (z1 != "") & (t2 != "")
    g["S_zt_t1"]   = (t == "T1") & (t1 != "") & (z2 != "")
    g["S_t1_cmp"]  = (t == "T1") & ~(g["S_zz_t1"] | g["S_tz_t1"] | g["S_zt_t1"])
    g["S_t335"]    = (t == "T3") & (rsi < 35)
    g["S_t3_cmp"]  = (t == "T3") & (rsi >= 35)
    g["S_t935"]    = (t == "T9") & (rsi < 35)
    g["S_t935_n"]  = g["S_t935"] & fs.str.startswith("N")
    g["S_t9_cmp"]  = (t == "T9") & (rsi >= 35)
    g["S_z1geur"]  = ((t == "T2G") & (t1 == "T1") & (z2 == "Z1G")
                      & (fs == "EUR") & rsi.between(35, 60))
    g["S_t2g_cmp"] = (t == "T2G") & ~g["S_z1geur"]
    g["S_new_any"] = False
    for c in NEW_COLS:
        if c in g:
            g["S_new_any"] = g["S_new_any"] | g[c].fillna(False)
    return g

for tk in grp:
    grp[tk] = addmasks(grp[tk])
print("masks ready", flush=True)

def bucket(gdict, col, lo, hi):
    out = {}
    for tk, g in gdict.items():
        g2 = g.copy()
        g2[col] = g2[col] & g2["close"].between(lo, hi)
        out[tk] = g2
    return out

def run(col, lo=None, hi=None):
    gd = grp if lo is None else bucket(grp, col, lo, hi)
    tr = er._pathsim(gd, col, "trail", 0.10, 0.25, 0.25, 60)
    if len(tr) == 0:
        return None
    yr = tr.groupby("yr")["ret"].median() * 100
    wins = tr["ret"] > 0
    pf_d = -tr.loc[~wins, "ret"].sum()
    pf = tr.loc[wins, "ret"].sum() / pf_d if pf_d > 0 else float("inf")
    return {"n": len(tr), "med": tr["ret"].median() * 100, "win": wins.mean() * 100,
            "pf": pf, "yr": {int(k): round(v, 1) for k, v in yr.items()},
            "pos": int((yr > 0).sum()), "ny": len(yr), "worst": yr.min()}

def overlap_new(col):
    both = tot = 0
    for g in grp.values():
        m = g[col]
        tot += int(m.sum()); both += int((m & g["S_new_any"]).sum())
    return 100.0 * both / tot if tot else 0.0

ROWS = [
    ("🎯 T-Z-T4 (raw)",      "S_tzt4",    "S_t4_cmp"),
    ("🎯 T-Z-T4 RSI>=60",    "S_tzt4_60", "S_t4_cmp"),
    ("🔺 T-T-T6 (raw)",      "S_ttt6",    "S_t6_cmp"),
    ("🔺 T-T-T6 tier1",      "S_ttt6_t1", "S_t6_cmp"),
    ("⚡ ZZ→T1",              "S_zz_t1",   "S_t1_cmp"),
    ("⚡ TZ→T1",              "S_tz_t1",   "S_t1_cmp"),
    ("⚡ ZT→T1",              "S_zt_t1",   "S_t1_cmp"),
    ("🟡 T3·35",             "S_t335",    "S_t3_cmp"),
    ("🔵 T9·35",             "S_t935",    "S_t9_cmp"),
    ("🔵 T9·35 N*",          "S_t935_n",  "S_t9_cmp"),
    ("🟢 Z1G→EUR",           "S_z1geur",  "S_t2g_cmp"),
]

for label, (lo, hi) in [("ALL-PRICE (dv-floor $3M)", (None, None)),
                        ("$21-89", (21, 89))]:
    print(f"\n===== {label} =====", flush=True)
    hdr = f"{'signal':22s} {'n':>6s} {'med':>7s} {'win':>5s} {'pf':>5s}  " \
          f"{'2021':>5s}{'2022':>6s}{'2023':>6s}{'2024':>6s}{'2025':>6s}{'2026':>6s}" \
          f"  {'pos':>4s} {'worst':>6s} {'Δcmp':>7s}"
    print(hdr, flush=True)
    for name, col, cmpcol in ROWS:
        r = run(col, lo, hi)
        if r is None or r["n"] == 0:
            print(f"{name:22s}  n=0", flush=True)
            continue
        c = run(cmpcol, lo, hi)
        d = r["med"] - c["med"] if c else float("nan")
        ys = "".join(f"{r['yr'].get(y, float('nan')):>6.1f}" for y in range(2021, 2027))
        print(f"{name:22s} {r['n']:>6d} {r['med']:>+7.2f} {r['win']:>5.1f} {r['pf']:>5.2f} "
              f"{ys}  {r['pos']}/{r['ny']} {r['worst']:>+6.1f} {d:>+7.2f}", flush=True)

print("\n===== overlap with the NEW 🟡 seq family (share of fires co-firing) =====", flush=True)
for name, col, _ in ROWS:
    print(f"  {name:22s} {overlap_new(col):5.1f}%", flush=True)
print("\nDONE", flush=True)
