"""Daily short FLOW — does the covering moment pay, where the standing short book did not?

Phase 1 (short_interest) was VETOed: a crowded short book is a suppressor, because shorts
crowd into names that deserve it. Phase 2 asks a different question with a different data
shape — not "how many shorts are there" but "what are they doing this week".

Two candidate readings, both taken from CAR:
  HIGH FLOW  — short_volume_ratio pinned at 75-87% through the base = relentless supply.
               Expect the same negative sign as the standing book; this is the control that
               tells us whether phase 2 is just phase 1 in daily clothing.
  COLLAPSE   — svr_drop = 5-day mean minus its own 60-day mean. On CAR this went sharply
               negative exactly at the blow-off (75-87% → 57%/54%). That is shorts BUYING,
               and it is the only part of this data with a 1-day lag instead of 12.

POINT-IN-TIME: FINRA posts the daily file after the close, so every feature is shifted one
bar — a signal bar sees yesterday's flow, never its own. Verified by construction below.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

N_TRIALS = 24
SV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "short_volume.parquet")
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)

sv = pd.read_parquet(SV_PATH)
print(f"daily short volume: {len(sv):,} rows · {sv.ticker.nunique():,} tickers · "
      f"{sv.date.min().date()} → {sv.date.max().date()}", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp)} tickers", flush=True)

SV_BY_TK = {t: d for t, d in sv.groupby("ticker", sort=False)}
matched = 0
for tk, g in grp.items():
    s = SV_BY_TK.get(tk)
    g["_dt"] = pd.to_datetime(g["date"]).astype("datetime64[ns]")
    if s is None or s.empty:
        for c in ("svr", "svr5", "svrz", "svrdrop"):
            g[c] = np.nan
        continue
    s = s.set_index("date")
    idx = g["_dt"]
    # SHIFT ONE BAR: the file for day D is public after D's close, so bar D may use D-1
    g["svr"] = s["short_volume_ratio"].reindex(idx).to_numpy()
    g["svr5"] = s["svr_5"].reindex(idx).to_numpy()
    g["svrz"] = s["svr_z"].reindex(idx).to_numpy()
    g["svrdrop"] = s["svr_drop"].reindex(idx).to_numpy()
    for c in ("svr", "svr5", "svrz", "svrdrop"):
        g[c] = pd.Series(g[c], index=g.index).shift(1).to_numpy()
    matched += 1
print(f"joined (shifted 1 bar) for {matched:,} tickers", flush=True)
cov = np.mean([g["svr5"].notna().mean() for g in grp.values()])
print(f"bar coverage: {cov*100:.1f}%", flush=True)

# ── 1. DECIDING TEST — is the FLOW different from the standing BOOK? ───────────
print("\n===== 1. is daily flow new vs the (vetoed) standing short book? =====", flush=True)
rows = []
for tk, g in grp.items():
    m = g["svr5"].notna()
    if not m.any():
        continue
    rows.append(pd.DataFrame({
        "svr": g["svr"][m], "svr5": g["svr5"][m], "svrz": g["svrz"][m],
        "drop": g["svrdrop"][m], "rsi": g["rsi_14"][m], "close": g["close"][m],
        "dv": (g["close"][m] * g["volume"][m]) if "volume" in g else np.nan,
    }))
X = pd.concat(rows, ignore_index=True)
print(f"  bars: {len(X):,}", flush=True)
print("  svr percentiles: " + " · ".join(
    f"p{int(p*100)} {X.svr.quantile(p):.1f}" for p in [.05, .25, .5, .75, .95]), flush=True)
print("  svr_drop percentiles: " + " · ".join(
    f"p{int(p*100)} {X['drop'].quantile(p):+.1f}" for p in [.05, .25, .5, .75, .95]), flush=True)
for c, lab in [("dv", "dollar volume"), ("close", "price"), ("rsi", "RSI")]:
    if X[c].notna().any():
        print(f"    corr(svr_drop, {lab:14s}) = {X['drop'].corr(X[c], method='spearman'):+.3f}",
              flush=True)

fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
print(f"\nboard family: {len(fam)} setups", flush=True)


def run(label, build):
    for tk, g in grp.items():
        g["_B"] = build(g).fillna(False).astype(bool)
    tr = er._pathsim(grp, "_B", "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 80:
        print(f"  {label:40s} n={len(tr)} thin", flush=True); return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=N_TRIALS)
    ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in range(2021, 2027))
    print(f"  {label:40s} n={len(tr):>6d} med{tr['ret'].median()*100:>+7.2f} "
          f"win{w.mean()*100:>5.1f} pf{pf:>5.2f} |{ys} | {int((ym>0).sum())}/{len(ym)} "
          f"worst{ym.min():>+6.2f} DSR{d['dsr']:>6.3f}", flush=True)
    return dict(med=tr["ret"].median() * 100, worst=ym.min(), yrs=int((ym > 0).sum()))


EDGES = [("QZ-Capit", "E_qzcapit"), ("Washout", "E_washout"), ("🧊Coil-Floor", "E_coilfloor")]
g0 = next(iter(grp.values()))
EDGES = [(n, c) for n, c in EDGES if c in g0]

# ── 2. the COLLAPSE reading (the one with a 1-day lag) ────────────────────────
print("\n===== 2. svr_drop (5d flow vs its own 60d norm) as a gate =====", flush=True)
DROP = [("drop <= -8  (hard covering)", lambda g: g["svrdrop"] <= -8),
        ("drop -8..-3", lambda g: g["svrdrop"].between(-8, -3)),
        ("drop -3..+3  (normal)", lambda g: g["svrdrop"].between(-3, 3)),
        ("drop >= +3   (shorting harder)", lambda g: g["svrdrop"] >= 3)]
for nm, col in EDGES:
    print(f"\n  -- {nm} --", flush=True)
    run("base (no gate)", lambda g, c=col: g[c])
    for lab, f in DROP:
        run(f"  {lab}", lambda g, c=col, f=f: g[c] & f(g))

# ── 3. the LEVEL reading — is this phase 1 in daily clothing? ─────────────────
print("\n===== 3. svr LEVEL — same suppressor as the standing book? =====", flush=True)
LEV = [("svr5 < 45", lambda g: g["svr5"] < 45),
       ("svr5 45-55", lambda g: g["svr5"].between(45, 55)),
       ("svr5 55-65", lambda g: g["svr5"].between(55, 65)),
       ("svr5 >= 65", lambda g: g["svr5"] >= 65)]
for nm, col in EDGES:
    print(f"\n  -- {nm} --", flush=True)
    for lab, f in LEV:
        run(f"  {lab}", lambda g, c=col, f=f: g[c] & f(g))

# ── 4. CONTROLS ───────────────────────────────────────────────────────────────
print("\n===== 4. CONTROLS — is any of this a trigger on its own? =====", flush=True)
run("BASELINE (10th bar)", lambda g: pd.Series(np.arange(len(g)) % 10 == 0, index=g.index))
run("drop<=-8 alone (10th bar)",
    lambda g: pd.Series(np.arange(len(g)) % 10 == 0, index=g.index) & (g["svrdrop"] <= -8))
run("drop<=-8 + RSI<40 (no edge)", lambda g: (g["svrdrop"] <= -8) & (g["rsi_14"] < 40)
    & pd.Series(np.arange(len(g)) % 5 == 0, index=g.index))
run("svr5>=65 alone (10th bar)",
    lambda g: pd.Series(np.arange(len(g)) % 10 == 0, index=g.index) & (g["svr5"] >= 65))

print("\n===== 5. price buckets · best drop cell on Washout =====", flush=True)
for lo, hi in [(5, 21), (21, 89), (89, 377)]:
    run(f"Washout+drop<=-8 ${lo}-{hi}",
        lambda g, a=lo, b=hi: g["E_washout"] & (g["svrdrop"] <= -8) & g["close"].between(a, b))

print("\nDONE", flush=True)
