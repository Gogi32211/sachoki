"""The five tokens the TZ-package replication filter enriched — tested on OUR data.

Cross-TF replication of the 5YR package (232,952 rules, permutation-controlled: enrichment
0.99 at rep>=3 rising to 4.44 at rep>=8) left 81 surviving rules. Their token composition
vs all tested rules:  Z7 3.04x · T12 2.96x · Z6 1.73x · Z2G 1.57x · Z1G 1.51x.
Two of those (Z7, T12) had ALREADY surfaced independently in our own 1H work — which is why
the tokens, not the 81 weak rules, are the thing worth chasing.

Everything here runs on the validated machine: ATRx12 exit, per-year, price buckets, DSR,
and the new lead-in-lag gate. Trial count fixed in advance.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

TOKENS = ["Z7", "T12", "Z6", "Z2G", "Z1G"]
N_TRIALS = len(TOKENS) * 4                      # 20, fixed before looking
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}", flush=True)

print("\n===== 1. EVENT or STATE? persistence of each token =====", flush=True)
print(f"  {'token':6s} {'bars':>9s} {'share':>7s} {'P(t|t-1)':>9s} {'vs base':>8s} "
      f"{'mean run':>9s}", flush=True)
for tokn in TOKENS:
    tot = hit = cont = runs = runlen = 0
    for g in grp.values():
        code = np.where(g["t"].to_numpy() != "", g["t"].to_numpy(), g["z"].to_numpy())
        m = (code == tokn)
        tot += len(m); hit += int(m.sum())
        if len(m) > 1:
            cont += int((m[1:] & m[:-1]).sum())
        d = np.diff(np.concatenate(([0], m.astype(np.int8), [0])))
        st = np.flatnonzero(d == 1); en = np.flatnonzero(d == -1)
        if len(st):
            runs += len(st); runlen += int((en - st).sum())
    base = hit / max(tot, 1) * 100
    cond = cont / max(hit, 1) * 100
    print(f"  {tokn:6s} {hit:>9,} {base:>6.2f}% {cond:>8.1f}% {cond/max(base,.001):>7.1f}x "
          f"{runlen/max(runs,1):>9.2f}", flush=True)

for tk, g in grp.items():
    code = np.where(g["t"].to_numpy() != "", g["t"].to_numpy(), g["z"].to_numpy())
    rsi = g["rsi_14"].to_numpy(float)
    lead = g["lead_in_lag"].to_numpy(bool) if "lead_in_lag" in g else np.zeros(len(g), bool)
    for tokn in TOKENS:
        b = (code == tokn)
        g[f"K_{tokn}_alone"] = b
        g[f"K_{tokn}_rsi"] = b & (rsi < 40)
        g[f"K_{tokn}_lead"] = b & lead
        g[f"K_{tokn}_both"] = b & (rsi < 40) & lead
    g["K_base10"] = (np.arange(len(g)) % 10 == 0)
print("masks built", flush=True)

fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
print(f"board family: {len(fam)} setups", flush=True)


def row(col, label, lo=None, hi=None, dsr=False):
    use = col
    if lo is not None:
        for tk, g in grp.items():
            g["_B"] = g[col] & g["close"].between(lo, hi)
        use = "_B"
    tr = er._pathsim(grp, use, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 60:
        print(f"    {label:26s} n={len(tr)} thin", flush=True); return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in range(2021, 2027))
    ds = ""
    if dsr:
        d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=N_TRIALS)
        ds = f" DSR{d['dsr']:>6.3f}"
    print(f"    {label:26s} n={len(tr):>6d} med{tr['ret'].median()*100:>+7.2f} "
          f"win{w.mean()*100:>5.1f} pf{pf:>5.2f} |{ys} | {int((ym>0).sum())}/{len(ym)} "
          f"worst{ym.min():>+6.2f}{ds}", flush=True)
    return dict(n=len(tr), med=tr["ret"].median() * 100, pos=int((ym > 0).sum()),
                ny=len(ym), worst=float(ym.min()), key=col)


print("\n===== 2. BASELINE (every 10th bar, same exit) =====", flush=True)
row("K_base10", "baseline")

print("\n===== 3. each token x 4 pre-specified variants =====", flush=True)
res = {}
for tokn in TOKENS:
    print(f"\n  -- {tokn} --", flush=True)
    for sfx, lab in [("alone", "alone"), ("rsi", "+ RSI<40"),
                     ("lead", "+ LEAD-in-LAG"), ("both", "+ RSI<40 + LEAD")]:
        res[(tokn, sfx)] = row(f"K_{tokn}_{sfx}", lab, dsr=True)

print("\n===== 4. price buckets - best variant per token =====", flush=True)
for tokn in TOKENS:
    cands = [(k[1], v) for k, v in res.items() if k[0] == tokn and v]
    if not cands:
        continue
    sfx, best = max(cands, key=lambda x: x[1]["med"])
    print(f"  -- {tokn} (best: {sfx}) --", flush=True)
    for lo, hi in [(5, 21), (21, 89), (89, 377)]:
        row(f"K_{tokn}_{sfx}", f"${lo}-{hi}", lo=lo, hi=hi)

print("\nDONE", flush=True)
