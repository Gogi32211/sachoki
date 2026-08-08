"""Is "any Z/T absorption bar + RSI<40 + LEAD-in-LAG" a NEW entry, or a reskin of the
capitulation family we already own? (2026-08-07)

The five-token test showed the token is irrelevant once the state is fixed: every one of
Z7/T12/Z6/Z2G/Z1G goes from ~0 to +4.2..+4.7 under RSI<40 + lead_in_lag, and the spread
between them is noise. So the candidate is the STATE, not the token:

    GEN = (any TZ code fires) & rsi_14 < 40 & lead_in_lag

The 🏦-study lesson applies directly: our QZC / WSH / D+L1 / coil-floor already live in the
RSI 20-40 zone, so this could be the same trades wearing a different name. Deciding test =
overlap. <40% overlap with every existing setup and a positive DISJOINT remainder → new.
Otherwise it is a reskin and gets closed.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}", flush=True)

CAPIT = ["E_qzcapit", "E_washout", "E_dl1", "E_coilfloor", "E_t1capbounce",
         "E_h1bottom", "E_spring", "E_zoneretest"]

for tk, g in grp.items():
    code = np.where(g["t"].to_numpy() != "", g["t"].to_numpy(), g["z"].to_numpy())
    rsi = g["rsi_14"].to_numpy(float)
    lead = g["lead_in_lag"].to_numpy(bool) if "lead_in_lag" in g else np.zeros(len(g), bool)
    gen = (code != "") & (rsi < 40) & lead
    g["G_gen"] = gen
    known = np.zeros(len(g), bool)
    for c in CAPIT:
        if c in g:
            known |= g[c].fillna(False).to_numpy(bool)
    anyE = np.zeros(len(g), bool)
    for _, c in er.SETUPS:
        if c in g:
            anyE |= g[c].fillna(False).to_numpy(bool)
    g["G_gen_disj_capit"] = gen & ~known      # disjoint from the capitulation family
    g["G_gen_disj_all"] = gen & ~anyE         # disjoint from EVERY board setup
print("masks built", flush=True)

fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))


def row(col, label, lo=None, hi=None):
    use = col
    if lo is not None:
        for tk, g in grp.items():
            g["_B"] = g[col] & g["close"].between(lo, hi)
        use = "_B"
    tr = er._pathsim(grp, use, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 60:
        print(f"  {label:34s} n={len(tr)} thin", flush=True); return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in range(2021, 2027))
    d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=25)
    print(f"  {label:34s} n={len(tr):>6d} med{tr['ret'].median()*100:>+7.2f} "
          f"win{w.mean()*100:>5.1f} pf{pf:>5.2f} |{ys} | {int((ym>0).sum())}/{len(ym)} "
          f"worst{ym.min():>+6.2f} DSR{d['dsr']:>6.3f}", flush=True)
    return dict(n=len(tr), med=tr["ret"].median() * 100, dsr=d["dsr"])


print("\n===== 1. OVERLAP with every existing board setup =====", flush=True)
tot = sum(int(g["G_gen"].sum()) for g in grp.values())
hits = []
for name, col in er.SETUPS:
    inter = sum(int((g["G_gen"] & g[col].fillna(False)).sum()) for g in grp.values() if col in g)
    if inter:
        hits.append((100.0 * inter / tot, name, inter))
hits.sort(reverse=True)
print(f"  GEN fires: {tot:,}", flush=True)
for p, n, i in hits[:12]:
    print(f"    {n:26s} {p:>6.1f}%  ({i:,} shared)", flush=True)
cap_share = sum(int(g["G_gen_disj_capit"].sum()) for g in grp.values())
all_share = sum(int(g["G_gen_disj_all"].sum()) for g in grp.values())
print(f"\n  disjoint from the CAPIT family : {cap_share:,} ({100*cap_share/tot:.1f}%)", flush=True)
print(f"  disjoint from EVERY board setup: {all_share:,} ({100*all_share/tot:.1f}%)", flush=True)

print("\n===== 2. does the DISJOINT remainder still pay? =====", flush=True)
row("G_gen", "GEN (all fires)")
row("G_gen_disj_capit", "GEN minus capit family")
row("G_gen_disj_all", "GEN minus EVERY setup")

print("\n===== 3. price buckets of the fully-disjoint remainder =====", flush=True)
for lo, hi in [(5, 21), (21, 89), (89, 377)]:
    row("G_gen_disj_all", f"disjoint ${lo}-{hi}", lo=lo, hi=hi)

print("\nDONE", flush=True)
