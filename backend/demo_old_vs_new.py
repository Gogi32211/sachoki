"""Same signal, same data, two analyses — what the guards change in practice.

Signal: a T1G bar on a name that is RS-intact and oversold (RSI<45), 5 years, ⚡ATR×12 exit.
This is a real case from 2026-08-09: the old framing said BUILD, the matched framing said
NULL, and the difference was entirely which baseline the cell was compared against.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
import edge_replay as er
import overfit_stats as ofs
from analysis_kit import Study, GuardError

grp, as_of = er._frame(60, 3_000_000)


def tok(g):
    t = g["t"].astype(str).to_numpy(); z = g["z"].astype(str).to_numpy()
    o = np.where((t != "") & (t != "nan") & (t != "None"), t, z)
    return np.where((o == "nan") | (o == "None"), "", o)


# the gated universe: RS-intact AND oversold. Everything is measured inside THIS.
for tkr, g in grp.items():
    g["_tok"] = tok(g)
    g["_univ"] = (g["rs_intact"].fillna(False).astype(bool) & (g["rsi_14"] < 45)).to_numpy()

fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))

# ══════════════════════════════════════════════════════════════════════════════
# THE OLD WAY — what every study in this repo has printed all session
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "#" * 92)
print("#  OLD ANALYSIS — one number, one global baseline")
print("#" * 92, flush=True)


def old(label, col):
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=8)
    print(f"  {label:34s} n={len(tr):>6d} med{tr['ret'].median()*100:>+7.2f} "
          f"win{w.mean()*100:>5.1f} pf{pf:>5.2f} {int((ym>0).sum())}/{len(ym)}yr "
          f"worst{ym.min():>+6.2f} DSR{d['dsr']:>6.3f}", flush=True)
    return tr


for tkr, g in grp.items():
    g["_b10"] = np.arange(len(g)) % 10 == 0
old("BASELINE (every 10th bar)", "_b10")
for tkr, g in grp.items():
    g["_sig"] = (g["_tok"] == "T1G") & g["_univ"]
tr_sig = old("T1G + RS + RSI<45", "_sig")
print("\n  → the conclusion this framing invites:", flush=True)
print("     '+3.6 vs a baseline of +0.1 = more than +3pp of lift, 5/5 years,", flush=True)
print("      worst year positive, n>2000 — that is a BUILD.'", flush=True)

# ══════════════════════════════════════════════════════════════════════════════
# THE NEW WAY — same numbers, run through the guards
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "#" * 92)
print("#  NEW ANALYSIS — matched universe, controls, intervals, verdict")
print("#" * 92, flush=True)

# path-sim the WHOLE gated universe once, so baseline and cell come from the same rows
for tkr, g in grp.items():
    g["_all"] = g["_univ"]
tr_all = er._pathsim(grp, "_all", "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)

# label each trade with the token of its SIGNAL bar (entry is the next bar's open)
lut = {}
for tkr, g in grp.items():
    d = g["date"].astype(str).to_numpy()
    tk_ = g["_tok"]
    for i in range(len(d) - 1):
        lut[(tkr, d[i + 1])] = tk_[i]
tr_all["tok"] = [lut.get((t, d), "") for t, d in zip(tr_all["ticker"], tr_all["date_in"])]
tr_all["ret_pct"] = tr_all["ret"] * 100
tr_all["date"] = pd.to_datetime(tr_all["date_in"])

st = Study("does the T1G token add anything on an RS-intact oversold name?",
           n_trials=8, outcome="ret_pct", time_col="date", unit="%")

st.describe(tr_all, "ret_pct")
print("\n  the universe every cell is drawn from: RS-intact AND RSI<45", flush=True)
st.baseline(tr_all, "BASELINE — same universe, ANY token")

st.cell(tr_all, "T1G", tr_all.tok == "T1G")
st.controls(tr_all, {
    "T1G token": tr_all.tok == "T1G",
    "green bar": tr_all.tok.str.startswith("T"),
})

print("\n  the same 'BUILD' cell, judged against the MATCHED baseline:", flush=True)
cell = [c for c in st.cells if c.label == "T1G"][0]
d_sig = ofs.dsr(tr_sig["ret"].to_numpy(), fam, n_trials=8)
st.verdict(cell, dsr=d_sig["dsr"])

print("\n" + "=" * 92)
print("WHAT CHANGED — same data, same signal, same exit:")
print("=" * 92)
print("  old: lift measured against every-10th-bar across the whole market")
print("  new: lift measured against the same RS-intact oversold rows, minus the token")
print("  the signal did not change. The question did.")
print("\nDONE", flush=True)
