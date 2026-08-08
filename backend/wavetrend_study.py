"""WaveTrend (LazyBear / "Market Cipher B") — is it new, or a reskin of what we own?

Four Pine scripts, one core. wt1 = EMA21(CCI10) on hlc3 — literally a twice-smoothed CCI,
and we already carry cci_20 and an RSI-based oversold everywhere. So the ONLY question that
matters first is overlap; everything else is wasted work if the answer is "same bars".

DECIDING FIRST TEST: WT-oversold vs RSI<40 and vs cci_20<-100. >80% agreement => close it.
Then, only if independent:
  - the zone-gated rule (cross with wt2 <= -45, and the author's own -60 tier = a free plateau)
  - the naked cross (no zone) as the control — does the ZONE do the work, or the cross?
  - 💥 double 28-bar-low reclaim (the one component unique to the WeloTrades build)
  - overlap with the board: if >60% Coil-Floor/QZC it is a rebrand (the 🏦-study lesson)
All on the ATR exit, per-year, price buckets, DSR against a pre-specified trial count.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

N_TRIALS = 12
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)

grp, as_of = er._frame(60, 3_000_000)
g0 = next(iter(grp.values()))
for c in ["wt1", "wt2", "wt_cross_up", "wt_bull_dot", "wt_bull_strong", "wt_dbl_reclaim"]:
    print(f"  {c}: {'present' if c in g0 else 'MISSING'}", flush=True)

# ── 1. DECIDING TEST: is WT-oversold the same bars as RSI<40 / cci_20 ? ─────────
print("\n===== 1. is WT-oversold NEW information? =====", flush=True)
rows = []
for tk, g in grp.items():
    m = g["wt2"].notna()
    if not m.any():
        continue
    rows.append(pd.DataFrame({
        "wt_os": (g["wt2"] <= -45)[m], "wt_os2": (g["wt2"] <= -60)[m],
        "rsi40": (g["rsi_14"] < 40)[m],
        "cci": (g["cci20"] < -100)[m] if "cci20" in g else np.nan,
        "wt2": g["wt2"][m], "rsi": g["rsi_14"][m],
    }))
X = pd.concat(rows, ignore_index=True)
print(f"  bars: {len(X):,}", flush=True)
print(f"  WT<=-45 share {X['wt_os'].mean()*100:.1f}% · WT<=-60 {X['wt_os2'].mean()*100:.1f}% "
      f"· RSI<40 {X['rsi40'].mean()*100:.1f}%", flush=True)
print(f"  WT<=-45 vs RSI<40 agreement: {(X['wt_os']==X['rsi40']).mean()*100:.1f}%  "
      f"(corr wt2~rsi {X['wt2'].corr(X['rsi']):+.3f})", flush=True)
if X["cci"].notna().any():
    print(f"  WT<=-45 vs cci20<-100 agreement: {(X['wt_os']==X['cci'].astype(bool)).mean()*100:.1f}%",
          flush=True)
# of the WT-oversold bars, how many are ALSO rsi<40?
sub = X[X["wt_os"]]
print(f"  of WT-oversold bars, {sub['rsi40'].mean()*100:.1f}% are also RSI<40 "
      f"(=> {100-sub['rsi40'].mean()*100:.1f}% are NEW territory)", flush=True)

# ── 2. does it pay? ────────────────────────────────────────────────────────────
fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
print(f"\nboard family: {len(fam)} setups", flush=True)


def row(col, label, lo=None, hi=None, extra=None):
    use = col
    if lo is not None or extra is not None:
        for tk, g in grp.items():
            m = g[col].copy()
            if extra is not None:
                m = m & extra(g)
            if lo is not None:
                m = m & g["close"].between(lo, hi)
            g["_B"] = m
        use = "_B"
    tr = er._pathsim(grp, use, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 60:
        print(f"  {label:30s} n={len(tr)} thin", flush=True); return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in range(2021, 2027))
    d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=N_TRIALS)
    print(f"  {label:30s} n={len(tr):>6d} med{tr['ret'].median()*100:>+7.2f} "
          f"win{w.mean()*100:>5.1f} pf{pf:>5.2f} |{ys} | {int((ym>0).sum())}/{len(ym)} "
          f"worst{ym.min():>+6.2f} DSR{d['dsr']:>6.3f}", flush=True)
    return dict(n=len(tr), med=tr["ret"].median() * 100, dsr=d["dsr"])


print("\n===== 2. the rule, and what does the work =====", flush=True)
for tk, g in grp.items():
    g["_base10"] = np.arange(len(g)) % 10 == 0
row("_base10", "BASELINE (10th bar)")
row("wt_cross_up", "naked cross (no zone)")
row("wt_bull_dot", "cross @ wt2<=-45")
row("wt_bull_strong", "cross @ wt2<=-60  (author tier)")
row("wt_dbl_reclaim", "💥 double 28-bar reclaim")

print("\n===== 3. does it add over RSI<40 alone? =====", flush=True)
for tk, g in grp.items():
    g["_rsi40"] = g["rsi_14"] < 40
    g["_dot_norsi"] = g["wt_bull_dot"] & (g["rsi_14"] >= 40)
row("_rsi40", "RSI<40 alone (any bar)")
row("wt_bull_dot", "  + WT dot", extra=lambda g: g["rsi_14"] < 40)
row("_dot_norsi", "WT dot where RSI>=40 (new)")

print("\n===== 4. price buckets · best variant =====", flush=True)
for lo, hi in [(5, 21), (21, 89), (89, 377)]:
    row("wt_bull_dot", f"dot ${lo}-{hi}", lo=lo, hi=hi)

print("\n===== 5. overlap with the board (the 🏦 lesson) =====", flush=True)
for col, nm in [("wt_bull_dot", "WT dot"), ("wt_dbl_reclaim", "💥 reclaim")]:
    tot = sum(int(g[col].sum()) for g in grp.values())
    hits = []
    for name, ecol in er.SETUPS:
        inter = sum(int((g[col] & g[ecol].fillna(False)).sum())
                    for g in grp.values() if ecol in g)
        if inter and tot:
            hits.append((100.0 * inter / tot, name))
    hits.sort(reverse=True)
    print(f"  {nm:12s} fires {tot:>6,} · " +
          (" · ".join(f"{n} {p:.0f}%" for p, n in hits[:6]) or "no overlap"), flush=True)

print("\nDONE", flush=True)
