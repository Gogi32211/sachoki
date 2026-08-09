"""Post-crash absorption base — is it more than the compression we already own? (2026-08-09)

Born from CAR's February-March base. That base was NOT just a narrow range, and the narrow
range is the part we have already mined to death (🧊Coil-Floor is built, ❄️CONSO is a 69%-of-
bars regime, the COIL V4 graded score measured NEGATIVE on 4 of 5 edges yesterday, Zanger
showed a coil SUBTRACTS before a breakout, and L-clustering MUTES moves). Re-running "find a
tight range" would be re-running dead ground.

Three things in that base are NOT compression and have never been measured here:

  CLA   close-location asymmetry. Over the base, DOWN bars closed at 46% of their range while
        UP bars closed at 82%. Sellers had the volume and could not close the stock on its low;
        buyers could close it on its high. That is the book's central law (absorbed effort =
        buy) written as a number, and it does not exist as a feature anywhere in the frame.
  DECAY volume DYING, not range narrowing: 3.1M on the crash bar → 710k base average (−77%).
  CRASH the base sat AFTER a vertical drop, in its lower half. A drifting consolidation at the
        top of a range is a different animal and must not be pooled with it.

DECIDING FIRST TEST: is CLA new information vs `conso` and vs `E_coilfloor`? >80% agreement
with either and this closes immediately, exactly as WaveTrend did.

L2 IS DOUBLE HERE: the cell must beat the plain baseline by +1pp AND beat 🧊Coil-Floor by
+1pp. Coil-Floor already buys a held compressed base at its floor; anything that cannot clear
it is a rebrand of an edge we own, not a discovery.

All features are causal — every window is shifted one bar, so a signal bar never sees itself.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

N_TRIALS = 24
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp)} tickers", flush=True)

W = 20            # base window
print("computing the three features...", flush=True)
for tk, g in grp.items():
    o = g["open"].astype(float); h = g["high"].astype(float)
    l = g["low"].astype(float);  c = g["close"].astype(float)
    v = g["volume"].astype(float)
    rng = (h - l).replace(0, np.nan)
    loc = (c - l) / rng * 100.0                      # close location inside the bar
    up = c > c.shift(1)

    # ── CLA: mean close-location on up bars minus on down bars, prior W bars ───
    loc_up = loc.where(up); loc_dn = loc.where(~up)
    mu = loc_up.rolling(W, min_periods=6).mean()
    md = loc_dn.rolling(W, min_periods=6).mean()
    g["cla"] = (mu - md).shift(1).to_numpy()         # SHIFTED — causal

    # ── volume decay: recent 20-bar volume vs the prior 40 ─────────────────────
    v20 = v.rolling(W, min_periods=10).mean()
    v60 = v.rolling(60, min_periods=30).mean()
    g["vdecay"] = (v20 / v60.replace(0, np.nan)).shift(1).to_numpy()

    # ── post-crash: a ≥X% drawdown inside the prior 40 bars, low in the OLDER
    #    half, i.e. the crash happened first and the base sat after it ──────────
    hi40 = c.rolling(40, min_periods=25).max()
    lo40 = c.rolling(40, min_periods=25).min()
    dd = (lo40 / hi40 - 1.0) * 100.0                 # negative
    g["dd40"] = dd.shift(1).to_numpy()
    # where in the window is the low? argmin over the last 40, 0=oldest
    pos = c.rolling(40, min_periods=25).apply(lambda s: float(np.argmin(s)) / (len(s) - 1),
                                              raw=True)
    g["lowpos"] = pos.shift(1).to_numpy()

    # ── compression, for the control (this is the KNOWN-DEAD component) ────────
    rng40 = (h.rolling(W, min_periods=10).max() - l.rolling(W, min_periods=10).min())
    g["comp"] = (rng40 / l.rolling(W, min_periods=10).min()).shift(1).to_numpy()

    # ── terminating absorption bar: heavy volume, small body, price did not move
    vx = v / v.rolling(W, min_periods=10).mean()
    body = (c - o).abs() / rng * 100.0
    g["absbar"] = ((vx >= 2.0) & (body <= 35) & (loc >= 40)).fillna(False).to_numpy()
print("done", flush=True)

# ── 1. DECIDING TEST ───────────────────────────────────────────────────────────
print("\n===== 1. is CLA new information? =====", flush=True)
rows = []
for tk, g in grp.items():
    m = pd.Series(g["cla"]).notna()
    if not m.any():
        continue
    rows.append(pd.DataFrame({
        "cla": g["cla"][m.to_numpy()], "vdecay": g["vdecay"][m.to_numpy()],
        "dd40": g["dd40"][m.to_numpy()], "comp": g["comp"][m.to_numpy()],
        "conso": g["conso"][m.to_numpy()].astype(bool),
        "cf": g["E_coilfloor"][m.to_numpy()].fillna(False).astype(bool),
        "rsi": g["rsi_14"][m.to_numpy()],
    }))
X = pd.concat(rows, ignore_index=True)
print(f"  bars: {len(X):,}", flush=True)
print("  CLA percentiles: " + " · ".join(
    f"p{int(p*100)} {X.cla.quantile(p):+.1f}" for p in [.05, .25, .5, .75, .95]), flush=True)
print("  vdecay percentiles: " + " · ".join(
    f"p{int(p*100)} {X.vdecay.quantile(p):.2f}" for p in [.05, .25, .5, .75, .95]), flush=True)
hi = X.cla >= 20
print(f"  CLA>=20 fires on {hi.mean()*100:.1f}% of bars · conso on {X.conso.mean()*100:.1f}%",
      flush=True)
print(f"  CLA>=20 vs conso agreement: {(hi == X.conso).mean()*100:.1f}%", flush=True)
print(f"  of CLA>=20 bars, {X.loc[hi,'cf'].mean()*100:.2f}% are also 🧊Coil-Floor", flush=True)
for c, lab in [("comp", "compression"), ("vdecay", "vol decay"), ("rsi", "RSI"),
               ("dd40", "40-bar drawdown")]:
    print(f"    corr(CLA, {lab:16s}) = {X['cla'].corr(X[c], method='spearman'):+.3f}", flush=True)

fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
print(f"\nboard family: {len(fam)} setups", flush=True)


def run(label, build):
    for tk, g in grp.items():
        g["_B"] = pd.Series(build(g), index=g.index).fillna(False).astype(bool)
    tr = er._pathsim(grp, "_B", "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 80:
        print(f"  {label:42s} n={len(tr)} thin", flush=True); return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=N_TRIALS)
    ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in range(2021, 2027))
    med = tr["ret"].median() * 100
    print(f"  {label:42s} n={len(tr):>6d} med{med:>+7.2f} win{w.mean()*100:>5.1f} "
          f"pf{pf:>5.2f} |{ys} | {int((ym>0).sum())}/{len(ym)} worst{ym.min():>+6.2f} "
          f"DSR{d['dsr']:>6.3f}", flush=True)
    return dict(med=med, worst=ym.min(), yrs=int((ym > 0).sum()), n=len(tr))


# ── 2. the two benchmarks this must beat ──────────────────────────────────────
print("\n===== 2. the bar to clear =====", flush=True)
run("BASELINE (10th bar)", lambda g: np.arange(len(g)) % 10 == 0)
CF = run("🧊Coil-Floor (what we already own)", lambda g: g["E_coilfloor"])

# ── 3. CONTROLS FIRST — each component alone ──────────────────────────────────
print("\n===== 3. CONTROLS — each component alone (compression is known-dead) =====",
      flush=True)
run("compression alone (range<=20%)", lambda g: (g["comp"] <= 0.20)
    & (np.arange(len(g)) % 5 == 0))
run("CLA>=20 alone", lambda g: (g["cla"] >= 20) & (np.arange(len(g)) % 5 == 0))
run("vol decay<=0.7 alone", lambda g: (g["vdecay"] <= 0.7) & (np.arange(len(g)) % 5 == 0))
run("post-crash alone (dd<=-20, low old)",
    lambda g: (g["dd40"] <= -20) & (g["lowpos"] <= 0.5) & (np.arange(len(g)) % 5 == 0))

# ── 4. CLA in bands, on an oversold post-crash base ───────────────────────────
print("\n===== 4. CLA bands on a post-crash base (rsi<45, dd<=-20) =====", flush=True)


def base(g):
    return (g["dd40"] <= -20) & (g["lowpos"] <= 0.6) & (g["rsi_14"] < 45)


for lo, hi_ in [(-100, 0), (0, 10), (10, 20), (20, 30), (30, 200)]:
    run(f"  CLA {lo}..{hi_}", lambda g, a=lo, b=hi_: base(g) & (g["cla"] >= a) & (g["cla"] < b))

# ── 5. the full combination, and the trigger-bar variant ──────────────────────
print("\n===== 5. the combination =====", flush=True)
run("base + CLA>=20", lambda g: base(g) & (g["cla"] >= 20))
run("base + CLA>=20 + decay<=0.8", lambda g: base(g) & (g["cla"] >= 20) & (g["vdecay"] <= 0.8))
run("  + compression<=25% (the dead part)",
    lambda g: base(g) & (g["cla"] >= 20) & (g["vdecay"] <= 0.8) & (g["comp"] <= 0.25))
run("  + 🏆RS instead of compression",
    lambda g: base(g) & (g["cla"] >= 20) & (g["vdecay"] <= 0.8) & g["rs_intact"])
print("\n  --- entry on the TERMINATING absorption bar vs inside the base ---", flush=True)
run("ABS-BAR: base + CLA>=20 + absorption bar",
    lambda g: base(g) & (g["cla"] >= 20) & g["absbar"])
run("ABS-BAR + decay<=0.8",
    lambda g: base(g) & (g["cla"] >= 20) & g["vdecay"].le(0.8) & g["absbar"])
if "iv_vspike" in next(iter(grp.values())):
    run("ABS-BAR + 💥15m volume event",
        lambda g: base(g) & (g["cla"] >= 20) & g["absbar"] & g["iv_vspike"].fillna(False))

# ── 6. overlap with the board + price buckets ─────────────────────────────────
print("\n===== 6. overlap with what we already own =====", flush=True)
for tk, g in grp.items():
    g["_BEST"] = (base(g) & (g["cla"] >= 20) & (g["vdecay"] <= 0.8)).fillna(False)
tot = sum(int(g["_BEST"].sum()) for g in grp.values())
hits = []
for name, ecol in er.SETUPS:
    inter = sum(int((g["_BEST"] & g[ecol].fillna(False)).sum())
                for g in grp.values() if ecol in g)
    if inter and tot:
        hits.append((100.0 * inter / tot, name))
hits.sort(reverse=True)
print(f"  fires {tot:,} · " + (" · ".join(f"{n} {p:.0f}%" for p, n in hits[:8]) or "no overlap"),
      flush=True)

print("\n===== 7. price buckets =====", flush=True)
for lo, hi_ in [(5, 21), (21, 89), (89, 377)]:
    run(f"best cell ${lo}-{hi_}",
        lambda g, a=lo, b=hi_: g["_BEST"] & g["close"].between(a, b))

print("\nDONE", flush=True)
