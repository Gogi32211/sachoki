"""CLA<0 cell — can a different EXIT turn +31% MFE into a tradeable return? (2026-08-09)

The signal side is done: 93 independent episodes, 79 tickers, 6/6 years including 2022, and
the effect survives deleting the top-10 tickers. What it does NOT survive is our exit — the
⚡ATR×12 trail harvests +6.86 median out of a +31.0% median MFE. Median MAE is −14.0% and the
median peak is 41 bars away, so the trail is being taken out by the wobble long before the
move completes.

This is the same question the ATR study asked, and it is answered the same way: sweep the exit
over the SAME signal set and require a PLATEAU, not a peak.

⚠ MULTIPLICITY IS THE RISK HERE, and it must be said out loud: this is ~20 exit variants on
n≈93-186. A single best cell means nothing at that size. The only result worth anything is a
NEIGHBOURHOOD of parameters that all agree, plus per-year stability. A lone spike is noise and
is reported as noise.

Signals are deduplicated to one per ticker per 90 days before anything is measured, so a single
episode firing six times cannot vote six times.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

N_TRIALS = 24
W, DEDUP = 20, 90
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}", flush=True)

# ── build the signal column, deduplicated to one per ticker per 90 days ───────
n_raw = n_kept = 0
for tk, g in grp.items():
    h = g["high"].astype(float); l = g["low"].astype(float); c = g["close"].astype(float)
    rng = (h - l).replace(0, np.nan)
    loc = (c - l) / rng * 100.0
    up = c > c.shift(1)
    cla = (loc.where(up).rolling(W, min_periods=6).mean()
           - loc.where(~up).rolling(W, min_periods=6).mean()).shift(1)
    hi40 = c.rolling(40, min_periods=25).max()
    lo40 = c.rolling(40, min_periods=25).min()
    dd = ((lo40 / hi40 - 1.0) * 100.0).shift(1)
    pos = c.rolling(40, min_periods=25).apply(
        lambda s: float(np.argmin(s)) / (len(s) - 1), raw=True).shift(1)
    m = ((cla < 0) & (dd <= -20) & (pos <= 0.6) & (g["rsi_14"] < 45)).fillna(False).to_numpy()
    n_raw += int(m.sum())
    if m.any():                              # keep only the FIRST fire of each episode
        dts = pd.to_datetime(g["date"]).to_numpy()
        keep = np.zeros(len(m), bool); lastd = None
        for i in np.where(m)[0]:
            d = dts[i]
            if lastd is None or (d - lastd) / np.timedelta64(1, "D") >= DEDUP:
                keep[i] = True; lastd = d
        m = keep
    n_kept += int(m.sum())
    g["E_cla"] = m
print(f"signals: {n_raw} raw → {n_kept} after {DEDUP}-day dedup\n", flush=True)

fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))

HDR = (f"  {'exit':38s} {'n':>4s} {'med':>7s} {'mean':>7s} {'win':>5s} {'pf':>5s} "
       f"{'hold':>5s} {'21':>6s}{'22':>6s}{'23':>6s}{'24':>6s}{'25':>6s}{'26':>6s} "
       f"{'yrs':>4s} {'worst':>7s} {'DSR':>6s}")


def run(label, mode, stop, target, trail, maxh, atr_k=None):
    tr = er._pathsim(grp, "E_cla", mode, stop, target, trail, maxh, atr_k=atr_k)
    if len(tr) < 40:
        print(f"  {label:38s} n={len(tr)} thin", flush=True); return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=N_TRIALS)
    ys = "".join(f"{ym.get(str(y), float('nan')):>6.1f}" for y in range(2021, 2027))
    med = tr["ret"].median() * 100
    print(f"  {label:38s} {len(tr):>4d} {med:>+7.2f} {tr['ret'].mean()*100:>+7.2f} "
          f"{w.mean()*100:>5.1f} {pf:>5.2f} {tr['hold'].mean():>5.0f} {ys} "
          f"{int((ym>0).sum()):>2d}/{len(ym)} {ym.min():>+7.2f} {d['dsr']:>6.3f}", flush=True)
    return dict(med=med, worst=ym.min(), yrs=int((ym > 0).sum()), n=len(tr), dsr=d["dsr"])


print("===== A. TRAIL family =====", flush=True)
print(HDR, flush=True)
run("⚡ATR×12 (current default)", "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
run("⚡ATR×16", "trail", 0.10, 0.25, 0.25, 60, atr_k=16.0)
run("⚡ATR×20", "trail", 0.10, 0.25, 0.25, 60, atr_k=20.0)
for t in (0.25, 0.35, 0.45, 0.55):
    run(f"fixed trail {int(t*100)}%", "trail", 0.10, 0.25, t, 60)

print("\n===== B. STOP + TARGET family =====", flush=True)
print(HDR, flush=True)
for s in (0.15, 0.20, 0.25):
    for t in (0.15, 0.20, 0.25, 0.30):
        run(f"stop {int(s*100)}% → target {int(t*100)}%", "st", s, t, 0.25, 60)

print("\n===== C. TIME exit (hold N bars, wide stop) =====", flush=True)
print(HDR, flush=True)
for mh in (20, 40, 50, 60, 80):
    run(f"hold {mh} bars · stop 25%", "st", 0.25, 9.99, 0.25, mh)
run("hold 60 bars · NO stop", "st", 0.99, 9.99, 0.25, 60)

print("\n===== D. longer horizon on the best families =====", flush=True)
print(HDR, flush=True)
run("⚡ATR×12 · maxh 90", "trail", 0.10, 0.25, 0.25, 90, atr_k=12.0)
run("fixed trail 45% · maxh 90", "trail", 0.10, 0.25, 0.45, 90)
run("stop 25% → target 30% · maxh 90", "st", 0.25, 0.30, 0.25, 90)
run("hold 80 bars · stop 25%", "st", 0.25, 9.99, 0.25, 80)

print("\n⚠ read the PLATEAU, not the peak: a winner whose neighbours disagree is noise at n≈"
      f"{n_kept}.", flush=True)
print("\nDONE", flush=True)
