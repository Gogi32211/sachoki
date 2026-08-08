"""ADX/DI regime gate — is it new information, or Hurst under another name? (2026-08-07)

From the user's Pine v6 port (260807 V1). Its claim: ADX tells you WHEN breakout/momentum
edges should work vs when mean-reversion is better. That is a regime gate — the one category
where this book keeps finding real things (VIX direction, lead-in-lag) after three shape
searches failed. So it deserves a proper test.

TWO CORRECTIONS applied to the script before testing:
  1. it uses ta.sma(dx, len) — textbook Wilder ADX uses RMA. Both variants are computed so
     we can tell whether any result belongs to ADX or to the deviation.
  2. it has no warmup guard: the smTR/smDM accumulators start at 0, so the first ~len bars
     of every ticker are wrong. Harmless on a chart, contamination in a backtest. Masked here.

DECIDING FIRST TEST: agreement with the gates we already own (hurst, conso). If ADX regime
is >80% the same call as hurst, it adds nothing and we stop — that is the trap three earlier
searches fell into (a new name for something already in the book).
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

LEN = 14
ADX_TREND, ADX_RANGE = 25, 20
WARMUP = 3 * LEN                      # accumulators need ~3x len to settle

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}", flush=True)


def wilder_adx(h, l, c, n=LEN, smooth="rma"):
    """Wilder TR/+DM/-DM with the script's accumulator form; ADX via RMA (correct) or SMA."""
    pc = np.roll(c, 1); pc[0] = c[0]
    ph = np.roll(h, 1); ph[0] = h[0]
    pl = np.roll(l, 1); pl[0] = l[0]
    tr = np.maximum(np.maximum(h - l, np.abs(h - pc)), np.abs(l - pc))
    up, dn = h - ph, pl - l
    dmp = np.where((up > dn) & (up > 0), up, 0.0)
    dmm = np.where((dn > up) & (dn > 0), dn, 0.0)
    # Wilder accumulation: x - x/n + new
    def acc(x):
        out = np.zeros(len(x)); s = 0.0
        for i in range(len(x)):
            s = s - s / n + x[i]
            out[i] = s
        return out
    sTR, sP, sM = acc(tr), acc(dmp), acc(dmm)
    with np.errstate(invalid="ignore", divide="ignore"):
        dip = np.where(sTR != 0, sP / sTR * 100, 0.0)
        dim = np.where(sTR != 0, sM / sTR * 100, 0.0)
        s = dip + dim
        dx = np.where(s != 0, np.abs(dip - dim) / s * 100, 0.0)
    ser = pd.Series(dx)
    adx = (ser.ewm(alpha=1 / n, adjust=False).mean() if smooth == "rma"
           else ser.rolling(n).mean()).to_numpy()
    return adx, dip, dim


print("computing ADX on the frame...", flush=True)
for tk, g in grp.items():
    h = g["high"].to_numpy(float); l = g["low"].to_numpy(float); c = g["close"].to_numpy(float)
    for sm, tag in [("rma", ""), ("sma", "_sma")]:
        adx, dip, dim = wilder_adx(h, l, c, smooth=sm)
        ok = np.arange(len(g)) >= WARMUP
        reg = np.zeros(len(g), dtype=np.int8)            # 0 transition
        reg = np.where((adx >= ADX_TREND) & (dip > dim), 1, reg)   # trend up
        reg = np.where((adx >= ADX_TREND) & (dim > dip), 2, reg)   # trend down
        reg = np.where(adx <= ADX_RANGE, 3, reg)                   # range
        reg = np.where(ok, reg, -1)                                # warmup masked
        g[f"adx{tag}"] = adx
        g[f"adxreg{tag}"] = reg
print("done", flush=True)

# ── 1. DECIDING TEST: is this new information? ─────────────────────────────────
print("\n===== 1. agreement with the gates we already own =====", flush=True)
rows = []
for tk, g in grp.items():
    m = g["adxreg"] >= 0
    if not m.any():
        continue
    rows.append(pd.DataFrame({
        "reg": g["adxreg"][m], "reg_sma": g["adxreg_sma"][m], "adx": g["adx"][m],
        "hurst": g["hurst"][m] if "hurst" in g else np.nan,
        "conso": g["conso"][m] if "conso" in g else np.nan,
    }))
X = pd.concat(rows, ignore_index=True)
print(f"  bars with a valid ADX regime: {len(X):,}", flush=True)
print(f"  regime mix: {X['reg'].value_counts(normalize=True).mul(100).round(1).to_dict()} "
      f"(0=transition 1=up 2=down 3=range)", flush=True)
print(f"  RMA vs SMA variant agreement: {(X['reg']==X['reg_sma']).mean()*100:.1f}%", flush=True)
if X["hurst"].notna().any():
    h = X["hurst"]
    hur_trend = h > 0.55
    adx_trend = X["reg"].isin([1, 2])
    agree = (hur_trend == adx_trend).mean() * 100
    print(f"  ADX-trend vs Hurst>0.55 agreement: {agree:.1f}%  "
          f"(corr adx~hurst {X['adx'].corr(h):+.3f})", flush=True)
if X["conso"].notna().any():
    rng_adx = X["reg"] == 3
    print(f"  ADX-range vs conso agreement: {(rng_adx == X['conso'].astype(bool)).mean()*100:.1f}%",
          flush=True)

# ── 2. the actual hypothesis ────────────────────────────────────────────────────
REV = [("QZC", "E_qzcapit"), ("WSH", "E_washout"), ("SPR", "E_spring"),
       ("D+L1", "E_dl1"), ("CF", "E_coilfloor")]
MOM = [("G3", "E_g3"), ("G3A", "E_g3abs"), ("ATM", "E_atomic"), ("L43", "E_l43triple")]
g0 = next(iter(grp.values()))
REV = [(n, c) for n, c in REV if c in g0]
MOM = [(n, c) for n, c in MOM if c in g0]
N_TRIALS = (len(REV) + len(MOM) + 2) * 4
print(f"\nPRE-SPECIFIED TRIAL COUNT: {N_TRIALS}", flush=True)

fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))

REGNAME = {1: "TREND-UP", 2: "TREND-DN", 3: "RANGE", 0: "TRANSITION"}


def by_regime(pairs, label):
    frames = []
    for n, col in pairs:
        for tk, g in grp.items():
            pass
        tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
        tr["edge"] = n
        frames.append(tr)
    T = pd.concat(frames, ignore_index=True)
    T["d"] = pd.to_datetime(T["date_in"]).astype(str).str[:10]
    # attach the regime of the SIGNAL bar (= the bar before entry)
    lut = {}
    for tk, g in grp.items():
        dd = pd.to_datetime(g["date"]).astype(str).str[:10].to_numpy()
        rr = g["adxreg"].to_numpy()
        nxt = np.roll(dd, -1); nxt[-1] = dd[-1]
        for i in range(len(dd) - 1):
            lut[(tk, nxt[i])] = rr[i]
    T["reg"] = [lut.get((t, d), -1) for t, d in zip(T["ticker"], T["d"])]
    print(f"\n===== {label} (n={len(T):,}) =====", flush=True)
    base = T["ret"].median() * 100
    print(f"  {'regime':12s} {'n':>7s} {'med':>7s} {'Δ':>6s} {'win':>5s} {'pf':>5s} "
          f"{'yrs':>5s} {'worst':>7s} {'DSR':>6s}", flush=True)
    for r in [1, 3, 2, 0]:
        sub = T[T["reg"] == r]
        if len(sub) < 200:
            print(f"  {REGNAME[r]:12s} n={len(sub)} thin", flush=True); continue
        ym = sub.groupby("yr")["ret"].median() * 100
        w = sub["ret"] > 0
        den = -sub.loc[~w, "ret"].sum()
        pf = (sub.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
        d = ofs.dsr(sub["ret"].to_numpy(), fam, n_trials=N_TRIALS)
        print(f"  {REGNAME[r]:12s} {len(sub):>7,} {sub['ret'].median()*100:>+7.2f} "
              f"{sub['ret'].median()*100-base:>+6.2f} {w.mean()*100:>5.1f} {pf:>5.2f} "
              f"{int((ym>0).sum())}/{len(ym)} {ym.min():>+7.2f} {d['dsr']:>6.3f}", flush=True)
    return T


Trev = by_regime(REV, "REVERSAL family — does RANGE help?")
Tmom = by_regime(MOM, "MOMENTUM/gap family — does TREND-UP help?")

print("\n===== 3. per-edge deltas (best regime vs the edge's own base) =====", flush=True)
for T, lab in [(Trev, "REV"), (Tmom, "MOM")]:
    for e, sub in T.groupby("edge"):
        b = sub["ret"].median() * 100
        line = []
        for r in [1, 3]:
            s2 = sub[sub["reg"] == r]
            if len(s2) >= 100:
                line.append(f"{REGNAME[r]} {s2['ret'].median()*100-b:+.2f}")
        if line:
            print(f"  {lab} {e:6s} base {b:+.2f} → " + " · ".join(line), flush=True)

print("\nDONE", flush=True)
