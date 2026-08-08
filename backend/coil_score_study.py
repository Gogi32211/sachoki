"""COIL V4 — is a GRADED compression score better than our binary 🧊CONSO gate? (2026-08-08)

From the user's own Pine "260703 V4 COIL MTF Screener". Its FIRE half (breakout above the
coil top on 1.2x volume) is already refuted twice in this book — Zanger decompose said the
breakout is the worst of three entries and 1.5-2x volume is the worst band, and 🕯️ mid-close
said the "strong breakout" colour rule runs backwards. So FIRE is tested here ONLY as a
control: an independent implementation should reproduce the refutation, or we learn something.

What is genuinely new is the DETECTION half. Our `conso` measures the LEVEL of compression
(6-bar range<=3.5% OR ATR%<=3 OR ema-spread<=2%) and fires on 69% of bars — a regime whose
value is its inverse (NOT-CONSO = -3.67). The script measures other axes:
  BLOCK  — the TRAJECTORY of compression: 3 consecutive 5-bar blocks with monotonically
           shrinking ranges (r0 <= r1*0.9 <= r2*0.9, and r0 <= r2*0.7). Level != trajectory.
  FLAG   — compression RELATIVE TO the prior impulse (priorMove >= coilRange*1.5)
  INSIDE — closes inside a large mother bar, with an ATR wick-poke tolerance (>=3 bars)
  PIVOT  — swing width shrinking vs the previous swing (script calls it a bonus vote)
and sums them into a 0-4 SCORE, which hands us a free plateau test.

DECIDING FIRST TEST (pre-specified): agreement of score>=2 with `conso`. >80% => close it,
exactly as WaveTrend was closed this session. Only if independent do we test whether the
GRADE is monotone on the setups CONSO already helps.

NOTE on PIVOT: the pasted script was truncated inside the INSIDE block, so INSIDE and PIVOT
are reconstructed from the documented parameters (motherATR/pokeTolATR/minInside, pLen/
coilFrac). Both are lag-free here: a pivot is only used pLen bars AFTER it forms.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

# ── script parameters, verbatim from the Pine inputs ───────────────────────────
BLOCK_LEN, BLOCK_FRAC, TOTAL_FRAC = 5, 0.9, 0.7
COIL_N, TIGHT_ATR, IMP_MULT = 10, 2.5, 1.5
MOTHER_ATR, POKE_TOL, MIN_INSIDE = 1.5, 0.15, 3
P_LEN, COIL_FRAC = 3, 0.8
FIRE_WIN, FIRE_VOL = 12, 1.2

N_TRIALS = 20          # 5 setups x 4 score levels — pre-specified, no peeking
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp)} tickers", flush=True)


def pivot_vote(h, l, n=P_LEN, frac=COIL_FRAC):
    """swing width (last confirmed PH - last confirmed PL) <= frac x previous swing width.
    A pivot at i-n is only CONFIRMED at bar i, so nothing here looks ahead."""
    N = len(h)
    out = np.zeros(N, dtype=bool)
    ph1 = ph2 = pl1 = pl2 = np.nan
    for i in range(2 * n, N):
        c = i - n
        if h[c] == h[c - n:i + 1].max():
            ph2, ph1 = ph1, h[c]
        if l[c] == l[c - n:i + 1].min():
            pl2, pl1 = pl1, l[c]
        if not (np.isnan(ph1) or np.isnan(ph2) or np.isnan(pl1) or np.isnan(pl2)):
            w1, w2 = ph1 - pl1, ph2 - pl2
            if w2 > 0 and w1 > 0 and w1 <= w2 * frac:
                out[i] = True
    return out


print("computing COIL votes...", flush=True)
for tk, g in grp.items():
    h = g["high"].astype(float); l = g["low"].astype(float); c = g["close"].astype(float)
    hn, ln, cn = h.to_numpy(), l.to_numpy(), c.to_numpy()
    atr14 = g["atr_14"].astype(float)
    atr50 = (h - l).rolling(50, min_periods=25).mean()          # proxy for ta.atr(50)

    # ── BLOCK: three consecutive 5-bar blocks, shrinking ranges
    hb = h.rolling(BLOCK_LEN).max(); lb = l.rolling(BLOCK_LEN).min()
    r0 = hb - lb
    r1 = r0.shift(BLOCK_LEN)
    r2 = r0.shift(BLOCK_LEN * 2)
    mono = (r2 > 0) & (r1 <= r2 * BLOCK_FRAC) & (r0 <= r1 * BLOCK_FRAC) & (r0 <= r2 * TOTAL_FRAC)
    tight = (r2 > 0) & (r0 <= r2 * TOTAL_FRAC) & (r0 <= r1)
    v_block = (mono | tight).fillna(False)

    # ── FLAG: tight coil, small vs the prior impulse
    coil_rng = h.rolling(COIL_N).max() - l.rolling(COIL_N).min()
    prior = (c.shift(COIL_N) - c.shift(COIL_N * 2)).abs()
    v_flag = ((coil_rng <= atr50 * TIGHT_ATR) & (coil_rng > 0)
              & (prior >= coil_rng * IMP_MULT)).fillna(False)

    # ── INSIDE (soft): >=3 closes inside a big mother bar, wick pokes tolerated
    rng = h - l
    v_ins = pd.Series(False, index=g.index)
    for k in range(MIN_INSIDE, 11):
        cmax = c.rolling(k).max(); cmin = c.rolling(k).min()
        tol = atr14.shift(k) * POKE_TOL
        ok = ((rng.shift(k) >= atr14.shift(k) * MOTHER_ATR)
              & (cmax <= h.shift(k) + tol) & (cmin >= l.shift(k) - tol))
        v_ins = v_ins | ok.fillna(False)

    # ── PIVOT (bonus vote)
    v_piv = pd.Series(pivot_vote(hn, ln), index=g.index)

    g["v_block"] = v_block.to_numpy()
    g["v_flag"] = v_flag.to_numpy()
    g["v_inside"] = v_ins.to_numpy()
    g["v_pivot"] = v_piv.to_numpy()
    g["coil_score"] = (v_block.astype(int) + v_flag.astype(int)
                       + v_ins.astype(int) + v_piv.astype(int)).to_numpy()
    # FIRE control: close breaks the prior coil top on volume
    top = h.rolling(COIL_N).max().shift(1)
    vsma = g["volume"].astype(float).rolling(20).mean()
    coil_recent = (g["coil_score"] >= 2).rolling(FIRE_WIN, min_periods=1).max().astype(bool)
    g["coil_fire"] = ((c > top) & (g["volume"].astype(float) >= vsma * FIRE_VOL)
                      & coil_recent).fillna(False).to_numpy()
print("done", flush=True)

# ── 1. DECIDING TEST ───────────────────────────────────────────────────────────
print("\n===== 1. is the COIL score NEW information vs our 🧊CONSO? =====", flush=True)
rows = []
for tk, g in grp.items():
    m = g["coil_score"].notna() & g["conso"].notna()
    if not m.any():
        continue
    rows.append(pd.DataFrame({
        "s": g["coil_score"][m], "conso": g["conso"][m].astype(bool),
        "blk": g["v_block"][m], "flg": g["v_flag"][m],
        "ins": g["v_inside"][m], "piv": g["v_pivot"][m],
    }))
X = pd.concat(rows, ignore_index=True)
print(f"  bars: {len(X):,}", flush=True)
print("  score mix: " + " · ".join(
    f"{k}:{v*100:.1f}%" for k, v in X["s"].value_counts(normalize=True).sort_index().items()),
    flush=True)
print(f"  conso fires on {X['conso'].mean()*100:.1f}% of bars", flush=True)
s2 = X["s"] >= 2
print(f"  score>=2 fires on {s2.mean()*100:.1f}% · agreement with conso: "
      f"{(s2 == X['conso']).mean()*100:.1f}%", flush=True)
for lv in [1, 2, 3]:
    sl = X["s"] >= lv
    print(f"    score>={lv}: {sl.mean()*100:>5.1f}% of bars · agree {(sl==X['conso']).mean()*100:>5.1f}%"
          f" · of these {X.loc[sl,'conso'].mean()*100:>5.1f}% are also conso", flush=True)
print("  per-vote share & conso-agreement:", flush=True)
for cn in ["blk", "flg", "ins", "piv"]:
    v = X[cn].astype(bool)
    print(f"    {cn}: fires {v.mean()*100:>5.1f}% · agree with conso {(v==X['conso']).mean()*100:>5.1f}%",
          flush=True)

# ── family for DSR ─────────────────────────────────────────────────────────────
fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
print(f"\nboard family: {len(fam)} setups", flush=True)


def run(col, label, lo=None, hi=None):
    if lo is not None:
        for tk, g in grp.items():
            g["_B"] = g[col] & g["close"].between(lo, hi)
        col = "_B"
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 80:
        print(f"  {label:34s} n={len(tr)} thin", flush=True); return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=N_TRIALS)
    med = tr["ret"].median() * 100
    print(f"  {label:34s} n={len(tr):>6d} med{med:>+7.2f} win{w.mean()*100:>5.1f} "
          f"pf{pf:>5.2f} | {int((ym>0).sum())}/{len(ym)}yr worst{ym.min():>+6.2f} DSR{d['dsr']:>6.3f}",
          flush=True)
    return dict(med=med, n=len(tr), worst=ym.min(), yrs=int((ym > 0).sum()), dsr=d["dsr"])


# ── 2. is the GRADE monotone on the setups CONSO helps? ────────────────────────
BASE = [("Washout", "E_washout"), ("RTB-Base", "E_rtb_base"), ("Spring", "E_spring"),
        ("QZ-Capit", "E_qzcapit"), ("Coil-Floor", "E_coilfloor")]
g0 = next(iter(grp.values()))
BASE = [(n, c) for n, c in BASE if c in g0]

print("\n===== 2. graded score vs binary conso, per setup =====", flush=True)
summary = []
for nm, col in BASE:
    print(f"\n  -- {nm} --", flush=True)
    b = run(col, "base (no gate)")
    for tk, g in grp.items():
        g["_C"] = g[col] & g["conso"]
    c_ = run("_C", "  + 🧊CONSO (what we own)")
    lev = {}
    for lv in [1, 2, 3]:
        for tk, g in grp.items():
            g["_S"] = g[col] & (g["coil_score"] >= lv)
        lev[lv] = run("_S", f"  + COIL score>={lv}")
    summary.append((nm, b, c_, lev))

print("\n===== 2b. plateau — is the grade monotone? =====", flush=True)
for nm, b, c_, lev in summary:
    if not b:
        continue
    seq = " → ".join(f"{lv}:{(lev[lv]['med'] if lev[lv] else float('nan')):+.2f}" for lv in [1, 2, 3])
    print(f"  {nm:12s} base {b['med']:+.2f} · conso {(c_['med'] if c_ else float('nan')):+.2f} "
          f"| score {seq}", flush=True)

# ── 3. CONTROL: the FIRE trigger (breakout + volume) ───────────────────────────
print("\n===== 3. CONTROL — FIRE (breakout above coil top + 1.2x vol) =====", flush=True)
for tk, g in grp.items():
    g["_base10"] = np.arange(len(g)) % 10 == 0
run("_base10", "BASELINE (10th bar)")
run("coil_fire", "FIRE (script's own trigger)")
for tk, g in grp.items():
    g["_fire_q"] = g["coil_fire"] & (g["close"].between(21, 89))
run("_fire_q", "FIRE $21-89")

# ── 4. price buckets on the best gate ──────────────────────────────────────────
print("\n===== 4. price buckets · Washout + best gate =====", flush=True)
for tk, g in grp.items():
    g["_W2"] = g["E_washout"] & (g["coil_score"] >= 2)
for lo, hi in [(5, 21), (21, 89), (89, 377)]:
    run("_W2", f"Washout+score>=2 ${lo}-{hi}", lo=lo, hi=hi)

print("\nDONE", flush=True)
