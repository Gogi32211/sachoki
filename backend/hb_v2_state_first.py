"""🏦 research v2 — STATE FIRST, shape second (redesign after the v1 null).

WHY v1 failed (diagnosis, not excuse):
  1. It mined pure SHAPE. The book's central law — proven three times (fractal matching,
     cup geometry, harmonic patterns) — is STATE > SHAPE. Every validated edge we own
     carries an RSI zone / location / RS condition. v1's 5,389 cells carried none.
  2. It targeted the fwd10 MEDIAN. We trade a trailing stop, so what matters is the
     ASYMMETRY (does +10% arrive before −5%?), which a median can hide entirely.
  3. 5,389 hypotheses is self-defeating: sr* grows with the trial count, so the bar was
     unreachable by construction.

v2 design:
  - 6 PRE-SPECIFIED states (nothing chosen after looking)
  - alphabet cut to the codes step-1's permutation test proved informative
  - two targets: median fwd10 AND first-touch asymmetry P(+10 before −5, 20 bars)
  - the trial count is fixed in advance and printed, so DSR has an honest denominator
  - the same grid on the FULL universe as a control — 🏦 is by construction the segment
    with the smallest deviations, so restricting to it may have been self-defeating too
"""
import os, sys
import numpy as np
import pandas as pd
import duckdb
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB

BASE = os.path.dirname(os.path.abspath(__file__))
SEG = pd.read_csv(os.path.join(BASE, "seg_frozen_2123.csv"), index_col=0)["seg_is2123"]
BANK = set(SEG[SEG == "🏦"].index)

con = duckdb.connect(ANALYTICS_DB, read_only=True)
D = con.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt,
           any_value("close") AS cl, any_value(high) AS hi, any_value(low) AS lo,
           any_value(rsi_14) AS rsi,
           coalesce(any_value(t_sig),'') AS t, coalesce(any_value(z_sig),'') AS z,
           coalesce(any_value(l_sig),'') AS l
    FROM bars WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000
    GROUP BY ticker, date ORDER BY ticker, date
""").fetchdf()
con.close()
print(f"full frame {len(D):,} bars · {D['ticker'].nunique()} tickers", flush=True)

tk = D["ticker"].to_numpy()
CL = D["cl"].to_numpy(float); HI = D["hi"].to_numpy(float); LO = D["lo"].to_numpy(float)
RSI = D["rsi"].to_numpy(float)
Y = D["dt"].str[:4].to_numpy()
tz = np.where(D["t"].to_numpy() != "", D["t"].to_numpy(), D["z"].to_numpy())
lsig = np.where(D["l"].to_numpy() != "", D["l"].to_numpy(), "·")

# fwd10 (boundary-safe)
f10 = np.full(len(D), np.nan); f10[:-10] = CL[10:] / CL[:-10] - 1
b = np.zeros(len(D), bool); b[:-10] = tk[10:] != tk[:-10]; f10[b] = np.nan
F = np.clip(f10 * 100, -60, 60)

# rolling helpers per ticker (vectorised via pandas groupby)
g = D.groupby("ticker")["cl"]
ema200 = g.transform(lambda s: s.ewm(span=200, adjust=False).mean()).to_numpy()
low20 = D.groupby("ticker")["lo"].transform(lambda s: s.rolling(20).min()).to_numpy()
red = (D["cl"] < D["cl"].shift(1)) & (D["ticker"] == D["ticker"].shift(1))
red3 = red.rolling(3).sum().to_numpy()

# ── 6 PRE-SPECIFIED states (fixed before looking at any result) ─────────────────
STATES = [
    ("S1 RSI<35",          RSI < 35),
    ("S2 RSI 35-45",       (RSI >= 35) & (RSI < 45)),
    ("S3 ≤5% off 20d-low", CL <= low20 * 1.05),
    ("S4 below EMA200",    CL < ema200),
    ("S5 above EMA200",    CL > ema200),
    ("S6 3+ red closes",   red3 >= 3),
]
# ── alphabet: only the codes step-1's permutation control proved informative ─────
CODES = ["T5", "T12", "T11", "Z6", "Z1G", "Z5", "T2", "T2G", "Z2", "T4"]
LLINES = ["L46", "L5", "L25"]          # the three L-lines that led step-1's L1 table

N_TRIALS = len(STATES) * (len(CODES) + len(LLINES) * 3)   # fixed in advance
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)


def first_touch(idx, up=0.10, dn=0.05, maxh=20):
    """P(+10% before −5%) using real bar paths; ties (same bar) count as the DOWN case."""
    winc = 0; tot = 0
    n = len(CL)
    for i in idx:
        if i + 1 >= n or tk[i] != tk[min(i + maxh, n - 1)]:
            continue
        e = CL[i]
        if not np.isfinite(e) or e <= 0:
            continue
        hit = 0
        for j in range(i + 1, min(i + 1 + maxh, n)):
            if tk[j] != tk[i]:
                break
            if LO[j] <= e * (1 - dn):
                hit = -1; break
            if HI[j] >= e * (1 + up):
                hit = 1; break
        if hit != 0:
            tot += 1; winc += (hit == 1)
    return (winc / tot * 100 if tot else float("nan")), tot


def run(universe_mask, label):
    print(f"\n{'='*86}\n### {label}\n{'='*86}", flush=True)
    base_ok = universe_mask & ~np.isnan(F)
    gb = np.nanmedian(F[base_ok])
    bi = np.flatnonzero(base_ok)
    base_asym, base_n = first_touch(bi[::37])       # sampled baseline (cost control)
    print(f"baseline: med {gb:+.3f} · asymmetry P(+10 before −5) {base_asym:.1f}% "
          f"(n={base_n:,} sampled)\n", flush=True)
    print(f"  {'cell':30s} {'n':>6s} {'med':>7s} {'Δ':>6s} {'yrs':>5s} {'worst':>7s} "
          f"{'asym%':>6s} {'Δasym':>6s}", flush=True)
    rows = []
    for sname, smask in STATES:
        cells = [(c, (tz == c)) for c in CODES]
        for ll in LLINES:
            for c in ["T5", "Z1G", "T2G"]:
                cells.append((f"{c}|{ll}", (tz == c) & (lsig == ll)))
        print(f"  ── {sname} ──", flush=True)
        for cname, cmask in cells:
            m = base_ok & smask & cmask
            n = int(m.sum())
            if n < 150:
                continue
            v = F[m]; ym = pd.Series(v).groupby(pd.Series(Y[m])).median()
            idx = np.flatnonzero(m)
            step = max(1, len(idx) // 4000)
            asym, an = first_touch(idx[::step])
            rows.append(dict(state=sname, cell=cname, n=n, med=float(np.median(v)),
                             pos=int((ym > 0).sum()), ny=len(ym), worst=float(ym.min()),
                             asym=asym, dasym=asym - base_asym))
            flag = ""
            if (ym > 0).sum() >= 5 and ym.min() >= -2 and asym - base_asym >= 5:
                flag = " ✅"
            print(f"    {cname:28s} {n:>6d} {np.median(v):>+7.2f} {np.median(v)-gb:>+6.2f} "
                  f"{int((ym>0).sum())}/{len(ym)} {ym.min():>+7.2f} {asym:>6.1f} "
                  f"{asym-base_asym:>+6.1f}{flag}", flush=True)
    R = pd.DataFrame(rows)
    R.to_csv(os.path.join(BASE, f"hb_v2_{label.split()[0].lower()}.csv"), index=False)
    return R, gb, base_asym


isbank = np.isin(tk, list(BANK))
R1, g1, a1 = run(isbank, "🏦 universe")
R2, g2, a2 = run(np.ones(len(D), bool), "FULL universe (control)")

print("\n\n===== best asymmetry lifts (both universes) =====", flush=True)
for R, lab, ba in [(R1, "🏦", a1), (R2, "FULL", a2)]:
    if R.empty:
        continue
    top = R.sort_values("dasym", ascending=False).head(6)
    print(f"\n— {lab} (baseline asym {ba:.1f}%) —", flush=True)
    for _, r in top.iterrows():
        print(f"  {r['state']:20s} {r['cell']:14s} n={int(r['n']):>6d} "
              f"asym {r['asym']:.1f}% (Δ{r['dasym']:+.1f}) med {r['med']:+.2f} "
              f"yr{int(r['pos'])}/{int(r['ny'])} worst {r['worst']:+.2f}", flush=True)
print("\nDONE", flush=True)
