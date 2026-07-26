"""
validate_trendlines.py — do descending-trendline BREAKOUTS (cross from below to
above) and their RETESTS carry forward edge? Walk-forward honest: a line only
exists once its 2nd anchor pivot is CONFIRMED (k bars later); the event is the
FIRST close above the line after that. No future pivots used.

Events (per ticker, deduped per day):
    CROSS    first close > line·(1+pen) of a respected descending resistance
             (≥3 touches before the break, line held ≥10 bars past anchor2)
    RETEST   within 40 bars after CROSS: low back to line·(1±tol) AND close holds above
Context slices: RSI<50 / ≥50, touches≥4, steep vs gentle slope.
Path-sim trail25/60 (gap-realistic, stop-first, entry next open) per-year.
Expectation from prior research: raw breakout ≈ no edge; retest-with-context is
where zone edge lived. READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats
from validate_mtf_ema import _daily_universe

K = 4; MIN_SPAN = 25; MIN_TOUCH = 3; HOLD_AFTER_J = 10; RETEST_WIN = 40
TOL_PCT = 0.012; PEN_PCT = 0.007
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pivots_high(h: np.ndarray, k: int) -> list[int]:
    n = len(h); out = []
    for i in range(k, n - k):
        if h[i] >= h[i - k:i + k + 1].max():
            if out and i - out[-1] <= k and h[i] == h[out[-1]]:
                out[-1] = i
            else:
                out.append(i)
    return out


def _events(df: pd.DataFrame):
    h = df.high.to_numpy(float); c = df.close.to_numpy(float); l = df.low.to_numpy(float)
    n = len(c)
    piv = _pivots_high(h, K)
    cross, retest, cr_meta = set(), set(), {}
    for ai in range(len(piv)):
        for bi in range(ai + 1, len(piv)):
            i, j = piv[ai], piv[bi]
            if j - i < MIN_SPAN or h[i] <= 0 or h[j] >= h[i]:   # descending only
                continue
            slope = (h[j] - h[i]) / (j - i)
            s0 = j + K                                          # anchor2 confirmed
            if s0 >= n - 1:
                continue
            t = np.arange(s0, n)
            line = h[i] + slope * (t - i)
            if line[-1] <= 0:
                continue
            above = c[s0:] > line * (1 + PEN_PCT)
            idx = np.flatnonzero(above)
            if len(idx) == 0:
                continue
            b = s0 + int(idx[0])
            if b - j < HOLD_AFTER_J:                            # broke immediately → not respected
                continue
            tline = h[i] + slope * (np.arange(i, b) - i)
            touches = int((np.abs(h[i:b] - tline) <= tline * TOL_PCT).sum())
            if touches < MIN_TOUCH:
                continue
            steep = abs(slope) / max(h[i], 1e-9) * 252 > 0.35    # >35%/yr decline rate
            if b not in cross:
                cross.add(b); cr_meta[b] = {"touches": touches, "steep": steep}
            # retest: back to the (now broken) line and HOLD above
            for r in range(b + 1, min(b + 1 + RETEST_WIN, n)):
                lv = h[i] + slope * (r - i)
                if l[r] <= lv * (1 + TOL_PCT) and c[r] > lv:
                    retest.add(r); break
    return cross, retest, cr_meta


def main():
    t0 = time.time()
    daily = _daily_universe()
    print(f"daily rows {len(daily):,} · tickers {daily.ticker.nunique():,}", flush=True)
    cols = ["CROSS", "CROSS_rsi_lo", "CROSS_rsi_hi", "CROSS_t4", "CROSS_steep", "CROSS_gentle", "RETEST"]
    parts = []
    done = 0
    for tk, g in daily.groupby("ticker", sort=False):
        g = g.reset_index(drop=True)
        for col in cols:
            g[col] = False
        if len(g) >= 120 and (g["supp"] == 0).any():
            cross, retest, meta = _events(g)
            rsi = g.rsi_14.to_numpy(float)
            supp = g.supp.to_numpy(int)
            for b in cross:
                if supp[b]:
                    continue
                g.loc[b, "CROSS"] = True
                g.loc[b, "CROSS_rsi_lo" if rsi[b] < 50 else "CROSS_rsi_hi"] = True
                if meta[b]["touches"] >= 4:
                    g.loc[b, "CROSS_t4"] = True
                g.loc[b, "CROSS_steep" if meta[b]["steep"] else "CROSS_gentle"] = True
            for r in retest:
                if not supp[r]:
                    g.loc[r, "RETEST"] = True
        parts.append(g)
        done += 1
        if done % 500 == 0:
            print(f"  {done} tickers ({(time.time()-t0)/60:.1f}min)", flush=True)
    m = pd.concat(parts, ignore_index=True)
    grp = {tk: g.reset_index(drop=True) for tk, g in m.groupby("ticker", sort=False)}
    print(f"events: CROSS {int(m.CROSS.sum()):,} · RETEST {int(m.RETEST.sum()):,} ({(time.time()-t0)/60:.1f}min)\n", flush=True)

    print("trail25/60 · gap-realistic · entry next open · dv≥3M · suppressor-clean\n")
    for k in cols:
        s = _stats(k, _pathsim(grp, k, "trail", 0.10, 0.25, 0.25, 60))
        if not s or s.get("n", 0) == 0:
            print(f"  {k:14s} n=0"); continue
        py = s["per_year"]
        yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
        print(f"  {k:14s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} win{s['win']:4.1f} "
              f"pf{str(s['pf']):>4} y{s['pos_years']}/{s['total_years']} | {yr}")
    print(f"\ndone {(time.time()-t0)/60:.1f}min")


if __name__ == "__main__":
    main()
