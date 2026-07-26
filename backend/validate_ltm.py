"""
validate_ltm.py — universe path-sim of the LTM (Liquidity Trail Matrix) core signal.

Replicates the indicator's Balanced defaults:
  · 4-band ATR SuperTrend: base 4.0, step 0.25 → m=[4,5,6,7]×ATR(13), src=close
  · flip when close crosses Band 3 (prev bar)
  · retest: price dips into band stack (depth 1-4), then reclaims Band 1 with a
    directional candle (long: close>ts1 & close>open) within 8 bars; cooldown 5
  · score 0-100 = depth(25/18/15/10) + candle(20/12/5) + age(15/8/5) +
    vol(20/12/5) + HTF-bias(20/10/0); default entry gate score>=80
LONG signals only (equity long universe). Entry = NEXT open (realistic).

Two exit models:
  A) our trail25 stop-first (comparable to Edge Replay setups)
  B) the LTM risk model itself — wick SL, BE after TP1, exit at SL/BE/TP3 →
     outcome ∈ {-1R, ~0R scratch, +3R}. Reports THEIR win (TP1 touched) vs
     HONEST win (net positive), to quantify the win-rate inflation.
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB

M = [4.0, 5.0, 6.0, 7.0]; ATRL = 13; FLIP = 2   # band index 2 == Band 3
RETEST_WIN = 8; COOLDOWN = 5; SLIP = 0.0015
ATRL_RISK = 14; TP1R, TP3R = 1.0, 3.0


def wilder_atr(h, l, c, n):
    pc = np.roll(c, 1); pc[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    atr = np.empty_like(tr); atr[0] = tr[0]
    a = 1.0 / n
    for i in range(1, len(tr)):
        atr[i] = atr[i - 1] + a * (tr[i] - atr[i - 1])
    return atr


def signals_for(o, h, l, c, atr, atrR, ema50):
    """Return list of (idx, depth, score) for confirmed LONG retests."""
    n = len(c)
    ts = [np.nan] * 4
    trend = 1; tstart = 0
    pend = 0; pdepth = 0; last_sig = -10000
    volsma = None
    out = []
    ts_prev_flipband = np.nan
    for i in range(n):
        src = c[i]
        up = [src - atr[i] * m for m in M]
        lo = [src + atr[i] * m for m in M]
        prev_trend = trend
        flipPrev = ts_prev_flipband
        if trend == 1:
            if not np.isnan(flipPrev) and src < flipPrev:
                trend = -1; ts = lo[:]
            else:
                ts = [max(up[k], ts[k]) if not np.isnan(ts[k]) else up[k] for k in range(4)]
        else:
            if not np.isnan(flipPrev) and src > flipPrev:
                trend = 1; ts = up[:]
            else:
                ts = [min(lo[k], ts[k]) if not np.isnan(ts[k]) else lo[k] for k in range(4)]
        ts_prev_flipband = ts[FLIP]           # for NEXT bar's flip check
        flipBar = trend != prev_trend
        if flipBar:
            tstart = i; pend = 0; pdepth = 0
        barsIn = i - tstart
        pend = max(pend - 1, 0)
        if pend == 0:
            pdepth = 0
        # touch depth (long)
        depth = 0
        if trend == 1 and not np.isnan(ts[0]):
            depth = 4 if l[i] <= ts[3] else 3 if l[i] <= ts[2] else 2 if l[i] <= ts[1] else 1 if l[i] <= ts[0] else 0
        if depth > 0:
            pend = RETEST_WIN; pdepth = max(pdepth, depth)
        reclaim = pend > 0 and trend == 1 and not np.isnan(ts[0]) and c[i] > ts[0] and c[i] > o[i]
        if reclaim and (i - last_sig >= COOLDOWN) and i >= max(ATRL * 3, 60):
            rng = h[i] - l[i]
            clr = (c[i] - l[i]) / rng if rng > 0 else 0.5
            dp = {2: 25, 3: 18, 1: 15, 4: 10}.get(pdepth, 0)
            cp = 20 if clr > 0.7 else 12 if clr > 0.5 else 5
            ap = 15 if 10 <= barsIn <= 150 else 8 if barsIn < 10 else 5
            vb = volsma if volsma is not None else 0.0
            vp = 5
            bias = 1 if (i > 0 and not np.isnan(ema50[i - 1]) and c[i - 1] > ema50[i - 1]) else -1
            bp = 20 if bias == 1 else 0
            score = dp + cp + ap + vp + bp   # vol filled below per-row
            out.append((i, pdepth, dp, cp, ap, bp, clr))
            last_sig = i; pend = 0; pdepth = 0
    return out


def main():
    t0 = time.time()
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    df = a.execute("""SELECT ticker, CAST(date AS VARCHAR) d, open, high, low, close, volume FROM (
      SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
      WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    a.close()
    print(f"loaded {len(df):,} rows ({time.time()-t0:.0f}s)")

    rows = []   # per-signal record
    for tk, g in df.groupby("ticker", sort=False):
        if len(g) < 80:
            continue
        o = g.open.to_numpy(float); h = g.high.to_numpy(float); l = g.low.to_numpy(float)
        c = g.close.to_numpy(float); v = g.volume.to_numpy(float)
        dts = g.d.to_numpy(); n = len(c)
        atr = wilder_atr(h, l, c, ATRL); atrR = wilder_atr(h, l, c, ATRL_RISK)
        ema50 = pd.Series(c).ewm(span=50, adjust=False).mean().to_numpy()
        vsma = pd.Series(v).rolling(20).mean().shift(1).to_numpy()
        sigs = signals_for(o, h, l, c, atr, atrR, ema50)
        for (i, pdepth, dp, cp, ap, bp, clr) in sigs:
            if i + 1 >= n:
                continue
            # volume pts
            vb = vsma[i]
            vp = (20 if v[i] > vb * 1.2 else 12 if v[i] > vb else 5) if (vb and vb > 0) else 12
            score = dp + cp + ap + bp + vp
            ep = o[i + 1] * (1 + SLIP)                    # realistic next-open entry
            if ep <= 0 or (c[i] * v[i]) < 2_000_000 or c[i] < 3:
                continue
            # --- A) trail25 stop-first ---
            pk = ep; retT = None; end = min(i + 1 + 60, n)
            for j in range(i + 1, end):
                tsl = pk * 0.75
                if j > i + 1 and o[j] <= tsl:
                    retT = o[j] / ep - 1 - SLIP; break
                pk = max(pk, h[j]); ts_ = pk * 0.75
                if l[j] <= ts_:
                    retT = ts_ / ep - 1 - SLIP; break
            if retT is None:
                retT = c[end - 1] / ep - 1 - SLIP
            # --- B) LTM risk model: wick SL, BE after TP1, exit SL/BE/TP3 ---
            slw = min(l[i] - atrR[i] * 0.25, c[i] - atrR[i] * 0.5)
            risk = ep - slw
            outcomeR = None; tp1_touched = False
            if risk > 0:
                tp1 = ep + risk * TP1R; tp3 = ep + risk * TP3R; sl = slw; be = False
                for j in range(i + 1, min(i + 1 + 120, n)):
                    # SL-first pessimistic
                    if l[j] <= sl:
                        outcomeR = 0.0 if be else -1.0; break
                    if not tp1_touched and h[j] >= tp1:
                        tp1_touched = True; be = True; sl = ep       # move to BE
                    if h[j] >= tp3:
                        outcomeR = 3.0; break
                if outcomeR is None:
                    outcomeR = (c[min(i + 1 + 120, n) - 1] - ep) / risk
            rows.append({"tk": tk, "yr": dts[i][:4], "score": score, "depth": pdepth,
                         "trail": retT * 100, "R": outcomeR, "tp1": tp1_touched})
    R = pd.DataFrame(rows)
    print(f"\nsignals: {len(R):,}  ({time.time()-t0:.0f}s)")
    for thr in [0, 60, 70, 80]:
        s = R[R.score >= thr]
        if len(s) < 50:
            continue
        tr = s.trail.values
        pf = tr[tr > 0].sum() / abs(tr[tr < 0].sum())
        their_win = s.tp1.mean() * 100
        honest_win = (s.R > 0.5).mean() * 100          # actually reached +3R (real win)
        scratch = ((s.R > -0.5) & (s.R <= 0.5)).mean() * 100
        print(f"\n=== score>={thr}  n={len(s):,} ===")
        print(f"  trail25: mean {tr.mean():+.2f}% median {np.median(tr):+.2f}% win {(tr>0).mean()*100:.0f}% PF {pf:.2f}")
        print(f"  LTM-model: meanR {s.R.mean():+.2f}  THEIR win(TP1 touched) {their_win:.0f}%  vs  HONEST win(+3R) {honest_win:.0f}%  scratch(BE) {scratch:.0f}%")
        yr = s.groupby("yr")["trail"].median()
        print("  trail25 per-year median: " + "  ".join(f"{y[2:]}:{val:+.1f}" for y, val in yr.items()))
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
