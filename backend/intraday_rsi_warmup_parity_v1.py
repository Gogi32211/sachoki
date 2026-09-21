"""INTRADAY_RSI_WARMUP_PARITY_V1 — how much lookback does a Wilder-14 actually need?

update_intraday_db fetches FETCH_DAYS = 15 calendar days and enriches that frame, with the comment
"short warm-up is fine for last 3 days". For a Wilder RMA it is not: the recursion is seeded from
the first bar of whatever frame it is given, and 15 calendar days is ~22 bars on 4H against the ~81
the seed needs to decay. Measured on 2026-09: stored rsi_14 differs from a full-history Wilder by a
median of 9.6 points on 4H (max 45.2) and 0.8 on 1H (max 9.5), while every month through 2026-08 —
the repaired window included — is exact.

This finds the SHORTEST window that reproduces the full-history value, per timeframe, instead of
raising the constant by guess. No vendor calls: slicing the canonical series to the last N calendar
days reproduces exactly what the updater would have fetched.

ACCEPTANCE, frozen before the run:
    PRIMARY   p95 |Δ| <= 0.1  AND  max |Δ| <= 0.5
    STRICT    max |Δ| <= 0.1        (restores the ~0.0 parity production had through 2026-08)
"""
from __future__ import annotations
import os, sys
import duckdb, numpy as np, pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mtf_rev_build import wilder_rsi14

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "data") + os.sep
CANDIDATES = [15, 30, 45, 60, 75, 90]
TARGET_DATES = ["2026-08-05", "2026-08-12", "2026-08-19", "2026-08-26"]   # inside the EXACT-parity era
N_TICKERS = 60
SEED = 20260921
GATE_P95, GATE_MAX, STRICT_MAX = 0.1, 0.5, 0.1
L = lambda c="═", n=92: print(c * n)


def run(tf: str):
    con = duckdb.connect(f"{R}studio_{tf}.duckdb", read_only=True)
    rng = np.random.default_rng(SEED)
    tks = [r[0] for r in con.execute("""SELECT ticker FROM bars
        WHERE CAST(date AS DATE) = DATE '2026-08-26' AND close > 5 GROUP BY 1 ORDER BY ticker""").fetchall()]
    tks = list(rng.choice(tks, size=min(N_TICKERS, len(tks)), replace=False))
    out = {n: [] for n in CANDIDATES}
    bars_per_day = []
    for tk in tks:
        g = con.execute("SELECT date, close FROM bars WHERE ticker=? ORDER BY date", [tk]).fetchdf()
        if len(g) < 600:
            continue
        g["d"] = pd.to_datetime(g.date)
        full = wilder_rsi14(g.close).to_numpy()
        for td in TARGET_DATES:
            t = pd.Timestamp(td)
            idx = g.index[(g.d >= t) & (g.d < t + pd.Timedelta(days=1))]
            if not len(idx):
                continue
            i = int(idx[-1])                       # the session's last bar, as the updater would enrich it
            if not np.isfinite(full[i]):
                continue
            for n in CANDIDATES:
                start = g.d.searchsorted(t + pd.Timedelta(days=1) - pd.Timedelta(days=n))
                sl = g.close.to_numpy()[start:i + 1]
                if n == CANDIDATES[0]:
                    bars_per_day.append(len(sl) / n)
                if len(sl) < 20:
                    continue
                r = wilder_rsi14(pd.Series(sl)).to_numpy()[-1]
                if np.isfinite(r):
                    out[n].append(abs(r - full[i]))
    con.close()
    return out, (np.mean(bars_per_day) if bars_per_day else float("nan"))


if __name__ == "__main__":
    L(); print("INTRADAY_RSI_WARMUP_PARITY_V1 · shortest lookback that reproduces a full-history Wilder-14")
    print(f"  gate: p95 |Δ| <= {GATE_P95} AND max |Δ| <= {GATE_MAX}   ·   strict: max |Δ| <= {STRICT_MAX}"); L()
    pick = {}
    for tf in ("1h", "4h"):
        out, bpd = run(tf)
        print(f"\n  {tf.upper()}   ~{bpd:.1f} bars per calendar day")
        print(f"  {'days':>5s} {'bars':>6s} {'n':>6s} {'median':>9s} {'p95':>9s} {'max':>9s}"
              f" {'exact':>7s} {'<=0.1':>7s} {'<=0.5':>7s}  gate")
        for n in CANDIDATES:
            e = np.array(out[n])
            if not len(e):
                continue
            p95, mx = float(np.percentile(e, 95)), float(e.max())
            ok = p95 <= GATE_P95 and mx <= GATE_MAX
            strict = mx <= STRICT_MAX
            print(f"  {n:>5d} {n*bpd:>6.0f} {len(e):>6,} {np.median(e):>9.3f} {p95:>9.3f} {mx:>9.3f}"
                  f" {100*(e==0).mean():>6.1f}% {100*(e<=0.1).mean():>6.1f}% {100*(e<=0.5).mean():>6.1f}%"
                  f"  {'STRICT PASS' if strict else 'PASS' if ok else 'fail'}")
            if ok and tf not in pick:
                pick[tf] = (n, strict)
    L(); print("SHORTEST WINDOW PASSING THE GATE")
    for tf in ("1h", "4h"):
        if tf in pick:
            n, strict = pick[tf]
            print(f"  {tf.upper()}: {n} calendar days" + ("  (also meets the strict target)" if strict else ""))
        else:
            print(f"  {tf.upper()}: none of {CANDIDATES} passes — a longer window or a canonical seed is required")
    L()
