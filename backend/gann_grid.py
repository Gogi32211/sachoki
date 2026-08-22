"""GANN_VIBRATION_GRID_V1 — X-only construction of the legacy Pine vibration lattice.

The grid used on decision day D is built ONLY from data through D-1. D's own high, low
and close never touch the choice of L, H, tL, tH, M or B — the frozen grid is merely
projected onto D. That is the whole point: a lattice that today's extreme could re-fit
would make every "touch" self-confirming.

    HIST_LOOKBACK      504 trading sessions ending at D-1 (must be complete)
    AUTO_LEVELS        6
    MIN_BARS_PER_STEP  20      — the FLOOR on B, never an anchor-separation filter

    L   = minimum Low in the window,  tL = MOST RECENT occurrence
    H   = maximum High in the window, tH = MOST RECENT occurrence
    M   = (H/L)^(1/6)
    B   = max(20, barsDiff/6),  barsDiff = |tH - tL|      <- integer semantics PENDING

    ascending    P(t,i) = L * M^( i + (t - tL)/B )
    descending   P(t,i) = L * M^( i - (t - tL)/B )

BOTH families are anchored at the LOW. H and tH calibrate M and B only. That is the
legacy Pine hypothesis and it is preserved deliberately; a High-anchored descending
family would be a separate V2, never a silent correction.

Levels are the full integer lattice i in Z. Nothing is enumerated: for a given bar the
integer levels intersecting [Low, High] are solved analytically, so UI rendering limits
(numUpAsc=12, numDnDn=6, ...) cannot become research parameters. Half steps (i+0.5) are
NOT V1 evidence.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, math, os, sys, time                             # noqa: E402
from collections import deque                                        # noqa: E402
import duckdb, numpy as np, pandas as pd                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402

ROOT = os.path.dirname(HERE)
DB1D = os.path.join(ROOT, "data", "studio_analytics.duckdb")
OUT_EVENTS = os.path.join(ROOT, "data", "gann_grid_events_v1.parquet")
OUT_GRID = os.path.join(ROOT, "data", "gann_grid_state_v1.parquet")
AUDIT = os.path.join(HERE, "GANN_GRID_X_AUDIT_V1.json")

HIST_LOOKBACK = 504
AUTO_LEVELS = 6
MIN_BARS_PER_STEP = 20
ONSET_QUIET_BARS = 5
UNIVERSES = ("sp500", "nasdaq", "russell2k")

# ── the one open item: how the legacy Pine reduces barsDiff/6 to an integer ──────
# Pine's finalBarsPerStep is an int. floor(), round() and int() differ on 119/6, 121/6,
# 125/6, 131/6 — so the rule is NOT guessed here. Until the Pine fixture parity is
# sealed, B_RULE stays PENDING and freeze() refuses to run.
B_RULE = os.environ.get("GANN_B_RULE", "PENDING")
B_RULES = {
    "floor": lambda d: math.floor(d / AUTO_LEVELS),
    "round": lambda d: int(round(d / AUTO_LEVELS)),
    "trunc": lambda d: int(d / AUTO_LEVELS),
    "exact": lambda d: d / AUTO_LEVELS,          # no integer reduction at all
}


def bars_per_step(bars_diff: int) -> float:
    """B = max(MIN_BARS_PER_STEP, reduce(barsDiff / AUTO_LEVELS))."""
    if B_RULE not in B_RULES:
        raise RuntimeError(
            "GANN_B_RULE is PENDING: the legacy Pine integer semantics for "
            "finalBarsPerStep must be established by fixture parity before any grid is "
            "built. Set GANN_B_RULE only from a sealed parity artifact.")
    return max(MIN_BARS_PER_STEP, B_RULES[B_RULE](bars_diff))


# ── sliding-window extremes with MOST-RECENT tie-breaking ───────────────────────
def rolling_argmin(vals: np.ndarray, w: int) -> np.ndarray:
    """Index of the window minimum for each position, ties -> MOST RECENT.

    Monotonic deque: strictly-greater elements are evicted, and an incoming EQUAL value
    also evicts the older one, which is exactly the most-recent tie rule.
    """
    n = len(vals)
    out = np.full(n, -1, dtype=np.int64)
    dq = deque()
    for i in range(n):
        while dq and vals[dq[-1]] >= vals[i]:
            dq.pop()
        dq.append(i)
        while dq[0] <= i - w:
            dq.popleft()
        if i >= w - 1:
            out[i] = dq[0]
    return out


def rolling_argmax(vals: np.ndarray, w: int) -> np.ndarray:
    n = len(vals)
    out = np.full(n, -1, dtype=np.int64)
    dq = deque()
    for i in range(n):
        while dq and vals[dq[-1]] <= vals[i]:
            dq.pop()
        dq.append(i)
        while dq[0] <= i - w:
            dq.popleft()
        if i >= w - 1:
            out[i] = dq[0]
    return out


def integer_levels_in_range(logL, logM, phase, lo, hi):
    """Integer i with lo <= L*M^(i+phase) <= hi, solved analytically.

    i ranges over ceil(x_lo - phase) .. floor(x_hi - phase) where x = log(price/L)/log M.
    Returns (i_first, i_last, count) with count 0 when the range holds no lattice point.
    """
    x_lo = (math.log(lo) - logL) / logM - phase
    x_hi = (math.log(hi) - logL) / logM - phase
    i0, i1 = math.ceil(x_lo), math.floor(x_hi)
    return i0, i1, max(0, i1 - i0 + 1)


def build_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """One ticker: grid state per decision day and the raw per-family touch flags."""
    o, h, l, c = (df.open.to_numpy(), df.high.to_numpy(),
                  df.low.to_numpy(), df.close.to_numpy())
    n = len(df)
    if n < HIST_LOOKBACK + 2:
        return pd.DataFrame()
    # window ENDS at D-1: the extremes are computed on bars [D-504 .. D-1]
    amin = rolling_argmin(l, HIST_LOOKBACK)
    amax = rolling_argmax(h, HIST_LOOKBACK)
    rows = []
    for d in range(HIST_LOOKBACK, n):
        j = d - 1                                   # last bar of the frozen window
        iL, iH = amin[j], amax[j]
        if iL < 0 or iH < 0:
            continue
        L, H = l[iL], h[iH]
        if not (L > 0 and H > 0 and H > L):
            continue
        M = (H / L) ** (1.0 / AUTO_LEVELS)
        if not (M > 1.0):
            continue
        logL, logM = math.log(L), math.log(M)
        B = bars_per_step(abs(int(iH) - int(iL)))
        dt = d - iL                                  # Delta t from the LOW anchor
        rows.append((d, iL, iH, L, H, M, B, dt, logL, logM))
    if not rows:
        return pd.DataFrame()
    G = pd.DataFrame(rows, columns=["di", "iL", "iH", "L", "H", "M", "B", "dt",
                                    "logL", "logM"])
    asc, desc = [], []
    for r in G.itertuples():
        d = int(r.di)
        lo, hi = l[d], h[d]
        if not (lo > 0 and hi >= lo):
            asc.append((0, np.nan, np.nan)); desc.append((0, np.nan, np.nan)); continue
        pa = r.dt / r.B
        for phase, bag in ((+pa, asc), (-pa, desc)):
            i0, i1, cnt = integer_levels_in_range(r.logL, r.logM, phase, lo, hi)
            if cnt == 0:
                bag.append((0, np.nan, np.nan)); continue
            # deterministic pick: the touched line whose projected log-price is closest
            # to log(Close[D-1]) — no outcome is consulted
            target = math.log(c[d - 1])
            best_i, best_gap = None, None
            for i in range(i0, i1 + 1):
                lp = r.logL + (i + phase) * r.logM
                gap = abs(lp - target)
                if best_gap is None or gap < best_gap:
                    best_i, best_gap = i, gap
            # the SAME line evaluated at D-1 fixes the direction, again outcome-blind
            phase_prev = ((d - 1) - r.iL) / r.B * (1 if phase > 0 or phase == 0 else -1)
            phase_prev = phase_prev if phase >= 0 else -abs(phase_prev)
            line_prev = math.exp(r.logL + (best_i + phase_prev) * r.logM)
            bag.append((cnt, best_i, line_prev))
    G["asc_n"], G["asc_i"], G["asc_line_prev"] = zip(*asc)
    G["desc_n"], G["desc_i"], G["desc_line_prev"] = zip(*desc)
    G["date"] = df.date.to_numpy()[G.di.to_numpy()]
    G["prev_close"] = c[G.di.to_numpy() - 1]
    G["ticker"] = df.ticker.iloc[0]
    return G


def main():
    t0 = time.time()
    if B_RULE not in B_RULES:
        raise SystemExit(
            "GANN_B_RULE is PENDING — seal the Pine parity fixtures first "
            "(gann_b_parity.py). Refusing to build a grid on a guessed rule.")
    conn = duckdb.connect(DB1D, read_only=True)
    tks = [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM bars WHERE universe IN "
        f"{UNIVERSES} ORDER BY ticker").fetchall()]
    print(f"tickers {len(tks):,} · lookback {HIST_LOOKBACK} · B rule {B_RULE}", flush=True)
    out = []
    for k, tk in enumerate(tks):
        df = conn.execute("""
            SELECT date, any_value(open) AS "open", any_value(high) AS "high",
                   any_value(low) AS "low", any_value(close) AS "close",
                   ? AS ticker
            FROM bars WHERE ticker=? AND universe IN {u}
            GROUP BY date ORDER BY date""".format(u=str(UNIVERSES)),
            [tk, tk]).fetchdf()
        g = build_ticker(df)
        if len(g):
            out.append(g)
        if (k + 1) % 250 == 0:
            print(f"  {k+1:,}/{len(tks):,} · rows {sum(len(x) for x in out):,} · "
                  f"{time.time()-t0:.0f}s", flush=True)
    conn.close()
    G = pd.concat(out, ignore_index=True)
    G.to_parquet(OUT_GRID, index=False, compression="zstd")
    print(f"grid state rows {len(G):,} · {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
