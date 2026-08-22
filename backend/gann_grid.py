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
    B   = max(20, floor(barsDiff/6)),  barsDiff = |tH - tL|   (GANN_B_RULE_V1,
                       LEGACY_INTENT_OPERATIONALIZATION — not runtime parity)

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
# bound to GANN_B_RULE_V1 (LEGACY_INTENT_OPERATIONALIZATION): B = max(20, floor(d/6)).
# The env var can only NARROW to the sealed rule; anything else is refused below.
B_RULE = os.environ.get("GANN_B_RULE", "floor")
_SEALED_RULE = "floor"
B_RULES = {
    "floor": lambda d: math.floor(d / AUTO_LEVELS),
    "round": lambda d: int(round(d / AUTO_LEVELS)),
    "trunc": lambda d: int(d / AUTO_LEVELS),
    "exact": lambda d: d / AUTO_LEVELS,          # no integer reduction at all
}


def bars_per_step(bars_diff: int) -> float:
    """B = max(MIN_BARS_PER_STEP, reduce(barsDiff / AUTO_LEVELS))."""
    if B_RULE != _SEALED_RULE:
        raise RuntimeError(
            f"B rule '{B_RULE}' is not the sealed one. GANN_B_RULE_V1 freezes "
            f"'{_SEALED_RULE}' (B = max(20, floor(barsDiff/6))) as a legacy-intent "
            "operationalization; searching alternative reductions is forbidden in V1.")
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
    """One ticker, fully vectorised: grid state and per-family touch flags per decision day.

    The nearest touched line needs no search. Minimising |logL + (i+phase)logM - logTarget|
    over integer i is just rounding the real solution and clamping it into the touched
    range, so the whole family reduces to array arithmetic.
    """
    h, l, c = df.high.to_numpy(), df.low.to_numpy(), df.close.to_numpy()
    n = len(df)
    if n < HIST_LOOKBACK + 2:
        return pd.DataFrame()
    amin = rolling_argmin(l, HIST_LOOKBACK)     # window [d-504 .. d-1] via index d-1
    amax = rolling_argmax(h, HIST_LOOKBACK)
    d = np.arange(HIST_LOOKBACK, n)
    iL, iH = amin[d - 1], amax[d - 1]
    ok = (iL >= 0) & (iH >= 0)
    L, H = l[iL], h[iH]
    ok &= (L > 0) & (H > 0) & (H > L) & (l[d] > 0) & (h[d] >= l[d]) & (c[d - 1] > 0)
    if not ok.any():
        return pd.DataFrame()
    d, iL, iH, L, H = d[ok], iL[ok], iH[ok], L[ok], H[ok]
    M = (H / L) ** (1.0 / AUTO_LEVELS)
    good = M > 1.0
    d, iL, iH, L, H, M = d[good], iL[good], iH[good], L[good], H[good], M[good]
    logL, logM = np.log(L), np.log(M)
    bars_diff = np.abs(iH - iL)
    B = np.maximum(MIN_BARS_PER_STEP,
                   np.floor(bars_diff / AUTO_LEVELS)).astype(float)
    if B_RULE != _SEALED_RULE:                  # the sealed rule, asserted on the vector path
        raise RuntimeError(f"B rule '{B_RULE}' != sealed '{_SEALED_RULE}'")
    dt = (d - iL).astype(float)
    out = dict(di=d, iL=iL, iH=iH, L=L, H=H, M=M, B=B, bars_diff=bars_diff, dt=dt)
    for name, sign in (("asc", +1.0), ("desc", -1.0)):
        phase = sign * dt / B                             # at D
        phase_prev = sign * (dt - 1.0) / B                # the SAME line at D-1
        x_lo = (np.log(l[d]) - logL) / logM - phase
        x_hi = (np.log(h[d]) - logL) / logM - phase
        i0, i1 = np.ceil(x_lo), np.floor(x_hi)
        cnt = np.maximum(0, (i1 - i0 + 1)).astype(int)
        x_tgt = (np.log(c[d - 1]) - logL) / logM - phase  # closest to Close[D-1]
        pick = np.clip(np.round(x_tgt), i0, i1)
        line_prev = np.exp(logL + (pick + phase_prev) * logM)
        line_at_d = np.exp(logL + (pick + phase) * logM)
        out[f"{name}_n"] = cnt
        out[f"{name}_i"] = np.where(cnt > 0, pick, np.nan)
        out[f"{name}_line_prev"] = np.where(cnt > 0, line_prev, np.nan)
        out[f"{name}_line_d"] = np.where(cnt > 0, line_at_d, np.nan)
    G = pd.DataFrame(out)
    G["date"] = df.date.to_numpy()[d]
    G["prev_close"] = c[d - 1]
    G["low_d"], G["high_d"] = l[d], h[d]
    G["ticker"] = df.ticker.iloc[0]
    return G


def events_from_grid(G: pd.DataFrame) -> pd.DataFrame:
    """The three predeclared claims, with onset and direction — both outcome-blind.

    ASC_TOUCH / DESC_TOUCH  an integer line of that family intersects D's High-Low range
    CONFLUENCE_TOUCH        both families touch on the same D

    Onset: a family touch opens a new opportunity only when that family had no touch in
    the previous ONSET_QUIET_BARS sessions, so a week of rubbing along one line is one
    opportunity, not five.

    Direction: the contacted line is evaluated at D-1 and compared with Close[D-1].
    CONFLUENCE has two contacted lines, so it takes the one closer to Close[D-1] in log
    price — the same tie-break the per-family line pick already uses. That extension is
    NOT in the registered decisions and is flagged in the STOP report.
    """
    G = G.sort_values(["ticker", "di"], kind="stable").reset_index(drop=True)
    out = []
    for claim, col in (("GANN_ASC_TOUCH_V1", "asc"), ("GANN_DESC_TOUCH_V1", "desc"),
                       ("GANN_CONFLUENCE_TOUCH_V1", None)):
        if col is None:
            hit = (G.asc_n > 0) & (G.desc_n > 0)
        else:
            hit = G[f"{col}_n"] > 0
        H = G[hit].copy()
        if not len(H):
            continue
        # onset: no touch of this claim within the previous 5 SESSIONS (by bar index)
        H["prev_di"] = H.groupby("ticker").di.shift(1)
        H = H[(H.prev_di.isna()) | (H.di - H.prev_di > ONSET_QUIET_BARS)]
        if col is None:
            da = (np.log(H.asc_line_prev) - np.log(H.prev_close)).abs()
            dd = (np.log(H.desc_line_prev) - np.log(H.prev_close)).abs()
            use_asc = da <= dd
            line_prev = np.where(use_asc, H.asc_line_prev, H.desc_line_prev)
            line_d = np.where(use_asc, H.asc_line_d, H.desc_line_d)
            level_i = np.where(use_asc, H.asc_i, H.desc_i)
            n_lines = H.asc_n + H.desc_n
            side = np.where(use_asc, "ASC", "DESC")
        else:
            line_prev = H[f"{col}_line_prev"].to_numpy()
            line_d = H[f"{col}_line_d"].to_numpy()
            level_i = H[f"{col}_i"].to_numpy()
            n_lines = H[f"{col}_n"].to_numpy()
            side = np.full(len(H), col.upper())
        direction = np.where(H.prev_close.to_numpy() > line_prev, "LONG",
                             np.where(H.prev_close.to_numpy() < line_prev, "SHORT",
                                      "INVALID"))
        out.append(pd.DataFrame(dict(
            ticker=H.ticker.to_numpy(), claim_id=claim, decision_date=H.date.to_numpy(),
            bar_index=H.di.to_numpy(), direction=direction, side=side,
            level_i=level_i, n_lines_touched=n_lines,
            line_at_prev=line_prev, line_at_d=line_d,
            prev_close=H.prev_close.to_numpy(),
            low_d=H.low_d.to_numpy(), high_d=H.high_d.to_numpy(),
            anchor_L=H.L.to_numpy(), anchor_H=H.H.to_numpy(),
            anchor_iL=H.iL.to_numpy(), anchor_iH=H.iH.to_numpy(),
            M=H.M.to_numpy(), B=H.B.to_numpy(), bars_diff=H.bars_diff.to_numpy(),
            dt=H["dt"].to_numpy())))
    E = pd.concat(out, ignore_index=True)
    dup = E.duplicated(["ticker", "claim_id", "decision_date"])
    if dup.any():
        raise RuntimeError(f"opportunity grain broken: {int(dup.sum())} duplicate "
                           "(ticker, claim_id, decision_date) rows")
    return E


def load_ticker(conn, tk):
    return conn.execute("""
        SELECT date, any_value(open) AS "open", any_value(high) AS "high",
               any_value(low) AS "low", any_value(close) AS "close", ? AS ticker
        FROM bars WHERE ticker=? AND universe IN {u}
        GROUP BY date ORDER BY date""".format(u=str(UNIVERSES)), [tk, tk]).fetchdf()


def build_all(conn, cutoff=None, tickers=None, verbose=True):
    tks = tickers or [r[0] for r in conn.execute(
        f"SELECT DISTINCT ticker FROM bars WHERE universe IN {UNIVERSES} "
        "ORDER BY ticker").fetchall()]
    out, t0 = [], time.time()
    for k, tk in enumerate(tks):
        df = load_ticker(conn, tk)
        if cutoff is not None:
            df = df[df.date.astype(str) <= cutoff]
        g = build_ticker(df)
        if len(g):
            out.append(g)
        if verbose and (k + 1) % 500 == 0:
            print(f"  {k+1:,}/{len(tks):,} · {time.time()-t0:.0f}s", flush=True)
    return (pd.concat(out, ignore_index=True) if out else pd.DataFrame()), len(tks)


def main():
    t0 = time.time()
    if B_RULE != _SEALED_RULE:
        raise SystemExit(f"B rule '{B_RULE}' != sealed '{_SEALED_RULE}' — refusing.")
    r = json.load(open("GANN_B_RULE_V1.json"))
    assert r["rule"]["formula"] == "B = max(20, floor(barsDiff / 6))", r["rule"]
    assert r["classification"] == "LEGACY_INTENT_OPERATIONALIZATION"
    ledger = json.load(open("GANN_DESIGN_EXPOSURE_LEDGER_V1.json"))
    exposed = {e["ticker"] for e in ledger["entries"]}

    conn = duckdb.connect(DB1D, read_only=True)
    print(f"lookback {HIST_LOOKBACK} · levels {AUTO_LEVELS} · B = max(20, floor(d/6))",
          flush=True)
    G, n_tk = build_all(conn, verbose=True)
    print(f"grid rows {len(G):,} over {G.ticker.nunique():,} tickers · "
          f"{time.time()-t0:.0f}s", flush=True)
    E = events_from_grid(G)
    E["design_exposed"] = E.ticker.isin(exposed)
    G.to_parquet(OUT_GRID, index=False, compression="zstd")
    E.to_parquet(OUT_EVENTS, index=False, compression="zstd")

    fwd = [c for c in list(E.columns) + list(G.columns)
           if c.startswith(("fwd_", "mfe", "mae", "ret", "mtm"))]
    if fwd:
        raise RuntimeError(f"outcome columns leaked into the X tables: {fwd}")

    per = E.groupby("claim_id").agg(events=("ticker", "size"),
                                    tickers=("ticker", "nunique"),
                                    dates=("decision_date", "nunique"))
    audit = dict(
        spec_id="GANN_GRID_X_AUDIT_V1", family_id="GANN_VIBRATION_GRID_V1",
        status="X_ONLY_PRE_Y",
        construction=dict(hist_lookback=HIST_LOOKBACK, auto_levels=AUTO_LEVELS,
                          min_bars_per_step=MIN_BARS_PER_STEP,
                          b_rule=ART.file_digest("GANN_B_RULE_V1.json"),
                          onset_quiet_bars=ONSET_QUIET_BARS,
                          anchors="both families anchored at the LOW; H and tH calibrate "
                                  "M and B only",
                          levels="integer lattice i in Z, solved analytically; half steps "
                                 "excluded from V1 evidence",
                          pit="grid frozen on data through D-1; D's own H/L/C never enter "
                              "the choice of L, H, tL, tH, M or B"),
        universe=dict(tickers_scanned=n_tk, tickers_with_grid=int(G.ticker.nunique()),
                      grid_rows=int(len(G)),
                      date_range=[str(G.date.min()), str(G.date.max())]),
        claims={c: dict(events=int(r_.events), tickers=int(r_.tickers),
                        dates=int(r_.dates)) for c, r_ in per.iterrows()},
        direction_census=E.groupby(["claim_id", "direction"]).size()
                          .unstack(fill_value=0).to_dict(),
        n_lines_touched=dict(
            median=float(E.n_lines_touched.median()),
            q90=float(E.n_lines_touched.quantile(.9)),
            max=int(E.n_lines_touched.max())),
        anchors=dict(bars_diff_median=float(G.bars_diff.median()),
                     B_median=float(G.B.median()), B_at_floor=float((G.B == 20).mean()),
                     M_median=float(G.M.median()),
                     M_q10=float(G.M.quantile(.1)), M_q90=float(G.M.quantile(.9))),
        design_exposure=dict(ledger=ART.file_digest(
            "GANN_DESIGN_EXPOSURE_LEDGER_V1.json"), excluded_tickers=sorted(exposed),
            events_marked=int(E.design_exposed.sum())),
        grain="(ticker, claim_id, decision_date) — duplicate grain hard-fails",
        y_status="NO OUTCOME COLUMN EXISTS IN THESE TABLES",
        artifacts=dict(grid=os.path.basename(OUT_GRID), events=os.path.basename(OUT_EVENTS),
                       grid_digest=ART.file_digest(OUT_GRID),
                       events_digest=ART.file_digest(OUT_EVENTS)),
        runtime_min=round((time.time() - t0) / 60, 1))
    json.dump(audit, open(AUDIT, "w"), indent=1, default=str)
    print("\nclaims:")
    for c, r_ in per.iterrows():
        print(f"  {c:<26} events {int(r_.events):>8,} · tickers {int(r_.tickers):>5,} · "
              f"dates {int(r_.dates):>5,}")
    print(f"\nGANN_GRID_X_AUDIT_V1 written · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
