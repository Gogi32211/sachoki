"""gex_engine.py — Gamma-Exposure levels from an options chain (2026-07-22).

ISOLATED, ADDITIVE. Takes the per-strike OI+gamma chain from data_options.fetch_chain
and derives the dealer-positioning levels the OptionFlow extension shows:

  net_gex        — total dealer gamma ($ per 1% move). Positive = dealers dampen moves
                   (mean-reversion / pinning regime); negative = dealers amplify (trend).
  gamma_flip     — the spot price where cumulative net GEX crosses zero (the regime line).
  power_zone     — strike with the largest ABSOLUTE gamma·OI (the strongest magnet).
  call_wall      — strike with the largest CALL gamma·OI above spot (resistance).
  put_wall       — strike with the largest PUT gamma·OI below spot (support).
  max_pain       — strike minimizing total ITM payout to option holders (pin target).
  atm_iv, ivr    — at-the-money implied vol + a rough IV-rank proxy (needs history later).

Sign convention (standard "naive" dealer book): dealers are net LONG calls and SHORT
puts from customer flow, so per-strike GEX = gamma·OI·100·S²·0.01 with +calls, −puts.
This matches SpotGamma/GEX-style dashboards. Levels are descriptive until we path-sim
whether Edge-near-a-level actually improves forward returns.
"""
from __future__ import annotations
import logging
from typing import Optional

log = logging.getLogger(__name__)


def _num(v):
    try:
        f = float(v)
        return f if f == f else None   # drop NaN
    except (TypeError, ValueError):
        return None


def compute_gex(chain: list[dict], spot: Optional[float]) -> Optional[dict]:
    """chain: list of {strike,type,oi,gamma,iv,dte,...} from data_options.fetch_chain.
    Returns the levels dict, or None if the chain is unusable."""
    if not chain or not spot or spot <= 0:
        return None
    rows = []
    for c in chain:
        k = _num(c.get("strike")); g = _num(c.get("gamma")); oi = c.get("oi")
        if k is None or g is None or oi is None or oi <= 0:
            continue
        typ = c.get("type")
        sign = 1.0 if typ == "call" else -1.0
        # dollar gamma per 1% move: gamma × OI × 100 shares × S² × 0.01
        gex = g * oi * 100.0 * (spot ** 2) * 0.01 * sign
        rows.append({"strike": k, "type": typ, "oi": oi, "gamma": g,
                     "iv": _num(c.get("iv")), "gex": gex,
                     "absg": abs(g) * oi})          # |gamma|·OI magnet strength
    if not rows:
        return None

    net_gex = sum(r["gex"] for r in rows)

    # ── per-strike aggregation ────────────────────────────────────────────────
    by_strike: dict = {}
    for r in rows:
        s = by_strike.setdefault(r["strike"], {"gex": 0.0, "absg": 0.0,
                                               "call_absg": 0.0, "put_absg": 0.0})
        s["gex"] += r["gex"]
        s["absg"] += r["absg"]
        if r["type"] == "call":
            s["call_absg"] += r["absg"]
        else:
            s["put_absg"] += r["absg"]
    strikes = sorted(by_strike)

    # power zone = strongest absolute magnet
    power_zone = max(strikes, key=lambda k: by_strike[k]["absg"])

    # call wall (above spot, strongest call magnet) / put wall (below spot)
    calls_above = [k for k in strikes if k >= spot and by_strike[k]["call_absg"] > 0]
    puts_below = [k for k in strikes if k <= spot and by_strike[k]["put_absg"] > 0]
    call_wall = max(calls_above, key=lambda k: by_strike[k]["call_absg"]) if calls_above else None
    put_wall = max(puts_below, key=lambda k: by_strike[k]["put_absg"]) if puts_below else None

    # ── gamma flip: spot where cumulative GEX from the top crosses zero ────────
    # Walk strikes ascending, accumulate net GEX; the flip is between the strike
    # where cumulative sign changes. Approximated at the crossing strike.
    gamma_flip = _gamma_flip(strikes, by_strike, spot)

    # ── max pain: strike minimizing total payout to holders ───────────────────
    max_pain = _max_pain(rows, strikes)

    # ── ATM IV + rough IVR proxy (front-month ATM; true IVR needs history) ─────
    atm = min(rows, key=lambda r: abs(r["strike"] - spot))
    atm_iv = atm.get("iv")

    # ── directional LEAN from location (NOT from gamma — gamma is regime, not
    # direction). A soft bias combining: max-pain pin pull, wall proximity
    # (put-wall=support/up, call-wall=resistance/down), gamma-flip side. UNVALIDATED
    # context — a lean, not a buy signal (path-sim pending via the forward log).
    lean_score = 0.0
    if max_pain:
        d = (max_pain - spot) / spot
        lean_score += max(-1.0, min(1.0, d / 0.03))              # ±1 at ≥3% from pin
    if put_wall and abs(put_wall - spot) / spot <= 0.025:
        lean_score += 1.0                                        # sitting on support
    if call_wall and abs(call_wall - spot) / spot <= 0.025:
        lean_score -= 1.0                                        # sitting under resistance
    if gamma_flip:
        lean_score += -0.5 if spot < gamma_flip else 0.3         # below flip = accel-down risk
    lean = "up" if lean_score >= 0.6 else "down" if lean_score <= -0.6 else "flat"

    return {
        "spot":       round(spot, 2),
        "net_gex":    net_gex,
        "regime":     "positive" if net_gex >= 0 else "negative",
        "gamma_flip": gamma_flip,
        "power_zone": power_zone,
        "call_wall":  call_wall,
        "put_wall":   put_wall,
        "max_pain":   max_pain,
        "atm_iv":     round(atm_iv * 100, 1) if atm_iv else None,   # as a %
        "lean":       lean,
        "lean_score": round(lean_score, 2),
        "n_strikes":  len(strikes),
        "n_contracts": len(rows),
        "total_oi":   sum(r["oi"] for r in rows),
    }


def _gamma_flip(strikes: list[float], by_strike: dict, spot: float) -> Optional[float]:
    """Zero-gamma level: the strike where cumulative net GEX (ascending) crosses zero —
    the regime line (positive-gamma/stable above, negative-gamma/accelerant below).
    Prefers a true sign-crossing nearest spot; falls back to the balance point (strike
    whose cumulative is closest to zero) so a value is always returned when data exists."""
    # only trust a flip near spot — a deep-OTM balance point is meaningless. A truly
    # accurate zero-gamma level needs Black-Scholes recomputation of gamma vs spot;
    # this strike-cumulative proxy is reliable only when the crossing sits near price.
    lo, hi = spot * 0.7, spot * 1.3
    cum = 0.0
    prev_k = None
    prev_cum = 0.0
    flip = None
    best_bal_k = None
    best_bal = None
    for k in strikes:
        cum += by_strike[k]["gex"]
        if lo <= k <= hi and (best_bal is None or abs(cum) < best_bal):
            best_bal, best_bal_k = abs(cum), k
        if prev_k is not None and lo <= k <= hi and (prev_cum < 0) != (cum < 0):
            span = cum - prev_cum
            frac = (0 - prev_cum) / span if span else 0.5
            cand = prev_k + frac * (k - prev_k)
            if flip is None or abs(cand - spot) < abs(flip - spot):
                flip = cand
        prev_k, prev_cum = k, cum
    if flip is not None:
        return round(flip, 2)
    # balance-point fallback only if it lands reasonably close to spot (±15%)
    if best_bal_k is not None and abs(best_bal_k - spot) <= spot * 0.15:
        return round(float(best_bal_k), 2)
    return None


def _max_pain(rows: list[dict], strikes: list[float]) -> Optional[float]:
    """Strike at which the total intrinsic value paid to ALL option holders is
    minimized (where the most OI expires worthless — the classic pin target)."""
    oi_by = {}
    for r in rows:
        oi_by.setdefault((r["strike"], r["type"]), 0)
        oi_by[(r["strike"], r["type"])] += r["oi"]
    best_k, best_pain = None, None
    for K in strikes:
        pain = 0.0
        for (k, typ), oi in oi_by.items():
            if typ == "call" and K > k:
                pain += (K - k) * oi
            elif typ == "put" and K < k:
                pain += (k - K) * oi
        if best_pain is None or pain < best_pain:
            best_pain, best_k = pain, K
    return best_k


def gex_for_ticker(ticker: str, max_dte: int = 60, expiration: Optional[str] = None,
                   with_expirations: bool = False, source: str = "massive") -> Optional[dict]:
    """Fetch chain + spot and compute GEX. `expiration` (YYYY-MM-DD) targets one expiry
    (like the OptionFlow dropdown); else aggregates all contracts ≤ max_dte. With
    with_expirations=True, also attaches the available-expirations list for the UI.
    source='cboe' (2026-08-03) reads the FREE Cboe delayed CDN instead of Massive — same
    row contract, iv rescaled to one unit; runs in parallel for the cancellation parity week.
    Cboe also serves true INDEX chains (SPX/NDX/RUT), which Massive never had."""
    if source == "cboe":
        from data_options_cboe import (fetch_chain_cboe as fetch_chain,
                                       spot_price_cboe as spot_price,
                                       list_expirations_cboe as list_expirations)
    else:
        from data_options import fetch_chain, spot_price, list_expirations
    chain = fetch_chain(ticker, max_dte=max_dte, expiration=expiration)
    if not chain:
        # still surface the expiration calendar so the dropdown can populate even if
        # the selected expiry had no usable greeks
        if with_expirations:
            exps = list_expirations(ticker)
            if exps:
                return {"ticker": ticker.upper(), "regime": None,
                        "available_expirations": exps, "expiration": expiration}
        return None
    spot = spot_price(ticker)
    if not spot:
        return None
    res = compute_gex(chain, spot)
    if res:
        res["ticker"] = ticker.upper()
        res["max_dte"] = max_dte
        res["expiration"] = expiration
        res["source"] = source
        if with_expirations:
            res["available_expirations"] = list_expirations(ticker)
        _attach_vrp(res, ticker)
    return res


def _attach_vrp(res: dict, ticker: str) -> None:
    """⚖️ IV↔ATR dissonance / Variance Risk Premium (2026-07-26, from the BS-formulas study).
    rv_atr = ATR-realized vol annualized (ATR%·√252 — a RANGE-based estimate, systematically
    ~25-30% higher than close-to-close vol, so "fair" reads near 0.8 on this scale, not 1.0).
    vrp = atm_iv / rv_atr: the options market's expected vol vs what the stock actually does.
    Thresholds calibrated 2026-07-26 on the live cross-section of 40 liquid names
    (min 0.55 · p25 0.77 · MEDIAN 0.81 · p75 1.31 · max 1.96) so that each tag flags a genuine
    outlier (~10% of names each) instead of half the board:
      ≥1.35 EVENT-PRICED — options expensive, a move is already priced (earnings/news risk);
                           if nothing happens, IV deflates. (BSX 1.96, RACE 1.76, NCLH 1.46)
      ≤0.65 COMPLACENT   — the stock moves far more than options price in; often precedes a
                           volatility expansion. (IBM 0.55, INTC 0.58, AAPL 0.61)
      0.65-1.35 BALANCED — the normal state, no signal (LMT 0.78, NVDA 0.80, AMD 0.81)
    DESCRIPTIVE + forward-only (no IV history exists — validation via gex_edge_log accumulation,
    same regime as the whole GEX layer). NOT a validated buy/sell signal."""
    try:
        iv = res.get("atm_iv")
        if not iv:
            return
        import duckdb, math
        from studio.db import tf_db_path
        tk = str(ticker).upper().replace("'", "")
        c = duckdb.connect(tf_db_path("1d"), read_only=True)
        try:
            row = c.execute(
                "SELECT atr_14/close FROM bars WHERE ticker = ? AND atr_14 > 0 AND close > 0 "
                "ORDER BY date DESC LIMIT 1", [tk]).fetchone()
        finally:
            c.close()
        if not row or not row[0]:
            return
        rv = round(float(row[0]) * math.sqrt(252) * 100, 1)     # annualized realized vol, %
        vrp = round(iv / rv, 2) if rv > 0 else None
        res["rv_atr"] = rv
        res["vrp"] = vrp
        if vrp is not None:
            res["vrp_state"] = ("EVENT-PRICED" if vrp >= 1.35
                                else "COMPLACENT" if vrp <= 0.65 else "BALANCED")
    except Exception:
        log.debug("vrp attach failed for %s", ticker, exc_info=True)
