"""brain/spine.py — the decision orchestrator. Chains the layers into ONE verdict + full log.

    regime (L2) -> candidate from registry (L3/4) -> disqualifiers -> sizing (L5) -> portfolio (L8) -> DECISION

Every stage can veto with a reason, so a "NO" is always explainable and a "BUY" carries its full
chain (why, how many shares, where the stop/target, which limits bound it). The spine is thin: the
knowledge lives in the registry, the math in sizing/portfolio, the permission in regime.
"""
from __future__ import annotations
from . import registry, sizing, portfolio, regime as regime_mod

HARD_STOP = 0.15      # path-sim standard hard stop (-15%); a structural stop can override later
TARGET = 0.25         # notional target (+25%) for the R:R gate (real exit is trail25)


MIN_STOP = 0.04       # never risk a stop tighter than -4% (whipsaw); floor the structural stop


def decide(ticker: str, fired_edges: list[str], price: float, *,
           sector: str = "?", atr_pct: float | None = None, adv_dollars: float | None = None,
           swing_low: float | None = None,
           open_positions: list[dict] | None = None, losing_streak: int = 0, drawdown: float = 0.0,
           regime: dict | None = None, cfg_sizing=None, cfg_pf=None) -> dict:
    """fired_edges: registry ids or edge_replay cols that fired on this ticker's latest bar.
    price: current/entry price. Returns {decision: BUY|NO, ...full plan..., log:[...]}."""
    log: list[str] = []
    openp = open_positions or []

    # ── L2 regime permission ──────────────────────────────────────────────────
    reg = regime or regime_mod.current_regime()
    log.append(f"regime: risk×{reg['risk_mult']} setups={reg['setups']} · {'; '.join(reg.get('reasons', []))}")
    if reg["risk_mult"] <= 0:
        return {"decision": "NO", "ticker": ticker, "reason": "regime: no trading", "log": log}

    # ── L3/4 candidate from the knowledge registry, filtered by regime ────────
    edges = registry.live_edges(direction="long")
    by_col = {e["col"]: e for e in edges if e.get("col")}
    by_id = {e["id"]: e for e in edges}
    matched = []
    for f in fired_edges:
        e = by_col.get(f) or by_id.get(f)
        if not e:
            continue
        if reg["setups"] == "robust_only" and e.get("tier") != "core":
            continue
        if reg["setups"] == "survivors_only" and e["id"] not in (reg.get("survivors") or []):
            continue
        matched.append(e)
    if not matched:
        return {"decision": "NO", "ticker": ticker,
                "reason": "no registry edge fired within regime's allowed class", "log": log}
    # rank by historical median (edge quality) first, core-tier as the tiebreaker. A mined combo is
    # a validated SUBSET of its base (base ∧ conditioner) that beat the base on the OOS gate, so its
    # higher median SHOULD win when both co-fire — otherwise the always-co-firing base buries it.
    best = max(matched, key=lambda e: (e.get("stats", {}).get("median", 0), e.get("tier") == "core"))
    log.append(f"candidate: {best['title']} [{best.get('tier')}] (also: {[m['id'] for m in matched if m is not best]})")

    # ── L5 geometry + sizing ──────────────────────────────────────────────────
    entry = float(price)
    # STRUCTURAL stop = just under the recent swing/base low, clamped to [-MIN_STOP, -HARD_STOP].
    # A stop below the structure that held is where the idea is DEAD by meaning (not by %).
    if swing_low and 0 < swing_low < entry:
        struct = swing_low * 0.995
        stop = min(struct, entry * (1 - MIN_STOP))     # not tighter than -4%
        stop = max(stop, entry * (1 - HARD_STOP))      # not wider than -15% (risk cap)
        stop = round(stop, 2)
        stop_kind = "structural (base low)"
    else:
        stop = round(entry * (1 - HARD_STOP), 2)
        stop_kind = "fallback -15%"
    target = round(entry + 2.0 * (entry - stop), 2)     # R:R 2.0 notional (real exit = trail25)
    size = sizing.size_trade(entry, stop, target, tier=best.get("tier", "core"),
                             losing_streak=losing_streak, atr_pct=atr_pct,
                             adv_dollars=adv_dollars, cfg=cfg_sizing)
    if not size["ok"]:
        return {"decision": "NO", "ticker": ticker, "reason": f"sizing: {size['reason']}", "log": log}
    reg_shares = int(size["shares"] * reg["risk_mult"])
    if reg_shares < 1:
        return {"decision": "NO", "ticker": ticker, "reason": "regime risk-mult -> size < 1 share", "log": log}
    log.append(f"sizing: {size['shares']}sh (stop {stop_kind} {size['stop_pct']:.0%}, risk {size['risk_pct_actual']:.1%}, R:R {size['rr']}, x{size['size_mult']}) -> regime x{reg['risk_mult']} = {reg_shares}sh")

    # ── L8 portfolio envelope ─────────────────────────────────────────────────
    cand = {"sector": sector, "risk_dollars": reg_shares * (entry - stop), "position_value": reg_shares * entry}
    pf = portfolio.portfolio_check(cand, openp, drawdown=drawdown, cfg=cfg_pf)
    if not pf["ok"]:
        return {"decision": "NO", "ticker": ticker, "reason": f"portfolio: {pf['reason']}", "log": log}
    final_shares = int(reg_shares * pf["allowed_fraction"])
    if final_shares < 1:
        return {"decision": "NO", "ticker": ticker, "reason": "portfolio room -> size < 1 share", "log": log}
    if pf["allowed_fraction"] < 1:
        log.append(f"portfolio: x{pf['allowed_fraction']} ({pf['binding']} binding) -> {final_shares}sh")

    return {
        "decision": "BUY",
        "ticker": ticker,
        "edge": best["id"],
        "edge_title": best["title"],
        "tier": best.get("tier"),
        "sector": sector,
        "shares": final_shares,
        "entry": round(entry, 2),
        "stop": stop,
        "target": target,
        "risk_dollars": round(final_shares * (entry - stop), 2),
        "position_value": round(final_shares * entry, 2),
        "regime_risk_mult": reg["risk_mult"],
        "log": log,
    }


if __name__ == "__main__":
    import json
    reg = {"risk_mult": 1.0, "setups": "all", "reasons": ["test uptrend"]}
    print("── coil-floor fires, $50, empty book ──")
    print(json.dumps(decide("DEMO", ["E_coilfloor"], 50.0, sector="Technology",
                            open_positions=[], regime=reg), indent=2))
    print("\n── watch-tier engulf, in a downtrend (robust_only) -> filtered out ──")
    reg2 = {"risk_mult": 0.5, "setups": "robust_only", "reasons": ["bear"]}
    print(json.dumps(decide("DEMO", ["E_engulf_absorb_rev"], 50.0, regime=reg2), indent=2))
