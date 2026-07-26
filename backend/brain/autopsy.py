"""brain/autopsy.py — post-trade forensics (Layer 9). Every CLOSED trade is dissected against the
EDGE'S OWN base-rate: did it behave as the edge predicts, or fail — and was the failure the edge's
fault, the regime's, or the entry geometry? This is what turns a win/loss into a LESSON instead of
just a number. Pure & deterministic (no LLM): it compares the realized path to the plan and the
edge's historical stats. agents.postmortem() can layer an LLM narrative on top; this is the spine.

verdict vocabulary (what actually happened, mechanically):
  target_hit    reached ~the planned target      -> edge worked
  good_exit     green, exited before target       -> edge worked, early exit
  stopped_out   hit the structural stop           -> thesis invalidated
  weak_exit     red, cut before the stop          -> exited into weakness / time-stop
attribution (whose 'fault' the outcome is, given the edge's base-rate win% and median):
  as_expected   outcome consistent with base-rate (incl. a loss inside a <100%-win edge)
  edge_miss     a positive-median edge lost here — one draw from the distribution
  regime_drag   lost while regime was hostile      -> not the edge, the tape
  size_ok/geometry notes on stop distance vs realized
"""
from __future__ import annotations
from . import registry


def analyze(trade: dict) -> dict:
    """Dissect one closed trade. `trade` = a journal 'closed' record (entry/stop/target/exit/pnl/
    shares/edge/reason + optional opened_regime). Returns a structured autopsy — no side effects."""
    entry = float(trade.get("entry") or 0)
    exitp = float(trade.get("exit") or 0)
    stop = float(trade.get("stop") or 0)
    target = float(trade.get("target") or 0)
    if not entry:
        return {"verdict": "unknown", "note": "no entry price"}

    ret = exitp / entry - 1
    stop_pct = (stop / entry - 1) if stop else None            # negative
    tgt_pct = (target / entry - 1) if target else None          # positive
    r_mult = (ret / abs(stop_pct)) if stop_pct else None        # realized R
    plan_rr = (tgt_pct / abs(stop_pct)) if (stop_pct and tgt_pct) else None

    # ── what happened mechanically ──
    if tgt_pct and ret >= tgt_pct * 0.9:
        verdict, worked = "target_hit", True
    elif stop_pct and ret <= stop_pct * 0.95:
        verdict, worked = "stopped_out", False
    elif ret > 0:
        verdict, worked = "good_exit", True
    else:
        verdict, worked = "weak_exit", False

    # ── against the edge's OWN base-rate ──
    edge = registry.get(trade.get("edge") or "") or {}
    st = edge.get("stats", {}) or {}
    exp_med = st.get("median")
    exp_win = st.get("win")
    exp_pf = st.get("pf")
    tier = edge.get("tier") or trade.get("tier")

    factors = []
    if plan_rr is not None:
        factors.append(f"planned R:R {plan_rr:.1f} · realized {r_mult:+.2f}R")
    if exp_med is not None:
        factors.append(f"edge base median {exp_med:+.2f}% (this trade {ret*100:+.2f}%)")
    if exp_win is not None:
        factors.append(f"edge base win {exp_win}% — losses are expected {100-exp_win}% of the time")

    # attribution
    reg = (trade.get("opened_regime") or {})
    reg_mult = reg.get("risk_mult") if isinstance(reg, dict) else None
    if reg_mult is None:
        reg_mult = trade.get("regime_risk_mult")
    reg_hostile = reg_mult is not None and reg_mult < 1
    if worked:
        attribution = "as_expected"
        if exp_med is not None and ret * 100 > exp_med:
            factors.append("beat the edge's median — clean instance")
    else:
        if reg_hostile:
            attribution = "regime_drag"
            factors.append("regime was risk-OFF at entry — tape drag, not the edge")
        elif exp_win is not None and exp_win < 100:
            attribution = "edge_miss"
            factors.append(f"one loss inside a {exp_win}%-win edge — within base rate, not a broken edge")
        else:
            attribution = "edge_miss"

    # geometry sanity
    if stop_pct is not None and verdict == "stopped_out" and abs(stop_pct) < 0.05:
        factors.append("stop was tight (<5%) — check if the structural low gave more room")

    # ── the lesson (what the brain should take away) ──
    if worked:
        lesson = f"{trade.get('edge')} behaved as validated ({tier}); keep tier."
    elif attribution == "regime_drag":
        lesson = "loss attributable to regime, not the edge — don't demote on this; regime gate should have sized it down."
    else:
        # is this part of a pattern? outcome_stats decides demotion; a single miss is noise
        lesson = (f"single loss within {trade.get('edge')}'s base rate — no tier action on its own; "
                  "calibrate() demotes only if the realized win-rate stays below the prior over ≥8 trades.")

    quality = "good_buy" if worked else ("acceptable_loss" if attribution != "edge_miss" else "failed_trade")
    return {
        "verdict": verdict, "attribution": attribution, "quality": quality,
        "ret_pct": round(ret * 100, 2), "r_multiple": round(r_mult, 2) if r_mult is not None else None,
        "planned_rr": round(plan_rr, 1) if plan_rr is not None else None,
        "edge": trade.get("edge"), "edge_title": edge.get("title") or trade.get("edge_title"),
        "tier": tier, "expected": {"median": exp_med, "win": exp_win, "pf": exp_pf},
        "factors": factors, "lesson": lesson,
    }


if __name__ == "__main__":
    import json
    demo = {"ticker": "TEST", "edge": "gem1_capbounce", "entry": 10, "stop": 8.5,
            "target": 13, "exit": 13.1, "shares": 100, "pnl": 310}
    print(json.dumps(analyze(demo), indent=2, ensure_ascii=False))
