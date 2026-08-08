"""brain/portfolio.py — Layer 8: the portfolio / risk envelope (the other decisive layer).

The number of concurrent positions is NOT hardcoded — it emerges from limits on total open
RISK and EXPOSURE. When the sum of open-position risk hits the cap, there is simply no room for
another trade; as winners move to break-even (risk removed), room re-opens. This is the account-
protection layer the 9-layer model calls decisive, and it was empty in our system.

Pure & config-driven (no I/O). The spine feeds it the sizing candidate + current open positions.
"""
from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class PortfolioConfig:
    capital: float = 10_000.0
    # 2026-08-07 PORTFOLIO SIMULATION (portfolio_sim2.py) — the first test of this layer.
    # The board fires ~114 times/day (p90 308) and the median hold under the ATR exit is 60
    # bars, so slot count — not signal quality — is the binding constraint: at the old 6%
    # cap the account took 61 trades in 5 YEARS, i.e. 0.03% of fires. Simulated slot sweep
    # (equal-weight slots, best-edge-first, mark-to-market curve):
    #     3 slots  CAGR +14.8%  maxDD -55.6%  Sharpe 0.57   (the old envelope)
    #     5        +15.0        -40.0         0.71
    #     8        +18.6        -25.0         1.02          <- both better
    #    10        +17.1        -29.8
    #    20        +11.1        -33.8         0.94
    #    50         +7.7        -33.0         0.89
    # 8-10 concurrent raises return AND halves drawdown — pure diversification, the one
    # place both move the right way. 2022 alone: -49.4% at 3 slots vs -28.0% at 10.
    # ⚠ per-trade worst-years (-1..-3%) CANNOT see this: correlated drawdown is invisible
    # to a per-trade statistic. ⚠ the sim used equal-weight slots, not risk-based sizing,
    # so this maps the SLOT COUNT, not the exact dollar geometry.
    max_total_risk_pct: float = 0.10    # sum of open risks <= 10% of capital (=> ~10 full-risk slots)
    max_gross_pct: float = 1.00         # gross exposure <= 100% (cash account, no leverage)
    max_sector_pct: float = 0.40        # <= 40% of capital in one sector
    cash_reserve_pct: float = 0.10      # keep >= 10% cash
    # drawdown-based size throttle: {drawdown_fraction: size_multiplier} (0.0 = full stop)
    dd_cut: dict = field(default_factory=lambda: {0.05: 0.75, 0.10: 0.50, 0.15: 0.0})


def portfolio_check(candidate: dict, open_positions: list[dict], *,
                    drawdown: float = 0.0, cfg: PortfolioConfig | None = None) -> dict:
    """candidate: {sector, risk_dollars, position_value} (from sizing).
    open_positions: list of the same for currently-held trades.
    drawdown: current peak-to-trough fraction (0.08 = -8%).
    Returns how much of the candidate fits: allowed_fraction (0..1), the binding constraint,
    the drawdown throttle, and the live portfolio state. allowed_fraction<1 => size down."""
    cfg = cfg or PortfolioConfig()
    cap = cfg.capital
    cand_risk = max(float(candidate.get("risk_dollars", 0.0)), 1e-9)
    cand_val = max(float(candidate.get("position_value", 0.0)), 1e-9)
    sec = candidate.get("sector", "?")

    total_risk = sum(float(p.get("risk_dollars", 0.0)) for p in open_positions)
    gross = sum(float(p.get("position_value", 0.0)) for p in open_positions)
    sec_exp = sum(float(p.get("position_value", 0.0)) for p in open_positions if p.get("sector") == sec)

    state = {
        "open_positions": len(open_positions),
        "total_risk_pct": round(total_risk / cap, 4),
        "gross_pct": round(gross / cap, 4),
        "sector_pct": round(sec_exp / cap, 4),
    }

    # ── drawdown throttle (Layer 8.2) ─────────────────────────────────────────
    dd_mult = 1.0
    for d in sorted(cfg.dd_cut):
        if drawdown >= d:
            dd_mult = cfg.dd_cut[d]
    if dd_mult == 0.0:
        return {"ok": False, "reason": f"drawdown {drawdown:.0%} >= stop threshold — trading halted",
                "binding": "drawdown", "dd_mult": 0.0, "state": state}

    # ── room on each axis, expressed as a fraction of the candidate ───────────
    rooms = {
        "total_risk": (cfg.max_total_risk_pct * cap - total_risk) / cand_risk,
        "gross":      (cfg.max_gross_pct * cap - gross) / cand_val,
        "cash":       ((1 - cfg.cash_reserve_pct) * cap - gross) / cand_val,
        "sector":     (cfg.max_sector_pct * cap - sec_exp) / cand_val,
    }
    tight = min(rooms, key=rooms.get)
    frac = min(1.0, rooms[tight], dd_mult)
    binding = tight if rooms[tight] < 1.0 else ("drawdown" if dd_mult < 1.0 else "none")

    if frac <= 0:
        return {"ok": False, "reason": f"no room — {tight} limit hit", "binding": tight,
                "rooms": {k: round(v, 3) for k, v in rooms.items()}, "state": state}

    return {
        "ok": True,
        "allowed_fraction": round(frac, 3),
        "binding": binding,
        "dd_mult": dd_mult,
        "rooms": {k: round(v, 3) for k, v in rooms.items()},
        "state": state,
    }


if __name__ == "__main__":
    import json
    cfg = PortfolioConfig()
    # 5 open positions, each ~1% risk / ~$650 -> total risk 5%, gross ~33%
    openp = [{"sector": "Technology", "risk_dollars": 100, "position_value": 650} for _ in range(5)]
    cand = {"sector": "Energy", "risk_dollars": 100, "position_value": 650}
    print("6th trade (risk room left ~1%):");  print(json.dumps(portfolio_check(cand, openp), indent=2))
    openp7 = openp + [cand]
    print("\n7th trade (risk cap 6% hit):");    print(json.dumps(portfolio_check(cand, openp7), indent=2))
    print("\nsame but -12% drawdown:");         print(json.dumps(portfolio_check(cand, openp, drawdown=0.12), indent=2))
