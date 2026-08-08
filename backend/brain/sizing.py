"""brain/sizing.py — Layer 5: position geometry & sizing (the biggest missing lever).

Turns a candidate (entry, stop, target) into a CONCRETE order or a hard NO. Risk-based sizing:
the number of shares is set so that hitting the stop loses a fixed fraction of capital — so the
same edge on a $20 and a $200 stock risks the same dollars. This is what actually decides
results (per the 9-layer model), and it was empty in our system.

Capital base $10,000 (user). Everything is config-driven and pure (no I/O, no live-app import).
"""
from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class SizingConfig:
    capital: float = 10_000.0        # account base
    risk_pct: float = 0.01           # risk per trade = 1% = $100
    min_rr: float = 1.5              # reject if reward:risk below this
    # 0.25 was set for a ~3-6 slot book; with the 2026-08-07 envelope targeting 8-10
    # concurrent (portfolio.py), a single 25% position would defeat the diversification
    # that made 8 slots better than 3 on BOTH return and drawdown. 0.12 keeps ~8 positions
    # inside the 90% deployable cash with room to spare.
    max_pos_pct: float = 0.12        # cap one position at 12% of capital
    liq_cap_pct: float = 0.01        # cap size at 1% of ADV (dollar) to avoid impact
    watch_mult: float = 0.5          # watch-tier edges trade at half size
    high_vol_atr: float = 0.08       # ATR% above this -> downsize
    high_vol_mult: float = 0.75
    # losing-streak downgrades: {consecutive_losses: size_multiplier}
    streak_cut: dict = field(default_factory=lambda: {3: 0.5, 5: 0.25})


def size_trade(entry: float, stop: float, target: float | None = None, *,
               tier: str = "core", losing_streak: int = 0,
               atr_pct: float | None = None, adv_dollars: float | None = None,
               cfg: SizingConfig | None = None) -> dict:
    """Return an order plan {ok, shares, position_value, risk_dollars, rr, reasons} or a hard NO.
    entry/stop/target in price; stop must be BELOW entry (long). All downgrades multiply."""
    cfg = cfg or SizingConfig()
    if entry <= 0 or stop <= 0 or stop >= entry:
        return {"ok": False, "reason": "invalid: stop must be > 0 and below entry (long)"}

    stop_dist = entry - stop
    stop_pct = stop_dist / entry

    # ── R:R disqualifier (Layer 4.3.3 / 5.1.4) ────────────────────────────────
    rr = None
    if target is not None:
        rr = (target - entry) / stop_dist
        if rr < cfg.min_rr:
            return {"ok": False, "reason": f"R:R {rr:.2f} < min {cfg.min_rr}", "rr": round(rr, 2)}

    # ── risk budget + downgrade coefficients (Layer 5.3.4) ────────────────────
    reasons: list[str] = []
    mult = 1.0
    if tier == "watch":
        mult *= cfg.watch_mult
        reasons.append(f"watch-tier x{cfg.watch_mult}")
    for n in sorted(cfg.streak_cut):
        if losing_streak >= n:
            mult = min(mult, cfg.streak_cut[n])
    if losing_streak:
        reasons.append(f"losing-streak {losing_streak} -> x{mult}")
    if atr_pct is not None and atr_pct > cfg.high_vol_atr:
        mult *= cfg.high_vol_mult
        reasons.append(f"high-vol (ATR {atr_pct:.0%}) x{cfg.high_vol_mult}")

    eff_risk = cfg.capital * cfg.risk_pct * mult
    shares = int(eff_risk / stop_dist)         # floor
    pos_value = shares * entry

    # ── caps (Layer 5.3.3, 5.4) ───────────────────────────────────────────────
    max_val = cfg.capital * cfg.max_pos_pct
    if pos_value > max_val:
        shares = int(max_val / entry)
        pos_value = shares * entry
        reasons.append(f"capped to {cfg.max_pos_pct:.0%} of capital")
    if adv_dollars:
        max_liq = adv_dollars * cfg.liq_cap_pct
        if pos_value > max_liq:
            shares = int(max_liq / entry)
            pos_value = shares * entry
            reasons.append(f"capped to {cfg.liq_cap_pct:.0%} of ADV")

    if shares < 1:
        return {"ok": False, "reason": "size < 1 share (stop too wide for risk budget)",
                "stop_pct": round(stop_pct, 3), "hint": "tighter stop, higher risk_pct, or skip"}

    return {
        "ok": True,
        "shares": shares,
        "position_value": round(pos_value, 2),
        "risk_dollars": round(shares * stop_dist, 2),
        "risk_pct_actual": round(shares * stop_dist / cfg.capital, 4),
        "stop_pct": round(stop_pct, 3),
        "rr": round(rr, 2) if rr is not None else None,
        "size_mult": round(mult, 3),
        "reasons": reasons or ["full size"],
    }


if __name__ == "__main__":
    import json
    # smoke: a $50 coil-floor entry, stop 15% down, target 30% up
    print(json.dumps(size_trade(50.0, 42.5, 65.0, tier="core"), indent=2))
    print(json.dumps(size_trade(50.0, 42.5, 65.0, tier="watch", losing_streak=3), indent=2))
    print(json.dumps(size_trade(50.0, 49.0, 51.0), indent=2))   # R:R too low -> NO
