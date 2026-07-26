"""brain/regime.py — Layer 2: collapse the many gates into ONE permission verdict.

Not a signal — a PERMISSION: {allowed directions, risk multiplier, allowed setup class}. The
system is LONG-BIASED on purpose: we have no validated short edge (the extended-engulf short
failed), so bad regimes REDUCE long risk rather than flip to shorts.

Inputs are the market-level facts our research already validated as regime drivers:
  · SPY trend (above/below EMA200)          — market direction
  · breadth (share of names participating)  — is the move broad
  · season (Dec-Mar suppressor)             — validated gate (project_season_gate)
  · VIX-extreme (risk-off)                  — optional

regime_verdict() is pure; current_regime() fetches live facts from the DB and calls it.
"""
from __future__ import annotations

# setup classes, from most-permissive to survivors-only (mapped to registry tiers/ids downstream)
SETUPS_ALL = "all"                 # core + watch edges
SETUPS_ROBUST = "robust_only"      # core edges only (drop watch)
SETUPS_SURVIVORS = "survivors_only"  # only 2022-survivors (era-robust core subset)
SETUPS_NONE = "none"

# curated 2022-survivors (era-robust) — registry ids
SURVIVORS = ("coil_floor", "gem1_capbounce", "d_l1_reversal", "z_absorb_turn", "cluster_bottom")


def regime_verdict(*, month: int, spy_above_ema200: bool, breadth_pct: float,
                   vix_extreme: bool = False) -> dict:
    """Pure permission verdict from market-level facts."""
    reasons: list[str] = []
    score = 0
    if spy_above_ema200:
        score += 1
    else:
        reasons.append("SPY below EMA200 (downtrend)")
    if breadth_pct >= 0.45:
        score += 1
    else:
        reasons.append(f"weak breadth {breadth_pct:.0%}")
    if month in (12, 1, 2, 3):
        reasons.append("Dec-Mar season suppressor")
    else:
        score += 1
    if vix_extreme:
        score -= 1
        reasons.append("VIX extreme (risk-off)")

    if score >= 3:
        risk, setups = 1.0, SETUPS_ALL
        reasons = reasons or ["clean uptrend + broad + in-season"]
    elif score == 2:
        risk, setups = 0.75, SETUPS_ALL
    elif score == 1:
        risk, setups = 0.5, SETUPS_ROBUST
    elif score == 0:
        risk, setups = 0.25, SETUPS_SURVIVORS
    else:
        risk, setups = 0.0, SETUPS_NONE

    return {
        "score": score,
        "risk_mult": risk,
        "directions": ["long"] if risk > 0 else [],
        "setups": setups,
        "survivors": list(SURVIVORS) if setups == SETUPS_SURVIVORS else None,
        "reasons": reasons,
    }


def current_regime() -> dict:
    """Fetch live market facts (SPY trend + breadth + month) and return the verdict.
    Breadth proxy = share of dv>=3M names whose last-bar RSI14 > 50 (fast, stored column)."""
    try:
        import duckdb
        from studio.db import tf_db_path
        con = duckdb.connect(tf_db_path("1d"), read_only=True)
        # SPY EMA200 vs last close
        spy = con.execute("SELECT close FROM bars WHERE ticker='SPY' ORDER BY date").fetchdf()
        # last trading day + breadth
        maxd = con.execute("SELECT max(date) FROM bars WHERE universe<>'index'").fetchone()[0]
        br = con.execute(
            """SELECT avg(CASE WHEN rsi_14 > 50 THEN 1.0 ELSE 0.0 END) breadth
               FROM bars WHERE universe<>'index' AND close>=5 AND close*volume>=3000000
                 AND substr(CAST(date AS VARCHAR),1,10)=substr(CAST(? AS VARCHAR),1,10)""",
            [maxd]).fetchone()[0]
        con.close()
        c = spy["close"].astype(float).to_numpy()
        a = 2 / 201.0
        ema = c[0]
        for x in c[1:]:
            ema = a * x + (1 - a) * ema
        spy_above = bool(c[-1] > ema)
        breadth = float(br) if br is not None else 0.5
        month = int(str(maxd)[5:7])
        v = regime_verdict(month=month, spy_above_ema200=spy_above, breadth_pct=breadth)
        v["facts"] = {"as_of": str(maxd)[:10], "spy_above_ema200": spy_above,
                      "breadth_pct": round(breadth, 3), "month": month}
        return v
    except Exception as e:
        # fail-safe: neutral permission (half risk, robust only) so the brain never over-trades on error
        return {"score": None, "risk_mult": 0.5, "directions": ["long"], "setups": SETUPS_ROBUST,
                "reasons": [f"regime fetch failed ({e}) — fail-safe half-risk"], "facts": {}}


if __name__ == "__main__":
    import json
    print("clean uptrend:", json.dumps(regime_verdict(month=5, spy_above_ema200=True, breadth_pct=0.6)))
    print("bear+weak:    ", json.dumps(regime_verdict(month=10, spy_above_ema200=False, breadth_pct=0.30)))
    print("Dec season:   ", json.dumps(regime_verdict(month=1, spy_above_ema200=True, breadth_pct=0.5)))
    print("\ncurrent_regime():")
    print(json.dumps(current_regime(), indent=2))
