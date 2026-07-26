"""
price_zones.py — the validated Fibonacci price-zone law as a single source of truth.

From the 2026-06-30 full-spectrum study (all 10 Edge setups, path-sim trail25/60):
win% and catastrophe% improve MONOTONICALLY with price; mean is a hump peaking $21-89.
  $1-3   casino   median −7..−13%, catastrophe 33-43% (moonshot lottery, win coin-flip)
  $3-8   knife    median −3..−5%,  catastrophe 24-35%
  $8-21  dead     median ~0/neg,   catastrophe 22-26% (sharp CLIFF up at exactly $21)
  $21-89 QUALITY  median +2..+9%,  catastrophe 12-22%  ← peak risk-adjusted return
  $89-377 safe    median +1..+3%,  catastrophe 13-18%  (reliable, smaller % moves)
  >$377  thin     median +1..+2%,  catastrophe 10-16%  (safest, mega-cap)

EXCEPTION: G3-gap has a cheap-momentum spillover at $8-10 (win 57.8%/med +3.1) — not
penalized for that setup. Z11-T11 is structurally $21+ (barely fires cheap).

`score_delta` is an additive adjustment for a 0-100 setup score (sinks dead/knife
candidates in ranking; lifts the quality zone). Display: emoji + short label.
"""
from __future__ import annotations


def classify(px, setup: str | None = None) -> dict:
    """Return {zone, emoji, label, score_delta, tradeable, floor_ok} for a close price."""
    if px is None:
        return {"zone": "unknown", "emoji": "", "label": "?", "score_delta": 0,
                "tradeable": True, "floor_ok": True}
    px = float(px)
    # G3-gap cheap-momentum spillover: $8-10 is a real edge for G3 only
    if setup == "g3" and 8.0 <= px < 10.0:
        return {"zone": "g3-spill", "emoji": "⚡", "label": "g3-cheap", "score_delta": 4,
                "tradeable": True, "floor_ok": True}
    if px < 1.0:
        z = ("casino", "🎰", "casino", -28, False, False)
    elif px < 8.0:
        z = ("knife", "🔪", "knife", -18, False, False)
    elif px < 21.0:
        z = ("dead", "💀", "dead", -10, True, False)      # tradeable but down-weighted; below the $21 floor
    elif px < 89.0:
        z = ("quality", "✅", "quality", 10, True, True)
    elif px < 377.0:
        z = ("safe", "🛡", "safe", 4, True, True)
    else:
        z = ("thin", "❄️", "thin", 0, True, True)
    return {"zone": z[0], "emoji": z[1], "label": z[2], "score_delta": z[3],
            "tradeable": z[4], "floor_ok": z[5]}


def apply(score: int, px, setup: str | None = None) -> tuple[int, dict]:
    """Convenience: clamp(score + delta) + the zone dict. Use in scanners."""
    z = classify(px, setup)
    return max(0, min(int(score) + z["score_delta"], 100)), z
