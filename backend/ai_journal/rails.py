"""
ai_journal/rails.py — deterministic guardrails. Code decides "can we?" and "how
much?"; the LLM only decides "which of the allowed, and why". Pure Python.
"""
from __future__ import annotations

# Config (could move to journal_state.config_json later).
MAX_OPEN          = 8
MAX_POS_PCT       = 0.12     # max % of capital in one position
BASE_POS_PCT      = 0.05
MAX_SECTOR        = 3
STOP_ATR_MULT     = 1.5
TARGET_ATR_MULT   = 2.5
HORIZON_DAYS      = 7
V3_MIN            = 20       # entry filter floor
TOP_N             = 12
# Market-cap rule (validated): micro caps have the WORST structural continuation
# (HH 59.5%) AND negative drift (fwd5 -1.16%) — their only payoff is the rare
# news-driven lottery pop (P(fwd5>=10%) 11.4%), which is unpredictable. The
# structural strategy avoids them; 'unknown' (ETFs/funds/junk) is worst of all.
MCAP_BLOCK        = {"micro", "unknown", ""}


def entry_filter(candidates: list[dict], top_n: int = TOP_N) -> list[dict]:
    """Keep structurally-eligible candidates, rank by V3 (proxy for setup quality),
    cap to top_n. Drops the micro/unknown market-cap lottery bucket."""
    elig = [c for c in candidates
            if float(c.get("prebreak_v3") or 0) >= V3_MIN
            and (c.get("mcap_bucket") or "unknown") not in MCAP_BLOCK]
    elig.sort(key=lambda c: float(c.get("prebreak_v3") or 0), reverse=True)
    return elig[:top_n]


def position_size(conviction: int, tier1_hh_edge_pp: float, capital: float) -> float:
    """% of capital. Scales with conviction and the candidate's best HH edge, capped."""
    conv = max(0, min(100, conviction)) / 100.0
    edge_mult = 1.0 + max(0.0, min(1.0, (tier1_hh_edge_pp or 0) / 20.0))   # +0..100%
    pct = BASE_POS_PCT * (0.5 + conv) * edge_mult
    return round(min(pct, MAX_POS_PCT), 4)


def stop_target(entry_px: float, atr: float) -> tuple[float, float]:
    atr = atr or (entry_px * 0.03)   # fallback 3% if ATR missing
    return round(entry_px - STOP_ATR_MULT * atr, 4), round(entry_px + TARGET_ATR_MULT * atr, 4)


def can_open(ticker: str, sector: str, open_positions: list[dict],
             blacklist_patterns: set[str], fingerprint: str,
             size_pct: float, capital: float, mcap_bucket: str = "") -> tuple[bool, str]:
    """Refusal logic — the code can veto an LLM BUY."""
    if any(p.get("ticker") == ticker for p in open_positions):
        return False, "already open"
    if (mcap_bucket or "unknown") in MCAP_BLOCK:
        return False, f"market-cap blocked ({mcap_bucket})"
    if len(open_positions) >= MAX_OPEN:
        return False, "max open positions"
    if fingerprint in blacklist_patterns:
        return False, "blacklisted fingerprint"
    if sector and sum(1 for p in open_positions if p.get("sector") == sector) >= MAX_SECTOR:
        return False, f"sector cap ({sector})"
    if size_pct <= 0 or size_pct * capital < 1:
        return False, "size too small"
    return True, "ok"
