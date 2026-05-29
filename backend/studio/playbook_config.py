"""
studio/playbook_config.py — the Playbook's signal taxonomy + predefined setups.

This is the *declarative* half of the Playbook (the engine is studio/playbook.py).
It encodes the lesson from this project's reversal study (REVERSAL_*_260528.txt):
~80 raw signals are mostly noise individually; the edge lives in a SMALL set of
regime-gated confluences, and NONE of them earns a place until it survives a
realised backtest (entry/stop/target, both time-halves). So we do NOT build a
mega-score of all 80 — we hand-pick a handful of candidate setups grounded in the
empirical lifts, then let studio.playbook run each through the backtest gate.

Every flag named in a setup MUST be whitelisted in seq_backtest._ALLOWED_FLAGS
(otherwise _safe_flags silently drops it) AND exist as a 0/1 column in `bars`.
All flags below were verified present in the live DB on 2026-05-29.
"""
from __future__ import annotations

# ── Signal role taxonomy (handoff §3) ─────────────────────────────────────────
# Reference map used by the UI to explain *why* a signal is (or isn't) used.
# Not all of these are 0/1 trigger flags — wyc_phase / rtb_phase / final_regime
# are string regimes, applied as gates rather than ANDed trigger conditions.
SIGNAL_ROLES: dict[str, dict] = {
    "gate": {
        "label": "🚪 Regime gate (when to look, not entry)",
        "signals": ["wyc_phase", "rtb_phase", "price_gt_200", "price_gt_50",
                    "final_regime", "pb_macro_penalty"],
    },
    "bottom": {
        "label": "🟢 Bottom setup (validated edge at lows)",
        "signals": ["rsi_le_35", "wyc_in_tr", "d_spring", "d_absorb_bull",
                    "pb_stop_cause", "sig_bias_dn"],
    },
    "top": {
        "label": "🔴 Top setup (validated edge at highs)",
        "signals": ["sig_vol_20x", "sig_vol_10x", "sig_sc", "d_blast_bear_grn",
                    "d_absorb_bear", "d_upthrust", "wvf_spike"],
    },
    "trigger": {
        "label": "⏱️ Trigger (timing)",
        "signals": ["bf_buy", "bf_sell", "t_sig", "z_sig"],
    },
    "context": {
        "label": "📊 Context-only (describes the bar, doesn't predict)",
        "signals": ["composite_full_suffix", "bar_body_wick", "bar_gap_range", "bar_line5"],
    },
    "drop": {
        "label": "🗑️ Dropped / noise",
        "signals": ["F1–F11", "B1–B11", "redundant Δ-variants",
                    "GOG / CTX (not computed in DB)", "RGTI / SMX (disabled)"],
    },
}

# ── Predefined candidate setups ───────────────────────────────────────────────
# Each is a FUNNEL row: gate (wyc_phase) → setup confluence (signals, all = 1) →
# backtest rule (side, target/stop/max_hold). The engine runs each through the
# realised-backtest gate; only survivors become "the Playbook". Numbers in the
# thesis are empirical lifts / realised stats from REVERSAL_*_260528.txt.
PLAYBOOK_SETUPS: list[dict] = [
    # ───────────────────────── BOTTOM / LONG ─────────────────────────
    {
        "id": "bottom_rsi_dip_markup",
        "name": "Oversold dip in uptrend",
        "group": "bottom",
        "side": "long",
        "signals": ["rsi_le_35"],
        "wyc_phase": "MARKUP",
        "target_pct": 8, "stop_pct": 4, "max_hold": 15,
        "thesis": "RSI≤35 pullback inside an established uptrend (buy-the-dip). "
                  "§5 realised PF≈1.15 — marginal, but the most tradeable bottom "
                  "edge on liquid names.",
    },
    {
        "id": "bottom_rsi_range",
        "name": "Oversold in trading range",
        "group": "bottom",
        "side": "long",
        "signals": ["rsi_le_35", "wyc_in_tr"],
        "wyc_phase": None,
        "target_pct": 8, "stop_pct": 4, "max_hold": 15,
        "thesis": "RSI≤35 while price sits in a Wyckoff trading range — spring zone. "
                  "wyc_in_tr +2.7 lift, rsi_le_35 +1.5.",
    },
    {
        "id": "bottom_spring_confluence",
        "name": "Spring confluence (rare)",
        "group": "bottom",
        "side": "long",
        "signals": ["rsi_le_35", "wyc_in_tr", "d_spring"],
        "wyc_phase": None,
        "target_pct": 10, "stop_pct": 5, "max_hold": 20,
        "thesis": "The validated 3-way bottom confluence (win20 48–52%). WARNING: "
                  "very rare on liquid names — expect few trades; mostly fires on "
                  "illiquid russell2k.",
    },
    {
        "id": "bottom_stop_cause",
        "name": "Stopping action + oversold",
        "group": "bottom",
        "side": "long",
        "signals": ["pb_stop_cause", "rsi_le_35"],
        "wyc_phase": None,
        "target_pct": 8, "stop_pct": 4, "max_hold": 15,
        "thesis": "pb_stop_cause carried the single highest bottom lift (+4.3); "
                  "paired with oversold for confirmation.",
    },
    {
        "id": "bottom_spring_absorb",
        "name": "Spring + bull absorption",
        "group": "bottom",
        "side": "long",
        "signals": ["d_spring", "d_absorb_bull"],
        "wyc_phase": None,
        "target_pct": 8, "stop_pct": 4, "max_hold": 15,
        "thesis": "Delta spring with bullish absorption on a down-bar — demand "
                  "stepping in at the lows.",
    },
    # ───────────────────────── TOP / SHORT ─────────────────────────
    {
        "id": "top_vol20_blast",
        "name": "Volume climax + bear blast",
        "group": "top",
        "side": "short",
        "signals": ["sig_vol_20x", "d_blast_bear_grn"],
        "wyc_phase": None,
        "target_pct": 8, "stop_pct": 4, "max_hold": 15,
        "thesis": "20× volume climax with a bearish delta blast on a green bar. "
                  "§5 confluence win20≈80% (idealised). vol_20x −19.5 lift.",
    },
    {
        "id": "top_vol20_absorb",
        "name": "Volume climax + bear absorption",
        "group": "top",
        "side": "short",
        "signals": ["sig_vol_20x", "d_absorb_bear"],
        "wyc_phase": None,
        "target_pct": 8, "stop_pct": 4, "max_hold": 15,
        "thesis": "20× volume with bearish absorption — supply overwhelming demand "
                  "at the highs.",
    },
    {
        "id": "top_vol10_selling_climax",
        "name": "Heavy volume + selling climax",
        "group": "top",
        "side": "short",
        "signals": ["sig_vol_10x", "sig_sc"],
        "wyc_phase": None,
        "target_pct": 8, "stop_pct": 4, "max_hold": 15,
        "thesis": "10× volume with a VABS selling-climax tag. vol_10x −12.6, sig_sc −8.4.",
    },
    {
        "id": "top_upthrust_wvf",
        "name": "Upthrust + volatility spike",
        "group": "top",
        "side": "short",
        "signals": ["d_upthrust", "wvf_spike"],
        "wyc_phase": None,
        "target_pct": 8, "stop_pct": 4, "max_hold": 15,
        "thesis": "Upthrust (failed breakout) with a Williams VIX-fix spike. "
                  "d_upthrust −6.0, wvf_spike −5.7.",
    },
]
