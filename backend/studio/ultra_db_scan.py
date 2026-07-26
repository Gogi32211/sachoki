"""
studio/ultra_db_scan.py — DB-backed ULTRA scan.

Returns the latest bar per ticker from the enriched Studio DB in a row shape
compatible with the existing UltraScanPanel.jsx client-side filtering.

Speed: ~1-2 seconds for ~3,700 SP500+NASDAQ rows (vs ~30-60 min live scan).
Freshness: matches DB's last bar (updated daily at 17:00 ET via incremental refresh).
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

import pandas as pd

from studio.db import get_conn

log = logging.getLogger(__name__)


# ── Columns we expose in the row payload (UI-compatible field names) ─────────
# The UltraScanPanel.jsx client reads these field names from each row.
_DB_TO_UI_COL_MAP = {
    # Identity
    "ticker":            "ticker",
    "universe":          "universe",
    "sector":            "sector",
    "date":              "scan_date",
    # OHLC / price
    "close":             "last_price",
    "open":              "open",
    "high":              "high",
    "low":               "low",
    "volume":            "volume",
    "avg_vol_20d":       "avg_vol",
    # Scoring
    "turbo_score":       "turbo_score",
    "turbo_score_n3":    "turbo_score_n3",
    "turbo_score_n5":    "turbo_score_n5",
    "turbo_score_n10":   "turbo_score_n10",
    "ultra_score":       "ultra_score",
    "final_bull_score":  "final_bull_score",
    "final_regime":      "final_regime",
    "gog_score":         "gog_score",
    "gog_tier":          "gog_tier",
    "beta_score":        "beta_score",
    "beta_zone":         "beta_zone",
    # PreBreakout v2 (data-derived OOS-validated score)
    "prebreak_v2":       "prebreak_v2",
    "prebreak_v2_band":  "prebreak_v2_band",
    "prebreak_v3":       "prebreak_v3",
    "prebreak_v3_reasons": "prebreak_v3_reasons",
    # Profile
    "profile_score":     "profile_score",
    "profile_category":  "profile_category",
    "sweet_spot_active": "sweet_spot_active",
    "late_warning":      "late_warning",
    # Numeric indicators (computed in enricher)
    "rsi_14":            "rsi",
    "cci_20":            "cci",
    "change_pct":        "change_pct",
    # T/Z state
    "t_sig":             "tz_sig_t",
    "z_sig":             "tz_sig_z",
    "tz_bull":           "tz_bull",
    # L signal (chart-format) and TZ/WLNBB strings
    "l_sig":             "tz_wlnbb_l_signal",
    "full_suffix":       "tz_wlnbb_ne_suffix",  # approximation
    "ne_suffix":         "tz_wlnbb_ne_suffix",
    "wick_suffix":       "tz_wlnbb_wick_suffix",
    "bar_body_wick":     "tz_wlnbb_bar_body_wick",
    "bar_gap_range":     "tz_wlnbb_bar_gap_range",
    "bar_line5":         "tz_wlnbb_bar_line5",
    # RTB
    "rtb_phase":         "rtb_phase",
    "rtb_total":         "rtb_total",
    # 260523
    "ad_fresh":          "ad_fresh",
    "ad_cluster":        "ad_cluster",
    "wyc_phase":         "wyc_phase",
    "wyc_spring":        "wyc_spring",
    "wyc_sos":           "wyc_sos",
    "wyc_in_tr":         "wyc_in_tr",
    "wyc_sow":           "wyc_sow",
    "swing_type":        "swing_type",
    "prebreak_score":    "prebreak_score",
    "prebreak_prime":    "prebreak_prime",
    "prebreak_ready":    "prebreak_ready",
    "prebreak_watch":    "prebreak_watch",
    "pb_lvbo":           "pb_lvbo",
    "pb_wvf_confirm":    "pb_wvf_confirm",
    "pb_stop_cause":     "pb_stop_cause",
    "pb_macro_penalty":  "pb_macro_penalty",
    "pb_pp_rtv":         "pb_pp_rtv",
    "pb_fly_cd_c":       "pb_fly_cd_c",
    "pb_follow_confirm": "pb_follow_confirm",
    "seq_l34_eb":        "seq_l34_eb",
    # Williams pivots
    "swing_type_3":      "swing_type_3",
    "swing_type_5":      "swing_type_5",
    "pct_to_next_hh_3":  "pct_to_next_hh_3",
    "pct_to_next_hl_3":  "pct_to_next_hl_3",
}

# Frontend-filter-key → DB-column alias map. The DB stores signals as `sig_*`
# (CSV-import convention), but the Ultra UI's SIG_GROUPS filter keys use the
# live-scan naming (`best_sig`, `vol_spike_20x`, `preup66`, etc.). Without
# aliases the JS filter `!!r[k]` always evaluates to false in DB-instant mode.
_UI_KEY_TO_DB_COL: dict[str, str] = {
    # VABS
    "best_sig":       "sig_best",
    "strong_sig":     "sig_strong",
    "abs_sig":        "sig_abs",
    "climb_sig":      "sig_clm",
    "load_sig":       "load",
    "vol_spike_5x":   "sig_vol_5x",
    "vol_spike_10x":  "sig_vol_10x",
    "vol_spike_20x":  "sig_vol_20x",
    # Wyckoff VABS
    "ns":             "sig_ns_vabs",
    "nd":             "sig_nd_vabs",
    "sc":             "sig_sc",
    "bc":             "sig_bc",
    # Combo / 2809
    "buy_2809":       "sig_buy",
    "sig3g":          "sig_3g",
    "bias_up":        "sig_bias_up",
    "bias_down":      "sig_bias_dn",
    "svs_2809":       "svs",
    "conso_2809":     "sig_conso",
    "va":             "sig_va",
    # F / G
    "cd":             "sig_cd",
    "ca":             "sig_ca",
    "cw":             "sig_cw",
    "g1":             "sig_g1",
    "g2":             "sig_g2",
    "g4":             "sig_g4",
    "g6":             "sig_g6",
    "g11":            "sig_g11",
    "seq_bcont":      "sig_seq_bcont",
    # WLNBB / TZ_WLNBB-style L flags
    "fri34":          "sig_fri34",
    "fri43":          "sig_fri43",
    "fri64":          "sig_fri64",
    "l555":           "sig_l555",
    "only_l2l4":      "sig_l2l4",
    "blue":           "sig_blue",
    "cci_ready":      "sig_cci",
    "cci_0_retest":   "sig_cci0r",
    "cci_blue_turn":  "sig_ccib",
    "fuchsia_rh":     "sig_rh",
    "fuchsia_rl":     "sig_rl",
    "pre_pump":       "sig_pp",
    # Wick X
    "x2_wick":        "sig_x2",
    "x1g_wick":       "sig_x1g",
    "x1_wick":        "sig_x1",
    "x3_wick":        "sig_x3",
    "wick_bull":      "sig_wk_up",
    "wick_bear":      "sig_wk_dn",
    # ULTRA v2
    "best_long":      "sig_best_up",
    "fbo_bull":       "sig_fbo_up",
    "fbo_bear":       "sig_fbo_dn",
    "eb_bull":        "sig_eb_up",
    "eb_bear":        "sig_eb_dn",
    "ultra_3up":      "sig_3up",
    # Delta / CISD / PARA
    "d_flip_bull":       "sig_flp_up",
    "d_orange_bull":     "sig_org_up",
    "d_blast_bull_red":  "sig_dd_up_red",
    "d_surge_bull_red":  "sig_d_up_red",
    "d_surge_bear_grn":  "sig_d_dn_green",
    "d_blast_bear_grn":  "sig_dd_dn_green",
    "d_vd_div_bull":     "sig_ns_delta",
    "d_vd_div_bear":     "sig_nd_delta",
    # PREUP / PREDN
    "preup66": "sig_p66", "preup55": "sig_p55", "preup89": "sig_p89",
    "preup3":  "sig_p3",  "preup2":  "sig_p2",  "preup50": "sig_p50",
    "predn66": "sig_d66", "predn55": "sig_d55", "predn89": "sig_d89",
    "predn3":  "sig_d3",  "predn2":  "sig_d2",  "predn50": "sig_d50",
    # T/Z transitions
    "tz_bull_flip":   "sig_tz_flip",
    # Down-variants (BX↓/BE↓/FBO↓/EB↓/VBO↓)
    "bo_dn":          "sig_vbo_dn",   # closest semantic match
    "bx_dn":          "sig_vbo_dn",
    "be_dn":          "sig_vbo_dn",
    "vbo_dn":         "sig_vbo_dn",
    "fbo_bear":       "sig_fbo_dn",
    "eb_bear":        "sig_eb_dn",
    "wick_bear":      "sig_wk_dn",
    # PARA — Parabola Start Detector (Pine 260420)
    "para_prep":      "sig_para_prep",
    "para_start":     "sig_para_start",
    "para_plus":      "sig_para_plus",
    "para_retest":    "sig_para_retest",
    # FLY — ABCD EMA DP (Pine 260424)
    "fly_abcd":       "sig_fly_abcd",
    "fly_cd":         "sig_fly_cd",
    "fly_bd":         "sig_fly_bd",
    "fly_ad":         "sig_fly_ad",
    # GOG context — DB has these as bare cols (g1p, g2p, g3p, g1l, g2l, g1c, g2c, g3c)
    "gog_g1p":        "g1p",
    "gog_g2p":        "g2p",
    "gog_g3p":        "g3p",
    "gog_g1l":        "g1l",
    "gog_g2l":        "g2l",
    "gog_g1c":        "g1c",
    "gog_g2c":        "g2c",
    "gog_g3c":        "g3c",
    # !EXT alias — frontend filter `r => !r.already_extended`
    "already_extended": "already_extended_flag",
    # B-family signals (B1..B11) — direct sig_b* mapping
    **{f"b{n}": f"sig_b{n}" for n in range(1, 12)},
    "any_b":          "sig_any_b",
    # F-family signals (F1..F11) — direct sig_f* mapping
    **{f"f{n}": f"sig_f{n}" for n in range(1, 12)},
    "any_f":          "sig_any_f",
    # CISD / Para mini-flags + GOG plus
    "cisd_cplus":      "sig_cisd_cplus",
    "cisd_cplus_minus":"sig_cisd_cplus_minus",
    "cisd_cplus_mm":   "sig_cisd_cplus_mm",
    "gog_plus":        "sig_gog_plus",
    "any_p":           "sig_any_p",
    "any_d":           "sig_any_d",
    "not_ext":         "sig_not_ext",
    # 260308 + L88 — backfilled via direct UPDATE from CSV.
    "sig_l88":         "sig_l88",
    "sig_260308":      "sig_260308",
    # TZ/WLNBB volume-bucket alias for the L-signal panel "VB / B / N" filters.
    # DB stores `vol_bucket` ("B" / "L" / "N"); UI filter checks `tz_wlnbb_volume_bucket`.
    # We mirror the value as-is so the JS filter (== 'VB' / 'B' / 'N') still works
    # when the user's CSV emitted those code letters.
}


# Signals exposed as-is (DB col name == UI key)
_PASSTHROUGH_SIGNAL_COLS = [
    # T family
    *(f"sig_t{n}" for n in range(1, 13)),
    "sig_t1g", "sig_t2g",
    # Z family
    *(f"sig_z{n}" for n in range(1, 13)),
    "sig_z1g", "sig_z2g",
    # L family
    "sig_l_any", "l34", "l43", "l22", "be_up", "bo_up", "bx_up", "vbo_up",
    "sig_l1", "sig_l2", "sig_l3", "sig_l4", "sig_l5", "sig_l6",
    "sig_fri34", "sig_fri43", "sig_fri64", "sig_l555", "sig_l2l4",
    "sig_blue", "sig_cci", "sig_cci0r", "sig_ccib",
    "sig_rl", "sig_rh", "sig_pp", "sig_be_any",
    # GOG
    "sig_g1", "sig_g2", "sig_g4", "sig_g6", "sig_g11", "sig_gog_plus",
    "g1p", "g2p", "g3p", "g1l", "g2l", "g1c", "g2c", "g3c",
    # FLY
    "sig_fly_abcd", "sig_fly_cd", "sig_fly_bd", "sig_fly_ad",
    # WICK
    "sig_wk_up", "sig_wk_dn", "sig_x1", "sig_x2", "sig_x1g", "sig_x3",
    # TZ state
    "sig_tz", "sig_tz_flip", "sig_bias_up", "sig_bias_dn",
    # VABS / volume
    "sig_best", "sig_strong", "sig_abs", "sig_clm", "sig_sc", "sig_bc",
    "sig_best_up", "sig_fbo_up", "sig_eb_up", "sig_3up",
    "sig_fbo_dn", "sig_eb_dn", "sig_vbo_dn",
    "sig_va", "sig_vol_5x", "sig_vol_10x", "sig_vol_20x",
    # PREUP/PREDN
    "sig_p2", "sig_p3", "sig_p50", "sig_p55", "sig_p66", "sig_p89", "sig_any_p",
    "sig_d2", "sig_d3", "sig_d50", "sig_d55", "sig_d66", "sig_d89", "sig_any_d",
    # Combo
    "sig_buy", "sig_3g", "sig_conso", "sig_svs",
    "sig_cd", "sig_ca", "sig_cw", "sig_seq_bcont",
    "rocket", "hilo_buy", "three_g", "svs", "sq", "load", "f8",
    # Delta / CISD / PARA
    "sig_flp_up", "sig_org_up", "sig_dd_up_red", "sig_d_up_red",
    "sig_d_dn_green", "sig_dd_dn_green",
    "sig_cisd_cplus", "sig_cisd_cplus_minus", "sig_cisd_cplus_mm",
    "sig_para_prep", "sig_para_start", "sig_para_plus", "sig_para_retest",
    # EMA / RSI position
    "price_gt_20", "price_gt_50", "price_gt_89", "price_gt_200",
    "price_lt_20", "price_lt_50", "price_lt_89", "price_lt_200",
    "rsi_le_35", "rsi_ge_70",
    # Meta
    "sig_not_ext", "already_extended_flag",
    # 260523 enrichment booleans
    "wvf_spike", "vix_range", "psar_bull", "psar_bear",
    # 260308 + L88 (backfilled + enricher)
    "sig_260308", "sig_l88",
    # ULTRA v2 (enricher-computed)
    "eb_bull", "eb_bear", "fbo_bull", "fbo_bear",
    "bf_buy", "bf_sell", "ultra_3up", "ultra_3dn",
    "best_long", "best_short",
    # PARA (also has sig_para_* via CSV import; pass-through preferred)
    "para_prep", "para_start", "para_plus", "para_retest",
    # FLY (also has sig_fly_* via CSV import)
    "fly_abcd", "fly_cd", "fly_bd", "fly_ad",
    # Delta / order-flow signals (enricher-computed)
    "d_strong_bull", "d_strong_bear",
    "d_absorb_bull", "d_absorb_bear",
    "d_div_bull",    "d_div_bear",
    "d_cd_bull",     "d_cd_bear",
    "d_surge_bull",  "d_surge_bear",
    "d_blast_bull",  "d_blast_bear",
    "d_vd_div_bull", "d_vd_div_bear",
    "d_spring",      "d_upthrust",
    "d_flip_bull",   "d_flip_bear",   "d_orange_bull",
    "d_blast_bull_red", "d_blast_bear_grn",
    "d_surge_bull_red", "d_surge_bear_grn",
    # Wyckoff V2 Soft state machine (260529)
    "w2_sc", "w2_ar", "w2_st", "w2_spring", "w2_sos", "w2_jac", "w2_lps",
    "w2_evr", "w2_accum", "w2_break", "w2_state",
    # Wyckoff structure triggers (260529_WYCK_TRIG)
    "wt_valid_tr", "wt_sos", "wt_spring", "wt_lps", "wt_evr",
]


def _row_to_dict(row: pd.Series) -> dict:
    """Convert a DB row to the UI-compatible row dict.

    - All booleans/SMALLINT cols passthrough as 0/1.
    - Renamed via _DB_TO_UI_COL_MAP for fields with different UI names.
    - Computed fields: tz_sig (combined T/Z), tz_bull boolean.
    """
    out = {}
    # Renamed fields
    for db_col, ui_col in _DB_TO_UI_COL_MAP.items():
        if db_col in row.index:
            val = row[db_col]
            if pd.isna(val):
                val = None
            elif isinstance(val, float) and (val != val or val in (float("inf"), float("-inf"))):
                val = None
            out[ui_col] = val

    # Passthrough signal flags
    for col in _PASSTHROUGH_SIGNAL_COLS:
        if col in row.index:
            v = row[col]
            try:
                out[col] = int(v) if v is not None and not pd.isna(v) else 0
            except (TypeError, ValueError):
                out[col] = 0

    # Frontend-filter aliases — copy DB sig_* values onto the live-scan key
    # names that SIG_GROUPS in UltraScanPanel.jsx expects. Without these the
    # client-side `!!r[k]` filter always fails for non-T/Z signals.
    for ui_key, db_col in _UI_KEY_TO_DB_COL.items():
        if ui_key in out:
            continue  # don't overwrite if already set elsewhere
        if db_col in row.index:
            v = row[db_col]
            try:
                out[ui_key] = int(v) if v is not None and not pd.isna(v) else 0
            except (TypeError, ValueError):
                out[ui_key] = 0

    # ── String-field aliases for the TZ/WLNBB L-signal panel filters ────────
    # The UI checks r.tz_wlnbb_volume_bucket / r.tz_wlnbb_l_signal / ne / wick
    # / body_wick / gap_range / line5 directly with === string equality.
    vol_bucket = row.get("vol_bucket") if "vol_bucket" in row.index else None
    if vol_bucket is not None and not pd.isna(vol_bucket):
        out["tz_wlnbb_volume_bucket"] = str(vol_bucket)
        out["vol_bucket"]             = str(vol_bucket)
    # L-signal chart string (e.g. "L34", "L43", "FRI34")
    lsig = row.get("l_sig") if "l_sig" in row.index else None
    if lsig is not None and not pd.isna(lsig):
        out["tz_wlnbb_l_signal"] = str(lsig)
    # NE suffix / wick suffix / body-wick / gap-range / line5
    for db_col, ui_key in (
        ("ne_suffix",        "tz_wlnbb_ne_suffix"),
        ("wick_suffix",      "tz_wlnbb_wick_suffix"),
        ("bar_body_wick",    "tz_wlnbb_bar_body_wick"),
        ("bar_gap_range",    "tz_wlnbb_bar_gap_range"),
        ("bar_line5",        "tz_wlnbb_bar_line5"),
    ):
        if db_col in row.index:
            v = row[db_col]
            if v is not None and not pd.isna(v):
                out[ui_key] = str(v)
    # Full line-2 suffix (NE + wick + penetration P/R/H + close A/O/I) so the UI
    # can filter the penetration / close sub-codes that ne/wick alone don't carry.
    for _sfx in ("composite_full_suffix", "full_suffix"):
        if _sfx in row.index:
            _v = row[_sfx]
            if _v is not None and not pd.isna(_v) and str(_v):
                out["tz_wlnbb_full_suffix"] = str(_v)
                break

    # Wyckoff V2 float context (quality scores + TR levels)
    for _fc in ("w2_tr_quality", "wt_quality", "wt_support", "wt_resistance"):
        if _fc in row.index:
            _v = row[_fc]
            if _v is not None and not pd.isna(_v):
                out[_fc] = float(_v)

    # Swing type — DB's `swing_type` is rarely populated; the enricher fills
    # swing_type_3 (Williams 3-3 pivot label: HH/HL/LH/LL). Mirror the 3-3
    # value to r.swing_type so the Ultra Swing-filter (HL/LL/HH/LH/pivot)
    # works in DB mode.
    swing_3 = row.get("swing_type_3") if "swing_type_3" in row.index else None
    if swing_3 is not None and not pd.isna(swing_3) and str(swing_3) != "":
        out["swing_type"] = str(swing_3)

    # Composite tz_sig (combined string)
    tsig = row.get("t_sig") or ""
    zsig = row.get("z_sig") or ""
    out["tz_sig"] = (tsig + zsig) if (tsig or zsig) else ""

    # ULTRA score: compute fresh from DB row signals when not already stored.
    # compute_ultra_score() handles both live and stock_stat/DB row shapes via
    # ultra_signal_parser, so the DB passthrough columns (sig_t2g, l_sig, etc.)
    # give a real ULTRA score that differs from turbo_score.
    if not out.get("ultra_score"):
        try:
            from ultra_score import compute_ultra_score as _compute_ultra_score
            _row_d = dict(row)  # pd.Series → plain dict for ultra_score helper
            _us = _compute_ultra_score(_row_d)
            out["ultra_score"]                    = _us["ultra_score"]
            out["ultra_score_band"]               = _us.get("ultra_score_band", "")
            out["ultra_score_band_v2"]            = _us.get("ultra_score_band_v2", "")
            out["ultra_score_priority"]           = _us.get("ultra_score_priority", "")
            out["ultra_score_reasons"]            = _us.get("ultra_score_reasons", [])
            out["ultra_score_flags"]              = _us.get("ultra_score_flags", [])
            out["ultra_score_raw_before_penalty"] = _us.get("ultra_score_raw_before_penalty", 0)
            out["ultra_score_penalty_total"]      = _us.get("ultra_score_penalty_total", 0)
            out["ultra_score_regime_bonus"]       = _us.get("ultra_score_regime_bonus", 0)
            out["ultra_score_caps_applied"]       = _us.get("ultra_score_caps_applied", [])
            out["ultra_score_cap_reason"]         = _us.get("ultra_score_cap_reason", "")
        except Exception:
            # Last-resort fallback to avoid breaking the scan
            out["ultra_score"] = out.get("final_bull_score")

    # ULTRA Score v3 — reweighted ranker (2026-07-18), attached ALONGSIDE the old score on the
    # DB-instant path too (the frontend UV3 column). The DB row uses rsi_14/close; map them to
    # the keys v3 reads, and inject the 🏆RS/🎯cluster/🎋TLS axes from the cached edge frame.
    try:
        from ultra_score import compute_ultra_score_v3 as _compute_ultra_score_v3
        _v3row = dict(row)
        if _v3row.get("rsi") in (None, ""):
            _v3row["rsi"] = _v3row.get("rsi_14")
        if _v3row.get("last_price") in (None, ""):
            _v3row["last_price"] = _v3row.get("close")
        try:
            from ultra_orchestrator import _v3_axes_map
            _ax = _v3_axes_map().get(out.get("ticker") or _v3row.get("ticker"))
            if _ax:
                _v3row["rs_intact"], _v3row["conf_n"], _v3row["tls_bar"] = _ax
        except Exception:
            pass
        _v3 = _compute_ultra_score_v3(_v3row)
        out["ultra_score_v3"]         = _v3["ultra_score_v3"]
        out["ultra_score_v3_band"]    = _v3["ultra_score_v3_band"]
        out["ultra_score_v3_reasons"] = _v3["ultra_score_v3_reasons"]
    except Exception:
        out["ultra_score_v3"] = None; out["ultra_score_v3_band"] = ""; out["ultra_score_v3_reasons"] = []

    # BUY score (2026-07-03) — the screener's headline Score column: prebreak_v2 backbone
    # (saturated at its HOT threshold) + RSI oversold-position + vol=B, with a two-sided
    # veto (RSI≥60 EXTENDED / RSI<28 KNIFE). Validated path-sim-monotone; see buy_score.py.
    try:
        from buy_score import compute_buy_score as _cbs
        _bs = _cbs(out.get("prebreak_v2"), out.get("rsi"), out.get("vol_bucket"))
        out["buy_score"] = _bs["buy_score"]
        out["buy_tag"]   = _bs["buy_tag"]
    except Exception:
        out["buy_score"] = None
        out["buy_tag"] = ""

    # tz_state — pulled from final_regime if available
    fr = row.get("final_regime") or ""
    out["tz_state"] = "bull" if str(fr).lower() == "bull" else ("bear" if str(fr).lower() == "bear" else "")

    # data_source = always "studio_db" for these results
    out["data_source"] = "studio_db"

    return out


_CONF_CACHE: list = [0.0, {}]


def _conf_map() -> dict:
    """{ticker: (conf, top)} for the LATEST bar (2026-07-21, CONF column). EMAs come
    from a light close-history query; every other feature from the last bar itself.
    TTL 15 min."""
    import time
    if _CONF_CACHE[1] and (time.time() - _CONF_CACHE[0]) < 900:
        return _CONF_CACHE[1]
    m = {}
    try:
        from conf_score import compute, needed_raw_columns
        from ai_journal.db import get_analytics_conn
        raw = needed_raw_columns()
        rawsel = ", ".join(f'coalesce(CAST("{c}" AS TINYINT),0) AS "{c}"' for c in raw)
        a = get_analytics_conn()
        try:
            last = a.execute(f"""WITH r AS (SELECT ticker, date, open, close, rsi_14, cci_20,
                coalesce(t_sig,'') tt, coalesce(z_sig,'') zz, coalesce(l_sig,'') ll,
                coalesce(bar_gap_class,'') gap, coalesce(vol_bucket,'') vb,
                coalesce(wyc_phase,'') wp,
                coalesce(setup_tokens,'') sut, coalesce(context_tokens,'') cxt, {rawsel},
                row_number() OVER (PARTITION BY ticker ORDER BY date DESC, universe) rn
                FROM bars WHERE close >= 5)
                SELECT * EXCLUDE rn FROM r WHERE rn = 1""").fetchdf()
            hist = a.execute("""WITH r AS (SELECT ticker, date, max(close) AS cl
                FROM bars WHERE close >= 5 GROUP BY 1, 2)
                SELECT ticker, cl FROM (SELECT *, row_number() OVER
                  (PARTITION BY ticker ORDER BY date DESC) rn FROM r)
                WHERE rn <= 210 ORDER BY ticker, rn DESC""").fetchdf()
        finally:
            a.close()
        import pandas as pd
        g = hist.groupby("ticker")["cl"]
        emas = pd.DataFrame({
            "e20": g.apply(lambda s: s.ewm(span=20, adjust=False).mean().iloc[-1]),
            "e50": g.apply(lambda s: s.ewm(span=50, adjust=False).mean().iloc[-1]),
            "e200": g.apply(lambda s: s.ewm(span=200, adjust=False).mean().iloc[-1]),
        }).reset_index()
        last = last.merge(emas, on="ticker", how="left")
        last = last[last.e200.notna()].reset_index(drop=True)
        sc, det, ext, extd = compute(last, with_ext=True)
        for i, tk in enumerate(last["ticker"]):
            if sc[i] != 0 or ext[i] != 0:
                m[str(tk)] = (round(float(sc[i]), 1), det[i], round(float(ext[i]), 1), extd[i])
        _CONF_CACHE[0] = time.time()
        _CONF_CACHE[1] = m
    except Exception:
        log.debug("conf map failed", exc_info=True)
    return m


_SEQ34_CACHE: list = [0.0, {}]


def _seq34_map() -> dict:
    """{ticker: fire} — tickers whose LATEST trading day completed a frozen-OOS-verified
    2-4-bar robust sequence with a good OOS win rate (2026-07-20, user request):
    tier OOS✓ · depth>=2, any ending (T-rule removed 2026-07-20), NO win/ps gate (2026-07-20: confluence axis — frequency is
    fine, the chip shows win% so quality stays visible). 🏆 = DSR>=0.6
    (selection-proof). Reuses seq_scan (the Robust Seqs tab engine). TTL 15 min."""
    import time
    if _SEQ34_CACHE[1] and (time.time() - _SEQ34_CACHE[0]) < 900:
        return _SEQ34_CACHE[1]
    m = {}
    try:
        from seq_scan import today_seq_map
        m = today_seq_map()
        _SEQ34_CACHE[0] = time.time()
        _SEQ34_CACHE[1] = m
    except Exception:
        log.debug("seq34 map failed", exc_info=True)
    return m


def _enrich_buy_flags(results: list) -> None:
    """🟢 REV / 🔵 BRK buy-flags on the latest bar (validated 2026-07-18, flagval.py; the
    actionable output of the two-zone study). Needs 5-bar RSI lag features not in the
    latest-bar row → one batched DuckDB query for the result tickers' recent daily bars.
      REV = min-5 RSI<38 & RSI 30-55 & up-bar & beta≤13  → +1.04%/win44/PF1.15/+8.4σ/4-6yr
      BRK = RSI crosses 50 up & up-bar & turbo≤28         → +0.57%/+2.1σ/4-6yr (weaker)
    Sets rev_buy/brk_buy (bool) + buy_flag ('🟢REV'|'🔵BRK'|'')."""
    if not results:
        return
    from ai_journal.db import get_analytics_conn
    tks = sorted({str(r.get("ticker")) for r in results if r.get("ticker")})
    if not tks:
        return
    # EDGE fires map (build=False: a cold frame returns {} instead of blocking the scan;
    # the startup warmer fills the (60,3M) frame minutes after boot)
    try:
        from edge_replay import latest_edges_map
        _edge_fires = latest_edges_map()
    except Exception:
        _edge_fires = {}
    _seq34 = _seq34_map()
    _confm = _conf_map()
    a = get_analytics_conn()
    try:
        ph = ",".join("?" * len(tks))
        df = a.execute(f"""
            WITH r AS (SELECT ticker, date, close, rsi_14, coalesce(atr_14,0) atr_14,
                 coalesce(z_sig,'') z, coalesce(t_sig,'') t, coalesce(l_sig,'') l,
                 coalesce(close_suffix,'') sx, coalesce(bar_body_wick,'') bw,
                 coalesce(bar_gap_range,'') gr, coalesce(bar_line5,'') q5,
                 coalesce(vol_bucket,'') vb,
                 row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
               FROM bars WHERE ticker IN ({ph})
                 AND date >= (SELECT max(date) FROM bars) - INTERVAL 12 DAY)
            SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date
        """, tks).fetchdf()
    finally:
        a.close()
    from buyseq_context import make_tokens, lookup as _ctx_lookup
    by = {}
    toks = {}
    _atr_db = {}   # {ticker: atr_pct} straight from stored bars.atr_14 (no frame/API dependency)
    for tk, g in df.groupby("ticker"):
        g = g.sort_values("date")
        by[tk] = (g["rsi_14"].to_numpy(float), g["close"].to_numpy(float),
                  [str(x)[:10] for x in g["date"]])
        _lc = float(g["close"].iloc[-1]); _la = float(g["atr_14"].iloc[-1])
        if _lc > 0 and _la > 0:
            _atr_db[tk] = round(_la / _lc, 4)
        _ct, _ft = [], []
        for _z, _t, _l in zip(g["z"], g["t"], g["l"]):
            _c, _f = make_tokens(str(_z), str(_t), str(_l))
            _ct.append(_c); _ft.append(_f)
        toks[tk] = {"c": _ct, "f": _ft,
                    "sx": [str(x) for x in g["sx"]], "bw": [str(x) for x in g["bw"]],
                    "gr": [str(x) for x in g["gr"]], "q5": [str(x) for x in g["q5"]],
                    "vb": [str(x) for x in g["vb"]]}
    # ── MTF confirmation sets (validated 2026-07-19, project_mtf_confirmation) ─────────
    # Per intraday TF, two day-sets over the last ~2 weeks for these tickers:
    #   rev:  the strict REV-turn printed (min5-RSI<38 · RSI 30-55 · up · rising)
    #   bs60: intraday buy_score>=60 (v2 backbone; 15m computes v2 on the fly — its column
    #         is empty there by design, see the nightly hook note in studio_api)
    _rev_sets: dict = {}
    _bs_sets: dict = {}
    try:
        import duckdb as _dk
        from studio.paths import db_path as _idbp
        from prebreak_v2 import prebreak_v2_score_sql as _pv2sql
        _BS_T = ("LEAST(GREATEST((1.5*LEAST(GREATEST(COALESCE({v2},0),0),27)"
                 " + 12*(CASE WHEN upper(COALESCE(vol_bucket,''))='B' THEN 1 ELSE 0 END)"
                 " + 0.9*GREATEST(0, 55-COALESCE(rsi_14,50)))*1.3, 0), 100)")
        def _veto(v2col):
            _b = _BS_T.format(v2=v2col)
            return (f"CASE WHEN rsi_14>=60 THEN LEAST({_b},20) "
                    f"WHEN rsi_14<28 THEN LEAST({_b},60) ELSE {_b} END")
        for _tfdb, _v2c in (("4h", "prebreak_v2"), ("1h", "prebreak_v2"),
                            ("15m", f"({_pv2sql()})")):
            try:
                _c = _dk.connect(_idbp(f"studio_{_tfdb}.duckdb"), read_only=True)
                _rd = _c.execute(f"""
                    WITH r AS (SELECT ticker, date, close, rsi_14,
                        MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date
                            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
                        LAG(close)  OVER (PARTITION BY ticker ORDER BY date) cp,
                        LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
                      FROM bars WHERE ticker IN ({ph}) AND close >= 5
                        AND date >= (SELECT max(date) FROM bars) - INTERVAL 14 DAY)
                    SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d
                    FROM r WHERE m5 < 38 AND rsi_14 BETWEEN 30 AND 55
                      AND close > cp AND rsi_14 > rp
                """, tks).fetchdf()
                _bd = _c.execute(f"""
                    SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d
                    FROM bars WHERE ticker IN ({ph}) AND close >= 5
                      AND date >= (SELECT max(date) FROM bars) - INTERVAL 6 DAY
                      AND ({_veto(_v2c)}) >= 60
                """, tks).fetchdf()
                _c.close()
                _rev_sets[_tfdb] = set(zip(_rd["ticker"], _rd["d"]))
                _bs_sets[_tfdb] = set(zip(_bd["ticker"], _bd["d"]))
            except Exception:
                _rev_sets[_tfdb] = set(); _bs_sets[_tfdb] = set()
    except Exception:
        pass
    _TFS = ("4h", "1h", "15m")
    for r in results:
        rev = brk = False
        turn_ok = False
        d0 = d1 = ""
        tk = str(r.get("ticker"))
        d = by.get(tk)
        if d is not None and len(d[0]) >= 2:
            rs, cl, dts = d
            rr, rp, c, cp = rs[-1], rs[-2], cl[-1], cl[-2]
            d0, d1 = dts[-1], dts[-2]
            m5 = float(min(rs[-5:])) if len(rs) >= 5 else float(min(rs))
            up = c > cp
            beta = float(r.get("beta_score") or 0)
            turbo = float(r.get("turbo_score") or 0)
            if rr == rr and rp == rp and c > 0 and cp > 0:      # not-NaN guard
                if m5 < 38 and 30 <= rr <= 55 and up and beta <= 13:
                    rev = True
                if rp < 50 <= rr and up and turbo <= 28:
                    brk = True
                turn_ok = up and rr > rp and rr < 55
        r["rev_buy"] = rev
        r["brk_buy"] = brk
        # MTF annotations (same semantics as the Superchart BUY row)
        def _hit(s):
            return (tk, d0) in s or (tk, d1) in s
        if rev:
            r["mtf_echo"] = _hit(_rev_sets.get("4h", set())) or _hit(_rev_sets.get("1h", set()))
        n_rev = sum(1 for t in _TFS if _hit(_rev_sets.get(t, set())))
        n_bs = sum(1 for t in _TFS if _hit(_bs_sets.get(t, set())))
        if float(r.get("buy_score") or 0) >= 60:
            r["mtf_score_conf"] = n_bs                          # 0-3 (0 = validated hard-skip)
        if turn_ok and n_rev > 0 and not rev and not brk:
            r["turn_echo_n"] = n_rev                            # ①②③
        r["h4_rev_today"] = (tk, d0) in _rev_sets.get("4h", set())
        r["h1_rev_today"] = (tk, d0) in _rev_sets.get("1h", set())
        # heavy institutional VSA line on the latest bar (triple-confluence leg, 2026-07-20:
        # 🟢REV + RED L34 + ▲4H = +2.05%/PF1.32/5-6yr, TRAIN +2.27 ≈ TEST +1.85).
        # RED only (close<open, absorbed weakness): green L34 on a reversal bar is the
        # trap type (−1.01%, PF 0.87, TRAIN −2.81). l_sig never takes L43/L64/L22.
        try:
            _l34_red = float(r.get("last_price") or 0) < float(r.get("open") or 0)
        except (TypeError, ValueError):
            _l34_red = False
        r["heavy_l"] = str(r.get("tz_wlnbb_l_signal") or "") == "L34" and _l34_red
        # ⏱ ATR% for the client-side time-to-target forecast (2026-07-26) — straight from
        # stored bars.atr_14 (nightly-enriched), NOT the edge frame and NOT a Massive call:
        # no cold-frame gaps, no network, always present.
        _ap = _atr_db.get(tk)
        if _ap:
            r["atr_pct"] = _ap
        # ✅ EDGE fires (2026-07-20): validated Edge-board setups on the last 5 bars,
        # from the SAME edge_replay masks the backtest uses. "G3" = today, "G3·2d" = 2 bars ago.
        _ef = _edge_fires.get(tk)
        if _ef:
            r["edges"] = [c if age == 0 else f"{c}·{age}d" for c, age in _ef]
            r["edge_n"] = sum(1 for _, age in _ef if age == 0)
            # EDGE🟢 premium combo (validated 2026-07-20, edgebuy.py): location-reversal
            # setups get a REAL boost from same-bar 🟢REV — QZC +1.65→+2.69 (med flips +),
            # D+L1 +1.60→+3.46 (TRAIN turns +), RTB →+2.04 6/6yr, P55 0.25→+1.89.
            # NOT the others: WSH/ZRT are hurt by REV; CAP/Z11/L43/G3A never coincide.
            # mtf_echo=False is the validated hard veto (−1.07%, 0/6yr) — a vetoed
            # ⚠️REV must not light the premium combo.
            r["edge_rev"] = bool(rev) and r.get("mtf_echo") is not False and any(
                age == 0 and c in ("QZC", "D+L1", "RTB", "P55") for c, age in _ef)
        _sq = _seq34.get(tk)
        if _sq:
            r["seq34"] = _sq
        _cf = _confm.get(tk)
        if _cf:
            if _cf[0] != 0:
                r["conf_score"] = _cf[0]
                if _cf[1]:
                    r["conf_top"] = _cf[1]
            elif len(_cf) > 2 and _cf[2]:
                # gray info-only tier — unvalidated sub-threshold cells, core silent
                r["conf_ext"] = _cf[2]
                if _cf[3]:
                    r["conf_ext_top"] = _cf[3]
        # ⤴/⤵ preceding-sequence context on the BUY signals (2026-07-20 redesign):
        # the fire bar's PRECEDING 2-4 bars looked up in the era-consistent
        # conditioner table (buyseq_context.json). Strongest |lift| wins.
        _sigs = []
        if rev: _sigs.append("rev")
        if brk: _sigs.append("brk")
        if r.get("h4_rev_today"): _sigs.append("h4")
        if r.get("heavy_l"): _sigs.append("lh")
        if r.get("edge_n"): _sigs.append("ea")
        if r.get("edge_rev"): _sigs.append("ep")
        if any(r.get(k) for k in ("fly_abcd", "fly_cd", "fly_bd", "fly_ad")): _sigs.append("fly")
        if r.get("mtf_score_conf") is not None: _sigs.append("score")   # digits (score-day)
        if r.get("turn_echo_n"): _sigs.append("turn")                   # ①②③
        if r.get("h1_rev_today"): _sigs.append("h1")                    # △
        _sigs.append("anyb")   # per-bar mini-forecast (2026-07-20b): every bar gets a look
        if _sigs:
            _tt = toks.get(tk)
            if _tt and len(_tt["c"]) >= 2:
                # sequence ends ON the latest bar (2026-07-20e user fix)
                _hit = _ctx_lookup(dict(_tt), _sigs)
                if _hit:
                    r["seq_ctx"] = _hit
        _flag = ("⚠️REV" if (rev and r.get("mtf_echo") is False) else
                 "🟢REV" if rev else
                 "🔵BRK" if brk else
                 (f"{n_bs}/3" if r.get("mtf_score_conf") is not None else
                  ({1: "①", 2: "②", 3: "③"}.get(r.get("turn_echo_n"), ""))))
        r["buy_flag"] = _flag + ("▲" if r["h4_rev_today"] and _flag else "")


def _enrich_seq_patterns(results: list, lookback_n: int = 10) -> None:
    """In-place enrichment: tzt4 / ttt6 / t1seq patterns for DB-mode scan results.
    Runs one DuckDB query per universe group. Sets *_match/*_age/*_tier/*_suffix/*_rsi."""
    if not results:
        return
    import pandas as _pd
    from collections import defaultdict

    # Group by actual universe for the WHERE clause (split rows use ticker-only query)
    by_uni: dict[str, list] = defaultdict(list)
    for r in results:
        uni = r.get("universe") or "sp500"
        by_uni["__any__" if uni == "split" else uni].append(r)

    conn = get_conn(read_only=True)
    lookback = max(int(lookback_n), 1)
    z1g_lb = max(lookback, 65)  # z1gt2g is rare (~9/yr), needs longer window
    try:
        for uni, rows in by_uni.items():
            tickers = [r.get("ticker") for r in rows if r.get("ticker")]
            if not tickers:
                continue
            ph = ",".join("?" * len(tickers))
            uni_where = f"AND universe = '{uni}'" if uni != "__any__" else ""
            try:
                df = conn.execute(f"""
                    WITH deduped AS (
                      SELECT ticker, date,
                             sig_t4, sig_z, sig_t, sig_t1, sig_t6,
                             sig_t2, sig_t2g, sig_t3, sig_t5, sig_t9, sig_t10,
                             sig_t1g, sig_t11, sig_t12, sig_z1g,
                             rsi_14, composite_full_suffix
                      FROM bars
                      WHERE ticker IN ({ph}) {uni_where}
                      QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
                    ),
                    ranked AS (
                      SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                      FROM deduped
                    ),
                    pat AS (
                      SELECT ticker, rn - 1 AS age, rn, rsi_14,
                             composite_full_suffix AS sfx,
                             sig_t4, sig_t1, sig_t6, sig_t3, sig_t9, sig_t2g,
                             LEAD(sig_z,   1) OVER (PARTITION BY ticker ORDER BY rn) AS z_1,
                             LEAD(sig_t,   1) OVER (PARTITION BY ticker ORDER BY rn) AS t_1,
                             LEAD(sig_z,   2) OVER (PARTITION BY ticker ORDER BY rn) AS z_2,
                             LEAD(sig_t,   2) OVER (PARTITION BY ticker ORDER BY rn) AS t_2,
                             LEAD(sig_t4,  2) OVER (PARTITION BY ticker ORDER BY rn) AS t4_2,
                             LEAD(sig_t3,  2) OVER (PARTITION BY ticker ORDER BY rn) AS t3_2,
                             LEAD(sig_t9,  2) OVER (PARTITION BY ticker ORDER BY rn) AS t9_2,
                             LEAD(sig_t10, 2) OVER (PARTITION BY ticker ORDER BY rn) AS t10_2,
                             LEAD(sig_t2,  2) OVER (PARTITION BY ticker ORDER BY rn) AS t2_2,
                             LEAD(sig_t2g, 2) OVER (PARTITION BY ticker ORDER BY rn) AS t2g_2,
                             LEAD(sig_t5,  2) OVER (PARTITION BY ticker ORDER BY rn) AS t5_2,
                             LEAD(sig_t1g, 2) OVER (PARTITION BY ticker ORDER BY rn) AS t1g_2,
                             LEAD(sig_t11, 2) OVER (PARTITION BY ticker ORDER BY rn) AS t11_2,
                             LEAD(sig_t12, 2) OVER (PARTITION BY ticker ORDER BY rn) AS t12_2,
                             LEAD(sig_t1,  1) OVER (PARTITION BY ticker ORDER BY rn) AS t1_1,
                             LEAD(sig_t2,  1) OVER (PARTITION BY ticker ORDER BY rn) AS t2_1,
                             LEAD(sig_t2g, 1) OVER (PARTITION BY ticker ORDER BY rn) AS t2g_1,
                             LEAD(sig_t3,  1) OVER (PARTITION BY ticker ORDER BY rn) AS t3_1,
                             LEAD(sig_t4,  1) OVER (PARTITION BY ticker ORDER BY rn) AS t4_1,
                             LEAD(sig_t5,  1) OVER (PARTITION BY ticker ORDER BY rn) AS t5_1,
                             LEAD(sig_t9,  1) OVER (PARTITION BY ticker ORDER BY rn) AS t9_1,
                             LEAD(sig_t10, 1) OVER (PARTITION BY ticker ORDER BY rn) AS t10_1,
                             LEAD(sig_t11, 1) OVER (PARTITION BY ticker ORDER BY rn) AS t11_1,
                             LEAD(sig_t12, 1) OVER (PARTITION BY ticker ORDER BY rn) AS t12_1,
                             LEAD(sig_t1g, 1) OVER (PARTITION BY ticker ORDER BY rn) AS t1g_1,
                             LEAD(sig_t3,  3) OVER (PARTITION BY ticker ORDER BY rn) AS t3_3,
                             LEAD(sig_z1g, 2) OVER (PARTITION BY ticker ORDER BY rn) AS z1g_2,
                             LEAD(composite_full_suffix, 1) OVER (PARTITION BY ticker ORDER BY rn) AS sfx_1,
                             LEAD(composite_full_suffix, 2) OVER (PARTITION BY ticker ORDER BY rn) AS sfx_2
                      FROM ranked WHERE rn <= {z1g_lb + 4}
                    ),
                    tzt4_m AS (
                      SELECT ticker, age, rsi_14, sfx,
                             CASE WHEN t4_2>0 THEN 'T1'
                                  WHEN t3_2>0 OR t9_2>0 OR t10_2>0 THEN 'T2'
                                  WHEN t2_2>0 OR t2g_2>0 OR t5_2>0 THEN 'T3'
                                  ELSE 'T4' END AS tier
                      FROM pat WHERE sig_t4>0 AND z_1>0
                        AND (t4_2>0 OR t3_2>0 OR t9_2>0 OR t10_2>0 OR t2_2>0
                             OR t2g_2>0 OR t5_2>0 OR t1g_2>0 OR t11_2>0 OR t12_2>0)
                        AND age < {lookback}
                    ),
                    ttt6_m AS (
                      SELECT ticker, age, rsi_14, sfx,
                             CASE WHEN t3_2>0 OR t2_2>0 OR t10_2>0 THEN 'T1'
                                  WHEN t4_2>0 OR t1g_2>0 THEN 'T2'
                                  WHEN t2g_2>0 OR t9_2>0 THEN 'T3'
                                  ELSE 'T4' END AS tier
                      FROM pat WHERE sig_t6>0
                        AND (t1_1>0 OR t2_1>0 OR t2g_1>0 OR t3_1>0 OR t4_1>0
                             OR t5_1>0 OR t9_1>0 OR t10_1>0 OR t11_1>0 OR t12_1>0 OR t1g_1>0)
                        AND (t3_2>0 OR t2_2>0 OR t10_2>0 OR t4_2>0 OR t1g_2>0 OR t2g_2>0
                             OR t9_2>0 OR t5_2>0 OR t11_2>0 OR t12_2>0)
                        AND age < {lookback}
                    ),
                    t1seq_m AS (
                      SELECT ticker, age, rsi_14, sfx,
                             CASE WHEN z_2>0 AND z_1>0 THEN 'T1'
                                  WHEN t_2>0 AND z_1>0 THEN 'T2'
                                  WHEN z_2>0 AND t_1>0 THEN 'T3'
                                  ELSE 'T4' END AS tier
                      FROM pat WHERE sig_t1>0 AND (z_2>0 OR t_2>0) AND (z_1>0 OR t_1>0)
                        AND age < {lookback}
                    ),
                    t3seq_m AS (
                      SELECT ticker, age, rsi_14, sfx,
                             CASE WHEN COALESCE(t3_1,0)=0 AND sfx LIKE 'NBI%' THEN 'fresh-nbi'
                                  WHEN COALESCE(t3_1,0)=0 THEN 'fresh'
                                  WHEN COALESCE(t3_1,0)>0 AND COALESCE(t3_2,0)>0 THEN 'streak'
                                  ELSE 'plain' END AS tier
                      FROM pat WHERE sig_t3>0 AND rsi_14<35 AND age < {lookback}
                    ),
                    t9rsi_m AS (
                      SELECT ticker, age, rsi_14, sfx,
                             CASE WHEN sfx LIKE 'N%' THEN 'premium' ELSE 'base' END AS tier
                      FROM pat WHERE sig_t9>0 AND rsi_14<35 AND age < {lookback}
                    ),
                    z1gt2g_m AS (
                      SELECT ticker, age, rsi_14, sfx_1 AS t1_sfx,
                             CASE WHEN sfx_1 LIKE 'NHA%' AND sfx_2 LIKE 'EDP%' THEN 'premium'
                                  WHEN sfx_1 LIKE 'NHA%' THEN 'hi-nha'
                                  WHEN sfx_2 LIKE 'EDP%' THEN 'hi-edp'
                                  ELSE 'base' END AS tier
                      FROM pat WHERE sig_t2g>0 AND sfx LIKE 'EUR%'
                        AND COALESCE(t1_1,0)>0 AND COALESCE(z1g_2,0)>0
                        AND rsi_14 BETWEEN 35 AND 60 AND age < {z1g_lb}
                    ),
                    agg4 AS (SELECT ticker, MIN(age) AS age4,
                               ARG_MIN(tier,age) AS tier4, ARG_MIN(sfx,age) AS sfx4,
                               ARG_MIN(rsi_14,age) AS rsi4 FROM tzt4_m GROUP BY ticker),
                    agg6 AS (SELECT ticker, MIN(age) AS age6,
                               ARG_MIN(tier,age) AS tier6, ARG_MIN(sfx,age) AS sfx6,
                               ARG_MIN(rsi_14,age) AS rsi6 FROM ttt6_m GROUP BY ticker),
                    agg1 AS (SELECT ticker, MIN(age) AS age1,
                               ARG_MIN(tier,age) AS tier1, ARG_MIN(sfx,age) AS sfx1,
                               ARG_MIN(rsi_14,age) AS rsi1 FROM t1seq_m GROUP BY ticker),
                    agg_t3 AS (SELECT ticker, MIN(age) AS aget3,
                               ARG_MIN(tier,age) AS tiert3, ARG_MIN(sfx,age) AS sfxt3,
                               ARG_MIN(rsi_14,age) AS rsit3 FROM t3seq_m GROUP BY ticker),
                    agg_t9 AS (SELECT ticker, MIN(age) AS aget9,
                               ARG_MIN(tier,age) AS tiert9, ARG_MIN(sfx,age) AS sfxt9,
                               ARG_MIN(rsi_14,age) AS rsit9 FROM t9rsi_m GROUP BY ticker),
                    agg_z1g AS (SELECT ticker, MIN(age) AS agez,
                               ARG_MIN(tier,age) AS tierz, ARG_MIN(t1_sfx,age) AS sfxz,
                               ARG_MIN(rsi_14,age) AS rsiz FROM z1gt2g_m GROUP BY ticker)
                    SELECT r.ticker,
                           a4.age4, a4.tier4, a4.sfx4, a4.rsi4,
                           a6.age6, a6.tier6, a6.sfx6, a6.rsi6,
                           a1.age1, a1.tier1, a1.sfx1, a1.rsi1,
                           at3.aget3, at3.tiert3, at3.sfxt3, at3.rsit3,
                           at9.aget9, at9.tiert9, at9.sfxt9, at9.rsit9,
                           az.agez, az.tierz, az.sfxz, az.rsiz
                    FROM (SELECT DISTINCT ticker FROM ranked WHERE rn=1) r
                    LEFT JOIN agg4  a4  USING (ticker)
                    LEFT JOIN agg6  a6  USING (ticker)
                    LEFT JOIN agg1  a1  USING (ticker)
                    LEFT JOIN agg_t3 at3 USING (ticker)
                    LEFT JOIN agg_t9 at9 USING (ticker)
                    LEFT JOIN agg_z1g az USING (ticker)
                """, tickers).fetchdf()
            except Exception as exc:
                log.warning("_enrich_seq_patterns query failed for %s: %s", uni, exc)
                continue

            lookup = {row.ticker: row for row in df.itertuples(index=False)}
            for r in rows:
                row = lookup.get(r.get("ticker"))
                if row is None:
                    r["tzt4_match"]   = False; r["tzt4_age"]   = None
                    r["ttt6_match"]   = False; r["ttt6_age"]   = None
                    r["t1seq_match"]  = False; r["t1seq_age"]  = None
                    r["t3seq_match"]  = False; r["t3seq_age"]  = None
                    r["t9rsi_match"]  = False; r["t9rsi_age"]  = None
                    r["z1gt2g_match"] = False; r["z1gt2g_age"] = None
                    continue
                def _set(prefix, age_v, tier_v, sfx_v, rsi_v):
                    if age_v is None or (_pd.isna(age_v) if hasattr(_pd, "isna") else False):
                        r[f"{prefix}_match"] = False; r[f"{prefix}_age"] = None
                    else:
                        r[f"{prefix}_match"]  = True
                        r[f"{prefix}_age"]    = int(age_v)
                        r[f"{prefix}_tier"]   = str(tier_v or "")
                        r[f"{prefix}_suffix"] = str(sfx_v or "")
                        r[f"{prefix}_rsi"]    = round(float(rsi_v or 0), 1)
                _set("tzt4",   row.age4,  row.tier4,  row.sfx4,  row.rsi4)
                _set("ttt6",   row.age6,  row.tier6,  row.sfx6,  row.rsi6)
                _set("t1seq",  row.age1,  row.tier1,  row.sfx1,  row.rsi1)
                _set("t3seq",  row.aget3, row.tiert3, row.sfxt3, row.rsit3)
                _set("t9rsi",  row.aget9, row.tiert9, row.sfxt9, row.rsit9)
                _set("z1gt2g", row.agez,  row.tierz,  row.sfxz,  row.rsiz)
    finally:
        conn.close()


def run_ultra_db_scan(
    universes:   list[str] | None = None,
    min_price:   float | None     = None,
    min_volume:  int   | None     = None,
    max_age_days: int             = 7,    # only return tickers whose latest bar is within N days
    age_signals: list[str] | None = None, # if provided, compute bars-ago for these signals
    age_lookback: int             = 20,
) -> dict:
    """Get the most recent enriched bar per ticker as ULTRA-compatible rows.

    Returns dict with `results` (list of row dicts) + metadata.

    Special universe: if universes=['split'], fetches sp500+nasdaq from DB and
    cross-filters to the live reverse-split universe (D-7→D+90 window), then
    enriches each row with split lifecycle metadata.
    """
    universes = universes or ["sp500", "nasdaq"]
    started = time.time()

    # ── SPLIT universe: resolve to real DB universes + capture live ticker set ─
    split_meta: dict = {}   # ticker → split metadata dict (populated if split mode)
    split_mode = len(universes) == 1 and universes[0] == "split"
    if split_mode:
        try:
            from split_universe import split_service, normalize_split_symbol
            sresult = split_service.get_split_universe_result()
            live_tickers = frozenset(sresult.tickers)
            split_meta   = {r["ticker"]: r for r in sresult.rows}
            log.info("UltraDB SPLIT mode: %d live split tickers", len(live_tickers))
        except Exception as exc:
            log.warning("UltraDB SPLIT mode: split_service failed (%s) — returning empty", exc)
            return {
                "results": [], "running": False, "stage": "done", "progress_pct": 100,
                "elapsed_seconds": 0, "data_source": "studio_db",
                "universes": ["split"], "row_count": 0,
                "min_price": min_price, "min_volume": min_volume,
                "scanned_at": pd.Timestamp.utcnow().isoformat(),
                "split_count": 0, "split_error": str(exc),
            }
        universes = ["sp500", "nasdaq"]  # query all standard universes from DB

    # ── ZONE universe: tickers whose latest bar sits inside an active HV zone ──
    zone_meta: dict = {}    # ticker → zone metadata (populated if zone mode)
    zone_mode = len(universes) == 1 and universes[0] == "zone"
    if zone_mode:
        try:
            from ai_journal.zone_events import tickers_in_zone
            zone_meta = tickers_in_zone(vol_min=5.0, lb_max=90)
            log.info("UltraDB ZONE mode: %d tickers currently inside a zone", len(zone_meta))
        except Exception as exc:
            log.warning("UltraDB ZONE mode: tickers_in_zone failed (%s) — returning empty", exc)
            return {
                "results": [], "running": False, "stage": "done", "progress_pct": 100,
                "elapsed_seconds": 0, "data_source": "studio_db",
                "universes": ["zone"], "row_count": 0,
                "min_price": min_price, "min_volume": min_volume,
                "scanned_at": pd.Timestamp.utcnow().isoformat(),
                "zone_count": 0, "zone_error": str(exc),
            }
        universes = ["sp500", "nasdaq", "russell2k"]  # widest standard coverage

    conn = get_conn(read_only=True)
    try:
        placeholders = ",".join("?" * len(universes))

        # ── Get each ticker's most recent bar ─────────────────────────────────
        # Dedup to ONE row per ticker. A ticker dual-listed across universes
        # (e.g. LNT/MDLZ/NWSA in both sp500 & nasdaq) would otherwise produce a
        # row per universe → duplicate entries in the screener. Prefer sp500,
        # then nasdaq, then anything else, taking the most recent bar.
        latest = conn.execute(f"""
            WITH ranked AS (
              SELECT *,
                     ROW_NUMBER() OVER (
                       PARTITION BY ticker
                       ORDER BY date DESC,
                                CASE universe
                                  WHEN 'sp500'  THEN 0
                                  WHEN 'nasdaq' THEN 1
                                  ELSE 2
                                END
                     ) AS rn
              FROM bars
              WHERE universe IN ({placeholders})
            )
            SELECT * FROM ranked WHERE rn = 1
        """, list(universes)).fetchdf()

        log.info("UltraDB scan: fetched %d latest bars for %s", len(latest), universes)

        # ── Stale filter: drop tickers whose latest bar is older than `max_age_days`
        # relative to the DB-wide max date. Without this, delisted/halted tickers
        # would show up with months-old prices (e.g. MCTA $29.36 from 2025-11-11).
        if max_age_days is not None and not latest.empty:
            db_max_date = pd.to_datetime(latest["date"]).max()
            cutoff      = db_max_date - pd.Timedelta(days=max_age_days)
            before      = len(latest)
            latest      = latest[pd.to_datetime(latest["date"]) >= cutoff]
            log.info("UltraDB scan: stale-bar filter (max_age=%dd, cutoff=%s): %d → %d",
                     max_age_days, cutoff.date(), before, len(latest))

        # Optional filters
        if min_price is not None:
            latest = latest[latest["close"] >= min_price]
        if min_volume is not None and "avg_vol_20d" in latest.columns:
            latest = latest[latest["avg_vol_20d"] >= min_volume]

        # ── Compute sig_ages JSON via per-ticker window (last `age_lookback` bars) ─
        # Default: compute for all T/Z + L + common toggle signals so the Ultra
        # frontend's N=lookback filter (key format `tz_t2g`, `tz_z3`, etc.) works
        # out of the box without the caller passing `age_signals`.
        if age_signals is None:
            age_signals = [
                # Individual T signals
                *(f"sig_t{n}" for n in range(1, 13)), "sig_t1g", "sig_t2g",
                # Individual Z signals
                *(f"sig_z{n}" for n in range(1, 13)), "sig_z1g", "sig_z2g",
                # Aggregate T/Z
                "sig_t", "sig_z", "sig_tz_flip",
                # L family (chart format flags)
                "sig_l_any", "l34", "l43", "l22", "be_up", "bo_up", "bx_up", "vbo_up",
                # GOG
                "sig_g1", "sig_g2", "sig_g4", "sig_g6", "sig_g11",
                # Common combo
                "sig_buy", "sig_3g", "sig_conso", "sig_svs",
                "sig_va", "sig_vol_5x", "sig_vol_10x", "sig_vol_20x",
                # PREUP/PREDN
                "sig_p2", "sig_p3", "sig_p50", "sig_p55", "sig_p66", "sig_p89", "sig_any_p",
                "sig_d2", "sig_d3", "sig_d50", "sig_d55", "sig_d66", "sig_d89", "sig_any_d",
                # 260523 — AD-FRESH / AD-CLUSTER + WYC phases + PREBREAK tiers +
                # PB sub-flags. Frontend `evHit(col)` looks at r.<col>_age, which
                # we emit as a separate column in _row_to_dict() so the dotted
                # filter (WYC ACC_TR, PREBREAK PRIME, AD-FRESH, etc.) actually
                # filters out older signals when N=1.
                "ad_fresh", "ad_cluster",
                "prebreak_prime", "prebreak_ready", "prebreak_watch",
                "pb_lvbo", "pb_stop_cause", "pb_wvf_confirm", "pb_macro_penalty",
                "pb_pp_rtv", "pb_fly_cd_c", "pb_follow_confirm", "seq_l34_eb",
                "wyc_spring", "wyc_sos", "wyc_in_tr", "wyc_sow",
            ]

        # Map sig_* (DB col) → frontend filter key. Most are `tz_*` for T/Z,
        # others use their bare name. Frontend SIG_GROUPS keys must match.
        # EXACT matches go first; startswith would otherwise catch them wrong
        # (e.g. "sig_t"[4:] == "t" → "tz_t", but we want "tz_any_t").
        def _filter_key(sig_col: str) -> str:
            if sig_col == "sig_t":       return "tz_any_t"
            if sig_col == "sig_z":       return "tz_any_z"
            if sig_col == "sig_tz_flip": return "tz_bull_flip"
            if sig_col.startswith("sig_t"):
                # sig_t2g → tz_t2g, sig_t10 → tz_t10
                return "tz_t" + sig_col[5:]
            if sig_col.startswith("sig_z"):
                return "tz_z" + sig_col[5:]
            if sig_col.startswith("sig_"):
                return sig_col[4:]   # sig_buy → buy, sig_g1 → g1
            return sig_col           # l34, be_up, etc.

        sig_ages_by_id: dict = {}
        # Filter to columns that actually exist (defensive)
        avail_cols_df = conn.execute("DESCRIBE bars").fetchdf()
        avail_cols = set(avail_cols_df["column_name"].tolist())
        age_signals_existing = [c for c in age_signals if c in avail_cols]

        if age_signals_existing:
            # Single-pass SQL aggregation — for each signal compute MIN(rn) where
            # sig=1 within the last `age_lookback` bars per ticker. Avoids the
            # pandas groupby/iteration loop which took 18s on 3,000+ NASDAQ
            # tickers × 50 signals. DuckDB does this in ~1-2s.
            min_rn_exprs = ", ".join(
                f"MIN(CASE WHEN {sig} = 1 THEN rn END) AS {sig}_age"
                for sig in age_signals_existing
            )
            age_df = conn.execute(f"""
                WITH ranked AS (
                  SELECT ticker, universe,
                         {", ".join(age_signals_existing)},
                         ROW_NUMBER() OVER (PARTITION BY ticker, universe ORDER BY date DESC) AS rn
                  FROM bars
                  WHERE universe IN ({placeholders})
                )
                SELECT ticker, universe, {min_rn_exprs}
                FROM ranked
                WHERE rn <= ?
                GROUP BY ticker, universe
            """, [*universes, age_lookback]).fetchdf()

            # Build sig_ages_by_id from the aggregated DataFrame. rn=1 is the
            # current bar so subtract 1 to get bars-ago (age 0 = today).
            for row in age_df.itertuples(index=False):
                ages = {}
                ticker = row.ticker
                universe = row.universe
                for sig in age_signals_existing:
                    val = getattr(row, f"{sig}_age", None)
                    # DuckDB returns pd.NA for NULL; guard with pd.isna
                    if val is None or pd.isna(val):
                        continue
                    age = int(val) - 1
                    ages[sig] = age
                    fk = _filter_key(sig)
                    if fk != sig:
                        ages[fk] = age
                if ages:
                    sig_ages_by_id[(ticker, universe)] = ages

        # ── Build row dicts ───────────────────────────────────────────────────
        # Subset of age-tracked cols that the JS `evHit(col)` helper looks up
        # via r.<col>_age (rather than via sig_ages JSON). These need direct
        # row fields so PREBREAK / WYC / AD-FRESH filters can honour N=lookback.
        _EMIT_AGE_FIELDS = {
            "ad_fresh", "ad_cluster",
            "prebreak_prime", "prebreak_ready", "prebreak_watch",
            "pb_lvbo", "pb_stop_cause", "pb_wvf_confirm", "pb_macro_penalty",
            "pb_pp_rtv", "pb_fly_cd_c", "pb_follow_confirm", "seq_l34_eb",
            "wyc_spring", "wyc_sos", "wyc_in_tr", "wyc_sow",
            "swing_type",
        }

        results = []
        for _, row in latest.iterrows():
            d = _row_to_dict(row)
            key = (d.get("ticker"), d.get("universe"))
            ages = sig_ages_by_id.get(key) or {}
            if ages:
                d["sig_ages"] = json.dumps(ages)
            # Emit <col>_age direct fields for the JS evHit() helper, since the
            # client-side filter doesn't look these up inside the sig_ages JSON.
            for col in _EMIT_AGE_FIELDS:
                if col in ages:
                    d[f"{col}_age"] = ages[col]
                elif d.get(col):
                    # Field is set on current bar but age wasn't tracked → 0.
                    d[f"{col}_age"] = 0
            results.append(d)

    finally:
        conn.close()

    # ── ZONE / SPLIT post-filter: keep only the matched set + add metadata ─────
    out_universes = ["split"] if split_mode else (["zone"] if zone_mode else universes)
    extra_meta: dict = {}
    if zone_mode:
        try:
            filtered = []
            for r in results:
                z = zone_meta.get(str(r.get("ticker", "")))
                if z:
                    r.update(z)                       # zone_low/high/date/mult/dir/age/pos/dist_*
                    r["universe"] = "zone"            # tag row as zone universe
                    filtered.append(r)
            results = filtered
            extra_meta["zone_count"]    = len(zone_meta)
            extra_meta["zone_in_db"]    = len(results)
            extra_meta["zone_missing"]  = len(zone_meta) - len(results)
            log.info("UltraDB ZONE: %d/%d in-zone tickers found in DB", len(results), len(zone_meta))
        except Exception as exc:
            log.warning("UltraDB ZONE post-filter failed: %s", exc)
    if split_mode:
        try:
            from split_universe import normalize_split_symbol
            filtered = []
            for r in results:
                norm = normalize_split_symbol(r.get("ticker", ""))
                if norm in live_tickers:
                    s = split_meta.get(norm, {})
                    r["split_date"]            = s.get("split_date", "")
                    r["split_ratio"]           = s.get("ratio_str", "")
                    r["split_status"]          = s.get("split_status", "")
                    r["split_days_offset"]     = s.get("days_offset", 0)
                    r["split_phase"]           = s.get("phase", "")
                    r["split_wave"]            = s.get("wave", "")
                    r["split_watch_until"]     = s.get("watch_until", "")
                    r["split_next_wave_label"] = s.get("next_wave_label", "")
                    r["split_next_wave_start"] = s.get("next_wave_start_date", "")
                    r["split_next_wave_end"]   = s.get("next_wave_end_date", "")
                    r["split_heat_score"]      = s.get("heat_score", 0)
                    r["split_notes"]           = s.get("notes", "")
                    r["split_watch_days"]      = s.get("watch_days", 60)
                    r["universe"]              = "split"  # tag row as split universe
                    filtered.append(r)
            results = filtered
            extra_meta["split_count"]       = len(live_tickers)
            extra_meta["split_in_db"]       = len(results)
            extra_meta["split_missing_db"]  = len(live_tickers) - len(results)
            log.info("UltraDB SPLIT: %d/%d split tickers found in DB", len(results), len(live_tickers))
        except Exception as exc:
            log.warning("UltraDB SPLIT post-filter failed: %s", exc)

    try:
        _enrich_seq_patterns(results, lookback_n=age_lookback)
    except Exception as exc:
        log.warning("_enrich_seq_patterns failed: %s", exc)

    try:
        _enrich_buy_flags(results)
    except Exception as exc:
        log.warning("_enrich_buy_flags failed: %s", exc)

    duration = time.time() - started
    return {
        "results":        results,
        "running":        False,
        "stage":          "done",
        "progress_pct":   100,
        "elapsed_seconds": round(duration, 2),
        "data_source":    "studio_db",
        "universes":      out_universes,
        "row_count":      len(results),
        "min_price":      min_price,
        "min_volume":     min_volume,
        "scanned_at":     pd.Timestamp.utcnow().isoformat(),
        **extra_meta,
    }
