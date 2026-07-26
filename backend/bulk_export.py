"""
bulk_export.py — Full Superchart-format signal export for any universe.

Usage (default output → <project>/exports/<universe>_full_signals_1d.csv):
    python bulk_export.py --universe sp500  --bars 150
    python bulk_export.py --universe nasdaq --bars 150
    python bulk_export.py --universe russell2k --bars 150 --out /custom/path.csv

Produces the SAME column format as the Superchart "Export CSV" button.
Each ticker's api_bar_signals() output is written as rows.
Progress saved to /tmp/bulk_export_progress.json every 10 tickers.
Incremental CSV checkpoint written every 50 tickers.
"""
import os, sys, json, time, argparse, traceback, csv, logging
from pathlib import Path

# ── env + path ────────────────────────────────────────────────────────────────
os.chdir(Path(__file__).parent)
sys.path.insert(0, str(Path(__file__).parent))
from dotenv import load_dotenv
load_dotenv(".env")

# Suppress noisy loggers from the main module
logging.disable(logging.WARNING)

import pandas as pd

# Import the canonical api_bar_signals function directly from main.py
# (it's a pure function with no side-effects when called standalone)
from main import api_bar_signals
from scanner import get_universe_tickers

logging.disable(logging.NOTSET)  # re-enable for our own messages

_PROGRESS_FILE = "/tmp/bulk_export_progress.json"
_PREUP_SET = {"P2", "P3", "P50", "P89"}

# ── Column spec — mirrors SuperchartPanel.jsx exportCsv() exactly ─────────────
HEADERS = [
    'ticker',
    'date','open','high','low','close','volume','vol_bucket','turbo_score',
    'rtb_phase','rtb_total','rtb_transition',
    'rtb_build','rtb_turn','rtb_ready','rtb_late','rtb_bonus3',
    'dbg_context_ready','dbg_t4_ctx','dbg_t6_ctx','dbg_t4t6_activation_plus',
    'dbg_launch_cluster_count','dbg_pending_phase','dbg_pending_phase_count',
    'Z','T','L','F','FLY','G','B','Combo','ULT','VOL','VABS','WICK',
    # Text summary
    'SETUP','CONTEXT','GOG_TIER','ALL_SIGNALS',
    # Primary scores
    'GOG_SCORE','SIGNAL_SCORE','SIGNAL_BUCKET','RESEARCH_SCORE','REGIME',
    # New score system
    'CLEAN_ENTRY_SCORE','SHAKEOUT_ABSORB_SCORE','ROCKET_SCORE',
    'EXTRA_BULL_SCORE','EXPERIMENTAL_SCORE',
    'HARD_BEAR_SCORE','VOLATILITY_RISK_SCORE',
    'FINAL_BULL_SCORE','FINAL_REGIME','FINAL_SCORE_BUCKET',
    # Model booleans
    'MDL_UM_GOG1','MDL_BH_GOG1','MDL_F8_GOG1','MDL_F8_BCT','MDL_F8_LRP',
    'MDL_L22_BCT','MDL_L22_LRP','MDL_BE_GOG1','MDL_BO_GOG1','MDL_Z10_GOG1',
    'MDL_LOAD_GOG1','MDL_260_GOG1','MDL_RKT_GOG1','MDL_F8_SVS','MDL_F8_CONS',
    'MDL_L22_SQB','MDL_3UP_GOG1','MDL_BLUE_GOG1','MDL_BX_GOG1','MDL_UM_LRP',
    'HAS_ELITE_MODEL','HAS_BEAR_MODEL',
    # Backward compat
    'BEARISH_RISK_SCORE',
    # Score sub-components
    'GOG_BASE_SCORE','PREMIUM_CONTEXT_SCORE','LOAD_CONTEXT_SCORE','L_RECLAIM_SCORE',
    'COMPRESSION_CONTEXT_SCORE','SQ_BCT_SCORE','BASE_SETUP_SCORE','RAW_SUPPORT_SCORE',
    'RISK_PENALTY','RESEARCH_FORWARD_SCORE',
    # Setup / GOG booleans
    'A','SM','N','MX',
    'GOG1','GOG2','GOG3','G1P','G2P','G3P','G1L','G2L','G3L','G1C','G2C','G3C',
    # Context signals
    'LD','LDS','LDC','LDP','LRC','LRP','WRC','F8C','SQB','BCT','SVS',
    # Raw signals
    'LOAD','SQ','W','F8',
    'L34','L43','L64','L22',
    'VBO_UP','BO_UP','BE_UP','BX_UP',
    'T10','T11','T12','Z10','Z11','Z12','Z4','Z6','Z9',
    'F3','F4','F6','F11','4BF','SIG_260308','L88','UM','SVS_RAW','CONS',
    'BUY_HERE','ATR_BREAKOUT','BOLL_BREAKOUT','HILO_BUY','RTV','THREE_G','ROCKET',
    # Diagnostics
    'ALREADY_EXTENDED_FLAG',
    'PCT_CHANGE_3D','PCT_CHANGE_5D','PCT_CHANGE_10D',
    'PCT_FROM_20D_HIGH','PCT_FROM_20D_LOW','DIST_20D_HIGH','VOL_RATIO_20D',
    'DOLLAR_VOLUME','GAP_PCT',
    # BETA Score
    'BETA_SCORE','BETA_RAW','BETA_SETUP','BETA_MOMENTUM','BETA_EXCESS','BETA_ZONE','BETA_AUTO_BUY',
    # Forward returns
    'FWD_1D','FWD_3D','FWD_5D','FWD_10D','MAX_HIGH_5D','MAX_HIGH_10D',
    'HIT_5PCT_5D','HIT_10PCT_5D','HIT_5PCT_10D','HIT_10PCT_10D',
    # Next event
    'BARS_TO_VBO','BARS_TO_GOG',
    'VBO_W5','VBO_W10','GOG_W5','GOG_W10',
    'RET_TO_NEXT_VBO_CLOSE','RET_TO_NEXT_VBO_HIGH',
    'RET_TO_NEXT_GOG_CLOSE','RET_TO_NEXT_GOG_HIGH',
    # All TurboScan signal booleans — VABS
    'SIG_BEST','SIG_STRONG','SIG_VBO_DN',
    'SIG_NS_VABS','SIG_ND_VABS','SIG_SC','SIG_BC','SIG_ABS','SIG_CLM',
    # UltraV2
    'SIG_BEST_UP','SIG_FBO_UP','SIG_EB_UP','SIG_3UP',
    'SIG_FBO_DN','SIG_EB_DN','SIG_4BF_DN',
    # L sub
    'SIG_FRI34','SIG_FRI43','SIG_FRI64',
    'SIG_L555','SIG_L2L4','SIG_BLUE',
    'SIG_CCI','SIG_CCI0R','SIG_CCIB',
    'SIG_BO_DN','SIG_BX_DN','SIG_BE_DN',
    'SIG_RL','SIG_RH','SIG_PP',
    # G individual
    'SIG_G1','SIG_G2','SIG_G4','SIG_G6','SIG_G11',
    # B individual
    'SIG_B1','SIG_B2','SIG_B3','SIG_B4','SIG_B5','SIG_B6',
    'SIG_B7','SIG_B8','SIG_B9','SIG_B10','SIG_B11',
    # F individual
    'SIG_F1','SIG_F2','SIG_F3','SIG_F4','SIG_F5','SIG_F6',
    'SIG_F7','SIG_F8','SIG_F9','SIG_F10','SIG_F11',
    # FLY sub
    'SIG_FLY_ABCD','SIG_FLY_CD','SIG_FLY_BD','SIG_FLY_AD',
    # Wick sub
    'SIG_WK_UP','SIG_WK_DN','SIG_X1','SIG_X2','SIG_X1G','SIG_X3',
    # Combo sub
    'SIG_BIAS_UP','SIG_BIAS_DN','SIG_SVS','SIG_CONSO',
    'SIG_P2','SIG_P3','SIG_P50','SIG_P89','SIG_BUY','SIG_3G',
    # VA + vol
    'SIG_VA','SIG_VOL_5X','SIG_VOL_10X','SIG_VOL_20X',
    # TZ / state
    'SIG_TZ','SIG_T','SIG_Z',
    'SIG_TZ3','SIG_TZ2','SIG_TZ_FLIP',
    'SIG_CD','SIG_CA','SIG_CW','SIG_SEQ_BCONT',
    # NS/ND Delta
    'SIG_NS_DELTA','SIG_ND_DELTA',
    # Meta family any-flags
    'SIG_ANY_F','SIG_ANY_B','SIG_ANY_P','SIG_ANY_D',
    'SIG_L_ANY','SIG_BE_ANY','SIG_GOG_PLUS','SIG_NOT_EXT',
    # Price vs EMA
    'PRICE_GT_20','PRICE_GT_50','PRICE_GT_89','PRICE_GT_200',
    'PRICE_LT_20','PRICE_LT_50','PRICE_LT_89','PRICE_LT_200',
    # RSI filters
    'RSI_LE_35','RSI_GE_70',
    # Source / cross-engine
    'YF_SOURCE','CROSS_2PLUS','CROSS_3PLUS','CROSS_4PLUS','EARLY_E',
    # P66/P55
    'SIG_P66','SIG_P55',
    # D-family PREDN
    'SIG_D66','SIG_D55','SIG_D89','SIG_D50','SIG_D3','SIG_D2',
    # Delta extras
    'SIG_FLP_UP','SIG_ORG_UP','SIG_DD_UP_RED','SIG_D_UP_RED',
    'SIG_D_DN_GREEN','SIG_DD_DN_GREEN',
    # CISD
    'SIG_CISD_CPLUS','SIG_CISD_CPLUS_MINUS','SIG_CISD_CPLUS_MM',
    # PARA context
    'SIG_PARA_PREP','SIG_PARA_START','SIG_PARA_PLUS','SIG_PARA_RETEST',
    # 260523 / ULTRA signals (sync with ULTRA screener)
    'ad_fresh','ad_cluster',
    'wyc_phase','wyc_spring','wyc_sos','wyc_in_tr','wyc_sow',
    'prebreak_score','prebreak_prime','prebreak_ready','prebreak_watch',
    'pb_lvbo','pb_wvf_confirm','pb_stop_cause','pb_macro_penalty',
    'swing_type',
    # Chart-format L code (added 260525) — exact label as shown on chart tooltip,
    # e.g. "L1", "L34", "L43", "BO↑", "FRI34", or "" if no L signal.
    # Generated by wlnbb_engine.l_signal_label() priority list.
    'L_CHART',
]


def _join(lst):
    if not lst:
        return ""
    if isinstance(lst, list):
        return " ".join(str(x) for x in lst)
    return str(lst)


def _ctx(b, tok):
    return 1 if tok in (b.get("context") or []) else 0


def _s(b, k):
    return b.get(k) or 0


def bar_to_row(ticker: str, b: dict) -> list:
    """Convert one bar dict from api_bar_signals() to a CSV row matching HEADERS."""
    tz = b.get("tz", "")
    return [
        ticker,
        b.get("date", ""),
        round(b.get("open", 0), 2),
        round(b.get("high", 0), 2),
        round(b.get("low", 0), 2),
        round(b.get("close", 0), 2),
        b.get("volume", 0),
        b.get("vol_bucket", ""),
        b.get("turbo_score", 0),
        b.get("rtb_phase", ""),
        b.get("rtb_total", 0),
        b.get("rtb_transition", ""),
        b.get("rtb_build", 0),
        b.get("rtb_turn", 0),
        b.get("rtb_ready", 0),
        b.get("rtb_late", 0),
        b.get("rtb_bonus3", 0),
        1 if b.get("dbg_context_ready") else 0,
        1 if b.get("dbg_t4_ctx") else 0,
        1 if b.get("dbg_t6_ctx") else 0,
        1 if b.get("dbg_t4t6_activation_plus") else 0,
        b.get("dbg_launch_cluster_count", 0),
        b.get("dbg_pending_phase", ""),
        b.get("dbg_pending_phase_count", 0),
        tz if tz.startswith("Z") else "",
        tz if tz.startswith("T") else "",
        _join(b.get("l")),
        _join(b.get("f")),
        _join(b.get("fly")),
        _join(b.get("g")),
        _join(b.get("b")),
        _join([s for s in (b.get("combo") or []) if s not in _PREUP_SET]),
        _join(b.get("ultra")),
        _join(b.get("vol")),
        _join(b.get("vabs")),
        _join(b.get("wick")),
        # Text summary
        _join(b.get("setup")),
        _join(b.get("context")),
        b.get("gog_tier", ""),
        b.get("all_signals", ""),
        # Primary scores
        b.get("gog_score", 0),
        b.get("signal_score", 0),
        b.get("signal_bucket", ""),
        b.get("research_score", 0),
        b.get("regime", ""),
        # New score system
        b.get("clean_entry_score") or b.get("CLEAN_ENTRY_SCORE", 0),
        b.get("shakeout_absorb_score") or b.get("SHAKEOUT_ABSORB_SCORE", 0),
        b.get("rocket_score") or b.get("ROCKET_SCORE", 0),
        b.get("extra_bull_score") or b.get("EXTRA_BULL_SCORE", 0),
        b.get("experimental_score") or b.get("EXPERIMENTAL_SCORE", 0),
        b.get("hard_bear_score") or b.get("HARD_BEAR_SCORE", 0),
        b.get("volatility_risk_score") or b.get("VOLATILITY_RISK_SCORE", 0),
        b.get("final_bull_score") or b.get("FINAL_BULL_SCORE", 0),
        b.get("final_regime") or b.get("FINAL_REGIME", ""),
        b.get("final_score_bucket") or b.get("FINAL_SCORE_BUCKET", ""),
        # Model booleans
        b.get("mdl_um_gog1", 0),
        b.get("mdl_bh_gog1", 0),
        b.get("mdl_f8_gog1", 0),
        b.get("mdl_f8_bct", 0),
        b.get("mdl_f8_lrp", 0),
        b.get("mdl_l22_bct", 0),
        b.get("mdl_l22_lrp", 0),
        b.get("mdl_be_gog1", 0),
        b.get("mdl_bo_gog1", 0),
        b.get("mdl_z10_gog1", 0),
        b.get("mdl_load_gog1", 0),
        b.get("mdl_260_gog1", 0),
        b.get("mdl_rkt_gog1", 0),
        b.get("mdl_f8_svs", 0),
        b.get("mdl_f8_cons", 0),
        b.get("mdl_l22_sqb", 0),
        b.get("mdl_3up_gog1", 0),
        b.get("mdl_blue_gog1", 0),
        b.get("mdl_bx_gog1", 0),
        b.get("mdl_um_lrp", 0),
        b.get("has_elite_model") or b.get("HAS_ELITE_MODEL", 0),
        b.get("has_bear_model", 0),
        # Backward compat
        b.get("bearish_risk_score", 0),
        # Score sub-components
        b.get("gog_base_score", 0),
        b.get("premium_context_score", 0),
        b.get("load_context_score", 0),
        b.get("l_reclaim_score", 0),
        b.get("compression_context_score", 0),
        b.get("sq_bct_score", 0),
        b.get("base_setup_score", 0),
        b.get("raw_support_score", 0),
        b.get("risk_penalty", 0),
        b.get("research_forward_score", 0),
        # Setup / GOG booleans
        1 if "A"  in (b.get("setup") or []) else 0,
        1 if "SM" in (b.get("setup") or []) else 0,
        1 if "N"  in (b.get("setup") or []) else 0,
        1 if "MX" in (b.get("setup") or []) else 0,
        b.get("gog1", 0),
        b.get("gog2", 0),
        b.get("gog3", 0),
        b.get("g1p") or b.get("G1P", 0),
        b.get("g2p") or b.get("G2P", 0),
        b.get("g3p") or b.get("G3P", 0),
        b.get("g1l") or b.get("G1L", 0),
        b.get("g2l") or b.get("G2L", 0),
        b.get("g3l") or b.get("G3L", 0),
        b.get("g1c") or b.get("G1C", 0),
        b.get("g2c") or b.get("G2C", 0),
        b.get("g3c") or b.get("G3C", 0),
        # Context signals
        _ctx(b, "LD"), _ctx(b, "LDS"), _ctx(b, "LDC"), _ctx(b, "LDP"),
        _ctx(b, "LRC"), _ctx(b, "LRP"), _ctx(b, "WRC"), _ctx(b, "F8C"),
        _ctx(b, "SQB"), _ctx(b, "BCT"), _ctx(b, "SVS"),
        # Raw signals
        b.get("raw_load", 0), b.get("raw_sq", 0), b.get("raw_w", 0), b.get("raw_f8", 0),
        b.get("raw_l34", 0), b.get("raw_l43", 0), b.get("raw_l64", 0), b.get("raw_l22", 0),
        b.get("raw_vbo_up", 0), b.get("raw_bo_up", 0), b.get("raw_be_up", 0), b.get("raw_bx_up", 0),
        b.get("raw_t10", 0), b.get("raw_t11", 0), b.get("raw_t12", 0),
        b.get("raw_z10", 0), b.get("raw_z11", 0), b.get("raw_z12", 0),
        b.get("raw_z4", 0), b.get("raw_z6", 0), b.get("raw_z9", 0),
        b.get("raw_f3", 0), b.get("raw_f4", 0), b.get("raw_f6", 0), b.get("raw_f11", 0),
        b.get("raw_bf4", 0), b.get("raw_sig260308", 0), b.get("raw_l88", 0), b.get("raw_um", 0),
        b.get("raw_svs_raw", 0), b.get("raw_cons", 0),
        b.get("raw_buy_here", 0), b.get("raw_atr_brk", 0), b.get("raw_bb_brk", 0),
        b.get("raw_hilo_buy", 0), b.get("raw_rtv", 0), b.get("raw_three_g", 0), b.get("raw_rocket", 0),
        # Diagnostics
        b.get("already_extended", 0),
        b.get("pct_change_3d", ""), b.get("pct_change_5d", ""), b.get("pct_change_10d", ""),
        b.get("pct_from_20d_high", ""), b.get("pct_from_20d_low", ""),
        b.get("distance_to_20d_high_pct", ""), b.get("volume_ratio_20d", ""),
        b.get("dollar_volume", ""), b.get("gap_pct", ""),
        # BETA Score
        b.get("beta_score", ""), b.get("beta_raw", ""), b.get("beta_setup", ""),
        b.get("beta_momentum", ""), b.get("beta_excess", ""),
        b.get("beta_zone", ""), 1 if b.get("beta_auto_buy") else 0,
        # Forward returns
        b.get("fwd_close_1d", ""), b.get("fwd_close_3d", ""),
        b.get("fwd_close_5d", ""), b.get("fwd_close_10d", ""),
        b.get("max_high_5d_pct", ""), b.get("max_high_10d_pct", ""),
        b.get("hit_5pct_5d", 0), b.get("hit_10pct_5d", 0),
        b.get("hit_5pct_10d", 0), b.get("hit_10pct_10d", 0),
        # Next event
        b.get("bars_to_next_vbo", ""), b.get("bars_to_next_gog", ""),
        b.get("vbo_within_5", 0), b.get("vbo_within_10", 0),
        b.get("gog_within_5", 0), b.get("gog_within_10", 0),
        b.get("ret_to_next_vbo_close", ""), b.get("ret_to_next_vbo_high", ""),
        b.get("ret_to_next_gog_close", ""), b.get("ret_to_next_gog_high", ""),
        # All TurboScan signal booleans
        _s(b, "sig_best"), _s(b, "sig_strong"), _s(b, "sig_vbo_dn"),
        _s(b, "sig_ns_vabs"), _s(b, "sig_nd_vabs"), _s(b, "sig_sc"), _s(b, "sig_bc"),
        _s(b, "sig_abs"), _s(b, "sig_clm"),
        _s(b, "sig_best_up"), _s(b, "sig_fbo_up"), _s(b, "sig_eb_up"), _s(b, "sig_3up"),
        _s(b, "sig_fbo_dn"), _s(b, "sig_eb_dn"), _s(b, "sig_4bf_dn"),
        _s(b, "sig_fri34"), _s(b, "sig_fri43"), _s(b, "sig_fri64"),
        _s(b, "sig_l555"), _s(b, "sig_l2l4"), _s(b, "sig_blue"),
        _s(b, "sig_cci"), _s(b, "sig_cci0r"), _s(b, "sig_ccib"),
        _s(b, "sig_bo_dn"), _s(b, "sig_bx_dn"), _s(b, "sig_be_dn"),
        _s(b, "sig_rl"), _s(b, "sig_rh"), _s(b, "sig_pp"),
        _s(b, "sig_g1"), _s(b, "sig_g2"), _s(b, "sig_g4"), _s(b, "sig_g6"), _s(b, "sig_g11"),
        _s(b, "sig_b1"), _s(b, "sig_b2"), _s(b, "sig_b3"), _s(b, "sig_b4"),
        _s(b, "sig_b5"), _s(b, "sig_b6"), _s(b, "sig_b7"), _s(b, "sig_b8"),
        _s(b, "sig_b9"), _s(b, "sig_b10"), _s(b, "sig_b11"),
        _s(b, "sig_f1"), _s(b, "sig_f2"), _s(b, "sig_f3"), _s(b, "sig_f4"),
        _s(b, "sig_f5"), _s(b, "sig_f6"), _s(b, "sig_f7"), _s(b, "sig_f8"),
        _s(b, "sig_f9"), _s(b, "sig_f10"), _s(b, "sig_f11"),
        _s(b, "sig_fly_abcd"), _s(b, "sig_fly_cd"), _s(b, "sig_fly_bd"), _s(b, "sig_fly_ad"),
        _s(b, "sig_wk_up"), _s(b, "sig_wk_dn"), _s(b, "sig_x1"), _s(b, "sig_x2"),
        _s(b, "sig_x1g"), _s(b, "sig_x3"),
        _s(b, "sig_bias_up"), _s(b, "sig_bias_dn"), _s(b, "sig_svs"), _s(b, "sig_conso"),
        _s(b, "sig_p2"), _s(b, "sig_p3"), _s(b, "sig_p50"), _s(b, "sig_p89"),
        _s(b, "sig_buy"), _s(b, "sig_3g"),
        _s(b, "sig_va"), _s(b, "sig_vol_5x"), _s(b, "sig_vol_10x"), _s(b, "sig_vol_20x"),
        _s(b, "sig_tz"), _s(b, "sig_t"), _s(b, "sig_z"),
        _s(b, "sig_tz3"), _s(b, "sig_tz2"), _s(b, "sig_tz_flip"),
        _s(b, "sig_cd"), _s(b, "sig_ca"), _s(b, "sig_cw"), _s(b, "sig_seq_bcont"),
        # NS/ND Delta
        _s(b, "sig_ns_delta"), _s(b, "sig_nd_delta"),
        # Meta flags
        _s(b, "sig_any_f"), _s(b, "sig_any_b"), _s(b, "sig_any_p"), _s(b, "sig_any_d"),
        _s(b, "sig_l_any"), _s(b, "sig_be_any"), _s(b, "sig_gog_plus"), _s(b, "sig_not_ext"),
        # Price vs EMA
        _s(b, "sig_price_gt_20"), _s(b, "sig_price_gt_50"),
        _s(b, "sig_price_gt_89"), _s(b, "sig_price_gt_200"),
        _s(b, "sig_price_lt_20"), _s(b, "sig_price_lt_50"),
        _s(b, "sig_price_lt_89"), _s(b, "sig_price_lt_200"),
        # RSI
        _s(b, "sig_rsi_le_35"), _s(b, "sig_rsi_ge_70"),
        # Source / cross
        _s(b, "sig_yf_source"),
        _s(b, "sig_cross_2plus"), _s(b, "sig_cross_3plus"),
        _s(b, "sig_cross_4plus"), _s(b, "sig_early_e"),
        # P66/P55
        _s(b, "sig_p66"), _s(b, "sig_p55"),
        # D-family
        _s(b, "sig_d66"), _s(b, "sig_d55"), _s(b, "sig_d89"),
        _s(b, "sig_d50"), _s(b, "sig_d3"),  _s(b, "sig_d2"),
        # Delta extras
        _s(b, "sig_flp_up"),    _s(b, "sig_org_up"),
        _s(b, "sig_dd_up_red"), _s(b, "sig_d_up_red"),
        _s(b, "sig_d_dn_green"), _s(b, "sig_dd_dn_green"),
        # CISD
        _s(b, "sig_cisd_cplus"), _s(b, "sig_cisd_cplus_minus"), _s(b, "sig_cisd_cplus_mm"),
        # PARA context
        _s(b, "sig_para_prep"), _s(b, "sig_para_start"),
        _s(b, "sig_para_plus"), _s(b, "sig_para_retest"),
        # 260523 / ULTRA signals
        1 if b.get("ad_fresh") else 0,
        1 if b.get("ad_cluster") else 0,
        b.get("wyc_phase", ""),
        1 if b.get("wyc_spring") else 0,
        1 if b.get("wyc_sos") else 0,
        1 if b.get("wyc_in_tr") else 0,
        1 if b.get("wyc_sow") else 0,
        b.get("prebreak_score", 0),
        1 if b.get("prebreak_prime") else 0,
        1 if b.get("prebreak_ready") else 0,
        1 if b.get("prebreak_watch") else 0,
        1 if b.get("pb_lvbo") else 0,
        1 if b.get("pb_wvf_confirm") else 0,
        1 if b.get("pb_stop_cause") else 0,
        1 if b.get("pb_macro_penalty") else 0,
        b.get("swing_type", ""),
        b.get("l_chart", ""),     # chart-format L code from l_signal_label()
    ]


def _save_progress(done, total, ticker, errors, out_path, started_at):
    pct = round(done / max(total, 1) * 100, 1)
    now = time.time()
    elapsed = now - started_at
    eta = round(elapsed / max(done, 1) * (total - done)) if done > 0 else 0
    d = {
        "done": done, "total": total, "pct": pct,
        "current_ticker": ticker, "errors": len(errors),
        "error_list": errors[-5:],
        "out_path": str(out_path),
        "started_at": started_at,
        "eta_seconds": eta,
        "elapsed_seconds": round(elapsed),
    }
    with open(_PROGRESS_FILE, "w") as f:
        json.dump(d, f)


def main():
    parser = argparse.ArgumentParser(description="Bulk Superchart CSV export")
    parser.add_argument("--universe", default="sp500",
                        help="sp500 | nasdaq | russell2k | naka | etc.")
    parser.add_argument("--bars",  type=int, default=150,
                        help="Number of bars per ticker (default 150)")
    parser.add_argument("--out",   default=None,
                        help="Output CSV path (default <project>/exports/<universe>_full_signals_1d.csv)")
    parser.add_argument("--tf",    default="1d",
                        help="Timeframe (default 1d)")
    args = parser.parse_args()

    universe = args.universe
    bars     = args.bars
    tf       = args.tf
    if args.out:
        out_path = Path(args.out).expanduser()
    else:
        from studio.paths import export_path
        out_path = Path(export_path(f"{universe}_full_signals_1d.csv"))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Getting ticker list for universe={universe}...")
    tickers = get_universe_tickers(universe)
    total   = len(tickers)
    print(f"Found {total} tickers → {out_path}")
    print(f"Column count: {len(HEADERS)}")

    errors    = []
    t0        = time.time()
    row_count = 0

    _save_progress(0, total, "", errors, out_path, t0)

    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADERS)

        for idx, ticker in enumerate(tickers, 1):
            try:
                # Fetch ≥150 bars for engine warm-up; trim to requested window
                effective_bars = max(bars, 150)
                bars_data = api_bar_signals(ticker, tf, effective_bars, universe)
                if len(bars_data) > bars:
                    bars_data = bars_data[-bars:]

                for b in bars_data:
                    row = bar_to_row(ticker, b)
                    writer.writerow(row)
                    row_count += 1

            except Exception as e:
                errors.append(f"{ticker}: {e}")
                if len(errors) <= 5:
                    traceback.print_exc()

            if idx % 10 == 0 or idx == total:
                elapsed = time.time() - t0
                eta     = round(elapsed / idx * (total - idx)) if idx > 0 else 0
                pct     = round(idx / total * 100, 1)
                print(f"[{idx}/{total}] {pct}% | {elapsed:.0f}s elapsed | ETA {eta}s "
                      f"| rows {row_count:,} | errors {len(errors)}")
                _save_progress(idx, total, ticker, errors, out_path, t0)

                # Flush every 50 tickers so partial progress is visible
                if idx % 50 == 0:
                    fh.flush()
                    sz = out_path.stat().st_size / 1024**2
                    print(f"   → flushed: {sz:.1f} MB written so far")

    # Summary
    elapsed = time.time() - t0
    sz = out_path.stat().st_size / 1024**2
    parquet_path = out_path.with_suffix(".parquet")
    try:
        pd.read_csv(out_path).to_parquet(parquet_path, index=False)
        pq_sz = parquet_path.stat().st_size / 1024**2
        print(f"   Parquet: {parquet_path}  ({pq_sz:.1f} MB)")
    except Exception as e:
        print(f"   Parquet write failed: {e}")

    _save_progress(total, total, "DONE", errors, out_path, t0)
    print(f"\n✅ Done in {elapsed:.0f}s")
    print(f"   CSV:     {out_path}  ({sz:.1f} MB)")
    print(f"   Rows:    {row_count:,}  |  Cols: {len(HEADERS)}")
    print(f"   Tickers: {total - len(errors)} OK  |  {len(errors)} errors")
    if errors:
        print("   First errors:", errors[:5])


if __name__ == "__main__":
    main()
