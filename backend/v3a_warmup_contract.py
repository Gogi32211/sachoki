"""v3a_warmup_contract.py — the two-layer maturity contract for V3-A. 2026-09-12.

WHY THIS EXISTS. The first V3-A run produced Δ+3.43 for `sig_l2` and Δ+3.01 for `sig_t`, the two
highest-ranked primitives in the whole screen. Both collapse to ≈ +0.2 once the first 20 bars of
each ticker's series are excluded: 71.3 % and 61.5 % of their events sat there, on 3.43 % of rows.
Two independent mechanisms produced that, and this module addresses the first.

    (1) THE TARGET WAS CONTAMINATED. Y8 = MFE20 / ATR14[t]. ATR14 is a Wilder RMA seeded on the
        first bar, so at the start of a series it is roughly 20 % too small and the ratio is
        correspondingly too large: Y8 runs 13.13 % at bar 0-3 against 8.47 % deep in the series.
    (2) THE EVENT COLLAPSE DEGENERATES FOR DENSE STATES. `sig_t` is true on 47.1 % of bars; under
        a 10-bar cooldown that yields 1.64 events per ticker over ~650 bars, i.e. essentially
        "the first bar of the series". Mechanism (2) is NOT fixed here — it is reported as a
        diagnostic (`raw_rate`, `events_per_ticker`) and left for an explicit decision.

TWO LAYERS, as instructed.

  A. GLOBAL TARGET MATURITY — `bar_index >= GLOBAL_TARGET_WARMUP` for every observation, whatever
     the primitive. Mandatory because the TARGET itself is the contaminated object.

  B. FEATURE-SPECIFIC MATURITY — each primitive additionally requires `bar_index >=
     min_history(source)`, derived from the PRODUCER'S DEFINITION and never from Y8.

  Final eligibility = target_maturity AND feature_maturity AND source_available AND the existing
  label/split masks. For a conjunction it is the intersection of both constituents' masks.

──────────────────────────────────────────────────────────────────────────────────────────────
HOW min_history IS DERIVED — precedence, highest first:

  1. THE PRODUCER DECLARES A CONTRACT. `rolling(n, min_periods=n)`, `ewm(..., min_periods=n)`, or
     an explicit refusal such as wyckoff_trig_engine's `if n < _LOOKBACK + 2*_SW_WIN + 2: return`.
     A declared contract wins, including for recursive filters.
  2. NON-RECURSIVE WINDOW OF LENGTH n  ->  n bars. This is the codebase's own convention wherever
     it bothers to declare one (bar_physics `rolling(n, min_periods=n)`, para_engine, shape_ctx).
     NOTE that most engines write `min_periods=1`, which does not make the window shorter — it
     makes it SILENTLY WRONG. compute_wlnbb is the clearest case: its NaNs are substituted with
     0.0 in signal_extraction, so for the first 19 bars every volume-Bollinger comparison is
     `v < 0.0` = False and the volume bucket collapses to one category for the whole warm-up.
     That is the actual mechanism behind the L_VSA artefact, and the declared window is 20.
  3. RECURSIVE FILTER WITH NO DECLARED CONTRACT  ->  seed weight below e**-6 = 0.25 %.
     DOCUMENTED CHOICE, made before any result of the rerun was seen, per instruction. It is the
     only free parameter here, it is strict rather than permissive (the purpose of the rerun is
     to remove an artefact, so the error should cost us primitives, not buy us findings), and it
     is one edit away from 1 or 2 time-constants if that is preferred.
         EMA(span N)   alpha = 2/(N+1)   ->  n = 6 / -ln(1-alpha)  ~  3N
         Wilder RMA(L) alpha = 1/L       ->  n = 6 / -ln(1-1/L)    ~  6L
  4. STATEFUL TRACKER WITH NO WINDOW AND NO CONTRACT (cisd_engine, rtb_engine) -> the global
     floor, explicitly, so that the absence of a contract is visible in the audit table rather
     than hidden behind a number that looks derived.

⚠ ONE KNOWN INCOHERENCE, DELIBERATELY LEFT VISIBLE. GLOBAL_TARGET_WARMUP is frozen at 20 by
instruction. The target's own ATR14 is a Wilder RMA(14); rule 3 applied to it gives 81 bars
(`SEED_MATURITY_ATR14` below). So ATR14 is required to be 81 bars mature when it feeds a FEATURE
and 20 when it feeds the TARGET. Raising the global floor to 81 is a one-line change and is
strictly stronger; it is not made unilaterally because the value was frozen.
"""
from __future__ import annotations

import math

# ── layer A ────────────────────────────────────────────────────────────────
GLOBAL_TARGET_WARMUP = 20          # FROZEN BY INSTRUCTION. See the incoherence note above.

# ── the one documented free parameter ──────────────────────────────────────
SEED_DECADES = 6.0                 # seed weight <= e**-6 = 0.25 %


def ema_hist(span: int) -> int:
    """Bars for an ewm(span=...) seed to decay below e**-SEED_DECADES."""
    alpha = 2.0 / (span + 1.0)
    return int(math.ceil(SEED_DECADES / -math.log1p(-alpha)))


def rma_hist(length: int) -> int:
    """Wilder RMA / ta.rma / ta.atr — alpha = 1/length."""
    alpha = 1.0 / length
    return int(math.ceil(SEED_DECADES / -math.log1p(-alpha)))


EMA9, EMA20, EMA50, EMA89, EMA200 = (ema_hist(n) for n in (9, 20, 50, 89, 200))
SEED_MATURITY_ATR14 = rma_hist(14)          # 81 — see the incoherence note
RSI14 = rma_hist(14)

# ── layer B: source column -> (min_history, why) ───────────────────────────
# Every entry names the producer and the constant it came from, so the number can be checked
# against the code rather than trusted.
EXACT: dict[str, tuple[int, str]] = {}
PREFIX: list[tuple[str, int, str]] = []


def _x(cols: str, n: int, why: str) -> None:
    for c in cols.split():
        assert c not in EXACT, f"duplicate contract entry for {c}"
        EXACT[c] = (n, why)


def _p(prefixes: str, n: int, why: str) -> None:
    for p in prefixes.split():
        PREFIX.append((p, n, why))


# ── FAMILY 18, computed in v3a_frame_build._price_derived — exact by construction ──
_x("range_pct body_pct upper_wick_pct lower_wick_pct dollar_volume", 1, "single-bar arithmetic")
_x("gap_pct pct_change_1d", 2, "needs the previous bar")
_x("pct_change_3d", 4, "close.shift(3)")
_x("pct_change_5d", 6, "close.shift(5)")
_x("pct_change_10d", 11, "close.shift(10)")
_x("pct_from_20d_high pct_from_20d_low close_pos_20d hi20_age", 21,
   "rolling(20).max/min on high/low shifted by 1")
_x("pct_from_60d_high", 61, "rolling(60).max on high shifted by 1")
_x("vol_ratio_20d", 20, "avg_vol_20d — 20-bar mean from the DB")
_x("atr_pct", SEED_MATURITY_ATR14, f"ATR14 Wilder RMA seed -> {SEED_MATURITY_ATR14}")
_x("atr_pct_chg_20d", SEED_MATURITY_ATR14 + 20, "ATR14 RMA then a 20-bar ratio")
_x("dist_ema20_atr", max(EMA20, SEED_MATURITY_ATR14), f"ewm(span=20) -> {EMA20}, over ATR14")
_x("dist_ema50_atr", max(EMA50, SEED_MATURITY_ATR14), f"ewm(span=50) -> {EMA50}, over ATR14")
_x("dist_ema200_atr", max(EMA200, SEED_MATURITY_ATR14), f"ewm(span=200) -> {EMA200}, over ATR14")
_x("consec_up", 2, "run length of close > close.shift(1)")

# ── TZ — analyzers/tz_wlnbb/signal_logic.py, T/Z raws ──────────────────────
# READ THE CODE: T1..T12 and Z1..Z12 are pure two-bar candle comparisons of o/h/l/c against the
# previous bar. No EMA, no rolling window, no ATR enters a T or Z raw. The EMAs passed into
# compute_tz_wlnbb_for_bar feed the P/D (EMA-cross) block only.
_x("sig_t sig_z t_sig z_sig tz_bull", 2, "two-bar candle pattern (signal_logic T/Z raws)")
_p("sig_t sig_z", 2, "two-bar candle pattern (signal_logic T/Z raws)")
_x("sig_tz2 sig_tz3 sig_tz_flip", 3, "T/Z compared across two prior bars")
_x("sig_bias_up sig_bias_dn", max(20, SEED_MATURITY_ATR14),
   "combo_engine bias = cons_atr & score; cons_atr is ATR14-based over a 20-bar window")
_x("sig_wk_up sig_wk_dn", max(20, SEED_MATURITY_ATR14), "combo_engine wick block, same inputs")

# ── L_VSA — volume buckets from compute_wlnbb ──────────────────────────────
_L = (20 + 1, "WLNBB_MA_PERIOD=20 volume Bollinger + previous bar; below 20 the bands are NaN and "
              "signal_extraction substitutes 0.0, which forces one bucket for the whole warm-up")
_x("l22 l34 l43 l_sig sig_blue sig_pp sig_fri34 sig_fri43 sig_fri64", *_L)
_p("sig_l", *_L)

# ── BAR_TAXONOMY ───────────────────────────────────────────────────────────
_x("close_suffix wick_suffix penetration_suffix full_suffix", 2, "previous-bar suffix logic")
_x("bar_body_wick bar_gap_range bar_range_class bar_gap_class", SEED_MATURITY_ATR14,
   "compute_bar_shape_fields divides range and gap by ATR14")
_x("bar_line5", 50, "compute_line5 wvf_range_len=50 (also wvf_lookback=22, sdev_len=20)")
_x("composite_full_suffix composite_vol", 21, "composite = T/Z + L + suffix; inherits L_VSA's 20")

# ── EMA_CROSS — signal_logic P/D block, per-signal EMA ─────────────────────
_x("price_gt_20", EMA20, "close vs ewm(span=20)")
_x("price_gt_50", EMA50, "close vs ewm(span=50)")
_x("price_gt_89", EMA89, "close vs ewm(span=89)")
_x("price_gt_200", EMA200, "close vs ewm(span=200)")
_x("sig_p2 sig_d2", EMA20, "raw_p2/d2 = cross of ema9 and ema20")
_x("sig_p3 sig_d3 sig_p50 sig_d50", EMA50, "raw_p3/p50 involve ema50")
_x("sig_p55 sig_d55 sig_p89 sig_d89", EMA89, "raw_p55 / P89 involve ema89")
_x("sig_p66 sig_d66", EMA200, "raw_p66/d66 require cross_ema200")
_x("sig_d_dn_green sig_d_up_red sig_dd_dn_green sig_dd_up_red", EMA200,
   "predn_signal priority chain spans D66, i.e. ema200")

# ── OSCILLATOR ─────────────────────────────────────────────────────────────
_x("rsi_14 rsi_ge_70 rsi_le_35", RSI14, f"RSI14 Wilder RMA seed -> {RSI14}")
_x("cci_20", 20, "20-bar CCI")
_x("rsi2_state", rma_hist(2), "RSI2 = ewm(alpha=0.5)")
_x("psar_bull", GLOBAL_TARGET_WARMUP,
   "PSAR is a stateful recursive tracker with no declared warm-up; global floor, not a derived number")
_x("wvf_spike vix_range", 50, "compute_line5 wvf_range_len=50")

# ── PHYS — studio/bar_physics.py constants ─────────────────────────────────
# CORRECTION made during the rerun, before Phase 3 was read: bar_physics normalises its axes by
# `atr_safe = df["atr_14"]` (lines 155/167/191/214/239 — R, C, M, K and gap_true all divide by it),
# so every physics column inherits the ATR14 RMA requirement. The first draft gave phys_k 60 from
# K_EMA_LEN alone, which also broke the identity phys_k_x == dist_ema20_atr: they are the same
# formula, (close - EMA20) / ATR14, and must therefore carry the same history requirement.
_A = SEED_MATURITY_ATR14
_x("phys_r phys_r_raw", max(30, _A), "R_VOL_LEN=20, R_MED_LEN=30, ATR14-normalised")
_x("phys_c phys_c_raw", max(50, _A), "C_MED_LEN=50, C_PIV_LR=3, C_MAX_LVLS=12, ATR14-normalised")
_x("phys_h phys_h_raw", max(20, _A), "H_ENT_LEN=20, inside an ATR14-normalised module")
_x("phys_m phys_m_raw", max(100, _A), "M_VOL_SLOW=100, M_MED_LEN=50, M_SLOPE_BARS=10 over ATR14")
_x("phys_e phys_e_raw phys_e_release", max(50, _A), "E_AVG_LEN=50, E_MED_LEN=50, E_WIN=15, ATR14")
_x("phys_k phys_k_x", max(EMA20, _A), "k_x = (close - ema(20)) / atr_14 — identical to dist_ema20_atr")
_x("phys_ad", max(12, _A), "AD_LOOKBACK=12, AD_CLUSTER_WIN=8, inside an ATR14-normalised module")
_x("phys_gap_true", SEED_MATURITY_ATR14, "gap measured in ATR14")
_x("phys_s phys_s_net phys_line6 phys_wyc", 100, "composite of every physics axis; module max M_VOL_SLOW=100")

# ── WYCKOFF ────────────────────────────────────────────────────────────────
_x("w2_accum w2_ar w2_break w2_evr w2_jac w2_sc w2_sos w2_spring w2_st w2_state w2_tr_quality",
   max(160, EMA50),
   "wyckoff_v2_engine: declared refusal n < _Q_LOOKBACK(90)+_PIVOT_LEN(6)+2, _CYCLE_MAX=160, _EMA_SLOW=50")
_x("wt_evr wt_lps wt_quality wt_resistance wt_sos wt_spring wt_support", 98,
   "wyckoff_trig_engine declared refusal: n < _LOOKBACK(90) + 2*_SW_WIN(3) + 2")
_x("wyc_in_tr wyc_phase", EMA200, "signal_extraction.compute_wyc_phase uses ema50 and ema200")

# ── CISD / RTB — stateful, no declared contract ────────────────────────────
_p("sig_cisd", GLOBAL_TARGET_WARMUP,
   "cisd_engine is a stateful structure tracker with no window and no declared warm-up; global floor")
_x("rtb_phase", GLOBAL_TARGET_WARMUP,
   "rtb_engine is a phase state machine with no window and no declared warm-up; global floor")

# ── PARA / FLY / GOG / B / F / LEVEL_BREAK / VOL_ABSORB ────────────────────
_p("sig_para para_", max(20, rma_hist(14)),
   "para_engine BASE_LEN/BRK_LEN/VOL_LEN=20 over _rma(ATR_LEN=14)")
_p("fly_ sig_fly sig_flp sig_fbo", EMA200, "fly_engine LOOKBACK=30/WIN_AB=30 over _ema(9..200)")
_GOG = (max(24, RSI14), "gog_engine _SEQ_LOOKBACK=24, _SUPPORT_LOOKBACK=18, rolling(20), _RSI_LENGTH=14 RMA")
_p("sig_g gog g1 g2 g3 setup_tokens context_tokens sig_b sig_f f8", *_GOG)
_p("bo_ bx_ be_ vbo_ sig_vbo", *_GOG)
_VAB = (max(20, rma_hist(14)),
        "vabs_engine _MA_PERIOD=20, _VOL_LEN=20, _BREAK_BARS=10 over ATR14 RMA")
_p("sig_va sig_svs svs sig_abs sig_clm sig_sc sig_bc sig_ns sig_nd sig_vol_ sq load cons rtv vol_bucket",
   *_VAB)

# ── DISPLAY layers ─────────────────────────────────────────────────────────
_x("OVD_TOKENS", 20, "ovd_map_build RVOL_LEN=20, rolling(20).median with default min_periods")
_x("SHAPE_CODE SHAPE_GRADE SHAPE_LSTUP_VETO SHAPE_TOUCHES", 200,
   "shape_ctx_build declares ewm(span=RS_LEN=200, min_periods=RS_LEN) — a declared contract, so it wins")
_p("LBAL LVX VOL7", GLOBAL_TARGET_WARMUP,
   "lbal_build declares no window for these marks; global floor, flagged rather than invented")


# ── columns that carry a family but generated no primitive in the first run; covered so the
# contract is complete over the frame rather than over whatever happened to survive a filter ──
_x("ne_suffix", 2, "E if close exceeds the previous bar's high or low")
_x("price_lt_20", EMA20, "close vs ewm(span=20)")
_x("price_lt_50", EMA50, "close vs ewm(span=50)")
_x("price_lt_89", EMA89, "close vs ewm(span=89)")
_x("price_lt_200", EMA200, "close vs ewm(span=200)")
_x("psar_bear", GLOBAL_TARGET_WARMUP,
   "PSAR is a stateful recursive tracker with no declared warm-up; global floor, not a derived number")
_x("phys_regime", 100, "composite of every physics axis; module max M_VOL_SLOW=100")
_x("w2_lps", max(160, EMA50), "wyckoff_v2_engine: _CYCLE_MAX=160, _EMA_SLOW=50")
_x("wt_valid_tr", 98, "wyckoff_trig_engine declared refusal: n < _LOOKBACK(90) + 2*_SW_WIN(3) + 2")
_x("wyc_sos wyc_spring", EMA200, "signal_extraction.compute_wyc_phase uses ema50 and ema200")

# ── the two frozen baseline comparators (v3a_discovery.baselines) ──────────
_x("__BL3_rsi14__", RSI14, f"BL3 = RSI14 > 50; RSI14 Wilder RMA seed -> {RSI14}")
_x("__BL4_brk20vol__", 21, "BL4 = close > rolling(20).max(high).shift(1) AND volume > 1.5*avg_vol_20d")


def min_history(source: str | None) -> tuple[int, str]:
    """(bars of own-series history required, why). Raises if a column has no explicit rule —
    nothing may fall through to a silent default."""
    if source is None:
        return GLOBAL_TARGET_WARMUP, "no source column"
    if source in EXACT:
        return EXACT[source]
    best = None
    for p, n, why in PREFIX:
        if source.startswith(p) and (best is None or len(p) > best[0]):
            best = (len(p), n, why)
    if best is not None:
        return best[1], best[2]
    raise KeyError(f"no warm-up contract for source column {source!r} — add it explicitly")


def required(source: str | None) -> int:
    """Layer A and layer B together."""
    return max(GLOBAL_TARGET_WARMUP, min_history(source)[0])


if __name__ == "__main__":
    print(f"GLOBAL_TARGET_WARMUP {GLOBAL_TARGET_WARMUP} · seed decades {SEED_DECADES}")
    print(f"  EMA9 {EMA9} · EMA20 {EMA20} · EMA50 {EMA50} · EMA89 {EMA89} · EMA200 {EMA200}")
    print(f"  Wilder RMA14 (ATR14, RSI14) {SEED_MATURITY_ATR14}")
