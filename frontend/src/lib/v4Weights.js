// V4 WEIGHTS — the ONLY V4 map (2026-10-07, user: "es axali davtovot marto … yvelgan gamocvale": Ultra,
// Superchart, CSV and the stored history all read THIS file). The previous all-5 map is kept for reference at
// research_out/v4Weights.pre_dedup_2026-10-07.js. Edit weights here, one line per key, as before.
//
// Origin of this map, 2026-10-07. User: "igive signalebi ogond wonebi Seicvleba, marto im
// signalebs darCeba wonebi romlebic sxva signalebisgan ar Sedgeba an msgavsia logikit" — same catalog,
// weights kept ONLY on signals that are not built from other signals and do not repeat another one, so
// no information is counted twice.
//
// How it was derived (research_out/V4_DEDUP_PROPOSAL.csv — one row per key with the reason):
// co-firing of all 730 catalog keys on 1.5M random bars of data/v4_signals.parquet.
//   DUP      fires on the same bars as another key (Jaccard ≥ 0.97) → 0; one representative keeps the
//            weight (a base key before an x_ mirror) — so 26 former-0 keys now carry 5 (e.g. uf_edge_atm)
//   UNION    ≥ 2 narrower keys each imply it and together cover ≥ 95 % of its bars (ANY T, L34, PARA…) → 0
//   AND      implies ≥ 2 broader keys and is ≈ their conjunction (BEST↑, ⚡Vol-Bull, XBB = X + BB…) → 0
//   SIMILAR  Jaccard 0.80-0.97 with a kept key → 0
// Not decided yet (left as in v4Weights.js): the 272 independent keys that are 0 today, and whether
// UNION/AND members inherit the composite's points. Keys that never fire in the stored history keep
// their current weight (no co-firing evidence either way).
// Result: 196 weighted keys / 980 points (was 232 / 1160); mean bar score 54.7 → 42.7; corr 0.956.
// 2026-10-07 (user: "yvela dadebit signalebs 5, yvela daRmavals −5"): the independent keys that were 0 now
// carry their DIRECTION — 95 bullish +5, 59 bearish −5; 96 direction-less (volume levels, gap/range
// class, body size, physics C/H/K0/E0/E1, VOL7 grid, RSI≤35/≥70 thresholds…) stay 0. See the `direction` column.
export const V4_WEIGHTS = {
  // ── ★ composite setups (backtested edge, 8M-bar fwd-return analysis) ──
  _combo_volbull: 0,  // ⚡Vol-Bull  [was 5 — AND: = AND of 5 signals (fires on 100% of their joint bars): vol_spike_5x, bias_up, _l5_any, _g]
  _combo_structbo: 5,  // ❖Struct-BO
  _combo_absorbeb: 5,  // ❖Absorb→EB
  _combo_blowoff: 0,  // ⚡Blowoff↓
  // ── ★ combo (backtested) ──
  best_sig: 5,  // BEST★
  strong_sig: 5,  // STRONG
  vol_spike_20x: 0,  // V×20
  vol_spike_10x: 0,  // V×10
  vol_spike_5x: 0,  // V×5
  vbo_up: 5,  // VBO↑
  abs_sig: 5,  // ABS
  climb_sig: 5,  // CLB
  load_sig: 5,  // LD
  ns: 5,  // NS
  sq: 5,  // SQ
  sc: 5,  // SC
  nd: 5,  // ND
  buy_2809: 5,  // BUY  [was 0 — bull +5, 2026-10-07]
  rocket: 5,  // 🚀  [was 0 — bull +5, 2026-10-07]
  sig3g: 5,  // 3G
  rtv: 5,  // RTV  [was 0 — bull +5, 2026-10-07]
  hilo_buy: 5,  // HILO↑
  atr_brk: 5,  // ATR↑
  bb_brk: 5,  // BB↑
  va: 0,  // VA
  bias_up: 5,  // ↑BIAS  [was 0 — bull +5, 2026-10-07]
  um_2809: 0,  // UM
  svs_2809: 0,  // SVS
  conso_2809: 0,  // CON
  pt5: 0,  // PT5
  pt5_h1_any: 0,  // PT5·1H
  pt5_m15_any: 0,  // PT5·15
  pt5_strong: 5,  // PT5+
  pt5_xr_volw_va: 0,  // XR:VOL_W↔VA
  mt5_strong: 0,  // MT5+
  pt9: 0,  // PT9
  pt9_h1_any: 0,  // PT9·1H
  pt9_m15_any: 0,  // PT9·15
  pt9_strong: 5,  // PT9+
  pt3: 0,  // PT3
  pt3_h1_any: 0,  // PT3·1H
  pt1: 0,  // PT1
  pt1_h1_any: 0,  // PT1·1H
  pt_any: 0,  // PT·ALL
  pt_any_h1: 0,  // PT·ALL1H
  pt_any_strong: 5,  // PT·ALL+
  // ── ★ L-BAL (15m L-label balance vs candle · V and all counts, both shown · descriptive) ──
  lbal_star_v: 5,  // ★V
  lbal_conflict_v: 5,  // ★★V
  lbal_half_up_v: 5,  // ★★★V
  lbal_half_dn_v: 5,  // ○○○V
  lbal_nn_v: 5,  // XXXV
  lbal_star_a: 5,  // ★a
  lbal_conflict_a: 5,  // ★★a
  lbal_half_up_a: 5,  // ★★★a
  lbal_half_dn_a: 5,  // ○○○a
  lbal_nn_a: 5,  // XXXa
  // ── L-VX (daily L34/L46 · V vol>SMA20 · VL/VH one lower TF · VX both · descriptive) ──
  lvx_l34_v: 5,  // L34V
  lvx_l34_vl: 5,  // L34VL
  lvx_l34_vh: 0,  // L34VH  [was 5 — AND: = AND of 6 signals (fires on 90% of their joint bars): lvx_l34_v, lvx_l34_vl, _wl_l3, _ne_]
  lvx_l34_vx: 5,  // L34VX
  lvx_l46_v: 5,  // L46V
  lvx_l46_vl: 5,  // L46VL
  lvx_l46_vh: 0,  // L46VH  [was 5 — DUP: same bars as lvx_l46_vl (Jaccard 1.00)]
  lvx_l46_vx: 0,  // L46VX  [was 5 — DUP: same bars as lvx_l46_vl (Jaccard 0.97)]
  // ── OVD daily map (opening / closing 15m volume logics · descriptive) ──
  ovdmap_ob30: 5,  // OB·30
  ovdmap_ob60: 5,  // OB·60
  ovdmap_rc30: 5,  // RC·30  [was 0 — bull +5, 2026-10-07]
  ovdmap_rc60: 5,  // RC·60  [was 0 — bull +5, 2026-10-07]
  ovdmap_cd30: 5,  // CD·30
  ovdmap_cd60: 5,  // CD·60
  ovdmap_ho30: 5,  // HO·30
  ovdmap_ho60: 5,  // HO·60
  ovdmap_nm: 0,  // NM?
  // ── VOL7 (7-level volume regime · median-ratio vs sigma · jumps · VB2 · SHIFT · descriptive) ──
  vol7_m0: 0,  // M0
  vol7_m5: 0,  // M5
  vol7_m6: 0,  // M6
  vol7_up2: 5,  // ▲+2
  vol7_dn2: 0,  // ▼−2
  vol7_up3: 5,  // ◆+3
  vol7_dn3: 0,  // ◆−3
  vol7_sigma_plus: 0,  // Σ+  [was 5 — UNION: fires when any of 6 signals fires (covers 100%): x_vol7_m4_sg6, x_vol7_m0_sg2, x_vol7_m3_s]
  vol7_mr_plus: 0,  // MR+  [was 5 — UNION: fires when any of 4 signals fires (covers 100%): x_vol7_m5_sg3, x_vol7_m6_sg3, x_vol7_m6_s]
  vol7_vb2: 0,  // VB2
  vol7_shift_up: 5,  // SHIFT↑
  vol7_shift_dn: 0,  // SHIFT↓
  // ── SHAPE × CONTEXT (body-nest shapes · effort · cluster · descriptive — clustering measured WORSE) ──
  shape_mth: 0,  // MTH  [was 5 — UNION: fires when any of 2 signals fires (covers 96%): x_shape_mth_up, x_shape_mth_dn]
  shape_cl4: 0,  // CL4  [was 5 — UNION: fires when any of 2 signals fires (covers 96%): x_shape_cl4_up, x_shape_cl4_dn]
  shape_mid: 5,  // MID
  shape_exp: 0,  // EXP  [was 5 — UNION: fires when any of 2 signals fires (covers 100%): x_shape_exp_dn, x_shape_exp_up]
  shape_con: 5,  // CON
  shape_lst: 0,  // LST  [was 5 — UNION: fires when any of 2 signals fires (covers 100%): x_shape_lst_dn, shape_lstup_veto]
  shape_wrp: 0,  // WRP  [was 5 — UNION: fires when any of 2 signals fires (covers 99%): x_shape_wrp_dn, x_shape_wrp_up]
  shape_lstup_veto: -5,  // ⛔LST↑  [was 0 — bear −5, 2026-10-07]
  shape_veto: -5,  // ⛔KNF  [was 0 — bear −5, 2026-10-07]
  shape_absorb: 5,  // 💨abs
  shape_dry: 5,  // ⛔dry
  shape_sweet: 5,  // ✅swt
  shape_floor: 5,  // 📍flr
  shape_key: 5,  // 🧱key
  shape_rs: 5,  // 🏆rs
  shape_by_fam: 0,  // 🎯div
  shape_by_den: 0,  // 🔁den
  // ── PRICE × VOLUME · price = OHLC4 (Pine default) — descriptive; 0 BUILD / 4 VETO / 5 NULL ──
  pv_o_div: 5,  // DIV
  pv_o_upp: 5,  // UPP
  pv_o_upr: 5,  // UPR
  pv_o_rev: 5,  // REV
  pv_o_rup: 5,  // RUP
  pv_o_vup: 5,  // VUP  [was 0 — bull +5, 2026-10-07]
  pv_o_turn: 5,  // TURN
  pv_o_up4: 5,  // UP4
  pv_o_re2: 0,  // RE2
  pv_veto_o: 0,  // ⛔veto
  pv_agree: 0,  // =both
  // ── PRICE × VOLUME · price = CLOSE ──
  pv_c_div: 5,  // DIV
  pv_c_upp: 5,  // UPP
  pv_c_upr: 5,  // UPR
  pv_c_rev: 5,  // REV
  pv_c_rup: 5,  // RUP
  pv_c_vup: 5,  // VUP  [was 0 — bull +5, 2026-10-07]
  pv_c_turn: 5,  // TURN
  pv_c_up4: 5,  // UP4
  pv_c_re2: 0,  // RE2
  pv_veto_c: 0,  // ⛔veto
  cd: 0,  // CD
  ca: 0,  // CA
  cw: 0,  // CW
  g1: 5,  // G1  [was 0 — bull +5, 2026-10-07]
  g2: 5,  // G2  [was 0 — bull +5, 2026-10-07]
  g4: 5,  // G4  [was 0 — bull +5, 2026-10-07]
  g6: 5,  // G6  [was 0 — bull +5, 2026-10-07]
  g11: 5,  // G11  [was 0 — bull +5, 2026-10-07]
  seq_bcont: 5,  // SBC  [was 0 — bull +5, 2026-10-07]
  tz_any_t: 0,  // ANY T  [was 5 — UNION: fires when any of 105 signals fires (covers 100%): fly_bd, fly_cd, fly_ad, fly_abcd, x_ana]
  tz_t1g: 5,  // T1G  [was 0 — bull +5, 2026-10-07]
  tz_t2g: 5,  // T2G  [was 0 — bull +5, 2026-10-07]
  tz_t1: 5,  // T1  [was 0 — bull +5, 2026-10-07]
  tz_t2: 5,  // T2  [was 0 — bull +5, 2026-10-07]
  tz_t3: 5,  // T3  [was 0 — bull +5, 2026-10-07]
  tz_t4: 5,  // T4  [was 0 — bull +5, 2026-10-07]
  tz_t5: 5,  // T5  [was 0 — bull +5, 2026-10-07]
  tz_t6: 5,  // T6  [was 0 — bull +5, 2026-10-07]
  tz_t9: 5,  // T9  [was 0 — bull +5, 2026-10-07]
  tz_t10: 5,  // T10  [was 0 — bull +5, 2026-10-07]
  tz_t11: 5,  // T11  [was 0 — bull +5, 2026-10-07]
  tz_t12: 5,  // T12  [was 0 — bull +5, 2026-10-07]
  tz_bull_flip: 0,  // TZ→3
  tz_attempt: 0,  // TZ→2
  tz_weak_bull: 0,  // W
  tz_any_z: 0,  // ANY Z
  tz_z1g: -5,  // Z1G  [was 0 — bear −5, 2026-10-07]
  tz_z2g: -5,  // Z2G  [was 0 — bear −5, 2026-10-07]
  tz_z1: -5,  // Z1  [was 0 — bear −5, 2026-10-07]
  tz_z2: -5,  // Z2  [was 0 — bear −5, 2026-10-07]
  tz_z3: -5,  // Z3  [was 0 — bear −5, 2026-10-07]
  tz_z4: -5,  // Z4  [was 0 — bear −5, 2026-10-07]
  tz_z5: -5,  // Z5  [was 0 — bear −5, 2026-10-07]
  tz_z6: -5,  // Z6  [was 0 — bear −5, 2026-10-07]
  tz_z7: -5,  // Z7  [was 0 — bear −5, 2026-10-07]
  tz_z9: -5,  // Z9  [was 0 — bear −5, 2026-10-07]
  tz_z10: -5,  // Z10  [was 0 — bear −5, 2026-10-07]
  tz_z11: -5,  // Z11  [was 0 — bear −5, 2026-10-07]
  tz_z12: -5,  // Z12  [was 0 — bear −5, 2026-10-07]
  // ── L1 · L code ──
  _wl_any: 0,  // ANY L
  _wl_l1: 5,  // L1  [was 0 — bull +5, 2026-10-07]
  _wl_l2: 5,  // L2  [was 0 — bull +5, 2026-10-07]
  _wl_l3: 5,  // L3
  _wl_l4: 0,  // L4
  _wl_l5: -5,  // L5  [was 0 — bear −5, 2026-10-07]
  _wl_l6: 0,  // L6
  _wl_l34: 0,  // L34  [was 5 — UNION: fires when any of 10 signals fires (covers 100%): l34, lvx_l34_vl, lvx_l34_vh, l22, lvx_l3]
  _wl_l46: 5,  // L46
  // ── L2 suffix ──
  _ne_n: 0,  // N
  _ne_e: 0,  // E
  _wk_u: 0,  // U
  _wk_d: 0,  // D
  _wk_b: 0,  // B
  _pen_p: 0,  // P
  _pen_r: 0,  // R
  _pen_h: 0,  // H
  _cl_a: 5,  // A  [was 0 — bull +5, 2026-10-07]
  _cl_o: -5,  // O  [was 0 — bear −5, 2026-10-07]
  _cl_i: 0,  // I
  // ── L3 body·wick ──
  _bw_x: 0,  // X
  _bw_m: 0,  // M
  _bw_s: 0,  // S
  _bw_j: 0,  // J
  _bw_tb: 0,  // TB
  _bw_bb: 5,  // BB  [was 0 — inherits a duplicate's weight]
  _bw_f: 0,  // F
  _bw_xf: 0,  // XF
  _bw_mf: 0,  // MF
  // ── ⚛ physics (260815) ──
  _ph_ra: 0,  // RA
  _ph_rn: 0,  // RN
  _ph_rf: 0,  // RF
  _ph_u: 0,  // ·U
  _ph_d: 0,  // ·D
  _ph_e2: 0,  // E2  [was 5 — UNION: fires when any of 2 signals fires (covers 100%): x_phys_e2, x_phys_e2_star]
  _ph_es: 0,  // E★  [was 5 — UNION: fires when any of 2 signals fires (covers 100%): x_phys_e2_star, x_phys_e1_star]
  _ph_k2: 0,  // K2
  _ph_k1: 0,  // K1
  _ph_k0: 0,  // K0
  _ph_c0: 0,  // C0
  _ph_c1: 0,  // C1
  _ph_c2: 0,  // C2
  _ph_h0: 0,  // H0
  _ph_h2: 0,  // H2
  _ph_m2: 0,  // M2
  _ph_s3u: 5,  // S3U
  _ph_s3d: -5,  // S3D  [was 0 — bear −5, 2026-10-07]
  _ph_ad: 0,  // AD any  [was 5 — UNION: fires when any of 4 signals fires (covers 100%): _ph_ad2, _ph_ad1, _ph_ada, _ph_sos]
  _ph_ad1: 5,  // ★
  _ph_ad2: 5,  // ★★
  _ph_ada: 5,  // ★A
  _ph_gg3: 5,  // gG3
  _ph_spr: 5,  // SPRING⚛
  _ph_sprs: 5,  // SPRING★⚛
  _ph_utad: -5,  // UTAD⚛  [was 0 — bear −5, 2026-10-07]
  _ph_sos: 5,  // SOS★⚛  [was 0 — bull +5, 2026-10-07]
  _ph_acc: 5,  // ACC-TR⚛  [was 0 — bull +5, 2026-10-07]
  _ph_dist: -5,  // DIST-TR⚛  [was 0 — bear −5, 2026-10-07]
  _ph_mkup: 5,  // MARKUP⚛  [was 0 — bull +5, 2026-10-07]
  _ph_mkdn: -5,  // MKDN⚛  [was 0 — bear −5, 2026-10-07]
  // ── ⟂ CISD (260815) ──
  _ci_ps: 5,  // +S  [was 0 — bull +5, 2026-10-07]
  _ci_ms: -5,  // −S  [was 0 — bear −5, 2026-10-07]
  _ci_sq: 0,  // ⟂seq
  _ci_mp: 0,  // ⟂mpm
  // ── L4 gap·range ──
  _gr_g1: 0,  // G1
  _gr_g2: 0,  // G2
  _gr_g3: 0,  // G3
  _gr_v: 0,  // V
  _gr_c: 0,  // C
  _gr_n: 0,  // N
  // ── L5 vix·psar·rsi2 ──
  _l5_any: 0,  // L5∗
  _l5_vx: 0,  // VX
  _l5_vr: 0,  // VR
  _l5_pb: 5,  // PB  [was 0 — bull +5, 2026-10-07]
  _l5_ps: -5,  // PS  [was 0 — bear −5, 2026-10-07]
  _l5_r2h: 0,  // R2H
  _l5_r2l: 0,  // R2L
  _l5_r2x: 0,  // R2X
  _l5_r2d: 0,  // R2D
  // ── L6 vol ──
  _vb_vb: 0,  // VB
  _vb_b: 0,  // B
  _vb_n: 0,  // N
  _vb_l: 0,  // L
  _vb_w: 0,  // W
  // ── Wyckoff cycle (260529) ──
  w2_accum: 5,  // ACC  [was 0 — bull +5, 2026-10-07]
  w2_break: 5,  // BRK  [was 0 — bull +5, 2026-10-07]
  w2_sc: 0,  // SC
  w2_ar: 5,  // AR  [was 0 — bull +5, 2026-10-07]
  w2_st: 0,  // ST
  w2_spring: 5,  // SPR
  w2_sos: 5,  // SOS
  w2_jac: 5,  // JAC  [was 0 — bull +5, 2026-10-07]
  w2_lps: 5,  // LPS
  w2_evr: 0,  // EVR
  // ── Wyckoff trig (agent) ──
  wt_valid_tr: 0,  // TR
  wt_spring: 5,  // tSPR
  wt_sos: 5,  // tSOS
  wt_lps: 5,  // tLPS
  wt_evr: 0,  // tEVR
  _l_any: 0,  // L∗
  _be_any: 0,  // BE
  fri34: 0,  // FRI34
  fri43: 0,  // FRI43
  fri64: 0,  // FRI64
  l34: 5,  // L34  [was 0 — inherits a duplicate's weight]
  l43: 5,  // L43  [was 0 — bull +5, 2026-10-07]
  l22: 5,  // L22  [was 0 — inherits a duplicate's weight]
  l555: -5,  // L555  [was 0 — bear −5, 2026-10-07]
  blue: 5,  // BL
  cci_ready: 5,  // CCI  [was 0 — bull +5, 2026-10-07]
  cci_0_retest: 5,  // CCI0R  [was 0 — bull +5, 2026-10-07]
  cci_blue_turn: 5,  // CCIB  [was 0 — bull +5, 2026-10-07]
  bo_up: 5,  // BO↑
  bo_dn: -5,  // BO↓  [was 0 — bear −5, 2026-10-07]
  bx_up: 5,  // BX↑
  bx_dn: -5,  // BX↓  [was 0 — bear −5, 2026-10-07]
  be_up: 5,  // BE↑
  be_dn: -5,  // BE↓  [was 0 — bear −5, 2026-10-07]
  fuchsia_rh: 5,  // RH
  fuchsia_rl: 5,  // RL
  pre_pump: 5,  // PP
  x2g_wick: 0,  // X2G
  x2_wick: 0,  // X2
  x1g_wick: 0,  // X1G
  x1_wick: 0,  // X1
  x3_wick: 0,  // X3
  wick_bull: 5,  // WK↑  [was 0 — bull +5, 2026-10-07]
  best_long: 0,  // BEST↑  [was 5 — AND: = AND of 7 signals (fires on 100% of their joint bars): _ne_e, _wk_b, _cl_a, _l5_any, fbo_]
  fbo_bull: 5,  // FBO↑
  fbo_bear: -5,  // FBO↓  [was 0 — bear −5, 2026-10-07]
  eb_bull: 5,  // EB↑
  eb_bear: -5,  // EB↓  [was 0 — bear −5, 2026-10-07]
  bf_buy: 5,  // 4BF  [was 0 — bull +5, 2026-10-07]
  bf_sell: -5,  // 4BF↓  [was 0 — bear −5, 2026-10-07]
  ultra_3up: 5,  // 3↑  [was 0 — bull +5, 2026-10-07]
  sig_l88: 0,  // L88
  sig_260308: 0,  // 260308
  d_blast_bull: 5,  // ΔΔ↑  [was 0 — bull +5, 2026-10-07]
  d_surge_bull: 5,  // Δ↑  [was 0 — bull +5, 2026-10-07]
  d_strong_bull: 5,  // B/S↑  [was 0 — bull +5, 2026-10-07]
  d_absorb_bull: 5,  // Ab↑
  d_spring: 0,  // dSPR  [was 5 — AND: = AND of 5 signals (fires on 100% of their joint bars): _l5_any, d_absorb_bull, d_div_bull]
  d_div_bull: 5,  // T↓  [was 0 — bull +5, 2026-10-07]
  d_vd_div_bull: 5,  // NS  [was 0 — bull +5, 2026-10-07]
  d_cd_bull: 5,  // cd↑  [was 0 — bull +5, 2026-10-07]
  d_flip_bull: 5,  // FLP↑
  d_orange_bull: 0,  // ORG↑
  d_blast_bull_red: 5,  // ΔΔ↑R  [was 0 — bull +5, 2026-10-07]
  d_surge_bull_red: 5,  // Δ↑R  [was 0 — bull +5, 2026-10-07]
  d_surge_bear_grn: -5,  // Δ↓G  [was 0 — bear −5, 2026-10-07]
  d_blast_bear_grn: -5,  // ΔΔ↓G  [was 0 — bear −5, 2026-10-07]
  d_vd_div_bear: -5,  // ND  [was 0 — bear −5, 2026-10-07]
  _any_p: 0,  // ANY P
  preup66: 5,  // P66
  preup55: 5,  // P55
  preup89: 5,  // P89
  preup3: 5,  // P3
  preup2: 5,  // P2
  preup50: 5,  // P50
  _any_d: 0,  // ANY D
  predn66: -5,  // D66  [was 0 — bear −5, 2026-10-07]
  predn55: -5,  // D55  [was 0 — bear −5, 2026-10-07]
  predn89: -5,  // D89  [was 0 — bear −5, 2026-10-07]
  predn3: 0,  // D3
  predn2: -5,  // D2  [was 0 — bear −5, 2026-10-07]
  predn50: -5,  // D50  [was 0 — bear −5, 2026-10-07]
  _gt_ema200: 5,  // P>200  [was 0 — bull +5, 2026-10-07]
  _gt_ema89: 5,  // P>89  [was 0 — bull +5, 2026-10-07]
  _gt_ema50: 0,  // P>50
  _gt_ema20: 0,  // P>20
  _lt_ema20: 0,  // P<20
  _lt_ema50: -5,  // P<50  [was 0 — bear −5, 2026-10-07]
  _lt_ema89: 0,  // P<89
  _lt_ema200: -5,  // P<200  [was 0 — bear −5, 2026-10-07]
  rs_strong: 5,  // RS+
  rs: 0,  // RS
  para_prep: 5,  // PREP  [was 0 — bull +5, 2026-10-07]
  para_start: 0,  // PARA  [was 5 — UNION: fires when any of 2 signals fires (covers 99%): para_plus, uf_edge_par]
  para_plus: 5,  // PARA+
  para_retest: 5,  // RETEST
  rgti_ll: 0,  // LL
  rgti_up: 0,  // UP
  rgti_upup: 5,  // ↑↑
  rgti_upupup: 5,  // ↑↑↑
  rgti_orange: 0,  // ORG
  rgti_green: 5,  // GRN
  rgti_greencirc: 5,  // GC
  smx: 0,  // SMX
  // ── 🥇 Macro / sector (2026-08-06) ──
  lead_lag: 5,  // 🥇LEAD-in-LAG
  lead_g3: 0,  // 🥇G3  [was 5 — DUP: same bars as lead_lag (Jaccard 0.98)]
  lead_g3a: 5,  // 🥇G3A
  lead_l43: 5,  // 🥇L43
  // ── 🟡 Seq-edges (2026-08-04) ──
  seq_any: 5,  // 🟡SEQ*
  seq_crown: 5,  // 👑Z1G
  seq_z1gt4: 0,  // 🌉Z1G4  [was 5 — AND: = AND of 5 signals (fires on 100% of their joint bars): tz_t4, _l5_any, seq_any, _not_ext,]
  seq_v2: 5,  // 🌉v2
  seq_z9hl: 5,  // 🧲Z9HL
  seq_20: 5,  // 🧺SEQ  [was 0 — bull +5, 2026-10-07]
  // ── 📐 MTF-EMA (15m·1h·4h DB) ──
  mtf_k0: 0,  // 📐K0
  mtf_smx: 0,  // 📐SMX
  mtf_orange: 0,  // 📐ORG
  mtf_up: 5,  // 📐UP
  mtf_upup: 5,  // 📐↑↑
  mtf_upupup: 5,  // 📐↑↑↑
  mtf_ll: 0,  // 📐LL
  akan_sig: 0,  // A
  smx_sig: 0,  // SM
  nnn_sig: 0,  // N
  mx_sig: 0,  // MX
  gog_sig: 0,  // GOG
  gog_g1p: 5,  // G1P  [was 0 — bull +5, 2026-10-07]
  gog_g2p: 0,  // G2P
  gog_g3p: 0,  // G3P
  gog_g1l: 5,  // G1L  [was 0 — bull +5, 2026-10-07]
  gog_g2l: 5,  // G2L  [was 0 — bull +5, 2026-10-07]
  gog_g3l: 0,  // G3L
  gog_g1c: 5,  // G1C  [was 0 — bull +5, 2026-10-07]
  gog_g2c: 0,  // G2C
  gog_g3c: 0,  // G3C
  _gog_any_p: 0,  // ★GOG+
  _not_ext: 0,  // !EXT
  ctx_lds: 5,  // LDS
  ctx_ldc: 5,  // LDC
  ctx_ldp: 5,  // LDP
  ctx_lrc: 5,  // LRC
  ctx_lrp: 5,  // LRP
  ctx_wrc: 5,  // WRC
  ctx_sqb: 5,  // SQB
  ctx_bct: 5,  // BCT
  fly_abcd: 5,  // ABCD
  fly_cd: 0,  // CD  [was 5 — SIMILAR: overlaps fly_abcd (Jaccard 0.90)]
  fly_bd: 0,  // BD  [was 5 — UNION: fires when any of 7 signals fires (covers 99%): fly_cd, fly_ad, fly_abcd, x_fly_fresh_cd, ]
  fly_ad: 0,  // AD  [was 5 — SIMILAR: overlaps fly_abcd (Jaccard 0.85)]
  _rsi_os: 0,  // RSI≤35
  _rsi_ob: 0,  // RSI≥70
  _yf_only: 0,  // yf
  _cross2: 0,  // ⚡×2+
  _cross3: 5,  // ⚡×3+  [was 0 — bull +5, 2026-10-07]
  _cross4: 5,  // ⚡×4+  [was 0 — bull +5, 2026-10-07]
  _early: 5,  // [E]

  // ── EXTRA (V4_EXTRA_GROUPS, src/lib/v4ExtraGroups.js) — RANK/CONF/EDGES/SEQ/MTF/DIV,
  // fields that exist on the row but were never in SIG_GROUPS (added 2026-09-23) ──
  x_rank_fires: 0,  // RANK any
  x_conf_fires: 0,  // CONF any
  x_edges_any: 0,  // EDGES any
  // EDGE-board, per code (edge_replay.py DISPLAY_SETUPS — same on Ultra AND Superchart)
  x_edge_dualrec: 0,  // 🔄DR dual-reclaim
  x_edge_h1dr: 0,  // 🕐DR 1H-confirmed bottom
  x_edge_cap: 0,  // CAP T1-Capit-Bounce
  x_edge_qzc: 0,  // QZC QZ-Capit-Reversal
  x_edge_dl1: 0,  // D+L1 bear-trap reversal
  x_edge_g3: 0,  // G3 gap reclaim
  x_edge_g3a: 0,  // ⚡G3-Abs
  x_edge_atm: 0,  // ATM Atomic edge  [was 5 — DUP: same bars as uf_edge_atm (Jaccard 1.00)]
  x_edge_atmr: 0,  // ATMR Atomic-R  [was 5 — DUP: same bars as uf_edge_atmr (Jaccard 1.00)]
  x_edge_spr: 0,  // SPR Wyckoff Spring  [was 5 — DUP: same bars as uf_edge_spr (Jaccard 1.00)]
  x_edge_z11: 0,  // Z11 Z11-T11  [was 5 — DUP: same bars as uf_edge_z11 (Jaccard 1.00)]
  x_edge_l43: 0,  // L43 L43-TRIPLE  [was 5 — DUP: same bars as uf_edge_l43 (Jaccard 1.00)]
  x_edge_l43_quiet: 0,  // L43🔇 quiet-tape (validated tier)  [was 5 — DUP: same bars as uf_edge_l43_quiet (Jaccard 1.00)]
  x_edge_wsh: 0,  // WSH Washout/capitulation  [was 5 — DUP: same bars as uf_edge_wsh (Jaccard 1.00)]
  x_edge_h1b: 5,  // H1B 1H-bottom base
  x_edge_eng: 0,  // ENG Engulf-Absorption  [was 5 — DUP: same bars as uf_edge_eng (Jaccard 1.00)]
  x_edge_el46: 0,  // EL46 Engulf-L46 (GEM2)  [was 5 — DUP: same bars as uf_edge_el46 (Jaccard 1.00)]
  x_edge_zrt: 0,  // ZRT Zone-Retest  [was 5 — DUP: same bars as uf_edge_zrt (Jaccard 1.00)]
  x_edge_zrt_l46: 0,  // ZRT🟢 Zone-Retest, L46-quality  [was 5 — DUP: same bars as uf_edge_zrt_l46 (Jaccard 1.00)]
  x_edge_hb15: 0,  // HB15 High-Base 15m-Dip
  x_edge_rtb: 0,  // RTB RTB-Base
  x_edge_p55: 0,  // P55 refined setup
  x_edge_par: 0,  // PAR Parabola ride  [was 5 — DUP: same bars as uf_edge_par (Jaccard 1.00)]
  x_edge_conf3: 0,  // 🎯3 Cluster-Bottom ≥3 families  [was 5 — DUP: same bars as uf_edge_conf3 (Jaccard 1.00)]
  x_edge_conf4: 0,  // 🎯4 Cluster-Bottom ≥4 families  [was 5 — DUP: same bars as uf_edge_conf4 (Jaccard 1.00)]
  x_edge_g3rl: 0,  // G3RL G3 gap-chain variant  [was 5 — DUP: same bars as uf_edge_g3rl (Jaccard 1.00)]
  x_edge_g3g3: 0,  // G3² G3→G3 gap-chain  [was 5 — DUP: same bars as uf_edge_g3g3 (Jaccard 1.00)]
  x_edge_g3g3rl: 0,  // G3²RL🟡 WATCH-tier (day-clustered weaker)  [was 5 — DUP: same bars as uf_edge_g3g3rl (Jaccard 1.00)]
  x_edge_l34camp: 0,  // 💠L34C L34-camp reversal  [was 5 — DUP: same bars as uf_edge_l34camp (Jaccard 1.00)]
  x_edge_sc46: 0,  // SC46
  x_edge_nssc: 0,  // NSSC
  x_edge_g3l46: 0,  // G3L46 G3+L46 chain  [was 5 — DUP: same bars as uf_edge_g3l46 (Jaccard 1.00)]
  x_edge_fbt: 0,  // ⚔️FBT fail-bear trap
  x_edge_zat: 0,  // 💤ZAT Z-Absorb-Turn
  x_edge_cf: 0,  // 🧊CF Coil-floor absorption
  x_edge_ear: 0,  // 🌀EAR Engulf-Absorb-Reversal
  x_edge_t1rs: 0,  // 🎯T1RS T1 + RS dip
  x_edge_t3rs: 0,  // 🎯T3RS T3 + RS dip
  x_edge_l34cont: 0,  // 🏆L34C L34→L34 continuity  [was 5 — DUP: same bars as uf_edge_l34cont (Jaccard 1.00)]
  x_edge_svc: 0,  // 🎬SVC stop-volume confirm
  x_edge_g3_lead: 0,  // 🥇G3 G3 + Leader-in-Laggard  [was 5 — DUP: same bars as lead_lag (Jaccard 0.98)]
  x_edge_g3a_lead: 0,  // 🥇G3A G3-Abs + Leader gate  [was 5 — DUP: same bars as lead_g3a (Jaccard 1.00)]
  x_edge_l43_lead: 5,  // 🥇L43 L43-TRIPLE + Leader gate
  x_edge_sand: 0,  // 🥪SAND T2G-Sandwich
  x_edge_gnb: 0,  // 🪨GNB T1G-NB + RS
  x_edge_gnb_l34: 0,  // 🪨+L34 T1G-NB + L34 pre-condition
  x_edge_wsh_iv: 0,  // WSH🔎 Washout, intraday demand confirmed
  x_edge_wsh_vs: 0,  // WSH💥 Washout, volume event confirmed
  x_seqctx_up: 5,  // SEQ⤴ booster  [was 0 — bull +5, 2026-10-07]
  x_seqctx_dn: -5,  // SEQ⤵ suppressor  [was 0 — bear −5, 2026-10-07]
  x_seqctx_tail: 0,  // SEQ🎲 tail
  x_seqens_fires: 0,  // SEQ_ENS any
  x_seq34_fires: 0,  // SEQ34 any
  x_mtf_echo_true: 5,  // MTF echo confirm  [was 0 — bull +5, 2026-10-07]
  x_mtf_echo_false: -5,  // MTF echo ZERO  [was 0 — bear −5, 2026-10-07]
  x_fly_fresh: 5,  // FLY fresh (any ✦type)
  x_fly_fresh_abcd: 0,  // ✦FLY-ABCD  [was 5 — AND: = AND of 4 signals (fires on 100% of their joint bars): _l5_any, _not_ext, fly_abcd, x_fly]
  x_fly_fresh_cd: 5,  // ✦FLY-CD
  x_fly_fresh_bd: 0,  // ✦FLY-BD  [was 5 — DUP: same bars as x_fly_fresh (Jaccard 0.98)]
  x_fly_fresh_ad: 0,  // ✦FLY-AD  [was 5 — AND: = AND of 4 signals (fires on 100% of their joint bars): _l5_any, _not_ext, fly_ad, x_fly_f]
  x_turn_echo: 0,  // TURN echo (any ①②③)
  x_turn_echo_1: 5,  // ① 1/3 intraday TFs echoed
  x_turn_echo_2: 5,  // ② 2/3 intraday TFs echoed
  x_turn_echo_3: 5,  // ③ 3/3 intraday TFs echoed
  x_div_buy: 5,  // DIV buy
  x_div_deep: 0,  // DIV deep
  x_div_top: 0,  // DIV top (suppressor)

  // ▽△ Bottom-Anatomy verdict (Superchart-only — no Ultra equivalent, see v4ExtraGroups.js)
  x_anat_rev_rs: 5,  // 🔻💪 precise bottom (rev+RS)
  x_anat_rev_norm: 5,  // 🔻 structural bottom (rev, no RS)  [was 0 — bull +5, 2026-10-07]
  x_anat_shake: 5,  // 🌀 shakeout/spring
  x_anat_cont: 5,  // 🔺 continuation (markup)

  // ⚛ PHYS regime combos — scored separately from _ph_ra/_ph_rn/_ph_rf and _ph_u/_ph_d above;
  // set those to 0 if you only want the COMBINATION (e.g. RA·D) to carry a score
  x_phys_ra_u: 5,  // RA·U  [was 0 — bull +5, 2026-10-07]
  x_phys_ra_d: 5,  // RA·D
  x_phys_rn_u: 5,  // RN·U
  x_phys_rn_d: -5,  // RN·D  [was 0 — bear −5, 2026-10-07]
  x_phys_rf_u: 5,  // RF·U
  x_phys_rf_d: -5,  // RF·D  [was 0 — bear −5, 2026-10-07]

  // ⚛ PHYS energy tier × ★ — clean, non-overlapping (unlike _ph_e2/_ph_es above, which
  // together miss plain E0/E1 and double-count E2★ — see the note in v4ExtraGroups.js)
  x_phys_e0: 0,  // E0 (no ★)
  x_phys_e1: 0,  // E1 (no ★)
  x_phys_e2: 5,  // E2 (no ★)
  x_phys_e0_star: 5,  // E0★ released
  x_phys_e1_star: 5,  // E1★ released
  x_phys_e2_star: 5,  // E2★ released

  // ═══ 2026-09-23 composite sweep (v4ExtraGroups.js has the full audit notes) ═══
  // ▭ Body+Wick, Superchart-side mirror of _bw_*
  x_bw2_x: 0,  // X body (expanded)
  x_bw2_m: 0,  // M body (minimal)
  x_bw2_s: 0,  // S body (normal)
  x_bw2_j: 0,  // J wick (doji)
  x_bw2_tb: 0,  // TB wick (upper)
  x_bw2_bb: 0,  // BB wick (lower, "the hammer")  [was 5 — DUP: same bars as _bw_bb (Jaccard 1.00)]
  x_bw2_f: 0,  // F wick (flat)
  x_bw2_xf: 0,  // XF (expanded + flat)
  x_bw2_mf: 0,  // MF (minimal + flat)
  x_bw_x_bare: 0,  // X (expanded, no strong wick)
  x_bw_s_bare: 0,  // S (normal, no strong wick)
  x_bw_m_bare: 0,  // M (minimal, no strong wick)
  x_bw_x_j: 0,  // XJ (expanded + doji)
  x_bw_x_tb: 0,  // XTB (expanded + upper wick)
  x_bw_x_bb: 0,  // XBB (expanded + lower wick, "the hammer")  [was 5 — AND: = AND of 4 signals (fires on 100% of their joint bars): _l5_any, _not_ext, x_bw2_x, x_bw2_]
  x_bw_s_j: 0,  // SJ (normal + doji)
  x_bw_s_tb: 0,  // STB (normal + upper wick)  [was 5 — AND: = AND of 4 signals (fires on 100% of their joint bars): _l5_any, _not_ext, x_bw2_s, x_bw2_]
  x_bw_s_bb: 0,  // SBB (normal + lower wick)  [was 5 — AND: = AND of 4 signals (fires on 100% of their joint bars): _l5_any, _not_ext, x_bw2_s, x_bw2_]
  x_bw_s_f: 0,  // SF (normal + flat)
  x_bw_m_j: 0,  // MJ (minimal + doji)  [was 5 — AND: = AND of 4 signals (fires on 100% of their joint bars): _l5_any, _not_ext, x_bw2_m, x_bw2_]
  x_bw_m_tb: 0,  // MTB (minimal + upper wick)  [was 5 — AND: = AND of 4 signals (fires on 100% of their joint bars): _l5_any, _not_ext, x_bw2_m, x_bw2_]
  x_bw_m_bb: 0,  // MBB (minimal + lower wick)  [was 5 — AND: = AND of 4 signals (fires on 100% of their joint bars): _l5_any, _not_ext, x_bw2_m, x_bw2_]
  // 🧬 SEQ34 tier
  x_seq34_gold: 5,  // 🧬🏆 DSR≥0.6 selection-proof
  x_seq34_coarse: 0,  // 🧬° coarse match (no-L token)
  x_seq34_exact: 0,  // 🧬 exact match
  // ▭ L34 grade × color × TRIPLE
  x_l34_red: 0,  // red L34 (absorption line)  [was 5 — DUP: same bars as l22 (Jaccard 0.98)]
  x_l34_green: 0,  // green L34 (trap type)  [was 5 — DUP: same bars as l34 (Jaccard 1.00)]
  x_l34_grade2plus: 5,  // red L34, grade ≥2/3
  x_l34_grade3: 5,  // red L34, grade 3/3 (top of ladder)
  x_l34_triple: 0,  // 💠 TRIPLE (red-L34 + 🟢REV + ▲4H)  [was 5 — AND: = AND of 11 signals (fires on 100% of their joint bars): _wl_l3, _ne_n, _l5_any, l22, _lt_]
  // 🥇 EDGE premium-combo gate
  x_edge_premium_rev: 0,  // EDGE🟢 premium (QZC/D+L1/RTB/P55 + REV-confirmed)  [was 5 — DUP: same bars as uf_edge_premium_rev (Jaccard 1.00)]
  // VOL7 remaining tiers
  x_vol7_m1: 0,  // M1
  x_vol7_m2: 0,  // M2
  x_vol7_m3: 0,  // M3
  x_vol7_m4: 0,  // M4
  x_vol7_sg0: 0,  // σ0
  x_vol7_sg1: 0,  // σ1
  x_vol7_sg2: 0,  // σ2
  x_vol7_sg3: 0,  // σ3
  x_vol7_sg4: 0,  // σ4
  x_vol7_sg5: 0,  // σ5 (+1σ..+2σ)
  x_vol7_sg6: 0,  // σ6 (≥+2σ)
  // 🔊 VOL7 M{mr}·σ{sg} full grid — the exact-pair combo, separate from the marginals above
  x_vol7_m0_sg0: 0,  // M0·σ0
  x_vol7_m0_sg1: 0,  // M0·σ1
  x_vol7_m0_sg2: 0,  // M0·σ2
  x_vol7_m0_sg3: 0,  // M0·σ3
  x_vol7_m0_sg4: 0,  // M0·σ4
  x_vol7_m0_sg5: 0,  // M0·σ5
  x_vol7_m0_sg6: 0,  // M0·σ6
  x_vol7_m1_sg0: 0,  // M1·σ0
  x_vol7_m1_sg1: 0,  // M1·σ1
  x_vol7_m1_sg2: 0,  // M1·σ2
  x_vol7_m1_sg3: 0,  // M1·σ3
  x_vol7_m1_sg4: 0,  // M1·σ4
  x_vol7_m1_sg5: 0,  // M1·σ5
  x_vol7_m1_sg6: 0,  // M1·σ6
  x_vol7_m2_sg0: 0,  // M2·σ0
  x_vol7_m2_sg1: 0,  // M2·σ1
  x_vol7_m2_sg2: 0,  // M2·σ2
  x_vol7_m2_sg3: 0,  // M2·σ3
  x_vol7_m2_sg4: 0,  // M2·σ4
  x_vol7_m2_sg5: 0,  // M2·σ5
  x_vol7_m2_sg6: 0,  // M2·σ6
  x_vol7_m3_sg0: 0,  // M3·σ0
  x_vol7_m3_sg1: 0,  // M3·σ1
  x_vol7_m3_sg2: 0,  // M3·σ2
  x_vol7_m3_sg3: 0,  // M3·σ3
  x_vol7_m3_sg4: 5,  // M3·σ4
  x_vol7_m3_sg5: 0,  // M3·σ5
  x_vol7_m3_sg6: 0,  // M3·σ6
  x_vol7_m4_sg0: 0,  // M4·σ0
  x_vol7_m4_sg1: 0,  // M4·σ1
  x_vol7_m4_sg2: 0,  // M4·σ2
  x_vol7_m4_sg3: 5,  // M4·σ3
  x_vol7_m4_sg4: 0,  // M4·σ4
  x_vol7_m4_sg5: 0,  // M4·σ5
  x_vol7_m4_sg6: 5,  // M4·σ6
  x_vol7_m5_sg0: 0,  // M5·σ0
  x_vol7_m5_sg1: 0,  // M5·σ1
  x_vol7_m5_sg2: 0,  // M5·σ2
  x_vol7_m5_sg3: 0,  // M5·σ3
  x_vol7_m5_sg4: 0,  // M5·σ4
  x_vol7_m5_sg5: 5,  // M5·σ5
  x_vol7_m5_sg6: 5,  // M5·σ6
  x_vol7_m6_sg0: 0,  // M6·σ0
  x_vol7_m6_sg1: 0,  // M6·σ1
  x_vol7_m6_sg2: 0,  // M6·σ2
  x_vol7_m6_sg3: 0,  // M6·σ3
  x_vol7_m6_sg4: 5,  // M6·σ4
  x_vol7_m6_sg5: 5,  // M6·σ5
  x_vol7_m6_sg6: 5,  // M6·σ6
  // 🔷 SHAPE code × direction
  x_shape_mth_up: 5,  // MTH↑
  x_shape_mth_dn: -5,  // MTH↓  [was 0 — bear −5, 2026-10-07]
  x_shape_cl4_up: 5,  // CL4↑
  x_shape_cl4_dn: -5,  // CL4↓  [was 0 — bear −5, 2026-10-07]
  x_shape_mid_up: 5,  // MID↑
  x_shape_mid_dn: -5,  // MID↓  [was 0 — bear −5, 2026-10-07]
  x_shape_exp_up: 5,  // EXP↑
  x_shape_exp_dn: -5,  // EXP↓  [was 0 — bear −5, 2026-10-07]
  x_shape_con_up: 5,  // CON↑
  x_shape_con_dn: -5,  // CON↓  [was 0 — bear −5, 2026-10-07]
  x_shape_lst_dn: -5,  // LST↓  [was 0 — bear −5, 2026-10-07]
  x_shape_wrp_up: 5,  // WRP↑
  x_shape_wrp_dn: -5,  // WRP↓  [was 0 — bear −5, 2026-10-07]

  // ⚛ PHYS K-stretch × direction — clean, non-overlapping (unlike _ph_k1/_ph_k2 above,
  // which fire on either direction alike)
  x_phys_k1_u: 5,  // K1U
  x_phys_k1_d: -5,  // K1D  [was 0 — bear −5, 2026-10-07]
  x_phys_k2_u: 5,  // K2U (extended above EMA20)
  x_phys_k2_d: -5,  // K2D (extended below EMA20)  [was 0 — bear −5, 2026-10-07]

  // ⚛ PHYS S-resonance — only S3U/S3D (_ph_s3u/_ph_s3d above) had keys before
  x_phys_s0: 0,  // S0 (flat/no alignment)
  x_phys_s1_u: 0,  // S1U
  x_phys_s2_u: 5,  // S2U  [was 0 — bull +5, 2026-10-07]
  x_phys_s1_d: 0,  // S1D
  x_phys_s2_d: -5,  // S2D  [was 0 — bear −5, 2026-10-07]

  // ⏱ 4H/1H intraday-leads — validated +0.84pp/6-6yr (lead4h.py)
  x_h4_rev_today: 5,  // ▲ 4H REV-trigger (early entry)
  x_h1_rev_today: 5,  // △ 1H REV-trigger only (4H silent)

  // 🟢🔵⚠️ BUY row — validated 6yr path-sim
  x_rev_buy_echo: 5,  // 🟢 REV-buy + MTF echo
  x_rev_buy_noecho: 0,  // ⚠️ REV-buy, NO echo (suppressor)
  x_brk_buy: 5,  // 🔵 BRK-buy
  x_mtf_conf_0: -5,  // mtf_conf 0/3 (HARD SKIP)  [was 0 — bear −5, 2026-10-07]
  x_mtf_conf_1: 5,  // mtf_conf 1/3
  x_mtf_conf_2: 5,  // mtf_conf 2/3
  x_mtf_conf_3: 5,  // mtf_conf 3/3

  // ● whisper (Cat row) — strong seq-context + same-bar intraday echo
  x_whisper: 5,  // ● whisper

  // Cat row — profile_category
  x_profile_sweet_spot: 5,  // ⭐ Sweet Spot
  x_profile_building: 5,  // ↑ Building
  x_profile_late: 5,  // ⚠ Late
  // ── keys not listed in v4Weights.js that now carry weight (DUP representatives) ──
  uf_edge_atm: 5,  // ATM  [inherits a duplicate's weight]
  uf_edge_atmr: 5,  // ATMR  [inherits a duplicate's weight]
  uf_edge_spr: 5,  // SPR  [inherits a duplicate's weight]
  uf_edge_z11: 5,  // Z11  [inherits a duplicate's weight]
  uf_edge_l43: 5,  // L43  [inherits a duplicate's weight]
  uf_edge_l43_quiet: 5,  // L43🔇  [inherits a duplicate's weight]
  uf_edge_wsh: 5,  // WSH  [inherits a duplicate's weight]
  uf_edge_eng: 5,  // ENG  [inherits a duplicate's weight]
  uf_edge_el46: 5,  // EL46  [inherits a duplicate's weight]
  uf_edge_zrt: 5,  // ZRT  [inherits a duplicate's weight]
  uf_edge_zrt_l46: 5,  // ZRT🟢  [inherits a duplicate's weight]
  uf_edge_par: 5,  // PAR  [inherits a duplicate's weight]
  uf_edge_conf3: 5,  // 🎯3  [inherits a duplicate's weight]
  uf_edge_conf4: 5,  // 🎯4  [inherits a duplicate's weight]
  uf_edge_g3rl: 5,  // G3RL  [inherits a duplicate's weight]
  uf_edge_g3g3: 5,  // G3²  [inherits a duplicate's weight]
  uf_edge_g3g3rl: 5,  // G3²RL🟡  [inherits a duplicate's weight]
  uf_edge_l34camp: 5,  // 💠L34C  [inherits a duplicate's weight]
  uf_edge_g3l46: 5,  // G3L46  [inherits a duplicate's weight]
  uf_edge_l34cont: 5,  // 🏆L34C  [inherits a duplicate's weight]
  uf_edge_premium_rev: 5,  // EDGE🟢  [inherits a duplicate's weight]
  // ── independent signals with a direction, not listed in v4Weights.js (2026-10-07) ──
  uf_edge_t3rs: 5,  // 🎯T3RS  [bull +5, 2026-10-07]
  uf_edge_cf: 5,  // 🧊CF  [bull +5, 2026-10-07]
  uf_edge_t1rs: 5,  // 🎯T1RS  [bull +5, 2026-10-07]
  uf_edge_nssc: 5,  // NSSC  [bull +5, 2026-10-07]
  uf_edge_p55: 5,  // P55  [bull +5, 2026-10-07]
  uf_comp_z2gl46ed: 5,  // Z2GL46ED  [bull +5, 2026-10-07]
  uf_edge_h1dr: 5,  // 🕐DR  [bull +5, 2026-10-07]
  uf_edge_g3a: 5,  // ⚡G3-Abs  [bull +5, 2026-10-07]
  uf_edge_dualrec: 5,  // 🔄DR  [bull +5, 2026-10-07]
  uf_edge_cap: 5,  // CAP  [bull +5, 2026-10-07]
  uf_edge_svc: 5,  // 🎬SVC  [bull +5, 2026-10-07]
  uf_edge_wsh_vs: 5,  // WSH💥  [bull +5, 2026-10-07]
  uf_edge_g3: 5,  // G3  [bull +5, 2026-10-07]
  uf_edge_dl1: 5,  // D+L1  [bull +5, 2026-10-07]
  uf_edge_gnb: 5,  // 🪨GNB  [bull +5, 2026-10-07]
  uf_edge_sand: 5,  // 🥪SAND  [bull +5, 2026-10-07]
  uf_comp_t5l46ed: 5,  // T5L46ED  [bull +5, 2026-10-07]
  ve_bov: 5,  // BOV▲  [bull +5, 2026-10-07]
  uf_edge_qzc: 5,  // QZC  [bull +5, 2026-10-07]
  uf_edge_ear: 5,  // 🌀EAR  [bull +5, 2026-10-07]
  ve_rel_up: 5,  // ▲rel  [bull +5, 2026-10-07]
  ve_bo: 5,  // BO▲  [bull +5, 2026-10-07]
  uf_edge_hb15: 5,  // HB15  [bull +5, 2026-10-07]
  uf_edge_sc46: 5,  // SC46  [bull +5, 2026-10-07]
  uf_edge_rtb: 5,  // RTB  [bull +5, 2026-10-07]
  uf_edge_fbt: 5,  // ⚔️FBT  [bull +5, 2026-10-07]
  uf_edge_wsh_iv: 5,  // WSH🔎  [bull +5, 2026-10-07]
  ve_bd: -5,  // BD▼  [bear −5, 2026-10-07]
  ve_qr_rel_veto: -5,  // ⛔QR▲  [bear −5, 2026-10-07]
  ve_bdv: -5,  // BDV▼  [bear −5, 2026-10-07]
  ve_rel_dn: -5,  // ▼rel  [bear −5, 2026-10-07]
}
