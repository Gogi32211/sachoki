// V4 WEIGHTS — per-signal points, user-directed (2026-09-23).
//
// The user: "everyone starts at 0, I will tell you incrementally who gets 5 or some other
// number." So the default for a signal NOT in this map is 0, and it is mapped EXPLICITLY
// for the full 402-entry SIG_GROUPS catalog rather than left implicit, so nothing is
// silently missing and every edit the user asks for is a one-line change here.
//
// THIS FILE IS THE ONLY REFERENCE — the user reads and edits it directly (2026-09-23: "marto
// es iyos", after a separate research_out/V4_ALL_SIGNALS.md table was tried and dropped, so
// there is one place, not two that can drift apart). Each line is `key: weight, // label` —
// grouped under the same section comments SIG_GROUPS itself uses in UltraScanPanel.jsx, so a
// signal here can always be found again by its key in that file.
//
// v4Score.js sums weights[fired signal's key] (?? 0) — a key with weight 0 costs nothing
// but the tooltip on both surfaces still SHOWS it fired, so the user can see what a ticker
// is doing before deciding whether it should count.
export const V4_WEIGHTS = {
  // ── ★ composite setups (backtested edge, 8M-bar fwd-return analysis) ──
  _combo_volbull: 5,  // ⚡Vol-Bull
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
  buy_2809: 0,  // BUY
  rocket: 0,  // 🚀
  sig3g: 5,  // 3G
  rtv: 0,  // RTV
  hilo_buy: 5,  // HILO↑
  atr_brk: 5,  // ATR↑
  bb_brk: 5,  // BB↑
  va: 0,  // VA
  bias_up: 0,  // ↑BIAS
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
  lvx_l34_vh: 5,  // L34VH
  lvx_l34_vx: 5,  // L34VX
  lvx_l46_v: 5,  // L46V
  lvx_l46_vl: 5,  // L46VL
  lvx_l46_vh: 5,  // L46VH
  lvx_l46_vx: 5,  // L46VX
  // ── OVD daily map (opening / closing 15m volume logics · descriptive) ──
  ovdmap_ob30: 5,  // OB·30
  ovdmap_ob60: 5,  // OB·60
  ovdmap_rc30: 0,  // RC·30
  ovdmap_rc60: 0,  // RC·60
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
  vol7_sigma_plus: 5,  // Σ+
  vol7_mr_plus: 5,  // MR+
  vol7_vb2: 0,  // VB2
  vol7_shift_up: 5,  // SHIFT↑
  vol7_shift_dn: 0,  // SHIFT↓
  // ── SHAPE × CONTEXT (body-nest shapes · effort · cluster · descriptive — clustering measured WORSE) ──
  shape_mth: 5,  // MTH
  shape_cl4: 5,  // CL4
  shape_mid: 5,  // MID
  shape_exp: 5,  // EXP
  shape_con: 5,  // CON
  shape_lst: 5,  // LST
  shape_wrp: 5,  // WRP
  shape_lstup_veto: 0,  // ⛔LST↑
  shape_veto: 0,  // ⛔KNF
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
  pv_o_vup: 0,  // VUP
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
  pv_c_vup: 0,  // VUP
  pv_c_turn: 5,  // TURN
  pv_c_up4: 5,  // UP4
  pv_c_re2: 0,  // RE2
  pv_veto_c: 0,  // ⛔veto
  cd: 0,  // CD
  ca: 0,  // CA
  cw: 0,  // CW
  g1: 0,  // G1
  g2: 0,  // G2
  g4: 0,  // G4
  g6: 0,  // G6
  g11: 0,  // G11
  seq_bcont: 0,  // SBC
  tz_any_t: 5,  // ANY T
  tz_t1g: 0,  // T1G
  tz_t2g: 0,  // T2G
  tz_t1: 0,  // T1
  tz_t2: 0,  // T2
  tz_t3: 0,  // T3
  tz_t4: 0,  // T4
  tz_t5: 0,  // T5
  tz_t6: 0,  // T6
  tz_t9: 0,  // T9
  tz_t10: 0,  // T10
  tz_t11: 0,  // T11
  tz_t12: 0,  // T12
  tz_bull_flip: 0,  // TZ→3
  tz_attempt: 0,  // TZ→2
  tz_weak_bull: 0,  // W
  tz_any_z: 0,  // ANY Z
  tz_z1g: 0,  // Z1G
  tz_z2g: 0,  // Z2G
  tz_z1: 0,  // Z1
  tz_z2: 0,  // Z2
  tz_z3: 0,  // Z3
  tz_z4: 0,  // Z4
  tz_z5: 0,  // Z5
  tz_z6: 0,  // Z6
  tz_z7: 0,  // Z7
  tz_z9: 0,  // Z9
  tz_z10: 0,  // Z10
  tz_z11: 0,  // Z11
  tz_z12: 0,  // Z12
  // ── L1 · L code ──
  _wl_any: 0,  // ANY L
  _wl_l1: 0,  // L1
  _wl_l2: 0,  // L2
  _wl_l3: 5,  // L3
  _wl_l4: 0,  // L4
  _wl_l5: 0,  // L5
  _wl_l6: 0,  // L6
  _wl_l34: 5,  // L34
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
  _cl_a: 0,  // A
  _cl_o: 0,  // O
  _cl_i: 0,  // I
  // ── L3 body·wick ──
  _bw_x: 0,  // X
  _bw_m: 0,  // M
  _bw_s: 0,  // S
  _bw_j: 0,  // J
  _bw_tb: 0,  // TB
  _bw_bb: 0,  // BB
  _bw_f: 0,  // F
  _bw_xf: 0,  // XF
  _bw_mf: 0,  // MF
  // ── ⚛ physics (260815) ──
  _ph_ra: 0,  // RA
  _ph_rn: 0,  // RN
  _ph_rf: 0,  // RF
  _ph_u: 0,  // ·U
  _ph_d: 0,  // ·D
  _ph_e2: 5,  // E2
  _ph_es: 5,  // E★
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
  _ph_s3d: 0,  // S3D
  _ph_ad: 5,  // AD any
  _ph_ad1: 5,  // ★
  _ph_ad2: 5,  // ★★
  _ph_ada: 5,  // ★A
  _ph_gg3: 5,  // gG3
  _ph_spr: 5,  // SPRING⚛
  _ph_sprs: 5,  // SPRING★⚛
  _ph_utad: 0,  // UTAD⚛
  _ph_sos: 0,  // SOS★⚛
  _ph_acc: 0,  // ACC-TR⚛
  _ph_dist: 0,  // DIST-TR⚛
  _ph_mkup: 0,  // MARKUP⚛
  _ph_mkdn: 0,  // MKDN⚛
  // ── ⟂ CISD (260815) ──
  _ci_ps: 0,  // +S
  _ci_ms: 0,  // −S
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
  _l5_pb: 0,  // PB
  _l5_ps: 0,  // PS
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
  w2_accum: 0,  // ACC
  w2_break: 0,  // BRK
  w2_sc: 0,  // SC
  w2_ar: 0,  // AR
  w2_st: 0,  // ST
  w2_spring: 5,  // SPR
  w2_sos: 5,  // SOS
  w2_jac: 0,  // JAC
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
  l34: 0,  // L34
  l43: 0,  // L43
  l22: 0,  // L22
  l555: 0,  // L555
  blue: 5,  // BL
  cci_ready: 0,  // CCI
  cci_0_retest: 0,  // CCI0R
  cci_blue_turn: 0,  // CCIB
  bo_up: 5,  // BO↑
  bo_dn: 0,  // BO↓
  bx_up: 5,  // BX↑
  bx_dn: 0,  // BX↓
  be_up: 5,  // BE↑
  be_dn: 0,  // BE↓
  fuchsia_rh: 5,  // RH
  fuchsia_rl: 5,  // RL
  pre_pump: 5,  // PP
  x2g_wick: 0,  // X2G
  x2_wick: 0,  // X2
  x1g_wick: 0,  // X1G
  x1_wick: 0,  // X1
  x3_wick: 0,  // X3
  wick_bull: 0,  // WK↑
  best_long: 5,  // BEST↑
  fbo_bull: 5,  // FBO↑
  fbo_bear: 0,  // FBO↓
  eb_bull: 5,  // EB↑
  eb_bear: 0,  // EB↓
  bf_buy: 0,  // 4BF
  bf_sell: 0,  // 4BF↓
  ultra_3up: 0,  // 3↑
  sig_l88: 0,  // L88
  sig_260308: 0,  // 260308
  d_blast_bull: 0,  // ΔΔ↑
  d_surge_bull: 0,  // Δ↑
  d_strong_bull: 0,  // B/S↑
  d_absorb_bull: 5,  // Ab↑
  d_spring: 5,  // dSPR
  d_div_bull: 0,  // T↓
  d_vd_div_bull: 0,  // NS
  d_cd_bull: 0,  // cd↑
  d_flip_bull: 5,  // FLP↑
  d_orange_bull: 0,  // ORG↑
  d_blast_bull_red: 0,  // ΔΔ↑R
  d_surge_bull_red: 0,  // Δ↑R
  d_surge_bear_grn: 0,  // Δ↓G
  d_blast_bear_grn: 0,  // ΔΔ↓G
  d_vd_div_bear: 0,  // ND
  _any_p: 0,  // ANY P
  preup66: 5,  // P66
  preup55: 5,  // P55
  preup89: 5,  // P89
  preup3: 5,  // P3
  preup2: 5,  // P2
  preup50: 5,  // P50
  _any_d: 0,  // ANY D
  predn66: 0,  // D66
  predn55: 0,  // D55
  predn89: 0,  // D89
  predn3: 0,  // D3
  predn2: 0,  // D2
  predn50: 0,  // D50
  _gt_ema200: 0,  // P>200
  _gt_ema89: 0,  // P>89
  _gt_ema50: 0,  // P>50
  _gt_ema20: 0,  // P>20
  _lt_ema20: 0,  // P<20
  _lt_ema50: 0,  // P<50
  _lt_ema89: 0,  // P<89
  _lt_ema200: 0,  // P<200
  rs_strong: 5,  // RS+
  rs: 0,  // RS
  para_prep: 0,  // PREP
  para_start: 5,  // PARA
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
  lead_g3: 5,  // 🥇G3
  lead_g3a: 5,  // 🥇G3A
  lead_l43: 5,  // 🥇L43
  // ── 🟡 Seq-edges (2026-08-04) ──
  seq_any: 5,  // 🟡SEQ*
  seq_crown: 5,  // 👑Z1G
  seq_z1gt4: 5,  // 🌉Z1G4
  seq_v2: 5,  // 🌉v2
  seq_z9hl: 5,  // 🧲Z9HL
  seq_20: 0,  // 🧺SEQ
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
  gog_g1p: 0,  // G1P
  gog_g2p: 0,  // G2P
  gog_g3p: 0,  // G3P
  gog_g1l: 0,  // G1L
  gog_g2l: 0,  // G2L
  gog_g3l: 0,  // G3L
  gog_g1c: 0,  // G1C
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
  fly_cd: 5,  // CD
  fly_bd: 5,  // BD
  fly_ad: 5,  // AD
  _rsi_os: 0,  // RSI≤35
  _rsi_ob: 0,  // RSI≥70
  _yf_only: 0,  // yf
  _cross2: 0,  // ⚡×2+
  _cross3: 0,  // ⚡×3+
  _cross4: 0,  // ⚡×4+
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
  x_edge_atm: 5,  // ATM Atomic edge
  x_edge_atmr: 5,  // ATMR Atomic-R
  x_edge_spr: 5,  // SPR Wyckoff Spring
  x_edge_z11: 5,  // Z11 Z11-T11
  x_edge_l43: 5,  // L43 L43-TRIPLE
  x_edge_l43_quiet: 5,  // L43🔇 quiet-tape (validated tier)
  x_edge_wsh: 5,  // WSH Washout/capitulation
  x_edge_h1b: 5,  // H1B 1H-bottom base
  x_edge_eng: 5,  // ENG Engulf-Absorption
  x_edge_el46: 5,  // EL46 Engulf-L46 (GEM2)
  x_edge_zrt: 5,  // ZRT Zone-Retest
  x_edge_zrt_l46: 5,  // ZRT🟢 Zone-Retest, L46-quality
  x_edge_hb15: 0,  // HB15 High-Base 15m-Dip
  x_edge_rtb: 0,  // RTB RTB-Base
  x_edge_p55: 0,  // P55 refined setup
  x_edge_par: 5,  // PAR Parabola ride
  x_edge_conf3: 5,  // 🎯3 Cluster-Bottom ≥3 families
  x_edge_conf4: 5,  // 🎯4 Cluster-Bottom ≥4 families
  x_edge_g3rl: 5,  // G3RL G3 gap-chain variant
  x_edge_g3g3: 5,  // G3² G3→G3 gap-chain
  x_edge_g3g3rl: 5,  // G3²RL🟡 WATCH-tier (day-clustered weaker)
  x_edge_l34camp: 5,  // 💠L34C L34-camp reversal
  x_edge_sc46: 0,  // SC46
  x_edge_nssc: 0,  // NSSC
  x_edge_g3l46: 5,  // G3L46 G3+L46 chain
  x_edge_fbt: 0,  // ⚔️FBT fail-bear trap
  x_edge_zat: 0,  // 💤ZAT Z-Absorb-Turn
  x_edge_cf: 0,  // 🧊CF Coil-floor absorption
  x_edge_ear: 0,  // 🌀EAR Engulf-Absorb-Reversal
  x_edge_t1rs: 0,  // 🎯T1RS T1 + RS dip
  x_edge_t3rs: 0,  // 🎯T3RS T3 + RS dip
  x_edge_l34cont: 5,  // 🏆L34C L34→L34 continuity
  x_edge_svc: 0,  // 🎬SVC stop-volume confirm
  x_edge_g3_lead: 5,  // 🥇G3 G3 + Leader-in-Laggard
  x_edge_g3a_lead: 5,  // 🥇G3A G3-Abs + Leader gate
  x_edge_l43_lead: 5,  // 🥇L43 L43-TRIPLE + Leader gate
  x_edge_sand: 0,  // 🥪SAND T2G-Sandwich
  x_edge_gnb: 0,  // 🪨GNB T1G-NB + RS
  x_edge_gnb_l34: 0,  // 🪨+L34 T1G-NB + L34 pre-condition
  x_edge_wsh_iv: 0,  // WSH🔎 Washout, intraday demand confirmed
  x_edge_wsh_vs: 0,  // WSH💥 Washout, volume event confirmed
  x_seqctx_up: 0,  // SEQ⤴ booster
  x_seqctx_dn: 0,  // SEQ⤵ suppressor
  x_seqctx_tail: 0,  // SEQ🎲 tail
  x_seqens_fires: 0,  // SEQ_ENS any
  x_seq34_fires: 0,  // SEQ34 any
  x_mtf_echo_true: 0,  // MTF echo confirm
  x_mtf_echo_false: 0,  // MTF echo ZERO
  x_fly_fresh: 5,  // FLY fresh (any ✦type)
  x_fly_fresh_abcd: 5,  // ✦FLY-ABCD
  x_fly_fresh_cd: 5,  // ✦FLY-CD
  x_fly_fresh_bd: 5,  // ✦FLY-BD
  x_fly_fresh_ad: 5,  // ✦FLY-AD
  x_turn_echo: 0,  // TURN echo (any ①②③)
  x_turn_echo_1: 5,  // ① 1/3 intraday TFs echoed
  x_turn_echo_2: 5,  // ② 2/3 intraday TFs echoed
  x_turn_echo_3: 5,  // ③ 3/3 intraday TFs echoed
  x_div_buy: 5,  // DIV buy
  x_div_deep: 0,  // DIV deep
  x_div_top: 0,  // DIV top (suppressor)

  // ▽△ Bottom-Anatomy verdict (Superchart-only — no Ultra equivalent, see v4ExtraGroups.js)
  x_anat_rev_rs: 5,  // 🔻💪 precise bottom (rev+RS)
  x_anat_rev_norm: 0,  // 🔻 structural bottom (rev, no RS)
  x_anat_shake: 5,  // 🌀 shakeout/spring
  x_anat_cont: 5,  // 🔺 continuation (markup)

  // ⚛ PHYS regime combos — scored separately from _ph_ra/_ph_rn/_ph_rf and _ph_u/_ph_d above;
  // set those to 0 if you only want the COMBINATION (e.g. RA·D) to carry a score
  x_phys_ra_u: 0,  // RA·U
  x_phys_ra_d: 5,  // RA·D
  x_phys_rn_u: 5,  // RN·U
  x_phys_rn_d: 0,  // RN·D
  x_phys_rf_u: 5,  // RF·U
  x_phys_rf_d: 0,  // RF·D

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
  x_bw2_bb: 5,  // BB wick (lower, "the hammer")
  x_bw2_f: 0,  // F wick (flat)
  x_bw2_xf: 0,  // XF (expanded + flat)
  x_bw2_mf: 0,  // MF (minimal + flat)
  x_bw_x_bare: 0,  // X (expanded, no strong wick)
  x_bw_s_bare: 0,  // S (normal, no strong wick)
  x_bw_m_bare: 0,  // M (minimal, no strong wick)
  x_bw_x_j: 0,  // XJ (expanded + doji)
  x_bw_x_tb: 0,  // XTB (expanded + upper wick)
  x_bw_x_bb: 5,  // XBB (expanded + lower wick, "the hammer")
  x_bw_s_j: 0,  // SJ (normal + doji)
  x_bw_s_tb: 5,  // STB (normal + upper wick)
  x_bw_s_bb: 5,  // SBB (normal + lower wick)
  x_bw_s_f: 0,  // SF (normal + flat)
  x_bw_m_j: 5,  // MJ (minimal + doji)
  x_bw_m_tb: 5,  // MTB (minimal + upper wick)
  x_bw_m_bb: 5,  // MBB (minimal + lower wick)
  // 🧬 SEQ34 tier
  x_seq34_gold: 5,  // 🧬🏆 DSR≥0.6 selection-proof
  x_seq34_coarse: 0,  // 🧬° coarse match (no-L token)
  x_seq34_exact: 0,  // 🧬 exact match
  // ▭ L34 grade × color × TRIPLE
  x_l34_red: 5,  // red L34 (absorption line)
  x_l34_green: 5,  // green L34 (trap type)
  x_l34_grade2plus: 5,  // red L34, grade ≥2/3
  x_l34_grade3: 5,  // red L34, grade 3/3 (top of ladder)
  x_l34_triple: 5,  // 💠 TRIPLE (red-L34 + 🟢REV + ▲4H)
  // 🥇 EDGE premium-combo gate
  x_edge_premium_rev: 5,  // EDGE🟢 premium (QZC/D+L1/RTB/P55 + REV-confirmed)
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
  x_shape_mth_dn: 0,  // MTH↓
  x_shape_cl4_up: 5,  // CL4↑
  x_shape_cl4_dn: 0,  // CL4↓
  x_shape_mid_up: 5,  // MID↑
  x_shape_mid_dn: 0,  // MID↓
  x_shape_exp_up: 5,  // EXP↑
  x_shape_exp_dn: 0,  // EXP↓
  x_shape_con_up: 5,  // CON↑
  x_shape_con_dn: 0,  // CON↓
  x_shape_lst_dn: 0,  // LST↓
  x_shape_wrp_up: 5,  // WRP↑
  x_shape_wrp_dn: 0,  // WRP↓

  // ⚛ PHYS K-stretch × direction — clean, non-overlapping (unlike _ph_k1/_ph_k2 above,
  // which fire on either direction alike)
  x_phys_k1_u: 5,  // K1U
  x_phys_k1_d: 0,  // K1D
  x_phys_k2_u: 5,  // K2U (extended above EMA20)
  x_phys_k2_d: 0,  // K2D (extended below EMA20)

  // ⚛ PHYS S-resonance — only S3U/S3D (_ph_s3u/_ph_s3d above) had keys before
  x_phys_s0: 0,  // S0 (flat/no alignment)
  x_phys_s1_u: 0,  // S1U
  x_phys_s2_u: 0,  // S2U
  x_phys_s1_d: 0,  // S1D
  x_phys_s2_d: 0,  // S2D

  // ⏱ 4H/1H intraday-leads — validated +0.84pp/6-6yr (lead4h.py)
  x_h4_rev_today: 5,  // ▲ 4H REV-trigger (early entry)
  x_h1_rev_today: 5,  // △ 1H REV-trigger only (4H silent)

  // 🟢🔵⚠️ BUY row — validated 6yr path-sim
  x_rev_buy_echo: 5,  // 🟢 REV-buy + MTF echo
  x_rev_buy_noecho: 0,  // ⚠️ REV-buy, NO echo (suppressor)
  x_brk_buy: 5,  // 🔵 BRK-buy
  x_mtf_conf_0: 0,  // mtf_conf 0/3 (HARD SKIP)
  x_mtf_conf_1: 5,  // mtf_conf 1/3
  x_mtf_conf_2: 5,  // mtf_conf 2/3
  x_mtf_conf_3: 5,  // mtf_conf 3/3

  // ● whisper (Cat row) — strong seq-context + same-bar intraday echo
  x_whisper: 5,  // ● whisper

  // Cat row — profile_category
  x_profile_sweet_spot: 5,  // ⭐ Sweet Spot
  x_profile_building: 5,  // ↑ Building
  x_profile_late: 5,  // ⚠ Late
}
