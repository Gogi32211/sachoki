// V4_EXTRA_GROUPS — signal-like fields that exist on the merged bar (Ultra scan row AND
// Superchart's /api/bar_signals row) but are NOT in UltraScanPanel's SIG_GROUPS catalog,
// because they were never built as filter chips. Added 2026-09-23 on request: "me minda
// rom am CSV failSi rac ki rame velebi arsebobs signalebis... yvelafris arCeva Semezlos
// rom qulebi mivaniWo" — give V4 access to RANK/CONF/EDGES/SEQ/MTF/DIV too, not just the
// 402 SIG_GROUPS entries.
//
// v4Score()/v4FiredLabels() are called with [...SIG_GROUPS, ...V4_EXTRA_GROUPS] so these
// entries score exactly like any other custom SIG_GROUPS entry. They are NOT added to
// SIG_GROUPS itself, on purpose — SIG_GROUPS also drives Ultra's filter-chip UI, and
// wiring 15 new checkboxes there was not asked for; this stays V4-only.
//
// Deliberately EXCLUDED from this file (checked against the live backend before adding
// anything, not assumed from the CSV column list alone):
//   - MDL_* (20 CSV columns, bulk_export.py) — grepped the whole backend: nothing ever
//     assigns b["mdl_um_gog1"] etc. The CSV always exports 0/'' for these. Dead columns,
//     not a signal family — do not score.
//   - FWD_*, MAX_HIGH_*, HIT_*_PCT_*, BARS_TO_VBO/GOG, VBO_W5/W10, GOG_W5/W10,
//     RET_TO_NEXT_*, SEQ34_WIN — forward-looking by construction (computed from bars
//     AFTER the row's date). Never scorable, same rule as ultra_score.py's
//     _FORWARD_RETURN_FIELDS and the Trump-Xi audit's forbidden-column list.
//   - Pre-existing composite scores already on the row (turbo_score, ultra_score*,
//     prebreak_v2/v3, gog_score, signal_score, research_score, clean_entry_score,
//     shakeout_absorb_score, rocket_score, extra_bull_score, experimental_score,
//     rtb_total, BUY_SCORE, PROFILE_SCORE) — each is already a weighted sum over some
//     subset of the SAME atomic signals V4 scores directly. Scoring them INTO V4 would
//     double-count those atomic signals through a second, opaque path. Not included;
//     flag to the user if this turns out to be wanted anyway.
//   - CONF_EXT / CONF_EXT_TOP — fired on 254/300 bars in a spot-check (~85% base rate).
//     That dense a hit rate is context, not an event (see project_signal_cooccurrence_map:
//     "40%+ base rate = context, not signal"). Left out; plain CONF (45/300, ~15%) is in.
//   - Raw OHLCV/date/ticker/vol_bucket and other non-signal metadata columns.

export const V4_EXTRA_GROUPS = [
  { divider: true, label: '🏅 RANK / CONF / EDGES (composite families, not in SIG_GROUPS)' },
  { key: 'x_rank_fires', label: 'RANK any', cls: 'text-amber-300',
    custom: b => b.rank_pct != null },
  { key: 'x_conf_fires', label: 'CONF any', cls: 'text-fuchsia-300',
    custom: b => b.conf != null },
  { key: 'x_edges_any', label: 'EDGES any', cls: 'text-yellow-300',
    custom: b => (b.edges ?? []).length > 0 },

  // Per-code EDGE-board breakout (2026-09-23, user asked: "es EDGES yvela aris sadme
  // damatebuli satitaod?" — is each one scoreable on its own, not just the x_edges_any
  // bucket). Source of truth: backend/edge_replay.py::DISPLAY_SETUPS (the exact list the
  // Superchart EDGE row and Ultra EDGE column both render from — same `b.edges` array on
  // BOTH surfaces, backend/main.py:5801 ticker_edges() for Superchart, latest_edges_map()
  // for Ultra's scan — so unlike the ▽△ anatomy row above, these DO fire identically on
  // both). Each is `b.edges.includes('<display code>')`. 5 codes are deliberately left OUT
  // because they are exact duplicates of signals already scored elsewhere in this file —
  // scoring both would double-count the same fire: 🌉Z1G4🟡→seq_z1gt4, 🧲Z9HL🟡→seq_z9hl,
  // 🌉v2🟡→seq_v2, 🧺SEQ🟡→seq_20, 👑Z1G🟡→seq_crown.
  //
  // ⚠️ x_edges_any above ALSO fires whenever any one of these fires — assign weight to
  // BOTH the bucket and individual codes only if you actually want that fire to count
  // twice (e.g. a small "something fired" bonus on top of the specific edge's own score).
  { divider: true, label: '🥇 EDGE-board, per code (edge_replay.py DISPLAY_SETUPS)' },
  { key: 'x_edge_dualrec', label: '🔄DR dual-reclaim', cls: 'text-sky-300',
    custom: b => (b.edges ?? []).includes('🔄DR') },
  { key: 'x_edge_h1dr', label: '🕐DR 1H-confirmed bottom', cls: 'text-sky-300',
    custom: b => (b.edges ?? []).includes('🕐DR') },
  { key: 'x_edge_cap', label: 'CAP T1-Capit-Bounce', cls: 'text-lime-300',
    custom: b => (b.edges ?? []).includes('CAP') },
  { key: 'x_edge_qzc', label: 'QZC QZ-Capit-Reversal', cls: 'text-lime-300',
    custom: b => (b.edges ?? []).includes('QZC') },
  { key: 'x_edge_dl1', label: 'D+L1 bear-trap reversal', cls: 'text-lime-300',
    custom: b => (b.edges ?? []).includes('D+L1') },
  { key: 'x_edge_g3', label: 'G3 gap reclaim', cls: 'text-emerald-300',
    custom: b => (b.edges ?? []).includes('G3') },
  { key: 'x_edge_g3a', label: '⚡G3-Abs', cls: 'text-emerald-300',
    custom: b => (b.edges ?? []).includes('⚡G3A') },
  { key: 'x_edge_atm', label: 'ATM Atomic edge', cls: 'text-yellow-300',
    custom: b => (b.edges ?? []).includes('ATM') },
  { key: 'x_edge_atmr', label: 'ATMR Atomic-R', cls: 'text-yellow-300',
    custom: b => (b.edges ?? []).includes('ATMR') },
  { key: 'x_edge_spr', label: 'SPR Wyckoff Spring', cls: 'text-teal-300',
    custom: b => (b.edges ?? []).includes('SPR') },
  { key: 'x_edge_z11', label: 'Z11 Z11-T11', cls: 'text-teal-300',
    custom: b => (b.edges ?? []).includes('Z11') },
  { key: 'x_edge_l43', label: 'L43 L43-TRIPLE', cls: 'text-cyan-300',
    custom: b => (b.edges ?? []).includes('L43') },
  { key: 'x_edge_l43_quiet', label: 'L43🔇 quiet-tape (validated tier)', cls: 'text-cyan-200',
    custom: b => (b.edges ?? []).includes('L43🔇') },
  { key: 'x_edge_wsh', label: 'WSH Washout/capitulation', cls: 'text-orange-300',
    custom: b => (b.edges ?? []).includes('WSH') },
  { key: 'x_edge_h1b', label: 'H1B 1H-bottom base', cls: 'text-orange-300',
    custom: b => (b.edges ?? []).includes('H1B') },
  { key: 'x_edge_eng', label: 'ENG Engulf-Absorption', cls: 'text-fuchsia-300',
    custom: b => (b.edges ?? []).includes('ENG') },
  { key: 'x_edge_el46', label: 'EL46 Engulf-L46 (GEM2)', cls: 'text-fuchsia-300',
    custom: b => (b.edges ?? []).includes('EL46') },
  { key: 'x_edge_zrt', label: 'ZRT Zone-Retest', cls: 'text-blue-300',
    custom: b => (b.edges ?? []).includes('ZRT') },
  { key: 'x_edge_zrt_l46', label: 'ZRT🟢 Zone-Retest, L46-quality', cls: 'text-blue-200',
    custom: b => (b.edges ?? []).includes('ZRT🟢') },
  { key: 'x_edge_hb15', label: 'HB15 High-Base 15m-Dip', cls: 'text-purple-300',
    custom: b => (b.edges ?? []).includes('HB15') },
  { key: 'x_edge_rtb', label: 'RTB RTB-Base', cls: 'text-purple-300',
    custom: b => (b.edges ?? []).includes('RTB') },
  { key: 'x_edge_p55', label: 'P55 refined setup', cls: 'text-rose-300',
    custom: b => (b.edges ?? []).includes('P55') },
  { key: 'x_edge_par', label: 'PAR Parabola ride', cls: 'text-rose-300',
    custom: b => (b.edges ?? []).includes('PAR') },
  { key: 'x_edge_conf3', label: '🎯3 Cluster-Bottom ≥3 families', cls: 'text-amber-300',
    custom: b => (b.edges ?? []).includes('🎯3') },
  { key: 'x_edge_conf4', label: '🎯4 Cluster-Bottom ≥4 families', cls: 'text-amber-400',
    custom: b => (b.edges ?? []).includes('🎯4') },
  { key: 'x_edge_g3rl', label: 'G3RL G3 gap-chain variant', cls: 'text-emerald-200',
    custom: b => (b.edges ?? []).includes('G3RL') },
  { key: 'x_edge_g3g3', label: 'G3² G3→G3 gap-chain', cls: 'text-emerald-200',
    custom: b => (b.edges ?? []).includes('G3²') },
  { key: 'x_edge_g3g3rl', label: 'G3²RL🟡 WATCH-tier (day-clustered weaker)', cls: 'text-emerald-100',
    custom: b => (b.edges ?? []).includes('G3²RL🟡') },
  { key: 'x_edge_l34camp', label: '💠L34C L34-camp reversal', cls: 'text-indigo-300',
    custom: b => (b.edges ?? []).includes('💠L34C') },
  { key: 'x_edge_sc46', label: 'SC46', cls: 'text-slate-300',
    custom: b => (b.edges ?? []).includes('SC46') },
  { key: 'x_edge_nssc', label: 'NSSC', cls: 'text-slate-300',
    custom: b => (b.edges ?? []).includes('NSSC') },
  { key: 'x_edge_g3l46', label: 'G3L46 G3+L46 chain', cls: 'text-emerald-200',
    custom: b => (b.edges ?? []).includes('G3L46') },
  { key: 'x_edge_fbt', label: '⚔️FBT fail-bear trap', cls: 'text-red-300',
    custom: b => (b.edges ?? []).includes('⚔️FBT') },
  { key: 'x_edge_zat', label: '💤ZAT Z-Absorb-Turn', cls: 'text-indigo-300',
    custom: b => (b.edges ?? []).includes('💤ZAT') },
  { key: 'x_edge_cf', label: '🧊CF Coil-floor absorption', cls: 'text-sky-300',
    custom: b => (b.edges ?? []).includes('🧊CF') },
  { key: 'x_edge_ear', label: '🌀EAR Engulf-Absorb-Reversal', cls: 'text-violet-300',
    custom: b => (b.edges ?? []).includes('🌀EAR') },
  { key: 'x_edge_t1rs', label: '🎯T1RS T1 + RS dip', cls: 'text-amber-300',
    custom: b => (b.edges ?? []).includes('🎯T1RS') },
  { key: 'x_edge_t3rs', label: '🎯T3RS T3 + RS dip', cls: 'text-amber-300',
    custom: b => (b.edges ?? []).includes('🎯T3RS') },
  { key: 'x_edge_l34cont', label: '🏆L34C L34→L34 continuity', cls: 'text-yellow-300',
    custom: b => (b.edges ?? []).includes('🏆L34C') },
  { key: 'x_edge_svc', label: '🎬SVC stop-volume confirm', cls: 'text-slate-300',
    custom: b => (b.edges ?? []).includes('🎬SVC') },
  { key: 'x_edge_g3_lead', label: '🥇G3 G3 + Leader-in-Laggard', cls: 'text-emerald-300',
    custom: b => (b.edges ?? []).includes('🥇G3') },
  { key: 'x_edge_g3a_lead', label: '🥇G3A G3-Abs + Leader gate', cls: 'text-emerald-300',
    custom: b => (b.edges ?? []).includes('🥇G3A') },
  { key: 'x_edge_l43_lead', label: '🥇L43 L43-TRIPLE + Leader gate', cls: 'text-cyan-300',
    custom: b => (b.edges ?? []).includes('🥇L43') },
  { key: 'x_edge_sand', label: '🥪SAND T2G-Sandwich', cls: 'text-rose-300',
    custom: b => (b.edges ?? []).includes('🥪SAND') },
  { key: 'x_edge_gnb', label: '🪨GNB T1G-NB + RS', cls: 'text-stone-300',
    custom: b => (b.edges ?? []).includes('🪨GNB') },
  { key: 'x_edge_gnb_l34', label: '🪨+L34 T1G-NB + L34 pre-condition', cls: 'text-stone-300',
    custom: b => (b.edges ?? []).includes('🪨+L34') },
  { key: 'x_edge_wsh_iv', label: 'WSH🔎 Washout, intraday demand confirmed', cls: 'text-orange-200',
    custom: b => (b.edges ?? []).includes('WSH🔎') },
  { key: 'x_edge_wsh_vs', label: 'WSH💥 Washout, volume event confirmed', cls: 'text-orange-200',
    custom: b => (b.edges ?? []).includes('WSH💥') },

  { divider: true, label: '🧬 Sequence context (booster/suppressor ending on this bar)' },
  { key: 'x_seqctx_up', label: 'SEQ⤴ booster', cls: 'text-green-300',
    custom: b => b.seq_ctx?.dir === 'up' },
  { key: 'x_seqctx_dn', label: 'SEQ⤵ suppressor', cls: 'text-red-300',
    custom: b => b.seq_ctx?.dir === 'down' },
  { key: 'x_seqctx_tail', label: 'SEQ🎲 tail', cls: 'text-violet-300',
    custom: b => b.seq_ctx?.kind === 'tail' },
  { key: 'x_seqens_fires', label: 'SEQ_ENS any', cls: 'text-purple-300',
    custom: b => b.seq_ens != null },
  { key: 'x_seq34_fires', label: 'SEQ34 any', cls: 'text-indigo-300',
    custom: b => !!b.seq34 },

  { divider: true, label: '🕐 MTF confirmation layer' },
  { key: 'x_mtf_echo_true', label: 'MTF echo confirm', cls: 'text-green-300',
    custom: b => b.mtf_echo === true },
  { key: 'x_mtf_echo_false', label: 'MTF echo ZERO', cls: 'text-red-300',
    custom: b => b.mtf_echo === false },
  { key: 'x_fly_fresh', label: 'FLY fresh (any ✦type)', cls: 'text-pink-300',
    custom: b => !!b.fly_fresh },
  // ✦FLY × type (2026-09-23, user: "✦FLY esec ar aris") — SuperchartPanel.jsx:648-652: the ✦
  // prefix shows ONLY on the first element of b.fly when fly_fresh is true, e.g. "✦FLY-CD".
  // x_fly_fresh above is the "any type, fresh" bucket; these are fresh × the specific type
  // (fly_abcd/fly_cd/fly_bd/fly_ad already exist as type-only flags, type-blind to freshness).
  { key: 'x_fly_fresh_abcd', label: '✦FLY-ABCD', cls: 'text-lime-300',
    custom: b => !!b.fly_fresh && !!b.fly_abcd },
  { key: 'x_fly_fresh_cd', label: '✦FLY-CD', cls: 'text-cyan-300',
    custom: b => !!b.fly_fresh && !!b.fly_cd },
  { key: 'x_fly_fresh_bd', label: '✦FLY-BD', cls: 'text-blue-300',
    custom: b => !!b.fly_fresh && !!b.fly_bd },
  { key: 'x_fly_fresh_ad', label: '✦FLY-AD', cls: 'text-violet-300',
    custom: b => !!b.fly_fresh && !!b.fly_ad },
  { key: 'x_turn_echo', label: 'TURN echo (any ①②③)', cls: 'text-cyan-300',
    custom: b => !!b.turn_echo_n },
  // ①②③ turn-echo tiers (2026-09-23, user asked "② es romeli signalia") — SuperchartPanel.jsx:
  // 2508/2522: turn_echo_n is 0-3, how many of 4H/1H/15M printed the strict REV-turn on D/D-1
  // for a loose daily up-turn. x_turn_echo above is the "any" bucket (≥1); these are the
  // individual tiers, same split as every other collapsed-bucket case above.
  { key: 'x_turn_echo_1', label: '① 1/3 intraday TFs echoed', cls: 'text-cyan-200',
    custom: b => b.turn_echo_n === 1 },
  { key: 'x_turn_echo_2', label: '② 2/3 intraday TFs echoed', cls: 'text-cyan-300',
    custom: b => b.turn_echo_n === 2 },
  { key: 'x_turn_echo_3', label: '③ 3/3 intraday TFs echoed', cls: 'text-cyan-400',
    custom: b => b.turn_echo_n === 3 },

  { divider: true, label: '📐 Oscillator divergence × 🏆RS (validated: BUY/DEEP long, TOP suppressor)' },
  { key: 'x_div_buy', label: 'DIV buy', cls: 'text-lime-300',
    custom: b => !!b.div_buy },
  { key: 'x_div_deep', label: 'DIV deep', cls: 'text-emerald-300',
    custom: b => !!b.div_deep },
  { key: 'x_div_top', label: 'DIV top (suppressor)', cls: 'text-red-400',
    custom: b => !!b.div_top },

  // ▽△ Bottom-Anatomy verdict (AnatRow, backend main.py:4576 via /api/day1h) — Superchart-ONLY,
  // see the caveat next to barsV4 in SuperchartPanel.jsx. Added 2026-09-23 on request ("ki
  // minda" to a screenshot of this exact row). DEFINITION/detector, not a pre-validated edge —
  // project_bottom_anatomy_mtf.md found MTF intraday echo itself has no tradeable lift; scoring
  // it here is the user's own call, same as any other descriptive signal in this file.
  { divider: true, label: '▽△ Bottom-Anatomy verdict (Superchart-only, no Ultra equivalent)' },
  { key: 'x_anat_rev_rs', label: '🔻💪 precise bottom (rev+RS)', cls: 'text-amber-300',
    custom: b => b.anat_v === 'rev' && !!b.anat_rs },
  { key: 'x_anat_rev_norm', label: '🔻 structural bottom (rev, no RS)', cls: 'text-orange-400',
    custom: b => b.anat_v === 'rev' && !b.anat_rs },
  { key: 'x_anat_shake', label: '🌀 shakeout/spring', cls: 'text-violet-300',
    custom: b => b.anat_v === 'shake' },
  { key: 'x_anat_cont', label: '🔺 continuation (markup)', cls: 'text-green-300',
    custom: b => b.anat_v === 'cont' },

  // ⚛ PHYS regime × U/D — the ONLY place in the app that visually GLUES two independently-
  // scored SIG_GROUPS atoms into one chip (SuperchartPanel.jsx:581/586: `phys_r + '·' +
  // phys_regime`, e.g. the chip "RA·D"). _ph_ra/_ph_rn/_ph_rf and _ph_u/_ph_d already score
  // the two HALVES separately — this section is for the COMBINATION itself, so the user can
  // (2026-09-23, user's own example) set _ph_ra=0 and _ph_d=0 but still give RA·D its own
  // weight, without RA-alone or D-alone ever contributing points on a bar where only one of
  // the two fired. Searched the rest of SuperchartPanel.jsx's ROWS for the same '+joiner+'
  // pattern (grep on '·') — this is the only row that does it; the L-code family (l34/l43/
  // fri34/…), the GOG grid, and the ★ combo section at the top of SIG_GROUPS are ALSO
  // "composite, scored as their own thing" but each already has its OWN dedicated key, so
  // there was nothing left to add for them. If another chip like this turns up, say which one
  // and it gets the same treatment.
  { divider: true, label: '⚛ PHYS regime combos (RA/RN/RF × U/D) — scored separately from RA/RN/RF and U/D alone' },
  { key: 'x_phys_ra_u', label: 'RA·U', cls: 'text-sky-300',
    custom: b => b.phys_r === 'RA' && b.phys_regime === 'U' },
  { key: 'x_phys_ra_d', label: 'RA·D', cls: 'text-sky-400',
    custom: b => b.phys_r === 'RA' && b.phys_regime === 'D' },
  { key: 'x_phys_rn_u', label: 'RN·U', cls: 'text-slate-300',
    custom: b => b.phys_r === 'RN' && b.phys_regime === 'U' },
  { key: 'x_phys_rn_d', label: 'RN·D', cls: 'text-slate-400',
    custom: b => b.phys_r === 'RN' && b.phys_regime === 'D' },
  { key: 'x_phys_rf_u', label: 'RF·U', cls: 'text-orange-300',
    custom: b => b.phys_r === 'RF' && b.phys_regime === 'U' },
  { key: 'x_phys_rf_d', label: 'RF·D', cls: 'text-orange-400',
    custom: b => b.phys_r === 'RF' && b.phys_regime === 'D' },

  // ⚛ PHYS energy tier × ★ (2026-09-23, user asked "E1★ es?"). phys_e has 3 tiers (E0/E1/E2,
  // studio/bar_physics.py:207) each optionally suffixed '★' ("released": range expanded past
  // ATR baseline after the spring charged) — 6 real combinations. The two EXISTING keys don't
  // cleanly cover them: _ph_e2 (startsWith 'E2') fires on E2 AND E2★ alike; _ph_es (includes
  // '★') fires on E0★/E1★/E2★ alike. Net effect found while answering the question: E0 and E1
  // WITHOUT a star score nothing at all (neither key matches), while E2★ scores TWICE (both
  // keys fire on the same bar) — not by design, just how two overlapping buckets happened to
  // land. These 6 are clean and non-overlapping; _ph_e2/_ph_es are left as they were (broader,
  // coarser buckets) — zero them out if only the granular tier×star combos should count.
  { divider: true, label: '⚛ PHYS energy tier × ★ (E0/E1/E2 × released) — non-overlapping, unlike _ph_e2/_ph_es' },
  { key: 'x_phys_e0', label: 'E0 (no ★)', cls: 'text-zinc-400',
    custom: b => b.phys_e === 'E0' },
  { key: 'x_phys_e1', label: 'E1 (no ★)', cls: 'text-zinc-300',
    custom: b => b.phys_e === 'E1' },
  { key: 'x_phys_e2', label: 'E2 (no ★)', cls: 'text-amber-300',
    custom: b => b.phys_e === 'E2' },
  { key: 'x_phys_e0_star', label: 'E0★ released', cls: 'text-violet-200',
    custom: b => b.phys_e === 'E0★' },
  { key: 'x_phys_e1_star', label: 'E1★ released', cls: 'text-violet-300',
    custom: b => b.phys_e === 'E1★' },
  { key: 'x_phys_e2_star', label: 'E2★ released', cls: 'text-violet-400',
    custom: b => b.phys_e === 'E2★' },

  // ═══ 2026-09-23 sweep: user asked to find EVERY other place a single chip glues 2+
  // independent fields together (same shape as RA·D and E1★ above) and fix each the same
  // way. Below are the confirmed real findings — each verified against the exact getSigs/
  // sigTitle logic in SuperchartPanel.jsx before being added, not guessed from the chip text.

  // ▭ Body+Wick shape (bar_body_wick = body-class{X/S/M} + wick-class{TB/BB/J/F} glued with
  // no separator, e.g. "XBB"). Ultra's SIG_GROUPS already has _bw_x/_bw_m/_bw_s/_bw_j/_bw_tb/
  // _bw_bb/_bw_f/_bw_xf/_bw_mf — but those read `tz_wlnbb_bar_body_wick` (Ultra's aliased
  // scan-row field), NOT `bar_body_wick` (Superchart's /api/bar_signals field, confirmed at
  // SuperchartPanel.jsx:491). Same field, different name on each surface — the SAME known
  // alias gap already documented for bias_up/vol_spike_*/etc (see the caveat next to barsV4).
  // These mirror _bw_* exactly but read the Superchart-side name, so THIS family scores
  // correctly on Superchart even though the underlying alias gap itself stays unfixed.
  { divider: true, label: '▭ Body+Wick (bar_body_wick) — Superchart-side mirror of _bw_* (Ultra reads a differently-named field)' },
  { key: 'x_bw2_x', label: 'X body (expanded)', cls: 'text-red-300',
    custom: b => (b.bar_body_wick || '').startsWith('X') },
  { key: 'x_bw2_m', label: 'M body (minimal)', cls: 'text-slate-300',
    custom: b => (b.bar_body_wick || '').startsWith('M') },
  { key: 'x_bw2_s', label: 'S body (normal)', cls: 'text-slate-300',
    custom: b => (b.bar_body_wick || '').startsWith('S') },
  { key: 'x_bw2_j', label: 'J wick (doji)', cls: 'text-amber-300',
    custom: b => (b.bar_body_wick || '').includes('J') },
  { key: 'x_bw2_tb', label: 'TB wick (upper)', cls: 'text-cyan-300',
    custom: b => (b.bar_body_wick || '').includes('TB') },
  { key: 'x_bw2_bb', label: 'BB wick (lower, "the hammer")', cls: 'text-emerald-300',
    custom: b => (b.bar_body_wick || '').includes('BB') },
  { key: 'x_bw2_f', label: 'F wick (flat)', cls: 'text-zinc-300',
    custom: b => (b.bar_body_wick || '').includes('F') },
  { key: 'x_bw2_xf', label: 'XF (expanded + flat)', cls: 'text-orange-300',
    custom: b => b.bar_body_wick === 'XF' },
  { key: 'x_bw2_mf', label: 'MF (minimal + flat)', cls: 'text-orange-200',
    custom: b => b.bar_body_wick === 'MF' },
  // Full exact-match grid (2026-09-23, user asked about "XBB" specifically) — body×wick is a
  // plain concatenation (studio/enricher.py:150, bar_body_wick = body_class + wick_class), so
  // every one of the 15 real strings gets its own key, same treatment as VOL7's M×σ grid.
  // x_bw2_x/m/s/j/tb/bb/f above are MARGINALS (startsWith/includes) — a bar with body_wick
  // "XBB" already fires x_bw2_x AND x_bw2_bb; these exact-match keys are additive on top,
  // same double-count note as everywhere else in this file.
  { key: 'x_bw_x_bare', label: 'X (expanded, no strong wick)', cls: 'text-red-200',
    custom: b => b.bar_body_wick === 'X' },
  { key: 'x_bw_s_bare', label: 'S (normal, no strong wick)', cls: 'text-slate-300',
    custom: b => b.bar_body_wick === 'S' },
  { key: 'x_bw_m_bare', label: 'M (minimal, no strong wick)', cls: 'text-slate-400',
    custom: b => b.bar_body_wick === 'M' },
  { key: 'x_bw_x_j', label: 'XJ (expanded + doji)', cls: 'text-amber-300',
    custom: b => b.bar_body_wick === 'XJ' },
  { key: 'x_bw_x_tb', label: 'XTB (expanded + upper wick)', cls: 'text-cyan-300',
    custom: b => b.bar_body_wick === 'XTB' },
  { key: 'x_bw_x_bb', label: 'XBB (expanded + lower wick, "the hammer")', cls: 'text-emerald-400 font-bold',
    custom: b => b.bar_body_wick === 'XBB' },
  { key: 'x_bw_s_j', label: 'SJ (normal + doji)', cls: 'text-amber-200',
    custom: b => b.bar_body_wick === 'SJ' },
  { key: 'x_bw_s_tb', label: 'STB (normal + upper wick)', cls: 'text-cyan-200',
    custom: b => b.bar_body_wick === 'STB' },
  { key: 'x_bw_s_bb', label: 'SBB (normal + lower wick)', cls: 'text-emerald-200',
    custom: b => b.bar_body_wick === 'SBB' },
  { key: 'x_bw_s_f', label: 'SF (normal + flat)', cls: 'text-zinc-200',
    custom: b => b.bar_body_wick === 'SF' },
  { key: 'x_bw_m_j', label: 'MJ (minimal + doji)', cls: 'text-amber-100',
    custom: b => b.bar_body_wick === 'MJ' },
  { key: 'x_bw_m_tb', label: 'MTB (minimal + upper wick)', cls: 'text-cyan-100',
    custom: b => b.bar_body_wick === 'MTB' },
  { key: 'x_bw_m_bb', label: 'MBB (minimal + lower wick)', cls: 'text-emerald-100',
    custom: b => b.bar_body_wick === 'MBB' },

  // 🧬 SEQ34 — the chip `🧬${coarse?'°':''}${dsr>=0.6?'🏆':''}${win}` (SuperchartPanel.jsx:694)
  // glues THREE facts: exact-vs-coarse match, the DSR≥0.6 "fully-trustable" tier (row's own
  // comment: "🏆 = DSR≥0.6 selection-proof (the only fully-trustable tier)"), and the win-rate.
  // x_seq34_fires (above) only knows "a SEQ34 fired" — a 🏆-tier fire and an ordinary
  // sub-threshold fire score identically today. These split it out.
  { divider: true, label: '🧬 SEQ34 tier (DSR / coarse) — x_seq34_fires above does not distinguish these' },
  { key: 'x_seq34_gold', label: '🧬🏆 DSR≥0.6 selection-proof', cls: 'text-yellow-300',
    custom: b => b.seq34 && (b.seq34.dsr ?? 0) >= 0.6 },
  { key: 'x_seq34_coarse', label: '🧬° coarse match (no-L token)', cls: 'text-slate-300',
    custom: b => b.seq34?.coarse === true },
  { key: 'x_seq34_exact', label: '🧬 exact match', cls: 'text-emerald-300',
    custom: b => !!b.seq34 && b.seq34.coarse !== true },

  // ▭ L34 — the chip `·L34${sup[grade]}` (SuperchartPanel.jsx:421) glues the 0–3 grade ladder
  // (row comment: "validated ladder: grade5 ps +3.46% vs grade0 −0.48%") with red/green body
  // color (chipCls, close<open = red = "the type that carries the edge; green L34 on reversal
  // bars is the trap"), and separately the 💠 TRIPLE combo (red + rev_buy + h4_rev_today,
  // +2.05%/win47/PF1.29 — the SAME boolean the app itself computes at line 2736, confirmed
  // before reusing it here). Only a flat "L34 fired" existed before (_wl_l34 / l34).
  { divider: true, label: '▭ L34 grade × color × TRIPLE — _wl_l34/l34 above only knows "L34 fired", not which of these' },
  { key: 'x_l34_red', label: 'red L34 (absorption line)', cls: 'text-amber-300',
    custom: b => b.l_sig === 'L34' && Number(b.close) < Number(b.open) },
  { key: 'x_l34_green', label: 'green L34 (trap type)', cls: 'text-red-400',
    custom: b => b.l_sig === 'L34' && !(Number(b.close) < Number(b.open)) },
  { key: 'x_l34_grade2plus', label: 'red L34, grade ≥2/3', cls: 'text-amber-400',
    custom: b => b.l_sig === 'L34' && Number(b.close) < Number(b.open) && (b.l34_grade ?? 0) >= 2 },
  { key: 'x_l34_grade3', label: 'red L34, grade 3/3 (top of ladder)', cls: 'text-amber-500',
    custom: b => b.l_sig === 'L34' && Number(b.close) < Number(b.open) && (b.l34_grade ?? 0) >= 3 },
  { key: 'x_l34_triple', label: '💠 TRIPLE (red-L34 + 🟢REV + ▲4H)', cls: 'text-amber-400 font-bold',
    custom: b => b.l_sig === 'L34' && Number(b.close) < Number(b.open) && !!b.rev_buy && !!b.h4_rev_today },

  // 🥇 EDGE row "premium" gating (SuperchartPanel.jsx:666/674): QZC/D+L1/RTB/P55 get amber
  // "premium combo" styling when same-bar rev_buy fires and mtf_echo hasn't gone to false —
  // this exact boolean is ALREADY a CSV column (EDGE_GOLD) but was never a v4 key, since
  // EDGE_GOLD only exists as a derived CSV cell, not a field on the bar object `b`.
  { divider: true, label: '🥇 EDGE premium-combo gate (same boolean as the CSV EDGE_GOLD column)' },
  { key: 'x_edge_premium_rev', label: 'EDGE🟢 premium (QZC/D+L1/RTB/P55 + REV-confirmed)', cls: 'text-amber-400 font-bold',
    custom: b => !!(b.rev_buy && b.mtf_echo !== false && (b.edges ?? []).some(c => ['QZC', 'D+L1', 'RTB', 'P55'].includes(c))) },

  // VOL7 — vol7_label = "M{mr}·σ{sg}" (mr, sg each 0-6, SuperchartPanel.jsx:1195). Only M0/M5/M6
  // had keys before (M1-M4 scored nothing) and no σ-level had a key at all — only the two
  // DERIVED divergence flags (Σ+ = sigma≥mr+2, MR+ = mr≥sigma+2) were scored. LADDER_V1
  // (project_ladder_v1 memory) validated M6 specifically as a VETO (−11.97/−12.11 day-
  // clustered) — that finding is about vol7_m6, already in v4Weights.js; these fill the
  // remaining tiers with neutral 0s so nothing is silently unscoreable.
  { divider: true, label: '🔊 VOL7 remaining median-ratio + sigma tiers (M1-M4, σ0-σ6 — previously unscored)' },
  { key: 'x_vol7_m1', label: 'M1', cls: 'text-slate-300',
    custom: b => b.vol7_mr === 1 },
  { key: 'x_vol7_m2', label: 'M2', cls: 'text-slate-300',
    custom: b => b.vol7_mr === 2 },
  { key: 'x_vol7_m3', label: 'M3', cls: 'text-slate-300',
    custom: b => b.vol7_mr === 3 },
  { key: 'x_vol7_m4', label: 'M4', cls: 'text-yellow-200',
    custom: b => b.vol7_mr === 4 },
  { key: 'x_vol7_sg0', label: 'σ0', cls: 'text-slate-400',
    custom: b => b.vol7_sg === 0 },
  { key: 'x_vol7_sg1', label: 'σ1', cls: 'text-slate-300',
    custom: b => b.vol7_sg === 1 },
  { key: 'x_vol7_sg2', label: 'σ2', cls: 'text-slate-300',
    custom: b => b.vol7_sg === 2 },
  { key: 'x_vol7_sg3', label: 'σ3', cls: 'text-slate-200',
    custom: b => b.vol7_sg === 3 },
  { key: 'x_vol7_sg4', label: 'σ4', cls: 'text-yellow-200',
    custom: b => b.vol7_sg === 4 },
  { key: 'x_vol7_sg5', label: 'σ5 (+1σ..+2σ)', cls: 'text-orange-300',
    custom: b => b.vol7_sg === 5 },
  { key: 'x_vol7_sg6', label: 'σ6 (≥+2σ)', cls: 'text-red-300',
    custom: b => b.vol7_sg === 6 },

  // 🔷 SHAPE code × direction — shape_label glues shape_code (MTH/CL4/MID/EXP/CON/LST/WRP)
  // with the engulfed-body direction (shape_dir_up/shape_dir_dn, gated by shape_can_dir).
  // Only ⛔LST↑ (the one cell with two-window VETO evidence) was ever split out; the other
  // 13 real code×direction combos collapsed into the direction-blind shape_code boolean.
  // Descriptive family (0 BUILD per project_shapectx_display) — filled for completeness, all
  // start at 0 same as everything else here.
  { divider: true, label: '🔷 SHAPE code × direction (only LST↑ had its own key before — 13 more combos)' },
  { key: 'x_shape_mth_up', label: 'MTH↑', cls: 'text-sky-300',
    custom: b => b.shape_code === 'MTH' && !!b.shape_dir_up },
  { key: 'x_shape_mth_dn', label: 'MTH↓', cls: 'text-sky-400',
    custom: b => b.shape_code === 'MTH' && !!b.shape_dir_dn },
  { key: 'x_shape_cl4_up', label: 'CL4↑', cls: 'text-teal-300',
    custom: b => b.shape_code === 'CL4' && !!b.shape_dir_up },
  { key: 'x_shape_cl4_dn', label: 'CL4↓', cls: 'text-teal-400',
    custom: b => b.shape_code === 'CL4' && !!b.shape_dir_dn },
  { key: 'x_shape_mid_up', label: 'MID↑', cls: 'text-indigo-300',
    custom: b => b.shape_code === 'MID' && !!b.shape_dir_up },
  { key: 'x_shape_mid_dn', label: 'MID↓', cls: 'text-indigo-400',
    custom: b => b.shape_code === 'MID' && !!b.shape_dir_dn },
  { key: 'x_shape_exp_up', label: 'EXP↑', cls: 'text-violet-300',
    custom: b => b.shape_code === 'EXP' && !!b.shape_dir_up },
  { key: 'x_shape_exp_dn', label: 'EXP↓', cls: 'text-violet-400',
    custom: b => b.shape_code === 'EXP' && !!b.shape_dir_dn },
  { key: 'x_shape_con_up', label: 'CON↑', cls: 'text-fuchsia-300',
    custom: b => b.shape_code === 'CON' && !!b.shape_dir_up },
  { key: 'x_shape_con_dn', label: 'CON↓', cls: 'text-fuchsia-400',
    custom: b => b.shape_code === 'CON' && !!b.shape_dir_dn },
  { key: 'x_shape_lst_dn', label: 'LST↓', cls: 'text-rose-400',
    custom: b => b.shape_code === 'LST' && !!b.shape_dir_dn },
  { key: 'x_shape_wrp_up', label: 'WRP↑', cls: 'text-lime-300',
    custom: b => b.shape_code === 'WRP' && !!b.shape_dir_up },
  { key: 'x_shape_wrp_dn', label: 'WRP↓', cls: 'text-lime-400',
    custom: b => b.shape_code === 'WRP' && !!b.shape_dir_dn },

  // ⚠️ Checked but NOT changed: G3/🥇G3, G3A/🥇G3A, L43/🥇L43 CAN legitimately co-fire in the
  // SAME b.edges array — confirmed in backend/edge_replay.py:848 (`E_g3_lead = E_g3 &
  // lead_in_lag`, a strict AND) — unlike phys_e, this overlap is BY DESIGN: 🥇G3 is a
  // refinement of G3 ("G3, and it's also a leader stock"), not an accident of two unrelated
  // buckets. x_edge_g3 and x_edge_g3_lead intentionally both fire on a leader-gated G3 bar —
  // give x_edge_g3_lead extra points as a bonus ON TOP of x_edge_g3's own score, or zero
  // x_edge_g3 out if you only want the gated version to count. Same for G3A/L43 pairs.
  // 🎯3/🎯4 and WSH/WSH🔎/WSH💥 were also checked (edge_replay.py:1740 _collapse_codes) — the
  // backend already strips the lower tier before either reaches `edges`, so x_edge_conf3/
  // conf4 and the WSH family do NOT double-fire; no action needed there.

  // 🔊 VOL7 full mr × sg grid (2026-09-23, user asked specifically about M4·σ6). The M1-M4/
  // σ0-σ6 entries above are MARGINAL tiers — mr and sg scored independently, so a bar with
  // both M4 and σ6 sums BOTH keys (5+5=10 in the screenshot the user showed), not one
  // "M4·σ6" fire. This is the full 7×7=49 cross so any SPECIFIC pair can be its own weight,
  // same treatment as the PHYS regime/energy grids above. Zero out x_vol7_m4/x_vol7_sg6
  // (the marginals) if only the exact-pair combo should count.
  { divider: true, label: '🔊 VOL7 M{mr}·σ{sg} — full grid, every specific pair its own key' },
  { key: 'x_vol7_m0_sg0', label: 'M0·σ0', cls: 'text-gray-500', custom: b => b.vol7_mr === 0 && b.vol7_sg === 0 },
  { key: 'x_vol7_m0_sg1', label: 'M0·σ1', cls: 'text-gray-500', custom: b => b.vol7_mr === 0 && b.vol7_sg === 1 },
  { key: 'x_vol7_m0_sg2', label: 'M0·σ2', cls: 'text-gray-500', custom: b => b.vol7_mr === 0 && b.vol7_sg === 2 },
  { key: 'x_vol7_m0_sg3', label: 'M0·σ3', cls: 'text-gray-500', custom: b => b.vol7_mr === 0 && b.vol7_sg === 3 },
  { key: 'x_vol7_m0_sg4', label: 'M0·σ4', cls: 'text-gray-500', custom: b => b.vol7_mr === 0 && b.vol7_sg === 4 },
  { key: 'x_vol7_m0_sg5', label: 'M0·σ5', cls: 'text-gray-500', custom: b => b.vol7_mr === 0 && b.vol7_sg === 5 },
  { key: 'x_vol7_m0_sg6', label: 'M0·σ6', cls: 'text-gray-500', custom: b => b.vol7_mr === 0 && b.vol7_sg === 6 },
  { key: 'x_vol7_m1_sg0', label: 'M1·σ0', cls: 'text-slate-400', custom: b => b.vol7_mr === 1 && b.vol7_sg === 0 },
  { key: 'x_vol7_m1_sg1', label: 'M1·σ1', cls: 'text-slate-400', custom: b => b.vol7_mr === 1 && b.vol7_sg === 1 },
  { key: 'x_vol7_m1_sg2', label: 'M1·σ2', cls: 'text-slate-400', custom: b => b.vol7_mr === 1 && b.vol7_sg === 2 },
  { key: 'x_vol7_m1_sg3', label: 'M1·σ3', cls: 'text-slate-400', custom: b => b.vol7_mr === 1 && b.vol7_sg === 3 },
  { key: 'x_vol7_m1_sg4', label: 'M1·σ4', cls: 'text-slate-400', custom: b => b.vol7_mr === 1 && b.vol7_sg === 4 },
  { key: 'x_vol7_m1_sg5', label: 'M1·σ5', cls: 'text-slate-400', custom: b => b.vol7_mr === 1 && b.vol7_sg === 5 },
  { key: 'x_vol7_m1_sg6', label: 'M1·σ6', cls: 'text-slate-400', custom: b => b.vol7_mr === 1 && b.vol7_sg === 6 },
  { key: 'x_vol7_m2_sg0', label: 'M2·σ0', cls: 'text-slate-300', custom: b => b.vol7_mr === 2 && b.vol7_sg === 0 },
  { key: 'x_vol7_m2_sg1', label: 'M2·σ1', cls: 'text-slate-300', custom: b => b.vol7_mr === 2 && b.vol7_sg === 1 },
  { key: 'x_vol7_m2_sg2', label: 'M2·σ2', cls: 'text-slate-300', custom: b => b.vol7_mr === 2 && b.vol7_sg === 2 },
  { key: 'x_vol7_m2_sg3', label: 'M2·σ3', cls: 'text-slate-300', custom: b => b.vol7_mr === 2 && b.vol7_sg === 3 },
  { key: 'x_vol7_m2_sg4', label: 'M2·σ4', cls: 'text-slate-300', custom: b => b.vol7_mr === 2 && b.vol7_sg === 4 },
  { key: 'x_vol7_m2_sg5', label: 'M2·σ5', cls: 'text-slate-300', custom: b => b.vol7_mr === 2 && b.vol7_sg === 5 },
  { key: 'x_vol7_m2_sg6', label: 'M2·σ6', cls: 'text-slate-300', custom: b => b.vol7_mr === 2 && b.vol7_sg === 6 },
  { key: 'x_vol7_m3_sg0', label: 'M3·σ0', cls: 'text-zinc-300', custom: b => b.vol7_mr === 3 && b.vol7_sg === 0 },
  { key: 'x_vol7_m3_sg1', label: 'M3·σ1', cls: 'text-zinc-300', custom: b => b.vol7_mr === 3 && b.vol7_sg === 1 },
  { key: 'x_vol7_m3_sg2', label: 'M3·σ2', cls: 'text-zinc-300', custom: b => b.vol7_mr === 3 && b.vol7_sg === 2 },
  { key: 'x_vol7_m3_sg3', label: 'M3·σ3', cls: 'text-zinc-300', custom: b => b.vol7_mr === 3 && b.vol7_sg === 3 },
  { key: 'x_vol7_m3_sg4', label: 'M3·σ4', cls: 'text-zinc-300', custom: b => b.vol7_mr === 3 && b.vol7_sg === 4 },
  { key: 'x_vol7_m3_sg5', label: 'M3·σ5', cls: 'text-zinc-300', custom: b => b.vol7_mr === 3 && b.vol7_sg === 5 },
  { key: 'x_vol7_m3_sg6', label: 'M3·σ6', cls: 'text-zinc-300', custom: b => b.vol7_mr === 3 && b.vol7_sg === 6 },
  { key: 'x_vol7_m4_sg0', label: 'M4·σ0', cls: 'text-yellow-200', custom: b => b.vol7_mr === 4 && b.vol7_sg === 0 },
  { key: 'x_vol7_m4_sg1', label: 'M4·σ1', cls: 'text-yellow-200', custom: b => b.vol7_mr === 4 && b.vol7_sg === 1 },
  { key: 'x_vol7_m4_sg2', label: 'M4·σ2', cls: 'text-yellow-200', custom: b => b.vol7_mr === 4 && b.vol7_sg === 2 },
  { key: 'x_vol7_m4_sg3', label: 'M4·σ3', cls: 'text-yellow-200', custom: b => b.vol7_mr === 4 && b.vol7_sg === 3 },
  { key: 'x_vol7_m4_sg4', label: 'M4·σ4', cls: 'text-yellow-200', custom: b => b.vol7_mr === 4 && b.vol7_sg === 4 },
  { key: 'x_vol7_m4_sg5', label: 'M4·σ5', cls: 'text-yellow-300', custom: b => b.vol7_mr === 4 && b.vol7_sg === 5 },
  { key: 'x_vol7_m4_sg6', label: 'M4·σ6', cls: 'text-yellow-400', custom: b => b.vol7_mr === 4 && b.vol7_sg === 6 },
  { key: 'x_vol7_m5_sg0', label: 'M5·σ0', cls: 'text-orange-200', custom: b => b.vol7_mr === 5 && b.vol7_sg === 0 },
  { key: 'x_vol7_m5_sg1', label: 'M5·σ1', cls: 'text-orange-200', custom: b => b.vol7_mr === 5 && b.vol7_sg === 1 },
  { key: 'x_vol7_m5_sg2', label: 'M5·σ2', cls: 'text-orange-200', custom: b => b.vol7_mr === 5 && b.vol7_sg === 2 },
  { key: 'x_vol7_m5_sg3', label: 'M5·σ3', cls: 'text-orange-200', custom: b => b.vol7_mr === 5 && b.vol7_sg === 3 },
  { key: 'x_vol7_m5_sg4', label: 'M5·σ4', cls: 'text-orange-200', custom: b => b.vol7_mr === 5 && b.vol7_sg === 4 },
  { key: 'x_vol7_m5_sg5', label: 'M5·σ5', cls: 'text-orange-300', custom: b => b.vol7_mr === 5 && b.vol7_sg === 5 },
  { key: 'x_vol7_m5_sg6', label: 'M5·σ6', cls: 'text-orange-400', custom: b => b.vol7_mr === 5 && b.vol7_sg === 6 },
  // M6 = the script's VB tier — LADDER_V1 measured plain M6 as a VETO (−11.97/−12.11 day-
  // clustered); these split it further by simultaneous σ level.
  { key: 'x_vol7_m6_sg0', label: 'M6·σ0', cls: 'text-red-200', custom: b => b.vol7_mr === 6 && b.vol7_sg === 0 },
  { key: 'x_vol7_m6_sg1', label: 'M6·σ1', cls: 'text-red-200', custom: b => b.vol7_mr === 6 && b.vol7_sg === 1 },
  { key: 'x_vol7_m6_sg2', label: 'M6·σ2', cls: 'text-red-200', custom: b => b.vol7_mr === 6 && b.vol7_sg === 2 },
  { key: 'x_vol7_m6_sg3', label: 'M6·σ3', cls: 'text-red-200', custom: b => b.vol7_mr === 6 && b.vol7_sg === 3 },
  { key: 'x_vol7_m6_sg4', label: 'M6·σ4', cls: 'text-red-200', custom: b => b.vol7_mr === 6 && b.vol7_sg === 4 },
  { key: 'x_vol7_m6_sg5', label: 'M6·σ5', cls: 'text-red-300', custom: b => b.vol7_mr === 6 && b.vol7_sg === 5 },
  { key: 'x_vol7_m6_sg6', label: 'M6·σ6', cls: 'text-red-400', custom: b => b.vol7_mr === 6 && b.vol7_sg === 6 },

  // ⚛ PHYS K (Hooke stretch) × direction — missed in the 2026-09-23 sweep, found by the user
  // asking "K2U es signali?". Same shape as phys_r/phys_e: studio/bar_physics.py:218 appends
  // 'U'/'D' to K1/K2 (K0 never gets a direction — it's the inside-elastic-zone default).
  // _ph_k1/_ph_k2 (startsWith) fire on EITHER direction alike — these split it cleanly.
  { divider: true, label: '⚛ PHYS K-stretch × direction (K1/K2 × U/D) — _ph_k1/_ph_k2 above do not distinguish these' },
  { key: 'x_phys_k1_u', label: 'K1U', cls: 'text-orange-200',
    custom: b => b.phys_k === 'K1U' },
  { key: 'x_phys_k1_d', label: 'K1D', cls: 'text-orange-300',
    custom: b => b.phys_k === 'K1D' },
  { key: 'x_phys_k2_u', label: 'K2U (extended above EMA20)', cls: 'text-red-300',
    custom: b => b.phys_k === 'K2U' },
  { key: 'x_phys_k2_d', label: 'K2D (extended below EMA20)', cls: 'text-red-400',
    custom: b => b.phys_k === 'K2D' },

  // ⚛ PHYS S (resonance / EMA-stack alignment) — same shape again: studio/bar_physics.py:227
  // builds phys_s as "S" + align-count(0-3) + U/D, but only the EXTREME tier (S3U/S3D) ever
  // got a key (_ph_s3u/_ph_s3d). S0/S1U/S2U/S1D/S2D fire on real bars (row's own tooltip:
  // "S3 holds for weeks, printed only when it CHANGES" — implying S1/S2 are the more common
  // transitional states) but scored nothing until now.
  { divider: true, label: '⚛ PHYS S-resonance (EMA-stack alignment count × direction) — only S3U/S3D had keys before' },
  { key: 'x_phys_s0', label: 'S0 (flat/no alignment)', cls: 'text-slate-400',
    custom: b => b.phys_s === 'S0' },
  { key: 'x_phys_s1_u', label: 'S1U', cls: 'text-lime-200',
    custom: b => b.phys_s === 'S1U' },
  { key: 'x_phys_s2_u', label: 'S2U', cls: 'text-lime-300',
    custom: b => b.phys_s === 'S2U' },
  { key: 'x_phys_s1_d', label: 'S1D', cls: 'text-rose-200',
    custom: b => b.phys_s === 'S1D' },
  { key: 'x_phys_s2_d', label: 'S2D', cls: 'text-rose-300',
    custom: b => b.phys_s === 'S2D' },

  // ⏱ 4H/1H intraday-leads (2026-09-23, user asked about the "4H⏱" row's hollow △). Validated
  // 2026-07-19, lead4h.py, n=244k matched pairs: entering on the 4H trigger instead of waiting
  // for the daily close beats it by +0.84pp mean, 6/6yr incl. 2022. Used before only as a
  // filter and as one ingredient of x_l34_triple — never scorable on its own until now.
  { divider: true, label: '⏱ 4H/1H intraday-leads (lead4h.py — validated +0.84pp/6-6yr)' },
  { key: 'x_h4_rev_today', label: '▲ 4H REV-trigger (early entry)', cls: 'text-cyan-300 font-bold',
    custom: b => !!b.h4_rev_today },
  { key: 'x_h1_rev_today', label: '△ 1H REV-trigger only (4H silent)', cls: 'text-cyan-400',
    custom: b => !!b.h1_rev_today && !b.h4_rev_today },

  // 🟢🔵⚠️ BUY row (2026-09-23, user asked about the green ball). rev_buy/brk_buy/mtf_score_conf
  // were used before only as INGREDIENTS of other composites (x_l34_triple, x_edge_premium_rev)
  // — never scorable on their own. SuperchartPanel.jsx:2500-2528 is the source of every stat below.
  { divider: true, label: '🟢🔵⚠️ BUY row (rev_buy/brk_buy/mtf_score_conf — validated 6yr path-sim)' },
  { key: 'x_rev_buy_echo', label: '🟢 REV-buy + MTF echo', cls: 'text-green-400 font-bold',
    custom: b => !!b.rev_buy && b.mtf_echo !== false },
  { key: 'x_rev_buy_noecho', label: '⚠️ REV-buy, NO echo (suppressor)', cls: 'text-amber-400',
    custom: b => !!b.rev_buy && b.mtf_echo === false },
  { key: 'x_brk_buy', label: '🔵 BRK-buy', cls: 'text-blue-400',
    custom: b => !!b.brk_buy },
  { key: 'x_mtf_conf_0', label: 'mtf_conf 0/3 (HARD SKIP)', cls: 'text-red-400',
    custom: b => b.mtf_score_conf === 0 },
  { key: 'x_mtf_conf_1', label: 'mtf_conf 1/3', cls: 'text-amber-300',
    custom: b => b.mtf_score_conf === 1 },
  { key: 'x_mtf_conf_2', label: 'mtf_conf 2/3', cls: 'text-lime-300',
    custom: b => b.mtf_score_conf === 2 },
  { key: 'x_mtf_conf_3', label: 'mtf_conf 3/3', cls: 'text-green-300',
    custom: b => b.mtf_score_conf === 3 },

  // ● whisper (2026-09-24, user asked about the teal circle in the "Cat" row). Not a
  // researched/validated edge — a named PATTERN (RGTI-0908, "coil flagged before ignition"):
  // strong bullish seq-context (⤴≥60, era-consistent cell) + an intraday echo (turn-echo, MTF
  // conf, or 4H/1H trigger) on the SAME bar. SuperchartPanel.jsx:2753-2762. Every ingredient
  // was already scorable separately (x_seqctx_up, x_turn_echo, x_mtf_conf_*, x_h4/h1_rev_today)
  // — this is the exact conjunction the Cat row's own display logic uses to light up the ●.
  { divider: true, label: '● whisper (Cat row) — strong seq-context + same-bar intraday echo' },
  { key: 'x_whisper', label: '● whisper', cls: 'text-teal-300 font-bold',
    custom: b => b.seq_ctx?.dir === 'up' && (b.seq_ctx?.up ?? 0) >= 60
      && ((b.turn_echo_n ?? 0) > 0 || (b.mtf_score_conf ?? 0) > 0 || !!b.h4_rev_today || !!b.h1_rev_today) },

  // Cat row's plain profile_category (⭐/↑/⚠) — same row, visible right next to the ● whisper
  // circle in the screenshot. Used only as an Ultra filter before (buildingFilter/watchFilter),
  // never scorable. "Profile score is additive context only — does not replace canonical
  // score" per the app's own UI copy, so this is descriptive, same status as SHAPE/VOL7.
  { divider: true, label: 'Cat row — profile_category (⭐ Sweet Spot / ↑ Building / ⚠ Late)' },
  { key: 'x_profile_sweet_spot', label: '⭐ Sweet Spot', cls: 'text-green-300 font-bold',
    custom: b => b.profile_category === 'SWEET_SPOT' },
  { key: 'x_profile_building', label: '↑ Building', cls: 'text-yellow-400',
    custom: b => b.profile_category === 'BUILDING' },
  { key: 'x_profile_late', label: '⚠ Late', cls: 'text-amber-500',
    custom: b => b.profile_category === 'LATE' },
]
