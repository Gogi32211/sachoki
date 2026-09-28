// TURN·58 — the count gauge from research_out/TURN_SET_V1.md (2026-09-27, user: "gaakete").
//
// On a bar whose low is a 10-bar low (the study's candidate), count how many of the 58 keys that
// were ENRICHED at turns in MINE 2021-23 fired on any of the last 3 bars (t-2..t). The list is
// FROZEN from the sealed study (turnset_mine.json `S`); editing it breaks the link to the evidence.
//
// What the evidence says (read before reading the number):
//   · turn likelihood rises with the count and REPLICATED in 2024-26 at every threshold
//     (lift over an average 10-bar low: ≥20 → 1.34 · ≥24 → 1.37 · ≥28 → 1.48 · ≥30 → 1.41).
//   · as a TRADE (ATR×12 trail) it did NOT beat other 10-bar lows (Δ ≈ 0 for ≥20…30; only ≥32
//     passed, a single-point peak). So this is a DESCRIPTIVE turn-likelihood gauge — never a
//     buy strength, never a score input.
export const TURN58 = [
  'strong_sig', 'abs_sig', 'climb_sig', 'load_sig', 'sq', 'rtv', 'hilo_buy', 'va', 'svs_2809',
  'lbal_half_up_a', 'lvx_l34_v', 'ovdmap_rc60', 'ovdmap_nm', 'vol7_m5', 'vol7_up2', 'vol7_sigma_plus',
  'shape_absorb', 'shape_sweet', 'pv_veto_c', 've_spike', 'tz_any_t', 'tz_t3', 'tz_t5', 'tz_t9',
  '_wl_l3', '_wl_l34', '_cl_a', '_cl_i', '_bw_bb', '_ph_ad', '_ph_gg3', '_gr_g3', '_gr_v', '_vb_b',
  'wt_evr', '_l_any', 'fri64', 'l34', 'l43', 'blue', 'fbo_bull', 'd_blast_bull', 'd_surge_bull',
  'd_div_bull', 'd_vd_div_bull', 'd_flip_bull', '_any_p', 'ctx_lrc', 'fly_abcd', 'x_edge_h1dr',
  'x_edge_atm', 'x_edge_zrt', 'x_edge_conf3', 'x_anat_rev_rs', 'x_phys_e0', 'x_vol7_m4',
  'x_vol7_sg6', 'x_vol7_m4_sg6',
]
const SET58 = new Set(TURN58)

// VERIFY 2024-26 turn lift by count band (TURN_SET_V1 plateau table), for the tooltip only.
export const TURN_LIFT = [[32, 1.35], [30, 1.41], [28, 1.48], [24, 1.37], [20, 1.34]]

/** bars: oldest→newest, each with `low` and `_v4keys` (array of fired catalog keys).
 *  Returns a new array with turn_cand (10-bar low, ≥10 bars of history), turn_n (0-58) and
 *  turn_keys (the matched keys) — same window and candidate rule as the study. */
export function withTurnCount(bars) {
  return bars.map((b, i) => {
    if (i < 9 || b.low == null) return { ...b, turn_cand: false, turn_n: null, turn_keys: [] }
    let lo = Infinity
    for (let j = i - 9; j <= i; j++) lo = Math.min(lo, Number(bars[j].low))
    const cand = Number(b.low) <= lo
    const hit = new Set()
    for (let j = Math.max(0, i - 2); j <= i; j++) {
      for (const k of bars[j]._v4keys || []) if (SET58.has(k)) hit.add(k)
    }
    return { ...b, turn_cand: cand, turn_n: cand ? hit.size : null, turn_keys: cand ? [...hit] : [] }
  })
}
