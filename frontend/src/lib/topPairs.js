// TOP·58 — the best turn identifiers among the 58 TURN keys (research_out/TURN58_TOP_V1.md,
// 2026-09-28, user: "damimate eseni oriveshi"). Frozen from the sealed study: 10 singles and 10 pairs
// selected on MINE 2021-23, all 20 PASSED once on VERIFY 2024-26 (all 5,739 US tickers in the DB).
// A key counts as "on" if it fired on any of the last 3 bars (t-2..t), exactly as in the study.
//
// What the evidence says (read before reading a chip):
//   · `lift` = turn-zone likelihood vs a bar with the same price location, ATR% AND recent-low status
//     (10-bar low in t-3..t) — CORRECTED 2026-09-28 (TZL_BOTTOM_SEQ_V1): the first-published lifts
//     (1.3-2.7) leaked the recent low. After the correction only the 🕐DR family (1.2-1.33) keeps a real
//     increment; the rest ≈ 1.0 — they mostly re-state that a low was just printed.
//   · most items IDENTIFY turn zones but do not out-earn the day's other bars (same-day Δ ≈ 0 or
//     negative). Only the 🕐DR family shows a positive same-day return (`sd`, pp, ATR×12 trail) —
//     and that is descriptive: none of these has been path-sim-tested as a setup.
//   → DESCRIPTIVE turn-zone gauge, never a buy signal, never a score input.

export const TOP_SINGLES = [
  { code: 'dr',   key: 'x_edge_h1dr',   label: '🕐DR', lift: 1.21, sd: +2.39 },
  { code: 'fbo',  key: 'fbo_bull',      label: 'FBO↑', lift: 1.01, sd: -0.66 },
  { code: 'rtv',  key: 'rtv',           label: 'RTV',  lift: 0.97, sd: -0.87 },
  { code: 'c3',   key: 'x_edge_conf3',  label: '🎯3',  lift: 1.11, sd: +0.11 },
  { code: 'gg3',  key: '_ph_gg3',       label: 'gG3',  lift: 1.05, sd: -0.36 },
  { code: 'hilo', key: 'hilo_buy',      label: 'HILO↑', lift: 1.01, sd: -0.28 },
  { code: 'zrt',  key: 'x_edge_zrt',    label: 'ZRT',  lift: 1.01, sd: -0.33 },
  { code: 'm4s6', key: 'x_vol7_m4_sg6', label: 'M4·σ6', lift: 1.11, sd: +0.43 },
  { code: 'svs',  key: 'svs_2809',      label: 'SVS',  lift: 1.06, sd: -0.00 },
  { code: 'g3',   key: '_gr_g3',        label: 'G3',   lift: 1.04, sd: +0.35 },
]

export const TOP_PAIRS = [
  { code: 'gg3_dr',   a: '_ph_gg3',     b: 'x_edge_h1dr', label: 'gG3+🕐DR',  lift: 1.33, sd: +3.00 },
  { code: 'rtv_gg3',  a: 'rtv',         b: '_ph_gg3',     label: 'RTV+gG3',   lift: 0.99, sd: -1.17 },
  { code: 'flp_dr',   a: 'd_flip_bull', b: 'x_edge_h1dr', label: 'FLP↑+🕐DR', lift: 1.20, sd: +3.21 },
  { code: 'fbo_p',    a: 'fbo_bull',    b: '_any_p',      label: 'FBO↑+P',    lift: 1.04, sd: +0.07 },
  { code: 'gg3_fbo',  a: '_ph_gg3',     b: 'fbo_bull',    label: 'gG3+FBO↑',  lift: 1.06, sd: +0.56 },
  { code: 'v_fbo',    a: '_gr_v',       b: 'fbo_bull',    label: 'V+FBO↑',    lift: 1.03, sd: -0.71 },
  { code: 'svs_fbo',  a: 'svs_2809',    b: 'fbo_bull',    label: 'SVS+FBO↑',  lift: 1.03, sd: -1.34 },
  { code: 'hilo_gg3', a: 'hilo_buy',    b: '_ph_gg3',     label: 'HILO↑+gG3', lift: 1.03, sd: -0.96 },
  { code: 'gg3_zrt',  a: '_ph_gg3',     b: 'x_edge_zrt',  label: 'gG3+ZRT',   lift: 1.05, sd: +0.28 },
  { code: 'g3_fbo',   a: '_gr_g3',      b: 'fbo_bull',    label: 'G3+FBO↑',   lift: 1.02, sd: +0.10 },
]

/** Per bar: which of the 10 singles / 10 pairs are on over t-2..t.
 *  keysPerBar[i] = array of fired catalog keys (oldest→newest, one ticker). */
export function topPairsFlags(keysPerBar) {
  const S = keysPerBar.map(k => new Set(k))
  return S.map((_, i) => {
    const on = new Set()
    for (let j = Math.max(0, i - 2); j <= i; j++) for (const k of S[j]) on.add(k)
    const singles = TOP_SINGLES.filter(s => on.has(s.key)).map(s => s.code)
    const pairs = TOP_PAIRS.filter(p => on.has(p.a) && on.has(p.b)).map(p => p.code)
    return { singles, pairs }
  })
}

/** bars: oldest→newest, each with `_v4keys` (fired catalog keys). */
export function withTopPairs(bars) {
  const f = topPairsFlags(bars.map(b => b._v4keys || []))
  return bars.map((b, i) => ({ ...b, top58: f[i] }))
}
