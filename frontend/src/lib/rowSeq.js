// ⟲ROW-TURN — per-row turn-zone tiers from research_out/ROWSEQ_V1.md (2026-09-28, user: "ki gaakete").
//
// For each signal row, its own 5-bar sequence (t-4..t) is scored with the features that were enriched
// at turn zones in MINE 2021-23: key@lag j, key repeated ≥2×, ordered pair k1 (t-4..t-1) → k2 (t).
// Tier 1 = EARLY (score ≥ MINE q80), tier 2 = CONFIRMED (≥ MINE q95). Features, thresholds and the
// row list are FROZEN in rowSeqSpec.json (exported from the sealed study with a tier-parity assert);
// editing either file breaks the link to the evidence.
//
// What the evidence says (read before reading a chip):
//   · target a (turn zone: 21-bar pivot within ±3 bars, then +3 ATR in 20 bars, pivot holds) —
//     every shown row identifies turn zones beyond price location. Lift after removing
//     "near the 10-bar low" × ATR% strata, VERIFY 2024-26: see ROW_LIFT below.
//   · many rows together add NOTHING over the best single row (COUNT ≥11: 1.51 vs MTF 1.52);
//     only the VOL7 ∧ MTF pair adds (1.88, fires on ~0.4 % of bars).
//   · target b (≥ +30 % under the ATR×12 trail) = NULL: all of it was volatility.
//   · same-day trade return ≈ 0 → DESCRIPTIVE turn-zone gauge, never a buy signal, never a score input.
import SPEC from './rowSeqSpec.json'

export const ROW_ORDER = ['FLY', 'GR', 'MTF', 'PHYS', 'VOL7', 'PV', 'BREAK', 'OVD', 'DELTA']
export const ROW_SHORT = { FLY: 'FLY', GR: 'GR', MTF: 'MTF', PHYS: '⚛', VOL7: 'VOL7', PV: 'PV', BREAK: 'BRK', OVD: 'OVD', DELTA: 'Δ' }
// adjusted turn-zone lift of the CONFIRMED tier (location × ATR% strata, VERIFY 2024-26)
export const ROW_LIFT = { FLY: 1.63, GR: 1.60, MTF: 1.52, PHYS: 1.45, VOL7: 1.43, PV: 1.42, BREAK: 1.41, OVD: 1.41, DELTA: 1.40 }
export const PAIR_LIFT = 1.88

// compile once: per row → lag map (key → Set of lags), rep keys, pairs (k2 → [k1…])
const COMPILED = Object.fromEntries(ROW_ORDER.map(r => {
  const lag = new Map(); const rep = []; const pair = new Map()
  for (const f of SPEC[r].features) {
    if (f[0] === 'lag') { if (!lag.has(f[1])) lag.set(f[1], []); lag.get(f[1]).push(f[2]) }
    else if (f[0] === 'rep') rep.push(f[1])
    else { if (!pair.has(f[2])) pair.set(f[2], []); pair.get(f[2]).push(f[1]) }
  }
  return [r, { lag, rep, pair, early: SPEC[r].early, confirmed: SPEC[r].confirmed }]
}))

// the ⚛ tokens the study built from the raw studio-DB phys_* fields (same strings as physMap)
function physTokens(b) {
  const out = []
  const v = (x) => (x == null ? '' : String(x))
  if (v(b.phys_r) && v(b.phys_regime)) out.push(`⚛rg:${b.phys_r}·${b.phys_regime}`)
  for (const [f, c] of [['phys_m', 'm'], ['phys_e', 'e'], ['phys_k', 'k'], ['phys_c', 'c'], ['phys_h', 'h'], ['phys_s', 's'], ['phys_ad', 'ad']]) {
    if (v(b[f])) out.push(`⚛${c}:${b[f]}`)
  }
  if (v(b.phys_gap_true)) out.push(`⚛gp:g${b.phys_gap_true}`)
  if (v(b.phys_wyc)) out.push(`⚛w:${b.phys_wyc}`)
  return out
}

/** Tiers from per-bar key lists (oldest→newest). keysPerBar[i] = array of fired keys incl. ⚛ tokens.
 *  Returns [{FLY: 0|1|2, …, pair: bool}] — exported separately so the parity test can feed the
 *  research history directly. */
export function rowSeqTiers(keysPerBar) {
  const S = keysPerBar.map(k => new Set(k))
  return S.map((cur, i) => {
    const out = {}
    for (const r of ROW_ORDER) {
      const C = COMPILED[r]; let score = 0
      for (const [k, lags] of C.lag) for (const j of lags) if (i - j >= 0 && S[i - j].has(k)) score++
      for (const k of C.rep) {
        let n = 0
        for (let j = 0; j <= 4 && i - j >= 0; j++) if (S[i - j].has(k)) n++
        if (n >= 2) score++
      }
      for (const [k2, k1s] of C.pair) {
        if (!cur.has(k2)) continue
        for (const k1 of k1s) {
          for (let j = 1; j <= 4 && i - j >= 0; j++) if (S[i - j].has(k1)) { score++; break }
        }
      }
      out[r] = score >= C.confirmed ? 2 : score >= C.early ? 1 : 0
    }
    out.pair = out.VOL7 === 2 && out.MTF === 2
    return out
  })
}

// BO↓ / BX↓ / BE↓: the live bar_signals values disagree with the nightly `bars` values the study
// learned on, so when the endpoint supplies the studio-DB copies (_db_*) those decide these keys.
const DB_DN = [['bo_dn', '_db_bo_dn'], ['bx_dn', '_db_bx_dn'], ['be_dn', '_db_be_dn']]
function keysFor(b) {
  let ks = b._v4keys || []
  if (b._db_bo_dn != null || b._db_bx_dn != null || b._db_be_dn != null) {
    const drop = new Set(['bo_dn', 'bx_dn', 'be_dn', '_be_any'])
    ks = ks.filter(k => !drop.has(k))
    for (const [k, f] of DB_DN) if (b[f]) ks.push(k)
    if (b.be_up || b._db_be_dn) ks.push('_be_any')
  }
  return [...ks, ...physTokens(b)]
}

/** bars: oldest→newest, each with `_v4keys` (fired catalog keys) and the phys_* fields. */
export function withRowSeq(bars) {
  const tiers = rowSeqTiers(bars.map(keysFor))
  return bars.map((b, i) => ({ ...b, rs_tiers: tiers[i] }))
}
