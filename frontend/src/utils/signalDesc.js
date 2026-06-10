/**
 * signalDesc.js — human-readable descriptions for the signals shown across the
 * Zone Edge panel (and reusable elsewhere). `descFor(name)` returns a plain-text
 * explanation for ANY signal: an explicit entry when we have one, otherwise a
 * category description derived from the name family — so every signal, even the
 * long tail of the ~220-flag Ultra suite, gets at least a meaningful label.
 *
 * It also understands the two compound forms used in Zone Edge:
 *   "vbo_up@-2"      → a signal on a specific lead-in bar (offset −2)
 *   "vol_bucket=B"   → a categorical bar-code slot at a given value
 */

// ── Explicit descriptions ─────────────────────────────────────────────────────
export const SIGNAL_DESC = {
  // ── Lead-in / move-initiation (the sequence-miner set) ──────────────────────
  vbo_up:        'Volume BreakOut ↑ — price breaks out on a volume expansion.',
  eb_bull:       'Bullish Engulfing Bar — green bar fully engulfs the prior bar (strong buyers).',
  be_up:         'Bullish break / entry trigger (BE↑).',
  fbo_bull:      'Failed-breakdown reversal (bullish) — price faked below, then reclaimed.',
  bo_up:         'Breakout ↑ — close clears a recent range high.',
  bx_up:         'Breakout-extension ↑ — continuation past an existing breakout.',
  prebreak_ready:'Pre-breakout READY — coiling, the earliest pre-breakout tier.',
  prebreak_prime:'Pre-breakout PRIME — primed setup, tighter coil before the break.',
  prebreak_v3:   'Pre-breakout v3 — pre-breakout score tier 3.',
  prebreak_v4:   'Pre-breakout v4 — strongest pre-breakout score tier (≥8 used by the journal).',
  pb_lvbo:       'Low-Volume BreakOut — healthy, non-climactic breakout (quiet, not exhaustion).',
  sig_conso:     'Consolidation — price compressing into a tight range.',
  sq:            'Squeeze — volatility compression (Bollinger inside Keltner) before expansion.',
  sig_abs:       'Absorption (VABS) — sellers being absorbed on volume; demand soaking supply.',
  close_o:       'Atomic: close=O — bar closes WEAK (below the prior body). The dominant 5-yr edge component (+0.3 lift); strongest on spring/bounce lead-ins.',
  gap_up:        'Atomic: gap-up (G2/G3) — bar gapped up. +2–3pp win on spring/bounce lead-ins.',
  r2l_os:        'Atomic: R2L — RSI2 oversold. The "buy oversold weakness" axis (+1–1.4pp).',
  atomic:        'Atomic: weak-close gap-up (close=O + gap). The 5-yr-validated edge; +2–4.6pp win as a spring/bounce lead-in.',
  l34:           'WLNBB L34 — absorption level transition (L-line 3→4), demand stepping in.',
  wyc_spring:    'Wyckoff Spring — false break BELOW support that reverses up (shakeout).',
  wyc_sos:       'Wyckoff Sign Of Strength — strong up-move confirming accumulation.',
  d_absorb_bull: 'Volume-delta absorption (bullish) — buyers absorbing at lows.',
  d_spring:      'Delta spring — spring confirmed by buying delta.',
  w2_spring:     'Wyckoff (schematic) Spring — spring in the v2 Wyckoff model.',
  w2_sos:        'Wyckoff (schematic) Sign Of Strength.',
  d_surge_bull:  'Delta surge (bullish) — a burst of net buying pressure.',
  d_blast_bull:  'Delta blast (bullish) — an outsized buying-delta event.',
  d_strong_bull: 'Strong bullish delta — sustained net buying.',
  d_div_bull:    'Bullish delta divergence — price down but buying delta up (hidden demand).',
  sig_t1g:       'TZ_WLNBB T1G — strongest bullish 2-bar reversal WITH a gap (best flip code).',
  sig_t2g:       'TZ_WLNBB T2G — bullish 2-bar reversal with a gap.',
  sig_t6:        'TZ_WLNBB T6 — bullish CONTINUATION (green engulfs green).',
  sig_buy:       'TZ buy signal — TZ_WLNBB long trigger.',
  is_pivot_low_3:'Swing pivot LOW (3-bar fractal) — local bottom, structure support.',
  is_pivot_low_5:'Swing pivot LOW (5-bar fractal) — stronger local bottom.',
  psar_bull:     'Parabolic SAR bullish — SAR dots flipped below price (uptrend).',
  sig_vol_5x:    'Volume ≥ 5× the 20-day average.',
  sig_vol_10x:   'Volume ≥ 10× the 20-day average.',
  para_prep:     'Parabolic PREP — setup before a parabolic acceleration.',
  para_start:    'Parabolic START — the launch of a parabolic move.',
  rocket:        'Rocket — strong momentum thrust (multi-factor breakout).',
  // ── Common context signals (surface in combos / context-lift) ───────────────
  tz_bull:       'TZ bullish — a bullish T-code is active on this bar.',
  tz_up_next3:   'FLIP — bar is NOT yet bullish but TZ turns bullish within the next 3 bars (the follow-through).',
  at_fib:        'At a Fibonacci level — close within 0.5×ATR of a Fib of the trailing range.',
  eb_bear:       'Bearish Engulfing Bar — red bar engulfs the prior (strong sellers).',
  fbo_bear:      'Failed-breakout (bearish) — faked above, then rejected.',
  is_pivot_high_3:'Swing pivot HIGH (3-bar fractal) — local top, structure resistance.',
  is_pivot_high_5:'Swing pivot HIGH (5-bar fractal) — stronger local top.',
  d_surge_bear:  'Delta surge (bearish) — a burst of net selling pressure.',
  d_blast_bull_red:'Delta blast (bullish) on a red bar — buying into weakness.',
  prebreak_watch:'Pre-breakout WATCH — early coil, on the radar.',
  sig_strong:    'VABS STRONG — strong volume-absorption setup.',
  sig_best:      'VABS BEST — best-grade volume-absorption setup.',
}

// ── Value-level descriptions for categorical bar-code slots ───────────────────
export const VOL_BUCKET_DESC = {
  W: 'Weak volume (below the lower band).', L: 'Low volume.', N: 'Normal volume.',
  B: 'Big volume (above upper band) — controlled, the edge bucket.',
  VB: 'Very Big volume — climactic; often an exhaustion TRAP on retest.',
}
const FLIP_DESC = {
  T1G: 'Flip via T1G — gap reversal, the STRONGEST flip (~64% win).',
  T1:  'Flip via T1 — bullish 2-bar reversal (~57% win).',
  T4:  'Flip via T4 — reversal after a red bar (~53% win).',
  T2G: 'Flip via T2G — gap reversal variant.',
  T9:  'Flip via T9 — weaker reversal (~48%).',
  T3:  'Flip via T3 — weak reversal (~42%).',
  T5:  'Flip via T5 — historically a TRAP (~33% win); avoid.',
}

// ── Field-level descriptions for the bar-code slots ───────────────────────────
const FIELD_DESC = {
  t_sig: 'T-code (TZ_WLNBB bullish 2-bar pattern) on the bar', z_sig: 'Z-code (bearish 2-bar pattern) on the bar',
  l_sig: 'L-line code (WLNBB level)', flip_code: 'which T-code drove the follow-through flip',
  vol_bucket: 'volume class (Bollinger band on volume)', bar_body_wick: 'body-vs-wick shape',
  bar_line5: 'line-5 micro-structure code', bar_range_class: 'range class (N=narrow … wide)',
  bar_gap_class: 'gap class', composite_full_suffix: 'full close-position suffix (EBA/EBO/NDI…)',
  full_suffix: 'close-position suffix', p1_tz: 'bar −1 T-code', p2_tz: 'bar −2 T-code',
  p1_z: 'bar −1 Z-code', p2_z: 'bar −2 Z-code', p1_vol: 'bar −1 volume class', p1_l5: 'bar −1 line-5 code',
  fib_level: 'Fibonacci level the bar sits on',
}

// ── Prefix/family fallback so EVERY signal gets a description ──────────────────
function familyDesc(name) {
  const n = name.toLowerCase()
  const mTZ = n.match(/^(?:sig_|tz_)?t(\d{1,2})g?$/)
  if (mTZ) return `TZ_WLNBB bullish 2-bar pattern T${mTZ[1]}${n.endsWith('g') ? ' (gap variant)' : ''}.`
  const mZ = n.match(/^(?:sig_|tz_)?z(\d{1,2})g?$/)
  if (mZ) return `TZ_WLNBB bearish 2-bar pattern Z${mZ[1]}${n.endsWith('g') ? ' (gap variant)' : ''}.`
  const mL = n.match(/^sig_l(\d+)/)
  if (mL) return `WLNBB L-line level L${mL[1]}.`
  const mEMA = n.match(/^price_(gt|lt)_(\d+)/)
  if (mEMA) return `Price ${mEMA[1] === 'gt' ? 'ABOVE' : 'BELOW'} the ${mEMA[2]}-period EMA (trend filter).`
  const mGT = n.match(/^_?(gt|lt)_ema(\d+)/)
  if (mGT) return `Price ${mGT[1] === 'gt' ? 'above' : 'below'} EMA${mGT[2]}.`
  if (n.startsWith('d_'))        return 'Volume-delta / order-flow signal (Δ engine).'
  if (n.startsWith('w2_') || n.startsWith('wt_') || n.startsWith('wyc_')) return 'Wyckoff structural signal.'
  if (n.startsWith('pb_') || n.startsWith('prebreak')) return 'Pre-breakout (coil → break) signal.'
  if (n.startsWith('gog'))       return 'GOG engine signal.'
  if (n.startsWith('para'))      return 'Parabolic-phase signal.'
  if (n.startsWith('fly'))       return 'ABCD / harmonic (fly) pattern signal.'
  if (n.startsWith('psar'))      return 'Parabolic-SAR trend signal.'
  if (n.startsWith('rsi'))       return 'RSI threshold signal.'
  if (n.includes('pivot'))       return 'Swing-pivot structure signal.'
  if (n.includes('vol'))         return 'Volume-based signal.'
  if (n.startsWith('sig_'))      return 'Engine signal (' + name + ').'
  return name
}

/**
 * descFor(label) → plain-text description for any Zone Edge signal label.
 * Handles "@-k" sequence offsets and "field=value" categoricals.
 */
// ── Styled badges for the lead-in / sequence signals ─────────────────────────
// Pill look: tinted bg + colored border + light text, grouped by signal FAMILY
// so a color tells you the kind of signal at a glance (green=momentum,
// cyan=coil, amber=absorption, teal=delta, violet=T-timing, slate=structure,
// yellow=volume, orange=parabolic).
// Ultra-screener badge palette (SignalChip): solid dark bg + white border + mono.
const _C = {
  green:  'bg-green-900 text-green-300',
  cyan:   'bg-cyan-900 text-cyan-300',
  amber:  'bg-amber-900 text-amber-300',
  teal:   'bg-teal-900 text-teal-300',
  violet: 'bg-violet-900 text-violet-200',
  slate:  'bg-slate-700 text-slate-200',
  yellow: 'bg-yellow-900 text-yellow-200',
  orange: 'bg-orange-900 text-orange-300',
  fuchsia:'bg-fuchsia-900 text-fuchsia-200',
  gray:   'bg-white/5 text-md-on-surface-var',
}
export const FAMILY_LEGEND = [
  ['green', 'momentum / breakout'], ['cyan', 'coil / prebreak'],
  ['amber', 'absorption / Wyckoff'], ['teal', 'delta / order-flow'],
  ['violet', 'T-timing'], ['slate', 'structure / trend'],
  ['yellow', 'volume'], ['orange', 'parabolic'], ['fuchsia', 'atomic (5-yr)'],
]
export const FAMILY_CLS = _C
const _BADGE = {
  // momentum / breakout
  vbo_up: ['VBO↑', 'green'], bo_up: ['BO↑', 'green'], bx_up: ['BX↑', 'green'],
  eb_bull: ['EB↑', 'green'], be_up: ['BE↑', 'green'], fbo_bull: ['FBO↑', 'green'],
  rocket: ['🚀 RKT', 'green'],
  // coil / prebreak
  prebreak_ready: ['PB·R', 'cyan'], prebreak_prime: ['PB·P', 'cyan'],
  prebreak_v3: ['PB3', 'cyan'], prebreak_v4: ['PB4', 'cyan'],
  pb_lvbo: ['LVBO', 'cyan'], sig_conso: ['CONSO', 'cyan'], sq: ['SQ', 'cyan'],
  // absorption / Wyckoff
  sig_abs: ['ABS', 'amber'], l34: ['L34', 'amber'], wyc_spring: ['SPRING', 'amber'],
  wyc_sos: ['SOS', 'amber'], d_absorb_bull: ['Ab↑', 'amber'], d_spring: ['dSPR', 'amber'],
  w2_spring: ['W·SPR', 'amber'], w2_sos: ['W·SOS', 'amber'],
  // delta / order-flow
  d_surge_bull: ['Δ↑', 'teal'], d_blast_bull: ['ΔΔ↑', 'teal'],
  d_strong_bull: ['B/S↑', 'teal'], d_div_bull: ['Δdiv', 'teal'],
  // T-timing
  sig_t1g: ['T1G', 'violet'], sig_t2g: ['T2G', 'violet'], sig_buy: ['BUY', 'violet'],
  sig_t6: ['T6', 'violet'],
  // structure / trend
  is_pivot_low_3: ['PvL3', 'slate'], is_pivot_low_5: ['PvL5', 'slate'],
  psar_bull: ['PSAR', 'slate'],
  // volume
  sig_vol_5x: ['V×5', 'yellow'], sig_vol_10x: ['V×10', 'yellow'],
  // parabolic
  para_prep: ['PARA·p', 'orange'], para_start: ['PARA', 'orange'],
  // atomic (5-year-validated weak-close / oversold / gap axis)
  close_o: ['c=O', 'fuchsia'], gap_up: ['GAP↑', 'fuchsia'],
  r2l_os: ['R2L', 'fuchsia'], atomic: ['⚛', 'fuchsia'],
}

/** badgeFor(signal) → {label, cls, fam} styled pill for a lead-in signal. */
export function badgeFor(sig) {
  const b = _BADGE[sig]
  if (b) return { label: b[0], cls: _C[b[1]], fam: b[1] }
  // fallback: known label from descFor's families, neutral pill
  const lbl = (sig || '').replace(/^sig_/, '').replace(/_/g, ' ')
  return { label: lbl, cls: _C.gray, fam: 'gray' }
}


export function descFor(label) {
  if (!label) return ''
  // sequence offset: "vbo_up@-2"
  let off = null, name = label
  const at = label.indexOf('@-')
  if (at >= 0) { name = label.slice(0, at); off = label.slice(at + 2) }
  // categorical: "field=value"
  const eq = name.indexOf('=')
  if (eq >= 0) {
    const field = name.slice(0, eq), val = name.slice(eq + 1)
    let d
    if (field === 'flip_code') d = FLIP_DESC[val] || `Flip via ${val}.`
    else if (field === 'vol_bucket') d = `Volume class ${val}: ${VOL_BUCKET_DESC[val] || ''}`
    else {
      const fd = FIELD_DESC[field] || field
      d = `${fd} = ${val || '(blank)'}`
    }
    return off != null ? `${d}  ·  on bar ${off === '0' ? 'exit' : '−' + off}` : d
  }
  const base = SIGNAL_DESC[name] || familyDesc(name)
  return off != null ? `${base}  ·  fires on bar ${off === '0' ? 'exit (0)' : '−' + off}` : base
}

export default descFor
