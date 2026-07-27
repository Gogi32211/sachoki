import { useState, useEffect, useReducer } from 'react'
import { pwlHas } from './PersonalWatchlistPanel'
import SharedSignalChip from './SignalChip'
import SignalChipList from './SignalChipList'
import TickerCell from './TickerCell'
import TableScrollContainer from './TableScrollContainer'
import { requestGex, getGex, subscribeGex } from '../gexStore'
import { getAnatomy } from '../anatomyStore'
import { atrForecast } from '../atrForecast'

// ── ▽△ Bottom-Anatomy cell — latest-bar verdict from the shared universe map
// (../anatomyStore, one server-cached fetch). 🔻 structural bottom / 🔻💪 durable
// (RS-intact) / 🔺 continues. A DETECTOR (1.37× lift), not a trade signal.
function AnatScanCell({ ticker }) {
  const a = getAnatomy(ticker)
  if (!a) return <span className="text-gray-700">·</span>
  if (a.v === 'rev')
    return <span className="font-mono text-[10px] font-bold whitespace-nowrap"
      title={`🔻${a.rs ? '💪 durable (RS-intact)' : ' structural'} bottom-anatomy · score ${a.s}/8${a.rs ? ' — RS separates the tradeable subset' : ' — structure only, lower precision'}`}
      style={{ color: a.rs ? '#fbbf24' : '#fdba74' }}>{a.rs ? '🔻💪' : '🔻'}{a.s}</span>
  if (a.v === 'shake')
    return <span className="font-mono text-[10px] font-bold whitespace-nowrap" style={{ color: '#c4b5fd' }}
      title={`🌀 shakeout/spring — bearish-engulf at held floor + late hi-vol T-reversal at the close (the tell 🔻 misses). Intraday-only signal. score ${a.s}`}>🌀{a.s}</span>
  if (a.v === 'cont')
    return <span className="font-mono text-[10px] font-bold text-emerald-300 whitespace-nowrap"
      title={`🔺 continuation / markup · score ${a.s}`}>🔺</span>
  return <span className="text-gray-700">·</span>
}

// ── 💠 GEX cell — options context per row: regime + directional LEAN (from
// max-pain/wall location, NOT gamma — gamma is regime) + IV. Shared lazy store
// (../gexStore) so the SORT comparator reads the same cache. Blank on non-optionable.
function GexScanCell({ ticker }) {
  const [, force] = useReducer(x => x + 1, 0)
  useEffect(() => { requestGex(ticker); return subscribeGex(force) }, [ticker])
  const g = getGex(ticker)
  if (g === undefined || g === null) return <span className="text-gray-700">·</span>
  if (!g.available) return <span className="text-gray-800" title="no options chain">–</span>
  const neg = g.regime === 'negative'
  const lean = g.lean
  const arrow = lean === 'up' ? '↑' : lean === 'down' ? '↓' : '→'
  const arrowCls = lean === 'up' ? 'text-emerald-300' : lean === 'down' ? 'text-rose-300' : 'text-md-on-surface-var/50'
  // ⚖️ VRP — IV vs ATR-realized vol dissonance (descriptive, forward-only like all GEX)
  const vrpTxt = g.vrp != null
    ? ` · ⚖️VRP ${g.vrp} (IV ${g.atm_iv}% vs realized ${g.rv_atr}%) ${g.vrp_state === 'EVENT-PRICED' ? '— options EXPENSIVE, the move is already priced in (event risk?)' : g.vrp_state === 'COMPLACENT' ? '— stock moves far MORE than options price in (vol expansion often follows)' : '— normal range (0.65-1.35), no signal'}`
    : ''
  const vrpMark = g.vrp_state === 'EVENT-PRICED' ? <span className="text-amber-300" style={{ fontSize: 9 }}>⚖️</span>
                : g.vrp_state === 'COMPLACENT' ? <span className="text-violet-300" style={{ fontSize: 9 }}>⚖️</span> : null
  return (
    <span className="font-mono text-[10px] whitespace-nowrap"
      title={`GEX ${g.regime}-γ · lean ${lean} (${g.lean_score}) · ATM IV ${g.atm_iv ?? '—'}% · max-pain ${g.max_pain ?? '—'} · put/call wall ${g.put_wall ?? '—'}/${g.call_wall ?? '—'}${g.near ? ' · hugging ' + g.near : ''}${vrpTxt} — LOCATION bias, unvalidated`}>
      <span title={neg ? 'negative gamma — dealers amplify (trend/volatile)' : 'positive gamma — dealers dampen (range/pin)'}>{neg ? '⚡' : '🛡'}</span>
      <b className={arrowCls}>{arrow}</b>
      <span className="text-md-on-surface-var/60"> {g.atm_iv != null ? Math.round(g.atm_iv) : ''}</span>
      {vrpMark}
    </span>
  )
}

// ⚖️ VRP cell — IV ÷ ATR-realized vol from the same lazy gexStore (2026-07-26).
// Descriptive + forward-only (no IV history) — a dissonance gauge, not a signal.
function VrpScanCell({ ticker }) {
  const [, force] = useReducer(x => x + 1, 0)
  useEffect(() => { requestGex(ticker); return subscribeGex(force) }, [ticker])
  const g = getGex(ticker)
  if (g === undefined || g === null) return <span className="text-gray-700">·</span>
  if (!g.available || g.vrp == null) return <span className="text-gray-800" title="no options / IV">–</span>
  const cls = g.vrp_state === 'EVENT-PRICED' ? 'text-amber-300 font-bold'
            : g.vrp_state === 'COMPLACENT' ? 'text-violet-300 font-bold'
            : 'text-md-on-surface-var/60'
  return (
    <span className={`font-mono text-[10px] ${cls}`}
      title={`⚖️ VRP ${g.vrp} — IV ${g.atm_iv}% vs ATR-realized ${g.rv_atr}% · ${g.vrp_state === 'EVENT-PRICED' ? 'EVENT-PRICED (≥1.35): options expensive, the move is already priced in (earnings/news risk)' : g.vrp_state === 'COMPLACENT' ? 'COMPLACENT (≤0.65): the stock moves far more than options price in — often precedes a vol expansion' : 'balanced'} — descriptive, forward-only`}>
      {g.vrp.toFixed(2)}
    </span>
  )
}

// ── Colour helpers (shared) ───────────────────────────────────────────────────
const TZ_STRONG = new Set(['T4','T6','T1G','T2G'])
const TZ_BEAR   = new Set(['Z4','Z6','Z1G','Z2G','Z1','Z2','Z3','Z5','Z7','Z9','Z10','Z11','Z12'])

function scoreColor(s) {
  if (s >= 65) return 'text-lime-300 font-bold'
  if (s >= 50) return 'text-yellow-300 font-semibold'
  if (s >= 35) return 'text-blue-300'
  if (s >= 20) return 'text-md-on-surface'
  return 'text-md-on-surface-var/70'
}

function ultraScoreCls(s) {
  if (s == null) return 'text-gray-700'
  if (s >= 90) return 'text-emerald-200 font-extrabold'
  if (s >= 80) return 'text-emerald-300 font-bold'
  if (s >= 65) return 'text-teal-300 font-semibold'
  if (s >= 50) return 'text-yellow-200/90'
  return 'text-md-on-surface-var'
}

function ultraBandV2Label(s, fallback) {
  if (s == null) return fallback || ''
  if (s >= 90) return 'A+'
  if (s >= 80) return 'A'
  if (s >= 65) return 'B'
  if (s >= 50) return 'C'
  return 'D'
}

function betaZoneCls(zone) {
  switch (zone) {
    case 'ELITE':       return 'text-amber-200 font-bold'
    case 'OPTIMAL':     return 'text-emerald-300 font-bold'
    case 'BUY':         return 'text-blue-300 font-semibold'
    case 'WATCH':       return 'text-violet-300'
    case 'BUILDING':    return 'text-yellow-400'
    case 'EXTENDED':    return 'text-amber-400'
    case 'SHORT_WATCH': return 'text-red-400'
    default:            return 'text-md-on-surface-var/70'
  }
}

function gogTierCls(tier) {
  if (!tier) return ''
  if (tier.endsWith('P')) return 'bg-green-800 text-green-100 ring-1 ring-green-400 font-bold'
  if (tier.endsWith('L')) return 'bg-emerald-800 text-emerald-100 ring-1 ring-emerald-400'
  if (tier.endsWith('C')) return 'bg-teal-800 text-teal-100 ring-1 ring-teal-400'
  return 'bg-fuchsia-800 text-fuchsia-100 ring-1 ring-fuchsia-400'
}

function ctxTokCls(tok) {
  if (tok === 'LDP' || tok === 'LRP') return 'bg-green-900 text-green-200 font-semibold'
  if (tok === 'LDC' || tok === 'LRC') return 'bg-teal-900 text-teal-200'
  if (tok === 'LDS' || tok === 'LD')  return 'bg-cyan-900 text-cyan-300'
  if (tok === 'BCT')                  return 'bg-blue-900 text-blue-200 font-semibold'
  if (tok === 'SQB')                  return 'bg-blue-900 text-blue-300'
  return 'bg-md-surface-high text-md-on-surface-var'
}

const CTX_PRIO = [
  ['ctx_ldp','LDP'],['ctx_lrp','LRP'],
  ['ctx_ldc','LDC'],['ctx_lrc','LRC'],
  ['ctx_lds','LDS'],['ctx_ld','LD'],
  ['ctx_bct','BCT'],['ctx_sqb','SQB'],
  ['ctx_wrc','WRC'],['ctx_f8c','F8C'],['ctx_svs','SVS'],
]
function ctxTokens(r) {
  return CTX_PRIO.filter(([k]) => r[k]).map(([, t]) => t)
}

function scoreCls(n) {
  if (n >= 120) return 'text-yellow-300 font-bold'
  if (n >= 100) return 'text-lime-300 font-bold'
  if (n >= 80)  return 'text-green-300 font-semibold'
  if (n >= 60)  return 'text-teal-300'
  return 'text-md-on-surface-var'
}

// 🎲 score-hits — backend key → short label for the cell tooltip (see compute_score_hits)
const _HIT_LABEL = {
  ultra_score_v3: 'UV3', ultra_score: 'ULTRA', buy_score: 'BUY',
  prebreak_v2: 'V2', prebreak_v3: 'V3', conf_n: 'CONF-n',
}

const fmt = (v, d = 2) => v == null ? '—' : Number(v).toFixed(d)

// ── Row left-border by profile category ──────────────────────────────────────
function profileBorderCls(cat) {
  switch (cat) {
    case 'SWEET_SPOT':  return 'border-l-2 border-l-emerald-500'
    case 'WATCH':       return 'border-l-2 border-l-yellow-500'
    case 'SHORT_WATCH': return 'border-l-2 border-l-red-500'
    case 'BUILDING':    return 'border-l-2 border-l-blue-500'
    default:            return 'border-l-2 border-l-transparent'
  }
}

// ── T/Z cell chip — delegates to the shared SignalChip so colors match Superchart
function TZChip({ label }) {
  return <SharedSignalChip signal={label} size="sm" />
}

// ── StatusChip for profile category ──────────────────────────────────────────
function StatusChip({ cat }) {
  const cfgMap = {
    SWEET_SPOT:  'bg-emerald-900/50 text-emerald-300 text-[10px] px-1 py-0.5 rounded',
    WATCH:       'bg-yellow-900/50 text-yellow-300 text-[10px] px-1 py-0.5 rounded',
    BUILDING:    'bg-blue-900/40 text-blue-300 text-[10px] px-1 py-0.5 rounded',
    SHORT_WATCH: 'bg-red-900/40 text-red-400 text-[10px] px-1 py-0.5 rounded',
  }
  const cls = cfgMap[cat] || 'bg-md-surface-high text-md-on-surface-var text-[10px] px-1 py-0.5 rounded'
  const short = { SWEET_SPOT: 'SWEET', WATCH: 'WATCH', BUILDING: 'BUILD', SHORT_WATCH: 'SHORT' }
  return <span className={cls}>{short[cat] || cat}</span>
}

// ── Collect all active signals for a row in priority order ───────────────────
// type: 'bull' | 'bear' | 'info'
export function collectSignals(r) {
  const sigs = []
  const bull = (label, priority) => sigs.push({ label, priority, type: 'bull' })
  const bear = (label, priority) => sigs.push({ label, priority, type: 'bear' })
  const info = (label, priority) => sigs.push({ label, priority, type: 'info' })

  // VABS / Vol
  if (r.vol_spike_20x) info('V×20', 10)
  else if (r.vol_spike_10x) info('V×10', 9)
  else if (r.vol_spike_5x)  info('V×5', 8)
  if (r.best_sig)   bull('BEST★', 10)
  if (r.strong_sig && !r.best_sig) bull('STR', 7)
  if (r.vbo_up)     bull('VBO↑', 7)
  if (r.abs_sig)    bull('ABS', 6)
  if (r.load_sig)   bull('LD', 5)
  if (r.climb_sig)  bull('CLB', 5)

  // Delta
  if (r.d_spring)      bull('dSPR', 9)
  if (r.d_blast_bull)  bull('ΔΔ↑', 8)
  else if (r.d_surge_bull) bull('Δ↑', 7)
  if (r.d_strong_bull) bull('B/S↑', 6)
  if (r.d_absorb_bull) bull('Ab↑', 5)

  // Combo
  if (r.rocket)    bull('🚀', 10)
  if (r.buy_2809)  bull('BUY', 8)
  if (r.para_plus) bull('PARA+', 9)
  else if (r.para_start) bull('PARA', 8)
  if (r.smx)       info('SMX', 7)
  if (r.akan_sig)  info('A', 7)
  if (r.smx_sig)   info('SM', 7)
  if (r.nnn_sig)   info('N', 7)
  if (r.mx_sig)    info('MX', 7)
  if (r.gog_sig)   bull('GOG', 7)
  if (r.rs_strong) bull('RS+', 7)
  else if (r.rs)   bull('RS', 6)
  if (r.fly_abcd)  bull('ABCD', 8)
  if (r.sig_l88)   bull('L88', 6)

  // TZ transitions
  if (r.tz_bull_flip) bull('TZ→3', 8)
  else if (r.tz_attempt) bull('TZ→2', 7)

  // Wyckoff
  if (r.ns) info('NS', 6)
  if (r.sq) info('SQ', 5)

  // L-signals
  if (r.fri34) bull('FRI34', 7)
  else if (r.fri43) bull('FRI43', 7)
  if (r.l34 && !r.fri34) bull('L34', 6)
  if (r.be_up) bull('BE↑', 6)

  // Breakout
  if (r.best_long)  bull('BEST↑', 9)
  else if (r.fbo_bull) bull('FBO↑', 7)
  if (r.x2g_wick) bull('X2G', 8)

  // PREUP
  if (r.preup66)      bull('P66', 6)
  else if (r.preup55) bull('P55', 5)
  else if (r.preup89) bull('P89', 5)
  else if (r.preup3)  bull('P3', 4)

  // B signals (B1–B11) intentionally NOT shown — retired from display per user.
  // Still computed in the backend (feeds CA/CD/CW combos), just never rendered.

  // G signals
  for (const k of ['g1','g2','g4','g6','g11']) {
    if (r[k]) { bull(k.toUpperCase(), 4); break }
  }

  // 260523 — AD / WYC / PREBREAK / Pullback events
  // High-conviction reversal markers first.
  if (r.ad_cluster) bull('AD-CLU', 9)
  else if (r.ad_fresh) bull('AD-FR', 8)
  if (r.wyc_spring) bull('SPRING', 9)
  if (r.wyc_sos)    bull('SOS', 8)
  // Wyckoff macro phase context (only emit non-NEUTRAL)
  const wyp = r.wyc_phase
  if (wyp && wyp !== 'NEUTRAL') {
    if (wyp === 'MARKUP')      bull('MARKUP', 6)
    else if (wyp === 'MKDN')   bear('MKDN', 6)
    else if (wyp === 'ACC_TR') info('ACC_TR', 5)
    else if (wyp === 'DIST_TR')info('DIST_TR', 5)
    else if (wyp === 'UTAD')   bear('UTAD', 7)
  }
  if (r.wyc_in_tr) info('InTR', 4)
  if (r.wyc_sow)   bear('SOW', 6)
  // PREBREAK score tier
  if (r.prebreak_prime)      bull('PRIME★', 9)
  else if (r.prebreak_ready) bull('READY', 7)
  else if (r.prebreak_watch) bull('WATCH', 5)
  // Pullback-miner per-bar events
  if (r.pb_lvbo)         bull('LVBO', 7)
  if (r.pb_wvf_confirm)  bull('WVF', 6)
  if (r.pb_stop_cause)   bull('W-PH', 6)
  if (r.pb_macro_penalty) bear('PEN', 4)
  // Swing classification (HL/LL/HH/LH)
  const st = r.swing_type
  if (st === 'HL') bull('HL', 5)
  else if (st === 'LL') bull('LL', 4)
  else if (st === 'HH') info('HH', 4)
  else if (st === 'LH') bear('LH', 5)

  sigs.sort((a, b) => b.priority - a.priority)
  return sigs
}

// ── Small neutral badge (used outside Signals column) ─────────────────────────
function SmallBadge({ label, cls = '' }) {
  return (
    <span className={`text-[10px] px-1 py-0.5 rounded bg-md-surface-high text-md-on-surface-var ${cls}`}>
      {label}
    </span>
  )
}

// ── StarBtn ───────────────────────────────────────────────────────────────────
function StarBtn({ ticker, tf, onToggle }) {
  const [saved, setSaved] = useState(() => pwlHas(ticker, tf))
  return (
    <button
      title={saved ? 'Remove from watchlist' : 'Save to watchlist'}
      className={`text-sm transition-colors ${saved ? 'text-yellow-400' : 'text-gray-700 hover:text-yellow-400'}`}
      onClick={e => {
        e.stopPropagation()
        onToggle?.()
        setSaved(s => !s)
      }}>
      ★
    </button>
  )
}

// ── Row expansion detail panel ────────────────────────────────────────────────
function ExpandedRow({ r, colSpan }) {
  const allSigs = collectSignals(r)
  const ctx = ctxTokens(r)

  return (
    <tr className="bg-md-surface-con/80 border-b border-white/[0.06]">
      <td colSpan={colSpan} className="px-4 py-3">
        <div className="grid grid-cols-2 gap-4 text-[11px] md:grid-cols-3 lg:grid-cols-4">

          {/* All signals */}
          <div>
            <div className="text-md-on-surface-var/70 mb-1 font-semibold uppercase tracking-wide text-[9px]">All Signals</div>
            <SignalChipList signals={allSigs.map(s => s.label)} mode="table" />
          </div>

          {/* GOG / Context */}
          {(r.gog_tier || ctx.length > 0 || (r.signal_score ?? 0) > 0) && (
            <div>
              <div className="text-md-on-surface-var/70 mb-1 font-semibold uppercase tracking-wide text-[9px]">GOG / Context</div>
              <div className="flex flex-wrap gap-0.5">
                {r.gog_tier && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded ${gogTierCls(r.gog_tier)}`}>{r.gog_tier}</span>
                )}
                {ctx.map(tok => (
                  <span key={tok} className={`text-[10px] px-1 py-0.5 rounded ${ctxTokCls(tok)}`}>{tok}</span>
                ))}
                {(r.signal_score ?? 0) > 0 && (
                  <span className={`font-mono text-[10px] ${scoreCls(r.signal_score)}`}>{r.signal_score}</span>
                )}
              </div>
            </div>
          )}

          {/* Score breakdown */}
          <div>
            <div className="text-md-on-surface-var/70 mb-1 font-semibold uppercase tracking-wide text-[9px]">Score Breakdown</div>
            <div className="space-y-0.5 text-md-on-surface-var">
              {r.turbo_score != null && (
                <div>Turbo: <span className={scoreColor(r.turbo_score)}>{fmt(r.turbo_score, 1)}</span></div>
              )}
              {r.ultra_score != null && (
                <div>Ultra: <span className={ultraScoreCls(r.ultra_score)}>{r.ultra_score}</span>
                  {' '}<span className="text-md-on-surface-var/60 text-[9px]">
                    {r.ultra_score_band_v2 || ultraBandV2Label(r.ultra_score, r.ultra_score_band) || ''}
                  </span>
                </div>
              )}
              {r.beta_score > 0 && (
                <div>Beta: <span className={betaZoneCls(r.beta_zone)}>{r.beta_score} {r.beta_zone}</span></div>
              )}
              {r.rtb_phase && r.rtb_phase !== '0' && (
                <div>RTB: Phase {r.rtb_phase} · {(r.rtb_total ?? 0).toFixed(0)}</div>
              )}
              {r.profile_score != null && (
                <div>Profile: {r.profile_score} {r.profile_category || ''}</div>
              )}
            </div>
          </div>

          {/* EMA levels */}
          <div>
            <div className="text-md-on-surface-var/70 mb-1 font-semibold uppercase tracking-wide text-[9px]">EMA Levels</div>
            <div className="space-y-0.5 font-mono text-md-on-surface-var">
              {r.ema20  > 0 && <div>EMA20: ${fmt(r.ema20)}</div>}
              {r.ema50  > 0 && <div>EMA50: ${fmt(r.ema50)}</div>}
              {r.ema89  > 0 && <div>EMA89: ${fmt(r.ema89)}</div>}
              {r.ema200 > 0 && <div>EMA200: ${fmt(r.ema200)}</div>}
            </div>
          </div>

          {/* RSI / CCI / Volume */}
          <div>
            <div className="text-md-on-surface-var/70 mb-1 font-semibold uppercase tracking-wide text-[9px]">Indicators</div>
            <div className="space-y-0.5 font-mono text-md-on-surface-var">
              <div>RSI: <span className={r.rsi <= 35 ? 'text-lime-400' : r.rsi >= 70 ? 'text-red-400' : ''}>{r.rsi != null ? fmt(r.rsi, 0) : '—'}</span></div>
              <div>CCI: <span className={r.cci >= 100 ? 'text-lime-400' : r.cci <= -100 ? 'text-red-400' : ''}>{r.cci != null ? fmt(r.cci, 0) : '—'}</span></div>
              {r.avg_vol > 0 && (
                <div>Vol: {r.avg_vol >= 1_000_000 ? `${(r.avg_vol/1_000_000).toFixed(1)}M`
                  : r.avg_vol >= 1_000 ? `${Math.round(r.avg_vol/1_000)}K`
                  : Math.round(r.avg_vol)}</div>
              )}
            </div>
          </div>

          {/* ABR / ULTRA enrichment (if present) */}
          {r.abr?.category && (
            <div>
              <div className="text-md-on-surface-var/70 mb-1 font-semibold uppercase tracking-wide text-[9px]">ABR</div>
              <div className="space-y-0.5 text-md-on-surface-var">
                <div>Category: <span className={
                  r.abr.category === 'A'  ? 'text-emerald-300' :
                  r.abr.category === 'B+' ? 'text-cyan-300' :
                  r.abr.category === 'B'  ? 'text-blue-300' :
                  r.abr.category === 'R'  ? 'text-red-400' : ''
                }>{r.abr.category}</span></div>
                {r.abr.med10d_pct  != null && <div>Med10d: {r.abr.med10d_pct}</div>}
                {r.abr.fail10d_pct != null && <div>Fail10d: {r.abr.fail10d_pct}</div>}
                {r.abr.action_hint && <div>Hint: {r.abr.action_hint}</div>}
              </div>
            </div>
          )}

          {/* Pullback */}
          {r.pullback?.evidence_tier && (
            <div>
              <div className="text-md-on-surface-var/70 mb-1 font-semibold uppercase tracking-wide text-[9px]">Pullback</div>
              <div className="text-md-on-surface-var space-y-0.5">
                <div>{r.pullback.evidence_tier}</div>
                {r.pullback.pattern_key && <div>Key: {r.pullback.pattern_key}</div>}
                {r.pullback.score != null && <div>Score: {r.pullback.score}</div>}
                {r.pullback.median_10d_return != null && <div>Med10d: {r.pullback.median_10d_return}</div>}
              </div>
            </div>
          )}

          {/* Rare Reversal */}
          {r.rare_reversal?.evidence_tier && (
            <div>
              <div className="text-md-on-surface-var/70 mb-1 font-semibold uppercase tracking-wide text-[9px]">Rare Reversal</div>
              <div className="text-md-on-surface-var space-y-0.5">
                <div>{r.rare_reversal.evidence_tier}</div>
                {r.rare_reversal.base4_key && <div>Key: {r.rare_reversal.base4_key}</div>}
                {r.rare_reversal.score != null && <div>Score: {r.rare_reversal.score}</div>}
                {r.rare_reversal.median_10d_return != null && <div>Med10d: {r.rare_reversal.median_10d_return}</div>}
              </div>
            </div>
          )}

        </div>
      </td>
    </tr>
  )
}

// ── Skeleton loading row ───────────────────────────────────────────────────────
function SkeletonRow({ colSpan }) {
  return (
    <tr className="border-b border-white/[0.06] animate-pulse">
      {Array.from({ length: colSpan }).map((_, i) => (
        <td key={i} className="px-2 py-2">
          <div className="h-3 bg-md-surface-high/60 rounded" style={{ width: i === 2 ? '70%' : i === 3 ? '40%' : '60%' }} />
        </td>
      ))}
    </tr>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN COMPONENT
// ─────────────────────────────────────────────────────────────────────────────

/**
 * ScannerDataGrid
 *
 * Props:
 *   results        - array of scanner rows (already filtered + sorted by parent)
 *   onSelectTicker - fn(ticker) called when row is clicked
 *   onWatchlistToggle - fn(row) called when star is clicked
 *   localTf        - current timeframe string (for watchlist key)
 *   pickedTickers  - Set of selected ticker strings
 *   onTogglePicked - fn(ticker, e) toggle row checkbox
 *   sortBy         - current sort column key
 *   sortDir        - 'asc' | 'desc'
 *   onSort         - fn(col) toggle sort
 *   isLoading      - bool: show skeleton
 *   error          - string: show error state
 *   effectiveScoreCol - column key to use for score display
 *   universe       - current universe key (for split column)
 *   variant        - 'ultra' | 'turbo' (controls extra columns)
 *   onPickAll      - fn(checked) select/deselect all
 *   allPicked      - bool: header checkbox state
 */
export default function ScannerDataGrid({
  results = [],
  onSelectTicker,
  onWatchlistToggle,
  localTf = '1d',
  pickedTickers = new Set(),
  onTogglePicked,
  sortBy,
  sortDir,
  onSort,
  isLoading = false,
  error = null,
  effectiveScoreCol = 'turbo_score',
  universe = 'sp500',
  variant = 'turbo',
  pmData = {},
  onPickAll,
  allPicked = false,
  handleRowEnter,
  handleRowLeave,
  pinned = false,          // single-row strip (header hidden) shown above the chart
}) {
  // Row click selects the ticker (switches the top chart) — no inline expansion.

  // Number of columns for colSpan calculation
  // ultra adds: ULTRA + UV3 + 🎲 + BUY + EDGE + PM + ⏱ + ⚖️ columns (+8 vs turbo); split adds Split column (+1)
  const baseColCount = variant === 'ultra' ? 29 : 16
  const colCount = (universe === 'split' || universe === 'zone') ? baseColCount + 1 : baseColCount

  const SortTh = ({ col, children, cls = '' }) => (
    <th
      className={`px-2 py-1.5 font-medium cursor-pointer select-none hover:text-white transition-colors whitespace-nowrap ${cls}`}
      onClick={() => onSort?.(col)}>
      {children}{sortBy === col ? (sortDir === 'desc' ? ' ↓' : ' ↑') : ''}
    </th>
  )

  return (
    <div className={pinned ? 'overflow-x-auto' : 'overflow-auto flex-1'}>
      <table className="w-full border-collapse text-xs">
        {!pinned && (
        <thead className="sticky top-0 z-10 bg-md-surface-con text-md-on-surface-var text-left [&>tr>th]:shadow-[0_1px_0_0_rgba(255,255,255,0.07)]">
          <tr>
            {/* Checkbox col */}
            <th className="px-2 py-1.5 w-5 sticky left-0 z-20 bg-md-surface-con">
              <input type="checkbox" className="accent-indigo-500 cursor-pointer"
                title="Select/deselect all visible"
                checked={allPicked}
                onChange={e => onPickAll?.(e.target.checked)} />
            </th>
            {/* Star col */}
            <th className="px-1 py-1.5 w-5 sticky left-[28px] z-20 bg-md-surface-con font-medium text-md-on-surface-var" title="Watchlist">★</th>
            {/* Ticker */}
            <SortTh col="ticker" cls="sticky left-[52px] z-20 bg-md-surface-con min-w-[72px] max-w-[100px]">Ticker</SortTh>
            {/* Score */}
            <SortTh col="turbo_score" cls="text-right min-w-[50px]">Score</SortTh>
            {/* ULTRA Score — only in ultra variant */}
            {variant === 'ultra' && (
              <SortTh col="ultra_score" cls="text-right min-w-[50px]" title="ULTRA Score — independent confluence ranking">ULTRA</SortTh>
            )}
            {/* UV3 — ULTRA Score v3, the reweighted ranker (NOT the PreBreakout V3 column) */}
            {variant === 'ultra' && (
              <SortTh col="ultra_score_v3" cls="text-right min-w-[46px]" title="ULTRA Score v3 (2026-07-18) — reweighted ranker: oversold(RSI) + price-zone($21-89) + earners(BX↑/STR/absorb) + 🏆RS/🎯cluster/🎋TLS. Ranks forward return (Spearman +0.08 vs the old score's −0.00, 6yr path-sim); DEMOTES the overbought/extended names the old ULTRA tops. Own bands A≥60/B≥45/C≥30. Hover a value for reasons.">UV3</SortTh>
            )}
            {/* 🎲 — score AGREEMENT: how many rankers sit in their own measured good zone */}
            {variant === 'ultra' && (
              <SortTh col="score_hits" cls="text-center min-w-[40px]" title="🎲 SCORE-HITS (2026-07-27) — how many of our 6 rankers sit in THEIR OWN measured good zone (UV3 >18 · ULTRA 8-20 · BUY 39-57 · V2 10-12 · V3 5 · CONF-n ≥5). Note these are NOT 'high is good': ULTRA and BUY are inverted-U, and high V2 is the single worst cell in the system (med −3.80). Each component alone is near-worthless — the AGREEMENT is the edge. Full 6yr path-sim: hits 0→5 = −1.06 · −0.67 · +0.00 · +1.05 · +2.12 · +3.79 (monotone); hits≥4 = 6/6yr with BOTH bear years positive. Zones picked on 2021-23 only and the ladder HELD out-of-sample on 2024-26 (+0.43 → +3.54, pf 2.28). hits=5 is rare (~400/yr universe-wide) — that rarity is the point.">🎲</SortTh>
            )}
            {/* BUY — validated zone buy-flags */}
            {variant === 'ultra' && (
              <SortTh col="buy_flag" cls="text-center min-w-[44px]" title="Validated BUY flag (6yr path-sim). 🟢REV = bounce off oversold (min-5 RSI<38, RSI 30-55, up bar) + LOW beta ≤13 → +1.04%/win44/PF1.15/+8.4σ/4-6yr. 🔵BRK = RSI crosses 50 up + up bar + LOW turbo ≤28 → +0.57%/+2.1σ (weaker). Both long-only (lose in bear years). Sort to group the fires.">BUY</SortTh>
            )}
            {/* EDGE — validated Edge-board setup fires (last 5 bars) */}
            {variant === 'ultra' && (<>
              <SortTh col="edge_n" cls="text-center min-w-[52px]" title="✅ EDGE fires — validated Edge-board setups on the last 5 bars, from the SAME edge_replay masks the backtest uses. Plain code (G3, QZC, 🎯3…) = fired TODAY; 'G3·2d' = 2 bars ago. Sorts by today's fire count.">EDGE</SortTh>
              <th className="text-center min-w-[46px]" title="🧬 frozen-OOS 2-4-bar robust sequence completed TODAY (tier OOS✓ · OOS win≥55% · ps_med>0; bright ≥60; mined 2021-23, verified 2024-26). Number = OOS win%. 🏆 = DSR≥0.6 selection-proof.">SEQ</th>
              <SortTh col="conf_score" cls="text-center min-w-[46px]" title="CONF — all-vs-all confluence score: 812 dual-gate-qualified signal pairs on the LATEST bar (validated monotone decile ladder: D9 ≥10 → path-sim +4.01%/med+1.51/6-6yr · D0 ≤−15 → −3.04%/0-6yr; both tails era-robust). Hover a value for the driving cells.">CONF</SortTh>
              <SortTh col="gex" cls="text-center min-w-[52px]" title="💠 GEX — options dealer context (liquid names only; lazy-loaded). ⚡=negative gamma (dealers amplify = trend/volatile) · 🛡=positive gamma (dampen = range/pin). Arrow = LOCATION lean from max-pain + walls (↑ support/put-wall · ↓ resistance/call-wall/below-max-pain · → flat). Number = ATM IV%. SORT groups by lean_score (↑ up-lean first desc); sorting fetches GEX for all shown rows. UNVALIDATED context, not a buy signal.">💠GEX</SortTh>
              <SortTh col="anat" cls="text-center min-w-[48px]" title="▽△ Bottom-Anatomy (nested 1D→1H→15m) — LATEST bar. 🔻 = structural bottom (held/tested floor + multi-TF Z-absorption + intraday reversal; 1.37× lift / 76% recall vs random swing-lows). 🔻💪 = + RS-intact = the durable/tradeable subset (path-sim +). 🔺 = continuation/markup. Number = anatomy score /8. A DETECTOR, not a trade signal (necessary-not-sufficient). Sort groups 🔻💪 → 🔻 → 🔺.">▽△</SortTh>
              <SortTh col="atr_pct" cls="text-center min-w-[50px]" title="⏱ ATR time-to-target forecast (OOS-calibrated, TRAIN 2021-23 ≈ TEST 2024-26): typical (median) days for THIS stock to move +10%, given its current ATR% (stored bars.atr_14, nightly-enriched). Cell = 'days · hit%' (hit = % that reach +10% within 90 bars). Hover shows −10% stop-timing too. days ≈ (10/ATR%)^0.67 — pure volatility law. TIMING/expectation context, NOT a buy signal (targets hit at ~base-rate; the edge lives in downside). Sort (desc) = fastest movers first.">⏱+10%</SortTh>
              <SortTh col="vrp" cls="text-center min-w-[46px]" title="⚖️ VRP — options implied vol (ATM IV) ÷ ATR-realized vol. Calibrated on the live cross-section of 40 liquid names (p25 0.77 · MEDIAN 0.81 · p75 1.31) — note ATR is range-based so 'fair' sits near 0.8, not 1.0. ≥1.35 EVENT-PRICED (amber): options expensive, a move is already priced in (earnings/news); if nothing happens IV deflates. ≤0.65 COMPLACENT (violet): the stock moves far MORE than options price in — often precedes a volatility expansion. In between = the normal state, no signal. Lazy-loaded with 💠GEX (liquid names only); sorting fetches all shown rows — desc = most EVENT-PRICED first, asc = most COMPLACENT first. DESCRIPTIVE + forward-only (no IV history; accumulating in gex_edge_log) — NOT a validated signal.">⚖️VRP</SortTh>
            </>)}
            {/* BETA */}
            <SortTh col="beta_score" cls="text-center min-w-[60px]" title="BETA Score — non-linear quality rank">BETA</SortTh>
            {/* V2 — PreBreakout v2 (data-derived, OOS-validated) */}
            <SortTh col="prebreak_v2" cls="text-center min-w-[52px]" title="PreBreakout v2 — data-derived, OOS-validated breakout-probability. BUY=sweet spot, HOT=overbought/lottery, WATCH=avoid">V2</SortTh>
            {/* V3 — simple additive pre-breakout cluster (0..50, heuristic) */}
            <SortTh col="prebreak_v3" cls="text-center min-w-[44px]" title="PreBreakout v3 — additive signal cluster 0..50 (FLY/ULT/ABS+BC/SVS/CONSO/PhaseD/WICK/LOAD/SQ). Hover a value for the reasons. Heuristic, not OOS-validated.">V3</SortTh>
            {/* RTB */}
            <SortTh col="rtb_total" cls="text-center min-w-[40px]">RTB</SortTh>
            {/* T/Z */}
            <SortTh col="tz_sig" cls="text-center min-w-[44px]">T/Z</SortTh>
            {/* Category */}
            <th className="px-2 py-1.5 font-medium text-center min-w-[60px]">Cat</th>
            {/* Signals — all chips shown, may wrap or grow row height */}
            <th className="px-2 py-1.5 font-medium min-w-[180px]">Signals</th>
            {/* ABR */}
            <th className="px-2 py-1.5 font-medium text-center min-w-[36px]">ABR</th>
            {/* RSI */}
            <SortTh col="rsi" cls="text-right min-w-[36px]">RSI</SortTh>
            {/* CCI */}
            <SortTh col="cci" cls="text-right min-w-[44px]">CCI</SortTh>
            {/* Price */}
            <SortTh col="last_price" cls="text-right min-w-[64px]">Price</SortTh>
            {/* % */}
            <SortTh col="change_pct" cls="text-right min-w-[52px]">%</SortTh>
            {/* Real-time intraday % (ultra only) */}
            {variant === 'ultra' && (
              <SortTh col="rt_chg_pct" cls="text-right min-w-[64px] text-sky-300"
                title="RT%: real-time regular-session % change vs prev close (Massive snapshot) · live during market hours, full-day move after close · refreshes every 15 min">
                RT%
              </SortTh>
            )}
            {/* Pre-market (ultra only) */}
            {variant === 'ultra' && (
              <SortTh col="pm_chg_pct" cls="text-right min-w-[72px] text-violet-300"
                title="Pre-market % change vs prev close · refreshes every 15 min · null = no PM trades yet">
                PM%
              </SortTh>
            )}
            {/* Split (only for split universe) */}
            {universe === 'split' && (
              <th className="px-2 py-1.5 font-medium text-amber-300 min-w-[64px]" title="Split ratio + phase">Split</th>
            )}
            {/* Zone (only for zone universe) */}
            {universe === 'zone' && (
              <th className="px-2 py-1.5 font-medium text-emerald-300 min-w-[120px]"
                  title="Position inside the active HV zone · spike× · age">Zone</th>
            )}
          </tr>
        </thead>
        )}

        <tbody>
          {/* Loading skeleton */}
          {isLoading && results.length === 0 && (
            Array.from({ length: 8 }).map((_, i) => (
              <SkeletonRow key={i} colSpan={colCount} />
            ))
          )}

          {/* Error state */}
          {error && !isLoading && (
            <tr>
              <td colSpan={colCount} className="px-4 py-8 text-center text-red-400">
                {error}
              </td>
            </tr>
          )}

          {/* Empty state */}
          {!isLoading && !error && results.length === 0 && (
            <tr>
              <td colSpan={colCount} className="px-4 py-10 text-center text-md-on-surface-var">
                No tickers match current filters. Try relaxing filters.
              </td>
            </tr>
          )}

          {/* Data rows */}
          {results.map((r, rowIdx) => {
            const sc = r[effectiveScoreCol] ?? r.turbo_score ?? 0
            const isEven = rowIdx % 2 === 0
            const rowBg = isEven ? 'bg-md-surface-con' : ''

            // Collect all signals — every chip is shown in the Signals column.
            const allSigs = collectSignals(r)

            const chg = r.change_pct ?? 0

            return [
              <tr key={r.ticker}
                className={`border-b border-white/[0.06] cursor-pointer transition-colors
                  hover:bg-white/5
                  ${rowBg}
                  ${profileBorderCls(r.profile_category)}`}
                onClick={() => onSelectTicker?.(r.ticker, r, { variant, effectiveScoreCol, universe, localTf, pmData })}
              >

                {/* Checkbox */}
                <td className={`px-2 py-1 w-5 sticky left-0 z-10 ${isEven ? 'bg-md-surface-con' : 'bg-md-surface'}`}
                  onClick={e => e.stopPropagation()}>
                  <input type="checkbox" className="accent-indigo-500 cursor-pointer"
                    checked={pickedTickers.has(r.ticker)}
                    onChange={e => onTogglePicked?.(r.ticker, e)} />
                </td>

                {/* Star */}
                <td className={`px-1 py-1 w-5 sticky left-[28px] z-10 ${isEven ? 'bg-md-surface-con' : 'bg-md-surface'}`}>
                  <StarBtn ticker={r.ticker} tf={localTf} onToggle={() => onWatchlistToggle?.(r)} />
                </td>

                {/* Ticker — compact symbol-only cell. Hover here (not the whole row) opens the mini chart. */}
                <td className={`px-2 py-1 sticky left-[52px] z-10 ${isEven ? 'bg-md-surface-con' : 'bg-md-surface'}`}
                    title={[r.ticker, r.sector || r.vol_bucket].filter(Boolean).join(' · ')}
                    onMouseEnter={handleRowEnter ? (e => handleRowEnter(e, r)) : undefined}
                    onMouseLeave={handleRowLeave || undefined}>
                  <TickerCell
                    symbol={r.ticker}
                    company={r.company || r.name}
                    sector={r.sector || r.vol_bucket}
                    compact
                  />
                  {r.data_source === 'yfinance' && (
                    <span className="text-[8px] text-orange-400/60 ml-0.5">yf</span>
                  )}
                </td>

                {/* Score (BUY score when effectiveScoreCol='buy_score': V2+RSI+volB, two-sided veto) */}
                <td className="px-2 py-1 text-right"
                  title={r.buy_tag === 'EXTENDED' ? 'RSI≥60 — overextended, worst forward zone (veto-capped)'
                       : r.buy_tag === 'KNIFE' ? 'RSI<28 — falling-knife zone (guard-capped)'
                       : `Score: ${sc}`}>
                  <span className={`font-mono text-xs ${scoreColor(sc)}`}>{fmt(sc, 0)}</span>
                  {effectiveScoreCol === 'buy_score' && r.buy_tag && (
                    <div className={`text-[8px] leading-tight font-semibold ${r.buy_tag === 'EXTENDED' ? 'text-red-400' : 'text-amber-400'}`}>
                      {r.buy_tag === 'EXTENDED' ? '🔴EXT' : '🔪KNIFE'}
                    </div>
                  )}
                </td>

                {/* ULTRA Score */}
                {variant === 'ultra' && (
                  <td className="px-2 py-1 text-right"
                    title={r.ultra_score_reasons || (r.ultra_score != null ? `ULTRA ${r.ultra_score}` : '')}>
                    {r.ultra_score != null ? (
                      <span className={`font-mono text-xs ${ultraScoreCls(r.ultra_score)}`}>{r.ultra_score}</span>
                    ) : <span className="text-gray-700">—</span>}
                  </td>
                )}

                {/* UV3 — ULTRA Score v3 (reweighted ranker) */}
                {variant === 'ultra' && (
                  <td className="px-2 py-1 text-right"
                    title={Array.isArray(r.ultra_score_v3_reasons) ? r.ultra_score_v3_reasons.join(' · ')
                      : (r.ultra_score_v3_reasons || (r.ultra_score_v3 != null ? `UV3 ${r.ultra_score_v3}` : ''))}>
                    {r.ultra_score_v3 != null && r.ultra_score_v3 !== '' ? (
                      <span className={`font-mono text-xs ${
                        r.ultra_score_v3_band === 'A' ? 'text-green-300 font-semibold'
                        : r.ultra_score_v3_band === 'B' ? 'text-lime-300'
                        : r.ultra_score_v3_band === 'C' ? 'text-amber-300'
                        : 'text-md-on-surface-var/60'}`}>{r.ultra_score_v3}</span>
                    ) : <span className="text-gray-700">—</span>}
                  </td>
                )}

                {/* 🎲 — score-hits: agreement count across the 6 rankers' own good zones */}
                {variant === 'ultra' && (
                  <td className="px-1 py-1 text-center"
                    title={Array.isArray(r.score_hits_which) && r.score_hits_which.length
                      ? `in-zone: ${r.score_hits_which.map(k => _HIT_LABEL[k] || k).join(' · ')}`
                      : 'no ranker in its good zone'}>
                    {r.score_hits != null ? (
                      <span className={`font-mono text-xs ${
                        r.score_hits >= 5 ? 'text-green-300 font-semibold'
                        : r.score_hits === 4 ? 'text-lime-300'
                        : r.score_hits === 3 ? 'text-amber-300'
                        : 'text-md-on-surface-var/40'}`}>{r.score_hits}</span>
                    ) : <span className="text-gray-700">—</span>}
                  </td>
                )}

                {/* BUY — validated buy-flags + MTF confirmations (same language as Superchart) */}
                {variant === 'ultra' && (
                  <td className="px-1 py-1 text-center whitespace-nowrap"
                    title={r.rev_buy
                      ? (r.mtf_echo === false
                        ? '⚠️ REV-buy WITHOUT 4H/1H echo — validated VETO (−1.07%/win39, 0/6yr): skip'
                        : '🟢 REV-buy + intraday echo (+1.09…+1.17%, 6/6yr)')
                      : r.brk_buy ? '🔵 BRK-buy — RSI>50 cross + low turbo (+0.57%, weaker)'
                      : r.mtf_score_conf != null
                        ? (r.mtf_score_conf === 0
                          ? '0/3 — 1D buy_score≥60 but NO intraday TF confirms — HARD SKIP (−2.10%/win36, 0/6yr)'
                          : `${r.mtf_score_conf}/3 intraday TFs confirm the good 1D buy_score (+1.3…+1.6%, 5-6/6yr)`)
                      : r.turn_echo_n
                        ? `①②③ turn-echo: ${r.turn_echo_n}/3 intraday TFs printed the REV-turn on this up-turn (+0.7…+1.05%, 5/6yr)`
                        : ''}>
                    {r.rev_buy ? (r.mtf_echo === false ? '⚠️' : '🟢')
                      : r.brk_buy ? '🔵'
                      : r.mtf_score_conf != null ? (
                        <span className={`font-mono font-bold ${
                          r.mtf_score_conf === 0 ? 'text-red-400'
                          : r.mtf_score_conf === 1 ? 'text-amber-300'
                          : r.mtf_score_conf === 2 ? 'text-lime-300'
                          : 'text-green-300'}`}>{r.mtf_score_conf}</span>
                      ) : r.turn_echo_n ? (
                        <span className="text-cyan-300 font-bold" style={{ fontSize: 16 }}>
                          {r.turn_echo_n === 1 ? '①' : r.turn_echo_n === 2 ? '②' : '③'}
                        </span>
                      ) : <span className="text-gray-700">·</span>}
                    {r.h4_rev_today && (r.rev_buy || r.brk_buy || r.mtf_score_conf != null || r.turn_echo_n)
                      ? <span className="text-cyan-300 font-bold ml-0.5" title="▲ 4H REV-trigger fired inside this daily bar — early intraday entry existed (+0.84pp vs waiting for the close, 6/6yr)">▲</span>
                      : null}
                  </td>
                )}

                {/* EDGE — validated Edge-board setup fires (last 5 bars, backtest-identical masks) */}
                {variant === 'ultra' && (<>
                  <td className="px-1 py-1 text-center whitespace-nowrap"
                    title={(r.edges ?? []).length
                      ? `Edge fires (≤5 bars): ${r.edges.join(' · ')} — plain code = today`
                      : 'No Edge-board setup fired on the last 5 bars'}>
                    {(r.edges ?? []).length ? (
                      <span className="inline-flex gap-0.5 flex-wrap justify-center">
                        {r.edges.slice(0, 3).map(e => (
                          <span key={e}
                            className={`px-1 rounded font-mono text-[10px] leading-tight ${e.includes('·')
                              ? 'bg-emerald-950 text-emerald-400/70'
                              : r.edge_rev && ['QZC', 'D+L1', 'RTB', 'P55'].includes(e)
                                ? 'bg-amber-600 text-amber-50 font-bold ring-1 ring-amber-300'
                                : 'bg-emerald-800 text-emerald-100 font-semibold ring-1 ring-emerald-400/60'}`}
                            title={r.edge_rev && ['QZC', 'D+L1', 'RTB', 'P55'].includes(e)
                              ? `EDGE🟢 premium: ${e} + same-bar 🟢REV (QZC +2.69% · D+L1 +3.46% · RTB +2.04% 6/6yr · P55 +1.89%)`
                              : undefined}>
                            {e}
                          </span>
                        ))}
                        {r.edges.length > 3 && <span className="text-emerald-300 text-[10px]">+{r.edges.length - 3}</span>}
                      </span>
                    ) : <span className="text-gray-700">·</span>}
                  </td>
                  <td className="text-center"
                    title={r.seq34
                      ? `🧬 frozen-OOS ${r.seq34.depth}-bar ${r.seq34.coarse ? 'COARSE (no-L token, ~6× denser) ' : 'exact '}sequence completed TODAY: ${r.seq34.seq} — OOS win ${r.seq34.win}% · ps_med +${r.seq34.ps_med}%${(r.seq34.dsr ?? 0) >= 0.6 ? ' · 🏆 DSR≥0.6 selection-proof' : ''}`
                      : 'no verified 2-4-bar sequence completed today'}>
                    <span className="inline-flex gap-0.5 justify-center">
                      {r.seq34 && (
                        <span className={`px-1 rounded font-mono text-[10px] leading-tight font-semibold ${(r.seq34.dsr ?? 0) >= 0.6
                          ? 'bg-violet-600 text-violet-50 font-bold ring-1 ring-violet-300'
                          : (r.seq34.win ?? 0) >= 60
                            ? 'bg-violet-800 text-violet-100 ring-1 ring-violet-400/60'
                            : 'bg-violet-950 text-violet-300/80'}`}>
                          {`🧬${r.seq34.coarse ? '°' : ''}${(r.seq34.dsr ?? 0) >= 0.6 ? '🏆' : ''}${Math.round(r.seq34.win)}`}
                        </span>
                      )}
                      {r.seq_ctx && (
                        <span className={`px-1 rounded font-mono text-[10px] leading-tight font-bold ${r.seq_ctx.kind === 'tail'
                          ? 'bg-amber-700 text-amber-50 ring-1 ring-amber-300'
                          : r.seq_ctx.dir === 'up'
                          ? ((r.seq_ctx.up ?? 0) >= 60
                              ? 'bg-teal-400 text-teal-950 ring-2 ring-teal-100'
                              : 'bg-teal-700 text-teal-50 ring-1 ring-teal-300')
                          : 'bg-rose-800 text-rose-100 ring-1 ring-rose-400'}`}
                          title={`${r.seq_ctx.kind === 'tail' ? '🎲 TAIL (rare-but-fat: mean +' + r.seq_ctx.mean + '%)' : r.seq_ctx.dir === 'up' ? '⤴ BOOSTER' : '⤵ SUPPRESSOR'} context for ${r.seq_ctx.sig.toUpperCase()} [layer ${r.seq_ctx.layer ?? 'TZ'}]: ${r.seq_ctx.seq} → fwd-20 up ${r.seq_ctx.up}% (baseline ${r.seq_ctx.base_up}%, lift ${r.seq_ctx.lift > 0 ? '+' : ''}${r.seq_ctx.lift}pp, n=${r.seq_ctx.n})`}>
                          {`${r.seq_ctx.kind === 'tail' ? '🎲' : r.seq_ctx.dir === 'up' ? '⤴' : '⤵'}${Math.round(r.seq_ctx.up)}`}
                        </span>
                      )}
                      {!r.seq34 && !r.seq_ctx && <span className="text-gray-700">·</span>}
                    </span>
                  </td>
                  <td className="px-1 py-1 text-center"
                    title={r.conf_score != null ? `CONF ${r.conf_score} — ${r.conf_top ?? ''}`
                      : r.conf_ext != null ? `CONF~ ${r.conf_ext} (info-ტიერი — sub-threshold, არავალიდირებული) — ${r.conf_ext_top ?? ''}`
                      : 'no qualified confluence cells on the latest bar'}>
                    {r.conf_score != null ? (
                      <span className={`px-1 rounded font-mono text-[11px] font-semibold ${r.conf_score >= 10
                        ? 'bg-emerald-500 text-emerald-950 font-bold ring-1 ring-emerald-200'
                        : r.conf_score >= 7 ? 'bg-emerald-800 text-emerald-100'
                        : r.conf_score >= 3 ? 'text-teal-300'
                        : r.conf_score <= -15 ? 'bg-red-600 text-red-50 font-bold ring-1 ring-red-200'
                        : r.conf_score <= -7 ? 'bg-rose-900 text-rose-200'
                        : 'text-md-on-surface-var'}`}>
                        {r.conf_score}
                      </span>
                    ) : r.conf_ext != null ? (
                      <span className="px-1 rounded font-mono text-[11px] text-zinc-500">{r.conf_ext}</span>
                    ) : <span className="text-gray-700">·</span>}
                  </td>
                  {/* 💠 GEX — options dealer context (lazy, liquid names only) */}
                  <td className="px-2 py-1 text-center"><GexScanCell ticker={r.ticker} /></td>
                  {/* ▽△ Bottom-Anatomy verdict (latest bar, shared universe map) */}
                  <td className="px-2 py-1 text-center"><AnatScanCell ticker={r.ticker} /></td>
                  {/* ⏱ ATR time-to-target forecast — computed in-cell from r.atr_pct (bars.atr_14) */}
                  {(() => {
                    const _f = r.atr_pct > 0 ? atrForecast(r.atr_pct) : null
                    return (
                      <td className="px-2 py-1 text-center"
                        title={_f
                          ? `⏱ ATR ${_f.atrPct}% → +10% typically ~${_f.up10.days}d (hit ${_f.up10.hit}% within 90 bars) · +25% ~${_f.up25.days}d · −10% ~${_f.dn10.days}d (stop-timing). OOS-calibrated volatility law — timing context, not a buy signal.`
                          : 'no ATR data'}>
                        {/* ⛔ no intraday volume event today — validated across all 29 TZ/L codes */}
                        {r.no_vol_event ? (
                          <span className="text-red-400/90" style={{ fontSize: 10, marginRight: 2 }}
                            title="⛔ NO intraday volume event today — the biggest 15m bar never reached 2.5× that session's own average. Validated across ALL 29 TZ/L signal codes: on such a day EVERY signal's median falls 4-8 points (Z9 −8.0, T1 −7.1; only Z11 resists). Rare (~3% of days) but severe → distrust today's signals on this name.">⛔</span>
                        ) : null}
                        {_f ? (
                          <span className="font-mono text-xs text-sky-300/90">{_f.up10.days}d<span className="text-sky-500/60 text-[10px]">·{_f.up10.hit}%</span></span>
                        ) : <span className="text-slate-600">—</span>}
                      </td>
                    )
                  })()}
                  {/* ⚖️ VRP — IV vs ATR-realized vol dissonance (lazy gexStore) */}
                  <td className="px-2 py-1 text-center"><VrpScanCell ticker={r.ticker} /></td>
                </>)}

                {/* BETA */}
                <td className="px-2 py-1 text-center"
                  title={r.beta_zone ? `BETA ${r.beta_score} · ${r.beta_zone}${r.beta_auto_buy ? ' ★AUTO-BUY' : ''}` : 'No BETA data'}>
                  {r.beta_score > 0 ? (
                    <div className="leading-none">
                      <span className={`font-mono text-xs ${betaZoneCls(r.beta_zone)}`}>
                        {r.beta_auto_buy ? '★ ' : ''}{r.beta_score}
                      </span>
                      <div className={`text-[9px] ${betaZoneCls(r.beta_zone)} opacity-80`}>{r.beta_zone}</div>
                    </div>
                  ) : <span className="text-gray-700">—</span>}
                </td>

                {/* V2 — PreBreakout v2 (same size as Score/ULTRA) */}
                <td className="px-2 py-1 text-center"
                  title={r.prebreak_v2 != null ? `PreBreakout v2 = ${r.prebreak_v2} (≈breakout probability) · ${r.prebreak_v2_band}` : 'No v2 data'}>
                  {r.prebreak_v2 != null ? (
                    <div className="leading-none">
                      <span className={`font-mono text-xs font-semibold ${
                        r.prebreak_v2_band === 'BUY' ? 'text-green-300'
                        : r.prebreak_v2_band === 'HOT' ? 'text-amber-300'
                        : 'text-md-on-surface-var'}`}>{r.prebreak_v2}</span>
                      {r.prebreak_v2_band !== 'WATCH' && (
                        <div className={`text-[9px] opacity-80 ${r.prebreak_v2_band === 'BUY' ? 'text-green-300' : 'text-amber-300'}`}>
                          {r.prebreak_v2_band}
                        </div>
                      )}
                    </div>
                  ) : <span className="text-gray-700">—</span>}
                </td>

                {/* V3 — additive pre-breakout cluster (0..50) + reasons on hover */}
                <td className="px-2 py-1 text-center"
                  title={r.prebreak_v3 ? `PreBreakout v3 = ${r.prebreak_v3}/50 · ${r.prebreak_v3_reasons || ''}` : 'No v3 signals'}>
                  {r.prebreak_v3 ? (
                    <span className={`font-mono text-xs font-semibold ${
                      r.prebreak_v3 >= 30 ? 'text-lime-300'
                      : r.prebreak_v3 >= 18 ? 'text-yellow-300'
                      : 'text-md-on-surface-var'}`}>{r.prebreak_v3}</span>
                  ) : <span className="text-gray-700">—</span>}
                </td>

                {/* RTB — now carries the Wyckoff-faithful phase (w2_state) with the event mark.
                    A=SC/AR · B=ST · C=SPRING (money, green) · D=SOS/LPS (markup/breakout, muted). */}
                <td className="px-2 py-1 text-center"
                  title={r.rtb_phase ? `Phase ${r.rtb_phase}${r.rtb_mark ? ' · ' + r.rtb_mark : ''}${
                    r.rtb_phase === 'C' ? ' (SPRING — buy the shakeout, the money phase)' :
                    r.rtb_phase === 'D' ? ' (SOS/LPS markup/breakout — validated weak, chasing)' :
                    r.rtb_phase === 'A' ? ' (SC/AR stopping action — watch with oversold)' :
                    r.rtb_phase === 'B' ? ' (ST — building the range)' : ''}` : ''}>
                  {r.rtb_phase && r.rtb_phase !== '0' ? (
                    <div className="leading-none">
                      <span className={`inline-block font-bold text-[10px] px-1 rounded ${
                        r.rtb_phase === 'C' ? 'bg-emerald-700/80 text-emerald-100 ring-1 ring-emerald-400' :
                        r.rtb_phase === 'A' ? 'bg-amber-800/70 text-amber-200' :
                        r.rtb_phase === 'B' ? 'bg-gray-700/60 text-gray-300' :
                        'bg-orange-900/40 text-orange-300/70'
                      }`}>{r.rtb_phase}</span>
                      {r.rtb_mark && (
                        <div className="text-[8px] text-md-on-surface-var/70 mt-0.5">{r.rtb_mark}</div>
                      )}
                    </div>
                  ) : <span className="text-gray-700">—</span>}
                </td>

                {/* T/Z */}
                <td className="px-2 py-1 text-center">
                  {r.tz_sig ? (
                    <TZChip label={r.tz_sig} />
                  ) : <span className="text-gray-700">—</span>}
                  {r.tzt4_match && r.tzt4_tier && (
                    <div
                      title={`T-Z-T4: ${r.tzt4_tier} · ${r.tzt4_suffix || '?'} · RSI ${r.tzt4_rsi}${r.tzt4_age > 0 ? ` · ${r.tzt4_age}d ago` : ''}`}
                      className={`mt-0.5 text-[9px] font-bold px-1 rounded border leading-tight ${
                        r.tzt4_tier === 'T1' ? 'text-emerald-300 border-emerald-700 bg-emerald-950/60' :
                        r.tzt4_tier === 'T2' ? 'text-teal-300 border-teal-700 bg-teal-950/60' :
                        r.tzt4_tier === 'T3' ? 'text-cyan-300 border-cyan-700 bg-cyan-950/60' :
                                               'text-slate-400 border-slate-700 bg-slate-950/60'
                      }`}>
                      🎯{r.tzt4_tier}{(r.tzt4_suffix==='EBA'||r.tzt4_suffix==='EUR')&&(r.tzt4_rsi||0)>=60?'★':''}
                    </div>
                  )}
                  {r.ttt6_match && r.ttt6_tier && (
                    <div
                      title={`T-T-T6: ${r.ttt6_tier} · ${r.ttt6_suffix || '?'} · RSI ${r.ttt6_rsi}${r.ttt6_age > 0 ? ` · ${r.ttt6_age}d ago` : ''}`}
                      className={`mt-0.5 text-[9px] font-bold px-1 rounded border leading-tight ${
                        r.ttt6_tier === 'T1' ? 'text-violet-300 border-violet-700 bg-violet-950/60' :
                        r.ttt6_tier === 'T2' ? 'text-purple-300 border-purple-700 bg-purple-950/60' :
                        r.ttt6_tier === 'T3' ? 'text-fuchsia-300 border-fuchsia-700 bg-fuchsia-950/60' :
                                               'text-slate-400 border-slate-700 bg-slate-950/60'
                      }`}>
                      🔺{r.ttt6_tier}{(r.ttt6_suffix==='EBA'||r.ttt6_suffix==='EUR')&&(r.ttt6_rsi||0)>=60?'★':''}
                    </div>
                  )}
                  {r.t1seq_match && r.t1seq_tier && (
                    <div
                      title={`T1-seq: ${r.t1seq_tier === 'T1' ? 'Z-Z→T1' : r.t1seq_tier === 'T2' ? 'T-Z→T1' : r.t1seq_tier === 'T3' ? 'Z-T→T1' : 'T-T→T1'} · ${r.t1seq_suffix || '?'} · RSI ${r.t1seq_rsi}${r.t1seq_age > 0 ? ` · ${r.t1seq_age}d ago` : ''}`}
                      className={`mt-0.5 text-[9px] font-bold px-1 rounded border leading-tight ${
                        r.t1seq_tier === 'T1' ? 'text-amber-300 border-amber-700 bg-amber-950/60' :
                        r.t1seq_tier === 'T2' ? 'text-orange-300 border-orange-700 bg-orange-950/60' :
                        r.t1seq_tier === 'T3' ? 'text-yellow-300 border-yellow-700 bg-yellow-950/60' :
                                                'text-slate-400 border-slate-700 bg-slate-950/60'
                      }`}>
                      ⚡{r.t1seq_tier === 'T1' ? 'ZZ' : r.t1seq_tier === 'T2' ? 'TZ' : r.t1seq_tier === 'T3' ? 'ZT' : 'TT'}·T1{(r.t1seq_suffix==='EBA'||r.t1seq_suffix==='EUR')&&(r.t1seq_rsi||0)>=60?'★':''}
                    </div>
                  )}
                  {r.t3seq_match && r.t3seq_tier && (
                    <div
                      title={`T3·35: ${r.t3seq_tier} · ${r.t3seq_suffix || '?'} · RSI ${r.t3seq_rsi}${r.t3seq_age > 0 ? ` · ${r.t3seq_age}d ago` : ''}`}
                      className={`mt-0.5 text-[9px] font-bold px-1 rounded border leading-tight ${
                        r.t3seq_tier === 'fresh-nbi' ? 'text-amber-300 border-amber-600 bg-amber-950/60' :
                        r.t3seq_tier === 'fresh'     ? 'text-orange-300 border-orange-700 bg-orange-950/60' :
                        r.t3seq_tier === 'streak'    ? 'text-blue-300 border-blue-700 bg-blue-950/60' :
                                                       'text-slate-400 border-slate-700 bg-slate-950/60'
                      }`}>
                      🟡T3{r.t3seq_tier === 'fresh-nbi' ? '★NBI' : r.t3seq_tier === 'fresh' ? `↑${r.t3seq_suffix || '?'}` : r.t3seq_tier === 'streak' ? '×3' : `·${r.t3seq_suffix || '?'}`}
                    </div>
                  )}
                  {r.t9rsi_match && r.t9rsi_tier && (
                    <div
                      title={`T9·35: ${r.t9rsi_tier} · ${r.t9rsi_suffix || '?'} · RSI ${r.t9rsi_rsi}${r.t9rsi_age > 0 ? ` · ${r.t9rsi_age}d ago` : ''}`}
                      className={`mt-0.5 text-[9px] font-bold px-1 rounded border leading-tight ${
                        r.t9rsi_tier === 'premium' ? 'text-teal-300 border-teal-600 bg-teal-950/60' :
                                                     'text-cyan-400 border-cyan-800 bg-cyan-950/40'
                      }`}>
                      🔵T9↓{r.t9rsi_rsi}{r.t9rsi_tier === 'premium' ? '★' : ''}
                    </div>
                  )}
                  {r.z1gt2g_match && r.z1gt2g_tier && (
                    <div
                      title={`Z1G→EUR: Z1G→T1(${r.z1gt2g_suffix || '?'})→T2G·EUR · RSI ${r.z1gt2g_rsi}${r.z1gt2g_age > 0 ? ` · ${r.z1gt2g_age}d ago` : ''} · ${r.z1gt2g_tier}`}
                      className={`mt-0.5 text-[9px] font-bold px-1 rounded border leading-tight ${
                        r.z1gt2g_tier === 'premium' ? 'text-emerald-300 border-emerald-600 bg-emerald-950/60' :
                        r.z1gt2g_tier === 'hi-nha'  ? 'text-green-300 border-green-700 bg-green-950/60' :
                        r.z1gt2g_tier === 'hi-edp'  ? 'text-lime-300 border-lime-700 bg-lime-950/60' :
                                                      'text-slate-400 border-slate-700 bg-slate-950/60'
                      }`}>
                      🟢Z1G→EUR{r.z1gt2g_tier === 'premium' ? '★' : r.z1gt2g_tier === 'hi-nha' ? '·NHA' : r.z1gt2g_tier === 'hi-edp' ? '·EDP' : ''}
                    </div>
                  )}
                  {r.vol3t5_match && (
                    <div
                      title={`T5·Vol↑↑↑: T5+3bar rising vol+RSI drop ${r.vol3t5_drop}pt · RSI ${r.vol3t5_rsi}${r.vol3t5_age > 0 ? ` · ${r.vol3t5_age}d ago` : ''} · absorption+recovery`}
                      className="mt-0.5 text-[9px] font-bold px-1 rounded border leading-tight text-teal-300 border-teal-700 bg-teal-950/60">
                      📈T5·Vol
                    </div>
                  )}
                  {r.vol3t9_match && (
                    <div
                      title={`T9·Vol↑↑↑: T9+3bar rising vol · RSI ${r.vol3t9_rsi}${r.vol3t9_age > 0 ? ` · ${r.vol3t9_age}d ago` : ''} · ${r.vol3t9_tier === 'premium' ? 'RSI30-35 PREMIUM (exp+1.43%)' : 'base RSI25-40'}`}
                      className={`mt-0.5 text-[9px] font-bold px-1 rounded border leading-tight ${
                        r.vol3t9_tier === 'premium' ? 'text-sky-200 border-sky-500 bg-sky-950/60' : 'text-sky-400 border-sky-700 bg-sky-950/40'
                      }`}>
                      📈T9·Vol{r.vol3t9_tier === 'premium' ? '★' : ''}
                    </div>
                  )}
                  {r.vol3t12_match && (
                    <div
                      title={`T12·Vol↑↑↑: T12+3bar rising vol+RSI drop · RSI ${r.vol3t12_rsi}${r.vol3t12_age > 0 ? ` · ${r.vol3t12_age}d ago` : ''} · ${r.vol3t12_tier}`}
                      className={`mt-0.5 text-[9px] font-bold px-1 rounded border leading-tight ${
                        r.vol3t12_tier === 'premium' ? 'text-violet-200 border-violet-500 bg-violet-950/60' :
                        r.vol3t12_tier === 'hi-rsi'  ? 'text-violet-300 border-violet-600 bg-violet-950/50' :
                        r.vol3t12_tier === 'hi-2bar' ? 'text-purple-300 border-purple-700 bg-purple-950/50' :
                                                       'text-slate-400 border-slate-700 bg-slate-950/60'
                      }`}>
                      📈T12·Vol{r.vol3t12_tier === 'premium' ? '★' : r.vol3t12_tier === 'hi-rsi' ? '·R' : r.vol3t12_tier === 'hi-2bar' ? '·2↓' : ''}
                    </div>
                  )}
                </td>

                {/* Category */}
                <td className="px-2 py-1 text-center">
                  {r.profile_category ? (
                    <StatusChip cat={r.profile_category} />
                  ) : <span className="text-gray-700">—</span>}
                </td>

                {/* Signals — every chip rendered, wraps inside cell. No static "+N more". */}
                <td className="px-2 py-1 align-middle">
                  <SignalChipList signals={allSigs.map(s => s.label)} mode="table" />
                </td>

                {/* ABR */}
                <td className="px-2 py-1 text-center">
                  {r.abr?.category && r.abr.category !== 'UNKNOWN' ? (
                    <span className={`text-[10px] font-mono ${
                      r.abr.category === 'A'  ? 'text-emerald-300' :
                      r.abr.category === 'B+' ? 'text-cyan-300' :
                      r.abr.category === 'B'  ? 'text-blue-300' :
                      r.abr.category === 'R'  ? 'text-red-400' :
                      'text-md-on-surface-var'
                    }`}>{r.abr.category}</span>
                  ) : <span className="text-[10px] text-md-on-surface-var/50 italic">n/a</span>}
                </td>

                {/* RSI */}
                <td className={`px-2 py-1 text-right font-mono text-xs ${
                  r.rsi <= 35 ? 'text-lime-400' : r.rsi >= 70 ? 'text-red-400' : 'text-md-on-surface-var'
                }`}>
                  {r.rsi != null ? fmt(r.rsi, 0) : '—'}
                </td>

                {/* CCI */}
                <td className={`px-2 py-1 text-right font-mono text-xs ${
                  r.cci >= 100 ? 'text-lime-400' : r.cci <= -100 ? 'text-red-400' : 'text-md-on-surface-var'
                }`}>
                  {r.cci != null ? fmt(r.cci, 0) : '—'}
                </td>

                {/* Price */}
                <td className="px-2 py-1 text-right font-mono text-xs text-md-on-surface">
                  ${fmt(r.last_price)}
                </td>

                {/* % change */}
                <td className={`px-2 py-1 text-right font-mono text-xs ${chg >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
                  {chg >= 0 ? '+' : ''}{fmt(chg)}%
                </td>

                {/* Real-time intraday % (ultra only) */}
                {variant === 'ultra' && (() => {
                  const pm = pmData[r.ticker]
                  const pct = pm?.rt_chg_pct
                  if (pct == null) {
                    return <td className="px-2 py-1 text-right font-mono text-xs text-gray-600">—</td>
                  }
                  const clr = pct >= 0 ? 'text-sky-300' : 'text-rose-400'
                  return (
                    <td className={`px-2 py-1 text-right font-mono text-xs ${clr}`}
                        title={`RT: ${pm.rt_price != null ? '$' + pm.rt_price + ' ' : ''}(${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%) vs prev close`}>
                      {pct >= 0 ? '+' : ''}{pct.toFixed(2)}%
                    </td>
                  )
                })()}

                {/* Pre-market % (ultra only) */}
                {variant === 'ultra' && (() => {
                  const pm = pmData[r.ticker]
                  if (!pm || pm.pm_price == null) {
                    return <td className="px-2 py-1 text-right font-mono text-xs text-gray-600">—</td>
                  }
                  const pct = pm.pm_chg_pct
                  const clr = pct == null ? 'text-gray-500'
                            : pct >= 0   ? 'text-violet-300'
                            :              'text-rose-400'
                  return (
                    <td className={`px-2 py-1 text-right font-mono text-xs ${clr}`}
                        title={`PM: $${pm.pm_price}${pct != null ? ` (${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%)` : ''}`}>
                      {pct != null ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%` : `$${pm.pm_price}`}
                    </td>
                  )
                })()}

                {/* Split */}
                {universe === 'split' && (
                  <td className="px-2 py-1 text-center font-mono text-xs">
                    {r.split_date ? (() => {
                      const ph = r.split_phase || ''
                      const wave = r.split_wave || ''
                      const doff = r.split_days_offset ?? 0
                      const dLabel = doff === 0 ? 'D0' : doff > 0 ? `D+${doff}` : `D${doff}`
                      return (
                        <span className="text-md-on-surface-var">
                          {r.split_ratio} <span className="opacity-60">{wave}</span> <span>{dLabel}</span>
                        </span>
                      )
                    })() : '—'}
                  </td>
                )}
                {/* Zone — position-in-band gauge + spike× + age */}
                {universe === 'zone' && (
                  <td className="px-2 py-1 font-mono text-xs">
                    {(r.zone_low != null && r.zone_high != null) ? (() => {
                      const pos  = Math.max(0, Math.min(1, r.zone_pos ?? 0.5))   // 0=floor 1=ceil
                      const bull = r.zone_dir === 'bull'
                      const dotCls = bull ? 'bg-emerald-400' : 'bg-rose-400'
                      return (
                        <div className="flex items-center gap-1.5" title={`zone ${r.zone_low}–${r.zone_high} · spike ${r.zone_mult}× · ${r.zone_age_days}d old · close ${(pos*100).toFixed(0)}% up the band`}>
                          <div className="relative h-1.5 w-14 rounded bg-white/10">
                            <div className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 h-2.5 w-1.5 rounded-sm"
                                 style={{ left: `${pos*100}%` }}>
                              <div className={`h-full w-full rounded-sm ${dotCls}`} />
                            </div>
                          </div>
                          <span className="text-emerald-300/80">×{r.zone_mult}</span>
                          <span className="text-md-on-surface-var/60">{r.zone_age_days}d</span>
                        </div>
                      )
                    })() : '—'}
                  </td>
                )}
              </tr>
            ]
          })}
        </tbody>
      </table>
    </div>
  )
}
