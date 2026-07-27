import { useEffect, useState, useCallback, useRef, useReducer } from 'react'
import { pwlAdd } from './PersonalWatchlistPanel'
import { downloadTV } from '../utils/setupsJournal'
import MiniChartPopup from './MiniChartPopup'

// ── 💠 GEX (options gamma) context — module-level batched store + self-filling tag ──
// Isolated options layer (2026-07-22). A row drops <GexTag ticker/> which lazily
// requests GEX via a debounced batch (capped, server-cached 10min) and renders a
// compact regime badge. Only optionable names light up; everything else stays blank.
const _gexCache = {}          // ticker → {available, regime, atm_iv, near, ...} | null (in-flight)
const _gexPending = new Set()
const _gexSubs = new Set()
let _gexTimer = null
function _flushGex() {
  const batch = [..._gexPending].slice(0, 30)
  _gexPending.clear()
  if (!batch.length) return
  batch.forEach(t => { if (_gexCache[t] === undefined) _gexCache[t] = null })
  fetch(`/api/gex-batch?tickers=${batch.join(',')}`).then(r => r.json())
    .then(d => { Object.assign(_gexCache, d.gex || {}); _gexSubs.forEach(fn => fn()) })
    .catch(() => {})
}
function _requestGex(ticker) {
  if (!ticker || _gexCache[ticker] !== undefined) return
  _gexPending.add(ticker)
  clearTimeout(_gexTimer)
  _gexTimer = setTimeout(_flushGex, 350)
}
function GexTag({ ticker }) {
  const [, force] = useReducer(x => x + 1, 0)
  useEffect(() => {
    _requestGex(ticker)
    _gexSubs.add(force)
    return () => { _gexSubs.delete(force) }
  }, [ticker])
  const g = _gexCache[ticker]
  if (!g || !g.available) return null
  const neg = g.regime === 'negative'
  const nearLabel = { put_wall: '🎯Pwall', call_wall: '🎯Cwall', power_zone: '🎯PZ', gamma_flip: '🎯flip' }[g.near]
  return (
    <span className={`ml-1 text-[8px] font-mono px-1 rounded border align-middle ${
      neg ? 'border-rose-600/50 text-rose-300/90 bg-rose-950/40' : 'border-emerald-700/50 text-emerald-300/90 bg-emerald-950/40'}`}
      title={`GEX ${g.regime}-γ · ATM IV ${g.atm_iv ?? '—'}%${g.near ? ' · price hugging ' + g.near : ''} · put-wall ${g.put_wall ?? '—'} / call-wall ${g.call_wall ?? '—'}`}>
      {neg ? '⚡' : '🛡'}{g.atm_iv != null ? Math.round(g.atm_iv) : ''}{nearLabel ? ' ' + nearLabel : ''}
    </span>
  )
}

/**
 * ✅ Edge Board — the de-noised spine.
 * Shows ONLY the two 5-year path-sim-validated setups, separated and labeled honestly:
 *   🔥 PREMIUM — Capit→Atomic confluence (weak-close gap-up that follows a recent B+ capit).
 *                win 67% · med +4.24% vs +1.41% baseline.
 *   ✅ CORE    — Weak-close gap-up (the atomic edge). exp +0.84 sp500 / +0.70 r2k · positive 5/6 yr.
 * Reuses the existing /api/atomic-scan endpoint (read-only — no backend change).
 * This board exists to surface the few signals that survived validation, NOT to add new ones.
 */

const ATOM_CLS = {
  'close=O': 'bg-sky-900/50 text-sky-200 border-sky-700/50',
  'gap': 'bg-violet-900/50 text-violet-200 border-violet-700/50',
  'R2L': 'bg-emerald-900/50 text-emerald-200 border-emerald-700/50',
  'EO': 'bg-amber-900/40 text-amber-200 border-amber-700/50',
  'vol=B': 'bg-teal-900/50 text-teal-200 border-teal-700/50',
  'wick=D': 'bg-rose-900/40 text-rose-200 border-rose-700/40',
  'G3': 'bg-fuchsia-900/50 text-fuchsia-200 border-fuchsia-700/50',
}
const REG_CLS = {
  RISK_ON: 'bg-emerald-900/50 text-emerald-200 border-emerald-600',
  NEUTRAL: 'bg-amber-900/40 text-amber-200 border-amber-700/50',
  RISK_OFF: 'bg-rose-900/50 text-rose-200 border-rose-600',
}
const scoreCls = (s) => s >= 90 ? 'text-emerald-300' : s >= 70 ? 'text-lime-300' : s >= 55 ? 'text-amber-300' : 'text-md-on-surface-var'

// Live price cell (shared by the custom setup tables)
function LiveCell({ p }) {
  if (!p) return <span className="text-md-on-surface-var/30">—</span>
  return (
    <span>${p.price}{p.change_pct != null && (
      <span className={`ml-1 text-[10px] ${p.change_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
        {p.change_pct >= 0 ? '+' : ''}{p.change_pct}%
      </span>
    )}</span>
  )
}

// displacement-in-ATR cell — sweet-spot (0.5–1.5) lime, exhaustion (>1.5) orange ⚠
function DispCell({ r }) {
  if (r.disp_atr == null) return <td className="text-right px-2 py-1.5 font-mono text-[10px] text-md-on-surface-var/40">—</td>
  const cls = r.gap_band === 'sweet' ? 'text-lime-300 font-bold'
    : r.gap_band === 'exhaust' ? 'text-orange-400' : 'text-md-on-surface-var/50'
  const tip = r.gap_band === 'sweet' ? 'displacement sweet-spot 0.5–1.5×ATR (the edge zone)'
    : r.gap_band === 'exhaust' ? 'exhaustion >1.5×ATR — blow-off, weaker (3/6yr)' : 'small displacement (G2 / <0.5×ATR)'
  return (
    <td className="text-right px-2 py-1.5 font-mono text-[10px]" title={tip}>
      <span className={cls}>{r.disp_atr}×{r.gap_band === 'exhaust' ? '⚠' : ''}</span>
    </td>
  )
}

// reusable disp column for EdgeTable extras (inner content only — EdgeTable wraps the <td>)
const DISP_EXTRA = {
  header: 'disp', align: 'right', title: 'overnight displacement |open−prev_close| in ATR · sweet 0.5–1.5×',
  cell: r => r.disp_atr == null ? '—'
    : <span className={r.gap_band === 'sweet' ? 'text-lime-300 font-bold' : r.gap_band === 'exhaust' ? 'text-orange-400' : 'text-md-on-surface-var/50'}>{r.disp_atr}×{r.gap_band === 'exhaust' ? '⚠' : ''}</span>,
}

// Fibonacci price-zone law (mirrors backend price_zones.py). win%↑ & catastrophe%↓ with
// price; mean humps at $21-89. $21 is a hard trade/no-trade cliff. G3 keeps a $8-10 spill.
function zoneOf(px, setup) {
  if (px == null) return null
  if (setup === 'g3' && px >= 8 && px < 10) return { z: 'g3-spill', e: '⚡', c: 'text-amber-300', t: 'G3 cheap-momentum spillover ($8-10) — the one cheap exception' }
  if (px < 1)   return { z: 'casino',  e: '🎰', c: 'text-fuchsia-400', t: 'CASINO <$1 — median −7..−13%, catastrophe 33-43% (moonshot lottery, avoid)' }
  if (px < 8)   return { z: 'knife',   e: '🔪', c: 'text-red-400',     t: 'KNIFE $1-8 — negative median, catastrophe 24-35% (avoid)' }
  if (px < 21)  return { z: 'dead',    e: '💀', c: 'text-orange-400/80', t: 'DEAD $8-21 — negative median, no edge until the $21 cliff' }
  if (px < 89)  return { z: 'quality', e: '✅', c: 'text-emerald-400',  t: 'QUALITY $21-89 — peak risk-adjusted return (win 59-62%, cat ~1-2%)' }
  if (px < 377) return { z: 'safe',    e: '🛡', c: 'text-teal-300',     t: 'SAFE $89-377 — reliable, low catastrophe, smaller % moves' }
  return { z: 'thin', e: '❄️', c: 'text-sky-300/70', t: '>$377 — thinnest/safest, mega-cap (lowest catastrophe)' }
}
const ZONE_META = {
  casino: { e: '🎰', c: 'text-fuchsia-400', t: 'CASINO <$1 — median −7..−13%, catastrophe 33-43% (lottery, avoid)' },
  knife:  { e: '🔪', c: 'text-red-400', t: 'KNIFE $1-8 — negative median, catastrophe 24-35% (avoid)' },
  dead:   { e: '💀', c: 'text-orange-400/80', t: 'DEAD $8-21 — negative median, no edge until the $21 cliff' },
  quality:{ e: '✅', c: 'text-emerald-400', t: 'QUALITY $21-89 — peak risk-adjusted return' },
  safe:   { e: '🛡', c: 'text-teal-300', t: 'SAFE $89-377 — reliable, low catastrophe' },
  thin:   { e: '❄️', c: 'text-sky-300/70', t: '>$377 — thinnest/safest, mega-cap' },
  'g3-spill': { e: '⚡', c: 'text-amber-300', t: 'G3 cheap-momentum spillover ($8-10) — the one cheap exception' },
}
// Prefer the backend-classified zone (handles the G3 $8-10 exception); else derive from price.
function ZoneBadge({ r }) {
  const z = (r?.price_zone && ZONE_META[r.price_zone]) ? ZONE_META[r.price_zone] : zoneOf(r?.close)
  if (!z) return null
  const e = z.e, c = z.c, t = z.t
  return <span className={`ml-1 ${c}`} title={t}>{e}</span>
}

function SetupRow({ r, live, liveLoading, onSelectTicker, accent, onTkEnter, onTkLeave }) {
  const isPrem = (r.atoms || []).some(a => a.includes('premium'))   // ★premium = R2L+G3+RSI<45 (+2.45%)
  return (
    <tr className={`border-t border-white/5 hover:bg-white/[0.03] ${accent ? 'bg-amber-500/[0.06] border-l-2 border-l-amber-500' : isPrem ? 'bg-emerald-500/[0.07] border-l-2 border-l-emerald-500' : ''}`}>
      <td className={`text-right px-2 py-1.5 font-mono font-bold ${scoreCls(r.score)}`}>{r.score}</td>
      <td className="px-2 py-1.5">
        <button onClick={() => onSelectTicker?.(r.ticker)} onMouseEnter={e => onTkEnter?.(e, r)} onMouseLeave={onTkLeave} className="font-mono font-semibold hover:text-sky-300">{r.ticker}</button><GexTag ticker={r.ticker} />
      </td>
      <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var/70">{r.universe}</td>
      <td className="px-2 py-1.5 font-mono text-[10px] text-sky-300/80">{r.t_sig}</td>
      <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var whitespace-nowrap">{r.close != null ? '$' + r.close : '—'}<ZoneBadge r={r} /></td>
      <td className="text-right px-2 py-1.5 font-mono">
        {live[r.ticker]
          ? <span>${live[r.ticker].price}{live[r.ticker].change_pct != null && <span className={`ml-1 text-[10px] ${live[r.ticker].change_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{live[r.ticker].change_pct >= 0 ? '+' : ''}{live[r.ticker].change_pct}%</span>}</span>
          : <span className="text-md-on-surface-var/30">{liveLoading ? '…' : '—'}</span>}
      </td>
      <td className={`text-right px-2 py-1.5 font-mono ${r.rsi != null && r.rsi <= 35 ? 'text-emerald-300' : 'text-md-on-surface-var'}`}>{r.rsi ?? '—'}</td>
      <DispCell r={r} />
      <td className="px-3 py-1.5">
        <div className="flex flex-wrap gap-1">
          {r.atoms.map((a, i) => <span key={i} className={`text-[9px] font-mono px-1 rounded border ${a.startsWith('🔥') ? 'bg-amber-500/25 border-amber-400 text-amber-200 font-bold' : (ATOM_CLS[a] || 'border-white/10')}`}>{a}</span>)}
        </div>
      </td>
      <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var/70">{r.dv_m != null ? r.dv_m + 'M' : '—'}</td>
      <td className="text-right px-2 py-1.5 text-md-on-surface-var/70 font-mono" title={`${r.age_days}d ago`}>{r.signal_date}</td>
      <td className="px-2 py-1.5 text-center">
        <button onClick={() => pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.close, tz_sig: 'edge:' + r.atoms.join('+') })} title="Add to Watchlist"
          className="px-1 rounded border border-emerald-700/40 text-[10px] text-emerald-300 hover:bg-emerald-900/30">★</button>
      </td>
    </tr>
  )
}

// 🕯️ mid-close toggle — shared by the L43-TRIPLE and G3-Abs cards (the two setups the gate was
// DSR-proven on). Filters to fires whose close sits in the middle 38-62% of the bar's own range.
const MidChip = ({ on, set, n, title }) => (
  <button title={title} disabled={!n} onClick={() => set(v => !v)}
    className={`px-1.5 py-0.5 rounded border font-mono text-[10px] transition-colors ${on ? 'border-orange-400 bg-orange-500/20 text-orange-100' : n ? 'border-orange-700/40 text-orange-300/80 hover:border-orange-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
    🕯️ mid<span className="opacity-50"> {n}</span>
  </button>
)

const EDGE_SECS = [
  ['all', 'ALL'], ['conf', '🔗 თანხვედრა'], ['gem1', '🏆 GEM1'], ['z11', '🎯 Z11-fam'],
  ['g3', '⚡ G3'], ['l43', '🧩 L43'], ['core', '✅ CORE'], ['capitatom', '🔥 Capit→Atom'],
  ['engulf', '🥊 Engulf'], ['absorbp', '🪤 Absorb→P'], ['dl1', '🔄 D+L1'], ['spring', '🌀 Spring'],
  ['t6sc', '🎋 T6-SC'], ['sweep', '🕳️ Sweep'], ['mtf', '📐 MTF-EMA'], ['washout', '🌊 Washout'],
  ['h1bot', '🕐 1H-Bottom'], ['qzcapit', '🎯 QZ-Capit'], ['cluster', '🎯 Cluster'], ['g3abs', '⚡ G3-Abs'], ['p55', '🧬 P55'], ['parabola', '📈 Parabola'],
  ['zoneretest', '🔁 Zone-Retest'], ['highbase', '🧗 High-Base'], ['goga', '🥊 Goga'], ['radar', '🎆 Radar'],
  ['zabsorb', '💤 Z-Absorb'], ['l34cont', '🏆 L34→L34'],
]

function SetupTable({ rows, live, liveLoading, onSelectTicker, accent, emptyMsg, onToggleDate, dateArrow, onTkEnter, onTkLeave, wlPrefix = 'setup' }) {
  return (
    <table className="w-full text-xs border border-white/10 rounded overflow-hidden mb-1">
      <thead className="bg-md-surface-high text-md-on-surface-var">
        <tr>
          <th className="text-right px-2 py-1.5">score</th>
          <th className="text-left px-2 py-1.5">ticker</th>
          <th className="text-left px-2 py-1.5">univ</th>
          <th className="text-left px-2 py-1.5">sig</th>
          <th className="text-right px-2 py-1.5">close</th>
          <th className="text-right px-2 py-1.5">live</th>
          <th className="text-right px-2 py-1.5">rsi</th>
          <th className="text-right px-2 py-1.5" title="overnight displacement |open−prev_close| in ATR units · sweet-spot 0.5–1.5×">disp</th>
          <th className="text-left px-3 py-1.5">atoms</th>
          <th className="text-right px-2 py-1.5">$vol</th>
          <th className="text-right px-2 py-1.5"><button onClick={onToggleDate} className="hover:text-sky-300" title="sort by date">date{dateArrow}</button></th>
          <th className="text-center px-2 py-1.5">
            {rows.length > 0 && (
              <button onClick={() => downloadTV(`edge_${wlPrefix}_${new Date().toISOString().slice(0, 10)}.txt`,
                  [{ name: `${wlPrefix} ${new Date().toISOString().slice(0, 10)}`, tickers: [...new Set(rows.map(r => r.ticker))] }])}
                title={`Download ${new Set(rows.map(r => r.ticker)).size} tickers → TradingView watchlist .txt`}
                className="px-1 rounded border border-sky-700/50 text-[10px] text-sky-300 hover:bg-sky-900/30">⬇</button>
            )}
          </th>
        </tr>
      </thead>
      <tbody>
        {/* key: ticker alone collides when a name fires on several dates → React duplicate-key
            rendering artifacts (repeated/omitted rows when a filter re-renders the list) */}
        {rows.map((r, i) => (
          <SetupRow key={`${r.ticker}|${r.signal_date || r.date || ''}|${i}`} r={r} live={live} liveLoading={liveLoading} onSelectTicker={onSelectTicker} accent={accent} onTkEnter={onTkEnter} onTkLeave={onTkLeave} />
        ))}
        {!rows.length && <tr><td colSpan={12} className="px-3 py-4 text-center text-md-on-surface-var/50">{emptyMsg}</td></tr>}
      </tbody>
    </table>
  )
}

// Uniform Edge setup table — IDENTICAL leading columns for every list, left→right:
//   score · ticker · univ · tier · TZ · L · RSI · close · live · [setup extras] · atoms · date · ★
// tz/L auto-resolve from whatever field a scanner uses. `extras` = per-setup columns.
function EdgeTable({ rows, accent = {}, extras = [], live, onSelectTicker, onTkEnter, onTkLeave, onToggleDate, dateArrow, emptyMsg, wlPrefix = 'edge' }) {
  const tzOf = r => r.t_sig || r.z_sig || r.tz || r.resolve || r.anchor || r.p_type || '—'
  const lOf = r => r.l_sig || r.l || r.entry_l || '—'
  const ncol = 11 + extras.length
  return (
    <table className="w-full text-xs border border-white/10 rounded overflow-hidden mb-1">
      <thead className="bg-md-surface-high text-md-on-surface-var">
        <tr>
          <th className="text-right px-2 py-1.5">score</th>
          <th className="text-left px-2 py-1.5">ticker</th>
          <th className="text-left px-2 py-1.5">univ</th>
          <th className="text-center px-2 py-1.5">tier</th>
          <th className="text-left px-2 py-1.5" title="active T/Z (or P) signal">TZ</th>
          <th className="text-left px-2 py-1.5" title="WLNBB volume line">L</th>
          <th className="text-right px-2 py-1.5">rsi</th>
          <th className="text-right px-2 py-1.5">close</th>
          <th className="text-right px-2 py-1.5">live</th>
          {extras.map((e, i) => <th key={i} className={`px-2 py-1.5 ${e.align === 'right' ? 'text-right' : e.align === 'center' ? 'text-center' : 'text-left'}`} title={e.title}>{e.header}</th>)}
          <th className="text-left px-3 py-1.5">atoms</th>
          <th className="text-right px-2 py-1.5"><button onClick={onToggleDate} className="hover:text-sky-300" title="sort by date">date{dateArrow}</button></th>
          <th className="text-center px-2 py-1.5 whitespace-nowrap">
            {rows.length > 0 && (<>
              <button onClick={() => rows.forEach(r => pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.close, tz_sig: wlPrefix + ':' + (r.atoms || []).join('+') }))}
                title={`Add all ${new Set(rows.map(r => r.ticker)).size} to the app Watchlist`}
                className="px-1 rounded border border-emerald-700/50 text-[10px] text-emerald-300 hover:bg-emerald-900/30 mr-0.5">★{rows.length}</button>
              <button onClick={() => downloadTV(`edge_${wlPrefix}_${new Date().toISOString().slice(0, 10)}.txt`,
                  [{ name: `${wlPrefix} ${new Date().toISOString().slice(0, 10)}`, tickers: [...new Set(rows.map(r => r.ticker))] }])}
                title={`Download ${new Set(rows.map(r => r.ticker)).size} tickers → TradingView watchlist .txt`}
                className="px-1 rounded border border-sky-700/50 text-[10px] text-sky-300 hover:bg-sky-900/30">⬇</button>
            </>)}
          </th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => {
          const isPrem = r.tier === 'premium' || (r.atoms || []).some(a => a.includes('premium'))
          const tierTxt = r.tier || (isPrem ? '★' : '—')
          return (
            // key: ticker alone collides on multi-date fires → duplicate-key render artifacts
            <tr key={`${r.ticker}|${r.signal_date || r.date || ''}|${i}`} className={`border-t border-white/5 hover:bg-white/[0.03] ${isPrem ? (accent.rowPrem || '') : ''}`}>
              <td className={`text-right px-2 py-1.5 font-mono font-bold ${scoreCls(r.score)}`}>{r.score}</td>
              <td className="px-2 py-1.5"><button onClick={() => onSelectTicker?.(r.ticker)} onMouseEnter={e => onTkEnter?.(e, r)} onMouseLeave={onTkLeave} className={`font-mono font-semibold ${accent.hover || 'hover:text-sky-300'}`}>{r.ticker}</button><GexTag ticker={r.ticker} /></td>
              <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var/70">{r.universe}</td>
              <td className="text-center px-2 py-1.5"><span className={`text-[9px] font-bold px-1 rounded border ${(tierTxt === 'premium' || tierTxt === '★') ? (accent.badge || 'text-amber-200 border-amber-400 bg-amber-950/50') : 'text-slate-400 border-slate-600'}`}>{tierTxt}</span></td>
              <td className={`px-2 py-1.5 font-mono text-[10px] ${accent.tz || 'text-sky-300/80'}`}>{tzOf(r)}</td>
              <td className="px-2 py-1.5 font-mono text-[10px] text-md-on-surface-var/80">{lOf(r)}</td>
              <td className={`text-right px-2 py-1.5 font-mono ${r.rsi != null && r.rsi < 35 ? 'text-emerald-300 font-bold' : 'text-md-on-surface-var'}`}>{r.rsi ?? '—'}</td>
              <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var whitespace-nowrap">{r.close != null ? '$' + r.close : '—'}<ZoneBadge r={r} /></td>
              <td className="text-right px-2 py-1.5 font-mono"><LiveCell p={live[r.ticker]} /></td>
              {extras.map((e, i) => <td key={i} className={e.cellClass ? e.cellClass(r) : `px-2 py-1.5 font-mono text-[10px] ${e.align === 'right' ? 'text-right' : ''} text-md-on-surface-var/70`}>{e.cell(r)}</td>)}
              <td className="px-3 py-1.5"><div className="flex flex-wrap gap-1">{(r.atoms || []).map((a, i) => <span key={i} className={`text-[9px] font-mono px-1 rounded border ${a.startsWith('⛔') ? 'bg-rose-600/25 border-rose-500 text-rose-300 font-bold' : a.startsWith('🌀') ? 'bg-sky-500/25 border-sky-400 text-sky-200 font-bold' :a.startsWith('🔥') ? 'bg-amber-500/25 border-amber-400 text-amber-200 font-bold' : a.startsWith('⚡') ? 'bg-amber-500/20 border-amber-500/60 text-amber-200' : a.startsWith('🕐') ? 'bg-fuchsia-500/20 border-fuchsia-500/60 text-fuchsia-200' : a.startsWith('1H·') ? 'bg-fuchsia-500/20 border-fuchsia-500/60 text-fuchsia-200' : a.startsWith('L43') ? 'bg-emerald-500/20 border-emerald-500/60 text-emerald-200 font-bold' : (ATOM_CLS[a] || 'border-white/10 text-md-on-surface-var/80')}`}>{a}</span>)}</div></td>
              <td className="text-right px-2 py-1.5 text-md-on-surface-var/70 font-mono whitespace-nowrap" title={`${r.age_days}d ago`}>{r.signal_date}</td>
              <td className="px-2 py-1.5 text-center"><button onClick={() => pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.close, tz_sig: wlPrefix + ':' + (r.atoms || []).join('+') })} title="Add to Watchlist" className="px-1 rounded border border-emerald-700/40 text-[10px] text-emerald-300 hover:bg-emerald-900/30">★</button></td>
            </tr>
          )
        })}
        {!rows.length && <tr><td colSpan={ncol} className="px-3 py-4 text-center text-md-on-surface-var/50">{emptyMsg}</td></tr>}
      </tbody>
    </table>
  )
}

// Per-section filter bar (each setup gets its OWN tier chips + RSI-band quick-filter) wrapping EdgeTable.
function FilteredEdgeTable({ rows, accent = {}, ...rest }) {
  const [tiers, setTiers] = useState(() => new Set())   // empty = all tiers
  const [rsiBand, setRsiBand] = useState('all')          // 'all' | '<40' | '<30'
  const list = rows || []
  const distinctTiers = [...new Set(list.map(r => r.tier).filter(Boolean))]
  const hasRsi = list.some(r => r.rsi != null)
  const filtered = list.filter(r => {
    if (tiers.size && !tiers.has(r.tier)) return false
    if (rsiBand === '<40' && !(r.rsi != null && r.rsi < 40)) return false
    if (rsiBand === '<30' && !(r.rsi != null && r.rsi < 30)) return false
    return true
  })
  const toggleTier = t => setTiers(s => { const n = new Set(s); n.has(t) ? n.delete(t) : n.add(t); return n })
  const chip = on => `px-1.5 py-0.5 rounded border text-[9px] ${on ? (accent.badge || 'text-amber-200 border-amber-400 bg-amber-950/50') : 'bg-md-surface border-white/10 text-md-on-surface-var/60 hover:text-white'}`
  // >=1 (was >1): a single graded tier (e.g. QZ-Capit's lone "premium") still gets its chip,
  // so premium-only filtering works in one-tier sections too (2026-07-13, user request)
  const showBar = distinctTiers.length >= 1 || hasRsi
  return (
    <>
      {showBar && (
        <div className="flex items-center gap-1 mb-1 flex-wrap">
          {distinctTiers.length >= 1 && distinctTiers.map(t =>
            <button key={t} onClick={() => toggleTier(t)} className={chip(tiers.has(t))}>{t}</button>)}
          {hasRsi && <span className="text-[9px] text-md-on-surface-var/40 ml-1">RSI</span>}
          {hasRsi && ['all', '<40', '<30'].map(b =>
            <button key={b} onClick={() => setRsiBand(b)} className={chip(rsiBand === b)}>{b}</button>)}
          {(tiers.size || rsiBand !== 'all') ? <span className="text-[9px] text-md-on-surface-var/50 ml-1">{filtered.length}/{list.length}</span> : null}
        </div>
      )}
      <EdgeTable rows={filtered} accent={accent} {...rest} />
    </>
  )
}

export default function EdgeBoardPanel({ onSelectTicker }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)
  const [uni, setUni] = useState('all')
  const [minScore, setMinScore] = useState(55)
  const [qPrem, setQPrem] = useState(false)    // quality: show only premium-tier / ★premium rows
  const [qSweet, setQSweet] = useState(false)  // quality: gap displacement sweet-spot only (0.5–1.5×ATR)
  const [qKnife, setQKnife] = useState(false)  // quality: hide falling-knife rows (RSI<25)
  const [qZone, setQZone] = useState(false)    // price-zone: ≥$21 only (drop dead/knife/casino, keep g3-spill)
  const [qAtomicR, setQAtomicR] = useState(false)  // Atomic-R: OOS-validated selective (vol=B + $21-89 + risk-off)
  const [qScSuper, setQScSuper] = useState(false)
  const [qCharged, setQCharged] = useState(false)  // ⚡ only fires on CHARGED names (9/10 setups better)
  const [qNoSub200, setQNoSub200] = useState(false)  // ⛔ hide sub-200-rally suppressor rows
  const [qLiveUp, setQLiveUp] = useState(false)   // 🟢 only rows green TODAY by live price  // 🌀 SC-SUPER: only Edge signals firing in the Wyckoff SC zone (±5% support)
  const [live, setLive] = useState({})
  const [liveLoading, setLiveLoading] = useState(false)
  const [msg, setMsg] = useState('')
  const [p55, setP55] = useState(null)
  const [dl1, setDl1] = useState(null)
  const [g3, setG3] = useState(null)
  const [washout, setWashout] = useState(null)  // Washout/capitulation reversal (beta-capped)
  const [h1bot, setH1bot] = useState(null)      // 1H-confirmed bottom (multi-TF)
  const [l43t, setL43t] = useState(null)        // L43-TRIPLE (6-level validated confluence)
  const [qzcapit, setQzcapit] = useState(null)  // 🎯 QZ-Capit-Reversal (quality-zone capit + 1H-T1G)
  const [qzGates, setQzGates] = useState(() => new Set())  // 🔑/🏛️/🧱 gate filters (AND-stacked)
  const [zabGates, setZabGates] = useState(() => new Set())  // 💤 Z-Absorb atom filters (AND-stacked)
  const [cluster, setCluster] = useState(null)  // 🎯 Confluence / Cluster-Bottom (≥N edge families in 10 bars)
  const [clusterRS, setClusterRS] = useState(false)  // 🏆 filter cluster rows to RS-intact only (the 6/6yr tier)
  const [g3abs, setG3abs] = useState(null)      // ⚡ G3-Abs contradiction bar (gap-up absorbed into weak close)
  const [pPara, setPPara] = useState(null)  // P PARABOLA RIDE — any-P + accumulation, trailing exit
  const [absp, setAbsp] = useState(null)  // ABSORPTION → P reversal flagship
  const [z11t11, setZ11t11] = useState(null)  // Z11→T3/T5→T11/T12 oversold reversal (rare)
  const [zabsorb, setZabsorb] = useState(null)  // 💤 Z-Absorb-Turn (Z5/Z11+wt_evr+red-L34 → T3/T9)
  const [l34c, setL34c] = useState(null)        // 🏆 L34→L34 continuity (same L34 on the Z-absorption AND the T1 demand bar)
  const [l34Gates, setL34Gates] = useState(() => new Set())  // 🏆 L34→L34 atom filters (AND-stacked)
  const [midOnly, setMidOnly] = useState(false)  // 🕯️ mid-close gate — shared by the L43 & G3-Abs cards
  const [z11ReqL12, setZ11ReqL12] = useState(false)  // require L12 absorption on the anchor
  const [z11Anchor, setZ11Anchor] = useState([])  // anchor-family filter ([] = all): Z11/Z3/Z1G/Z5
  const [z11SharpL, setZ11SharpL] = useState(false)  // require sharp entry-L (L5/L46) on resolution bar
  const [wyspring, setWyspring] = useState(null)  // WYCKOFF SPRING accumulation entry
  const [t1cb, setT1cb] = useState(null)      // 🏆 T1 capitulation-bounce (GEM1 — most robust, 6/6yr)
  const [engulf, setEngulf] = useState(null)  // 🥊 Engulf-reversal (GEM2)
  const [sweep, setSweep] = useState(null)    // 🕳️ T1 low-sweep (SWEEP-only, GEM1 excluded)
  const [mtfEma, setMtfEma] = useState(null)  // 📐 Multi-TF EMA stacks (SMX/RGTI Pine ports)
  const [t6sc, setT6sc] = useState(null)      // 🎋 T6-SC-oversold (T6 @ SC floor · RSI<40)
  const [wyMode, setWyMode] = useState('spring')  // 'spring' (entry 1) | 'continuation' (markup add)
  const [absPFilter, setAbsPFilter] = useState([])  // confirming P-type filter for absorption section
  const [pTypeFilter, setPTypeFilter] = useState([])  // selected P-types in the parabola section ([] = all)
  const [paraMode, setParaMode] = useState('all')  // parabola view: 'all' | 'smooth' (tight-base high-win) | 'lottery' (non-smooth, parabola-tail)
  const [paraRS, setParaRS] = useState(false)   // 🏆 RS-intact only (the one axis that lifts Parabola)
  const [dl1Mode, setDl1Mode] = useState('all')    // D+L1→P view: 'all' | 'oversold' (RSI<40, validated +2.58%/6yr) | 'normal' (RSI≥40)
  const [dateSort, setDateSort] = useState(null)  // null | 'desc' | 'asc' — signal-date sort across ALL blocks
  const [hoverPopup, setHoverPopup] = useState(null)  // { r, pos } — mini chart popup on ticker-name hover
  const hoverTimer = useRef(null)
  const [dsrRows, setDsrRows] = useState([])
  const [spikeR, setSpikeR] = useState(null)  // 🎆 Spike-Radar — volatility watchlist (NOT a buy list)  // 🧪 DSR per setup from /api/edge-overfit (62mo precomputed)
  const [zoneRt, setZoneRt] = useState(null)  // 🔁 Zone-Retest — buy the 2nd+ touch of support, not the first drop
  const [gogaD, setGogaD] = useState(null)    // 🥊 Engulf-Goga accumulation DESCRIPTOR (not an edge)
  const [hiBase, setHiBase] = useState(null)  // 🧗 High-Base 15m-Dip — the board's first high-base setup
  const [edgeSec, setEdgeSec] = useState(() => { try { return localStorage.getItem('edge_sec') || 'all' } catch { return 'all' } })
  const pickSec = (k) => { setEdgeSec(k); try { localStorage.setItem('edge_sec', k) } catch {} }
  const _v = (k) => edgeSec === 'all' || edgeSec === k

  // 🧪 DSR strip — one fetch, sorted best-first
  useEffect(() => {
    let dead = false
    fetch('/api/edge-overfit')
      .then(r => r.json())
      .then(d => {
        if (dead || !d?.rows?.length) return
        setDsrRows([...d.rows].sort((a, b) => b.dsr - a.dsr))
      })
      .catch(() => {})
    return () => { dead = true }
  }, [])

  // Mini chart popup — appears next to the ticker NAME (not the whole row), like the Ultra screener.
  const handleTkEnter = useCallback((e, r) => {
    clearTimeout(hoverTimer.current)
    const rect = e.currentTarget.getBoundingClientRect()
    const pos = { x: rect.right, y: rect.top + rect.height / 2 }
    hoverTimer.current = setTimeout(() => setHoverPopup({ r, pos }), 300)
  }, [])
  const handleTkLeave = useCallback(() => { clearTimeout(hoverTimer.current); setHoverPopup(null) }, [])
  useEffect(() => () => clearTimeout(hoverTimer.current), [])

  const fetchLive = useCallback((tks) => {
    if (!tks.length) return
    setLiveLoading(true)
    fetch(`/api/live-prices?tickers=${tks.join(',')}`).then(r => r.json())
      .then(d => setLive(prev => ({ ...prev, ...(d.prices || {}) }))).catch(() => {}).finally(() => setLiveLoading(false))
  }, [])


  const load = useCallback(() => {
    let dead = false; setLoading(true); setErr(null); setLive({})
    fetch('/api/atomic-scan?max_age_days=4').then(r => r.json()).then(d => {
      if (dead) return
      if (d.error) { setErr(d.error); return }
      setData(d); fetchLive((d.rows || []).map(r => r.ticker).slice(0, 250))
    }).catch(e => { if (!dead) setErr(String(e)) }).finally(() => { if (!dead) setLoading(false) })
    fetch('/api/p55-setup-scan?max_age_days=4').then(r => r.json()).then(d => { if (!dead) { setP55(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/d-l1-scan?max_age_days=4').then(r => r.json()).then(d => { if (!dead) { setDl1(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/g3-gap-scan?max_age_days=4').then(r => r.json()).then(d => { if (!dead) { setG3(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/washout-reversal-scan?max_age_days=5').then(r => r.json()).then(d => { if (!dead) { setWashout(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/h1-bottom-scan?max_age_days=6').then(r => r.json()).then(d => { if (!dead) { setH1bot(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/l43-triple-scan?max_age_days=6').then(r => r.json()).then(d => { if (!dead) { setL43t(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/qz-capit-scan?max_age_days=6').then(r => r.json()).then(d => { if (!dead) { setQzcapit(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/confluence-scan?min_fam=3&max_age_days=6').then(r => r.json()).then(d => { if (!dead) { setCluster(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/g3abs-scan?max_age_days=6').then(r => r.json()).then(d => { if (!dead) { setG3abs(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/spike-radar').then(r => r.json()).then(d => { if (!dead) setSpikeR(d) }).catch(() => {})
    fetch('/api/zone-retest-scan?max_age_days=4').then(r => r.json()).then(d => { if (!dead) { setZoneRt(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/engulf-goga-scan?max_age_days=4').then(r => r.json()).then(d => { if (!dead) { setGogaD(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/highbase-scan?max_age_days=4').then(r => r.json()).then(d => { if (!dead) { setHiBase(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    // 7 = the last week (2026-07-17): a ride develops over days, so date-sorting the week shows
    // WHEN each name entered; a ticker that re-qualifies on several of those days carries ×Nd.
    // limit 400 (>the ~356 weekly candidates) so the week is not silently cut to its last 2 days:
    // rows sort by age, and today alone fills 108 of a 120 cap.
    fetch('/api/p-parabola-scan?max_age_days=7&limit=400').then(r => r.json()).then(d => { if (!dead) { setPPara(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/absorption-p-scan?max_age_days=4').then(r => r.json()).then(d => { if (!dead) { setAbsp(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/z11-t11-scan?max_age_days=30').then(r => r.json()).then(d => { if (!dead) { setZ11t11(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/z-absorb-scan?max_age_days=45').then(r => r.json()).then(d => { if (!dead) { setZabsorb(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/l34cont-scan?max_age_days=45').then(r => r.json()).then(d => { if (!dead) { setL34c(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/t1-capbounce-scan?max_age_days=6').then(r => r.json()).then(d => { if (!dead) { setT1cb(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/engulf-scan?max_age_days=6').then(r => r.json()).then(d => { if (!dead) { setEngulf(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/sweep-scan?max_age_days=6').then(r => r.json()).then(d => { if (!dead) { setSweep(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/mtf-ema-scan').then(r => r.json()).then(d => { if (!dead) { setMtfEma(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    fetch('/api/t6-sc-scan?max_age_days=6').then(r => r.json()).then(d => { if (!dead) { setT6sc(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    return () => { dead = true }
  }, [fetchLive])

  useEffect(() => { const cancel = load(); return cancel }, [load])

  // Wyckoff spring — re-fetch when the spring/continuation mode toggles
  useEffect(() => {
    let dead = false; setWyspring(null)
    fetch(`/api/wyckoff-spring-scan?max_age_days=8&mode=${wyMode}`).then(r => r.json())
      .then(d => { if (!dead) { setWyspring(d); fetchLive((d.rows || []).map(r => r.ticker)) } }).catch(() => {})
    return () => { dead = true }
  }, [wyMode, fetchLive])

  // quality filters — show only the best: premium-tier/★premium, sweet-spot gap, no falling-knife.
  // Applies to ALL lists; graceful where a notion doesn't exist (qPrem only filters quality-graded
  // tiers so liquidity-ranked lists like parabola/p55 aren't emptied; sweet/knife skip if absent).
  const _GRADED = ['premium', 'strong', 'base', 'medium', 'dip']
  const applyQ = (rows) => (rows || []).filter(r => {
    if (qPrem) {
      const isPrem = r.tier === 'premium' || (r.atoms || []).some(a => a.includes('premium'))
      if (_GRADED.includes(r.tier) && !isPrem) return false
    }
    if (qSweet && r.gap_band && r.gap_band !== 'sweet') return false
    if (qKnife && (r.atoms || []).some(a => /falling-knife/i.test(a))) return false
    if (qZone) {                                  // ≥$21: drop dead/knife/casino, keep g3-spill + quality+
      const zk = (r.price_zone) || (zoneOf(r.close)?.z)
      if (zk === 'dead' || zk === 'knife' || zk === 'casino') return false
    }
    if (qAtomicR && !r.is_atomicR) return false   // OOS-validated: vol=B + $21-89 + breadth risk-off
    if (qScSuper && !r.sc_super) return false      // 🌀 SC-SUPER: fired within ±5% of Wyckoff range support
    if (qCharged && !r.charged) return false       // ⚡ CHARGED energy state at entry
    if (qNoSub200 && r.sub200_rally) return false  // ⛔ hide bear-market-rally fires
    if (qLiveUp && !((live[r.ticker]?.change_pct ?? -1) > 0)) return false  // 🟢 green today (live)
    return true
  })
  const qActive = qPrem || qSweet || qKnife || qZone || qAtomicR || qScSuper || qCharged || qNoSub200 || qLiveUp

  const all = data?.rows || []
  const visible = all.filter(r => (uni === 'all' || r.universe === uni) && r.score >= minScore)
  const premium = applyQ(visible.filter(r => r.post_capit))
  const core = applyQ(visible.filter(r => !r.post_capit))
  const reg = data?.regime
  const riskOff = reg?.label === 'RISK_OFF'

  const addAll = () => {
    visible.forEach(r => pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.close, tz_sig: 'edge:' + r.atoms.join('+') }))
    setMsg(`★ ${visible.length} → Watchlist`); setTimeout(() => setMsg(''), 2500)
  }

  // Shared signal-date sort — clicking any "date" header cycles desc → asc → off, applied to every block.
  const toggleDateSort = () => setDateSort(d => d === null ? 'desc' : d === 'desc' ? 'asc' : null)
  const dateArrow = dateSort === 'desc' ? ' ▼' : dateSort === 'asc' ? ' ▲' : ' ↕'
  const sortByDate = (rows) => {
    if (!dateSort || !rows) return rows
    const s = [...rows].sort((a, b) => String(a.signal_date || '').localeCompare(String(b.signal_date || '')))
    return dateSort === 'desc' ? s.reverse() : s
  }

  // ABSORPTION → P — confirming-P-type filter (row matches if any of its P-types is selected)
  const ABSP_PTYPES = ['P50', 'P89', 'P55', 'P2', 'P3', 'P66']
  const absRows = sortByDate(applyQ((absp?.rows || []).filter(r =>
    (uni === 'all' || r.universe === uni) &&
    (absPFilter.length === 0 || String(r.p_type).split('/').some(p => absPFilter.includes(p))))))
  const toggleAbsP = (p) => setAbsPFilter(f => f.includes(p) ? f.filter(x => x !== p) : [...f, p])

  // Oversold-reversal FAMILY (Z11/Z3/Z1G/Z5 → T3/T5 → T11/T12); optional L12 sharpener + anchor filter
  const Z11_ANCHORS = ['Z11', 'Z3', 'Z1G', 'Z5']
  const z11Rows = sortByDate(applyQ((z11t11?.rows || []).filter(r =>
    (uni === 'all' || r.universe === uni) && (!z11ReqL12 || r.l12) && (!z11SharpL || r.sharp_l) &&
    (z11Anchor.length === 0 || z11Anchor.includes(r.anchor)))))
  const toggleZ11Anchor = (a) => setZ11Anchor(f => f.includes(a) ? f.filter(x => x !== a) : [...f, a])

  // WYCKOFF SPRING — accumulation shakeout entry (buy the spring, not the breakout)
  const wyRows = sortByDate(applyQ((wyspring?.rows || []).filter(r => uni === 'all' || r.universe === uni)))
  const t1cbRows = sortByDate(applyQ((t1cb?.rows || []).filter(r => uni === 'all' || r.universe === uni)))
  const engulfRows = sortByDate(applyQ((engulf?.rows || []).filter(r => uni === 'all' || r.universe === uni)))
  const sweepRows = sortByDate(applyQ((sweep?.rows || []).filter(r => uni === 'all' || r.universe === uni)))
  const mtfEmaRows = applyQ((mtfEma?.rows || []).filter(r => uni === 'all' || r.universe === uni))
  const t6scRows = sortByDate(applyQ((t6sc?.rows || []).filter(r => uni === 'all' || r.universe === uni)))

  // P PARABOLA RIDE — P-type filter (row matches if any of its '/'-joined P-types is selected)
  const PARA_PTYPES = ['P2', 'P3', 'P50', 'P55', 'P66', 'P89']
  const paraRows = applyQ((pPara?.rows || []).filter(r =>
    (uni === 'all' || r.universe === uni) &&
    (pTypeFilter.length === 0 || String(r.p_type).split('/').some(p => pTypeFilter.includes(p))) &&
    (paraMode === 'all' || (paraMode === 'smooth' ? r.smooth : !r.smooth)) &&
    (!paraRS || (r.atoms || []).some(a => a === '🏆RS' || a === '💪sec-lead'))
  ))
  const paraUniRows = (pPara?.rows || []).filter(r => uni === 'all' || r.universe === uni)
  const paraSmoothN = paraUniRows.filter(r => r.smooth).length
  const paraLotteryN = paraUniRows.length - paraSmoothN

  // D+L1→P — oversold (RSI<40) is the validated premium refinement (+2.58%, win 58%, 6/6yr)
  const dl1IsOversold = (r) => r.rsi != null && r.rsi < 40
  const dl1UniRows = (dl1?.rows || []).filter(r => uni === 'all' || r.universe === uni)
  const dl1OversoldN = dl1UniRows.filter(dl1IsOversold).length
  const dl1NormalN = dl1UniRows.length - dl1OversoldN
  const dl1Rows = sortByDate(applyQ(dl1UniRows.filter(r =>
    dl1Mode === 'all' || (dl1Mode === 'oversold' ? dl1IsOversold(r) : !dl1IsOversold(r)))))
  const togglePType = (p) => setPTypeFilter(f => f.includes(p) ? f.filter(x => x !== p) : [...f, p])
  const addParaToWatchlist = () => {
    paraRows.forEach(r => pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.close, tz_sig: 'pparabola:' + r.atoms.join('+') }))
    setMsg(`★ ${paraRows.length} P-parabola → Watchlist`); setTimeout(() => setMsg(''), 2500)
  }

  // Per-section export: download just that list's tickers as a TradingView .txt
  const ExportBtn = ({ rows, label }) => {
    const tks = (rows || []).filter(r => uni === 'all' || r.universe === uni).map(r => r.ticker)
    if (!tks.length) return null
    return (
      <button onClick={(e) => { e.stopPropagation(); downloadTV(`edge_${label}_${data?.as_of || ''}.txt`.replace('__', '_'), [{ name: `${label} ${data?.as_of || ''}`.trim(), tickers: tks }]) }}
        title={`Export ${tks.length} ${label} tickers → TradingView .txt`}
        className="text-[10px] px-1.5 py-0.5 rounded border border-sky-700/50 text-sky-300 hover:bg-sky-900/30">⬇ {tks.length}</button>
    )
  }

  // ── 🔗 Multi-edge confluence: tickers appearing across ≥2 different edge setups ──
  // (same or nearby bars). Aggregates every section's rows client-side — higher conviction.
  const EDGE_SOURCES = [
    { label: 'Capit', rows: premium }, { label: 'CORE', rows: core },
    { label: 'P55', rows: p55?.rows }, { label: 'D+L1', rows: dl1?.rows },
    { label: 'G3', rows: g3?.rows }, { label: 'Washout', rows: washout?.rows },
    { label: '1H-bot', rows: h1bot?.rows }, { label: 'L43-trip', rows: l43t?.rows }, { label: 'QZ-Capit', rows: qzcapit?.rows }, { label: 'Cluster', rows: cluster?.rows }, { label: 'G3-Abs', rows: g3abs?.rows }, { label: 'Absorb→P', rows: absp?.rows },
    { label: 'Family', rows: z11t11?.rows }, { label: 'Spring', rows: wyspring?.rows },
    { label: 'Parabola', rows: pPara?.rows },
  ]
  const _cmap = {}
  EDGE_SOURCES.forEach(src => (src.rows || []).filter(r => (uni === 'all' || r.universe === uni) && (!qScSuper || r.sc_super)).forEach(r => {
    const tk = r.ticker
    if (!_cmap[tk]) _cmap[tk] = { ticker: tk, universe: r.universe, close: r.close, edges: {}, maxScore: 0 }
    const d = r.signal_date || r.date || ''
    if (!_cmap[tk].edges[src.label] || d > _cmap[tk].edges[src.label]) _cmap[tk].edges[src.label] = d
    _cmap[tk].maxScore = Math.max(_cmap[tk].maxScore, r.score || 0)
  }))
  const confluence = Object.values(_cmap).map(c => {
    const dates = Object.values(c.edges).filter(Boolean).sort()
    const span = dates.length >= 2 ? Math.round((new Date(dates[dates.length - 1]) - new Date(dates[0])) / 864e5) : 0
    return { ...c, nEdges: Object.keys(c.edges).length, lastDate: dates[dates.length - 1] || '', span }
  }).filter(c => c.nEdges >= 2).sort((a, b) => b.nEdges - a.nEdges || b.maxScore - a.maxScore)

  return (
    <div className="p-4 text-md-on-surface">
      {hoverPopup && (
        <MiniChartPopup ticker={hoverPopup.r.ticker} tf="1d" pos={hoverPopup.pos}
          price={live[hoverPopup.r.ticker]?.price ?? hoverPopup.r.close}
          changePct={live[hoverPopup.r.ticker]?.change_pct}
          rsi={hoverPopup.r.rsi}
          sub={hoverPopup.r.seq || hoverPopup.r.t_sig || hoverPopup.r.p_type} />
      )}
      <h1 className="text-base font-bold mb-1">✅ Edge — მხოლოდ რაც მუშაობს</h1>
      <p className="text-[11px] text-md-on-surface-var mb-2 max-w-3xl">
        ეს board აჩვენებs <b>მხოლოდ ის 2 setup-ს რომელმაც 5-წლიანი path-sim ვალიდაცია გაუძლო</b> — დანარჩენი
        35 tab-ის ხმაურის გარეშე. <b>fade strength · buy absorbed weakness.</b> click ticker → chart.
      </p>

      {/* honest cost + regime framing — the audit's key finding */}
      <div className="flex flex-wrap items-center gap-2 mb-3">
        {reg && (
          <div className={`inline-flex items-center gap-2 px-3 py-1 rounded border text-xs ${REG_CLS[reg.label] || ''}`}>
            <b>REGIME: {reg.label}</b>
            <span className="opacity-80">score {reg.score} · size ×{reg.conv_mult}</span>
          </div>
        )}
        <div className="inline-flex items-center px-3 py-1 rounded border border-amber-700/40 bg-amber-950/30 text-[10px] text-amber-200/90">
          ⚠ gross edge ~+0.5%/trade · microcap costs 0.5–1% round-trip → <b className="mx-1">net thin</b> · small size · structured exits · paper-track first
        </div>
        {/* 🗓️ season block — validate_edge_season 2026-07-06: Dec-Mar is negative for ALL 14
            setups (5-6/6 yrs, incl. bull years); Apr-Jun+Sep-Nov strongly positive for all;
            Jul-Aug flat/negative for most. Blocks frozen from the K0/universe study. */}
        {(() => {
          const mo = new Date().getMonth() + 1
          const blk = [12, 1, 2, 3].includes(mo) ? 'BAD' : [7, 8].includes(mo) ? 'MID' : 'GOOD'
          const cls = blk === 'GOOD' ? 'border-emerald-600 bg-emerald-900/30 text-emerald-200'
            : blk === 'BAD' ? 'border-rose-600 bg-rose-900/40 text-rose-200'
            : 'border-amber-600 bg-amber-900/30 text-amber-200'
          const txt = blk === 'GOOD'
            ? '🗓️ სეზონი: GOOD (აპრ-ივნ·სექ-ნოე) — 14/14 setup მედიან-დადებითი ამ ბლოკში'
            : blk === 'BAD'
              ? '🗓️ სეზონი: BAD (დეკ-მარ) — 14/14 setup ისტორიულად ზარალიანია (med −2..−8, 5-6/6 წ). გამონაკლისი: Z11-T11 · GEM1. watch-only ტონი'
              : '🗓️ სეზონი: MID (ივლ-აგვ) — უმეტესი setup ფლეტი/სუსტია; მუშაობს: Atomic-R (PF 2.75) · Engulf-L46 · G3'
          return (
            <div title="validate_edge_season.py — 62mo path-sim, blocks frozen from the K0/universe seasonality study (Dec-Mar negative every year 2021-26 universe-wide)"
                 className={`inline-flex items-center px-3 py-1 rounded border text-[11px] font-semibold ${cls}`}>
              {txt}
            </div>
          )
        })()}
        {riskOff && (
          <div className="inline-flex items-center px-3 py-1 rounded border border-rose-600 bg-rose-900/40 text-[11px] text-rose-200 font-bold">
            🛑 RISK_OFF — ეს edge bear tape-ში აგებს. STAND DOWN / watch-only.
          </div>
        )}
      </div>

      {/* 🧪 DSR strip — Deflated Sharpe per setup (edge_overfit.json, 62mo, Bailey & López de Prado).
          Ranks which setups' per-trade strength survives the "we tried ~100 variants" correction. */}
      {dsrRows.length > 0 && (
        <div className="flex flex-wrap items-center gap-1.5 mb-3 text-[10px]"
             title="DSR = P(true edge beats the luck of N≈100 tested variants). ≥0.9 selection-proof · 0.6+ decent · <0.25 weak per-trade (edge still real — PSR₀=1.0 — but thin per trade). Full table + PBO in the 🔁 Edge Replay tab.">
          <span className="text-md-on-surface-var font-bold">🧪 DSR</span>
          {dsrRows.map(r => (
            <span key={r.setup} className={`px-1.5 py-0.5 rounded border font-mono ${
              r.dsr >= 0.9 ? 'bg-emerald-900/50 border-emerald-500 text-emerald-200 font-bold'
              : r.dsr >= 0.6 ? 'bg-teal-900/40 border-teal-600 text-teal-200'
              : r.dsr >= 0.25 ? 'bg-yellow-900/30 border-yellow-700 text-yellow-300/90'
              : 'bg-md-surface border-white/10 text-md-on-surface-var/60'}`}>
              {r.setup} {r.dsr.toFixed(2)}
            </span>
          ))}
          <span className="text-md-on-surface-var/50">· სრული ცხრილი + PBO → 🔁 Edge Replay</span>
        </div>
      )}

      {/* controls */}
      <div className="flex items-center gap-2 text-xs mb-4 flex-wrap">
        {['all', 'sp500', 'nasdaq', 'russell2k'].map(u => (
          <button key={u} onClick={() => setUni(u)}
            className={`px-2 py-0.5 rounded border ${uni === u ? 'bg-md-surface-high border-white/30 text-white' : 'bg-md-surface border-white/10 hover:text-white'}`}>{u}</button>
        ))}
        <span className="text-md-on-surface-var/60 ml-2">min score:</span>
        {[55, 70, 90].map(s => (
          <button key={s} onClick={() => setMinScore(s)}
            className={`px-2 py-0.5 rounded border ${minScore === s ? 'bg-sky-900/60 text-sky-200 border-sky-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>{s}+</button>
        ))}
        <span className="text-md-on-surface-var/60 ml-2" title="quality filters — apply to G3 · CORE · Capit · family · spring">best:</span>
        <button onClick={() => setQPrem(v => !v)} title="show only premium-tier / ★premium rows (gap sweet-spot × RSI 25–35)"
          className={`px-2 py-0.5 rounded border ${qPrem ? 'bg-emerald-900/60 text-emerald-200 border-emerald-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>⭐ premium</button>
        <button onClick={() => setQSweet(v => !v)} title="gap displacement sweet-spot only (0.5–1.5×ATR) — excludes exhaustion & small gaps"
          className={`px-2 py-0.5 rounded border ${qSweet ? 'bg-lime-900/60 text-lime-200 border-lime-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>💎 sweet</button>
        <button onClick={() => setQKnife(v => !v)} title="hide falling-knife rows (RSI<25, deep but inconsistent 3/6yr)"
          className={`px-2 py-0.5 rounded border ${qKnife ? 'bg-amber-900/60 text-amber-200 border-amber-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>⚠ hide knife</button>
        <button onClick={() => setQZone(v => !v)} title="price-zone law: ≥$21 only — drops the dead($8-21)/knife($1-8)/casino(<$1) zones (negative median, high catastrophe), keeps G3 $8-10 spillover"
          className={`px-2 py-0.5 rounded border ${qZone ? 'bg-emerald-900/60 text-emerald-200 border-emerald-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>✅ ≥$21</button>
        <button onClick={() => setQAtomicR(v => !v)} title="Atomic-R — the OOS-validated SELECTIVE atomic edge: vol=B (controlled) + price $21-89 (quality zone) + market breadth risk-OFF (fear). Backtest mean +4.37% / PF 1.84 / 5-6 of 6yr (2x the base atomic). Empty in euphoria = stand down (the edge lives in fear, dies in euphoria)."
          className={`px-2 py-0.5 rounded border font-semibold ${qAtomicR ? 'bg-rose-900/60 text-rose-200 border-rose-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>🎯 Atomic-R</button>
        <button onClick={() => setQScSuper(v => !v)} title="🌀 SC-SUPER — only Edge signals firing within ±5% of the Wyckoff range support (SC floor). Validated 2026-07-03: a band-plateau + 2×-slip-safe MEDIAN-lifting tier (kills the negative tail) on 6 setups — T1-CapBounce, D+L1, Spring, Atomic, H1-bottom, Washout. Setups without the badge won't show under this filter (their edge is zone-agnostic or above-range = worse)."
          className={`px-2 py-0.5 rounded border font-semibold ${qScSuper ? 'bg-sky-900/60 text-sky-200 border-sky-400' : 'bg-md-surface border-white/10 hover:text-white'}`}>🌀 SC-SUPER</button>
        <button onClick={() => setQCharged(v => !v)} title="⚡ CHARGED — only fires on 'charged' names (3d hot volume + expanded range + diffuse intraday flow). Validated 2026-07-06: 9/10 setups improve at charged entry (Z11 med +4→+14.6 win 81%, G3 +0.7→+4.8, L43 +1.8→+8.6); the weak tier (Atomic/Washout/H1) flips median-positive. D+L1 doesn't benefit."
          className={`px-2 py-0.5 rounded border font-semibold ${qCharged ? 'bg-amber-900/60 text-amber-200 border-amber-400' : 'bg-md-surface border-white/10 hover:text-white'}`}>⚡ charged</button>
        <button onClick={() => setQNoSub200(v => !v)} title="⛔ hide sub-200-rally — exclude fires where the daily close is under EMA200 with a raised short stack (bear-market rally). Validated 2026-07-05: such fires are worse on EVERY setup (Δ −1..−3.3pp, era-independent)."
          className={`px-2 py-0.5 rounded border font-semibold ${qNoSub200 ? 'bg-rose-900/60 text-rose-200 border-rose-400' : 'bg-md-surface border-white/10 hover:text-white'}`}>⛔ hide sub200</button>
        <button onClick={() => setQLiveUp(v => !v)} title="🟢 live + — only rows whose LIVE price is up today (change_pct > 0). Uses the same live feed as the 'live' column; rows without a live quote yet are hidden while active."
          className={`px-2 py-0.5 rounded border font-semibold ${qLiveUp ? 'bg-emerald-900/60 text-emerald-200 border-emerald-400' : 'bg-md-surface border-white/10 hover:text-white'}`}>🟢 live +</button>
        {qAtomicR && data?.atomic_regime?.breadth != null && (
          <span className={`text-[11px] ${data.atomic_regime.risk_off ? 'text-emerald-300' : 'text-amber-300'}`}>
            {data.atomic_regime.risk_off
              ? `🩸 fear (breadth ${data.atomic_regime.breadth}) — Atomic-R ACTIVE`
              : `😀 euphoria (breadth ${data.atomic_regime.breadth}) — stand down; list empty by design`}
          </span>
        )}
        {loading && <span className="text-sky-400 animate-pulse">scanning…</span>}
        {data && <span className="text-md-on-surface-var/60">{visible.length} / {all.length} · {data.as_of}</span>}
        <button onClick={load} disabled={loading}
          className="px-2 py-0.5 rounded border border-white/15 hover:bg-white/5 disabled:opacity-40">↻ refresh</button>
        <button onClick={() => fetchLive(visible.map(r => r.ticker).slice(0, 250))} disabled={!visible.length || liveLoading}
          className="px-2 py-0.5 rounded border border-sky-700/50 text-sky-300 hover:bg-sky-900/30 disabled:opacity-40">{liveLoading ? '↻ live…' : '↻ live'}</button>
        <button onClick={() => downloadTV(`edge_${data?.as_of || 'scan'}.txt`, [{ name: `Edge board ${data?.as_of || ''}`.trim(), tickers: visible.map(r => r.ticker) }])}
          disabled={!visible.length} className="px-2 py-0.5 rounded border border-sky-700/50 text-sky-300 hover:bg-sky-900/30 disabled:opacity-40" title="TradingView .txt">⬇ TV .txt</button>
        <button onClick={addAll} disabled={!visible.length}
          className="px-2 py-0.5 rounded border border-emerald-700/50 text-emerald-300 hover:bg-emerald-900/30 disabled:opacity-40">★ all → Watchlist</button>
        {msg && <span className="text-emerald-400 text-[11px]">{msg}</span>}
      </div>

      {err && <div className="text-rose-400 text-xs mb-2">error: {err}</div>}

      {/* ── section TABS — open each edge separately (2026-07-08) ── */}
      <div className="sticky top-0 z-20 py-1.5 mb-2 bg-md-surface/95 backdrop-blur flex flex-wrap gap-1 text-[10px] border-b border-white/10">
        {EDGE_SECS.map(([k, lab]) => (
          <button key={k} onClick={() => pickSec(k)}
            className={`px-1.5 py-0.5 rounded border font-mono ${edgeSec === k ? 'bg-white/15 border-white/40 text-white' : 'bg-md-surface border-white/10 text-md-on-surface-var hover:text-white'}`}>
            {lab}
          </button>
        ))}
      </div>
      {_v('conf') && (<>
      {/* ── 🔗 MULTI-EDGE CONFLUENCE — tickers stacked across ≥2 different edges ──── */}
      <div className="mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-yellow-300">🔗 MULTI-EDGE CONFLUENCE <span className="text-yellow-400/70">— tickers in ≥2 edges (same/nearby bar)</span></h2>
        <ExportBtn rows={confluence} label="confluence" />
        <span className="text-[10px] text-md-on-surface-var/70">
          highest conviction — independent setups agreeing on the same name{confluence.length ? ` · ${confluence.length}` : ''}
        </span>
      </div>
      <table className="w-full text-xs border border-yellow-500/20 rounded overflow-hidden mb-1">
        <thead className="bg-md-surface-high text-md-on-surface-var">
          <tr>
            <th className="text-center px-2 py-1.5" title="number of distinct edges"># </th>
            <th className="text-left px-2 py-1.5">ticker</th>
            <th className="text-left px-2 py-1.5">univ</th>
            <th className="text-right px-2 py-1.5">close</th>
            <th className="text-right px-2 py-1.5">live</th>
            <th className="text-left px-3 py-1.5">edges (with signal date)</th>
            <th className="text-center px-2 py-1.5" title="days between earliest & latest edge">span</th>
            <th className="text-right px-2 py-1.5">best</th>
            <th className="text-right px-2 py-1.5">last</th>
            <th className="text-center px-2 py-1.5"></th>
          </tr>
        </thead>
        <tbody>
          {confluence.map(c => (
            <tr key={c.ticker} className={`border-t border-white/5 hover:bg-white/[0.03] ${c.nEdges >= 3 ? 'bg-yellow-500/[0.08] border-l-2 border-l-yellow-500' : 'bg-yellow-500/[0.03]'}`}>
              <td className="text-center px-2 py-1.5"><span className={`font-mono font-bold ${c.nEdges >= 3 ? 'text-yellow-300' : 'text-yellow-200/80'}`}>{c.nEdges}×</span></td>
              <td className="px-2 py-1.5"><button onClick={() => onSelectTicker?.(c.ticker)} onMouseEnter={e => handleTkEnter(e, c)} onMouseLeave={handleTkLeave} className="font-mono font-semibold hover:text-yellow-300">{c.ticker}</button></td>
              <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var/70">{c.universe}</td>
              <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var">{c.close != null ? '$' + c.close : '—'}</td>
              <td className="text-right px-2 py-1.5 font-mono"><LiveCell p={live[c.ticker]} /></td>
              <td className="px-3 py-1.5">
                <div className="flex flex-wrap gap-1">
                  {Object.entries(c.edges).sort((a, b) => (b[1] || '').localeCompare(a[1] || '')).map(([e, d], i) =>
                    <span key={i} className="text-[9px] font-mono px-1 rounded border border-yellow-500/40 bg-yellow-500/10 text-yellow-100/90">{e}<span className="text-yellow-200/50"> {String(d).slice(5)}</span></span>)}
                </div>
              </td>
              <td className="text-center px-2 py-1.5 font-mono text-[10px] text-md-on-surface-var/70">{c.span === 0 ? 'same' : c.span + 'd'}</td>
              <td className={`text-right px-2 py-1.5 font-mono font-bold ${scoreCls(c.maxScore)}`}>{c.maxScore}</td>
              <td className="text-right px-2 py-1.5 text-md-on-surface-var/70 font-mono whitespace-nowrap">{c.lastDate}</td>
              <td className="px-2 py-1.5 text-center">
                <button onClick={() => pwlAdd({ ticker: c.ticker, _tf: '1d', last_price: c.close, tz_sig: 'confluence:' + Object.keys(c.edges).join('+') })} title="Add to Watchlist"
                  className="px-1 rounded border border-emerald-700/40 text-[10px] text-emerald-300 hover:bg-emerald-900/30">★</button>
              </td>
            </tr>
          ))}
          {!confluence.length &&
            <tr><td colSpan={10} className="px-3 py-4 text-center text-md-on-surface-var/50">{loading ? 'scanning…' : 'no ticker appears in 2+ edges right now'}</td></tr>}
        </tbody>
      </table>

      </>)}
      {_v('gem1') && (<>
      {/* ── 🏆 T1 CAPITULATION-BOUNCE (GEM1, 2026-07-01 — most robust edge found) ── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-amber-300">🏆 T1 CAPITULATION-BOUNCE — small T1 off a big-bear Z <span className="text-amber-500/80">(most robust)</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~55% · 6/6yr</span></h2>
        <ExportBtn rows={t1cbRows} label="t1_capbounce" />
        <span className="text-[10px] text-md-on-surface-var/70">
          T1 body &lt;0.5× the prior Z (capitulation) body · RSI 30-50 · vol=B · <b className="text-amber-200/90">6/6yr +5..+9 · TRAIN≈TEST era-independent · PF 2.32 · med +5.4 · win 60</b> · 🧱 = cap-bar L5/L46 absorption {t1cbRows.length ? `· ${t1cbRows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={t1cbRows} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="t1_capbounce"
        accent={{ rowPrem: 'bg-amber-500/[0.08] border-l-2 border-l-amber-500', hover: 'hover:text-amber-300', tz: 'text-amber-300/90', badge: 'text-amber-200 border-amber-400 bg-amber-950/60' }}
        extras={[{ header: 'cap-Z', align: 'right', title: 'how many × bigger the prior Z (capitulation) bar was', cell: r => r.z_mult != null ? r.z_mult + '×' : '—', cellClass: () => 'text-right px-2 py-1.5 font-mono text-[10px] text-amber-300/70' }]}
        emptyMsg={!t1cb ? 'scanning…' : 'no T1 capitulation-bounce in the last 6d (T1 · big prior-Z · RSI30-50 · vol=B)'} />

      </>)}
      {_v('z11') && (<>
      {/* ── 🎯 Oversold reversal FAMILY (Z11/Z3/Z1G/Z5 → T3/T5 → T11/T12) ───────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-violet-300">🎯 Oversold reversal FAMILY — bear anchor → T3/T5 → T11/T12 <span className="text-violet-500/80">(rare)</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~56%</span></h2>
        <ExportBtn rows={z11Rows} label="z11_family" />
        <span className="text-[10px] text-md-on-surface-var/70">
          bear anchor @ RSI 30-45 → T3/T5 → T11/T12 at +2 → enter@T11/T12 · <b className="text-violet-200/90">Z11 +2.04 · Z3 +2.47 · Z1G +2.67 · Z5 +1.30 /win 54-59%</b> · <b className="text-emerald-300/90">entry-L: L5 +4.40/6yr · L46 +2.58 (sharp) vs L25 +1.44</b>
          {z11Rows.length ? ` · ${z11Rows.length}` : ''}
        </span>
      </div>
      <div className="mb-1.5 flex items-center gap-1.5 flex-wrap text-[10px]">
        <span className="text-md-on-surface-var/60">anchor:</span>
        {Z11_ANCHORS.map(a => {
          const on = z11Anchor.includes(a)
          const n = (z11t11?.rows || []).filter(r => (uni === 'all' || r.universe === uni) && r.anchor === a).length
          const dip = a === 'Z1G'
          return (
            <button key={a} onClick={() => toggleZ11Anchor(a)} disabled={!n}
              title={a === 'Z11' ? 'flagship' : a === 'Z3' ? 'robust (well-distributed, 5/6yr)' : a === 'Z1G' ? 'strong but clustered — market-dip reversal play' : 'moderate third'}
              className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${on ? 'border-violet-400 bg-violet-500/20 text-violet-100' : n ? (dip ? 'border-white/10 text-md-on-surface-var/50 hover:border-amber-600' : 'border-white/15 text-md-on-surface-var/80 hover:border-violet-600') : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
              {a}<span className="opacity-50"> {n}</span>
            </button>
          )
        })}
        {z11Anchor.length > 0 && <button onClick={() => setZ11Anchor([])} className="px-1.5 py-0.5 rounded border border-white/15 text-md-on-surface-var/70 hover:border-white/30">clear</button>}
        <button onClick={() => setZ11SharpL(v => !v)}
          title="Require a SHARP entry-L (L5/L46) on the T11/T12 resolution bar — the reversal bar's volume character. L5 +4.40%/win63/6yr · L46 +2.58 vs L25 +1.44 (weak)"
          className={`ml-1 px-1.5 py-0.5 rounded border font-mono transition-colors ${z11SharpL ? 'border-emerald-400 bg-emerald-500/20 text-emerald-100' : 'border-white/15 text-md-on-surface-var/70 hover:border-emerald-600'}`}>
          sharp entry-L (L5/L46)
        </button>
        <button onClick={() => setZ11ReqL12(v => !v)}
          title="Require an L12 absorption line on the anchor bar — fewer, cleaner candidates"
          className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${z11ReqL12 ? 'border-violet-400 bg-violet-500/20 text-violet-100' : 'border-white/15 text-md-on-surface-var/70 hover:border-violet-600'}`}>
          require L12
        </button>
        <span className="text-violet-300/70 ml-1">→ {z11Rows.length} shown</span>
      </div>
      <FilteredEdgeTable rows={z11Rows} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="z11_t11"
        accent={{ rowPrem: 'bg-violet-500/[0.08] border-l-2 border-l-violet-500', hover: 'hover:text-violet-300', tz: 'text-violet-300/90', badge: 'text-violet-200 border-violet-400 bg-violet-950/60' }}
        extras={[{ header: 'sequence', title: 'anchor → confirm → resolve', cell: r => <span title={`${r.anchor} anchor ${r.anchor_date}${r.l12 ? ' (L12)' : ''} → resolution ${r.signal_date}`}>{r.seq}{r.l12 ? ' ·L12' : ''}{r.sharp_l ? <span className="text-emerald-400 ml-1">·sharp</span> : null}</span> }]}
        emptyMsg={!z11t11 ? 'scanning…' : (z11Anchor.length || z11ReqL12 ? 'no candidates for the selected anchor/filter' : 'no oversold-reversal family signals in the last 30d')} />

      </>)}
      {_v('g3') && (<>
      {/* ── ⚡ G3 GAP RECLAIM — large gap-up on oversold ─────────────────────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-lime-300">⚡ G3 GAP RECLAIM — large gap-up on oversold<span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~51%</span></h2>
        <ExportBtn rows={g3?.rows} label="g3gap" />
        <span className="text-[10px] text-md-on-surface-var/70">
          catalyst displacement (|open−prev_close|) + RSI&lt;45 + T + non-VB → momentum · <b className="text-lime-200/90">premium = disp 0.5-1.5×ATR × RSI 25-35: +2.12%/win57/6yr</b> · <b className="text-emerald-300/90">+L43✓ (VSA supply-absorbed body) = +2.77%/win61</b> · &gt;1.5×ATR ⚠ exhaustion · RSI&lt;25 knife
          {g3?.rows?.length ? ` · ${g3.rows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((g3?.rows || []).filter(r => uni === 'all' || r.universe === uni)))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="g3gap"
        accent={{ rowPrem: 'bg-lime-500/[0.07] border-l-2 border-l-lime-500', hover: 'hover:text-lime-300', tz: 'text-lime-300/80', badge: 'text-lime-200 border-lime-400 bg-lime-950/60' }}
        extras={[DISP_EXTRA, { header: 'cci', align: 'right', cell: r => r.cci ?? '—' }]}
        emptyMsg={!g3 ? 'scanning…' : 'no G3 gap-reclaim candidates today'} />

      </>)}
      {_v('l43') && (<>
      {/* ── 🧩 L43-TRIPLE — VSA-absorption × reversal-T × gap-sweet (6-level validated) ── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-emerald-300">🧩 L43-TRIPLE <span className="text-emerald-400/80">— orthogonal STATE stack, backtest-proven</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~52%</span></h2>
        <ExportBtn rows={l43t?.rows} label="l43triple" />
        <span className="text-[10px] text-md-on-surface-var/70">
          L43 (VSA absorbed body) + reversal-T (T11/T12/engulf) + gap-sweet (0.5-1.5×ATR), clean of suppressors, RSI&lt;40 · <b className="text-emerald-200/90">path-sim +2.13%/PF1.65/6yr · Monte-Carlo P(&gt;0)=100% · beats random +1.62pp</b> · stop−10/12% target+25/30%
          {l43t?.rows?.length ? ` · ${l43t.rows.length}` : ''}
        </span>
        <div className="ml-auto"><MidChip on={midOnly} set={setMidOnly} n={(l43t?.rows || []).filter(r => r.mid_close).length}
          title="🕯️ mid-close (2026-07-27) — the close sits in the MIDDLE 38-62% of the bar's own range. An INVERTED-U gate: a STRONG close hurts, the middle pays. L43-TRIPLE med +2.69→+6.18, win 57→64%, pf 1.89→2.85, worst-year +0.9→+3.2 (the best worst-year on the whole Replay board), DSR 0.999. Plateau-wide: every cut from 30-70 to 45-55 works. $21-89 only — the $89-377 bucket is 4/6yr worst −2.0." /></div>
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((l43t?.rows || []).filter(r => (uni === 'all' || r.universe === uni) && (!midOnly || r.mid_close))))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="l43triple"
        accent={{ rowPrem: 'bg-emerald-500/[0.08] border-l-2 border-l-emerald-500', hover: 'hover:text-emerald-300', tz: 'text-emerald-300/90', badge: 'text-emerald-200 border-emerald-400 bg-emerald-950/60' }}
        extras={[DISP_EXTRA]}
        emptyMsg={!l43t ? 'scanning…' : 'no L43-TRIPLE today (needs L43 + reversal-T + gap-sweet + oversold)'} />

      </>)}
      {_v('qzcapit') && (<>
      {/* ── 🎯 QZ-Capit-Reversal — quality-zone capitulation + 1H-T1G reversal ── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-amber-300">🎯 QZ-Capit-Reversal <span className="text-amber-400/80">— quality-zone bottom + 1H reversal</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win 51% · PF1.32 · 5-6yr</span></h2>
        <ExportBtn rows={qzcapit?.rows} label="qzcapit" />
        <span className="text-[10px] text-md-on-surface-var/70">
          $21-89 quality zone · daily-Z oversold (RSI&lt;45) · FRESH 15d-low · 1H Z2G/Z1G capitulation + 1H reversal-cluster (T1G/Z11/T5/Z1G/T11) · <b className="text-amber-200/90">+1.98/med+0.41/win51/PF1.32 · 5-6yr</b> · STATE carries it, 1H flips median −1.3→+0.7 · price-bucket-rescued from an LLY chart obs
          {qzcapit?.rows?.length ? ` · ${qzcapit.rows.length}` : ''}
        </span>
        <div className="flex items-center gap-1.5 text-[10px] ml-auto">
          {[['🎋TLS-entry', '🎋 TLS-entry', 'Three Line Strike completed ≤5 bars after the QZ-Capit — the validated ENTRY refinement (enter this bar). +3.22/med+2.47/win55/PF1.57/5-6yr vs base +2.04 (lift +1.18pp, TRAIN & TEST both positive, +2.98σ). Distinct, later bar than the state row.'],
            ['🔑key', '🔑 key-level', 'support tested ≥2× = real level (med +0.90 vs weak −0.42)'],
            ['🏛️BOS', '🏛️ BOS', 'structure shifted bullish (downtrend swing-high broken) — med +1.98/win55/6-6yr'],
            ['🧱OB', '🧱 OB', 'order-block retest (institutional absorption) — med +4.20/PF2.50/6-6yr']].map(([atom, label, title]) => {
            const n = (qzcapit?.rows || []).filter(r => (r.atoms || []).includes(atom)).length
            const on = qzGates.has(atom)
            return (
              <button key={atom} title={title} disabled={!n}
                onClick={() => setQzGates(s => { const x = new Set(s); x.has(atom) ? x.delete(atom) : x.add(atom); return x })}
                className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${on ? 'border-amber-400 bg-amber-500/20 text-amber-100' : n ? 'border-amber-700/40 text-amber-300/80 hover:border-amber-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
                {label}<span className="opacity-50"> {n}</span>
              </button>
            )
          })}
        </div>
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((qzcapit?.rows || []).filter(r => (uni === 'all' || r.universe === uni) && [...qzGates].every(g => (r.atoms || []).includes(g)))))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="qzcapit"
        accent={{ rowPrem: 'bg-amber-500/[0.08] border-l-2 border-l-amber-500', hover: 'hover:text-amber-300', tz: 'text-amber-300/90', badge: 'text-amber-200 border-amber-400 bg-amber-950/60' }}
        emptyMsg={!qzcapit ? 'scanning…' : (qzGates.size ? `no QZ-Capit with ${[...qzGates].join('+')} today` : 'no QZ-Capit-Reversal today ($21-89 · RSI<45 · fresh 15d-low · 1H Z2G/Z1G + 1H-T1G)')} />
      </>)}
      {_v('zabsorb') && (<>
      {/* ── 💤 Z-Absorb-Turn — deep-bear Z5/Z11 absorption (wt_evr + red-L34) → T3/T9 confirm ── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-indigo-300">💤 Z-Absorb-Turn <span className="text-indigo-400/80">— capitulation absorbed, then a T3/T9 turn</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win 58% · PF2.44 · 5/6yr</span></h2>
        <ExportBtn rows={zabsorb?.rows} label="zabsorb" />
        <span className="text-[10px] text-md-on-surface-var/70">
          PRIOR bar = deep-bear <b className="text-indigo-200/90">Z5/Z11 + wt_evr + red-L34</b> (institutional soak into a no-result down bar) · THIS bar confirms with <b className="text-indigo-200/90">T3 or T9</b> · enter next open · <b className="text-indigo-200/90">+5.54/med+2.51/win58/PF2.44 · 5/6yr (2022 +9.4)</b> · TRAIN+3.0 TEST+7.9 · ablation 3× vs bare · exit-invariant · z+2.69 · 1D-native (echo weak, like GEM1) · Z3/Z4 never co-occur so add nothing
          {zabsorb?.rows?.length ? ` · ${zabsorb.rows.length}` : ''}
        </span>
        <div className="flex items-center gap-1.5 text-[10px] ml-auto">
          {[['💎$21-89', '💎 $21-89', 'quality price zone — +7.45 vs pooled +5.54 (the cheap-stock bucket dilutes)'],
            ['🔥deep', '🔥 deep', 'RSI<40 on the confirm bar — deeper oversold'],
            ['⤴T3', '⤴ T3', 'higher-open bull confirm (T3) — the 5/5yr era-stable branch'],
            ['⤴T9', '⤴ T9', 'inside-bull confirm (T9) — the widening that doubled n at same edge']].map(([atom, label, title]) => {
            const n = (zabsorb?.rows || []).filter(r => (r.atoms || []).includes(atom)).length
            const on = zabGates.has(atom)
            return (
              <button key={atom} title={title} disabled={!n}
                onClick={() => setZabGates(s => { const x = new Set(s); x.has(atom) ? x.delete(atom) : x.add(atom); return x })}
                className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${on ? 'border-indigo-400 bg-indigo-500/20 text-indigo-100' : n ? 'border-indigo-700/40 text-indigo-300/80 hover:border-indigo-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
                {label}<span className="opacity-50"> {n}</span>
              </button>
            )
          })}
        </div>
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((zabsorb?.rows || []).filter(r => (uni === 'all' || r.universe === uni) && [...zabGates].every(g => (r.atoms || []).includes(g)))))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="zabsorb"
        accent={{ rowPrem: 'bg-indigo-500/[0.08] border-l-2 border-l-indigo-500', hover: 'hover:text-indigo-300', tz: 'text-indigo-300/90', badge: 'text-indigo-200 border-indigo-400 bg-indigo-950/60' }}
        emptyMsg={!zabsorb ? 'scanning…' : (zabGates.size ? `no Z-Absorb-Turn with ${[...zabGates].join('+')} in the last 45d` : 'no Z-Absorb-Turn in the last 45d (Z5/Z11 + wt_evr + red-L34 → T3/T9) — a rare setup (~1.7/mo universe-wide)')} />
      </>)}
      {_v('l34cont') && (<>
      {/* ── 🏆 L34→L34 continuity — the SAME L34 volume-line on the Z-absorption AND the T1 demand bar ── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-sky-300">🏆 L34→L34 <span className="text-sky-400/80">— the same volume-line persists from absorption into demand</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">+2.60 · 5/6yr · both bear yrs +</span></h2>
        <ExportBtn rows={l34c?.rows} label="l34cont" />
        <span className="text-[10px] text-md-on-surface-var/70">
          PRIOR bar = <b className="text-sky-200/90">any Z absorption carrying L34</b> · THIS bar = <b className="text-sky-200/90">T1 demand carrying the SAME L34</b> · enter next open · the <b className="text-sky-200/90">CONTINUITY</b> is the edge — L34 on the T1 bar alone is null (+0.59/med−0.91/4-6yr) · <b className="text-sky-200/90">+2.60/med+0.53 · 5/6yr, 2021 +2.65 & 2022 +2.52</b>, worst −0.5, DSR 0.84 · not RSI-subsumed (2021 flips −2.4→+2.6) · L46/L25 never persist across Z→T1 (n0), L3→L3 fails — L34 specifically · 1D-native
          {l34c?.rows?.length ? ` · ${l34c.rows.length}` : ''}
        </span>
        <div className="flex items-center gap-1.5 text-[10px] ml-auto">
          {[['🏆RS', '🏆 RS', 'the flagship gate — RS-intact → 5/5yr ALL-positive, worst +2.60, med +1.90, DSR 0.93'],
            ['💎$21-89', '💎 $21-89', 'the bucket the edge was mined in (<$8 and $8-21 are dead; 89+ has a weaker median)'],
            ['🔥deep', '🔥 deep', 'RSI<45 on the demand bar — deeper oversold']].map(([atom, label, title]) => {
            const n = (l34c?.rows || []).filter(r => (r.atoms || []).includes(atom)).length
            const on = l34Gates.has(atom)
            return (
              <button key={atom} title={title} disabled={!n}
                onClick={() => setL34Gates(s => { const x = new Set(s); x.has(atom) ? x.delete(atom) : x.add(atom); return x })}
                className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${on ? 'border-sky-400 bg-sky-500/20 text-sky-100' : n ? 'border-sky-700/40 text-sky-300/80 hover:border-sky-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
                {label}<span className="opacity-50"> {n}</span>
              </button>
            )
          })}
        </div>
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((l34c?.rows || []).filter(r => (uni === 'all' || r.universe === uni) && [...l34Gates].every(g => (r.atoms || []).includes(g)))))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="l34cont"
        accent={{ rowPrem: 'bg-sky-500/[0.08] border-l-2 border-l-sky-500', hover: 'hover:text-sky-300', tz: 'text-sky-300/90', badge: 'text-sky-200 border-sky-400 bg-sky-950/60' }}
        emptyMsg={!l34c ? 'scanning…' : (l34Gates.size ? `no L34→L34 with ${[...l34Gates].join('+')} in the last 45d` : 'no L34→L34 continuity in the last 45d (Z+L34 → T1+L34, $21-377) — a rare setup')} />
      </>)}
      {_v('cluster') && (<>
      {/* ── 🎯 Confluence / Cluster-Bottom — ≥N distinct edge families in a 10-bar window ── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-teal-300">🎯 Cluster-Bottom <span className="text-teal-400/80">— confluence of several edges = real bottom</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">×3→🔥×6+ ladder · ×6+ win72·med+9.6</span></h2>
        <ExportBtn rows={cluster?.rows} label="cluster" />
        <span className="text-[10px] text-md-on-surface-var/70">
          $21+ · <b className="text-teal-200/90">distinct edge FAMILIES in the last 10 bars</b> (capit/retest/spring/gap/atomic/Z11T11/L43/engulf, de-duplicated) · LADDER $21-89 (episode entry@birth, 4yr): ×3 <b className="text-teal-200/90">+3.80/med+2.09/win55</b> · ×4 <b className="text-teal-200/90">+4.72/win56</b> · ★×5 <b className="text-teal-200/90">+5.61/med+4.08/win59</b> · 🔥×6+ <b className="text-amber-200/90">+12.93/med+9.58/win72 · 27% end &gt;+25%</b> · <b className="text-sky-200/90">💎$89+ bucket holds its own ladder</b> (×3 med+2.42/win57 · ×5 +5.90 · ALL tiers 2022-positive — confluence bends the Fib price law; AMD ×4 @$195 → +50%) · 60% of all big winners are ×3-only — the base tier feeds, ×6+ snipes · buildup: {'{capit·retest·atomic}'} accumulate → <b className="text-teal-200/90">gap ignites</b> (completer 39%)
          {cluster?.rows?.length ? ` · ${cluster.rows.length}` : ''}
        </span>
        {(() => {
          const rsN = (cluster?.rows || []).filter(r => (r.atoms || []).some(a => a === '🏆RS' || a === '💪sec-lead')).length
          return (
            <button onClick={() => setClusterRS(v => !v)} disabled={!rsN}
              title="🏆 RS-intact only — rs=close/sector-ETF above its own EMA200 (quality dip, not a structural knife). The Replay edge '🎯Cluster🏆RS': +4.34/med+3.16/win58/PF1.94, 6/6yr incl 2022 +0.6."
              className={`ml-auto px-1.5 py-0.5 rounded border font-mono text-[10px] transition-colors ${clusterRS ? 'border-amber-400 bg-amber-500/20 text-amber-100' : rsN ? 'border-amber-700/40 text-amber-300/80 hover:border-amber-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
              🏆RS<span className="opacity-50"> {rsN}</span>
            </button>
          )
        })()}
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((cluster?.rows || []).filter(r => (uni === 'all' || r.universe === uni) && (!clusterRS || (r.atoms || []).some(a => a === '🏆RS' || a === '💪sec-lead')))))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="cluster"
        accent={{ rowPrem: 'bg-teal-500/[0.08] border-l-2 border-l-teal-500', hover: 'hover:text-teal-300', tz: 'text-teal-300/90', badge: 'text-teal-200 border-teal-400 bg-teal-950/60' }}
        emptyMsg={!cluster ? 'scanning…' : (clusterRS ? 'no RS-intact cluster today' : 'no cluster-bottom today (≥3 distinct edge families in 10 bars · $21+)')} />

      </>)}
      {_v('g3abs') && (<>
      {/* ── ⚡ G3-Abs — the "contradiction bar": G3 gap-up absorbed into a weak close ── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-lime-300">⚡ G3-Abs <span className="text-lime-400/80">— gap-up ABSORBED (contradiction bar)</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win 56 · PF1.83 · 5-6yr · 🏆RS PF2.08</span></h2>
        <ExportBtn rows={g3abs?.rows} label="g3abs" />
        <span className="text-[10px] text-md-on-surface-var/70">
          same-bar <b className="text-lime-200/90">G3 gap-up + Atomic weak close (O)</b> — buyers gapped, sellers unloaded all day, price held = absorption · $21+ · RSI&lt;45 · <b className="text-lime-200/90">+4.24/med+2.33/win56/PF1.83 · 5-6yr</b> · beats G3 alone (+3.10) &amp; dwarfs Atomic alone (+1.60/med+0.13) · inside a ≥3 cluster +4.52/PF1.93 · <b className="text-amber-200/90">🏆RS-intact tier +5.00/med+3.95/win60/PF2.08</b> · typical peak +13.7% med @ ~7 weeks — trail, don't hold
          {g3abs?.rows?.length ? ` · ${g3abs.rows.length}` : ''}
        </span>
        <div className="ml-auto"><MidChip on={midOnly} set={setMidOnly} n={(g3abs?.rows || []).filter(r => (r.atoms || []).includes('🕯️mid')).length}
          title="🕯️ mid-close (2026-07-27) — the close sits in the MIDDLE 38-62% of the bar's own range. An INVERTED-U gate: a STRONG close hurts, the middle pays. G3-Abs med +1.96→+3.43, pf 1.71→2.32, 5/6yr worst −0.8 → 6/6yr worst +1.5, DSR 1.000. The ONLY G3-Abs gate that is 6/6 with a positive worst year (🎋TLS 5/6 −1.9 · 🏆RS 4/5 −0.2) and it fires 3× as often as RS. Survives both price buckets, so no cap." /></div>
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((g3abs?.rows || []).filter(r => (uni === 'all' || r.universe === uni) && (!midOnly || (r.atoms || []).includes('🕯️mid')))))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="g3abs"
        accent={{ rowPrem: 'bg-lime-500/[0.08] border-l-2 border-l-lime-500', hover: 'hover:text-lime-300', tz: 'text-lime-300/90', badge: 'text-lime-200 border-lime-400 bg-lime-950/60' }}
        emptyMsg={!g3abs ? 'scanning…' : 'no G3-Abs today (G3 gap-up + weak O-close · $21+ · RSI<45)'} />

      </>)}
      {_v('core') && (<>
      {/* ── ✅ CORE — Weak-close gap-up ──────────────────────────────────────── */}
      <div className="mt-5 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-sky-300">✅ CORE — Weak-close gap-up (atomic)<span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~48% · R:56%</span></h2>
        <ExportBtn rows={core} label="core" />
        <span className="text-[10px] text-md-on-surface-var/70">
          bull T-signal that closes WEAK on a gap-up bar · <b className="text-sky-200/90">★premium = disp 0.5-1.5×ATR × RSI 25-35: +2.45%/win58/6yr/risk1.59</b> · sweet×RSI≥45 dead · &gt;1.5×ATR ⚠ exhaustion {core.length ? `· ${core.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={sortByDate(core)} live={live} onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave}
        onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="core"
        accent={{ rowPrem: 'bg-emerald-500/[0.07] border-l-2 border-l-emerald-500', hover: 'hover:text-sky-300', tz: 'text-sky-300/80', badge: 'text-emerald-200 border-emerald-400 bg-emerald-950/50' }}
        extras={[DISP_EXTRA, { header: '$vol', align: 'right', cell: r => r.dv_m != null ? r.dv_m + 'M' : '—' }]}
        emptyMsg={loading ? 'scanning…' : `no weak-close gap-up candidates ≥ score ${minScore}`} />

      </>)}
      {_v('capitatom') && (<>
      {/* ── 🔥 PREMIUM — Capit→Atomic confluence ────────────────────────────── */}
      <div className="mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-amber-300">🔥 PREMIUM — Capit→Atomic confluence<span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~67%</span></h2>
        <ExportBtn rows={premium} label="premium" />
        <span className="text-[10px] text-md-on-surface-var/70">
          weak-close gap-up that follows a recent B+ capitulation · <b className="text-amber-200/90">win 67% · med +4.24% vs +1.41% baseline</b> · the best combined edge {premium.length ? `· ${premium.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={sortByDate(premium)} live={live} onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave}
        onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="premium"
        accent={{ rowPrem: 'bg-amber-500/[0.07] border-l-2 border-l-amber-500', hover: 'hover:text-amber-300', tz: 'text-amber-300/80', badge: 'text-amber-200 border-amber-400 bg-amber-950/50' }}
        extras={[DISP_EXTRA, { header: '$vol', align: 'right', cell: r => r.dv_m != null ? r.dv_m + 'M' : '—' }]}
        emptyMsg={loading ? 'scanning…' : 'no Capit→Atomic confluence today (this is normal — it is rare and that is the point)'} />

      </>)}
      {_v('engulf') && (<>
      {/* ── 🥊 ENGULF-REVERSAL (GEM2, 2026-07-01) ─────────────────────────────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-rose-300">🥊 ENGULF-REVERSAL — bull-T engulfs prior 2, absorbed <span className="text-rose-500/80">(era-tilted)</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~53%</span></h2>
        <ExportBtn rows={engulfRows} label="engulf" />
        <span className="text-[10px] text-md-on-surface-var/70">
          bull-T range-engulfs prior 2 bars · ≥$21 · RSI&lt;45 · swallowed bar L46/L5 absorption · <b className="text-rose-200/90">'Engulf-L46' PF 2.94 / 6-of-6yr</b> · live proxy — paper-track {engulfRows.length ? `· ${engulfRows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={engulfRows} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="engulf"
        accent={{ rowPrem: 'bg-rose-500/[0.08] border-l-2 border-l-rose-500', hover: 'hover:text-rose-300', tz: 'text-rose-300/90', badge: 'text-rose-200 border-rose-400 bg-rose-950/60' }}
        extras={[{ header: 'swallowed', title: 'the VSA absorption line on the swallowed bar', cell: r => r.swallowed_L || '—' }]}
        emptyMsg={!engulf ? 'scanning…' : 'no engulf-reversal signals in the last 6d'} />

      </>)}
      {_v('absorbp') && (<>
      {/* ── 🪤 ABSORPTION → P reversal (flagship, 2026-06) ──────────────────────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-emerald-300">🪤 ABSORPTION → P — absorbed-supply reversal <span className="text-emerald-500/80">(flagship)</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~57%</span></h2>
        <ExportBtn rows={absRows} label="absorption_p" />
        <span className="text-[10px] text-md-on-surface-var/70">
          oversold absorption combo (distribution-Z/T + absorption-L + RSI2-extreme) → P-confirm within 3 bars → enter@P · <b className="text-emerald-200/90">P50 +2.29%/win 61% · any-P +1.70%/win 57% · 6/6yr · best in 2022 (+4.1%)</b>
          {absRows.length ? ` · ${absRows.length}` : ''}
        </span>
      </div>
      <div className="mb-1.5 flex items-center gap-1.5 flex-wrap text-[10px]">
        <span className="text-md-on-surface-var/60">confirm P-type:</span>
        {ABSP_PTYPES.map(p => {
          const on = absPFilter.includes(p)
          const n = (absp?.rows || []).filter(r => (uni === 'all' || r.universe === uni) && String(r.p_type).split('/').includes(p)).length
          const weak = p === 'P3' || p === 'P66'
          return (
            <button key={p} onClick={() => toggleAbsP(p)} disabled={!n}
              title={p === 'P50' ? 'best confirm — premium (win 61%)' : weak ? 'weak confirm (win 53%) — medium tier' : 'strong confirm'}
              className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${on ? 'border-emerald-400 bg-emerald-500/20 text-emerald-100' : n ? (weak ? 'border-white/10 text-md-on-surface-var/50 hover:border-amber-600' : 'border-white/15 text-md-on-surface-var/80 hover:border-emerald-600') : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
              {p}<span className="opacity-50"> {n}</span>
            </button>
          )
        })}
        {absPFilter.length > 0 && <button onClick={() => setAbsPFilter([])} className="px-1.5 py-0.5 rounded border border-white/15 text-md-on-surface-var/70 hover:border-white/30">clear</button>}
        <span className="text-emerald-300/70 ml-1">→ {absRows.length} shown</span>
      </div>
      <FilteredEdgeTable rows={absRows} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="absorption_p"
        accent={{ rowPrem: 'bg-emerald-500/[0.08] border-l-2 border-l-emerald-500', hover: 'hover:text-emerald-300', tz: 'text-emerald-300/90', badge: 'text-emerald-200 border-emerald-400 bg-emerald-950/60' }}
        extras={[{ header: 'P-confirm', cell: r => r.p_type || '—' },
                 { header: 'absorption', cell: r => <span className="text-[9px] text-md-on-surface-var/70">{r.absorb}</span> }]}
        emptyMsg={!absp ? 'scanning…' : (absPFilter.length ? 'no candidates for the selected P-type(s)' : 'no absorption→P reversals today (rare, high-confluence — that is the point)')} />

      </>)}
      {_v('dl1') && (<>
      {/* ── 🔄 D+L1 BEAR-TRAP REVERSAL ───────────────────────────────────────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-rose-300">🔄 D+L1→P — bear-trap reversal (P-confirmed)<span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~47%</span></h2>
        <ExportBtn rows={dl1?.rows} label="dl1" />
        <span className="text-[10px] text-md-on-surface-var/70">
          D breakdown + L12 absorption, CONFIRMED by a P reclaim within 5 bars → LONG · <b className="text-rose-200/90">enter@P: EXP +3.24% · win 59% · 6/6yr (survived 2022)</b> · no-P D+L1 = −0.86% (avoided)
          {dl1?.confirmed != null ? ` · ${dl1.confirmed} confirmed` : ''}
        </span>
        <div className="flex items-center gap-1.5 text-[10px] ml-auto">
          <button onClick={() => setDl1Mode(m => m === 'oversold' ? 'all' : 'oversold')} disabled={!dl1OversoldN}
            title="RSI<40 deep-oversold reversal — the validated premium refinement: +2.58%, win 58%, 6/6yr (strongest, survived 2022)."
            className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${dl1Mode === 'oversold' ? 'border-emerald-400 bg-emerald-500/20 text-emerald-100' : dl1OversoldN ? 'border-emerald-700/40 text-emerald-300/80 hover:border-emerald-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
            ↓ oversold<span className="opacity-50"> {dl1OversoldN}</span>
          </button>
          <button onClick={() => setDl1Mode(m => m === 'normal' ? 'all' : 'normal')} disabled={!dl1NormalN}
            title="RSI≥40 — the base D+L1→P reversal (+0.97% standalone). Less deep, weaker than the oversold tier."
            className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${dl1Mode === 'normal' ? 'border-rose-400 bg-rose-500/20 text-rose-100' : dl1NormalN ? 'border-rose-700/40 text-rose-300/80 hover:border-rose-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
            ~ non-oversold<span className="opacity-50"> {dl1NormalN}</span>
          </button>
        </div>
      </div>
      <FilteredEdgeTable rows={dl1Rows} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="dl1"
        accent={{ rowPrem: 'bg-rose-500/[0.07] border-l-2 border-l-rose-500', hover: 'hover:text-rose-300', tz: 'text-rose-300/80', badge: 'text-rose-200 border-rose-400 bg-rose-950/60' }}
        extras={[{ header: 'cci', align: 'right', cell: r => r.cci ?? '—' }]}
        emptyMsg={!dl1 ? 'scanning…' : (dl1Mode !== 'all' ? `no ${dl1Mode} D+L1 reversals today` : 'no D+L1 bear-trap reversals today')} />

      </>)}
      {_v('spring') && (<>
      {/* ── 🌀 WYCKOFF SPRING — accumulation shakeout (2026-06-27) ─────────────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-teal-300">🌀 WYCKOFF SPRING — buy the shakeout, not the breakout <span className="text-teal-500/80">(structural)</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~45%</span></h2>
        <div className="flex items-center gap-0.5 text-[10px] font-mono">
          {['spring', 'continuation'].map(m => (
            <button key={m} onClick={() => setWyMode(m)}
              title={m === 'spring' ? 'the shakeout entry (entry 1): +1.06/win54/6yr' : 'post-spring markup pullback-resume (entry 2/add): +0.70/win53; +L5 +3.01/win61/6yr'}
              className={`px-1.5 py-0.5 rounded border transition-colors ${wyMode === m ? 'border-teal-400 bg-teal-500/20 text-teal-100' : 'border-white/15 text-md-on-surface-var/60 hover:border-teal-600'}`}>
              {m === 'spring' ? 'spring' : '+continuation'}
            </button>
          ))}
        </div>
        <ExportBtn rows={wyRows} label={'wyckoff_' + wyMode} />
        <span className="text-[10px] text-md-on-surface-var/70">
          {wyMode === 'spring'
            ? <>shakeout below TR support → enter@spring, stop below spring-low · <b className="text-teal-200/90">+1.06%/win54/6yr · premium gap=G3-V +2.87</b> · <b className="text-emerald-300/90">+L43✓ (VSA absorbed body) = +4.17%/win65</b> · breakout = no edge</>
            : <>post-spring markup pullback (≤15b, bull-T, RSI&lt;60) → add · <b className="text-teal-200/90">+0.70/win53 · premium L5 +3.01/win61/6yr · RSI&lt;40 +1.04</b> · LPS/breakout/BU = dead</>}
          {wyRows.length ? ` · ${wyRows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={wyRows} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="wyckoff_spring"
        accent={{ rowPrem: 'bg-teal-500/[0.08] border-l-2 border-l-teal-500', hover: 'hover:text-teal-300', tz: 'text-teal-300/90', badge: 'text-teal-200 border-teal-400 bg-teal-950/60' }}
        extras={[{ header: 'sharpener', cell: r => (r.sharp || []).join(' ') || '—' },
                 { header: 'spring-low', align: 'right', title: 'structural stop reference', cell: r => r.spring_low != null ? '$' + r.spring_low : '—', cellClass: () => 'text-right px-2 py-1.5 font-mono text-[10px] text-rose-300/70' }]}
        emptyMsg={!wyspring ? 'scanning…' : (wyMode === 'spring' ? 'no Wyckoff springs in the last 8d (RSI 35-45 + bull-T + non-VB)' : 'no post-spring continuation entries in the last 8d')} />

      </>)}
      {_v('t6sc') && (<>
      {/* ── 🎋 T6-SC-OVERSOLD (T6 @ Wyckoff SC floor · RSI<40 — 2026-07-04) ────────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-sky-300">🎋 T6-SC-OVERSOLD — T6 at the Wyckoff support floor <span className="text-sky-500/80">(modest · robust)</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~53%</span></h2>
        <ExportBtn rows={t6scRows} label="t6_sc" />
        <span className="text-[10px] text-md-on-surface-var/70">
          T6 within ±5% of range support (SC floor) · RSI&lt;40 · non-VB · <b className="text-sky-200/90">+1.92/med+1.33/PF1.36/5-6yr/'22+0.28</b> · band+RSI plateau, 2×-slip-safe · 🌀deep=RSI&lt;35 {t6scRows.length ? `· ${t6scRows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={t6scRows} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="t6_sc"
        accent={{ rowPrem: 'bg-sky-500/[0.08] border-l-2 border-l-sky-500', hover: 'hover:text-sky-300', tz: 'text-sky-300/90', badge: 'text-sky-200 border-sky-400 bg-sky-950/60' }}
        extras={[{ header: 'SC dist', title: 'distance of close to the range support (SC floor)', cell: r => r.sc_dist_pct != null ? `${r.sc_dist_pct}%` : '—' }]}
        emptyMsg={!t6sc ? 'scanning…' : 'no T6-SC-oversold signals in the last 6d'} />

      </>)}
      {_v('sweep') && (<>
      {/* ── 🕳️ T1 LOW-SWEEP (SWEEP-only, GEM1 excluded — 2026-07-01) ──────────────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-sky-300">🕳️ T1 LOW-SWEEP — sweeps t-2(+t-3) lows, GEM1 excluded <span className="text-sky-500/80">(modest — track)</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~52%</span></h2>
        <ExportBtn rows={sweepRows} label="sweep" />
        <span className="text-[10px] text-md-on-surface-var/70">
          T1 sweeps the t-2(+t-3) lows · ≥$21 · RSI 30-50 · vol=B · GEM1 magnitude-cell removed · <b className="text-sky-200/90">SWEEP-only +2.36 / med +0.67 / PF 1.39 / 5-6yr / '22 +1.2</b> · weakest of the four cells — STATE&gt;SHAPE, refine before sizing {sweepRows.length ? `· ${sweepRows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={sweepRows} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="sweep"
        accent={{ rowPrem: 'bg-sky-500/[0.08] border-l-2 border-l-sky-500', hover: 'hover:text-sky-300', tz: 'text-sky-300/90', badge: 'text-sky-200 border-sky-400 bg-sky-950/60' }}
        extras={[{ header: 'sweep', title: 'swept t-2+t-3 lows (deeper) vs t-2 only', cell: r => r.swept_both ? 't-2+t-3' : 't-2' }]}
        emptyMsg={!sweep ? 'scanning…' : 'no T1 low-sweep signals in the last 6d'} />

      </>)}
      {_v('mtf') && (<>
      {/* ── 📐 MULTI-TF EMA STACKS (SMX/RGTI Pine ports — 2026-07-03) ─────────────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-lime-300">📐 MULTI-TF EMA — 15m·1H·4H stacks (SMX/RGTI) <span className="text-lime-500/80">(SMX = validated tier)</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~51%</span></h2>
        <ExportBtn rows={mtfEmaRows} label="mtf_ema" />
        <span className="text-[10px] text-md-on-surface-var/70">
          EMA 9/20/50/200 geometry on 15m+1H+4H, Daily RSI/vol base · <b className="text-lime-200/90">5yr path-sim: SMX +1.52/PF1.22/TR+0.30 (best) · ORANGE +1.01 · UP/LL/UPUP ≈ baseline</b> · UPUPUP near-impossible (n=2/5yr) · EOD bars {mtfEmaRows.length ? `· ${mtfEmaRows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={mtfEmaRows} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="mtf_ema"
        accent={{ rowPrem: 'bg-lime-500/[0.08] border-l-2 border-l-lime-500', hover: 'hover:text-lime-300', tz: 'text-lime-300/90', badge: 'text-lime-200 border-lime-400 bg-lime-950/60' }}
        extras={[{ header: 'stacks', title: 'which EMA-stack variants fired (SMX = the validated one)', cell: r => (r.variants || []).join('·') }]}
        emptyMsg={!mtfEma ? 'scanning…' : 'no multi-TF EMA stack matches today'} />

      </>)}
      {_v('washout') && (<>
      {/* ── 🌊 WASHOUT REVERSAL — quality oversold in a VIX-spike panic (beta-capped) ─── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-indigo-300">🌊 WASHOUT REVERSAL <span className="text-indigo-400/80">— quality capitulation, beta-capped</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~46%</span></h2>
        <ExportBtn rows={washout?.rows} label="washout" />
        <span className="text-[10px] text-md-on-surface-var/70">
          quality (β 0.6-1.5) oversold in a VIX-spike panic + RSI2 + L12/L46 absorb, non-VB · <b className="text-indigo-200/90">CORE +1.73%·win57·6/6yr</b> · ⚡energy/semis +4.4% · <b className="text-emerald-300/90">+L43✓ absorbed = +1.96%</b> · spec β&gt;1.5 & biotech/China excluded
          {washout?.rows?.length ? ` · ${washout.rows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((washout?.rows || []).filter(r => uni === 'all' || r.universe === uni)))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="washout"
        accent={{ rowPrem: 'bg-indigo-500/[0.08] border-l-2 border-l-indigo-500', hover: 'hover:text-indigo-300', tz: 'text-indigo-300/80', badge: 'text-indigo-200 border-indigo-400 bg-indigo-950/60' }}
        extras={[{ header: 'β', align: 'right', title: 'real market beta — band 0.6-1.5; spec >1.5 excluded', cell: r => r.beta, cellClass: r => `text-right px-2 py-1.5 font-mono text-[10px] ${r.beta >= 1.3 ? 'text-amber-300' : 'text-md-on-surface-var/70'}` }]}
        emptyMsg={!washout ? 'scanning…' : 'no washout-reversal candidates (needs a VIX-spike oversold regime)'} />

      </>)}
      {_v('h1bot') && (<>
      {/* ── 🕐 1H-CONFIRMED BOTTOM — 1D deep-oversold + 1H VX-climax→R2X reclaim (multi-TF) ── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-fuchsia-300">🕐 1H-CONFIRMED BOTTOM <span className="text-fuchsia-400/80">— multi-TF, flag-free</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~48%</span></h2>
        <ExportBtn rows={h1bot?.rows} label="h1bottom" />
        <span className="text-[10px] text-md-on-surface-var/70">
          1D deep-oversold (RSI&lt;35) + 1H VX-climax (RSI&lt;30)→R2X reclaim · <b className="text-fuchsia-200/90">flips the knife −0.23%→+1.38%; RSI&lt;35 +1.07/win54/6yr, RSI&lt;30 +1.38/win55/6yr</b> · caught MRNA &amp; RKLB bottoms · bypasses broken w2_spring
          {h1bot?.rows?.length ? ` · ${h1bot.rows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((h1bot?.rows || []).filter(r => uni === 'all' || r.universe === uni)))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="h1bottom"
        accent={{ rowPrem: 'bg-fuchsia-500/[0.08] border-l-2 border-l-fuchsia-500', hover: 'hover:text-fuchsia-300', tz: 'text-fuchsia-300/80', badge: 'text-fuchsia-200 border-fuchsia-400 bg-fuchsia-950/60' }}
        extras={[]}
        emptyMsg={!h1bot ? 'scanning…' : 'no 1H-confirmed bottoms (needs 1D-oversold + 1H VX-climax reclaim)'} />

      </>)}
      {_v('p55') && (<>
      {/* ── 🧬 P55 GRIND — the refined P55 setup (2026-06 research) ───────────── */}
      <div className="mt-6 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-violet-300">🧬 P55 GRIND — accumulation → shakeout → reclaim<span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~50%</span></h2>
        <ExportBtn rows={p55?.rows} label="p55grind" />
        <span className="text-[10px] text-md-on-surface-var/70">
          1D+1H P55 · good-T (T5/T6/T9-12) · non-VB · Z1G/T5 prelude · P→D→P · shallow shakeout · <b className="text-violet-200/90">clip25 ~+1% · win ~50-54% · median-positive · 5/6yr</b>
          {p55 && !p55.h1_available && <span className="text-amber-300/80"> · ⚠ 1H DB busy (degraded)</span>}
          {p55?.rows?.length ? ` · ${p55.rows.length}` : ''}
        </span>
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((p55?.rows || []).filter(r => uni === 'all' || r.universe === uni)))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="p55grind"
        accent={{ rowPrem: 'bg-violet-500/[0.07] border-l-2 border-l-violet-500', hover: 'hover:text-violet-300', tz: 'text-violet-300/80', badge: 'text-violet-200 border-violet-400 bg-violet-950/60' }}
        extras={[{ header: 'shakeout', align: 'right', cell: r => r.shakeout_pct != null ? r.shakeout_pct + '%' : '—' }]}
        emptyMsg={!p55 ? 'scanning…' : 'no P55 grind candidates today (rare — strict 6-layer filter)'} />

      </>)}
      {_v('parabola') && (<>
      {/* ── 📈 P PARABOLA RIDE — any-P + accumulation → trailing trend-ride ─────── */}
      <div className="mt-7 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-cyan-300">📈 P PARABOLA RIDE <span className="text-cyan-500/80">— trend-follow, trailing exit</span><span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~47%</span></h2>
        <ExportBtn rows={paraRows} label="pparabola" />
        <span className="text-[10px] text-md-on-surface-var/70">
          any P-signal + clean accumulation (≥50% strong closes · non-VB · Z-absent · RSI rising · advanced) → ride · <b className="text-cyan-200/90">mean +5-6% · win ~47% · edge in tail (~8% reach +50%)</b>
          {' · '}<b className="text-cyan-300/90">last 7 days</b> (sort by date to see the development; <b className="text-cyan-200">×N</b> = qualified on N days, <b className="text-emerald-300">·M</b> = M of them back-to-back — descriptive, not predictive)
          {pPara?.rows?.length ? ` · ${pPara.rows.length}` : ''}
        </span>
      </div>
      <div className="mb-1.5 rounded border border-cyan-800/40 bg-cyan-950/25 px-2.5 py-1.5 text-[10px] text-cyan-200/90 max-w-4xl">
        ℹ️ <b>Trend-following profile</b> — median ≈ 0 (many small losers), positive MEAN from the right tail (the parabolas). Win &lt; 50% is normal here.
        <b className="text-cyan-100"> The 25% TRAILING stop is mandatory</b> (−15% initial hard stop, 120-bar cap) — a fixed stop/target kills this edge.
        <br/><span className="text-cyan-300/70"><b>Signal- & anatomy-agnostic</b> (validated): which P, and the P-bar anatomy (T-type/L/EU/R2H/RSI/strong-closes), were tested across <b>all feature combos → zero lift</b> over base rate — winners look like that because it IS the base rate, not an edge. The one real filter is <b>non-VB</b>; the edge is the trailing exit. <b>Score/tier = liquidity + recency only</b> (not a quality rank). Gate: non-VB + advanced ≥3% since the P.</span>
      </div>
      {/* P-type filter + bulk export to Watchlist */}
      <div className="mb-1.5 flex items-center gap-1.5 flex-wrap text-[10px]">
        <span className="text-md-on-surface-var/60">filter P-type:</span>
        {PARA_PTYPES.map(p => {
          const on = pTypeFilter.includes(p)
          const n = (pPara?.rows || []).filter(r => (uni === 'all' || r.universe === uni) && String(r.p_type).split('/').includes(p)).length
          return (
            <button key={p} onClick={() => togglePType(p)} disabled={!n}
              className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${on ? 'border-cyan-400 bg-cyan-500/20 text-cyan-100' : n ? 'border-white/15 text-md-on-surface-var/80 hover:border-cyan-600' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
              {p}<span className="opacity-50"> {n}</span>
            </button>
          )
        })}
        {pTypeFilter.length > 0 && (
          <button onClick={() => setPTypeFilter([])} className="px-1.5 py-0.5 rounded border border-white/15 text-md-on-surface-var/70 hover:border-white/30">clear</button>
        )}
        <button onClick={() => setParaMode(m => m === 'smooth' ? 'all' : 'smooth')} disabled={!paraSmoothN}
          title="Tight-base near a recent high — validated VARIANCE-CUT mode: same mean, but win ~52% / median ≈0 (vs 41% / −6.8). Fewer parabolas, smoother ride."
          className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${paraMode === 'smooth' ? 'border-emerald-400 bg-emerald-500/20 text-emerald-100' : paraSmoothN ? 'border-emerald-700/40 text-emerald-300/80 hover:border-emerald-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
          ✓ smooth<span className="opacity-50"> {paraSmoothN}</span>
        </button>
        <button onClick={() => setParaMode(m => m === 'lottery' ? 'all' : 'lottery')} disabled={!paraLotteryN}
          title="Non-smooth (looser/volatile base) — the parabola-LOTTERY: more big tails (P≥50 ~11%) but lower win (~34-40%), median-negative. Where the moonshots hide."
          className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${paraMode === 'lottery' ? 'border-fuchsia-400 bg-fuchsia-500/20 text-fuchsia-100' : paraLotteryN ? 'border-fuchsia-700/40 text-fuchsia-300/80 hover:border-fuchsia-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
          🎰 lottery<span className="opacity-50"> {paraLotteryN}</span>
        </button>
        {(() => {
          const rsN = paraUniRows.filter(r => (r.atoms || []).some(a => a === '🏆RS' || a === '💪sec-lead')).length
          return (
            <button onClick={() => setParaRS(v => !v)} disabled={!rsN}
              title="🏆 RS-intact only — the ONE axis that lifts Parabola (paraf.py): mean +1.41→+2.60, median −1.33→−0.10, win 47→50, tail 2× with a momentum sector. Orthogonal (relative strength vs absolute momentum); filters the risk-off defensive-drift names."
              className={`px-1.5 py-0.5 rounded border font-mono transition-colors ${paraRS ? 'border-amber-400 bg-amber-500/20 text-amber-100' : rsN ? 'border-amber-700/40 text-amber-300/80 hover:border-amber-500' : 'border-white/5 text-md-on-surface-var/30 cursor-not-allowed'}`}>
              🏆 RS<span className="opacity-50"> {rsN}</span>
            </button>
          )
        })()}
        <span className="text-cyan-300/70 ml-1">→ {paraRows.length} shown</span>
        <button onClick={addParaToWatchlist} disabled={!paraRows.length}
          title={`Add all ${paraRows.length} shown tickers to the Watchlist`}
          className="ml-auto px-2 py-0.5 rounded border border-cyan-600/50 text-cyan-200 hover:bg-cyan-900/40 disabled:opacity-40 disabled:cursor-not-allowed font-semibold">★ → Watchlist ({paraRows.length})</button>
      </div>
      <FilteredEdgeTable rows={sortByDate(paraRows)} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} onToggleDate={toggleDateSort} dateArrow={dateArrow} wlPrefix="pparabola"
        accent={{ rowPrem: 'bg-cyan-500/[0.06] border-l-2 border-l-cyan-500/60', hover: 'hover:text-cyan-300', tz: 'text-cyan-300/80', badge: 'text-cyan-200 border-cyan-400 bg-cyan-950/60' }}
        extras={[{ header: 'P-type', cell: r => r.p_type || '—' },
                 { header: '×d ·row', align: 'right', title: '×N = how many days of the shown week this ticker qualified as a ride entry (date column = the freshest one). ·M = how many of those ran BACK-TO-BACK (they differ on 26% of bars: a ×3 with a skipped day is only ·2). ⚠ BOTH DESCRIPTIVE ONLY — 6yr path-sim (refire.py / streak.py) found no ladder in either: ×1 +0.57/PF1.07 vs ×3+ +0.31/PF1.04; streak 2-in-a-row (n=6844, ample power) = −0.41σ vs a random same-size draw, and the 3/4+ "lift" only tracks the n-collapse (810→77). Shows what the ride is DOING; never rank or filter by it.',
                   cell: r => r.n_fires > 1
                     ? <span title={`qualified ${r.n_fires} days (first ${r.first_date}, latest ${r.signal_date})` + (r.streak > 1 ? ` · ${r.streak} back-to-back since ${r.streak_from}` : ' · all gapped — no back-to-back run')}>
                         <span className="text-cyan-200 font-bold">×{r.n_fires}</span>
                         {r.streak > 1 ? <span className="text-emerald-300/80">·{r.streak}</span> : <span className="text-md-on-surface-var/30">·—</span>}
                       </span>
                     : <span className="text-md-on-surface-var/35">1</span>,
                   cellClass: () => 'text-right px-2 py-1.5 font-mono text-[10px]' },
                 { header: 'accum', align: 'right', cell: r => r.accum_pct != null ? '+' + r.accum_pct + '%' : '—', cellClass: () => 'text-right px-2 py-1.5 font-mono text-[10px] text-cyan-300/80' }]}
        emptyMsg={!pPara ? 'scanning…' : (pTypeFilter.length ? 'no candidates for the selected P-type(s)' : 'no P parabola-ride candidates today')} />

      </>)}
      {_v('zoneretest') && (<>
      {/* ── 🔁 ZONE-RETEST — buy the 2nd+ touch of support, not the first drop (2026-07-07) ── */}
      <div className="mt-7 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-cyan-300">🔁 ZONE-RETEST <span className="text-cyan-400/80">— buy the retest, not the first drop</span>
          <span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~50% · 4/6yr</span></h2>
        <span className="text-[10px] text-md-on-surface-var/70">
          2nd+ touch of a 25-bar support that HOLDS (green, closes above) · first-touch = knife (med −1.80 vs retest −0.06) ·
          <b className="text-cyan-200">📉DiT = premium tier</b> (e50&gt;e20&gt;e200 dip-in-trend) {zoneRt?.count ? `· ${zoneRt.count}` : ''}
        </span>
      </div>
      <div className="mb-1.5 rounded border border-cyan-800/40 bg-cyan-950/25 px-2.5 py-1.5 text-[10px] text-cyan-200/90 max-w-4xl">
        ℹ️ tiers: base retest +1.37/PF1.22 (med −0.06) · 🔥absorb +1.73/PF1.29 · <b>📉DiT +2.11/med+0.79/PF1.37</b> — dip-in-trend (short EMA pulled back but long-term up) is the best. Entry trigger-თან დააწყვილე. dip-gate RSI≤52.
      </div>
      <EdgeTable rows={sortByDate(applyQ((zoneRt?.rows || []).filter(r => uni === 'all' || r.universe === uni)))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} wlPrefix="zoneretest"
        onToggleDate={toggleDateSort} dateArrow={dateSort}
        accent={{ rowPrem: 'bg-cyan-500/[0.08] border-l-2 border-l-cyan-500', hover: 'hover:text-cyan-300', tz: 'text-cyan-300/90', badge: 'text-cyan-200 border-cyan-400 bg-cyan-950/60' }}
        extras={[{ header: '📉DiT', align: 'center', title: 'dip-in-trend geometry e50>e20>e200 — the premium tier (retest & DiT +2.11/med+0.79/PF1.37 vs base +1.37)', cell: r => r.dit ? '📉' : '·', cellClass: r => `text-center px-2 py-1.5 ${r.dit ? '' : 'text-md-on-surface-var/30'}` },
                 { header: 'support', align: 'right', title: '25-bar support level being retested', cell: r => '$' + r.support },
                 { header: '+%', align: 'right', title: '% above support at the retest close', cell: r => '+' + r.pct_above + '%', cellClass: r => `text-right px-2 py-1.5 font-mono text-[10px] ${r.pct_above <= 3 ? 'text-emerald-300' : 'text-md-on-surface-var/70'}` },
                 { header: 'touch', align: 'right', title: 'prior touches of this support in the last 15 bars (retest depth)', cell: r => '×' + r.prior_touch }]}
        emptyMsg={!zoneRt ? 'scanning…' : 'no zone-retests today (dv≥3M, RSI≤52)'} />

      </>)}
      {_v('highbase') && (<>
      {/* ── 🧗 HIGH-BASE 15m-DIP — the board's first high-base setup (validated 2026-07-08) ── */}
      <div className="mt-7 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-violet-300">🧗 HIGH-BASE 15m-DIP <span className="text-violet-400/80">— strong base, deep intraday dip</span>
          <span className="ml-1 px-1.5 py-0.5 rounded bg-white/10 border border-white/15 text-[10px] font-mono font-normal text-white/85">win ~51% · 5/6yr</span></h2>
        <span className="text-[10px] text-md-on-surface-var/70">
          above EMA200 · ≤15% off 20d-high · RSI1d 40-60 · green · <b className="text-violet-200">day's min 15m-RSI ≤28</b> {hiBase?.count ? `· ${hiBase.count}` : ''}
        </span>
      </div>
      <div className="mb-1.5 rounded border border-violet-800/40 bg-violet-950/25 px-2.5 py-1.5 text-[10px] text-violet-200/90 max-w-4xl">
        ℹ️ ავსებს <b>RGTI-2025 ტიპის ხვრელს</b>: ძლიერი რე-აკუმულაცია რომელიც დღიურ oversold-ს ვერასდროს აღწევს (მთელი dip-ბორდი დუმს), მაგრამ 15m-ზე dip ღრმაა. +1.86/med+0.34/PF1.31/5-6yr vs random +1.37±0.08 (<b>6σ</b>). Modest tier — entry trigger-თან დააწყვილე.
      </div>
      <FilteredEdgeTable rows={sortByDate(applyQ((hiBase?.rows || []).filter(r => uni === 'all' || r.universe === uni)))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} wlPrefix="highbase"
        onToggleDate={toggleDateSort} dateArrow={dateSort}
        accent={{ rowPrem: 'bg-violet-500/[0.08] border-l-2 border-l-violet-500', hover: 'hover:text-violet-300', tz: 'text-violet-300/90', badge: 'text-violet-200 border-violet-400 bg-violet-950/60' }}
        extras={[{ header: 'base', align: 'right', title: '% off the 20d high (how tight the base is)', cell: r => (r.off_high_pct > 0 ? '+' : '') + r.off_high_pct + '%', cellClass: r => `text-right px-2 py-1.5 font-mono text-[10px] ${r.off_high_pct >= -8 ? 'text-emerald-300' : 'text-md-on-surface-var/70'}` },
                 { header: '15m↓', align: 'right', title: "the day's minimum 15m RSI — the intraday washout depth", cell: r => r.rsi15_min, cellClass: r => `text-right px-2 py-1.5 font-mono text-[10px] ${r.rsi15_min <= 22 ? 'text-violet-300 font-bold' : 'text-md-on-surface-var/80'}` }]}
        emptyMsg={!hiBase ? 'scanning…' : 'no high-base 15m-dips today (dv≥3M)'} />

      </>)}
      {_v('goga') && (<>
      {/* ── 🥊 ENGULF-GOGA — accumulation DESCRIPTOR (NOT a validated edge, 2026-07-07) ── */}
      <div className="mt-7 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-fuchsia-300">🥊 ENGULF-GOGA <span className="text-fuchsia-400/80">— accumulation descriptor · NOT a buy list</span></h2>
        <span className="text-[10px] text-md-on-surface-var/70">
          green bars ABSORBING the prior distribution (net = swallowed 🔴 − 🟢 ≥3 / 34 bars) {gogaD?.count ? `· ${gogaD.count}` : ''}
        </span>
      </div>
      <div className="mb-1.5 rounded border border-fuchsia-800/40 bg-fuchsia-950/25 px-2.5 py-1.5 text-[10px] text-fuchsia-200/90 max-w-4xl">
        ⚠️ <b>descriptor, არა edge</b> — net-მა random-control 7× ვერ გაიარა (no forward edge). ეს <b>აკუმულაცია-ზონების საჩვენებელი canvas-ია</b>: სად ყლაპავს მწვანე ბარი წინა განაწილებას. ნამდვილი tradeable STATE = 📉DiT + RSI (იხ. Zone-Retest), არა net. ბარის პატერნი დაადე მოქმედებამდე.
      </div>
      <EdgeTable rows={sortByDate(applyQ((gogaD?.rows || []).filter(r => uni === 'all' || r.universe === uni)))} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} wlPrefix="goga"
        accent={{ rowPrem: 'bg-fuchsia-500/[0.08] border-l-2 border-l-fuchsia-500', hover: 'hover:text-fuchsia-300', tz: 'text-fuchsia-300/90', badge: 'text-fuchsia-200 border-fuchsia-400 bg-fuchsia-950/60' }}
        extras={[{ header: 'net', align: 'right', title: 'swallowed RED − GREEN over 34 bars (absorption strength — descriptive only)', cell: r => '+' + r.net },
                 { header: 'Σ🟢/🔴', align: 'right', title: 'total green / red candles swallowed', cell: r => r.swall_green + '/' + r.swall_red },
                 { header: '📉DiT', align: 'center', title: 'dip-in-trend geometry e50>e20>e200 (the real tradeable state)', cell: r => r.dit ? '📉' : '·', cellClass: r => `text-center px-2 py-1.5 ${r.dit ? '' : 'text-md-on-surface-var/30'}` }]}
        emptyMsg={!gogaD ? 'scanning…' : 'no accumulation signatures today (net≥3, dv≥3M)'} />

      </>)}
      {_v('radar') && (<>
      {/* ── 🎆 SPIKE-RADAR — volatility watchlist (validated 2026-07-06) ────────── */}
      <div className="mt-7 mb-1.5 flex items-baseline gap-2 flex-wrap">
        <h2 className="text-sm font-bold text-orange-300">🎆 SPIKE-RADAR <span className="text-orange-400/80">— volatility watchlist · NOT a buy list</span></h2>
        <span className="text-[10px] text-md-on-surface-var/70">
          P(+15% დღე ≤5დ) უჯრედები, 6/6yr: <b className="text-orange-200/90">mom-up 5.3× · crashed 3.6× · mom-HIGH 2.9× · vol-diffuse 1.5×</b> (base 1.7%)
          · quiet-coil/SC-zone გამორიცხული {spikeR?.rows?.length ? `· ${spikeR.rows.length}` : ''}
        </span>
      </div>
      <div className="mb-1.5 rounded border border-orange-800/40 bg-orange-950/25 px-2.5 py-1.5 text-[10px] text-orange-200/90 max-w-4xl">
        ⚠️ <b>მიმართულება არ ჟონავს</b> — ეს სია ამბობს "აქ ენერგიაა", არა "იყიდე". ყიდვის median ამ ჯგუფში უარყოფითია.
        Entry მხოლოდ Edge-ანკერით (GEM1/Z11/Spring ფაირი ამ სახელზე) ან დისკრეციულად. მეგა-რანერზე რადარი მუდმივად ანთია — watchlist-ფიდერია, არა თაიმერი.
      </div>
      <EdgeTable rows={(spikeR?.rows || []).filter(r => uni === 'all' || r.universe === uni).slice(0, 30)} live={live}
        onSelectTicker={onSelectTicker} onTkEnter={handleTkEnter} onTkLeave={handleTkLeave} wlPrefix="spikeradar"
        accent={{ rowPrem: 'bg-orange-500/[0.08] border-l-2 border-l-orange-500', hover: 'hover:text-orange-300', tz: 'text-orange-300/90', badge: 'text-orange-200 border-orange-400 bg-orange-950/60' }}
        extras={[{ header: 'drift5', align: 'right', title: '5-day price drift — the main cell driver', cell: r => (r.drift5 > 0 ? '+' : '') + r.drift5 + '%', cellClass: r => `text-right px-2 py-1.5 font-mono text-[10px] ${r.drift5 >= 10 ? 'text-emerald-300' : r.drift5 <= -10 ? 'text-rose-300' : 'text-md-on-surface-var/70'}` },
                 { header: 'rvol5', align: 'right', title: '5d volume vs own 20d baseline', cell: r => r.rvol5 != null ? r.rvol5 + '×' : '—' },
                 { header: 'lift', align: 'right', title: 'historical spike-probability multiplier of the cell', cell: r => r.lift + '×' }]}
        emptyMsg={!spikeR ? 'scanning…' : 'no radar candidates (filters: dv≥2M, price≥3, ex quiet-coil/SC)'} />

      <p className="text-[10px] text-md-on-surface-var/50 mt-3 max-w-3xl">
        <b>Entry rule (backtested):</b> next-bar open · −15% stop / +100% target · 20-bar hold · small fractional size · stand down in RISK_OFF.
        <br/><b>atoms:</b> close=O (weak close) + gap = base · R2L = oversold RSI2 · EO = escaped range + weak close · vol=B = controlled volume · wick=D = lower wick · G3 = large gap · 🔥post-capit = follows a recent B+ capitulation.
        <br/>Swing-grade, regime-dependent. The edge is small and asymmetric — this board exists to keep it <i>visible</i>, not to oversell it.
      </p>
      </>)}
    </div>
  )
}
