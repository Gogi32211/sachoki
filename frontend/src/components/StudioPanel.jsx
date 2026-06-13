/**
 * StudioPanel.jsx — Analytic Studio: DuckDB-backed signal analytics.
 *
 * Sub-tabs:
 *   Overview   — DB stats + import trigger
 *   Events     — detect & browse labelled events
 *   Patterns   — lift-based pre-event pattern mining
 *   Missed     — missed opportunities analysis
 *   False Pos  — false positive analysis
 *   Scoring Lab— define & backtest custom scoring weights
 */

import { useState, useEffect, useCallback, useRef } from 'react'
import { api } from '../api'
import StudioDatePicker from './StudioDatePicker'
import SignalChip from './SignalChip'
import CodeCandleChart from './CodeCandleChart'
import { colLabel as colToLabel } from '../utils/colLabels'

// ── DB column → display label: now sourced from utils/colLabels (single source
// of truth shared with the QLIB tab). `colToLabel` is imported above.

// ── tiny design helpers ───────────────────────────────────────────────────────
const cls = (...args) => args.filter(Boolean).join(' ')

const Card = ({ children, className = '' }) => (
  <div className={cls('rounded-xl bg-md-surface-con border border-md-outline-var p-4', className)}>
    {children}
  </div>
)

const Btn = ({ onClick, disabled, children, variant = 'primary', size = 'sm', className = '' }) => {
  const base = 'inline-flex items-center gap-1.5 font-medium rounded-lg transition-colors duration-100 disabled:opacity-40 disabled:cursor-not-allowed'
  const sizes = { sm: 'px-3 py-1.5 text-xs', md: 'px-4 py-2 text-sm', lg: 'px-5 py-2.5 text-base' }
  const variants = {
    primary:  'bg-md-primary text-md-on-primary hover:bg-md-primary/90',
    secondary:'bg-md-surface-high text-md-on-surface border border-md-outline-var hover:bg-white/5',
    danger:   'bg-red-600 text-white hover:bg-red-700',
    ghost:    'text-md-on-surface-var hover:bg-white/5',
  }
  return (
    <button onClick={onClick} disabled={disabled}
      className={cls(base, sizes[size], variants[variant], className)}>
      {children}
    </button>
  )
}

const Badge = ({ children, color = 'blue' }) => {
  const colors = {
    blue:   'bg-blue-500/20 text-blue-300 border-blue-500/30',
    green:  'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
    red:    'bg-red-500/20 text-red-300 border-red-500/30',
    yellow: 'bg-yellow-500/20 text-yellow-300 border-yellow-500/30',
    purple: 'bg-purple-500/20 text-purple-300 border-purple-500/30',
    gray:   'bg-white/10 text-white/60 border-white/20',
  }
  return (
    <span className={cls('inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold border', colors[color] || colors.gray)}>
      {children}
    </span>
  )
}

const Input = ({ label, value, onChange, type = 'text', placeholder = '', className = '' }) => (
  <label className={cls('flex flex-col gap-1', className)}>
    {label && <span className="text-[10px] text-md-on-surface-var font-medium uppercase tracking-wide">{label}</span>}
    <input
      type={type}
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      className="bg-md-surface border border-md-outline-var rounded-lg px-3 py-1.5 text-sm text-md-on-surface placeholder:text-md-on-surface-var/40 focus:outline-none focus:border-md-primary"
    />
  </label>
)

const Select = ({ label, value, onChange, options, className = '' }) => (
  <label className={cls('flex flex-col gap-1', className)}>
    {label && <span className="text-[10px] text-md-on-surface-var font-medium uppercase tracking-wide">{label}</span>}
    <select
      value={value}
      onChange={e => onChange(e.target.value)}
      className="bg-md-surface border border-md-outline-var rounded-lg px-3 py-1.5 text-sm text-md-on-surface focus:outline-none focus:border-md-primary"
    >
      {options.map(o => (
        <option key={o.value ?? o} value={o.value ?? o}>{o.label ?? o}</option>
      ))}
    </select>
  </label>
)

const Spinner = () => (
  <svg className="animate-spin h-4 w-4 text-md-primary" fill="none" viewBox="0 0 24 24">
    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.4 0 0 5.4 0 12h4z"/>
  </svg>
)

const fmtPct = (v, dec = 1) => v == null ? '–' : `${v >= 0 ? '+' : ''}${(+v).toFixed(dec)}%`
const fmtNum = (v) => v == null ? '–' : (+v).toLocaleString()
const fmtDate = (v) => v ? String(v).slice(0, 10) : '–'

// ── Sub-tab IDs ───────────────────────────────────────────────────────────────
const SUBTABS = [
  { id: 'overview',  label: '📊 Overview'     },
  { id: 'hunter',    label: '🚪 Exit Hunter'  },
  { id: 'edge',      label: '🔥 Today\'s Edge' },
  { id: 'playbook',  label: '📒 Playbook'     },
  { id: 'sigstats',  label: '📈 Signal Stats' },
  { id: 'exact',     label: '🎯 Exact Sequence' },
  { id: 'seqlab',    label: '🧬 Seq Lab'      },
  { id: 'dbchart',   label: '🕯️ DB Chart'     },
  { id: 'events',    label: '🎯 Events'       },
  { id: 'patterns',  label: '🔬 Patterns'     },
  { id: 'miss',      label: '🕵️ Missed'       },
  { id: 'fp',        label: '⚠️ False Pos'    },
  { id: 'scoring',   label: '🧪 Scoring Lab'  },
]

const PRESET_LABELS = {
  BULL_20PCT_5D:  '+20% / 5d',
  BULL_30PCT_10D: '+30% / 10d',
  BULL_50PCT_20D: '+50% / 20d',
  BULL_2X_60D:    '×2 / 60d',
  BULL_3X_90D:    '×3 / 90d',
  BEAR_DROP_20D:  '-20% drop',
  BEAR_DROP_30D:  '-30% drop',
  SIGNAL_CATCH:   'Turbo≥50 → +8%/5d',
  FALSE_POS:      'Turbo≥50 → -10%/10d',
  MISS:           'No signal → +40%/20d',
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── EXIT HUNTER TAB — find tickers about to exit accumulation (ACC_TR → MARKUP)
// ═══════════════════════════════════════════════════════════════════════════════
const HUNTER_STAGE_OPTS = [
  { value: '',          label: 'All Stages' },
  { value: 'PRIME★★',   label: 'PRIME ★★ (high signal)' },
  { value: 'READY',     label: 'READY' },
  { value: 'BUILDING',  label: 'BUILDING' },
  { value: 'SPRING★',   label: 'SPRING ★' },
  { value: 'SOS★',      label: 'SOS ★' },
  { value: 'ACC',       label: 'ACC (early)' },
  { value: 'MARKUP★',   label: 'MARKUP ★ (just broke out)' },
]
const HUNTER_STAGE_COLORS = {
  'PRIME★★':  'text-violet-300',
  'READY':    'text-emerald-300',
  'BUILDING': 'text-lime-300',
  'SPRING★':  'text-sky-300',
  'SOS★':     'text-teal-300',
  'ACC':      'text-amber-300',
  'MARKUP★':  'text-pink-300',
  'MARKUP':   'text-pink-200',
  'MKDN':     'text-red-300',
  'DIST':     'text-red-400',
  'UTAD':     'text-orange-400',
}

function ExitHunterTab() {
  const [universe, setUniverse]   = useState('')
  const [minAes,   setMinAes]     = useState(10)
  const [minPrice, setMinPrice]   = useState(5)
  const [stage,    setStage]      = useState('')
  const [preBoOnly, setPreBoOnly] = useState(true)
  const [limit,    setLimit]      = useState(100)
  const [rows,     setRows]       = useState([])
  const [loading,  setLoading]    = useState(false)
  const [error,    setError]      = useState(null)
  const [meta,     setMeta]       = useState(null)
  const [mining,   setMining]     = useState(false)
  const [lifts,    setLifts]      = useState([])
  const [calibrating, setCalibrating] = useState(false)
  const [calibSummary, setCalibSummary] = useState(null)
  const [drilldownTicker, setDrilldownTicker] = useState(null)
  const [drilldownLifts,  setDrilldownLifts]  = useState([])

  const load = async () => {
    setLoading(true); setError(null)
    try {
      const r = await api.studioAccHunter({
        universe: universe || undefined,
        min_aes: minAes,
        min_price: minPrice,
        stage: stage || undefined,
        pre_bo_only: preBoOnly ? 'true' : 'false',
        limit,
      })
      setRows(r.rows || [])
      setMeta(r)
    } catch (e) { setError(e.message) }
    finally     { setLoading(false) }
  }

  const startCalibration = async () => {
    setCalibrating(true)
    try {
      await api.studioCalibTrigger()
      const poll = async () => {
        const s = await api.studioCalibStatus()
        setCalibSummary(s.summary)
        if (s.running) setTimeout(poll, 3000)
        else { setCalibrating(false); load() }
      }
      poll()
    } catch (e) { setCalibrating(false); setError(e.message) }
  }

  const openDrilldown = async (ticker, universe) => {
    setDrilldownTicker(ticker)
    setDrilldownLifts([])
    try {
      const r = await api.studioTickerLifts(ticker, universe)
      setDrilldownLifts(r.rows || [])
    } catch {}
  }

  // ── Export helpers ────────────────────────────────────────────────────
  const downloadFile = (filename, content, mime = 'text/plain') => {
    const blob = new Blob([content], { type: mime })
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement('a')
    a.href = url; a.download = filename
    document.body.appendChild(a); a.click(); a.remove()
    URL.revokeObjectURL(url)
  }

  const exportCsv = () => {
    if (!rows.length) return
    const cols = [
      'ticker','universe','close','wyc_phase','aes_stage','aes_score',
      'prebreak_v2','prebreak_v2_band',
      'aes_trend_5d','acc_exit_class','acc_exit_in_n',
      'turbo_score','rsi_14','cci_20','change_pct','rtb_phase','sector',
      'pb_lvbo','wyc_spring','ad_fresh','ad_cluster',
      'prebreak_prime','prebreak_ready','prebreak_watch',
      'pb_wvf_confirm','pb_stop_cause',
      't_sig','z_sig','l_sig','composite_full_suffix','bar_body_wick',
      'bar_gap_range','bar_line5',
    ]
    const head = cols.join(',')
    const body = rows.map(r => cols.map(c => {
      const v = r[c]
      if (v == null) return ''
      const s = String(v)
      return s.includes(',') || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s
    }).join(',')).join('\n')
    const stamp = new Date().toISOString().slice(0, 16).replace(/[:T]/g, '-')
    downloadFile(`exit_hunter_${stamp}.csv`, head + '\n' + body, 'text/csv')
  }

  const exportTickersTV = () => {
    if (!rows.length) return
    // TradingView watchlist import format: EXCHANGE:TICKER per line (or comma-separated)
    const list = rows.map(r => {
      const ex = r.universe === 'sp500' ? 'NYSE'  // best-effort default; SP500 covers NYSE + NASDAQ
                  : r.universe === 'nasdaq' ? 'NASDAQ' : ''
      return ex ? `${ex}:${r.ticker}` : r.ticker
    }).join(',')
    const stamp = new Date().toISOString().slice(0, 16).replace(/[:T]/g, '-')
    downloadFile(`tv_watchlist_${stamp}.txt`, list, 'text/plain')
  }

  const loadLifts = async () => {
    try {
      const r = await api.studioAccLifts()
      setLifts(r.rows || [])
    } catch {}
  }

  useEffect(() => { load(); loadLifts() }, [])
  useEffect(() => { load() }, [universe, minAes, minPrice, stage, limit, preBoOnly])

  const startMining = async () => {
    setMining(true)
    try {
      await api.studioAccMine()
      const poll = async () => {
        const s = await api.studioAccStatus()
        if (s.running) setTimeout(poll, 3000)
        else { setMining(false); loadLifts(); load() }
      }
      poll()
    } catch (e) { setMining(false); setError(e.message) }
  }

  return (
    <div className="space-y-4">
      {/* Intro */}
      <Card>
        <h3 className="text-sm font-semibold text-md-on-surface mb-1">
          🚪 Exit Hunter — Stocks About to Exit Accumulation
        </h3>
        <p className="text-xs text-md-on-surface-var">
          Ranks tickers by <span className="font-mono">AES</span> (Accumulation-Exit Score) —
          a composite of pre-breakout signals weighted by their empirical historical lift
          for ACC_TR → MARKUP transition. Catches stocks <span className="text-emerald-400">2-5 bars before</span>
          {' '}the move starts, not after the breakout has already happened.
        </p>
      </Card>

      {/* Mined lifts overview */}
      <Card>
        <div className="flex items-center justify-between mb-2">
          <h4 className="text-sm font-semibold text-md-on-surface">📊 Mined Signal Lifts (Global)</h4>
          <div className="flex gap-2">
            <Btn onClick={startMining} disabled={mining || calibrating} size="sm">
              {mining ? <><Spinner /> Mining...</> : '🔬 Re-mine Global'}
            </Btn>
            <Btn onClick={startCalibration} disabled={mining || calibrating} size="sm">
              {calibrating ? <><Spinner /> Calibrating...</> : '🎯 Calibrate per-Ticker'}
            </Btn>
          </div>
        </div>
        {calibSummary && (
          <p className="text-[10px] text-emerald-400 mb-2">
            ✓ Per-ticker calibrated: {calibSummary.eligible_tickers} tickers, {fmtNum(calibSummary.rows_stored)} rows
            in {calibSummary.duration_sec}s
          </p>
        )}
        {lifts.length === 0 ? (
          <p className="text-xs text-md-on-surface-var/70">
            No mined lifts yet — click "Re-mine Lifts" to compute empirical lift for each pre-breakout signal.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-[11px]">
              <thead>
                <tr className="text-md-on-surface-var border-b border-md-outline-var">
                  <th className="text-left  px-2 py-1">Signal</th>
                  <th className="text-right px-2 py-1">n (ACC)</th>
                  <th className="text-right px-2 py-1">Lift close</th>
                  <th className="text-right px-2 py-1">Lift 2-3d</th>
                  <th className="text-right px-2 py-1">Lift 4-5d</th>
                </tr>
              </thead>
              <tbody>
                {lifts.slice(0, 12).map((l, i) => (
                  <tr key={l.signal} className="border-b border-md-outline-var/40">
                    <td className="px-2 py-1 font-mono text-md-on-surface">{l.signal}</td>
                    <td className="px-2 py-1 text-right text-md-on-surface-var">{fmtNum(l.n_acc)}</td>
                    <td className={cls('px-2 py-1 text-right font-mono',
                          l.lift_close > 2 ? 'text-lime-300'
                          : l.lift_close > 1.3 ? 'text-lime-500'
                          : l.lift_close < 0.8 ? 'text-red-400' : 'text-md-on-surface-var')}>
                      {l.lift_close?.toFixed(2)}x
                    </td>
                    <td className={cls('px-2 py-1 text-right font-mono font-bold',
                          l.lift_2_3 > 2 ? 'text-lime-300'
                          : l.lift_2_3 > 1.3 ? 'text-lime-500'
                          : l.lift_2_3 < 0.8 ? 'text-red-400' : 'text-md-on-surface-var')}>
                      {l.lift_2_3?.toFixed(2)}x
                    </td>
                    <td className="px-2 py-1 text-right text-md-on-surface-var/70">
                      {l.lift_4_5?.toFixed(2)}x
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {/* Hunter filters + table */}
      <Card>
        <div className="flex flex-wrap items-center gap-2 mb-3">
          <span className="text-[11px] text-md-on-surface-var">universe:</span>
          <select value={universe} onChange={e => setUniverse(e.target.value)}
            className="bg-md-surface-high border border-md-outline-var rounded text-[11px] text-md-on-surface px-2 py-0.5">
            <option value="">All</option>
            <option value="sp500">SP500</option>
            <option value="nasdaq">NASDAQ</option>
          </select>
          <span className="text-[11px] text-md-on-surface-var ml-2">stage:</span>
          <select value={stage} onChange={e => setStage(e.target.value)}
            className="bg-md-surface-high border border-md-outline-var rounded text-[11px] text-md-on-surface px-2 py-0.5">
            {HUNTER_STAGE_OPTS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
          <span className="text-[11px] text-md-on-surface-var ml-2">min AES:</span>
          <input type="number" value={minAes} onChange={e => setMinAes(+e.target.value || 0)}
            className="w-16 bg-md-surface-high border border-md-outline-var rounded text-[11px] text-md-on-surface px-2 py-0.5" />
          <span className="text-[11px] text-md-on-surface-var ml-1">min $:</span>
          <input type="number" value={minPrice} onChange={e => setMinPrice(+e.target.value || 0)}
            className="w-16 bg-md-surface-high border border-md-outline-var rounded text-[11px] text-md-on-surface px-2 py-0.5" />
          <span className="text-[11px] text-md-on-surface-var ml-1">limit:</span>
          <input type="number" value={limit} onChange={e => setLimit(+e.target.value || 100)}
            className="w-16 bg-md-surface-high border border-md-outline-var rounded text-[11px] text-md-on-surface px-2 py-0.5" />
          <label className="flex items-center gap-1 text-[11px] text-md-on-surface-var ml-2 cursor-pointer">
            <input type="checkbox" checked={preBoOnly} onChange={e => setPreBoOnly(e.target.checked)}
              className="accent-md-primary" />
            <span>Pre-BO only (ACC_TR / SPRING)</span>
          </label>
          <div className="ml-auto flex items-center gap-2">
            {meta && (
              <span className="text-[11px] text-md-on-surface-var">{meta.count} matches</span>
            )}
            <button onClick={exportCsv} disabled={!rows.length}
              title="Export full table as CSV for analysis (all columns)"
              className={cls(
                'px-2.5 py-1 rounded text-[11px] font-medium border transition-colors',
                rows.length
                  ? 'bg-md-surface-high text-md-on-surface border-md-outline-var hover:border-md-primary/60'
                  : 'bg-md-surface text-md-on-surface-var/40 border-md-outline-var cursor-not-allowed'
              )}>
              📥 CSV
            </button>
            <button onClick={exportTickersTV} disabled={!rows.length}
              title="Export ticker names only (TradingView watchlist format: EXCHANGE:TICKER, comma-separated)"
              className={cls(
                'px-2.5 py-1 rounded text-[11px] font-medium border transition-colors',
                rows.length
                  ? 'bg-md-surface-high text-md-on-surface border-md-outline-var hover:border-md-primary/60'
                  : 'bg-md-surface text-md-on-surface-var/40 border-md-outline-var cursor-not-allowed'
              )}>
              📋 TradingView
            </button>
          </div>
        </div>

        {error && <div className="text-xs text-red-400 py-2">{error}</div>}
        {loading && <div className="text-xs text-md-on-surface-var py-2"><Spinner /> Loading...</div>}

        {!loading && rows.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-[11px]">
              <thead>
                <tr className="text-md-on-surface-var border-b border-md-outline-var">
                  <th className="text-left  px-2 py-1.5">#</th>
                  <th className="text-left  px-2 py-1.5">Ticker</th>
                  <th className="text-left  px-2 py-1.5">Uni</th>
                  <th className="text-right px-2 py-1.5">$</th>
                  <th className="text-left  px-2 py-1.5">Stage</th>
                  <th className="text-right px-2 py-1.5 font-bold">AES</th>
                  <th className="text-center px-2 py-1.5 font-bold" title="PreBreakout v2 — data-derived, OOS-validated breakout-probability score. BUY=sweet spot, HOT=overbought/lottery, WATCH=avoid">v2</th>
                  <th className="text-right px-2 py-1.5">5d Δ</th>
                  <th className="text-left  px-2 py-1.5">Phase</th>
                  <th className="text-left  px-2 py-1.5">Class</th>
                  <th className="text-left  px-2 py-1.5">Pre-BO Signals</th>
                  <th className="text-right px-2 py-1.5">RSI</th>
                  <th className="text-right px-2 py-1.5">Turbo</th>
                  <th className="text-right px-2 py-1.5">%</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => {
                  const sigs = []
                  if (r.pb_lvbo)          sigs.push('LVBO')
                  if (r.wyc_spring)       sigs.push('SPRING')
                  if (r.wyc_sos)          sigs.push('SOS')
                  if (r.ad_cluster)       sigs.push('AD★★')
                  else if (r.ad_fresh)    sigs.push('AD')
                  if (r.prebreak_prime)   sigs.push('PRIME')
                  else if (r.prebreak_ready) sigs.push('READY')
                  else if (r.prebreak_watch) sigs.push('WATCH')
                  if (r.pb_wvf_confirm)   sigs.push('WVF')
                  if (r.pb_stop_cause)    sigs.push('STOP')
                  return (
                    <tr key={`${r.ticker}-${r.universe}-${i}`}
                        className="border-b border-md-outline-var/40 hover:bg-white/3">
                      <td className="px-2 py-1.5 text-md-on-surface-var/60">{i + 1}</td>
                      <td className="px-2 py-1.5 font-bold text-md-on-surface cursor-pointer hover:text-md-primary"
                          onClick={() => openDrilldown(r.ticker, r.universe)}
                          title="Click to inspect per-ticker signal calibration">
                        {r.ticker}
                      </td>
                      <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var">{r.universe}</td>
                      <td className="px-2 py-1.5 text-right text-md-on-surface-var">
                        ${typeof r.close === 'number' ? r.close.toFixed(2) : '—'}
                      </td>
                      <td className={cls('px-2 py-1.5 font-semibold',
                            HUNTER_STAGE_COLORS[r.aes_stage] || 'text-md-on-surface')}>
                        {r.aes_stage || '—'}
                      </td>
                      <td className={cls('px-2 py-1.5 text-right font-mono font-bold',
                            r.aes_score >= 70 ? 'text-violet-300'
                            : r.aes_score >= 50 ? 'text-emerald-300'
                            : r.aes_score >= 30 ? 'text-lime-400'
                            : 'text-md-on-surface-var')}>
                        {r.aes_score?.toFixed(0)}
                      </td>
                      <td className="px-2 py-1.5 text-center whitespace-nowrap">
                        {r.prebreak_v2 != null ? (
                          <span title={`PreBreakout v2 = ${r.prebreak_v2} (≈breakout probability). ${r.prebreak_v2_band}`}
                            className={cls('inline-block px-1.5 py-0.5 rounded border text-[10px] font-semibold',
                              r.prebreak_v2_band === 'BUY'  ? 'bg-green-500/20 text-green-300 border-green-500/40'
                              : r.prebreak_v2_band === 'HOT' ? 'bg-amber-500/20 text-amber-300 border-amber-500/40'
                              : 'bg-white/5 text-md-on-surface-var border-md-outline-var')}>
                            {r.prebreak_v2} {r.prebreak_v2_band === 'BUY' ? 'BUY' : r.prebreak_v2_band === 'HOT' ? 'HOT' : ''}
                          </span>
                        ) : <span className="text-md-on-surface-var">—</span>}
                      </td>
                      <td className={cls('px-2 py-1.5 text-right font-mono',
                            r.aes_trend_5d > 5 ? 'text-lime-400'
                            : r.aes_trend_5d > 0 ? 'text-lime-500'
                            : r.aes_trend_5d < -5 ? 'text-red-400'
                            : 'text-md-on-surface-var/60')}>
                        {r.aes_trend_5d != null ? `${r.aes_trend_5d > 0 ? '+' : ''}${r.aes_trend_5d.toFixed(0)}` : '—'}
                      </td>
                      <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var">{r.wyc_phase || '—'}</td>
                      <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var">
                        {r.acc_exit_class || '—'}
                        {r.acc_exit_in_n != null && <span className="text-md-primary"> +{r.acc_exit_in_n}d</span>}
                      </td>
                      <td className="px-2 py-1.5 font-mono text-[10px] text-md-on-surface">
                        {sigs.join(' · ') || '—'}
                      </td>
                      <td className={cls('px-2 py-1.5 text-right',
                            r.rsi_14 >= 70 ? 'text-red-400'
                            : r.rsi_14 <= 35 ? 'text-lime-400' : 'text-md-on-surface-var')}>
                        {r.rsi_14?.toFixed(0) ?? '—'}
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono text-md-on-surface-var">
                        {r.turbo_score?.toFixed(0) ?? '—'}
                      </td>
                      <td className={cls('px-2 py-1.5 text-right font-mono',
                            r.change_pct > 0 ? 'text-lime-400' : r.change_pct < 0 ? 'text-red-400' : 'text-md-on-surface-var')}>
                        {r.change_pct != null ? `${r.change_pct > 0 ? '+' : ''}${r.change_pct.toFixed(1)}%` : '—'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
        {!loading && rows.length === 0 && !error && (
          <div className="py-6 text-center text-md-on-surface-var/60 text-xs">
            No tickers match — try lowering min AES or running enrichment first.
          </div>
        )}
      </Card>

      {/* Drilldown modal */}
      {drilldownTicker && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60"
             onClick={() => setDrilldownTicker(null)}>
          <div className="bg-md-surface rounded-xl border border-md-outline-var max-w-3xl w-full max-h-[80vh] overflow-auto p-4"
               onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-semibold text-md-on-surface">
                🎯 Per-Ticker Calibration: <span className="text-md-primary">{drilldownTicker}</span>
              </h3>
              <button onClick={() => setDrilldownTicker(null)}
                className="text-md-on-surface-var hover:text-md-on-surface">✕</button>
            </div>
            <p className="text-[11px] text-md-on-surface-var mb-3">
              Empirical lift for this ticker (vs global). Bayesian-shrunk values weight
              local data against the prior of <span className="font-mono">n=20</span>. High
              <span className="font-mono"> lift_blend_2_3</span> = this ticker has historically had
              this signal active 2-3 days before its breakouts.
            </p>
            {drilldownLifts.length === 0 ? (
              <p className="text-xs text-md-on-surface-var/70 py-4 text-center">
                No per-ticker data yet — run "🎯 Calibrate per-Ticker" first.
              </p>
            ) : (
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-md-on-surface-var border-b border-md-outline-var">
                    <th className="text-left  px-2 py-1">Signal</th>
                    <th className="text-right px-2 py-1">n local</th>
                    <th className="text-right px-2 py-1">lift_local 2-3d</th>
                    <th className="text-right px-2 py-1">lift_global</th>
                    <th className="text-right px-2 py-1 font-bold">lift_blended ⭐</th>
                  </tr>
                </thead>
                <tbody>
                  {drilldownLifts.map(l => (
                    <tr key={l.signal} className="border-b border-md-outline-var/40">
                      <td className="px-2 py-1 font-mono">{l.signal}</td>
                      <td className="px-2 py-1 text-right text-md-on-surface-var">{l.n_local}</td>
                      <td className={cls('px-2 py-1 text-right font-mono',
                            l.lift_local_2_3 > 2 ? 'text-lime-300'
                            : l.lift_local_2_3 > 1.3 ? 'text-lime-500'
                            : l.lift_local_2_3 < 0.8 ? 'text-red-400' : 'text-md-on-surface-var')}>
                        {l.lift_local_2_3?.toFixed(2)}x
                      </td>
                      <td className="px-2 py-1 text-right text-md-on-surface-var/70 font-mono">
                        {l.lift_global_2_3?.toFixed(2)}x
                      </td>
                      <td className={cls('px-2 py-1 text-right font-mono font-bold',
                            l.lift_blend_2_3 > 2 ? 'text-emerald-300'
                            : l.lift_blend_2_3 > 1.3 ? 'text-lime-400'
                            : l.lift_blend_2_3 < 0.8 ? 'text-red-400' : 'text-md-on-surface-var')}>
                        {l.lift_blend_2_3?.toFixed(2)}x
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// ── TODAY'S EDGE TAB — auto-scan all tickers and rank by Pattern Quality Score
// ═══════════════════════════════════════════════════════════════════════════════
const EDGE_TABS = [
  { id: 'top_buys',   label: '📈 Top BUY',    color: 'text-lime-300' },
  { id: 'top_sells',  label: '📉 Top SELL',   color: 'text-red-300'  },
  { id: 'by_quality', label: '⭐ Sweet Spot', color: 'text-amber-300' },
]

function EdgeScannerTab() {
  const [activeView, setActiveView]   = useState('top_buys')
  const [universe,   setUniverse]     = useState('')   // '' = all
  const [minN,       setMinN]         = useState(30)
  const [minPrice,   setMinPrice]     = useState(15)
  const [running,    setRunning]      = useState(false)
  const [status,     setStatus]       = useState(null)
  const [rows,       setRows]         = useState([])
  const [meta,       setMeta]         = useState(null)
  const [loading,    setLoading]      = useState(false)

  // ── Poll status when running ──────────────────────────────────────────
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const s = await api.studioEdgeStatus()
        if (cancelled) return
        setStatus(s)
        const isRun = !!s.running
        setRunning(isRun)
        if (isRun) setTimeout(tick, 2000)
        else       loadRows(activeView)
      } catch {}
    }
    tick()
    return () => { cancelled = true }
  }, [])

  // ── Load tab results ──────────────────────────────────────────────────
  const loadRows = async (tab = activeView) => {
    setLoading(true)
    try {
      const r = await api.studioEdgeResults({
        tab, limit: 100,
        universe: universe || undefined,
        min_n: minN || undefined,
      })
      if (r.error) {
        setRows([]); setMeta({ error: r.error })
      } else {
        setRows(r.rows || []); setMeta(r)
      }
    } catch (e) {
      setRows([]); setMeta({ error: e.message })
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadRows(activeView) }, [activeView, universe, minN])

  // ── Trigger fresh scan ────────────────────────────────────────────────
  const startScan = async () => {
    setRunning(true)
    try {
      await api.studioEdgeTrigger({
        universes: ['sp500', 'nasdaq'],
        n_bars: 3,
        min_matches: 20,
        min_price: minPrice,
        min_volume: 100000,
      })
      const poll = async () => {
        const s = await api.studioEdgeStatus()
        setStatus(s)
        if (s.running) setTimeout(poll, 2000)
        else { setRunning(false); loadRows(activeView) }
      }
      poll()
    } catch (e) {
      setRunning(false); setMeta({ error: e.message })
    }
  }

  const prog = status?.progress
  const pct  = prog?.pct ?? 0
  const eta  = prog?.eta_seconds

  return (
    <div className="space-y-4">
      {/* Intro */}
      <Card>
        <div className="flex items-baseline justify-between mb-1">
          <h3 className="text-sm font-semibold text-md-on-surface">
            🔥 Today's Edge — Best Setups Across SP500 + NASDAQ
          </h3>
          {meta?.scanned_at && (
            <span className="text-[10px] text-md-on-surface-var">
              last scan: {new Date(meta.scanned_at).toLocaleString()}
            </span>
          )}
        </div>
        <p className="text-xs text-md-on-surface-var mb-3">
          Scans every ticker's most recent 3-bar profile, queries the Studio DB for
          historical matches, computes <span className="font-mono">Pattern Quality
          Score = (HH%−50) × avg_gain × log₁₀(n) / |drawdown|</span>, and ranks the
          top setups for buying and selling. Click any ticker to open it in chart.
        </p>
        <div className="flex flex-wrap gap-2 items-center">
          <Btn onClick={startScan} disabled={running} size="md">
            {running ? <><Spinner /> Scanning...</> : '🔄 Run Edge Scan'}
          </Btn>
          <span className="text-[11px] text-md-on-surface-var">min price:</span>
          <input type="number" value={minPrice} onChange={e => setMinPrice(+e.target.value || 0)}
            disabled={running}
            className="w-20 bg-md-surface-high border border-md-outline-var rounded text-[11px] text-md-on-surface px-2 py-0.5" />
          {meta && !meta.error && (
            <span className="text-[11px] text-md-on-surface-var font-mono">
              {meta.qualifying ?? '—'} qualifying / {meta.total_tickers ?? '—'} tickers
              {meta.last_data_date && (
                <> · last bars: {Object.entries(meta.last_data_date)
                  .map(([u,d]) => `${u}=${d}`).join(' · ')}</>
              )}
            </span>
          )}
        </div>

        {prog && prog.total > 0 && running && (
          <div className="mt-3">
            <div className="flex justify-between text-[10px] text-md-on-surface-var mb-1">
              <span>{prog.stage} — {prog.done}/{prog.total}</span>
              <span>{pct}% {eta != null && `· ETA ${eta}s`}</span>
            </div>
            <div className="h-2 bg-md-surface-high rounded-full overflow-hidden">
              <div className="h-full bg-md-primary transition-all duration-300"
                   style={{ width: `${Math.min(pct, 100)}%` }} />
            </div>
            {prog.cached_sequences != null && (
              <p className="text-[10px] text-md-on-surface-var/60 mt-1 font-mono">
                {fmtNum(prog.cached_sequences || 0)} distinct sequences cached
                {prog.results_so_far != null && ` · ${prog.results_so_far} qualifying`}
              </p>
            )}
          </div>
        )}
      </Card>

      {/* Tabs + filters */}
      <Card>
        <div className="flex flex-wrap items-center gap-2 mb-3">
          {EDGE_TABS.map(t => (
            <button key={t.id} onClick={() => setActiveView(t.id)}
              className={cls(
                'px-3 py-1 rounded-lg text-xs font-medium transition-colors border',
                activeView === t.id
                  ? `bg-md-surface-high border-md-primary/60 ${t.color}`
                  : 'bg-md-surface text-md-on-surface-var border-md-outline-var hover:bg-white/5'
              )}>
              {t.label}
            </button>
          ))}

          <div className="w-px h-4 bg-white/10 mx-1" />

          <span className="text-[11px] text-md-on-surface-var">universe:</span>
          <select value={universe} onChange={e => setUniverse(e.target.value)}
            className="bg-md-surface-high border border-md-outline-var rounded text-[11px] text-md-on-surface px-2 py-0.5">
            <option value="">All</option>
            <option value="sp500">SP500</option>
            <option value="nasdaq">NASDAQ</option>
          </select>

          <span className="text-[11px] text-md-on-surface-var ml-2">min n:</span>
          <input type="number" value={minN} onChange={e => setMinN(+e.target.value || 0)}
            className="w-16 bg-md-surface-high border border-md-outline-var rounded text-[11px] text-md-on-surface px-2 py-0.5" />
        </div>

        {meta?.error && (
          <div className="text-xs text-red-400 py-2">{meta.error}</div>
        )}
        {loading && <div className="text-xs text-md-on-surface-var py-2"><Spinner /> Loading...</div>}

        {!loading && !meta?.error && rows.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-[11px]">
              <thead>
                <tr className="text-md-on-surface-var border-b border-md-outline-var">
                  <th className="text-left  px-2 py-1.5">#</th>
                  <th className="text-left  px-2 py-1.5">Ticker</th>
                  <th className="text-left  px-2 py-1.5">Uni</th>
                  <th className="text-right px-2 py-1.5">$</th>
                  <th className="text-left  px-2 py-1.5">Sequence</th>
                  <th className="text-right px-2 py-1.5">HH%</th>
                  <th className="text-right px-2 py-1.5">avg→HH</th>
                  <th className="text-right px-2 py-1.5">avg→HL</th>
                  <th className="text-right px-2 py-1.5">10d fwd</th>
                  <th className="text-right px-2 py-1.5">win 10d</th>
                  <th className="text-right px-2 py-1.5">n</th>
                  <th className="text-right px-2 py-1.5 font-bold">PQS</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => (
                  <tr key={`${r.ticker}-${r.universe}-${i}`}
                      className="border-b border-md-outline-var/40 hover:bg-white/3">
                    <td className="px-2 py-1.5 text-md-on-surface-var/60">{i + 1}</td>
                    <td className="px-2 py-1.5 font-bold text-md-on-surface">{r.ticker}</td>
                    <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var">{r.universe}</td>
                    <td className="px-2 py-1.5 text-right text-[10px] text-md-on-surface-var">
                      {r.close != null ? `$${r.close}` : '—'}
                    </td>
                    <td className="px-2 py-1.5 font-mono text-[10px] text-md-on-surface/80 truncate max-w-[160px]"
                        title={r.sequence_label}>
                      {r.tz}{r.l ? '/' + r.l : ''}
                    </td>
                    <td className={cls('px-2 py-1.5 text-right font-mono',
                          (r.hh_pct || 0) >= 65 ? 'text-lime-400'
                          : (r.hh_pct || 0) >= 55 ? 'text-lime-500'
                          : (r.hh_pct || 0) >= 45 ? 'text-md-on-surface-var'
                          : 'text-red-400')}>
                      {r.hh_pct ?? '—'}%
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono text-lime-400">
                      {r.avg_pct_to_hh != null ? `+${r.avg_pct_to_hh}%` : '—'}
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono text-amber-400">
                      {r.avg_pct_to_hl != null ? `${r.avg_pct_to_hl}%` : '—'}
                    </td>
                    <td className={cls('px-2 py-1.5 text-right font-mono',
                          (r.avg_fwd_10d || 0) > 0 ? 'text-lime-400'
                          : (r.avg_fwd_10d || 0) < 0 ? 'text-red-400'
                          : 'text-md-on-surface-var')}>
                      {r.avg_fwd_10d != null ? `${r.avg_fwd_10d > 0 ? '+' : ''}${r.avg_fwd_10d}%` : '—'}
                    </td>
                    <td className="px-2 py-1.5 text-right text-md-on-surface-var">
                      {r.win_10d_pct ?? '—'}%
                    </td>
                    <td className="px-2 py-1.5 text-right text-md-on-surface-var">
                      {fmtNum(r.matches || 0)}
                    </td>
                    <td className={cls('px-2 py-1.5 text-right font-mono font-bold',
                          (r.pqs || 0) > 30 ? 'text-lime-300'
                          : (r.pqs || 0) > 10 ? 'text-lime-500'
                          : (r.pqs || 0) > 0 ? 'text-md-on-surface'
                          : (r.pqs || 0) > -10 ? 'text-amber-400'
                          : 'text-red-400')}>
                      {r.pqs ?? '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {!loading && !meta?.error && rows.length === 0 && (
          <div className="py-6 text-center text-md-on-surface-var/60 text-xs">
            No results yet — click "Run Edge Scan" to populate.
          </div>
        )}
      </Card>
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// ── EXACT SEQUENCE TAB ────────────────────────────────────────────────────────
// User enters N-bar sequence with 5 lines each (TZ+L / suffix / body_wick /
// gap_range / line5). Backend matches against enriched Studio DB and returns
// HL/HH outcome statistics.
// ═══════════════════════════════════════════════════════════════════════════════
const EXACT_UNI_OPTS = [
  { value: 'sp500',     label: 'SP500' },
  { value: 'nasdaq',    label: 'NASDAQ' },
  { value: 'russell2k', label: 'Russell 2K' },
  { value: 'both',      label: 'All (SP+NQ+R2K)' },
]
const EXACT_N_OPTS   = [2, 3, 4, 5]
const EXACT_LINE_LABELS = {
  line1: 'L1 — T/Z signal',
  line2: 'L2 — L (WLNBB)',
  line3: 'L3 — suffix',
  line4: 'L4 — body/wick',
  line5: 'L5 — gap/range',
  line6: 'L6 — VIX/PSAR/RSI2',
  line7: 'L7 — volume (W/L/N/B/VB)',
  line8: 'L8 — EMA cross (P/D)',
  line9: 'L9 — RSI range (e.g. 20-35)',
}

const EXACT_EMPTY_BAR = { tz: '', l: '', suffix: '', body_wick: '', gap_range: '', line5: '', vol: '', ema: '', rsi: '' }

function ExactBarSlot({ idx, isLast, bar, onChange, totalBars }) {
  const upd = (k, v) => onChange({ ...bar, [k]: v })
  const label = isLast
    ? 'bar 0 (now)'
    : `bar -${totalBars - 1 - idx}`
  return (
    <div className="rounded-lg border border-md-outline-var bg-md-surface/30 p-2 flex-1 min-w-[170px]">
      <div className="text-[10px] text-md-on-surface-var/70 font-mono mb-1 text-center">
        {label}
      </div>
      {[
        ['tz',        'TZ',      'e.g. T2G or T*'],
        ['l',         'L',       'e.g. L34 or L*'],
        ['suffix',    'suffix',  'e.g. EU or *'],
        ['body_wick', 'body/wk', 'e.g. STB or *'],
        ['gap_range', 'gap/rng', 'e.g. G1-C or *'],
        ['line5',     'l5',      'e.g. PS-R2X or *'],
        ['vol',       'volume',  'W/L/N/B/VB or *'],
        ['ema',       'EMA P/D', 'P2 D50 P* D* or *'],
        ['rsi',       'RSI rng', 'e.g. 20-35'],
      ].map(([k, lab, ph]) => (
        <div key={k} className="flex items-center gap-1 mb-1">
          <span className="text-[9px] text-md-on-surface-var/60 font-mono w-12">{lab}</span>
          <input
            type="text"
            value={bar[k] || ''}
            onChange={e => upd(k, e.target.value)}
            placeholder={ph}
            className="flex-1 min-w-0 bg-md-surface-high border border-md-outline-var rounded
                       px-1.5 py-0.5 text-[11px] font-mono text-md-on-surface
                       focus:outline-none focus:border-md-primary/50"
          />
        </div>
      ))}
    </div>
  )
}

// One compact outcomes band: HH / HL pivot boxes + 5/10/20 forward-return cells,
// all in a single row. Reused for 1D and 1H so the two timeframes line up.
function SeqBand({ o, label, fwdUnits, matches, baseline, hint, accent, labelColor }) {
  if (!o) return null
  const Fwd = ({ u, avg, win, n }) => (
    <div className="rounded bg-md-surface-high/40 px-2 py-1 text-center min-w-[82px]">
      <div className="text-[9px] text-md-on-surface-var/60">{u}</div>
      <div className={cls('text-base font-mono font-bold',
        avg > 0 ? 'text-lime-400' : avg < 0 ? 'text-red-400' : 'text-md-on-surface-var')}>
        {avg != null ? `${avg > 0 ? '+' : ''}${avg}%` : '—'}</div>
      <div className="text-[8px] text-md-on-surface-var/55">w {win ?? '—'}% · n {fmtNum(n)}</div>
    </div>
  )
  return (
    <div className={cls('rounded border p-2', accent)}>
      <div className="text-[10px] font-semibold mb-1.5 flex flex-wrap items-baseline gap-x-2">
        <span className={labelColor}>{label}</span>
        {matches != null && <span className="font-mono text-md-on-surface-var/70">{fmtNum(matches)} matches{baseline ? ` · ${(matches / baseline * 100).toFixed(3)}%` : ''}</span>}
        {hint && <span className="font-normal text-md-on-surface-var/45">· {hint}</span>}
      </div>
      <div className="flex gap-2 flex-wrap items-stretch">
        <div className="rounded border border-lime-700/30 bg-lime-900/15 px-3 py-2 flex-1 min-w-[210px]">
          <div className="text-[11px] text-lime-300 font-semibold mb-1">↗ Next pivot HH</div>
          <div className="flex items-baseline gap-2 mb-1">
            <span className="text-2xl font-mono font-bold text-lime-300">{o.hh_pct ?? '—'}%</span>
            <span className="text-[10px] text-md-on-surface-var/60">({o.hh_count}/{o.next_pivot_known})</span>
          </div>
          <div className="text-[12px] font-mono text-md-on-surface-var">avg gain <span className="text-lime-400 font-bold">{o.avg_pct_to_hh ?? '—'}%</span> · <span className="text-md-on-surface">{o.avg_bars_to_hh ?? '—'}</span> bars</div>
        </div>
        <div className="rounded border border-amber-700/30 bg-amber-900/15 px-3 py-2 flex-1 min-w-[210px]">
          <div className="text-[11px] text-amber-300 font-semibold mb-1">↘ Next pivot HL</div>
          <div className="flex items-baseline gap-2 mb-1">
            <span className="text-2xl font-mono font-bold text-amber-300">{o.hl_pct ?? '—'}%</span>
            <span className="text-[10px] text-md-on-surface-var/60">({o.hl_count}/{o.next_pivot_known})</span>
          </div>
          <div className="text-[12px] font-mono text-md-on-surface-var">avg drawdown <span className="text-amber-400 font-bold">{o.avg_pct_to_hl ?? '—'}%</span> · <span className="text-md-on-surface">{o.avg_bars_to_hl ?? '—'}</span> bars</div>
        </div>
        <Fwd u={fwdUnits[0]} avg={o.avg_fwd_5d}  win={o.win_5d_pct}  n={o.fwd_5d_n} />
        <Fwd u={fwdUnits[1]} avg={o.avg_fwd_10d} win={o.win_10d_pct} n={o.fwd_10d_n} />
        <Fwd u={fwdUnits[2]} avg={o.avg_fwd_20d} win={o.win_20d_pct} n={o.fwd_20d_n} />
      </div>
    </div>
  )
}

// Intraday timeframes compared under the 1D band (each loaded async, slowest last).
// Ordered 4H → 1H (descending timeframe after the 1D band on top).
const SEQ_INTRADAY = [
  { tf: '4h', label: '⏱ 4H · same sequence on 4-hour bars', units: ['5 bars', '10 bars', '20 bars'],
    hint: 'fwd = N bars (≈ N×4 hours)', accent: 'border-violet-700/40 bg-violet-900/15', color: 'text-violet-300' },
  { tf: '1h', label: '⏱ 1H · same sequence on hourly bars', units: ['5 bars', '10 bars', '20 bars'],
    hint: 'fwd = N bars (≈ hours), not days', accent: 'border-sky-700/40 bg-sky-900/15', color: 'text-sky-300' },
]

function ExactSequenceTab() {
  const [bars, setBars] = useState([
    { ...EXACT_EMPTY_BAR },
    { ...EXACT_EMPTY_BAR },
    { ...EXACT_EMPTY_BAR },
  ])
  const [uni,      setUni]      = useState('sp500')
  const [pivotLr,  setPivotLr]  = useState(3)
  const [strict,   setStrict]   = useState({
    line1: true, line2: true, line3: false, line4: false, line5: false, line6: false, line7: false, line8: false, line9: false,
  })
  const [result,   setResult]   = useState(null)
  const [loading,  setLoading]  = useState(false)
  const [error,    setError]    = useState(null)
  const [tfData,    setTfData]    = useState({})   // { '4h': result, '1h': result } — loaded async
  const [tfLoading, setTfLoading] = useState({})   // { '4h': bool, '1h': bool }

  const updateBar = (i, newBar) =>
    setBars(prev => prev.map((b, j) => j === i ? newBar : b))

  const changeN = (n) => {
    setBars(prev => {
      const cur = prev.length
      if (n > cur) return [...prev, ...Array(n - cur).fill(null).map(() => ({ ...EXACT_EMPTY_BAR }))]
      return prev.slice(0, n)
    })
  }

  const reset = () => {
    setBars(prev => prev.map(() => ({ ...EXACT_EMPTY_BAR })))
    setResult(null); setError(null)
  }

  const run = async () => {
    setLoading(true); setError(null); setResult(null); setTfData({}); setTfLoading({})
    try {
      // "both" → omit universe (backend treats null as "all universes")
      const body = { bars, strictness: strict, pivot_lr: pivotLr }
      if (uni !== 'both') body.universe = uni
      const r = await api.studioExactSequence(body)
      if (r.error) { setError(r.error); return }
      setResult(r)
      // intraday DBs are tens-of-M-bar queries (~10-20s) — load each in the BACKGROUND
      // so the 1D band stays instant.
      if (r.matches > 0) {
        SEQ_INTRADAY.forEach(({ tf }) => {
          setTfLoading(p => ({ ...p, [tf]: true }))
          api.studioExactSequence({ ...body, tf })
            .then(h => setTfData(p => ({ ...p, [tf]: h })))
            .catch(e => setTfData(p => ({ ...p, [tf]: { error: e.message } })))
            .finally(() => setTfLoading(p => ({ ...p, [tf]: false })))
        })
      }
    } catch (e) { setError(e.message) }
    finally     { setLoading(false) }
  }

  const o = result?.outcomes

  return (
    <div className="space-y-4">
      {/* Intro card */}
      <Card>
        <h3 className="text-sm font-semibold text-md-on-surface mb-1">
          Exact 9-Line Sequence — HL/HH Predictor
        </h3>
        <p className="text-xs text-md-on-surface-var">
          Type each bar's chart codes — nine independent lines:
          {' '}<span className="font-mono">TZ</span> · <span className="font-mono">L (WLNBB)</span> ·
          {' '}<span className="font-mono">suffix</span> · <span className="font-mono">body/wick</span> ·
          {' '}<span className="font-mono">gap/range</span> · <span className="font-mono">line5 (VIX/PSAR/RSI2)</span> ·
          {' '}<span className="font-mono">volume</span> · <span className="font-mono">EMA cross (P/D)</span> ·
          {' '}<span className="font-mono">RSI range</span>.
          Backend searches the enriched Studio DB for exact historical matches and
          returns Williams-pivot HL/HH outcome statistics + forward returns.
          Toggle the <span className="font-mono">LINE</span> chips to control which lines participate.
          Syntax: <span className="font-mono text-amber-300">*</span> = wildcard
          (<span className="font-mono text-amber-300">T*</span> = any T);
          {' '}<span className="font-mono text-rose-300">!</span> = NOT
          (<span className="font-mono text-rose-300">T* !T1</span> = any T except T1);
          {' '}space = OR (<span className="font-mono text-sky-300">T2 T3</span> = T2 or T3);
          {' '}EMA line: <span className="font-mono">P2 D50 P* D*</span>;
          {' '}RSI line: <span className="font-mono">20-35</span> (range).
        </p>
      </Card>

      {/* Builder */}
      <div className="rounded-xl border border-md-outline-var overflow-hidden">
        <div className="px-3 py-2 bg-emerald-900/30 border-b border-md-outline-var flex items-center justify-between">
          <span className="text-sm font-bold text-emerald-300">Sequence Builder</span>
          <button onClick={reset}
            className="px-2 py-0.5 rounded text-[10px] bg-md-surface-high text-md-on-surface-var hover:text-md-on-surface transition-colors">
            ↺ Clear
          </button>
        </div>

        {/* Bar inputs */}
        <div className="px-3 py-3 flex gap-2 flex-wrap bg-md-surface/30">
          {bars.map((bar, i) => (
            <ExactBarSlot key={i} idx={i} bar={bar}
                          isLast={i === bars.length - 1}
                          totalBars={bars.length}
                          onChange={(b) => updateBar(i, b)} />
          ))}
        </div>

        {/* Strictness + controls */}
        <div className="px-3 py-2 border-t border-md-outline-var bg-md-surface-con/40 flex items-center gap-2 flex-wrap">
          <span className="text-[10px] text-md-on-surface-var/70">match:</span>
          {Object.keys(EXACT_LINE_LABELS).map(k => (
            <button key={k}
              onClick={() => setStrict(prev => ({ ...prev, [k]: !prev[k] }))}
              title={EXACT_LINE_LABELS[k]}
              className={cls(
                'px-1.5 py-0.5 rounded text-[10px] font-mono transition-colors border',
                strict[k]
                  ? 'bg-emerald-600/30 text-emerald-300 border-emerald-700/60'
                  : 'bg-md-surface-high text-md-on-surface-var/70 border-md-outline-var hover:text-md-on-surface'
              )}>
              {k.toUpperCase()}
            </button>
          ))}

          <div className="w-px h-4 bg-white/10 mx-1" />

          <span className="text-[10px] text-md-on-surface-var/70">bars:</span>
          {EXACT_N_OPTS.map(n => (
            <button key={n} onClick={() => changeN(n)}
              className={cls(
                'px-2 py-0.5 rounded text-[10px] font-mono',
                bars.length === n
                  ? 'bg-emerald-600 text-white'
                  : 'bg-md-surface-high text-md-on-surface-var hover:text-md-on-surface'
              )}>
              {n}b
            </button>
          ))}

          <span className="text-[10px] text-md-on-surface-var/70 ml-1">pivot:</span>
          {[3, 5].map(lr => (
            <button key={lr} onClick={() => setPivotLr(lr)}
              className={cls(
                'px-2 py-0.5 rounded text-[10px] font-mono',
                pivotLr === lr
                  ? 'bg-emerald-600 text-white'
                  : 'bg-md-surface-high text-md-on-surface-var hover:text-md-on-surface'
              )}>
              {lr}-{lr}
            </button>
          ))}

          <select value={uni} onChange={e => setUni(e.target.value)}
            className="bg-md-surface-high border border-md-outline-var rounded text-[11px] text-md-on-surface px-2 py-0.5">
            {EXACT_UNI_OPTS.map(u => (
              <option key={u.value} value={u.value}>{u.label}</option>
            ))}
          </select>

          <Btn onClick={run} disabled={loading} size="sm" className="ml-auto">
            {loading ? <><Spinner /> Searching...</> : '▶ Find Matches'}
          </Btn>
        </div>

        {error && <div className="px-4 py-2 text-red-400 text-xs border-t border-md-outline-var">{error}</div>}
      </div>

      {/* Results */}
      {result && (
        <Card>
          <div className="flex items-center gap-3 mb-3 text-[11px]">
            <span className="text-md-on-surface-var/70">sequence:</span>
            <span className="font-mono text-md-on-surface">{result.sequence_label}</span>
            <span className="ml-auto text-md-on-surface-var/70">
              universe: {fmtNum(result.baseline)} bars · pivot {result.pivot_lr}-{result.pivot_lr}
            </span>
          </div>

          {result.matches === 0 ? (
            <div className="px-4 py-6 text-center text-amber-400/80 text-sm">
              0 historical matches. Try loosening match criteria (toggle off some LINE chips),
              reduce bar count, or verify input format.
            </div>
          ) : (
            <div className="space-y-3">
              {/* Matches headline */}
              <div className="flex items-baseline gap-3 px-3 py-2 bg-emerald-900/20 rounded border border-emerald-700/30">
                <span className="text-[10px] text-emerald-300/70">matches</span>
                <span className="text-2xl font-mono font-bold text-emerald-300">
                  {fmtNum(result.matches)}
                </span>
                <span className="text-[10px] text-md-on-surface-var/70">
                  ({(result.matches / result.baseline * 100).toFixed(3)}% of universe)
                </span>
              </div>

              {/* 1D band — HH/HL pivots + 5/10/20d forward returns, one row */}
              <SeqBand o={o} label="1D" labelColor="text-emerald-300"
                       fwdUnits={['5d', '10d', '20d']}
                       accent="border-emerald-700/30 bg-emerald-900/10" />

              {/* Intraday bands — SAME sequence on 4H / 1H bars, each loaded async */}
              {SEQ_INTRADAY.map(({ tf, label, units, hint, accent, color }) => {
                const d = tfData[tf]
                if (tfLoading[tf]) return (
                  <div key={tf} className={cls('rounded border px-2 py-2 text-[10px] animate-pulse', accent, color)}>
                    ⏱ loading {tf.toUpperCase()} comparison (same sequence on intraday bars)…
                  </div>
                )
                if (!d) return null
                if (d.error) return <div key={tf} className="text-[9px] text-amber-400/60">⏱ {tf.toUpperCase()} unavailable: {d.error}</div>
                if (d.matches === 0) return <div key={tf} className="text-[9px] text-md-on-surface-var/50">⏱ {tf.toUpperCase()}: 0 matches for this sequence</div>
                return (
                  <SeqBand key={tf} o={d.outcomes} label={label} labelColor={color}
                           fwdUnits={units} matches={d.matches} baseline={d.baseline}
                           hint={hint} accent={accent} />
                )
              })}

              {/* Active strictness footer */}
              <div className="text-[9px] text-md-on-surface-var/50 font-mono">
                strictness: {Object.entries(result.strictness)
                  .filter(([_, v]) => v).map(([k]) => k).join(', ') || 'none'}
              </div>
            </div>
          )}
        </Card>
      )}
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// ── OVERVIEW TAB ──────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
function OverviewTab() {
  const [stats, setStats] = useState(null)
  const [loading, setLoading] = useState(false)
  const [importing, setImporting] = useState(false)
  const [importStatus, setImportStatus] = useState(null)
  const [universes, setUniverses] = useState(['sp500', 'nasdaq'])
  const pollRef = useRef(null)

  const loadStats = useCallback(async () => {
    setLoading(true)
    try { setStats(await api.studioStats()) } catch (e) { setStats({ error: e.message }) }
    finally { setLoading(false) }
  }, [])

  useEffect(() => { loadStats() }, [loadStats])

  const startImport = async () => {
    setImporting(true)
    try {
      await api.studioImport(universes)
      pollRef.current = setInterval(async () => {
        const s = await api.studioImportStatus()
        setImportStatus(s)
        if (!s.running) {
          clearInterval(pollRef.current)
          setImporting(false)
          loadStats()
        }
      }, 2000)
    } catch (e) {
      alert(e.message)
      setImporting(false)
    }
  }

  useEffect(() => () => clearInterval(pollRef.current), [])

  const univOpts = [
    { key: 'sp500',     label: 'S&P 500'    },
    { key: 'nasdaq',    label: 'NASDAQ'     },
    { key: 'russell2k', label: 'Russell 2K' },
  ]

  return (
    <div className="flex flex-col gap-4">
      {/* DB Stats */}
      <Card>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-semibold text-md-on-surface">Database Status</h3>
          <Btn onClick={loadStats} disabled={loading} variant="secondary">
            {loading ? <Spinner /> : '↻'} Refresh
          </Btn>
        </div>
        {stats?.updating ? (
          <p className="text-xs text-amber-400 flex items-center gap-1.5">
            <span className="animate-pulse">⏳</span>
            {stats.message || 'Database is updating — stats available again shortly.'}
          </p>
        ) : stats?.error ? (
          <p className="text-xs text-red-400">Error: {stats.error}</p>
        ) : stats ? (
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
            {[
              { label: 'Total Rows', value: fmtNum(stats.rows), color: 'blue' },
              { label: 'Tickers', value: fmtNum(stats.tickers), color: 'green' },
              { label: 'Events', value: fmtNum(stats.events), color: 'purple' },
              { label: 'Date From', value: fmtDate(stats.date_from), color: 'gray' },
              { label: 'Date To', value: fmtDate(stats.date_to), color: 'gray' },
              { label: 'DB Path', value: stats.db_path?.split('/').pop(), color: 'gray' },
            ].map(s => (
              <div key={s.label} className="flex flex-col gap-0.5">
                <span className="text-[10px] text-md-on-surface-var uppercase tracking-wide">{s.label}</span>
                <span className="text-sm font-mono font-bold text-md-on-surface">{s.value}</span>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-xs text-md-on-surface-var">Loading...</p>
        )}
        {stats?.universes && (
          <div className="mt-3 flex flex-wrap gap-2">
            {Object.entries(stats.universes).map(([u, n]) => (
              <Badge key={u} color={u === 'sp500' ? 'blue' : u === 'nasdaq' ? 'green' : 'yellow'}>
                {u}: {fmtNum(n)} rows
              </Badge>
            ))}
          </div>
        )}
      </Card>

      {/* Import Panel */}
      <Card>
        <h3 className="text-sm font-semibold text-md-on-surface mb-3">Import CSV Data</h3>
        <p className="text-xs text-md-on-surface-var mb-3">
          Imports bulk_export CSVs (sp500/nasdaq/russell2k) from ~/Downloads into DuckDB.
          Computes forward returns, MFE, MAE, and event flags automatically.
        </p>
        <div className="flex flex-wrap gap-2 mb-4">
          {univOpts.map(u => (
            <button
              key={u.key}
              onClick={() => setUniverses(prev =>
                prev.includes(u.key) ? prev.filter(x => x !== u.key) : [...prev, u.key]
              )}
              className={cls(
                'px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors',
                universes.includes(u.key)
                  ? 'bg-md-primary/20 text-md-primary border-md-primary/40'
                  : 'bg-md-surface text-md-on-surface-var border-md-outline-var hover:bg-white/5'
              )}
            >
              {u.label}
            </button>
          ))}
        </div>
        <Btn onClick={startImport} disabled={importing || universes.length === 0} size="md">
          {importing ? <><Spinner /> Importing...</> : '⬆ Start Import'}
        </Btn>

        {importStatus && (
          <div className="mt-3 text-xs">
            <p className="text-md-on-surface-var mb-1">
              Status: {importStatus.running ? '🔄 Running...' : '✅ Done'}
            </p>
            {importStatus.results?.map((r, i) => (
              <div key={i} className="font-mono text-[11px] text-md-on-surface/80">
                {r.universe}: {fmtNum(r.rows_imported)} rows · {r.tickers_imported} tickers · {r.duration_sec?.toFixed(1)}s
              </div>
            ))}
          </div>
        )}
      </Card>

      {/* Enrich Panel */}
      <EnrichCard />

      {/* Daily Refresh Panel */}
      <IncrementalRefreshCard />
    </div>
  )
}


// ── Daily Incremental Refresh Card ────────────────────────────────────────
// Adds latest bars to DB without re-scanning history. Auto-runs daily at
// 17:00 ET (market close + 1h) Mon-Fri via APScheduler.
function IncrementalRefreshCard() {
  const [universes, setUniverses] = useState(['sp500', 'nasdaq'])
  const [running, setRunning]     = useState(false)
  const [status, setStatus]       = useState(null)

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const s = await api.studioIncrementalStatus()
        if (cancelled) return
        setStatus(s)
        setRunning(!!s.running)
        if (s.running) setTimeout(tick, 2000)
      } catch {}
    }
    tick()
    return () => { cancelled = true }
  }, [])

  const start = async () => {
    setRunning(true)
    try {
      await api.studioIncremental(universes)
      const poll = async () => {
        const s = await api.studioIncrementalStatus()
        setStatus(s)
        if (s.running) setTimeout(poll, 2000)
        else            setRunning(false)
      }
      poll()
    } catch (e) {
      setRunning(false)
      setStatus({ error: e.message })
    }
  }

  const prog = status?.progress
  const pct  = prog?.pct ?? 0
  const eta  = prog?.eta_seconds
  const res  = status?.results
  const uniSummaries = res?.universes || {}

  return (
    <Card>
      <div className="flex items-baseline justify-between mb-2">
        <h3 className="text-sm font-semibold text-md-on-surface">Daily Incremental Refresh</h3>
        <span className="text-[10px] text-md-on-surface-var">
          🕔 auto-runs daily 17:00 ET (Mon-Fri)
        </span>
      </div>
      <p className="text-xs text-md-on-surface-var mb-3">
        Adds latest bar(s) to existing tickers without re-scanning the full history.
        Per ticker: detect last date in DB → fetch new bars only → insert → re-enrich.
        Typically completes in 5–10 minutes for SP500+NASDAQ combined.
      </p>

      <div className="flex flex-wrap gap-2 mb-3 items-center">
        <span className="text-[11px] text-md-on-surface-var">Universes:</span>
        {['sp500', 'nasdaq'].map(u => (
          <button key={u}
            onClick={() => setUniverses(prev =>
              prev.includes(u) ? prev.filter(x => x !== u) : [...prev, u]
            )}
            className={cls(
              'px-3 py-1 text-xs font-medium rounded-lg border transition-colors',
              universes.includes(u)
                ? 'bg-md-primary/20 text-md-primary border-md-primary/40'
                : 'bg-md-surface text-md-on-surface-var border-md-outline-var hover:bg-white/5'
            )}>
            {u}
          </button>
        ))}
        <Btn onClick={start} disabled={running || universes.length === 0} size="md">
          {running ? <><Spinner /> Refreshing...</> : '🔄 Refresh Today'}
        </Btn>
      </div>

      {/* Progress bar */}
      {prog && prog.total > 0 && (
        <div className="mt-2">
          <div className="flex justify-between text-[10px] text-md-on-surface-var mb-1">
            <span>{prog.stage} — {prog.done}/{prog.total}</span>
            <span>{pct}% {eta != null && `· ETA ${eta}s`}</span>
          </div>
          <div className="h-2 bg-md-surface-high rounded-full overflow-hidden">
            <div className="h-full bg-md-primary transition-all duration-300"
                 style={{ width: `${Math.min(pct, 100)}%` }} />
          </div>
          {prog.new_rows != null && (
            <p className="text-[10px] text-md-on-surface-var/60 mt-1 font-mono">
              {fmtNum(prog.new_rows)} new rows
              {prog.affected_tickers != null && ` · ${prog.affected_tickers} tickers affected`}
              {prog.errors > 0 && ` · ${prog.errors} errors`}
            </p>
          )}
        </div>
      )}

      {/* Last result */}
      {!running && Object.keys(uniSummaries).length > 0 && (
        <div className="mt-3 text-[11px] font-mono text-md-on-surface/80 space-y-0.5">
          {Object.entries(uniSummaries).map(([uni, s]) => (
            <div key={uni}>
              ✅ <span className="text-md-primary">{uni}</span>: {fmtNum(s.new_rows_inserted || 0)} new
              rows · {s.affected_tickers || 0} tickers
              {s.errors > 0 && <span className="text-amber-400"> · {s.errors} errors</span>}
            </div>
          ))}
          {res?.duration_sec != null && (
            <div className="text-[10px] text-md-on-surface-var/60 mt-1">
              total duration: {res.duration_sec}s
            </div>
          )}
        </div>
      )}
      {res?.error && (
        <div className="mt-3 text-[11px] text-red-400">Error: {res.error}</div>
      )}
    </Card>
  )
}


// ── Enrich Card — adds bar shape / suffix / pivots / HL-HH outcomes ──────────
function EnrichCard() {
  const [universe, setUniverse] = useState('sp500')
  const [running,  setRunning]  = useState(false)
  const [status,   setStatus]   = useState(null)

  // Poll status while running
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const s = await api.studioEnrichStatus()
        if (cancelled) return
        setStatus(s)
        setRunning(!!s.running)
        if (s.running) setTimeout(tick, 2000)
      } catch {}
    }
    tick()
    return () => { cancelled = true }
  }, [])

  const start = async () => {
    setRunning(true)
    try {
      await api.studioEnrich(universe, 1)
      const poll = async () => {
        const s = await api.studioEnrichStatus()
        setStatus(s)
        if (s.running) setTimeout(poll, 2000)
        else            setRunning(false)
      }
      poll()
    } catch (e) {
      setRunning(false)
      setStatus({ error: e.message })
    }
  }

  const prog = status?.progress
  const pct  = prog?.pct ?? 0
  const eta  = prog?.eta_seconds
  const res  = status?.results

  return (
    <Card>
      <h3 className="text-sm font-semibold text-md-on-surface mb-3">Bar Enrichment</h3>
      <p className="text-xs text-md-on-surface-var mb-3">
        Computes derived per-bar columns from OHLC: <span className="font-mono">suffixes</span>,
        <span className="font-mono"> bar_body_wick</span>, <span className="font-mono">bar_gap_range</span>,
        <span className="font-mono"> bar_line5</span> (VIX-Fix/PSAR/RSI2), Williams pivots
        (3-3 + 5-5) with HL/HH outcomes, and L digit flags. Idempotent — safe to re-run.
      </p>

      <div className="flex flex-wrap gap-2 mb-3 items-center">
        <span className="text-[11px] text-md-on-surface-var">Universe:</span>
        {['sp500', 'nasdaq'].map(u => (
          <button key={u} onClick={() => setUniverse(u)}
            className={cls(
              'px-3 py-1 text-xs font-medium rounded-lg border transition-colors',
              universe === u
                ? 'bg-md-primary/20 text-md-primary border-md-primary/40'
                : 'bg-md-surface text-md-on-surface-var border-md-outline-var hover:bg-white/5'
            )}>
            {u}
          </button>
        ))}
        <Btn onClick={start} disabled={running} size="md">
          {running ? <><Spinner /> Enriching...</> : '⚡ Enrich Bars'}
        </Btn>
      </div>

      {/* Progress bar */}
      {prog && prog.total > 0 && (
        <div className="mt-2">
          <div className="flex justify-between text-[10px] text-md-on-surface-var mb-1">
            <span>{prog.stage} — {prog.done}/{prog.total} tickers</span>
            <span>{pct}% {eta != null && `· ETA ${eta}s`}</span>
          </div>
          <div className="h-2 bg-md-surface-high rounded-full overflow-hidden">
            <div className="h-full bg-md-primary transition-all duration-300"
                 style={{ width: `${Math.min(pct, 100)}%` }} />
          </div>
          {prog.rows_updated != null && (
            <p className="text-[10px] text-md-on-surface-var/60 mt-1 font-mono">
              {fmtNum(prog.rows_updated)} rows enriched
              {prog.errors > 0 && ` · ${prog.errors} errors`}
            </p>
          )}
        </div>
      )}

      {/* Last result */}
      {res && res.universe && !running && (
        <div className="mt-3 text-[11px] font-mono text-md-on-surface/80">
          ✅ {res.universe}: {fmtNum(res.rows_updated)} rows ·
          {' '}{res.tickers} tickers · {res.duration_sec}s
          {res.errors > 0 && <span className="text-amber-400"> · {res.errors} errors</span>}
        </div>
      )}
      {res?.error && (
        <div className="mt-3 text-[11px] text-red-400">Error: {res.error}</div>
      )}
    </Card>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── EVENTS TAB ────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
function EventsTab() {
  const [preset, setPreset] = useState('BULL_2X_60D')
  const [universes, setUniverses] = useState(['sp500', 'nasdaq'])
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [priceMin, setPriceMin] = useState('')
  const [priceMax, setPriceMax] = useState('')
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState(null)
  const [summary, setSummary] = useState(null)
  const [events, setEvents] = useState([])
  const [evFilter, setEvFilter] = useState('BULL_2X_60D')
  const [evLoading, setEvLoading] = useState(false)

  const loadSummary = async () => {
    try { setSummary(await api.studioEventsSummary()) } catch {}
  }
  useEffect(() => { loadSummary() }, [])

  const detect = async () => {
    setRunning(true)
    setResult(null)
    try {
      const body = {
        event_type: preset,
        universes,
        ...(dateFrom && { date_from: dateFrom }),
        ...(dateTo   && { date_to:   dateTo   }),
        ...(priceMin && { price_min: +priceMin }),
        ...(priceMax && { price_max: +priceMax }),
        clear_existing: true,
      }
      const r = await api.studioEventsDetect(body)
      setResult(r)
      loadSummary()
    } catch (e) {
      setResult({ error: e.message })
    } finally {
      setRunning(false)
    }
  }

  const loadEvents = async () => {
    setEvLoading(true)
    try {
      const rows = await api.studioEventsList({ event_type: evFilter, limit: 100 })
      setEvents(rows)
    } catch {}
    finally { setEvLoading(false) }
  }

  const univOpts = ['sp500', 'nasdaq', 'russell2k']

  const liftColor = (lift) => lift >= 10 ? 'text-emerald-400' : lift >= 5 ? 'text-yellow-300' : 'text-md-on-surface'

  return (
    <div className="flex flex-col gap-4">
      {/* Summary chips */}
      {summary && (
        <div className="flex flex-wrap gap-2">
          {summary.by_type?.map(row => (
            <button
              key={row.event_type}
              onClick={() => { setEvFilter(row.event_type); }}
              className={cls(
                'px-2.5 py-1 text-[11px] font-medium rounded-full border transition-colors',
                evFilter === row.event_type
                  ? 'bg-md-primary/20 text-md-primary border-md-primary/40'
                  : 'bg-md-surface text-md-on-surface-var border-md-outline-var hover:bg-white/5'
              )}
            >
              {PRESET_LABELS[row.event_type] || row.event_type} — {fmtNum(row.n)}
            </button>
          ))}
          <span className="ml-auto text-[11px] text-md-on-surface-var self-center">
            Total: {fmtNum(summary.total)}
          </span>
        </div>
      )}

      {/* Detect Form */}
      <Card>
        <h3 className="text-sm font-semibold mb-3">Detect Events</h3>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3 mb-4">
          <Select
            label="Event Type"
            value={preset}
            onChange={setPreset}
            options={Object.entries(PRESET_LABELS).map(([k, v]) => ({ value: k, label: v }))}
            className="lg:col-span-2"
          />
          <StudioDatePicker label="Date From" value={dateFrom} onChange={setDateFrom} placeholder="From date…" />
          <StudioDatePicker label="Date To"   value={dateTo}   onChange={setDateTo}   placeholder="To date…" />
          <Input label="Price Min" type="number" value={priceMin} onChange={setPriceMin} placeholder="e.g. 1" />
        </div>

        <div className="flex flex-wrap gap-2 mb-4">
          {univOpts.map(u => (
            <button key={u} onClick={() => setUniverses(prev =>
              prev.includes(u) ? prev.filter(x => x !== u) : [...prev, u]
            )} className={cls(
              'px-3 py-1 text-xs font-medium rounded-lg border',
              universes.includes(u)
                ? 'bg-md-primary/20 text-md-primary border-md-primary/40'
                : 'bg-md-surface text-md-on-surface-var border-md-outline-var hover:bg-white/5'
            )}>
              {u}
            </button>
          ))}
        </div>

        <Btn onClick={detect} disabled={running}>
          {running ? <><Spinner /> Detecting...</> : '🎯 Detect Events'}
        </Btn>

        {result && !result.error && (
          <div className="mt-3 grid grid-cols-2 sm:grid-cols-4 gap-3">
            {[
              { label: 'Events Found', value: fmtNum(result.total_events) },
              { label: 'Avg MFE 60d', value: result.avg_mfe_60d != null ? fmtPct(result.avg_mfe_60d) : '–' },
              ...Object.entries(result.by_universe || {}).map(([u, n]) => ({ label: u, value: fmtNum(n) })),
            ].map(s => (
              <div key={s.label} className="text-xs">
                <div className="text-md-on-surface-var">{s.label}</div>
                <div className="font-bold text-md-on-surface">{s.value}</div>
              </div>
            ))}
          </div>
        )}
        {result?.error && <p className="mt-2 text-xs text-red-400">{result.error}</p>}
      </Card>

      {/* Event List */}
      <Card>
        <div className="flex items-center gap-2 mb-3">
          <h3 className="text-sm font-semibold flex-1">Event List</h3>
          <Select
            value={evFilter}
            onChange={setEvFilter}
            options={Object.entries(PRESET_LABELS).map(([k, v]) => ({ value: k, label: v }))}
            className="w-48"
          />
          <Btn onClick={loadEvents} disabled={evLoading} variant="secondary">
            {evLoading ? <Spinner /> : '↻'} Load
          </Btn>
        </div>
        {events.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b border-md-outline-var text-md-on-surface-var">
                  <th className="text-left py-1.5 pr-3">Ticker</th>
                  <th className="text-left py-1.5 pr-3">Date</th>
                  <th className="text-left py-1.5 pr-3">Universe</th>
                  <th className="text-right py-1.5 pr-3">Close</th>
                  <th className="text-right py-1.5 pr-3">MFE 60d</th>
                  <th className="text-right py-1.5 pr-3">Fwd 30d</th>
                  <th className="text-right py-1.5">Turbo</th>
                </tr>
              </thead>
              <tbody>
                {events.map((e, i) => (
                  <tr key={i} className="border-b border-md-outline-var/30 hover:bg-white/5">
                    <td className="py-1.5 pr-3 font-mono font-bold text-md-primary">{e.ticker}</td>
                    <td className="py-1.5 pr-3 text-md-on-surface-var">{fmtDate(e.event_date)}</td>
                    <td className="py-1.5 pr-3">
                      <Badge color={e.universe === 'sp500' ? 'blue' : e.universe === 'nasdaq' ? 'green' : 'yellow'}>
                        {e.universe}
                      </Badge>
                    </td>
                    <td className="py-1.5 pr-3 text-right font-mono">${(+e.close_price).toFixed(2)}</td>
                    <td className={cls('py-1.5 pr-3 text-right font-mono font-bold', e.mfe_60d > 100 ? 'text-emerald-400' : '')}>
                      {e.mfe_60d != null ? fmtPct(e.mfe_60d) : '–'}
                    </td>
                    <td className={cls('py-1.5 pr-3 text-right font-mono', +e.fwd_30d > 0 ? 'text-emerald-400' : 'text-red-400')}>
                      {e.fwd_30d != null ? fmtPct(e.fwd_30d) : '–'}
                    </td>
                    <td className="py-1.5 text-right font-mono">
                      {e.turbo_at_event != null ? Math.round(e.turbo_at_event) : '–'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-xs text-md-on-surface-var">
            Click "Load" to browse events. Detect first if table is empty.
          </p>
        )}
      </Card>
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── PATTERNS TAB ─────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
function PatternsTab() {
  const [eventType, setEventType] = useState('BULL_2X_60D')
  const [preWindow, setPreWindow] = useState('10')
  const [minLift, setMinLift] = useState('2.0')
  const [minN, setMinN] = useState('20')
  const [comboDepth, setComboDepth] = useState('2')
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState(null)
  const [view, setView] = useState('single_signals') // single_signals | combos_2way | combos_3way | sequences

  const mine = async () => {
    setRunning(true)
    setResult(null)
    try {
      const r = await api.studioPatternsMine({
        event_type:   eventType,
        pre_window:   +preWindow,
        min_lift:     +minLift,
        min_n:        +minN,
        combo_depth:  +comboDepth,
        include_seqs: true,
      })
      setResult(r)
    } catch (e) {
      setResult({ error: e.message })
    } finally {
      setRunning(false)
    }
  }

  const liftBadgeColor = (lift) =>
    lift >= 15 ? 'green' : lift >= 8 ? 'blue' : lift >= 4 ? 'yellow' : 'gray'

  const freqBar = (freq) => (
    <div className="flex items-center gap-1.5">
      <div className="h-1.5 rounded-full bg-md-primary/30 flex-1 max-w-[60px]">
        <div className="h-full rounded-full bg-md-primary" style={{ width: `${Math.min(freq, 100)}%` }} />
      </div>
      <span className="text-[10px] text-md-on-surface-var">{freq?.toFixed(1)}%</span>
    </div>
  )

  const patternData = result && !result.error ? (result[view] || []) : []

  return (
    <div className="flex flex-col gap-4">
      {/* Config */}
      <Card>
        <h3 className="text-sm font-semibold mb-3">Mine Pre-Event Patterns</h3>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3 mb-4">
          <Select label="Event Type" value={eventType} onChange={setEventType}
            options={Object.entries(PRESET_LABELS).map(([k, v]) => ({ value: k, label: v }))}
            className="lg:col-span-2"
          />
          <Input label="Pre-Window (bars)" type="number" value={preWindow} onChange={setPreWindow} placeholder="10" />
          <Input label="Min Lift" type="number" value={minLift} onChange={setMinLift} placeholder="2.0" />
          <Input label="Min Events" type="number" value={minN} onChange={setMinN} placeholder="20" />
        </div>
        <div className="flex items-center gap-3">
          <Select label="Max Combo" value={comboDepth} onChange={setComboDepth}
            options={[{ value: '1', label: 'Singles only' }, { value: '2', label: 'Up to 2-way' }, { value: '3', label: 'Up to 3-way' }]}
          />
          <div className="flex-1" />
          <Btn onClick={mine} disabled={running} size="md">
            {running ? <><Spinner /> Mining...</> : '🔬 Mine Patterns'}
          </Btn>
        </div>
        {result?.error && <p className="mt-2 text-xs text-red-400">{result.error}</p>}
        {result && !result.error && (
          <div className="mt-3 flex flex-wrap gap-3 text-xs text-md-on-surface-var">
            <span>Events total: <b className="text-md-on-surface">{fmtNum(result.n_events_total)}</b></span>
            <span>With pre-window: <b className="text-md-on-surface">{fmtNum(result.n_events_with_prewindow)}</b></span>
            <span>Singles: <b className="text-md-on-surface">{result.single_signals?.length || 0}</b></span>
            <span>2-way combos: <b className="text-md-on-surface">{result.combos_2way?.length || 0}</b></span>
            <span>3-way combos: <b className="text-md-on-surface">{result.combos_3way?.length || 0}</b></span>
            <span>Sequences: <b className="text-md-on-surface">{result.sequences?.length || 0}</b></span>
          </div>
        )}
      </Card>

      {result && !result.error && (
        <Card>
          {/* View selector */}
          <div className="flex gap-1 mb-4 border-b border-md-outline-var pb-3 overflow-x-auto">
            {[
              { id: 'single_signals', label: `Singles`, count: result.single_signals?.length || 0 },
              { id: 'combos_2way',    label: `2-way`,   count: result.combos_2way?.length || 0 },
              { id: 'combos_3way',    label: `3-way`,   count: result.combos_3way?.length || 0 },
              { id: 'sequences',      label: `Seqs`,    count: result.sequences?.length || 0 },
            ].map(v => (
              <button key={v.id} onClick={() => setView(v.id)}
                className={cls(
                  'flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg transition-colors whitespace-nowrap',
                  view === v.id
                    ? 'bg-md-primary/20 text-md-primary'
                    : 'text-md-on-surface-var hover:bg-white/5'
                )}>
                {v.label}
                <span className={cls(
                  'text-[10px] px-1.5 py-0.5 rounded-full font-bold',
                  view === v.id ? 'bg-md-primary/30 text-md-primary' : 'bg-white/10 text-white/40'
                )}>
                  {v.count}
                </span>
              </button>
            ))}
          </div>

          {/* Table */}
          {patternData.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-xs border-collapse">
                <thead>
                  <tr className="border-b border-md-outline-var text-md-on-surface-var text-[11px]">
                    <th className="text-left py-2 pr-4 font-medium">Signal(s)</th>
                    <th className="text-right py-2 pr-4 font-medium w-20">Lift</th>
                    <th className="text-left py-2 pr-4 font-medium w-40">Freq events</th>
                    <th className="text-left py-2 pr-4 font-medium w-40">Freq base</th>
                    <th className="text-right py-2 font-medium w-16">N</th>
                  </tr>
                </thead>
                <tbody>
                  {patternData
                    // B1–B11 retired from display — drop any pattern that references a B signal
                    .filter(p => {
                      const rs = p.sequence ?? (Array.isArray(p.signals) ? p.signals : p.signal ? [p.signal] : [])
                      return !rs.some(c => /^(sig_)?b\d+$/i.test(String(c)))
                    })
                    .slice(0, 100).map((p, i) => {
                    // Normalise: singles have .signal (str), combos have .signals ([]), seqs have .sequence ([])
                    const rawSigs = p.sequence ?? (Array.isArray(p.signals) ? p.signals : p.signal ? [p.signal] : [])
                    const isSeq   = !!p.sequence
                    const lift    = p.lift
                    const freq    = p.freq_in_events
                    const base    = p.base_freq
                    const n       = p.n_events_with_signal ?? p.n

                    return (
                      <tr key={i} className="border-b border-md-outline-var/20 hover:bg-white/4 group">
                        {/* Signal chips */}
                        <td className="py-2 pr-4">
                          <div className="flex flex-wrap items-center gap-1">
                            {rawSigs.map((col, si) => (
                              <span key={si} className="flex items-center gap-1">
                                {si > 0 && (
                                  <span className="text-[10px] text-md-on-surface-var/50 font-mono">
                                    {isSeq ? '→' : '+'}
                                  </span>
                                )}
                                <SignalChip signal={colToLabel(col)} size="sm" />
                              </span>
                            ))}
                          </div>
                        </td>

                        {/* Lift badge */}
                        <td className="py-2 pr-4 text-right">
                          {lift != null ? (
                            <span className={cls(
                              'inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-bold border',
                              lift >= 15 ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30'
                              : lift >= 8  ? 'bg-blue-500/20 text-blue-300 border-blue-500/30'
                              : lift >= 4  ? 'bg-yellow-500/20 text-yellow-300 border-yellow-500/30'
                              : 'bg-white/10 text-white/50 border-white/20'
                            )}>
                              {lift.toFixed(1)}×
                            </span>
                          ) : (
                            <span className="text-md-on-surface-var/40">–</span>
                          )}
                        </td>

                        {/* Freq bars */}
                        <td className="py-2 pr-4">
                          <div className="flex items-center gap-2">
                            <div className="h-1.5 rounded-full bg-md-primary/20 w-24">
                              <div className="h-full rounded-full bg-md-primary transition-all"
                                style={{ width: `${Math.min(freq ?? 0, 100)}%` }} />
                            </div>
                            <span className="text-[10px] text-md-on-surface-var tabular-nums w-10 text-right">
                              {freq?.toFixed(1)}%
                            </span>
                          </div>
                        </td>
                        <td className="py-2 pr-4">
                          <div className="flex items-center gap-2">
                            <div className="h-1.5 rounded-full bg-white/10 w-24">
                              <div className="h-full rounded-full bg-white/30 transition-all"
                                style={{ width: `${Math.min(base ?? 0, 100)}%` }} />
                            </div>
                            <span className="text-[10px] text-md-on-surface-var/50 tabular-nums w-10 text-right">
                              {base?.toFixed(1)}%
                            </span>
                          </div>
                        </td>

                        {/* N */}
                        <td className="py-2 text-right font-mono text-[11px] text-md-on-surface-var">
                          {fmtNum(n)}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-xs text-md-on-surface-var">No patterns found for this view / filters.</p>
          )}
        </Card>
      )}
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── MISSED TAB ───────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
function MissedTab() {
  const [eventType, setEventType] = useState('BULL_2X_60D')
  const [turboMax, setTurboMax] = useState('15')
  const [preWindow, setPreWindow] = useState('20')
  const [topN, setTopN] = useState('20')
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState(null)

  const analyze = async () => {
    setRunning(true)
    setResult(null)
    try {
      const r = await api.studioMiss({
        event_type: eventType,
        turbo_max:  +turboMax,
        pre_window: +preWindow,
        top_n:      +topN,
      })
      setResult(r)
    } catch (e) {
      setResult({ error: e.message })
    } finally {
      setRunning(false)
    }
  }

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <h3 className="text-sm font-semibold mb-1">Missed Opportunities</h3>
        <p className="text-xs text-md-on-surface-var mb-3">
          Find events where the stock made a big move, but our turbo score was low (we missed it).
          Shows which signals WERE present in the pre-window — things to look for in future.
        </p>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
          <Select label="Event Type" value={eventType} onChange={setEventType}
            options={Object.entries(PRESET_LABELS).map(([k, v]) => ({ value: k, label: v }))}
            className="col-span-2"
          />
          <Input label="Turbo Max (missed)" type="number" value={turboMax} onChange={setTurboMax} placeholder="15" />
          <Input label="Pre-Window" type="number" value={preWindow} onChange={setPreWindow} placeholder="20" />
        </div>
        <Btn onClick={analyze} disabled={running}>
          {running ? <><Spinner /> Analyzing...</> : '🕵️ Analyze Misses'}
        </Btn>
        {result?.error && <p className="mt-2 text-xs text-red-400">{result.error}</p>}
      </Card>

      {result && !result.error && <MissResults result={result} />}
    </div>
  )
}

function MissResults({ result }) {
  return (
    <div className="flex flex-col gap-3">
      {/* Summary stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {[
          { label: 'Total Events', value: fmtNum(result.total_events) },
          { label: 'Missed', value: fmtNum(result.missed) },
          { label: 'Caught', value: fmtNum(result.caught) },
          { label: 'Miss Rate', value: result.miss_rate_pct != null ? `${result.miss_rate_pct.toFixed(1)}%` : '–' },
        ].map(s => (
          <Card key={s.label} className="py-2">
            <div className="text-[10px] text-md-on-surface-var uppercase tracking-wide">{s.label}</div>
            <div className="text-lg font-bold text-md-on-surface">{s.value}</div>
          </Card>
        ))}
      </div>

      {/* Why we missed — feature discriminators */}
      {result.why_missed?.length > 0 && (
        <Card>
          <h4 className="text-xs font-semibold mb-2 text-md-on-surface">
            What Distinguished Missed vs Caught Events
          </h4>
          <p className="text-[11px] text-md-on-surface-var mb-3">
            Signals where frequency in pre-window was notably different between missed and caught.
            Positive diff = signal appeared MORE in misses (was present, but turbo still low).
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b border-md-outline-var text-md-on-surface-var">
                  <th className="text-left py-1.5 pr-4">Feature</th>
                  <th className="text-right py-1.5 pr-4">% in Missed</th>
                  <th className="text-right py-1.5 pr-4">% in Caught</th>
                  <th className="text-right py-1.5">Diff</th>
                </tr>
              </thead>
              <tbody>
                {result.why_missed.map((s, i) => (
                  <tr key={i} className="border-b border-md-outline-var/30 hover:bg-white/5">
                    <td className="py-1.5 pr-4 font-mono text-md-primary">{s.feature}</td>
                    <td className="py-1.5 pr-4 text-right">{s.in_missed_pct?.toFixed(1)}%</td>
                    <td className="py-1.5 pr-4 text-right text-md-on-surface-var">{s.in_caught_pct?.toFixed(1)}%</td>
                    <td className="py-1.5 text-right">
                      <Badge color={s.diff > 10 ? 'green' : s.diff < -10 ? 'red' : 'gray'}>
                        {s.diff > 0 ? '+' : ''}{s.diff?.toFixed(1)}pp
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {/* Top missed tickers */}
      {result.examples?.length > 0 && (
        <Card>
          <h4 className="text-xs font-semibold mb-2">Top Missed Examples</h4>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b border-md-outline-var text-md-on-surface-var">
                  <th className="text-left py-1.5 pr-3">Ticker</th>
                  <th className="text-left py-1.5 pr-3">Date</th>
                  <th className="text-right py-1.5 pr-3">MFE 60d</th>
                  <th className="text-right py-1.5 pr-3">Turbo</th>
                  <th className="text-right py-1.5 pr-3">Max Turbo Pre</th>
                  <th className="text-left py-1.5">Signals in Pre</th>
                </tr>
              </thead>
              <tbody>
                {result.examples.map((e, i) => {
                  const topSigs = e.signals_in_prewindow
                    ? Object.entries(e.signals_in_prewindow).sort((a,b) => b[1]-a[1]).slice(0,3).map(([k]) => k).join(', ')
                    : '–'
                  return (
                    <tr key={i} className="border-b border-md-outline-var/30 hover:bg-white/5">
                      <td className="py-1.5 pr-3 font-mono font-bold text-md-primary">{e.ticker}</td>
                      <td className="py-1.5 pr-3 text-md-on-surface-var">{fmtDate(e.event_date)}</td>
                      <td className="py-1.5 pr-3 text-right font-mono text-emerald-400 font-bold">{e.mfe_60d != null ? fmtPct(e.mfe_60d) : '–'}</td>
                      <td className="py-1.5 pr-3 text-right font-mono text-red-400">{e.turbo_at_event?.toFixed(0) ?? '–'}</td>
                      <td className="py-1.5 pr-3 text-right font-mono text-yellow-300">{e.max_turbo_in_prewindow?.toFixed(0) ?? '–'}</td>
                      <td className="py-1.5 text-[10px] text-md-on-surface-var">{topSigs}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── FALSE POSITIVES TAB ───────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
function FalsePostitiveTab() {
  const [turboMin, setTurboMin] = useState('50')
  const [fwdMax, setFwdMax] = useState('-10')
  const [fwdCol, setFwdCol] = useState('fwd_10d')
  const [preWindow, setPreWindow] = useState('5')
  const [topN, setTopN] = useState('20')
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState(null)

  const analyze = async () => {
    setRunning(true)
    setResult(null)
    try {
      const r = await api.studioFP({
        turbo_min:  +turboMin,
        fwd_max:    +fwdMax,
        fwd_col:    fwdCol,
        pre_window: +preWindow,
        top_n:      +topN,
      })
      setResult(r)
    } catch (e) {
      setResult({ error: e.message })
    } finally {
      setRunning(false)
    }
  }

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <h3 className="text-sm font-semibold mb-1">False Positive Analysis</h3>
        <p className="text-xs text-md-on-surface-var mb-3">
          Find bars where turbo score was high (signal fired) but price dropped.
          Discover which signals co-appear on false positives so we can add penalty weights.
        </p>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
          <Input label="Turbo Min (signal)" type="number" value={turboMin} onChange={setTurboMin} />
          <Input label={`Fwd Max (drop threshold)`} type="number" value={fwdMax} onChange={setFwdMax} />
          <Select label="Forward Column" value={fwdCol} onChange={setFwdCol}
            options={['fwd_5d','fwd_10d','fwd_20d','fwd_30d'].map(v => ({ value: v, label: v }))}
          />
          <Input label="Pre-Window" type="number" value={preWindow} onChange={setPreWindow} />
        </div>
        <Btn onClick={analyze} disabled={running}>
          {running ? <><Spinner /> Analyzing...</> : '⚠️ Analyze False Positives'}
        </Btn>
        {result?.error && <p className="mt-2 text-xs text-red-400">{result.error}</p>}
      </Card>

      {result && !result.error && <FPResults result={result} />}
    </div>
  )
}

function FPResults({ result }) {
  return (
    <div className="flex flex-col gap-3">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {[
          { label: 'Total w/ Signal', value: fmtNum(result.total_bars_with_signal) },
          { label: 'False Positives', value: fmtNum(result.fp) },
          { label: 'Winners', value: fmtNum(result.winners) },
          { label: 'FP Rate', value: result.fp_rate_pct != null ? `${result.fp_rate_pct.toFixed(1)}%` : '–' },
        ].map(s => (
          <Card key={s.label} className="py-2">
            <div className="text-[10px] text-md-on-surface-var uppercase tracking-wide">{s.label}</div>
            <div className="text-lg font-bold text-md-on-surface">{s.value}</div>
          </Card>
        ))}
      </div>

      {/* Feature discriminators */}
      {result.discriminators?.length > 0 && (
        <Card>
          <h4 className="text-xs font-semibold mb-2 text-red-400">⚠ FP Discriminating Features</h4>
          <p className="text-[11px] text-md-on-surface-var mb-3">
            Features with highest frequency difference between FPs and winners.
            Positive diff = appears more on FPs → add penalty weight in Scoring Lab.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b border-md-outline-var text-md-on-surface-var">
                  <th className="text-left py-1.5 pr-4">Feature</th>
                  <th className="text-right py-1.5 pr-4">% in FP</th>
                  <th className="text-right py-1.5 pr-4">% in Win</th>
                  <th className="text-right py-1.5 pr-4">Diff</th>
                  <th className="text-right py-1.5">Power</th>
                </tr>
              </thead>
              <tbody>
                {result.discriminators.map((d, i) => (
                  <tr key={i} className="border-b border-md-outline-var/30 hover:bg-white/5">
                    <td className="py-1.5 pr-4 font-mono text-md-primary">{d.feature}</td>
                    <td className={cls('py-1.5 pr-4 text-right font-mono', d.diff > 0 ? 'text-red-400' : '')}>{d.in_fp_pct?.toFixed(1)}%</td>
                    <td className="py-1.5 pr-4 text-right text-md-on-surface-var">{d.in_win_pct?.toFixed(1)}%</td>
                    <td className="py-1.5 pr-4 text-right font-mono">
                      <span className={d.diff > 0 ? 'text-red-400' : 'text-emerald-400'}>
                        {d.diff > 0 ? '+' : ''}{d.diff?.toFixed(1)}pp
                      </span>
                    </td>
                    <td className="py-1.5 text-right">
                      <Badge color={d.power === 'HIGH' ? 'red' : d.power === 'MEDIUM' ? 'yellow' : 'gray'}>
                        {d.power}
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {result.fp_killer_combos?.length > 0 && (
        <Card>
          <h4 className="text-xs font-semibold mb-2">FP Killer Combos</h4>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b border-md-outline-var text-md-on-surface-var">
                  <th className="text-left py-1.5 pr-4">Combo</th>
                  <th className="text-right py-1.5 pr-4">FP Precision</th>
                  <th className="text-right py-1.5">N</th>
                </tr>
              </thead>
              <tbody>
                {result.fp_killer_combos.map((c, i) => (
                  <tr key={i} className="border-b border-md-outline-var/30 hover:bg-white/5">
                    <td className="py-1.5 pr-4 font-mono text-[11px]">
                      {c.combo.split(' + ').map((s, si) => (
                        <span key={si} className="inline-block mr-1 px-1.5 py-0.5 bg-red-500/10 text-red-300 rounded">{s}</span>
                      ))}
                    </td>
                    <td className="py-1.5 pr-4 text-right">
                      <Badge color={c.fp_precision > 70 ? 'red' : c.fp_precision > 55 ? 'yellow' : 'gray'}>
                        {c.fp_precision?.toFixed(0)}%
                      </Badge>
                    </td>
                    <td className="py-1.5 text-right font-mono text-md-on-surface-var">{fmtNum(c.n)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {result.fp_examples?.length > 0 && (
        <Card>
          <h4 className="text-xs font-semibold mb-2">FP Examples</h4>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b border-md-outline-var text-md-on-surface-var">
                  <th className="text-left py-1.5 pr-3">Ticker</th>
                  <th className="text-left py-1.5 pr-3">Date</th>
                  <th className="text-right py-1.5 pr-3">Close</th>
                  <th className="text-right py-1.5">Fwd Return</th>
                </tr>
              </thead>
              <tbody>
                {result.fp_examples.map((e, i) => {
                  const fwdVal = e[result.fwd_col]
                  return (
                    <tr key={i} className="border-b border-md-outline-var/30 hover:bg-white/5">
                      <td className="py-1.5 pr-3 font-mono font-bold text-md-primary">{e.ticker}</td>
                      <td className="py-1.5 pr-3 text-md-on-surface-var">{fmtDate(e.date)}</td>
                      <td className="py-1.5 pr-3 text-right font-mono">${(+e.close).toFixed(2)}</td>
                      <td className="py-1.5 text-right font-mono text-red-400 font-bold">
                        {fwdVal != null ? fmtPct(fwdVal) : '–'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── SCORING LAB TAB ───────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// Common signal options for the weight editor
const SCORE_SIGNAL_OPTS = [
  // Scores
  'turbo_score','turbo_score_n5','turbo_score_n10',
  // T signals (individual)
  'sig_t1g','sig_t2g',
  'sig_t1','sig_t2','sig_t3','sig_t4','sig_t5','sig_t6',
  'sig_t9','sig_t10','sig_t11','sig_t12',
  // Z signals (individual)
  'sig_z1g','sig_z2g',
  'sig_z1','sig_z2','sig_z3','sig_z4','sig_z5','sig_z6',
  'sig_z9','sig_z10','sig_z11','sig_z12',
  // TZ state
  'sig_tz_flip',
  // Volume / VABS
  'sig_va','sig_abs','sig_bc','sig_sc','sig_vol_5x','sig_vol_10x','sig_vol_20x',
  // WLNBB / L
  'wyc_spring','wyc_sos','sig_fri34','sig_fri43','l34','l43','l22',
  'sig_cci','sig_l_any','sig_blue',
  // AD / Prebreak
  'ad_cluster','ad_fresh',
  'prebreak_prime','prebreak_ready','prebreak_watch','pb_lvbo','pb_wvf_confirm',
  // GOG
  'sig_g1','sig_g2','sig_g4','sig_g6','sig_g11',
  // PARA
  'sig_para_prep','sig_para_start','sig_para_plus','sig_para_retest',
  // Combo
  'sig_pp','sig_svs','sig_seq_bcont','sig_3g','three_g',
  'rocket','hilo_buy','sig_buy',
  // EMA / PREUP
  'sig_p66','sig_p55','sig_p89',
  // Delta
  'sig_flp_up','sig_org_up','sig_dd_up_red','sig_d_up_red',
  // 3UP / Breakout
  'sig_3up','sig_fbo_up','sig_eb_up','bo_up','bx_up',
  // Meta
  'price_gt_200','price_gt_89','rsi_le_35',
  'already_extended_flag',
]

function ScoringLabTab() {
  const [scores, setScores] = useState([])
  const [loading, setLoading] = useState(false)

  // Define form
  const [name, setName] = useState('My Score v1')
  const [threshold, setThreshold] = useState('45')
  const [weights, setWeights] = useState([
    { signal: 'turbo_score', weight: 1.0 },
    { signal: 'sig_va', weight: 3.0 },
    { signal: 'ad_cluster', weight: 8.0 },
    { signal: 'wyc_spring', weight: 10.0 },
    { signal: 'prebreak_prime', weight: 12.0 },
    { signal: 'already_extended_flag', weight: -15.0 },
  ])
  const [saving, setSaving] = useState(false)
  const [saveResult, setSaveResult] = useState(null)

  // Backtest form
  const [btScoreId, setBtScoreId] = useState('')
  const [btEventType, setBtEventType] = useState('BULL_2X_60D')
  const [btDateFrom, setBtDateFrom] = useState('')
  const [btDateTo, setBtDateTo] = useState('')
  const [btRunning, setBtRunning] = useState(false)
  const [btResult, setBtResult] = useState(null)

  const loadScores = useCallback(async () => {
    setLoading(true)
    try { setScores(await api.studioScoreList()) } catch {}
    finally { setLoading(false) }
  }, [])

  useEffect(() => { loadScores() }, [loadScores])

  const addWeight = () => setWeights(w => [...w, { signal: 'sig_va', weight: 1.0 }])
  const removeWeight = (i) => setWeights(w => w.filter((_, j) => j !== i))
  const updateWeight = (i, field, val) => setWeights(w => w.map((x, j) => j === i ? { ...x, [field]: field === 'weight' ? +val : val } : x))

  const saveScore = async () => {
    setSaving(true)
    setSaveResult(null)
    try {
      const w = {}
      weights.forEach(({ signal, weight }) => { if (signal) w[signal] = weight })
      const r = await api.studioScoreDefine({
        name,
        weights: w,
        threshold: +threshold,
      })
      setSaveResult(r)
      loadScores()
    } catch (e) {
      setSaveResult({ error: e.message })
    } finally {
      setSaving(false)
    }
  }

  const runBacktest = async () => {
    if (!btScoreId) return alert('Select a saved score to backtest')
    setBtRunning(true)
    setBtResult(null)
    try {
      const r = await api.studioScoreBacktest({
        score_id: btScoreId,
        event_type: btEventType,
        ...(btDateFrom && { date_from: btDateFrom }),
        ...(btDateTo   && { date_to:   btDateTo   }),
      })
      setBtResult(r)
    } catch (e) {
      setBtResult({ error: e.message })
    } finally {
      setBtRunning(false)
    }
  }

  const metricColor = (val, threshold = 0.5) =>
    val == null ? '' : val >= threshold ? 'text-emerald-400' : val >= threshold * 0.7 ? 'text-yellow-300' : 'text-red-400'

  return (
    <div className="flex flex-col gap-4">
      {/* Saved Scores */}
      <Card>
        <div className="flex items-center gap-2 mb-3">
          <h3 className="text-sm font-semibold flex-1">Saved Scores</h3>
          <Btn onClick={loadScores} disabled={loading} variant="secondary">{loading ? <Spinner /> : '↻'}</Btn>
        </div>
        {scores.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {scores.map(s => (
              <button key={s.score_id}
                onClick={() => setBtScoreId(s.score_id)}
                className={cls(
                  'text-left px-3 py-2 rounded-lg border text-xs transition-colors',
                  btScoreId === s.score_id
                    ? 'bg-md-primary/20 border-md-primary/40 text-md-primary'
                    : 'bg-md-surface border-md-outline-var text-md-on-surface hover:bg-white/5'
                )}>
                <div className="font-semibold">{s.name}</div>
                <div className="text-md-on-surface-var text-[10px]">ID: {s.score_id?.slice(0,8)}… · threshold {s.threshold}</div>
              </button>
            ))}
          </div>
        ) : (
          <p className="text-xs text-md-on-surface-var">No scores saved yet. Define one below.</p>
        )}
      </Card>

      {/* Define Score */}
      <Card>
        <h3 className="text-sm font-semibold mb-3">Define Custom Score</h3>
        <div className="grid grid-cols-2 gap-3 mb-4">
          <Input label="Score Name" value={name} onChange={setName} placeholder="My Score v1" />
          <Input label="Threshold (0-100)" type="number" value={threshold} onChange={setThreshold} />
        </div>

        <h4 className="text-xs font-medium text-md-on-surface-var mb-2 uppercase tracking-wide">Signal Weights</h4>
        <div className="flex flex-col gap-2 mb-3">
          {weights.map((w, i) => (
            <div key={i} className="flex gap-2 items-center">
              <select
                value={w.signal}
                onChange={e => updateWeight(i, 'signal', e.target.value)}
                className="flex-1 bg-md-surface border border-md-outline-var rounded-lg px-2.5 py-1.5 text-xs text-md-on-surface focus:outline-none focus:border-md-primary font-mono"
              >
                {SCORE_SIGNAL_OPTS.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
              <input
                type="number"
                step="0.5"
                value={w.weight}
                onChange={e => updateWeight(i, 'weight', e.target.value)}
                className={cls(
                  'w-20 bg-md-surface border rounded-lg px-2 py-1.5 text-xs text-center font-mono focus:outline-none',
                  w.weight < 0
                    ? 'border-red-500/40 text-red-300 focus:border-red-400'
                    : 'border-md-outline-var text-emerald-300 focus:border-md-primary'
                )}
              />
              <button onClick={() => removeWeight(i)} className="text-md-on-surface-var/40 hover:text-red-400 text-xs px-1">✕</button>
            </div>
          ))}
        </div>
        <div className="flex gap-2">
          <Btn onClick={addWeight} variant="secondary" size="sm">+ Add Signal</Btn>
          <div className="flex-1" />
          <Btn onClick={saveScore} disabled={saving} size="md">
            {saving ? <><Spinner /> Saving...</> : '💾 Save Score'}
          </Btn>
        </div>
        {saveResult && !saveResult.error && (
          <p className="mt-2 text-xs text-emerald-400">✅ Saved! ID: {saveResult.score_id?.slice(0,12)}…</p>
        )}
        {saveResult?.error && <p className="mt-2 text-xs text-red-400">{saveResult.error}</p>}
      </Card>

      {/* Backtest */}
      <Card>
        <h3 className="text-sm font-semibold mb-3">Backtest Score</h3>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
          <Select label="Saved Score" value={btScoreId} onChange={setBtScoreId}
            options={[{ value: '', label: '— select —' }, ...scores.map(s => ({ value: s.score_id, label: s.name }))]}
            className="col-span-2"
          />
          <Select label="Event Type" value={btEventType} onChange={setBtEventType}
            options={Object.entries(PRESET_LABELS).map(([k, v]) => ({ value: k, label: v }))}
          />
          <StudioDatePicker label="Date From" value={btDateFrom} onChange={setBtDateFrom} placeholder="From date…" />
        </div>
        <Btn onClick={runBacktest} disabled={btRunning || !btScoreId} size="md">
          {btRunning ? <><Spinner /> Running...</> : '▶ Run Backtest'}
        </Btn>
        {btResult?.error && <p className="mt-2 text-xs text-red-400">{btResult.error}</p>}

        {btResult && !btResult.error && (
          <div className="mt-4">
            <h4 className="text-xs font-semibold mb-3 text-md-on-surface">Results</h4>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 mb-4">
              {/* Custom score metrics */}
              <div className="bg-md-primary/5 border border-md-primary/20 rounded-lg p-3">
                <div className="text-[10px] text-md-on-surface-var uppercase tracking-wide mb-1">Custom Score</div>
                {[
                  { label: 'Precision', value: btResult.custom?.precision_, format: v => `${(v*100).toFixed(1)}%` },
                  { label: 'Recall',    value: btResult.custom?.recall_,    format: v => `${(v*100).toFixed(1)}%` },
                  { label: 'F1',        value: btResult.custom?.f1_,        format: v => `${(v*100).toFixed(1)}%` },
                  { label: 'Fwd 20d',   value: btResult.custom?.avg_fwd_20d, format: v => fmtPct(v * 100) },
                  { label: 'Fwd 60d',   value: btResult.custom?.avg_fwd_60d, format: v => fmtPct(v * 100) },
                  { label: 'Caught',    value: btResult.custom?.caught_count, format: fmtNum },
                ].map(m => (
                  <div key={m.label} className="flex justify-between text-xs py-0.5">
                    <span className="text-md-on-surface-var">{m.label}</span>
                    <span className={cls('font-mono font-semibold', metricColor(m.value))}>
                      {m.value != null ? m.format(m.value) : '–'}
                    </span>
                  </div>
                ))}
              </div>

              {/* Turbo comparison */}
              {btResult.turbo_baseline && (
                <div className="bg-white/3 border border-md-outline-var rounded-lg p-3">
                  <div className="text-[10px] text-md-on-surface-var uppercase tracking-wide mb-1">Turbo Baseline</div>
                  {[
                    { label: 'Precision', value: btResult.turbo_baseline?.precision_, format: v => `${(v*100).toFixed(1)}%` },
                    { label: 'Recall',    value: btResult.turbo_baseline?.recall_,    format: v => `${(v*100).toFixed(1)}%` },
                    { label: 'F1',        value: btResult.turbo_baseline?.f1_,        format: v => `${(v*100).toFixed(1)}%` },
                    { label: 'Fwd 20d',   value: btResult.turbo_baseline?.avg_fwd_20d, format: v => fmtPct(v * 100) },
                    { label: 'Fwd 60d',   value: btResult.turbo_baseline?.avg_fwd_60d, format: v => fmtPct(v * 100) },
                    { label: 'Caught',    value: btResult.turbo_baseline?.caught_count, format: fmtNum },
                  ].map(m => (
                    <div key={m.label} className="flex justify-between text-xs py-0.5">
                      <span className="text-md-on-surface-var">{m.label}</span>
                      <span className="font-mono font-semibold text-md-on-surface">
                        {m.value != null ? m.format(m.value) : '–'}
                      </span>
                    </div>
                  ))}
                </div>
              )}

              {/* Delta */}
              {btResult.custom && btResult.turbo_baseline && (
                <div className="bg-white/3 border border-md-outline-var rounded-lg p-3">
                  <div className="text-[10px] text-md-on-surface-var uppercase tracking-wide mb-1">Δ vs Turbo</div>
                  {[
                    { label: 'Precision', c: btResult.custom?.precision_, t: btResult.turbo_baseline?.precision_, format: v => `${(v*100).toFixed(1)}pp` },
                    { label: 'Recall',    c: btResult.custom?.recall_,    t: btResult.turbo_baseline?.recall_,    format: v => `${(v*100).toFixed(1)}pp` },
                    { label: 'F1',        c: btResult.custom?.f1_,        t: btResult.turbo_baseline?.f1_,        format: v => `${(v*100).toFixed(1)}pp` },
                    { label: 'Fwd 20d',   c: btResult.custom?.avg_fwd_20d, t: btResult.turbo_baseline?.avg_fwd_20d, format: v => fmtPct(v * 100) },
                    { label: 'Fwd 60d',   c: btResult.custom?.avg_fwd_60d, t: btResult.turbo_baseline?.avg_fwd_60d, format: v => fmtPct(v * 100) },
                  ].map(m => {
                    const delta = m.c != null && m.t != null ? m.c - m.t : null
                    return (
                      <div key={m.label} className="flex justify-between text-xs py-0.5">
                        <span className="text-md-on-surface-var">{m.label}</span>
                        <span className={cls('font-mono font-semibold', delta > 0 ? 'text-emerald-400' : delta < 0 ? 'text-red-400' : 'text-md-on-surface')}>
                          {delta != null ? (delta >= 0 ? '+' : '') + m.format(delta) : '–'}
                        </span>
                      </div>
                    )
                  })}
                </div>
              )}
            </div>

            {btResult.summary && (
              <p className="text-xs text-md-on-surface-var bg-md-surface rounded-lg p-3">
                {btResult.summary}
              </p>
            )}
          </div>
        )}
      </Card>
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── SIGNAL STATS TAB ──────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// Signal groups for the picker
const SIG_PICKER_GROUPS = [
  { label: 'T signals', sigs: [
    'sig_t1g','sig_t2g','sig_t1','sig_t2','sig_t3','sig_t4',
    'sig_t5','sig_t6','sig_t9','sig_t10','sig_t11','sig_t12',
  ]},
  { label: 'Z signals', sigs: [
    'sig_z1g','sig_z2g','sig_z1','sig_z2','sig_z3','sig_z4',
    'sig_z5','sig_z6','sig_z9','sig_z10','sig_z11','sig_z12',
  ]},
  { label: 'L / WLNBB', sigs: [
    'sig_fri34','sig_fri43','sig_fri64','sig_l555','sig_l2l4',
    'sig_blue','sig_rl','sig_rh','sig_pp','sig_cci0r','sig_ccib',
    'l34','l43','l22','bo_up','be_up','bx_up','vbo_up',
  ]},
  { label: 'GOG', sigs: [
    'sig_g1','sig_g2','sig_g4','sig_g6','sig_g11','sig_gog_plus',
    'g1p','g2p','g3p','g1l','g2l','g1c','g2c',
  ]},
  { label: 'VABS / Volume', sigs: [
    'sig_abs','sig_clm','sig_sc','sig_bc','sig_fbo_up','sig_eb_up',
    'sig_3up','sig_best_up','sig_vol_5x','sig_vol_10x','sig_vol_20x',
  ]},
  { label: 'FLY / Wick', sigs: [
    'sig_fly_abcd','sig_fly_cd','sig_fly_bd','sig_fly_ad',
    'sig_wk_up','sig_wk_dn','sig_x1','sig_x2','sig_x1g','sig_x3',
  ]},
  { label: 'TZ / Combo', sigs: [
    'sig_tz_flip','sig_bias_up','sig_buy','sig_3g','sig_conso',
    'sig_svs','sig_cd','sig_ca','sig_cw','sig_va','rocket','sq',
  ]},
  { label: 'Prebreak / WYC', sigs: [
    'prebreak_prime','prebreak_ready','prebreak_watch',
    'pb_lvbo','pb_wvf_confirm','wyc_spring','wyc_sos','wyc_in_tr',
    'ad_fresh','ad_cluster',
  ]},
  { label: 'PARA / CISD', sigs: [
    'sig_para_prep','sig_para_start','sig_para_plus','sig_para_retest',
    'sig_cisd_cplus',
  ]},
  { label: 'EMA / RSI', sigs: [
    'sig_p55','sig_p66','sig_p89',
    'price_gt_89','price_gt_200','rsi_le_35','rsi_ge_70',
  ]},
]

const SORT_OPTIONS = [
  { value: 'win_5d',        label: 'Win% 5D'    },
  { value: 'avg_5d',        label: 'Avg% 5D'    },
  { value: 'hit_5d',        label: 'Hit5% 5D'   },
  { value: 'win_10d',       label: 'Win% 10D'   },
  { value: 'avg_10d',       label: 'Avg% 10D'   },
  { value: 'hit_10d',       label: 'Hit10% 10D' },
  { value: 'exp_5d',        label: 'Exp 5D'     },
  { value: 'win_1d',        label: 'Win% 1D'    },
]

function StatCell({ val, baseline, bold = false, isWin = false, isHit = false }) {
  if (val == null) return <td className="px-2 py-1.5 text-center text-white/30 text-xs">–</td>
  const v = +val
  // Color: green if above baseline (or if > 50 for win rates)
  let color = 'text-white/60'
  if (isWin || isHit) {
    color = v >= 65 ? 'text-emerald-400' : v >= 55 ? 'text-emerald-300/80' : v >= 50 ? 'text-white/80' : 'text-red-400/80'
  } else {
    // avg return
    color = v >= 3 ? 'text-emerald-400' : v >= 1 ? 'text-emerald-300/80' : v >= 0 ? 'text-white/70' : 'text-red-400/80'
  }
  const bl = baseline != null ? +baseline : null
  const diff = bl != null ? v - bl : null
  return (
    <td className={`px-2 py-1.5 text-center text-xs ${bold ? 'font-bold' : ''}`}>
      <span className={color}>{v.toFixed(1)}{isWin || isHit ? '%' : '%'}</span>
      {diff != null && (
        <span className={cls('ml-1 text-[9px]', diff > 0 ? 'text-emerald-400/70' : 'text-red-400/70')}>
          {diff > 0 ? '+' : ''}{diff.toFixed(1)}
        </span>
      )}
    </td>
  )
}

function ComboStatsCard({ stats, baseline, label }) {
  if (!stats || stats.insufficient) return (
    <div className="text-center py-4 text-white/40 text-xs">
      {stats?.n != null ? `N=${stats.n} — too few bars (need ≥5)` : 'No data'}
    </div>
  )
  const tfs = [
    { key: '1d',  wk: 'win_1d',  avg: 'avg_1d',  hit: null,     exp: 'exp_1d'  },
    { key: '3d',  wk: 'win_3d',  avg: 'avg_3d',  hit: null,     exp: 'exp_3d'  },
    { key: '5d',  wk: 'win_5d',  avg: 'avg_5d',  hit: 'hit_5d', exp: 'exp_5d'  },
    { key: '10d', wk: 'win_10d', avg: 'avg_10d', hit: 'hit_10d',exp: 'exp_10d' },
    { key: '20d', wk: 'win_20d', avg: 'avg_20d', hit: 'hit_20d',exp: 'exp_20d' },
  ]
  return (
    <div className="rounded-xl bg-md-surface border border-md-outline-var overflow-hidden">
      <div className="px-4 py-2 border-b border-md-outline-var flex items-center justify-between">
        <span className="text-sm font-semibold text-md-on-surface">{label}</span>
        <span className="text-xs text-white/50">N = {(stats.n ?? 0).toLocaleString()} bars</span>
      </div>
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-md-outline-var/50">
            <th className="px-2 py-1.5 text-left text-[10px] text-white/40 font-medium">TF</th>
            <th className="px-2 py-1.5 text-center text-[10px] text-white/40 font-medium">Win%</th>
            <th className="px-2 py-1.5 text-center text-[10px] text-white/40 font-medium">Avg Ret</th>
            <th className="px-2 py-1.5 text-center text-[10px] text-white/40 font-medium">Expectancy</th>
            <th className="px-2 py-1.5 text-center text-[10px] text-white/40 font-medium">Hit</th>
            <th className="px-2 py-1.5 text-center text-[10px] text-white/40 font-medium">N</th>
          </tr>
        </thead>
        <tbody>
          {tfs.map(({ key, wk, avg, hit, exp }) => {
            const winV = stats[wk]
            if (winV == null) return null
            return (
              <tr key={key} className="border-b border-md-outline-var/20 hover:bg-white/3">
                <td className="px-2 py-1.5 font-bold text-white/70">{key.toUpperCase()}</td>
                <StatCell val={stats[wk]}  baseline={baseline?.[wk]}  isWin bold />
                <StatCell val={stats[avg]} baseline={baseline?.[avg]} />
                <StatCell val={stats[exp]} baseline={baseline?.[exp]} />
                <StatCell val={hit ? stats[hit] : null} isHit />
                <td className="px-2 py-1.5 text-center text-white/40">{stats[`n_${key}`] ?? stats.n}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function SignalStatsTab() {
  const [filters, setFilters]         = useState(null)
  const [selectedSigs, setSelected]   = useState([])
  const [universe, setUniverse]       = useState('')
  const [regime, setRegime]           = useState('')
  const [dateFrom, setDateFrom]       = useState('')
  const [dateTo, setDateTo]           = useState('')
  const [turboMin, setTurboMin]       = useState('')
  const [sortBy, setSortBy]           = useState('win_5d')
  const [minN, setMinN]               = useState(30)
  const [pickerOpen, setPickerOpen]   = useState(false)
  const [pickerGroup, setPickerGroup] = useState(0)

  const [comboResult, setComboResult] = useState(null)
  const [rankResult, setRankResult]   = useState(null)
  const [loading, setLoading]         = useState(false)
  const [rankLoading, setRankLoading] = useState(false)
  const [error, setError]             = useState(null)

  // Load filters on mount
  useEffect(() => {
    api.studioSigFilters().then(setFilters).catch(() => {})
  }, [])

  const baseFilters = () => ({
    universe:  universe  || null,
    regime:    regime    || null,
    date_from: dateFrom  || null,
    date_to:   dateTo    || null,
    turbo_min: turboMin  ? +turboMin : null,
  })

  const runCombo = async () => {
    setLoading(true); setError(null); setComboResult(null)
    try {
      const res = await api.studioSigQuery({ ...baseFilters(), signals: selectedSigs, min_n: 5 })
      setComboResult(res)
    } catch(e) { setError(e.message) }
    finally { setLoading(false) }
  }

  const runRanking = async () => {
    setRankLoading(true); setError(null); setRankResult(null)
    try {
      const res = await api.studioSigRank({ ...baseFilters(), sort_by: sortBy, min_n: minN, top_n: 60 })
      setRankResult(res)
    } catch(e) { setError(e.message) }
    finally { setRankLoading(false) }
  }

  const toggleSig = (s) => setSelected(prev =>
    prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s]
  )

  const universOpts = [{ value: '', label: 'All universes' },
    ...(filters?.universes || []).map(u => ({ value: u, label: u }))]
  const regimeOpts  = [{ value: '', label: 'All regimes' },
    ...(filters?.regimes   || []).map(r => ({ value: r, label: r }))]

  return (
    <div className="flex flex-col gap-4">

      {/* ── Filters bar ── */}
      <Card>
        <div className="flex flex-wrap gap-3 items-end">
          <Select label="Universe" value={universe} onChange={setUniverse}
            options={universOpts} className="w-32" />
          <Select label="Regime"   value={regime}   onChange={setRegime}
            options={regimeOpts}  className="w-40" />
          <Input  label="Date from" value={dateFrom} onChange={setDateFrom}
            placeholder={filters?.date_min || 'YYYY-MM-DD'} className="w-32" />
          <Input  label="Date to"   value={dateTo}   onChange={setDateTo}
            placeholder={filters?.date_max || 'YYYY-MM-DD'} className="w-32" />
          <Input  label="Turbo≥"    value={turboMin} onChange={setTurboMin}
            type="number" placeholder="0" className="w-20" />
        </div>
      </Card>

      {/* ── Two columns: Combo Builder | Ranking ── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">

        {/* LEFT — Combo Builder */}
        <div className="flex flex-col gap-3">
          <Card>
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm font-semibold text-md-on-surface">Combo Builder</span>
              {selectedSigs.length > 0 && (
                <button onClick={() => setSelected([])}
                  className="text-[10px] text-red-400/70 hover:text-red-400">
                  Clear all
                </button>
              )}
            </div>

            {/* Selected chips */}
            <div className="flex flex-wrap gap-1 min-h-[28px] mb-2">
              {selectedSigs.length === 0
                ? <span className="text-xs text-white/30 italic">No signals selected — pick below</span>
                : selectedSigs.map(s => (
                  <button key={s} onClick={() => toggleSig(s)}
                    className="px-2 py-0.5 rounded-full text-[11px] font-medium bg-md-primary/20 text-md-primary border border-md-primary/40 hover:bg-red-500/20 hover:text-red-300 hover:border-red-400/40 transition-colors">
                    {colToLabel(s)} ✕
                  </button>
                ))
              }
            </div>

            {/* Signal picker toggle */}
            <button onClick={() => setPickerOpen(v => !v)}
              className="text-xs text-md-primary hover:underline">
              {pickerOpen ? '▲ Hide picker' : '▼ Pick signals'}
            </button>

            {pickerOpen && (
              <div className="mt-2 border border-md-outline-var rounded-lg overflow-hidden">
                {/* Group tabs */}
                <div className="flex flex-wrap gap-0 border-b border-md-outline-var bg-md-surface">
                  {SIG_PICKER_GROUPS.map((g, i) => (
                    <button key={i} onClick={() => setPickerGroup(i)}
                      className={cls(
                        'px-2 py-1 text-[10px] font-medium transition-colors',
                        pickerGroup === i
                          ? 'bg-md-surface-con text-md-primary'
                          : 'text-white/50 hover:text-white/80'
                      )}>
                      {g.label}
                    </button>
                  ))}
                </div>
                {/* Signal chips */}
                <div className="p-2 flex flex-wrap gap-1 bg-md-surface/50">
                  {SIG_PICKER_GROUPS[pickerGroup].sigs.map(s => (
                    <button key={s} onClick={() => toggleSig(s)}
                      className={cls(
                        'px-2 py-0.5 rounded-full text-[11px] font-medium border transition-colors',
                        selectedSigs.includes(s)
                          ? 'bg-md-primary/25 text-md-primary border-md-primary/50'
                          : 'bg-white/5 text-white/60 border-white/15 hover:bg-white/10 hover:text-white/90'
                      )}>
                      {colToLabel(s)}
                    </button>
                  ))}
                </div>
              </div>
            )}

            <div className="mt-3 flex gap-2">
              <Btn onClick={runCombo} disabled={loading || selectedSigs.length === 0}>
                {loading ? <><Spinner /> Computing…</> : `▶ Analyze combo (${selectedSigs.length} signals)`}
              </Btn>
              {selectedSigs.length === 0 && (
                <Btn onClick={() => { setSelected([]); runCombo() }} variant="secondary"
                  disabled={loading}>
                  Baseline only
                </Btn>
              )}
            </div>
          </Card>

          {error && <div className="text-xs text-red-400 bg-red-500/10 rounded-lg px-3 py-2">{error}</div>}

          {comboResult && (
            <div className="flex flex-col gap-3">
              <ComboStatsCard
                stats={comboResult.combo}
                baseline={comboResult.baseline}
                label={selectedSigs.length > 0
                  ? selectedSigs.map(s => colToLabel(s)).join(' + ')
                  : 'All bars (baseline)'}
              />
              <ComboStatsCard
                stats={comboResult.baseline}
                baseline={null}
                label="Baseline (random bar)"
              />
              {Object.keys(comboResult.regime_breakdown || {}).length > 0 && (
                <div>
                  <div className="text-xs font-semibold text-white/50 uppercase tracking-wide mb-1">By Regime</div>
                  <div className="flex flex-col gap-2">
                    {Object.entries(comboResult.regime_breakdown).map(([reg, s]) => (
                      <ComboStatsCard key={reg} stats={s} baseline={comboResult.baseline} label={reg} />
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* RIGHT — Signal Ranking */}
        <div className="flex flex-col gap-3">
          <Card>
            <div className="flex items-center gap-3 flex-wrap">
              <span className="text-sm font-semibold text-md-on-surface">Signal Ranking</span>
              <Select label="" value={sortBy} onChange={setSortBy}
                options={SORT_OPTIONS} className="w-32" />
              <div className="flex flex-col gap-0.5">
                <span className="text-[10px] text-white/40 uppercase tracking-wide">Min N</span>
                <input type="number" value={minN} onChange={e => setMinN(+e.target.value)}
                  className="w-16 bg-md-surface border border-md-outline-var rounded px-2 py-1 text-xs text-md-on-surface" />
              </div>
              <Btn onClick={runRanking} disabled={rankLoading} className="mt-auto">
                {rankLoading ? <><Spinner /> Ranking…</> : '▶ Run ranking'}
              </Btn>
            </div>
          </Card>

          {rankResult && (
            <div className="rounded-xl border border-md-outline-var overflow-hidden">
              <div className="px-4 py-2 border-b border-md-outline-var flex items-center justify-between bg-md-surface-con/50">
                <span className="text-xs font-semibold text-white/70">
                  Top {rankResult.rows?.length} signals  ·  sorted by {rankResult.sort_by}
                </span>
                <span className="text-[10px] text-white/40">{rankResult.total} total qualifying</span>
              </div>
              <div className="overflow-y-auto max-h-[600px]">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-md-surface z-10">
                    <tr className="border-b border-md-outline-var/50">
                      <th className="px-2 py-1.5 text-left text-[10px] text-white/40">#</th>
                      <th className="px-2 py-1.5 text-left text-[10px] text-white/40">Signal</th>
                      <th className="px-2 py-1.5 text-center text-[10px] text-white/40">N</th>
                      <th className="px-2 py-1.5 text-center text-[10px] text-white/40">Win 5D</th>
                      <th className="px-2 py-1.5 text-center text-[10px] text-white/40">Avg 5D</th>
                      <th className="px-2 py-1.5 text-center text-[10px] text-white/40">Hit5% 5D</th>
                      <th className="px-2 py-1.5 text-center text-[10px] text-white/40">Win 10D</th>
                      <th className="px-2 py-1.5 text-center text-[10px] text-white/40">Avg 10D</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(rankResult.rows || []).map((r, i) => {
                      const isSorted = r[sortBy] != null
                      return (
                        <tr key={r.signal}
                          className="border-b border-md-outline-var/20 hover:bg-white/3 cursor-pointer"
                          onClick={() => { setSelected([r.signal]); setPickerOpen(false) }}>
                          <td className="px-2 py-1.5 text-white/30">{i+1}</td>
                          <td className="px-2 py-1.5">
                            <button className="font-medium text-md-primary hover:underline text-left">
                              {colToLabel(r.signal)}
                            </button>
                            <span className="ml-1 text-[9px] text-white/30">{r.signal}</span>
                          </td>
                          <td className="px-2 py-1.5 text-center text-white/50">{(r.n||0).toLocaleString()}</td>
                          <StatCell val={r.win_5d}  isWin bold={sortBy==='win_5d'}  />
                          <StatCell val={r.avg_5d}       bold={sortBy==='avg_5d'}  />
                          <StatCell val={r.hit_5d}  isHit bold={sortBy==='hit_5d'} />
                          <StatCell val={r.win_10d} isWin bold={sortBy==='win_10d'} />
                          <StatCell val={r.avg_10d}      bold={sortBy==='avg_10d'} />
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── SEQ LAB TAB ───────────────────────────────────────────────────────────────
// Rank N-bar T/Z sequences by forward outcome vs baseline (exploratory).
// ═══════════════════════════════════════════════════════════════════════════════
const SEQLAB_SEL = 'bg-md-surface border border-md-outline-var rounded-lg px-2 py-1.5 text-xs text-md-on-surface focus:outline-none focus:border-md-primary'
const Field = ({ label, children }) => (
  <label className="flex flex-col gap-0.5">
    <span className="text-[10px] text-md-on-surface-var/70 uppercase tracking-wide">{label}</span>
    {children}
  </label>
)
const SEQLAB_HORIZONS = [
  { v: 'fwd_1d',  l: '+1d' },
  { v: 'fwd_3d',  l: '+3d' },
  { v: 'fwd_5d',  l: '+5d' },
  { v: 'fwd_10d', l: '+10d' },
  { v: 'fwd_swing_ret_3', l: 'swing→pivot (3-3)' },
  { v: 'fwd_swing_ret_5', l: 'swing→pivot (5-5)' },
]
const SEQLAB_PHASES = ['', 'MARKUP', 'MKDN', 'ACC_TR', 'DIST_TR', 'SPRING', 'UTAD', 'SOS', 'SOW']

// backtest-expert verdict chip (Deploy / Refine / Abandon) shown per sequence row
const VERDICT_STYLE = {
  Deploy:  'bg-green-500/20 text-green-300 border-green-500/40',
  Refine:  'bg-amber-500/20 text-amber-300 border-amber-500/40',
  Abandon: 'bg-red-500/20 text-red-300 border-red-500/40',
}
function VerdictChip({ v }) {
  if (!v || !v.verdict || v.verdict === 'n/a')
    return <span className="text-md-on-surface-var">—</span>
  const cls = VERDICT_STYLE[v.verdict] || 'bg-white/10 text-md-on-surface-var border-md-outline-var'
  const tip = `score ${v.score}/100`
    + (v.forced ? ' · FORCED by a gate' : '')
    + (v.net_edge != null ? ` · net ${v.net_edge.toFixed(2)}%/trade` : '')
    + ` — ${v.reason}`
  return (
    <span title={tip}
      className={`inline-block px-1.5 py-0.5 rounded border text-[10px] font-semibold ${cls}`}>
      {v.verdict}{v.forced ? ' ⚠' : ''}
    </span>
  )
}

function SeqLabTab() {
  const [p, setP] = useState({
    universe: 'sp500', n_bars: 4, mode: 'color', horizon: 'fwd_1d',
    min_occ: 500, wyc_phase: '', prefix: '', sort: 'win', limit: 25, by_phase: false,
    evaluate: true, cost: 0.5, confirm_lag: 3,
  })
  const [data, setData]   = useState(null)
  const [loading, setLd]  = useState(false)
  const [error, setError] = useState(null)
  const upd = (k, v) => setP(prev => ({ ...prev, [k]: v }))

  const run = async () => {
    setLd(true); setError(null)
    try {
      const params = { ...p }
      if (params.universe === 'all') delete params.universe
      const r = await api.studioSeqLab(params)
      if (r.error) setError(r.error); else setData(r)
    } catch (e) { setError(e.message) } finally { setLd(false) }
  }

  const base = data?.baseline
  const swing = p.horizon.startsWith('fwd_swing')
  const fmtPct = (v) => v == null ? '—' : v.toFixed(2)

  // edge colour vs baseline win
  const winCls = (w) => {
    if (!base || w == null) return ''
    const d = w - base.win
    return d >= 3 ? 'text-green-400 font-semibold' : d >= 1 ? 'text-green-300'
         : d <= -3 ? 'text-red-400' : 'text-md-on-surface'
  }

  return (
    <div className="space-y-3">
      <Card>
        <h3 className="text-sm font-semibold text-md-on-surface mb-1">🧬 TZ Sequence Lab</h3>
        <p className="text-xs text-md-on-surface-var">
          Ranks N-bar T/Z sequences by forward outcome, with a <b>baseline</b> row to judge real
          edge. <span className="font-mono">color</span> = bar direction (T=up/Z=down) e.g. ZZZT;
          {' '}<span className="font-mono">signal</span> = exact T/Z label per bar (sparse).
          Watch <b>avg%</b> + <b>mfe20</b> next to <b>win%</b>: a high win% with tiny avg/mfe is a
          weak edge, not a signal. <b>swing</b> mode sequences the HL/LL/HH/LH pivots — use the
          <b> Confirm</b> dropdown (+3 bars) for a leak-free entry, since a 3-3 pivot is only known
          3 bars later (raw = look-ahead, inflated win%). The <b>×phase</b> toggle splits each
          sequence by Wyckoff phase — usually the most revealing view.
        </p>
      </Card>

      {/* controls */}
      <div className="flex flex-wrap items-end gap-2 text-xs">
        <Field label="Universe">
          <select value={p.universe} onChange={e => upd('universe', e.target.value)} className={SEQLAB_SEL}>
            {['sp500', 'nasdaq', 'russell2k', 'all'].map(u => <option key={u} value={u}>{u}</option>)}
          </select>
        </Field>
        <Field label="Bars">
          <select value={p.n_bars} onChange={e => upd('n_bars', Number(e.target.value))} className={SEQLAB_SEL}>
            {[2, 3, 4, 5, 6].map(n => <option key={n} value={n}>{n}-bar</option>)}
          </select>
        </Field>
        <Field label="Mode">
          <select value={p.mode} onChange={e => upd('mode', e.target.value)} className={SEQLAB_SEL}>
            <option value="color">color (T/Z)</option>
            <option value="signal">signal label</option>
            <option value="lsig">volume L (L1–L6)</option>
            <option value="vol">vol bucket (W/L/N/B/VB)</option>
            <option value="combo">combined (T/Z + L + vol)</option>
            <option value="swing">swing pivot (HL/LL/HH/LH)</option>
            <option value="wyckoff">wyckoff stage (SC/AR/ST/SPR/SOS/JAC/LPS)</option>
          </select>
        </Field>
        <Field label="Horizon">
          <select value={p.horizon} onChange={e => upd('horizon', e.target.value)} className={SEQLAB_SEL}>
            {SEQLAB_HORIZONS.map(h => <option key={h.v} value={h.v}>{h.l}</option>)}
          </select>
        </Field>
        {p.mode === 'swing' && (
          <Field label="Confirm">
            <select value={p.confirm_lag} onChange={e => upd('confirm_lag', Number(e.target.value))} className={SEQLAB_SEL}
              title="Enter N bars AFTER the pivot prints (leak-free — a 3-3 pivot is only known 3 bars later). >0 forces a fixed-day horizon.">
              <option value={0}>raw (look-ahead ⚠)</option>
              <option value={3}>+3 bars (confirmed)</option>
              <option value={5}>+5 bars</option>
            </select>
          </Field>
        )}
        <Field label="Min occ.">
          <input type="number" value={p.min_occ} onChange={e => upd('min_occ', Number(e.target.value))}
            className={SEQLAB_SEL + ' w-20'} />
        </Field>
        <Field label="Wyckoff">
          <select value={p.wyc_phase} onChange={e => upd('wyc_phase', e.target.value)} className={SEQLAB_SEL}>
            {SEQLAB_PHASES.map(ph => <option key={ph} value={ph}>{ph || 'all'}</option>)}
          </select>
        </Field>
        <Field label="Prefix">
          <input value={p.prefix} onChange={e => upd('prefix', e.target.value.toUpperCase())}
            placeholder="e.g. ZZ" className={SEQLAB_SEL + ' w-20 font-mono'} />
        </Field>
        <Field label="Sort">
          <select value={p.sort} onChange={e => upd('sort', e.target.value)} className={SEQLAB_SEL}>
            <option value="win">win% ↓</option>
            <option value="avg">avg% ↓</option>
            <option value="mfe">mfe20 ↓</option>
            <option value="n">N ↓</option>
            <option value="avg_lo">avg% ↑ (bearish)</option>
          </select>
        </Field>
        <label className="flex items-center gap-1 cursor-pointer">
          <input type="checkbox" checked={p.by_phase} onChange={e => upd('by_phase', e.target.checked)} />
          <span>×phase</span>
        </label>
        <label className="flex items-center gap-1 cursor-pointer" title="Score each sequence with the backtest-expert gates (significance / Bonferroni / net-edge)">
          <input type="checkbox" checked={p.evaluate} onChange={e => upd('evaluate', e.target.checked)} />
          <span>verdict</span>
        </label>
        <Field label="Cost %">
          <input type="number" step="0.1" value={p.cost} disabled={!p.evaluate}
            onChange={e => upd('cost', Number(e.target.value))}
            title="Round-trip cost per trade — a sequence whose edge is below this is flagged Abandon"
            className={SEQLAB_SEL + ' w-16 disabled:opacity-40'} />
        </Field>
        <button onClick={run} disabled={loading}
          className="px-3 py-1.5 rounded-lg bg-md-primary text-md-on-primary font-medium disabled:opacity-50">
          {loading ? 'running…' : '▶ Run'}
        </button>
      </div>

      {error && <div className="text-xs text-red-400">{error}</div>}

      {/* baseline */}
      {base && (
        <div className="text-xs bg-md-surface-high rounded-lg px-3 py-2 border border-md-outline-var">
          <span className="text-md-on-surface-var">baseline ({data.params.universe}
            {data.params.wyc_phase !== 'all' ? ` · ${data.params.wyc_phase}` : ''}): </span>
          <span className="font-mono">N={base.n.toLocaleString()} · win {fmtPct(base.win)}% ·
            avg {fmtPct(base.avg_ret)}% · mfe20 {fmtPct(base.mfe20)}%</span>
          <span className="text-md-on-surface-var/60"> — beat this to have edge</span>
        </div>
      )}

      {/* results */}
      {data?.rows && (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="text-md-on-surface-var border-b border-md-outline-var">
              <tr>
                <th className="text-left py-1.5 px-2">sequence (b-{p.n_bars - 1} → b0)</th>
                {p.by_phase && <th className="text-left px-2">phase</th>}
                <th className="text-right px-2">N</th>
                <th className="text-right px-2">win%</th>
                <th className="text-right px-2">avg{swing ? ' swing' : ''}%</th>
                <th className="text-right px-2">mfe20%</th>
                <th className="text-right px-2">Δwin vs base</th>
                {data.evaluated && <th className="text-center px-2">verdict</th>}
              </tr>
            </thead>
            <tbody className="font-mono">
              {data.rows.map((r, i) => (
                <tr key={i} className="border-b border-md-outline-var/40 hover:bg-white/5">
                  <td className="py-1 px-2 text-md-on-surface">{r.seq}</td>
                  {p.by_phase && <td className="px-2 text-md-on-surface-var">{r.wyc_phase}</td>}
                  <td className="text-right px-2 text-md-on-surface-var">{r.n.toLocaleString()}</td>
                  <td className={`text-right px-2 ${winCls(r.win)}`}>{fmtPct(r.win)}</td>
                  <td className={`text-right px-2 ${r.avg_ret > 0 ? 'text-green-300' : 'text-red-300'}`}>{fmtPct(r.avg_ret)}</td>
                  <td className="text-right px-2 text-md-on-surface-var">{fmtPct(r.mfe20)}</td>
                  <td className="text-right px-2 text-md-on-surface-var">
                    {base && r.win != null ? (r.win - base.win >= 0 ? '+' : '') + (r.win - base.win).toFixed(1) : '—'}
                  </td>
                  {data.evaluated && <td className="text-center px-2"><VerdictChip v={r.verdict} /></td>}
                </tr>
              ))}
              {data.rows.length === 0 && (
                <tr><td colSpan={(p.by_phase ? 7 : 6) + (data.evaluated ? 1 : 0)} className="py-4 text-center text-md-on-surface-var">
                  no sequences ≥ {p.min_occ} occurrences — lower Min occ. or bars
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      <SeqBacktestPanel />
    </div>
  )
}

// ── Realized backtest sub-panel (turns a signal condition into a tradeable rule) ──
function SeqBacktestPanel() {
  const [p, setP] = useState({
    signals: 'rsi_le_35,wyc_in_tr', universe: 'sp500', wyc_phase: 'MARKUP',
    target_pct: 8, stop_pct: 4, max_hold: 15, side: 'long',
  })
  const [res, setRes] = useState(null)
  const [loading, setLd] = useState(false)
  const [err, setErr] = useState(null)
  const upd = (k, v) => setP(prev => ({ ...prev, [k]: v }))
  const run = async () => {
    setLd(true); setErr(null)
    try {
      const params = { ...p }
      if (params.universe === 'all') delete params.universe
      if (!params.wyc_phase) delete params.wyc_phase
      const r = await api.studioSeqBacktest(params)
      if (r.error) { setErr(r.error); setRes(null) } else setRes(r)
    } catch (e) { setErr(e.message) } finally { setLd(false) }
  }
  const M = ({ m, label }) => !m || m.n === 0 ? <span className="text-md-on-surface-var">{label}: n=0</span> : (
    <span className="font-mono">{label}: n={m.n.toLocaleString()} · win {m.win_pct}% · avg {m.avg_ret}% ·
      med {m.median_ret}% · PF {m.profit_factor ?? '—'} · DD {m.max_drawdown}% · hold {m.avg_hold}b</span>
  )
  return (
    <div className="mt-4 pt-3 border-t border-md-outline-var space-y-2">
      <h4 className="text-sm font-semibold text-md-on-surface">🎯 Realized Backtest <span className="text-xs font-normal text-md-on-surface-var">— does the edge survive a real entry/exit/stop?</span></h4>
      <p className="text-[11px] text-md-on-surface-var">
        Entry = next bar OPEN when ALL listed signal flags = 1 (one position/ticker, no pyramiding).
        Exit = first of target +%, stop −%, or time (N bars). The <b>median</b> trade and <b>profit factor</b>
        tell the real story; a high raw win% often collapses here. <b>first vs second half</b> = out-of-sample sniff.
      </p>
      <div className="flex flex-wrap items-end gap-2 text-xs">
        <Field label="Signals (comma)">
          <input value={p.signals} onChange={e => upd('signals', e.target.value)}
            placeholder="rsi_le_35,d_spring" className={SEQLAB_SEL + ' w-56 font-mono'} />
        </Field>
        <Field label="Universe">
          <select value={p.universe} onChange={e => upd('universe', e.target.value)} className={SEQLAB_SEL}>
            {['sp500', 'nasdaq', 'russell2k', 'all'].map(u => <option key={u} value={u}>{u}</option>)}
          </select>
        </Field>
        <Field label="Phase">
          <select value={p.wyc_phase} onChange={e => upd('wyc_phase', e.target.value)} className={SEQLAB_SEL}>
            {SEQLAB_PHASES.map(ph => <option key={ph} value={ph}>{ph || 'all'}</option>)}
          </select>
        </Field>
        <Field label="Side">
          <select value={p.side} onChange={e => upd('side', e.target.value)} className={SEQLAB_SEL}>
            <option value="long">long</option><option value="short">short</option>
          </select>
        </Field>
        <Field label="Target %"><input type="number" value={p.target_pct} onChange={e => upd('target_pct', Number(e.target.value))} className={SEQLAB_SEL + ' w-16'} /></Field>
        <Field label="Stop %"><input type="number" value={p.stop_pct} onChange={e => upd('stop_pct', Number(e.target.value))} className={SEQLAB_SEL + ' w-16'} /></Field>
        <Field label="Max hold"><input type="number" value={p.max_hold} onChange={e => upd('max_hold', Number(e.target.value))} className={SEQLAB_SEL + ' w-16'} /></Field>
        <button onClick={run} disabled={loading} className="px-3 py-1.5 rounded-lg bg-md-tertiary text-md-on-tertiary font-medium disabled:opacity-50">
          {loading ? 'running…' : '▶ Backtest'}
        </button>
      </div>
      {err && <div className="text-xs text-amber-400">{err}</div>}
      {res && (
        <div className="text-xs space-y-1 bg-md-surface-high rounded-lg px-3 py-2 border border-md-outline-var">
          <div className="text-md-on-surface"><b>OVERALL</b> ({res.params.side}, +{res.params.target_pct}/−{res.params.stop_pct}/{res.params.max_hold}b · {res.date_range[0]}→{res.date_range[1]})</div>
          <div className={res.overall.expectancy > 0 ? 'text-green-300' : 'text-red-300'}><M m={res.overall} label="all" /></div>
          <div className="text-md-on-surface-var"><M m={res.first_half} label="1st½" /></div>
          <div className="text-md-on-surface-var"><M m={res.second_half} label="2nd½" /></div>
          <div className="text-md-on-surface-var/70">exits: {Object.entries(res.exit_reasons).map(([k, v]) => `${k} ${v}`).join(' · ')}</div>
          <div className="text-[10px] text-md-on-surface-var/60">
            tradeable edge = expectancy &gt; 0 AND PF &gt; 1 AND holds in BOTH halves AND enough trades. DD is on equal-weight %-sum (not a sized account).
          </div>
        </div>
      )}
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── DB CHART TAB ──────────────────────────────────────────────────────────────
// DB-sourced candlestick chart so on-chart signals == Sequence Builder matches.
// ═══════════════════════════════════════════════════════════════════════════════
// ── Codes V2 — auto sequence predictor straight off the loaded chart ─────────
// Takes the loaded ticker's last N bars (the SAME 6-line codes the chart shows),
// auto-runs the exact-sequence DB match, and shows the next up/down probability.
// No manual typing — pick how many lines (LINE1-7) + bars + universe and it
// recomputes live. Same engine as the Sequence Builder, just wired to the chart.
const _V2_LINES = [
  ['line1', 'TZ'], ['line2', 'L'], ['line3', 'suffix'], ['line4', 'body/wk'],
  ['line5', 'gap/rng'], ['line6', 'l5'], ['line7', 'vol'],
]
const _V2_FIELD = {  // strictness key → bar field it constrains (for the dimming display)
  line1: 'tz', line2: 'l', line3: 'suffix', line4: 'body_wick',
  line5: 'gap_range', line6: 'line5', line7: 'vol',
}
const _V2_UNI = [['all', 'All US'], ['nasdaq', 'Nasdaq'], ['sp500', 'S&P 500'], ['russell2k', 'Russell 2K']]

function CodesV2Panel({ ticker }) {
  const [nBars,   setNBars]   = useState(3)
  const [pivotLr, setPivotLr] = useState(3)
  const [uni,     setUni]     = useState('all')   // 'all' = whole DB (omit universe)
  const [strict,  setStrict]  = useState({
    line1: true, line2: true, line3: false, line4: false, line5: false, line6: false, line7: false,
  })
  const [seq,     setSeq]     = useState([])      // extracted bars, oldest → newest
  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)
  const runRef = useRef(0)

  // 1) Pull the last N bars' codes from the DB (newest-first → reverse to oldest→newest)
  useEffect(() => {
    let cancelled = false
    setError(null)
    api.studioBars(ticker, nBars).then(rows => {
      if (cancelled) return
      const top = (rows || []).slice(0, nBars).reverse()
      setSeq(top.map(r => ({
        date:      String(r.date).slice(0, 10),
        tz:        r.t_sig || r.z_sig || '',
        l:         r.l_sig || '',
        suffix:    r.composite_full_suffix || r.full_suffix || '',
        body_wick: r.bar_body_wick || '',
        gap_range: r.bar_gap_range || '',
        line5:     r.bar_line5 || '',
        vol:       r.vol_bucket || '',
      })))
    }).catch(e => { if (!cancelled) setError(e.message) })
    return () => { cancelled = true }
  }, [ticker, nBars])

  // 2) Auto-run the exact-sequence match on any change (debounced; stale-guarded)
  useEffect(() => {
    if (!seq.length) { setResult(null); return }
    const my = ++runRef.current
    setLoading(true); setError(null)
    const body = {
      bars: seq.map(b => ({
        tz: b.tz, l: b.l, suffix: b.suffix, body_wick: b.body_wick,
        gap_range: b.gap_range, line5: b.line5, vol: b.vol,
      })),
      strictness: strict, pivot_lr: pivotLr,
    }
    if (uni !== 'all') body.universe = uni
    const t = setTimeout(() => {
      api.studioExactSequence(body)
        .then(r => { if (my === runRef.current) { r.error ? setError(r.error) : setResult(r) } })
        .catch(e => { if (my === runRef.current) setError(e.message) })
        .finally(() => { if (my === runRef.current) setLoading(false) })
    }, 180)
    return () => clearTimeout(t)
  }, [seq, strict, pivotLr, uni])

  const toggle = k => setStrict(s => ({ ...s, [k]: !s[k] }))
  const o = result?.outcomes
  const up10 = o?.win_10d_pct

  return (
    <Card>
      <div className="flex items-center gap-2 mb-2">
        <h3 className="text-sm font-semibold text-md-on-surface">🧬 Codes V2 — auto sequence predictor</h3>
        <span className="text-[10px] text-md-on-surface-var/70">last {nBars} bars of {ticker} → DB match</span>
        {loading && <span className="text-[10px] text-sky-300 animate-pulse ml-auto">computing…</span>}
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 mb-3">
        <div className="flex items-center gap-1">
          <span className="text-[10px] text-md-on-surface-var/70 mr-1">bars</span>
          {[2, 3, 4, 5].map(n => (
            <button key={n} onClick={() => setNBars(n)}
              className={cls('px-2 py-0.5 rounded text-xs font-mono',
                nBars === n ? 'bg-emerald-700/60 text-emerald-100 font-semibold' : 'bg-md-surface-high text-md-on-surface-var hover:text-white')}>
              {n}b
            </button>
          ))}
        </div>
        <div className="flex items-center gap-1">
          <span className="text-[10px] text-md-on-surface-var/70 mr-1">match lines</span>
          {_V2_LINES.map(([k, lbl]) => (
            <button key={k} onClick={() => toggle(k)} title={lbl}
              className={cls('px-2 py-0.5 rounded text-xs',
                strict[k] ? 'bg-emerald-700/60 text-emerald-100 font-semibold border border-emerald-500' : 'bg-md-surface-high text-md-on-surface-var border border-md-outline-var hover:text-white')}>
              {lbl}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-1">
          <span className="text-[10px] text-md-on-surface-var/70 mr-1">pivot</span>
          {[3, 5].map(p => (
            <button key={p} onClick={() => setPivotLr(p)}
              className={cls('px-2 py-0.5 rounded text-xs font-mono',
                pivotLr === p ? 'bg-emerald-700/60 text-emerald-100 font-semibold' : 'bg-md-surface-high text-md-on-surface-var hover:text-white')}>
              {p}-{p}
            </button>
          ))}
        </div>
        <select value={uni} onChange={e => setUni(e.target.value)}
          className="bg-md-surface border border-md-outline-var rounded px-2 py-0.5 text-xs text-md-on-surface focus:outline-none focus:border-md-primary">
          {_V2_UNI.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
      </div>

      {/* Extracted sequence — what is actually being matched (active lines highlighted) */}
      {seq.length > 0 && (
        <div className="flex gap-2 mb-3 overflow-x-auto">
          {seq.map((b, i) => (
            <div key={i} className="rounded border border-md-outline-var bg-md-surface/30 p-2 min-w-[84px]">
              <div className="text-[9px] text-md-on-surface-var/60 mb-1">
                {i === seq.length - 1 ? 'bar 0 (now)' : `bar -${seq.length - 1 - i}`} · {b.date.slice(5)}
              </div>
              {_V2_LINES.map(([k, lbl]) => {
                const val = b[_V2_FIELD[k]]
                return (
                  <div key={k} className={cls('text-[10px] font-mono leading-tight',
                    strict[k] ? 'text-md-on-surface' : 'text-md-on-surface-var/30')}>
                    {val || '·'}
                  </div>
                )
              })}
            </div>
          ))}
        </div>
      )}

      {error && <div className="text-red-400 text-xs mb-2">{error}</div>}

      {/* Result */}
      {result && (result.matches === 0 ? (
        <div className="px-3 py-4 text-center text-amber-400/80 text-xs rounded border border-amber-700/30 bg-amber-900/10">
          0 historical matches for this sequence. Loosen by toggling off some lines or reducing bars.
        </div>
      ) : (
        <div className="space-y-3">
          <div className="flex items-center gap-3 text-[11px]">
            <span className="text-md-on-surface-var/70">sequence:</span>
            <span className="font-mono text-md-on-surface">{result.sequence_label}</span>
            <span className="ml-auto text-md-on-surface-var/70">
              {fmtNum(result.matches)} matches · {fmtNum(result.baseline)} universe bars · pivot {result.pivot_lr}-{result.pivot_lr}
            </span>
          </div>

          {/* Most likely next bar (next-bar T/Z signal distribution) */}
          {result.next_bar?.length > 0 && (() => {
            const best   = result.next_bar[0]
            const topSig = result.next_bar.find(r => r.is_bull || r.is_bear)
            const sb     = result.next_bar.filter(r => r.is_bull).reduce((a, r) => a + r.pct, 0)
            const sz     = result.next_bar.filter(r => r.is_bear).reduce((a, r) => a + r.pct, 0)
            return (
              <div className={cls('rounded-lg border-2 p-3 flex items-center gap-4 flex-wrap',
                best.is_bull ? 'border-lime-500/60 bg-lime-900/25'
                : best.is_bear ? 'border-red-500/60 bg-red-900/25'
                : 'border-md-outline-var bg-md-surface/50')}>
                <div className="flex flex-col">
                  <span className="text-[11px] uppercase tracking-wide font-semibold text-md-on-surface-var/80">🎯 predicted next bar</span>
                  <span className="text-[10px] text-md-on-surface-var/55">most common over {fmtNum(result.next_bar_total)} matches</span>
                </div>
                <div className="flex items-baseline gap-2">
                  <span className={cls('text-4xl font-mono font-extrabold leading-none',
                    best.is_bull ? 'text-lime-300' : best.is_bear ? 'text-red-300' : 'text-md-on-surface-var')}>
                    {best.sig}
                  </span>
                  <span className="text-2xl font-bold text-md-on-surface/80">{best.pct}%</span>
                </div>
                {best.sig === 'NONE' && topSig && (
                  <div className="text-xs text-md-on-surface-var/60">
                    top signal <span className={cls('text-lg font-mono font-bold', topSig.is_bull ? 'text-lime-300' : 'text-red-300')}>{topSig.sig}</span> {topSig.pct}%
                  </div>
                )}
                <div className="ml-auto text-right font-mono">
                  <div className="text-base font-bold">
                    <span className="text-lime-400">↑{sb.toFixed(0)}%</span>
                    <span className="text-md-on-surface-var/30"> · </span>
                    <span className="text-red-400">↓{sz.toFixed(0)}%</span>
                  </div>
                  <div className="text-[10px] text-md-on-surface-var/50">bull / bear lean</div>
                </div>
              </div>
            )
          })()}

          {/* Headline up/down probability (10d forward) */}
          {up10 != null && (
            <div className="flex items-stretch gap-2">
              <div className="flex-1 rounded border border-lime-700/30 bg-lime-900/15 p-3 text-center">
                <div className="text-[10px] text-lime-300/80">↑ UP next 10d</div>
                <div className="text-3xl font-mono font-bold text-lime-300">{up10}%</div>
                <div className="text-[10px] text-md-on-surface-var/70">avg {o.avg_fwd_10d > 0 ? '+' : ''}{o.avg_fwd_10d}% · n={o.fwd_10d_n}</div>
              </div>
              <div className="flex-1 rounded border border-red-700/30 bg-red-900/15 p-3 text-center">
                <div className="text-[10px] text-red-300/80">↓ DOWN next 10d</div>
                <div className="text-3xl font-mono font-bold text-red-300">{(100 - up10).toFixed(1)}%</div>
                <div className="text-[10px] text-md-on-surface-var/70">share of matches that fell</div>
              </div>
            </div>
          )}

          {/* Williams pivot split */}
          <div className="grid grid-cols-2 gap-3">
            <div className="rounded border border-lime-700/30 bg-lime-900/10 p-2">
              <div className="text-[10px] text-lime-300 font-semibold">↗ next pivot HH — {o.hh_pct ?? '—'}%</div>
              <div className="text-[10px] text-md-on-surface-var font-mono">avg +{o.avg_pct_to_hh ?? '—'}% in {o.avg_bars_to_hh ?? '—'} bars · {o.hh_count}/{o.next_pivot_known}</div>
            </div>
            <div className="rounded border border-amber-700/30 bg-amber-900/10 p-2">
              <div className="text-[10px] text-amber-300 font-semibold">↘ next pivot HL — {o.hl_pct ?? '—'}%</div>
              <div className="text-[10px] text-md-on-surface-var font-mono">avg {o.avg_pct_to_hl ?? '—'}% in {o.avg_bars_to_hl ?? '—'} bars · {o.hl_count}/{o.next_pivot_known}</div>
            </div>
          </div>

          {/* Forward returns grid */}
          <div className="grid grid-cols-3 gap-2 text-center">
            {[['5d', o.avg_fwd_5d, o.win_5d_pct, o.fwd_5d_n],
              ['10d', o.avg_fwd_10d, o.win_10d_pct, o.fwd_10d_n],
              ['20d', o.avg_fwd_20d, o.win_20d_pct, o.fwd_20d_n]].map(([tf, avg, win, n]) => (
              <div key={tf} className="rounded bg-md-surface-high/40 p-2">
                <div className="text-[9px] text-md-on-surface-var/70">{tf}</div>
                <div className={cls('text-base font-mono font-bold',
                  avg > 0 ? 'text-lime-400' : avg < 0 ? 'text-red-400' : 'text-md-on-surface-var')}>
                  {avg != null ? `${avg > 0 ? '+' : ''}${avg}%` : '—'}
                </div>
                <div className="text-[9px] text-md-on-surface-var/60">win {win ?? '—'}% · n={n}</div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </Card>
  )
}

function DbChartTab() {
  const [ticker, setTicker]   = useState(() => { try { return localStorage.getItem('studio_dbchart_ticker') || 'AAPL' } catch { return 'AAPL' } })
  const [inputVal, setInputVal] = useState(ticker)
  const go = () => {
    const t = inputVal.trim().toUpperCase()
    if (!t) return
    setTicker(t)
    try { localStorage.setItem('studio_dbchart_ticker', t) } catch {}
  }
  return (
    <div className="space-y-3">
      <Card>
        <h3 className="text-sm font-semibold text-md-on-surface mb-1">🕯️ DB Candlestick Chart</h3>
        <p className="text-xs text-md-on-surface-var">
          Renders candles + 6-line chart codes (TZ · L · suffix · body/wick · gap/range · line5)
          straight from the Studio DB. These are the <span className="font-mono text-amber-300">exact</span> codes
          the Sequence Builder searches — so what you read here will match. (Your TradingView chart uses a
          different price feed and can differ bar-to-bar.)
        </p>
      </Card>
      <div className="flex items-center gap-2">
        <input
          value={inputVal}
          onChange={e => setInputVal(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && go()}
          placeholder="Ticker (e.g. AAPL)"
          className="bg-md-surface border border-md-outline-var rounded-lg px-3 py-1.5 text-sm font-mono text-md-on-surface focus:outline-none focus:border-md-primary w-40"
        />
        <button onClick={go} className="px-3 py-1.5 text-sm rounded-lg bg-md-primary text-md-on-primary font-medium">Load</button>
      </div>
      <CodeCandleChart ticker={ticker} tf="1d" initialLimit={300} showFooter />
      <CodesV2Panel ticker={ticker} />
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── PLAYBOOK TAB ────────────────────────────────────────────────────────────────
// Predefined regime-gated setups, each run through the realised-backtest gate.
// Only survivors (expectancy>0 & PF>1 & positive in BOTH halves & enough trades)
// get a live watchlist. NOT a mega-score of all 80 signals — a small, honest funnel.
// ═══════════════════════════════════════════════════════════════════════════════
const PB_METRIC = ({ m }) => (!m || m.n === 0)
  ? <span className="text-md-on-surface-var">n=0</span>
  : (
    <span className="font-mono">
      n={m.n.toLocaleString()} · win {m.win_pct}% · PF {m.profit_factor ?? '—'} ·
      exp {fmtPct(m.expectancy, 2)} · med {fmtPct(m.median_ret, 2)} ·
      DD {m.max_drawdown}% · hold {m.avg_hold}b
    </span>
  )

function PlaybookSetupCard({ s }) {
  const bt = s.backtest || {}
  const ov = bt.overall || {}
  const err = bt.error
  return (
    <div className={cls(
      'rounded-xl border p-3 space-y-2',
      s.passed ? 'bg-emerald-950/20 border-emerald-700/50' : 'bg-md-surface-con border-md-outline-var'
    )}>
      <div className="flex items-center flex-wrap gap-2">
        <span className="text-sm font-semibold text-md-on-surface">{s.name}</span>
        <Badge color={s.side === 'short' ? 'red' : 'green'}>{s.side}</Badge>
        <Badge color={s.group === 'top' ? 'red' : 'green'}>{s.group}</Badge>
        {s.passed
          ? <Badge color="green">✓ PASS</Badge>
          : <Badge color="gray">✗ rejected</Badge>}
        <span className="ml-auto text-[10px] text-md-on-surface-var font-mono">
          +{s.rule.target_pct}/−{s.rule.stop_pct}/{s.rule.max_hold}b
          {s.wyc_phase !== 'all' && <> · gate {s.wyc_phase}</>}
        </span>
      </div>

      {/* signal confluence */}
      <div className="flex items-center flex-wrap gap-1">
        {s.signals.map(c => (
          <span key={c} className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-md-surface-high border border-md-outline-var text-md-on-surface">
            {colToLabel(c)}
          </span>
        ))}
        {s.missing?.length > 0 && (
          <span className="text-[10px] text-amber-400" title={s.missing.join(', ')}>
            ({s.missing.length} flag(s) not in DB)
          </span>
        )}
      </div>

      <p className="text-[11px] text-md-on-surface-var leading-snug">{s.thesis}</p>

      {/* realised stats */}
      {err ? (
        <div className="text-[11px] text-amber-400">backtest: {err}</div>
      ) : (
        <div className="text-[11px] space-y-0.5 bg-md-surface-high rounded-lg px-2.5 py-1.5 border border-md-outline-var">
          <div className={ov.expectancy > 0 ? 'text-green-300' : 'text-red-300'}>
            <b>all</b> · <PB_METRIC m={ov} />
          </div>
          <div className="text-md-on-surface-var">1st½ · <PB_METRIC m={bt.first_half} /></div>
          <div className="text-md-on-surface-var">2nd½ · <PB_METRIC m={bt.second_half} /></div>
          {bt.date_range && (
            <div className="text-[10px] text-md-on-surface-var/60">
              {bt.date_range[0]} → {bt.date_range[1]}
              {bt.truncated && <span className="text-amber-400"> · ⚠ sample of {fmtNum(bt.n_tickers)}/{fmtNum(bt.n_tickers_total)} tickers (hit trade cap)</span>}
            </div>
          )}
        </div>
      )}

      {!s.passed && s.reject_reason && (
        <div className="text-[11px] text-md-on-surface-var/80">⛔ gate: {s.reject_reason}</div>
      )}

      {/* live matches (survivors only) */}
      {s.passed && (
        <div>
          <div className="text-[11px] text-emerald-300 font-medium mb-1">
            📡 Live now ({s.n_live})
          </div>
          {s.live_tickers.length === 0 ? (
            <span className="text-[11px] text-md-on-surface-var/60">no tickers currently match this setup</span>
          ) : (
            <div className="flex flex-wrap gap-1.5">
              {s.live_tickers.map(t => (
                <span key={t.ticker}
                  title={`${t.date} · ${t.wyc_phase || '—'} · avg vol ${fmtNum(t.avg_vol_20d)}`}
                  className="px-2 py-0.5 rounded-lg text-[11px] font-mono bg-md-surface border border-md-outline-var text-md-on-surface">
                  <b>{t.ticker}</b> <span className="text-md-on-surface-var">${t.close}</span>
                </span>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function PlaybookTab() {
  const [p, setP] = useState({ universe: 'sp500', min_trades: 30, min_price: 5, min_volume: 100000 })
  const [data, setData]   = useState(null)
  const [loading, setLd]  = useState(false)
  const [error, setError] = useState(null)
  const upd = (k, v) => setP(prev => ({ ...prev, [k]: v }))

  const run = async () => {
    setLd(true); setError(null)
    try {
      const r = await api.studioPlaybook(p)
      if (r.error && !r.setups?.length) setError(r.error)
      setData(r)
    } catch (e) { setError(e.message) } finally { setLd(false) }
  }

  const passed = data?.setups?.filter(s => s.passed) || []
  const failed = data?.setups?.filter(s => !s.passed) || []

  return (
    <div className="space-y-3">
      <Card>
        <h3 className="text-sm font-semibold text-md-on-surface mb-1">📒 Playbook — validated, regime-gated setups</h3>
        <p className="text-xs text-md-on-surface-var">
          A <b>funnel, not a score</b>: each predefined setup (regime gate → signal confluence → entry rule)
          is run through the <b>realised backtest gate</b> — it only enters the Playbook if
          <span className="font-mono"> expectancy&gt;0 AND profit_factor&gt;1 AND positive in BOTH time-halves AND enough trades</span>.
          Survivors get today's live tickers; rejects stay visible with their reason. Honest expectation:
          <b> a handful of modest edges</b> used as a watchlist / bias overlay — not a money-printer
          (this project's backtests show even the best signals are marginal, PF≈1.15, or rare).
        </p>
      </Card>

      {/* controls */}
      <div className="flex flex-wrap items-end gap-2 text-xs">
        <Field label="Universe">
          <select value={p.universe} onChange={e => upd('universe', e.target.value)} className={SEQLAB_SEL}>
            {['sp500', 'nasdaq', 'russell2k'].map(u => <option key={u} value={u}>{u}</option>)}
          </select>
        </Field>
        <Field label="Min trades">
          <input type="number" value={p.min_trades} onChange={e => upd('min_trades', Number(e.target.value))}
            className={SEQLAB_SEL + ' w-20'} />
        </Field>
        <Field label="Min $ (live)">
          <input type="number" value={p.min_price} onChange={e => upd('min_price', Number(e.target.value))}
            className={SEQLAB_SEL + ' w-20'} />
        </Field>
        <Field label="Min vol (live)">
          <input type="number" value={p.min_volume} onChange={e => upd('min_volume', Number(e.target.value))}
            className={SEQLAB_SEL + ' w-28'} />
        </Field>
        <button onClick={run} disabled={loading}
          className="px-3 py-1.5 rounded-lg bg-md-primary text-md-on-primary font-medium disabled:opacity-50">
          {loading ? 'building…' : '▶ Build Playbook'}
        </button>
      </div>

      {error && <div className="text-xs text-red-400">{error}</div>}

      {data && (
        <div className="text-xs bg-md-surface-high rounded-lg px-3 py-2 border border-md-outline-var">
          <span className="font-mono">{data.n_passed}</span> of <span className="font-mono">{data.n_total}</span> setups
          passed the backtest gate · universe <span className="font-mono">{data.universe}</span>
          {data.date_range && <> · {data.date_range[0]} → {data.date_range[1]}</>}
        </div>
      )}

      {passed.length > 0 && (
        <div className="space-y-2">
          <h4 className="text-xs font-semibold text-emerald-300 uppercase tracking-wide">✓ The Playbook ({passed.length})</h4>
          {passed.map(s => <PlaybookSetupCard key={s.id} s={s} />)}
        </div>
      )}

      {failed.length > 0 && (
        <div className="space-y-2">
          <h4 className="text-xs font-semibold text-md-on-surface-var uppercase tracking-wide">✗ Rejected candidates ({failed.length})</h4>
          {failed.map(s => <PlaybookSetupCard key={s.id} s={s} />)}
        </div>
      )}

      {data && data.n_total === 0 && !error && (
        <div className="py-6 text-center text-md-on-surface-var/60 text-xs">No setups defined.</div>
      )}
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── MAIN STUDIO PANEL ─────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
export default function StudioPanel() {
  const [activeTab, setActiveTab] = useState(() => {
    try { return localStorage.getItem('studio_tab') || 'overview' } catch { return 'overview' }
  })

  const switchTab = (id) => {
    setActiveTab(id)
    try { localStorage.setItem('studio_tab', id) } catch {}
  }

  return (
    <div className="flex flex-col gap-3 min-h-[600px]">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div>
          <h2 className="text-base font-bold text-md-on-surface">📊 Analytic Studio</h2>
          <p className="text-[11px] text-md-on-surface-var">
            DuckDB-powered signal analytics · 1.2M+ bars · lift-based pattern mining
          </p>
        </div>
      </div>

      {/* Sub-tab bar */}
      <div className="flex gap-1 border-b border-md-outline-var pb-0.5 overflow-x-auto">
        {SUBTABS.map(t => (
          <button
            key={t.id}
            onClick={() => switchTab(t.id)}
            className={cls(
              'px-3 py-2 text-xs font-medium rounded-t-lg whitespace-nowrap transition-colors',
              activeTab === t.id
                ? 'bg-md-surface-con text-md-on-surface border-b-2 border-md-primary -mb-0.5'
                : 'text-md-on-surface-var hover:text-md-on-surface hover:bg-white/5'
            )}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="flex-1">
        {activeTab === 'overview'  && <OverviewTab />}
        {activeTab === 'hunter'    && <ExitHunterTab />}
        {activeTab === 'edge'      && <EdgeScannerTab />}
        {activeTab === 'playbook'  && <PlaybookTab />}
        {activeTab === 'sigstats'  && <SignalStatsTab />}
        {activeTab === 'exact'     && <ExactSequenceTab />}
        {activeTab === 'seqlab'    && <SeqLabTab />}
        {activeTab === 'dbchart'   && <DbChartTab />}
        {activeTab === 'events'    && <EventsTab />}
        {activeTab === 'patterns'  && <PatternsTab />}
        {activeTab === 'miss'      && <MissedTab />}
        {activeTab === 'fp'        && <FalsePostitiveTab />}
        {activeTab === 'scoring'   && <ScoringLabTab />}
      </div>
    </div>
  )
}
