import { useState, useRef, useMemo, useEffect } from 'react'
import { createPortal } from 'react-dom'

const BASE = import.meta.env.VITE_API_URL || ''
const TOOLTIP_W = 520

const CSV_COLS = [
  'ticker','date','close','volume',
  'final_signal','composite_pattern','seq4','lane1','lane3',
  'role','score','quality','action',
  'vol_bucket','wick_suffix',
  'above_ema20','above_ema50','above_ema89',
  'ema20_reclaim','ema50_reclaim','ema89_reclaim',
  'conflict_flag','conflict_resolution','conflicting_rule_ids',
  'good_flags','reject_flags',
  'price_position_4bar','breaks_4bar_high','breaks_4bar_low',
  'final_volume_vs_prev1','final_volume_vs_prev2','final_volume_vs_prev3',
  'matched_rule_id','matched_rule_type','matched_universe','matched_status',
  'matched_med10d_pct','matched_fail10d_pct','matched_avg10d_pct',
  'matched_source_file','matched_rule_notes',
  'matched_composite_rule_id','matched_seq4_rule_id','matched_reject_rule_id',
  // PULLBACK_GO proof fields
  'prior_pullback_ready_found','prior_pullback_ready_bars_ago',
  'prior_pullback_ready_signal','prior_pullback_ready_composite',
  'prior_pullback_ready_role','pullback_high','current_close_above_pullback_high',
  // Liquidity fields
  'dollar_volume','liquidity_tier',
  'explanation',
]

function exportCSV(rows) {
  const lines = [CSV_COLS.join(',')]
  for (const r of rows) {
    lines.push(CSV_COLS.map(c => {
      let v = r[c] ?? ''
      if (Array.isArray(v)) v = v.join(';')
      v = String(v)
      return v.includes(',') || v.includes('"') || v.includes('\n')
        ? `"${v.replace(/"/g, '""')}"` : v
    }).join(','))
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `tz_intelligence_${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

async function apiGet(path) {
  const res = await fetch(BASE + path)
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

// ── Sort helpers ──────────────────────────────────────────────────────────────

const ROLE_SORT_ORDER = {
  BULL_A: 1, BULL_B: 2, PULLBACK_GO: 3,
  PULLBACK_READY_A: 4, PULLBACK_CONFIRMING: 5, PULLBACK_READY_B: 6,
  DEEP_PULLBACK_WATCH: 7, PULLBACK_WATCH: 8,
  MIXED_WATCH: 9, BULL_WATCH: 10,
  SHORT_WATCH: 11, SHORT_GO: 12,
  REJECT_LONG: 13, REJECT: 14, NO_EDGE: 15,
}
const QUALITY_SORT_ORDER = { A: 1, B: 2, Watch: 3, Reject: 4, '—': 5 }
const RULE_TYPE_SHORT = {
  COMPOSITE: 'COMP', REJECT_COMPOSITE: 'R-COMP',
  SEQ4: 'SEQ4', REJECT_SEQ4: 'R-SEQ4', BASELINE: 'BASE',
}

function getSortValue(row, key) {
  switch (key) {
    case 'close':               return parseFloat(row.close) || 0
    case 'score':               return parseFloat(row.score) || 0
    case 'price_position_4bar': return parseFloat(row.price_position_4bar) || 0
    case 'matched_med10d_pct':  return parseFloat(row.matched_med10d_pct) || -999
    case 'matched_fail10d_pct': return parseFloat(row.matched_fail10d_pct) || 9999
    case 'role':                return ROLE_SORT_ORDER[row.role] ?? 99
    case 'quality':             return QUALITY_SORT_ORDER[row.quality] ?? 99
    case 'ema':
      return (row.above_ema20 ? 1 : 0) + (row.above_ema50 ? 1 : 0) + (row.above_ema89 ? 1 : 0)
    default:
      return String(row[key] ?? '').toLowerCase()
  }
}

function defaultSort(rows) {
  return [...rows].sort((a, b) => {
    const rp = (ROLE_SORT_ORDER[a.role] ?? 99) - (ROLE_SORT_ORDER[b.role] ?? 99)
    if (rp !== 0) return rp
    const sd = (parseFloat(b.score) || 0) - (parseFloat(a.score) || 0)
    if (sd !== 0) return sd
    return (a.ticker ?? '').localeCompare(b.ticker ?? '')
  })
}

// ── Constants ─────────────────────────────────────────────────────────────────

const UNIVERSES = [
  { key: 'sp500',      label: 'S&P 500'     },
  { key: 'nasdaq',     label: 'NASDAQ'      },
  { key: 'nasdaq_gt5', label: 'NASDAQ > $5' },
  { key: 'russell2k',  label: 'Russell 2K'  },
  { key: 'all_us',     label: 'All US'      },
  { key: 'split',      label: '✂️ SPLIT'    },
]

const NASDAQ_BATCHES = [
  { key: 'a_m', label: 'A–M (½)' },
  { key: 'n_z', label: 'N–Z (½)' },
]

const TF_OPTS = ['1d', '4h', '1h', '1wk']

const ROLES = [
  { key: 'all',                label: 'All'               },
  { key: 'BULL_A',             label: '🟢 BULL A'         },
  { key: 'BULL_B',             label: '🟩 BULL B'         },
  { key: 'PULLBACK_GO',        label: '🚀 PB GO'          },
  { key: 'PULLBACK_CONFIRMING',label: '⏳ PB Confirming'  },
  { key: 'PULLBACK_READY_A',   label: '🔵 PB Ready A'     },
  { key: 'PULLBACK_READY_B',   label: '🔵 PB Ready B'     },
  { key: 'PULLBACK_WATCH',     label: '🔷 PB Watch'       },
  { key: 'DEEP_PULLBACK_WATCH',label: '⬇ Deep PB'         },
  { key: 'BULL_WATCH',         label: '👀 Bull Watch'     },
  { key: 'MIXED_WATCH',        label: '⚖ Mixed Watch'    },
  { key: 'SHORT_WATCH',        label: '🔴 Short Watch'    },
  { key: 'SHORT_GO',           label: '🔴 SHORT GO'       },
  { key: 'REJECT_LONG',        label: '🚫 Reject Long'    },
  { key: 'REJECT',             label: '❌ Reject'         },
]

const ROLE_COLORS = {
  BULL_A:               'bg-green-800/60 text-green-200 border-green-600/50',
  BULL_B:               'bg-green-900/50 text-green-300 border-green-700/40',
  PULLBACK_GO:          'bg-indigo-700/60 text-indigo-100 border-indigo-500/60',
  PULLBACK_CONFIRMING:  'bg-indigo-900/50 text-indigo-300 border-indigo-700/40',
  PULLBACK_READY_A:     'bg-blue-800/60 text-blue-200 border-blue-600/50',
  PULLBACK_READY_B:     'bg-blue-900/50 text-blue-300 border-blue-700/40',
  PULLBACK_WATCH:       'bg-cyan-900/50 text-cyan-300 border-cyan-700/40',
  DEEP_PULLBACK_WATCH:  'bg-slate-800/60 text-slate-400 border-slate-600/40',
  BULL_WATCH:           'bg-teal-900/50 text-teal-300 border-teal-700/40',
  MIXED_WATCH:          'bg-yellow-900/50 text-yellow-300 border-yellow-700/40',
  SHORT_WATCH:          'bg-red-900/50 text-red-300 border-red-700/40',
  SHORT_GO:             'bg-red-700/70 text-red-100 border-red-500/60',
  REJECT_LONG:          'bg-orange-900/50 text-orange-300 border-orange-700/40',
  REJECT:               'bg-gray-800/60 text-gray-400 border-gray-600/40',
  NO_EDGE:              'bg-gray-900/40 text-gray-600 border-gray-700/30',
}

const QUALITY_COLORS = {
  A:      'text-green-300',
  B:      'text-teal-300',
  Watch:  'text-yellow-400',
  Reject: 'text-gray-500',
  '—':    'text-gray-600',
}

const ACTION_COLORS = {
  BUY_TRIGGER:               'text-green-400 font-semibold',
  WAIT_FOR_T_CONFIRMATION:   'text-teal-400',
  PULLBACK_ENTRY_READY:      'text-indigo-300 font-semibold',
  WATCH_PULLBACK:            'text-blue-300',
  WAIT_FOR_CONFIRMATION:     'text-gray-400',
  WAIT_FOR_BREAKDOWN:        'text-orange-400',
  SHORT_TRIGGER:             'text-red-400 font-semibold',
  DO_NOT_BUY:                'text-orange-500',
  IGNORE:                    'text-gray-600',
}

// ── Sub-components ────────────────────────────────────────────────────────────

function RoleBadge({ role }) {
  const color = ROLE_COLORS[role] || 'bg-gray-800 text-gray-400'
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-xs font-bold border ${color}`}>
      {role?.replace(/_/g, ' ') || '—'}
    </span>
  )
}

function ScoreBar({ score }) {
  const pct = Math.min(100, Math.max(0, ((score + 40) / 140) * 100))
  const color = score >= 80 ? 'bg-green-500' : score >= 60 ? 'bg-teal-500' : score >= 35 ? 'bg-yellow-500' : score >= 0 ? 'bg-gray-500' : 'bg-red-500'
  return (
    <div className="flex items-center gap-1.5">
      <div className="w-14 h-1.5 bg-gray-800 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <span className={`text-xs font-mono ${score > 0 ? 'text-gray-300' : 'text-red-400'}`}>
        {score > 0 ? `+${score}` : score}
      </span>
    </div>
  )
}

function EMADots({ above20, above50, above89 }) {
  return (
    <div className="flex gap-0.5 text-xs">
      <span className={above20 ? 'text-green-400' : 'text-gray-700'} title="EMA20">20</span>
      <span className="text-gray-700">/</span>
      <span className={above50 ? 'text-green-400' : 'text-gray-700'} title="EMA50">50</span>
      <span className="text-gray-700">/</span>
      <span className={above89 ? 'text-green-400' : 'text-gray-700'} title="EMA89">89</span>
    </div>
  )
}

function SortTh({ label, colKey, sortKey, sortDir, onSort, align = 'left', className = '' }) {
  const active = sortKey === colKey
  return (
    <th
      className={`p-1 font-medium cursor-pointer select-none whitespace-nowrap
        text-${align} hover:text-gray-200 transition-colors
        ${active ? 'text-blue-400' : 'text-gray-400'} ${className}`}
      onClick={() => onSort(colKey)}
    >
      {label}
      <span className={`ml-0.5 text-xs ${active ? 'text-blue-400' : 'opacity-20'}`}>
        {active ? (sortDir === 'asc' ? '▲' : '▼') : '⇅'}
      </span>
    </th>
  )
}

// Portal tooltip — opens LEFT of icon, stays inside viewport
function ReasonTooltip({ codes, explanation }) {
  const [pos, setPos] = useState(null)
  const btnRef = useRef(null)
  const timerRef = useRef(null)

  function showTooltip() {
    clearTimeout(timerRef.current)
    if (!btnRef.current) return
    const rect = btnRef.current.getBoundingClientRect()
    const margin = 8
    let left
    // Try left of icon first
    if (rect.left - TOOLTIP_W - margin >= 4) {
      left = rect.left - TOOLTIP_W - margin
    // Fallback: right of icon
    } else if (rect.right + TOOLTIP_W + margin <= window.innerWidth - 4) {
      left = rect.right + margin
    // Last resort: clamp to left edge
    } else {
      left = Math.max(4, rect.left - TOOLTIP_W)
    }
    const top = Math.max(4, Math.min(rect.top, window.innerHeight - 340))
    setPos({ left, top })
  }

  function hideTooltip() {
    timerRef.current = setTimeout(() => setPos(null), 120)
  }

  if (!codes?.length && !explanation) return null
  return (
    <>
      <button
        ref={btnRef}
        onMouseEnter={showTooltip}
        onMouseLeave={hideTooltip}
        className="text-gray-500 hover:text-blue-400 text-xs px-1 transition-colors"
        aria-label="Show reason codes"
      >
        ℹ
      </button>
      {pos && createPortal(
        <div
          style={{
            position: 'fixed',
            left: pos.left,
            top: pos.top,
            zIndex: 9999,
            width: TOOLTIP_W,
            maxHeight: 340,
          }}
          className="bg-gray-900 border border-gray-700 rounded shadow-2xl overflow-y-auto"
          onMouseEnter={() => clearTimeout(timerRef.current)}
          onMouseLeave={hideTooltip}
        >
          <div className="p-3">
            {explanation && (
              <div className="mb-2 text-xs text-gray-300 font-medium leading-relaxed border-b border-gray-800 pb-2">
                {explanation}
              </div>
            )}
            <div className="space-y-0.5">
              {codes?.map((c, i) => (
                <div key={i} className="text-xs text-gray-500 font-mono break-all leading-4">{c}</div>
              ))}
            </div>
          </div>
        </div>,
        document.body
      )}
    </>
  )
}

// ── Main panel ────────────────────────────────────────────────────────────────

export default function TZIntelligencePanel({ onSelectTicker }) {
  const [universe, setUniverse]       = useState('sp500')
  const [nasdaqBatch, setNasdaqBatch] = useState('a_m')
  const [tf, setTf]                   = useState('1d')
  const [roleFilter, setRoleFilter]   = useState('all')
  const [minPrice, setMinPrice]       = useState('')
  const [maxPrice, setMaxPrice]       = useState('')
  const [minVolume, setMinVolume]     = useState('')

  const [results, setResults] = useState([])
  const [total, setTotal]     = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState(null)

  // Sort state — null/null = default order
  const [sortKey, setSortKey] = useState(null)
  const [sortDir, setSortDir] = useState(null)

  // NASDAQ > $5 enforces min price = 5
  useEffect(() => {
    if (universe === 'nasdaq_gt5') {
      if (minPrice === '' || parseFloat(minPrice) < 5) {
        setMinPrice('5')
      }
    }
  }, [universe])

  async function handleScan() {
    setLoading(true)
    setError(null)
    setSortKey(null)
    setSortDir(null)
    try {
      const qs = new URLSearchParams({ universe, tf, role_filter: roleFilter })
      if (universe === 'nasdaq') qs.set('nasdaq_batch', nasdaqBatch)
      // nasdaq_gt5 always enforces min_price >= 5
      const effectiveMinPrice = universe === 'nasdaq_gt5'
        ? String(Math.max(5, parseFloat(minPrice) || 5))
        : minPrice
      if (effectiveMinPrice) qs.set('min_price', effectiveMinPrice)
      if (maxPrice)  qs.set('max_price', maxPrice)
      if (minVolume) qs.set('min_volume', minVolume)
      const data = await apiGet(`/api/tz-intelligence/scan?${qs}`)
      if (data.error) { setError(data.error); setResults([]) }
      else { setResults(data.results || []); setTotal(data.total || 0) }
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  function handleSort(key) {
    if (sortKey === key) {
      if (sortDir === 'asc') setSortDir('desc')
      else { setSortKey(null); setSortDir(null) }   // third click → reset
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  // Sorted rows — used for both table display and CSV export
  const displayRows = useMemo(() => {
    if (!sortKey) return defaultSort(results)
    return [...results].sort((a, b) => {
      const va = getSortValue(a, sortKey)
      const vb = getSortValue(b, sortKey)
      const cmp = (typeof va === 'number' && typeof vb === 'number')
        ? va - vb
        : String(va).localeCompare(String(vb))
      return sortDir === 'desc' ? -cmp : cmp
    })
  }, [results, sortKey, sortDir])

  const thProps = { sortKey, sortDir, onSort: handleSort }

  return (
    <div className="bg-gray-950 text-gray-100 p-3 flex flex-col gap-3">
      {/* Header */}
      <div className="flex items-center gap-2">
        <span className="text-lg font-bold text-white">🧠 TZ Signal Intelligence</span>
        <span className="text-xs text-gray-500">Matrix-based role classifier — BULL_A / SHORT_GO / REJECT / …</span>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap gap-2 items-end">
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Universe</label>
          <select value={universe} onChange={e => setUniverse(e.target.value)}
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700">
            {UNIVERSES.map(u => <option key={u.key} value={u.key}>{u.label}</option>)}
          </select>
        </div>

        {universe === 'nasdaq' && (
          <div className="flex flex-col gap-1">
            <label className="text-xs text-gray-500">Batch</label>
            <div className="flex gap-1">
              {NASDAQ_BATCHES.map(b => (
                <button key={b.key} onClick={() => setNasdaqBatch(b.key)}
                  className={`text-xs px-2 py-1 rounded transition-colors
                    ${nasdaqBatch === b.key ? 'bg-amber-600 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
                  {b.label}
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Timeframe</label>
          <div className="flex gap-1">
            {TF_OPTS.map(t => (
              <button key={t} onClick={() => setTf(t)}
                className={`text-xs px-2 py-1 rounded transition-colors
                  ${tf === t ? 'bg-blue-600 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
                {t}
              </button>
            ))}
          </div>
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Role</label>
          <select value={roleFilter} onChange={e => setRoleFilter(e.target.value)}
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700">
            {ROLES.map(r => <option key={r.key} value={r.key}>{r.label}</option>)}
          </select>
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">
            Min ${universe === 'nasdaq_gt5' && <span className="text-amber-400 ml-1" title="NASDAQ > $5 requires min price 5 — analytics matrix built on price>$5 data">⚠ ≥5</span>}
          </label>
          <input type="number" value={minPrice}
            onChange={e => {
              const v = parseFloat(e.target.value)
              if (universe === 'nasdaq_gt5' && !isNaN(v) && v < 5) return
              setMinPrice(e.target.value)
            }}
            placeholder={universe === 'nasdaq_gt5' ? '5' : '0'}
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700 w-20" />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Max $</label>
          <input type="number" value={maxPrice} onChange={e => setMaxPrice(e.target.value)}
            placeholder="∞" className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700 w-20" />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Min Vol</label>
          <input type="number" value={minVolume} onChange={e => setMinVolume(e.target.value)}
            placeholder="0" className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700 w-24" />
        </div>

        <button onClick={handleScan} disabled={loading}
          className="px-3 py-1.5 bg-purple-600 hover:bg-purple-500 disabled:bg-gray-700 text-white text-xs font-semibold rounded transition-colors self-end">
          {loading ? 'Classifying…' : '🧠 Classify'}
        </button>
      </div>

      <div className="text-xs text-gray-600 px-1">
        Requires TZ/WLNBB stock_stat CSV — run <span className="text-gray-400">📡 TZ/WLNBB → Generate Stock Stat</span> first.
      </div>

      {error && (
        <div className="p-2 bg-red-900/30 border border-red-700 rounded text-red-300 text-xs">{error}</div>
      )}

      {displayRows.length > 0 && (
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-500">
            Showing {displayRows.length} of {total} classified tickers
            {sortKey && <span className="ml-1 text-blue-400/70">· sorted by {sortKey} {sortDir === 'asc' ? '▲' : '▼'}</span>}
          </span>
          <button
            onClick={() => exportCSV(displayRows)}
            className="px-2 py-1 bg-gray-800 hover:bg-gray-700 text-gray-300 text-xs rounded border border-gray-700 transition-colors"
          >
            ⬇ CSV (current sort)
          </button>
        </div>
      )}

      {displayRows.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead className="sticky top-0 z-10 bg-gray-950">
              <tr className="border-b border-gray-800">
                <SortTh label="Ticker"    colKey="ticker"             {...thProps} />
                <SortTh label="Date"      colKey="date"               {...thProps} />
                <SortTh label="Close"     colKey="close"              {...thProps} align="right" />
                <SortTh label="Signal"    colKey="final_signal"       {...thProps} />
                <SortTh label="Composite" colKey="composite_pattern"  {...thProps} />
                <SortTh label="Seq4"      colKey="seq4"               {...thProps} />
                <SortTh label="Role"      colKey="role"               {...thProps} />
                <SortTh label="Score"     colKey="score"              {...thProps} />
                <SortTh label="Qual"      colKey="quality"            {...thProps} />
                <SortTh label="Action"    colKey="action"             {...thProps} />
                <SortTh label="Vol"       colKey="vol_bucket"         {...thProps} />
                <SortTh label="EMA"       colKey="ema"                {...thProps} />
                <SortTh label="Pos%"      colKey="price_position_4bar" {...thProps} align="right" />
                <SortTh label="Med10d"    colKey="matched_med10d_pct"  {...thProps} align="right" />
                <SortTh label="Fail10d"   colKey="matched_fail10d_pct" {...thProps} align="right" />
                <SortTh label="Rule"      colKey="matched_rule_type"   {...thProps} />
                <th className="p-1 text-gray-600 font-normal w-6"></th>
              </tr>
            </thead>
            <tbody>
              {displayRows.map((row, i) => (
                <tr key={`${row.ticker}-${row.date}-${i}`}
                  className="border-b border-gray-800/50 hover:bg-gray-900/40 transition-colors cursor-pointer"
                  onClick={() => onSelectTicker?.(row.ticker)}
                >
                  <td className="p-1 font-semibold text-white">{row.ticker}</td>
                  <td className="p-1 text-gray-400">{row.date}</td>
                  <td className="p-1 text-right text-gray-200">
                    {row.close ? Number(row.close).toFixed(2) : '—'}
                  </td>
                  <td className="p-1">
                    <span className={`font-mono text-xs
                      ${row.final_signal?.startsWith('T') ? 'text-blue-300'
                        : row.final_signal?.startsWith('Z') ? 'text-red-300'
                        : 'text-yellow-300'}`}>
                      {row.final_signal || '—'}
                    </span>
                  </td>
                  <td className="p-1 text-gray-400 font-mono text-xs">{row.composite_pattern || '—'}</td>
                  <td className="p-1 font-mono text-xs" title={row.seq4 || ''}>
                    {row.seq4
                      ? row.seq4.split('|').map((s, idx, arr) => (
                          <span key={idx}>
                            <span className={
                              idx === arr.length - 1
                                ? (s.startsWith('T') ? 'text-blue-300' : s.startsWith('Z') ? 'text-red-300' : 'text-yellow-300')
                                : 'text-gray-600'
                            }>{s}</span>
                            {idx < arr.length - 1 && <span className="text-gray-700">|</span>}
                          </span>
                        ))
                      : <span className="text-gray-700">—</span>
                    }
                  </td>
                  <td className="p-1"><RoleBadge role={row.role} /></td>
                  <td className="p-1"><ScoreBar score={row.score || 0} /></td>
                  <td className={`p-1 font-semibold ${QUALITY_COLORS[row.quality] || 'text-gray-400'}`}>
                    {row.quality}
                  </td>
                  <td className={`p-1 text-xs ${ACTION_COLORS[row.action] || 'text-gray-400'}`}>
                    {row.action?.replace(/_/g, ' ')}
                  </td>
                  <td className="p-1">
                    <span className={`text-xs
                      ${row.vol_bucket === 'VB' ? 'text-red-300'
                        : row.vol_bucket === 'B' ? 'text-orange-300'
                        : 'text-gray-500'}`}>
                      {row.vol_bucket || ''}
                    </span>
                  </td>
                  <td className="p-1">
                    <EMADots above20={row.above_ema20} above50={row.above_ema50} above89={row.above_ema89} />
                  </td>
                  <td className="p-1 text-right text-gray-400 tabular-nums">
                    {row.price_position_4bar != null
                      ? `${(row.price_position_4bar * 100).toFixed(0)}%`
                      : '—'}
                  </td>
                  <td className="p-1 text-right tabular-nums">
                    {row.matched_med10d_pct != null && row.matched_med10d_pct !== ''
                      ? <span className={parseFloat(row.matched_med10d_pct) >= 0.8 ? 'text-green-400' : parseFloat(row.matched_med10d_pct) >= 0 ? 'text-gray-300' : 'text-red-400'}>
                          {parseFloat(row.matched_med10d_pct).toFixed(2)}
                        </span>
                      : <span className="text-gray-700">—</span>
                    }
                  </td>
                  <td className="p-1 text-right tabular-nums">
                    {row.matched_fail10d_pct != null && row.matched_fail10d_pct !== ''
                      ? <span className={parseFloat(row.matched_fail10d_pct) < 25 ? 'text-green-400' : parseFloat(row.matched_fail10d_pct) < 28 ? 'text-yellow-400' : 'text-red-400'}>
                          {parseFloat(row.matched_fail10d_pct).toFixed(1)}%
                        </span>
                      : <span className="text-gray-700">—</span>
                    }
                  </td>
                  <td className="p-1 text-gray-500 text-xs font-mono">
                    {RULE_TYPE_SHORT[row.matched_rule_type] || row.matched_rule_type || '—'}
                  </td>
                  <td className="p-1 text-right" onClick={e => e.stopPropagation()}>
                    <ReasonTooltip codes={row.reason_codes} explanation={row.explanation} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && !error && displayRows.length === 0 && (
        <div className="text-gray-600 text-xs py-6 text-center">
          No results. Run "🧠 Classify" to start — needs TZ/WLNBB stock_stat CSV.
        </div>
      )}
    </div>
  )
}
