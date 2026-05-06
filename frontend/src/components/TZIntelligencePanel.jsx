import { useState } from 'react'

const BASE = import.meta.env.VITE_API_URL || ''

function exportCSV(results) {
  const cols = ['ticker','date','close','final_signal','composite_pattern','seq4','role','score','quality','action','vol_bucket','wick_suffix','above_ema20','above_ema50','above_ema89','explanation']
  const lines = [cols.join(',')]
  for (const r of results) {
    lines.push(cols.map(c => {
      const v = r[c] ?? ''
      return String(v).includes(',') ? `"${v}"` : v
    }).join(','))
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `tz_intelligence_${new Date().toISOString().slice(0,10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

async function apiGet(path) {
  const res = await fetch(BASE + path)
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

const UNIVERSES = [
  { key: 'sp500',     label: 'S&P 500'    },
  { key: 'nasdaq',    label: 'NASDAQ'     },
  { key: 'russell2k', label: 'Russell 2K' },
  { key: 'all_us',    label: 'All US'     },
  { key: 'split',     label: '✂️ SPLIT'   },
]

const NASDAQ_BATCHES = [
  { key: 'a_m', label: 'A–M (½)' },
  { key: 'n_z', label: 'N–Z (½)' },
]

const TF_OPTS = ['1d', '4h', '1h', '1wk']

const ROLES = [
  { key: 'all',            label: 'All'              },
  { key: 'BULL_A',         label: '🟢 BULL A'        },
  { key: 'BULL_B',         label: '🟩 BULL B'        },
  { key: 'PULLBACK_READY_A', label: '🔵 PB Ready A'  },
  { key: 'PULLBACK_READY_B', label: '🔵 PB Ready B'  },
  { key: 'PULLBACK_WATCH', label: '🔷 PB Watch'      },
  { key: 'BULL_WATCH',     label: '👀 Bull Watch'    },
  { key: 'SHORT_WATCH',    label: '🔴 Short Watch'   },
  { key: 'SHORT_GO',       label: '🔴 SHORT GO'      },
  { key: 'REJECT',         label: '❌ Reject'        },
]

const ROLE_COLORS = {
  BULL_A:          'bg-green-800/60 text-green-200 border-green-600/50',
  BULL_B:          'bg-green-900/50 text-green-300 border-green-700/40',
  PULLBACK_READY_A:'bg-blue-800/60 text-blue-200 border-blue-600/50',
  PULLBACK_READY_B:'bg-blue-900/50 text-blue-300 border-blue-700/40',
  PULLBACK_WATCH:  'bg-cyan-900/50 text-cyan-300 border-cyan-700/40',
  BULL_WATCH:      'bg-teal-900/50 text-teal-300 border-teal-700/40',
  SHORT_WATCH:     'bg-red-900/50 text-red-300 border-red-700/40',
  SHORT_GO:        'bg-red-700/70 text-red-100 border-red-500/60',
  REJECT:          'bg-gray-800/60 text-gray-400 border-gray-600/40',
  NO_EDGE:         'bg-gray-900/40 text-gray-600 border-gray-700/30',
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
  PULLBACK_ENTRY_READY:      'text-blue-400 font-semibold',
  WATCH_PULLBACK:            'text-blue-300',
  WAIT_FOR_CONFIRMATION:     'text-gray-400',
  WAIT_FOR_BREAKDOWN:        'text-orange-400',
  SHORT_TRIGGER:             'text-red-400 font-semibold',
  IGNORE:                    'text-gray-600',
}

function RoleBadge({ role }) {
  const color = ROLE_COLORS[role] || 'bg-gray-800 text-gray-400'
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-xs font-bold border ${color}`}>
      {role?.replace('_', ' ') || '—'}
    </span>
  )
}

function ScoreBar({ score }) {
  const pct = Math.min(100, Math.max(0, ((score + 40) / 140) * 100))
  const color = score >= 70 ? 'bg-green-500' : score >= 55 ? 'bg-teal-500' : score >= 35 ? 'bg-yellow-500' : score >= 0 ? 'bg-gray-500' : 'bg-red-500'
  return (
    <div className="flex items-center gap-1.5">
      <div className="w-16 h-1.5 bg-gray-800 rounded-full overflow-hidden">
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

function ReasonTooltip({ codes, explanation }) {
  const [show, setShow] = useState(false)
  if (!codes?.length && !explanation) return null
  return (
    <div className="relative inline-block">
      <button
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        className="text-gray-500 hover:text-gray-300 text-xs"
      >
        ℹ
      </button>
      {show && (
        <div className="absolute z-50 left-4 top-0 w-64 bg-gray-900 border border-gray-700 rounded p-2 text-xs text-gray-300 shadow-xl">
          <div className="mb-1 text-gray-400">{explanation}</div>
          {codes?.map((c, i) => <div key={i} className="text-gray-500 font-mono">{c}</div>)}
        </div>
      )}
    </div>
  )
}

export default function TZIntelligencePanel({ onSelectTicker }) {
  const [universe, setUniverse]       = useState('sp500')
  const [nasdaqBatch, setNasdaqBatch] = useState('a_m')
  const [tf, setTf]                   = useState('1d')
  const [roleFilter, setRoleFilter]   = useState('all')
  const [minPrice, setMinPrice]       = useState('')
  const [maxPrice, setMaxPrice]       = useState('')
  const [minVolume, setMinVolume]     = useState('')

  const [results, setResults]   = useState([])
  const [total, setTotal]       = useState(0)
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState(null)

  async function handleScan() {
    setLoading(true)
    setError(null)
    try {
      const qs = new URLSearchParams({ universe, tf, role_filter: roleFilter })
      if (universe === 'nasdaq') qs.set('nasdaq_batch', nasdaqBatch)
      if (minPrice)  qs.set('min_price', minPrice)
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

  return (
    <div className="bg-gray-950 text-gray-100 p-3 flex flex-col gap-3">
      {/* Header */}
      <div className="flex items-center gap-2">
        <span className="text-lg font-bold text-white">🧠 TZ Signal Intelligence</span>
        <span className="text-xs text-gray-500">Matrix-based role classifier — BULL_A / SHORT_GO / REJECT / …</span>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap gap-2 items-end">
        {/* Universe */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Universe</label>
          <select
            value={universe}
            onChange={e => setUniverse(e.target.value)}
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700"
          >
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

        {/* Timeframe */}
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

        {/* Role filter */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Role</label>
          <select
            value={roleFilter}
            onChange={e => setRoleFilter(e.target.value)}
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700"
          >
            {ROLES.map(r => <option key={r.key} value={r.key}>{r.label}</option>)}
          </select>
        </div>

        {/* Price filters */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Min $</label>
          <input type="number" value={minPrice} onChange={e => setMinPrice(e.target.value)}
            placeholder="0" className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700 w-20" />
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

      {/* Info note */}
      <div className="text-xs text-gray-600 px-1">
        Requires TZ/WLNBB stock_stat CSV — run <span className="text-gray-400">📡 TZ/WLNBB → Generate Stock Stat</span> first.
      </div>

      {/* Error */}
      {error && (
        <div className="p-2 bg-red-900/30 border border-red-700 rounded text-red-300 text-xs">{error}</div>
      )}

      {/* Results count */}
      {results.length > 0 && (
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-500">
            Showing {results.length} of {total} classified tickers
          </span>
          <button
            onClick={() => exportCSV(results)}
            className="px-2 py-1 bg-gray-800 hover:bg-gray-700 text-gray-300 text-xs rounded border border-gray-700 transition-colors"
          >
            ⬇ CSV
          </button>
        </div>
      )}

      {/* Results table */}
      {results.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="text-gray-400 border-b border-gray-800">
                <th className="text-left p-1 font-medium">Ticker</th>
                <th className="text-left p-1 font-medium">Date</th>
                <th className="text-right p-1 font-medium">Close</th>
                <th className="text-left p-1 font-medium">Signal</th>
                <th className="text-left p-1 font-medium">Composite</th>
                <th className="text-left p-1 font-medium">Seq4</th>
                <th className="text-left p-1 font-medium">Role</th>
                <th className="text-left p-1 font-medium">Score</th>
                <th className="text-left p-1 font-medium">Quality</th>
                <th className="text-left p-1 font-medium">Action</th>
                <th className="text-left p-1 font-medium">Vol</th>
                <th className="text-left p-1 font-medium">EMA</th>
                <th className="text-left p-1 font-medium"></th>
              </tr>
            </thead>
            <tbody>
              {results.map((row, i) => (
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
                    <span className={`font-mono text-xs ${row.final_signal?.startsWith('T') ? 'text-blue-300' : row.final_signal?.startsWith('Z') ? 'text-red-300' : 'text-yellow-300'}`}>
                      {row.final_signal || '—'}
                    </span>
                  </td>
                  <td className="p-1 text-gray-400 font-mono text-xs">{row.composite_pattern || '—'}</td>
                  <td className="p-1 text-gray-500 font-mono text-xs" title={row.seq4}>
                    {row.seq4 ? row.seq4.split('|').slice(-2).join('|') : '—'}
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
                    <span className={`text-xs ${row.vol_bucket === 'VB' ? 'text-red-300' : row.vol_bucket === 'B' ? 'text-orange-300' : 'text-gray-500'}`}>
                      {row.vol_bucket || ''}
                    </span>
                  </td>
                  <td className="p-1">
                    <EMADots above20={row.above_ema20} above50={row.above_ema50} above89={row.above_ema89} />
                  </td>
                  <td className="p-1">
                    <ReasonTooltip codes={row.reason_codes} explanation={row.explanation} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && !error && results.length === 0 && (
        <div className="text-gray-600 text-xs py-6 text-center">
          No results. Run "🧠 Classify" to start — needs TZ/WLNBB stock_stat CSV.
        </div>
      )}
    </div>
  )
}
