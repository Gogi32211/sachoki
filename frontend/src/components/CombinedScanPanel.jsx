import { useState, useEffect, useCallback } from 'react'
import { api } from '../api'
import { exportToTV } from '../utils/exportTickers'

const TABS = [
  { id: 'bull',   label: 'Bull ≥4',    color: 'text-green-400' },
  { id: 'strong', label: 'Strong ≥6',  color: 'text-emerald-300' },
  { id: 'fire',   label: 'Fire ≥8',    color: 'text-yellow-400' },
  { id: 'bear',   label: 'Bear ≥3',    color: 'text-red-400' },
  { id: 'all',    label: 'All',        color: 'text-gray-400' },
]

// Volume bucket colors
const BUCKET_COLORS = {
  W:  { bg: '#c3c0d3', text: '#1a1a2e' },
  L:  { bg: '#0099ff', text: '#fff' },
  N:  { bg: '#ffd000', text: '#1a1a1a' },
  B:  { bg: '#e48100', text: '#fff' },
  VB: { bg: '#b02020', text: '#fff' },
}

const MIN_SCORES = [0, 2, 4, 6, 8]

function ScoreBadge({ score, isBull }) {
  const base = isBull ? 'bg-green-900/60 text-green-300' : 'bg-red-900/60 text-red-300'
  const highlight = score >= 8 ? 'ring-1 ring-yellow-400' : score >= 6 ? 'ring-1 ring-green-400' : ''
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-bold ${base} ${highlight}`}>
      {score}
    </span>
  )
}

function SigBadge({ sig_id, sig_name }) {
  const bull = sig_id >= 1 && sig_id <= 11
  const bear = sig_id >= 12 && sig_id <= 25
  if (!bull && !bear) return <span className="text-gray-600">—</span>
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-xs font-mono font-semibold
      ${bull ? 'bg-green-900/50 text-green-300' : 'bg-red-900/50 text-red-300'}`}>
      {sig_name}
    </span>
  )
}

function LSigBadge({ label }) {
  if (!label) return <span className="text-gray-600">—</span>
  const colors = {
    FRI34: 'text-cyan-300', L34: 'text-blue-300', L43: 'text-teal-300',
    L64: 'text-orange-400', L22: 'text-red-400',
    CCI_READY: 'text-violet-300', BLUE: 'text-sky-300',
    BO_UP: 'text-lime-300', BX_UP: 'text-lime-400',
    BO_DN: 'text-rose-400', BX_DN: 'text-rose-500',
    PRE_PUMP: 'text-purple-300',
  }
  return (
    <span className={`font-mono text-xs ${colors[label] || 'text-gray-400'}`}>
      {label}
    </span>
  )
}

function BucketCell({ bucket }) {
  if (!bucket) return <span className="text-gray-600">—</span>
  const c = BUCKET_COLORS[bucket]
  if (!c) return <span className="text-xs font-mono text-gray-400">{bucket}</span>
  return (
    <span
      className="text-xs font-bold px-1 py-0.5 rounded font-mono"
      style={{ backgroundColor: c.bg, color: c.text }}
    >
      {bucket}
    </span>
  )
}

function CandleDirCell({ dir }) {
  if (!dir) return null
  const cfg = { U: '▲', D: '▼', O: '●' }
  const cls  = { U: 'text-green-400', D: 'text-red-400', O: 'text-gray-500' }
  return <span className={`text-xs font-bold ${cls[dir] || 'text-gray-500'}`}>{cfg[dir] || dir}</span>
}

function rowBg(row) {
  if (row.bull_score >= 8) return 'bg-yellow-950/30'
  if (row.bull_score >= 6) return 'bg-green-950/30'
  if (row.bull_score >= 4) return 'bg-green-950/10'
  if (row.bear_score >= 3) return 'bg-red-950/20'
  return ''
}

export default function CombinedScanPanel({ tf, onSelectTicker }) {
  const [tab, setTab]         = useState('bull')
  const [minScore, setMinScore] = useState(4)
  const [results, setResults] = useState([])
  const [lastScan, setLastScan] = useState(null)
  const [loading, setLoading] = useState(false)
  const [scanning, setScanning] = useState(false)
  const [error, setError]     = useState(null)

  const load = useCallback(() => {
    setLoading(true)
    setError(null)
    api.combinedScan(tf, minScore, tab)
      .then(d => {
        setResults(d.results || [])
        setLastScan(d.last_scan)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [tf, minScore, tab])

  useEffect(() => { load() }, [load])

  // Auto-refresh every 60s
  useEffect(() => {
    const id = setInterval(load, 60_000)
    return () => clearInterval(id)
  }, [load])

  const scan = () => {
    setScanning(true)
    api.scanTrigger(tf)
      .then(() => setTimeout(() => { setScanning(false); load() }, 3000))
      .catch(e => { setError(e.message); setScanning(false) })
  }

  const fmtTime = (iso) => {
    if (!iso) return null
    try { return new Date(iso).toLocaleString() } catch { return iso }
  }

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <div className="flex items-center gap-3">
          <span className="font-semibold text-sm text-white">Combined Scan</span>
          {lastScan && (
            <span className="text-xs text-gray-500">
              Last: {fmtTime(lastScan)}
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {results.length > 0 && (
            <button
              onClick={() => exportToTV(results.map(r => r.ticker), 'combined_scan.txt')}
              className="text-xs px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
              title="Export tickers for TradingView watchlist"
            >
              Export TV
            </button>
          )}
          <button
            onClick={scan}
            disabled={scanning}
            className="text-xs px-3 py-1 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-white"
          >
            {scanning ? 'Scanning…' : 'Scan Now'}
          </button>
        </div>
      </div>

      {/* Sub-tabs + min score */}
      <div className="flex items-center gap-1 px-4 py-2 border-b border-gray-800 flex-wrap">
        {TABS.map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`text-xs px-2 py-1 rounded transition-colors
              ${tab === t.id
                ? `bg-gray-700 ${t.color} font-semibold`
                : 'text-gray-500 hover:text-gray-300'}`}
          >
            {t.label}
          </button>
        ))}
        <div className="ml-auto flex items-center gap-1">
          <span className="text-xs text-gray-500">Min score:</span>
          <select
            value={minScore}
            onChange={e => setMinScore(Number(e.target.value))}
            className="text-xs bg-gray-800 text-gray-300 border border-gray-700 rounded px-2 py-0.5"
          >
            {MIN_SCORES.map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
      </div>

      {error && <div className="px-4 py-2 text-xs text-red-400">{error}</div>}

      {/* Table */}
      <div className="overflow-auto flex-1">
        {loading ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">Loading…</div>
        ) : results.length === 0 ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">
            No results — trigger a scan or lower the min score filter.
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-800 sticky top-0 bg-gray-900">
                <th className="text-left px-3 py-2">Ticker</th>
                <th className="text-center px-2 py-2">Score</th>
                <th className="text-center px-2 py-2">T/Z</th>
                <th className="text-center px-2 py-2">L-Sig</th>
                <th className="text-center px-1 py-2 hidden md:table-cell">Bkt</th>
                <th className="text-center px-1 py-2 hidden md:table-cell">Dir</th>
                <th className="text-left px-2 py-2 hidden lg:table-cell">3-Bar</th>
                <th className="text-right px-2 py-2">Price</th>
                <th className="text-right px-2 py-2">Chg%</th>
              </tr>
            </thead>
            <tbody>
              {results.map((row, i) => (
                <tr
                  key={i}
                  onClick={() => onSelectTicker?.(row.ticker)}
                  className={`border-b border-gray-800/40 cursor-pointer hover:bg-gray-800/50 ${rowBg(row)}`}
                >
                  <td className="px-3 py-2 font-semibold text-white">{row.ticker}</td>
                  <td className="text-center px-2 py-2">
                    <ScoreBadge
                      score={tab === 'bear' ? row.bear_score : row.bull_score}
                      isBull={tab !== 'bear'}
                    />
                  </td>
                  <td className="text-center px-2 py-2">
                    <SigBadge sig_id={row.sig_id} sig_name={row.sig_name} />
                  </td>
                  <td className="text-center px-2 py-2">
                    <LSigBadge label={row.l_signal} />
                  </td>
                  <td className="text-center px-1 py-2 hidden md:table-cell">
                    <BucketCell bucket={row.vol_bucket} />
                  </td>
                  <td className="text-center px-1 py-2 hidden md:table-cell">
                    <CandleDirCell dir={row.candle_dir} />
                  </td>
                  <td className="px-2 py-2 text-gray-400 font-mono hidden lg:table-cell max-w-[140px] truncate">
                    {row.pattern_3bar}
                  </td>
                  <td className="text-right px-2 py-2 text-gray-200">
                    ${row.last_price?.toFixed(2)}
                  </td>
                  <td className={`text-right px-2 py-2 font-medium
                    ${row.change_pct >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {row.change_pct >= 0 ? '+' : ''}{row.change_pct?.toFixed(2)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
