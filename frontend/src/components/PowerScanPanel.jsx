import { useState, useEffect, useCallback } from 'react'
import { api } from '../api'

// ── 260323 signal badge colours ───────────────────────────────────────────────
const COMBO_STYLE = {
  ROCKET: 'text-red-400 bg-red-900/60',
  BUY:    'text-lime-400 bg-lime-900/60',
  '3G':   'text-cyan-300 bg-cyan-900/60',
  'BB↑':  'text-orange-400 bg-orange-900/50',
  'ATR↑': 'text-green-400 bg-green-900/50',
  RTV:    'text-blue-400 bg-blue-900/50',
  P3:     'text-yellow-300 bg-yellow-900/50',
  P2:     'text-orange-300 bg-orange-900/40',
  P50:    'text-pink-400 bg-pink-900/50',
  P89:    'text-purple-400 bg-purple-900/50',
  'HILO↑':'text-green-300 bg-green-900/40',
  'HILO↓':'text-red-300 bg-red-900/40',
  '↑BIAS':'text-emerald-300 bg-emerald-900/50',
  '↓BIAS':'text-rose-400 bg-rose-900/50',
  CONS:   'text-yellow-200 bg-yellow-900/30',
  SVS:    'text-orange-200 bg-orange-900/30',
  UM:     'text-teal-300 bg-teal-900/40',
  CONSO:  'text-gray-300 bg-gray-800',
}

function ComboBadge({ label }) {
  const cls = COMBO_STYLE[label] || 'text-gray-400 bg-gray-800'
  return (
    <span className={`px-1.5 py-0.5 rounded text-xs font-mono font-semibold ${cls}`}>
      {label}
    </span>
  )
}

function TZBadge({ sig }) {
  if (!sig) return null
  return (
    <span className="px-1.5 py-0.5 rounded text-xs font-mono font-semibold text-green-300 bg-green-900/50">
      {sig}
    </span>
  )
}

function LBadge({ sig }) {
  if (!sig) return null
  return (
    <span className="px-1.5 py-0.5 rounded text-xs font-mono font-semibold text-sky-300 bg-sky-900/40">
      {sig}
    </span>
  )
}

function ScoreBar({ value, max = 20 }) {
  const pct  = Math.min(100, Math.round((value / max) * 100))
  const color = value >= 14 ? 'bg-yellow-400'
              : value >= 9  ? 'bg-green-500'
              : 'bg-blue-500'
  return (
    <div className="flex items-center gap-1.5">
      <div className="w-16 h-1.5 bg-gray-700 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <span className={`text-xs font-bold tabular-nums
        ${value >= 14 ? 'text-yellow-400' : value >= 9 ? 'text-green-400' : 'text-blue-400'}`}>
        {value}
      </span>
    </div>
  )
}

export default function PowerScanPanel({ tf, onSelectTicker }) {
  const [results,  setResults]  = useState([])
  const [lastScan, setLastScan] = useState(null)
  const [loading,  setLoading]  = useState(false)
  const [scanning, setScanning] = useState(false)
  const [error,    setError]    = useState(null)

  const load = useCallback(() => {
    setLoading(true)
    setError(null)
    api.powerScan(300)
      .then(d => { setResults(d.results || []); setLastScan(d.last_scan) })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => { load() }, [load])
  useEffect(() => {
    const id = setInterval(load, 60_000)
    return () => clearInterval(id)
  }, [load])

  const scan = () => {
    setScanning(true)
    api.powerScanTrigger(tf)
      .then(() => setTimeout(() => { setScanning(false); load() }, 3000))
      .catch(e => { setError(e.message); setScanning(false) })
  }

  const fmtTime = iso => {
    if (!iso) return null
    try { return new Date(iso).toLocaleString() } catch { return iso }
  }

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 flex flex-col h-full">

      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <div className="flex items-center gap-3">
          <span className="font-semibold text-sm text-white">Power Scan</span>
          <span className="text-xs text-gray-500">
            260323 Combo × T/Z Signal × WLNBB
          </span>
          {lastScan && (
            <span className="text-xs text-gray-600">Last: {fmtTime(lastScan)}</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500">{results.length} tickers</span>
          <button
            onClick={scan}
            disabled={scanning}
            className="text-xs px-3 py-1 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-white"
          >
            {scanning ? 'Scanning…' : 'Scan Now'}
          </button>
        </div>
      </div>

      {/* ── Score legend ───────────────────────────────────────────────────── */}
      <div className="flex items-center gap-4 px-4 py-1.5 border-b border-gray-800 text-xs text-gray-500">
        <span>Power score = combo×2 + T/Z weight + WLNBB bull</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-yellow-400 inline-block"/> ≥14 strong</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-green-500 inline-block"/> ≥9 good</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-blue-500 inline-block"/> &lt;9 weak</span>
      </div>

      {error && <div className="px-4 py-2 text-xs text-red-400">{error}</div>}

      {/* ── Table ──────────────────────────────────────────────────────────── */}
      <div className="overflow-auto flex-1">
        {loading ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">Loading…</div>
        ) : results.length === 0 ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">
            No results — press <span className="text-white font-semibold">Scan Now</span> first.
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-800 sticky top-0 bg-gray-900">
                <th className="text-left px-3 py-2 w-20">Ticker</th>
                <th className="text-left px-2 py-2">260323</th>
                <th className="text-left px-2 py-2 w-16">T/Z</th>
                <th className="text-left px-2 py-2 w-14">L</th>
                <th className="text-left px-2 py-2 w-12">WLNBB</th>
                <th className="text-left px-2 py-2 w-24">Score</th>
                <th className="text-right px-2 py-2 w-20">Price</th>
                <th className="text-right px-2 py-2 w-16">Chg%</th>
              </tr>
            </thead>
            <tbody>
              {results.map((row, i) => {
                const comboLabels = row.combo_signals
                  ? row.combo_signals.split(',').filter(Boolean)
                  : []
                return (
                  <tr
                    key={i}
                    onClick={() => onSelectTicker?.(row.ticker)}
                    className="border-b border-gray-800/40 cursor-pointer hover:bg-gray-800/50"
                  >
                    <td className="px-3 py-2 font-semibold text-white">{row.ticker}</td>
                    <td className="px-2 py-1.5">
                      <div className="flex flex-wrap gap-1">
                        {comboLabels.map(lbl => <ComboBadge key={lbl} label={lbl} />)}
                      </div>
                    </td>
                    <td className="px-2 py-1.5">
                      <TZBadge sig={row.tz_sig} />
                    </td>
                    <td className="px-2 py-1.5">
                      <LBadge sig={row.l_signal} />
                    </td>
                    <td className="px-2 py-2 text-center text-gray-300">
                      {row.wlnbb_bull > 0 && (
                        <span className={`font-semibold ${
                          row.wlnbb_bull >= 7 ? 'text-green-300' :
                          row.wlnbb_bull >= 4 ? 'text-blue-300' : 'text-gray-400'
                        }`}>{row.wlnbb_bull}</span>
                      )}
                    </td>
                    <td className="px-2 py-2">
                      <ScoreBar value={row.power_score} />
                    </td>
                    <td className="text-right px-2 py-2 text-gray-200 font-mono">
                      ${row.last_price?.toFixed(2)}
                    </td>
                    <td className={`text-right px-2 py-2 font-medium
                      ${row.change_pct >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                      {row.change_pct >= 0 ? '+' : ''}{row.change_pct?.toFixed(2)}%
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
