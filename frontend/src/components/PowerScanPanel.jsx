import { useState, useEffect, useCallback, useMemo } from 'react'
import { api } from '../api'
import { exportToTV } from '../utils/exportTickers'

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

const TZ_SIGNALS = ['T4', 'T6', 'T1G', 'T2G', 'T1', 'T2', 'T9', 'T10', 'T3', 'T11', 'T5']

const L_SIGNALS = [
  { key: 'fri34',     label: 'FRI34',   color: 'text-cyan-300' },
  { key: 'l34',       label: 'L34',     color: 'text-blue-300' },
  { key: 'l43',       label: 'L43',     color: 'text-teal-300' },
  { key: 'l64',       label: 'L64',     color: 'text-orange-400' },
  { key: 'l22',       label: 'L22',     color: 'text-red-400' },
  { key: 'cci_ready', label: 'CCI_RDY', color: 'text-violet-300' },
  { key: 'blue',      label: 'BLUE',    color: 'text-sky-300' },
  { key: 'bo_up',     label: 'BO_UP',   color: 'text-lime-300' },
  { key: 'bx_up',     label: 'BX_UP',   color: 'text-lime-400' },
  { key: 'pre_pump',  label: 'PRE_PMP', color: 'text-purple-300' },
  { key: 'fuchsia_rh',label: 'RH',      color: 'text-fuchsia-400' },
  { key: 'fuchsia_rl',label: 'RL',      color: 'text-fuchsia-300' },
  { key: 'sq',        label: 'SQ',      color: 'text-cyan-400' },
  { key: 'ns',        label: 'NS',      color: 'text-lime-400' },
  { key: 'nd',        label: 'ND',      color: 'text-red-400' },
  { key: 'sig3_up',   label: '3↑',      color: 'text-blue-300' },
  { key: 'sig3_dn',   label: '3↓',      color: 'text-orange-400' },
  { key: 'wick_bull', label: 'WICK↑',   color: 'text-emerald-300' },
  { key: 'wick_bear', label: 'WICK↓',   color: 'text-rose-400' },
  { key: 'cisd_seq',  label: 'C++--',   color: 'text-lime-300' },
  { key: 'cisd_ppm',  label: 'C++-',    color: 'text-green-300' },
  { key: 'cisd_mpm',  label: 'C-+-',    color: 'text-red-300' },
  { key: 'cisd_pmm',  label: 'C+--',    color: 'text-fuchsia-300' },
]

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

function ExtraBadges({ row }) {
  const active = L_SIGNALS.filter(s => row[s.key])
  if (active.length === 0) return <span className="text-gray-600">—</span>
  return (
    <div className="flex flex-wrap gap-0.5">
      {active.map(s => (
        <span key={s.key} className={`text-xs font-mono ${s.color}`}>{s.label}</span>
      ))}
    </div>
  )
}

function ScoreBar({ value, max = 20 }) {
  const pct   = Math.min(100, Math.round((value / max) * 100))
  const color = value >= 14 ? 'bg-yellow-400' : value >= 9 ? 'bg-green-500' : 'bg-blue-500'
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
  const [allResults, setAllResults] = useState([])
  const [lastScan,   setLastScan]   = useState(null)
  const [loading,    setLoading]    = useState(false)
  const [scanning,   setScanning]   = useState(false)
  const [error,      setError]      = useState(null)
  const [selectedTZ, setSelectedTZ] = useState(new Set())  // OR filter
  const [selectedL,  setSelectedL]  = useState(new Set())  // AND filter

  const load = useCallback(() => {
    setLoading(true)
    setError(null)
    api.powerScan(300)
      .then(d => { setAllResults(d.results || []); setLastScan(d.last_scan) })
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

  const toggle = (set, setFn, key) => {
    setFn(prev => {
      const next = new Set(prev)
      next.has(key) ? next.delete(key) : next.add(key)
      return next
    })
  }

  const results = useMemo(() => {
    return allResults.filter(row => {
      if (selectedTZ.size > 0 && !selectedTZ.has(row.tz_sig)) return false
      if (selectedL.size  > 0 && ![...selectedL].every(k => row[k])) return false
      return true
    })
  }, [allResults, selectedTZ, selectedL])

  const anyFilter = selectedTZ.size > 0 || selectedL.size > 0

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
          <span className="text-xs text-gray-500">260323 × T/Z × WLNBB</span>
          {lastScan && <span className="text-xs text-gray-600">Last: {fmtTime(lastScan)}</span>}
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500">{results.length} tickers</span>
          {anyFilter && (
            <button
              onClick={() => { setSelectedTZ(new Set()); setSelectedL(new Set()) }}
              className="text-xs px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-yellow-400"
            >Clear filters</button>
          )}
          {results.length > 0 && (
            <button
              onClick={() => exportToTV(results.map(r => r.ticker), 'power_scan.txt')}
              className="text-xs px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
              title="Export for TradingView"
            >Export TV</button>
          )}
          <button
            onClick={scan} disabled={scanning}
            className="text-xs px-3 py-1 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-white"
          >
            {scanning ? 'Scanning…' : 'Scan Now'}
          </button>
        </div>
      </div>

      {/* ── T/Z filter (OR) ──────────────────────────────────────────────── */}
      <div className="flex items-center gap-1 px-3 py-1.5 border-b border-gray-800/60 flex-wrap">
        <span className="text-[10px] text-gray-600 uppercase tracking-wide w-12 shrink-0">T/Z</span>
        <button
          onClick={() => setSelectedTZ(new Set())}
          className={`text-xs px-2 py-0.5 rounded transition-colors
            ${selectedTZ.size === 0 ? 'bg-gray-700 text-white font-semibold' : 'text-gray-500 hover:text-gray-300'}`}
        >All</button>
        {TZ_SIGNALS.map(sig => (
          <button key={sig}
            onClick={() => toggle(selectedTZ, setSelectedTZ, sig)}
            className={`text-xs px-2 py-0.5 rounded font-mono transition-colors
              ${selectedTZ.has(sig)
                ? 'bg-green-900/60 text-green-300 font-semibold ring-1 ring-green-400'
                : 'text-gray-500 hover:text-gray-300'}`}
          >{sig}</button>
        ))}
        {selectedTZ.size >= 2 && (
          <span className="ml-1 text-xs text-green-400 font-semibold bg-green-900/30 px-2 py-0.5 rounded">
            OR ×{selectedTZ.size}
          </span>
        )}
      </div>

      {/* ── L-Sig filter (AND) ───────────────────────────────────────────── */}
      <div className="flex items-center gap-1 px-3 py-1.5 border-b border-gray-800 flex-wrap">
        <span className="text-[10px] text-gray-600 uppercase tracking-wide w-12 shrink-0">L-Sig</span>
        <button
          onClick={() => setSelectedL(new Set())}
          className={`text-xs px-2 py-0.5 rounded transition-colors
            ${selectedL.size === 0 ? 'bg-gray-700 text-white font-semibold' : 'text-gray-500 hover:text-gray-300'}`}
        >All</button>
        {L_SIGNALS.map(s => (
          <button key={s.key}
            onClick={() => toggle(selectedL, setSelectedL, s.key)}
            className={`text-xs px-2 py-0.5 rounded font-mono transition-colors
              ${selectedL.has(s.key)
                ? `bg-gray-700 ${s.color} font-semibold ring-1 ring-current`
                : 'text-gray-500 hover:text-gray-300'}`}
          >{s.label}</button>
        ))}
        {selectedL.size >= 2 && (
          <span className="ml-1 text-xs text-sky-400 font-semibold bg-sky-900/30 px-2 py-0.5 rounded">
            AND ×{selectedL.size}
          </span>
        )}
      </div>

      {/* ── Score legend ───────────────────────────────────────────────────── */}
      <div className="flex items-center gap-4 px-4 py-1 border-b border-gray-800 text-xs text-gray-500">
        <span>Power = combo×2 + T/Z + WLNBB</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-yellow-400 inline-block"/> ≥14</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-green-500 inline-block"/> ≥9</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-blue-500 inline-block"/> &lt;9</span>
      </div>

      {error && <div className="px-4 py-2 text-xs text-red-400">{error}</div>}

      {/* ── Table ──────────────────────────────────────────────────────────── */}
      <div className="overflow-auto flex-1">
        {loading ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">Loading…</div>
        ) : results.length === 0 ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">
            {anyFilter
              ? 'No tickers match the current filters.'
              : 'No results — press Scan Now first.'}
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-800 sticky top-0 bg-gray-900">
                <th className="text-left px-3 py-2 w-20">Ticker</th>
                <th className="text-left px-2 py-2">260323</th>
                <th className="text-left px-2 py-2 w-14">T/Z</th>
                <th className="text-left px-2 py-2">Signals</th>
                <th className="text-left px-2 py-2 w-10">WL</th>
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
                  <tr key={i}
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
                      <ExtraBadges row={row} />
                    </td>
                    <td className="px-2 py-2 text-center">
                      {row.wlnbb_bull > 0 && (
                        <span className={`font-semibold text-xs ${
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
