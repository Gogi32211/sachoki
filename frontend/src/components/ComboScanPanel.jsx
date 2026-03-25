import { useState, useEffect, useCallback } from 'react'
import { api } from '../api'

// ── Signal definitions — matches Pine Script 260323 ───────────────────────────
const SIGNALS = [
  { key: 'rocket',    label: 'ROCKET', color: 'text-red-400',      bg: 'bg-red-900/60' },
  { key: 'buy_2809',  label: 'BUY',    color: 'text-lime-400',     bg: 'bg-lime-900/60' },
  { key: 'sig3g',     label: '3G',     color: 'text-cyan-300',     bg: 'bg-cyan-900/60' },
  { key: 'bb_brk',    label: 'BB↑',    color: 'text-orange-400',   bg: 'bg-orange-900/50' },
  { key: 'atr_brk',   label: 'ATR↑',   color: 'text-green-400',    bg: 'bg-green-900/50' },
  { key: 'rtv',       label: 'RTV',    color: 'text-blue-400',     bg: 'bg-blue-900/50' },
  { key: 'preup3',    label: 'P3',     color: 'text-yellow-300',   bg: 'bg-yellow-900/50' },
  { key: 'preup2',    label: 'P2',     color: 'text-orange-300',   bg: 'bg-orange-900/40' },
  { key: 'preup50',   label: 'P50',    color: 'text-pink-400',     bg: 'bg-pink-900/50' },
  { key: 'preup89',   label: 'P89',    color: 'text-purple-400',   bg: 'bg-purple-900/50' },
  { key: 'hilo_buy',  label: 'HILO↑',  color: 'text-green-300',    bg: 'bg-green-900/40' },
  { key: 'hilo_sell', label: 'HILO↓',  color: 'text-red-300',      bg: 'bg-red-900/40' },
  { key: 'bias_up',   label: '↑BIAS',  color: 'text-emerald-300',  bg: 'bg-emerald-900/50' },
  { key: 'bias_down', label: '↓BIAS',  color: 'text-rose-400',     bg: 'bg-rose-900/50' },
  { key: 'cons_atr',  label: 'CONS',   color: 'text-yellow-200',   bg: 'bg-yellow-900/30' },
  { key: 'svs_2809',  label: 'SVS',    color: 'text-orange-200',   bg: 'bg-orange-900/30' },
  { key: 'um_2809',   label: 'UM',     color: 'text-teal-300',     bg: 'bg-teal-900/40' },
]

const LABEL_TO_SIG = Object.fromEntries(SIGNALS.map(s => [s.label, s]))

function SignalBadge({ label }) {
  const sig = LABEL_TO_SIG[label]
  if (!sig) return (
    <span className="px-1.5 py-0.5 rounded text-xs font-mono text-gray-400 bg-gray-800">
      {label}
    </span>
  )
  return (
    <span className={`px-1.5 py-0.5 rounded text-xs font-mono font-semibold ${sig.color} ${sig.bg}`}>
      {label}
    </span>
  )
}

function rowBg(signals) {
  const labels = signals ? signals.split(',') : []
  if (labels.includes('ROCKET'))   return 'bg-red-950/20'
  if (labels.includes('BUY'))      return 'bg-lime-950/20'
  if (labels.includes('3G'))       return 'bg-cyan-950/20'
  if (labels.includes('P3'))       return 'bg-yellow-950/20'
  return ''
}

export default function ComboScanPanel({ tf, onSelectTicker }) {
  const [filterKey, setFilterKey] = useState('all')
  const [results,   setResults]   = useState([])
  const [lastScan,  setLastScan]  = useState(null)
  const [loading,   setLoading]   = useState(false)
  const [scanning,  setScanning]  = useState(false)
  const [error,     setError]     = useState(null)

  const load = useCallback(() => {
    setLoading(true)
    setError(null)
    api.comboScan(filterKey, 200)
      .then(d => {
        setResults(d.results || [])
        setLastScan(d.last_scan)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [filterKey])

  useEffect(() => { load() }, [load])

  useEffect(() => {
    const id = setInterval(load, 60_000)
    return () => clearInterval(id)
  }, [load])

  const scan = () => {
    setScanning(true)
    api.comboScanTrigger(tf)
      .then(() => setTimeout(() => { setScanning(false); load() }, 3000))
      .catch(e => { setError(e.message); setScanning(false) })
  }

  const fmtTime = (iso) => {
    if (!iso) return null
    try { return new Date(iso).toLocaleString() } catch { return iso }
  }

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 flex flex-col h-full">

      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <div className="flex items-center gap-3">
          <span className="font-semibold text-sm text-white">260323 Combo Scan</span>
          {lastScan && (
            <span className="text-xs text-gray-500">Last: {fmtTime(lastScan)}</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={scan}
            disabled={scanning}
            className="text-xs px-3 py-1 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-white"
          >
            {scanning ? 'Scanning…' : 'Scan Now'}
          </button>
        </div>
      </div>

      {/* ── Signal filter buttons ───────────────────────────────────────────── */}
      <div className="flex items-center gap-1 px-3 py-2 border-b border-gray-800 flex-wrap">
        <button
          onClick={() => setFilterKey('all')}
          className={`text-xs px-2 py-1 rounded transition-colors
            ${filterKey === 'all'
              ? 'bg-gray-600 text-white font-semibold'
              : 'text-gray-500 hover:text-gray-300'}`}
        >
          All
        </button>
        {SIGNALS.map(s => (
          <button
            key={s.key}
            onClick={() => setFilterKey(filterKey === s.key ? 'all' : s.key)}
            className={`text-xs px-2 py-1 rounded font-mono transition-colors
              ${filterKey === s.key
                ? `${s.bg} ${s.color} font-semibold ring-1 ring-current`
                : 'text-gray-500 hover:text-gray-300'}`}
          >
            {s.label}
          </button>
        ))}
        <span className="ml-auto text-xs text-gray-500">{results.length} tickers</span>
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
                <th className="text-left px-2 py-2">Signals</th>
                <th className="text-right px-2 py-2 w-20">Price</th>
                <th className="text-right px-2 py-2 w-16">Chg%</th>
              </tr>
            </thead>
            <tbody>
              {results.map((row, i) => {
                const labels = row.signals ? row.signals.split(',').filter(Boolean) : []
                return (
                  <tr
                    key={i}
                    onClick={() => onSelectTicker?.(row.ticker)}
                    className={`border-b border-gray-800/40 cursor-pointer hover:bg-gray-800/50 ${rowBg(row.signals)}`}
                  >
                    <td className="px-3 py-2 font-semibold text-white">{row.ticker}</td>
                    <td className="px-2 py-1.5">
                      <div className="flex flex-wrap gap-1">
                        {labels.map(lbl => <SignalBadge key={lbl} label={lbl} />)}
                      </div>
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

      {/* ── Legend ─────────────────────────────────────────────────────────── */}
      <div className="px-4 py-2 border-t border-gray-800 flex flex-wrap gap-x-4 gap-y-1">
        <span className="text-xs text-gray-500 font-semibold">Legend:</span>
        {[
          ['ROCKET', 'BUY + strong candle + vol×2 + PSAR'],
          ['BUY',    '2809: upmove + cons + breakout'],
          ['3G',     'Gap above EMA9+20+50'],
          ['BB↑',    'Close > BB upper + vol spike + RSI>55'],
          ['ATR↑',   'ATR consolidation + range breakout'],
          ['RTV',    'RSI(2) reversal + Williams VIX Fix'],
          ['P3',     'Candle crosses EMA9+20+50'],
          ['CONS',   'ATR < ATR_MA×0.8 (compression)'],
          ['↑BIAS',  'Bull bias during compression'],
        ].map(([lbl, tip]) => (
          <span key={lbl} className="text-xs text-gray-500 flex items-center gap-1">
            <SignalBadge label={lbl} />
            <span className="hidden lg:inline">{tip}</span>
          </span>
        ))}
      </div>
    </div>
  )
}
