import { useState, useEffect, useCallback, useMemo } from 'react'
import { api } from '../api'
import { exportToTV } from '../utils/exportTickers'

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

const KEY_TO_SIG   = Object.fromEntries(SIGNALS.map(s => [s.key,   s]))
const LABEL_TO_SIG = Object.fromEntries(SIGNALS.map(s => [s.label, s]))

// Map server-returned label → signal key (for multi-select matching)
const LABEL_TO_KEY = Object.fromEntries(SIGNALS.map(s => [s.label, s.key]))

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
  const [selected,   setSelected]  = useState(new Set())   // selected signal keys
  const [allResults, setAllResults] = useState([])
  const [lastScan,   setLastScan]  = useState(null)
  const [loading,    setLoading]   = useState(false)
  const [scanning,   setScanning]  = useState(false)
  const [error,      setError]     = useState(null)
  const [debug,      setDebug]     = useState(null)   // { ticker, loading, data, error }

  // Always fetch all results; filtering is client-side
  const load = useCallback(() => {
    setLoading(true)
    setError(null)
    api.comboScan('all', 500)
      .then(d => {
        setAllResults(d.results || [])
        setLastScan(d.last_scan)
      })
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
    api.comboScanTrigger(tf)
      .then(() => setTimeout(() => { setScanning(false); load() }, 3000))
      .catch(e => { setError(e.message); setScanning(false) })
  }

  const toggleSignal = (key) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const clearAll = () => setSelected(new Set())

  // Client-side AND filter: ticker must have ALL selected signals
  const results = useMemo(() => {
    if (selected.size === 0) return allResults
    return allResults.filter(row => {
      const rowLabels = row.signals ? row.signals.split(',').filter(Boolean) : []
      const rowKeys   = new Set(rowLabels.map(l => LABEL_TO_KEY[l]).filter(Boolean))
      return [...selected].every(k => rowKeys.has(k))
    })
  }, [allResults, selected])

  const openDebug = (ticker) => {
    setDebug({ ticker, loading: true, data: null, error: null })
    api.comboScanDebug(ticker, tf)
      .then(d  => setDebug(prev => prev?.ticker === ticker ? { ...prev, loading: false, data: d } : prev))
      .catch(e => setDebug(prev => prev?.ticker === ticker ? { ...prev, loading: false, error: e.message } : prev))
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
          {results.length > 0 && (
            <button
              onClick={() => exportToTV(results.map(r => r.ticker), 'combo_scan.txt')}
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

      {/* ── Signal filter buttons (multi-select AND) ─────────────────────── */}
      <div className="flex items-center gap-1 px-3 py-2 border-b border-gray-800 flex-wrap">
        {/* All / Clear button */}
        <button
          onClick={clearAll}
          className={`text-xs px-2 py-1 rounded transition-colors
            ${selected.size === 0
              ? 'bg-gray-600 text-white font-semibold'
              : 'text-gray-500 hover:text-gray-300'}`}
        >
          All
        </button>

        {SIGNALS.map(s => {
          const active = selected.has(s.key)
          return (
            <button
              key={s.key}
              onClick={() => toggleSignal(s.key)}
              className={`text-xs px-2 py-1 rounded font-mono transition-colors
                ${active
                  ? `${s.bg} ${s.color} font-semibold ring-1 ring-current`
                  : 'text-gray-500 hover:text-gray-300'}`}
            >
              {s.label}
            </button>
          )
        })}

        {/* AND badge when multiple selected */}
        {selected.size >= 2 && (
          <span className="ml-1 text-xs text-indigo-400 font-semibold bg-indigo-900/40 px-2 py-0.5 rounded">
            AND ×{selected.size}
          </span>
        )}

        <span className="ml-auto text-xs text-gray-500">{results.length} tickers</span>
      </div>

      {error && <div className="px-4 py-2 text-xs text-red-400">{error}</div>}

      {/* ── Table ──────────────────────────────────────────────────────────── */}
      <div className="overflow-auto flex-1">
        {loading ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">Loading…</div>
        ) : results.length === 0 ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">
            {selected.size > 0
              ? `No tickers with all ${selected.size} selected signals.`
              : 'No results — press Scan Now first.'}
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
                    onClick={e => e.shiftKey ? openDebug(row.ticker) : onSelectTicker?.(row.ticker)}
                    title="Click → chart | Shift+Click → debug bars"
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

      {/* ── Debug Modal ────────────────────────────────────────────────────── */}
      {debug && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70"
             onClick={() => setDebug(null)}>
          <div className="bg-gray-900 border border-gray-700 rounded-xl p-5 w-[700px] max-w-full max-h-[80vh] overflow-auto shadow-2xl"
               onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between mb-3">
              <span className="font-bold text-white text-sm">{debug.ticker} — bolo 7 bari (n_bars=3)</span>
              <button onClick={() => setDebug(null)} className="text-gray-400 hover:text-white text-lg leading-none">×</button>
            </div>

            {debug.loading && <div className="text-xs text-gray-400 py-4 text-center">Loading…</div>}
            {debug.error   && <div className="text-xs text-red-400 py-2">{debug.error}</div>}

            {debug.data && (
              <>
                {/* Active signals summary */}
                <div className="mb-3 flex flex-wrap gap-1 items-center">
                  <span className="text-xs text-gray-500 mr-1">Active (last {debug.data.n_bars} bars):</span>
                  {debug.data.active.length === 0
                    ? <span className="text-xs text-gray-600">none</span>
                    : debug.data.active.map(lbl => <SignalBadge key={lbl} label={lbl} />)
                  }
                </div>

                {/* Per-bar table */}
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-gray-400 border-b border-gray-700">
                      <th className="text-left py-1 pr-3 w-28">Date</th>
                      <th className="text-left py-1">Signals fired</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...debug.data.bars].reverse().map((bar, i) => (
                      <tr key={i} className={`border-b border-gray-800/50 ${i < debug.data.n_bars ? 'bg-indigo-950/30' : ''}`}>
                        <td className="py-1.5 pr-3 font-mono text-gray-300">
                          {bar.date}
                          {i < debug.data.n_bars && (
                            <span className="ml-1 text-indigo-400 text-[10px]">← n_bars</span>
                          )}
                        </td>
                        <td className="py-1 flex flex-wrap gap-1">
                          {bar.signals.length === 0
                            ? <span className="text-gray-700">—</span>
                            : bar.signals.map(lbl => <SignalBadge key={lbl} label={lbl} />)
                          }
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <p className="mt-2 text-[10px] text-gray-600">Highlighted rows = last {debug.data.n_bars} bars checked by scan</p>
              </>
            )}
          </div>
        </div>
      )}

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
