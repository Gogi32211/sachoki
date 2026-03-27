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

// ── T/Z signal filter options (bullish) ───────────────────────────────────────
const TZ_SIGNALS = [
  'T4', 'T6', 'T1G', 'T2G', 'T1', 'T2', 'T9', 'T10', 'T3', 'T11', 'T5',
]

// ── WLNBB L signal filter options ─────────────────────────────────────────────
const L_SIGNALS = [
  // WLNBB L signals
  { key: 'fri34',     label: 'FRI34',    color: 'text-cyan-300' },
  { key: 'l34',       label: 'L34',      color: 'text-blue-300' },
  { key: 'l43',       label: 'L43',      color: 'text-teal-300' },
  { key: 'l64',       label: 'L64',      color: 'text-orange-400' },
  { key: 'l22',       label: 'L22',      color: 'text-red-400' },
  { key: 'cci_ready', label: 'CCI_RDY', color: 'text-violet-300' },
  { key: 'blue',      label: 'BLUE',     color: 'text-sky-300' },
  { key: 'bo_up',     label: 'BO_UP',    color: 'text-lime-300' },
  { key: 'bx_up',     label: 'BX_UP',    color: 'text-lime-400' },
  { key: 'pre_pump',  label: 'PRE_PMP',  color: 'text-purple-300' },
  // WLNBB FUCHSIA (RH/RL from 260315)
  { key: 'fuchsia_rh', label: 'RH',     color: 'text-fuchsia-400' },
  { key: 'fuchsia_rl', label: 'RL',     color: 'text-fuchsia-300' },
  // 260312 VSA signals
  { key: 'sq',        label: 'SQ',       color: 'text-cyan-400' },
  { key: 'ns',        label: 'NS',       color: 'text-lime-400' },
  { key: 'nd',        label: 'ND',       color: 'text-red-400' },
  { key: 'sig3_up',   label: '3↑',       color: 'text-blue-300' },
  { key: 'sig3_dn',   label: '3↓',       color: 'text-orange-400' },
  // 3112_2C wick reversal
  { key: 'wick_bull', label: 'WICK↑',    color: 'text-emerald-300' },
  { key: 'wick_bear', label: 'WICK↓',    color: 'text-rose-400' },
  // 250115 CISD sequences
  { key: 'cisd_seq',  label: 'C++--',    color: 'text-lime-300' },
  { key: 'cisd_ppm',  label: 'C++-',     color: 'text-green-300' },
  { key: 'cisd_mpm',  label: 'C-+-',     color: 'text-red-300' },
  { key: 'cisd_pmm',  label: 'C+--',     color: 'text-fuchsia-300' },
]

const LABEL_TO_SIG = Object.fromEntries(SIGNALS.map(s => [s.label, s]))
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

function TZBadge({ sig }) {
  if (!sig) return <span className="text-gray-600">—</span>
  return (
    <span className="px-1.5 py-0.5 rounded text-xs font-mono font-semibold text-green-300 bg-green-900/50">
      {sig}
    </span>
  )
}

function LBadges({ row }) {
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

function rowBg(signals) {
  const labels = signals ? signals.split(',') : []
  if (labels.includes('ROCKET')) return 'bg-red-950/20'
  if (labels.includes('BUY'))    return 'bg-lime-950/20'
  if (labels.includes('3G'))     return 'bg-cyan-950/20'
  if (labels.includes('P3'))     return 'bg-yellow-950/20'
  return ''
}

export default function ComboScanPanel({ tf, onSelectTicker }) {
  const [selected,   setSelected]   = useState(new Set())   // 260323 combo AND filter
  const [selectedTZ, setSelectedTZ] = useState(new Set())   // T/Z OR filter
  const [selectedL,  setSelectedL]  = useState(new Set())   // L signal AND filter
  const [allResults, setAllResults] = useState([])
  const [lastScan,   setLastScan]   = useState(null)
  const [loading,    setLoading]    = useState(false)
  const [scanning,   setScanning]   = useState(false)
  const [error,      setError]      = useState(null)
  const [debug,      setDebug]      = useState(null)

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
      .then(() => _pollUntilDone())
      .catch(e => { setError(e.message); setScanning(false) })
  }

  const _pollUntilDone = () => {
    const iv = setInterval(() => {
      api.comboScanStatus()
        .then(s => {
          if (!s.running) {
            clearInterval(iv)
            setScanning(false)
            load()
          }
        })
        .catch(() => { clearInterval(iv); setScanning(false) })
    }, 2000)
    setTimeout(() => { clearInterval(iv); setScanning(false); load() }, 300_000)
  }

  const toggle = (set, setFn, key) => {
    setFn(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  // Three-layer client-side filter:
  //   1. 260323 combo: AND — all selected combo signal keys must be present
  //   2. T/Z: OR — at least one selected T/Z name must match tz_sig
  //   3. L: AND — all selected L keys must be 1
  const results = useMemo(() => {
    return allResults.filter(row => {
      // 260323 combo AND filter
      if (selected.size > 0) {
        const rowLabels = row.signals ? row.signals.split(',').filter(Boolean) : []
        const rowKeys   = new Set(rowLabels.map(l => LABEL_TO_KEY[l]).filter(Boolean))
        if (![...selected].every(k => rowKeys.has(k))) return false
      }
      // T/Z OR filter
      if (selectedTZ.size > 0) {
        if (!selectedTZ.has(row.tz_sig)) return false
      }
      // L AND filter
      if (selectedL.size > 0) {
        if (![...selectedL].every(k => row[k])) return false
      }
      return true
    })
  }, [allResults, selected, selectedTZ, selectedL])

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

  const anyFilter = selected.size > 0 || selectedTZ.size > 0 || selectedL.size > 0

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
          {anyFilter && (
            <button
              onClick={() => { setSelected(new Set()); setSelectedTZ(new Set()); setSelectedL(new Set()) }}
              className="text-xs px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-yellow-400"
            >
              Clear filters
            </button>
          )}
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

      {/* ── 260323 Combo filter (AND) ─────────────────────────────────────── */}
      <div className="flex items-center gap-1 px-3 py-1.5 border-b border-gray-800/60 flex-wrap">
        <span className="text-[10px] text-gray-600 uppercase tracking-wide w-12 shrink-0">260323</span>
        <button
          onClick={() => setSelected(new Set())}
          className={`text-xs px-2 py-0.5 rounded transition-colors
            ${selected.size === 0
              ? 'bg-gray-700 text-white font-semibold'
              : 'text-gray-500 hover:text-gray-300'}`}
        >
          All
        </button>
        {SIGNALS.map(s => {
          const active = selected.has(s.key)
          return (
            <button
              key={s.key}
              onClick={() => toggle(selected, setSelected, s.key)}
              className={`text-xs px-2 py-0.5 rounded font-mono transition-colors
                ${active
                  ? `${s.bg} ${s.color} font-semibold ring-1 ring-current`
                  : 'text-gray-500 hover:text-gray-300'}`}
            >
              {s.label}
            </button>
          )
        })}
        {selected.size >= 2 && (
          <span className="ml-1 text-xs text-indigo-400 font-semibold bg-indigo-900/40 px-2 py-0.5 rounded">
            AND ×{selected.size}
          </span>
        )}
      </div>

      {/* ── T/Z filter (OR) ──────────────────────────────────────────────── */}
      <div className="flex items-center gap-1 px-3 py-1.5 border-b border-gray-800/60 flex-wrap">
        <span className="text-[10px] text-gray-600 uppercase tracking-wide w-12 shrink-0">T/Z</span>
        <button
          onClick={() => setSelectedTZ(new Set())}
          className={`text-xs px-2 py-0.5 rounded transition-colors
            ${selectedTZ.size === 0
              ? 'bg-gray-700 text-white font-semibold'
              : 'text-gray-500 hover:text-gray-300'}`}
        >
          All
        </button>
        {TZ_SIGNALS.map(sig => {
          const active = selectedTZ.has(sig)
          return (
            <button
              key={sig}
              onClick={() => toggle(selectedTZ, setSelectedTZ, sig)}
              className={`text-xs px-2 py-0.5 rounded font-mono transition-colors
                ${active
                  ? 'bg-green-900/60 text-green-300 font-semibold ring-1 ring-green-400'
                  : 'text-gray-500 hover:text-gray-300'}`}
            >
              {sig}
            </button>
          )
        })}
        {selectedTZ.size >= 2 && (
          <span className="ml-1 text-xs text-green-400 font-semibold bg-green-900/30 px-2 py-0.5 rounded">
            OR ×{selectedTZ.size}
          </span>
        )}
      </div>

      {/* ── L signal filter (AND) ────────────────────────────────────────── */}
      <div className="flex items-center gap-1 px-3 py-1.5 border-b border-gray-800 flex-wrap">
        <span className="text-[10px] text-gray-600 uppercase tracking-wide w-12 shrink-0">L-Sig</span>
        <button
          onClick={() => setSelectedL(new Set())}
          className={`text-xs px-2 py-0.5 rounded transition-colors
            ${selectedL.size === 0
              ? 'bg-gray-700 text-white font-semibold'
              : 'text-gray-500 hover:text-gray-300'}`}
        >
          All
        </button>
        {L_SIGNALS.map(s => {
          const active = selectedL.has(s.key)
          return (
            <button
              key={s.key}
              onClick={() => toggle(selectedL, setSelectedL, s.key)}
              className={`text-xs px-2 py-0.5 rounded font-mono transition-colors
                ${active
                  ? `bg-gray-700 ${s.color} font-semibold ring-1 ring-current`
                  : 'text-gray-500 hover:text-gray-300'}`}
            >
              {s.label}
            </button>
          )
        })}
        {selectedL.size >= 2 && (
          <span className="ml-1 text-xs text-sky-400 font-semibold bg-sky-900/30 px-2 py-0.5 rounded">
            AND ×{selectedL.size}
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
                <th className="text-left px-2 py-2">L-Sig</th>
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
                    <td className="px-2 py-1.5">
                      <TZBadge sig={row.tz_sig} />
                    </td>
                    <td className="px-2 py-1.5">
                      <LBadges row={row} />
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
                <div className="mb-3 flex flex-wrap gap-1 items-center">
                  <span className="text-xs text-gray-500 mr-1">Active (last {debug.data.n_bars} bars):</span>
                  {debug.data.active.length === 0
                    ? <span className="text-xs text-gray-600">none</span>
                    : debug.data.active.map(lbl => <SignalBadge key={lbl} label={lbl} />)
                  }
                </div>
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
