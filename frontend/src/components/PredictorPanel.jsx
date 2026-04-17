import { useEffect, useState, useCallback, useRef } from 'react'
import { api } from '../api'

// ── Signal metadata ───────────────────────────────────────────────────────────
const _SIG_NAMES = {
  0:'—', 1:'T1G', 2:'T1', 3:'T2G', 4:'T2', 5:'T3', 6:'T4', 7:'T5', 8:'T6',
  9:'T9', 10:'T10', 11:'T11', 12:'T12',
  13:'Z1G', 14:'Z1', 15:'Z2G', 16:'Z2', 17:'Z3', 18:'Z4', 19:'Z5', 20:'Z6',
  21:'Z7', 22:'Z9', 23:'Z10', 24:'Z11', 25:'Z12',
}
const _COL_IDS = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]
const _ROW_IDS = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]

function _cellBg(pct, colId) {
  if (pct < 1) return undefined
  const op = Math.min(pct / 28, 1) * 0.80
  if (colId === 0)  return `rgba(107,114,128,${op})`
  if (colId <= 12)  return `rgba(34,197,94,${op})`
  return `rgba(239,68,68,${op})`
}

// ── T/Z Transition Matrix (chess-board) ───────────────────────────────────────
function TZMatrix({ matrixData, label, sublabel, offset }) {
  const data = (offset === 1 ? matrixData?.bar1 : matrixData?.bar2) ?? {}
  const hasData = Object.keys(data).length > 0

  return (
    <div className="mb-4">
      {/* Panel header */}
      <div className="px-3 py-2 bg-violet-900/40 rounded-t-lg text-sm font-bold text-violet-300 flex items-center justify-between">
        <span>T/Z Transition Matrix — Bar +{offset}</span>
        {sublabel && <span className="text-[10px] font-normal opacity-70">{sublabel}</span>}
      </div>
      <div className="border border-gray-800 rounded-b-lg overflow-hidden">
        {!hasData ? (
          <div className="px-3 py-6 text-center text-gray-600 text-xs">No data</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="text-[10px] border-collapse w-full">
              <thead>
                <tr className="bg-gray-900/80">
                  <th className="sticky left-0 z-10 bg-gray-900 px-2 py-1.5 text-left text-gray-500 font-normal whitespace-nowrap border-r border-gray-800" style={{minWidth:'46px'}}>
                    ↓ / →
                  </th>
                  {_COL_IDS.map(c => (
                    <th key={c}
                      className={`px-1 py-1.5 text-center font-mono font-bold
                        ${c === 0 ? 'text-gray-500' : c <= 12 ? 'text-green-400' : 'text-red-400'}
                        ${c === 13 ? 'border-l border-gray-700' : ''}`}
                      style={{minWidth:'34px'}}>
                      {_SIG_NAMES[c]}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {_ROW_IDS.map((rowId, idx) => {
                  const row  = data[String(rowId)] ?? {}
                  const tot  = Object.values(row).reduce((a, b) => a + b, 0)
                  const isZ  = rowId >= 13
                  return (
                    <tr key={rowId}
                      className={`border-t border-gray-800/50
                        ${idx === 11 ? 'border-t-2 border-gray-600' : ''}
                      `}>
                      {/* Row header */}
                      <td className={`sticky left-0 z-10 bg-gray-950 px-2 py-0.5 font-mono font-bold border-r border-gray-800
                        ${isZ ? 'text-red-400' : 'text-green-400'}`}>
                        {_SIG_NAMES[rowId]}
                      </td>
                      {_COL_IDS.map(colId => {
                        const cnt = row[String(colId)] ?? 0
                        const pct = tot > 0 ? cnt / tot * 100 : 0
                        return (
                          <td key={colId}
                            className={`text-center px-0.5 py-0.5 font-mono
                              ${colId === 13 ? 'border-l border-gray-700' : ''}
                              ${pct >= 2 ? 'text-white' : 'text-transparent'}`}
                            style={{backgroundColor: _cellBg(pct, colId)}}>
                            {pct >= 2 ? Math.round(pct) + '%' : '·'}
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

// ── T/Z outcome table ─────────────────────────────────────────────────────────
function TZOutcomeTable({ data, title, color, pooled = false }) {
  return (
    <div className="flex-1 min-w-0">
      <div className={`px-3 py-2 rounded-t-lg text-sm font-bold flex items-center justify-between ${color}`}>
        <span>{title}</span>
        {pooled && <span className="text-[10px] font-normal opacity-70">SP500 Pooled</span>}
      </div>
      <div className="border border-gray-800 rounded-b-lg overflow-hidden">
        <div className="px-3 py-2 bg-gray-900 border-b border-gray-800 text-xs text-gray-400">
          <span className="font-mono">{data.pattern || '—'}</span>
          {data.signals && (
            <span className="ml-2 text-gray-500 text-xs">{data.signals}</span>
          )}
        </div>
        <div className={`px-3 py-1.5 bg-gray-900 border-b border-gray-800 text-xs flex items-center gap-2
          ${data.total_matches >= 50 ? 'text-lime-500' : data.total_matches >= 15 ? 'text-yellow-500' : 'text-gray-500'}`}>
          <span>{data.total_matches} matches</span>
          {data.total_matches >= 50 && <span className="text-[9px] bg-lime-900/40 px-1 rounded">high confidence</span>}
          {data.total_matches >= 15 && data.total_matches < 50 && <span className="text-[9px] bg-yellow-900/40 px-1 rounded">moderate</span>}
          {data.total_matches > 0 && data.total_matches < 15 && <span className="text-[9px] bg-gray-800 px-1 rounded">low</span>}
          {/* Regime split */}
          {data.bull_matches > 0 && (
            <span className="ml-auto text-[9px] text-gray-500">
              <span className="text-lime-400">🟢{data.bull_bull_pct}%</span>
              <span className="text-gray-600"> ({data.bull_matches}n) </span>
              <span className="text-red-400">🔴{data.bear_bull_pct}%</span>
              <span className="text-gray-600"> ({data.bear_matches}n)</span>
            </span>
          )}
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-gray-500 bg-gray-900">
              <th className="px-3 py-1 text-left">#</th>
              <th className="px-3 py-1 text-left">Signal</th>
              <th className="px-3 py-1 text-right">Count</th>
              <th className="px-3 py-1 text-right">%</th>
            </tr>
          </thead>
          <tbody>
            {(!data.top_outcomes || data.top_outcomes.length === 0) && (
              <tr>
                <td colSpan={4} className="px-3 py-4 text-center text-gray-600 text-xs">
                  No data
                </td>
              </tr>
            )}
            {data.top_outcomes?.map((row, i) => (
              <tr key={i}
                className={`border-t border-gray-800 ${
                  row.is_bull ? 'bg-green-950/30 text-green-300'
                  : row.is_bear ? 'bg-red-950/30 text-red-300'
                  : 'text-gray-400'
                }`}>
                <td className="px-3 py-1.5 text-gray-500">{i + 1}</td>
                <td className="px-3 py-1.5 font-mono font-semibold">{row.sig_name}</td>
                <td className="px-3 py-1.5 text-right">{row.count}</td>
                <td className="px-3 py-1.5 text-right font-bold">{row.pct}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── T/Z signal frequency statistics ──────────────────────────────────────────
function TZStatsSection({ tickerStats, benchStats, ticker, showTicker, showPooled }) {
  if (!showTicker && !showPooled) return null
  const ts = tickerStats
  const bs = benchStats
  if (!ts && !bs) return null

  const benchLabel = bs?.bench_ticker ?? 'Bench'

  // Render one half-panel (ticker or benchmark)
  function StatsPanel({ s, label, sublabel, colorCls }) {
    if (!s || s.total_bars === 0) return (
      <div className="flex-1 min-w-0">
        <div className={`px-3 py-2 rounded-t-lg text-sm font-bold flex items-center justify-between ${colorCls}`}>
          <span>{label}</span>
          {sublabel && <span className="text-[10px] font-normal opacity-70">{sublabel}</span>}
        </div>
        <div className="border border-gray-800 rounded-b-lg px-3 py-6 text-center text-gray-600 text-xs">No data</div>
      </div>
    )

    const tSigs = s.t_signals ?? []
    const zSigs = s.z_signals ?? []

    return (
      <div className="flex-1 min-w-0">
        {/* Header */}
        <div className={`px-3 py-2 rounded-t-lg text-sm font-bold flex items-center justify-between ${colorCls}`}>
          <span>{label}</span>
          {sublabel && <span className="text-[10px] font-normal opacity-70">{sublabel}</span>}
        </div>
        <div className="border border-gray-800 rounded-b-lg overflow-hidden">
          {/* Bar type counts */}
          <div className="px-3 py-2 bg-gray-900 border-b border-gray-800 flex flex-wrap gap-3 text-xs text-gray-400">
            <span className="text-gray-500">{s.total_bars} bars</span>
            <span><span className="text-green-400">{s.bull_bars}</span> bull</span>
            <span><span className="text-red-400">{s.bear_bars}</span> bear</span>
            <span><span className="text-gray-400">{s.doji_bars}</span> doji</span>
            <span className="ml-auto">
              <span className="text-green-500 font-mono">{s.t_total}T</span>
              <span className="text-gray-600 mx-1">/</span>
              <span className="text-red-400 font-mono">{s.z_total}Z</span>
            </span>
          </div>
          {/* T + Z tables side by side */}
          <div className="flex">
            {/* T signals */}
            <div className="flex-1 border-r border-gray-800">
              <div className="px-2 py-1 bg-green-950/30 border-b border-gray-800 text-[10px] font-bold text-green-400 flex gap-2">
                <span className="flex-1">T Signal</span>
                <span className="w-8 text-right">n</span>
                <span className="w-10 text-right">grp%</span>
                <span className="w-10 text-right">bar%</span>
              </div>
              {tSigs.map(row => (
                <div key={row.sig_id}
                  className={`px-2 py-0.5 flex items-center gap-2 border-t border-gray-800/60 text-xs
                    ${row.count > 0 ? 'text-green-300' : 'text-gray-600'}`}>
                  <span className="flex-1 font-mono font-semibold">{row.name}</span>
                  <span className="w-8 text-right">{row.count || ''}</span>
                  <span className="w-10 text-right">{row.count ? row.group_pct + '%' : ''}</span>
                  <span className="w-10 text-right">{row.count ? row.bar_pct + '%' : ''}</span>
                </div>
              ))}
            </div>
            {/* Z signals */}
            <div className="flex-1">
              <div className="px-2 py-1 bg-red-950/30 border-b border-gray-800 text-[10px] font-bold text-red-400 flex gap-2">
                <span className="flex-1">Z Signal</span>
                <span className="w-8 text-right">n</span>
                <span className="w-10 text-right">grp%</span>
                <span className="w-10 text-right">bar%</span>
              </div>
              {zSigs.map(row => (
                <div key={row.sig_id}
                  className={`px-2 py-0.5 flex items-center gap-2 border-t border-gray-800/60 text-xs
                    ${row.count > 0 ? (row.sig_id === 20 ? 'text-gray-400' : 'text-red-300') : 'text-gray-600'}`}>
                  <span className="flex-1 font-mono font-semibold">{row.name}</span>
                  <span className="w-8 text-right">{row.count || ''}</span>
                  <span className="w-10 text-right">{row.count ? row.group_pct + '%' : ''}</span>
                  <span className="w-10 text-right">{row.count ? row.bar_pct + '%' : ''}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex gap-3">
      {showTicker && (
        <StatsPanel
          s={ts}
          label="T/Z Signal Frequency"
          sublabel={ticker}
          colorCls="bg-violet-900/50 text-violet-300"
        />
      )}
      {showPooled && (
        <StatsPanel
          s={bs}
          label="T/Z Signal Frequency"
          sublabel={benchLabel}
          colorCls="bg-violet-800/40 text-violet-200"
        />
      )}
    </div>
  )
}

// ── L-combo outcome table ─────────────────────────────────────────────────────
function LOutcomeTable({ data, title, color, pooled = false }) {
  return (
    <div className="flex-1 min-w-0">
      <div className={`px-3 py-2 rounded-t-lg text-sm font-bold flex items-center justify-between ${color}`}>
        <span>{title}</span>
        {pooled && <span className="text-[10px] font-normal opacity-70">SP500 Pooled</span>}
      </div>
      <div className="border border-gray-800 rounded-b-lg overflow-hidden">
        <div className="px-3 py-2 bg-gray-900 border-b border-gray-800 text-xs text-gray-400">
          <span className="font-mono text-cyan-400">{data.pattern || '—'}</span>
        </div>
        <div className={`px-3 py-1.5 bg-gray-900 border-b border-gray-800 text-xs flex items-center gap-2
          ${data.total_matches >= 100 ? 'text-lime-500' : data.total_matches >= 30 ? 'text-yellow-500' : 'text-gray-500'}`}>
          <span>{data.total_matches} matches</span>
          {data.total_matches >= 100 && <span className="text-[9px] bg-lime-900/40 px-1 rounded">high confidence</span>}
          {data.total_matches >= 30 && data.total_matches < 100 && <span className="text-[9px] bg-yellow-900/40 px-1 rounded">moderate</span>}
          {data.total_matches > 0 && data.total_matches < 30 && <span className="text-[9px] bg-gray-800 px-1 rounded">low</span>}
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-gray-500 bg-gray-900">
              <th className="px-3 py-1 text-left">#</th>
              <th className="px-3 py-1 text-left">L-Combo</th>
              <th className="px-3 py-1 text-right">Count</th>
              <th className="px-3 py-1 text-right">%</th>
            </tr>
          </thead>
          <tbody>
            {(!data.top_outcomes || data.top_outcomes.length === 0) && (
              <tr>
                <td colSpan={4} className="px-3 py-4 text-center text-gray-600 text-xs">
                  No data
                </td>
              </tr>
            )}
            {data.top_outcomes?.map((row, i) => (
              <tr key={i}
                className={`border-t border-gray-800 ${
                  row.is_bullish === true  ? 'bg-green-950/30 text-green-300'
                  : row.is_bullish === false ? 'bg-red-950/30 text-red-300'
                  : 'text-gray-400'
                }`}>
                <td className="px-3 py-1.5 text-gray-500">{i + 1}</td>
                <td className="px-3 py-1.5 font-mono font-semibold">
                  {row.l_combo}
                  {row.is_bullish === true  && <span className="ml-1 text-green-500">▲</span>}
                  {row.is_bullish === false && <span className="ml-1 text-red-500">▼</span>}
                </td>
                <td className="px-3 py-1.5 text-right">{row.count}</td>
                <td className="px-3 py-1.5 text-right font-bold">{row.pct}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Pooled stats status bar ────────────────────────────────────────────────────
function PooledStatusBar({ universe, interval, onBuildDone }) {
  const [status,   setStatus]   = useState(null)
  const [building, setBuilding] = useState(false)
  const [error,    setError]    = useState(null)
  const autoTriggered = useRef(false)

  const build = useCallback((silent = false) => {
    if (!silent) setError(null)
    api.pooledStatsBuild(universe, interval, 2000)
      .then(() => { setBuilding(true) })
      .catch(e => {
        // 409 = already running — treat as success
        if (e.message?.startsWith('409')) { setBuilding(true); return }
        if (!silent) setError(e.message)
      })
  }, [universe, interval])

  const fetchStatus = useCallback(() => {
    api.pooledStatsStatus(universe, interval).then(s => {
      setStatus(s)
      if (s.job?.running) {
        setBuilding(true)
        setTimeout(fetchStatus, 3000)  // poll while building
      } else if (building) {
        setBuilding(false)
        onBuildDone?.()
      }
      // No auto-build — user must press Build manually
    }).catch(() => {})
  }, [universe, interval, building, build])

  // Reset auto-trigger flag when universe/interval changes
  useEffect(() => {
    autoTriggered.current = false
    fetchStatus()
  }, [universe, interval])

  const data = status?.data
  const job  = status?.job

  return (
    <div className="flex items-center gap-3 px-3 py-2 bg-gray-900/60 border-b border-gray-800 text-xs">
      <span className="text-gray-500 font-medium">SP500 Pooled:</span>
      {data?.available ? (
        <>
          <span className="text-lime-400">✓ Built</span>
          <span className="text-gray-500">
            {data.ticker_count} tickers · {(data.tz_patterns + data.l_patterns).toLocaleString()} patterns
          </span>
          <span className="text-gray-600">
            {data.built_at ? new Date(data.built_at).toLocaleString() : ''}
          </span>
        </>
      ) : (
        <span className="text-yellow-500">
          {job?.running || building ? 'Building…' : 'Not built yet'}
        </span>
      )}
      {(job?.running || building) ? (
        <span className="text-violet-400 animate-pulse">
          ⚡ {job?.done ?? '…'}/{job?.total ?? '…'} tickers
          {job?.elapsed ? ` (${job.elapsed}s)` : ''}
        </span>
      ) : (
        <button onClick={() => build(false)}
          className="px-2 py-0.5 rounded bg-violet-700 hover:bg-violet-600 text-white text-xs font-medium ml-1">
          {data?.available ? '↺ Rebuild' : '⚡ Build'}
        </button>
      )}
      {error && <span className="text-red-400">{error}</span>}
    </div>
  )
}

// ── Main panel ────────────────────────────────────────────────────────────────
const SOURCES = [
  { key: 'ticker', label: 'This Ticker' },
  { key: 'pooled', label: 'SP500 Pooled' },
  { key: 'both',   label: 'Both' },
]

const UNIVERSES_POOL = ['sp500', 'nasdaq', 'russell2k']

// ── localStorage cache helpers ────────────────────────────────────────────────
const _lsKey = (type, ticker, tf, uni = '') =>
  `sachoki_pred_${type}_${ticker}_${tf}${uni ? '_' + uni : ''}`

const _lsGet = (key) => {
  try { return JSON.parse(localStorage.getItem(key) || 'null') } catch { return null }
}
const _lsSet = (key, val) => {
  try { localStorage.setItem(key, JSON.stringify(val)) } catch {}
}

const TF_OPTS_PRED = ['1wk', '1d', '4h', '1h']

export default function PredictorPanel({ ticker, tf }) {
  const [localTf,       setLocalTf]       = useState(tf || '1d')
  const [tickerData,    setTickerData]    = useState(() => _lsGet(_lsKey('tz', ticker, tf)))
  const [pooledData,    setPooledData]    = useState(() => _lsGet(_lsKey('pool', ticker, tf, 'sp500')))
  const [loading,       setLoading]       = useState(false)
  const [error,         setError]         = useState(null)
  const [source,        setSource]        = useState('both')
  const [poolUni,       setPoolUni]       = useState('sp500')
  const [view,          setView]          = useState('stats')   // 'stats' | 'matrix'
  const [matrixOffset,  setMatrixOffset]  = useState(1)          // 1 = bar+1, 2 = bar+2

  const fetchTicker = useCallback(() => {
    if (!ticker) return
    setError(null)
    setLoading(true)
    api.predict(ticker, localTf)
      .then(d => {
        setTickerData(d)
        _lsSet(_lsKey('tz', ticker, localTf), d)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, localTf])

  const fetchPooled = useCallback(() => {
    if (!ticker) return
    api.pooledPredict(ticker, localTf, poolUni)
      .then(d => {
        if (!d.error) {
          setPooledData(d)
          _lsSet(_lsKey('pool', ticker, localTf, poolUni), d)
        }
      })
      .catch(() => {})
  }, [ticker, localTf, poolUni])

  // Restore cached data when ticker/localTf/poolUni changes, then fetch fresh in background
  useEffect(() => {
    const cached = _lsGet(_lsKey('tz', ticker, localTf))
    if (cached) setTickerData(cached)
    fetchTicker()
  }, [fetchTicker])

  useEffect(() => {
    const cached = _lsGet(_lsKey('pool', ticker, localTf, poolUni))
    if (cached) setPooledData(cached)
    fetchPooled()
  }, [fetchPooled])

  const empty = { pattern: '', signals: '', total_matches: 0, top_outcomes: [] }
  const td = tickerData
  const pd = pooledData

  const showTicker = source === 'ticker' || source === 'both'
  const showPooled = source === 'pooled' || source === 'both'

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800">

      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800 flex-wrap gap-2">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="font-semibold text-sm">
            Next-Bar Predictor — {ticker}
            {td?.current_regime === 'bull' && (
              <span className="ml-2 text-[9px] bg-lime-900/50 text-lime-400 px-1.5 py-0.5 rounded font-normal">🟢 Bull Regime</span>
            )}
            {td?.current_regime === 'bear' && (
              <span className="ml-2 text-[9px] bg-red-900/50 text-red-400 px-1.5 py-0.5 rounded font-normal">🔴 Bear Regime</span>
            )}
          </span>
          <div className="flex gap-0.5 border border-gray-700 rounded p-0.5">
            {TF_OPTS_PRED.map(t => (
              <button key={t} onClick={() => setLocalTf(t)}
                className={`px-2 py-0.5 rounded text-xs font-medium transition-colors
                  ${localTf === t ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white'}`}>
                {t.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
          {!loading && (
            <button onClick={() => { fetchTicker(); fetchPooled() }}
              title="Refresh predictions"
              className="px-2 py-0.5 rounded text-xs bg-gray-800 text-gray-400 hover:text-white transition-colors">
              ↺
            </button>
          )}
          {error   && <span className="text-xs text-red-400">{error}</span>}

          {/* Pool universe selector */}
          <div className="flex gap-0.5">
            {UNIVERSES_POOL.map(u => (
              <button key={u} onClick={() => setPoolUni(u)}
                className={`px-2 py-0.5 rounded text-xs transition-colors
                  ${poolUni === u ? 'bg-indigo-700 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
                {u === 'sp500' ? 'SP500' : u === 'nasdaq' ? 'NASDAQ' : 'R2K'}
              </button>
            ))}
          </div>

          {/* Source toggle */}
          <div className="flex gap-0.5 border border-gray-700 rounded p-0.5">
            {SOURCES.map(s => (
              <button key={s.key} onClick={() => setSource(s.key)}
                className={`px-2 py-0.5 rounded text-xs transition-colors
                  ${source === s.key ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white'}`}>
                {s.label}
              </button>
            ))}
          </div>

          {/* Stats / Matrix view toggle */}
          <div className="flex gap-0.5 border border-gray-700 rounded p-0.5">
            <button onClick={() => setView('stats')}
              className={`px-2 py-0.5 rounded text-xs transition-colors
                ${view === 'stats' ? 'bg-violet-700 text-white' : 'text-gray-400 hover:text-white'}`}>
              Stats
            </button>
            <button onClick={() => setView('matrix')}
              className={`px-2 py-0.5 rounded text-xs transition-colors
                ${view === 'matrix' ? 'bg-violet-700 text-white' : 'text-gray-400 hover:text-white'}`}>
              Matrix
            </button>
          </div>
        </div>
      </div>

      {/* Pooled stats status + build */}
      {(source === 'pooled' || source === 'both') && (
        <PooledStatusBar universe={poolUni} interval={localTf} onBuildDone={fetchPooled} />
      )}

      {/* ── Matrix view ── */}
      {view === 'matrix' && (
        <div className="p-3">
          {/* Bar offset toggle */}
          <div className="flex gap-1 mb-3">
            {[1, 2].map(o => (
              <button key={o} onClick={() => setMatrixOffset(o)}
                className={`px-3 py-1 rounded text-xs font-medium transition-colors
                  ${matrixOffset === o ? 'bg-indigo-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
                Bar +{o}
              </button>
            ))}
            <span className="ml-2 text-[10px] text-gray-600 self-center">
              % = how often each signal appears after the row signal
            </span>
          </div>

          {showTicker && (
            <TZMatrix
              matrixData={td?.tz_matrix}
              label={ticker}
              sublabel={ticker}
              offset={matrixOffset}
            />
          )}
          {showPooled && (
            <TZMatrix
              matrixData={pd?.bench_tz_matrix}
              label="T/Z Transition Matrix"
              sublabel={pd?.bench_tz_stats?.bench_ticker ?? poolUni}
              offset={matrixOffset}
            />
          )}
        </div>
      )}

      {/* ── Stats view (predictions + frequency) ── */}
      {view === 'stats' && (
      <div className="p-3 space-y-3">

        {/* Row 1: T/Z 3-bar */}
        <div className="flex gap-3">
          {showTicker && (
            <TZOutcomeTable
              data={td?.tz_3bar ?? empty}
              title="T/Z 3-Bar"
              color="bg-blue-900/50 text-blue-300"
              pooled={false}
            />
          )}
          {showPooled && (
            <TZOutcomeTable
              data={pd?.tz_3bar ?? empty}
              title="T/Z 3-Bar"
              color="bg-blue-800/40 text-blue-200"
              pooled={true}
            />
          )}
        </div>

        {/* Row 2: T/Z 2-bar */}
        <div className="flex gap-3">
          {showTicker && (
            <TZOutcomeTable
              data={td?.tz_2bar ?? empty}
              title="T/Z 2-Bar"
              color="bg-orange-900/50 text-orange-300"
              pooled={false}
            />
          )}
          {showPooled && (
            <TZOutcomeTable
              data={pd?.tz_2bar ?? empty}
              title="T/Z 2-Bar"
              color="bg-orange-800/40 text-orange-200"
              pooled={true}
            />
          )}
        </div>

        {/* Row 3: L-Signal 3-bar */}
        <div className="flex gap-3">
          {showTicker && (
            <LOutcomeTable
              data={td?.l_3bar ?? empty}
              title="L-Signal 3-Bar"
              color="bg-teal-900/50 text-teal-300"
              pooled={false}
            />
          )}
          {showPooled && (
            <LOutcomeTable
              data={pd?.l_3bar ?? empty}
              title="L-Signal 3-Bar"
              color="bg-teal-800/40 text-teal-200"
              pooled={true}
            />
          )}
        </div>

        {/* Row 4: L-Signal 2-bar */}
        <div className="flex gap-3">
          {showTicker && (
            <LOutcomeTable
              data={td?.l_2bar ?? empty}
              title="L-Signal 2-Bar"
              color="bg-amber-900/50 text-amber-300"
              pooled={false}
            />
          )}
          {showPooled && (
            <LOutcomeTable
              data={pd?.l_2bar ?? empty}
              title="L-Signal 2-Bar"
              color="bg-amber-800/40 text-amber-200"
              pooled={true}
            />
          )}
        </div>

        {/* Row 5: T/Z Signal Frequency Statistics */}
        <TZStatsSection
          tickerStats={td?.tz_stats}
          benchStats={pd?.bench_tz_stats}
          ticker={ticker}
          showTicker={showTicker}
          showPooled={showPooled}
        />
      </div>
      )}  {/* end stats view */}

      {!ticker && (
        <div className="pb-6 text-center text-gray-600 text-sm">Select a ticker</div>
      )}
    </div>
  )
}
