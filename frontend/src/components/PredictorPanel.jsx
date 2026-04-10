import { useEffect, useState, useCallback } from 'react'
import { api } from '../api'

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
  const [status, setStatus] = useState(null)
  const [building, setBuilding] = useState(false)
  const [error, setError] = useState(null)

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
    }).catch(() => {})
  }, [universe, interval, building])

  useEffect(() => { fetchStatus() }, [universe, interval])

  const build = () => {
    setError(null)
    api.pooledStatsBuild(universe, interval, 2000)
      .then(() => { setBuilding(true); setTimeout(fetchStatus, 3000) })
      .catch(e => setError(e.message))
  }

  const data = status?.data
  const job  = status?.job

  return (
    <div className="flex items-center gap-3 px-3 py-2 bg-gray-900/60 border-b border-gray-800 text-xs">
      <span className="text-gray-500 font-medium">SP500 Pooled:</span>
      {data?.available ? (
        <>
          <span className="text-lime-400">✓ Built</span>
          <span className="text-gray-500">
            {data.ticker_count} tickers{data.ticker_count < 2100 ? ' (random sample)' : ''} · {(data.tz_patterns + data.l_patterns).toLocaleString()} patterns
          </span>
          <span className="text-gray-600">
            {data.built_at ? new Date(data.built_at).toLocaleString() : ''}
          </span>
        </>
      ) : (
        <span className="text-yellow-500">Not built yet</span>
      )}
      {job?.running ? (
        <span className="text-violet-400 animate-pulse">
          ⚡ Building… {job.done}/{job.total} tickers ({job.elapsed}s)
        </span>
      ) : (
        <button onClick={build}
          className="px-2 py-0.5 rounded bg-violet-700 hover:bg-violet-600 text-white text-xs font-medium ml-1">
          {data?.available ? '↺ Rebuild' : '⚡ Build SP500 Stats'}
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

export default function PredictorPanel({ ticker, tf }) {
  const [tickerData, setTickerData] = useState(() => _lsGet(_lsKey('tz', ticker, tf)))
  const [pooledData, setPooledData] = useState(() => _lsGet(_lsKey('pool', ticker, tf, 'sp500')))
  const [loading,    setLoading]    = useState(false)
  const [error,      setError]      = useState(null)
  const [source,     setSource]     = useState('both')
  const [poolUni,    setPoolUni]    = useState('sp500')

  const fetchTicker = useCallback(() => {
    if (!ticker) return
    setError(null)
    setLoading(true)
    api.predict(ticker, tf)
      .then(d => {
        setTickerData(d)
        _lsSet(_lsKey('tz', ticker, tf), d)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, tf])

  const fetchPooled = useCallback(() => {
    if (!ticker) return
    api.pooledPredict(ticker, tf, poolUni)
      .then(d => {
        if (!d.error) {
          setPooledData(d)
          _lsSet(_lsKey('pool', ticker, tf, poolUni), d)
        }
      })
      .catch(() => {})
  }, [ticker, tf, poolUni])

  // Restore cached data when ticker/tf/poolUni changes, then fetch fresh in background
  useEffect(() => {
    const cached = _lsGet(_lsKey('tz', ticker, tf))
    if (cached) setTickerData(cached)
    fetchTicker()
  }, [fetchTicker])

  useEffect(() => {
    const cached = _lsGet(_lsKey('pool', ticker, tf, poolUni))
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
        <span className="font-semibold text-sm">Next-Bar Predictor — {ticker}</span>
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
        </div>
      </div>

      {/* Pooled stats status + build */}
      {(source === 'pooled' || source === 'both') && (
        <PooledStatusBar universe={poolUni} interval={tf} onBuildDone={fetchPooled} />
      )}

      {/* 4 tables (2×2 or 2×4 depending on source) */}
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
      </div>

      {!ticker && (
        <div className="pb-6 text-center text-gray-600 text-sm">Select a ticker</div>
      )}
    </div>
  )
}
