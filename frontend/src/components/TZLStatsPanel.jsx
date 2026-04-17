import { useEffect, useState } from 'react'
import { api } from '../api'

const L_COLS = ["L1", "L2", "L3", "L4", "L5", "L6", "L34", "L22", "L64", "L43", "L1L2", "L2L5"]

// ── Cell renderers ────────────────────────────────────────────────────────────

function CellNormal({ meta }) {
  if (!meta || meta.count === 0)
    return <span className="text-gray-700 text-xs">—</span>
  const { count, pct, color } = meta
  const bg =
    color === 'gold'  ? 'bg-yellow-900/60 text-yellow-300 ring-1 ring-yellow-500' :
    color === 'green' ? 'bg-green-900/50 text-green-300' :
    color === 'red'   ? 'bg-red-900/50 text-red-300' :
    'bg-gray-800 text-gray-400'
  return (
    <div className={`px-1 py-0.5 rounded text-center text-xs font-mono ${bg}`}>
      <div className="font-bold">{count}</div>
      <div className="text-xs opacity-70">{pct}%</div>
    </div>
  )
}

function CellDiff({ tickerMeta, benchMeta }) {
  const tp = tickerMeta?.pct ?? 0
  const bp = benchMeta?.pct  ?? 0
  const diff = tp - bp
  if (tp === 0 && bp === 0)
    return <span className="text-gray-700 text-xs">—</span>
  const cls =
    diff > 8  ? 'bg-green-900/70 text-green-200 ring-1 ring-green-600' :
    diff > 3  ? 'bg-green-900/40 text-green-400' :
    diff < -8 ? 'bg-red-900/70 text-red-200 ring-1 ring-red-600' :
    diff < -3 ? 'bg-red-900/40 text-red-400' :
    'bg-gray-800/60 text-gray-400'
  const sign = diff > 0 ? '+' : ''
  return (
    <div className={`px-1 py-0.5 rounded text-center text-xs font-mono ${cls}`}>
      <div className="font-bold">{tp}%</div>
      <div className="text-[10px] opacity-80">{sign}{diff}pp</div>
    </div>
  )
}

// ── Matrix table ──────────────────────────────────────────────────────────────

function MatrixTable({ matrix, benchMatrix, view, ticker }) {
  if (!matrix?.length)
    return <div className="py-8 text-center text-gray-600 text-sm">No data</div>

  const isDiff = view === 'vs_spy' || view === 'vs_qqq'

  // Build lookup for bench by sig_id
  const benchLookup = {}
  if (isDiff && benchMatrix?.length) {
    for (const row of benchMatrix) benchLookup[row.sig_id] = row
  }

  const displayMatrix = isDiff ? matrix : (
    view === 'spy' ? benchMatrix :
    view === 'qqq' ? benchMatrix :
    matrix
  )

  if (!displayMatrix?.length)
    return <div className="py-8 text-center text-gray-600 text-sm">Benchmark data loading…</div>

  const label = {
    ticker: ticker,
    spy:    'SPY (S&P 500)',
    qqq:    'QQQ (NASDAQ)',
    vs_spy: `${ticker} vs SPY`,
    vs_qqq: `${ticker} vs QQQ`,
  }[view]

  return (
    <div className="overflow-auto flex-1">
      {isDiff && (
        <div className="px-3 py-1.5 bg-gray-900/60 border-b border-gray-800 text-[10px] text-gray-500">
          Cell shows: <span className="text-white">ticker %</span> / <span className="text-lime-400">+diff</span> or <span className="text-red-400">−diff</span> vs benchmark (pp = percentage points)
        </div>
      )}
      <table className="text-xs w-full border-separate border-spacing-0">
        <thead>
          <tr>
            <th className="text-left px-3 py-2 text-gray-500 sticky left-0 bg-gray-900 z-10 border-b border-gray-800 min-w-[60px]">
              Signal
            </th>
            <th className="text-center px-1 py-2 text-gray-500 border-b border-gray-800 min-w-[32px]">n</th>
            {L_COLS.map(col => (
              <th key={col} className="text-center px-1 py-2 text-gray-400 border-b border-gray-800 min-w-[52px] font-mono">
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {(isDiff ? matrix : displayMatrix).map((row, idx) => {
            const benchRow = isDiff ? benchLookup[row.sig_id] : null
            return (
              <>
                {idx === 12 && (
                  <tr key="sep">
                    <td colSpan={L_COLS.length + 2} className="py-1">
                      <div className="border-t border-gray-700" />
                    </td>
                  </tr>
                )}
                <tr key={row.sig_id} className="border-b border-gray-800/40 hover:bg-gray-800/30">
                  <td className="sticky left-0 bg-gray-900 z-10 px-3 py-1.5 border-b border-gray-800/40">
                    <span className={`font-mono font-bold text-xs ${row.is_bull ? 'text-green-400' : 'text-red-400'}`}>
                      {row.sig_name}
                    </span>
                  </td>
                  <td className="text-center px-1 py-1.5 text-gray-500">{row.total}</td>
                  {L_COLS.map(col => (
                    <td key={col} className="text-center px-1 py-1.5">
                      {isDiff
                        ? <CellDiff tickerMeta={row.cols?.[col]} benchMeta={benchRow?.cols?.[col]} />
                        : <CellNormal meta={row.cols?.[col]} />
                      }
                    </td>
                  ))}
                </tr>
              </>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// ── Main panel ────────────────────────────────────────────────────────────────

const VIEWS = [
  { key: 'ticker', label: 'Ticker'  },
  { key: 'spy',    label: 'SPY'     },
  { key: 'qqq',    label: 'QQQ'     },
  { key: 'vs_spy', label: 'vs SPY'  },
  { key: 'vs_qqq', label: 'vs QQQ'  },
]

const TF_OPTS = ['1wk', '1d', '4h', '1h']

export default function TZLStatsPanel({ ticker, tf }) {
  const [localTf, setLocalTf] = useState(tf || '1d')
  const [data,    setData]    = useState(null)   // {matrix, bench_spy, bench_qqq}
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)
  const [view,    setView]    = useState('ticker')

  useEffect(() => {
    if (!ticker) return
    setLoading(true)
    setError(null)
    api.tzLStats(ticker, localTf)
      .then(d => setData(d))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, localTf])

  const benchMatrix =
    (view === 'spy' || view === 'vs_spy') ? data?.bench_spy :
    (view === 'qqq' || view === 'vs_qqq') ? data?.bench_qqq :
    null

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800 flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <span className="font-semibold text-sm">T/Z × L Co-occurrence — {ticker}</span>
          <div className="flex gap-0.5 border border-gray-700 rounded p-0.5">
            {TF_OPTS.map(t => (
              <button key={t} onClick={() => setLocalTf(t)}
                className={`px-2 py-0.5 rounded text-xs font-medium transition-colors
                  ${localTf === t ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white'}`}>
                {t.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-3 flex-wrap">
          {/* Legend */}
          <div className="flex items-center gap-2 text-xs">
            <span className="px-1.5 py-0.5 rounded bg-yellow-900/60 text-yellow-300 ring-1 ring-yellow-500">Gold</span>
            <span className="text-gray-500">= best match</span>
            <span className="px-1.5 py-0.5 rounded bg-green-900/50 text-green-300">Green</span>
            <span className="text-gray-500">= aligned</span>
            <span className="px-1.5 py-0.5 rounded bg-red-900/50 text-red-300">Red</span>
            <span className="text-gray-500">= conflict</span>
          </div>

          {/* Source toggle */}
          <div className="flex gap-0.5 border border-gray-700 rounded p-0.5">
            {VIEWS.map(v => (
              <button key={v.key} onClick={() => setView(v.key)}
                className={`px-2 py-0.5 rounded text-xs transition-colors
                  ${view === v.key
                    ? (v.key.startsWith('vs') ? 'bg-indigo-600 text-white' : 'bg-blue-600 text-white')
                    : 'text-gray-400 hover:text-white'}`}>
                {v.label}
              </button>
            ))}
          </div>

          {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
          {error   && <span className="text-xs text-red-400">{error}</span>}
        </div>
      </div>

      {/* Body */}
      {!data && !loading && (
        <div className="py-8 text-center text-gray-600 text-sm">Select a ticker to load stats</div>
      )}
      {data && (
        <MatrixTable
          matrix={data.matrix}
          benchMatrix={benchMatrix}
          view={view}
          ticker={ticker}
        />
      )}
    </div>
  )
}
