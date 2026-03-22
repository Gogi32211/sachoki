import { useEffect, useState } from 'react'
import { api } from '../api'

const L_COLS = ["L1", "L2", "L3", "L4", "L5", "L6", "L34", "L22", "L64", "L43", "L1L2", "L2L5"]

function CellColor({ meta }) {
  if (!meta) return null
  const { count, pct, color } = meta
  if (count === 0) return <span className="text-gray-700 text-xs">—</span>

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

export default function TZLStatsPanel({ ticker, tf }) {
  const [matrix, setMatrix] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!ticker) return
    setLoading(true)
    setError(null)
    api.tzLStats(ticker, tf)
      .then(d => setMatrix(d.matrix))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, tf])

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 flex flex-col">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <span className="font-semibold text-sm">T/Z × L Co-occurrence — {ticker}</span>
        <div className="flex items-center gap-3">
          {/* Legend */}
          <div className="flex items-center gap-2 text-xs">
            <span className="px-1.5 py-0.5 rounded bg-yellow-900/60 text-yellow-300 ring-1 ring-yellow-500">Gold</span>
            <span className="text-gray-500">= best match</span>
            <span className="px-1.5 py-0.5 rounded bg-green-900/50 text-green-300">Green</span>
            <span className="text-gray-500">= aligned</span>
            <span className="px-1.5 py-0.5 rounded bg-red-900/50 text-red-300">Red</span>
            <span className="text-gray-500">= conflict</span>
          </div>
          {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
          {error && <span className="text-xs text-red-400">{error}</span>}
        </div>
      </div>

      <div className="overflow-auto flex-1 p-3">
        {!matrix && !loading && (
          <div className="py-8 text-center text-gray-600 text-sm">Select a ticker to load stats</div>
        )}

        {matrix && (
          <table className="text-xs w-full border-separate border-spacing-0">
            <thead>
              <tr>
                <th className="text-left px-3 py-2 text-gray-500 sticky left-0 bg-gray-900 z-10 border-b border-gray-800 min-w-[60px]">
                  Signal
                </th>
                <th className="text-center px-1 py-2 text-gray-500 border-b border-gray-800 min-w-[32px]">n</th>
                {L_COLS.map(col => (
                  <th
                    key={col}
                    className="text-center px-1 py-2 text-gray-400 border-b border-gray-800 min-w-[52px] font-mono"
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {/* Separator between T and Z groups */}
              {matrix.map((row, idx) => (
                <>
                  {idx === 11 && (
                    <tr key="separator">
                      <td colSpan={L_COLS.length + 2} className="py-1">
                        <div className="border-t border-gray-700" />
                      </td>
                    </tr>
                  )}
                  <tr
                    key={row.sig_id}
                    className={`border-b border-gray-800/40 hover:bg-gray-800/30 ${
                      row.is_bull ? '' : 'opacity-90'
                    }`}
                  >
                    <td className="sticky left-0 bg-gray-900 z-10 px-3 py-1.5 border-b border-gray-800/40">
                      <span className={`font-mono font-bold text-xs ${
                        row.is_bull ? 'text-green-400' : 'text-red-400'
                      }`}>
                        {row.sig_name}
                      </span>
                    </td>
                    <td className="text-center px-1 py-1.5 text-gray-500">{row.total}</td>
                    {L_COLS.map(col => (
                      <td key={col} className="text-center px-1 py-1.5">
                        <CellColor meta={row.cols?.[col]} />
                      </td>
                    ))}
                  </tr>
                </>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
