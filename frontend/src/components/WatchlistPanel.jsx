import { useEffect, useState, useCallback } from 'react'
import { api } from '../api'

function SigBadge({ sig_id, sig_name, is_bull, is_bear }) {
  if (!sig_name || sig_name === 'NONE') return null
  const cls = is_bull
    ? 'bg-green-900/60 text-green-300'
    : is_bear
    ? 'bg-red-900/60 text-red-300'
    : 'bg-gray-700 text-gray-300'
  return (
    <span className={`text-xs font-bold px-1.5 py-0.5 rounded font-mono ${cls}`}>
      {sig_name}
    </span>
  )
}

function ScoreBar({ bull, bear }) {
  if (!bull && !bear) return null
  const score = bull || bear
  const isBull = !!bull
  const pct = Math.min(score / 10, 1) * 100
  return (
    <div className="flex items-center gap-1 mt-0.5">
      <div className="flex-1 bg-gray-800 rounded-full h-1 overflow-hidden">
        <div
          className={`h-full rounded-full ${isBull ? 'bg-green-500' : 'bg-red-500'}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className={`text-xs font-bold ${isBull ? 'text-green-400' : 'text-red-400'}`}>
        {score}
      </span>
    </div>
  )
}

export default function WatchlistPanel({ tickers, tf, selected, onSelect }) {
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)

  const refresh = useCallback(async () => {
    if (!tickers.length) return
    setLoading(true)
    try {
      const res = await api.watchlist(tickers, tf)
      setData(res)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [tickers, tf])

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 60_000)
    return () => clearInterval(id)
  }, [refresh])

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 h-full overflow-auto">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <span className="font-semibold text-sm text-white">Watchlist</span>
        {loading && <span className="text-xs text-gray-500 animate-pulse">refreshing…</span>}
      </div>

      <div className="divide-y divide-gray-800">
        {data.length === 0 && !loading && (
          <div className="px-4 py-6 text-center text-gray-600 text-sm">No tickers</div>
        )}
        {data.map((row) => (
          <button
            key={row.ticker}
            onClick={() => onSelect(row.ticker)}
            className={`w-full text-left px-4 py-2.5 hover:bg-gray-800 transition-colors
              ${selected === row.ticker ? 'bg-gray-800 border-l-2 border-blue-500' : ''}`}
          >
            <div className="flex items-center justify-between">
              <span className="font-bold text-sm text-white">{row.ticker}</span>
              <div className="flex items-center gap-1.5">
                {row.l_signal && (
                  <span className="text-xs text-cyan-400 font-mono">{row.l_signal}</span>
                )}
                <SigBadge {...row} />
              </div>
            </div>
            {row.error ? (
              <div className="text-xs text-red-400 mt-0.5">{row.error}</div>
            ) : (
              <>
                <div className="flex items-center gap-2 mt-0.5">
                  <span className="text-sm text-gray-200">${row.price?.toFixed(2)}</span>
                  <span className={`text-xs font-medium
                    ${(row.change_pct ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {(row.change_pct ?? 0) >= 0 ? '+' : ''}
                    {row.change_pct?.toFixed(2)}%
                  </span>
                </div>
                <ScoreBar bull={row.bull_score} bear={row.bear_score} />
              </>
            )}
          </button>
        ))}
      </div>
    </div>
  )
}
