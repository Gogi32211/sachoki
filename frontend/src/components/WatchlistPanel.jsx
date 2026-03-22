import { useEffect, useState, useCallback } from 'react'
import { api } from '../api'

function SigBadge({ sig_id, sig_name, is_bull, is_bear }) {
  if (!sig_name || sig_name === 'NONE') return null
  const cls = is_bull
    ? 'bg-green-700 text-green-100'
    : is_bear
    ? 'bg-red-700 text-red-100'
    : 'bg-gray-700 text-gray-200'
  return (
    <span className={`text-xs font-bold px-2 py-0.5 rounded ${cls}`}>
      {sig_name}
    </span>
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
    <div className="bg-gray-900 rounded-xl border border-gray-800 h-full">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <span className="font-semibold text-sm">Watchlist</span>
        {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
      </div>

      <div className="divide-y divide-gray-800">
        {data.length === 0 && !loading && (
          <div className="px-4 py-6 text-center text-gray-600 text-sm">No tickers</div>
        )}
        {data.map((row) => (
          <button
            key={row.ticker}
            onClick={() => onSelect(row.ticker)}
            className={`w-full text-left px-4 py-3 hover:bg-gray-800 transition-colors
              ${selected === row.ticker ? 'bg-gray-800 border-l-2 border-blue-500' : ''}`}
          >
            <div className="flex items-center justify-between">
              <span className="font-bold text-sm">{row.ticker}</span>
              <SigBadge {...row} />
            </div>
            {row.error ? (
              <div className="text-xs text-red-400 mt-0.5">{row.error}</div>
            ) : (
              <div className="flex items-center gap-2 mt-0.5">
                <span className="text-sm">${row.price?.toFixed(2)}</span>
                <span
                  className={`text-xs ${
                    row.change_pct >= 0 ? 'text-green-400' : 'text-red-400'
                  }`}
                >
                  {row.change_pct >= 0 ? '+' : ''}
                  {row.change_pct?.toFixed(2)}%
                </span>
              </div>
            )}
          </button>
        ))}
      </div>
    </div>
  )
}
