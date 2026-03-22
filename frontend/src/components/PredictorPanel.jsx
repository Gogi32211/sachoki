import { useEffect, useState } from 'react'
import { api } from '../api'

function OutcomeTable({ data, title, color }) {
  return (
    <div className="flex-1 min-w-0">
      <div className={`px-3 py-2 rounded-t-lg text-sm font-bold ${color}`}>
        {title}
      </div>
      <div className="border border-gray-800 rounded-b-lg overflow-hidden">
        {/* Pattern */}
        <div className="px-3 py-2 bg-gray-900 border-b border-gray-800 text-xs text-gray-400">
          <span className="font-mono">{data.pattern || '—'}</span>
          {data.signals && (
            <span className="ml-2 text-gray-500">{data.signals}</span>
          )}
        </div>

        {/* Total */}
        <div className="px-3 py-1.5 bg-gray-900 border-b border-gray-800 text-xs text-gray-500">
          {data.total_matches} historical matches
        </div>

        {/* Rows */}
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
            {data.top_outcomes?.length === 0 && (
              <tr>
                <td colSpan={4} className="px-3 py-4 text-center text-gray-600 text-xs">
                  No data
                </td>
              </tr>
            )}
            {data.top_outcomes?.map((row, i) => (
              <tr
                key={i}
                className={`border-t border-gray-800 ${
                  row.is_bull
                    ? 'bg-green-950/30 text-green-300'
                    : row.is_bear
                    ? 'bg-red-950/30 text-red-300'
                    : 'text-gray-400'
                }`}
              >
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

export default function PredictorPanel({ ticker, tf }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    setError(null)
    setLoading(true)
    api.predict(ticker, tf)
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, tf])

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <span className="font-semibold text-sm">Next-Bar Predictor — {ticker}</span>
        {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
        {error && <span className="text-xs text-red-400">{error}</span>}
      </div>

      <div className="p-3 flex gap-3">
        {data ? (
          <>
            <OutcomeTable
              data={data['3bar']}
              title="3-Bar Pattern"
              color="bg-blue-900/50 text-blue-300"
            />
            <OutcomeTable
              data={data['2bar']}
              title="2-Bar Pattern"
              color="bg-orange-900/50 text-orange-300"
            />
          </>
        ) : (
          !loading && (
            <div className="w-full py-8 text-center text-gray-600 text-sm">
              Select a ticker
            </div>
          )
        )}
      </div>
    </div>
  )
}
