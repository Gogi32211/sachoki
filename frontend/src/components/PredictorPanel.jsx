import { useEffect, useState } from 'react'
import { api } from '../api'

// ── T/Z outcome table ─────────────────────────────────────────────────────────
function TZOutcomeTable({ data, title, color }) {
  return (
    <div className="flex-1 min-w-0">
      <div className={`px-3 py-2 rounded-t-lg text-sm font-bold ${color}`}>
        {title}
      </div>
      <div className="border border-gray-800 rounded-b-lg overflow-hidden">
        <div className="px-3 py-2 bg-gray-900 border-b border-gray-800 text-xs text-gray-400">
          <span className="font-mono">{data.pattern || '—'}</span>
          {data.signals && (
            <span className="ml-2 text-gray-500 text-xs">{data.signals}</span>
          )}
        </div>
        <div className="px-3 py-1.5 bg-gray-900 border-b border-gray-800 text-xs text-gray-500">
          {data.total_matches} historical matches
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

// ── L-combo outcome table ─────────────────────────────────────────────────────
function LOutcomeTable({ data, title, color }) {
  return (
    <div className="flex-1 min-w-0">
      <div className={`px-3 py-2 rounded-t-lg text-sm font-bold ${color}`}>
        {title}
      </div>
      <div className="border border-gray-800 rounded-b-lg overflow-hidden">
        <div className="px-3 py-2 bg-gray-900 border-b border-gray-800 text-xs text-gray-400">
          <span className="font-mono text-cyan-400">{data.pattern || '—'}</span>
        </div>
        <div className="px-3 py-1.5 bg-gray-900 border-b border-gray-800 text-xs text-gray-500">
          {data.total_matches} historical matches
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
              <tr
                key={i}
                className={`border-t border-gray-800 ${
                  row.is_bullish === true
                    ? 'bg-green-950/30 text-green-300'
                    : row.is_bullish === false
                    ? 'bg-red-950/30 text-red-300'
                    : 'text-gray-400'
                }`}
              >
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

// ── Main panel ────────────────────────────────────────────────────────────────
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

  const empty = { pattern: '', total_matches: 0, top_outcomes: [] }

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <span className="font-semibold text-sm">Next-Bar Predictor — {ticker}</span>
        {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
        {error && <span className="text-xs text-red-400">{error}</span>}
      </div>

      {/* 2×2 grid: TZ 3-bar | TZ 2-bar / L 3-bar | L 2-bar */}
      <div className="p-3 grid grid-cols-1 md:grid-cols-2 gap-3">
        {/* Row 1: T/Z predictors */}
        <TZOutcomeTable
          data={data?.tz_3bar ?? empty}
          title="T/Z 3-Bar Pattern"
          color="bg-blue-900/50 text-blue-300"
        />
        <TZOutcomeTable
          data={data?.tz_2bar ?? empty}
          title="T/Z 2-Bar Pattern"
          color="bg-orange-900/50 text-orange-300"
        />

        {/* Row 2: L-signal predictors */}
        <LOutcomeTable
          data={data?.l_3bar ?? empty}
          title="L-Signal 3-Bar Pattern"
          color="bg-teal-900/50 text-teal-300"
        />
        <LOutcomeTable
          data={data?.l_2bar ?? empty}
          title="L-Signal 2-Bar Pattern"
          color="bg-amber-900/50 text-amber-300"
        />
      </div>

      {!data && !loading && (
        <div className="pb-6 text-center text-gray-600 text-sm">Select a ticker</div>
      )}
    </div>
  )
}
