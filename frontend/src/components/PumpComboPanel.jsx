import { useState, useEffect } from 'react'
import { api } from '../api'

const THRESHOLDS = [
  { label: '2x (100%)', value: 2.0 },
  { label: '3x (200%)', value: 3.0 },
  { label: '5x (400%)', value: 5.0 },
]
const WINDOWS = [
  { label: '10 days', value: 10 },
  { label: '20 days', value: 20 },
  { label: '30 days', value: 30 },
]
const COMBO_LENS = [
  { label: '2-bar', value: 2 },
  { label: '3-bar', value: 3 },
]

export default function PumpComboPanel() {
  const [combos, setCombos]     = useState([])
  const [loading, setLoading]   = useState(false)
  const [mining, setMining]     = useState(false)
  const [error, setError]       = useState(null)
  const [threshold, setThreshold] = useState(2.0)
  const [window, setWindow]     = useState(20)
  const [comboLen, setComboLen] = useState(3)

  const load = () => {
    setLoading(true)
    setError(null)
    api.pumpCombos(threshold, window, comboLen)
      .then(d => setCombos(d.combos || []))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [threshold, window, comboLen])

  const mine = () => {
    setMining(true)
    setError(null)
    api.pumpTrigger(threshold, window, comboLen)
      .then(() => {
        // Poll every 30s until results arrive (mining takes ~15 min)
        const poll = () => {
          api.pumpCombos(threshold, window, comboLen)
            .then(d => {
              if (d.combos && d.combos.length > 0) {
                setCombos(d.combos)
                setMining(false)
              } else {
                setTimeout(poll, 30_000)
              }
            })
            .catch(() => setTimeout(poll, 30_000))
        }
        setTimeout(poll, 30_000)
      })
      .catch(e => { setError(e.message); setMining(false) })
  }

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <span className="font-semibold text-sm text-white">Pump Combo Miner</span>
        <button
          onClick={mine}
          disabled={mining}
          className="text-xs px-3 py-1 bg-purple-600 hover:bg-purple-500 disabled:opacity-50 rounded text-white"
        >
          {mining ? 'Mining…' : 'Mine Pumps'}
        </button>
      </div>

      {/* Controls */}
      <div className="flex gap-2 px-4 py-2 border-b border-gray-800 flex-wrap">
        <select
          value={threshold}
          onChange={e => setThreshold(Number(e.target.value))}
          className="text-xs bg-gray-800 text-gray-300 border border-gray-700 rounded px-2 py-1"
        >
          {THRESHOLDS.map(t => (
            <option key={t.value} value={t.value}>{t.label}</option>
          ))}
        </select>
        <select
          value={window}
          onChange={e => setWindow(Number(e.target.value))}
          className="text-xs bg-gray-800 text-gray-300 border border-gray-700 rounded px-2 py-1"
        >
          {WINDOWS.map(w => (
            <option key={w.value} value={w.value}>{w.label}</option>
          ))}
        </select>
        <select
          value={comboLen}
          onChange={e => setComboLen(Number(e.target.value))}
          className="text-xs bg-gray-800 text-gray-300 border border-gray-700 rounded px-2 py-1"
        >
          {COMBO_LENS.map(c => (
            <option key={c.value} value={c.value}>{c.label}</option>
          ))}
        </select>
      </div>

      {/* Mining status */}
      {mining && (
        <div className="px-4 py-2 text-xs text-purple-400 animate-pulse">
          Mining 700 tickers… this takes ~15 min. Results will appear when done.
        </div>
      )}

      {error && (
        <div className="px-4 py-2 text-xs text-red-400">{error}</div>
      )}

      {/* Table */}
      <div className="overflow-auto flex-1">
        {loading ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">Loading…</div>
        ) : combos.length === 0 ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">
            No results yet — click "Mine Pumps" to start.
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-800">
                <th className="text-left px-3 py-2">Combo</th>
                <th className="text-right px-3 py-2">Count</th>
                <th className="text-right px-3 py-2">Avg Gain</th>
                <th className="text-right px-3 py-2">Max Gain</th>
              </tr>
            </thead>
            <tbody>
              {combos.map((row, i) => (
                <tr
                  key={i}
                  className="border-b border-gray-800/50 hover:bg-gray-800/40"
                >
                  <td className="px-3 py-2 text-purple-300 font-mono">{row.combo}</td>
                  <td className="text-right px-3 py-2 text-white">{row.count}</td>
                  <td className="text-right px-3 py-2 text-green-400">
                    +{row.avg_gain_pct?.toFixed(1)}%
                  </td>
                  <td className="text-right px-3 py-2 text-emerald-300">
                    +{row.max_gain_pct?.toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
