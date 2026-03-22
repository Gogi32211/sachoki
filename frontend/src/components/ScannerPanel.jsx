import { useState, useEffect, useCallback, useRef } from 'react'
import { api } from '../api'

function SigBadge({ sig_id, sig_name }) {
  const bull = sig_id >= 1 && sig_id <= 11
  const bear = sig_id >= 12 && sig_id <= 25
  if (!bull && !bear) return null
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-xs font-mono font-semibold
      ${bull ? 'bg-green-900/50 text-green-300' : 'bg-red-900/50 text-red-300'}`}>
      {sig_name}
    </span>
  )
}

export default function ScannerPanel({ tf, onSelectTicker }) {
  const [results, setResults] = useState([])
  const [lastScan, setLastScan] = useState(null)
  const [loading, setLoading]  = useState(false)
  const [scanning, setScanning] = useState(false)
  const [progress, setProgress] = useState(null)   // { done, total, found }
  const [error, setError]      = useState(null)
  const pollRef = useRef(null)

  const load = useCallback(() => {
    setLoading(true)
    setError(null)
    api.scanResults(tf, 100)
      .then(d => {
        setResults(d.results || [])
        setLastScan(d.last_scan)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [tf])

  useEffect(() => { load() }, [load])

  // Poll /api/scan/status while scanning
  const startPolling = () => {
    if (pollRef.current) return
    pollRef.current = setInterval(() => {
      api.scanStatus()
        .then(s => {
          setProgress({ done: s.done, total: s.total, found: s.found })
          if (!s.running) {
            stopPolling()
            setScanning(false)
            setProgress(null)
            load()
          }
        })
        .catch(() => {})
    }, 1000)
  }

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current)
      pollRef.current = null
    }
  }

  useEffect(() => () => stopPolling(), [])

  const scan = () => {
    setScanning(true)
    setProgress({ done: 0, total: 0, found: 0 })
    setError(null)
    api.scanTrigger(tf)
      .then(() => startPolling())
      .catch(e => { setError(e.message); setScanning(false); setProgress(null) })
  }

  const fmtTime = (iso) => {
    if (!iso) return null
    try { return new Date(iso).toLocaleString() } catch { return iso }
  }

  const pct = progress && progress.total > 0
    ? Math.round((progress.done / progress.total) * 100)
    : null

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 flex flex-col h-full">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <div className="flex items-center gap-3">
          <span className="font-semibold text-sm text-white">T/Z Scanner</span>
          {lastScan && !scanning && (
            <span className="text-xs text-gray-500">Last: {fmtTime(lastScan)}</span>
          )}
        </div>
        <button
          onClick={scan}
          disabled={scanning}
          className="text-xs px-3 py-1 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 rounded text-white"
        >
          {scanning ? 'Scanning…' : 'Scan Now'}
        </button>
      </div>

      {/* Progress bar */}
      {scanning && progress && (
        <div className="px-4 py-2 border-b border-gray-800">
          <div className="flex items-center justify-between text-xs text-gray-400 mb-1">
            <span>
              {progress.total > 0
                ? `${progress.done} / ${progress.total} tickers`
                : 'Starting…'}
            </span>
            <span>{progress.found} signals found</span>
          </div>
          <div className="w-full bg-gray-700 rounded-full h-1.5">
            <div
              className="bg-indigo-500 h-1.5 rounded-full transition-all duration-300"
              style={{ width: pct != null ? `${pct}%` : '0%' }}
            />
          </div>
          {pct != null && (
            <div className="text-right text-xs text-gray-500 mt-0.5">{pct}%</div>
          )}
        </div>
      )}

      {error && <div className="px-4 py-2 text-xs text-red-400">{error}</div>}

      <div className="overflow-auto flex-1">
        {loading ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">Loading…</div>
        ) : results.length === 0 ? (
          <div className="px-4 py-8 text-center text-gray-500 text-xs">
            No results — trigger a scan first.
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-800 sticky top-0 bg-gray-900">
                <th className="text-left px-3 py-2">Ticker</th>
                <th className="text-center px-2 py-2">Signal</th>
                <th className="text-left px-2 py-2 hidden md:table-cell">3-Bar Pattern</th>
                <th className="text-right px-2 py-2">Price</th>
                <th className="text-right px-2 py-2">Chg%</th>
              </tr>
            </thead>
            <tbody>
              {results.map((row, i) => (
                <tr
                  key={i}
                  onClick={() => onSelectTicker?.(row.ticker)}
                  className="border-b border-gray-800/40 cursor-pointer hover:bg-gray-800/50"
                >
                  <td className="px-3 py-2 font-semibold text-white">{row.ticker}</td>
                  <td className="text-center px-2 py-2">
                    <SigBadge sig_id={row.sig_id} sig_name={row.sig_name} />
                  </td>
                  <td className="px-2 py-2 text-gray-400 font-mono hidden md:table-cell max-w-[160px] truncate">
                    {row.pattern_3bar}
                  </td>
                  <td className="text-right px-2 py-2 text-gray-200">
                    {row.last_price ? `$${Number(row.last_price).toFixed(2)}` : '—'}
                  </td>
                  <td className={`text-right px-2 py-2 font-medium
                    ${(row.change_pct ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {row.change_pct != null
                      ? `${row.change_pct >= 0 ? '+' : ''}${Number(row.change_pct).toFixed(2)}%`
                      : '—'}
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
