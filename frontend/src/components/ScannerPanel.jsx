import { useEffect, useState } from 'react'
import { api } from '../api'

function SigBadge({ sig_id, sig_name }) {
  const bull = sig_id >= 1 && sig_id <= 11
  const cls = bull ? 'bg-green-700 text-green-100' : 'bg-red-700 text-red-100'
  return (
    <span className={`text-xs font-bold px-2 py-0.5 rounded font-mono ${cls}`}>
      {sig_name}
    </span>
  )
}

export default function ScannerPanel({ tf }) {
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [scanning, setScanning] = useState(false)
  const [lastScan, setLastScan] = useState(null)

  function loadResults() {
    setLoading(true)
    api.scanResults(tf, 50)
      .then((data) => {
        setRows(data)
        if (data.length) setLastScan(data[0].scanned_at)
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }

  function triggerScan() {
    setScanning(true)
    api.scanTrigger(tf)
      .then(() => {
        // Poll for results after 30s
        setTimeout(loadResults, 30_000)
      })
      .catch(console.error)
      .finally(() => setTimeout(() => setScanning(false), 30_000))
  }

  useEffect(() => {
    loadResults()
  }, [tf])

  const ts = lastScan
    ? new Date(lastScan).toLocaleTimeString()
    : 'never'

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <div>
          <span className="font-semibold text-sm">Scanner</span>
          <span className="ml-2 text-xs text-gray-500">last: {ts}</span>
        </div>
        <button
          onClick={triggerScan}
          disabled={scanning}
          className="bg-blue-700 hover:bg-blue-600 disabled:opacity-50 rounded px-3 py-1 text-xs font-semibold"
        >
          {scanning ? 'Scanning…' : 'Scan Now'}
        </button>
      </div>

      <div className="overflow-auto max-h-72">
        {loading && (
          <div className="py-6 text-center text-gray-600 text-sm animate-pulse">
            Loading results…
          </div>
        )}
        {!loading && rows.length === 0 && (
          <div className="py-6 text-center text-gray-600 text-sm">
            No results — click Scan Now
          </div>
        )}
        {rows.length > 0 && (
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-gray-500 bg-gray-950 sticky top-0">
                <th className="px-3 py-2 text-left">Ticker</th>
                <th className="px-3 py-2 text-left">Signal</th>
                <th className="px-3 py-2 text-left">Pattern (3-bar)</th>
                <th className="px-3 py-2 text-right">Time</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr
                  key={i}
                  className="border-t border-gray-800 hover:bg-gray-800 transition-colors"
                >
                  <td className="px-3 py-2 font-bold font-mono">{r.ticker}</td>
                  <td className="px-3 py-2">
                    <SigBadge sig_id={r.sig_id} sig_name={r.sig_name} />
                  </td>
                  <td className="px-3 py-2 text-gray-400 text-xs font-mono">
                    {r.pattern_3bar}
                  </td>
                  <td className="px-3 py-2 text-right text-xs text-gray-500">
                    {r.scanned_at
                      ? new Date(r.scanned_at).toLocaleTimeString()
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
