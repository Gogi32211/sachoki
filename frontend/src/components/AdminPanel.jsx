import { useState, useEffect, useRef } from 'react'
import { api } from '../api'

const UNIVERSES = [
  { key: 'sp500',      label: 'S&P 500' },
  { key: 'nasdaq_low', label: 'NASDAQ $3–20' },
  { key: 'nasdaq_mid', label: 'NASDAQ $21–50' },
  { key: 'russell2k',  label: 'Russell 2000' },
  { key: 'all_us',     label: 'All US' },
]
const TFS = ['1d', '4h', '1h', '1wk']

function fmt(sec) {
  if (sec == null) return '—'
  const m = Math.floor(sec / 60), s = Math.floor(sec % 60)
  return m > 0 ? `${m}m ${s}s` : `${s}s`
}

function pct(done, total) {
  if (!total) return 0
  return Math.min(100, Math.round((done / total) * 100))
}

export default function AdminPanel() {
  const [status,   setStatus]   = useState(null)
  const [history,  setHistory]  = useState([])
  const [universe, setUniverse] = useState('sp500')
  const [tf,       setTf]       = useState('1d')
  const [error,    setError]    = useState(null)
  const pollRef = useRef(null)

  const fetchStatus = () =>
    api.turboScanStatus().then(setStatus).catch(() => {})

  const fetchHistory = () =>
    api.adminScanHistory().then(setHistory).catch(() => {})

  // poll every 2s while running
  useEffect(() => {
    fetchStatus()
    fetchHistory()
    pollRef.current = setInterval(() => {
      fetchStatus()
    }, 2000)
    return () => clearInterval(pollRef.current)
  }, [])

  // refresh history when scan finishes
  const prevRunning = useRef(false)
  useEffect(() => {
    if (prevRunning.current && status && !status.running) {
      fetchHistory()
    }
    prevRunning.current = status?.running ?? false
  }, [status?.running])

  const startScan = () => {
    setError(null)
    api.adminScanStart(tf, universe)
      .then(() => fetchStatus())
      .catch(e => setError(e?.detail || e?.message || String(e)))
  }

  const resetScan = () => {
    api.turboScanReset().then(() => fetchStatus())
  }

  const running   = status?.running ?? false
  const done      = status?.done ?? 0
  const total     = status?.total ?? 0
  const found     = status?.found ?? 0
  const failed    = status?.failed ?? 0
  const fetched   = status?.fetched_from_massive ?? 0
  const elapsed   = status?.elapsed ?? 0
  const eta       = status?.eta
  const scanErr   = status?.error
  const progress  = pct(done, total)

  return (
    <div className="p-4 max-w-3xl mx-auto text-gray-100 text-sm space-y-6">

      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <h2 className="text-base font-semibold text-white">Scan Admin</h2>
        <span className={`px-2 py-0.5 rounded text-xs font-medium ${
          running ? 'bg-yellow-700 text-yellow-100' : 'bg-gray-700 text-gray-300'
        }`}>
          {running ? '⟳ Running' : 'Idle'}
        </span>
      </div>

      {/* ── Start Controls ── */}
      <div className="bg-gray-900 rounded-lg p-4 space-y-3 border border-gray-800">
        <div className="text-xs text-gray-400 font-medium uppercase tracking-wide">Start New Scan</div>
        <div className="flex flex-wrap gap-2 items-center">
          <div className="flex gap-1">
            {UNIVERSES.map(u => (
              <button key={u.key}
                onClick={() => setUniverse(u.key)}
                disabled={running}
                className={`px-2.5 py-1 rounded text-xs border transition-colors disabled:opacity-40
                  ${universe === u.key
                    ? 'bg-blue-800 border-blue-500 text-white'
                    : 'bg-gray-800 border-gray-700 text-gray-400 hover:border-gray-500'}`}>
                {u.label}
              </button>
            ))}
          </div>
          <div className="flex gap-1">
            {TFS.map(t => (
              <button key={t}
                onClick={() => setTf(t)}
                disabled={running}
                className={`px-2.5 py-1 rounded text-xs border transition-colors disabled:opacity-40
                  ${tf === t
                    ? 'bg-purple-800 border-purple-500 text-white'
                    : 'bg-gray-800 border-gray-700 text-gray-400 hover:border-gray-500'}`}>
                {t}
              </button>
            ))}
          </div>
          <button
            onClick={startScan}
            disabled={running}
            className="px-4 py-1.5 rounded text-xs font-semibold bg-yellow-600 hover:bg-yellow-500 text-black disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
            ⚡ Start Scan
          </button>
          {running && (
            <button onClick={resetScan}
              className="px-3 py-1.5 rounded text-xs border border-red-700 text-red-400 hover:bg-red-900/40">
              Force Stop
            </button>
          )}
        </div>
        {error && <div className="text-red-400 text-xs">{error}</div>}
      </div>

      {/* ── Live Progress ── */}
      {(running || scanErr || (status && done > 0)) && (
        <div className="bg-gray-900 rounded-lg p-4 border border-gray-800 space-y-3">
          <div className="text-xs text-gray-400 font-medium uppercase tracking-wide">
            {running ? 'Live Progress' : 'Last Scan Result'}
          </div>

          {/* progress bar */}
          <div className="w-full bg-gray-800 rounded-full h-2">
            <div
              className={`h-2 rounded-full transition-all ${running ? 'bg-yellow-500' : 'bg-green-600'}`}
              style={{ width: `${progress}%` }}
            />
          </div>

          {/* stats grid */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {[
              { label: 'Fetched from Massive', value: fetched || total || '—', color: 'text-blue-400' },
              { label: 'Scanned',  value: `${done} / ${total}`,  color: 'text-gray-200' },
              { label: 'Into Turbo', value: found, color: 'text-green-400' },
              { label: 'Skipped / Failed', value: failed, color: failed > 0 ? 'text-red-400' : 'text-gray-500' },
              { label: 'Progress', value: `${progress}%`, color: 'text-yellow-400' },
              { label: 'Elapsed', value: fmt(elapsed), color: 'text-gray-300' },
              { label: 'ETA', value: running ? fmt(eta) : '—', color: 'text-gray-400' },
              { label: 'Universe', value: status?.universe ?? '—', color: 'text-gray-400' },
            ].map(({ label, value, color }) => (
              <div key={label} className="bg-gray-800 rounded p-2">
                <div className="text-gray-500 text-xs">{label}</div>
                <div className={`font-mono font-semibold text-sm ${color}`}>{String(value)}</div>
              </div>
            ))}
          </div>

          {scanErr && (
            <div className="bg-red-900/30 border border-red-700 rounded p-2 text-red-300 text-xs">
              Error: {scanErr}
            </div>
          )}
          {!running && !scanErr && done > 0 && (
            <div className="text-green-400 text-xs">✓ Scan completed — {found} tickers pushed to Turbo</div>
          )}
        </div>
      )}

      {/* ── Scan History ── */}
      <div className="bg-gray-900 rounded-lg p-4 border border-gray-800 space-y-2">
        <div className="text-xs text-gray-400 font-medium uppercase tracking-wide">Scan History (last 20)</div>
        {history.length === 0
          ? <div className="text-gray-600 text-xs">No scans yet</div>
          : (
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-500 border-b border-gray-800">
                  <th className="text-left py-1 pr-3">#</th>
                  <th className="text-left py-1 pr-3">Universe</th>
                  <th className="text-left py-1 pr-3">TF</th>
                  <th className="text-left py-1 pr-3">Started</th>
                  <th className="text-left py-1 pr-3">Duration</th>
                  <th className="text-right py-1">Results</th>
                </tr>
              </thead>
              <tbody>
                {history.map(r => {
                  const dur = r.started_at && r.completed_at
                    ? Math.round((new Date(r.completed_at) - new Date(r.started_at)) / 1000)
                    : null
                  const complete = r.completed_at != null
                  return (
                    <tr key={r.id} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                      <td className="py-1 pr-3 text-gray-600">{r.id}</td>
                      <td className="py-1 pr-3 text-gray-300">{r.universe}</td>
                      <td className="py-1 pr-3 text-gray-400">{r.tf}</td>
                      <td className="py-1 pr-3 text-gray-500">{r.started_at?.slice(0, 16).replace('T', ' ')}</td>
                      <td className="py-1 pr-3 text-gray-400">{complete ? fmt(dur) : <span className="text-yellow-500">running…</span>}</td>
                      <td className="py-1 text-right font-mono">
                        <span className={r.result_count > 0 ? 'text-green-400' : 'text-gray-500'}>
                          {r.result_count ?? '—'}
                        </span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}
      </div>

    </div>
  )
}
