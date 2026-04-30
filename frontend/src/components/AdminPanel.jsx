import { useState, useEffect, useRef } from 'react'
import { api } from '../api'
import { turboCacheSet } from './TurboScanPanel'
import { getCacheBackend, setCacheBackend } from '../turboCache'

const UNIVERSES = [
  { key: 'sp500',     label: 'S&P 500'    },
  { key: 'nasdaq',    label: 'NASDAQ'      },
  { key: 'russell2k', label: 'Russell 2K'  },
  { key: 'all_us',    label: 'All US'      },
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

function NumInput({ label, value, onChange, min, max, step = 1, placeholder }) {
  return (
    <div className="flex flex-col gap-0.5">
      <label className="text-gray-500 text-xs">{label}</label>
      <input
        type="number" value={value} min={min} max={max} step={step}
        placeholder={placeholder}
        onChange={e => onChange(e.target.value === '' ? '' : Number(e.target.value))}
        className="w-20 px-2 py-1 rounded bg-gray-800 border border-gray-700 text-gray-200 text-xs focus:border-blue-500 outline-none"
      />
    </div>
  )
}

export default function AdminPanel() {
  const [status,   setStatus]   = useState(null)
  const [history,  setHistory]  = useState([])
  const [universe, setUniverse] = useState('sp500')
  const [tf,       setTf]       = useState('1d')
  const [error,    setError]    = useState(null)
  const [priceMin, setPriceMin] = useState('')
  const [priceMax, setPriceMax] = useState('')
  const [rsiMin,   setRsiMin]   = useState('')
  const [rsiMax,   setRsiMax]   = useState('')
  const [cciMin,   setCciMin]   = useState('')
  const [cciMax,   setCciMax]   = useState('')
  const [caching,     setCaching]     = useState(false)
  const [cacheMode,   setCacheModeS]  = useState(() => getCacheBackend())
  const pollRef    = useRef(null)
  const scanParams = useRef({ tf: '1d', uni: 'sp500' })

  // ── Stock Stat state ──
  const [ssUni,    setSsUni]    = useState('sp500')
  const [ssTf,     setSsTf]     = useState('1d')
  const [ssBars,   setSsBars]   = useState(60)
  const [ssStatus, setSsStatus] = useState(null)
  const [ssError,  setSsError]  = useState(null)
  const ssPollRef  = useRef(null)

  const setCacheMode = (val) => { setCacheBackend(val); setCacheModeS(val) }

  const filters = {
    price_min: priceMin !== '' ? Number(priceMin) : 0,
    price_max: priceMax !== '' ? Number(priceMax) : 1e9,
    rsi_min:   rsiMin   !== '' ? Number(rsiMin)   : 0,
    rsi_max:   rsiMax   !== '' ? Number(rsiMax)   : 100,
    cci_min:   cciMin   !== '' ? Number(cciMin)   : -9999,
    cci_max:   cciMax   !== '' ? Number(cciMax)   : 9999,
  }

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

  // refresh history + cache results when scan finishes
  const prevRunning = useRef(false)
  useEffect(() => {
    if (prevRunning.current && status && !status.running && !status.error) {
      fetchHistory()
      // Save scan results to localStorage so Turbo panel shows them instantly
      const { tf: scanTf, uni: scanUni } = scanParams.current
      setCaching(true)
      api.turboScan(10000, 0, 'all', scanTf, scanUni, {})
        .then(d => {
          const results = d.results || []
          if (results.length > 0) {
            turboCacheSet(scanTf, scanUni, results, d.last_scan)
            try {
              localStorage.setItem('sachoki_turbo_tf',  scanTf)
              localStorage.setItem('sachoki_turbo_uni', scanUni)
            } catch {}
            window.dispatchEvent(new CustomEvent('sachoki:scan-cached', { detail: { tf: scanTf, uni: scanUni, results, lastScan: d.last_scan } }))
          }
        })
        .finally(() => setCaching(false))
    }
    prevRunning.current = status?.running ?? false
  }, [status?.running])

  // ── Stock Stat polling ──
  const fetchSsStatus = () =>
    api.stockStatStatus().then(setSsStatus).catch(() => {})

  useEffect(() => {
    fetchSsStatus()
    ssPollRef.current = setInterval(fetchSsStatus, 2000)
    return () => clearInterval(ssPollRef.current)
  }, [])

  const startStockStat = () => {
    setSsError(null)
    api.stockStatTrigger(ssTf, ssUni, ssBars)
      .then(() => fetchSsStatus())
      .catch(e => setSsError(e?.detail || e?.message || String(e)))
  }

  const startScan = () => {
    setError(null)
    scanParams.current = { tf, uni: universe }
    api.adminScanStart(tf, universe, cacheMode === 'idb' ? 0 : 5)
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

      {/* ── Cache Backend Toggle ── */}
      <div className="bg-gray-900 rounded-lg p-4 border border-gray-800 space-y-2">
        <div className="text-xs text-gray-400 font-medium uppercase tracking-wide">Cache Backend</div>
        <div className="flex gap-2 flex-wrap items-start">
          {[
            { key: 'ls',  label: 'D+A — localStorage',  desc: 'score≥5 filter · fast · 5 MB limit' },
            { key: 'idb', label: 'C — IndexedDB',        desc: 'all tickers · no size limit · robust' },
          ].map(m => (
            <button key={m.key}
              onClick={() => setCacheMode(m.key)}
              className={`flex flex-col items-start px-3 py-2 rounded border text-xs transition-colors
                ${cacheMode === m.key
                  ? 'bg-teal-900 border-teal-500 text-teal-100'
                  : 'bg-gray-800 border-gray-700 text-gray-400 hover:border-gray-500'}`}>
              <span className="font-semibold">{m.label}</span>
              <span className="text-gray-500 mt-0.5">{m.desc}</span>
            </button>
          ))}
        </div>
        <p className="text-xs text-gray-600">
          {cacheMode === 'idb'
            ? 'IndexedDB: all tickers stored (no score filter). Scan stores ~5000 results.'
            : 'localStorage: only score≥5 tickers stored (~2000). Smaller, faster.'}
        </p>
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
        {/* ── Filters ── */}
        <div className="border-t border-gray-800 pt-3">
          <div className="text-xs text-gray-500 mb-2">Result Filters (applied to displayed results)</div>
          <div className="flex flex-wrap gap-4">
            <div className="flex gap-2 items-end">
              <NumInput label="Price Min $" value={priceMin} onChange={setPriceMin} min={0} step={0.1} placeholder="0" />
              <NumInput label="Price Max $" value={priceMax} onChange={setPriceMax} min={0} step={1}   placeholder="∞" />
            </div>
            <div className="flex gap-2 items-end">
              <NumInput label="RSI Min" value={rsiMin} onChange={setRsiMin} min={0}   max={100} placeholder="0"   />
              <NumInput label="RSI Max" value={rsiMax} onChange={setRsiMax} min={0}   max={100} placeholder="100" />
            </div>
            <div className="flex gap-2 items-end">
              <NumInput label="CCI Min" value={cciMin} onChange={setCciMin} min={-500} max={500} step={10} placeholder="-∞" />
              <NumInput label="CCI Max" value={cciMax} onChange={setCciMax} min={-500} max={500} step={10} placeholder="∞"  />
            </div>
          </div>
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
            <div className="text-green-400 text-xs">
              ✓ Scan completed — {found} tickers
              {caching ? ' · saving to Turbo cache…' : ' · cached in Turbo ✓'}
            </div>
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

      {/* ── Stock Stat ── */}
      <div className="bg-gray-900 rounded-lg p-4 border border-gray-800 space-y-3">
        <div className="text-xs text-gray-400 font-medium uppercase tracking-wide">
          Stock Stat — Bulk Signal CSV
        </div>
        <div className="text-xs text-gray-600">
          Runs bar-level signal computation for every ticker in the universe and exports a combined CSV (same columns as Superchart CSV).
          SP500 ≈ 5–10 min · Russell 2K ≈ 20–40 min · All US ≈ 1–2 h
        </div>
        <div className="flex flex-wrap gap-2 items-end">
          <div className="flex gap-1">
            {UNIVERSES.map(u => (
              <button key={u.key}
                onClick={() => setSsUni(u.key)}
                disabled={ssStatus?.running}
                className={`px-2.5 py-1 rounded text-xs border transition-colors disabled:opacity-40
                  ${ssUni === u.key
                    ? 'bg-blue-800 border-blue-500 text-white'
                    : 'bg-gray-800 border-gray-700 text-gray-400 hover:border-gray-500'}`}>
                {u.label}
              </button>
            ))}
          </div>
          <div className="flex gap-1">
            {TFS.map(t => (
              <button key={t}
                onClick={() => setSsTf(t)}
                disabled={ssStatus?.running}
                className={`px-2.5 py-1 rounded text-xs border transition-colors disabled:opacity-40
                  ${ssTf === t
                    ? 'bg-purple-800 border-purple-500 text-white'
                    : 'bg-gray-800 border-gray-700 text-gray-400 hover:border-gray-500'}`}>
                {t}
              </button>
            ))}
          </div>
          <NumInput label="Bars" value={ssBars} onChange={setSsBars} min={10} max={500} />
          <button
            onClick={startStockStat}
            disabled={ssStatus?.running}
            className="px-4 py-1.5 rounded text-xs font-semibold bg-teal-600 hover:bg-teal-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
            📊 Run Stock Stat
          </button>
        </div>

        {/* Progress */}
        {ssStatus && (ssStatus.running || ssStatus.output_path || ssStatus.error) && (
          <div className="space-y-2">
            <div className="w-full bg-gray-800 rounded-full h-2">
              <div
                className={`h-2 rounded-full transition-all ${ssStatus.running ? 'bg-teal-500' : 'bg-green-600'}`}
                style={{ width: `${pct(ssStatus.done ?? 0, ssStatus.total || 1)}%` }}
              />
            </div>
            <div className="flex flex-wrap gap-4 text-xs">
              <span className="text-gray-400">
                {ssStatus.done ?? 0} / {ssStatus.total ?? 0} tickers
              </span>
              <span className="text-gray-500">{fmt(ssStatus.elapsed)}</span>
              {ssStatus.universe && (
                <span className="text-gray-600">{ssStatus.universe} · {ssStatus.tf}</span>
              )}
            </div>
            {ssStatus.error && (
              <div className="text-red-400 text-xs">Error: {ssStatus.error}</div>
            )}
            {!ssStatus.running && !ssStatus.error && ssStatus.output_path && (
              <div className="flex items-center gap-3">
                <span className="text-green-400 text-xs">
                  ✓ Done — {ssStatus.done} tickers · {Math.round((ssStatus.output_size ?? 0) / 1024 / 1024 * 10) / 10} MB
                </span>
                <a
                  href={api.stockStatDownloadUrl()}
                  download
                  className="px-3 py-1 rounded text-xs font-semibold bg-green-700 hover:bg-green-600 text-white transition-colors">
                  ⬇ Download CSV
                </a>
              </div>
            )}
          </div>
        )}
        {ssError && <div className="text-red-400 text-xs">{ssError}</div>}
      </div>

    </div>
  )
}
