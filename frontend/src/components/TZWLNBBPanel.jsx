import { useState, useEffect, useRef } from 'react'

const BASE = import.meta.env.VITE_API_URL || ''

async function apiGet(path) {
  const res = await fetch(BASE + path)
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

async function apiPost(path, params = {}) {
  const qs = new URLSearchParams(params).toString()
  const url = qs ? `${BASE}${path}?${qs}` : BASE + path
  const res = await fetch(url, { method: 'POST' })
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

const UNIVERSES = [
  { key: 'sp500',     label: 'S&P 500'   },
  { key: 'nasdaq',    label: 'NASDAQ'    },
  { key: 'russell2k', label: 'Russell 2K'},
  { key: 'all_us',    label: 'All US'    },
]

const TF_OPTS = ['1d', '4h', '1h', '1wk']

const SIG_TYPES = [
  { key: 'all',   label: 'All'    },
  { key: 'T',     label: 'T'      },
  { key: 'Z',     label: 'Z'      },
  { key: 'L',     label: 'L'      },
  { key: 'PREUP', label: 'PREUP'  },
  { key: 'PREDN', label: 'PREDN'  },
  { key: 'Combo', label: 'Combo'  },
]

function Badge({ text, color }) {
  if (!text) return null
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-xs font-semibold ${color}`}>
      {text}
    </span>
  )
}

function tBadgeColor(sig) {
  if (!sig) return ''
  if (sig === 'T4' || sig === 'T6') return 'bg-blue-700 text-blue-100'
  if (sig.startsWith('T1G') || sig.startsWith('T2G')) return 'bg-blue-600 text-blue-100'
  return 'bg-blue-800 text-blue-200'
}

function zBadgeColor(sig) {
  if (!sig) return ''
  if (sig === 'Z4' || sig === 'Z6') return 'bg-red-700 text-red-100'
  if (sig.startsWith('Z1G') || sig.startsWith('Z2G')) return 'bg-red-600 text-red-100'
  return 'bg-red-800 text-red-200'
}

function lBadgeColor() {
  return 'bg-yellow-700 text-yellow-100'
}

function preBadgeColor(sig) {
  if (!sig) return ''
  return sig.startsWith('P') ? 'bg-emerald-700 text-emerald-100' : 'bg-orange-700 text-orange-100'
}

function DebugModal({ ticker, date, tf, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    const qs = new URLSearchParams({ ticker, tf })
    if (date) qs.set('date', date)
    apiGet(`/api/tz-wlnbb/debug?${qs}`)
      .then(d => setData(d))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, date, tf])

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-900 border border-gray-700 rounded-lg p-4 max-w-2xl w-full max-h-[80vh] overflow-auto">
        <div className="flex justify-between items-center mb-3">
          <h3 className="text-white font-semibold">{ticker} — Debug ({date || 'latest'})</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-white text-xl">✕</button>
        </div>
        {loading && <div className="text-gray-400">Loading…</div>}
        {error   && <div className="text-red-400">{error}</div>}
        {data && data.error && <div className="text-red-400">{data.error}</div>}
        {data && data.rows && data.rows.map((row, i) => (
          <div key={i} className="text-xs text-gray-300 space-y-1">
            {Object.entries(row).map(([k, v]) => (
              <div key={k} className="flex gap-2">
                <span className="text-gray-500 w-40 shrink-0">{k}</span>
                <span className="text-white break-all">{String(v)}</span>
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}

export default function TZWLNBBPanel() {
  const [universe, setUniverse] = useState('sp500')
  const [tf, setTf]             = useState('1d')
  const [signalType, setSignalType] = useState('all')
  const [signalName, setSignalName] = useState('')
  const [minPrice, setMinPrice] = useState('')
  const [maxPrice, setMaxPrice] = useState('')
  const [minVolume, setMinVolume] = useState('')
  const [recentWindow, setRecentWindow] = useState(1)

  const [results, setResults]   = useState([])
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState(null)
  const [genStatus, setGenStatus] = useState(null)
  const [genError, setGenError] = useState(null)
  const [status, setStatus]     = useState(null)

  const [debugRow, setDebugRow] = useState(null)

  // Replay state
  const [replayState, setReplayState]     = useState(null)  // {running, output, error}
  const [replayTopRows, setReplayTopRows] = useState([])
  const [replayTab, setReplayTab]         = useState('signal') // 'signal'|'combo'|'sequence'

  const pollRef       = useRef(null)
  const replayPollRef = useRef(null)

  // Poll status while running
  useEffect(() => {
    const poll = async () => {
      try {
        const s = await apiGet('/api/tz-wlnbb/status')
        setStatus(s)
        if (!s.running) {
          clearInterval(pollRef.current)
          pollRef.current = null
        }
      } catch {}
    }
    poll()
  }, [])

  function startPolling() {
    if (pollRef.current) return
    pollRef.current = setInterval(async () => {
      try {
        const s = await apiGet('/api/tz-wlnbb/status')
        setStatus(s)
        if (!s.running) {
          clearInterval(pollRef.current)
          pollRef.current = null
        }
      } catch {}
    }, 2000)
  }

  async function handleGenerate() {
    setGenStatus('starting')
    setGenError(null)
    try {
      await apiPost('/api/tz-wlnbb/generate-stock-stat', { universe, tf, bars: 252 })
      setGenStatus('started')
      startPolling()
    } catch (e) {
      setGenError(e.message)
      setGenStatus(null)
    }
  }

  async function handleScan() {
    setLoading(true)
    setError(null)
    try {
      const qs = new URLSearchParams({
        universe, tf,
        signal_type: signalType,
        recent_window: recentWindow,
      })
      if (signalName)  qs.set('signal_name', signalName)
      if (minPrice)    qs.set('min_price', minPrice)
      if (maxPrice)    qs.set('max_price', maxPrice)
      if (minVolume)   qs.set('min_volume', minVolume)
      const data = await apiGet(`/api/tz-wlnbb/scan?${qs}`)
      if (data.error) setError(data.error)
      setResults(data.results || [])
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  // ── Replay polling ────────────────────────────────────────────────────────
  useEffect(() => {
    apiGet('/api/tz-wlnbb/replay/status').then(s => setReplayState(s)).catch(() => {})
  }, [])

  function startReplayPolling() {
    if (replayPollRef.current) return
    replayPollRef.current = setInterval(async () => {
      try {
        const s = await apiGet('/api/tz-wlnbb/replay/status')
        setReplayState(s)
        if (!s.running) {
          clearInterval(replayPollRef.current)
          replayPollRef.current = null
        }
      } catch {}
    }, 2000)
  }

  async function handleReplay() {
    setReplayTopRows([])
    try {
      await apiPost('/api/tz-wlnbb/replay', { universe, tf })
      startReplayPolling()
    } catch (e) {
      setReplayState({ running: false, error: e.message, output: null })
    }
  }

  const isRunning = status?.running
  const replayRunning = replayState?.running

  return (
    <div className="bg-gray-950 text-gray-100 p-3 flex flex-col gap-3">
      <div className="flex items-center gap-2">
        <span className="text-lg font-bold text-white">📡 TZ/WLNBB Analyzer</span>
        <span className="text-xs text-gray-500">Pine Script conversion — candlestick + volume analysis</span>
      </div>

      {/* ── Controls ──────────────────────────────────────────────────────── */}
      <div className="flex flex-wrap gap-2 items-end">
        {/* Universe */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Universe</label>
          <select
            value={universe}
            onChange={e => setUniverse(e.target.value)}
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700"
          >
            {UNIVERSES.map(u => (
              <option key={u.key} value={u.key}>{u.label}</option>
            ))}
          </select>
        </div>

        {/* Timeframe */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Timeframe</label>
          <div className="flex gap-1">
            {TF_OPTS.map(t => (
              <button
                key={t}
                onClick={() => setTf(t)}
                className={`text-xs px-2 py-1 rounded transition-colors
                  ${tf === t ? 'bg-blue-600 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}
              >
                {t}
              </button>
            ))}
          </div>
        </div>

        {/* Signal Type */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Signal Type</label>
          <div className="flex flex-wrap gap-1">
            {SIG_TYPES.map(s => (
              <button
                key={s.key}
                onClick={() => setSignalType(s.key)}
                className={`text-xs px-2 py-1 rounded transition-colors
                  ${signalType === s.key ? 'bg-blue-600 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}
              >
                {s.label}
              </button>
            ))}
          </div>
        </div>

        {/* Signal Name */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Signal Name</label>
          <input
            type="text"
            value={signalName}
            onChange={e => setSignalName(e.target.value)}
            placeholder="e.g. T4, Z6, L34…"
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700 w-32"
          />
        </div>

        {/* Price filters */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Min Price</label>
          <input
            type="number"
            value={minPrice}
            onChange={e => setMinPrice(e.target.value)}
            placeholder="0"
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700 w-20"
          />
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Max Price</label>
          <input
            type="number"
            value={maxPrice}
            onChange={e => setMaxPrice(e.target.value)}
            placeholder="∞"
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700 w-20"
          />
        </div>

        {/* Min Volume */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Min Volume</label>
          <input
            type="number"
            value={minVolume}
            onChange={e => setMinVolume(e.target.value)}
            placeholder="0"
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700 w-24"
          />
        </div>

        {/* Recent Window */}
        <div className="flex flex-col gap-1">
          <label className="text-xs text-gray-500">Recent Bars</label>
          <input
            type="number"
            value={recentWindow}
            onChange={e => setRecentWindow(Number(e.target.value) || 1)}
            min={1}
            max={10}
            className="bg-gray-800 text-gray-100 text-xs px-2 py-1 rounded border border-gray-700 w-16"
          />
        </div>

        {/* Scan button */}
        <button
          onClick={handleScan}
          disabled={loading}
          className="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-700 text-white text-xs font-semibold rounded transition-colors self-end"
        >
          {loading ? 'Scanning…' : 'Scan'}
        </button>
      </div>

      {/* ── Generate Stock Stat ──────────────────────────────────────────── */}
      <div className="flex items-center gap-3 p-2 bg-gray-900 rounded border border-gray-800">
        <button
          onClick={handleGenerate}
          disabled={isRunning}
          className="px-3 py-1.5 bg-emerald-700 hover:bg-emerald-600 disabled:bg-gray-700 text-white text-xs font-semibold rounded transition-colors"
        >
          {isRunning ? 'Generating…' : 'Generate Stock Stat'}
        </button>
        <div className="text-xs text-gray-400">
          Runs TZ/WLNBB analysis on all tickers in the selected universe and saves to CSV.
          {isRunning && status && (
            <span className="ml-2 text-yellow-400">
              Running… {status.done || 0} / {status.total || '?'} tickers
            </span>
          )}
          {status && !status.running && status.output && (
            <span className="ml-2 text-green-400">Done: {status.output}</span>
          )}
          {status && !status.running && status.error && (
            <span className="ml-2 text-red-400">Error: {status.error}</span>
          )}
          {genError && (
            <span className="ml-2 text-red-400">{genError}</span>
          )}
        </div>
      </div>

      {/* ── Replay Analytics ────────────────────────────────────────────── */}
      <div className="flex flex-col gap-2 p-2 bg-gray-900 rounded border border-gray-800">
        <div className="flex items-center gap-3 flex-wrap">
          <button
            onClick={handleReplay}
            disabled={replayRunning}
            className="px-3 py-1.5 bg-purple-700 hover:bg-purple-600 disabled:bg-gray-700 text-white text-xs font-semibold rounded transition-colors"
          >
            {replayRunning ? 'Generating Replay…' : '🔄 Generate Replay'}
          </button>
          <span className="text-xs text-gray-500">
            Reads stock_stat CSV → computes forward returns → generates analytics ZIP.
          </span>
          {replayRunning && (
            <span className="text-xs text-yellow-400 animate-pulse">Running…</span>
          )}
          {replayState && !replayState.running && replayState.output && (
            <a
              href={`${BASE}/api/tz-wlnbb/download/${replayState.output}`}
              download
              className="text-xs px-2 py-1 bg-blue-700 hover:bg-blue-600 text-white rounded transition-colors"
            >
              ⬇ Download ZIP
            </a>
          )}
          {replayState && !replayState.running && replayState.error && (
            <span className="text-xs text-red-400">Error: {replayState.error}</span>
          )}
        </div>
      </div>

      {/* ── Error ────────────────────────────────────────────────────────── */}
      {error && (
        <div className="p-2 bg-red-900/30 border border-red-700 rounded text-red-300 text-xs">
          {error}
        </div>
      )}

      {/* ── Results count ────────────────────────────────────────────────── */}
      {results.length > 0 && (
        <div className="text-xs text-gray-500">
          {results.length} result{results.length !== 1 ? 's' : ''}
        </div>
      )}

      {/* ── Results table ────────────────────────────────────────────────── */}
      {results.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="text-gray-400 border-b border-gray-800">
                <th className="text-left p-1 font-medium">Ticker</th>
                <th className="text-left p-1 font-medium">Date</th>
                <th className="text-right p-1 font-medium">Close</th>
                <th className="text-right p-1 font-medium">Volume</th>
                <th className="text-left p-1 font-medium">T</th>
                <th className="text-left p-1 font-medium">Z</th>
                <th className="text-left p-1 font-medium">L</th>
                <th className="text-left p-1 font-medium">PRE</th>
                <th className="text-left p-1 font-medium">Lane 1</th>
                <th className="text-left p-1 font-medium">Lane 3</th>
                <th className="text-left p-1 font-medium">NE</th>
                <th className="text-left p-1 font-medium">Wk</th>
                <th className="text-left p-1 font-medium">Vol</th>
                <th className="text-center p-1 font-medium">Debug</th>
              </tr>
            </thead>
            <tbody>
              {results.map((row, i) => {
                const tSig   = row.t_signal     || ''
                const zSig   = row.z_signal     || ''
                const lSig   = row.l_signal     || ''
                const preSig = row.preup_signal || row.predn_signal || ''
                const hasAny = tSig || zSig || lSig || preSig

                return (
                  <tr
                    key={`${row.ticker}-${row.date}-${i}`}
                    className={`border-b border-gray-800/50 hover:bg-gray-900/50 transition-colors
                      ${hasAny ? '' : 'opacity-60'}`}
                  >
                    <td className="p-1 font-semibold text-white">{row.ticker}</td>
                    <td className="p-1 text-gray-400">{row.date}</td>
                    <td className="p-1 text-right text-gray-200">
                      {row.close ? Number(row.close).toFixed(2) : '—'}
                    </td>
                    <td className="p-1 text-right text-gray-400">
                      {row.volume
                        ? Number(row.volume) >= 1e6
                          ? `${(Number(row.volume) / 1e6).toFixed(1)}M`
                          : Number(row.volume) >= 1e3
                          ? `${(Number(row.volume) / 1e3).toFixed(0)}K`
                          : row.volume
                        : '—'}
                    </td>
                    <td className="p-1">
                      {tSig ? <Badge text={tSig} color={tBadgeColor(tSig)} /> : null}
                    </td>
                    <td className="p-1">
                      {zSig ? <Badge text={zSig} color={zBadgeColor(zSig)} /> : null}
                    </td>
                    <td className="p-1">
                      {lSig ? <Badge text={lSig} color={lBadgeColor()} /> : null}
                    </td>
                    <td className="p-1">
                      {preSig ? <Badge text={preSig} color={preBadgeColor(preSig)} /> : null}
                    </td>
                    <td className="p-1 text-blue-300 font-mono">{row.lane1_label || ''}</td>
                    <td className="p-1 text-red-300 font-mono">{row.lane3_label || ''}</td>
                    <td className="p-1 text-gray-400">{row.ne_suffix || ''}</td>
                    <td className="p-1 text-gray-400">{row.wick_suffix || ''}</td>
                    <td className="p-1 text-gray-500">
                      <span className={`px-1 rounded text-xs
                        ${row.volume_bucket === 'VB' ? 'text-red-300' :
                          row.volume_bucket === 'B'  ? 'text-orange-300' :
                          row.volume_bucket === 'N'  ? 'text-yellow-300' :
                          row.volume_bucket === 'L'  ? 'text-blue-300' :
                          row.volume_bucket === 'W'  ? 'text-gray-400' : ''}`}
                      >
                        {row.volume_bucket || ''}
                      </span>
                    </td>
                    <td className="p-1 text-center">
                      <button
                        onClick={() => setDebugRow({ ticker: row.ticker, date: row.date, tf })}
                        className="text-gray-500 hover:text-blue-400 transition-colors text-xs"
                        title="Debug"
                      >
                        🔍
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {!loading && !error && results.length === 0 && (
        <div className="text-gray-600 text-xs py-4 text-center">
          No results. Run "Generate Stock Stat" first, then click "Scan".
        </div>
      )}

      {/* ── Debug Modal ──────────────────────────────────────────────────── */}
      {debugRow && (
        <DebugModal
          ticker={debugRow.ticker}
          date={debugRow.date}
          tf={tf}
          onClose={() => setDebugRow(null)}
        />
      )}
    </div>
  )
}
