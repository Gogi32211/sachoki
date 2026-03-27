import { useState, useEffect, useMemo } from 'react'
import { api } from '../api'

// BR% score → colour
function brColor(score) {
  if (score >= 71) return 'text-lime-400 font-bold'
  if (score >= 50) return 'text-yellow-300 font-semibold'
  if (score >= 30) return 'text-gray-300'
  return 'text-red-400'
}

function brBg(score) {
  if (score >= 71) return 'bg-lime-900/30'
  if (score >= 50) return 'bg-yellow-900/20'
  if (score >= 30) return 'bg-gray-900/10'
  return ''
}

function Dot({ active, color = 'bg-lime-400' }) {
  return active
    ? <span className={`inline-block w-2 h-2 rounded-full ${color}`} />
    : <span className="inline-block w-2 h-2 rounded-full bg-gray-700" />
}

const ENTRY_FILTERS = [
  { key: 'all', label: 'ALL' },
  { key: 'buy', label: 'BUY' },
  { key: 'bc',  label: 'BC'  },
  { key: 'big', label: 'BIG' },
  { key: 'go',  label: 'GO'  },
  { key: 'up',  label: 'UP'  },
]

const BR_THRESHOLDS = [
  { label: 'BR≥0',  value: 0   },
  { label: 'BR≥30', value: 30  },
  { label: 'BR≥50', value: 50  },
  { label: 'BR≥71', value: 71  },
]

export default function BRScanPanel({ tf = '1d', onSelectTicker }) {
  const [results,   setResults]   = useState([])
  const [lastScan,  setLastScan]  = useState(null)
  const [scanning,  setScanning]  = useState(false)
  const [error,     setError]     = useState(null)
  const [entry,     setEntry]     = useState('all')
  const [minBr,     setMinBr]     = useState(0)

  const load = () => {
    api.brScan(300, minBr, entry)
      .then(d => { setResults(d.results || []); setLastScan(d.last_scan) })
      .catch(e => setError(e.message))
  }

  useEffect(() => { load() }, [entry, minBr])

  const _poll = () => {
    const iv = setInterval(() => {
      api.brScanStatus()
        .then(s => {
          if (!s.running) {
            clearInterval(iv)
            setScanning(false)
            load()
          }
        })
        .catch(() => { clearInterval(iv); setScanning(false) })
    }, 2000)
    setTimeout(() => { clearInterval(iv); setScanning(false); load() }, 300_000)
  }

  const scan = () => {
    setScanning(true)
    setError(null)
    api.brScanTrigger(tf)
      .then(() => _poll())
      .catch(e => { setError(e.message); setScanning(false) })
  }

  const fmt = (v, dec = 2) =>
    v == null ? '—' : Number(v).toFixed(dec)

  return (
    <div className="flex flex-col h-full bg-gray-950 text-gray-100 text-xs">

      {/* ── Toolbar ── */}
      <div className="flex flex-wrap items-center gap-2 px-3 py-2 border-b border-gray-800">

        {/* Scan button */}
        <button
          onClick={scan}
          disabled={scanning}
          className={`px-3 py-1 rounded text-xs font-semibold transition-colors
            ${scanning
              ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
              : 'bg-indigo-600 hover:bg-indigo-500 text-white'}`}
        >
          {scanning ? <span className="animate-pulse">● Scanning…</span> : '▶ Scan'}
        </button>

        {/* Entry filter */}
        <span className="text-gray-500 text-xs">Entry:</span>
        {ENTRY_FILTERS.map(f => (
          <button
            key={f.key}
            onClick={() => setEntry(f.key)}
            className={`px-2 py-0.5 rounded text-xs transition-colors
              ${entry === f.key
                ? 'bg-blue-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:text-white'}`}
          >
            {f.label}
          </button>
        ))}

        {/* BR threshold */}
        <span className="text-gray-500 text-xs ml-2">BR:</span>
        {BR_THRESHOLDS.map(t => (
          <button
            key={t.value}
            onClick={() => setMinBr(t.value)}
            className={`px-2 py-0.5 rounded text-xs transition-colors
              ${minBr === t.value
                ? 'bg-yellow-600 text-black font-semibold'
                : 'bg-gray-800 text-gray-400 hover:text-white'}`}
          >
            {t.label}
          </button>
        ))}

        <span className="ml-auto text-gray-600">
          {results.length} results
          {lastScan && ` · ${lastScan.slice(0, 16).replace('T', ' ')}`}
        </span>
      </div>

      {/* ── Progress banner ── */}
      {scanning && (
        <div className="px-4 py-1.5 border-b border-gray-800 bg-indigo-950/30 text-indigo-300">
          <span className="animate-pulse">●</span>
          {' '}Scanning 700 tickers — gamocdileba 1–3 wuti…
        </div>
      )}

      {error && (
        <div className="px-4 py-1.5 text-red-400 border-b border-gray-800">{error}</div>
      )}

      {/* ── Table ── */}
      <div className="overflow-auto flex-1">
        <table className="w-full border-collapse">
          <thead className="sticky top-0 bg-gray-900 z-10">
            <tr className="text-gray-500 text-left">
              <th className="px-2 py-1.5 font-medium">Ticker</th>
              <th className="px-2 py-1.5 font-medium text-center">BR%</th>
              <th className="px-2 py-1.5 font-medium text-center">CONS</th>
              <th className="px-2 py-1.5 font-medium text-center">CAP</th>
              <th className="px-2 py-1.5 font-medium text-center">ACCUM</th>
              <th className="px-2 py-1.5 font-medium text-center">ME</th>
              <th className="px-2 py-1.5 font-medium text-center">BUY</th>
              <th className="px-2 py-1.5 font-medium text-center">GO</th>
              <th className="px-2 py-1.5 font-medium text-center">Signals</th>
              <th className="px-2 py-1.5 font-medium text-right">Price</th>
              <th className="px-2 py-1.5 font-medium text-right">%</th>
            </tr>
          </thead>
          <tbody>
            {results.map(r => (
              <tr
                key={r.ticker}
                className={`border-b border-gray-800/50 hover:bg-gray-800/40 cursor-pointer ${brBg(r.br_score)}`}
                onClick={() => onSelectTicker?.(r.ticker)}
              >
                {/* Ticker */}
                <td className="px-2 py-1 font-mono font-semibold text-blue-300">
                  {r.ticker}
                </td>

                {/* BR% */}
                <td className={`px-2 py-1 text-center font-mono ${brColor(r.br_score)}`}>
                  {fmt(r.br_score, 1)}
                </td>

                {/* CONS bars */}
                <td className="px-2 py-1 text-center font-mono text-yellow-200">
                  {r.cons_bars > 0 ? r.cons_bars : '—'}
                </td>

                {/* CAP count */}
                <td className="px-2 py-1 text-center font-mono text-cyan-300">
                  {r.cap_count > 0 ? r.cap_count : '—'}
                </td>

                {/* ACCUM cluster */}
                <td className="px-2 py-1 text-center font-mono text-purple-300">
                  {r.accum_cluster > 0 ? r.accum_cluster : '—'}
                </td>

                {/* ME */}
                <td className="px-2 py-1 text-center">
                  {r.me_bull ? (
                    <span className="text-lime-400 font-bold">ME{r.me_count}</span>
                  ) : r.me_bear ? (
                    <span className="text-red-400 font-bold">ME↓{r.me_count}</span>
                  ) : '—'}
                </td>

                {/* BUY */}
                <td className="px-2 py-1 text-center">
                  {r.buy  ? <span className="text-lime-400 font-bold">BUY</span>  :
                   r.bc   ? <span className="text-teal-400">BC</span>   :
                   r.big  ? <span className="text-orange-400">BIG</span> : '—'}
                </td>

                {/* GO */}
                <td className="px-2 py-1 text-center">
                  {r.go ? <span className="text-lime-300 font-bold">GO</span>   :
                   r.up ? <span className="text-blue-400">UP</span>   : '—'}
                </td>

                {/* Extra signal badges */}
                <td className="px-2 py-1">
                  <div className="flex flex-wrap gap-0.5">
                    {r.tz_bull  ? <span className="text-lime-300 bg-lime-900/40 px-1 rounded">T</span>  : null}
                    {r.blue     ? <span className="text-sky-300  bg-sky-900/40  px-1 rounded">BL</span> : null}
                    {r.fri34    ? <span className="text-cyan-300 bg-cyan-900/40 px-1 rounded">FRI</span>: null}
                    {r.l34      ? <span className="text-blue-300 bg-blue-900/30 px-1 rounded">L34</span>: null}
                    {r.raw_p3   ? <span className="text-yellow-300 bg-yellow-900/30 px-1 rounded">P3</span>: null}
                    {r.sig3g    ? <span className="text-cyan-400  bg-cyan-900/30 px-1 rounded">3G</span>: null}
                    {r.rtv      ? <span className="text-blue-400  bg-blue-900/30 px-1 rounded">RTV</span>: null}
                    {r.wick_bull? <span className="text-emerald-300 bg-emerald-900/30 px-1 rounded">2W↑</span>: null}
                    {r.cisd_ppm ? <span className="text-green-300 bg-green-900/30 px-1 rounded">C++-</span>: null}
                    {r.cisd_seq ? <span className="text-lime-300  bg-lime-900/30  px-1 rounded">C++--</span>: null}
                  </div>
                </td>

                {/* Price */}
                <td className="px-2 py-1 text-right font-mono text-gray-200">
                  ${fmt(r.last_price)}
                </td>

                {/* Change % */}
                <td className={`px-2 py-1 text-right font-mono
                  ${r.change_pct >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
                  {r.change_pct >= 0 ? '+' : ''}{fmt(r.change_pct)}%
                </td>
              </tr>
            ))}

            {results.length === 0 && !scanning && (
              <tr>
                <td colSpan={11} className="px-4 py-8 text-center text-gray-600">
                  No results — press Scan to run
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
