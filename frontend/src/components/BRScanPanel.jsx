import { useState, useEffect, useMemo } from 'react'
import { api } from '../api'

// ── 260323 combo signal definitions ──────────────────────────────────────────
const COMBO_SIGS = [
  { key: 'rocket',   label: 'ROCKET', cls: 'text-red-400 bg-red-900/50' },
  { key: 'buy_2809', label: 'BUY',    cls: 'text-lime-400 bg-lime-900/50' },
  { key: 'sig3g',    label: '3G',     cls: 'text-cyan-300 bg-cyan-900/50' },
  { key: 'bb_brk',   label: 'BB↑',   cls: 'text-orange-400 bg-orange-900/40' },
  { key: 'atr_brk',  label: 'ATR↑',  cls: 'text-green-400 bg-green-900/40' },
  { key: 'rtv',      label: 'RTV',    cls: 'text-blue-400 bg-blue-900/40' },
  { key: 'preup3',   label: 'P3',     cls: 'text-yellow-300 bg-yellow-900/40' },
  { key: 'preup2',   label: 'P2',     cls: 'text-orange-300 bg-orange-900/30' },
  { key: 'preup50',  label: 'P50',    cls: 'text-pink-400 bg-pink-900/40' },
  { key: 'preup89',  label: 'P89',    cls: 'text-purple-400 bg-purple-900/40' },
  { key: 'hilo_buy', label: 'HILO↑', cls: 'text-green-300 bg-green-900/30' },
  { key: 'cons_atr', label: 'CONS',   cls: 'text-yellow-200 bg-yellow-900/20' },
  { key: 'svs_2809', label: 'SVS',    cls: 'text-orange-200 bg-orange-900/20' },
  { key: 'um_2809',  label: 'UM',     cls: 'text-teal-300 bg-teal-900/30' },
]

const L_FILTER_SIGS = [
  { key: 'fri34',     label: 'FRI34',  color: 'text-cyan-300' },
  { key: 'l34',       label: 'L34',    color: 'text-blue-300' },
  { key: 'l43',       label: 'L43',    color: 'text-teal-300' },
  { key: 'l64',       label: 'L64',    color: 'text-orange-400' },
  { key: 'l22',       label: 'L22',    color: 'text-red-400' },
  { key: 'cci_ready', label: 'CCI_RDY',color: 'text-violet-300' },
  { key: 'blue',      label: 'BLUE',   color: 'text-sky-300' },
  { key: 'raw_p3',    label: 'P3',     color: 'text-yellow-300' },
  { key: 'sig3g',     label: '3G',     color: 'text-cyan-400' },
  { key: 'rtv',       label: 'RTV',    color: 'text-blue-400' },
  { key: 'wick_bull', label: 'WICK↑',  color: 'text-emerald-300' },
  { key: 'cisd_ppm',  label: 'C++-',   color: 'text-green-300' },
  { key: 'cisd_seq',  label: 'C++--',  color: 'text-lime-300' },
]

const TZ_OPTS = ['T4','T6','T1G','T2G','T1','T2','T9','T10','T3','T11','T5']

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

const BR_TF_OPTIONS = ['1w', '1d', '4h', '1h']

export default function BRScanPanel({ tf: globalTf = '1d', onSelectTicker }) {
  const [localTf,    setLocalTf]   = useState('1d')
  const [allResults, setAllResults] = useState([])
  const [lastScan,   setLastScan]   = useState(null)
  const [scanning,   setScanning]   = useState(false)
  const [error,      setError]      = useState(null)
  const [entry,      setEntry]      = useState('all')
  const [minBr,      setMinBr]      = useState(0)
  const [selTZ,      setSelTZ]      = useState(new Set())   // T/Z OR filter
  const [selL,       setSelL]       = useState(new Set())   // L-sig AND filter

  const load = (tf = localTf) => {
    api.brScan(500, 0, 'all', tf)
      .then(d => { setAllResults(d.results || []); setLastScan(d.last_scan) })
      .catch(e => setError(e.message))
  }

  useEffect(() => { load(localTf) }, [localTf])

  // Client-side filter
  const results = useMemo(() => {
    return allResults.filter(r => {
      if (r.br_score < minBr) return false
      if (entry !== 'all' && !r[entry]) return false
      if (selTZ.size > 0 && !selTZ.has(r.tz_sig)) return false
      if (selL.size > 0 && ![...selL].every(k => r[k])) return false
      return true
    })
  }, [allResults, minBr, entry, selTZ, selL])

  const toggleSet = (setFn, key) => setFn(prev => {
    const n = new Set(prev)
    n.has(key) ? n.delete(key) : n.add(key)
    return n
  })

  const _poll = () => {
    const tf = localTf
    const iv = setInterval(() => {
      api.brScanStatus()
        .then(s => {
          if (!s.running) {
            clearInterval(iv)
            setScanning(false)
            load(tf)
          }
        })
        .catch(() => { clearInterval(iv); setScanning(false) })
    }, 2000)
    setTimeout(() => { clearInterval(iv); setScanning(false); load(tf) }, 300_000)
  }

  const scan = () => {
    setScanning(true)
    setError(null)
    api.brScanTrigger(localTf)
      .then(() => _poll())
      .catch(e => { setError(e.message); setScanning(false) })
  }

  const fmt = (v, dec = 2) =>
    v == null ? '—' : Number(v).toFixed(dec)

  return (
    <div className="flex flex-col h-full bg-gray-950 text-gray-100 text-xs">

      {/* ── Row 1: Scan + TF + Entry + BR threshold ── */}
      <div className="flex flex-wrap items-center gap-2 px-3 py-2 border-b border-gray-800">
        <button
          onClick={scan} disabled={scanning}
          className={`px-3 py-1 rounded text-xs font-semibold transition-colors
            ${scanning ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                       : 'bg-indigo-600 hover:bg-indigo-500 text-white'}`}
        >
          {scanning ? <span className="animate-pulse">● Scanning…</span> : '▶ Scan'}
        </button>

        {/* Timeframe selector */}
        <div className="flex gap-1 border border-gray-700 rounded p-0.5">
          {BR_TF_OPTIONS.map(t => (
            <button
              key={t}
              onClick={() => setLocalTf(t)}
              className={`px-2 py-0.5 rounded text-xs font-medium transition-colors
                ${localTf === t
                  ? 'bg-blue-600 text-white'
                  : 'text-gray-400 hover:text-white'}`}
            >
              {t.toUpperCase()}
            </button>
          ))}
        </div>

        <span className="text-gray-500">Entry:</span>
        {ENTRY_FILTERS.map(f => (
          <button key={f.key} onClick={() => setEntry(f.key)}
            className={`px-2 py-0.5 rounded text-xs transition-colors
              ${entry === f.key ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
            {f.label}
          </button>
        ))}

        <span className="text-gray-500 ml-2">BR:</span>
        {BR_THRESHOLDS.map(t => (
          <button key={t.value} onClick={() => setMinBr(t.value)}
            className={`px-2 py-0.5 rounded text-xs transition-colors
              ${minBr === t.value ? 'bg-yellow-600 text-black font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
            {t.label}
          </button>
        ))}

        <span className="ml-auto text-gray-600">
          {results.length} / {allResults.length}
          {lastScan && ` · ${lastScan.slice(0, 16).replace('T', ' ')}`}
        </span>
      </div>

      {/* ── Row 2: T/Z OR filter ── */}
      <div className="flex flex-wrap items-center gap-1 px-3 py-1.5 border-b border-gray-800 bg-gray-900/30">
        <span className="text-gray-500 text-xs w-8 shrink-0">T/Z</span>
        <button onClick={() => setSelTZ(new Set())}
          className={`px-2 py-0.5 rounded text-xs ${selTZ.size === 0 ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
          All
        </button>
        {TZ_OPTS.map(t => (
          <button key={t} onClick={() => toggleSet(setSelTZ, t)}
            className={`px-2 py-0.5 rounded text-xs transition-colors
              ${selTZ.has(t) ? 'bg-green-700 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
            {t}
          </button>
        ))}
      </div>

      {/* ── Row 3: L-SIG AND filter ── */}
      <div className="flex flex-wrap items-center gap-1 px-3 py-1.5 border-b border-gray-800 bg-gray-900/20">
        <span className="text-gray-500 text-xs w-8 shrink-0">SIG</span>
        <button onClick={() => setSelL(new Set())}
          className={`px-2 py-0.5 rounded text-xs ${selL.size === 0 ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
          All
        </button>
        {L_FILTER_SIGS.map(s => (
          <button key={s.key} onClick={() => toggleSet(setSelL, s.key)}
            className={`px-2 py-0.5 rounded text-xs transition-colors
              ${selL.has(s.key) ? `${s.color} bg-gray-700 font-semibold` : 'bg-gray-800 text-gray-500 hover:text-white'}`}>
            {s.label}
          </button>
        ))}
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
              <th className="px-2 py-1.5 font-medium text-center">Score</th>
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

                {/* Master score */}
                <td className={`px-2 py-1 text-center font-mono font-bold ${brColor(r.master_score ?? r.br_score)}`}>
                  {fmt(r.master_score ?? r.br_score, 1)}
                </td>

                {/* BR% */}
                <td className={`px-2 py-1 text-center font-mono text-gray-400`}>
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
                    {r.combo_labels && r.combo_labels.split(',').filter(Boolean).map(lb => (
                      <span key={lb} className="text-orange-300 bg-orange-900/30 px-1 rounded">{lb}</span>
                    ))}
                    {r.tz_bull  ? <span className="text-lime-300 bg-lime-900/40 px-1 rounded">T:{r.tz_sig}</span>  : null}
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
                <td colSpan={12} className="px-4 py-8 text-center text-gray-600">
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
