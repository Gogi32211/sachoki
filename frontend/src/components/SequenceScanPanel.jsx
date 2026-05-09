import { useState, useEffect, useMemo, useRef } from 'react'
import { api } from '../api'

const UNIVERSES = [
  { key: 'sp500',      label: 'S&P 500' },
  { key: 'nasdaq',     label: 'NASDAQ' },
  { key: 'nasdaq_gt5', label: 'NASDAQ > $5' },
  { key: 'russell2k',  label: 'Russell 2K' },
  { key: 'all_us',     label: 'All US' },
  { key: 'split',      label: 'SPLIT' },
]
const TF_OPTS    = ['1d', '4h', '1h']
const SEQ_LENGTHS = [2, 3, 4, 5, 6]
const MODES = [
  { key: 'type', label: 'T/Z type', help: 'Each bar contributes "T" or "Z" — e.g. "TZTZ"' },
  { key: 'full', label: 'Full label', help: 'Each bar contributes its full signal name — e.g. "T4|Z2|T1G|Z3"' },
]
const SORTS = [
  { key: 'score',        label: 'Score (WR × log count)' },
  { key: 'win_rate',     label: 'Win rate' },
  { key: 'count',        label: 'Occurrences' },
  { key: 'ticker_count', label: 'Breadth (ticker count)' },
]

function fmtPct(v) {
  if (v == null || isNaN(Number(v))) return '—'
  return (Number(v) * 100).toFixed(1) + '%'
}
function fmtNum(v, d = 4) {
  if (v == null || isNaN(Number(v))) return '—'
  return Number(v).toFixed(d)
}

function _csvCell(v) {
  if (v == null) return ''
  if (typeof v === 'number') return Number.isFinite(v) ? String(v) : ''
  let s = Array.isArray(v) ? v.join(';') : String(v)
  const isNumeric = s !== '' && !isNaN(Number(s)) && /^-?\d/.test(s)
  if (!isNumeric && /^[=+\-@]/.test(s)) s = "'" + s
  return s.includes(',') || s.includes('"') || s.includes('\n')
    ? `"${s.replace(/"/g, '""')}"` : s
}

export default function SequenceScanPanel() {
  const [universe,  setUniverse]  = useState('sp500')
  const [tf,        setTf]        = useState('1d')
  const [seqLen,    setSeqLen]    = useState(4)
  const [mode,      setMode]      = useState('type')
  const [minCount,  setMinCount]  = useState(10)
  const [sortBy,    setSortBy]    = useState('score')
  const [limit,     setLimit]     = useState(100)
  const [filter,    setFilter]    = useState('')

  const [status,    setStatus]    = useState({ status: 'idle', progress: 0, total: 0, pct: 0 })
  const [results,   setResults]   = useState([])
  const [meta,      setMeta]      = useState(null)
  const [error,     setError]     = useState('')
  const [loading,   setLoading]   = useState(false)
  const pollRef = useRef(null)

  const params = useMemo(
    () => ({ universe, tf, seq_len: seqLen, mode, min_count: minCount }),
    [universe, tf, seqLen, mode, minCount]
  )

  const fetchStatus = async () => {
    try {
      const s = await api.sequenceScanStatus(params)
      setStatus(s || {})
      return s
    } catch (e) { setError(String(e)); return null }
  }

  const fetchResults = async () => {
    try {
      setLoading(true)
      const r = await api.sequenceScanResults({ ...params, sort_by: sortBy, limit })
      setResults(r?.results || [])
      setMeta({
        total_sequences: r?.total_sequences ?? 0,
        completed_at:    r?.completed_at,
        cache_key:       r?.cache_key,
      })
    } catch (e) { setError(String(e)) }
    finally    { setLoading(false) }
  }

  // Initial load + reload on params change
  useEffect(() => {
    fetchStatus().then(s => { if (s?.status === 'done') fetchResults() })
    return () => clearInterval(pollRef.current)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [universe, tf, seqLen, mode, minCount])

  // Re-sort/relimit without re-querying
  useEffect(() => { if (status.status === 'done') fetchResults() }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  , [sortBy, limit])

  // Poll while running
  useEffect(() => {
    clearInterval(pollRef.current)
    if (status.status === 'running') {
      pollRef.current = setInterval(async () => {
        const s = await fetchStatus()
        if (!s || s.status !== 'running') {
          clearInterval(pollRef.current)
          if (s?.status === 'done') fetchResults()
        }
      }, 2000)
    }
    return () => clearInterval(pollRef.current)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status.status])

  const trigger = async () => {
    setError('')
    try {
      await api.sequenceScanTrigger(params)
      const s = await fetchStatus()
      setStatus(s || { status: 'running', progress: 0, total: 0, pct: 0 })
    } catch (e) {
      const msg = e?.detail || e?.message || String(e)
      if (msg.includes('409') || msg.toLowerCase().includes('already running')) {
        setError('Another sequence scan is already running — wait for it to finish.')
      } else if (msg.toLowerCase().includes('no stock_stat')) {
        setError('No TZ/WLNBB stock_stat CSV for this universe/tf — generate it first via the TZ/WLNBB tab.')
      } else {
        setError(msg)
      }
    }
  }

  const filteredRows = useMemo(() => {
    if (!filter) return results
    const f = filter.toUpperCase()
    return results.filter(r =>
      (r.sequence || '').toUpperCase().includes(f)
      || (r.type_seq || '').toUpperCase().includes(f)
    )
  }, [results, filter])

  const exportCsv = () => {
    if (!filteredRows.length) return
    const cols = ['sequence', 'type_seq', 'count', 'wins', 'win_rate',
                  'ticker_count', 'avg_ret_1d', 'med_ret_1d', 'std_ret', 'score']
    const lines = [cols.join(',')]
    for (const r of filteredRows) {
      lines.push(cols.map(c => _csvCell(r[c])).join(','))
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `sequences_${universe}_${tf}_${seqLen}bar_${mode}_${new Date().toISOString().slice(0,10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const isRunning = status.status === 'running'
  const pct = Math.max(0, Math.min(100, Number(status.pct ?? 0)))

  return (
    <div className="text-sm">
      {/* ── Controls ─────────────────────────────────────────────────── */}
      <div className="flex flex-wrap items-end gap-3 p-2 border border-gray-800 rounded bg-gray-900">
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Universe</label>
          <select value={universe} onChange={e => setUniverse(e.target.value)}
                  className="bg-gray-800 text-white text-xs rounded px-2 py-1">
            {UNIVERSES.map(u => <option key={u.key} value={u.key}>{u.label}</option>)}
          </select>
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">TF</label>
          <select value={tf} onChange={e => setTf(e.target.value)}
                  className="bg-gray-800 text-white text-xs rounded px-2 py-1">
            {TF_OPTS.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Seq length</label>
          <select value={seqLen} onChange={e => setSeqLen(Number(e.target.value))}
                  className="bg-gray-800 text-white text-xs rounded px-2 py-1">
            {SEQ_LENGTHS.map(n => <option key={n} value={n}>{n} bars</option>)}
          </select>
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Mode</label>
          <select value={mode} onChange={e => setMode(e.target.value)}
                  title={MODES.find(m => m.key === mode)?.help}
                  className="bg-gray-800 text-white text-xs rounded px-2 py-1">
            {MODES.map(m => <option key={m.key} value={m.key}>{m.label}</option>)}
          </select>
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Min count</label>
          <input type="number" min={1} value={minCount}
                 onChange={e => setMinCount(Number(e.target.value) || 1)}
                 className="bg-gray-800 text-white text-xs rounded px-2 py-1 w-20" />
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Sort by</label>
          <select value={sortBy} onChange={e => setSortBy(e.target.value)}
                  className="bg-gray-800 text-white text-xs rounded px-2 py-1">
            {SORTS.map(s => <option key={s.key} value={s.key}>{s.label}</option>)}
          </select>
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Limit</label>
          <input type="number" min={1} max={10000} value={limit}
                 onChange={e => setLimit(Number(e.target.value) || 100)}
                 className="bg-gray-800 text-white text-xs rounded px-2 py-1 w-20" />
        </div>
        <button onClick={trigger} disabled={isRunning}
                className="bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-white text-xs px-3 py-1 rounded font-semibold">
          {isRunning ? '⏳ Scanning…' : '▶ Run Sequence Scan'}
        </button>
        <button onClick={fetchResults}
                disabled={loading || status.status !== 'done'}
                className="bg-gray-700 hover:bg-gray-600 disabled:opacity-50 text-white text-xs px-3 py-1 rounded">
          ↻ Refresh
        </button>
        <button onClick={exportCsv} disabled={!filteredRows.length}
                className="bg-emerald-700 hover:bg-emerald-600 disabled:opacity-50 text-white text-xs px-3 py-1 rounded">
          ⬇ Export CSV
        </button>
        <input type="text" value={filter} placeholder="Filter sequences…"
               onChange={e => setFilter(e.target.value)}
               className="bg-gray-800 text-white text-xs rounded px-2 py-1 w-48" />
      </div>

      {/* ── Status / progress ─────────────────────────────────────────── */}
      <div className="mt-2 flex flex-wrap items-center gap-3 text-xs">
        <span className={`px-1.5 py-0.5 rounded font-medium
          ${status.status === 'running' ? 'bg-amber-900/50 text-amber-200' :
            status.status === 'done'    ? 'bg-emerald-900/50 text-emerald-200' :
            status.status === 'no_data' ? 'bg-red-900/40 text-red-200' :
            status.status === 'error'   ? 'bg-red-900/40 text-red-200' :
            'bg-gray-800 text-gray-400'}`}>
          status: {status.status || 'idle'}
        </span>
        {isRunning && (
          <>
            <span className="text-gray-300">
              {status.progress ?? 0} / {status.total ?? 0} tickers ({pct}%)
            </span>
            <div className="flex-1 max-w-md h-1.5 bg-gray-800 rounded overflow-hidden">
              <div className="bg-violet-500 h-full transition-all"
                   style={{ width: `${pct}%` }} />
            </div>
          </>
        )}
        {meta?.total_sequences != null && status.status === 'done' && (
          <span className="text-gray-400">
            {meta.total_sequences.toLocaleString()} sequences ≥ min_count
            {meta.completed_at && <> · {meta.completed_at.slice(0, 19).replace('T', ' ')}</>}
          </span>
        )}
        {status.error && (
          <span className="text-red-400">⚠ {status.error}</span>
        )}
      </div>

      {error && (
        <div className="mt-2 text-xs text-red-300 bg-red-950 border border-red-800 rounded p-2">
          {error}
        </div>
      )}

      {/* ── Table ─────────────────────────────────────────────────────── */}
      <div className="mt-2 overflow-auto border border-gray-800 rounded">
        <table className="min-w-full text-xs">
          <thead className="bg-gray-900 text-gray-400">
            <tr>
              <th className="px-2 py-1.5 text-left">Sequence</th>
              <th className="px-2 py-1.5 text-left">Type</th>
              <th className="px-2 py-1.5 text-right">Win rate</th>
              <th className="px-2 py-1.5 text-right">Count</th>
              <th className="px-2 py-1.5 text-right">Breadth</th>
              <th className="px-2 py-1.5 text-right">Avg Ret 1D</th>
              <th className="px-2 py-1.5 text-right">Med Ret 1D</th>
              <th className="px-2 py-1.5 text-right">Score</th>
            </tr>
          </thead>
          <tbody>
            {filteredRows.length === 0 && (
              <tr><td colSpan={8} className="text-center text-gray-600 py-6">
                {status.status === 'not_run'
                  ? 'No scan run yet — press ▶ Run Sequence Scan'
                  : status.status === 'no_data'
                    ? 'No TZ/WLNBB stock_stat CSV for this universe/tf — generate it first.'
                    : 'No sequences match current filter / min_count.'}
              </td></tr>
            )}
            {filteredRows.map((r, i) => (
              <tr key={`${r.sequence}-${i}`}
                  className="border-t border-gray-800 hover:bg-gray-900">
                <td className="px-2 py-1 font-mono text-blue-300">{r.sequence}</td>
                <td className="px-2 py-1 font-mono text-gray-400">{r.type_seq}</td>
                <td className={`px-2 py-1 text-right font-mono ${
                    (r.win_rate ?? 0) >= 0.6 ? 'text-emerald-300 font-semibold' :
                    (r.win_rate ?? 0) >= 0.5 ? 'text-teal-200' :
                    (r.win_rate ?? 0) >= 0.4 ? 'text-yellow-300' :
                    'text-red-300'}`}>
                  {fmtPct(r.win_rate)}
                </td>
                <td className="px-2 py-1 text-right font-mono text-gray-200">{r.count}</td>
                <td className="px-2 py-1 text-right font-mono text-gray-200">{r.ticker_count}</td>
                <td className={`px-2 py-1 text-right font-mono ${
                    (r.avg_ret_1d ?? 0) > 0 ? 'text-emerald-300' :
                    (r.avg_ret_1d ?? 0) < 0 ? 'text-red-300' : 'text-gray-400'}`}>
                  {fmtNum(r.avg_ret_1d, 4)}
                </td>
                <td className={`px-2 py-1 text-right font-mono ${
                    (r.med_ret_1d ?? 0) > 0 ? 'text-emerald-300' :
                    (r.med_ret_1d ?? 0) < 0 ? 'text-red-300' : 'text-gray-400'}`}>
                  {fmtNum(r.med_ret_1d, 4)}
                </td>
                <td className="px-2 py-1 text-right font-mono text-amber-200">{fmtNum(r.score, 4)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-2 text-[10px] text-gray-500">
        Score = win_rate × log1p(count) — balances rare high-WR vs common low-WR sequences.
        Excludes T7, T8, Z8 by spec. Requires TZ/WLNBB stock_stat CSV; generate it first.
      </div>
    </div>
  )
}
