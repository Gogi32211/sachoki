import { useState, useEffect, useCallback, useRef } from 'react'

const API = import.meta.env.VITE_API_URL || ''

// ─── Fetch helpers ─────────────────────────────────────────────────────────────
async function apiFetch(path) {
  const r = await fetch(API + path)
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

// ─── Status badge ──────────────────────────────────────────────────────────────
function StatusBadge({ status }) {
  const cls = {
    idle:      'bg-gray-700 text-gray-300',
    running:   'bg-yellow-700 text-yellow-200 animate-pulse',
    completed: 'bg-green-800 text-green-200',
    failed:    'bg-red-800 text-red-200',
  }[status] || 'bg-gray-700 text-gray-300'
  return <span className={`text-xs px-2 py-0.5 rounded font-mono ${cls}`}>{status}</span>
}

// ─── Sortable table ────────────────────────────────────────────────────────────
function ReplayTable({ rows, columns, emptyMsg = 'No data', pageSize = 100 }) {
  const [sortCol, setSortCol] = useState(null)
  const [sortDir, setSortDir] = useState('desc')
  const [page,    setPage]    = useState(1)

  const sorted = sortCol
    ? [...rows].sort((a, b) => {
        const av = a[sortCol], bv = b[sortCol]
        const an = parseFloat(av), bn = parseFloat(bv)
        const cmp = Number.isNaN(an) ? String(av).localeCompare(String(bv)) : an - bn
        return sortDir === 'desc' ? -cmp : cmp
      })
    : rows

  const pages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const visible = sorted.slice((page - 1) * pageSize, page * pageSize)

  const handleSort = (col) => {
    if (sortCol === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    else { setSortCol(col); setSortDir('desc') }
    setPage(1)
  }

  if (!rows.length) return (
    <p className="text-gray-500 text-sm py-6 text-center">{emptyMsg}</p>
  )

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between text-xs text-gray-500">
        <span>{rows.length.toLocaleString()} rows</span>
        {pages > 1 && (
          <div className="flex gap-1 items-center">
            <button onClick={() => setPage(p => Math.max(1, p-1))}
              disabled={page === 1}
              className="px-2 py-0.5 rounded bg-gray-800 hover:bg-gray-700 disabled:opacity-40">◀</button>
            <span>{page} / {pages}</span>
            <button onClick={() => setPage(p => Math.min(pages, p+1))}
              disabled={page === pages}
              className="px-2 py-0.5 rounded bg-gray-800 hover:bg-gray-700 disabled:opacity-40">▶</button>
          </div>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b border-gray-700">
              {columns.map(col => (
                <th key={col.key}
                  onClick={() => handleSort(col.key)}
                  className="text-left px-2 py-1 text-gray-400 cursor-pointer hover:text-white whitespace-nowrap select-none">
                  {col.label || col.key}
                  {sortCol === col.key && (sortDir === 'desc' ? ' ▼' : ' ▲')}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visible.map((row, i) => (
              <tr key={i} className={`border-b border-gray-800 hover:bg-gray-900 ${i % 2 === 0 ? '' : 'bg-gray-950'}`}>
                {columns.map(col => {
                  const v = row[col.key]
                  const n = parseFloat(v)
                  let cls = 'text-gray-300'
                  if (col.color && !Number.isNaN(n)) {
                    if (n > 0) cls = 'text-green-400'
                    else if (n < 0) cls = 'text-red-400'
                  }
                  if (col.key === 'final_regime' || col.key === 'signal') cls = 'text-blue-300 font-mono'
                  const display = col.fmt ? col.fmt(v, row) : (v === null || v === undefined || v === '' ? '—' : String(v))
                  return (
                    <td key={col.key} className={`px-2 py-1 ${cls} whitespace-nowrap max-w-xs overflow-hidden text-ellipsis`}
                      title={String(v || '')}>
                      {display}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─── Column helpers ────────────────────────────────────────────────────────────
const pct  = v => v === '' || v == null ? '—' : `${parseFloat(v) >= 0 ? '+' : ''}${parseFloat(v).toFixed(2)}%`
const rate = v => v === '' || v == null ? '—' : `${(parseFloat(v)*100).toFixed(1)}%`
const num2 = v => v === '' || v == null ? '—' : parseFloat(v).toFixed(2)

// ─── Sections config ──────────────────────────────────────────────────────────
const SECTIONS = [
  { id: 'score_bucket_perf',  label: 'Score Buckets',    icon: '📊' },
  { id: 'regime_perf',        label: 'Regime Perf',      icon: '🏷' },
  { id: 'signal_perf',        label: 'Signal Perf',      icon: '📈' },
  { id: 'pair_combo_perf',    label: 'Pair Combos',      icon: '🔗' },
  { id: 'triple_combo_perf',  label: 'Triple Combos',    icon: '⛓' },
  { id: 'model_perf',         label: 'Model Perf',       icon: '🤖' },
  { id: 'missed_winners',     label: 'Missed Winners',   icon: '🎯' },
  { id: 'false_positives',    label: 'False Positives',  icon: '⚠️' },
  { id: 'unscored_signals',   label: 'Unscored Signals', icon: '🔍' },
  { id: 'scored_weak',        label: 'Scored Weak',      icon: '📉' },
  { id: 'filter_miss_audit',  label: 'Filter Audit',     icon: '🔎' },
  { id: 'summary_md',         label: 'Summary',          icon: '📝' },
]

// ─── Per-section column definitions ───────────────────────────────────────────
const COLS = {
  score_bucket_perf: [
    { key: 'score_name', label: 'Score' },
    { key: 'bucket',     label: 'Bucket' },
    { key: 'count',      label: '#' },
    { key: 'avg_ret_1d',       label: 'Ret1D',    color: true, fmt: pct },
    { key: 'avg_ret_3d',       label: 'Ret3D',    color: true, fmt: pct },
    { key: 'avg_ret_5d',       label: 'Ret5D',    color: true, fmt: pct },
    { key: 'avg_ret_10d',      label: 'Ret10D',   color: true, fmt: pct },
    { key: 'median_ret_10d',   label: 'Med10D',   color: true, fmt: pct },
    { key: 'avg_max_high_5d',  label: 'MaxH5D',   color: true, fmt: pct },
    { key: 'avg_max_high_10d', label: 'MaxH10D',  color: true, fmt: pct },
    { key: 'big_win_10d_rate', label: 'BW10D%',   fmt: rate },
    { key: 'parabolic_10d_rate', label: 'Para%',  fmt: rate },
    { key: 'fail_10d_rate',    label: 'Fail%',    fmt: rate },
  ],
  regime_perf: [
    { key: 'final_regime',     label: 'Regime' },
    { key: 'count',            label: '#' },
    { key: 'avg_ret_5d',       label: 'Ret5D',    color: true, fmt: pct },
    { key: 'avg_ret_10d',      label: 'Ret10D',   color: true, fmt: pct },
    { key: 'median_ret_10d',   label: 'Med10D',   color: true, fmt: pct },
    { key: 'avg_max_high_10d', label: 'MaxH10D',  color: true, fmt: pct },
    { key: 'big_win_10d_rate', label: 'BW10D%',   fmt: rate },
    { key: 'parabolic_10d_rate', label: 'Para%',  fmt: rate },
    { key: 'fail_10d_rate',    label: 'Fail%',    fmt: rate },
    { key: 'top_examples',     label: 'Top' },
  ],
  signal_perf: [
    { key: 'signal',           label: 'Signal' },
    { key: 'count',            label: '#' },
    { key: 'frequency',        label: 'Freq',     fmt: rate },
    { key: 'is_scored',        label: 'Scored' },
    { key: 'avg_ret_5d',       label: 'Ret5D',    color: true, fmt: pct },
    { key: 'avg_ret_10d',      label: 'Ret10D',   color: true, fmt: pct },
    { key: 'avg_max_high_10d', label: 'MaxH10D',  color: true, fmt: pct },
    { key: 'big_win_10d_rate', label: 'BW10D%',   fmt: rate },
    { key: 'parabolic_10d_rate', label: 'Para%',  fmt: rate },
    { key: 'fail_10d_rate',    label: 'Fail%',    fmt: rate },
  ],
  pair_combo_perf: [
    { key: 'combo',            label: 'Combo' },
    { key: 'count',            label: '#' },
    { key: 'avg_ret_5d',       label: 'Ret5D',    color: true, fmt: pct },
    { key: 'avg_ret_10d',      label: 'Ret10D',   color: true, fmt: pct },
    { key: 'avg_max_high_10d', label: 'MaxH10D',  color: true, fmt: pct },
    { key: 'big_win_10d_rate', label: 'BW10D%',   fmt: rate },
    { key: 'parabolic_10d_rate', label: 'Para%',  fmt: rate },
    { key: 'fail_10d_rate',    label: 'Fail%',    fmt: rate },
  ],
  triple_combo_perf: [
    { key: 'combo',            label: 'Combo' },
    { key: 'count',            label: '#' },
    { key: 'avg_ret_5d',       label: 'Ret5D',    color: true, fmt: pct },
    { key: 'avg_ret_10d',      label: 'Ret10D',   color: true, fmt: pct },
    { key: 'avg_max_high_10d', label: 'MaxH10D',  color: true, fmt: pct },
    { key: 'big_win_10d_rate', label: 'BW10D%',   fmt: rate },
    { key: 'fail_10d_rate',    label: 'Fail%',    fmt: rate },
  ],
  model_perf: [
    { key: 'model',            label: 'Model' },
    { key: 'count',            label: '#' },
    { key: 'avg_ret_5d',       label: 'Ret5D',    color: true, fmt: pct },
    { key: 'avg_ret_10d',      label: 'Ret10D',   color: true, fmt: pct },
    { key: 'median_ret_10d',   label: 'Med10D',   color: true, fmt: pct },
    { key: 'avg_max_high_10d', label: 'MaxH10D',  color: true, fmt: pct },
    { key: 'big_win_10d_rate', label: 'BW10D%',   fmt: rate },
    { key: 'parabolic_10d_rate', label: 'Para%',  fmt: rate },
    { key: 'fail_10d_rate',    label: 'Fail%',    fmt: rate },
  ],
  missed_winners: [
    { key: 'ticker',             label: 'Ticker' },
    { key: 'date',               label: 'Date' },
    { key: 'close',              label: 'Close',   fmt: num2 },
    { key: 'final_bull_score',   label: 'FBS' },
    { key: 'turbo_score',        label: 'Turbo' },
    { key: 'signal_score',       label: 'Sig' },
    { key: 'rtb_score',          label: 'RTB' },
    { key: 'final_regime',       label: 'Regime' },
    { key: 'already_extended',   label: 'Ext' },
    { key: 'ret_5d',             label: 'Ret5D',  color: true, fmt: pct },
    { key: 'ret_10d',            label: 'Ret10D', color: true, fmt: pct },
    { key: 'max_high_5d',        label: 'Max5D',  fmt: pct },
    { key: 'max_high_10d',       label: 'Max10D', fmt: pct },
    { key: 'likely_miss_reason', label: 'Reason' },
    { key: 'active_signals',     label: 'Signals' },
  ],
  false_positives: [
    { key: 'ticker',             label: 'Ticker' },
    { key: 'date',               label: 'Date' },
    { key: 'close',              label: 'Close',   fmt: num2 },
    { key: 'final_bull_score',   label: 'FBS' },
    { key: 'final_regime',       label: 'Regime' },
    { key: 'hard_bear_score',    label: 'Bear' },
    { key: 'already_extended',   label: 'Ext' },
    { key: 'ret_5d',             label: 'Ret5D',  color: true, fmt: pct },
    { key: 'ret_10d',            label: 'Ret10D', color: true, fmt: pct },
    { key: 'max_high_10d',       label: 'Max10D', fmt: pct },
    { key: 'active_models',      label: 'Models' },
    { key: 'likely_fail_reason', label: 'Reason' },
    { key: 'active_signals',     label: 'Signals' },
  ],
  unscored_signals: [
    { key: 'signal',              label: 'Signal' },
    { key: 'count',               label: '#' },
    { key: 'avg_ret_10d',         label: 'Ret10D',  color: true, fmt: pct },
    { key: 'big_win_10d_rate',    label: 'BW10D%',  fmt: rate },
    { key: 'fail_10d_rate',       label: 'Fail%',   fmt: rate },
    { key: 'suggested_component', label: 'Suggested Component' },
    { key: 'suggested_action',    label: 'Action' },
  ],
  scored_weak: [
    { key: 'signal',             label: 'Signal' },
    { key: 'current_component',  label: 'Component' },
    { key: 'current_weight',     label: 'Wt' },
    { key: 'count',              label: '#' },
    { key: 'avg_ret_10d',        label: 'Ret10D', color: true, fmt: pct },
    { key: 'big_win_10d_rate',   label: 'BW10D%', fmt: rate },
    { key: 'fail_10d_rate',      label: 'Fail%',  fmt: rate },
    { key: 'recommendation',     label: 'Rec' },
  ],
  filter_miss_audit: [
    { key: 'filter_name',                label: 'Filter' },
    { key: 'excluded_count',             label: 'Excluded' },
    { key: 'missed_big_win_count',       label: 'Missed BW' },
    { key: 'missed_big_win_rate',        label: 'Miss Rate', fmt: rate },
    { key: 'avg_max_high_10d_excluded',  label: 'Avg MaxH Excl', fmt: pct },
    { key: 'avg_max_high_10d_missed',    label: 'Avg MaxH Missed', fmt: pct },
    { key: 'top_missed_examples',        label: 'Examples' },
  ],
}

// ─── Section view ──────────────────────────────────────────────────────────────
function SectionView({ sectionId, reportList }) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)
  const [search,  setSearch]  = useState('')
  const [tfFilter, setTf]     = useState('')
  const prevId = useRef(null)

  const load = useCallback(async () => {
    setLoading(true); setError(null)
    try {
      const d = await apiFetch(`/api/replay/report/${sectionId}?page_size=2000`)
      setData(d)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [sectionId])

  // Reset state when section changes
  useEffect(() => {
    setData(null); setSearch(''); setError(null)
    prevId.current = sectionId
  }, [sectionId])

  // Load when section or reportList changes and we have no data yet
  useEffect(() => {
    if (data || loading) return
    const hasReport = reportList.some(r => r.name === sectionId) || sectionId === 'summary_md'
    if (hasReport) load()
  }, [sectionId, reportList, data, loading, load])

  const exportCsv = () => {
    window.location.href = `${API}/api/replay/export/${sectionId}`
  }

  const hasReport = reportList.some(r => r.name === sectionId) || sectionId === 'summary_md'

  if (!hasReport && !loading) return (
    <div className="text-gray-500 text-sm py-8 text-center">
      <p>Run Replay Analytics to generate this report.</p>
    </div>
  )

  if (loading) return (
    <div className="flex items-center gap-2 text-gray-400 py-8">
      <div className="w-4 h-4 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
      Loading…
    </div>
  )

  if (error) return (
    <div className="text-red-400 text-sm py-4 bg-red-950 rounded px-3">{error}</div>
  )

  if (!data) return null

  // Markdown summary
  if (data.type === 'markdown') return (
    <div className="flex flex-col gap-2">
      <div className="flex justify-end">
        <button onClick={exportCsv}
          className="text-xs px-3 py-1 bg-gray-800 hover:bg-gray-700 rounded text-gray-300">
          ⬇ Export MD
        </button>
      </div>
      <pre className="text-xs text-gray-300 whitespace-pre-wrap bg-gray-900 rounded p-4 overflow-auto max-h-[70vh]">
        {data.content}
      </pre>
    </div>
  )

  const cols = COLS[sectionId]
  let rows   = data.rows || []

  // Filter by search
  if (search) {
    const q = search.toLowerCase()
    rows = rows.filter(r => Object.values(r).some(v => String(v).toLowerCase().includes(q)))
  }

  if (!cols) return (
    <div className="text-gray-500 text-sm">No column config for {sectionId}.</div>
  )

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center gap-2 flex-wrap">
        <input
          type="text"
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Search…"
          className="text-xs bg-gray-800 border border-gray-700 rounded px-2 py-1 text-gray-200 w-48"
        />
        <span className="text-xs text-gray-500">{rows.length.toLocaleString()} rows shown</span>
        <div className="ml-auto flex gap-2">
          <button onClick={load}
            className="text-xs px-3 py-1 bg-gray-800 hover:bg-gray-700 rounded text-gray-300">
            ↻ Refresh
          </button>
          <button onClick={exportCsv}
            className="text-xs px-3 py-1 bg-blue-800 hover:bg-blue-700 rounded text-blue-200">
            ⬇ Export CSV
          </button>
        </div>
      </div>
      <ReplayTable rows={rows} columns={cols} pageSize={250} />
    </div>
  )
}

// ─── Main panel ────────────────────────────────────────────────────────────────
export default function ReplayPanel() {
  const [status,     setStatus]     = useState({ status: 'idle', progress: 0, message: '' })
  const [reportList, setReportList] = useState([])
  const [activeSection, setActive]  = useState('score_bucket_perf')
  const [tf,         setTf]         = useState('1d')
  const [universe,   setUniverse]   = useState('sp500')
  const pollRef = useRef(null)

  const fetchStatus = useCallback(async () => {
    try {
      const s = await apiFetch('/api/replay/status')
      setStatus(s)
    } catch {}
  }, [])

  const fetchReports = useCallback(async () => {
    try {
      const d = await apiFetch('/api/replay/reports')
      setReportList(d.reports || [])
    } catch {}
  }, [])

  useEffect(() => {
    fetchStatus()
    fetchReports()
  }, [fetchStatus, fetchReports])

  // Poll while running
  useEffect(() => {
    if (status.status === 'running') {
      pollRef.current = setInterval(() => {
        fetchStatus()
        fetchReports()
      }, 2000)
    } else {
      clearInterval(pollRef.current)
      if (status.status === 'completed') fetchReports()
    }
    return () => clearInterval(pollRef.current)
  }, [status.status, fetchStatus, fetchReports])

  const runReplay = async () => {
    try {
      await fetch(`${API}/api/replay/run?tf=${tf}&universe=${universe}`, { method: 'POST' })
      fetchStatus()
    } catch (e) {
      alert('Error starting replay: ' + e.message)
    }
  }

  const exportAll = () => { window.location.href = `${API}/api/replay/export-all` }

  const progress = status.total_steps
    ? Math.round((status.progress / status.total_steps) * 100)
    : 0

  return (
    <div className="flex flex-col gap-3">
      {/* ── Header / controls ──────────────────────────────────────────── */}
      <div className="flex flex-wrap items-center gap-3 bg-gray-900 rounded p-3">
        <div>
          <h2 className="text-sm font-semibold text-white">Replay Analytics</h2>
          <p className="text-xs text-gray-500 mt-0.5">
            Offline diagnostics — does not touch live scoring
          </p>
        </div>
        <div className="flex gap-2 items-center ml-auto flex-wrap">
          {/* Universe / TF selectors */}
          <select value={universe} onChange={e => setUniverse(e.target.value)}
            className="text-xs bg-gray-800 border border-gray-700 rounded px-2 py-1 text-gray-200">
            {['sp500','nasdaq','russell2k','all_us'].map(u =>
              <option key={u} value={u}>{u}</option>)}
          </select>
          <select value={tf} onChange={e => setTf(e.target.value)}
            className="text-xs bg-gray-800 border border-gray-700 rounded px-2 py-1 text-gray-200">
            {['1d','4h','1h','30m'].map(t =>
              <option key={t} value={t}>{t}</option>)}
          </select>
          <StatusBadge status={status.status} />
          <button
            onClick={runReplay}
            disabled={status.status === 'running'}
            className="text-xs px-3 py-1.5 bg-blue-700 hover:bg-blue-600 disabled:opacity-50 rounded text-white font-semibold">
            {status.status === 'running' ? '⏳ Running…' : '▶ Run Replay'}
          </button>
          <button onClick={exportAll}
            disabled={!reportList.length}
            className="text-xs px-3 py-1.5 bg-gray-700 hover:bg-gray-600 disabled:opacity-40 rounded text-gray-200">
            ⬇ Export All ZIP
          </button>
        </div>
      </div>

      {/* ── Progress bar ───────────────────────────────────────────────── */}
      {status.status === 'running' && (
        <div className="flex flex-col gap-1">
          <div className="w-full bg-gray-800 rounded-full h-1.5">
            <div className="bg-blue-500 h-1.5 rounded-full transition-all duration-500"
              style={{ width: `${progress}%` }} />
          </div>
          <p className="text-xs text-gray-400">
            Step {status.progress}/{status.total_steps} — {status.message}
          </p>
        </div>
      )}
      {status.status === 'failed' && (
        <div className="text-xs text-red-400 bg-red-950 rounded px-3 py-2">
          ❌ {status.error}
        </div>
      )}
      {status.status === 'completed' && (
        <div className="text-xs text-green-400 bg-green-950 rounded px-3 py-2">
          ✓ Completed {status.completed_at} — {status.row_count?.toLocaleString()} rows
        </div>
      )}

      {/* ── Report info ────────────────────────────────────────────────── */}
      {reportList.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {reportList.map(r => (
            <span key={r.name} className="text-xs bg-gray-800 text-gray-400 px-2 py-0.5 rounded font-mono">
              {r.name} ({r.rows.toLocaleString()})
            </span>
          ))}
        </div>
      )}

      {/* ── Main layout: nav + content ─────────────────────────────────── */}
      <div className="flex gap-3 min-h-[500px]">
        {/* Section nav */}
        <div className="flex flex-col gap-0.5 min-w-[160px]">
          {SECTIONS.map(sec => (
            <button
              key={sec.id}
              onClick={() => setActive(sec.id)}
              className={`text-xs text-left px-3 py-2 rounded transition-colors
                ${activeSection === sec.id
                  ? 'bg-blue-900 text-blue-200 font-semibold'
                  : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800'}`}>
              {sec.icon} {sec.label}
              {reportList.some(r => r.name === sec.id) && (
                <span className="ml-1 text-green-500">●</span>
              )}
            </button>
          ))}
        </div>

        {/* Section content */}
        <div className="flex-1 min-w-0 bg-gray-900 rounded p-3">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-white">
              {SECTIONS.find(s => s.id === activeSection)?.icon}{' '}
              {SECTIONS.find(s => s.id === activeSection)?.label}
            </h3>
            {reportList.find(r => r.name === activeSection) && (
              <span className="text-xs text-gray-500">
                {reportList.find(r => r.name === activeSection)?.generated_at}
              </span>
            )}
          </div>
          <SectionView sectionId={activeSection} reportList={reportList} />
        </div>
      </div>
    </div>
  )
}
