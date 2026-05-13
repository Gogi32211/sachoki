import { Fragment, useState, useEffect, useCallback, useMemo } from 'react'

const API = import.meta.env.VITE_API_URL || ''

async function apiFetch(path, opts = {}) {
  const r = await fetch(API + path, opts)
  if (!r.ok) {
    let detail = `${r.status} ${r.statusText}`
    try { const j = await r.json(); detail = j.detail || JSON.stringify(j) } catch {}
    throw new Error(detail)
  }
  return r.json()
}

const UNIVERSE_OPTIONS = [
  { value: 'sp500',       label: 'S&P 500' },
  { value: 'nasdaq',      label: 'NASDAQ (all)' },
  { value: 'nasdaq_gt5',  label: 'NASDAQ (close ≥ $5)' },
  { value: 'split',       label: 'Split universe' },
]

const VERDICT_COLORS = {
  STRONG_EDGE:         'bg-emerald-700 text-emerald-100',
  GOOD_WITH_CONTEXT:   'bg-emerald-900 text-emerald-200',
  WATCH_ONLY:          'bg-yellow-900 text-yellow-200',
  NO_EDGE:             'bg-gray-800 text-gray-400',
  NEGATIVE_EDGE:       'bg-red-900 text-red-200',
  TOO_FEW_SAMPLES:     'bg-gray-700 text-gray-300',
}

function fmtPct(v, digits = 2) {
  if (v === null || v === undefined) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(digits)}%`
}

function fmtNum(v) {
  if (v === null || v === undefined) return '—'
  const n = Number(v); return Number.isFinite(n) ? n.toLocaleString() : '—'
}

function VerdictBadge({ verdict }) {
  const cls = VERDICT_COLORS[verdict] || 'bg-gray-800 text-gray-300'
  return <span className={`text-[10px] px-1.5 py-0.5 rounded ${cls}`}>{verdict || '—'}</span>
}

function ConfidenceBadge({ label }) {
  const cls = {
    HIGH:   'bg-blue-900 text-blue-200',
    MEDIUM: 'bg-blue-950 text-blue-300',
    LOW:    'bg-gray-800 text-gray-400',
    TOO_FEW_SAMPLES: 'bg-gray-800 text-gray-500',
  }[label] || 'bg-gray-800 text-gray-400'
  return <span className={`text-[10px] px-1.5 py-0.5 rounded ${cls}`}>{label || '—'}</span>
}

// ─── Run Settings Panel ──────────────────────────────────────────────────────

const MODE_OPTIONS = [
  { value: 'single_day',   label: 'Single Day' },
  { value: 'date_range',   label: 'Date Range' },
  { value: 'last_n_days',  label: 'Last N Days' },
  { value: 'ytd',          label: 'Year-to-Date' },
]

function SettingsPanel({ disabled, onStart }) {
  const today = new Date().toISOString().slice(0, 10)
  const [universe, setUniverse]       = useState('nasdaq_gt5')
  const [mode, setMode]               = useState('single_day')
  const [asOfDate, setAsOfDate]       = useState(today)
  const [startDate, setStartDate]     = useState(today)
  const [endDate, setEndDate]         = useState(today)
  const [lookbackDays, setLookbackDays] = useState(20)
  const [benchmark, setBenchmark]     = useState('QQQ')
  const [scope, setScope]             = useState('all_signals')

  const submit = () => {
    const payload = { universe, mode, benchmark_symbol: benchmark, event_scope: scope }
    if (mode === 'single_day')  payload.as_of_date = asOfDate
    if (mode === 'date_range')  { payload.start_date = startDate; payload.end_date = endDate }
    if (mode === 'last_n_days') payload.lookback_days = lookbackDays
    onStart(payload)
  }

  return (
    <div className="bg-gray-900 rounded p-3 grid grid-cols-1 md:grid-cols-3 gap-3">
      <div>
        <label className="text-xs text-gray-400">Universe</label>
        <select value={universe} onChange={e => setUniverse(e.target.value)}
                className="w-full bg-gray-800 text-gray-100 rounded px-2 py-1 text-sm">
          {UNIVERSE_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>
      </div>
      <div>
        <label className="text-xs text-gray-400">Mode</label>
        <div className="flex flex-wrap gap-1">
          {MODE_OPTIONS.map(m => (
            <button key={m.value} onClick={() => setMode(m.value)}
                    className={`text-xs px-2 py-1 rounded ${mode === m.value
                      ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400'}`}>
              {m.label}
            </button>
          ))}
        </div>
      </div>
      <div>
        <label className="text-xs text-gray-400">Benchmark</label>
        <select value={benchmark} onChange={e => setBenchmark(e.target.value)}
                className="w-full bg-gray-800 text-gray-100 rounded px-2 py-1 text-sm">
          <option value="QQQ">QQQ</option>
          <option value="SPY">SPY</option>
        </select>
      </div>

      {mode === 'single_day' && (
        <div className="md:col-span-3">
          <label className="text-xs text-gray-400">As-of Date</label>
          <input type="date" value={asOfDate} onChange={e => setAsOfDate(e.target.value)}
                 className="bg-gray-800 text-gray-100 rounded px-2 py-1 text-sm" />
        </div>
      )}
      {mode === 'date_range' && (
        <>
          <div>
            <label className="text-xs text-gray-400">Start Date</label>
            <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)}
                   className="w-full bg-gray-800 text-gray-100 rounded px-2 py-1 text-sm" />
          </div>
          <div>
            <label className="text-xs text-gray-400">End Date</label>
            <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)}
                   className="w-full bg-gray-800 text-gray-100 rounded px-2 py-1 text-sm" />
          </div>
          <div />
        </>
      )}
      {mode === 'last_n_days' && (
        <div className="md:col-span-3">
          <label className="text-xs text-gray-400">Trading Days to Look Back</label>
          <input type="number" min={1} max={500} value={lookbackDays}
                 onChange={e => setLookbackDays(Number(e.target.value) || 20)}
                 className="w-24 bg-gray-800 text-gray-100 rounded px-2 py-1 text-sm" />
        </div>
      )}
      {mode === 'ytd' && (
        <div className="md:col-span-3 text-xs text-gray-400 italic">
          Will scan Jan 1 {new Date().getFullYear()} → today using available trading days.
        </div>
      )}

      <div>
        <label className="text-xs text-gray-400">Event Scope</label>
        <select value={scope} onChange={e => setScope(e.target.value)}
                className="w-full bg-gray-800 text-gray-100 rounded px-2 py-1 text-sm">
          <option value="all_signals">All Signals</option>
          <option value="tz_only">T/Z only</option>
          <option value="scanner_visible_only">Scanner-visible only</option>
          <option value="watch_and_above">Watch and above</option>
        </select>
      </div>
      <div className="md:col-span-2 flex items-end justify-end">
        <button onClick={submit} disabled={disabled}
                className={`px-4 py-2 rounded text-sm font-semibold ${disabled
                  ? 'bg-gray-700 text-gray-500 cursor-not-allowed'
                  : 'bg-blue-600 text-white hover:bg-blue-500'}`}>
          ▶ Run Signal Replay
        </button>
      </div>
      <div className="md:col-span-3 text-[11px] text-gray-500 italic">
        Signal Replay uses Daily / 1D candles only.
      </div>
    </div>
  )
}

// ─── Progress Panel ──────────────────────────────────────────────────────────

function ProgressPanel({ state, onStop, onPause, onResume }) {
  if (!state || state.status === 'idle') return null
  const pct = state.symbols_total
    ? Math.round((state.symbols_completed / state.symbols_total) * 100) : 0
  const running = state.running
  const paused  = state.pause_requested

  const STATUS_CLS = {
    running:   'bg-blue-600 text-white',
    completed: 'bg-emerald-700 text-white',
    stopped:   'bg-yellow-700 text-white',
    failed:    'bg-red-700 text-white',
  }

  return (
    <div className="bg-gray-900 rounded p-3 space-y-2">
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <div className="text-sm">
          <span className={`inline-block px-2 py-0.5 rounded text-xs mr-2 ${STATUS_CLS[state.status] || 'bg-gray-700 text-gray-300'}`}>
            {paused ? 'PAUSED' : state.status}
          </span>
          run_id={state.run_id} · {state.universe} · {state.mode}
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500">elapsed: {state.elapsed_secs}s</span>
          {running && !paused && (
            <button onClick={onPause}
                    className="text-xs px-2 py-1 rounded bg-yellow-700 text-white hover:bg-yellow-600">
              ⏸ Pause
            </button>
          )}
          {running && paused && (
            <button onClick={onResume}
                    className="text-xs px-2 py-1 rounded bg-blue-600 text-white hover:bg-blue-500">
              ▶ Resume
            </button>
          )}
          {running && (
            <button onClick={onStop}
                    className="text-xs px-2 py-1 rounded bg-red-700 text-white hover:bg-red-600">
              ■ Stop
            </button>
          )}
        </div>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-7 gap-2 text-xs">
        <Stat label="Days"     val={`${state.days_completed}/${state.days_total}`} />
        <Stat label="Symbols"  val={`${state.symbols_completed}/${state.symbols_total}`} />
        <Stat label="Events"   val={fmtNum(state.events_found)} />
        <Stat label="Outcomes" val={fmtNum(state.outcomes_computed)} />
        <Stat label="Sig Stats"    val={fmtNum(state.statistics_rows)} />
        <Stat label="Patterns"     val={fmtNum(state.pattern_rows)} />
        <Stat label="Ctx Filters"  val={fmtNum(state.filter_impact_rows)} />
      </div>
      <div className="w-full bg-gray-800 rounded h-1.5 overflow-hidden">
        <div className="h-full bg-blue-500" style={{ width: `${pct}%` }} />
      </div>
      {state.error && (
        <div className="text-xs text-red-400 bg-red-950 rounded p-2">
          Error: {state.error}
        </div>
      )}
    </div>
  )
}

function Stat({ label, val }) {
  return (
    <div className="bg-gray-800 rounded px-2 py-1">
      <div className="text-[10px] text-gray-500">{label}</div>
      <div className="text-sm">{val}</div>
    </div>
  )
}

// ─── Signal Ranking Table ────────────────────────────────────────────────────

function SignalRankingTable({ runId }) {
  const [rows, setRows] = useState([])
  const [horizon, setHorizon] = useState('10d')
  const [statType, setStatType] = useState('SIGNAL')
  const [minN, setMinN] = useState(30)
  const [sortBy, setSortBy] = useState('median_return')
  const [sortDir, setSortDir] = useState('desc')
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)

  const load = useCallback(async () => {
    if (!runId) return
    setLoading(true); setErr(null)
    try {
      const qs = new URLSearchParams({
        horizon, stat_type: statType, min_sample_size: String(minN),
        sort_by: sortBy, sort_dir: sortDir, limit: '500',
      }).toString()
      const data = await apiFetch(`/api/signal-replay/${runId}/signal-statistics?${qs}`)
      setRows(Array.isArray(data) ? data : [])
    } catch (e) { setErr(e.message); setRows([]) }
    finally { setLoading(false) }
  }, [runId, horizon, statType, minN, sortBy, sortDir])

  useEffect(() => { load() }, [load])

  const headerClick = (col) => {
    if (sortBy === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    else { setSortBy(col); setSortDir('desc') }
  }

  const H = ({ col, children }) => (
    <th onClick={() => headerClick(col)}
        className="px-2 py-1 text-left cursor-pointer hover:bg-gray-800 select-none">
      {children}{sortBy === col ? (sortDir === 'desc' ? ' ▼' : ' ▲') : ''}
    </th>
  )

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2 items-center text-xs">
        <span>Horizon:</span>
        {['3d', '5d', '10d', '20d'].map(h => (
          <button key={h} onClick={() => setHorizon(h)}
                  className={`px-2 py-0.5 rounded ${horizon === h
                    ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400'}`}>{h}</button>
        ))}
        <span className="ml-3">Group by:</span>
        <select value={statType} onChange={e => setStatType(e.target.value)}
                className="bg-gray-800 text-gray-200 rounded px-2 py-0.5">
          <option value="SIGNAL">SIGNAL</option>
          <option value="SIGNAL_FAMILY">SIGNAL_FAMILY</option>
          <option value="SIGNAL_TYPE">SIGNAL_TYPE</option>
          <option value="SIGNAL_DIRECTION">SIGNAL_DIRECTION</option>
          <option value="ROLE">ROLE</option>
          <option value="SCORE_BUCKET">SCORE_BUCKET</option>
        </select>
        <span className="ml-3">Min N:</span>
        <input type="number" value={minN} onChange={e => setMinN(Number(e.target.value) || 0)}
               className="w-16 bg-gray-800 text-gray-200 rounded px-2 py-0.5" />
        <button onClick={load}
                className="ml-auto px-2 py-0.5 rounded bg-gray-800 text-gray-300 hover:bg-gray-700">
          ↻ Reload
        </button>
      </div>
      {err && <div className="text-xs text-red-400">Error: {err}</div>}
      {loading && <div className="text-xs text-gray-500">Loading…</div>}
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead className="bg-gray-900 text-gray-400">
            <tr>
              <H col="stat_key">Signal</H>
              <H col="sample_size">N</H>
              <H col="median_return">Median {horizon}</H>
              <H col="avg_return">Avg {horizon}</H>
              <H col="win_rate">Win%</H>
              <H col="hit_10pct_rate">Hit +10%</H>
              <H col="hit_20pct_rate">Hit +20%</H>
              <H col="fail_10pct_rate">Fail -10%</H>
              <th className="px-2 py-1 text-left">Avg Max Gain</th>
              <th className="px-2 py-1 text-left">Avg DD</th>
              <H col="confidence_score">Confidence</H>
              <th className="px-2 py-1 text-left">Verdict</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => (
              <tr key={r.id} className="border-b border-gray-800 hover:bg-gray-900">
                <td className="px-2 py-1 font-mono">{r.stat_key}</td>
                <td className="px-2 py-1">{fmtNum(r.sample_size)}</td>
                <td className={`px-2 py-1 ${Number(r.median_return) > 0
                  ? 'text-emerald-400' : Number(r.median_return) < 0 ? 'text-red-400' : ''}`}>
                  {fmtPct(r.median_return)}
                </td>
                <td className="px-2 py-1">{fmtPct(r.avg_return)}</td>
                <td className="px-2 py-1">{fmtPct(r.win_rate, 1)}</td>
                <td className="px-2 py-1 text-emerald-300">{fmtPct(r.hit_10pct_rate, 1)}</td>
                <td className="px-2 py-1 text-emerald-400">{fmtPct(r.hit_20pct_rate, 1)}</td>
                <td className="px-2 py-1 text-red-400">{fmtPct(r.fail_10pct_rate, 1)}</td>
                <td className="px-2 py-1">{fmtPct(r.avg_max_gain)}</td>
                <td className="px-2 py-1 text-red-300">{fmtPct(r.avg_max_drawdown)}</td>
                <td className="px-2 py-1"><ConfidenceBadge label={r.confidence_label} /></td>
                <td className="px-2 py-1"><VerdictBadge verdict={r.verdict} /></td>
              </tr>
            ))}
            {!loading && rows.length === 0 && (
              <tr><td colSpan={12} className="px-2 py-6 text-center text-gray-500">
                No stats yet. Run a replay or relax filters.
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─── Event Explorer ──────────────────────────────────────────────────────────

function EventExplorer({ runId }) {
  const [rows, setRows] = useState([])
  const [symbol, setSymbol] = useState('')
  const [sig, setSig]       = useState('')
  const [fam, setFam]       = useState('')
  const [expanded, setExpanded] = useState(null)
  const [loading, setLoading] = useState(false)

  const load = useCallback(async () => {
    if (!runId) return
    setLoading(true)
    try {
      const qs = new URLSearchParams({ limit: '200' })
      if (symbol) qs.set('symbol', symbol.toUpperCase())
      if (sig)    qs.set('event_signal', sig)
      if (fam)    qs.set('event_signal_family', fam)
      const data = await apiFetch(`/api/signal-replay/${runId}/events?${qs}`)
      setRows(data.rows || [])
    } catch {}
    finally { setLoading(false) }
  }, [runId, symbol, sig, fam])

  useEffect(() => { load() }, [load])

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2 text-xs">
        <input value={symbol} onChange={e => setSymbol(e.target.value)} placeholder="Symbol"
               className="w-24 bg-gray-800 text-gray-200 rounded px-2 py-0.5" />
        <input value={sig} onChange={e => setSig(e.target.value)} placeholder="event_signal"
               className="w-28 bg-gray-800 text-gray-200 rounded px-2 py-0.5" />
        <select value={fam} onChange={e => setFam(e.target.value)}
                className="bg-gray-800 text-gray-200 rounded px-2 py-0.5">
          <option value="">All families</option>
          {['T', 'Z', 'L', 'F', 'G', 'B', 'EMA', 'COMBO', 'ROLE'].map(f =>
            <option key={f} value={f}>{f}</option>
          )}
        </select>
        <button onClick={load} className="px-2 py-0.5 rounded bg-gray-800 text-gray-300 hover:bg-gray-700">
          ↻ Reload
        </button>
      </div>
      {loading && <div className="text-xs text-gray-500">Loading…</div>}
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead className="bg-gray-900 text-gray-400">
            <tr>
              <th className="px-2 py-1 text-left">Symbol</th>
              <th className="px-2 py-1 text-left">Date</th>
              <th className="px-2 py-1 text-left">Signal</th>
              <th className="px-2 py-1 text-left">Family</th>
              <th className="px-2 py-1 text-left">Seq 4-bar</th>
              <th className="px-2 py-1 text-left">ABR</th>
              <th className="px-2 py-1 text-left">EMA50</th>
              <th className="px-2 py-1 text-left">Pos 4b</th>
              <th className="px-2 py-1 text-left">Vol bkt</th>
              <th className="px-2 py-1 text-left">Score</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => (
              <Fragment key={r.id}>
                <tr className="border-b border-gray-800 hover:bg-gray-900 cursor-pointer"
                    onClick={() => setExpanded(e => e === r.id ? null : r.id)}>
                  <td className="px-2 py-1 font-mono">{r.symbol}</td>
                  <td className="px-2 py-1">{r.scan_date}</td>
                  <td className="px-2 py-1 font-mono">{r.event_signal}</td>
                  <td className="px-2 py-1">{r.event_signal_family}</td>
                  <td className="px-2 py-1 text-gray-400">{r.sequence_4bar}</td>
                  <td className="px-2 py-1">{r.abr_category || '—'}</td>
                  <td className="px-2 py-1">{r.ema50_state}</td>
                  <td className="px-2 py-1">{r.price_pos_4bar_bucket}</td>
                  <td className="px-2 py-1">{r.volume_bucket}</td>
                  <td className="px-2 py-1">{fmtNum(r.score)}</td>
                </tr>
                {expanded === r.id && (
                  <tr><td colSpan={10} className="px-2 py-2 bg-gray-950">
                    <pre className="text-[10px] text-gray-400 whitespace-pre-wrap">
                      {r.event_snapshot_json}
                    </pre>
                  </td></tr>
                )}
              </Fragment>
            ))}
            {!loading && rows.length === 0 && (
              <tr><td colSpan={10} className="px-2 py-6 text-center text-gray-500">
                No events.
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─── Summary ─────────────────────────────────────────────────────────────────

function SummaryPanel({ runId, runMeta }) {
  const [bestRows, setBestRows]   = useState([])
  const [worstRows, setWorstRows] = useState([])

  useEffect(() => {
    if (!runId) return
    apiFetch(`/api/signal-replay/${runId}/signal-statistics?horizon=10d&stat_type=SIGNAL&min_sample_size=30&sort_by=median_return&sort_dir=desc&limit=5`)
      .then(setBestRows).catch(() => setBestRows([]))
    apiFetch(`/api/signal-replay/${runId}/signal-statistics?horizon=10d&stat_type=SIGNAL&min_sample_size=30&sort_by=median_return&sort_dir=asc&limit=5`)
      .then(setWorstRows).catch(() => setWorstRows([]))
  }, [runId])

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-xs">
        <Stat label="Run ID"        val={runMeta?.id || '—'} />
        <Stat label="Universe"      val={runMeta?.universe || '—'} />
        <Stat label="Total Events"  val={fmtNum(runMeta?.total_events)} />
        <Stat label="Total Outcomes" val={fmtNum(runMeta?.total_outcomes)} />
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <MiniList title="🏆 Top Signals (10D median)" rows={bestRows} pos />
        <MiniList title="⚠️ Worst Signals (10D median)" rows={worstRows} />
      </div>
    </div>
  )
}

function MiniList({ title, rows, pos = false }) {
  return (
    <div className="bg-gray-900 rounded p-3">
      <div className="text-xs text-gray-400 mb-2">{title}</div>
      <table className="w-full text-xs">
        <tbody>
          {rows.map((r, i) => (
            <tr key={r.id} className="border-b border-gray-800">
              <td className="py-1 font-mono">{r.stat_key}</td>
              <td className="py-1 text-right">{fmtNum(r.sample_size)}</td>
              <td className={`py-1 text-right ${pos ? 'text-emerald-400' : 'text-red-400'}`}>
                {fmtPct(r.median_return)}
              </td>
              <td className="py-1 text-right"><VerdictBadge verdict={r.verdict} /></td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr><td className="py-3 text-center text-gray-500">No data</td></tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

// ─── Pattern Ranking ─────────────────────────────────────────────────────────

function PatternRankingTable({ runId }) {
  const [rows, setRows] = useState([])
  const [horizon, setHorizon] = useState('10d')
  const [patternType, setPatternType] = useState('SEQUENCE_4')
  const [terminal, setTerminal] = useState('')
  const [minN, setMinN] = useState(3)
  const [sortBy, setSortBy] = useState('median_return')
  const [sortDir, setSortDir] = useState('desc')
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)

  const load = useCallback(async () => {
    if (!runId) return
    setLoading(true); setErr(null)
    try {
      const qs = new URLSearchParams({
        horizon, min_sample_size: String(minN),
        sort_by: sortBy, sort_dir: sortDir, limit: '500',
      })
      if (patternType) qs.set('pattern_type', patternType)
      if (terminal) qs.set('terminal_signal', terminal.toUpperCase())
      const data = await apiFetch(`/api/signal-replay/${runId}/pattern-statistics?${qs}`)
      setRows(Array.isArray(data) ? data : [])
    } catch (e) { setErr(e.message); setRows([]) }
    finally { setLoading(false) }
  }, [runId, horizon, patternType, terminal, minN, sortBy, sortDir])

  useEffect(() => { load() }, [load])

  const headerClick = (col) => {
    if (sortBy === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    else { setSortBy(col); setSortDir('desc') }
  }
  const H = ({ col, children }) => (
    <th onClick={() => headerClick(col)}
        className="px-2 py-1 text-left cursor-pointer hover:bg-gray-800 select-none whitespace-nowrap">
      {children}{sortBy === col ? (sortDir === 'desc' ? ' ▼' : ' ▲') : ''}
    </th>
  )

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2 items-center text-xs">
        <span>Horizon:</span>
        {['3d', '5d', '10d', '20d'].map(h => (
          <button key={h} onClick={() => setHorizon(h)}
                  className={`px-2 py-0.5 rounded ${horizon === h
                    ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400'}`}>{h}</button>
        ))}
        <span className="ml-3">Pattern:</span>
        <select value={patternType} onChange={e => setPatternType(e.target.value)}
                className="bg-gray-800 text-gray-200 rounded px-2 py-0.5">
          {['', 'SEQUENCE_2', 'SEQUENCE_3', 'SEQUENCE_4', 'SEQUENCE_5', 'SEQUENCE_7'].map(p => (
            <option key={p} value={p}>{p || 'All'}</option>
          ))}
        </select>
        <span className="ml-2">Terminal:</span>
        <input value={terminal} onChange={e => setTerminal(e.target.value)} placeholder="e.g. T3"
               className="w-16 bg-gray-800 text-gray-200 rounded px-2 py-0.5" />
        <span className="ml-2">Min N:</span>
        <input type="number" value={minN} onChange={e => setMinN(Number(e.target.value) || 0)}
               className="w-14 bg-gray-800 text-gray-200 rounded px-2 py-0.5" />
        <button onClick={load}
                className="ml-auto px-2 py-0.5 rounded bg-gray-800 text-gray-300 hover:bg-gray-700">
          ↻ Reload
        </button>
      </div>
      {err && <div className="text-xs text-red-400">Error: {err}</div>}
      {loading && <div className="text-xs text-gray-500">Loading…</div>}
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead className="bg-gray-900 text-gray-400">
            <tr>
              <H col="pattern_value">Pattern</H>
              <th className="px-2 py-1 text-left">Type</th>
              <th className="px-2 py-1 text-left">Terminal</th>
              <H col="sample_size">N</H>
              <H col="median_return">Median {horizon}</H>
              <H col="avg_return">Avg</H>
              <H col="win_rate">Win%</H>
              <H col="hit_10pct_rate">Hit +10%</H>
              <H col="fail_10pct_rate">Fail -10%</H>
              <H col="confidence_score">Conf.</H>
              <th className="px-2 py-1 text-left">Verdict</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => (
              <tr key={r.id} className="border-b border-gray-800 hover:bg-gray-900">
                <td className="px-2 py-1 font-mono text-gray-300 max-w-[260px] truncate">{r.pattern_value}</td>
                <td className="px-2 py-1 text-gray-500 text-[10px]">{r.pattern_type?.replace('SEQUENCE_', '') + '-bar'}</td>
                <td className="px-2 py-1 font-mono">{r.terminal_signal || '—'}</td>
                <td className="px-2 py-1">{fmtNum(r.sample_size)}</td>
                <td className={`px-2 py-1 ${Number(r.median_return) > 0 ? 'text-emerald-400' : Number(r.median_return) < 0 ? 'text-red-400' : ''}`}>
                  {fmtPct(r.median_return)}
                </td>
                <td className="px-2 py-1">{fmtPct(r.avg_return)}</td>
                <td className="px-2 py-1">{fmtPct(r.win_rate, 1)}</td>
                <td className="px-2 py-1 text-emerald-300">{fmtPct(r.hit_10pct_rate, 1)}</td>
                <td className="px-2 py-1 text-red-400">{fmtPct(r.fail_10pct_rate, 1)}</td>
                <td className="px-2 py-1"><ConfidenceBadge label={r.confidence_label} /></td>
                <td className="px-2 py-1"><VerdictBadge verdict={r.verdict} /></td>
              </tr>
            ))}
            {!loading && rows.length === 0 && (
              <tr><td colSpan={11} className="px-2 py-6 text-center text-gray-500">
                No patterns yet. Run a replay to populate.
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─── Context Filter Impact ────────────────────────────────────────────────────

const FILTER_NAMES = [
  'ema50_state', 'volume_bucket', 'abr_category', 'candle_color',
  'price_pos_20bar_bucket', 'score_bucket',
  'had_t_last_3d', 'had_z_last_3d', 'had_wlnbb_l_last_5d',
  'had_ema50_reclaim_last_5d', 'had_volume_burst_last_5d',
]

function ContextFilterTable({ runId }) {
  const [rows, setRows] = useState([])
  const [horizon, setHorizon] = useState('10d')
  const [signal, setSignal] = useState('')
  const [filterName, setFilterName] = useState('')
  const [minN, setMinN] = useState(5)
  const [sortBy, setSortBy] = useState('lift_median_return')
  const [sortDir, setSortDir] = useState('desc')
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)

  const load = useCallback(async () => {
    if (!runId) return
    setLoading(true); setErr(null)
    try {
      const qs = new URLSearchParams({
        horizon, min_sample_size: String(minN),
        sort_by: sortBy, sort_dir: sortDir, limit: '500',
      })
      if (signal) qs.set('base_signal', signal.toUpperCase())
      if (filterName) qs.set('filter_name', filterName)
      const data = await apiFetch(`/api/signal-replay/${runId}/filter-impact?${qs}`)
      setRows(Array.isArray(data) ? data : [])
    } catch (e) { setErr(e.message); setRows([]) }
    finally { setLoading(false) }
  }, [runId, horizon, signal, filterName, minN, sortBy, sortDir])

  useEffect(() => { load() }, [load])

  const headerClick = (col) => {
    if (sortBy === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    else { setSortBy(col); setSortDir('desc') }
  }
  const H = ({ col, children }) => (
    <th onClick={() => headerClick(col)}
        className="px-2 py-1 text-left cursor-pointer hover:bg-gray-800 select-none whitespace-nowrap">
      {children}{sortBy === col ? (sortDir === 'desc' ? ' ▼' : ' ▲') : ''}
    </th>
  )

  const liftColor = (v) => {
    if (v === null || v === undefined) return ''
    return Number(v) > 0 ? 'text-emerald-400' : Number(v) < 0 ? 'text-red-400' : ''
  }

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2 items-center text-xs">
        <span>Horizon:</span>
        {['3d', '5d', '10d', '20d'].map(h => (
          <button key={h} onClick={() => setHorizon(h)}
                  className={`px-2 py-0.5 rounded ${horizon === h
                    ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400'}`}>{h}</button>
        ))}
        <span className="ml-3">Signal:</span>
        <input value={signal} onChange={e => setSignal(e.target.value)} placeholder="e.g. T3"
               className="w-16 bg-gray-800 text-gray-200 rounded px-2 py-0.5" />
        <span className="ml-2">Filter:</span>
        <select value={filterName} onChange={e => setFilterName(e.target.value)}
                className="bg-gray-800 text-gray-200 rounded px-2 py-0.5">
          <option value="">All filters</option>
          {FILTER_NAMES.map(f => <option key={f} value={f}>{f}</option>)}
        </select>
        <span className="ml-2">Min N:</span>
        <input type="number" value={minN} onChange={e => setMinN(Number(e.target.value) || 0)}
               className="w-14 bg-gray-800 text-gray-200 rounded px-2 py-0.5" />
        <button onClick={load}
                className="ml-auto px-2 py-0.5 rounded bg-gray-800 text-gray-300 hover:bg-gray-700">
          ↻ Reload
        </button>
      </div>
      {err && <div className="text-xs text-red-400">Error: {err}</div>}
      {loading && <div className="text-xs text-gray-500">Loading…</div>}
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead className="bg-gray-900 text-gray-400">
            <tr>
              <H col="base_signal">Signal</H>
              <H col="filter_name">Filter</H>
              <th className="px-2 py-1 text-left">Value</th>
              <H col="sample_size">N</H>
              <H col="median_return">Median {horizon}</H>
              <H col="lift_median_return">Lift Median</H>
              <H col="hit_10pct_rate">Hit +10%</H>
              <H col="lift_hit_10pct">Lift Hit</H>
              <H col="fail_10pct_rate">Fail -10%</H>
              <H col="confidence_score">Conf.</H>
              <th className="px-2 py-1 text-left">Verdict</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => (
              <tr key={r.id} className="border-b border-gray-800 hover:bg-gray-900">
                <td className="px-2 py-1 font-mono">{r.base_signal}</td>
                <td className="px-2 py-1 text-gray-400">{r.filter_name}</td>
                <td className="px-2 py-1">{r.filter_value}</td>
                <td className="px-2 py-1">{fmtNum(r.sample_size)}</td>
                <td className={`px-2 py-1 ${Number(r.median_return) > 0 ? 'text-emerald-400' : Number(r.median_return) < 0 ? 'text-red-400' : ''}`}>
                  {fmtPct(r.median_return)}
                </td>
                <td className={`px-2 py-1 font-semibold ${liftColor(r.lift_median_return)}`}>
                  {fmtPct(r.lift_median_return)}
                </td>
                <td className="px-2 py-1 text-emerald-300">{fmtPct(r.hit_10pct_rate, 1)}</td>
                <td className={`px-2 py-1 font-semibold ${liftColor(r.lift_hit_10pct)}`}>
                  {fmtPct(r.lift_hit_10pct, 1)}
                </td>
                <td className="px-2 py-1 text-red-400">{fmtPct(r.fail_10pct_rate, 1)}</td>
                <td className="px-2 py-1"><ConfidenceBadge label={r.confidence_label} /></td>
                <td className="px-2 py-1"><VerdictBadge verdict={r.verdict} /></td>
              </tr>
            ))}
            {!loading && rows.length === 0 && (
              <tr><td colSpan={11} className="px-2 py-6 text-center text-gray-500">
                No filter data yet. Run a replay to populate.
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─── Top-level panel ─────────────────────────────────────────────────────────

const TABS = [
  { id: 'summary',  label: '📊 Summary' },
  { id: 'ranking',  label: '🏆 Signal Ranking' },
  { id: 'patterns', label: '🔢 Patterns' },
  { id: 'context',  label: '🎛 Context Filters' },
  { id: 'events',   label: '🔬 Event Explorer' },
]

export default function SignalReplayPanel() {
  const [state, setState] = useState(null)
  const [runId, setRunId] = useState(null)
  const [runMeta, setRunMeta] = useState(null)
  const [tab, setTab] = useState('summary')
  const [history, setHistory] = useState([])

  // Poll status every 2s
  useEffect(() => {
    let alive = true
    const poll = async () => {
      try {
        const s = await apiFetch('/api/signal-replay/status')
        if (!alive) return
        setState(s)
        if (s.run_id && !runId) setRunId(s.run_id)
      } catch {}
    }
    poll()
    const t = setInterval(poll, 2000)
    return () => { alive = false; clearInterval(t) }
  }, [runId])

  // Refresh history when status changes
  useEffect(() => {
    apiFetch('/api/signal-replay/history?limit=20').then(setHistory).catch(() => {})
  }, [state?.status])

  // Load runMeta when runId or status changes
  useEffect(() => {
    if (!runId) { setRunMeta(null); return }
    apiFetch(`/api/signal-replay/${runId}`).then(setRunMeta).catch(() => setRunMeta(null))
  }, [runId, state?.status])

  const handleStart = async (payload) => {
    try {
      const r = await apiFetch('/api/signal-replay/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      setRunId(r.run_id)
    } catch (e) {
      alert(`Failed to start: ${e.message}`)
    }
  }

  const handleStop = async () => {
    try { await apiFetch('/api/signal-replay/stop', { method: 'POST' }) }
    catch (e) { alert(e.message) }
  }

  const handlePause = async () => {
    try { await apiFetch('/api/signal-replay/pause', { method: 'POST' }) }
    catch (e) { alert(e.message) }
  }

  const handleResume = async () => {
    try { await apiFetch('/api/signal-replay/resume', { method: 'POST' }) }
    catch (e) { alert(e.message) }
  }

  const handleDelete = async (id) => {
    if (!window.confirm(`Delete run #${id} and all its data from the database?`)) return
    try {
      await apiFetch(`/api/signal-replay/${id}`, { method: 'DELETE' })
      if (runId === id) { setRunId(null); setRunMeta(null) }
      apiFetch('/api/signal-replay/history?limit=20').then(setHistory).catch(() => {})
    } catch (e) {
      alert(`Delete failed: ${e.message}`)
    }
  }

  const handleExport = (id) => {
    const url = `${API}/api/signal-replay/${id}/export`
    const a = document.createElement('a')
    a.href = url
    a.download = `replay_${id}_export.json`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
  }

  const running = state?.running

  return (
    <div className="space-y-3">
      <SettingsPanel disabled={running} onStart={handleStart} />
      <ProgressPanel
        state={state}
        onStop={handleStop}
        onPause={handlePause}
        onResume={handleResume}
      />

      {history?.length > 0 && (
        <div className="bg-gray-900 rounded p-2 text-xs">
          <div className="text-gray-400 mb-1">Recent Runs:</div>
          <div className="flex flex-wrap gap-2">
            {history.map(h => (
              <div key={h.id}
                   className={`px-2 py-1 rounded flex items-center gap-2 ${
                     runId === h.id ? 'bg-blue-900 text-blue-200' : 'bg-gray-800 text-gray-300'}`}>
                <span className="cursor-pointer" onClick={() => setRunId(h.id)}>
                  #{h.id} · {h.universe} · {h.status} · {h.total_events ?? 0} ev
                </span>
                <button onClick={() => handleExport(h.id)} title="Export JSON"
                        className="text-gray-400 hover:text-blue-300">⬇</button>
                <button onClick={() => handleDelete(h.id)} title="Delete from DB"
                        className="text-red-400 hover:text-red-200">✕</button>
              </div>
            ))}
          </div>
        </div>
      )}

      {runId && (
        <>
          <div className="flex items-center justify-between border-b border-gray-800 pb-0">
            <div className="flex gap-1">
              {TABS.map(t => (
                <button key={t.id} onClick={() => setTab(t.id)}
                        className={`text-xs px-3 py-1.5 rounded-t border-b-2 ${
                          tab === t.id ? 'border-blue-500 text-blue-300 bg-gray-900'
                                       : 'border-transparent text-gray-500 hover:text-gray-300'}`}>
                  {t.label}
                </button>
              ))}
            </div>
            <div className="flex gap-2 pb-1">
              <button onClick={() => handleExport(runId)}
                      className="text-xs px-3 py-1 rounded bg-gray-800 text-blue-300 hover:bg-gray-700">
                ⬇ Export JSON
              </button>
              <button onClick={() => handleDelete(runId)}
                      className="text-xs px-3 py-1 rounded bg-gray-800 text-red-400 hover:bg-red-900">
                ✕ Delete Run
              </button>
            </div>
          </div>
          <div className="bg-gray-950 rounded p-3 min-h-[300px]">
            {tab === 'summary'  && <SummaryPanel runId={runId} runMeta={runMeta} />}
            {tab === 'ranking'  && <SignalRankingTable runId={runId} />}
            {tab === 'patterns' && <PatternRankingTable runId={runId} />}
            {tab === 'context'  && <ContextFilterTable runId={runId} />}
            {tab === 'events'   && <EventExplorer runId={runId} />}
          </div>
        </>
      )}
    </div>
  )
}
