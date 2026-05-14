import { Fragment, useState, useEffect, useCallback, useMemo, useRef } from 'react'
import SignalReplayPanel from './SignalReplayPanel'

const API = import.meta.env.VITE_API_URL || ''

async function api(path, opts = {}) {
  const r = await fetch(API + path, opts)
  if (!r.ok) {
    let detail = `${r.status} ${r.statusText}`
    try { const j = await r.json(); detail = j.detail || JSON.stringify(j) } catch {}
    throw new Error(detail)
  }
  return r.json()
}

const UNIVERSE_OPTIONS = [
  { value: 'all_us',     label: 'All US Stocks' },
  { value: 'sp500',      label: 'S&P 500' },
  { value: 'nasdaq',     label: 'NASDAQ (all)' },
  { value: 'nasdaq_gt5', label: 'NASDAQ (close ≥ $5)' },
  { value: 'split',      label: 'Split universe' },
]

const PUMP_HORIZONS         = [20, 30, 60, 90, 120]
const PRE_PUMP_WINDOWS      = [5, 7, 10, 14, 20]
const SCANNER_WINDOWS       = [3, 5, 7, 10, 14, 20]
const LOOKBACK_BARS_OPTIONS = [30, 100, 250, 500, 1000]
const SPLIT_WINDOWS         = [10, 20, 30, 60]

const TABS = [
  { id: 'overview',     label: 'Overview' },
  { id: 'x2',           label: 'X2 → X4' },
  { id: 'x4',           label: 'X4+' },
  { id: 'episodes',     label: 'Pump Episodes' },
  { id: 'caught',       label: 'Caught' },
  { id: 'missed',       label: 'Missed' },
  { id: 'signals',      label: 'Pre-Pump Signals' },
  { id: 'lift',         label: 'Pattern Lift' },
  { id: 'combos',       label: '14-Bar Combos' },
  { id: 'split',        label: 'Split Impact' },
  { id: 'recs',         label: 'Recommendations' },
  { id: 'exports',      label: 'Exports' },
  { id: 'history',      label: 'Run History' },
]

const VERDICT_BADGE = {
  PROMOTE:           'bg-emerald-700 text-emerald-100',
  WATCH:             'bg-yellow-900 text-yellow-200',
  OBSERVE:           'bg-blue-900 text-blue-200',
  SKIP:              'bg-md-surface-high text-md-on-surface-var',
  INSUFFICIENT_DATA: 'bg-gray-700 text-md-on-surface',
}

const SPLIT_BADGE = {
  CLEAN_NON_SPLIT:          'bg-emerald-900 text-emerald-200',
  POST_REVERSE_SPLIT_PUMP:  'bg-violet-900 text-violet-200',
  REVERSE_SPLIT_RELATED:    'bg-orange-900 text-orange-200',
  FORWARD_SPLIT_RELATED:    'bg-blue-900 text-blue-200',
  SPLIT_RELATED:            'bg-amber-900 text-amber-200',
  SPLIT_CONTAMINATED:       'bg-red-900 text-red-200',
  UNKNOWN:                  'bg-gray-700 text-md-on-surface',
}

function fmtNum(v) {
  if (v === null || v === undefined) return '—'
  const n = Number(v)
  return Number.isFinite(n) ? n.toLocaleString() : '—'
}
function fmtPct(v, d = 2) {
  if (v === null || v === undefined) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n.toFixed(d)}%`
}
function fmtFloat(v, d = 2) {
  if (v === null || v === undefined) return '—'
  const n = Number(v)
  return Number.isFinite(n) ? n.toFixed(d) : '—'
}

function Card({ title, value, sub }) {
  return (
    <div className="bg-md-surface-con border border-white/[0.07] rounded p-3">
      <div className="text-[10px] uppercase tracking-wide text-md-on-surface-var">{title}</div>
      <div className="text-xl font-semibold text-md-on-surface mt-1">{value}</div>
      {sub && <div className="text-[11px] text-md-on-surface-var mt-0.5">{sub}</div>}
    </div>
  )
}

function Tooltip({ text, children }) {
  return (
    <span className="group relative">
      {children}
      <span className="pointer-events-none absolute left-1/2 -translate-x-1/2 bottom-full mb-2 hidden group-hover:block z-50 w-64 p-2 bg-md-surface-high border border-white/[0.07] rounded text-[11px] text-md-on-surface whitespace-normal">
        {text}
      </span>
    </span>
  )
}

function SettingsPanel({ value, onChange, onRun, running }) {
  const set = (k, v) => onChange({ ...value, [k]: v })
  return (
    <div className="bg-md-surface-con border border-white/[0.07] rounded p-4 space-y-3">
      <div className="text-sm font-semibold text-md-on-surface">Research Settings</div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-[12px]">
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Universe</span>
          <select className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                  value={value.universe} onChange={e => set('universe', e.target.value)}>
            {UNIVERSE_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Pump Target</span>
          <select className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                  value={value.pump_target} onChange={e => set('pump_target', e.target.value)}>
            <option value="X2_TO_X4">X2 → X4</option>
            <option value="X4_PLUS">X4+ (Monster)</option>
            <option value="BOTH">Both</option>
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Mode</span>
          <select className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                  value={value.mode} onChange={e => set('mode', e.target.value)}>
            <option value="date_range">Date Range</option>
            <option value="single_day">Single Day</option>
            <option value="last_n_days">Last N Days</option>
            <option value="ytd">YTD</option>
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Pump Horizon (D)</span>
          <select className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                  value={value.pump_horizon} onChange={e => set('pump_horizon', Number(e.target.value))}>
            {PUMP_HORIZONS.map(o => <option key={o} value={o}>{o}D</option>)}
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Start Date</span>
          <input type="date" className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                 value={value.start_date || ''} onChange={e => set('start_date', e.target.value)} />
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">End Date</span>
          <input type="date" className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                 value={value.end_date || ''} onChange={e => set('end_date', e.target.value)} />
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Pre-Pump Window (bars)</span>
          <select className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                  value={value.pre_pump_window_bars} onChange={e => set('pre_pump_window_bars', Number(e.target.value))}>
            {PRE_PUMP_WINDOWS.map(o => <option key={o} value={o}>{o} bars</option>)}
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Scanner Detection (bars)</span>
          <select className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                  value={value.scanner_detection_window_bars} onChange={e => set('scanner_detection_window_bars', Number(e.target.value))}>
            {SCANNER_WINDOWS.map(o => <option key={o} value={o}>{o} bars</option>)}
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Detection Reference</span>
          <select className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                  value={value.detection_reference} onChange={e => set('detection_reference', e.target.value)}>
            <option value="before_first_x2_else_before_peak">before_first_x2 → else before_peak</option>
            <option value="before_first_x2">before first x2</option>
            <option value="before_peak">before peak</option>
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Lookback Bars</span>
          <select className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                  value={value.lookback_bars} onChange={e => set('lookback_bars', Number(e.target.value))}>
            {LOOKBACK_BARS_OPTIONS.map(o => <option key={o} value={o}>{o}</option>)}
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Split Impact Window (D)</span>
          <select className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                  value={value.split_impact_window_days} onChange={e => set('split_impact_window_days', Number(e.target.value))}>
            {SPLIT_WINDOWS.map(o => <option key={o} value={o}>{o}D</option>)}
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-md-on-surface-var">Benchmark</span>
          <input className="bg-md-surface-high border border-white/[0.07] rounded px-2 py-1"
                 value={value.benchmark_symbol} onChange={e => set('benchmark_symbol', e.target.value.toUpperCase())} />
        </label>
      </div>
      <div className="flex flex-wrap gap-2 pt-2">
        <button onClick={onRun} disabled={running}
                className="px-3 py-1.5 text-xs bg-emerald-700 hover:bg-emerald-600 disabled:bg-md-surface-high disabled:text-md-on-surface-var rounded">
          {running ? 'Run in progress…' : 'Start ULTRA Pump Research'}
        </button>
      </div>
    </div>
  )
}

function PumpTargetCards({ onPick }) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      <button onClick={() => onPick('X2_TO_X4')}
              className="bg-md-surface-con border border-white/[0.07] hover:border-emerald-600/40 rounded p-4 text-left">
        <div className="text-sm font-semibold text-emerald-200">X2 → X4 ULTRA Pump Research</div>
        <div className="text-[12px] text-md-on-surface-var mt-1">
          Episodes where max forward gain is ≥ 100% and &lt; 300% within the pump horizon.
          The bread-and-butter setup for finding 2× → 4× movers.
        </div>
      </button>
      <button onClick={() => onPick('X4_PLUS')}
              className="bg-md-surface-con border border-white/[0.07] hover:border-fuchsia-600/40 rounded p-4 text-left">
        <div className="text-sm font-semibold text-fuchsia-200">X4+ ULTRA Monster Pump Research</div>
        <div className="text-[12px] text-md-on-surface-var mt-1">
          Episodes where max forward gain is ≥ 300%. Rare, high-conviction monster
          patterns — what every researcher chases.
        </div>
      </button>
    </div>
  )
}

function ProgressBar({ state }) {
  if (!state || !state.running) return null
  const pct = state.symbols_total > 0
    ? Math.min(100, Math.round(state.symbols_completed * 100 / state.symbols_total))
    : 0
  return (
    <div className="bg-md-surface-con border border-white/[0.07] rounded p-3">
      <div className="flex justify-between text-[11px] text-md-on-surface-var">
        <span>Phase: <span className="text-md-on-surface">{state.phase || '—'}</span></span>
        <span>{state.symbols_completed}/{state.symbols_total} symbols · {state.episodes_found} episodes</span>
      </div>
      <div className="mt-1 h-1.5 bg-md-surface-high rounded overflow-hidden">
        <div className="h-full bg-emerald-600" style={{ width: `${pct}%` }} />
      </div>
      <div className="text-[11px] text-md-on-surface-var mt-1">{state.phase_message}</div>
    </div>
  )
}

function ControlBar({ runId, onAction, busy }) {
  if (!runId) return null
  return (
    <div className="flex gap-2 text-[11px]">
      <Tooltip text="Re-derive patterns / lift / split / recommendations from the existing episodes parquet. Does NOT re-fetch market bars or re-detect pumps. Fastest.">
        <button onClick={() => onAction('rebuild')} disabled={busy}
                className="px-2 py-1 bg-blue-900 hover:bg-blue-800 disabled:bg-md-surface-high rounded">Rebuild</button>
      </Tooltip>
      <Tooltip text="Same as Rebuild but lets you change derived-only settings (e.g. split_impact_window_days) without invalidating the detected episodes.">
        <button onClick={() => onAction('recalculate')} disabled={busy}
                className="px-2 py-1 bg-violet-900 hover:bg-violet-800 disabled:bg-md-surface-high rounded">Recalculate</button>
      </Tooltip>
      <Tooltip text="Re-run the ENTIRE pipeline from scratch using the original settings. Re-fetches all market bars and creates a NEW run_id. Slowest.">
        <button onClick={() => onAction('full-rescan')} disabled={busy}
                className="px-2 py-1 bg-orange-900 hover:bg-orange-800 disabled:bg-md-surface-high rounded">Full Rescan</button>
      </Tooltip>
    </div>
  )
}

function VerdictBadge({ v }) {
  return <span className={`text-[10px] px-1.5 py-0.5 rounded ${VERDICT_BADGE[v] || 'bg-md-surface-high'}`}>{v || '—'}</span>
}

function SplitBadge({ s }) {
  return <span className={`text-[10px] px-1.5 py-0.5 rounded ${SPLIT_BADGE[s] || 'bg-md-surface-high'}`}>{s || '—'}</span>
}

function DataTable({ columns, rows, emptyMsg }) {
  if (!rows || rows.length === 0) {
    return <div className="p-4 text-[12px] text-md-on-surface-var">{emptyMsg || 'No rows.'}</div>
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-[11px] border-collapse">
        <thead>
          <tr className="text-md-on-surface-var">
            {columns.map(c => (
              <th key={c.key} className="text-left px-2 py-1 border-b border-white/[0.07] font-medium">{c.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="hover:bg-white/[0.02]">
              {columns.map(c => (
                <td key={c.key} className="px-2 py-1 border-b border-white/[0.04] text-md-on-surface">
                  {c.render ? c.render(r) : (r[c.key] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const EPISODE_COLS = [
  { key: 'symbol', label: 'Symbol' },
  { key: 'category', label: 'Cat' },
  { key: 'anchor_date', label: 'Anchor' },
  { key: 'max_gain_pct', label: 'Max Gain', render: r => fmtPct(r.max_gain_pct, 1) },
  { key: 'days_to_peak', label: 'Days→Peak' },
  { key: 'days_to_first_x2', label: 'Days→2×' },
  { key: 'max_drawdown_before_peak_pct', label: 'DD pre-peak', render: r => fmtPct(r.max_drawdown_before_peak_pct, 1) },
  { key: 'caught_status', label: 'Status' },
]

const PATTERN_COLS = [
  { key: 'pattern_key', label: 'Pattern' },
  { key: 'pattern_type', label: 'Type' },
  { key: 'pump_count', label: 'Pumps' },
  { key: 'pump_episode_coverage_pct', label: 'Coverage', render: r => fmtPct(r.pump_episode_coverage_pct, 2) },
  { key: 'baseline_frequency_pct', label: 'Base Freq', render: r => fmtPct(r.baseline_frequency_pct, 2) },
  { key: 'lift_vs_baseline', label: 'Lift', render: r => fmtFloat(r.lift_vs_baseline, 2) },
  { key: 'precision', label: 'Prec', render: r => fmtFloat(r.precision, 3) },
  { key: 'median_future_gain', label: 'Med Gain', render: r => fmtPct(r.median_future_gain, 1) },
]

export default function UltraPumpResearchPanel() {
  const [legacy, setLegacy] = useState(false)
  const [activeTab, setActiveTab] = useState('overview')
  const [settings, setSettings] = useState(() => ({
    universe: 'all_us',
    mode: 'date_range',
    pump_target: 'X2_TO_X4',
    start_date: '',
    end_date: '',
    pump_horizon: 60,
    pre_pump_window_bars: 14,
    scanner_detection_window_bars: 14,
    detection_reference: 'before_first_x2_else_before_peak',
    lookback_bars: 500,
    split_impact_window_days: 30,
    benchmark_symbol: 'QQQ',
  }))

  const [state, setState] = useState(null)
  const [history, setHistory] = useState([])
  const [runId, setRunId] = useState(null)
  const [err, setErr] = useState(null)
  const [busy, setBusy] = useState(false)

  // Artifacts
  const [episodes, setEpisodes] = useState([])
  const [caught, setCaught] = useState([])
  const [missed, setMissed] = useState([])
  const [patterns, setPatterns] = useState([])
  const [lift, setLift] = useState([])
  const [splitImpact, setSplitImpact] = useState([])
  const [recs, setRecs] = useState(null)
  const [bundle, setBundle] = useState(null)
  const [manifest, setManifest] = useState(null)
  const [emptyConfirm, setEmptyConfirm] = useState(null)

  const refreshState = useCallback(async () => {
    try {
      const s = await api('/api/ultra-pump/status')
      setState(s)
      if (s.run_id && !runId) setRunId(s.run_id)
    } catch (e) { /* ignore */ }
  }, [runId])

  const refreshHistory = useCallback(async () => {
    try {
      const h = await api('/api/ultra-pump/history?limit=30')
      setHistory(h || [])
    } catch (e) { setErr(e.message) }
  }, [])

  const refreshArtifacts = useCallback(async (rid) => {
    if (!rid) return
    try {
      const [ep, ca, mi, pa, li, sp, re, bu, ma] = await Promise.all([
        api(`/api/ultra-pump/${rid}/episodes?limit=500`),
        api(`/api/ultra-pump/${rid}/caught?limit=500`),
        api(`/api/ultra-pump/${rid}/missed?limit=500`),
        api(`/api/ultra-pump/${rid}/ultra-patterns?limit=500`),
        api(`/api/ultra-pump/${rid}/pattern-lift?limit=500`),
        api(`/api/ultra-pump/${rid}/split-impact?limit=500`),
        api(`/api/ultra-pump/${rid}/recommendations`).catch(() => null),
        api(`/api/ultra-pump/${rid}/research-bundle`).catch(() => null),
        api(`/api/ultra-pump/${rid}/export-manifest`).catch(() => null),
      ])
      setEpisodes(ep?.rows || [])
      setCaught(ca?.rows || [])
      setMissed(mi?.rows || [])
      setPatterns(pa?.rows || [])
      setLift(li?.rows || [])
      setSplitImpact(sp?.rows || [])
      setRecs(re)
      setBundle(bu)
      setManifest(ma)
    } catch (e) { setErr(e.message) }
  }, [])

  useEffect(() => {
    refreshState()
    refreshHistory()
  }, [refreshState, refreshHistory])

  useEffect(() => {
    if (!state?.running) return
    const t = setInterval(refreshState, 2000)
    return () => clearInterval(t)
  }, [state?.running, refreshState])

  // When a run finishes, auto-refresh artifacts + history
  const prevRunning = useRef(false)
  useEffect(() => {
    if (prevRunning.current && state && !state.running) {
      refreshHistory()
      if (state.run_id) refreshArtifacts(state.run_id)
    }
    prevRunning.current = !!state?.running
  }, [state, refreshHistory, refreshArtifacts])

  useEffect(() => { if (runId) refreshArtifacts(runId) }, [runId, refreshArtifacts])

  const startRun = useCallback(async () => {
    setErr(null); setBusy(true)
    try {
      const r = await api('/api/ultra-pump/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settings),
      })
      setRunId(r.run_id)
      await refreshState()
      await refreshHistory()
    } catch (e) { setErr(e.message) }
    finally { setBusy(false) }
  }, [settings, refreshState, refreshHistory])

  const doAction = useCallback(async (kind) => {
    if (!runId) return
    setErr(null); setBusy(true)
    try {
      const r = await api(`/api/ultra-pump/${runId}/${kind}`, { method: 'POST' })
      if (r.run_id && kind === 'full-rescan') setRunId(r.run_id)
      await refreshState()
      await refreshHistory()
    } catch (e) { setErr(e.message) }
    finally { setBusy(false) }
  }, [runId, refreshState, refreshHistory])

  const exportArtifact = useCallback(async (part, fmt = 'json') => {
    if (!runId) return
    // Check manifest for empty status
    const entry = manifest?.artifacts?.find(a => a.artifact_name === part)
    if (entry && entry.row_count === 0) {
      setEmptyConfirm({ part, fmt })
      return
    }
    window.location.href = `${API}/api/ultra-pump/${runId}/export?part=${part}&fmt=${fmt}`
  }, [runId, manifest])

  const confirmEmptyDownload = useCallback(() => {
    if (!emptyConfirm) return
    const { part, fmt } = emptyConfirm
    setEmptyConfirm(null)
    window.location.href = `${API}/api/ultra-pump/${runId}/export?part=${part}&fmt=${fmt}`
  }, [emptyConfirm, runId])

  if (legacy) {
    return (
      <div className="space-y-3">
        <div className="flex items-center justify-between bg-md-surface-con border border-white/[0.07] rounded p-3">
          <div className="text-sm text-md-on-surface">Legacy Signal Replay</div>
          <button onClick={() => setLegacy(false)}
                  className="px-3 py-1 text-xs bg-emerald-700 hover:bg-emerald-600 rounded">
            Back to ULTRA Pump Research
          </button>
        </div>
        <SignalReplayPanel />
      </div>
    )
  }

  const summary = bundle?.summary || {}
  const verdicts = bundle?.verdict_counts || {}

  return (
    <div className="space-y-3">
      {/* Top action bar */}
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-base font-semibold text-md-on-surface">ULTRA Pump Research Engine</div>
        <div className="flex gap-2">
          <ControlBar runId={runId} onAction={doAction} busy={busy || state?.running} />
          <button onClick={() => setLegacy(true)}
                  className="px-3 py-1 text-xs bg-md-surface-high border border-white/[0.07] rounded hover:border-white/20">
            Legacy Signal Replay
          </button>
        </div>
      </div>

      {/* Pump target quick-picker */}
      <PumpTargetCards onPick={target => setSettings(s => ({ ...s, pump_target: target }))} />

      {/* Settings */}
      <SettingsPanel value={settings} onChange={setSettings} onRun={startRun} running={state?.running || busy} />

      {/* Progress */}
      <ProgressBar state={state} />

      {err && <div className="bg-red-900/40 border border-red-700 rounded p-2 text-[12px] text-red-200">{err}</div>}

      {/* Tabs */}
      <div className="border-b border-white/[0.07] overflow-x-auto">
        <div className="flex gap-1 text-[12px]">
          {TABS.map(t => (
            <button key={t.id} onClick={() => setActiveTab(t.id)}
                    className={`px-3 py-1.5 -mb-px border-b-2 whitespace-nowrap ${
                      activeTab === t.id
                        ? 'border-emerald-500 text-md-on-surface'
                        : 'border-transparent text-md-on-surface-var hover:text-md-on-surface'
                    }`}>
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* Tab content */}
      <div>
        {activeTab === 'overview' && (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Card title="Total Episodes" value={fmtNum(summary.total_episodes)} />
            <Card title="X2 → X4" value={fmtNum(summary.x2_to_x4_count)} />
            <Card title="X4+ Monsters" value={fmtNum(summary.x4_plus_count)} />
            <Card title="Caught / Missed"
                  value={`${fmtNum(summary.caught_count)} / ${fmtNum(summary.missed_count)}`} />
            <Card title="Patterns Mined" value={fmtNum(summary.pattern_count)} />
            <Card title="Baseline Total" value={fmtNum(summary.baseline_total)} />
            <Card title="Promote" value={fmtNum(verdicts.PROMOTE)} sub="lift≥3, prec≥0.3, n≥5" />
            <Card title="Watch" value={fmtNum(verdicts.WATCH)} sub="1.5≤lift<3, n≥3" />
          </div>
        )}

        {activeTab === 'x2' && (
          <DataTable columns={EPISODE_COLS}
                     rows={episodes.filter(e => e.category === 'X2_TO_X4')}
                     emptyMsg="No X2→X4 episodes yet — run the engine first." />
        )}

        {activeTab === 'x4' && (
          <DataTable columns={EPISODE_COLS}
                     rows={episodes.filter(e => e.category === 'X4_PLUS')}
                     emptyMsg="No X4+ monster pump episodes yet." />
        )}

        {activeTab === 'episodes' && (
          <DataTable columns={EPISODE_COLS} rows={episodes}
                     emptyMsg="No episodes yet — run the engine first." />
        )}

        {activeTab === 'caught' && (
          <DataTable columns={[
            { key: 'symbol', label: 'Symbol' },
            { key: 'anchor_date', label: 'Anchor' },
            { key: 'caught_bar_offset_from_anchor', label: 'Bars Before Anchor' },
            { key: 'strongest_pre_pump_score', label: 'Best Score', render: r => fmtFloat(r.strongest_pre_pump_score, 1) },
          ]} rows={caught} emptyMsg="No caught pumps yet — historical ULTRA scanner snapshots are needed to label CAUGHT vs MISSED accurately." />
        )}

        {activeTab === 'missed' && (
          <DataTable columns={[
            { key: 'symbol', label: 'Symbol' },
            { key: 'anchor_date', label: 'Anchor' },
            { key: 'missed_reason_primary', label: 'Primary Reason' },
            { key: 'missed_reason_secondary', label: 'Secondary' },
          ]} rows={missed} emptyMsg="No missed pumps." />
        )}

        {activeTab === 'signals' && (
          <DataTable columns={PATTERN_COLS}
                     rows={patterns.filter(p => p.pattern_type === 'signal')}
                     emptyMsg="No individual signals mined yet." />
        )}

        {activeTab === 'lift' && (
          <DataTable columns={[
            { key: 'pattern_key', label: 'Pattern' },
            { key: 'lift_all', label: 'All', render: r => fmtFloat(r.lift_all, 2) },
            { key: 'lift_clean_non_split', label: 'Clean', render: r => fmtFloat(r.lift_clean_non_split, 2) },
            { key: 'lift_split_related', label: 'Split-Related', render: r => fmtFloat(r.lift_split_related, 2) },
            { key: 'lift_post_reverse_split', label: 'Post-Rev-Split', render: r => fmtFloat(r.lift_post_reverse_split, 2) },
          ]} rows={lift} emptyMsg="No lift rows yet." />
        )}

        {activeTab === 'combos' && (
          <DataTable columns={PATTERN_COLS}
                     rows={patterns.filter(p => p.pattern_type === 'combo')}
                     emptyMsg="No combos mined yet." />
        )}

        {activeTab === 'split' && (
          <DataTable columns={[
            { key: 'pattern_key', label: 'Pattern' },
            { key: 'split_status', label: 'Partition', render: r => <SplitBadge s={r.split_status} /> },
            { key: 'count', label: 'Count' },
            { key: 'lift', label: 'Lift', render: r => fmtFloat(r.lift, 2) },
            { key: 'precision', label: 'Prec', render: r => fmtFloat(r.precision, 3) },
          ]} rows={splitImpact} emptyMsg="No split-impact stats yet." />
        )}

        {activeTab === 'recs' && (
          <DataTable columns={[
            { key: 'pattern_key', label: 'Pattern' },
            { key: 'verdict', label: 'Verdict', render: r => <VerdictBadge v={r.verdict} /> },
            { key: 'badges', label: 'Badges', render: r => (r.badges || []).map(b =>
                <span key={b} className="text-[10px] mr-1 px-1.5 py-0.5 rounded bg-md-surface-high">{b}</span>) },
            { key: 'lift_vs_baseline', label: 'Lift', render: r => fmtFloat(r.lift_vs_baseline, 2) },
            { key: 'lift_clean_non_split', label: 'Clean Lift', render: r => fmtFloat(r.lift_clean_non_split, 2) },
            { key: 'precision', label: 'Prec', render: r => fmtFloat(r.precision, 3) },
            { key: 'pump_count', label: 'Pumps' },
          ]} rows={recs?.recommendations || []}
             emptyMsg="No recommendations yet — run the engine and let Phase 5 build them." />
        )}

        {activeTab === 'exports' && (
          <div className="space-y-3">
            {[
              { group: 'Summary', items: [
                { part: 'run', label: 'Run Metadata' },
                { part: 'research_bundle', label: 'Research Bundle' },
                { part: 'export_manifest', label: 'Export Manifest' },
                { part: 'warnings', label: 'Warnings' },
              ] },
              { group: 'Pump Episodes', items: [
                { part: 'pump_episodes', label: 'All Episodes' },
                { part: 'x2_to_x4_episodes', label: 'X2 → X4' },
                { part: 'x4_plus_episodes', label: 'X4+ Monsters' },
                { part: 'scanner_caught_pumps', label: 'Caught' },
                { part: 'missed_pumps', label: 'Missed' },
              ] },
              { group: 'ULTRA Pre-Pump', items: [
                { part: 'pre_pump_ultra_bars', label: 'Pre-Pump Bars' },
                { part: 'pre_pump_ultra_signals', label: 'Pre-Pump Signals' },
                { part: 'pre_pump_ultra_combinations', label: 'Pre-Pump Combinations' },
              ] },
              { group: 'Pattern Research', items: [
                { part: 'ultra_pattern_stats', label: 'Pattern Stats' },
                { part: 'ultra_pattern_lift_stats', label: 'Pattern Lift' },
                { part: 'ultra_timing_stats', label: 'Timing' },
                { part: 'baseline_windows', label: 'Baseline Windows' },
                { part: 'baseline_pattern_stats', label: 'Baseline Pattern Stats' },
              ] },
              { group: 'Split Impact', items: [
                { part: 'split_impact_stats', label: 'Split Impact Stats' },
                { part: 'split_related_pumps', label: 'Split-Related' },
                { part: 'clean_non_split_pumps', label: 'Clean Non-Split' },
                { part: 'post_reverse_split_pumps', label: 'Post-Reverse-Split' },
              ] },
              { group: 'Diagnostics', items: [
                { part: 'missed_diagnostics', label: 'Missed Diagnostics' },
                { part: 'ultra_recommendations', label: 'Recommendations' },
              ] },
              { group: 'Full Package', items: [
                { part: 'all_non_empty_zip', label: 'All Non-Empty Artifacts (ZIP)' },
              ] },
            ].map(group => (
              <div key={group.group} className="bg-md-surface-con border border-white/[0.07] rounded p-3">
                <div className="text-[12px] font-semibold text-md-on-surface mb-2">{group.group}</div>
                <div className="flex flex-wrap gap-2">
                  {group.items.map(it => {
                    const entry = manifest?.artifacts?.find(a => a.artifact_name === it.part)
                    const empty = entry && entry.row_count === 0
                    return (
                      <div key={it.part} className="flex items-center gap-1">
                        <button onClick={() => exportArtifact(it.part, 'json')}
                                className={`px-2 py-1 text-[11px] rounded border border-white/[0.07] ${
                                  empty ? 'bg-md-surface-high text-md-on-surface-var' : 'bg-blue-900/40 hover:bg-blue-900/60 text-md-on-surface'
                                }`}>
                          {it.label} {empty && <span className="ml-1 text-[10px]">(empty)</span>}
                        </button>
                        {it.part !== 'all_non_empty_zip' && it.part !== 'run' && (
                          <button onClick={() => exportArtifact(it.part, 'csv')}
                                  className="px-1.5 py-1 text-[10px] rounded bg-md-surface-high hover:bg-md-surface-high/80 border border-white/[0.07]">CSV</button>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            ))}
          </div>
        )}

        {activeTab === 'history' && (
          <DataTable columns={[
            { key: 'id', label: 'Run' },
            { key: 'status', label: 'Status' },
            { key: 'universe', label: 'Universe' },
            { key: 'pump_target', label: 'Target' },
            { key: 'start_date', label: 'Start' },
            { key: 'end_date', label: 'End' },
            { key: 'total_episodes', label: 'Episodes' },
            { key: 'total_caught', label: 'Caught' },
            { key: 'total_missed', label: 'Missed' },
            { key: 'started_at', label: 'Started' },
            { key: 'finished_at', label: 'Finished' },
            { key: 'actions', label: 'Actions', render: r => (
              <button onClick={() => setRunId(r.id)}
                      className="text-emerald-300 hover:text-emerald-200">Load</button>
            ) },
          ]} rows={history} emptyMsg="No runs yet." />
        )}
      </div>

      {/* Empty-artifact confirm dialog */}
      {emptyConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="bg-md-surface-con border border-white/[0.07] rounded p-4 max-w-md">
            <div className="text-sm font-semibold text-md-on-surface mb-2">This artifact has 0 rows.</div>
            <div className="text-[12px] text-md-on-surface-var mb-3">
              Download anyway? The file will be valid JSON/CSV but contain no data — only schema + a
              "reason_if_empty" explanation.
            </div>
            <div className="flex justify-end gap-2">
              <button onClick={() => setEmptyConfirm(null)}
                      className="px-3 py-1 text-xs bg-md-surface-high rounded">Cancel</button>
              <button onClick={confirmEmptyDownload}
                      className="px-3 py-1 text-xs bg-emerald-700 hover:bg-emerald-600 rounded">Download anyway</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
