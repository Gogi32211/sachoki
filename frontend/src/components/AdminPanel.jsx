import { useState, useEffect, useRef, useCallback } from 'react'
import { api } from '../api'
import { turboCacheSet } from './TurboScanPanel'
import { getCacheBackend, setCacheBackend } from '../turboCache'
import {
  Card, CardHeader,
  Button,
  Badge,
  LinearProgress,
  Alert,
  Stat,
  PageHeader,
  StatusChip,
  EmptyState,
  FilterChip,
} from '../design-system'

const UNIVERSES = [
  { key: 'sp500',     label: 'S&P 500'   },
  { key: 'nasdaq',    label: 'NASDAQ'    },
  { key: 'russell2k', label: 'Russell 2K'},
  { key: 'all_us',    label: 'All US'    },
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

function ChipRow({ children }) {
  return <div className="flex flex-wrap gap-1.5">{children}</div>
}

function FilterNumInput({ label, value, onChange, min, max, step = 1, placeholder }) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-xs text-md-on-surface-var font-medium">{label}</label>
      <input
        type="number"
        value={value}
        min={min}
        max={max}
        step={step}
        placeholder={placeholder}
        onChange={e => onChange(e.target.value === '' ? '' : Number(e.target.value))}
        className="w-20 px-2 py-1.5 rounded-md-sm bg-md-surface-high border border-md-outline-var text-md-on-surface text-xs focus:border-md-primary focus:outline-none transition-colors"
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
  const [caching,     setCaching]    = useState(false)
  const [cacheMode,   setCacheModeS] = useState(() => getCacheBackend())
  const pollRef    = useRef(null)
  const scanParams = useRef({ tf: '1d', uni: 'sp500' })

  // ── Stock Stat state ──
  const [ssUni,    setSsUni]    = useState('sp500')
  const [ssTf,     setSsTf]     = useState('1d')
  const [ssBars,   setSsBars]   = useState(150)
  const [ssStatus, setSsStatus] = useState(null)
  const [ssError,  setSsError]  = useState(null)
  const ssPollRef  = useRef(null)

  const setCacheMode  = (val) => { setCacheBackend(val); setCacheModeS(val) }
  const fetchStatus   = () => api.turboScanStatus().then(setStatus).catch(() => {})
  const fetchHistory  = () => api.adminScanHistory().then(setHistory).catch(() => {})
  const fetchSsStatus = () => api.stockStatStatus().then(setSsStatus).catch(() => {})

  useEffect(() => {
    fetchStatus()
    fetchHistory()
    pollRef.current = setInterval(fetchStatus, 2000)
    return () => clearInterval(pollRef.current)
  }, [])

  useEffect(() => {
    fetchSsStatus()
    ssPollRef.current = setInterval(fetchSsStatus, 2000)
    return () => clearInterval(ssPollRef.current)
  }, [])

  const prevRunning = useRef(false)
  useEffect(() => {
    if (prevRunning.current && status && !status.running && !status.error) {
      fetchHistory()
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
            window.dispatchEvent(new CustomEvent('sachoki:scan-cached', {
              detail: { tf: scanTf, uni: scanUni, results, lastScan: d.last_scan },
            }))
          }
        })
        .finally(() => setCaching(false))
    }
    prevRunning.current = status?.running ?? false
  }, [status?.running])

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

  const startStockStat = () => {
    setSsError(null)
    api.stockStatTrigger(ssTf, ssUni, ssBars)
      .then(() => fetchSsStatus())
      .catch(e => setSsError(e?.detail || e?.message || String(e)))
  }

  const running  = status?.running ?? false
  const done     = status?.done    ?? 0
  const total    = status?.total   ?? 0
  const found    = status?.found   ?? 0
  const failed   = status?.failed  ?? 0
  const elapsed  = status?.elapsed ?? 0
  const eta      = status?.eta
  const scanErr  = status?.error
  const progress = pct(done, total)

  return (
    <div className="p-4 max-w-3xl mx-auto space-y-5">

      {/* ── Page Header ── */}
      <PageHeader
        title="Scan Admin"
        subtitle="Manage market scans and bulk signal computation"
        badge={
          <StatusChip
            status={running ? 'running' : 'idle'}
            label={running ? 'Running' : 'Idle'}
          />
        }
      />

      {/* ── Cache Backend ── */}
      <Card variant="outlined">
        <CardHeader title="Cache Backend" subtitle="Where scan results are stored between sessions" />
        <div className="flex gap-2 flex-wrap">
          {[
            { key: 'ls',  label: 'localStorage',  desc: 'score≥5 filter · fast · 5 MB limit' },
            { key: 'idb', label: 'IndexedDB',      desc: 'all tickers · no size limit · robust' },
          ].map(m => (
            <button
              key={m.key}
              onClick={() => setCacheMode(m.key)}
              className={[
                'flex flex-col items-start px-3 py-2 rounded-md-md border text-xs transition-colors',
                cacheMode === m.key
                  ? 'bg-md-primary-container border-md-primary/40 text-md-on-primary-container'
                  : 'bg-md-surface-high border-md-outline-var text-md-on-surface-var hover:border-md-outline',
              ].join(' ')}
            >
              <span className="font-semibold">{m.label}</span>
              <span className="opacity-70 mt-0.5">{m.desc}</span>
            </button>
          ))}
        </div>
        <p className="text-md-on-surface-var text-xs mt-3">
          {cacheMode === 'idb'
            ? 'IndexedDB mode: all tickers stored (no score filter). Scan saves ~5000 results.'
            : 'localStorage mode: only score≥5 tickers stored (~2000). Smaller, faster.'}
        </p>
      </Card>

      {/* ── Start New Scan ── */}
      <Card variant="outlined">
        <CardHeader title="Start New Scan" />
        <div className="space-y-3">

          <div className="flex flex-wrap gap-4">
            <div>
              <p className="text-md-on-surface-var text-xs mb-1.5">Universe</p>
              <ChipRow>
                {UNIVERSES.map(u => (
                  <FilterChip
                    key={u.key}
                    label={u.label}
                    selected={universe === u.key}
                    disabled={running}
                    onToggle={() => setUniverse(u.key)}
                  />
                ))}
              </ChipRow>
            </div>
            <div>
              <p className="text-md-on-surface-var text-xs mb-1.5">Timeframe</p>
              <ChipRow>
                {TFS.map(t => (
                  <FilterChip
                    key={t}
                    label={t}
                    selected={tf === t}
                    disabled={running}
                    onToggle={() => setTf(t)}
                  />
                ))}
              </ChipRow>
            </div>
          </div>

          {/* Filters */}
          <div className="border-t border-md-outline-var pt-3">
            <p className="text-md-on-surface-var text-xs mb-2">Result Filters</p>
            <div className="flex flex-wrap gap-4">
              <div className="flex gap-2 items-end">
                <FilterNumInput label="Price Min $" value={priceMin} onChange={setPriceMin} min={0} step={0.1} placeholder="0" />
                <FilterNumInput label="Price Max $" value={priceMax} onChange={setPriceMax} min={0} step={1}   placeholder="∞" />
              </div>
              <div className="flex gap-2 items-end">
                <FilterNumInput label="RSI Min" value={rsiMin} onChange={setRsiMin} min={0}   max={100} placeholder="0"   />
                <FilterNumInput label="RSI Max" value={rsiMax} onChange={setRsiMax} min={0}   max={100} placeholder="100" />
              </div>
              <div className="flex gap-2 items-end">
                <FilterNumInput label="CCI Min" value={cciMin} onChange={setCciMin} min={-500} max={500} step={10} placeholder="-∞" />
                <FilterNumInput label="CCI Max" value={cciMax} onChange={setCciMax} min={-500} max={500} step={10} placeholder="∞"  />
              </div>
            </div>
          </div>

          <div className="flex gap-2 pt-1">
            <Button variant="filled" onClick={startScan} disabled={running}>
              ⚡ Start Scan
            </Button>
            {running && (
              <Button variant="outlined" onClick={resetScan}>
                Force Stop
              </Button>
            )}
          </div>

          {error && <Alert variant="error">{error}</Alert>}
        </div>
      </Card>

      {/* ── Live Progress ── */}
      {(running || scanErr || (status && done > 0)) && (
        <Card variant="outlined">
          <CardHeader
            title={running ? 'Live Progress' : 'Last Scan'}
            action={
              <Badge variant={running ? 'warning' : scanErr ? 'error' : 'positive'}>
                {running ? `${progress}%` : scanErr ? 'Error' : 'Complete'}
              </Badge>
            }
          />

          <LinearProgress
            value={progress}
            color={running ? 'warning' : 'positive'}
            className="mb-4"
          />

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <Stat label="Fetched"    value={status?.fetched_from_massive || total || '—'} />
            <Stat label="Scanned"    value={`${done} / ${total}`} />
            <Stat label="Into Turbo" value={found}  positive />
            <Stat label="Failed"     value={failed} negative={failed > 0} />
            <Stat label="Elapsed"    value={fmt(elapsed)} />
            <Stat label="ETA"        value={running ? fmt(eta) : '—'} />
            <Stat label="Universe"   value={status?.universe ?? '—'} />
            <Stat label="Timeframe"  value={status?.tf ?? '—'} />
          </div>

          {scanErr && (
            <Alert variant="error" className="mt-3">Error: {scanErr}</Alert>
          )}
          {!running && !scanErr && done > 0 && (
            <Alert variant="success" className="mt-3">
              Scan completed — {found} tickers
              {caching ? ' · saving to Turbo cache…' : ' · cached in Turbo ✓'}
            </Alert>
          )}
        </Card>
      )}

      {/* ── Scan History ── */}
      <Card variant="outlined">
        <CardHeader title="Scan History" subtitle="Last 20 runs" />
        {history.length === 0 ? (
          <EmptyState compact message="No scans yet" icon="📋" />
        ) : (
          <div className="overflow-x-auto -mx-4 px-4">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-md-outline-var">
                  {['#', 'Universe', 'TF', 'Started', 'Duration', 'Results'].map((h, i) => (
                    <th
                      key={h}
                      className={`py-2 pr-3 text-md-on-surface-var font-medium ${i === 5 ? 'text-right pr-0' : 'text-left'}`}
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {history.map(r => {
                  const dur = r.started_at && r.completed_at
                    ? Math.round((new Date(r.completed_at) - new Date(r.started_at)) / 1000)
                    : null
                  const complete = r.completed_at != null
                  return (
                    <tr key={r.id} className="border-b border-md-outline-var/50 hover:bg-md-surface-high/50 transition-colors">
                      <td className="py-2 pr-3 text-md-on-surface-var">{r.id}</td>
                      <td className="py-2 pr-3 text-md-on-surface">{r.universe}</td>
                      <td className="py-2 pr-3 text-md-on-surface-var">{r.tf}</td>
                      <td className="py-2 pr-3 text-md-on-surface-var">{r.started_at?.slice(0, 16).replace('T', ' ')}</td>
                      <td className="py-2 pr-3 text-md-on-surface-var">
                        {complete
                          ? fmt(dur)
                          : <span className="text-md-warning">running…</span>}
                      </td>
                      <td className="py-2 text-right font-mono">
                        <span className={r.result_count > 0 ? 'text-md-positive' : 'text-md-on-surface-var'}>
                          {r.result_count ?? '—'}
                        </span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {/* ── Stock Stat ── */}
      <Card variant="outlined">
        <CardHeader
          title="Stock Stat — Bulk Signal CSV"
          subtitle="Bar-level signal computation for every ticker → combined CSV export"
          action={ssStatus?.running ? <StatusChip status="running" /> : undefined}
        />
        <p className="text-md-on-surface-var text-xs mb-3">
          S&P 500 ≈ 5–10 min · Russell 2K ≈ 20–40 min · All US ≈ 1–2 h
        </p>

        <div className="space-y-3">
          <div className="flex flex-wrap gap-4">
            <div>
              <p className="text-md-on-surface-var text-xs mb-1.5">Universe</p>
              <ChipRow>
                {UNIVERSES.map(u => (
                  <FilterChip
                    key={u.key}
                    label={u.label}
                    selected={ssUni === u.key}
                    disabled={ssStatus?.running}
                    onToggle={() => setSsUni(u.key)}
                  />
                ))}
              </ChipRow>
            </div>
            <div>
              <p className="text-md-on-surface-var text-xs mb-1.5">Timeframe</p>
              <ChipRow>
                {TFS.map(t => (
                  <FilterChip
                    key={t}
                    label={t}
                    selected={ssTf === t}
                    disabled={ssStatus?.running}
                    onToggle={() => setSsTf(t)}
                  />
                ))}
              </ChipRow>
            </div>
            <FilterNumInput
              label="Bars"
              value={ssBars}
              onChange={v => setSsBars(v === '' ? 150 : Number(v))}
              min={10}
              max={500}
            />
          </div>

          <Button
            variant="tonal"
            onClick={startStockStat}
            disabled={ssStatus?.running}
          >
            📊 Run Stock Stat
          </Button>

          {/* Progress */}
          {ssStatus && (ssStatus.running || ssStatus.output_path || ssStatus.error) && (
            <div className="space-y-2 pt-1">
              <LinearProgress
                value={pct(ssStatus.done ?? 0, ssStatus.total || 1)}
                color={ssStatus.running ? 'primary' : 'positive'}
              />
              <div className="flex flex-wrap gap-4 text-xs text-md-on-surface-var">
                <span>{ssStatus.done ?? 0} / {ssStatus.total ?? 0} tickers</span>
                <span>{fmt(ssStatus.elapsed)}</span>
                {ssStatus.universe && (
                  <span>{ssStatus.universe} · {ssStatus.tf}</span>
                )}
              </div>
              {ssStatus.error && (
                <Alert variant="error">Error: {ssStatus.error}</Alert>
              )}
              {!ssStatus.running && !ssStatus.error && ssStatus.output_path && (
                <div className="flex items-center gap-3">
                  <Alert variant="success" className="flex-1">
                    Done — {ssStatus.done} tickers ·{' '}
                    {Math.round((ssStatus.output_size ?? 0) / 1024 / 1024 * 10) / 10} MB
                  </Alert>
                  <a
                    href={api.stockStatDownloadUrl()}
                    download
                    className="shrink-0 inline-flex items-center px-3 py-1.5 rounded-md-md text-xs font-semibold bg-md-positive text-black hover:opacity-90 transition-opacity"
                  >
                    ⬇ Download CSV
                  </a>
                </div>
              )}
            </div>
          )}
          {ssError && <Alert variant="error">{ssError}</Alert>}
        </div>
      </Card>

      {/* ── Database Maintenance ── (260523 Phase 1) */}
      <DbMaintenanceCard />

    </div>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Database Maintenance — per-table prune by age
// ──────────────────────────────────────────────────────────────────────────

function DbMaintenanceCard() {
  const [stats, setStats]     = useState([])
  const [loading, setLoading] = useState(false)
  const [days, setDays]       = useState(30)
  const [error, setError]     = useState('')
  const [lastAction, setLastAction] = useState(null)

  const loadStats = useCallback(async () => {
    setLoading(true); setError('')
    try {
      const r = await fetch('/api/admin/db-stats')
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const j = await r.json()
      setStats(j.tables || [])
    } catch (e) {
      setError(String(e.message || e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { loadStats() }, [loadStats])

  const callPrune = async (table, dryRun) => {
    setError(''); setLastAction(null)
    if (!dryRun) {
      const ok = window.confirm(
        `Delete rows from ${table} older than ${days} days? This cannot be undone.`,
      )
      if (!ok) return
    }
    try {
      const u = new URLSearchParams({
        table, older_than_days: String(days),
        dry_run: String(dryRun),
        allow_protected: 'false',
      })
      const r = await fetch('/api/admin/db-prune?' + u.toString(), {method: 'POST'})
      const j = await r.json()
      if (!r.ok) throw new Error(j.detail || JSON.stringify(j))
      setLastAction(j)
      if (!dryRun) loadStats()
    } catch (e) {
      setError(String(e.message || e))
    }
  }

  const callPruneAll = async (dryRun) => {
    setError(''); setLastAction(null)
    if (!dryRun) {
      const ok = window.confirm(
        `Delete rows from ALL non-protected tables older than ${days} days? ` +
        `This cannot be undone.`,
      )
      if (!ok) return
    }
    try {
      const u = new URLSearchParams({
        older_than_days: String(days),
        dry_run: String(dryRun),
        include_protected: 'false',
      })
      const r = await fetch('/api/admin/db-prune-all?' + u.toString(), {method: 'POST'})
      const j = await r.json()
      if (!r.ok) throw new Error(j.detail || JSON.stringify(j))
      setLastAction(j)
      if (!dryRun) loadStats()
    } catch (e) {
      setError(String(e.message || e))
    }
  }

  const fmtRows = (n) => n == null ? '—' : n.toLocaleString()

  return (
    <Card variant="outlined">
      <CardHeader
        title="Database Maintenance"
        subtitle="Prune old rows from time-series tables"
      />

      <div className="flex items-center gap-3 mb-3">
        <label className="text-xs text-md-on-surface-var">Retention (days)</label>
        <input
          type="number"
          min={1}
          max={3650}
          value={days}
          onChange={e => setDays(Math.max(1, Number(e.target.value) || 30))}
          className="w-20 px-2 py-1 bg-md-surface-high border border-md-outline-var rounded-md-sm text-sm"
        />
        <button
          onClick={loadStats}
          disabled={loading}
          className="px-3 py-1 text-xs rounded-md-sm bg-md-surface-high border border-md-outline-var text-md-on-surface-var hover:text-white disabled:opacity-50">
          {loading ? '...' : 'Refresh stats'}
        </button>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="text-md-on-surface-var">
            <tr className="border-b border-md-outline-var/30">
              <th className="px-2 py-1.5 text-left">Table</th>
              <th className="px-2 py-1.5 text-right">Rows</th>
              <th className="px-2 py-1.5 text-left">Oldest</th>
              <th className="px-2 py-1.5 text-left">Newest</th>
              <th className="px-2 py-1.5 text-center">Actions</th>
            </tr>
          </thead>
          <tbody>
            {stats.map(s => (
              <tr key={s.table} className="border-b border-md-outline-var/20">
                <td className="px-2 py-1.5">
                  <span className="font-mono">{s.table}</span>
                  {s.protected && <span className="ml-2 px-1 py-0.5 rounded-sm bg-amber-900/40 text-amber-300 text-[10px] font-semibold">PROTECTED</span>}
                  {!s.exists && <span className="ml-2 text-gray-500 text-[10px]">(not created)</span>}
                  <div className="text-[10px] text-md-on-surface-var/70">{s.desc}</div>
                </td>
                <td className="px-2 py-1.5 text-right font-mono">{fmtRows(s.rows)}</td>
                <td className="px-2 py-1.5 font-mono text-[10px]">{s.oldest || '—'}</td>
                <td className="px-2 py-1.5 font-mono text-[10px]">{s.newest || '—'}</td>
                <td className="px-2 py-1.5 text-center">
                  {s.exists && !s.protected && s.rows > 0 ? (
                    <div className="flex gap-1 justify-center">
                      <button
                        onClick={() => callPrune(s.table, true)}
                        title="Show how many rows WOULD be deleted (no changes)"
                        className="px-2 py-0.5 text-[10px] rounded-sm bg-blue-900/50 text-blue-200 hover:bg-blue-900/70">
                        Dry run
                      </button>
                      <button
                        onClick={() => callPrune(s.table, false)}
                        title={`Delete rows older than ${days} days`}
                        className="px-2 py-0.5 text-[10px] rounded-sm bg-red-900/50 text-red-200 hover:bg-red-900/70 font-semibold">
                        Prune &gt;{days}d
                      </button>
                    </div>
                  ) : <span className="text-gray-600">—</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-4 p-3 bg-red-950/30 border border-red-900/40 rounded-md-sm">
        <div className="text-xs text-red-300 mb-2">Bulk prune all non-protected tables:</div>
        <div className="flex gap-2">
          <button
            onClick={() => callPruneAll(true)}
            className="px-3 py-1 text-xs rounded-sm bg-blue-900/50 text-blue-200 hover:bg-blue-900/70">
            Dry run ALL
          </button>
          <button
            onClick={() => callPruneAll(false)}
            className="px-3 py-1 text-xs rounded-sm bg-red-700/60 text-red-100 hover:bg-red-700/80 font-semibold">
            ⚠ Prune ALL non-protected &gt;{days}d
          </button>
        </div>
      </div>

      {lastAction && (
        <div className="mt-3 p-2 bg-md-surface-high border border-md-outline-var rounded-md-sm text-xs font-mono">
          {lastAction.dry_run ? '🔍 Dry run' : '✓ Executed'} —
          {' '}{JSON.stringify(lastAction.would_delete ?? lastAction.deleted ?? lastAction.total_affected ?? lastAction, null, 0)}
          {lastAction.table && ` on ${lastAction.table}`}
          {lastAction.cutoff && ` (cutoff ${lastAction.cutoff})`}
        </div>
      )}
      {error && <Alert variant="error" className="mt-2">{error}</Alert>}
    </Card>
  )
}
