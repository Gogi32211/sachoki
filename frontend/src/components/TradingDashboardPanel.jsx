/**
 * TradingDashboardPanel.jsx — Market Command Center
 *
 * Answers: What is the market doing RIGHT NOW?
 * Loads all dashboard sections from /api/dashboard/* endpoints.
 */
import { useState, useEffect, useCallback, useRef } from 'react'

const API = '/api/dashboard'
const REFRESH_INTERVAL = 60_000 // 1 min auto-refresh

// ── fetch helper ──────────────────────────────────────────────────────────────
async function apiFetch(path) {
  const r = await fetch(`${API}${path}`)
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
  return r.json()
}

// ── Market Status Timer ───────────────────────────────────────────────────────
function MarketStatusTimer({ market }) {
  const [elapsed, setElapsed] = useState(0)

  useEffect(() => {
    const t = setInterval(() => setElapsed(e => e + 1), 1000)
    return () => clearInterval(t)
  }, [])

  if (!market) return null

  const secs = Math.max(0, (market.secs_to_next || 0) - elapsed)
  const hh = String(Math.floor(secs / 3600)).padStart(2, '0')
  const mm = String(Math.floor((secs % 3600) / 60)).padStart(2, '0')
  const ss = String(secs % 60).padStart(2, '0')

  const phaseLabel = {
    regular:    'MARKET OPEN',
    pre_market: 'PRE-MARKET',
    after_hours:'AFTER HOURS',
    weekend:    'WEEKEND',
    overnight:  'CLOSED',
  }[market.phase] ?? market.phase?.toUpperCase()

  const nextLabel = {
    regular:    'closes in',
    pre_market: 'opens in',
    after_hours:'closes in',
    overnight:  'pre-market in',
    weekend:    '',
  }[market.phase] ?? ''

  const dotCls = market.status === 'open'
    ? 'bg-emerald-400 shadow-[0_0_6px_2px_rgba(52,211,153,0.5)]'
    : market.status === 'pre_market' || market.status === 'after_hours'
    ? 'bg-amber-400'
    : 'bg-slate-500'

  return (
    <div className="flex items-center gap-3 px-4 py-2.5 rounded-lg bg-md-surface-high border border-white/[0.07] select-none">
      <span className={`w-2.5 h-2.5 rounded-full shrink-0 ${dotCls}`} />
      <div className="flex flex-col leading-none gap-0.5">
        <span className="text-[11px] font-semibold tracking-widest text-md-on-surface-var uppercase">{phaseLabel}</span>
        <span className="text-xs text-md-on-surface-var">{market.local_date} · {market.day_of_week}</span>
      </div>
      {secs > 0 && nextLabel && (
        <div className="flex flex-col items-end leading-none gap-0.5 ml-2">
          <span className="font-mono text-base font-bold text-md-on-surface tracking-wider">{hh}:{mm}:{ss}</span>
          <span className="text-[10px] text-md-on-surface-var">{nextLabel}</span>
        </div>
      )}
    </div>
  )
}

// ── Market Pulse Card ─────────────────────────────────────────────────────────
function PulseCard({ item }) {
  if (!item) return null
  const up = item.change_1d >= 0
  const changeCls = up ? 'text-emerald-400' : 'text-rose-400'
  const bgCls = up ? 'border-emerald-500/20' : 'border-rose-500/20'

  const labels = {
    SPY: 'S&P 500', QQQ: 'Nasdaq 100', IWM: 'Russell 2k',
    VIX: 'Volatility', DIA: 'Dow Jones', IWO: 'R2k Growth',
  }

  return (
    <div className={`flex flex-col gap-1 px-3 py-2.5 rounded-lg bg-md-surface-high border ${bgCls} min-w-[120px]`}>
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs font-semibold text-md-on-surface">{item.ticker}</span>
        <span className="text-[10px] text-md-on-surface-var">{labels[item.ticker] ?? ''}</span>
      </div>
      <div className="flex items-end justify-between gap-2">
        <span className="text-base font-mono font-bold text-md-on-surface">{item.price?.toFixed(2)}</span>
        <span className={`text-xs font-semibold ${changeCls}`}>
          {up ? '+' : ''}{item.change_1d?.toFixed(2)}%
        </span>
      </div>
      <div className={`text-[10px] ${changeCls} opacity-70`}>
        5d: {item.change_5d >= 0 ? '+' : ''}{item.change_5d?.toFixed(2)}%
      </div>
    </div>
  )
}

// ── Ultra Score Band badge ────────────────────────────────────────────────────
function BandBadge({ band }) {
  const cls = {
    'S':  'bg-violet-500/20 text-violet-300 border-violet-500/30',
    'A+': 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
    'A':  'bg-teal-500/20 text-teal-300 border-teal-500/30',
    'B':  'bg-sky-500/20 text-sky-300 border-sky-500/30',
    'C':  'bg-slate-500/20 text-slate-300 border-slate-500/30',
  }[band] ?? 'bg-slate-500/10 text-slate-400 border-slate-500/20'
  return (
    <span className={`px-1.5 py-px text-[10px] font-bold rounded border ${cls} font-mono leading-none`}>
      {band || '—'}
    </span>
  )
}

// ── Action Bucket badge ───────────────────────────────────────────────────────
function ActionBucket({ bucket }) {
  const config = {
    BUY_READY:         { label: 'BUY READY',    cls: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40' },
    WATCH_CLOSELY:     { label: 'WATCH',         cls: 'bg-sky-500/20 text-sky-300 border-sky-500/40' },
    WAIT_CONFIRMATION: { label: 'WAIT CONF.',    cls: 'bg-amber-500/20 text-amber-300 border-amber-500/40' },
    TOO_LATE:          { label: 'TOO LATE',      cls: 'bg-orange-500/20 text-orange-300 border-orange-500/40' },
    AVOID:             { label: 'AVOID',         cls: 'bg-rose-500/20 text-rose-300 border-rose-500/40' },
  }[bucket] ?? { label: bucket, cls: 'bg-slate-500/20 text-slate-300 border-slate-500/40' }
  return (
    <span className={`px-2 py-0.5 text-[10px] font-bold rounded border ${config.cls} font-mono leading-none tracking-wide`}>
      {config.label}
    </span>
  )
}

// ── Top 50 Ultra Card ─────────────────────────────────────────────────────────
function UltraCard({ card, rank }) {
  const score = card.ultra_score ?? 0
  const up = (card.change_pct ?? 0) >= 0
  const barW = Math.min(100, Math.max(0, score))

  return (
    <div className="flex flex-col gap-1.5 px-3 py-2.5 rounded-lg bg-md-surface-high border border-white/[0.06] hover:border-white/[0.12] transition-colors group">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-md-on-surface-var font-mono w-5 shrink-0">{rank}</span>
          <span className="text-sm font-bold text-md-on-surface">{card.ticker}</span>
          <BandBadge band={card.band} />
        </div>
        <span className={`text-xs font-semibold ${up ? 'text-emerald-400' : 'text-rose-400'}`}>
          {up ? '+' : ''}{card.change_pct?.toFixed(2) ?? '—'}%
        </span>
      </div>
      {/* Score bar */}
      <div className="h-1 rounded-full bg-white/[0.07] overflow-hidden">
        <div
          className="h-full rounded-full bg-md-primary transition-all"
          style={{ width: `${barW}%` }}
        />
      </div>
      <div className="flex items-center justify-between text-[10px] text-md-on-surface-var">
        <span>Score <span className="font-mono font-semibold text-md-on-surface">{score.toFixed(0)}</span></span>
        <span className="capitalize">{card.vol_bucket?.toLowerCase() ?? ''} vol</span>
      </div>
    </div>
  )
}

// ── Sector Heatmap row ────────────────────────────────────────────────────────
function SectorRow({ sector, maxHot }) {
  const norm = maxHot > 0 ? sector.hotness / maxHot : 0
  const barW = Math.max(0, Math.min(100, (norm + 1) * 50)) // -1..+1 → 0%..100%
  const hot = sector.trend === 'hot'
  const cold = sector.trend === 'cold'
  const clr = hot ? 'bg-emerald-500' : cold ? 'bg-rose-500' : 'bg-slate-500'

  return (
    <div className="flex items-center gap-2 py-1.5 border-b border-white/[0.05] last:border-0">
      <span className="text-[10px] font-mono text-md-on-surface-var w-10 shrink-0">{sector.etf}</span>
      <div className="flex-1 h-1.5 rounded-full bg-white/[0.07] overflow-hidden">
        <div className={`h-full rounded-full ${clr}`} style={{ width: `${barW}%` }} />
      </div>
      <span className={`text-xs font-semibold w-14 text-right ${hot ? 'text-emerald-400' : cold ? 'text-rose-400' : 'text-md-on-surface-var'}`}>
        {sector.return_1d >= 0 ? '+' : ''}{sector.return_1d?.toFixed(2)}%
      </span>
    </div>
  )
}

// ── Risk Alert row ────────────────────────────────────────────────────────────
function AlertRow({ alert }) {
  const config = {
    high:   { cls: 'border-l-rose-500 bg-rose-500/5',   icon: '⚠', textCls: 'text-rose-300' },
    medium: { cls: 'border-l-amber-500 bg-amber-500/5', icon: '●', textCls: 'text-amber-300' },
    low:    { cls: 'border-l-slate-500 bg-slate-500/5', icon: '○', textCls: 'text-slate-300' },
  }[alert.severity] ?? { cls: 'border-l-slate-500 bg-slate-500/5', icon: '○', textCls: 'text-slate-300' }

  return (
    <div className={`flex items-start gap-2.5 px-3 py-2 rounded-r border-l-2 ${config.cls}`}>
      <span className={`text-xs mt-0.5 shrink-0 ${config.textCls}`}>{config.icon}</span>
      <span className="text-xs text-md-on-surface">{alert.message}</span>
    </div>
  )
}

// ── Scanner Status bar ────────────────────────────────────────────────────────
function ScannerStatus({ scanner, ultra }) {
  const scanPct = scanner?.total > 0 ? Math.round(scanner.done / scanner.total * 100) : 0
  const ultraPct = ultra?.total > 0 ? Math.round(ultra.done / ultra.total * 100) : 0

  function fmtTime(iso) {
    if (!iso) return 'never'
    try {
      const d = new Date(iso)
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    } catch { return iso }
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-col gap-1">
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-md-on-surface-var">T/Z Scanner</span>
          <span className="text-md-on-surface-var">
            {scanner?.running ? `${scanner.done}/${scanner.total}` : `Last: ${fmtTime(scanner?.last_scan)}`}
          </span>
        </div>
        {scanner?.running && (
          <div className="h-1 rounded-full bg-white/[0.07] overflow-hidden">
            <div className="h-full rounded-full bg-md-primary animate-pulse" style={{ width: `${scanPct}%` }} />
          </div>
        )}
      </div>
      <div className="flex flex-col gap-1">
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-md-on-surface-var">ULTRA Scanner</span>
          <span className="text-md-on-surface-var">
            {ultra?.running ? `${ultra.done}/${ultra.total}` : `Last: ${fmtTime(ultra?.last_scan)}`}
          </span>
        </div>
        {ultra?.running && (
          <div className="h-1 rounded-full bg-white/[0.07] overflow-hidden">
            <div className="h-full rounded-full bg-violet-500 animate-pulse" style={{ width: `${ultraPct}%` }} />
          </div>
        )}
      </div>
    </div>
  )
}

// ── Best Setups Card ──────────────────────────────────────────────────────────
function SetupCard({ setup }) {
  return (
    <div className="flex flex-col gap-1.5 px-3 py-2.5 rounded-lg bg-md-surface-high border border-white/[0.06]">
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <span className="text-sm font-bold text-md-on-surface">{setup.ticker}</span>
        <div className="flex items-center gap-1.5">
          <ActionBucket bucket={setup.action_bucket} />
          <span className="text-[10px] text-md-on-surface-var font-mono">
            {setup.confidence}/10
          </span>
        </div>
      </div>
      <p className="text-xs text-md-on-surface leading-relaxed">{setup.reason}</p>
      <p className="text-[11px] text-md-on-surface-var">Risk: {setup.risk}</p>
      {setup.source === 'claude' && (
        <span className="self-start text-[9px] font-mono text-violet-400 opacity-70">AI</span>
      )}
    </div>
  )
}

// ── Fresh Signal Row ──────────────────────────────────────────────────────────
function FreshSignalRow({ sig }) {
  const up = (sig.change_pct ?? 0) >= 0
  function fmtTime(iso) {
    if (!iso) return ''
    try { return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) }
    catch { return '' }
  }
  return (
    <div className="flex items-center gap-3 py-1.5 border-b border-white/[0.05] last:border-0">
      <span className="w-16 text-xs font-bold text-md-on-surface shrink-0">{sig.ticker}</span>
      <div className="flex items-center gap-1 flex-1 flex-wrap min-w-0">
        {[...Array(Math.min(sig.bull_score ?? 0, 10))].map((_, i) => (
          <span key={i} className="w-1.5 h-3 rounded-sm bg-emerald-500/70" />
        ))}
      </div>
      <span className={`text-xs font-semibold shrink-0 ${up ? 'text-emerald-400' : 'text-rose-400'}`}>
        {up ? '+' : ''}{sig.change_pct?.toFixed(2) ?? '—'}%
      </span>
      <span className="text-[10px] text-md-on-surface-var shrink-0">{fmtTime(sig.scanned_at)}</span>
    </div>
  )
}

// ── Summary Card ──────────────────────────────────────────────────────────────
function SummaryCard({ label, value, sub, accentCls = 'text-md-primary' }) {
  return (
    <div className="flex flex-col gap-0.5 px-4 py-3 rounded-lg bg-md-surface-high border border-white/[0.06] min-w-[100px]">
      <span className="text-[10px] text-md-on-surface-var uppercase tracking-wider">{label}</span>
      <span className={`text-2xl font-bold font-mono ${accentCls}`}>{value ?? '—'}</span>
      {sub && <span className="text-[10px] text-md-on-surface-var">{sub}</span>}
    </div>
  )
}

// ── Section shell ─────────────────────────────────────────────────────────────
function Section({ title, children, className = '' }) {
  return (
    <section className={`flex flex-col gap-3 ${className}`}>
      <h2 className="text-xs font-semibold tracking-widest uppercase text-md-on-surface-var px-0.5">
        {title}
      </h2>
      {children}
    </section>
  )
}

// ── Loading / Error states ────────────────────────────────────────────────────
function Spinner() {
  return <div className="w-4 h-4 rounded-full border-2 border-md-primary border-t-transparent animate-spin" />
}

// ═════════════════════════════════════════════════════════════════════════════
// Main panel
// ═════════════════════════════════════════════════════════════════════════════
export default function TradingDashboardPanel() {
  const [status,  setStatus]  = useState(null)
  const [pulse,   setPulse]   = useState(null)
  const [top50,   setTop50]   = useState(null)
  const [sectors, setSectors] = useState(null)
  const [fresh,   setFresh]   = useState(null)
  const [setups,  setSetups]  = useState(null)
  const [risk,    setRisk]    = useState(null)
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)
  const [lastRefresh, setLastRefresh] = useState(null)
  const [top50Limit, setTop50Limit]   = useState(20)

  const abortRef = useRef(null)

  const loadAll = useCallback(async (silent = false) => {
    if (abortRef.current) abortRef.current.abort()
    abortRef.current = new AbortController()

    if (!silent) setLoading(true)
    setError(null)

    try {
      // Load critical data first in parallel
      const [statusData, pulseData] = await Promise.all([
        apiFetch('/status'),
        apiFetch('/pulse'),
      ])
      setStatus(statusData)
      setPulse(pulseData)

      // Load secondary data
      const [top50Data, sectorsData, freshData, riskData, summaryData] = await Promise.allSettled([
        apiFetch('/top50?limit=50'),
        apiFetch('/sector-heat'),
        apiFetch('/fresh-signals?limit=25'),
        apiFetch('/risk-alerts'),
        apiFetch('/summary'),
      ])
      if (top50Data.status    === 'fulfilled') setTop50(top50Data.value)
      if (sectorsData.status  === 'fulfilled') setSectors(sectorsData.value)
      if (freshData.status    === 'fulfilled') setFresh(freshData.value)
      if (riskData.status     === 'fulfilled') setRisk(riskData.value)
      if (summaryData.status  === 'fulfilled') setSummary(summaryData.value)

      // AI setups last (slowest)
      try {
        const setupsData = await apiFetch('/best-setups')
        setSetups(setupsData)
      } catch (_) { /* non-critical */ }

      setLastRefresh(new Date())
    } catch (err) {
      if (err.name !== 'AbortError') setError(err.message)
    } finally {
      if (!silent) setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadAll()
    const t = setInterval(() => loadAll(true), REFRESH_INTERVAL)
    return () => { clearInterval(t); abortRef.current?.abort() }
  }, [loadAll])

  const pulseItems = pulse?.pulse ?? []
  const mainPulse  = pulseItems.filter(p => ['SPY', 'QQQ', 'IWM', 'VIX'].includes(p.ticker))
  const cards      = top50?.cards ?? []
  const sectorList = sectors?.sectors ?? []
  const maxHot     = Math.max(...sectorList.map(s => Math.abs(s.hotness)), 1)
  const signals    = fresh?.signals ?? []
  const alerts     = risk?.alerts ?? []
  const setupList  = setups?.setups ?? []

  return (
    <div className="min-h-screen bg-md-surface px-4 py-4 flex flex-col gap-6">

      {/* ── Top bar: market timer + refresh ──────────────────────────────── */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-3 flex-wrap">
          <h1 className="text-base font-bold text-md-on-surface tracking-tight">
            Command Center
          </h1>
          {status?.market && <MarketStatusTimer market={status.market} />}
        </div>
        <div className="flex items-center gap-3">
          {lastRefresh && (
            <span className="text-[10px] text-md-on-surface-var">
              Updated {lastRefresh.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
            </span>
          )}
          <button
            onClick={() => loadAll(false)}
            disabled={loading}
            className="text-xs px-3 py-1.5 rounded bg-md-surface-high border border-white/[0.08] text-md-on-surface-var hover:text-md-on-surface transition-colors disabled:opacity-40"
          >
            {loading ? 'Refreshing…' : '↻ Refresh'}
          </button>
        </div>
      </div>

      {error && (
        <div className="text-xs text-rose-400 px-3 py-2 rounded bg-rose-500/10 border border-rose-500/20">
          Error loading dashboard: {error}
        </div>
      )}

      {/* ── Summary cards ────────────────────────────────────────────────── */}
      {summary && (
        <div className="flex flex-wrap gap-3">
          <SummaryCard label="Bull Signals" value={summary.bull_count} sub="bull_score ≥ 4" accentCls="text-emerald-400" />
          <SummaryCard label="Strong Setups" value={summary.strong_count} sub="bull_score ≥ 6" accentCls="text-teal-400" />
          <SummaryCard label="Ultra Scanned" value={summary.ultra_total} sub="1D timeframe" />
          <SummaryCard label="Ultra Top Band" value={summary.ultra_top_band} sub="Band A/A+/S" accentCls="text-violet-400" />
          {summary.sector_leader && (
            <SummaryCard label="Hot Sector" value={summary.sector_leader} sub="today's leader" accentCls="text-amber-400" />
          )}
        </div>
      )}

      {/* ── Market Pulse ─────────────────────────────────────────────────── */}
      <Section title="Market Pulse">
        {loading && !pulseItems.length
          ? <Spinner />
          : (
            <div className="flex flex-wrap gap-3">
              {mainPulse.map(item => <PulseCard key={item.ticker} item={item} />)}
            </div>
          )
        }
      </Section>

      {/* ── Main content grid ────────────────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 xl:grid-cols-4 gap-5">

        {/* ── Top Ultra Candidates (col-span 2) ──────────────────────── */}
        <div className="lg:col-span-2">
          <Section title={`Top Ultra Candidates (${cards.length})`}>
            <div className="flex items-center gap-2 mb-1">
              {[10, 20, 50].map(n => (
                <button
                  key={n}
                  onClick={() => setTop50Limit(n)}
                  className={`text-xs px-2 py-0.5 rounded border transition-colors ${
                    top50Limit === n
                      ? 'bg-md-primary-container text-md-on-primary-container border-transparent'
                      : 'border-white/[0.08] text-md-on-surface-var hover:text-md-on-surface'
                  }`}
                >
                  Top {n}
                </button>
              ))}
            </div>
            {loading && !cards.length
              ? <Spinner />
              : (
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                  {cards.slice(0, top50Limit).map((card, i) => (
                    <UltraCard key={card.ticker} card={card} rank={i + 1} />
                  ))}
                </div>
              )
            }
          </Section>
        </div>

        {/* ── Right rail ─────────────────────────────────────────────── */}
        <div className="flex flex-col gap-5 xl:col-span-2">

          {/* AI Best Setups */}
          <Section title="Best Setups Today">
            {loading && !setupList.length
              ? <Spinner />
              : setupList.length === 0
              ? <span className="text-xs text-md-on-surface-var">Run an ULTRA scan to see setups.</span>
              : (
                <div className="flex flex-col gap-2">
                  {setupList.map(s => <SetupCard key={s.ticker} setup={s} />)}
                </div>
              )
            }
          </Section>

          {/* Sector Heatmap */}
          <Section title="Sector Heatmap">
            {loading && !sectorList.length
              ? <Spinner />
              : (
                <div className="px-3 py-2 rounded-lg bg-md-surface-high border border-white/[0.06]">
                  {sectorList.map(s => (
                    <SectorRow key={s.etf} sector={s} maxHot={maxHot} />
                  ))}
                </div>
              )
            }
          </Section>

          {/* Risk Alerts */}
          <Section title="Risk Alerts">
            {loading && !alerts.length
              ? <Spinner />
              : (
                <div className="flex flex-col gap-1.5">
                  {alerts.map((a, i) => <AlertRow key={i} alert={a} />)}
                </div>
              )
            }
          </Section>

          {/* Scanner Status */}
          <Section title="Scanner Status">
            <div className="px-3 py-3 rounded-lg bg-md-surface-high border border-white/[0.06]">
              <ScannerStatus scanner={status?.scanner} ultra={status?.ultra} />
            </div>
          </Section>

        </div>
      </div>

      {/* ── Fresh Signals Feed (full width) ──────────────────────────────── */}
      <Section title="Fresh Signals Feed">
        {loading && !signals.length
          ? <Spinner />
          : signals.length === 0
          ? <span className="text-xs text-md-on-surface-var">No fresh signals — run T/Z scanner first.</span>
          : (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-x-6 px-3 py-2 rounded-lg bg-md-surface-high border border-white/[0.06]">
              {signals.map(sig => (
                <FreshSignalRow key={sig.ticker} sig={sig} />
              ))}
            </div>
          )
        }
      </Section>

    </div>
  )
}
