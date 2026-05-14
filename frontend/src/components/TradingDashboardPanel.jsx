/**
 * TradingDashboardPanel.jsx — Redesigned Market Command Center
 * Decision-first layout: status → AI brief → best setups → candidates → news → sectors → signals → alerts
 */
import { useState, useEffect, useCallback, useRef } from 'react'

const API = '/api/dashboard'
const WATCHLIST_API = '/api/watchlist'

async function apiFetch(path) {
  const r = await fetch(`${API}${path}`)
  if (!r.ok) throw new Error(`${r.status}`)
  return r.json()
}

async function watchlistFetch(tickers) {
  if (!tickers?.length) return { items: [] }
  const r = await fetch(`${WATCHLIST_API}?tickers=${tickers.join(',')}&tf=1d`)
  if (!r.ok) throw new Error(`${r.status}`)
  return r.json()
}

// ─── Utility ─────────────────────────────────────────────────────────────────
const cx = (...args) => args.filter(Boolean).join(' ')

const BULL = 'text-emerald-400'
const BEAR = 'text-rose-400'
const WARN = 'text-amber-400'
const INFO = 'text-sky-400'
const MUTE = 'text-md-on-surface-var'

function relTime(ts) {
  if (!ts) return ''
  const diff = (Date.now() - new Date(ts).getTime()) / 1000
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`
  if (diff < 86400) return `${Math.round(diff / 3600)}h ago`
  return `${Math.round(diff / 86400)}d ago`
}

// ─── Atoms ────────────────────────────────────────────────────────────────────

function Spinner() {
  return <div className="w-4 h-4 rounded-full border-2 border-md-primary border-t-transparent animate-spin shrink-0" />
}

function SkeletonRow({ h = 'h-10', w = 'w-full' }) {
  return <div className={`${h} ${w} rounded-lg bg-white/[0.05] animate-pulse`} />
}

function EmptyState({ icon = '○', message, sub }) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 py-10 text-center px-4">
      <span className="text-3xl opacity-20">{icon}</span>
      <p className="text-sm text-md-on-surface-var">{message}</p>
      {sub && <p className="text-xs text-md-on-surface-var opacity-50">{sub}</p>}
    </div>
  )
}

function Card({ children, className = '' }) {
  return (
    <div className={cx('rounded-xl bg-md-surface-high border border-white/[0.06] p-4', className)}>
      {children}
    </div>
  )
}

function SectionTitle({ children, badge, action }) {
  return (
    <div className="flex items-center justify-between gap-2 mb-4">
      <div className="flex items-center gap-2">
        <h2 className="text-[11px] font-semibold tracking-widest uppercase text-md-on-surface-var">
          {children}
        </h2>
        {badge != null && (
          <span className="text-[10px] font-mono px-1.5 py-px rounded bg-white/[0.05] text-md-on-surface-var border border-white/[0.08]">
            {badge}
          </span>
        )}
      </div>
      {action}
    </div>
  )
}

function Badge({ label, variant = 'default' }) {
  const map = {
    default: 'bg-white/[0.07] text-md-on-surface-var border-white/[0.09]',
    bull:    'bg-emerald-500/15 text-emerald-300 border-emerald-500/25',
    bear:    'bg-rose-500/15    text-rose-300    border-rose-500/25',
    warn:    'bg-amber-500/15   text-amber-300   border-amber-500/25',
    info:    'bg-sky-500/15     text-sky-300     border-sky-500/25',
    ai:      'bg-violet-500/15  text-violet-300  border-violet-500/25',
    buy:     'bg-emerald-500/20 text-emerald-200 border-emerald-500/35',
    watch:   'bg-sky-500/20     text-sky-200     border-sky-500/35',
    wait:    'bg-amber-500/20   text-amber-200   border-amber-500/35',
    late:    'bg-orange-500/20  text-orange-200  border-orange-500/35',
    avoid:   'bg-rose-500/20    text-rose-200    border-rose-500/35',
  }
  return (
    <span className={cx(
      'inline-flex items-center px-2 py-px rounded text-[10px] font-bold border font-mono tracking-wide leading-none shrink-0',
      map[variant] ?? map.default
    )}>
      {label}
    </span>
  )
}

const BUCKET_CFG = {
  BUY_READY:         { label: 'BUY READY',  variant: 'buy' },
  WATCH_CLOSELY:     { label: 'WATCH',       variant: 'watch' },
  WAIT_CONFIRMATION: { label: 'WAIT CONF.',  variant: 'wait' },
  TOO_LATE:          { label: 'TOO LATE',    variant: 'late' },
  AVOID:             { label: 'AVOID',       variant: 'avoid' },
}
function ActionBadge({ bucket }) {
  const cfg = BUCKET_CFG[bucket] ?? { label: bucket ?? '—', variant: 'default' }
  return <Badge label={cfg.label} variant={cfg.variant} />
}

function BandBadge({ band }) {
  const m = {
    'S':  'bg-violet-500/20 text-violet-200 border-violet-500/30',
    'A+': 'bg-emerald-500/20 text-emerald-200 border-emerald-500/30',
    'A':  'bg-teal-500/20 text-teal-200 border-teal-500/30',
    'B':  'bg-sky-500/20 text-sky-200 border-sky-500/30',
    'C':  'bg-slate-500/15 text-slate-300 border-slate-500/25',
  }[band] ?? 'bg-slate-500/10 text-slate-400 border-slate-500/20'
  return (
    <span className={cx('inline-flex px-1.5 py-px text-[10px] font-bold rounded border font-mono leading-none', m)}>
      {band || '—'}
    </span>
  )
}

function ScoreBar({ value, max = 100, color = 'bg-md-primary' }) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100))
  return (
    <div className="h-1 rounded-full bg-white/[0.07] overflow-hidden">
      <div className={cx('h-full rounded-full transition-all', color)} style={{ width: `${pct}%` }} />
    </div>
  )
}

// ─── SECTION 1 — MARKET COMMAND BAR ──────────────────────────────────────────

function MarketTimer({ market }) {
  const [tick, setTick] = useState(0)
  useEffect(() => {
    const t = setInterval(() => setTick(n => n + 1), 1000)
    return () => clearInterval(t)
  }, [])

  if (!market) {
    return <SkeletonRow h="h-[60px]" w="w-[220px]" />
  }

  const secs = Math.max(0, (market.secs_to_next || 0) - tick)
  const hh = String(Math.floor(secs / 3600)).padStart(2, '0')
  const mm = String(Math.floor((secs % 3600) / 60)).padStart(2, '0')
  const ss = String(secs % 60).padStart(2, '0')

  const phaseLabel = {
    regular:     'MARKET OPEN',
    pre_market:  'PRE-MARKET',
    after_hours: 'AFTER HOURS',
    weekend:     'WEEKEND',
    overnight:   'CLOSED',
  }[market.phase] ?? market.phase?.toUpperCase()

  const nextLabel = {
    regular:     'closes in',
    pre_market:  'opens in',
    after_hours: 'closes in',
    overnight:   'pre-market in',
  }[market.phase] ?? ''

  const dotCls =
    market.status === 'open'
      ? 'bg-emerald-400 shadow-[0_0_8px_2px_rgba(52,211,153,0.45)]'
      : market.status === 'pre_market' || market.status === 'after_hours'
      ? 'bg-amber-400'
      : 'bg-slate-500'

  return (
    <div className="flex items-center gap-3 px-4 py-3 rounded-xl bg-md-surface-high border border-white/[0.07] shrink-0">
      <span className={cx('w-2.5 h-2.5 rounded-full shrink-0', dotCls)} />
      <div className="flex flex-col gap-0.5 leading-none">
        <span className="text-[11px] font-bold tracking-widest text-md-on-surface uppercase">{phaseLabel}</span>
        <span className="text-[10px] text-md-on-surface-var">{market.local_date} · {market.day_of_week}</span>
        <span className="text-[10px] text-md-on-surface-var font-mono">ET {market.local_time}</span>
      </div>
      {secs > 0 && nextLabel && (
        <div className="ml-3 pl-3 border-l border-white/[0.08] flex flex-col gap-0.5 leading-none">
          <span className="font-mono text-sm font-bold text-md-on-surface tracking-wider">{hh}:{mm}:{ss}</span>
          <span className="text-[10px] text-md-on-surface-var">{nextLabel}</span>
        </div>
      )}
    </div>
  )
}

function PulseChip({ item }) {
  if (!item) return null
  const up = item.change_1d >= 0
  const labels = { SPY: 'S&P 500', QQQ: 'Nasdaq', IWM: 'R2K', VIX: 'Volatility' }
  return (
    <div className={cx(
      'flex flex-col gap-1 px-3 py-2.5 rounded-xl bg-md-surface-high border shrink-0 min-w-[100px]',
      up ? 'border-emerald-500/20' : 'border-rose-500/20'
    )}>
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs font-bold text-md-on-surface">{item.ticker}</span>
        <span className="text-[10px] text-md-on-surface-var">{labels[item.ticker] ?? ''}</span>
      </div>
      <div className="flex items-end justify-between gap-2">
        <span className="text-sm font-mono font-bold text-md-on-surface">{item.price?.toFixed(2)}</span>
        <span className={cx('text-xs font-bold', up ? BULL : BEAR)}>
          {up ? '+' : ''}{item.change_1d?.toFixed(2)}%
        </span>
      </div>
      <span className={cx('text-[10px] opacity-60', up ? BULL : BEAR)}>
        5d: {item.change_5d >= 0 ? '+' : ''}{item.change_5d?.toFixed(2)}%
      </span>
    </div>
  )
}

function FreshnessChip({ scanner, ultra }) {
  function fresh(last) {
    if (!last) return { label: 'NEVER', cls: 'text-slate-500' }
    const age = (Date.now() - new Date(last).getTime()) / 60000
    if (age < 30) return { label: 'FRESH', cls: BULL }
    if (age < 120) return { label: 'OK', cls: WARN }
    return { label: 'STALE', cls: BEAR }
  }
  const sf = fresh(scanner?.last_scan)
  const uf = fresh(ultra?.last_scan)

  return (
    <div className="flex items-center gap-4 px-3 py-2.5 rounded-xl bg-md-surface-high border border-white/[0.06] shrink-0">
      <div className="flex flex-col gap-0.5">
        <span className="text-[10px] text-md-on-surface-var">T/Z Scan</span>
        <span className={cx('text-[11px] font-bold font-mono', sf.cls)}>{sf.label}</span>
      </div>
      <div className="w-px h-6 bg-white/[0.08]" />
      <div className="flex flex-col gap-0.5">
        <span className="text-[10px] text-md-on-surface-var">ULTRA</span>
        <span className={cx('text-[11px] font-bold font-mono', uf.cls)}>{uf.label}</span>
      </div>
    </div>
  )
}

function MarketCommandBar({ status, pulse, summary, onRefresh, loading, lastRefresh }) {
  const pulseItems = (pulse?.pulse ?? []).filter(p => ['SPY', 'QQQ', 'IWM', 'VIX'].includes(p.ticker))

  return (
    <div className="flex items-center gap-3 flex-wrap">
      <MarketTimer market={status?.market} />

      {/* Pulse chips */}
      <div className="flex gap-2 flex-wrap">
        {pulseItems.length > 0
          ? pulseItems.map(item => <PulseChip key={item.ticker} item={item} />)
          : ['SPY', 'QQQ', 'IWM', 'VIX'].map(t => (
              <div key={t} className="w-[104px] h-[70px] rounded-xl bg-md-surface-high border border-white/[0.04] animate-pulse" />
            ))
        }
      </div>

      {status && <FreshnessChip scanner={status.scanner} ultra={status.ultra} />}

      {/* Summary chips */}
      {summary && (
        <div className="flex gap-2">
          {summary.bull_count != null && (
            <div className="flex flex-col gap-0.5 px-3 py-2 rounded-xl bg-md-surface-high border border-white/[0.06]">
              <span className="text-[10px] text-md-on-surface-var">Bull Signals</span>
              <span className="text-sm font-bold font-mono text-emerald-400">{summary.bull_count}</span>
            </div>
          )}
          {summary.ultra_top_band != null && (
            <div className="flex flex-col gap-0.5 px-3 py-2 rounded-xl bg-md-surface-high border border-white/[0.06]">
              <span className="text-[10px] text-md-on-surface-var">Top Band</span>
              <span className="text-sm font-bold font-mono text-violet-400">{summary.ultra_top_band}</span>
            </div>
          )}
        </div>
      )}

      <div className="flex items-center gap-2 ml-auto">
        {lastRefresh && (
          <span className="text-[10px] text-md-on-surface-var opacity-40">
            {lastRefresh.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
          </span>
        )}
        <button
          onClick={onRefresh}
          disabled={loading}
          className="text-xs px-3 py-1.5 rounded-lg bg-md-surface-high border border-white/[0.08] text-md-on-surface-var hover:text-md-on-surface hover:border-white/[0.15] transition-all disabled:opacity-40"
        >
          {loading ? '…' : '↻ Refresh'}
        </button>
      </div>
    </div>
  )
}

// ─── SECTION 2 — AI MARKET BRIEF ─────────────────────────────────────────────

function AIMarketBrief({ brief, loading }) {
  const toneColor =
    brief?.market_tone?.toLowerCase().includes('bull') ? BULL :
    brief?.market_tone?.toLowerCase().includes('bear') ? BEAR : WARN

  return (
    <Card>
      <SectionTitle
        badge={brief?.source === 'claude' ? 'AI' : brief ? 'AUTO' : null}
        action={brief?.confidence && (
          <span className="text-[10px] text-md-on-surface-var opacity-50">
            Confidence: {brief.confidence}
          </span>
        )}
      >
        AI Market Brief
      </SectionTitle>

      {loading && !brief ? (
        <div className="flex flex-col gap-2.5">
          {[85, 70, 55].map(w => (
            <div key={w} className="h-3.5 rounded bg-white/[0.05] animate-pulse" style={{ width: `${w}%` }} />
          ))}
        </div>
      ) : !brief ? (
        <EmptyState icon="🧠" message="AI brief not available." sub="Run Ultra Scan to enable AI analysis." />
      ) : (
        <div className="flex flex-col gap-4">
          {/* Tone pill */}
          {brief.market_tone && (
            <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-white/[0.03] border border-white/[0.05]">
              <span className="text-[10px] text-md-on-surface-var uppercase tracking-wider shrink-0">Tone</span>
              <span className={cx('text-sm font-bold', toneColor)}>{brief.market_tone}</span>
            </div>
          )}

          {brief.focus_summary && (
            <p className="text-sm text-md-on-surface leading-relaxed">{brief.focus_summary}</p>
          )}

          {brief.what_to_focus_on?.length > 0 && (
            <div>
              <p className="text-[10px] text-emerald-400 font-bold uppercase tracking-wider mb-2">Focus On</p>
              <ul className="flex flex-col gap-1.5">
                {brief.what_to_focus_on.map((pt, i) => (
                  <li key={i} className="flex items-start gap-2 text-xs text-md-on-surface">
                    <span className="text-emerald-400 shrink-0 mt-0.5">›</span>
                    <span className="leading-relaxed">{pt}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {brief.what_to_avoid?.length > 0 && (
            <div>
              <p className="text-[10px] text-rose-400 font-bold uppercase tracking-wider mb-2">Avoid</p>
              <ul className="flex flex-col gap-1.5">
                {brief.what_to_avoid.map((pt, i) => (
                  <li key={i} className="flex items-start gap-2 text-xs text-md-on-surface">
                    <span className="text-rose-400 shrink-0 mt-0.5">›</span>
                    <span className="leading-relaxed">{pt}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {brief.hot_sectors?.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {brief.hot_sectors.map(s => <Badge key={s} label={s} variant="bull" />)}
            </div>
          )}

          <p className="text-[10px] text-md-on-surface-var opacity-30 pt-1 border-t border-white/[0.05]">
            Generated from scanner + sector + market data
          </p>
        </div>
      )}
    </Card>
  )
}

// ─── SECTION 3 — BEST SETUPS TODAY ───────────────────────────────────────────

const CATEGORY_LABELS = {
  BEST_PULLBACK:               'Best Pullback',
  BEST_BREAKOUT:               'Best Breakout',
  BEST_EMA_RECLAIM:            'EMA Reclaim',
  BEST_ABR_B_PLUS:             'ABR B+ Setup',
  BEST_LOW_VOLUME_ACCUMULATION:'Low Vol. Accumulation',
  BEST_SECTOR_LEADER:          'Sector Leader',
  BEST_FRESH_SIGNAL:           'Fresh Signal',
  BEST_RISK_REWARD:            'Best R/R',
  AVOID_TOO_LATE:              'Too Late / Avoid',
}

function SetupCard({ setup, onOpenChart, onAddToWatchlist }) {
  const [expanded, setExpanded] = useState(false)
  const ticker = setup.ticker || setup.symbol
  const isAvoid = setup.action_bucket === 'AVOID' || setup.category === 'AVOID_TOO_LATE'

  const confColor =
    setup.confidence >= 8 ? BULL :
    setup.confidence >= 5 ? WARN : MUTE

  return (
    <div className={cx(
      'flex flex-col rounded-xl bg-md-surface-high border border-white/[0.06] p-4 transition-colors',
      'hover:border-white/[0.11]',
      isAvoid && 'opacity-60'
    )}>
      {/* Header */}
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className="flex flex-col gap-1">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-base font-bold text-md-on-surface">{ticker}</span>
            {setup.band && <BandBadge band={setup.band} />}
            {setup.source === 'claude' && <Badge label="AI" variant="ai" />}
          </div>
          <span className="text-xs text-md-on-surface-var">
            {CATEGORY_LABELS[setup.category] ?? setup.category ?? 'Setup'}
          </span>
        </div>
        <div className="flex flex-col items-end gap-1.5 shrink-0">
          <ActionBadge bucket={setup.action_bucket} />
          <span className={cx('text-[10px] font-mono', confColor)}>
            {setup.confidence}/10 confidence
          </span>
        </div>
      </div>

      {/* Score bars */}
      {(setup.ultra_score != null || setup.setup_quality != null) && (
        <div className="grid grid-cols-2 gap-3 mb-3">
          {setup.ultra_score != null && (
            <div>
              <div className="flex justify-between text-[10px] mb-1">
                <span className="text-md-on-surface-var">Ultra Score</span>
                <span className="font-mono font-bold text-md-on-surface">{Math.round(setup.ultra_score)}</span>
              </div>
              <ScoreBar value={setup.ultra_score} color="bg-violet-500" />
            </div>
          )}
          {setup.setup_quality != null && (
            <div>
              <div className="flex justify-between text-[10px] mb-1">
                <span className="text-md-on-surface-var">Setup Quality</span>
                <span className="font-mono font-bold text-md-on-surface">{Math.round(setup.setup_quality)}</span>
              </div>
              <ScoreBar value={setup.setup_quality} color="bg-md-primary" />
            </div>
          )}
        </div>
      )}

      {/* Signal chips */}
      <div className="flex flex-wrap gap-1.5 mb-3">
        {setup.signal     && <Badge label={setup.signal} variant="info" />}
        {setup.abr        && <Badge label={`ABR ${setup.abr}`} variant={setup.abr === 'B+' || setup.abr === 'A' ? 'bull' : 'default'} />}
        {setup.wlnbb      && <Badge label={`WLNBB ${setup.wlnbb}`} variant="default" />}
        {setup.ema_state  && <Badge label={setup.ema_state} variant={setup.ema_state?.includes('Reclaim') ? 'bull' : 'default'} />}
        {setup.vol_bucket && <Badge label={`Vol: ${setup.vol_bucket}`} variant="default" />}
      </div>

      {/* Why selected */}
      {(setup.why_selected?.length > 0 || setup.reason) && (
        <div className="mb-3">
          <p className="text-[10px] text-md-on-surface-var font-bold uppercase tracking-wider mb-2">Why Selected</p>
          {setup.why_selected?.length > 0 ? (
            <ul className="flex flex-col gap-1.5">
              {setup.why_selected.map((w, i) => (
                <li key={i} className="flex items-start gap-2 text-xs text-md-on-surface">
                  <span className="text-emerald-400 shrink-0 mt-0.5">›</span>
                  <span className="leading-relaxed">{w}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-md-on-surface leading-relaxed">{setup.reason}</p>
          )}
        </div>
      )}

      {/* Historical edge */}
      {setup.historical_evidence && Object.keys(setup.historical_evidence).length > 0 && (
        <div className="mb-3 px-3 py-2.5 rounded-lg bg-white/[0.03] border border-white/[0.05]">
          <p className="text-[10px] text-amber-400 font-bold uppercase tracking-wider mb-2">Historical Edge</p>
          <div className="grid grid-cols-2 gap-x-6 gap-y-1.5 text-[11px]">
            {setup.historical_evidence.median_10d != null && (
              <>
                <span className="text-md-on-surface-var">Median 10D</span>
                <span className={cx('font-mono font-bold', setup.historical_evidence.median_10d >= 0 ? BULL : BEAR)}>
                  {setup.historical_evidence.median_10d >= 0 ? '+' : ''}{setup.historical_evidence.median_10d?.toFixed(1)}%
                </span>
              </>
            )}
            {setup.historical_evidence.hit_10pct != null && (
              <>
                <span className="text-md-on-surface-var">Hit +10%</span>
                <span className="font-mono font-bold text-emerald-400">{setup.historical_evidence.hit_10pct?.toFixed(1)}%</span>
              </>
            )}
            {setup.historical_evidence.fail_10pct != null && (
              <>
                <span className="text-md-on-surface-var">Fail -10%</span>
                <span className="font-mono font-bold text-rose-400">{setup.historical_evidence.fail_10pct?.toFixed(1)}%</span>
              </>
            )}
            {setup.historical_evidence.sample != null && (
              <>
                <span className="text-md-on-surface-var">Sample</span>
                <span className="font-mono">{setup.historical_evidence.sample}</span>
              </>
            )}
          </div>
        </div>
      )}

      {/* Expanded: risk + watch next */}
      {expanded && (
        <div className="border-t border-white/[0.06] pt-3 mt-1 flex flex-col gap-3">
          {(setup.risk_flags?.length > 0 || setup.risk) && (
            <div>
              <p className="text-[10px] text-rose-400 font-bold uppercase tracking-wider mb-2">Risk</p>
              {setup.risk_flags?.length > 0 ? (
                <ul className="flex flex-col gap-1.5">
                  {setup.risk_flags.map((r, i) => (
                    <li key={i} className="flex items-start gap-2 text-xs text-md-on-surface">
                      <span className="text-rose-400 shrink-0 mt-0.5">›</span>
                      <span className="leading-relaxed">{r}</span>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-xs text-md-on-surface">{setup.risk}</p>
              )}
            </div>
          )}
          {setup.what_to_watch_next?.length > 0 && (
            <div>
              <p className="text-[10px] text-sky-400 font-bold uppercase tracking-wider mb-2">Watch Next</p>
              <ul className="flex flex-col gap-1.5">
                {setup.what_to_watch_next.map((w, i) => (
                  <li key={i} className="flex items-start gap-2 text-xs text-md-on-surface">
                    <span className="text-sky-400 shrink-0 mt-0.5">›</span>
                    <span className="leading-relaxed">{w}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}

      {/* Actions */}
      <div className="flex items-center gap-2 mt-3 pt-3 border-t border-white/[0.05]">
        <button
          onClick={() => setExpanded(e => !e)}
          className="text-[11px] px-2.5 py-1 rounded-md bg-white/[0.05] text-md-on-surface-var hover:text-md-on-surface border border-white/[0.07] transition-colors"
        >
          {expanded ? 'Less ↑' : 'Details ↓'}
        </button>
        {onOpenChart && (
          <button
            onClick={() => onOpenChart(ticker)}
            className="text-[11px] px-2.5 py-1 rounded-md bg-white/[0.05] text-md-on-surface-var hover:text-md-on-surface border border-white/[0.07] transition-colors"
          >
            Chart
          </button>
        )}
        {onAddToWatchlist && (
          <button
            onClick={() => onAddToWatchlist(ticker)}
            className="text-[11px] px-2.5 py-1 rounded-md bg-white/[0.05] text-md-on-surface-var hover:text-md-on-surface border border-white/[0.07] transition-colors"
          >
            + Watchlist
          </button>
        )}
      </div>
    </div>
  )
}

function BestSetupsToday({ setups, loading, onOpenChart, onAddToWatchlist }) {
  const list = setups?.setups ?? []
  const aiCurated = list.some(s => s.source === 'claude')

  return (
    <div>
      <SectionTitle
        badge={list.length || null}
        action={aiCurated && <Badge label="AI Curated" variant="ai" />}
      >
        Best Setups Today
      </SectionTitle>
      {loading && !list.length ? (
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-3">
          {[1, 2, 3].map(i => <SkeletonRow key={i} h="h-52" />)}
        </div>
      ) : list.length === 0 ? (
        <Card>
          <EmptyState icon="📋" message="No valid setups found." sub="Run an Ultra Scan to populate best setups." />
        </Card>
      ) : (
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-3">
          {list.map(s => (
            <SetupCard
              key={s.ticker || s.symbol}
              setup={s}
              onOpenChart={onOpenChart}
              onAddToWatchlist={onAddToWatchlist}
            />
          ))}
        </div>
      )}
    </div>
  )
}

// ─── SECTION 4 — TOP CANDIDATES ──────────────────────────────────────────────

const FILTER_OPTS = ['All', 'High Score', 'ABR B+', 'EMA OK', 'Fresh']
const SORT_OPTS   = ['Ultra Score', '% Today', 'Bull Score']

function CandidateRow({ card, rank, onOpenChart, onAddToWatchlist }) {
  const up = (card.change_pct ?? 0) >= 0

  return (
    <div className="flex items-center gap-3 px-3 py-2.5 rounded-lg bg-md-surface-high border border-white/[0.05] hover:border-white/[0.12] transition-colors group">
      <span className="text-[11px] text-md-on-surface-var font-mono w-5 shrink-0 text-right">{rank}</span>

      <div className="flex items-center gap-2 min-w-[130px]">
        <span className="text-sm font-bold text-md-on-surface">{card.ticker}</span>
        <BandBadge band={card.band} />
      </div>

      <span className={cx('text-xs font-bold shrink-0 w-[60px] text-right tabular-nums', up ? BULL : BEAR)}>
        {up ? '+' : ''}{card.change_pct?.toFixed(2) ?? '—'}%
      </span>

      <div className="flex-1 min-w-[90px] hidden sm:block">
        <div className="flex justify-between text-[10px] mb-1">
          <span className="text-md-on-surface-var">Ultra</span>
          <span className="font-mono text-md-on-surface">{(card.ultra_score ?? 0).toFixed(0)}</span>
        </div>
        <ScoreBar value={card.ultra_score ?? 0} color="bg-violet-500" />
      </div>

      <div className="shrink-0">
        <ActionBadge bucket={card.action_bucket ?? 'WATCH_CLOSELY'} />
      </div>

      <span className="text-[10px] text-md-on-surface-var shrink-0 w-14 text-center capitalize hidden lg:block">
        {card.vol_bucket?.toLowerCase() ?? '—'}
      </span>

      {card.abr && (
        <div className="hidden xl:block">
          <Badge label={`ABR ${card.abr}`} variant={card.abr === 'B+' || card.abr === 'A' ? 'bull' : 'default'} />
        </div>
      )}

      <div className="flex items-center gap-1 ml-auto opacity-0 group-hover:opacity-100 transition-opacity shrink-0">
        {onOpenChart && (
          <button
            onClick={() => onOpenChart(card.ticker)}
            className="text-[10px] px-2 py-0.5 rounded bg-white/[0.07] text-md-on-surface-var hover:text-md-on-surface border border-white/[0.07]"
          >
            Chart
          </button>
        )}
        {onAddToWatchlist && (
          <button
            onClick={() => onAddToWatchlist(card.ticker)}
            className="text-[10px] px-2 py-0.5 rounded bg-white/[0.07] text-md-on-surface-var hover:text-md-on-surface border border-white/[0.07]"
          >
            +WL
          </button>
        )}
      </div>
    </div>
  )
}

function TopCandidatesPanel({ candidates, loading, onOpenChart, onAddToWatchlist }) {
  const [limit, setLimit]   = useState(20)
  const [filter, setFilter] = useState('All')
  const [sort, setSort]     = useState('Ultra Score')

  const cards = candidates?.cards ?? []

  const filtered = cards.filter(c => {
    if (filter === 'High Score') return (c.ultra_score ?? 0) >= 70
    if (filter === 'ABR B+')    return c.abr === 'B+' || c.abr === 'A'
    if (filter === 'EMA OK')    return c.ema_ok
    if (filter === 'Fresh')     return !!c.scanned_at
    return true
  })

  const sorted = [...filtered].sort((a, b) => {
    if (sort === 'Ultra Score') return (b.ultra_score ?? 0) - (a.ultra_score ?? 0)
    if (sort === '% Today')     return (b.change_pct ?? 0) - (a.change_pct ?? 0)
    if (sort === 'Bull Score')  return (b.bull_score ?? 0) - (a.bull_score ?? 0)
    return 0
  })

  return (
    <div>
      <SectionTitle badge={filtered.length || null}>Top Candidates</SectionTitle>

      {/* Controls */}
      <div className="flex items-center gap-2 flex-wrap mb-3">
        <div className="flex rounded-lg border border-white/[0.08] overflow-hidden">
          {[10, 20, 50].map((n, i) => (
            <button
              key={n}
              onClick={() => setLimit(n)}
              className={cx(
                'text-xs px-3 py-1.5 transition-colors',
                i > 0 && 'border-l border-white/[0.08]',
                limit === n
                  ? 'bg-md-primary-container text-md-on-primary-container'
                  : 'text-md-on-surface-var hover:text-md-on-surface'
              )}
            >
              Top {n}
            </button>
          ))}
        </div>

        <div className="flex gap-1.5 flex-wrap">
          {FILTER_OPTS.map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={cx(
                'text-[11px] px-2.5 py-1 rounded-md border transition-colors',
                filter === f
                  ? 'bg-md-primary/20 text-md-primary border-md-primary/30'
                  : 'text-md-on-surface-var border-white/[0.07] hover:border-white/[0.14]'
              )}
            >
              {f}
            </button>
          ))}
        </div>

        <select
          value={sort}
          onChange={e => setSort(e.target.value)}
          className="ml-auto text-[11px] px-2 py-1 rounded-md bg-md-surface-high border border-white/[0.08] text-md-on-surface-var"
        >
          {SORT_OPTS.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      {loading && !cards.length ? (
        <div className="flex flex-col gap-1.5">
          {[1, 2, 3, 4, 5].map(i => <SkeletonRow key={i} h="h-11" />)}
        </div>
      ) : sorted.length === 0 ? (
        <Card>
          <EmptyState icon="🔍" message="No candidates match the current filter." sub="Try a different filter or run Ultra Scan." />
        </Card>
      ) : (
        <div className="flex flex-col gap-1.5">
          {sorted.slice(0, limit).map((card, i) => (
            <CandidateRow
              key={card.ticker}
              card={card}
              rank={i + 1}
              onOpenChart={onOpenChart}
              onAddToWatchlist={onAddToWatchlist}
            />
          ))}
        </div>
      )}
    </div>
  )
}

// ─── SECTION 5 — MARKET NEWS ──────────────────────────────────────────────────

const NEWS_VARIANTS = {
  Market: 'info', Macro: 'info', Earnings: 'warn', FDA: 'ai',
  Analyst: 'default', Offering: 'bear', Merger: 'bull', Biotech: 'ai',
  Sector: 'default', Risk: 'bear', AI: 'ai', Crypto: 'warn',
}

function NewsItem({ item }) {
  return (
    <div className="flex flex-col gap-1.5 py-2.5 border-b border-white/[0.05] last:border-0">
      <div className="flex items-start justify-between gap-2">
        <p className="text-xs text-md-on-surface leading-relaxed flex-1">
          {item.headline || item.title}
        </p>
        <span className="text-[10px] text-md-on-surface-var shrink-0 mt-0.5 tabular-nums">
          {relTime(item.published_at || item.time)}
        </span>
      </div>
      <div className="flex items-center gap-2 flex-wrap">
        {item.category && <Badge label={item.category} variant={NEWS_VARIANTS[item.category] ?? 'default'} />}
        {item.ticker   && <span className="text-[10px] font-mono text-md-primary">{item.ticker}</span>}
        {item.source   && <span className="text-[10px] text-md-on-surface-var opacity-40">{item.source}</span>}
      </div>
    </div>
  )
}

function MarketNewsPanel({ news, loading }) {
  const items = news?.items ?? []

  return (
    <Card>
      <SectionTitle>Market News</SectionTitle>
      {loading && !items.length ? (
        <div className="flex flex-col gap-3">
          {[1, 2, 3].map(i => <SkeletonRow key={i} h="h-12" />)}
        </div>
      ) : items.length === 0 ? (
        <EmptyState
          icon="📰"
          message="No relevant market news found."
          sub={news?.message ?? 'Connect a news API to enable this section.'}
        />
      ) : (
        <div className="flex flex-col">
          {items.slice(0, 8).map((item, i) => <NewsItem key={i} item={item} />)}
        </div>
      )}
    </Card>
  )
}

// ─── SECTION 6 — SECTOR STRENGTH ─────────────────────────────────────────────

const SECTOR_NAMES = {
  XLK: 'Technology', XLV: 'Health Care', XLF: 'Financials', XLY: 'Cons. Disc.',
  XLP: 'Cons. Staples', XLE: 'Energy', XLI: 'Industrials', XLB: 'Materials',
  XLRE: 'Real Estate', XLU: 'Utilities', XLC: 'Comm. Svcs.',
  IBB: 'Biotech', XBI: 'Biotech ETF', SMH: 'Semis', GDX: 'Gold Miners',
}

function SectorCard({ sector }) {
  const hot  = sector.trend === 'hot'
  const cold = sector.trend === 'cold'
  const up   = (sector.return_1d ?? 0) >= 0
  const norm = Math.min(100, Math.max(0, (sector.hotness / 5 + 1) * 50))

  return (
    <div className={cx(
      'flex flex-col gap-2 px-3 py-3 rounded-xl bg-md-surface-high border transition-colors',
      hot  ? 'border-emerald-500/20' :
      cold ? 'border-rose-500/20' :
             'border-white/[0.06]'
    )}>
      <div className="flex items-start justify-between gap-2">
        <div className="flex flex-col gap-0.5">
          <span className="text-xs font-bold text-md-on-surface leading-tight">
            {SECTOR_NAMES[sector.etf] ?? sector.name ?? sector.etf}
          </span>
          <span className="text-[10px] text-md-on-surface-var font-mono">{sector.etf}</span>
        </div>
        <div className="flex flex-col items-end gap-1 shrink-0">
          <span className={cx('text-xs font-bold tabular-nums', up ? BULL : BEAR)}>
            {up ? '+' : ''}{sector.return_1d?.toFixed(2)}%
          </span>
          {hot  && <Badge label="HOT"  variant="bull" />}
          {cold && <Badge label="COLD" variant="bear" />}
        </div>
      </div>
      <div className="h-1.5 rounded-full bg-white/[0.07] overflow-hidden">
        <div
          className={cx('h-full rounded-full', hot ? 'bg-emerald-500' : cold ? 'bg-rose-500' : 'bg-slate-500')}
          style={{ width: `${norm}%` }}
        />
      </div>
      <span className="text-[10px] text-md-on-surface-var">
        5d: {sector.return_5d >= 0 ? '+' : ''}{sector.return_5d?.toFixed(2)}%
      </span>
    </div>
  )
}

function SectorStrengthPanel({ sectors, loading }) {
  const list = sectors?.sectors ?? []
  const hot  = list.filter(s => s.trend === 'hot')
  const rest = list.filter(s => s.trend !== 'hot')

  return (
    <div>
      <SectionTitle badge={list.length || null}>Sector Strength</SectionTitle>
      {loading && !list.length ? (
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2">
          {[1, 2, 3, 4, 5].map(i => <SkeletonRow key={i} h="h-[90px]" />)}
        </div>
      ) : list.length === 0 ? (
        <Card>
          <EmptyState icon="🌐" message="No sector data available." sub="Load the Sectors tab to populate sector data." />
        </Card>
      ) : (
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2">
          {[...hot, ...rest].map(s => <SectorCard key={s.etf} sector={s} />)}
        </div>
      )}
    </div>
  )
}

// ─── SECTION 7 — RISK ALERTS ──────────────────────────────────────────────────

function RiskAlertItem({ alert }) {
  const cfg = {
    critical: { bg: 'bg-rose-500/10 border-l-rose-400',   icon: '✕', textCls: 'text-rose-200',  badge: 'CRIT',   bv: 'avoid' },
    high:     { bg: 'bg-rose-500/7  border-l-rose-500',   icon: '⚠', textCls: 'text-rose-300',  badge: 'HIGH',   bv: 'bear' },
    medium:   { bg: 'bg-amber-500/7 border-l-amber-500',  icon: '●', textCls: 'text-amber-300', badge: 'MED',    bv: 'warn' },
    low:      { bg: 'bg-slate-500/7 border-l-slate-400',  icon: '○', textCls: 'text-slate-400', badge: 'LOW',    bv: 'default' },
  }[alert.severity] ?? { bg: 'bg-slate-500/7 border-l-slate-500', icon: '○', textCls: 'text-slate-400', badge: '—', bv: 'default' }

  return (
    <div className={cx('flex items-start gap-3 px-3 py-2.5 rounded-r-lg border-l-2', cfg.bg)}>
      <span className={cx('text-xs shrink-0 mt-0.5', cfg.textCls)}>{cfg.icon}</span>
      <p className="text-xs text-md-on-surface flex-1 leading-relaxed">{alert.message}</p>
      <Badge label={cfg.badge} variant={cfg.bv} />
    </div>
  )
}

function RiskAlertsPanel({ alerts, loading }) {
  const list = alerts?.alerts ?? []
  const allClear = list.length === 0 || (list.length === 1 && list[0].type === 'ok')

  return (
    <Card>
      <SectionTitle
        badge={!allClear ? list.filter(a => a.severity !== 'low').length || null : null}
      >
        Risk Alerts
      </SectionTitle>
      {loading && !list.length ? (
        <div className="flex flex-col gap-2">
          {[1, 2].map(i => <SkeletonRow key={i} h="h-10" />)}
        </div>
      ) : allClear ? (
        <div className="flex flex-col gap-1">
          <div className="flex items-center gap-2 text-xs text-emerald-400">
            <span>✓</span>
            <span>No critical risk alerts right now.</span>
          </div>
          <p className="text-[10px] text-md-on-surface-var opacity-40 mt-1">
            Review ticker-level risk badges before entering any position.
          </p>
        </div>
      ) : (
        <div className="flex flex-col gap-1.5">
          {list.map((a, i) => <RiskAlertItem key={i} alert={a} />)}
        </div>
      )}
    </Card>
  )
}

// ─── SECTION 8 — FRESH SIGNALS FEED ──────────────────────────────────────────

function FreshSignalRow({ sig }) {
  const up = (sig.change_pct ?? 0) >= 0
  return (
    <div className="flex items-center gap-3 py-2 border-b border-white/[0.05] last:border-0">
      <span className="text-xs font-bold text-md-on-surface w-16 shrink-0">{sig.ticker}</span>
      <div className="flex items-center gap-0.5 flex-1 min-w-0 overflow-hidden">
        {Array.from({ length: Math.min(sig.bull_score ?? 0, 10) }).map((_, i) => (
          <div key={i} className="w-1.5 h-3 rounded-sm bg-emerald-500/60 shrink-0" />
        ))}
        {sig.sig_name && (
          <span className="text-[10px] text-md-on-surface-var font-mono ml-2 truncate">{sig.sig_name}</span>
        )}
      </div>
      <span className={cx('text-xs font-bold shrink-0 tabular-nums', up ? BULL : BEAR)}>
        {up ? '+' : ''}{sig.change_pct?.toFixed(2) ?? '—'}%
      </span>
      <span className="text-[10px] text-md-on-surface-var shrink-0 w-10 text-right tabular-nums">
        {relTime(sig.scanned_at)}
      </span>
    </div>
  )
}

function FreshSignalsFeed({ signals, loading }) {
  const list = signals?.signals ?? []

  return (
    <div>
      <SectionTitle badge={list.length || null}>Fresh Signals Feed</SectionTitle>
      {loading && !list.length ? (
        <Card>
          <div className="flex flex-col gap-2">
            {[1, 2, 3].map(i => <SkeletonRow key={i} h="h-8" />)}
          </div>
        </Card>
      ) : list.length === 0 ? (
        <Card>
          <EmptyState
            icon="📡"
            message="No fresh signal changes yet."
            sub="Run the T/Z scanner or wait for the next auto-refresh."
          />
        </Card>
      ) : (
        <Card className="p-0">
          <div className="px-4 py-1 grid grid-cols-1 sm:grid-cols-2 gap-x-6">
            {list.map(sig => <FreshSignalRow key={sig.ticker} sig={sig} />)}
          </div>
        </Card>
      )}
    </div>
  )
}

// ─── SECTION 9 — WATCHLIST SNAPSHOT ──────────────────────────────────────────

const STATUS_CFG = {
  improving:   { label: 'Improving',   cls: BULL },
  valid:       { label: 'Valid',       cls: 'text-teal-400' },
  weakening:   { label: 'Weakening',   cls: WARN },
  invalidated: { label: 'Invalidated', cls: BEAR },
  moved:       { label: 'Moved',       cls: INFO },
  review:      { label: 'Review',      cls: MUTE },
}

function WatchlistSnapshot({ watchlistData, loading }) {
  const items = watchlistData ?? []

  return (
    <div>
      <SectionTitle badge={items.length || null}>Watchlist Snapshot</SectionTitle>
      {loading ? (
        <Card>
          <div className="flex flex-col gap-2">
            {[1, 2, 3].map(i => <SkeletonRow key={i} h="h-8" />)}
          </div>
        </Card>
      ) : items.length === 0 ? (
        <Card>
          <EmptyState
            icon="⭐"
            message="Your watchlist is empty."
            sub="Add tickers from Top Candidates using the +WL button."
          />
        </Card>
      ) : (
        <Card className="p-0">
          <div className="px-4 py-1 divide-y divide-white/[0.04]">
            {items.map(item => {
              const s = STATUS_CFG[item.status] ?? STATUS_CFG.review
              const up = (item.change_pct ?? 0) >= 0
              return (
                <div key={item.ticker} className="flex items-center gap-3 py-2.5">
                  <span className="text-xs font-bold text-md-on-surface w-14 shrink-0">{item.ticker}</span>
                  <span className={cx('text-[11px] shrink-0', s.cls)}>{s.label}</span>
                  <div className="flex-1" />
                  {item.sig_name && <Badge label={item.sig_name} variant="info" />}
                  <ActionBadge bucket={item.action_bucket} />
                  <span className={cx('text-xs font-bold shrink-0 w-14 text-right tabular-nums', up ? BULL : BEAR)}>
                    {item.change_pct != null ? `${up ? '+' : ''}${item.change_pct.toFixed(2)}%` : '—'}
                  </span>
                </div>
              )
            })}
          </div>
        </Card>
      )}
    </div>
  )
}

// ─── SECTION 10 — DATA HEALTH ─────────────────────────────────────────────────

function DataHealthCard({ scanner, ultra, loading }) {
  function healthOf(s) {
    if (!s) return { label: 'UNAVAIL.', cls: MUTE }
    if (s.running) return { label: 'RUNNING', cls: INFO }
    if (!s.last_scan) return { label: 'IDLE', cls: MUTE }
    const age = (Date.now() - new Date(s.last_scan).getTime()) / 60000
    if (age < 30) return { label: 'HEALTHY', cls: BULL }
    if (age < 120) return { label: 'OK', cls: WARN }
    return { label: 'STALE', cls: BEAR }
  }

  function fmtTime(iso) {
    if (!iso) return 'Never'
    try { return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) }
    catch { return iso }
  }

  const sh = healthOf(scanner)
  const uh = healthOf(ultra)

  return (
    <Card>
      <SectionTitle>Data Health</SectionTitle>
      {loading ? (
        <SkeletonRow h="h-16" />
      ) : (
        <div className="grid grid-cols-2 gap-4">
          <div className="flex flex-col gap-1.5">
            <span className="text-[10px] text-md-on-surface-var uppercase tracking-wider">T/Z Scanner</span>
            <span className={cx('text-sm font-bold font-mono', sh.cls)}>{sh.label}</span>
            <span className="text-[10px] text-md-on-surface-var">
              Last: {fmtTime(scanner?.last_scan)}
            </span>
            {scanner?.running && (
              <div className="h-1 rounded-full bg-white/[0.07] overflow-hidden">
                <div
                  className="h-full rounded-full bg-md-primary animate-pulse"
                  style={{ width: `${scanner.total > 0 ? Math.round(scanner.done / scanner.total * 100) : 0}%` }}
                />
              </div>
            )}
          </div>
          <div className="flex flex-col gap-1.5">
            <span className="text-[10px] text-md-on-surface-var uppercase tracking-wider">ULTRA Scanner</span>
            <span className={cx('text-sm font-bold font-mono', uh.cls)}>{uh.label}</span>
            <span className="text-[10px] text-md-on-surface-var">
              Last: {fmtTime(ultra?.last_scan)}
            </span>
            {ultra?.running && (
              <div className="h-1 rounded-full bg-white/[0.07] overflow-hidden">
                <div
                  className="h-full rounded-full bg-violet-500 animate-pulse"
                  style={{ width: `${ultra.total > 0 ? Math.round(ultra.done / ultra.total * 100) : 0}%` }}
                />
              </div>
            )}
          </div>
        </div>
      )}
    </Card>
  )
}

// ─── MAIN PANEL ───────────────────────────────────────────────────────────────

export default function TradingDashboardPanel({ onSelectTicker, onAddToWatchlist, watchlistTickers = [] }) {
  const [status,     setStatus]     = useState(null)
  const [pulse,      setPulse]      = useState(null)
  const [summary,    setSummary]    = useState(null)
  const [candidates, setCandidates] = useState(null)
  const [sectors,    setSectors]    = useState(null)
  const [fresh,      setFresh]      = useState(null)
  const [risk,       setRisk]       = useState(null)
  const [setups,     setSetups]     = useState(null)
  const [brief,      setBrief]      = useState(null)
  const [news,       setNews]       = useState(null)
  const [watchlist,  setWatchlist]  = useState(null)

  const [loading,     setLoading]     = useState(true)
  const [lastRefresh, setLastRefresh] = useState(null)
  const abortRef = useRef(null)

  const loadAll = useCallback(async (silent = false) => {
    if (abortRef.current) abortRef.current.abort()
    abortRef.current = new AbortController()
    if (!silent) setLoading(true)

    try {
      // Wave 1 — critical: status, pulse, summary
      const [statusData, pulseData, summaryData] = await Promise.all([
        apiFetch('/status'),
        apiFetch('/pulse'),
        apiFetch('/summary'),
      ])
      setStatus(statusData)
      setPulse(pulseData)
      setSummary(summaryData)

      // Wave 2 — secondary (non-blocking individually)
      const wave2 = await Promise.allSettled([
        apiFetch('/top50?limit=50'),
        apiFetch('/sector-heat'),
        apiFetch('/fresh-signals?limit=30'),
        apiFetch('/risk-alerts'),
        apiFetch('/news'),
      ])
      const [top50, sects, freshSigs, riskData, newsData] = wave2
      if (top50.status      === 'fulfilled') setCandidates(top50.value)
      if (sects.status      === 'fulfilled') setSectors(sects.value)
      if (freshSigs.status  === 'fulfilled') setFresh(freshSigs.value)
      if (riskData.status   === 'fulfilled') setRisk(riskData.value)
      if (newsData.status   === 'fulfilled') setNews(newsData.value)

      // Wave 2b — watchlist
      if (watchlistTickers.length > 0) {
        try {
          const wlData = await watchlistFetch(watchlistTickers)
          const items = (wlData?.items ?? wlData ?? []).map(item => {
            const score = item.bull_score ?? 0
            const status =
              score >= 6 ? 'improving' :
              score >= 4 ? 'valid' :
              score >= 2 ? 'weakening' : 'review'
            const action =
              score >= 7 ? 'BUY_READY' :
              score >= 5 ? 'WATCH_CLOSELY' :
              score >= 3 ? 'WAIT_CONFIRMATION' : 'AVOID'
            return { ...item, status, action_bucket: action }
          })
          setWatchlist(items)
        } catch (_) {}
      }

      // Wave 3 — AI (slowest, non-critical)
      const wave3 = await Promise.allSettled([
        apiFetch('/best-setups'),
        apiFetch('/ai-brief'),
      ])
      if (wave3[0].status === 'fulfilled') setSetups(wave3[0].value)
      if (wave3[1].status === 'fulfilled') setBrief(wave3[1].value)

      setLastRefresh(new Date())
    } catch (err) {
      if (err.name !== 'AbortError') console.error('Dashboard load error:', err)
    } finally {
      if (!silent) setLoading(false)
    }
  }, [watchlistTickers])

  useEffect(() => {
    loadAll()
    const t = setInterval(() => loadAll(true), 60_000)
    return () => { clearInterval(t); abortRef.current?.abort() }
  }, [loadAll])

  const handleOpenChart = useCallback((ticker) => {
    if (onSelectTicker) onSelectTicker(ticker)
  }, [onSelectTicker])

  const handleAddToWatchlist = useCallback((ticker) => {
    if (onAddToWatchlist) onAddToWatchlist(ticker)
  }, [onAddToWatchlist])

  return (
    <div className="flex flex-col gap-6 pb-8">

      {/* ── Market Command Bar ──────────────────────────────────────────── */}
      <MarketCommandBar
        status={status}
        pulse={pulse}
        summary={summary}
        onRefresh={() => loadAll(false)}
        loading={loading}
        lastRefresh={lastRefresh}
      />

      {/* ── Main 2-column grid ──────────────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 xl:grid-cols-4 gap-5">

        {/* Left: Best Setups + Top Candidates */}
        <div className="lg:col-span-2 xl:col-span-3 flex flex-col gap-6">
          <BestSetupsToday
            setups={setups}
            loading={loading}
            onOpenChart={handleOpenChart}
            onAddToWatchlist={handleAddToWatchlist}
          />
          <TopCandidatesPanel
            candidates={candidates}
            loading={loading}
            onOpenChart={handleOpenChart}
            onAddToWatchlist={handleAddToWatchlist}
          />
        </div>

        {/* Right rail: AI Brief + News + Risk + Health */}
        <div className="flex flex-col gap-4">
          <AIMarketBrief brief={brief} loading={loading} />
          <MarketNewsPanel news={news} loading={loading} />
          <RiskAlertsPanel alerts={risk} loading={loading} />
          <DataHealthCard scanner={status?.scanner} ultra={status?.ultra} loading={loading} />
        </div>
      </div>

      {/* ── Sector Strength ─────────────────────────────────────────────── */}
      <SectorStrengthPanel sectors={sectors} loading={loading} />

      {/* ── Bottom grid: Fresh Signals + Watchlist ───────────────────────  */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
        <FreshSignalsFeed signals={fresh} loading={loading} />
        <WatchlistSnapshot watchlistData={watchlist} loading={loading && !watchlist} />
      </div>

    </div>
  )
}
