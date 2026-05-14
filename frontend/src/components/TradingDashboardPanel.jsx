/**
 * TradingDashboardPanel.jsx — Decision-first Market Command Center
 * Uses existing design system (md-* tokens, rounded-md-*, spacing conventions).
 * No new palette or global style changes.
 */
import { useState, useEffect, useCallback, useRef } from 'react'

const API = '/api/dashboard'

async function apiFetch(path) {
  const r = await fetch(`${API}${path}`)
  if (!r.ok) throw new Error(r.status)
  return r.json()
}

async function watchlistApiFetch(tickers) {
  if (!tickers?.length) return []
  const r = await fetch(`/api/watchlist?tickers=${tickers.join(',')}&tf=1d`)
  if (!r.ok) throw new Error(r.status)
  const d = await r.json()
  return Array.isArray(d) ? d : (d.items ?? d.results ?? [])
}

// ─── Shared atoms (use existing system tokens only) ───────────────────────────

const cx = (...args) => args.filter(Boolean).join(' ')

const BUCKET_BORDER = {
  BUY_READY:         'border-l-emerald-500',
  WATCH_CLOSELY:     'border-l-amber-500',
  WAIT_CONFIRMATION: 'border-l-sky-500',
  TOO_LATE:          'border-l-orange-400',
  AVOID:             'border-l-rose-500',
}
const BUCKET_LABEL = {
  BUY_READY:         'BUY READY',
  WATCH_CLOSELY:     'WATCH',
  WAIT_CONFIRMATION: 'WAIT',
  TOO_LATE:          'TOO LATE',
  AVOID:             'AVOID',
}
const BUCKET_CLASS = {
  BUY_READY:         'bg-emerald-900/60 text-emerald-300 border-emerald-700/50',
  WATCH_CLOSELY:     'bg-amber-900/60   text-amber-300   border-amber-700/50',
  WAIT_CONFIRMATION: 'bg-sky-900/60     text-sky-300     border-sky-700/50',
  TOO_LATE:          'bg-orange-900/60  text-orange-300  border-orange-700/50',
  AVOID:             'bg-rose-900/60    text-rose-300    border-rose-700/50',
}

function scoreColor(v) {
  if (v >= 80) return 'text-lime-300 font-bold'
  if (v >= 65) return 'text-emerald-400 font-semibold'
  if (v >= 50) return 'text-yellow-300'
  if (v >= 35) return 'text-sky-400'
  return 'text-md-on-surface-var'
}

function ActionChip({ bucket, size = 'sm' }) {
  const cls   = BUCKET_CLASS[bucket] ?? 'bg-md-surface-high text-md-on-surface-var border-white/10'
  const label = BUCKET_LABEL[bucket] ?? bucket ?? '—'
  const sz    = size === 'sm' ? 'text-[10px] px-1.5 py-px' : 'text-xs px-2 py-0.5'
  return (
    <span className={cx(
      'inline-flex items-center font-bold font-mono leading-none rounded-md-sm border whitespace-nowrap shrink-0',
      sz, cls
    )}>
      {label}
    </span>
  )
}

function BandChip({ band }) {
  const cls = {
    'S':  'bg-violet-900/60 text-violet-300 border-violet-700/50',
    'A+': 'bg-emerald-900/60 text-emerald-300 border-emerald-700/50',
    'A':  'bg-teal-900/60 text-teal-300 border-teal-700/50',
    'B':  'bg-sky-900/60 text-sky-300 border-sky-700/50',
    'C':  'bg-slate-800/80 text-slate-400 border-slate-600/50',
  }[band] ?? 'bg-md-surface-high text-md-on-surface-var border-white/10'
  return (
    <span className={cx(
      'inline-flex px-1.5 py-px text-[10px] font-bold font-mono rounded-md-sm border leading-none shrink-0',
      cls
    )}>
      {band ?? '—'}
    </span>
  )
}

function MiniChip({ label, color = 'default' }) {
  const cls = {
    default: 'bg-md-surface-con text-md-on-surface-var border-md-outline-var',
    bull:    'bg-emerald-900/40 text-emerald-300 border-emerald-800/50',
    info:    'bg-sky-900/40 text-sky-300 border-sky-800/50',
    warn:    'bg-amber-900/40 text-amber-300 border-amber-800/50',
    ai:      'bg-violet-900/40 text-violet-300 border-violet-800/50',
    risk:    'bg-rose-900/40 text-rose-300 border-rose-800/50',
  }[color] ?? 'bg-md-surface-con text-md-on-surface-var border-md-outline-var'
  return (
    <span className={cx(
      'inline-flex items-center px-1.5 py-px text-[10px] font-medium rounded-md-sm border leading-none whitespace-nowrap shrink-0',
      cls
    )}>
      {label}
    </span>
  )
}

function SectionHead({ title, count, right }) {
  return (
    <div className="flex items-center justify-between mb-3">
      <div className="flex items-center gap-2">
        <h2 className="text-xs font-semibold uppercase tracking-widest text-md-on-surface-var">
          {title}
        </h2>
        {count != null && (
          <span className="text-[10px] font-mono px-1.5 py-px rounded-md-sm bg-md-surface-con text-md-on-surface-var border border-md-outline-var">
            {count}
          </span>
        )}
      </div>
      {right}
    </div>
  )
}

function Panel({ children, className = '' }) {
  return (
    <div className={cx(
      'rounded-md-md bg-md-surface-high border border-md-outline-var',
      className
    )}>
      {children}
    </div>
  )
}

function Divider({ vertical = false }) {
  return vertical
    ? <div className="w-px self-stretch bg-md-outline-var opacity-60" />
    : <div className="w-full h-px bg-md-outline-var opacity-60" />
}

function Skeleton({ h = 'h-10', w = 'w-full', className = '' }) {
  return <div className={cx(h, w, 'rounded-md-sm bg-md-surface-con animate-pulse', className)} />
}

function EmptySlate({ icon, title, sub }) {
  return (
    <div className="flex flex-col items-center justify-center gap-1.5 py-8 text-center">
      {icon && <span className="text-xl opacity-20">{icon}</span>}
      <p className="text-xs text-md-on-surface-var">{title}</p>
      {sub && <p className="text-[10px] text-md-on-surface-var opacity-50">{sub}</p>}
    </div>
  )
}

function relTime(ts) {
  if (!ts) return ''
  const d = (Date.now() - new Date(ts).getTime()) / 1000
  if (d < 3600)  return `${Math.round(d / 60)}m`
  if (d < 86400) return `${Math.round(d / 3600)}h`
  return `${Math.round(d / 86400)}d`
}

// ─── SECTOR CHIP ─────────────────────────────────────────────────────────────

const SECTOR_ABBR = {
  'Technology':               'Tech',
  'Communication Services':   'Comm',
  'Consumer Discretionary':   'Cons Disc',
  'Consumer Staples':         'Staples',
  'Health Care':              'Health',
  'Financials':               'Fin',
  'Energy':                   'Energy',
  'Industrials':              'Indust',
  'Materials':                'Matls',
  'Real Estate':              'RE',
  'Utilities':                'Util',
}

function SectorChip({ sector, theme }) {
  const label = theme ?? SECTOR_ABBR[sector] ?? sector
  if (!label) return null
  return <MiniChip label={label} />
}

// ─── EVENT BADGE ─────────────────────────────────────────────────────────────

const EVENT_COLOR = {
  EARNINGS:          'warn',
  FDA:               'ai',
  REVERSE_SPLIT:     'risk',
  OFFERING:          'risk',
  ANALYST_UPGRADE:   'bull',
  ANALYST_DOWNGRADE: 'warn',
  MERGER:            'info',
  INSIDER_BUY:       'bull',
  INSIDER_SELL:      'warn',
  NEWS_SPIKE:        'info',
  HALT:              'risk',
  SPLIT:             'info',
}

function EventBadge({ event }) {
  const color = EVENT_COLOR[event.event_type] ?? 'default'
  return <MiniChip label={event.label} color={color} />
}

// ─── SENTIMENT CHIP ──────────────────────────────────────────────────────────

const SENTIMENT_CFG = {
  BULLISH:        { cls: 'bg-lime-900/50 text-lime-300 border-lime-700/50',         label: 'Bullish' },
  MILDLY_BULLISH: { cls: 'bg-emerald-900/50 text-emerald-300 border-emerald-700/50', label: 'Mildly Bullish' },
  NEUTRAL:        { cls: 'bg-md-surface-con text-md-on-surface-var border-md-outline-var', label: 'Neutral' },
  MILDLY_BEARISH: { cls: 'bg-amber-900/50 text-amber-300 border-amber-700/50',      label: 'Mildly Bearish' },
  BEARISH:        { cls: 'bg-rose-900/50 text-rose-300 border-rose-700/50',         label: 'Bearish' },
  RISKY:          { cls: 'bg-orange-900/50 text-orange-300 border-orange-700/50',   label: 'Risky' },
}

function SentimentChip({ sentiment }) {
  const cfg = SENTIMENT_CFG[sentiment] ?? {
    cls:   'bg-md-surface-con text-md-on-surface-var border-md-outline-var',
    label: sentiment ?? '—',
  }
  return (
    <span className={cx(
      'inline-flex px-1.5 py-px text-[10px] font-bold rounded-md-sm border leading-none shrink-0',
      cfg.cls
    )}>
      {cfg.label}
    </span>
  )
}

// ─── NEWS DRAWER ─────────────────────────────────────────────────────────────

function NewsDrawer({ ticker, onClose }) {
  const [newsData,  setNewsData]  = useState(null)
  const [analyzing, setAnalyzing] = useState(false)

  useEffect(() => {
    if (!ticker) return
    setNewsData(null)
    setAnalyzing(false)
    fetch(`/api/dashboard/ticker-news/${ticker}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        setNewsData(d)
        if (d && d.news_count > 0 && !d.ai_summary) {
          setAnalyzing(true)
          fetch(`/api/dashboard/ticker-news/${ticker}/analyze`, { method: 'POST' })
            .then(r => r.ok ? r.json() : null)
            .then(analyzed => {
              if (analyzed) setNewsData(analyzed)
              setAnalyzing(false)
            })
            .catch(() => setAnalyzing(false))
        }
      })
      .catch(() => {})
  }, [ticker])

  if (!ticker) return null

  return (
    <div className="fixed inset-0 z-50 flex items-stretch justify-end" onClick={onClose}>
      <div className="absolute inset-0 bg-black/50" />
      <div
        className="relative w-full max-w-md h-full overflow-y-auto bg-md-surface border-l border-md-outline-var flex flex-col shadow-xl"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-md-outline-var shrink-0 bg-md-surface-high">
          <div className="flex items-center gap-2">
            <span className="text-base font-bold text-md-on-surface">{ticker}</span>
            <span className="text-xs text-md-on-surface-var">News & AI Analysis</span>
          </div>
          <button
            onClick={onClose}
            className="text-md-on-surface-var hover:text-md-on-surface p-1 text-lg leading-none"
          >
            ✕
          </button>
        </div>

        <div className="flex flex-col gap-4 p-4 flex-1">
          {!newsData ? (
            <div className="flex flex-col gap-2">
              {[1, 2, 3].map(i => <Skeleton key={i} h="h-8" />)}
            </div>
          ) : newsData.news_count === 0 ? (
            <EmptySlate icon="📰" title="No recent news found." sub={`No recent headlines for ${ticker}`} />
          ) : (
            <>
              {/* AI Summary */}
              <div className="rounded-md-sm bg-md-surface-con border border-md-outline-var p-3">
                <div className="flex items-center gap-2 mb-2.5">
                  <MiniChip label="AI Analysis" color="ai" />
                  {analyzing && (
                    <span className="text-[10px] text-md-on-surface-var animate-pulse">Analyzing…</span>
                  )}
                </div>
                {analyzing && !newsData.ai_summary ? (
                  <div className="flex flex-col gap-2">
                    {[1, 2].map(i => <Skeleton key={i} h="h-4" />)}
                  </div>
                ) : newsData.ai_summary ? (
                  <div className="flex flex-col gap-2.5">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <SentimentChip sentiment={newsData.ai_summary.sentiment} />
                      {newsData.ai_summary.setup_impact && newsData.ai_summary.setup_impact !== 'UNKNOWN' && (
                        <MiniChip
                          label={newsData.ai_summary.setup_impact.replace(/_/g, ' ')}
                          color={
                            newsData.ai_summary.setup_impact === 'SUPPORTS_SETUP' ? 'bull' :
                            newsData.ai_summary.setup_impact === 'WEAKENS_SETUP'  ? 'warn' : 'default'
                          }
                        />
                      )}
                      {newsData.ai_summary.catalyst_type && newsData.ai_summary.catalyst_type !== 'UNKNOWN' && (
                        <MiniChip
                          label={newsData.ai_summary.catalyst_type.replace(/_/g, ' ')}
                          color="info"
                        />
                      )}
                    </div>
                    {newsData.ai_summary.summary && (
                      <p className="text-xs text-md-on-surface leading-relaxed">
                        {newsData.ai_summary.summary}
                      </p>
                    )}
                    {newsData.ai_summary.why_it_matters?.length > 0 && (
                      <div>
                        <p className="text-[10px] font-semibold text-md-positive uppercase tracking-wider mb-1">
                          Why it matters
                        </p>
                        {newsData.ai_summary.why_it_matters.map((w, i) => (
                          <p key={i} className="flex items-start gap-1 text-[11px] text-md-on-surface leading-snug mb-0.5">
                            <span className="text-md-positive shrink-0 mt-px">›</span>
                            <span>{w}</span>
                          </p>
                        ))}
                      </div>
                    )}
                    {newsData.ai_summary.risks?.length > 0 && (
                      <div>
                        <p className="text-[10px] font-semibold text-md-negative uppercase tracking-wider mb-1">
                          Risks
                        </p>
                        {newsData.ai_summary.risks.map((r, i) => (
                          <p key={i} className="flex items-start gap-1 text-[11px] text-md-on-surface leading-snug mb-0.5">
                            <span className="text-md-negative shrink-0 mt-px">›</span>
                            <span>{r}</span>
                          </p>
                        ))}
                      </div>
                    )}
                  </div>
                ) : (
                  <p className="text-xs text-md-on-surface-var">Analysis not available.</p>
                )}
              </div>

              {/* Headlines */}
              <div>
                <p className="text-[10px] font-semibold text-md-on-surface-var uppercase tracking-wider mb-2">
                  {newsData.news_count} Headlines
                </p>
                <div className="flex flex-col divide-y divide-md-outline-var">
                  {newsData.items.map((item, i) => (
                    <div key={i} className="py-2.5">
                      <p className="text-xs text-md-on-surface leading-snug">{item.headline}</p>
                      <div className="flex items-center justify-between gap-2 mt-1">
                        <span className="text-[10px] text-md-on-surface-var truncate">{item.source}</span>
                        <span className="text-[10px] text-md-on-surface-var shrink-0">
                          {relTime(item.published_at)}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}

// ─── MARKET COMMAND STRIP ─────────────────────────────────────────────────────

function MarketClock({ market }) {
  const [tick, setTick] = useState(0)
  useEffect(() => {
    const t = setInterval(() => setTick(n => n + 1), 1000)
    return () => clearInterval(t)
  }, [])

  if (!market) return <Skeleton h="h-8" w="w-40" />

  const secs = Math.max(0, (market.secs_to_next || 0) - tick)
  const hh   = String(Math.floor(secs / 3600)).padStart(2, '0')
  const mm   = String(Math.floor((secs % 3600) / 60)).padStart(2, '0')
  const ss   = String(secs % 60).padStart(2, '0')

  const phaseLabel = {
    regular:     'OPEN',
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
    market.status === 'open'        ? 'bg-emerald-400 shadow-[0_0_6px_2px_rgba(52,211,153,0.4)]' :
    market.status === 'pre_market'  ? 'bg-amber-400' :
    market.status === 'after_hours' ? 'bg-amber-400' :
    'bg-slate-600'

  return (
    <div className="flex items-center gap-2.5 shrink-0">
      <span className={cx('w-2 h-2 rounded-full shrink-0', dotCls)} />
      <div className="flex flex-col gap-0">
        <span className="text-[11px] font-bold text-md-on-surface tracking-wider uppercase leading-tight">
          {phaseLabel}
        </span>
        <span className="text-[10px] text-md-on-surface-var leading-tight">
          {market.local_date} · ET {market.local_time}
        </span>
      </div>
      {secs > 0 && nextLabel && (
        <div className="flex flex-col gap-0 ml-1 pl-2.5 border-l border-md-outline-var">
          <span className="font-mono text-xs font-bold text-md-on-surface leading-tight">{hh}:{mm}:{ss}</span>
          <span className="text-[10px] text-md-on-surface-var leading-tight">{nextLabel}</span>
        </div>
      )}
    </div>
  )
}

function InlinePulse({ item }) {
  if (!item) return null
  const up = item.change_1d >= 0
  const LABELS = { SPY: 'S&P 500', QQQ: 'Nasdaq', IWM: 'R2K', VIX: 'VIX' }
  return (
    <div className="flex flex-col gap-0 shrink-0">
      <div className="flex items-baseline gap-1">
        <span className="text-[11px] font-bold text-md-on-surface">{item.ticker}</span>
        <span className="text-[10px] text-md-on-surface-var">{LABELS[item.ticker] ?? ''}</span>
      </div>
      <div className="flex items-center gap-1.5">
        <span className="text-[11px] font-mono font-semibold text-md-on-surface">
          {item.price?.toFixed(2)}
        </span>
        <span className={cx('text-[10px] font-bold', up ? 'text-md-positive' : 'text-md-negative')}>
          {up ? '+' : ''}{item.change_1d?.toFixed(2)}%
        </span>
      </div>
    </div>
  )
}

function CommandStrip({ status, pulse, summary, onRefresh, loading, lastRefresh }) {
  const pulseItems = (pulse?.pulse ?? []).filter(p => ['SPY', 'QQQ', 'IWM', 'VIX'].includes(p.ticker))

  function freshLabel(last) {
    if (!last) return { label: 'IDLE', cls: 'text-md-on-surface-var' }
    const age = (Date.now() - new Date(last).getTime()) / 60000
    if (age < 30)  return { label: 'FRESH', cls: 'text-md-positive' }
    if (age < 120) return { label: 'OK',    cls: 'text-amber-400' }
    return               { label: 'STALE', cls: 'text-md-negative' }
  }

  const sf = freshLabel(status?.scanner?.last_scan)
  const uf = freshLabel(status?.ultra?.last_scan)

  return (
    <Panel className="px-4 py-3">
      <div className="flex items-center gap-4 flex-wrap">
        <MarketClock market={status?.market} />
        <Divider vertical />

        {pulseItems.length > 0 ? (
          <div className="flex items-center gap-4 flex-wrap">
            {pulseItems.map(item => <InlinePulse key={item.ticker} item={item} />)}
          </div>
        ) : (
          <div className="flex items-center gap-4">
            {['SPY', 'QQQ', 'IWM', 'VIX'].map(t => <Skeleton key={t} h="h-7" w="w-16" />)}
          </div>
        )}

        <Divider vertical />

        <div className="flex items-center gap-3 text-[10px] shrink-0">
          <div className="flex flex-col gap-0">
            <span className="text-md-on-surface-var uppercase tracking-wider">T/Z</span>
            <span className={cx('font-bold font-mono', sf.cls)}>{sf.label}</span>
          </div>
          <div className="flex flex-col gap-0">
            <span className="text-md-on-surface-var uppercase tracking-wider">ULTRA</span>
            <span className={cx('font-bold font-mono', uf.cls)}>{uf.label}</span>
          </div>
        </div>

        {summary && (
          <>
            <Divider vertical />
            <div className="flex items-center gap-3 text-[10px] shrink-0">
              {summary.bull_count != null && (
                <div className="flex flex-col gap-0">
                  <span className="text-md-on-surface-var uppercase tracking-wider">Bull</span>
                  <span className="font-bold font-mono text-md-positive">{summary.bull_count}</span>
                </div>
              )}
              {summary.strong_count != null && (
                <div className="flex flex-col gap-0">
                  <span className="text-md-on-surface-var uppercase tracking-wider">Strong</span>
                  <span className="font-bold font-mono text-emerald-400">{summary.strong_count}</span>
                </div>
              )}
              {summary.ultra_top_band != null && (
                <div className="flex flex-col gap-0">
                  <span className="text-md-on-surface-var uppercase tracking-wider">Top Band</span>
                  <span className="font-bold font-mono text-violet-400">{summary.ultra_top_band}</span>
                </div>
              )}
            </div>
          </>
        )}

        <div className="ml-auto flex items-center gap-2 shrink-0">
          {lastRefresh && (
            <span className="text-[10px] text-md-on-surface-var opacity-40 hidden sm:block">
              {lastRefresh.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </span>
          )}
          <button
            onClick={onRefresh}
            disabled={loading}
            className="text-xs px-3 py-1.5 rounded-md-full border border-md-outline text-md-primary hover:bg-md-primary/10 active:bg-md-primary/12 transition-all disabled:opacity-40 whitespace-nowrap"
          >
            {loading ? '…' : '↻ Refresh'}
          </button>
        </div>
      </div>
    </Panel>
  )
}

// ─── AI MARKET BRIEF ─────────────────────────────────────────────────────────

const TONE_CFG = {
  'Strongly Bullish': { cls: 'text-lime-300',    bg: 'bg-lime-900/30 border-lime-700/40' },
  'Mildly Bullish':   { cls: 'text-emerald-400', bg: 'bg-emerald-900/20 border-emerald-700/30' },
  'Neutral':          { cls: 'text-md-on-surface-var', bg: 'bg-md-surface-con border-md-outline-var' },
  'Mildly Bearish':   { cls: 'text-amber-400',   bg: 'bg-amber-900/20 border-amber-700/30' },
  'Strongly Bearish': { cls: 'text-rose-400',    bg: 'bg-rose-900/20 border-rose-700/30' },
}
const CONFIDENCE_CFG = {
  HIGH:   'text-lime-300',
  MEDIUM: 'text-amber-400',
  LOW:    'text-md-on-surface-var',
}

function AIMarketBrief({ brief, loading }) {
  const tone    = brief?.market_tone
  const toneCfg = TONE_CFG[tone] ?? { cls: 'text-md-on-surface-var', bg: 'bg-md-surface-con border-md-outline-var' }

  return (
    <Panel className="p-4">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-xs font-semibold uppercase tracking-widest text-md-on-surface-var">
          AI Market Brief
        </h2>
        {brief && (
          <MiniChip
            label={brief.source === 'claude' ? 'AI' : 'AUTO'}
            color={brief.source === 'claude' ? 'ai' : 'default'}
          />
        )}
      </div>

      {loading && !brief ? (
        <div className="flex flex-col gap-2">
          {[90, 75, 60].map(w => <Skeleton key={w} h="h-3" w={`w-[${w}%]`} />)}
        </div>
      ) : !brief ? (
        <EmptySlate icon="🧠" title="No brief available" sub="Run Ultra Scan to generate analysis" />
      ) : (
        <div className="flex flex-col gap-3">
          {tone && (
            <div className={cx('flex items-center justify-between px-3 py-2 rounded-md-sm border', toneCfg.bg)}>
              <span className={cx('text-sm font-bold', toneCfg.cls)}>{tone}</span>
              {brief.confidence && (
                <span className={cx('text-[10px] font-mono font-semibold uppercase', CONFIDENCE_CFG[brief.confidence] ?? 'text-md-on-surface-var')}>
                  {brief.confidence} confidence
                </span>
              )}
            </div>
          )}
          {brief.focus_summary && (
            <p className="text-xs text-md-on-surface leading-relaxed">{brief.focus_summary}</p>
          )}
          {(brief.what_to_focus_on?.length > 0 || brief.what_to_avoid?.length > 0) && (
            <div className="grid grid-cols-2 gap-3">
              {brief.what_to_focus_on?.length > 0 && (
                <div>
                  <p className="text-[10px] font-semibold text-md-positive uppercase tracking-wider mb-1.5">Focus On</p>
                  <ul className="flex flex-col gap-1">
                    {brief.what_to_focus_on.slice(0, 3).map((pt, i) => (
                      <li key={i} className="flex items-start gap-1.5 text-[11px] text-md-on-surface">
                        <span className="text-md-positive shrink-0 mt-px">›</span>
                        <span className="leading-snug">{pt}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {brief.what_to_avoid?.length > 0 && (
                <div>
                  <p className="text-[10px] font-semibold text-md-negative uppercase tracking-wider mb-1.5">Avoid</p>
                  <ul className="flex flex-col gap-1">
                    {brief.what_to_avoid.slice(0, 3).map((pt, i) => (
                      <li key={i} className="flex items-start gap-1.5 text-[11px] text-md-on-surface">
                        <span className="text-md-negative shrink-0 mt-px">›</span>
                        <span className="leading-snug">{pt}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}
          {brief.hot_sectors?.length > 0 && (
            <div className="flex flex-wrap gap-1">
              {brief.hot_sectors.map(s => <MiniChip key={s} label={s} color="bull" />)}
            </div>
          )}
          <p className="text-[10px] text-md-on-surface-var opacity-40 pt-1 border-t border-md-outline-var">
            From scanner + sector + market data
          </p>
        </div>
      )}
    </Panel>
  )
}

// ─── BEST SETUPS TODAY ────────────────────────────────────────────────────────

const CAT_LABEL = {
  BEST_PULLBACK:                'Pullback Setup',
  BEST_BREAKOUT:                'Breakout Setup',
  BEST_EMA_RECLAIM:             'EMA Reclaim',
  BEST_ABR_B_PLUS:              'ABR B+ Setup',
  BEST_LOW_VOLUME_ACCUMULATION: 'Low Vol. Accum.',
  BEST_SECTOR_LEADER:           'Sector Leader',
  BEST_FRESH_SIGNAL:            'Fresh Signal',
  BEST_RISK_REWARD:             'Best R/R',
  AVOID_TOO_LATE:               'Too Late',
}

function PrimarySetupCard({ setup, ctx, onOpenChart, onAddWL, onOpenNews }) {
  const [open, setOpen] = useState(false)
  const ticker = setup.ticker || setup.symbol
  const border = BUCKET_BORDER[setup.action_bucket] ?? 'border-l-slate-500'
  const events = ctx?.events ?? []

  return (
    <Panel className={cx('border-l-4 p-4', border)}>
      {/* Top row */}
      <div className="flex items-start justify-between gap-3 mb-2">
        <div className="flex flex-col gap-1">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-lg font-bold text-md-on-surface">{ticker}</span>
            {setup.band && <BandChip band={setup.band} />}
            {setup.source === 'claude' && <MiniChip label="AI" color="ai" />}
          </div>
          {/* Sector / category row */}
          <div className="flex items-center gap-1.5 flex-wrap">
            <span className="text-xs text-md-on-surface-var">
              {CAT_LABEL[setup.category] ?? setup.category ?? 'Setup'}
            </span>
            {(ctx?.sector || ctx?.theme) && (
              <>
                <span className="text-md-on-surface-var opacity-30">·</span>
                <SectorChip sector={ctx.sector} theme={ctx.theme} />
              </>
            )}
            {ctx?.industry && !ctx?.theme && (
              <span className="text-[10px] text-md-on-surface-var opacity-60 truncate max-w-[160px]">
                {ctx.industry}
              </span>
            )}
          </div>
        </div>
        <div className="flex flex-col items-end gap-1.5 shrink-0">
          <ActionChip bucket={setup.action_bucket} size="md" />
          <span className="text-[10px] font-mono text-md-on-surface-var">
            {setup.confidence}/10 confidence
          </span>
        </div>
      </div>

      {/* Scores + signal chips */}
      {(setup.ultra_score != null || setup.setup_quality != null) && (
        <div className="flex items-center gap-4 mb-2.5 text-xs flex-wrap">
          {setup.ultra_score != null && (
            <div className="flex items-center gap-1.5">
              <span className="text-md-on-surface-var">Ultra</span>
              <span className={cx('font-mono text-sm', scoreColor(setup.ultra_score))}>
                {Math.round(setup.ultra_score)}
              </span>
            </div>
          )}
          {setup.setup_quality != null && (
            <div className="flex items-center gap-1.5">
              <span className="text-md-on-surface-var">Quality</span>
              <span className={cx('font-mono text-sm', scoreColor(setup.setup_quality))}>
                {Math.round(setup.setup_quality)}
              </span>
            </div>
          )}
          {setup.signal    && <MiniChip label={setup.signal} color="info" />}
          {setup.abr       && <MiniChip label={`ABR ${setup.abr}`} color={setup.abr === 'B+' || setup.abr === 'A' ? 'bull' : 'default'} />}
          {setup.ema_state && <MiniChip label={setup.ema_state} color={setup.ema_state?.includes('Reclaim') ? 'bull' : 'default'} />}
        </div>
      )}

      {/* Event badges */}
      {events.length > 0 && (
        <div className="flex items-center gap-1.5 mb-2.5 flex-wrap">
          {events.slice(0, 3).map((e, i) => <EventBadge key={i} event={e} />)}
          {events.length > 3 && (
            <MiniChip label={`+${events.length - 3} events`} />
          )}
        </div>
      )}

      {/* Why selected */}
      {(setup.why_selected?.length > 0 || setup.reason) && (
        <div className="mb-3">
          <p className="text-[10px] font-semibold text-md-on-surface-var uppercase tracking-wider mb-1.5">
            Why Selected
          </p>
          {setup.why_selected?.length > 0 ? (
            <ul className="flex flex-col gap-1">
              {setup.why_selected.map((w, i) => (
                <li key={i} className="flex items-start gap-1.5 text-xs text-md-on-surface">
                  <span className="text-md-positive shrink-0 mt-px">›</span>
                  <span className="leading-snug">{w}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-md-on-surface">{setup.reason}</p>
          )}
        </div>
      )}

      {/* Historical edge */}
      {setup.historical_evidence && Object.keys(setup.historical_evidence).length > 0 && (
        <div className="flex items-center gap-4 text-xs mb-3 px-3 py-2 rounded-md-sm bg-md-surface-con border border-md-outline-var">
          <span className="text-amber-400 font-semibold text-[10px] uppercase tracking-wider shrink-0">Edge</span>
          {setup.historical_evidence.median_10d != null && (
            <span className={setup.historical_evidence.median_10d >= 0 ? 'text-md-positive font-mono' : 'text-md-negative font-mono'}>
              10D: {setup.historical_evidence.median_10d >= 0 ? '+' : ''}{setup.historical_evidence.median_10d?.toFixed(1)}%
            </span>
          )}
          {setup.historical_evidence.hit_10pct != null && (
            <span className="text-md-positive font-mono">Hit: {setup.historical_evidence.hit_10pct?.toFixed(1)}%</span>
          )}
          {setup.historical_evidence.sample != null && (
            <span className="text-md-on-surface-var font-mono">n={setup.historical_evidence.sample}</span>
          )}
        </div>
      )}

      {/* Expandable: risk + watch next */}
      {open && (
        <div className="border-t border-md-outline-var pt-3 mt-1 flex flex-col gap-3">
          {(setup.risk_flags?.length > 0 || setup.risk) && (
            <div>
              <p className="text-[10px] font-semibold text-md-negative uppercase tracking-wider mb-1.5">Risk</p>
              <ul className="flex flex-col gap-1">
                {(setup.risk_flags ?? [setup.risk]).filter(Boolean).map((r, i) => (
                  <li key={i} className="flex items-start gap-1.5 text-xs text-md-on-surface">
                    <span className="text-md-negative shrink-0 mt-px">›</span>
                    <span>{r}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
          {setup.what_to_watch_next?.length > 0 && (
            <div>
              <p className="text-[10px] font-semibold text-sky-400 uppercase tracking-wider mb-1.5">Watch Next</p>
              <ul className="flex flex-col gap-1">
                {setup.what_to_watch_next.map((w, i) => (
                  <li key={i} className="flex items-start gap-1.5 text-xs text-md-on-surface">
                    <span className="text-sky-400 shrink-0 mt-px">›</span>
                    <span>{w}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}

      {/* Actions */}
      <div className="flex items-center gap-2 mt-3 pt-3 border-t border-md-outline-var flex-wrap">
        <button
          onClick={() => setOpen(e => !e)}
          className="text-xs px-3 py-1 rounded-md-full border border-md-outline text-md-on-surface-var hover:bg-md-primary/10 hover:text-md-primary transition-all"
        >
          {open ? 'Less ↑' : 'Details ↓'}
        </button>
        {onOpenChart && (
          <button
            onClick={() => onOpenChart(ticker)}
            className="text-xs px-3 py-1 rounded-md-full border border-md-outline text-md-on-surface-var hover:bg-md-primary/10 hover:text-md-primary transition-all"
          >
            Chart ↗
          </button>
        )}
        {onAddWL && (
          <button
            onClick={() => onAddWL(ticker)}
            className="text-xs px-3 py-1 rounded-md-full border border-md-outline text-md-on-surface-var hover:bg-md-primary/10 hover:text-md-primary transition-all"
          >
            + Watch
          </button>
        )}
        {onOpenNews && (
          <button
            onClick={() => onOpenNews(ticker)}
            className="text-xs px-3 py-1 rounded-md-full border border-md-outline text-md-on-surface-var hover:bg-violet-500/10 hover:text-violet-400 hover:border-violet-500/30 transition-all"
          >
            News AI
          </button>
        )}
      </div>
    </Panel>
  )
}

function SecondarySetupCard({ setup, ctx, onOpenChart, onAddWL, onOpenNews }) {
  const ticker = setup.ticker || setup.symbol
  const border = BUCKET_BORDER[setup.action_bucket] ?? 'border-l-slate-500'
  const events = ctx?.events ?? []

  return (
    <Panel className={cx('border-l-2 px-3 py-2.5', border)}>
      <div className="flex items-start justify-between gap-2 mb-1.5">
        <div className="flex flex-col gap-0.5 min-w-0">
          <div className="flex items-center gap-1.5 flex-wrap">
            <span className="text-sm font-bold text-md-on-surface">{ticker}</span>
            {setup.band && <BandChip band={setup.band} />}
          </div>
          {(ctx?.sector || ctx?.theme) && (
            <SectorChip sector={ctx.sector} theme={ctx.theme} />
          )}
        </div>
        <ActionChip bucket={setup.action_bucket} />
      </div>

      {/* Score + signals */}
      <div className="flex items-center gap-2 mb-1.5 flex-wrap">
        {setup.ultra_score != null && (
          <span className={cx('text-xs font-mono font-semibold', scoreColor(setup.ultra_score))}>
            {Math.round(setup.ultra_score)}
          </span>
        )}
        {setup.signal && <MiniChip label={setup.signal} color="info" />}
        {setup.abr    && <MiniChip label={`ABR ${setup.abr}`} color={setup.abr === 'B+' || setup.abr === 'A' ? 'bull' : 'default'} />}
      </div>

      {/* Event badges */}
      {events.length > 0 && (
        <div className="flex items-center gap-1 mb-1.5 flex-wrap">
          {events.slice(0, 2).map((e, i) => <EventBadge key={i} event={e} />)}
          {events.length > 2 && <MiniChip label={`+${events.length - 2}`} />}
        </div>
      )}

      {/* Top reason */}
      {(setup.why_selected?.[0] || setup.reason) && (
        <p className="text-[10px] text-md-on-surface-var leading-snug line-clamp-2">
          {setup.why_selected?.[0] ?? setup.reason}
        </p>
      )}

      <div className="flex items-center gap-1.5 mt-2 flex-wrap">
        {onOpenChart && (
          <button
            onClick={() => onOpenChart(ticker)}
            className="text-[10px] px-2 py-0.5 rounded-md-sm border border-md-outline-var text-md-on-surface-var hover:text-md-primary hover:border-md-primary/40 transition-all"
          >
            Chart ↗
          </button>
        )}
        {onAddWL && (
          <button
            onClick={() => onAddWL(ticker)}
            className="text-[10px] px-2 py-0.5 rounded-md-sm border border-md-outline-var text-md-on-surface-var hover:text-md-primary hover:border-md-primary/40 transition-all"
          >
            +WL
          </button>
        )}
        {onOpenNews && (
          <button
            onClick={() => onOpenNews(ticker)}
            className="text-[10px] px-2 py-0.5 rounded-md-sm border border-md-outline-var text-md-on-surface-var hover:text-violet-400 hover:border-violet-500/30 transition-all"
          >
            News
          </button>
        )}
      </div>
    </Panel>
  )
}

function BestSetupsToday({ setups, loading, ctxMap, onOpenChart, onAddWL, onOpenNews }) {
  const list       = setups?.setups ?? []
  const primary    = list[0]
  const secondary  = list.slice(1)
  const aiCurated  = list.some(s => s.source === 'claude')

  return (
    <div>
      <SectionHead
        title="Best Setups Today"
        count={list.length || null}
        right={aiCurated && <MiniChip label="AI Curated" color="ai" />}
      />
      {loading && !list.length ? (
        <div className="flex flex-col gap-2">
          <Skeleton h="h-44" />
          <div className="grid grid-cols-2 gap-2">
            <Skeleton h="h-24" />
            <Skeleton h="h-24" />
          </div>
        </div>
      ) : list.length === 0 ? (
        <Panel className="p-4">
          <EmptySlate icon="📋" title="No valid setups found." sub="Run an Ultra Scan to populate best setups." />
        </Panel>
      ) : (
        <div className="flex flex-col gap-2">
          <PrimarySetupCard
            setup={primary}
            ctx={ctxMap?.[primary.ticker || primary.symbol]}
            onOpenChart={onOpenChart}
            onAddWL={onAddWL}
            onOpenNews={onOpenNews}
          />
          {secondary.length > 0 && (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              {secondary.map(s => (
                <SecondarySetupCard
                  key={s.ticker || s.symbol}
                  setup={s}
                  ctx={ctxMap?.[s.ticker || s.symbol]}
                  onOpenChart={onOpenChart}
                  onAddWL={onAddWL}
                  onOpenNews={onOpenNews}
                />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ─── TOP CANDIDATES ───────────────────────────────────────────────────────────

const FILTER_OPTS = ['All', 'High Score', 'ABR B+', 'EMA OK', 'Fresh']
const SORT_OPTS   = ['Ultra Score', '% Today', 'Bull Score']

function CandidateRow({ card, rank, ctx, onOpenChart, onAddWL, onOpenNews }) {
  const up     = (card.change_pct ?? 0) >= 0
  const bucket = card.action_bucket ?? 'WATCH_CLOSELY'
  const border = BUCKET_BORDER[bucket] ?? 'border-l-amber-500'
  const events = ctx?.events ?? []

  return (
    <div className={cx(
      'flex items-center gap-3 px-3 py-2 border-l-2 bg-md-surface-high rounded-r-md-sm',
      'hover:bg-md-surface-con transition-colors group cursor-default',
      border
    )}>
      {/* Rank */}
      <span className="text-[10px] font-mono text-md-on-surface-var w-4 text-right shrink-0">
        {rank}
      </span>

      {/* Ticker + band + sector */}
      <div className="flex items-center gap-1.5 min-w-[110px]">
        <span className="text-sm font-bold text-md-on-surface">{card.ticker}</span>
        {card.band && <BandChip band={card.band} />}
      </div>

      {/* Sector chip — visible md+ */}
      <div className="hidden md:flex items-center shrink-0 min-w-[56px]">
        {ctx?.sector || ctx?.theme
          ? <SectorChip sector={ctx.sector} theme={ctx.theme} />
          : null
        }
      </div>

      {/* % change */}
      <span className={cx(
        'text-xs font-bold font-mono tabular-nums shrink-0 w-[58px] text-right',
        up ? 'text-md-positive' : 'text-md-negative'
      )}>
        {up ? '+' : ''}{card.change_pct?.toFixed(2) ?? '—'}%
      </span>

      {/* Ultra score */}
      <div className="flex items-center gap-1 shrink-0">
        <span className="text-[10px] text-md-on-surface-var">U</span>
        <span className={cx('text-sm font-mono font-bold', scoreColor(card.ultra_score ?? 0))}>
          {(card.ultra_score ?? 0).toFixed(0)}
        </span>
      </div>

      {/* Action chip */}
      <ActionChip bucket={bucket} />

      {/* Signal chips + event badges — wider screens */}
      <div className="hidden lg:flex items-center gap-1.5 flex-1 min-w-0 overflow-hidden">
        {card.abr && (
          <MiniChip label={`ABR ${card.abr}`} color={card.abr === 'B+' || card.abr === 'A' ? 'bull' : 'default'} />
        )}
        {card.ema_ok && <MiniChip label="EMA ✓" color="bull" />}
        {events.slice(0, 1).map((e, i) => <EventBadge key={i} event={e} />)}
      </div>

      {/* Hover actions */}
      <div className="flex items-center gap-1 ml-auto opacity-0 group-hover:opacity-100 transition-opacity shrink-0">
        {onOpenChart && (
          <button
            onClick={() => onOpenChart(card.ticker)}
            className="text-[10px] px-2 py-0.5 rounded-md-sm border border-md-outline-var text-md-on-surface-var hover:text-md-primary hover:border-md-primary/40 transition-all"
          >
            Chart ↗
          </button>
        )}
        {onOpenNews && (
          <button
            onClick={() => onOpenNews(card.ticker)}
            className="text-[10px] px-2 py-0.5 rounded-md-sm border border-md-outline-var text-md-on-surface-var hover:text-violet-400 hover:border-violet-500/30 transition-all"
          >
            News
          </button>
        )}
        {onAddWL && (
          <button
            onClick={() => onAddWL(card.ticker)}
            className="text-[10px] px-2 py-0.5 rounded-md-sm border border-md-outline-var text-md-on-surface-var hover:text-md-primary hover:border-md-primary/40 transition-all"
          >
            +WL
          </button>
        )}
      </div>
    </div>
  )
}

function TopCandidatesPanel({ candidates, loading, ctxMap, onOpenChart, onAddWL, onOpenNews }) {
  const [limit,  setLimit]  = useState(20)
  const [filter, setFilter] = useState('All')
  const [sort,   setSort]   = useState('Ultra Score')

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
    if (sort === '% Today')     return (b.change_pct ?? 0)   - (a.change_pct ?? 0)
    if (sort === 'Bull Score')  return (b.bull_score ?? 0)   - (a.bull_score ?? 0)
    return 0
  })

  return (
    <div>
      <SectionHead title="Top Candidates" count={filtered.length || null} />

      <div className="flex items-center gap-2 flex-wrap mb-3">
        <div className="flex rounded-md-sm overflow-hidden border border-md-outline-var">
          {[10, 20, 50].map((n, i) => (
            <button
              key={n}
              onClick={() => setLimit(n)}
              className={cx(
                'text-xs px-3 py-1.5 transition-colors',
                i > 0 && 'border-l border-md-outline-var',
                limit === n
                  ? 'bg-md-primary-container text-md-on-primary-container'
                  : 'text-md-on-surface-var hover:bg-white/5'
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
                'text-[11px] px-2.5 py-1 rounded-md-sm border transition-colors',
                filter === f
                  ? 'border-md-primary/50 bg-md-primary/15 text-md-primary'
                  : 'border-md-outline-var text-md-on-surface-var hover:border-md-outline hover:text-md-on-surface'
              )}
            >
              {f}
            </button>
          ))}
        </div>

        <select
          value={sort}
          onChange={e => setSort(e.target.value)}
          className="ml-auto text-[11px] px-2 py-1 rounded-md-sm bg-md-surface-high border border-md-outline-var text-md-on-surface-var"
        >
          {SORT_OPTS.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      {loading && !cards.length ? (
        <div className="flex flex-col gap-1">
          {[1, 2, 3, 4, 5].map(i => <Skeleton key={i} h="h-9" />)}
        </div>
      ) : sorted.length === 0 ? (
        <Panel className="p-4">
          <EmptySlate icon="🔍" title="No candidates match this filter." sub="Try a different filter or run Ultra Scan." />
        </Panel>
      ) : (
        <div className="flex flex-col gap-1">
          {sorted.slice(0, limit).map((card, i) => (
            <CandidateRow
              key={card.ticker}
              card={card}
              rank={i + 1}
              ctx={ctxMap?.[card.ticker]}
              onOpenChart={onOpenChart}
              onAddWL={onAddWL}
              onOpenNews={onOpenNews}
            />
          ))}
        </div>
      )}
    </div>
  )
}

// ─── MARKET NEWS ──────────────────────────────────────────────────────────────

const NEWS_COLORS = {
  Market: 'info', Macro: 'info', Earnings: 'warn', FDA: 'ai',
  Biotech: 'ai', Sector: 'default', Risk: 'default', AI: 'ai',
  Analyst: 'default', Offering: 'default', Merger: 'bull',
}

function NewsItem({ item }) {
  return (
    <div className="flex flex-col gap-1 py-2 border-b border-md-outline-var last:border-0">
      <p className="text-xs text-md-on-surface leading-snug">{item.headline || item.title}</p>
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5 flex-wrap">
          {item.category && <MiniChip label={item.category} color={NEWS_COLORS[item.category] ?? 'default'} />}
          {item.ticker   && <span className="text-[10px] font-mono text-md-primary">{item.ticker}</span>}
        </div>
        <span className="text-[10px] text-md-on-surface-var shrink-0 tabular-nums">
          {relTime(item.published_at || item.time)}
        </span>
      </div>
    </div>
  )
}

function MarketNewsPanel({ news, loading }) {
  const items = news?.items ?? []
  return (
    <Panel className="p-4">
      <SectionHead title="Market News" />
      {loading && !items.length ? (
        <div className="flex flex-col gap-2">
          {[1, 2, 3].map(i => <Skeleton key={i} h="h-10" />)}
        </div>
      ) : items.length === 0 ? (
        <EmptySlate icon="📰" title="No market news found." sub={news?.message ?? 'Connect a news API to enable.'} />
      ) : (
        <div>
          {items.slice(0, 7).map((item, i) => <NewsItem key={i} item={item} />)}
        </div>
      )}
    </Panel>
  )
}

// ─── RISK ALERTS ──────────────────────────────────────────────────────────────

const ALERT_CFG = {
  critical: { left: 'border-l-rose-400',  bg: 'bg-rose-950/40',  icon: '✕', tag: 'CRIT', tagCls: 'bg-rose-900/60 text-rose-300 border-rose-700/50' },
  high:     { left: 'border-l-rose-500',  bg: 'bg-rose-950/30',  icon: '⚠', tag: 'HIGH', tagCls: 'bg-rose-900/50 text-rose-300 border-rose-700/40' },
  medium:   { left: 'border-l-amber-500', bg: 'bg-amber-950/30', icon: '●', tag: 'MED',  tagCls: 'bg-amber-900/50 text-amber-300 border-amber-700/40' },
  low:      { left: 'border-l-slate-500', bg: 'bg-slate-900/20', icon: '○', tag: 'LOW',  tagCls: 'bg-slate-800/60 text-slate-400 border-slate-600/40' },
}

function RiskAlertsPanel({ alerts, loading }) {
  const list     = alerts?.alerts ?? []
  const allClear = list.length === 0 || (list.length === 1 && list[0].type === 'ok')

  return (
    <Panel className="p-4">
      <SectionHead title="Risk Alerts" />
      {loading && !list.length ? (
        <div className="flex flex-col gap-1.5">
          {[1, 2].map(i => <Skeleton key={i} h="h-8" />)}
        </div>
      ) : allClear ? (
        <div className="flex items-center gap-2 text-xs text-md-positive">
          <span>✓</span>
          <span>No critical risk alerts.</span>
        </div>
      ) : (
        <div className="flex flex-col gap-1">
          {list.map((a, i) => {
            const cfg = ALERT_CFG[a.severity] ?? ALERT_CFG.low
            return (
              <div key={i} className={cx(
                'flex items-center gap-2.5 px-2.5 py-2 border-l-2 rounded-r-md-sm',
                cfg.left, cfg.bg
              )}>
                <span className="text-xs shrink-0">{cfg.icon}</span>
                <p className="text-xs text-md-on-surface flex-1 leading-snug">{a.message}</p>
                <span className={cx(
                  'text-[10px] font-bold font-mono px-1.5 py-px rounded-md-sm border leading-none shrink-0',
                  cfg.tagCls
                )}>
                  {cfg.tag}
                </span>
              </div>
            )
          })}
        </div>
      )}
    </Panel>
  )
}

// ─── DATA HEALTH ──────────────────────────────────────────────────────────────

function DataHealthCard({ scanner, ultra, loading }) {
  function health(s) {
    if (!s)           return { label: 'UNAVAIL', cls: 'text-md-on-surface-var' }
    if (s.running)    return { label: 'RUNNING', cls: 'text-sky-400' }
    if (!s.last_scan) return { label: 'IDLE',    cls: 'text-md-on-surface-var' }
    const age = (Date.now() - new Date(s.last_scan).getTime()) / 60000
    if (age < 30)  return { label: 'HEALTHY', cls: 'text-md-positive' }
    if (age < 120) return { label: 'OK',      cls: 'text-amber-400' }
    return             { label: 'STALE',   cls: 'text-md-negative' }
  }
  function fmt(iso) {
    if (!iso) return 'Never'
    try { return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) }
    catch { return iso }
  }
  const sh = health(scanner)
  const uh = health(ultra)

  return (
    <Panel className="p-4">
      <SectionHead title="Data Health" />
      {loading ? <Skeleton h="h-12" /> : (
        <div className="grid grid-cols-2 gap-3 text-[10px]">
          <div className="flex flex-col gap-1">
            <span className="text-md-on-surface-var uppercase tracking-wider">T/Z Scanner</span>
            <span className={cx('text-sm font-bold font-mono', sh.cls)}>{sh.label}</span>
            <span className="text-md-on-surface-var">Last: {fmt(scanner?.last_scan)}</span>
            {scanner?.running && (
              <div className="h-0.5 rounded-full bg-md-outline-var overflow-hidden mt-1">
                <div className="h-full bg-md-primary animate-pulse"
                  style={{ width: `${scanner.total > 0 ? Math.round(scanner.done / scanner.total * 100) : 30}%` }} />
              </div>
            )}
          </div>
          <div className="flex flex-col gap-1">
            <span className="text-md-on-surface-var uppercase tracking-wider">ULTRA</span>
            <span className={cx('text-sm font-bold font-mono', uh.cls)}>{uh.label}</span>
            <span className="text-md-on-surface-var">Last: {fmt(ultra?.last_scan)}</span>
            {ultra?.running && (
              <div className="h-0.5 rounded-full bg-md-outline-var overflow-hidden mt-1">
                <div className="h-full bg-violet-500 animate-pulse"
                  style={{ width: `${ultra.total > 0 ? Math.round(ultra.done / ultra.total * 100) : 30}%` }} />
              </div>
            )}
          </div>
        </div>
      )}
    </Panel>
  )
}

// ─── SECTOR STRENGTH ─────────────────────────────────────────────────────────

const SECTOR_NAMES = {
  XLK:'Technology', XLV:'Health Care', XLF:'Financials', XLY:'Cons. Disc.',
  XLP:'Cons. Staples', XLE:'Energy', XLI:'Industrials', XLB:'Materials',
  XLRE:'Real Estate', XLU:'Utilities', XLC:'Comm. Svcs.',
  IBB:'Biotech', XBI:'Biotech ETF', SMH:'Semis', GDX:'Gold Miners',
}

function SectorRow({ sector, rank, maxHot }) {
  const hot  = sector.trend === 'hot'
  const cold = sector.trend === 'cold'
  const up   = sector.return_1d >= 0
  const norm = Math.min(100, Math.max(0, ((sector.hotness ?? 0) / (maxHot || 1) + 1) * 50))
  const barColor = hot ? 'bg-md-positive' : cold ? 'bg-md-negative' : 'bg-slate-600'

  return (
    <div className="flex items-center gap-3 py-1.5 border-b border-md-outline-var last:border-0">
      <span className="text-[10px] text-md-on-surface-var font-mono w-4 text-right shrink-0">{rank}</span>
      <div className="flex items-center gap-1.5 w-[130px] shrink-0">
        <span className="text-xs font-semibold text-md-on-surface truncate">
          {SECTOR_NAMES[sector.etf] ?? sector.name ?? sector.etf}
        </span>
        <span className="text-[10px] text-md-on-surface-var font-mono shrink-0">{sector.etf}</span>
      </div>
      <div className="flex-1 h-1.5 rounded-full bg-md-surface-con overflow-hidden">
        <div className={cx('h-full rounded-full transition-all', barColor)} style={{ width: `${norm}%` }} />
      </div>
      <span className={cx('text-xs font-bold font-mono tabular-nums w-14 text-right shrink-0', up ? 'text-md-positive' : 'text-md-negative')}>
        {up ? '+' : ''}{sector.return_1d?.toFixed(2)}%
      </span>
      {hot  && <MiniChip label="HOT"  color="bull" />}
      {cold && <MiniChip label="COLD" color="default" />}
      {!hot && !cold && <div className="w-[38px] shrink-0" />}
    </div>
  )
}

function SectorStrengthPanel({ sectors, loading }) {
  const list   = sectors?.sectors ?? []
  const maxHot = Math.max(...list.map(s => Math.abs(s.hotness ?? 0)), 1)
  const sorted = [...list].sort((a, b) => (b.hotness ?? 0) - (a.hotness ?? 0))

  return (
    <div>
      <SectionHead title="Sector Strength" count={list.length || null} />
      {loading && !list.length ? (
        <Panel className="p-4">
          <div className="flex flex-col gap-2">
            {[1, 2, 3, 4, 5].map(i => <Skeleton key={i} h="h-6" />)}
          </div>
        </Panel>
      ) : list.length === 0 ? (
        <Panel className="p-4">
          <EmptySlate icon="🌐" title="No sector data." sub="Visit the Sectors tab to load sector data." />
        </Panel>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-6">
          <Panel className="p-3">
            {sorted.slice(0, Math.ceil(sorted.length / 2)).map((s, i) => (
              <SectorRow key={s.etf} sector={s} rank={i + 1} maxHot={maxHot} />
            ))}
          </Panel>
          <Panel className="p-3">
            {sorted.slice(Math.ceil(sorted.length / 2)).map((s, i) => (
              <SectorRow key={s.etf} sector={s} rank={Math.ceil(sorted.length / 2) + i + 1} maxHot={maxHot} />
            ))}
          </Panel>
        </div>
      )}
    </div>
  )
}

// ─── FRESH SIGNALS FEED ───────────────────────────────────────────────────────

function FreshSignalRow({ sig }) {
  const up = (sig.change_pct ?? 0) >= 0
  return (
    <div className="flex items-center gap-3 py-1.5 border-b border-md-outline-var last:border-0">
      <span className="text-xs font-bold text-md-on-surface w-14 shrink-0">{sig.ticker}</span>
      <div className="flex items-center gap-0.5 flex-1 min-w-0">
        {Array.from({ length: Math.min(sig.bull_score ?? 0, 10) }).map((_, i) => (
          <div key={i} className="w-1.5 h-3 rounded-sm bg-md-positive opacity-60 shrink-0" />
        ))}
        {sig.sig_name && (
          <span className="text-[10px] text-md-on-surface-var font-mono ml-1.5 truncate">{sig.sig_name}</span>
        )}
      </div>
      <span className={cx('text-xs font-bold shrink-0 tabular-nums', up ? 'text-md-positive' : 'text-md-negative')}>
        {up ? '+' : ''}{sig.change_pct?.toFixed(2) ?? '—'}%
      </span>
      <span className="text-[10px] text-md-on-surface-var shrink-0 w-8 text-right">{relTime(sig.scanned_at)}</span>
    </div>
  )
}

function FreshSignalsFeed({ signals, loading }) {
  const list = signals?.signals ?? []
  return (
    <div>
      <SectionHead title="Fresh Signals" count={list.length || null} />
      {loading && !list.length ? (
        <Panel className="p-4">
          <div className="flex flex-col gap-2">
            {[1, 2, 3].map(i => <Skeleton key={i} h="h-6" />)}
          </div>
        </Panel>
      ) : list.length === 0 ? (
        <Panel className="p-4">
          <EmptySlate icon="📡" title="No fresh signals yet." sub="Run T/Z scanner or wait for auto-refresh." />
        </Panel>
      ) : (
        <Panel className="p-3">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6">
            {list.map(sig => <FreshSignalRow key={sig.ticker} sig={sig} />)}
          </div>
        </Panel>
      )}
    </div>
  )
}

// ─── WATCHLIST SNAPSHOT ───────────────────────────────────────────────────────

const WL_STATUS = {
  improving:   { label: 'Improving',   cls: 'text-md-positive' },
  valid:       { label: 'Valid',        cls: 'text-teal-400' },
  weakening:   { label: 'Weakening',   cls: 'text-amber-400' },
  invalidated: { label: 'Invalidated', cls: 'text-md-negative' },
  review:      { label: 'Review',      cls: 'text-md-on-surface-var' },
}

function WatchlistSnapshot({ watchlistData, loading }) {
  const items = watchlistData ?? []
  return (
    <div>
      <SectionHead title="Watchlist" count={items.length || null} />
      {loading ? (
        <Panel className="p-4">
          <div className="flex flex-col gap-2">
            {[1, 2, 3].map(i => <Skeleton key={i} h="h-7" />)}
          </div>
        </Panel>
      ) : items.length === 0 ? (
        <Panel className="p-4">
          <EmptySlate icon="⭐" title="Watchlist is empty." sub="Add tickers from Top Candidates using +WL." />
        </Panel>
      ) : (
        <Panel className="p-3">
          <div className="divide-y divide-md-outline-var">
            {items.map(item => {
              const s  = WL_STATUS[item.status] ?? WL_STATUS.review
              const up = (item.change_pct ?? 0) >= 0
              return (
                <div key={item.ticker} className="flex items-center gap-3 py-2">
                  <span className="text-xs font-bold text-md-on-surface w-14 shrink-0">{item.ticker}</span>
                  <span className={cx('text-[11px] shrink-0 w-20', s.cls)}>{s.label}</span>
                  <div className="flex items-center gap-1.5 flex-1 min-w-0">
                    {item.sig_name && <MiniChip label={item.sig_name} color="info" />}
                  </div>
                  <ActionChip bucket={item.action_bucket} />
                  <span className={cx(
                    'text-xs font-bold tabular-nums shrink-0 w-14 text-right',
                    up ? 'text-md-positive' : 'text-md-negative'
                  )}>
                    {item.change_pct != null
                      ? `${up ? '+' : ''}${item.change_pct.toFixed(2)}%`
                      : '—'}
                  </span>
                </div>
              )
            })}
          </div>
        </Panel>
      )}
    </div>
  )
}

// ─── MAIN PANEL ───────────────────────────────────────────────────────────────

export default function TradingDashboardPanel({
  onSelectTicker,
  onAddToWatchlist,
  watchlistTickers = [],
  onOpenChart: propOpenChart,
}) {
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

  const [ctxMap,      setCtxMap]      = useState({})
  const [newsDrawer,  setNewsDrawer]  = useState(null)
  const [loading,     setLoading]     = useState(true)
  const [lastRefresh, setLastRefresh] = useState(null)
  const abortRef = useRef(null)

  const loadAll = useCallback(async (silent = false) => {
    if (abortRef.current) abortRef.current.abort()
    abortRef.current = new AbortController()
    if (!silent) setLoading(true)

    try {
      const [statusData, pulseData, summaryData] = await Promise.all([
        apiFetch('/status'),
        apiFetch('/pulse'),
        apiFetch('/summary'),
      ])
      setStatus(statusData)
      setPulse(pulseData)
      setSummary(summaryData)

      const w2 = await Promise.allSettled([
        apiFetch('/top50?limit=50'),
        apiFetch('/sector-heat'),
        apiFetch('/fresh-signals?limit=30'),
        apiFetch('/risk-alerts'),
        apiFetch('/news'),
      ])
      if (w2[0].status === 'fulfilled') setCandidates(w2[0].value)
      if (w2[1].status === 'fulfilled') setSectors(w2[1].value)
      if (w2[2].status === 'fulfilled') setFresh(w2[2].value)
      if (w2[3].status === 'fulfilled') setRisk(w2[3].value)
      if (w2[4].status === 'fulfilled') setNews(w2[4].value)

      if (watchlistTickers.length > 0) {
        try {
          const wlRaw = await watchlistApiFetch(watchlistTickers)
          const items = wlRaw.map(item => {
            const score = item.bull_score ?? 0
            return {
              ...item,
              status:        score >= 6 ? 'improving' : score >= 4 ? 'valid' : score >= 2 ? 'weakening' : 'review',
              action_bucket: score >= 7 ? 'BUY_READY' : score >= 5 ? 'WATCH_CLOSELY' : score >= 3 ? 'WAIT_CONFIRMATION' : 'AVOID',
            }
          })
          setWatchlist(items)
        } catch (_) {}
      }

      const w3 = await Promise.allSettled([
        apiFetch('/best-setups'),
        apiFetch('/ai-brief'),
      ])
      if (w3[0].status === 'fulfilled') setSetups(w3[0].value)
      if (w3[1].status === 'fulfilled') setBrief(w3[1].value)

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

  // Batch-fetch ticker context (sector/industry/events) after candidates and setups load
  useEffect(() => {
    const tickers = new Set([
      ...(candidates?.cards?.slice(0, 30).map(c => c.ticker) ?? []),
      ...(setups?.setups?.map(s => s.ticker || s.symbol) ?? []),
    ])
    if (!tickers.size) return
    const missing = [...tickers].filter(t => t && !ctxMap[t])
    if (!missing.length) return

    Promise.allSettled(
      missing.map(t =>
        fetch(`/api/dashboard/ticker-context/${t}`).then(r => r.ok ? r.json() : null)
      )
    ).then(results => {
      const updates = {}
      missing.forEach((t, i) => {
        if (results[i].status === 'fulfilled' && results[i].value) {
          updates[t] = results[i].value
        }
      })
      if (Object.keys(updates).length) {
        setCtxMap(prev => ({ ...prev, ...updates }))
      }
    }).catch(() => {})
  }, [candidates, setups]) // eslint-disable-line react-hooks/exhaustive-deps

  const openChart = useCallback(t => {
    if (propOpenChart)  propOpenChart(t)
    else if (onSelectTicker) onSelectTicker(t)
  }, [propOpenChart, onSelectTicker])

  const addToWL  = useCallback(t => { if (onAddToWatchlist) onAddToWatchlist(t) }, [onAddToWatchlist])
  const openNews = useCallback(t => setNewsDrawer(t), [])

  return (
    <div className="flex flex-col gap-5 pb-8">

      {/* News Drawer */}
      {newsDrawer && <NewsDrawer ticker={newsDrawer} onClose={() => setNewsDrawer(null)} />}

      {/* ── MARKET COMMAND STRIP ─────────────────────────────────────────── */}
      <CommandStrip
        status={status}
        pulse={pulse}
        summary={summary}
        onRefresh={() => loadAll(false)}
        loading={loading}
        lastRefresh={lastRefresh}
      />

      {/* ── MAIN 2-COLUMN GRID ───────────────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 xl:grid-cols-4 gap-5">

        {/* Left: Best Setups + Top Candidates */}
        <div className="lg:col-span-2 xl:col-span-3 flex flex-col gap-6">
          <BestSetupsToday
            setups={setups}
            loading={loading}
            ctxMap={ctxMap}
            onOpenChart={openChart}
            onAddWL={addToWL}
            onOpenNews={openNews}
          />
          <TopCandidatesPanel
            candidates={candidates}
            loading={loading}
            ctxMap={ctxMap}
            onOpenChart={openChart}
            onAddWL={addToWL}
            onOpenNews={openNews}
          />
        </div>

        {/* Right Intelligence Column */}
        <div className="flex flex-col gap-4">
          <AIMarketBrief brief={brief} loading={loading} />
          <MarketNewsPanel news={news} loading={loading} />
          <RiskAlertsPanel alerts={risk} loading={loading} />
          <DataHealthCard scanner={status?.scanner} ultra={status?.ultra} loading={loading} />
        </div>
      </div>

      {/* ── SECTOR STRENGTH ─────────────────────────────────────────────── */}
      <SectorStrengthPanel sectors={sectors} loading={loading} />

      {/* ── BOTTOM: FRESH SIGNALS + WATCHLIST ────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
        <FreshSignalsFeed signals={fresh} loading={loading} />
        <WatchlistSnapshot watchlistData={watchlist} loading={loading && !watchlist} />
      </div>

    </div>
  )
}
