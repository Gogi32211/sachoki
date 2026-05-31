import { useEffect, useRef, useState, useCallback } from 'react'
import { createChart } from 'lightweight-charts'
import { api } from '../api'

// Volume-bucket colours (shared palette)
const BUCKET_HEX = { W: '#c3c0d3', L: '#0099ff', N: '#ffd000', B: '#e48100', VB: '#b02020' }
const SIG_COLOR  = { bull: '#22c55e', bear: '#ef4444' }
const BAR_OPTIONS = [120, 200, 300, 500, 1000]

const fmtDate = (d) => String(d ?? '').slice(0, 10)
const isIntradayTf = (tf) => ['30m', '15m', '1h', '4h'].includes(tf)

// lightweight-charts requires STRICTLY ascending, unique timestamps — drop any
// adjacent duplicate (intraday feeds occasionally repeat a bar's timestamp).
const dedupeByTime = (arr) => {
  const out = []
  let last
  for (const x of arr) { if (x.time === last) continue; out.push(x); last = x.time }
  return out
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unified candlestick chart used everywhere a price chart appears.
//
// HYBRID data source (decided with the user, 2026-05-31):
//   • tf === '1d'  → Studio DB (api.studioBars) → full 6-line code overlay
//                    (these are the EXACT codes the Sequence Builder matches).
//                    Falls back to live signals if the ticker isn't in the DB.
//   • intraday / weekly → live signals (api.signals) → signal arrow markers,
//                    no code overlay (those codes only exist for daily DB bars).
//
// Props let each host configure the chrome:
//   showToolbar     header row (title · legend · codes toggle · bar selector)
//   showBarSelector the 120-1000 dropdown inside the toolbar
//   showFooter      the "data straight from DB" caption
//   showSector      fetch + show the sector chip in the title
//   interactive     allow pan / zoom (off for hover popups)
//   bare            render ONLY the chart body (no toolbar/footer/border) —
//                   for hosts that supply their own header (screener popups)
// ═══════════════════════════════════════════════════════════════════════════════
export default function CodeCandleChart({
  ticker,
  tf = '1d',
  height = 460,
  initialLimit = 300,
  showToolbar = true,
  showBarSelector = true,
  showFooter = false,
  showSector = false,
  interactive = true,
  bare = false,
  onChartReady,
}) {
  const containerRef = useRef(null)
  const overlayRef   = useRef(null)
  const chartRef     = useRef(null)
  const seriesRef    = useRef(null)
  const volRef       = useRef(null)
  const signalsRef   = useRef([])           // [{time, low, high, isBull, neutral, lines, vol}]
  const showCodesRef = useRef(true)
  const [limit, setLimit]     = useState(initialLimit)
  const [showCodes, setShowCodes] = useState(true)
  const [error, setError]     = useState(null)
  const [loading, setLoading] = useState(false)
  const [meta, setMeta]       = useState(null) // {n, dmin, dmax, src}
  const [sector, setSector]   = useState(null)

  const intraday = isIntradayTf(tf)
  const useDb    = tf === '1d'                // weekly + intraday → live signals

  // ── per-bar 6-line code overlay (positioned imperatively from the coordinate
  //    API so it tracks pan / zoom / resize). Only populated for DB (1d) data. ──
  const renderOverlay = useCallback(() => {
    const ov = overlayRef.current, chart = chartRef.current, series = seriesRef.current
    if (!ov || !chart || !series) return
    ov.innerHTML = ''
    if (!showCodesRef.current) return
    const ts = chart.timeScale()
    for (const s of signalsRef.current) {
      const x = ts.timeToCoordinate(s.time)
      if (x == null) continue
      const below = s.isBull || s.neutral
      const y = series.priceToCoordinate(below ? s.low : s.high)
      if (y == null) continue
      const el = document.createElement('div')
      el.style.cssText =
        'position:absolute;text-align:center;line-height:1.1;font-family:ui-monospace,monospace;'
        + 'font-size:12px;white-space:nowrap;pointer-events:none;padding:1px 3px;border-radius:3px;'
        + 'background:rgba(3,7,18,0.55);color:#ffffff;'
      el.style.left = x + 'px'
      el.style.top  = y + 'px'
      el.style.transform = below
        ? 'translate(-50%, 10px)'
        : 'translate(-50%, calc(-100% - 10px))'
      el.innerHTML = s.lines.map((l, i) =>
        `<div style="${i === 0 ? 'font-weight:700;' : 'opacity:.9;'}">${l}</div>`).join('')
        + (s.vol ? `<div style="font-weight:700;">${s.vol}</div>` : '')
      ov.appendChild(el)
    }
  }, [])

  // optional sector chip
  useEffect(() => {
    if (!showSector || !ticker) { setSector(null); return }
    api.tickerInfo(ticker).then(d => setSector(d?.sector || null)).catch(() => {})
  }, [ticker, showSector])

  // init chart once
  useEffect(() => {
    if (!containerRef.current) return
    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: { background: { color: '#030712' }, textColor: '#9ca3af' },
      grid: { vertLines: { color: '#1f2937' }, horzLines: { color: '#1f2937' } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: '#374151' },
      timeScale: { borderColor: '#374151', timeVisible: intraday },
      width: containerRef.current.clientWidth || 600,
      height,
      handleScroll: interactive,
      handleScale: interactive,
    })
    const series = chart.addCandlestickSeries({
      upColor: '#22c55e', downColor: '#ef4444',
      borderUpColor: '#22c55e', borderDownColor: '#ef4444',
      wickUpColor: '#22c55e', wickDownColor: '#ef4444',
    })
    const vol = chart.addHistogramSeries({
      priceFormat: { type: 'volume' }, priceScaleId: 'vol', color: '#374151',
    })
    chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } })
    chart.timeScale().subscribeVisibleLogicalRangeChange(() => renderOverlay())

    chartRef.current = chart
    seriesRef.current = series
    volRef.current = vol
    onChartReady?.(chart)

    const ro = new ResizeObserver(() => requestAnimationFrame(renderOverlay))
    ro.observe(containerRef.current)

    return () => { ro.disconnect(); chart.remove(); onChartReady?.(null) }
    // height / interactive are fixed per mount; tf-driven options are applied below
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  // load data when ticker / tf / limit changes
  useEffect(() => {
    if (!seriesRef.current || !ticker) return
    let cancelled = false
    setError(null); setLoading(true)
    chartRef.current.applyOptions({ timeScale: { timeVisible: intraday } })
    // clear any stale overlay/markers immediately so a tf switch never shows old codes
    signalsRef.current = []
    if (overlayRef.current) overlayRef.current.innerHTML = ''

    // ── live-signals path (intraday, weekly, or DB-miss fallback) ──
    const loadSignals = (effTf) => api.signals(ticker, effTf, limit).then((rows) => {
      if (cancelled) return
      const intra = isIntradayTf(effTf)
      const toTime = (r) => {
        const d = r.date ?? r.Datetime ?? r.Date
        if (!d) return null
        if (intra) {
          const ms = new Date(String(d).replace(' ', 'T')).getTime()
          return isNaN(ms) ? null : Math.floor(ms / 1000)
        }
        return String(d).slice(0, 10)
      }
      const candles = dedupeByTime(rows.filter(r => r.close != null && toTime(r))
        .map(r => ({ time: toTime(r), open: +r.open, high: +r.high, low: +r.low, close: +r.close }))
        .sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0)))
      const volumes = dedupeByTime(rows.filter(r => r.volume != null && toTime(r))
        .map(r => ({ time: toTime(r), value: +r.volume, color: BUCKET_HEX[r.vol_bucket] ?? '#374151' }))
        .sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0)))
      const markers = rows.filter(r => r.sig_id > 0 && toTime(r)).map((r) => {
        const combo = r.l_combo && r.l_combo !== 'NONE' ? ` [${r.l_combo}]` : ''
        return {
          time: toTime(r),
          position: r.is_bull ? 'belowBar' : 'aboveBar',
          color: r.is_bull ? SIG_COLOR.bull : SIG_COLOR.bear,
          shape: r.is_bull ? 'arrowUp' : 'arrowDown',
          text: `${r.sig_name}${combo}`,
        }
      }).sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0))
      signalsRef.current = []                       // no code overlay for live data
      seriesRef.current.setData(candles)
      seriesRef.current.setMarkers(markers)
      volRef.current?.setData(volumes)
      chartRef.current.priceScale('right').applyOptions({ autoScale: true })
      chartRef.current.timeScale().fitContent()
      requestAnimationFrame(renderOverlay)
      const dmin = candles.length ? fmtDate(rows[0]?.date ?? rows[0]?.Datetime ?? rows[0]?.Date) : null
      setMeta(candles.length ? { n: candles.length, src: 'live', dmin: null, dmax: null } : null)
    })

    // ── DB path (1d) → candles + full 6-line code overlay ──
    const loadStudio = () => api.studioBars(ticker, limit).then((rows) => {
      if (cancelled) return
      const byTime = {}
      for (const r of rows) {
        if (r.close == null) continue
        const time = fmtDate(r.date)
        if (!byTime[time]) byTime[time] = r
      }
      const asc = Object.keys(byTime).sort().map(t => byTime[t])
      if (!asc.length) return loadSignals('1d')      // ticker not in DB → live fallback
      const candles = [], volumes = [], signals = []
      for (const r of asc) {
        const time = fmtDate(r.date)
        candles.push({ time, open: +r.open, high: +r.high, low: +r.low, close: +r.close })
        volumes.push({ time, value: +r.volume || 0, color: BUCKET_HEX[r.vol_bucket] ?? '#374151' })
        const tz = r.t_sig || r.z_sig
        const suffix = r.composite_full_suffix || r.full_suffix || ''
        if (tz || r.l_sig || suffix) {
          const lines = [
            tz ? `${tz}${r.l_sig || ''}` : (r.l_sig || ''),
            suffix,
            r.bar_body_wick || '',
            r.bar_gap_range || '',
            r.bar_line5 || '',
          ].filter(Boolean)
          signals.push({ time, low: +r.low, high: +r.high, isBull: !!r.t_sig, neutral: !tz, lines, vol: r.vol_bucket || '' })
        }
      }
      signalsRef.current = signals
      seriesRef.current.setData(candles)
      seriesRef.current.setMarkers([])
      volRef.current?.setData(volumes)
      chartRef.current.priceScale('right').applyOptions({ autoScale: true })
      chartRef.current.timeScale().fitContent()
      requestAnimationFrame(() => { try { chartRef.current?.timeScale().fitContent(); renderOverlay() } catch {} })
      setMeta(asc.length ? { n: asc.length, src: 'db', dmin: fmtDate(asc[0].date), dmax: fmtDate(asc[asc.length - 1].date) } : null)
    })

    const p = useDb ? loadStudio() : loadSignals(tf)
    p.catch((e) => { if (!cancelled) setError(e.message) })
     .finally(() => { if (!cancelled) setLoading(false) })

    return () => { cancelled = true }
  }, [ticker, tf, limit, useDb, intraday, renderOverlay])

  // toggle codes on/off
  useEffect(() => { showCodesRef.current = showCodes; renderOverlay() }, [showCodes, renderOverlay])

  const chartBody = (
    <div className="relative">
      <div ref={containerRef} className="w-full" style={{ height }} />
      <div ref={overlayRef} className="absolute inset-0 overflow-hidden pointer-events-none" style={{ zIndex: 4 }} />
      {loading && !meta && (
        <div className="absolute inset-0 flex items-center justify-center text-md-on-surface-var text-xs animate-pulse pointer-events-none">
          loading…
        </div>
      )}
    </div>
  )

  // ── bare: chart body only (host supplies its own header) ──
  if (bare) return chartBody

  const legend = (
    <div className="hidden md:flex items-center gap-1.5 text-xs text-md-on-surface-var">
      {Object.entries(BUCKET_HEX).map(([k, v]) => (
        <span key={k} className="flex items-center gap-0.5">
          <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: v }} />
          <span className="font-mono">{k}</span>
        </span>
      ))}
    </div>
  )

  return (
    <div className="bg-md-surface-con rounded-xl border border-md-outline-var">
      {showToolbar && (
        <div className="flex items-center justify-between px-4 py-2 border-b border-md-outline-var gap-3 flex-wrap">
          <span className="font-semibold text-sm">
            {ticker} <span className="text-md-on-surface-var font-normal">{useDb ? '· DB (Studio) · 1d' : `· ${tf}`}</span>
            {sector && (
              <span className="ml-2 text-xs font-normal text-md-on-surface-var bg-md-surface-high px-1.5 py-0.5 rounded">{sector}</span>
            )}
            {meta && (
              <span className="ml-2 text-xs text-md-on-surface-var">
                {meta.n} bars{meta.dmin ? ` · ${meta.dmin} → ${meta.dmax}` : ''}
              </span>
            )}
          </span>
          <div className="flex items-center gap-3">
            {useDb && (
              <label className="flex items-center gap-1 text-xs text-md-on-surface-var cursor-pointer select-none"
                     title="Show the full 6-line DB code on every signal bar">
                <input type="checkbox" checked={showCodes} onChange={e => setShowCodes(e.target.checked)} />
                <span>codes</span>
              </label>
            )}
            {legend}
            {showBarSelector && (
              <select value={limit} onChange={e => setLimit(Number(e.target.value))}
                className="bg-md-surface border border-md-outline-var rounded-lg px-2 py-1 text-xs text-md-on-surface">
                {BAR_OPTIONS.map(n => <option key={n} value={n}>{n} bars</option>)}
              </select>
            )}
            {loading && <span className="text-xs text-md-on-surface-var animate-pulse">loading…</span>}
            {error && <span className="text-xs text-red-400">{error}</span>}
          </div>
        </div>
      )}
      {chartBody}
      {showFooter && (
        <div className="px-4 py-1.5 text-[11px] text-md-on-surface-var border-t border-md-outline-var">
          {useDb
            ? 'Data straight from Studio DB — full 6-line code shown on every signal bar (toggle “codes”). These are the exact codes the Sequence Builder matches. May differ from your TradingView chart’s feed.'
            : 'Live signal feed — intraday codes are not stored; signal arrows shown instead. Switch to 1d for the full DB code overlay.'}
        </div>
      )}
    </div>
  )
}
