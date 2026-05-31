import { useEffect, useRef, useState, useCallback } from 'react'
import { createChart } from 'lightweight-charts'
import { api } from '../api'

// Volume-bucket colours (same palette as the live CandleChart)
const BUCKET_HEX = { W: '#c3c0d3', L: '#0099ff', N: '#ffd000', B: '#e48100', VB: '#b02020' }

const fmtDate = (d) => String(d ?? '').slice(0, 10)

// build the 6 label lines exactly as stored in the DB row
function sixLines(r) {
  const tz = r.t_sig || r.z_sig || ''
  return {
    tz,
    L:      r.l_sig || '',
    suffix: r.composite_full_suffix || r.full_suffix || '',
    bw:     r.bar_body_wick || '',
    gr:     r.bar_gap_range || '',
    l5:     r.bar_line5 || '',
    isBull: !!r.t_sig,
    isBear: !!r.z_sig,
    pivot:  r.swing_type_3 || '',
  }
}

export default function DbCandleChart({ ticker, limit = 300 }) {
  const containerRef = useRef(null)
  const overlayRef   = useRef(null)        // absolute layer holding the per-bar code labels
  const chartRef     = useRef(null)
  const seriesRef    = useRef(null)
  const volRef       = useRef(null)
  const byTimeRef    = useRef({})           // time -> row (for tooltip)
  const signalsRef   = useRef([])           // [{time, low, high, isBull, lines:[...]}]
  const showCodesRef = useRef(true)
  const [error, setError]   = useState(null)
  const [loading, setLoading] = useState(false)
  const [hover, setHover]   = useState(null) // hovered bar's 6 lines
  const [meta, setMeta]     = useState(null) // {n, dmin, dmax}
  const [showCodes, setShowCodes] = useState(true)

  // ── Multi-line code overlay (TradingView-style: full 5 lines ON every signal
  //    bar, always visible). Positioned imperatively from the chart's coordinate
  //    API so it tracks pan / zoom / resize. ──────────────────────────────────
  const renderOverlay = useCallback(() => {
    const ov = overlayRef.current, chart = chartRef.current, series = seriesRef.current
    if (!ov || !chart || !series) return
    ov.innerHTML = ''
    if (!showCodesRef.current) return
    const ts = chart.timeScale()
    for (const s of signalsRef.current) {
      const x = ts.timeToCoordinate(s.time)
      if (x == null) continue                                   // off-screen
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
      // bull / neutral → below the low; bear → above the high
      el.style.transform = below
        ? 'translate(-50%, 10px)'
        : 'translate(-50%, calc(-100% - 10px))'
      el.innerHTML = s.lines.map((l, i) =>
        `<div style="${i === 0 ? 'font-weight:700;' : 'opacity:.9;'}">${l}</div>`).join('')
      ov.appendChild(el)
    }
  }, [])

  // init chart once
  useEffect(() => {
    if (!containerRef.current) return
    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: { background: { color: '#030712' }, textColor: '#9ca3af' },
      grid: { vertLines: { color: '#1f2937' }, horzLines: { color: '#1f2937' } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: '#374151' },
      timeScale: { borderColor: '#374151', timeVisible: false },
      width: containerRef.current.clientWidth || 600,
      height: 460,
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

    chart.subscribeCrosshairMove((param) => {
      if (!param.time || !byTimeRef.current[param.time]) { setHover(null); return }
      setHover(sixLines(byTimeRef.current[param.time]))
    })
    // re-position the code overlay on every pan / zoom
    chart.timeScale().subscribeVisibleLogicalRangeChange(() => renderOverlay())

    chartRef.current = chart
    seriesRef.current = series
    volRef.current = vol

    // resize → coordinates change → re-position labels
    const ro = new ResizeObserver(() => requestAnimationFrame(renderOverlay))
    if (containerRef.current) ro.observe(containerRef.current)

    return () => { ro.disconnect(); chart.remove() }
  }, [renderOverlay])

  // load DB bars when ticker/limit changes
  useEffect(() => {
    if (!seriesRef.current || !ticker) return
    setError(null); setLoading(true); setHover(null)
    api.studioBars(ticker, limit)
      .then((rows) => {
        const byTime = {}
        for (const r of rows) {
          if (r.close == null) continue
          const time = fmtDate(r.date)
          if (!byTime[time]) byTime[time] = r       // first universe wins (OHLC same)
        }
        const asc = Object.keys(byTime).sort().map(t => byTime[t])
        const candles = [], volumes = [], markers = [], signals = []
        for (const r of asc) {
          const time = fmtDate(r.date)
          candles.push({ time, open: +r.open, high: +r.high, low: +r.low, close: +r.close })
          volumes.push({ time, value: +r.volume || 0, color: BUCKET_HEX[r.vol_bucket] ?? '#374151' })
          const tz = r.t_sig || r.z_sig
          const suffix = r.composite_full_suffix || r.full_suffix || ''
          // Some bars match no T/Z candle pattern (≈4%) so tz is empty — but they
          // still carry L / suffix / body-wick / gap / line5. Label those too, with
          // the code starting at the L line (neutral, positioned below the bar).
          if (tz || r.l_sig || suffix) {
            const isBull = !!r.t_sig
            const lines = [
              tz ? `${tz}${r.l_sig || ''}` : (r.l_sig || ''),          // TZ+L, or just L
              suffix,                                                  // suffix
              r.bar_body_wick || '',                                   // body/wick
              r.bar_gap_range || '',                                   // gap/range
              r.bar_line5 || '',                                       // line5
            ].filter(Boolean)
            signals.push({ time, low: +r.low, high: +r.high, isBull, neutral: !tz, lines })
          }
        }
        byTimeRef.current = byTime
        signalsRef.current = signals
        seriesRef.current.setData(candles)
        seriesRef.current.setMarkers([])
        volRef.current?.setData(volumes)
        chartRef.current.priceScale('right').applyOptions({ autoScale: true })
        chartRef.current.timeScale().fitContent()
        requestAnimationFrame(() => { try { chartRef.current?.timeScale().fitContent(); renderOverlay() } catch {} })
        setMeta(asc.length ? { n: asc.length, dmin: fmtDate(asc[0].date), dmax: fmtDate(asc[asc.length - 1].date) } : null)
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, limit, renderOverlay])

  // toggle codes on/off
  useEffect(() => { showCodesRef.current = showCodes; renderOverlay() }, [showCodes, renderOverlay])

  const Row = ({ k, v, hl }) => (
    <div className="flex justify-between gap-3">
      <span className="text-md-on-surface-var/60">{k}</span>
      <span className={`font-mono ${hl ? 'text-amber-300' : 'text-md-on-surface'}`}>{v || '—'}</span>
    </div>
  )

  return (
    <div className="bg-md-surface-con rounded-xl border border-md-outline-var">
      <div className="flex items-center justify-between px-4 py-2 border-b border-md-outline-var">
        <span className="font-semibold text-sm">
          {ticker} <span className="text-md-on-surface-var font-normal">· DB (Studio) · 1d</span>
          {meta && <span className="ml-2 text-xs text-md-on-surface-var">{meta.n} bars · {meta.dmin} → {meta.dmax}</span>}
        </span>
        <div className="flex items-center gap-3">
          <label className="flex items-center gap-1 text-xs text-md-on-surface-var cursor-pointer select-none"
                 title="Show the full 5-line DB code on every signal bar">
            <input type="checkbox" checked={showCodes} onChange={e => setShowCodes(e.target.checked)} />
            <span>codes</span>
          </label>
          <div className="hidden md:flex items-center gap-1.5 text-xs text-md-on-surface-var">
            {Object.entries(BUCKET_HEX).map(([k, v]) => (
              <span key={k} className="flex items-center gap-0.5">
                <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: v }} />
                <span className="font-mono">{k}</span>
              </span>
            ))}
          </div>
          {loading && <span className="text-xs text-md-on-surface-var animate-pulse">loading…</span>}
          {error && <span className="text-xs text-red-400">{error}</span>}
        </div>
      </div>
      <div className="relative">
        <div ref={containerRef} className="w-full" style={{ height: 460 }} />
        {/* per-bar full 5-line code overlay (TradingView-style) */}
        <div ref={overlayRef} className="absolute inset-0 overflow-hidden pointer-events-none" style={{ zIndex: 4 }} />
        {/* hover tooltip — exact 6 DB lines for the bar */}
        {hover && (
          <div className="absolute top-2 left-2 z-10 bg-md-surface-high/95 border border-md-outline-var rounded-lg px-3 py-2 text-xs space-y-0.5 pointer-events-none min-w-[160px]">
            <div className={`font-bold ${hover.isBull ? 'text-green-400' : hover.isBear ? 'text-red-400' : 'text-md-on-surface'}`}>
              {hover.isBull ? 'T (bull)' : hover.isBear ? 'Z (bear)' : 'no T/Z'}
              {hover.pivot && <span className="ml-2 text-md-on-surface-var">pivot {hover.pivot}</span>}
            </div>
            <Row k="TZ"      v={hover.tz}     hl />
            <Row k="L"       v={hover.L} />
            <Row k="suffix"  v={hover.suffix} hl />
            <Row k="body/wk" v={hover.bw} />
            <Row k="gap/rng" v={hover.gr} />
            <Row k="l5"      v={hover.l5}     hl />
          </div>
        )}
      </div>
      <div className="px-4 py-1.5 text-[11px] text-md-on-surface-var border-t border-md-outline-var">
        Data straight from Studio DB — full 5-line code shown on every signal bar (toggle “codes”).
        These are the exact codes the Sequence Builder matches. May differ from your TradingView chart’s feed.
      </div>
    </div>
  )
}
