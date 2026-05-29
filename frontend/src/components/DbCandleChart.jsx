import { useEffect, useRef, useState } from 'react'
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
  const chartRef     = useRef(null)
  const seriesRef    = useRef(null)
  const volRef       = useRef(null)
  const byTimeRef    = useRef({})           // time -> row (for tooltip)
  const [error, setError]   = useState(null)
  const [loading, setLoading] = useState(false)
  const [hover, setHover]   = useState(null) // hovered bar's 6 lines
  const [meta, setMeta]     = useState(null) // {n, dmin, dmax}

  // init chart once
  useEffect(() => {
    if (!containerRef.current) return
    // autoSize lets lightweight-charts size itself to the container via its own
    // ResizeObserver — fixes the "chart renders empty" race when this (lazy-loaded)
    // tab mounts before the container has a measured width (width:0 → no candles).
    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: { background: { color: '#030712' }, textColor: '#9ca3af' },
      grid: { vertLines: { color: '#1f2937' }, horzLines: { color: '#1f2937' } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: '#374151' },
      timeScale: { borderColor: '#374151', timeVisible: false },
      width: containerRef.current.clientWidth || 600,   // fallback if not yet measured
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

    chartRef.current = chart
    seriesRef.current = series
    volRef.current = vol
    return () => { chart.remove() }
  }, [])

  // load DB bars when ticker/limit changes
  useEffect(() => {
    if (!seriesRef.current || !ticker) return
    setError(null); setLoading(true); setHover(null)
    api.studioBars(ticker, limit)
      .then((rows) => {
        // endpoint returns DESC → sort ascending for charting.
        // DEDUPE BY DATE: a ticker can exist in >1 universe (e.g. RGTI in nasdaq
        // AND russell2k), so the same date arrives twice. lightweight-charts needs
        // strictly-unique ascending timestamps — duplicates make it render NOTHING.
        // Collapse to one row per date (OHLC is identical across universes).
        const byTime = {}
        for (const r of rows) {
          if (r.close == null) continue
          const time = fmtDate(r.date)
          if (!byTime[time]) byTime[time] = r       // first universe wins (OHLC same)
        }
        const asc = Object.keys(byTime).sort().map(t => byTime[t])
        const candles = [], volumes = [], markers = []
        for (const r of asc) {
          const time = fmtDate(r.date)
          candles.push({ time, open: +r.open, high: +r.high, low: +r.low, close: +r.close })
          volumes.push({ time, value: +r.volume || 0, color: BUCKET_HEX[r.vol_bucket] ?? '#374151' })
          const tz = r.t_sig || r.z_sig
          if (tz) {
            const lbl = `${tz}${r.l_sig || ''}`
            markers.push({
              time,
              position: r.t_sig ? 'belowBar' : 'aboveBar',
              color:    r.t_sig ? '#22c55e' : '#ef4444',
              shape:    r.t_sig ? 'arrowUp' : 'arrowDown',
              text:     lbl,
            })
          }
        }
        byTimeRef.current = byTime
        seriesRef.current.setData(candles)
        seriesRef.current.setMarkers(markers)
        volRef.current?.setData(volumes)
        chartRef.current.priceScale('right').applyOptions({ autoScale: true })
        chartRef.current.timeScale().fitContent()
        // re-fit on the next frame in case the container width was still settling
        // when the data arrived (otherwise candles can render off-screen / invisible)
        requestAnimationFrame(() => { try { chartRef.current?.timeScale().fitContent() } catch {} })
        setMeta(asc.length ? { n: asc.length, dmin: fmtDate(asc[0].date), dmax: fmtDate(asc[asc.length - 1].date) } : null)
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, limit])

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
        Data straight from Studio DB — these are the exact codes the Sequence Builder matches
        (hover any bar to read all 6 lines). May differ from your TradingView chart's feed.
      </div>
    </div>
  )
}
