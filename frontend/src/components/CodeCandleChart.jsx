import { useEffect, useRef, useState, useCallback } from 'react'
import { zoneColor as zColor } from '../utils/zoneColors'
import { createChart } from 'lightweight-charts'
import { api } from '../api'

// Volume-bucket colours (shared palette)
const BUCKET_HEX = { W: '#c3c0d3', L: '#0099ff', N: '#ffd000', B: '#e48100', VB: '#b02020' }
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
  codes = true,          // initial state of the code overlay (off for clean previews)
  onChartReady,
  zoneMarkers,           // [{date, rel}] — external markers to draw on bars (for HV-Zones panel)
  zoneSource = 'hv',     // 'hv' (cyan) | 'gann' (amber) — which zone overlay to draw
  sidePanelExtras = null,// optional JSX rendered in the fullscreen side panel
                         // (parent supplies its own settings/controls)
}) {
  const containerRef = useRef(null)
  const overlayRef   = useRef(null)
  const chartRef     = useRef(null)
  const seriesRef    = useRef(null)
  const volRef       = useRef(null)
  const signalsRef   = useRef([])           // [{time, low, high, isBull, neutral, lines, vol}]
  const showCodesRef = useRef(codes)
  const zoneLinesRef = useRef([])           // active priceLines for HV-zone overlay
  const histLinesRef = useRef([])           // grey/white history overlay (HV + Gann, merged)
  const candlesRef   = useRef([])           // base candles (no zone colors) — re-recolored on zone change
  const [hvZones, setHvZones] = useState([]) // for tiny "HV-Zone" info badge
  // Independent history pickers — user can show grey HV history and lime Gann
  // pivots at the same time on ANY chart, regardless of zoneSource.
  const [historyHvTier,   setHistoryHvTier]   = useState(0)   // 0/2/5/10 vol multiple
  const [historyGannTier, setHistoryGannTier] = useState(0)   // 0/5/10/20 pivot radius
  const [historyHvCount,   setHistoryHvCount]   = useState(0)
  const [historyGannCount, setHistoryGannCount] = useState(0)

  // Fullscreen toggle — wraps chart + side data panel.
  const [fullscreen, setFullscreen] = useState(false)
  useEffect(() => {
    if (!fullscreen) return
    const onKey = e => { if (e.key === 'Escape') setFullscreen(false) }
    window.addEventListener('keydown', onKey)
    document.body.style.overflow = 'hidden'
    return () => {
      window.removeEventListener('keydown', onKey)
      document.body.style.overflow = ''
    }
  }, [fullscreen])
  // Explicitly resize the chart on fullscreen change — autoSize sometimes
  // misses the layout transition (CSS hasn't applied yet on the same tick).
  useEffect(() => {
    if (!chartRef.current || !containerRef.current) return
    const apply = () => {
      const el = containerRef.current
      if (!el || !chartRef.current) return
      try {
        chartRef.current.applyOptions({ width: el.clientWidth, height: el.clientHeight })
        chartRef.current.timeScale().fitContent()
      } catch {}
    }
    const r1 = requestAnimationFrame(apply)
    const t1 = setTimeout(apply, 60)   // catch any late CSS transition
    const t2 = setTimeout(apply, 200)
    return () => { cancelAnimationFrame(r1); clearTimeout(t1); clearTimeout(t2) }
  }, [fullscreen])

  // Re-apply zone colors over the stored candles. Trigger-bar = colored
  // candle/border/wick; everything else uses the default green/red.
  const applyZoneColors = useCallback((zones) => {
    const series = seriesRef.current
    if (!series || !candlesRef.current.length) return
    if (!zones?.length) { try { series.setData(candlesRef.current) } catch {} ; return }
    const colorByDate = new Map()
    for (const z of zones) if (z.trigger_date) colorByDate.set(z.trigger_date, z._color)
    const recoloured = candlesRef.current.map(c => {
      const col = colorByDate.get(c.time)
      return col ? { ...c, color: col, borderColor: col, wickColor: col } : c
    })
    try { series.setData(recoloured) } catch {}
  }, [])
  const [limit, setLimit]     = useState(initialLimit)
  const [showCodes, setShowCodes] = useState(codes)
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
      grid: { vertLines: { visible: false }, horzLines: { visible: false } },
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

    // ResizeObserver: chart canvas itself is handled by lightweight-charts'
    // autoSize:true; we only need to re-position the code overlays.
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
      // Build the SAME white code overlay from the live feed (no arrows). The
      // full 6-line suffix/body-wick/gap/line5 are DB-enrichment only, so live
      // bars show the lines we have: TZ (sig_name) · L (l_combo) · vol bucket.
      const sigOverlay = []
      for (const r of rows) {
        const t = toTime(r)
        if (t == null || r.close == null) continue
        const tz = r.sig_id > 0 ? (r.sig_name || '') : ''
        const lc = r.l_combo && r.l_combo !== 'NONE' ? r.l_combo : ''
        if (!tz && !lc) continue
        sigOverlay.push({
          time: t, low: +r.low, high: +r.high,
          isBull: !!r.is_bull, neutral: !tz,
          lines: [tz, lc].filter(Boolean), vol: r.vol_bucket || '',
        })
      }
      signalsRef.current = sigOverlay
      candlesRef.current = candles
      seriesRef.current.setData(candles)
      seriesRef.current.setMarkers([])
      volRef.current?.setData(volumes)
      // If zones already loaded before candles, recolour the triggers now.
      if (hvZones?.length) applyZoneColors(hvZones)
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
      // build a signal-overlay object from a row (DB or live forming bar)
      const mkSig = (r) => {
        const time = fmtDate(r.date)
        const tz = r.t_sig || r.z_sig
        const suffix = r.composite_full_suffix || r.full_suffix || ''
        if (!(tz || r.l_sig || suffix)) return null
        const lines = [
          tz ? `${tz}${r.l_sig || ''}` : (r.l_sig || ''),
          suffix, r.bar_body_wick || '', r.bar_gap_range || '', r.bar_line5 || '',
        ].filter(Boolean)
        return { time, low: +r.low, high: +r.high, isBull: !!r.t_sig, neutral: !tz, lines, vol: r.vol_bucket || '' }
      }
      const candles = [], volumes = [], signals = []
      for (const r of asc) {
        const time = fmtDate(r.date)
        candles.push({ time, open: +r.open, high: +r.high, low: +r.low, close: +r.close })
        volumes.push({ time, value: +r.volume || 0, color: BUCKET_HEX[r.vol_bucket] ?? '#374151' })
        const s = mkSig(r); if (s) signals.push(s)
      }
      signalsRef.current = signals
      candlesRef.current = candles
      seriesRef.current.setData(candles)
      seriesRef.current.setMarkers([])
      volRef.current?.setData(volumes)
      if (hvZones?.length) applyZoneColors(hvZones)
      chartRef.current.priceScale('right').applyOptions({ autoScale: true })
      chartRef.current.timeScale().fitContent()
      requestAnimationFrame(() => { try { chartRef.current?.timeScale().fitContent(); renderOverlay() } catch {} })
      setMeta({ n: asc.length, src: 'db', dmin: fmtDate(asc[0].date), dmax: fmtDate(asc[asc.length - 1].date) })

      // ── append today's LIVE forming bar (Massive, 15-min delayed) ──────────
      // Only while the US regular session is open; backend returns [] otherwise.
      const lastDate = fmtDate(asc[asc.length - 1].date)
      api.studioLiveTail(ticker, lastDate).then((res) => {
        if (cancelled || !res?.bars?.length) return
        for (const r of res.bars) {
          if (r.close == null) continue
          const time = fmtDate(r.date)
          seriesRef.current.update({ time, open: +r.open, high: +r.high, low: +r.low, close: +r.close })
          volRef.current?.update({ time, value: +r.volume || 0, color: BUCKET_HEX[r.vol_bucket] ?? '#374151' })
          const s = mkSig(r); if (s) signalsRef.current.push(s)
        }
        const lr = res.bars[res.bars.length - 1]
        chartRef.current.timeScale().fitContent()
        requestAnimationFrame(() => { try { renderOverlay() } catch {} })
        setMeta(m => m ? { ...m, live: true, dmax: fmtDate(lr.date) } : m)
      }).catch(() => {})
    })

    const p = useDb ? loadStudio() : loadSignals(tf)
    p.catch((e) => { if (!cancelled) setError(e.message) })
     .finally(() => { if (!cancelled) setLoading(false) })

    return () => { cancelled = true }
  }, [ticker, tf, limit, useDb, intraday, renderOverlay])

  // toggle codes on/off
  useEffect(() => { showCodesRef.current = showCodes; renderOverlay() }, [showCodes, renderOverlay])

  // Combined HV + Gann history overlay. Both sources are grey by default; when
  // a price level appears in BOTH (within rounding tolerance), it's drawn
  // white to highlight the confluence.
  useEffect(() => {
    const series = seriesRef.current
    if (!series) return
    // Clear existing
    for (const ln of histLinesRef.current) { try { series.removePriceLine(ln) } catch {} }
    histLinesRef.current = []
    setHistoryHvCount(0); setHistoryGannCount(0)
    if (!ticker || tf !== '1d') return
    if (!historyHvTier && !historyGannTier) return
    let dead = false
    const firstDate = candlesRef.current?.[0]?.time
    const fromQ = firstDate ? `&from_date=${firstDate}` : ''
    const fetches = []
    if (historyHvTier > 0)   fetches.push(
      fetch(`/api/hv-zones/history/${ticker}?vol_min=${historyHvTier}&limit=500${fromQ}`)
        .then(r => r.json()).then(d => ({ kind: 'hv',   zones: d?.zones || [] })))
    if (historyGannTier > 0) fetches.push(
      fetch(`/api/gann-zones/history/${ticker}?pivot=${historyGannTier}&limit=500${fromQ}`)
        .then(r => r.json()).then(d => ({ kind: 'gann', zones: d?.zones || [] })))
    Promise.all(fetches).then(results => {
      if (dead) return
      // Bucket prices by rounded key so HV-bound and Gann-pivot at the same
      // level coalesce. 2 decimals for >$5, 4 for <$5 (penny precision).
      const toKey = (p) => p >= 5 ? p.toFixed(2) : p.toFixed(4)
      const bucket = new Map()                     // key → { price, hv: bool, gann: bool }
      for (const r of results) {
        if (r.kind === 'hv')   setHistoryHvCount(r.zones.length)
        else                   setHistoryGannCount(r.zones.length)
        for (const z of r.zones) {
          for (const p of [z.zone_high, z.zone_low]) {
            const k = toKey(p)
            const ex = bucket.get(k) || { price: p, hv: false, gann: false }
            ex[r.kind] = true
            bucket.set(k, ex)
          }
        }
      }
      for (const e of bucket.values()) {
        const both = e.hv && e.gann
        histLinesRef.current.push(series.createPriceLine({
          price: e.price,
          color: both ? '#ffffff' : '#64748b',
          lineWidth: both ? 1 : 1,
          lineStyle: 2,
          axisLabelVisible: false,
        }))
      }
    }).catch(() => {})
    return () => { dead = true }
  }, [ticker, tf, historyHvTier, historyGannTier, limit])

  // External zone-classification markers (one per bar with relation to HV-zone)
  // merged with trigger-bar markers (per-zone colored to match its lines).
  useEffect(() => {
    const series = seriesRef.current
    if (!series) return
    const REL = {
      inside:      { color: '#22c55e', position: 'belowBar', shape: 'circle',   text: 'IN' },
      cross:       { color: '#eab308', position: 'belowBar', shape: 'square',   text: 'CR' },
      touch_below: { color: '#22d3ee', position: 'belowBar', shape: 'arrowUp',  text: 'TB' },
      touch_above: { color: '#f472b6', position: 'aboveBar', shape: 'arrowDown',text: 'TA' },
    }
    const markers = []
    // 1) Classification markers — relation to zones, on every bar after the trigger.
    for (const b of (zoneMarkers || [])) {
      const m = REL[b.rel]
      if (!m) continue
      markers.push({ time: b.date, ...m })
    }
    // 2) Trigger-bar markers — square ABOVE the bar in the same color as the zone.
    for (const z of (hvZones || [])) {
      if (!z?.trigger_date) continue
      markers.push({
        time: z.trigger_date,
        position: 'aboveBar', shape: 'square', color: z._color,
        text: `Z${z._idx || ''}${z.kind ? ' ' + z.kind[0].toUpperCase() : ''} TRIG`,
      })
    }
    // setMarkers needs chronological order, else lightweight-charts warns.
    markers.sort((a, b) => String(a.time).localeCompare(String(b.time)))
    try { series.setMarkers(markers) } catch {}
  }, [zoneMarkers, hvZones])

  // ── HV-Zone overlay (drawn only on the 1d DB chart) ──────────────────────
  // Two horizontal lines per zone (zone_low / zone_high) using lightweight-
  // charts createPriceLine. They auto-track pan/zoom and stay above candles.
  useEffect(() => {
    const series = seriesRef.current
    if (!series) return
    // clear any prior zones first (also on ticker switch)
    for (const ln of zoneLinesRef.current) {
      try { series.removePriceLine(ln) } catch {}
    }
    zoneLinesRef.current = []
    setHvZones([])
    if (!ticker || tf !== '1d') return
    let dead = false
    const isGann = zoneSource === 'gann'
    const url = isGann ? `/api/gann-zones/zones/${ticker}` : `/api/zone-retest/zones/${ticker}`
    const labelTop = isGann ? 'Gann-top' : 'HV-top'
    const labelBot = isGann ? 'Gann-bot' : 'HV-bot'
    fetch(url).then(r => r.json()).then(d => {
      if (dead) return
      const zones = d?.zones || []
      const coloured = zones.map((z, i) => ({ ...z, _source: zoneSource, _color: zColor(i), _idx: i + 1 }))
      setHvZones(coloured)
      applyZoneColors(coloured)             // recolor trigger candles
      zones.forEach((z, i) => {
        const color = zColor(i)
        const kindTag = z.kind ? z.kind.toUpperCase().slice(0,3) + ' ' : ''
        const titlePref = `Z${i + 1} `
        zoneLinesRef.current.push(series.createPriceLine({
          price: z.zone_high, color, lineWidth: 1, lineStyle: 2,
          axisLabelVisible: true, title: `${titlePref}${kindTag}${labelTop} ${z.trigger_date}`,
        }))
        zoneLinesRef.current.push(series.createPriceLine({
          price: z.zone_low, color, lineWidth: 1, lineStyle: 2,
          axisLabelVisible: true, title: `${titlePref}${kindTag}${labelBot}`,
        }))
      })
    }).catch(() => {})
    return () => { dead = true }
  }, [ticker, tf, zoneSource])

  const chartBody = (
    <div className={fullscreen ? 'relative flex-1 min-h-0' : 'relative'}>
      <div ref={containerRef}
           className={fullscreen ? 'w-full h-full' : 'w-full'}
           style={fullscreen ? undefined : { height }} />
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

  // Reflect the ACTUAL data source (a 1d ticker missing from the DB falls back
  // to the live feed, which still renders white codes — just the lines it has).
  const srcIsDb = meta ? meta.src === 'db' : useDb

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

  const inner = (
    <div className={fullscreen
        ? 'flex-1 flex flex-col bg-md-surface-con overflow-hidden'
        : 'bg-md-surface-con rounded-xl border border-md-outline-var'}>
      {showToolbar && (
        <div className="flex items-center justify-between px-4 py-2 border-b border-md-outline-var gap-3 flex-wrap">
          <span className="font-semibold text-sm">
            {ticker} <span className="text-md-on-surface-var font-normal">{srcIsDb ? '· DB (Studio) · 1d' : `· ${tf} · live`}</span>
            {meta?.live && <span className="ml-1 text-[10px] text-lime-400 font-semibold" title="Today's forming bar appended live from Massive (15-min delayed)">+live</span>}
            {sector && (
              <span className="ml-2 text-xs font-normal text-md-on-surface-var bg-md-surface-high px-1.5 py-0.5 rounded">{sector}</span>
            )}
            {meta && (
              <span className="ml-2 text-xs text-md-on-surface-var">
                {meta.n} bars{meta.dmin ? ` · ${meta.dmin} → ${meta.dmax}` : ''}
              </span>
            )}
            {hvZones.length > 0 && (() => {
              const gann = zoneSource === 'gann'
              const cls = gann ? 'text-amber-300 bg-amber-950/60 ring-amber-700/50' : 'text-cyan-300 bg-cyan-950/60 ring-cyan-700/50'
              const icon = gann ? '📐' : '🎯'
              const label = gann ? 'Gann-Zone' : 'HV-Zone'
              return <span className={`ml-2 text-[10px] font-semibold px-1.5 py-0.5 rounded ring-1 ${cls}`}
                    title={hvZones.map((z, i) => `Z${i+1}${z.kind?` (${z.kind})`:''}: $${z.zone_low}-$${z.zone_high} · trig ${z.trigger_date}${z.trigger_vol_mult?` (vol×${z.trigger_vol_mult})`:''}`).join('\n')}>
                {icon} {label} × {hvZones.length}
              </span>
            })()}
          </span>
          <div className="flex items-center gap-3">
            {(
              <label className="flex items-center gap-1 text-xs text-md-on-surface-var cursor-pointer select-none"
                     title="Show the chart code on every signal bar">
                <input type="checkbox" checked={showCodes} onChange={e => setShowCodes(e.target.checked)} />
                <span>codes</span>
              </label>
            )}
            {/* Grey HV history picker */}
            <div className="flex items-center gap-0.5 text-[10px]"
                 title="Grey overlay: historical HV-spike zones (vol×N+) over the chart range.">
              <span className="mr-0.5" style={{ color: '#64748b' }}>📊</span>
              {[2, 5, 10].map(v => (
                <button key={v} onClick={() => setHistoryHvTier(historyHvTier === v ? 0 : v)}
                  className={`px-1.5 py-0.5 rounded font-mono border ${
                    historyHvTier === v
                      ? 'bg-slate-600/60 text-slate-100 border-slate-400'
                      : 'bg-md-surface text-md-on-surface-var border-white/10 hover:text-white'}`}>
                  ×{v}
                </button>
              ))}
              {historyHvTier > 0 && historyHvCount > 0 && (
                <span className="ml-0.5 text-slate-400">{historyHvCount}</span>
              )}
            </div>
            {/* Gann pivot picker (also grey lines; confluences with HV → white) */}
            <div className="flex items-center gap-0.5 text-[10px]"
                 title="Grey overlay: historical Gann pivots (swing highs/lows). Where a Gann pivot coincides with an HV-history level, the line is drawn WHITE to mark the confluence.">
              <span className="mr-0.5" style={{ color: '#64748b' }}>📐</span>
              {[5, 10, 20].map(v => (
                <button key={v} onClick={() => setHistoryGannTier(historyGannTier === v ? 0 : v)}
                  className={`px-1.5 py-0.5 rounded font-mono border ${
                    historyGannTier === v
                      ? 'bg-slate-600/60 text-slate-100 border-slate-400'
                      : 'bg-md-surface text-md-on-surface-var border-white/10 hover:text-white'}`}>
                  ±{v}
                </button>
              ))}
              {historyGannTier > 0 && historyGannCount > 0 && (
                <span className="ml-0.5 text-slate-400">{historyGannCount}</span>
              )}
            </div>
            {legend}
            {showBarSelector && (
              <select value={limit} onChange={e => setLimit(Number(e.target.value))}
                className="bg-md-surface border border-md-outline-var rounded-lg px-2 py-1 text-xs text-md-on-surface">
                {BAR_OPTIONS.map(n => <option key={n} value={n}>{n} bars</option>)}
              </select>
            )}
            {loading && <span className="text-xs text-md-on-surface-var animate-pulse">loading…</span>}
            {error && <span className="text-xs text-red-400">{error}</span>}
            <button onClick={() => setFullscreen(f => !f)}
              title={fullscreen ? 'Exit fullscreen (Esc)' : 'Open chart fullscreen with side data'}
              className="ml-1 px-1.5 py-0.5 rounded text-xs hover:bg-white/10 text-md-on-surface-var">
              {fullscreen ? '✕' : '⛶'}
            </button>
          </div>
        </div>
      )}
      {chartBody}
      {showFooter && (
        <div className="px-4 py-1.5 text-[11px] text-md-on-surface-var border-t border-md-outline-var">
          {srcIsDb
            ? 'Data straight from Studio DB — full 6-line code shown on every signal bar (toggle “codes”). These are the exact codes the Sequence Builder matches. May differ from your TradingView chart’s feed.'
            : 'Live feed (ticker not in the Studio DB or intraday) — codes show TZ · L · vol bucket only; the full 6-line suffix/body/gap/line5 exist for daily DB tickers.'}
        </div>
      )}
    </div>
  )

  if (!fullscreen) return inner

  // Fullscreen wrapper: chart on the left, contextual data sidebar on the right.
  return (
    <div className="fixed inset-0 z-[60] flex bg-md-surface">
      {inner}
      <FullscreenSidePanel ticker={ticker} hvZones={hvZones} candles={candlesRef.current}
                           historyHvTier={historyHvTier} historyHvCount={historyHvCount}
                           historyGannTier={historyGannTier} historyGannCount={historyGannCount}
                           zoneSource={zoneSource} extras={sidePanelExtras} />
    </div>
  )
}

function FullscreenSidePanel({ ticker, hvZones, candles, historyHvTier, historyHvCount, historyGannTier, historyGannCount, zoneSource, extras }) {
  const last = candles?.length ? candles[candles.length - 1] : null
  const lastN = candles ? candles.slice(-15).reverse() : []
  const isGann = zoneSource === 'gann'
  return (
    <div className="w-[380px] shrink-0 border-l border-white/10 bg-md-surface-high overflow-y-auto">
      <div className="p-3 border-b border-white/10 sticky top-0 bg-md-surface-high z-10">
        <div className="text-lg font-bold">{ticker}</div>
        {last && (
          <div className="text-xs text-md-on-surface-var mt-1 font-mono">
            <span className="mr-3">${last.close?.toFixed(2)}</span>
            <span className="mr-3">O ${last.open?.toFixed(2)}</span>
            <span className="mr-3">H ${last.high?.toFixed(2)}</span>
            <span>L ${last.low?.toFixed(2)}</span>
          </div>
        )}
      </div>

      {/* Parent-supplied settings (e.g. Gann lookback selector) */}
      {extras && (
        <div className="p-3 border-b border-white/5">
          <div className="text-xs font-semibold mb-2 uppercase tracking-wide text-md-on-surface-var">Settings</div>
          {extras}
        </div>
      )}

      {/* Active zones */}
      <div className="p-3 border-b border-white/5">
        <div className="text-xs font-semibold mb-1 uppercase tracking-wide text-md-on-surface-var">
          Active {isGann ? 'Gann' : 'HV'} zones ({hvZones?.length || 0})
        </div>
        {(!hvZones || !hvZones.length)
          ? <div className="text-xs text-md-on-surface-var/60 italic">none</div>
          : hvZones.map((z, i) => (
            <div key={i} className="text-xs font-mono mb-1 flex items-baseline gap-2"
                 style={{ color: z._color }}>
              <span className="font-bold">Z{i + 1}</span>
              <span>${z.zone_low} – ${z.zone_high}</span>
              <span className="text-md-on-surface-var">{z.trigger_date}</span>
              {z.trigger_vol_mult && <span className="text-amber-300">×{z.trigger_vol_mult}</span>}
              {z.kind && <span className={z.kind==='top'?'text-rose-300':'text-emerald-300'}>{z.kind}</span>}
              {z.direction && <span className={z.direction==='bull'?'text-emerald-400':'text-rose-400'}>{z.direction==='bull'?'▲':'▼'}</span>}
            </div>
          ))}
      </div>

      {/* History summary — both overlays can be on at once; confluence = white */}
      {(historyHvTier > 0 || historyGannTier > 0) && (
        <div className="p-3 border-b border-white/5 text-xs space-y-0.5 font-mono"
             style={{ color: '#94a3b8' }}>
          {historyHvTier > 0 && <div>📊 HV history ×{historyHvTier}+: {historyHvCount}</div>}
          {historyGannTier > 0 && <div>📐 Gann pivots ±{historyGannTier}: {historyGannCount}</div>}
          {historyHvTier > 0 && historyGannTier > 0 && (
            <div className="text-white">⚪ White = confluence (same price level in both)</div>
          )}
        </div>
      )}

      {/* Recent bars table */}
      <div className="p-3">
        <div className="text-xs font-semibold mb-1 uppercase tracking-wide text-md-on-surface-var">
          Recent bars (last 15)
        </div>
        <table className="w-full text-[11px] font-mono">
          <thead><tr className="text-md-on-surface-var border-b border-white/10">
            <th className="text-left px-1 py-0.5">Date</th>
            <th className="text-right px-1 py-0.5">O</th>
            <th className="text-right px-1 py-0.5">H</th>
            <th className="text-right px-1 py-0.5">L</th>
            <th className="text-right px-1 py-0.5">C</th>
            <th className="text-right px-1 py-0.5">Δ%</th>
          </tr></thead>
          <tbody>{lastN.map((b, i) => {
            const prev = lastN[i + 1]
            const ch = prev ? ((b.close - prev.close) / prev.close * 100) : null
            const dCls = ch == null ? 'text-md-on-surface-var' : ch >= 0 ? 'text-emerald-400' : 'text-rose-400'
            return (
              <tr key={b.time} className="border-b border-white/5">
                <td className="px-1 py-0.5">{b.time}</td>
                <td className="px-1 py-0.5 text-right">{b.open?.toFixed(2)}</td>
                <td className="px-1 py-0.5 text-right">{b.high?.toFixed(2)}</td>
                <td className="px-1 py-0.5 text-right">{b.low?.toFixed(2)}</td>
                <td className="px-1 py-0.5 text-right">{b.close?.toFixed(2)}</td>
                <td className={`px-1 py-0.5 text-right ${dCls}`}>{ch == null ? '—' : `${ch>=0?'+':''}${ch.toFixed(2)}`}</td>
              </tr>
            )
          })}</tbody>
        </table>
      </div>
    </div>
  )
}
