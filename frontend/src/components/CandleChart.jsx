import { useEffect, useRef, useState } from 'react'
import { createChart } from 'lightweight-charts'
import { api } from '../api'

const SIG_COLOR = {
  bull: '#22c55e', // green
  bear: '#ef4444', // red
}

export default function CandleChart({ ticker, tf }) {
  const containerRef = useRef(null)
  const chartRef = useRef(null)
  const seriesRef = useRef(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)

  // Init chart once
  useEffect(() => {
    if (!containerRef.current) return

    const chart = createChart(containerRef.current, {
      layout: { background: { color: '#030712' }, textColor: '#9ca3af' },
      grid: { vertLines: { color: '#1f2937' }, horzLines: { color: '#1f2937' } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: '#374151' },
      timeScale: { borderColor: '#374151', timeVisible: true },
      width: containerRef.current.clientWidth,
      height: 380,
    })

    const series = chart.addCandlestickSeries({
      upColor: '#22c55e',
      downColor: '#ef4444',
      borderUpColor: '#22c55e',
      borderDownColor: '#ef4444',
      wickUpColor: '#22c55e',
      wickDownColor: '#ef4444',
    })

    chartRef.current = chart
    seriesRef.current = series

    const ro = new ResizeObserver(() => {
      if (containerRef.current)
        chart.applyOptions({ width: containerRef.current.clientWidth })
    })
    ro.observe(containerRef.current)

    return () => {
      ro.disconnect()
      chart.remove()
    }
  }, [])

  // Load data when ticker/tf changes
  useEffect(() => {
    if (!seriesRef.current) return
    setError(null)
    setLoading(true)

    api.signals(ticker, tf, 150)
      .then((rows) => {
        const candles = rows
          .filter((r) => r.close != null)
          .map((r) => ({
            time: r.date?.split('T')[0] ?? r.date,
            open: r.open,
            high: r.high,
            low: r.low,
            close: r.close,
          }))

        const markers = rows
          .filter((r) => r.sig_id > 0)
          .map((r) => ({
            time: r.date?.split('T')[0] ?? r.date,
            position: r.is_bull ? 'belowBar' : 'aboveBar',
            color: r.is_bull ? SIG_COLOR.bull : SIG_COLOR.bear,
            shape: r.is_bull ? 'arrowUp' : 'arrowDown',
            text: r.sig_name,
          }))

        seriesRef.current.setData(candles)
        seriesRef.current.setMarkers(markers)
        chartRef.current.timeScale().fitContent()
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, tf])

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <span className="font-semibold text-sm">
          {ticker} <span className="text-gray-500 font-normal">{tf}</span>
        </span>
        {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
        {error && <span className="text-xs text-red-400">{error}</span>}
      </div>
      <div ref={containerRef} className="w-full" />
    </div>
  )
}
