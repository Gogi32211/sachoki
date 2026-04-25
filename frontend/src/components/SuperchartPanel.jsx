import { useState, useRef, useEffect, useCallback } from 'react'
import { api } from '../api'

const TF_OPTIONS = ['1d', '4h', '1h', '30m', '15m']

const CELL_W   = 52   // px per bar column
const HDR_W    = 46   // px for the sticky label column

const BUCKET_HEX = { W: '#c3c0d3', L: '#0099ff', N: '#ffd000', B: '#e48100', VB: '#b02020' }

// Signal rows — key matches the field in bar data (or 'tz' special-cased)
const ROWS = [
  {
    key: 'tz', label: 'T/Z',
    chipCls: (s) => s.startsWith('T') ? 'bg-green-900 text-green-300' : 'bg-red-900 text-red-300',
  },
  {
    key: 'l', label: 'L',
    chipCls: (s) => {
      if (s.startsWith('FRI'))   return 'bg-cyan-900 text-cyan-300'
      if (s === 'BL')            return 'bg-sky-900 text-sky-300'
      if (s === 'CCI')           return 'bg-violet-900 text-violet-300'
      if (s === 'RL')            return 'bg-fuchsia-900 text-fuchsia-300'
      if (s === 'RH')            return 'bg-fuchsia-900 text-fuchsia-400'
      if (s === 'PP')            return 'bg-yellow-900 text-yellow-300'
      if (s.includes('↑'))       return 'bg-lime-900 text-lime-300'
      if (s.includes('↓'))       return 'bg-red-900 text-red-400'
      return 'bg-blue-900 text-blue-300'
    },
  },
  {
    key: 'f', label: 'F',
    chipCls: () => 'bg-orange-900 text-orange-300',
  },
  {
    key: 'fly', label: 'FLY',
    chipCls: () => 'bg-purple-900 text-purple-200',
  },
  {
    key: 'g', label: 'G',
    chipCls: () => 'bg-violet-900 text-violet-200',
  },
  {
    key: 'b', label: 'B',
    chipCls: () => 'bg-amber-900 text-amber-300',
  },
  {
    key: 'combo', label: 'CMB',
    chipCls: (s) => {
      if (s === 'ROCKET' || s === 'BUY') return 'bg-green-900 text-green-200 font-bold'
      if (s.includes('↑') || s === '3G') return 'bg-lime-900 text-lime-300'
      if (s.includes('↓') || s === 'CONS' || s === '↓BIAS') return 'bg-red-900 text-red-300'
      return 'bg-teal-900 text-teal-300'
    },
  },
  {
    key: 'vol', label: 'VOL',
    chipCls: () => 'bg-pink-900 text-pink-300 font-bold',
  },
  {
    key: 'vabs', label: 'VABS',
    chipCls: (s) => s.includes('↑') || ['NS', 'ABS', 'CLM', 'LOAD'].includes(s)
      ? 'bg-lime-900 text-lime-300'
      : 'bg-red-900/70 text-red-300',
  },
  {
    key: 'wick', label: 'WICK',
    chipCls: (s) => s.includes('↑') ? 'bg-sky-900 text-sky-300' : 'bg-red-900/50 text-red-300',
  },
]

function barsForTf(tf) {
  return tf === '15m' ? 400 : ['30m', '1h'].includes(tf) ? 300 : tf === '4h' ? 200 : 150
}

function fmtDate(d, isIntraday) {
  if (typeof d === 'number') {
    const dt = new Date(d * 1000)
    if (isIntraday)
      return `${dt.getMonth() + 1}/${dt.getDate()} ${String(dt.getHours()).padStart(2, '0')}:${String(dt.getMinutes()).padStart(2, '0')}`
    return `${dt.getMonth() + 1}/${dt.getDate()}`
  }
  return String(d).slice(5) // MM-DD
}

export default function SuperchartPanel({ initialTicker = 'AAPL', initialTf = '1d', chartInstanceRef, onTickerChange }) {
  const [ticker, setTicker]     = useState(initialTicker)
  const [inputVal, setInputVal] = useState(initialTicker)
  const [tf, setTf]             = useState(initialTf)
  const [bars, setBars]         = useState([])
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState(null)
  const matrixRef   = useRef(null)
  const syncingRef  = useRef(false)

  const isIntraday = ['4h', '1h', '30m', '15m'].includes(tf)

  // Notify parent so global chart follows this ticker/tf
  useEffect(() => { onTickerChange?.(ticker, tf) }, [ticker, tf])

  const load = useCallback((t, f) => {
    setLoading(true)
    setError(null)
    api.barSignals(t, f, barsForTf(f))
      .then(data => {
        setBars(data)
        setTimeout(() => {
          if (matrixRef.current)
            matrixRef.current.scrollLeft = matrixRef.current.scrollWidth
        }, 80)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => { load(ticker, tf) }, [ticker, tf, load])

  // ── Chart ↔ Matrix synchronisation ─────────────────────────────────────────
  useEffect(() => {
    const chart = chartInstanceRef?.current
    if (!chart || !bars.length) return

    const onRangeChange = (range) => {
      if (syncingRef.current || !matrixRef.current || !range) return
      syncingRef.current = true
      const from = Math.max(0, Math.floor(range.from))
      matrixRef.current.scrollLeft = from * CELL_W
      requestAnimationFrame(() => { syncingRef.current = false })
    }

    chart.timeScale().subscribeVisibleLogicalRangeChange(onRangeChange)
    return () => chart.timeScale().unsubscribeVisibleLogicalRangeChange(onRangeChange)
  }, [chartInstanceRef?.current, bars.length])

  const handleMatrixScroll = () => {
    const chart = chartInstanceRef?.current
    if (syncingRef.current || !chart || !matrixRef.current) return
    syncingRef.current = true
    const from   = matrixRef.current.scrollLeft / CELL_W
    const range  = chart.timeScale().getVisibleLogicalRange()
    const width  = range ? range.to - range.from : 20
    chart.timeScale().setVisibleLogicalRange({ from, to: from + width })
    requestAnimationFrame(() => { syncingRef.current = false })
  }

  const handleSubmit = (e) => {
    e.preventDefault()
    const t = inputVal.trim().toUpperCase()
    if (t && t !== ticker) setTicker(t)
  }

  const changeTf = (f) => { setTf(f); setInputVal(ticker) }

  return (
    <div className="p-2 flex flex-col gap-2">
      {/* Controls */}
      <div className="flex items-center gap-2 flex-wrap">
        <form onSubmit={handleSubmit} className="flex gap-1">
          <input
            className="bg-gray-800 text-white text-sm px-2 py-1 rounded border border-gray-700 w-20 uppercase"
            value={inputVal}
            onChange={e => setInputVal(e.target.value.toUpperCase())}
            placeholder="TICKER"
          />
          <button type="submit" className="text-xs px-2 py-1 bg-blue-700 rounded hover:bg-blue-600 text-white">
            Go
          </button>
        </form>
        <div className="flex gap-1">
          {TF_OPTIONS.map(t => (
            <button key={t} onClick={() => changeTf(t)}
              className={`text-xs px-2 py-1 rounded transition-colors
                ${tf === t ? 'bg-blue-600 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
              {t}
            </button>
          ))}
        </div>
        {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
        {error   && <span className="text-xs text-red-400">{error}</span>}
      </div>

      {/* Signal matrix */}
      {bars.length > 0 && (
        <div className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
          <div
            ref={matrixRef}
            className="overflow-x-auto overflow-y-hidden"
            onScroll={handleMatrixScroll}
          >
            <table className="text-xs border-collapse" style={{ tableLayout: 'fixed' }}>
              <thead>
                <tr className="bg-gray-950">
                  {/* Sticky label header */}
                  <th
                    className="sticky left-0 z-10 bg-gray-950 text-gray-600 font-normal
                               px-1 py-0.5 text-right border-r border-gray-800"
                    style={{ width: HDR_W, minWidth: HDR_W }}
                  />
                  {bars.map((b, i) => (
                    <th key={i}
                      className="font-normal px-0 py-0.5 text-center border-r border-gray-900/40"
                      style={{ width: CELL_W, minWidth: CELL_W }}>
                      <span className="text-gray-600 font-mono" style={{ fontSize: 8 }}>
                        {fmtDate(b.date, isIntraday)}
                      </span>
                      <div className="mx-auto mt-px rounded-sm"
                        style={{ width: 28, height: 3, backgroundColor: BUCKET_HEX[b.vol_bucket] ?? '#374151' }} />
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {ROWS.map(row => (
                  <tr key={row.key} className="border-t border-gray-800/60 hover:bg-gray-800/20">
                    <td
                      className="sticky left-0 z-10 bg-gray-900 text-gray-500 px-1 py-0
                                 text-right border-r border-gray-800 font-mono whitespace-nowrap leading-none"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 9 }}>
                      {row.label}
                    </td>
                    {bars.map((b, i) => {
                      const sigs = row.key === 'tz' ? (b.tz ? [b.tz] : []) : (b[row.key] ?? [])
                      return (
                        <td key={i}
                          className="px-0 py-0 text-center border-r border-gray-900/20 align-top"
                          style={{ width: CELL_W, minWidth: CELL_W }}>
                          <div className="flex flex-col gap-px items-center py-px">
                            {sigs.map(s => (
                              <span key={s}
                                className={`px-0.5 rounded font-mono leading-tight ${row.chipCls(s)}`}
                                style={{ fontSize: 8 }}>
                                {s}
                              </span>
                            ))}
                          </div>
                        </td>
                      )
                    })}
                  </tr>
                ))}

                {/* Close price row */}
                <tr className="border-t border-gray-700">
                  <td
                    className="sticky left-0 z-10 bg-gray-900 text-gray-500 px-1 py-0.5
                               text-right border-r border-gray-800 font-mono"
                    style={{ width: HDR_W, minWidth: HDR_W, fontSize: 9 }}>
                    close
                  </td>
                  {bars.map((b, i) => {
                    const prev = i > 0 ? bars[i - 1].close : b.close
                    const up   = b.close >= prev
                    return (
                      <td key={i}
                        className={`px-0 py-0.5 text-center border-r border-gray-900/20 font-mono
                                    ${up ? 'text-green-400' : 'text-red-400'}`}
                        style={{ fontSize: 8, width: CELL_W, minWidth: CELL_W }}>
                        {b.close >= 1000 ? b.close.toFixed(0)
                          : b.close >= 100 ? b.close.toFixed(1)
                          : b.close.toFixed(2)}
                      </td>
                    )
                  })}
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
