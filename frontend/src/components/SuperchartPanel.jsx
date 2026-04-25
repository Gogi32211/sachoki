import { useState, useRef, useEffect, useCallback } from 'react'
import CandleChart from './CandleChart'
import { api } from '../api'

const TF_OPTIONS = ['1d', '4h', '1h', '30m', '15m']

const BUCKET_HEX = { W: '#c3c0d3', L: '#0099ff', N: '#ffd000', B: '#e48100', VB: '#b02020' }

const ROWS = [
  {
    key: 'tz',
    label: 'T/Z',
    chipCls: (s) => s.startsWith('T') ? 'bg-green-900 text-green-300' : 'bg-red-900 text-red-300',
  },
  {
    key: 'l',
    label: 'L/FRI',
    chipCls: (s) => {
      if (s.startsWith('FRI'))  return 'bg-cyan-900 text-cyan-300'
      if (s === 'BL')           return 'bg-sky-900 text-sky-300'
      if (s === 'CCI')          return 'bg-violet-900 text-violet-300'
      if (s === 'RL')           return 'bg-fuchsia-900 text-fuchsia-300'
      if (s === 'RH')           return 'bg-fuchsia-900 text-fuchsia-400'
      if (s.includes('↑'))      return 'bg-lime-900 text-lime-300'
      if (s.includes('↓'))      return 'bg-red-900 text-red-400'
      return 'bg-blue-900 text-blue-300'
    },
  },
  {
    key: 'f',
    label: 'F',
    chipCls: () => 'bg-orange-900 text-orange-300',
  },
  {
    key: 'fly',
    label: 'FLY',
    chipCls: () => 'bg-purple-900 text-purple-300',
  },
  {
    key: 'g',
    label: 'G',
    chipCls: () => 'bg-violet-900 text-violet-200',
  },
  {
    key: 'vabs',
    label: 'VABS',
    chipCls: (s) => s.includes('↑') || ['NS', 'ABS', 'CLM', 'LOAD'].includes(s)
      ? 'bg-lime-900 text-lime-300'
      : 'bg-red-900/70 text-red-300',
  },
  {
    key: 'wick',
    label: 'WICK',
    chipCls: (s) => s.includes('↑') ? 'bg-sky-900 text-sky-300' : 'bg-red-900/50 text-red-300',
  },
]

function fmtDate(d, isIntraday) {
  if (typeof d === 'number') {
    const dt = new Date(d * 1000)
    if (isIntraday) {
      return `${dt.getMonth() + 1}/${dt.getDate()} ${String(dt.getHours()).padStart(2, '0')}:${String(dt.getMinutes()).padStart(2, '0')}`
    }
    return `${dt.getMonth() + 1}/${dt.getDate()}`
  }
  return String(d).slice(5) // MM-DD
}

export default function SuperchartPanel({ initialTicker = 'AAPL' }) {
  const [ticker, setTicker]         = useState(initialTicker)
  const [inputVal, setInputVal]     = useState(initialTicker)
  const [tf, setTf]                 = useState('1d')
  const [bars, setBars]             = useState([])
  const [loading, setLoading]       = useState(false)
  const [error, setError]           = useState(null)
  const matrixRef = useRef(null)

  const isIntraday = ['4h', '1h', '30m', '15m'].includes(tf)

  const load = useCallback((t, f) => {
    setLoading(true)
    setError(null)
    api.barSignals(t, f)
      .then(data => {
        setBars(data)
        setTimeout(() => {
          if (matrixRef.current)
            matrixRef.current.scrollLeft = matrixRef.current.scrollWidth
        }, 60)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => { load(ticker, tf) }, [ticker, tf, load])

  const handleSubmit = (e) => {
    e.preventDefault()
    const t = inputVal.trim().toUpperCase()
    if (t && t !== ticker) setTicker(t)
  }

  return (
    <div className="p-3 flex flex-col gap-3">
      {/* Controls */}
      <div className="flex items-center gap-2 flex-wrap">
        <form onSubmit={handleSubmit} className="flex gap-1">
          <input
            className="bg-gray-800 text-white text-sm px-2 py-1 rounded border border-gray-700 w-24 uppercase"
            value={inputVal}
            onChange={e => setInputVal(e.target.value.toUpperCase())}
            placeholder="TICKER"
          />
          <button
            type="submit"
            className="text-xs px-2 py-1 bg-blue-700 rounded hover:bg-blue-600 text-white"
          >
            Go
          </button>
        </form>

        <div className="flex gap-1">
          {TF_OPTIONS.map(t => (
            <button
              key={t}
              onClick={() => setTf(t)}
              className={`text-xs px-2 py-1 rounded transition-colors
                ${tf === t ? 'bg-blue-600 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}
            >
              {t}
            </button>
          ))}
        </div>

        {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
        {error   && <span className="text-xs text-red-400">{error}</span>}
      </div>

      {/* Chart (no L-combo labels — markers still show T/Z signal names) */}
      <CandleChart ticker={ticker} tf={tf} />

      {/* Signal matrix */}
      {bars.length > 0 && (
        <div className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
          <div ref={matrixRef} className="overflow-x-auto">
            <table className="text-xs border-collapse" style={{ tableLayout: 'fixed' }}>
              <thead>
                <tr className="bg-gray-950">
                  <th className="sticky left-0 z-10 bg-gray-950 text-gray-600 font-normal
                                 px-2 py-1 text-right border-r border-gray-800"
                      style={{ width: 52, minWidth: 52 }}>
                    sig
                  </th>
                  {bars.map((b, i) => (
                    <th
                      key={i}
                      className="font-mono font-normal px-1 py-1 text-center border-r border-gray-900/50"
                      style={{ width: 60, minWidth: 60 }}
                    >
                      <span className="text-gray-600" style={{ fontSize: 9 }}>
                        {fmtDate(b.date, isIntraday)}
                      </span>
                      {/* vol bucket color bar */}
                      <div
                        className="mx-auto mt-0.5 rounded-sm"
                        style={{
                          width: 28,
                          height: 3,
                          backgroundColor: BUCKET_HEX[b.vol_bucket] ?? '#374151',
                        }}
                      />
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {ROWS.map(row => (
                  <tr key={row.key} className="border-t border-gray-800 hover:bg-gray-800/30">
                    <td
                      className="sticky left-0 z-10 bg-gray-900 text-gray-500 px-2 py-1
                                 text-right border-r border-gray-800 font-mono whitespace-nowrap"
                      style={{ width: 52, minWidth: 52 }}
                    >
                      {row.label}
                    </td>
                    {bars.map((b, i) => {
                      const sigs = row.key === 'tz'
                        ? (b.tz ? [b.tz] : [])
                        : (b[row.key] ?? [])
                      return (
                        <td
                          key={i}
                          className="px-0.5 py-0.5 text-center border-r border-gray-900/30 align-top"
                          style={{ width: 60, minWidth: 60 }}
                        >
                          <div className="flex flex-col gap-0.5 items-center">
                            {sigs.map(s => (
                              <span
                                key={s}
                                className={`px-1 rounded font-mono leading-tight ${row.chipCls(s)}`}
                                style={{ fontSize: 9 }}
                              >
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
                    className="sticky left-0 z-10 bg-gray-900 text-gray-500 px-2 py-1
                               text-right border-r border-gray-800 font-mono"
                    style={{ width: 52, minWidth: 52 }}
                  >
                    close
                  </td>
                  {bars.map((b, i) => {
                    const prev = i > 0 ? bars[i - 1].close : b.close
                    const up   = b.close >= prev
                    return (
                      <td
                        key={i}
                        className={`px-0.5 py-1 text-center border-r border-gray-900/30 font-mono
                                    ${up ? 'text-green-400' : 'text-red-400'}`}
                        style={{ fontSize: 9, width: 60, minWidth: 60 }}
                      >
                        {b.close >= 1000
                          ? b.close.toFixed(0)
                          : b.close >= 100
                            ? b.close.toFixed(1)
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
