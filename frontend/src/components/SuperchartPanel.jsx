import { useState, useRef, useEffect, useCallback, useMemo, Fragment } from 'react'
import { api } from '../api'

const TF_OPTIONS = ['1d', '4h', '1h', '30m', '15m']
const CELL_W  = 56   // px per bar column
const HDR_W   = 38   // px for the sticky label column
const MINI_H  = 24   // px height of mini-candle row

const BUCKET_HEX = { W: '#c3c0d3', L: '#0099ff', N: '#ffd000', B: '#e48100', VB: '#b02020' }
const PREUP_SET  = new Set(['P2', 'P3', 'P50', 'P89'])

// Row definitions — getSigs(bar) returns array of signal labels
const ROWS = [
  {
    key: 'z',
    label: 'Z',
    getSigs: (b) => {
      const z = b.tz?.startsWith('Z') ? [b.tz] : []
      const p = (b.combo ?? []).filter(s => PREUP_SET.has(s))
      return [...z, ...p]
    },
    chipCls: (s) => PREUP_SET.has(s)
      ? 'bg-gray-700 text-white'
      : 'bg-red-900 text-red-300',
  },
  {
    key: 't',
    label: 'T',
    getSigs: (b) => b.tz?.startsWith('T') ? [b.tz] : [],
    chipCls: () => 'bg-green-900 text-green-300',
  },
  {
    key: 'l',
    label: 'L',
    getSigs: (b) => b.l ?? [],
    chipCls: (s) => {
      if (s.startsWith('FRI'))                   return 'bg-cyan-900 text-cyan-300'
      if (s === 'BL')                            return 'bg-sky-900 text-sky-300'
      if (s === 'CCI' || s === 'CCI0R' || s === 'CCIB') return 'bg-violet-900 text-violet-300'
      if (s === 'RL')                            return 'bg-fuchsia-900 text-fuchsia-300'
      if (s === 'RH')                            return 'bg-fuchsia-900 text-fuchsia-400'
      if (s === 'PP')                            return 'bg-yellow-900 text-yellow-300'
      if (s === 'L555' || s === 'L22')           return 'bg-rose-900 text-rose-300'
      if (s === 'L2L4')                          return 'bg-sky-900 text-sky-400'
      if (s.includes('BE'))                      return 'bg-emerald-900 text-emerald-300'
      if (s.includes('↑'))                       return 'bg-lime-900 text-lime-300'
      if (s.includes('↓'))                       return 'bg-red-900 text-red-400'
      return 'bg-blue-900 text-blue-300'
    },
  },
  {
    key: 'f',
    label: 'F',
    getSigs: (b) => b.f ?? [],
    chipCls: () => 'bg-orange-900 text-orange-300',
  },
  {
    key: 'fly',
    label: 'FLY',
    getSigs: (b) => b.fly ?? [],
    chipCls: () => 'bg-purple-900 text-purple-200',
  },
  {
    key: 'g',
    label: 'G',
    getSigs: (b) => b.g ?? [],
    chipCls: () => 'bg-violet-900 text-violet-200',
  },
  {
    key: 'b',
    label: 'B',
    getSigs: (b) => b.b ?? [],
    chipCls: () => 'bg-amber-900 text-amber-300',
  },
  {
    key: 'combo',
    label: 'I',
    getSigs: (b) => (b.combo ?? []).filter(s => !PREUP_SET.has(s)),
    chipCls: (s) => {
      if (s === 'ROCKET' || s === 'BUY') return 'bg-green-900 text-green-200 font-bold'
      if (s.includes('↑') || s === '3G') return 'bg-lime-900 text-lime-300'
      if (s.includes('↓') || s === 'CONS' || s === '↓BIAS') return 'bg-red-900 text-red-300'
      return 'bg-teal-900 text-teal-300'
    },
  },
  {
    key: 'ultra',
    label: 'ULT',
    getSigs: (b) => b.ultra ?? [],
    chipCls: (s) => {
      if (s === 'BEST↑' || s === '4BF')   return 'bg-yellow-800 text-yellow-200 font-bold'
      if (s === 'FBO↑' || s === 'EB↑' || s === '3↑') return 'bg-lime-900 text-lime-300'
      if (s === 'FBO↓' || s === 'EB↓' || s === '4BF↓') return 'bg-red-900 text-red-300'
      if (s === 'L88')   return 'bg-violet-900 text-violet-200 font-bold'
      if (s === '260308') return 'bg-purple-900 text-purple-300'
      return 'bg-sky-900 text-sky-300'
    },
  },
  {
    key: 'vol',
    label: 'VOL',
    getSigs: (b) => b.vol ?? [],
    chipCls: () => 'bg-pink-900 text-pink-300 font-bold',
  },
  {
    key: 'vabs',
    label: 'VABS',
    getSigs: (b) => b.vabs ?? [],
    chipCls: (s) => {
      if (s === 'BEST★') return 'bg-lime-800 text-lime-200 font-bold'
      if (s === 'STRONG') return 'bg-emerald-900 text-emerald-200'
      if (s.includes('↑') || ['NS', 'ABS', 'CLM', 'LOAD'].includes(s))
        return 'bg-lime-900 text-lime-300'
      return 'bg-red-900/70 text-red-300'
    },
  },
  {
    key: 'wick',
    label: 'WICK',
    getSigs: (b) => b.wick ?? [],
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
  return String(d).slice(5)
}

function MiniCandle({ b, globalMin, globalRange, h = MINI_H }) {
  const cx  = CELL_W / 2
  const bw  = 10
  const toY = (p) => h - ((p - globalMin) / globalRange) * (h - 2) - 1
  const isUp = b.close >= b.open
  const color = isUp ? '#22c55e' : '#ef4444'
  const bodyTop = Math.min(toY(b.open), toY(b.close))
  const bodyH   = Math.max(1, Math.abs(toY(b.open) - toY(b.close)))
  return (
    <svg width={CELL_W} height={h} style={{ display: 'block' }}>
      <line x1={cx} y1={toY(b.high)} x2={cx} y2={toY(b.low)}
            stroke={color} strokeWidth={0.8} />
      <rect x={cx - bw / 2} y={bodyTop} width={bw} height={bodyH} fill={color} />
    </svg>
  )
}

export default function SuperchartPanel({
  initialTicker = 'AAPL', initialTf = '1d',
  onTickerChange,
}) {
  const [ticker, setTicker]       = useState(initialTicker)
  const [inputVal, setInputVal]   = useState(initialTicker)
  const [tf, setTf]               = useState(initialTf)
  const [bars, setBars]           = useState([])
  const [loading, setLoading]     = useState(false)
  const [error, setError]         = useState(null)
  const [showStats, setShowStats] = useState(false)
  const [statsData, setStatsData] = useState(null)
  const [statsLoading, setStatsLoading] = useState(false)
  const [statsSort, setStatsSort] = useState('avg_5bar')
  const matrixRef  = useRef(null)
  const isIntraday = ['4h', '1h', '30m', '15m'].includes(tf)

  // Stats rows sorted by selected column
  const sortedStats = useMemo(() => {
    if (!statsData?.results) return []
    return Object.entries(statsData.results)
      .filter(([, v]) => (v.n ?? 0) >= 3 && !v.warning)
      .sort(([, a], [, b]) => (b[statsSort] ?? -999) - (a[statsSort] ?? -999))
  }, [statsData, statsSort])

  // Mini-candle global price range
  const { globalMin, globalRange } = useMemo(() => {
    if (!bars.length) return { globalMin: 0, globalRange: 1 }
    const lo = Math.min(...bars.map(b => b.low))
    const hi = Math.max(...bars.map(b => b.high))
    return { globalMin: lo, globalRange: (hi - lo) || 1 }
  }, [bars])

  // Notify parent so global chart follows Superchart ticker/tf
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
        }, 120)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  const loadStats = useCallback((t, f) => {
    setStatsLoading(true)
    api.signalStats(t, f, [], false, 3)
      .then(d => setStatsData(d))
      .catch(() => setStatsData(null))
      .finally(() => setStatsLoading(false))
  }, [])

  const exportCsv = useCallback(() => {
    if (!bars.length) return
    const join = (arr) => (arr ?? []).join(' ')
    const headers = [
      'date','open','high','low','close','vol_bucket','turbo_score',
      'Z','T','L','F','FLY','G','B','Combo','ULT','VOL','VABS','WICK',
    ]
    const rows = bars.map(b => [
      b.date,
      b.open?.toFixed(2), b.high?.toFixed(2), b.low?.toFixed(2), b.close?.toFixed(2),
      b.vol_bucket ?? '',
      b.turbo_score ?? 0,
      b.tz?.startsWith('Z') ? b.tz : '',
      b.tz?.startsWith('T') ? b.tz : '',
      join(b.l),
      join(b.f),
      join(b.fly),
      join(b.g),
      join(b.b),
      join((b.combo ?? []).filter(s => !PREUP_SET.has(s))),
      join(b.ultra),
      join(b.vol),
      join(b.vabs),
      join(b.wick),
    ])
    const csv = [headers, ...rows]
      .map(r => r.map(v => `"${String(v ?? '').replace(/"/g, '""')}"`).join(','))
      .join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const a = document.createElement('a')
    a.href = URL.createObjectURL(blob)
    a.download = `${ticker}_${tf}_signals.csv`
    a.click()
    URL.revokeObjectURL(a.href)
  }, [bars, ticker, tf])

  useEffect(() => { load(ticker, tf) }, [ticker, tf, load])

  useEffect(() => {
    if (showStats) { setStatsData(null); loadStats(ticker, tf) }
  }, [ticker, tf])

  const handleSubmit = (e) => {
    e.preventDefault()
    const t = inputVal.trim().toUpperCase()
    if (t && t !== ticker) setTicker(t)
  }

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
            <button key={t} onClick={() => setTf(t)}
              className={`text-xs px-2 py-1 rounded transition-colors
                ${tf === t ? 'bg-blue-600 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
              {t}
            </button>
          ))}
        </div>
        <button
          onClick={() => {
            const next = !showStats
            setShowStats(next)
            if (next && !statsData) loadStats(ticker, tf)
          }}
          className={`text-xs px-2 py-1 rounded transition-colors border
            ${showStats
              ? 'bg-violet-700 border-violet-500 text-white'
              : 'bg-gray-800 border-gray-700 text-gray-400 hover:text-white'}`}>
          📊 Stats
        </button>
        {bars.length > 0 && (
          <button
            onClick={exportCsv}
            title={`Download ${ticker} ${tf.toUpperCase()} signal data as CSV`}
            className="text-xs px-2 py-1 rounded border border-gray-700 bg-gray-800 text-gray-400 hover:text-white transition-colors">
            ⬇ CSV
          </button>
        )}
        {loading && <span className="text-xs text-gray-500 animate-pulse">loading…</span>}
        {error   && <span className="text-xs text-red-400">{error}</span>}
      </div>

      {/* Matrix */}
      {bars.length > 0 && (
        <div className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
          <div
            ref={matrixRef}
            className="overflow-x-auto overflow-y-hidden"
          >
            <table className="text-xs border-collapse" style={{ tableLayout: 'fixed' }}>
              <thead>
                {/* Mini-candle row */}
                <tr className="bg-gray-950">
                  <th style={{ width: HDR_W, minWidth: HDR_W }}
                      className="sticky left-0 z-10 bg-gray-950 border-r border-gray-800" />
                  {bars.map((b, i) => (
                    <th key={i} style={{ width: CELL_W, minWidth: CELL_W, padding: 0 }}
                        className="border-r border-gray-900/30">
                      <MiniCandle b={b} globalMin={globalMin} globalRange={globalRange} />
                    </th>
                  ))}
                </tr>
                {/* Date + vol bucket row */}
                <tr className="bg-gray-950">
                  <th style={{ width: HDR_W, minWidth: HDR_W }}
                      className="sticky left-0 z-10 bg-gray-950 border-r border-gray-800" />
                  {bars.map((b, i) => (
                    <th key={i} style={{ width: CELL_W, minWidth: CELL_W }}
                        className="font-normal px-0 py-0 text-center border-r border-gray-900/40">
                      <div className="flex flex-col items-center gap-px pb-0.5">
                        <span className="text-gray-600 font-mono" style={{ fontSize: 11 }}>
                          {fmtDate(b.date, isIntraday)}
                        </span>
                        <div className="rounded-sm"
                          style={{ width: 28, height: 2, backgroundColor: BUCKET_HEX[b.vol_bucket] ?? '#374151' }} />
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {ROWS.map(row => (
                  <tr key={row.key} className="border-t border-gray-800/50 hover:bg-gray-800/20">
                    <td
                      className="sticky left-0 z-10 bg-gray-900 text-gray-500 px-1
                                 text-right border-r border-gray-800 font-mono whitespace-nowrap"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 11, lineHeight: 1 }}>
                      {row.label}
                    </td>
                    {bars.map((b, i) => {
                      const sigs = row.getSigs(b)
                      return (
                        <td key={i}
                          className="px-0 py-px text-center border-r border-gray-900/20 align-top"
                          style={{ width: CELL_W, minWidth: CELL_W }}>
                          <div className="flex flex-col gap-px items-center">
                            {sigs.map(s => (
                              <span key={s}
                                className={`px-0.5 rounded font-mono leading-none ${row.chipCls(s)}`}
                                style={{ fontSize: 11 }}>
                                {s}
                              </span>
                            ))}
                          </div>
                        </td>
                      )
                    })}
                  </tr>
                ))}

                {/* Turbo score row */}
                <tr className="border-t border-gray-700/60">
                  <td className="sticky left-0 z-10 bg-gray-900 text-gray-400 px-1
                                 text-right border-r border-gray-800 font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 11 }}>
                    turbo
                  </td>
                  {bars.map((b, i) => {
                    const s = b.turbo_score ?? 0
                    const cls = s >= 65 ? 'text-lime-300 font-bold'
                              : s >= 50 ? 'text-green-400 font-bold'
                              : s >= 35 ? 'text-yellow-400'
                              : s >= 20 ? 'text-gray-300'
                              : s > 0   ? 'text-gray-500'
                              : 'text-gray-700'
                    return (
                      <td key={i}
                        className={`px-0 py-0.5 text-center border-r border-gray-900/20 font-mono ${cls}`}
                        style={{ fontSize: 11, width: CELL_W, minWidth: CELL_W }}>
                        {s > 0 ? s : ''}
                      </td>
                    )
                  })}
                </tr>

                {/* Close price row */}
                <tr className="border-t border-gray-700">
                  <td className="sticky left-0 z-10 bg-gray-900 text-gray-500 px-1
                                 text-right border-r border-gray-800 font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 11 }}>
                    close
                  </td>
                  {bars.map((b, i) => {
                    const prev = i > 0 ? bars[i - 1].close : b.close
                    const up   = b.close >= prev
                    return (
                      <td key={i}
                        className={`px-0 py-0.5 text-center border-r border-gray-900/20 font-mono
                                    ${up ? 'text-green-400' : 'text-red-400'}`}
                        style={{ fontSize: 11, width: CELL_W, minWidth: CELL_W }}>
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

      {/* ── Signal Statistics Panel ── */}
      {showStats && (
        <div className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
          <div className="flex items-center gap-3 px-3 py-2 border-b border-gray-800 bg-gray-950">
            <span className="text-xs font-semibold text-violet-300">Signal Performance — {ticker} {tf.toUpperCase()}</span>
            <span className="text-xs text-gray-500">avg max-high over next N bars · sorted by</span>
            {statsLoading && <span className="text-xs text-gray-500 animate-pulse ml-auto">loading…</span>}
          </div>

          {statsLoading ? (
            <div className="p-6 text-xs text-gray-600 text-center animate-pulse">Computing stats for all signals…</div>
          ) : !statsData || statsData.error ? (
            <div className="p-4 text-xs text-red-400">Could not load stats — {statsData?.error ?? 'unknown error'}</div>
          ) : sortedStats.length === 0 ? (
            <div className="p-4 text-xs text-gray-500">Not enough data (need ≥3 occurrences per signal)</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-xs border-collapse">
                <thead>
                  <tr className="border-b border-gray-700 bg-gray-950 text-gray-500 select-none">
                    <th className="text-left px-3 py-1.5 sticky left-0 bg-gray-950 font-normal">Signal</th>
                    {[
                      ['n',         'N',     'occurrences'],
                      ['bull_rate', 'Bull%', 'next bar closed higher'],
                      ['avg_1bar',  '+1bar', 'avg % close next bar'],
                      ['avg_3bar',  'max3',  'avg max-high over 3 bars'],
                      ['avg_5bar',  'max5',  'avg max-high over 5 bars ★'],
                      ['mae_3',     'DD3',   'avg max drawdown over 3 bars'],
                      ['false_rate','False%','% fires with no gain over 3 bars'],
                    ].map(([col, label, title]) => (
                      <th key={col}
                        title={title}
                        onClick={() => setStatsSort(col)}
                        className={`text-right px-2 py-1.5 cursor-pointer whitespace-nowrap font-normal hover:text-white transition-colors
                          ${statsSort === col ? 'text-violet-300 bg-violet-950/40' : ''}`}>
                        {label}{statsSort === col ? ' ▼' : ''}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sortedStats.map(([key, st], idx) => {
                    const label = statsData.labels?.[key] ?? key
                    return (
                      <tr key={key}
                        className={`border-b border-gray-800/40 hover:bg-gray-800/30 ${idx === 0 && statsSort === 'avg_5bar' ? 'bg-violet-950/20' : ''}`}>
                        <td className="px-3 py-1 sticky left-0 bg-gray-900 text-gray-300 whitespace-nowrap font-mono" style={{ fontSize: 11 }}>
                          {label}
                        </td>
                        <td className="px-2 py-1 text-right font-mono text-gray-400">{st.n}</td>
                        <td className={`px-2 py-1 text-right font-mono
                          ${st.bull_rate >= 0.65 ? 'text-lime-300' : st.bull_rate >= 0.55 ? 'text-green-400' : st.bull_rate >= 0.45 ? 'text-yellow-400' : 'text-red-400'}`}>
                          {Math.round(st.bull_rate * 100)}%
                        </td>
                        <td className={`px-2 py-1 text-right font-mono ${st.avg_1bar > 0 ? 'text-green-400' : 'text-red-400'}`}>
                          {st.avg_1bar > 0 ? '+' : ''}{st.avg_1bar?.toFixed(1)}%
                        </td>
                        <td className={`px-2 py-1 text-right font-mono ${st.avg_3bar > 1.5 ? 'text-green-400' : st.avg_3bar > 0 ? 'text-gray-300' : 'text-gray-600'}`}>
                          {st.avg_3bar > 0 ? '+' : ''}{st.avg_3bar?.toFixed(1)}%
                        </td>
                        <td className={`px-2 py-1 text-right font-mono font-semibold
                          ${statsSort === 'avg_5bar' ? 'bg-violet-950/20' : ''}
                          ${st.avg_5bar > 4 ? 'text-lime-300' : st.avg_5bar > 2 ? 'text-green-400' : st.avg_5bar > 0 ? 'text-gray-300' : 'text-gray-600'}`}>
                          {st.avg_5bar > 0 ? '+' : ''}{st.avg_5bar?.toFixed(1)}%
                        </td>
                        <td className="px-2 py-1 text-right font-mono text-red-400/80">
                          {st.mae_3?.toFixed(1)}%
                        </td>
                        <td className={`px-2 py-1 text-right font-mono
                          ${st.false_rate < 0.25 ? 'text-green-400' : st.false_rate < 0.4 ? 'text-yellow-400' : 'text-red-400'}`}>
                          {Math.round(st.false_rate * 100)}%
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
              <div className="px-3 py-2 text-xs text-gray-600">
                {statsData.bars} bars analysed · signals with &lt;3 occurrences hidden · click column header to re-sort
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
