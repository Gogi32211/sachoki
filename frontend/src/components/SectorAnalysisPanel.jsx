import { useState, useEffect, useCallback, useRef } from 'react'
import { api } from '../api'

// ── Metric options for heatmap toggle ─────────────────────────────────────────
const METRICS = [
  { key: 'return_1d',   label: '1D %' },
  { key: 'return_5d',   label: '5D %' },
  { key: 'return_20d',  label: '20D %' },
  { key: 'return_50d',  label: '50D %' },
  { key: 'return_200d', label: '200D %' },
  { key: 'vs_spy_5d',   label: 'vs SPY 5D' },
  { key: 'vs_spy_20d',  label: 'vs SPY 20D' },
]

const SORT_COLS = [
  { key: 'name',        label: 'Sector',      sortable: false },
  { key: 'ticker',      label: 'ETF',         sortable: false },
  { key: 'close',       label: 'Last',        sortable: false },
  { key: 'return_1d',   label: '1D %',        sortable: true  },
  { key: 'return_5d',   label: '5D %',        sortable: true  },
  { key: 'return_20d',  label: '20D %',       sortable: true  },
  { key: 'return_50d',  label: '50D %',       sortable: true  },
  { key: 'return_200d', label: '200D %',      sortable: true  },
  { key: 'vs_spy_1d',   label: 'vSPY 1D',    sortable: true  },
  { key: 'vs_spy_5d',   label: 'vSPY 5D',    sortable: true  },
  { key: 'vs_spy_20d',  label: 'vSPY 20D',   sortable: true  },
  { key: 'ema_stack',   label: 'EMA',         sortable: false },
  { key: 'trend_label', label: 'Trend',       sortable: true  },
]

const TF_OPTIONS = ['1W', '1M', '3M', '6M', 'YTD', '1Y']
const TF_DAYS    = { '1W': 5, '1M': 21, '3M': 63, '6M': 126, '1Y': 252 }

const QUADRANT_DOT = {
  LEADING:   '#4ade80',
  IMPROVING: '#60a5fa',
  WEAKENING: '#fbbf24',
  LAGGING:   '#f87171',
  NEUTRAL:   '#9ca3af',
}

function sliceByTf(dates, arr, tf) {
  if (!dates?.length || !arr?.length) return { dates: [], arr: [] }
  let startIdx = 0
  if (tf === 'YTD') {
    const ytdStart = `${new Date().getFullYear()}-01-01`
    startIdx = dates.findIndex(d => d >= ytdStart)
    if (startIdx === -1) startIdx = 0
  } else {
    startIdx = Math.max(0, dates.length - (TF_DAYS[tf] ?? 63))
  }
  return { dates: dates.slice(startIdx), arr: arr.slice(startIdx) }
}

const SHORT_NAME = {
  'Communication Services': 'Comm Svcs',
  'Consumer Discretionary': 'Cnsmr Disc',
  'Consumer Staples':       'Cnsmr Stapl',
  'Health Care':            'Health Care',
  'Real Estate':            'Real Estate',
}
const shorten = (n) => SHORT_NAME[n] || n

// ── Formatting ─────────────────────────────────────────────────────────────────
const fmt2   = (v) => v == null ? '—' : Number(v).toFixed(2)
const fmtPct = (v) => v == null ? '—' : `${v > 0 ? '+' : ''}${Number(v).toFixed(2)}%`
const fmtPrice = (v) => v == null ? '—' : `$${Number(v).toFixed(2)}`

// ── Color helpers ──────────────────────────────────────────────────────────────
function pctCls(v) {
  if (v == null) return 'text-gray-500'
  if (v >=  1)  return 'text-green-400'
  if (v >   0)  return 'text-green-600'
  if (v === 0)  return 'text-gray-400'
  if (v >  -1)  return 'text-red-500'
  return 'text-red-400'
}

function heatCls(v) {
  if (v == null)  return 'bg-gray-800/60 text-gray-500'
  if (v >=  3)    return 'bg-green-700 text-green-100'
  if (v >=  1.5)  return 'bg-green-800 text-green-200'
  if (v >=  0.5)  return 'bg-green-900/80 text-green-300'
  if (v >   0)    return 'bg-green-950 text-green-400'
  if (v === 0)    return 'bg-gray-800 text-gray-400'
  if (v >  -0.5)  return 'bg-red-950 text-red-400'
  if (v >  -1.5)  return 'bg-red-900/80 text-red-300'
  if (v >  -3)    return 'bg-red-800 text-red-200'
  return 'bg-red-700 text-red-100'
}

function trendCls(t) {
  return { LEADING: 'bg-green-800/70 text-green-300', IMPROVING: 'bg-blue-800/70 text-blue-300',
           WEAKENING: 'bg-amber-800/70 text-amber-300', LAGGING: 'bg-red-800/70 text-red-300',
           NEUTRAL: 'bg-gray-700 text-gray-400' }[t] || 'bg-gray-700 text-gray-500'
}

function emaCls(s) {
  return { BULL: 'bg-green-800/70 text-green-300', PARTIAL_BULL: 'bg-emerald-900 text-emerald-400',
           NEUTRAL: 'bg-gray-700 text-gray-400', PARTIAL_BEAR: 'bg-amber-900/70 text-amber-400',
           BEAR: 'bg-red-800/70 text-red-300' }[s] || 'bg-gray-700 text-gray-500'
}

function regimeCls(m) {
  return { RISK_ON: 'bg-green-900/60 border-green-700 text-green-200',
           RISK_OFF: 'bg-red-900/60 border-red-700 text-red-200',
           NEUTRAL: 'bg-gray-800/60 border-gray-600 text-gray-300' }[m]
       || 'bg-gray-800/60 border-gray-600 text-gray-300'
}

function benchTrendCls(t) {
  return { Bullish: 'bg-green-800/50 text-green-400',
           Weak: 'bg-amber-800/50 text-amber-400',
           'Below key average': 'bg-red-800/50 text-red-400',
           Neutral: 'bg-gray-700 text-gray-400' }[t] || 'bg-gray-700 text-gray-400'
}

// ── Sub-components ─────────────────────────────────────────────────────────────

function MiniChart({ dates, values, label, uid }) {
  if (!values?.length) return (
    <div className="h-12 flex items-center justify-center text-xs text-gray-600">No data</div>
  )
  const valid = values.filter(v => v != null)
  if (!valid.length) return (
    <div className="h-12 flex items-center justify-center text-xs text-gray-600">No data</div>
  )
  const min = Math.min(...valid)
  const max = Math.max(...valid)
  const range = max - min || 1
  const W = 200, H = 44
  const pts = values
    .map((v, i) => v != null
      ? `${(i / Math.max(values.length - 1, 1)) * W},${H - ((v - min) / range) * (H - 2) - 1}`
      : null)
    .filter(Boolean).join(' ')
  const last  = valid[valid.length - 1]
  const first = valid[0]
  const pct   = first !== 0 ? ((last - first) / Math.abs(first)) * 100 : 0
  const isUp  = pct >= 0
  const stroke = isUp ? '#4ade80' : '#f87171'
  const gradId = `mg_${uid}`
  const fillPts = `0,${H} ${pts} ${W},${H}`
  return (
    <div className="flex flex-col gap-0.5">
      <div className="flex items-center justify-between">
        <span className="text-xs text-gray-500">{label}</span>
        <span className={`text-xs font-mono font-bold ${isUp ? 'text-green-400' : 'text-red-400'}`}>
          {isUp ? '+' : ''}{pct.toFixed(2)}%
        </span>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="w-full rounded" style={{ height: 44 }}>
        <defs>
          <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%"   stopColor={stroke} stopOpacity="0.25" />
            <stop offset="100%" stopColor={stroke} stopOpacity="0.0"  />
          </linearGradient>
        </defs>
        <polygon points={fillPts} fill={`url(#${gradId})`} />
        <polyline points={pts} fill="none" stroke={stroke} strokeWidth="1.5" strokeLinejoin="round" />
      </svg>
    </div>
  )
}

function HoldingsTable({ holdings }) {
  if (!holdings?.length) return (
    <div className="text-xs text-gray-600 text-center py-2">Holdings data unavailable.</div>
  )
  return (
    <div className="flex flex-col gap-1">
      <div className="flex items-center justify-between">
        <span className="text-xs text-gray-500 uppercase tracking-wide font-medium">Top Holdings</span>
        <span className="text-xs text-amber-600/70">Static / fallback data</span>
      </div>
      <div className="bg-gray-800/40 rounded-lg overflow-hidden">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-700/60">
              <th className="px-2 py-1 text-left text-gray-500 font-medium">Symbol</th>
              <th className="px-2 py-1 text-left text-gray-500 font-medium">Name</th>
              <th className="px-2 py-1 text-right text-gray-500 font-medium">Weight</th>
            </tr>
          </thead>
          <tbody>
            {holdings.map((h, i) => (
              <tr key={h.symbol || i} className="border-b border-gray-700/30 last:border-0">
                <td className="px-2 py-1 font-mono font-bold text-white">{h.symbol}</td>
                <td className="px-2 py-1 text-gray-400 truncate max-w-[120px]">{h.name}</td>
                <td className="px-2 py-1 text-right font-mono text-gray-300">
                  {h.weight != null ? `${Number(h.weight).toFixed(1)}%` : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function WatchlistExport({ sectors }) {
  const download = (type) => {
    let content, mimeType, filename
    if (type === 'txt') {
      content  = sectors.map(s => s.ticker).join('\n')
      mimeType = 'text/plain'
      filename = 'sector_watchlist.txt'
    } else {
      content  = 'ticker,sector\n' + sectors.map(s => `${s.ticker},${s.name}`).join('\n')
      mimeType = 'text/csv'
      filename = 'sector_watchlist.csv'
    }
    const url = URL.createObjectURL(new Blob([content], { type: mimeType }))
    const a   = document.createElement('a')
    a.href = url; a.download = filename; a.click()
    URL.revokeObjectURL(url)
  }
  return (
    <div className="flex items-center gap-1.5">
      <span className="text-xs text-gray-500">Export:</span>
      <button onClick={() => download('txt')}
        className="text-xs px-2 py-0.5 bg-gray-800 hover:bg-gray-700 text-gray-300 rounded transition-colors">
        TXT
      </button>
      <button onClick={() => download('csv')}
        className="text-xs px-2 py-0.5 bg-gray-800 hover:bg-gray-700 text-gray-300 rounded transition-colors">
        CSV
      </button>
    </div>
  )
}

function BenchCard({ b }) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-lg p-3 flex flex-col gap-1 min-w-0">
      <div className="flex items-center justify-between gap-1">
        <span className="text-sm font-bold text-white font-mono">{b.ticker}</span>
        {b.trend && (
          <span className={`text-xs px-1.5 py-0.5 rounded font-medium shrink-0 ${benchTrendCls(b.trend)}`}>
            {b.trend}
          </span>
        )}
      </div>
      <div className="text-xs text-gray-500 truncate">{b.name}</div>
      <div className="text-base font-mono font-semibold text-white mt-0.5">{fmtPrice(b.close)}</div>
      <div className={`text-sm font-mono font-bold ${pctCls(b.return_1d)}`}>{fmtPct(b.return_1d)}</div>
    </div>
  )
}

function RegimeBanner({ regime }) {
  if (!regime) return null
  const mode = regime.risk_mode || 'NEUTRAL'
  return (
    <div className={`rounded-lg border px-4 py-2.5 ${regimeCls(mode)}`}>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
        <span className="font-bold text-sm tracking-wide">{mode.replace('_', ' ')}</span>
        {regime.strong_sectors?.length > 0 && (
          <span className="text-xs opacity-80">
            <span className="text-green-400">↑</span> Strong: {regime.strong_sectors.join(', ')}
          </span>
        )}
        {regime.weak_sectors?.length > 0 && (
          <span className="text-xs opacity-80">
            <span className="text-red-400">↓</span> Weak: {regime.weak_sectors.join(', ')}
          </span>
        )}
      </div>
      {regime.explanation && (
        <div className="text-xs opacity-70 mt-1 leading-relaxed">{regime.explanation}</div>
      )}
    </div>
  )
}

function SectorHeatmap({ sectors, metric, selected, onSelect }) {
  if (!sectors.length) return (
    <div className="text-xs text-gray-500 py-4 text-center">No sector data</div>
  )
  return (
    <div className="grid gap-2" style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(120px, 1fr))' }}>
      {sectors.map(s => {
        const val  = s[metric]
        const isUp = val != null && val > 0
        const isDn = val != null && val < 0
        return (
          <button
            key={s.ticker}
            onClick={() => onSelect(s.ticker)}
            className={`${heatCls(val)} rounded-xl p-3 flex flex-col items-start gap-1 text-left
                        cursor-pointer transition-all border-2
                        ${selected === s.ticker ? 'border-blue-400 ring-1 ring-blue-400/50' : 'border-transparent'}
                        hover:brightness-125`}
          >
            <div className="flex items-center justify-between w-full">
              <span className="text-sm font-bold font-mono">{s.ticker}</span>
              <span className="text-sm">{isUp ? '↑' : isDn ? '↓' : '—'}</span>
            </div>
            <span className="text-xs opacity-75 leading-tight">{shorten(s.name)}</span>
            <span className="text-sm font-mono font-bold">
              {val == null ? '—' : `${val > 0 ? '+' : ''}${Number(val).toFixed(2)}%`}
            </span>
            {s.trend_label && (
              <span className={`text-xs px-1 py-0.5 rounded font-medium ${trendCls(s.trend_label)}`}>
                {s.trend_label}
              </span>
            )}
          </button>
        )
      })}
    </div>
  )
}

function MoneyFlowSection({ sectors, selected, onSelect }) {
  const buckets = { LEADING: [], IMPROVING: [], WEAKENING: [], LAGGING: [] }
  sectors.forEach(s => {
    const q = s.trend_label
    if (buckets[q]) buckets[q].push(s)
  })

  const leading   = buckets.LEADING.map(s => s.ticker)
  const improving = buckets.IMPROVING.map(s => s.ticker)
  const lagging   = buckets.LAGGING.map(s => s.ticker)

  let summary = ''
  if (leading.length)   summary += `Money is currently concentrated in ${leading.join(' and ')}. `
  if (improving.length) summary += `${improving.join(', ')} ${improving.length === 1 ? 'is' : 'are'} rotating into strength. `
  if (lagging.length)   summary += `${lagging.join(', ')} ${lagging.length === 1 ? 'is' : 'are'} lagging the market.`
  if (!summary) summary = 'Sector rotation direction is unclear. Leadership is mixed.'

  const COLS = [
    { key: 'LEADING',   label: 'Leading',   border: 'border-green-800/60', head: 'text-green-400'  },
    { key: 'IMPROVING', label: 'Improving', border: 'border-blue-800/60',  head: 'text-blue-400'   },
    { key: 'WEAKENING', label: 'Weakening', border: 'border-amber-800/60', head: 'text-amber-400'  },
    { key: 'LAGGING',   label: 'Lagging',   border: 'border-red-800/60',   head: 'text-red-400'    },
  ]

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-4 flex flex-col gap-3">
      <div className="text-xs text-gray-400 uppercase tracking-wide font-medium">Money Flow / Sector Rotation</div>
      <div className="grid grid-cols-2 xl:grid-cols-4 gap-3">
        {COLS.map(({ key, label, border, head }) => (
          <div key={key} className={`border ${border} rounded-lg p-2 flex flex-col gap-1.5`}>
            <div className={`text-xs font-bold uppercase tracking-wide ${head}`}>{label}</div>
            {buckets[key].length === 0
              ? <span className="text-xs text-gray-600 italic">None</span>
              : buckets[key].map(s => (
                  <button key={s.ticker} onClick={() => onSelect(s.ticker)}
                    className={`text-left rounded-md px-2 py-1 transition-colors
                      ${selected === s.ticker ? 'bg-blue-900/40 border border-blue-700/50' : 'bg-gray-800/50 hover:bg-gray-700/50'}`}>
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-mono font-bold text-white">{s.ticker}</span>
                      <span className={`text-xs font-mono ${pctCls(s.return_5d)}`}>{fmtPct(s.return_5d)}</span>
                    </div>
                    <div className="text-xs text-gray-500 truncate">{shorten(s.name)}</div>
                    <div className={`text-xs font-mono ${pctCls(s.vs_spy_20d)}`}>
                      vSPY 20D: {fmtPct(s.vs_spy_20d)}
                    </div>
                  </button>
                ))
            }
          </div>
        ))}
      </div>
      <div className="text-xs text-gray-400 bg-gray-800/40 rounded-lg px-3 py-2 leading-relaxed">{summary}</div>
    </div>
  )
}

function RRGChart({ data, selected, onSelect }) {
  const [hovered, setHovered] = useState(null)
  const valid = (data || []).filter(d => d.rs_ratio != null && d.rs_mom != null)

  if (!valid.length) return (
    <div className="text-xs text-gray-600 text-center py-8">RRG data unavailable.</div>
  )

  const W = 420, H = 340, PAD = 44
  const CX = W / 2, CY = H / 2

  const allX = valid.map(d => d.rs_ratio)
  const allY = valid.map(d => d.rs_mom)
  const xSpan = Math.max(Math.max(...allX) - Math.min(...allX), 3)
  const ySpan = Math.max(Math.max(...allY) - Math.min(...allY), 3)
  const xScale = (W - PAD * 2) / (xSpan * 1.4)
  const yScale = (H - PAD * 2) / (ySpan * 1.4)

  const toSvg = (rx, rm) => ({
    x: CX + (rx - 100) * xScale,
    y: CY - (rm - 100) * yScale,
  })

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full rounded-xl bg-gray-950 border border-gray-800"
      style={{ maxHeight: 340 }}>
      {/* Quadrant fills */}
      <rect x={CX} y={PAD}  width={W-CX-PAD} height={CY-PAD}   fill="#166534" fillOpacity="0.12" />
      <rect x={PAD} y={PAD} width={CX-PAD}   height={CY-PAD}   fill="#1e3a5f" fillOpacity="0.12" />
      <rect x={PAD} y={CY}  width={CX-PAD}   height={H-CY-PAD} fill="#7f1d1d" fillOpacity="0.12" />
      <rect x={CX}  y={CY}  width={W-CX-PAD} height={H-CY-PAD} fill="#713f12" fillOpacity="0.12" />
      {/* Quadrant labels */}
      <text x={W-PAD-4} y={PAD+12}    textAnchor="end"   fill="#4ade80" fontSize="9" opacity="0.7">LEADING</text>
      <text x={PAD+4}   y={PAD+12}    textAnchor="start" fill="#60a5fa" fontSize="9" opacity="0.7">IMPROVING</text>
      <text x={PAD+4}   y={H-PAD-4}   textAnchor="start" fill="#f87171" fontSize="9" opacity="0.7">LAGGING</text>
      <text x={W-PAD-4} y={H-PAD-4}   textAnchor="end"   fill="#fbbf24" fontSize="9" opacity="0.7">WEAKENING</text>
      {/* Axes */}
      <line x1={PAD} y1={CY} x2={W-PAD} y2={CY} stroke="#374151" strokeWidth="1" />
      <line x1={CX}  y1={PAD} x2={CX}  y2={H-PAD} stroke="#374151" strokeWidth="1" />
      <text x={W/2} y={H-6}   textAnchor="middle" fill="#6b7280" fontSize="9">RS Ratio →</text>
      <text x={10}  y={H/2}   textAnchor="middle" fill="#6b7280" fontSize="9"
        transform={`rotate(-90,10,${H/2})`}>Momentum ↑</text>
      {/* Trails */}
      {valid.map(d => {
        const tr = d.trail_ratio, tm = d.trail_mom
        if (!tr?.length || tr.length < 2) return null
        const pts = tr.map((rx, i) => {
          const p = toSvg(rx, tm?.[i] ?? 100)
          return `${p.x},${p.y}`
        }).join(' ')
        return <polyline key={`t_${d.ticker}`} points={pts} fill="none"
          stroke={QUADRANT_DOT[d.trend_label] || '#9ca3af'}
          strokeWidth="1" strokeOpacity="0.3" strokeDasharray="3,2" />
      })}
      {/* Dots */}
      {valid.map(d => {
        const { x, y } = toSvg(d.rs_ratio, d.rs_mom)
        const isSel = selected === d.ticker
        const isHov = hovered  === d.ticker
        const color = QUADRANT_DOT[d.trend_label] || '#9ca3af'
        return (
          <g key={d.ticker} style={{ cursor: 'pointer' }}
            onClick={() => onSelect(d.ticker)}
            onMouseEnter={() => setHovered(d.ticker)}
            onMouseLeave={() => setHovered(null)}>
            <circle cx={x} cy={y} r={isSel ? 9 : isHov ? 8 : 6}
              fill={color} fillOpacity={isSel ? 1 : 0.85}
              stroke={isSel ? '#fff' : 'none'} strokeWidth="2" />
            <text x={x} y={y-10} textAnchor="middle" fill={color} fontSize="9"
              fontWeight={isSel ? 'bold' : 'normal'}>{d.ticker}</text>
          </g>
        )
      })}
      {/* Hover tooltip */}
      {hovered && (() => {
        const d = valid.find(v => v.ticker === hovered)
        if (!d) return null
        const { x, y } = toSvg(d.rs_ratio, d.rs_mom)
        const tx = x > W * 0.7 ? x - 92 : x + 10
        const ty = y < 70 ? y + 10 : y - 60
        return (
          <g>
            <rect x={tx} y={ty} width={90} height={50} rx="4"
              fill="#1f2937" stroke="#374151" strokeWidth="1" />
            <text x={tx+6} y={ty+14} fill="#f9fafb" fontSize="10" fontWeight="bold">{d.ticker}</text>
            <text x={tx+6} y={ty+26} fill="#9ca3af" fontSize="8">{shorten(d.name)}</text>
            <text x={tx+6} y={ty+38} fill="#9ca3af" fontSize="8">
              RS {d.rs_ratio?.toFixed(1)} / Mom {d.rs_mom?.toFixed(1)}
            </text>
          </g>
        )
      })()}
    </svg>
  )
}

function Badge({ cls, label }) {
  if (!label) return <span className="text-gray-600">—</span>
  return <span className={`px-1.5 py-0.5 rounded text-xs font-medium font-mono ${cls}`}>{label}</span>
}

function SectorTable({ sectors, selected, onSelect }) {
  const [col, setCol] = useState('return_1d')
  const [dir, setDir] = useState(-1)  // -1 = desc

  const handleSort = (key) => {
    if (col === key) setDir(d => -d)
    else { setCol(key); setDir(-1) }
  }

  const sorted = [...sectors].sort((a, b) => {
    const av = a[col], bv = b[col]
    if (av == null && bv == null) return 0
    if (av == null) return 1
    if (bv == null) return -1
    return typeof av === 'string' ? av.localeCompare(bv) * dir : (av - bv) * dir
  })

  return (
    <div className="overflow-x-auto rounded-xl border border-gray-800">
      <table className="w-full text-xs border-collapse" style={{ minWidth: 820 }}>
        <thead>
          <tr className="bg-gray-900/90 border-b border-gray-700">
            {SORT_COLS.map(c => (
              <th
                key={c.key}
                onClick={() => c.sortable && handleSort(c.key)}
                className={`px-2 py-2 text-left font-medium whitespace-nowrap select-none
                  ${c.sortable ? 'cursor-pointer hover:text-white' : ''}
                  ${col === c.key ? 'text-blue-400' : 'text-gray-500'}`}
              >
                {c.label}{c.sortable && col === c.key ? (dir === -1 ? ' ↓' : ' ↑') : ''}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map(s => (
            <tr
              key={s.ticker}
              onClick={() => onSelect(s.ticker)}
              className={`border-b border-gray-800/40 cursor-pointer transition-colors
                ${selected === s.ticker
                  ? 'bg-blue-900/25 border-l-2 border-l-blue-500'
                  : 'hover:bg-gray-800/30'}`}
            >
              <td className="px-2 py-1.5 text-gray-300 max-w-[110px] truncate">{s.name}</td>
              <td className="px-2 py-1.5 font-mono font-bold text-white">{s.ticker}</td>
              <td className="px-2 py-1.5 font-mono text-gray-200">{fmt2(s.close)}</td>
              <td className={`px-2 py-1.5 font-mono ${pctCls(s.return_1d)}`}>{fmtPct(s.return_1d)}</td>
              <td className={`px-2 py-1.5 font-mono ${pctCls(s.return_5d)}`}>{fmtPct(s.return_5d)}</td>
              <td className={`px-2 py-1.5 font-mono ${pctCls(s.return_20d)}`}>{fmtPct(s.return_20d)}</td>
              <td className={`px-2 py-1.5 font-mono ${pctCls(s.return_50d)}`}>{fmtPct(s.return_50d)}</td>
              <td className={`px-2 py-1.5 font-mono ${pctCls(s.return_200d)}`}>{fmtPct(s.return_200d)}</td>
              <td className={`px-2 py-1.5 font-mono ${pctCls(s.vs_spy_1d)}`}>{fmtPct(s.vs_spy_1d)}</td>
              <td className={`px-2 py-1.5 font-mono ${pctCls(s.vs_spy_5d)}`}>{fmtPct(s.vs_spy_5d)}</td>
              <td className={`px-2 py-1.5 font-mono ${pctCls(s.vs_spy_20d)}`}>{fmtPct(s.vs_spy_20d)}</td>
              <td className="px-2 py-1.5">
                <Badge cls={emaCls(s.ema_stack)} label={s.ema_stack} />
              </td>
              <td className="px-2 py-1.5">
                <Badge cls={trendCls(s.trend_label)} label={s.trend_label} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function DetailMetricRow({ label, value, isCurrency, isPct, isDiff }) {
  let display, cls
  if (value == null) {
    display = '—'; cls = 'text-gray-600'
  } else if (isPct || isDiff) {
    display = fmtPct(value); cls = pctCls(value)
  } else if (isCurrency) {
    display = fmtPrice(value); cls = 'text-gray-200'
  } else {
    display = fmt2(value); cls = 'text-gray-300'
  }
  return (
    <div className="flex justify-between items-center py-0.5">
      <span className="text-xs text-gray-500">{label}</span>
      <span className={`text-xs font-mono font-medium ${cls}`}>{display}</span>
    </div>
  )
}

function DetailPanel({ etf, detail, loading, error, chartTf, onTfChange }) {
  if (!etf) return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 text-center text-gray-600 text-xs">
      Select a sector to view details
    </div>
  )
  if (loading) return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 text-center text-gray-500 text-xs animate-pulse">
      Loading {etf}…
    </div>
  )
  if (error) return (
    <div className="bg-gray-900 border border-red-900/40 rounded-xl p-4">
      <div className="text-red-400 text-xs">{error}</div>
    </div>
  )
  if (!detail?.data) return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-4 text-xs text-gray-600 text-center">
      No detail data
    </div>
  )

  const d   = detail.data
  const hist = d.history || {}
  const { arr: prices } = sliceByTf(hist.dates, hist.prices, chartTf)
  const { arr: rs }     = sliceByTf(hist.dates, hist.rs,     chartTf)

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-4 flex flex-col gap-3">
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div className="flex flex-col gap-0.5">
          <div className="flex items-center gap-2">
            <span className="text-base font-bold font-mono text-white">{d.ticker}</span>
            {d.trend_label && (
              <span className={`text-xs px-1.5 py-0.5 rounded font-medium ${trendCls(d.trend_label)}`}>
                {d.trend_label}
              </span>
            )}
          </div>
          <span className="text-xs text-gray-500">{d.name}</span>
        </div>
        <div className="text-right shrink-0">
          <div className="text-lg font-mono font-bold text-white">{fmtPrice(d.close)}</div>
          <div className={`text-sm font-mono font-bold ${pctCls(d.return_1d)}`}>{fmtPct(d.return_1d)}</div>
        </div>
      </div>

      {/* Timeframe selector */}
      <div className="flex gap-1 flex-wrap">
        {TF_OPTIONS.map(t => (
          <button key={t} onClick={() => onTfChange?.(t)}
            className={`text-xs px-2 py-0.5 rounded transition-colors
              ${chartTf === t ? 'bg-blue-600 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
            {t}
          </button>
        ))}
      </div>

      {/* Mini charts */}
      <div className="bg-gray-800/40 rounded-lg p-3 flex flex-col gap-3">
        <MiniChart values={prices} label="Price" uid={`${etf}_price`} />
        <MiniChart values={rs}     label="RS vs SPY" uid={`${etf}_rs`} />
      </div>

      {/* EMA Stack badge */}
      {d.ema_stack && (
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500">EMA Stack</span>
          <span className={`text-xs px-2 py-0.5 rounded font-mono font-medium ${emaCls(d.ema_stack)}`}>
            {d.ema_stack}
          </span>
        </div>
      )}

      {/* Performance */}
      <div className="bg-gray-800/40 rounded-lg p-3">
        <div className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-2">Performance</div>
        <DetailMetricRow label="5D Return"   value={d.return_5d}   isPct />
        <DetailMetricRow label="20D Return"  value={d.return_20d}  isPct />
        <DetailMetricRow label="50D Return"  value={d.return_50d}  isPct />
        <DetailMetricRow label="200D Return" value={d.return_200d} isPct />
      </div>

      {/* vs SPY */}
      <div className="bg-gray-800/40 rounded-lg p-3">
        <div className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-2">vs SPY</div>
        <DetailMetricRow label="1D"   value={d.vs_spy_1d}   isDiff />
        <DetailMetricRow label="5D"   value={d.vs_spy_5d}   isDiff />
        <DetailMetricRow label="20D"  value={d.vs_spy_20d}  isDiff />
        <DetailMetricRow label="50D"  value={d.vs_spy_50d}  isDiff />
        <DetailMetricRow label="200D" value={d.vs_spy_200d} isDiff />
      </div>

      {/* Technical */}
      <div className="bg-gray-800/40 rounded-lg p-3">
        <div className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-2">Technical Context</div>
        <DetailMetricRow label="EMA 20"  value={d.ema20}  isCurrency />
        <DetailMetricRow label="EMA 50"  value={d.ema50}  isCurrency />
        <DetailMetricRow label="EMA 200" value={d.ema200} isCurrency />
        <div className="border-t border-gray-700/50 my-1" />
        <DetailMetricRow label="vs EMA 20"  value={d.price_vs_ema20}  isDiff />
        <DetailMetricRow label="vs EMA 50"  value={d.price_vs_ema50}  isDiff />
        <DetailMetricRow label="vs EMA 200" value={d.price_vs_ema200} isDiff />
        <div className="border-t border-gray-700/50 my-1" />
        <div className="flex justify-between items-center py-0.5">
          <span className="text-xs text-gray-500">RS Ratio</span>
          <span className={`text-xs font-mono font-medium ${d.rs_ratio != null ? (d.rs_ratio >= 100 ? 'text-green-400' : 'text-red-400') : 'text-gray-600'}`}>
            {d.rs_ratio != null ? Number(d.rs_ratio).toFixed(2) : '—'}
          </span>
        </div>
        <div className="flex justify-between items-center py-0.5">
          <span className="text-xs text-gray-500">RS Momentum</span>
          <span className={`text-xs font-mono font-medium ${d.rs_mom != null ? (d.rs_mom >= 100 ? 'text-green-400' : 'text-red-400') : 'text-gray-600'}`}>
            {d.rs_mom != null ? Number(d.rs_mom).toFixed(2) : '—'}
          </span>
        </div>
      </div>

      {/* Holdings */}
      <HoldingsTable holdings={d.holdings} />

      {/* Top Movers */}
      <div className="bg-gray-800/40 rounded-lg p-3">
        <div className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-1">Top Movers</div>
        <div className="text-xs text-gray-600">Top movers data unavailable in this version.</div>
      </div>

      {detail.errors?.length > 0 && (
        <div className="text-xs text-amber-600/70">⚠ {detail.errors[0]}</div>
      )}
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────
export default function SectorAnalysisPanel({ onSelectTicker }) {
  const [overview,   setOverview]   = useState(null)
  const [ovLoading,  setOvLoading]  = useState(true)
  const [ovError,    setOvError]    = useState(null)

  const [selectedEtf, setSelectedEtf] = useState('XLK')
  const [detail,      setDetail]      = useState(null)
  const [detLoading,  setDetLoading]  = useState(false)
  const [detError,    setDetError]    = useState(null)

  const [rrgData,    setRrgData]    = useState(null)
  const [rrgLoading, setRrgLoading] = useState(false)

  const [heatMetric, setHeatMetric] = useState('return_1d')
  const [chartTf,    setChartTf]    = useState('3M')

  // Overview fetch (once on mount)
  useEffect(() => {
    setOvLoading(true)
    setOvError(null)
    api.sectorOverview()
      .then(d => { setOverview(d); setOvLoading(false) })
      .catch(e => { setOvError(e.message); setOvLoading(false) })
  }, [])

  useEffect(() => {
    setRrgLoading(true)
    api.sectorRRG(12)
      .then(d => { setRrgData(d?.data || []); setRrgLoading(false) })
      .catch(() => { setRrgData([]); setRrgLoading(false) })
  }, [])

  // Detail fetch whenever selected ETF changes
  useEffect(() => {
    if (!selectedEtf) return
    setDetLoading(true)
    setDetError(null)
    api.sectorDetail(selectedEtf)
      .then(d => { setDetail(d); setDetLoading(false) })
      .catch(e => { setDetError(e.message); setDetLoading(false) })
  }, [selectedEtf])

  const handleSelect = useCallback((etf) => {
    setSelectedEtf(etf)
    onSelectTicker?.(etf)
  }, [onSelectTicker])

  // ── States ─────────────────────────────────────────────────────────────────
  if (ovLoading) return (
    <div className="p-8 text-center text-gray-500 text-sm animate-pulse">
      Loading sector data…
    </div>
  )

  if (ovError) return (
    <div className="p-4 bg-gray-900 border border-red-900/50 rounded-xl">
      <div className="text-red-400 text-sm font-medium mb-1">Failed to load sector overview</div>
      <div className="text-red-500/70 text-xs font-mono">{ovError}</div>
      <button
        onClick={() => { setOvLoading(true); setOvError(null); api.sectorOverview().then(setOverview).catch(e => setOvError(e.message)).finally(() => setOvLoading(false)) }}
        className="mt-3 text-xs px-3 py-1 bg-gray-800 hover:bg-gray-700 text-gray-300 rounded"
      >
        Retry
      </button>
    </div>
  )

  if (!overview?.data) return (
    <div className="p-6 text-center text-gray-500 text-sm">No sector data available</div>
  )

  const { sectors = [], benchmarks = [], regime } = overview.data

  return (
    <div className="flex flex-col gap-3 p-1">

      {/* 1 ── Benchmark Cards + Export */}
      <div className="flex flex-col gap-2">
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
          {benchmarks.length > 0
            ? benchmarks.map(b => <BenchCard key={b.ticker} b={b} />)
            : ['SPY','QQQ','IWM','DIA'].map(t => (
                <div key={t} className="bg-gray-900 border border-gray-800 rounded-lg p-3 text-xs text-gray-600">{t} —</div>
              ))
          }
        </div>
        {sectors.length > 0 && <WatchlistExport sectors={sectors} />}
      </div>

      {/* 2 ── Risk Banner */}
      <RegimeBanner regime={regime} />

      {/* 3 ── Sector Heatmap */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-4 flex flex-col gap-3">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-gray-400 uppercase tracking-wide font-medium">Sector Heatmap</span>
          <div className="flex gap-1 flex-wrap">
            {METRICS.map(m => (
              <button key={m.key} onClick={() => setHeatMetric(m.key)}
                className={`text-xs px-2 py-0.5 rounded transition-colors
                  ${heatMetric === m.key ? 'bg-blue-600 text-white font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
                {m.label}
              </button>
            ))}
          </div>
        </div>
        <SectorHeatmap sectors={sectors} metric={heatMetric} selected={selectedEtf} onSelect={handleSelect} />
      </div>

      {/* 4 ── Money Flow / Rotation */}
      {sectors.length > 0 && (
        <MoneyFlowSection sectors={sectors} selected={selectedEtf} onSelect={handleSelect} />
      )}

      {/* 5+6 ── Table + Detail side by side */}
      <div className="flex flex-col xl:flex-row gap-3">

        {/* Overview Table */}
        <div className="flex-1 min-w-0">
          <div className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-1.5 px-1">
            Sector Overview — {sectors.length} ETFs
          </div>
          {sectors.length === 0
            ? <div className="text-xs text-gray-600 py-4 text-center">No sector data returned</div>
            : <SectorTable sectors={sectors} selected={selectedEtf} onSelect={handleSelect} />
          }
        </div>

        {/* Detail Panel */}
        <div className="xl:w-72 flex-shrink-0">
          <div className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-1.5 px-1">Detail</div>
          <DetailPanel etf={selectedEtf} detail={detail} loading={detLoading} error={detError}
            chartTf={chartTf} onTfChange={setChartTf} />
        </div>
      </div>

      {/* 7 ── Sector Rotation Map (RRG) */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-4 flex flex-col gap-2">
        <div className="text-xs text-gray-400 uppercase tracking-wide font-medium">Sector Rotation Map</div>
        <div className="text-xs text-gray-500">X-axis: RS Ratio vs SPY · Y-axis: RS Momentum · Dashed lines = trails</div>
        {rrgLoading
          ? <div className="h-40 flex items-center justify-center text-gray-500 text-xs animate-pulse">Loading RRG…</div>
          : <RRGChart data={rrgData} selected={selectedEtf} onSelect={handleSelect} />
        }
      </div>

      {/* Backend data warnings */}
      {overview.errors?.length > 0 && (
        <div className="text-xs text-amber-600/60 px-1">
          ⚠ Data issues for: {overview.errors.slice(0, 3).map(e => e.split(':')[0]).join(', ')}
        </div>
      )}
    </div>
  )
}
