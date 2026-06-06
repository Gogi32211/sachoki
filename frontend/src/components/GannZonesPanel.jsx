import { useState, useEffect, useMemo } from 'react'
import CodeCandleChart from './CodeCandleChart'
import { zoneColor } from '../utils/zoneColors'

const CAP_CLS = { mega:'text-amber-200', large:'text-emerald-300', mid:'text-sky-300',
                  small:'text-yellow-400', micro:'text-rose-400', unknown:'text-gray-500' }
const REL_CLS = {
  inside:      { color: '#22c55e', shape: '●', label: 'IN',  text: 'inside the zone' },
  cross:       { color: '#eab308', shape: '◆', label: 'CR',  text: 'bar spans the entire zone' },
  touch_below: { color: '#22d3ee', shape: '▲', label: 'TB',  text: 'touched from below' },
  touch_above: { color: '#f472b6', shape: '▼', label: 'TA',  text: 'touched from above' },
  above:       { color: '#94a3b8', shape: '↑', label: 'AB',  text: 'above the zone' },
  below:       { color: '#94a3b8', shape: '↓', label: 'BL',  text: 'below the zone' },
}
const LOOKBACKS = [30, 60, 90, 120, 180]

export default function GannZonesPanel() {
  const [lookback, setLookback] = useState(90)
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)
  const [selected, setSelected] = useState(null)
  const [search, setSearch] = useState('')
  const [showTop, setShowTop] = useState(true)
  const [showBot, setShowBot] = useState(true)
  const [zones, setZones] = useState([])

  const reload = () => {
    setData(null)
    fetch(`/api/gann-zones/scan?lookback=${lookback}`).then(r => r.json())
      .then(setData).catch(e => setErr(String(e)))
  }
  useEffect(() => { reload() }, [lookback])

  useEffect(() => {
    if (!selected) { setZones([]); return }
    fetch(`/api/gann-zones/zones/${selected}?lookback=${lookback}`)
      .then(r => r.json()).then(d => setZones(d.zones || []))
      .catch(() => setZones([]))
  }, [selected, lookback])

  // Merge bar classifications across top+bottom; strongest relation wins per date.
  const REL_PRIORITY = { cross: 4, touch_below: 3, touch_above: 3, inside: 2, above: 1, below: 1 }
  const mergedClassifications = useMemo(() => {
    if (!zones.length) return []
    const byDate = new Map()
    for (const z of zones) {
      for (const b of z.bar_classifications || []) {
        const cur = byDate.get(b.date)
        if (!cur || (REL_PRIORITY[b.rel] || 0) > (REL_PRIORITY[cur.rel] || 0)) byDate.set(b.date, b)
      }
    }
    return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date))
  }, [zones])

  const rows = useMemo(() => {
    const arr = (data?.rows || []).filter(r => {
      if (!showTop && r.current_rel_top === 'inside') {} // we want to keep only those matching enabled kind
      const topOk = showTop && r.current_rel_top === 'inside'
      const botOk = showBot && r.current_rel_bot === 'inside'
      if (!topOk && !botOk) return false
      if (search && !r.ticker.toUpperCase().includes(search.toUpperCase())) return false
      return true
    })
    return arr
  }, [data, search, showTop, showBot])

  useEffect(() => {
    if (rows.length && !rows.find(r => r.ticker === selected)) setSelected(rows[0].ticker)
  }, [rows, selected])

  if (err) return <div className="p-4 text-rose-300 text-xs font-mono">{err}</div>
  if (!data) return <div className="p-4 text-md-on-surface-var">Loading…</div>

  return (
    <div className="flex h-full overflow-hidden">
      <div className="w-[380px] shrink-0 border-r border-white/10 flex flex-col">
        <div className="p-3 border-b border-white/10 space-y-2">
          <div className="flex items-center gap-2">
            <div className="text-base font-bold">📐 Gann Zones</div>
            <div className="text-xs text-md-on-surface-var">{rows.length}/{data.count} · {lookback}d</div>
          </div>
          <div className="text-[10px] text-md-on-surface-var leading-tight">
            "The lowest stick of the highest bar, and the highest stick of the lowest bar" — W.D. Gann.
            Top zone = OHLC of period's HIGHEST high bar. Bottom = LOWEST low bar.
          </div>
          <input
            value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search ticker…"
            className="w-full px-2 py-1 text-xs rounded bg-md-surface-high border border-white/10 text-md-on-surface placeholder-md-on-surface-var/50"
          />
          <div className="flex gap-1 flex-wrap items-center">
            <span className="text-[10px] text-md-on-surface-var mr-1">Lookback:</span>
            {LOOKBACKS.map(lb => (
              <button key={lb} onClick={() => setLookback(lb)}
                className={`px-1.5 py-0.5 rounded text-[10px] font-mono border ${
                  lookback === lb
                    ? 'bg-amber-900/60 text-amber-300 border-amber-600'
                    : 'bg-md-surface text-md-on-surface-var border-white/10'}`}>{lb}d</button>
            ))}
          </div>
          <div className="flex gap-1 flex-wrap">
            <button onClick={() => setShowTop(s => !s)}
              className={`px-2 py-0.5 rounded text-[11px] font-semibold border ${
                showTop ? 'bg-rose-900/40 text-rose-300 border-rose-700' : 'bg-md-surface text-md-on-surface-var border-white/10 opacity-60'}`}>
              In Top Zone (resistance)
            </button>
            <button onClick={() => setShowBot(s => !s)}
              className={`px-2 py-0.5 rounded text-[11px] font-semibold border ${
                showBot ? 'bg-emerald-900/40 text-emerald-300 border-emerald-700' : 'bg-md-surface text-md-on-surface-var border-white/10 opacity-60'}`}>
              In Bottom Zone (support)
            </button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto">
          {rows.map(r => {
            const isSel = r.ticker === selected
            return (
              <div key={r.ticker} onClick={() => setSelected(r.ticker)}
                className={`px-3 py-2 cursor-pointer border-b border-white/5 hover:bg-white/5 ${isSel ? 'bg-amber-900/20 border-l-2 border-l-amber-500' : ''}`}>
                <div className="flex items-baseline gap-2">
                  <span className="font-bold text-amber-300">{r.ticker}</span>
                  <span className={`text-[10px] font-mono ${CAP_CLS[r.mcap_bucket] || 'text-gray-400'}`}>{r.mcap_bucket}</span>
                  <span className="text-[10px] text-md-on-surface-var ml-auto">${r.current_close}</span>
                </div>
                <div className="flex items-baseline gap-2 mt-0.5 text-[10px]">
                  {r.current_rel_top === 'inside' && <span className="px-1 rounded bg-rose-900/30 text-rose-300">▼ in TOP</span>}
                  {r.current_rel_bot === 'inside' && <span className="px-1 rounded bg-emerald-900/30 text-emerald-300">▲ in BOT</span>}
                  <span className="text-md-on-surface-var truncate">{r.sector}</span>
                </div>
                <div className="mt-0.5 text-[10px] text-md-on-surface-var font-mono">
                  top ${r.top_low}-${r.top_high} ({r.top_date}) · bot ${r.bot_low}-${r.bot_high} ({r.bot_date})
                </div>
              </div>
            )
          })}
          {!rows.length && <div className="p-4 text-xs text-md-on-surface-var italic">No tickers in zone.</div>}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-3">
        {selected ? (
          <>
            <Header row={(data.rows || []).find(r => r.ticker === selected)} />
            <div className="rounded-lg overflow-hidden border border-white/10 mb-3">
              <CodeCandleChart ticker={selected} tf="1d" initialLimit={Math.max(200, lookback + 30)}
                height={460} zoneMarkers={mergedClassifications}
                zoneSource="gann" showBarSelector={false}
                sidePanelExtras={
                  <div>
                    <div className="text-[11px] text-md-on-surface-var mb-1">Gann lookback</div>
                    <div className="flex gap-1 flex-wrap">
                      {LOOKBACKS.map(lb => (
                        <button key={lb} onClick={() => setLookback(lb)}
                          className={`px-2 py-0.5 rounded text-[11px] font-mono border ${
                            lookback === lb
                              ? 'bg-amber-900/60 text-amber-300 border-amber-600'
                              : 'bg-md-surface text-md-on-surface-var border-white/10'}`}>{lb}d</button>
                      ))}
                    </div>
                    <div className="mt-2 flex gap-1">
                      <button onClick={() => setShowTop(s => !s)}
                        className={`px-2 py-0.5 rounded text-[11px] font-semibold border ${
                          showTop ? 'bg-rose-900/40 text-rose-300 border-rose-700' : 'bg-md-surface text-md-on-surface-var border-white/10 opacity-60'}`}>
                        TOP zone
                      </button>
                      <button onClick={() => setShowBot(s => !s)}
                        className={`px-2 py-0.5 rounded text-[11px] font-semibold border ${
                          showBot ? 'bg-emerald-900/40 text-emerald-300 border-emerald-700' : 'bg-md-surface text-md-on-surface-var border-white/10 opacity-60'}`}>
                        BOT zone
                      </button>
                    </div>
                  </div>
                } />
            </div>
            <Legend />
            <ZoneTable zones={zones} />
            <RelMatrix zones={zones} />
          </>
        ) : (
          <div className="text-sm text-md-on-surface-var italic">Pick a ticker on the left.</div>
        )}
      </div>
    </div>
  )
}

function Header({ row }) {
  if (!row) return null
  return (
    <div className="mb-3 p-2 rounded-lg bg-md-surface-high border border-white/10 flex flex-wrap items-center gap-3 text-xs">
      <span className="text-lg font-bold text-amber-300">{row.ticker}</span>
      <span className={`font-mono ${CAP_CLS[row.mcap_bucket] || ''}`}>{row.mcap_bucket}</span>
      <span className="text-md-on-surface-var">{row.sector || '—'}</span>
      <div className="mx-2 h-5 border-r border-white/10" />
      <span className="font-mono">Price ${row.current_close}</span>
      {row.current_rel_top === 'inside' && <span className="font-mono text-rose-300">▼ in TOP ${row.top_low}-${row.top_high}</span>}
      {row.current_rel_bot === 'inside' && <span className="font-mono text-emerald-300">▲ in BOT ${row.bot_low}-${row.bot_high}</span>}
    </div>
  )
}

function Legend() {
  return (
    <div className="mb-3 px-2 text-[11px] text-md-on-surface-var flex flex-wrap gap-3">
      <span className="font-semibold">Markers:</span>
      {Object.entries(REL_CLS).map(([k, v]) => (
        <span key={k} style={{ color: v.color }} className="font-mono">{v.shape} {v.text}</span>
      ))}
    </div>
  )
}

function ZoneTable({ zones }) {
  if (!zones?.length) return null
  return (
    <div className="mb-3">
      <div className="text-xs font-semibold mb-1">Zones ({zones.length})</div>
      <table className="text-xs">
        <thead><tr className="border-b border-white/10 text-md-on-surface-var">
          <th className="text-left px-2 py-1">#</th>
          <th className="text-left px-2 py-1">Kind</th>
          <th className="text-left px-2 py-1">Bar date</th>
          <th className="text-right px-2 py-1">Zone</th>
        </tr></thead>
        <tbody>{zones.map((z, i) => (
          <tr key={i} className="border-b border-white/5">
            <td className="px-2 py-0.5 font-mono font-bold" style={{ color: zoneColor(i) }}>Z{i+1}</td>
            <td className={`px-2 py-0.5 font-mono ${z.kind === 'top' ? 'text-rose-300' : 'text-emerald-300'}`}>{z.kind?.toUpperCase()}</td>
            <td className="px-2 py-0.5 font-mono">{z.trigger_date}</td>
            <td className="px-2 py-0.5 text-right font-mono">${z.zone_low} – ${z.zone_high}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  )
}

function RelMatrix({ zones }) {
  if (!zones?.length) return null
  const dateSet = new Set()
  for (const z of zones) for (const b of z.bar_classifications || []) dateSet.add(b.date)
  const dates = [...dateSet].sort().slice(-20)
  const zoneIdx = zones.map(z => {
    const m = new Map()
    for (const b of z.bar_classifications || []) m.set(b.date, b)
    return m
  })
  return (
    <div>
      <div className="text-xs font-semibold mb-1">Recent bars × zones (last 20)</div>
      <div className="overflow-x-auto"><table className="text-xs">
        <thead><tr className="border-b border-white/10 text-md-on-surface-var">
          <th className="text-left px-2 py-1">Date</th>
          <th className="text-right px-2 py-1">Open</th>
          <th className="text-right px-2 py-1">High</th>
          <th className="text-right px-2 py-1">Low</th>
          <th className="text-right px-2 py-1">Close</th>
          {zones.map((z, i) => <th key={i} className="text-center px-2 py-1 font-bold" style={{ color: zoneColor(i) }}>Z{i+1} {z.kind?.[0]?.toUpperCase()}</th>)}
        </tr></thead>
        <tbody>{[...dates].reverse().map(date => {
          let ohlc = null
          for (const m of zoneIdx) if (m.has(date)) { ohlc = m.get(date); break }
          if (!ohlc) return null
          return (
            <tr key={date} className="border-b border-white/5">
              <td className="px-2 py-0.5 font-mono">{date}</td>
              <td className="px-2 py-0.5 text-right font-mono">{ohlc.open.toFixed(2)}</td>
              <td className="px-2 py-0.5 text-right font-mono">{ohlc.high.toFixed(2)}</td>
              <td className="px-2 py-0.5 text-right font-mono">{ohlc.low.toFixed(2)}</td>
              <td className="px-2 py-0.5 text-right font-mono">{ohlc.close.toFixed(2)}</td>
              {zoneIdx.map((m, i) => {
                const b = m.get(date)
                const v = b ? (REL_CLS[b.rel] || REL_CLS.inside) : null
                return <td key={i} className="px-2 py-0.5 text-center font-mono" style={v ? { color: v.color } : {}}>
                  {v ? `${v.shape} ${v.label}` : '·'}
                </td>
              })}
            </tr>
          )
        })}</tbody>
      </table></div>
    </div>
  )
}
