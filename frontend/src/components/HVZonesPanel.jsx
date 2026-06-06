import { useState, useEffect, useMemo } from 'react'
import CodeCandleChart from './CodeCandleChart'
import { zoneColor } from '../utils/zoneColors'

const CAP_CLS = { mega:'text-amber-200', large:'text-emerald-300', mid:'text-sky-300',
                  small:'text-yellow-400', micro:'text-rose-400', unknown:'text-gray-500' }
const TIER_CLS = {
  'x10+':  'bg-cyan-900/60 text-cyan-300 border-cyan-600',
  'x5-10': 'bg-teal-900/60 text-teal-300 border-teal-600',
  'x2-5':  'bg-sky-900/60 text-sky-300 border-sky-600',
}
const REL_CLS = {
  inside:      { color: '#22c55e', shape: '●', label: 'IN',  text: 'inside the zone' },
  cross:       { color: '#eab308', shape: '◆', label: 'CR',  text: 'bar spans the entire zone' },
  touch_below: { color: '#22d3ee', shape: '▲', label: 'TB',  text: 'touched from below' },
  touch_above: { color: '#f472b6', shape: '▼', label: 'TA',  text: 'touched from above' },
  above:       { color: '#94a3b8', shape: '↑', label: 'AB',  text: 'above the zone' },
  below:       { color: '#94a3b8', shape: '↓', label: 'BL',  text: 'below the zone' },
}

const fmtPct = (v) => v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(1)}%`

export default function HVZonesPanel() {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)
  const [selected, setSelected] = useState(null)
  const [tierFilter, setTierFilter] = useState({ 'x10+': true, 'x5-10': true, 'x2-5': true })
  const [relFilter, setRelFilter] = useState({ inside: true, cross: true, touch_below: true, touch_above: true, above: false, below: false })
  const [search, setSearch] = useState('')
  const [zones,  setZones]  = useState([])             // [{trigger_date, zone_low, zone_high, bar_classifications:[...]}]

  const reload = () => fetch('/api/zone-retest/scan').then(r => r.json()).then(setData).catch(e => setErr(String(e)))
  useEffect(() => { reload() }, [])

  // Load ALL zones for the selected ticker (each with its own bar_classifications)
  useEffect(() => {
    if (!selected) { setZones([]); return }
    fetch(`/api/zone-retest/zones/${selected}?classify=true`)
      .then(r => r.json())
      .then(d => setZones(d.zones || []))
      .catch(() => setZones([]))
  }, [selected])

  // Pick the "strongest" relation across all zones for each date — drives one
  // marker per bar on the chart. Priority: cross > touch > inside > others.
  const REL_PRIORITY = { cross: 4, touch_below: 3, touch_above: 3, inside: 2, above: 1, below: 1 }
  const mergedClassifications = useMemo(() => {
    if (!zones.length) return []
    const byDate = new Map()
    for (const z of zones) {
      for (const b of z.bar_classifications || []) {
        const cur = byDate.get(b.date)
        if (!cur || (REL_PRIORITY[b.rel] || 0) > (REL_PRIORITY[cur.rel] || 0)) {
          byDate.set(b.date, b)
        }
      }
    }
    return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date))
  }, [zones])

  const rows = useMemo(() => {
    const arr = (data?.rows || []).filter(r => {
      if (!tierFilter[r.tier]) return false
      if (!relFilter[r.current_rel]) return false
      if (search && !r.ticker.toUpperCase().includes(search.toUpperCase())) return false
      return true
    })
    // sort by ticker (alpha), then by trigger_date desc (newest zone first)
    arr.sort((a, b) => {
      if (a.ticker !== b.ticker) return a.ticker.localeCompare(b.ticker)
      return b.trigger_date.localeCompare(a.trigger_date)
    })
    // tag each row with its index within ticker (1-based) for "Z#" badge
    let prev = null, idx = 0
    arr.forEach(r => {
      if (r.ticker !== prev) { prev = r.ticker; idx = 1 } else { idx += 1 }
      r._zone_idx = idx
    })
    return arr
  }, [data, tierFilter, relFilter, search])

  // Auto-select first ticker when data loads or filter changes
  useEffect(() => {
    if (rows.length && !rows.some(r => r.ticker === selected)) setSelected(rows[0].ticker)
  }, [rows, selected])

  if (err) return <div className="p-4 text-rose-300 text-xs font-mono">{err}</div>
  if (!data) return <div className="p-4 text-md-on-surface-var">Loading…</div>

  return (
    <div className="flex h-full overflow-hidden">
      {/* ── Sidebar ────────────────────────────────────────────────────────── */}
      <div className="w-[380px] shrink-0 border-r border-white/10 flex flex-col">
        <div className="p-3 border-b border-white/10 space-y-2">
          <div className="flex items-center gap-2">
            <div className="text-base font-bold">🎯 HV-Zones</div>
            <div className="text-xs text-md-on-surface-var">
              as-of {data.as_of} · {new Set(rows.map(r => r.ticker)).size} tickers · {rows.length} zones (of {data.count})
            </div>
          </div>
          <input
            value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search ticker…"
            className="w-full px-2 py-1 text-xs rounded bg-md-surface-high border border-white/10 text-md-on-surface placeholder-md-on-surface-var/50"
          />
          <div className="flex gap-1 flex-wrap">
            {['x2-5', 'x5-10', 'x10+'].map(t => (
              <button key={t} onClick={() => setTierFilter(s => ({ ...s, [t]: !s[t] }))}
                className={`px-2 py-0.5 rounded text-[11px] font-semibold border ${tierFilter[t] ? TIER_CLS[t] : 'bg-md-surface text-md-on-surface-var border-white/10'}`}>
                {t}
              </button>
            ))}
          </div>
          <div className="flex gap-1 flex-wrap">
            {Object.entries(REL_CLS).map(([key, v]) => (
              <button key={key} onClick={() => setRelFilter(s => ({ ...s, [key]: !s[key] }))}
                title={v.text}
                className={`px-1.5 py-0.5 rounded text-[10px] font-mono border ${
                  relFilter[key] ? 'bg-md-surface-high border-white/30' : 'opacity-40 bg-md-surface border-white/10'
                }`}
                style={relFilter[key] ? { color: v.color } : {}}>
                {v.shape} {v.label}
              </button>
            ))}
          </div>
        </div>

        <div className="flex-1 overflow-y-auto">
          {rows.map((r, i) => {
            const rel = REL_CLS[r.current_rel] || REL_CLS.inside
            const isSel = r.ticker === selected
            const isFirstOfTicker = r._zone_idx === 1
            return (
              <div key={`${r.ticker}-${r.trigger_date}-${r.zone_low}`} onClick={() => setSelected(r.ticker)}
                className={`px-3 py-2 cursor-pointer border-b border-white/5 hover:bg-white/5 ${
                  isSel ? 'bg-violet-900/30 border-l-2 border-l-violet-500' : ''
                } ${!isFirstOfTicker ? 'border-t-0 pl-7' : ''}`}>
                <div className="flex items-baseline gap-2">
                  {isFirstOfTicker ? (
                    <span className="font-bold text-emerald-300">{r.ticker}</span>
                  ) : (
                    <span className="text-md-on-surface-var/60 text-[10px] font-mono">↳</span>
                  )}
                  <span className="text-[10px] font-mono font-semibold"
                        style={{ color: zoneColor(r._zone_idx - 1) }}
                        title={`Zone ${r._zone_idx} of ${r.n_zones}`}>
                    Z{r._zone_idx}{r.n_zones > 1 ? `/${r.n_zones}` : ''}
                  </span>
                  {isFirstOfTicker && <span className={`text-[10px] font-mono ${CAP_CLS[r.mcap_bucket] || 'text-gray-400'}`}>{r.mcap_bucket}</span>}
                  <span className="text-[10px] text-md-on-surface-var ml-auto">${r.current_close}</span>
                </div>
                <div className="flex items-baseline gap-2 mt-0.5 text-[10px]">
                  <span className={`px-1 rounded font-semibold border ${TIER_CLS[r.tier]}`}>{r.tier}</span>
                  <span style={{ color: rel.color }} className="font-mono" title={rel.text}>{rel.shape} {rel.label}</span>
                  {isFirstOfTicker && <span className="text-md-on-surface-var truncate">{r.sector}</span>}
                </div>
                <div className="mt-0.5 text-[10px] text-md-on-surface-var font-mono">
                  ${r.zone_low}-${r.zone_high} · {r.trigger_date} vol×{r.trigger_vol_mult}
                </div>
              </div>
            )
          })}
          {!rows.length && <div className="p-4 text-xs text-md-on-surface-var italic">No tickers match filters.</div>}
        </div>
      </div>

      {/* ── Main: chart + summary ────────────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto p-3">
        {selected ? (
          <>
            <SelectedHeader row={(data.rows || []).find(r => r.ticker === selected)} />
            <div className="rounded-lg overflow-hidden border border-white/10 mb-3">
              <CodeCandleChart ticker={selected} tf="1d" initialLimit={150}
                height={460} zoneMarkers={mergedClassifications}
                zoneSource="hv" showBarSelector={false} />
            </div>
            <Legend />
            <ZoneSummary zones={zones} />
            <RelMatrix zones={zones} />
          </>
        ) : (
          <div className="text-sm text-md-on-surface-var italic">Pick a ticker on the left.</div>
        )}
      </div>
    </div>
  )
}

function SelectedHeader({ row }) {
  if (!row) return null
  const rel = REL_CLS[row.current_rel] || REL_CLS.inside
  return (
    <div className="mb-3 p-2 rounded-lg bg-md-surface-high border border-white/10 flex flex-wrap items-center gap-3 text-xs">
      <span className="text-lg font-bold text-emerald-300">{row.ticker}</span>
      <span className={`px-1.5 py-0.5 rounded font-semibold border ${TIER_CLS[row.tier]}`}>{row.tier}</span>
      <span className={`font-mono ${CAP_CLS[row.mcap_bucket] || ''}`}>{row.mcap_bucket}</span>
      <span className="text-md-on-surface-var">{row.sector || '—'}</span>
      <div className="mx-2 h-5 border-r border-white/10" />
      <span style={{ color: rel.color }} className="font-mono font-bold">{rel.shape} {rel.text}</span>
      <div className="mx-2 h-5 border-r border-white/10" />
      <span className="font-mono">Price ${row.current_close}</span>
      <span className="font-mono">Zone ${row.zone_low} – ${row.zone_high}</span>
      <span className="font-mono text-md-on-surface-var">trig {row.trigger_date} vol×{row.trigger_vol_mult}</span>
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

function ZoneSummary({ zones }) {
  if (!zones?.length) return null
  return (
    <div className="mb-3">
      <div className="text-xs font-semibold mb-1">Zones ({zones.length})</div>
      <table className="text-xs">
        <thead><tr className="border-b border-white/10 text-md-on-surface-var">
          <th className="text-left px-2 py-1">#</th>
          <th className="text-left px-2 py-1">Trigger</th>
          <th className="text-right px-2 py-1">Zone</th>
          <th className="text-right px-2 py-1">Vol×</th>
          <th className="text-left px-2 py-1">Left date</th>
        </tr></thead>
        <tbody>{zones.map((z, i) => (
          <tr key={i} className="border-b border-white/5">
            <td className="px-2 py-0.5 font-mono font-bold" style={{ color: zoneColor(i) }}>Z{i+1}</td>
            <td className="px-2 py-0.5 font-mono">{z.trigger_date}</td>
            <td className="px-2 py-0.5 text-right font-mono">${z.zone_low} – ${z.zone_high}</td>
            <td className="px-2 py-0.5 text-right font-mono text-amber-300">×{z.trigger_vol_mult}</td>
            <td className="px-2 py-0.5 font-mono text-md-on-surface-var">{z.left_date || '—'}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  )
}

function RelMatrix({ zones }) {
  if (!zones?.length) return null
  // Build union of all bar dates across zones, then matrix [date × zone#] → relation
  const dateSet = new Set()
  for (const z of zones) for (const b of z.bar_classifications || []) dateSet.add(b.date)
  const dates = [...dateSet].sort().slice(-20)   // last 20 bars
  // For each zone, index by date for quick lookup
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
          {zones.map((_, i) => <th key={i} className="text-center px-2 py-1 font-bold" style={{ color: zoneColor(i) }}>Z{i+1}</th>)}
        </tr></thead>
        <tbody>{[...dates].reverse().map(date => {
          // Take OHLC from first zone that has this date
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
