import { useState, useEffect, useMemo } from 'react'
import CodeCandleChart from './CodeCandleChart'

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
  const [zoneClassifications, setZoneClassifications] = useState([])  // per-bar rels for selected ticker

  const reload = () => fetch('/api/zone-retest/scan').then(r => r.json()).then(setData).catch(e => setErr(String(e)))
  useEffect(() => { reload() }, [])

  // Load classified bars for the selected ticker, then push markers to the chart
  useEffect(() => {
    if (!selected) { setZoneClassifications([]); return }
    fetch(`/api/zone-retest/zones/${selected}?classify=true`)
      .then(r => r.json())
      .then(d => setZoneClassifications((d.zones || [])[0]?.bar_classifications || []))
      .catch(() => setZoneClassifications([]))
  }, [selected])

  const rows = useMemo(() => {
    const arr = (data?.rows || []).filter(r => {
      if (!tierFilter[r.tier]) return false
      if (!relFilter[r.current_rel]) return false
      if (search && !r.ticker.toUpperCase().includes(search.toUpperCase())) return false
      return true
    })
    arr.sort((a, b) => b.trigger_vol_mult - a.trigger_vol_mult)
    return arr
  }, [data, tierFilter, relFilter, search])

  // Auto-select first row when data loads or filter changes
  useEffect(() => {
    if (rows.length && !rows.find(r => r.ticker === selected)) setSelected(rows[0].ticker)
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
            <div className="text-xs text-md-on-surface-var">as-of {data.as_of} · {rows.length}/{data.count}</div>
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
          {rows.map(r => {
            const rel = REL_CLS[r.current_rel] || REL_CLS.inside
            const isSel = r.ticker === selected
            return (
              <div key={r.ticker} onClick={() => setSelected(r.ticker)}
                className={`px-3 py-2 cursor-pointer border-b border-white/5 hover:bg-white/5 ${isSel ? 'bg-violet-900/30 border-l-2 border-l-violet-500' : ''}`}>
                <div className="flex items-baseline gap-2">
                  <span className="font-bold text-emerald-300">{r.ticker}</span>
                  <span className={`text-[10px] font-mono ${CAP_CLS[r.mcap_bucket] || 'text-gray-400'}`}>{r.mcap_bucket}</span>
                  <span className="text-[10px] text-md-on-surface-var ml-auto">${r.current_close}</span>
                </div>
                <div className="flex items-baseline gap-2 mt-0.5 text-[10px]">
                  <span className={`px-1 rounded font-semibold border ${TIER_CLS[r.tier]}`}>{r.tier}</span>
                  <span style={{ color: rel.color }} className="font-mono" title={rel.text}>{rel.shape} {rel.label}</span>
                  <span className="text-md-on-surface-var truncate">{r.sector}</span>
                </div>
                <div className="mt-0.5 text-[10px] text-md-on-surface-var font-mono">
                  zone ${r.zone_low}-${r.zone_high} · trig {r.trigger_date} vol×{r.trigger_vol_mult}
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
                height={460} zoneMarkers={zoneClassifications}
                showBarSelector={false} />
            </div>
            <Legend />
            <RelTable bars={zoneClassifications} />
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

function RelTable({ bars }) {
  if (!bars?.length) return null
  // Show last 20 bars, most recent first
  const last = [...bars].slice(-20).reverse()
  return (
    <div>
      <div className="text-xs font-semibold mb-1">Recent bars (last 20)</div>
      <table className="w-full text-xs"><thead><tr className="border-b border-white/10 text-md-on-surface-var">
        <th className="text-left px-2 py-1">Date</th>
        <th className="text-right px-2 py-1">Open</th>
        <th className="text-right px-2 py-1">High</th>
        <th className="text-right px-2 py-1">Low</th>
        <th className="text-right px-2 py-1">Close</th>
        <th className="text-left px-2 py-1">Relation</th>
      </tr></thead>
      <tbody>{last.map((b, i) => {
        const v = REL_CLS[b.rel] || REL_CLS.inside
        return (
          <tr key={i} className="border-b border-white/5">
            <td className="px-2 py-0.5 font-mono">{b.date}</td>
            <td className="px-2 py-0.5 text-right font-mono">{b.open.toFixed(2)}</td>
            <td className="px-2 py-0.5 text-right font-mono">{b.high.toFixed(2)}</td>
            <td className="px-2 py-0.5 text-right font-mono">{b.low.toFixed(2)}</td>
            <td className="px-2 py-0.5 text-right font-mono">{b.close.toFixed(2)}</td>
            <td className="px-2 py-0.5 font-mono" style={{ color: v.color }}>{v.shape} {v.label}</td>
          </tr>
        )
      })}</tbody>
      </table>
    </div>
  )
}
