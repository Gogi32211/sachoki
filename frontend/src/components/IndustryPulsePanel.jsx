import { useState, useEffect } from 'react'

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(2)}%`)
const CAP_CLS = { mega:'text-amber-200', large:'text-emerald-300', mid:'text-sky-300',
                  small:'text-yellow-400', micro:'text-rose-400', unknown:'text-gray-500' }

export default function IndustryPulsePanel() {
  const [p, setP] = useState(null)
  const [err, setErr] = useState(null)
  const [busy, setBusy] = useState(false)
  const load = () => { setBusy(true); fetch('/api/journal/pulse').then(r=>r.json()).then(d=>{setP(d);setBusy(false)}).catch(e=>{setErr(String(e));setBusy(false)}) }
  useEffect(() => { load() }, [])

  if (err) return <div className="p-4 text-rose-300 text-xs font-mono">{err}</div>
  if (!p) return <div className="p-4 text-md-on-surface-var">Загрузка Industry Pulse…</div>

  const reg = p.regime || {}
  const regClr = reg.label==='RISK_ON'?'text-emerald-400':reg.label==='RISK_OFF'?'text-rose-400':'text-yellow-400'
  const heat = (v) => v==null ? 'text-gray-500' : v>=1?'text-emerald-400':v>=0?'text-emerald-300/70':v>=-1?'text-rose-300/70':'text-rose-400'

  return (
    <div className="p-4 text-md-on-surface space-y-5">
      <div className="flex items-center gap-4">
        <div className="text-lg font-bold">📡 Industry Pulse <span className="text-xs font-normal text-md-on-surface-var">as-of {p.as_of} · context, not alpha</span></div>
        <div className="flex-1" />
        <button onClick={load} disabled={busy} className="px-3 py-1 rounded bg-md-surface-high border border-white/15 text-sm hover:bg-white/10 disabled:opacity-50">{busy?'⏳':'↻ Refresh'}</button>
      </div>

      {/* Regime */}
      <div className="flex flex-wrap gap-5 p-3 rounded-lg bg-md-surface-high border border-white/10">
        <div><div className="text-xs text-md-on-surface-var">Market regime</div>
          <div className={`text-xl font-bold ${regClr}`}>{reg.label==='RISK_ON'?'🟢':reg.label==='RISK_OFF'?'🔴':'🟡'} {reg.label} · {reg.score}</div></div>
        {reg.breadth && Object.entries({'RSI>50':reg.breadth.pct_rsi_gt50+'%','med RSI':reg.breadth.median_rsi,'phase-D':reg.breadth.pct_phase_D+'%','setups':reg.breadth.setup_density+'%','up-day':reg.breadth.pct_up_day+'%'}).map(([k,v])=>(
          <div key={k}><div className="text-xs text-md-on-surface-var">{k}</div><div className="text-base font-mono">{v}</div></div>))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
        {/* Sector heat */}
        <div>
          <div className="text-sm font-semibold mb-1">Sector heat (день {p.prev}→{p.as_of})</div>
          <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
            <th className="px-2 py-1 text-left text-md-on-surface-var">Sector</th><th className="px-2 py-1 text-right text-md-on-surface-var">n</th>
            <th className="px-2 py-1 text-right text-md-on-surface-var">avg Δ</th><th className="px-2 py-1 text-right text-md-on-surface-var">%up</th>
            <th className="px-2 py-1 text-right text-md-on-surface-var">setups</th><th className="px-2 py-1 text-right text-md-on-surface-var">RSI</th></tr></thead>
          <tbody>{(p.sectors||[]).map(s=>(<tr key={s.sector} className="border-b border-white/5">
            <td className="px-2 py-1 font-semibold">{s.sector}</td><td className="px-2 py-1 text-right font-mono">{s.n}</td>
            <td className={`px-2 py-1 text-right font-mono font-bold ${heat(s.avg_chg)}`}>{fmtPct(s.avg_chg)}</td>
            <td className="px-2 py-1 text-right font-mono">{s.pct_up}%</td><td className="px-2 py-1 text-right font-mono">{s.setup_density}%</td>
            <td className="px-2 py-1 text-right font-mono">{s.med_rsi}</td></tr>))}</tbody></table>
        </div>

        {/* Movers */}
        <div className="space-y-4">
          <Movers title="Top gainers" rows={p.gainers} pos />
          <Movers title="Top losers" rows={p.losers} />
          <div>
            <div className="text-sm font-semibold mb-1">Market-cap distribution</div>
            <div className="flex flex-wrap gap-3 text-xs">{Object.entries(p.mcap_dist||{}).map(([k,v])=>(
              <span key={k} className={`font-mono ${CAP_CLS[k]||'text-gray-400'}`}>{k}: {v}</span>))}</div>
          </div>
        </div>
      </div>
      <div className="text-xs text-md-on-surface-var italic">Industry/sector — слабое альфа-измерение (валидировано: сигнал даёт ~+15pp HH в каждом секторе). Это ситуационный контекст тейпа, не источник пиков. Insider/SEC слой — отдельно (Phase 2).</div>
    </div>
  )
}

function Movers({ title, rows, pos }) {
  return (
    <div>
      <div className="text-sm font-semibold mb-1">{title}</div>
      <table className="w-full text-xs"><tbody>{(rows||[]).map(r=>(<tr key={r.ticker} className="border-b border-white/5">
        <td className="px-2 py-0.5 font-bold">{r.ticker}</td>
        <td className={`px-2 py-0.5 text-right font-mono ${pos?'text-emerald-400':'text-rose-400'}`}>{fmtPct(r.chg)}</td>
        <td className={`px-2 py-0.5 font-mono ${CAP_CLS[r.mcap]||''}`}>{r.mcap}</td>
        <td className="px-2 py-0.5 text-md-on-surface-var">{r.sector}</td>
        <td className="px-2 py-0.5 text-right font-mono text-md-on-surface-var">${r.price}</td></tr>))}</tbody></table>
    </div>
  )
}
