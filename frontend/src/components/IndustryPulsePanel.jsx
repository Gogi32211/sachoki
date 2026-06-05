import { useState, useEffect } from 'react'
import TickerDrawer from './TickerDrawer'

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(2)}%`)
const CAP_CLS = { mega:'text-amber-200', large:'text-emerald-300', mid:'text-sky-300',
                  small:'text-yellow-400', micro:'text-rose-400', unknown:'text-gray-500' }

export default function IndustryPulsePanel() {
  const [p, setP] = useState(null)
  const [ins, setIns] = useState(null)
  const [err, setErr] = useState(null)
  const [busy, setBusy] = useState(false)
  const [drawerTk, setDrawerTk] = useState(null)
  const load = () => { setBusy(true); fetch('/api/journal/pulse').then(r=>r.json()).then(d=>{setP(d);setBusy(false)}).catch(e=>{setErr(String(e));setBusy(false)}) }
  const loadIns = () => fetch('/api/journal/insider').then(r=>r.json()).then(setIns).catch(()=>{})
  const ingest = () => { setBusy(true); fetch('/api/journal/insider/ingest',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({days:10})}).then(r=>r.json()).then(()=>{loadIns();setBusy(false)}).catch(e=>{setErr(String(e));setBusy(false)}) }
  useEffect(() => { load(); loadIns() }, [])

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
          <Movers title="Top gainers" rows={p.gainers} pos onTicker={setDrawerTk} />
          <Movers title="Top losers" rows={p.losers} onTicker={setDrawerTk} />
          <div>
            <div className="text-sm font-semibold mb-1">Market-cap distribution</div>
            <div className="flex flex-wrap gap-3 text-xs">{Object.entries(p.mcap_dist||{}).map(([k,v])=>(
              <span key={k} className={`font-mono ${CAP_CLS[k]||'text-gray-400'}`}>{k}: {v}</span>))}</div>
          </div>
        </div>
      </div>
      {/* Insider (SEC Form 4) */}
      <div className="pt-2 border-t border-white/10">
        <div className="flex items-center gap-3 mb-1">
          <div className="text-sm font-semibold">🏛 Insider buys (SEC Form 4)</div>
          <button onClick={ingest} disabled={busy} className="px-2 py-0.5 rounded bg-md-surface-high border border-white/15 text-xs hover:bg-white/10 disabled:opacity-50">{busy?'⏳':'↻ Ingest 10d'}</button>
          <span className="text-xs text-md-on-surface-var">кластер = ≥2 разных инсайдера покупают один тикер</span>
        </div>
        {!ins || (!ins.recent?.length && !ins.clusters?.length) ?
          <Empty>Пока нет данных. Нажми «Ingest 10d» (тянет Form 4 из EDGAR, ~1-3 мин, SEC rate-limited).</Empty> :
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
          <div>
            <div className="text-xs font-semibold mb-1 text-amber-300">🔥 Clusters ({ins.clusters?.length||0})</div>
            {ins.clusters?.length ?
            <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
              <th className="px-2 py-1 text-left text-md-on-surface-var">Ticker</th><th className="px-2 py-1 text-right text-md-on-surface-var">#insiders</th>
              <th className="px-2 py-1 text-right text-md-on-surface-var">#buys</th><th className="px-2 py-1 text-right text-md-on-surface-var">$value</th><th className="px-2 py-1 text-right text-md-on-surface-var">last</th></tr></thead>
            <tbody>{ins.clusters.map(c=>(<tr key={c.ticker} className="border-b border-white/5">
              <td className="px-2 py-1"><TkBtn t={c.ticker} onTicker={setDrawerTk} cls="text-amber-300" /></td><td className="px-2 py-1 text-right font-mono">{c.n_insiders}</td>
              <td className="px-2 py-1 text-right font-mono">{c.n_tx}</td><td className="px-2 py-1 text-right font-mono">${Math.round(c.tot_value||0).toLocaleString()}</td>
              <td className="px-2 py-1 text-right font-mono text-md-on-surface-var">{c.last_buy}</td></tr>))}</tbody></table>
            : <Empty>Кластеров нет в окне.</Empty>}
          </div>
          <div>
            <div className="text-xs font-semibold mb-1">Recent buys</div>
            <table className="w-full text-xs"><tbody>{(ins.recent||[]).slice(0,14).map((r,i)=>(<tr key={i} className="border-b border-white/5">
              <td className="px-2 py-0.5"><TkBtn t={r.ticker} onTicker={setDrawerTk} /></td><td className="px-2 py-0.5 text-md-on-surface-var truncate max-w-[150px]" title={r.insider}>{r.insider}</td>
              <td className="px-2 py-0.5 text-right font-mono">${Math.round(r.value||0).toLocaleString()}</td>
              <td className="px-2 py-0.5 text-right font-mono text-md-on-surface-var">{r.tx_date}</td></tr>))}</tbody></table>
          </div>
        </div>}
      </div>
      <div className="text-xs text-md-on-surface-var italic">Industry/sector — слабое альфа-измерение (валидировано: ~+15pp HH в каждом секторе) → контекст, не пики. Insider clusters — реальный сигнал, но его forward-edge (Tier-1, горизонт недели) ещё не валидирован (нужен исторический backfill) — пока показываем как контекст.</div>

      <TickerDrawer ticker={drawerTk} onClose={() => setDrawerTk(null)} />
    </div>
  )
}

function Empty({ children }) { return <div className="text-xs text-md-on-surface-var italic p-2">{children}</div> }

function TkBtn({ t, onTicker, cls='' }) {
  return <button onClick={()=>onTicker?.(t)} className={`font-bold hover:underline cursor-pointer ${cls}`}>{t}</button>
}

function Movers({ title, rows, pos, onTicker }) {
  return (
    <div>
      <div className="text-sm font-semibold mb-1">{title}</div>
      <table className="w-full text-xs"><tbody>{(rows||[]).map(r=>(<tr key={r.ticker} className="border-b border-white/5">
        <td className="px-2 py-0.5"><TkBtn t={r.ticker} onTicker={onTicker} /></td>
        <td className={`px-2 py-0.5 text-right font-mono ${pos?'text-emerald-400':'text-rose-400'}`}>{fmtPct(r.chg)}</td>
        <td className={`px-2 py-0.5 font-mono ${CAP_CLS[r.mcap]||''}`}>{r.mcap}</td>
        <td className="px-2 py-0.5 text-md-on-surface-var">{r.sector}</td>
        <td className="px-2 py-0.5 text-right font-mono text-md-on-surface-var">${r.price}</td></tr>))}</tbody></table>
    </div>
  )
}
