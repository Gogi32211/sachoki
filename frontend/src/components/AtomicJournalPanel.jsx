import { useEffect, useState } from 'react'

const REG_CLS = { RISK_ON: 'bg-emerald-900/50 text-emerald-200 border-emerald-600',
  NEUTRAL: 'bg-amber-900/40 text-amber-200 border-amber-700/50',
  RISK_OFF: 'bg-rose-900/50 text-rose-200 border-rose-600' }
const ATOM_CLS = { 'close=O': 'bg-sky-900/50 text-sky-200', 'gap': 'bg-violet-900/50 text-violet-200',
  'R2L': 'bg-emerald-900/50 text-emerald-200', 'EO': 'bg-amber-900/40 text-amber-200',
  'vol=B': 'bg-teal-900/50 text-teal-200', 'wick=D': 'bg-rose-900/40 text-rose-200', 'G3': 'bg-fuchsia-900/50 text-fuchsia-200' }
const pnlCls = (p) => p == null ? 'text-md-on-surface-var' : p > 0 ? 'text-emerald-400' : p < 0 ? 'text-rose-400' : 'text-md-on-surface-var'

export default function AtomicJournalPanel({ onSelectTicker }) {
  const [data, setData] = useState(null)
  const [busy, setBusy] = useState('')
  const [msg, setMsg] = useState('')
  const load = () => fetch('/api/atomic-journal').then(r => r.json()).then(setData).catch(() => {})
  useEffect(() => { load() }, [])

  const act = (path, label) => {
    setBusy(label)
    fetch(path, { method: 'POST' }).then(r => r.json()).then(d => {
      setMsg(d.opened ? `opened ${d.count}` : d.closed != null ? `graded ${d.graded}, closed ${d.closed}` : (d.error || 'done'))
      setTimeout(() => setMsg(''), 3000); load()
    }).catch(() => {}).finally(() => setBusy(''))
  }
  const s = data?.stats || {}; const reg = data?.regime
  const open = data?.open || []; const closed = data?.closed || []

  return (
    <div className="p-4 text-md-on-surface">
      <h1 className="text-base font-bold mb-1">⚛️📓 Atomic Journal — weak-close gap-up (paper)</h1>
      <p className="text-[11px] text-md-on-surface-var mb-3">
        A <b>separate</b> paper journal for the atomic edge (independent of the main AI Journal). Opens from the atomic
        scan, sizes by regime, exits on <b>−15% stop / +100% target</b> (20-bar horizon, gap-aware). Backtest: positive
        5/6 years. Paper only — no real orders.
      </p>
      <div className="flex items-center gap-3 text-xs mb-3 flex-wrap">
        <span className="px-3 py-1 rounded bg-md-surface-high border border-white/10">equity <b className="font-mono">${(s.equity_live ?? s.equity)?.toLocaleString?.() ?? '—'}</b></span>
        <span>open <b>{s.open ?? 0}</b></span>
        <span>closed <b>{s.closed ?? 0}</b></span>
        <span>win <b>{s.win_rate != null ? s.win_rate + '%' : '—'}</b></span>
        <span>avg P&L <b className={pnlCls(s.avg_pnl)}>{s.avg_pnl != null ? s.avg_pnl + '%' : '—'}</b></span>
        <span>realized <b className={pnlCls(s.total_realized_pct)}>{s.total_realized_pct ?? 0}%</b></span>
        <span>Open P&L (live) <b className={`font-mono ${pnlCls(s.open_pnl_live)}`}>{s.open_pnl_live != null ? (s.open_pnl_live >= 0 ? '+' : '') + '$' + Math.round(s.open_pnl_live) : '—'}</b></span>
        {reg && <span className={`px-2 py-0.5 rounded border ${REG_CLS[reg.label] || ''}`}>{reg.label} · size ×{reg.conv_mult}</span>}
      </div>
      <div className="flex items-center gap-2 text-xs mb-4">
        <button onClick={() => act('/api/atomic-journal/open?top=15&min_score=70', 'open')} disabled={!!busy}
          className="px-3 py-1 rounded border border-fuchsia-600/60 text-fuchsia-200 bg-fuchsia-900/30 hover:bg-fuchsia-900/50 disabled:opacity-40">{busy === 'open' ? '…' : '⚛ Open from scan (score ≥70)'}</button>
        <button onClick={() => act('/api/atomic-journal/grade', 'grade')} disabled={!!busy}
          className="px-3 py-1 rounded border border-sky-700/50 text-sky-300 hover:bg-sky-900/30 disabled:opacity-40">{busy === 'grade' ? '…' : '↻ Grade now'}</button>
        <button onClick={load} disabled={!!busy}
          className="px-3 py-1 rounded border border-white/10 text-md-on-surface-var hover:text-white disabled:opacity-40" title="Refresh live prices / P&L">↻ live</button>
        {reg?.label === 'RISK_OFF' && <span className="text-rose-300/80 text-[11px]">⚠ RISK_OFF — positions auto-sized small (×{reg.conv_mult})</span>}
        {msg && <span className="text-emerald-400 text-[11px]">{msg}</span>}
      </div>

      <h2 className="text-sm font-semibold mb-1">Open ({open.length})</h2>
      <table className="w-full text-xs border border-white/10 rounded overflow-hidden mb-5">
        <thead className="bg-md-surface-high text-md-on-surface-var">
          <tr>
            <th className="text-left px-2 py-1.5">ticker</th><th className="text-left px-2 py-1.5">univ</th>
            <th className="text-right px-2 py-1.5">entry</th><th className="text-right px-2 py-1.5">stop</th>
            <th className="text-right px-2 py-1.5">target</th><th className="text-right px-2 py-1.5">now (live)</th>
            <th className="text-right px-2 py-1.5">uP&L</th><th className="text-right px-2 py-1.5">size</th>
            <th className="text-right px-2 py-1.5">$ buy</th>
            <th className="text-right px-2 py-1.5">sc</th><th className="text-left px-3 py-1.5">atoms</th>
            <th className="text-left px-2 py-1.5">opened</th>
          </tr>
        </thead>
        <tbody>
          {open.map(p => (
            <tr key={p.id} className="border-t border-white/5 hover:bg-white/[0.03]">
              <td className="px-2 py-1.5"><button onClick={() => onSelectTicker?.(p.ticker)} className="font-mono font-semibold hover:text-sky-300">{p.ticker}</button></td>
              <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var/70">{p.universe}</td>
              <td className="text-right px-2 py-1.5 font-mono">${p.entry_px}</td>
              <td className="text-right px-2 py-1.5 font-mono text-rose-300/70">${p.stop_px}</td>
              <td className="text-right px-2 py-1.5 font-mono text-emerald-300/70">${p.target_px}</td>
              <td className="text-right px-2 py-1.5 font-mono">{p.now_px != null ? '$' + p.now_px : '—'}</td>
              <td className={`text-right px-2 py-1.5 font-mono font-bold ${pnlCls(p.upnl_pct)}`}>{p.upnl_pct != null ? (p.upnl_pct > 0 ? '+' : '') + p.upnl_pct + '%' : '—'}</td>
              <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var">{p.size_pct}%</td>
              <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var/70">{p.dollar_buy != null ? '$' + p.dollar_buy.toLocaleString() : '—'}</td>
              <td className="text-right px-2 py-1.5 font-mono font-bold text-fuchsia-300">{p.atomic_score}</td>
              <td className="px-3 py-1.5"><div className="flex flex-wrap gap-1">{(p.atoms || []).map((a, i) => <span key={i} className={`text-[9px] font-mono px-1 rounded border border-white/10 ${ATOM_CLS[a] || ''}`}>{a}</span>)}</div></td>
              <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var/60">{p.open_date}</td>
            </tr>
          ))}
          {!open.length && <tr><td colSpan={12} className="px-3 py-4 text-center text-md-on-surface-var/50">no open positions — click ⚛ Open from scan</td></tr>}
        </tbody>
      </table>

      <h2 className="text-sm font-semibold mb-1">Closed ({closed.length})</h2>
      <table className="w-full text-xs border border-white/10 rounded overflow-hidden">
        <thead className="bg-md-surface-high text-md-on-surface-var">
          <tr>
            <th className="text-left px-2 py-1.5">ticker</th><th className="text-right px-2 py-1.5">entry</th>
            <th className="text-right px-2 py-1.5">exit</th><th className="text-right px-2 py-1.5">P&L</th>
            <th className="text-left px-2 py-1.5">reason</th><th className="text-left px-2 py-1.5">verdict</th>
            <th className="text-left px-2 py-1.5">closed</th>
          </tr>
        </thead>
        <tbody>
          {closed.map(p => (
            <tr key={p.id} className="border-t border-white/5">
              <td className="px-2 py-1.5"><button onClick={() => onSelectTicker?.(p.ticker)} className="font-mono font-semibold hover:text-sky-300">{p.ticker}</button></td>
              <td className="text-right px-2 py-1.5 font-mono">${p.entry_px}</td>
              <td className="text-right px-2 py-1.5 font-mono">${p.exit_px}</td>
              <td className={`text-right px-2 py-1.5 font-mono font-bold ${pnlCls(p.pnl_pct)}`}>{p.pnl_pct > 0 ? '+' : ''}{p.pnl_pct}%</td>
              <td className="px-2 py-1.5 text-[10px]">{p.exit_reason}</td>
              <td className="px-2 py-1.5 text-[10px]">{p.verdict}</td>
              <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var/60">{p.close_date}</td>
            </tr>
          ))}
          {!closed.length && <tr><td colSpan={7} className="px-3 py-4 text-center text-md-on-surface-var/50">no closed positions yet — grade after new bars print</td></tr>}
        </tbody>
      </table>
    </div>
  )
}
