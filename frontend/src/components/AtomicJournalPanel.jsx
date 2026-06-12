import { useEffect, useState } from 'react'

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(1)}%`)
const fmtNum = (v) => (v == null ? '—' : Number(v).toLocaleString())
const CAP_CLS = { mega: 'text-amber-200', large: 'text-emerald-300', mid: 'text-sky-300', small: 'text-violet-300', micro: 'text-rose-300' }
const pnlC = (p) => p == null ? '' : p >= 0 ? 'text-emerald-400' : 'text-rose-400'
const ATOM_CLS = { 'close=O': 'bg-sky-900/50 text-sky-200', 'gap': 'bg-violet-900/50 text-violet-200',
  'R2L': 'bg-emerald-900/50 text-emerald-200', 'EO': 'bg-amber-900/40 text-amber-200',
  'vol=B': 'bg-teal-900/50 text-teal-200', 'wick=D': 'bg-rose-900/40 text-rose-200', 'G3': 'bg-fuchsia-900/50 text-fuchsia-200' }

const Kpi = ({ label, v }) => <div><div className="text-xs text-md-on-surface-var">{label}</div><div className="text-lg font-bold font-mono">{v}</div></div>
const Th = ({ children, r }) => <th className={`px-2 py-1 font-semibold text-md-on-surface-var ${r ? 'text-right' : 'text-left'}`}>{children}</th>
const Td = ({ children, r, cls = '' }) => <td className={`px-2 py-1 font-mono ${r ? 'text-right' : 'text-left'} ${cls}`}>{children}</td>
const Cap = ({ b }) => { const k = b || 'unknown'; return <td className={`px-2 py-1 font-mono ${CAP_CLS[k] || 'text-gray-400'}`}>{k}</td> }
const Empty = ({ children }) => <div className="text-md-on-surface-var/50 text-xs py-6 text-center">{children}</div>
const Atoms = ({ a }) => <div className="flex flex-wrap gap-1">{(a || []).map((x, i) => <span key={i} className={`text-[9px] font-mono px-1 rounded border border-white/10 ${ATOM_CLS[x] || ''}`}>{x}</span>)}</div>

export default function AtomicJournalPanel({ onSelectTicker }) {
  const [data, setData] = useState(null)
  const [busy, setBusy] = useState('')
  const [msg, setMsg] = useState('')
  const [sub, setSub] = useState('positions')
  const load = () => fetch('/api/atomic-journal').then(r => r.json()).then(setData).catch(() => {})
  useEffect(() => { load() }, [])
  const act = (path, label) => {
    setBusy(label)
    fetch(path, { method: 'POST' }).then(r => r.json()).then(d => {
      setMsg(d.opened ? `opened ${d.count}` : d.closed != null ? `graded ${d.graded}, closed ${d.closed}` : (d.error || 'done'))
      setTimeout(() => setMsg(''), 3000); load()
    }).catch(() => {}).finally(() => setBusy(''))
  }
  const s = data?.stats || {}; const reg = data?.regime || {}
  const open = data?.open || []; const closed = data?.closed || []
  const equity = s.equity_live ?? s.equity ?? 0
  const eqPct = ((equity - 10000) / 10000) * 100

  return (
    <div className="p-4 text-md-on-surface">
      <p className="text-[11px] text-md-on-surface-var mb-2">⚛️📓 <b>Atomic Journal</b> — a separate, <b>mechanical</b> paper journal for the weak-close gap-up edge (close=O + gap). Rule-based exits (−15% stop / +100% target, 20-bar). Paper only.</p>
      {/* KPI strip — identical layout to the AI Journal */}
      <div className="flex flex-wrap items-center gap-4 mb-4 p-3 rounded-lg bg-md-surface-high border border-white/10">
        <div><div className="text-xs text-md-on-surface-var">Equity</div>
          <div className="text-lg font-bold font-mono">${fmtNum(Math.round(equity))} <span className={eqPct >= 0 ? 'text-emerald-400' : 'text-rose-400'}>{fmtPct(eqPct)}</span></div></div>
        <Kpi label="Open" v={s.open ?? 0} />
        <Kpi label="Pending" v={s.pending ?? 0} />
        <Kpi label="Closed" v={s.closed ?? 0} />
        <Kpi label="Win rate" v={s.win_rate == null ? '—' : `${Math.round(s.win_rate)}%`} />
        <Kpi label="Avg ret" v={s.avg_pnl == null ? '—' : fmtPct(s.avg_pnl)} />
        <div><div className="text-xs text-md-on-surface-var">Open P&L (live)</div>
          <div className={`text-lg font-bold font-mono ${(s.open_pnl_live ?? 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{s.open_pnl_live != null ? `${s.open_pnl_live >= 0 ? '+' : ''}$${Math.abs(s.open_pnl_live).toFixed(0)}` : '—'}</div></div>
        {reg.label && <div title={`breadth ${reg.score}`}>
          <div className="text-xs text-md-on-surface-var">Regime</div>
          <div className={`text-lg font-bold ${reg.label === 'RISK_ON' ? 'text-emerald-400' : reg.label === 'RISK_OFF' ? 'text-rose-400' : 'text-yellow-400'}`}>
            {reg.label === 'RISK_ON' ? '🟢' : reg.label === 'RISK_OFF' ? '🔴' : '🟡'} {reg.score}</div></div>}
        <div className="flex-1" />
        <button onClick={() => act('/api/atomic-journal/open?top=15&min_score=70', 'open')} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-violet-700 hover:bg-violet-600 text-white text-sm font-semibold disabled:opacity-50">
          {busy === 'open' ? '⏳…' : '⚛ Open from scan'}</button>
        <button onClick={() => act('/api/atomic-journal/grade', 'grade')} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface border border-white/15 hover:bg-white/10 text-sm disabled:opacity-50">
          {busy === 'grade' ? '⏳ Grading…' : 'Grade now'}</button>
        <button onClick={load} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface border border-white/15 hover:bg-white/10 text-sm disabled:opacity-50" title="Refresh live prices / P&L">↻ live</button>
      </div>

      <div className="mb-3 text-xs text-md-on-surface-var">
        Edge: weak-close gap-up · entry next-open · −15% stop / +100% target · regime-sized (×{reg.conv_mult ?? 1}).
        {reg.label === 'RISK_OFF' && <span className="text-rose-300/80"> ⚠ RISK_OFF — positions auto-sized small.</span>}
        {msg && <span className="text-emerald-400 ml-2">{msg}</span>}
      </div>

      {/* Sub-tabs */}
      <div className="flex gap-1 mb-3">
        {['positions', 'replay', 'knowledge'].map(t => (
          <button key={t} onClick={() => setSub(t)}
            className={`px-3 py-1 rounded text-sm capitalize ${sub === t ? 'bg-violet-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{t}</button>
        ))}
      </div>

      {sub === 'positions' ? (
        <>
          <div className="text-sm font-semibold mb-1">Open ({open.length})</div>
          {open.length === 0 ? <Empty>No open positions — click «⚛ Open from scan».</Empty> :
            <table className="w-full text-xs mb-5"><thead><tr className="border-b border-white/10">
              <Th>Ticker</Th><Th>Cap</Th><Th r>Conv</Th><Th r>Entry</Th><Th r>Now</Th><Th r>uP&L</Th><Th r>Stop</Th><Th r>Target</Th><Th r>Size%</Th><Th r>$ Buy</Th><Th>Thesis (atoms)</Th></tr></thead>
              <tbody>{open.map(p => (
                <tr key={p.id} className="border-b border-white/[0.04] hover:bg-white/[0.03]">
                  <td className="px-2 py-1"><button onClick={() => onSelectTicker?.(p.ticker)} className="font-bold text-emerald-300 hover:underline">{p.ticker}</button></td>
                  <Cap b={p.mcap_bucket} /><Td r>{p.atomic_score}</Td>
                  <Td r>${p.entry_px}</Td><Td r>{p.now_px != null ? '$' + p.now_px : '—'}</Td>
                  <Td r cls={`font-bold ${pnlC(p.upnl_pct)}`}>{p.upnl_pct != null ? fmtPct(p.upnl_pct) : '—'}</Td>
                  <Td r cls="text-rose-300/70">${p.stop_px}</Td><Td r cls="text-emerald-300/70">${p.target_px}</Td>
                  <Td r cls="text-md-on-surface-var">{p.size_pct}%</Td><Td r cls="text-md-on-surface-var/70">${fmtNum(p.dollar_buy)}</Td>
                  <td className="px-2 py-1"><Atoms a={p.atoms} /></td>
                </tr>))}</tbody></table>}

          <div className="text-sm font-semibold mb-1">Closed ({closed.length})</div>
          {closed.length === 0 ? <Empty>No closed positions yet — grade after new bars print.</Empty> :
            <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
              <Th>Ticker</Th><Th>Verdict</Th><Th r>P&L%</Th><Th r>Entry</Th><Th r>Exit</Th><Th>Reason</Th><Th>Closed</Th></tr></thead>
              <tbody>{closed.map(p => (
                <tr key={p.id} className="border-b border-white/[0.04]">
                  <td className="px-2 py-1"><button onClick={() => onSelectTicker?.(p.ticker)} className="font-bold hover:underline">{p.ticker}</button></td>
                  <Td>{p.verdict}</Td><Td r cls={`font-bold ${pnlC(p.pnl_pct)}`}>{fmtPct(p.pnl_pct)}</Td>
                  <Td r>${p.entry_px}</Td><Td r>${p.exit_px}</Td><Td cls="text-md-on-surface-var">{p.exit_reason}</Td>
                  <Td cls="text-md-on-surface-var/60">{p.close_date}</Td>
                </tr>))}</tbody></table>}
        </>
      ) : sub === 'replay' ? (
        <Replay />
      ) : (
        <Knowledge closed={closed} open={open} />
      )}
    </div>
  )
}

// Replay: historical backtest of the atomic edge over a period, using the exact journal rules
function Replay() {
  const [months, setMonths] = useState(6)
  const [cw, setCw] = useState(15)
  const [d, setD] = useState(null)
  const [loading, setLoading] = useState(false)
  const run = (m, w) => {
    setMonths(m); setCw(w); setLoading(true); setD(null)
    fetch(`/api/atomic-journal/replay?months=${m}&capit_window=${w}`).then(r => r.json()).then(setD).catch(() => {}).finally(() => setLoading(false))
  }
  const s = d?.stats || {}
  const pc = s.post_capit
  return (
    <div>
      <div className="flex items-center flex-wrap gap-2 mb-3 text-xs">
        <span className="text-md-on-surface-var">Backtest period:</span>
        {[3, 6, 12, 24].map(m => (
          <button key={m} onClick={() => run(m, cw)} disabled={loading}
            className={`px-2 py-1 rounded ${months === m && d ? 'bg-violet-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{m}mo</button>
        ))}
        <span className="text-md-on-surface-var ml-3">🔥 capit window:</span>
        {[10, 15].map(w => (
          <button key={w} onClick={() => run(months, w)} disabled={loading}
            className={`px-2 py-1 rounded ${cw === w ? 'bg-amber-600 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{w}d</button>
        ))}
        {loading && <span className="text-violet-400 animate-pulse">running…</span>}
        {d && <span className="text-md-on-surface-var/60">from {d.win_start} · entry next-open · −15% stop / +100% target / 20-bar</span>}
      </div>
      {!d && !loading && <Empty>Pick a period to replay the weak-close gap-up edge historically (≥$16 OR rescued by a B+ capit ≤{cw}d; entry next-open, equal 4% bets).</Empty>}
      {d && (
        <>
          <div className="flex flex-wrap gap-4 mb-4 p-3 rounded-lg bg-md-surface-high border border-white/10">
            <Kpi label="Trades" v={s.n} />
            <Kpi label="Win rate" v={s.win_rate != null ? `${s.win_rate}%` : '—'} />
            <Kpi label="Avg P&L" v={s.avg_pnl != null ? fmtPct(s.avg_pnl) : '—'} />
            <Kpi label="Median" v={s.median_pnl != null ? fmtPct(s.median_pnl) : '—'} />
            <div><div className="text-xs text-md-on-surface-var">Equity (4% bets)</div>
              <div className={`text-lg font-bold font-mono ${(s.equity_pct ?? 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>${fmtNum(s.equity_end)} <span className="text-sm">{fmtPct(s.equity_pct)}</span></div></div>
            <Kpi label="Best / Worst" v={`${fmtPct(s.best)} / ${fmtPct(s.worst)}`} />
            <Kpi label="Stop / Target" v={`${s.stop_pct}% / ${s.target_pct}%`} />
            <Kpi label="Still open" v={s.still_open} />
          </div>
          {pc && (
            <div className="flex flex-wrap gap-4 mb-4 p-3 rounded-lg bg-amber-500/10 border border-amber-500/30">
              <div className="text-xs font-semibold text-amber-300 self-center">🔥 Capit→Atomic confluence (post-capit ≤{cw}d):</div>
              <Kpi label="Trades" v={pc.n} />
              <Kpi label="Win rate" v={pc.win_rate != null ? `${pc.win_rate}%` : '—'} />
              <Kpi label="Avg P&L" v={fmtPct(pc.avg_pnl)} />
              <Kpi label="Median" v={fmtPct(pc.median_pnl)} />
            </div>
          )}
          <div className="text-sm font-semibold mb-1">By month</div>
          <table className="w-full text-xs mb-3"><thead><tr className="border-b border-white/10">
            <Th>Month</Th><Th r>Trades</Th><Th r>Win%</Th><Th r>Avg P&L</Th></tr></thead>
            <tbody>{(d.by_month || []).map(m => (
              <tr key={m.month} className="border-b border-white/[0.04]">
                <Td>{m.month}</Td><Td r>{m.n}</Td><Td r>{m.win_rate}%</Td>
                <Td r cls={pnlC(m.avg_pnl)}>{fmtPct(m.avg_pnl)}</Td>
              </tr>))}</tbody></table>
          <p className="text-[10px] text-md-on-surface-var/50">Retroactive track record — entry = next-session open, −15% stop / +100% target / 20-bar, one open per ticker, equal 4% paper bets. Tail-driven (the +100% target rarely hits but big winners carry it). Honest backtest, not a guarantee.</p>
        </>
      )}
    </div>
  )
}

// Knowledge: per-atom forward edge from the journal's own closed (+ open mark) trades
function Knowledge({ closed, open }) {
  const ATOMS = ['close=O', 'gap', 'R2L', 'EO', 'vol=B', 'wick=D', 'G3']
  const pool = [...closed.map(p => ({ atoms: p.atoms, pnl: p.pnl_pct })), ...open.map(p => ({ atoms: p.atoms, pnl: p.upnl_pct }))]
    .filter(x => x.pnl != null)
  const rows = ATOMS.map(a => {
    const w = pool.filter(x => (x.atoms || []).includes(a))
    const n = w.length, wins = w.filter(x => x.pnl > 0).length
    const avg = n ? w.reduce((s, x) => s + x.pnl, 0) / n : null
    return { a, n, win: n ? Math.round(wins / n * 100) : null, avg }
  })
  return (
    <div>
      <div className="text-sm font-semibold mb-1">Per-atom edge (journal trades, live + closed)</div>
      <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
        <Th>atom</Th><Th r>n</Th><Th r>win%</Th><Th r>avg P&L</Th></tr></thead>
        <tbody>{rows.map(r => (
          <tr key={r.a} className="border-b border-white/[0.04]">
            <td className="px-2 py-1"><span className={`text-[10px] font-mono px-1 rounded border border-white/10 ${ATOM_CLS[r.a] || ''}`}>{r.a}</span></td>
            <Td r>{r.n}</Td><Td r>{r.win != null ? r.win + '%' : '—'}</Td>
            <Td r cls={pnlC(r.avg)}>{r.avg != null ? fmtPct(r.avg) : '—'}</Td>
          </tr>))}</tbody></table>
      <p className="text-[10px] text-md-on-surface-var/50 mt-2">Live edge of each atom within this journal's own positions — accumulates as trades close. Validated 5-year lift (for reference): close=O +0.31 · R2L +0.30 · gap/G3 +0.4–0.6.</p>
    </div>
  )
}
