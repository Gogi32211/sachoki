import { useState, useEffect, useCallback } from 'react'
import TickerDrawer from './TickerDrawer'

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(1)}%`)
const fmtNum = (v) => (v == null ? '—' : Number(v).toLocaleString())

async function jget(path)  { const r = await fetch(path); if (!r.ok) throw new Error(await r.text()); return r.json() }
async function jpost(path, body) {
  const r = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) })
  if (!r.ok) throw new Error(await r.text()); return r.json()
}

export default function AiJournalPanel() {
  const [ov, setOv] = useState(null)
  const [kb, setKb] = useState(null)
  const [uni, setUni] = useState(null)
  const [sub, setSub] = useState('positions')
  const [drawerTk, setDrawerTk] = useState(null)
  const [live, setLive] = useState({})
  const [busy, setBusy] = useState(null)
  const [err, setErr] = useState(null)
  const [lastSession, setLastSession] = useState(null)

  const reload = useCallback(() => {
    jget('/api/journal/overview').then(setOv).catch(e => setErr(String(e)))
    jget('/api/journal/knowledge').then(setKb).catch(() => {})
  }, [])
  useEffect(() => { reload() }, [reload])
  useEffect(() => {
    if (sub === 'universe' && !uni) jget('/api/journal/universe').then(setUni).catch(e => setErr(String(e)))
  }, [sub, uni])
  // live prices for open/pending positions — refresh every 30s
  useEffect(() => {
    const pull = () => jget('/api/journal/live').then(d => setLive(d.prices || {})).catch(() => {})
    pull()
    const id = setInterval(pull, 30000)
    return () => clearInterval(id)
  }, [])

  const runSession = async () => {
    setBusy('session'); setErr(null)
    try { const r = await jpost('/api/journal/session', {}); setLastSession(r); reload() }
    catch (e) { setErr(String(e)) } finally { setBusy(null) }
  }
  const runFill = async () => {
    setBusy('fill'); setErr(null)
    try { await jpost('/api/journal/fill', {}); reload() }
    catch (e) { setErr(String(e)) } finally { setBusy(null) }
  }
  const runGrade = async () => {
    setBusy('grade'); setErr(null)
    try { await jpost('/api/journal/grade', {}); reload() }
    catch (e) { setErr(String(e)) } finally { setBusy(null) }
  }
  const runReflect = async () => {
    setBusy('reflect'); setErr(null)
    try { await jpost('/api/journal/reflect', {}); reload() }
    catch (e) { setErr(String(e)) } finally { setBusy(null) }
  }

  const st = ov?.state || {}
  const stats = ov?.stats || {}
  const equity = st.capital ?? 0
  const eqPct = st.start_capital ? ((equity - st.start_capital) / st.start_capital * 100) : 0

  return (
    <div className="p-4 text-md-on-surface">
      {/* KPI strip */}
      <div className="flex flex-wrap items-center gap-4 mb-4 p-3 rounded-lg bg-md-surface-high border border-white/10">
        <div><div className="text-xs text-md-on-surface-var">Equity</div>
          <div className="text-lg font-bold font-mono">${fmtNum(Math.round(equity))} <span className={eqPct>=0?'text-emerald-400':'text-rose-400'}>{fmtPct(eqPct)}</span></div></div>
        <Kpi label="Open" v={stats.open ?? 0} />
        <Kpi label="Pending" v={stats.pending ?? 0} />
        <Kpi label="Closed" v={stats.closed ?? 0} />
        <Kpi label="Win rate" v={stats.win_rate == null ? '—' : `${stats.win_rate.toFixed(0)}%`} />
        <Kpi label="Avg ret" v={stats.avg_ret_pct == null ? '—' : fmtPct(stats.avg_ret_pct)} />
        {(() => {
          const op = (ov?.open_positions || [])
          const tot = op.reduce((s, p) => s + (live[p.ticker]?.upnl ?? 0), 0)
          const has = op.some(p => live[p.ticker]?.upnl != null)
          return <div><div className="text-xs text-md-on-surface-var">Open P&L (live)</div>
            <div className={`text-lg font-bold font-mono ${tot>=0?'text-emerald-400':'text-rose-400'}`}>{has ? `${tot>=0?'+':''}$${Math.abs(tot).toFixed(0)}` : '—'}</div></div>
        })()}
        {ov?.regime && <div title={`breadth ${ov.regime.score} · RSI>50 ${ov.regime.breadth?.pct_rsi_gt50}% · phaseD ${ov.regime.breadth?.pct_phase_D}%`}>
          <div className="text-xs text-md-on-surface-var">Regime</div>
          <div className={`text-lg font-bold ${ov.regime.label==='RISK_ON'?'text-emerald-400':ov.regime.label==='RISK_OFF'?'text-rose-400':'text-yellow-400'}`}>
            {ov.regime.label==='RISK_ON'?'🟢':ov.regime.label==='RISK_OFF'?'🔴':'🟡'} {ov.regime.score}</div></div>}
        <div className="flex-1" />
        <button onClick={runSession} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-violet-700 hover:bg-violet-600 text-white text-sm font-semibold disabled:opacity-50">
          {busy==='session' ? '⏳ Running…' : '▶ Run session'}</button>
        <button onClick={runFill} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface border border-white/15 hover:bg-white/10 text-sm disabled:opacity-50"
          title="Fill PENDING_OPEN positions at the next session's open price">
          {busy==='fill' ? '⏳…' : '⏱ Fill opens'}</button>
        <button onClick={runGrade} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface border border-white/15 hover:bg-white/10 text-sm disabled:opacity-50">
          {busy==='grade' ? '⏳ Grading…' : 'Grade now'}</button>
        <button onClick={runReflect} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface border border-white/15 hover:bg-white/10 text-sm disabled:opacity-50"
          title="Agent B: rebuild pattern memory → draft lessons → promotion gate">
          {busy==='reflect' ? '⏳…' : '🧠 Reflect'}</button>
      </div>

      {err && <div className="mb-3 p-2 rounded bg-rose-900/40 text-rose-200 text-xs font-mono break-all">{err}</div>}
      {ov?.last_session && <div className="mb-3 text-xs text-md-on-surface-var">Last session: {ov.last_session.notes}</div>}
      {lastSession?.opened && <div className="mb-3 text-xs text-emerald-300">Session done — opened {lastSession.opened.length}, refused {lastSession.refused?.length||0} · {lastSession.usage?.model}</div>}

      {/* Sub-tabs */}
      <div className="flex gap-1 mb-3">
        {['positions','replay','knowledge','universe','lessons'].map(s => (
          <button key={s} onClick={() => setSub(s)}
            className={`px-3 py-1 rounded text-sm capitalize ${sub===s?'bg-violet-700 text-white':'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{s}</button>
        ))}
      </div>

      {sub === 'positions' && <Positions ov={ov} onTicker={setDrawerTk} live={live} />}
      {sub === 'replay'    && <JournalReplay />}
      {sub === 'knowledge' && <Knowledge kb={kb} />}
      {sub === 'universe'  && <Universe uni={uni} />}
      {sub === 'lessons'   && <Lessons ov={ov} />}

      <TickerDrawer ticker={drawerTk} onClose={() => setDrawerTk(null)} />
    </div>
  )
}

function Tk({ t, onTicker, cls = '' }) {
  return <td className={`px-2 py-1 ${cls}`}>
    <button onClick={() => onTicker?.(t)} className="font-bold hover:underline cursor-pointer">{t}</button></td>
}

function Kpi({ label, v }) {
  return <div><div className="text-xs text-md-on-surface-var">{label}</div><div className="text-lg font-bold font-mono">{v}</div></div>
}

function Th({ children, r }) { return <th className={`px-2 py-1 font-semibold text-md-on-surface-var ${r?'text-right':'text-left'}`}>{children}</th> }
function Td({ children, r, cls='' }) { return <td className={`px-2 py-1 font-mono ${r?'text-right':'text-left'} ${cls}`}>{children}</td> }

function Positions({ ov, onTicker, live = {} }) {
  const open = ov?.open_positions || [], closed = ov?.closed_positions || []
  const pending = ov?.pending_positions || []
  const cap = ov?.state?.capital ?? 0
  // dollar size of a buy: filled = shares×entry; not-yet-filled = target size%×capital
  const usdSize = (p) => (p.shares && p.entry_px) ? p.shares * p.entry_px : (p.size_pct || 0) * cap
  const fmtUsd = (v) => v ? `$${Math.round(v).toLocaleString()}` : '—'
  return (
    <div className="space-y-5">
      {pending.length > 0 && (
      <div>
        <div className="text-sm font-semibold mb-1 text-amber-300">⏱ Pending open ({pending.length})
          <span className="ml-2 text-xs font-normal text-md-on-surface-var">— решено при закрытой бирже, вход по open следующей сессии</span></div>
        <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
          <Th>Ticker</Th><Th>Cap</Th><Th r>Conv</Th><Th r>Size%</Th><Th r>$ Buy</Th><Th>Decided</Th><Th>Mode</Th><Th>Thesis</Th></tr></thead>
        <tbody>{pending.map(p => <tr key={p.id} className="border-b border-white/5">
          <Tk t={p.ticker} onTicker={onTicker} cls="text-amber-300" /><Cap b={p.mcap_bucket} /><Td r>{p.conviction}</Td>
          <Td r>{(p.size_pct*100).toFixed(1)}</Td><Td r cls="font-semibold text-amber-200">{fmtUsd(usdSize(p))}</Td><Td>{p.decision_date} ({p.decided_session})</Td>
          <Td className="text-amber-400">{p.entry_mode}</Td>
          <td className="px-2 py-1 text-md-on-surface-var max-w-[420px] truncate" title={p.thesis}>{p.thesis}</td></tr>)}</tbody></table>
      </div>)}
      <div>
        <div className="text-sm font-semibold mb-1">Open ({open.length})</div>
        {open.length === 0 ? <Empty>Нет открытых позиций — нажми «Run session».</Empty> :
        <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
          <Th>Ticker</Th><Th>Cap</Th><Th r>Conv</Th><Th r>Entry</Th><Th r>Now</Th><Th r>uP&L</Th><Th r>Stop</Th><Th r>Target</Th><Th r>Size%</Th><Th r>$ Buy</Th><Th>Thesis</Th></tr></thead>
        <tbody>{open.map(p => { const lv = live[p.ticker] || {}; const up = lv.upnl_pct
          return <tr key={p.id} className="border-b border-white/5">
          <Tk t={p.ticker} onTicker={onTicker} cls="text-emerald-300" /><Cap b={p.mcap_bucket} /><Td r>{p.conviction}</Td>
          <Td r>${p.entry_px?.toFixed(2)}</Td>
          <Td r cls="font-semibold">{lv.price != null ? `$${lv.price.toFixed(2)}` : '—'}</Td>
          <Td r cls={up==null?'text-gray-500':up>=0?'text-emerald-400 font-bold':'text-rose-400 font-bold'}>{up==null?'—':`${up>=0?'+':''}${up.toFixed(2)}%`}</Td>
          <Td r cls="text-rose-400">${p.stop_px?.toFixed(2)}</Td>
          <Td r cls="text-sky-300">${p.target_px?.toFixed(2)}</Td><Td r>{(p.size_pct*100).toFixed(1)}</Td>
          <Td r cls="font-semibold text-emerald-200">{fmtUsd(usdSize(p))}</Td>
          <td className="px-2 py-1 text-md-on-surface-var max-w-[360px] truncate" title={p.thesis}>{p.thesis}</td></tr> })}</tbody></table>}
      </div>
      <div>
        <div className="text-sm font-semibold mb-1">Closed ({closed.length})</div>
        {closed.length === 0 ? <Empty>Закрытых нет — появятся после грейдинга (нужны бары после даты входа).</Empty> :
        <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
          <Th>Ticker</Th><Th>Verdict</Th><Th r>P&L%</Th><Th>Exit</Th><Th>Thesis</Th></tr></thead>
        <tbody>{closed.map(p => <tr key={p.id} className="border-b border-white/5">
          <Tk t={p.ticker} onTicker={onTicker} />
          <Td><span className={p.verdict==='WIN'?'text-emerald-400':p.verdict==='LOSS'?'text-rose-400':'text-md-on-surface-var'}>{p.verdict}</span></Td>
          <Td r className={p.pnl_pct>=0?'text-emerald-400':'text-rose-400'}>{fmtPct(p.pnl_pct)}</Td>
          <Td>{p.exit_reason}</Td>
          <td className="px-2 py-1 text-md-on-surface-var max-w-[460px] truncate" title={p.thesis}>{p.thesis}</td></tr>)}</tbody></table>}
      </div>
    </div>
  )
}

function Knowledge({ kb }) {
  const rows = kb?.predicates || []
  return (
    <div>
      <div className="text-xs text-md-on-surface-var mb-2">
        Tier-1 база знаний (as-of {kb?.as_of || '…'}) — сортировано по <b>P&L edge на H={kb?.horizon || 10}d</b> (валидированный Combo Lab'ом). HH-edge показан рядом для сверки — но <b>HH не предсказывает P&L</b>, ориентируйся на P&L колонку.
      </div>
      <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
        <Th>Signal</Th><Th>Cat</Th><Th r>n</Th>
        <th colSpan={3} className="px-2 py-1 text-center bg-emerald-900/30 text-emerald-300">— P&L edge (real $) —</th>
        <th colSpan={3} className="px-2 py-1 text-center bg-amber-900/20 text-amber-300">— HH edge (structural) —</th>
        </tr><tr className="border-b border-white/10 text-md-on-surface-var">
        <Th></Th><Th></Th><Th></Th>
        <Th r>pnl avg</Th><Th r>pnl edge</Th><Th r>edge win</Th>
        <Th r>HH%</Th><Th r>HH edge</Th><Th r>fwd5med</Th></tr></thead>
      <tbody>{rows.map(p => <tr key={p.predicate} className="border-b border-white/5">
        <Td cls="font-bold">{p.predicate}</Td><Td className="text-md-on-surface-var">{p.category}</Td>
        <Td r>{fmtNum(p.n)}</Td>
        <Td r>{p.pnl_avg!=null?`${p.pnl_avg}%`:'—'}</Td>
        <Td r cls={p.pnl_edge==null?'text-gray-500':p.pnl_edge>=0.05?'text-emerald-300 font-bold':p.pnl_edge>=0?'text-emerald-400':'text-rose-400'}>{p.pnl_edge!=null?`${p.pnl_edge>=0?'+':''}${p.pnl_edge}%`:'—'}</Td>
        <Td r cls={p.pnl_edge_win==null?'text-gray-500':p.pnl_edge_win>=0?'text-emerald-400':'text-rose-400'}>{p.pnl_edge_win!=null?`${p.pnl_edge_win>=0?'+':''}${p.pnl_edge_win}pp`:'—'}</Td>
        <Td r>{p.hh5}</Td>
        <Td r cls={p.hh_edge_pp>=8?'text-amber-200':p.hh_edge_pp>=3?'text-amber-400':'text-md-on-surface-var'}>{p.hh_edge_pp>=0?'+':''}{p.hh_edge_pp}pp</Td>
        <Td r>{p.fwd5_med}</Td>
      </tr>)}</tbody></table>
    </div>
  )
}

function Lessons({ ov }) {
  const lessons = ov?.lessons || []
  const byStatus = (s) => lessons.filter(l => l.status === s)
  if (!lessons.length) return <Empty>Уроков пока нет — Agent B пишет их при закрытии сделок (v1.1). Активируются только при статистическом подтверждении (n≥N, lift vs base).</Empty>
  return (
    <div className="space-y-4">
      {['active','provisional','retired'].map(s => byStatus(s).length>0 && (
        <div key={s}>
          <div className="text-sm font-semibold capitalize mb-1">{s} ({byStatus(s).length})</div>
          {byStatus(s).map((l,i) => <div key={i} className={`text-xs p-2 mb-1 rounded border ${s==='active'?'border-emerald-700/50 bg-emerald-900/20':s==='retired'?'border-white/10 bg-white/5 line-through opacity-60':'border-white/15 bg-md-surface-high'}`}>
            <div>{l.lesson}</div>
            <div className="text-md-on-surface-var mt-0.5">scope: {l.scope_fingerprint} · n={l.evidence_n} · lift={l.evidence_lift}</div></div>)}
        </div>
      ))}
    </div>
  )
}

const CAP_CLS = { mega:'text-amber-200', large:'text-emerald-300', mid:'text-sky-300',
                  small:'text-yellow-400', micro:'text-rose-400', unknown:'text-gray-500' }
function Cap({ b }) {
  const k = b || 'unknown'
  return <td className={`px-2 py-1 font-mono ${CAP_CLS[k] || 'text-gray-400'}`}>{k}</td>
}

function Universe({ uni }) {
  if (!uni) return <Empty>Загрузка… (джойн ticker_meta × bars, пара секунд)</Empty>
  const blocked = new Set(uni.blocked_buckets || [])
  const bs = uni.bucket_stats || [], ss = uni.sector_stats || []
  const counts = Object.fromEntries((uni.bucket_counts||[]).map(b => [b.bucket, b.n]))
  return (
    <div className="space-y-5">
      <div>
        <div className="text-sm font-semibold mb-1">Market-cap — setup (V3≥25) forward-исходы</div>
        <div className="text-xs text-md-on-surface-var mb-2">Это рычаг, который реально работает. Заблокированные бакеты движок не торгует (лотерея/непредсказуемо).</div>
        <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
          <Th>Bucket</Th><Th r>Tickers</Th><Th r>setups n</Th><Th r>HH5</Th><Th r>fwd5 med</Th><Th r>P(fwd5≥10%)</Th><Th>Rule</Th></tr></thead>
        <tbody>{bs.map(r => <tr key={r.bucket} className="border-b border-white/5">
          <Cap b={r.bucket} /><Td r className="text-md-on-surface-var">{counts[r.bucket]??'—'}</Td><Td r>{(r.n||0).toLocaleString()}</Td>
          <Td r className="font-bold">{r.hh5}%</Td>
          <Td r className={r.fwd5_med>=0?'text-emerald-400':'text-rose-400'}>{r.fwd5_med}</Td>
          <Td r>{r.big10}%</Td>
          <Td>{blocked.has(r.bucket) ? <span className="text-rose-400">⛔ blocked</span> : <span className="text-emerald-400">✓ allowed</span>}</Td></tr>)}</tbody></table>
      </div>
      <div>
        <div className="text-sm font-semibold mb-1">Sector — setup HH vs сигнал-lift</div>
        <div className="text-xs text-md-on-surface-var mb-2">Слабое измерение: разброс HH мал, а сигнал даёт ~+15pp в КАЖДОМ секторе → для отбора не используем.</div>
        <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
          <Th>Sector</Th><Th r>setups n</Th><Th r>setup HH5</Th><Th r>signal lift</Th></tr></thead>
        <tbody>{ss.map(r => <tr key={r.sector} className="border-b border-white/5">
          <Td cls="font-bold">{r.sector}</Td><Td r>{(r.n||0).toLocaleString()}</Td><Td r>{r.setup_hh}%</Td>
          <Td r className="text-emerald-400">+{r.sig_lift}pp</Td></tr>)}</tbody></table>
      </div>
    </div>
  )
}

function Empty({ children }) { return <div className="text-xs text-md-on-surface-var italic p-3">{children}</div> }

// Replay: historical backtest of the journal's deterministic substrate (rails
// candidate pool + ATR exit), WITHOUT the LLM — the underlying edge the agent draws from.
function JournalReplay() {
  const [months, setMonths] = useState(6)
  const [d, setD] = useState(null)
  const [loading, setLoading] = useState(false)
  const pc = (p) => p == null ? '' : p >= 0 ? 'text-emerald-400' : 'text-rose-400'
  const num = (v) => v == null ? '—' : Number(v).toLocaleString()
  const run = (m) => {
    setMonths(m); setLoading(true); setD(null)
    fetch(`/api/journal/replay?months=${m}`).then(r => r.json()).then(setD).catch(() => {}).finally(() => setLoading(false))
  }
  const s = d?.stats || {}
  const K = ({ label, v }) => <div><div className="text-xs text-md-on-surface-var">{label}</div><div className="text-lg font-bold font-mono">{v}</div></div>
  return (
    <div>
      <p className="text-[11px] text-md-on-surface-var mb-2">🔁 <b>Replay</b> — historical backtest of the journal's <b>deterministic substrate</b> (rails candidate pool: prebreak_v3 ≥ 20, non-micro, top-12/day · entry next-open · stop −1.5×ATR / target +5×ATR / 10-bar). This is the edge the LLM picks FROM, measured without re-running it. Equal 5% paper bets.</p>
      <div className="flex items-center gap-2 mb-3 text-xs">
        <span className="text-md-on-surface-var">Period:</span>
        {[3, 6, 12, 24].map(m => (
          <button key={m} onClick={() => run(m)} disabled={loading}
            className={`px-2 py-1 rounded ${months === m && d ? 'bg-violet-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{m}mo</button>
        ))}
        {loading && <span className="text-violet-400 animate-pulse">running…</span>}
        {d && <span className="text-md-on-surface-var/60">from {d.win_start} · cap-filter {s.cap_filter ? 'on' : 'off'}</span>}
      </div>
      {!d && !loading && <Empty>Pick a period to backtest the rails substrate historically.</Empty>}
      {d && (
        <>
          <div className="flex flex-wrap gap-4 mb-4 p-3 rounded-lg bg-md-surface-high border border-white/10">
            <K label="Trades" v={s.n} />
            <K label="Win rate" v={s.win_rate != null ? `${s.win_rate}%` : '—'} />
            <K label="Avg P&L" v={s.avg_pnl != null ? fmtPct(s.avg_pnl) : '—'} />
            <K label="Median" v={s.median_pnl != null ? fmtPct(s.median_pnl) : '—'} />
            <div><div className="text-xs text-md-on-surface-var">Equity (5% bets)</div>
              <div className={`text-lg font-bold font-mono ${(s.equity_pct ?? 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>${num(s.equity_end)} <span className="text-sm">{fmtPct(s.equity_pct)}</span></div></div>
            <K label="Best / Worst" v={`${fmtPct(s.best)} / ${fmtPct(s.worst)}`} />
            <K label="Stop / Target" v={`${s.stop_pct}% / ${s.target_pct}%`} />
            <K label="Still open" v={s.still_open} />
          </div>
          <div className="text-sm font-semibold mb-1">By month</div>
          <table className="w-full text-xs mb-3"><thead><tr className="border-b border-white/10">
            <th className="text-left px-2 py-1 text-md-on-surface-var">Month</th>
            <th className="text-right px-2 py-1 text-md-on-surface-var">Trades</th>
            <th className="text-right px-2 py-1 text-md-on-surface-var">Win%</th>
            <th className="text-right px-2 py-1 text-md-on-surface-var">Avg P&L</th></tr></thead>
            <tbody>{(d.by_month || []).map(m => (
              <tr key={m.month} className="border-b border-white/[0.04]">
                <td className="px-2 py-1 font-mono">{m.month}</td>
                <td className="px-2 py-1 font-mono text-right">{m.n}</td>
                <td className="px-2 py-1 font-mono text-right">{m.win_rate}%</td>
                <td className={`px-2 py-1 font-mono text-right ${pc(m.avg_pnl)}`}>{fmtPct(m.avg_pnl)}</td>
              </tr>))}</tbody></table>
          <p className="text-[10px] text-md-on-surface-var/50">Systematic (no-LLM) track record: entry = next-session open, rails ATR exit, one ticker/day, top-12 by V3. Tail-driven (low win%, wide target carries it). The live journal's LLM aims to beat this by picking the better subset.</p>
        </>
      )}
    </div>
  )
}
