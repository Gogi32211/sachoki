import { useEffect, useState, useMemo, useRef, Fragment } from 'react'
import CodeCandleChart from './CodeCandleChart'
import JournalBench from './JournalBench'

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(1)}%`)
const fmtNum = (v) => (v == null ? '—' : Number(v).toLocaleString())
const CAP_CLS = { mega: 'text-amber-200', large: 'text-emerald-300', mid: 'text-sky-300', small: 'text-violet-300', micro: 'text-rose-300' }
const pnlC = (p) => p == null ? '' : p >= 0 ? 'text-emerald-400' : 'text-rose-400'

// Capit atoms are partly dynamic (RSI29, CCI-159, flush-15%, vol2.1x) — colour by prefix
const atomCls = (x) => {
  if (x.startsWith('L3') || x.startsWith('L4') || x.startsWith('L6')) return 'bg-sky-900/50 text-sky-200'
  if (x.startsWith('RSI')) return 'bg-indigo-900/50 text-indigo-200'
  if (x.startsWith('CCI')) return 'bg-violet-900/50 text-violet-200'
  if (x.startsWith('flush')) return 'bg-emerald-900/50 text-emerald-200'
  if (x === 'red') return 'bg-rose-900/40 text-rose-200'
  if (x.startsWith('vol')) return 'bg-teal-900/50 text-teal-200'
  if (x === 'FRI64' || x === 'BLUE') return 'bg-amber-900/40 text-amber-200'
  if (x === 'absorb') return 'bg-fuchsia-900/40 text-fuchsia-200'
  if (x === 'deep') return 'bg-purple-900/50 text-purple-200'
  if (x.startsWith('⚠')) return 'bg-rose-800/60 text-rose-100'
  return ''
}

const Kpi = ({ label, v }) => <div><div className="text-xs text-md-on-surface-var">{label}</div><div className="text-lg font-bold font-mono">{v}</div></div>
const Th = ({ children, r }) => <th className={`px-2 py-1 font-semibold text-md-on-surface-var ${r ? 'text-right' : 'text-left'}`}>{children}</th>
const Td = ({ children, r, cls = '' }) => <td className={`px-2 py-1 font-mono ${r ? 'text-right' : 'text-left'} ${cls}`}>{children}</td>
const Cap = ({ b }) => { const k = b || 'unknown'; return <td className={`px-2 py-1 font-mono ${CAP_CLS[k] || 'text-gray-400'}`}>{k}</td> }
const Empty = ({ children }) => <div className="text-md-on-surface-var/50 text-xs py-6 text-center">{children}</div>
const Atoms = ({ a }) => <div className="flex flex-wrap gap-1">{(a || []).map((x, i) => <span key={i} className={`text-[9px] font-mono px-1 rounded border border-white/10 ${atomCls(x)}`}>{x}</span>)}</div>

export default function CapitJournalPanel({ onSelectTicker }) {
  const [data, setData] = useState(null)
  const [busy, setBusy] = useState('')
  const [msg, setMsg] = useState('')
  const [sub, setSub] = useState('positions')
  const load = () => fetch('/api/capit-journal').then(r => r.json()).then(setData).catch(() => {})
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
      <p className="text-[11px] text-md-on-surface-var mb-2">💥📓 <b>Capit Journal</b> — a separate, <b>mechanical</b> paper journal for the validated capitulation-bounce edge (L34/L46 + RSI&lt;30 + CCI&lt;-100, drawdown knife-guard). The one edge that survived rigorous gap-aware path-sim (+4.6% cost-adj, 5/6 yrs). <b>EXIT = HOLD ~20 bars, no tight stop</b> (a stop cuts the bounce); only a −35% catastrophe floor. Auto-adds each match with its date. Paper only.</p>
      {s.closed ? <JournalBench stats={s} n={s.closed} /> : (
        <div className="mb-3 rounded border border-rose-700/40 bg-rose-950/20 px-3 py-2 text-[11px] text-rose-100/90 max-w-5xl">
          ⚠ <b>No closed trades yet — this journal has no track record.</b> Every number below except
          Open/Pending is <b>unrealised mark-to-market on positions that are still running</b>, so it is a
          snapshot, not a result: winners and losers are both still open, and in a rising window that always
          looks good. Nothing scheduled ever graded this journal until 2026-07-17 (the nightly
          <span className="font-mono"> paper_journals_daily </span> job now fills + closes matured positions);
          once trades close, this box is replaced by the vs-random-basket baseline.
        </div>
      )}
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
        <button onClick={() => act('/api/capit-journal/open?top=20&min_score=60', 'open')} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-amber-700 hover:bg-amber-600 text-white text-sm font-semibold disabled:opacity-50">
          {busy === 'open' ? '⏳…' : '💥 Open from scan'}</button>
        <button onClick={() => act('/api/capit-journal/grade', 'grade')} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface border border-white/15 hover:bg-white/10 text-sm disabled:opacity-50">
          {busy === 'grade' ? '⏳ Grading…' : 'Grade now'}</button>
        <button onClick={load} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface border border-white/15 hover:bg-white/10 text-sm disabled:opacity-50" title="Refresh live prices / P&L">↻ live</button>
      </div>

      <div className="mb-3 text-xs text-md-on-surface-var">
        Edge: L34/L46 + RSI&lt;30 + CCI&lt;-100 · drawdown sweet-spot −45..−10% · entry signal close · <b>hold ~20 bars, no stop</b> · −35% catastrophe floor · regime-sized (×{reg.conv_mult ?? 1}).
        {reg.label === 'RISK_OFF' && <span className="text-rose-300/80"> ⚠ RISK_OFF — positions auto-sized small.</span>}
        {msg && <span className="text-emerald-400 ml-2">{msg}</span>}
      </div>

      <div className="flex gap-1 mb-3">
        {['positions', 'replay', 'knowledge'].map(t => (
          <button key={t} onClick={() => setSub(t)}
            className={`px-3 py-1 rounded text-sm capitalize ${sub === t ? 'bg-amber-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{t}</button>
        ))}
      </div>

      {sub === 'positions' ? (
        <>
          <div className="text-sm font-semibold mb-1">Open ({open.length})</div>
          {open.length === 0 ? <Empty>No open positions — click «💥 Open from scan».</Empty> :
            <table className="w-full text-xs mb-5"><thead><tr className="border-b border-white/10">
              <Th>Ticker</Th><Th>Cap</Th><Th r>Score</Th><Th>Opened</Th><Th r>Entry</Th><Th r>Now</Th><Th r>uP&L</Th><Th r>Floor</Th><Th r>Size%</Th><Th r>$ Buy</Th><Th>Thesis (atoms)</Th></tr></thead>
              <tbody>{open.map(p => (
                <tr key={p.id} className="border-b border-white/[0.04] hover:bg-white/[0.03]">
                  <td className="px-2 py-1"><button onClick={() => onSelectTicker?.(p.ticker)} className="font-bold text-amber-300 hover:underline">{p.ticker}</button></td>
                  <Cap b={p.mcap_bucket} /><Td r>{p.cap_score}</Td>
                  <Td cls="text-md-on-surface-var/60">{p.open_date}</Td>
                  <Td r>${p.entry_px}</Td><Td r>{p.now_px != null ? '$' + p.now_px : '—'}</Td>
                  <Td r cls={`font-bold ${pnlC(p.upnl_pct)}`}>{p.upnl_pct != null ? fmtPct(p.upnl_pct) : '—'}</Td>
                  <Td r cls="text-rose-300/70">${p.floor_px}</Td>
                  <Td r cls="text-md-on-surface-var">{p.size_pct}%</Td><Td r cls="text-md-on-surface-var/70">${fmtNum(p.dollar_buy)}</Td>
                  <td className="px-2 py-1"><Atoms a={p.atoms} /></td>
                </tr>))}</tbody></table>}

          <div className="text-sm font-semibold mb-1">Closed ({closed.length})</div>
          {closed.length === 0 ? <Empty>No closed positions yet — grade after ~20 bars print (held to horizon).</Empty> :
            <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
              <Th>Ticker</Th><Th>Verdict</Th><Th r>P&L%</Th><Th>Opened</Th><Th r>Entry</Th><Th r>Exit</Th><Th>Reason</Th><Th>Closed</Th></tr></thead>
              <tbody>{closed.map(p => (
                <tr key={p.id} className="border-b border-white/[0.04]">
                  <td className="px-2 py-1"><button onClick={() => onSelectTicker?.(p.ticker)} className="font-bold hover:underline">{p.ticker}</button></td>
                  <Td>{p.verdict}</Td><Td r cls={`font-bold ${pnlC(p.pnl_pct)}`}>{fmtPct(p.pnl_pct)}</Td>
                  <Td cls="text-md-on-surface-var/60">{p.open_date}</Td>
                  <Td r>${p.entry_px}</Td><Td r>${p.exit_px}</Td><Td cls="text-md-on-surface-var">{p.exit_reason}</Td>
                  <Td cls="text-md-on-surface-var/60">{p.close_date}</Td>
                </tr>))}</tbody></table>}
        </>
      ) : sub === 'replay' ? (
        <Replay onSelectTicker={onSelectTicker} />
      ) : (
        <Knowledge closed={closed} open={open} />
      )}
    </div>
  )
}

// Replay: historical backtest of the edge over a period, using the exact journal rules
// Inline chart shown at the top of a journal replay — signal/buy/sell markers, no nav.
function InlineTradeChart({ trade, history, onClose, chartRef }) {
  if (!trade) return null
  return (
    <div ref={chartRef} className="mb-3 scroll-mt-16 rounded-lg border border-amber-700/30 bg-md-surface-high/40 overflow-hidden">
      <div className="flex items-center gap-2 px-3 py-1.5 text-xs border-b border-white/10">
        <span className="font-bold text-amber-300">{trade.ticker}</span>
        <span className="text-md-on-surface-var/70">
          ⚡{trade.signal_date} · 🟢BUY ${trade.entry} {trade.open_date} · 🔴SELL ${trade.exit} {trade.close_date}
          {trade.pnl != null && <span className={trade.pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}> · {fmtPct(trade.pnl)}</span>}
        </span>
        <button onClick={onClose} className="ml-auto text-md-on-surface-var/60 hover:text-md-on-surface">✕</button>
      </div>
      <CodeCandleChart ticker={trade.ticker} tf="1d" height={320} tradeMarkers={trade} tradeHistory={history} />
    </div>
  )
}

function Replay({ onSelectTicker }) {
  const [months, setMonths] = useState(6)
  const [chartTrade, setChartTrade] = useState(null)
  const chartRef = useRef(null)
  const pickTrade = (t) => { setChartTrade(t); setTimeout(() => chartRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' }), 40) }
  const [recipe, setRecipe] = useState('B')
  const [d, setD] = useState(null)
  const [loading, setLoading] = useState(false)
  const [openMonth, setOpenMonth] = useState(null)
  // MANUAL mode — hand-entered limit-entry % / target % / stop % / hold
  const [manual, setManual] = useState(false)
  const [entryPct, setEntryPct] = useState(4)    // limit % below signal close
  const [targetPct, setTargetPct] = useState(15) // take-profit %
  const [stopPct, setStopPct] = useState(0)      // stop-loss % (0 = none, only −35% floor)
  const [holdBars, setHoldBars] = useState(20)   // max bars held
  const [entryWin, setEntryWin] = useState(5)    // bars the limit stays live
  const run = (m, rc, man = manual) => {
    setMonths(m); setRecipe(rc); setManual(man); setLoading(true); setD(null); setOpenMonth(null)
    let url = `/api/capit-journal/replay?months=${m}&recipe=${rc}`
    if (man) url += `&entry_pct=${(+entryPct || 0) / 100}&target_pct=${(+targetPct || 0) / 100}`
           + `&stop_pct=${(+stopPct || 0) / 100}&hold=${+holdBars || 20}&entry_win=${+entryWin || 5}`
    fetch(url).then(r => r.json()).then(setD).catch(() => {}).finally(() => setLoading(false))
  }
  const s = d?.stats || {}
  const tradesByMonth = useMemo(() => {
    const g = {}
    for (const t of (d?.trades || [])) (g[t.month] ||= []).push(t)
    for (const k in g) g[k].sort((a, b) => (a.open_date < b.open_date ? -1 : 1))
    return g
  }, [d])
  const RECIPES = [['B', 'B ✓ prod'], ['e2', 'E2'], ['A', 'A vol'], ['baseline', 'baseline']]
  return (
    <div>
      <InlineTradeChart trade={chartTrade} chartRef={chartRef} onClose={() => setChartTrade(null)}
        history={(d?.trades || []).filter(t => chartTrade && t.ticker === chartTrade.ticker)} />
      <div className="flex flex-wrap items-center gap-2 mb-3 text-xs">
        <span className="text-md-on-surface-var">Period:</span>
        {[3, 6, 12, 24].map(m => (
          <button key={m} onClick={() => run(m, recipe)} disabled={loading}
            className={`px-2 py-1 rounded ${months === m && d ? 'bg-amber-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{m}mo</button>
        ))}
        <span className="text-md-on-surface-var ml-3">Recipe:</span>
        {RECIPES.map(([rc, lbl]) => (
          <button key={rc} onClick={() => run(months, rc)} disabled={loading}
            className={`px-2 py-1 rounded ${recipe === rc && d ? 'bg-violet-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`} title={
              rc === 'B' ? 'production: E2 + shallow-dip (chg20>-25) — most robust, lowest catastrophe' :
              rc === 'e2' ? 'E2: core + RSI≥15 + chg20>-45, no FRI64/absorb' :
              rc === 'A' ? 'A: E2 + vol L/VB (high in-sample mean, but overfit)' : 'baseline: pre-deep-dive'}>{lbl}</button>
        ))}
        {loading && <span className="text-amber-400 animate-pulse">running…</span>}
        <button onClick={() => setManual(v => !v)}
          className={`px-2 py-1 rounded ml-auto ${manual ? 'bg-cyan-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}
          title="Hand-enter your own limit entry %, target %, stop %, and hold — runs the same path-sim">⚙ Manual</button>
      </div>

      {/* MANUAL controls — limit-entry %, target %, stop %, hold (path-sim) */}
      {manual && (
        <div className="flex flex-wrap items-end gap-3 mb-3 p-2.5 rounded-lg bg-cyan-900/15 border border-cyan-700/40 text-xs">
          {[
            ['Entry limit −%', entryPct, setEntryPct, 'buy this % below the signal-bar close'],
            ['Target +%', targetPct, setTargetPct, 'take-profit this % above fill'],
            ['Stop −%', stopPct, setStopPct, '0 = no stop (only −35% floor)'],
            ['Hold (bars)', holdBars, setHoldBars, 'max bars before time-stop close'],
            ['Entry window', entryWin, setEntryWin, 'bars the limit stays live'],
          ].map(([lbl, val, setter, hint]) => (
            <label key={lbl} className="flex flex-col gap-0.5" title={hint}>
              <span className="text-cyan-300/80">{lbl}</span>
              <input type="number" value={val} onChange={e => setter(e.target.value)}
                className="w-20 px-2 py-1 rounded bg-md-surface border border-cyan-700/40 font-mono text-md-on-surface" />
            </label>
          ))}
          <button onClick={() => run(months, recipe, true)} disabled={loading}
            className="px-3 py-1.5 rounded bg-cyan-600 text-white font-semibold hover:bg-cyan-500 disabled:opacity-50">▶ Run manual</button>
          <span className="text-cyan-300/50 text-[10px] basis-full">
            limit buy −{entryPct}% (fills if price dips there within {entryWin} bars) · exit at +{targetPct}% target{+stopPct > 0 ? ` or −${stopPct}% stop` : ''} or {holdBars}-bar close
          </span>
        </div>
      )}
      {!d && !loading && <div className="text-md-on-surface-var/50 text-xs py-6 text-center">Pick a period to replay the Capit edge historically (entry next-open, hold 20 bars, equal 4% bets).</div>}
      {d && (
        <>
          <JournalBench stats={s} n={s.n} />
          <div className="flex flex-wrap gap-4 mb-4 p-3 rounded-lg bg-md-surface-high border border-white/10">
            <Kpi label="Trades" v={s.n} />
            <Kpi label="Win rate" v={s.win_rate != null ? `${s.win_rate}%` : '—'} />
            <Kpi label="Avg P&L" v={s.avg_pnl != null ? fmtPct(s.avg_pnl) : '—'} />
            <Kpi label="Median" v={s.median_pnl != null ? fmtPct(s.median_pnl) : '—'} />
            <div><div className="text-xs text-md-on-surface-var">Equity (4% bets)</div>
              <div className={`text-lg font-bold font-mono ${(s.equity_pct ?? 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>${fmtNum(s.equity_end)} <span className="text-sm">{fmtPct(s.equity_pct)}</span></div></div>
            <Kpi label="Best / Worst" v={`${fmtPct(s.best)} / ${fmtPct(s.worst)}`} />
            <Kpi label="Catastrophe" v={s.catastrophe_pct != null ? `${s.catastrophe_pct}%` : '—'} />
            <Kpi label="Avg spike ↑/↓" v={s.avg_mfe != null ? `${fmtPct(s.avg_mfe)} / ${fmtPct(s.avg_mae)}` : '—'} />
            {manual && <Kpi label="Fill rate" v={s.fill_rate != null ? `${s.fill_rate}%` : '—'} />}
            {manual && <Kpi label="Target hit" v={s.target_hit_pct != null ? `${s.target_hit_pct}%` : '—'} />}
            {manual && +stopPct > 0 && <Kpi label="Stop hit" v={s.stop_hit_pct != null ? `${s.stop_hit_pct}%` : '—'} />}
            <Kpi label="Still open" v={s.still_open} />
          </div>
          <div className="text-sm font-semibold mb-1">By month <span className="text-[10px] text-md-on-surface-var/50 font-normal">— click a month to see every trade (ticker · bought · exit · P&L)</span></div>
          <table className="w-full text-xs mb-4"><thead><tr className="border-b border-white/10">
            <Th>Month</Th><Th r>Trades</Th><Th r>Win%</Th><Th r>Avg P&L</Th></tr></thead>
            <tbody>{(d.by_month || []).map(m => {
              const open = openMonth === m.month
              const tr = tradesByMonth[m.month] || []
              const vis = tr.slice(0, 200)
              const hidden = tr.length - vis.length
              return (
              <Fragment key={m.month}>
                <tr className="border-b border-white/[0.04] cursor-pointer hover:bg-white/[0.04]"
                    onClick={() => setOpenMonth(open ? null : m.month)}>
                  <Td><span className="text-md-on-surface-var/40 mr-1">{open ? '▾' : '▸'}</span>{m.month}</Td>
                  <Td r>{m.n}</Td><Td r>{m.win_rate}%</Td>
                  <Td r cls={pnlC(m.avg_pnl)}>{fmtPct(m.avg_pnl)}</Td>
                </tr>
                {open && (
                  <tr><td colSpan={4} className="px-2 py-2 bg-black/20">
                    <table className="w-full text-[11px]"><thead><tr className="text-md-on-surface-var/60 border-b border-white/10">
                      <th className="text-left px-1 py-0.5">Ticker</th><th className="text-left px-1">Bought (open)</th>
                      <th className="text-left px-1">Exit</th><th className="text-right px-1">Entry</th>
                      <th className="text-right px-1">Exit$</th>
                      <th className="text-right px-1" title="max DOWN spike between fill and exit">↓ spike</th>
                      <th className="text-right px-1" title="max UP spike between fill and exit">↑ spike</th>
                      <th className="text-right px-1">P&L</th><th className="text-left px-1">Reason</th></tr></thead>
                      <tbody>{vis.map((t, i) => (
                        <tr key={i} className="border-b border-white/[0.03] cursor-pointer hover:bg-white/[0.03]"
                            onClick={() => pickTrade(t)}
                            title="show chart with signal · buy · sell markers">
                          <td className="px-1 py-0.5 font-mono font-semibold">{t.ticker}</td>
                          <td className="px-1 text-md-on-surface-var">{t.open_date}</td>
                          <td className="px-1 text-md-on-surface-var">{t.close_date}</td>
                          <td className="px-1 text-right font-mono">${t.entry}</td>
                          <td className="px-1 text-right font-mono">${t.exit}</td>
                          <td className="px-1 text-right font-mono text-rose-300/80" title="lowest the price went vs entry before exiting">{t.mae != null ? `${t.mae}%` : '—'}</td>
                          <td className="px-1 text-right font-mono text-emerald-300/80" title="highest the price went vs entry before exiting">{t.mfe != null ? `+${t.mfe}%` : '—'}</td>
                          <td className={`px-1 text-right font-mono ${pnlC(t.pnl)}`}>{fmtPct(t.pnl)}</td>
                          <td className="px-1 text-md-on-surface-var/70">{t.reason}</td>
                        </tr>))}
                        {hidden > 0 && <tr><td colSpan={9} className="px-2 py-1.5 text-center text-[10px] text-amber-500/70 italic">… {hidden} more trades not shown — reduce date range to see all</td></tr>}
                      </tbody></table>
                  </td></tr>
                )}
              </Fragment>
            )})}</tbody></table>
          <p className="text-[10px] text-md-on-surface-var/50">Retroactive track record using the EXACT journal rules — entry = next-session open (not signal close), hold 20 bars, −35% catastrophe floor, one open per ticker, equal 4% paper bets. Tail-driven (best can be +700%+); regime-varying month to month. This is the honest backtest, not a guarantee.</p>
        </>
      )}
    </div>
  )
}

// Knowledge: per-atom forward edge from the journal's own closed (+ open mark) trades
function Knowledge({ closed, open }) {
  const KEYS = ['L34', 'L46', 'red', 'flush', 'vol', 'FRI64', 'BLUE', 'absorb', 'deep']
  const pool = [...closed.map(p => ({ atoms: p.atoms, pnl: p.pnl_pct })), ...open.map(p => ({ atoms: p.atoms, pnl: p.upnl_pct }))]
    .filter(x => x.pnl != null)
  const rows = KEYS.map(k => {
    const w = pool.filter(x => (x.atoms || []).some(a => a === k || a.startsWith(k)))
    const n = w.length, wins = w.filter(x => x.pnl > 0).length
    const avg = n ? w.reduce((s, x) => s + x.pnl, 0) / n : null
    return { a: k, n, win: n ? Math.round(wins / n * 100) : null, avg }
  })
  return (
    <div>
      <div className="text-sm font-semibold mb-1">Per-atom edge (journal trades, live + closed)</div>
      <table className="w-full text-xs"><thead><tr className="border-b border-white/10">
        <Th>atom</Th><Th r>n</Th><Th r>win%</Th><Th r>avg P&L</Th></tr></thead>
        <tbody>{rows.map(r => (
          <tr key={r.a} className="border-b border-white/[0.04]">
            <td className="px-2 py-1"><span className={`text-[10px] font-mono px-1 rounded border border-white/10 ${atomCls(r.a)}`}>{r.a}</span></td>
            <Td r>{r.n}</Td><Td r>{r.win != null ? r.win + '%' : '—'}</Td>
            <Td r cls={pnlC(r.avg)}>{r.avg != null ? fmtPct(r.avg) : '—'}</Td>
          </tr>))}</tbody></table>
      <p className="text-[10px] text-md-on-surface-var/50 mt-2">Live edge of each atom within this journal's own positions — accumulates as trades close. Validated 5-yr (reference): core +0.85 median fwd excess (6/6 yrs); −45..−10% flush +1.12; path-sim hold-20 +4.6% cost-adj. ⚠ EXIT = hold ~20 bars, no tight stop.</p>
    </div>
  )
}
