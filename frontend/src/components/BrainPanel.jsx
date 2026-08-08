import { useEffect, useState, useCallback, Fragment } from 'react'
import { api } from '../api'
import BrainMap from './BrainMap'

// 📈 Realized equity curve of the paper book — pure SVG, no chart lib. Built from closed
// trades (start $10k + cumulative pnl by close date); dots carry per-trade tooltips.
function EquityChart({ closed }) {
  const START = 10000
  const trades = [...(closed || [])].filter(c => c.closed)
    .sort((a, b) => String(a.closed).localeCompare(String(b.closed)))
  if (!trades.length) return (
    <div className="text-[11px] text-slate-500 px-3 py-2">no closed trades yet — the equity curve draws itself as the paper book realizes P&L.</div>
  )
  let eq = START
  const pts = [{ d: trades[0].closed, v: START, t: 'start' },
    ...trades.map(c => { eq += (c.pnl || 0); return { d: c.closed, v: +eq.toFixed(2), t: c.ticker, pnl: c.pnl } })]
  const W = 640, H = 130, P = 8
  const vs = pts.map(p => p.v)
  const lo = Math.min(...vs, START), hi = Math.max(...vs, START), span = (hi - lo) || 1
  const x = i => P + i * (W - 2 * P) / Math.max(1, pts.length - 1)
  const y = v => H - P - (v - lo) * (H - 2 * P) / span
  const line = pts.map((p, i) => `${x(i)},${y(p.v)}`).join(' ')
  const last = pts[pts.length - 1]
  const up = last.v >= START
  const wins = trades.filter(c => (c.pnl || 0) >= 0).length
  return (
    <div className="rounded border border-slate-800 bg-slate-900/40 px-3 py-2 mb-4">
      <div className="flex items-center gap-4 text-[11px] text-slate-400 mb-1 flex-wrap">
        <span className="font-semibold text-slate-300">📈 EQUITY (realized, paper)</span>
        <span>now <b className={up ? 'text-emerald-300' : 'text-red-300'}>${last.v.toFixed(0)}</b></span>
        <span>P&L <b className={up ? 'text-emerald-300' : 'text-red-300'}>{up ? '+' : ''}{(last.v - START).toFixed(0)}</b></span>
        <span>trades <b className="text-slate-300">{trades.length}</b> · win <b className="text-slate-300">{(100 * wins / trades.length).toFixed(0)}%</b></span>
        <span>peak <b className="text-slate-300">${hi.toFixed(0)}</b></span>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 130 }}>
        <line x1={P} x2={W - P} y1={y(START)} y2={y(START)} stroke="#475569" strokeDasharray="4 3" strokeWidth="1" />
        <text x={W - P - 2} y={y(START) - 3} textAnchor="end" fontSize="9" fill="#64748b">$10k</text>
        <polyline points={line} fill="none" stroke={up ? '#34d399' : '#f87171'} strokeWidth="1.6" />
        {pts.map((p, i) => i > 0 && (
          <circle key={i} cx={x(i)} cy={y(p.v)} r="2.6"
            fill={(p.pnl || 0) >= 0 ? '#34d399' : '#f87171'} opacity="0.9">
            <title>{`${p.t} · ${p.d} · pnl ${(p.pnl || 0) >= 0 ? '+' : ''}${p.pnl} → $${p.v.toFixed(0)}`}</title>
          </circle>
        ))}
      </svg>
    </div>
  )
}

// 🧠 Decision-brain panel — today's risk-budgeted BUY plans with their full layer chain.
// Read-only view of /api/brain/decisions (regime L2 -> candidate L3/4 -> sizing L5 -> portfolio L8).
// Clicking a ticker opens the REAL Superchart (no popup — user request 2026-08-03) with the
// trade's anatomy as tradeMarkers: ⚡SIG (fire bar) · BUY (entry) · SELL (exit) + price lines.
export default function BrainPanel({ onSelectTicker, onOpenChart }) {
  // map any brain row (position / pending order / closed trade / decision) → tradeMarkers
  const openChart = (row) => {
    const t = row.ticker
    const trade = {
      ticker: t,
      signal_date: row.fire_date || row.pullback?.fire_date || null,
      open_date: row.opened || null,
      close_date: row.closed || null,
      entry: row.entry ?? row.below ?? row.pullback?.below ?? null,
      exit: row.exit ?? null,
    }
    if (onOpenChart) onOpenChart(t, trade)
    else onSelectTicker?.(t)
  }
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)
  const [open, setOpen] = useState({})
  const [showWatch, setShowWatch] = useState(false)
  const [critique, setCritique] = useState(false)
  const [closed, setClosed] = useState([])
  const [pm, setPm] = useState({})          // ticker -> LLM narrative
  const [learnLog, setLearnLog] = useState([])
  const [showLog, setShowLog] = useState(false)
  const [openCl, setOpenCl] = useState({})  // expanded closed-trade autopsies
  const [view, setView] = useState('decisions')  // 'decisions' | 'map'
  const [pend, setPend] = useState(null)    // 🎯 pending pullback orders
  const [reqs, setReqs] = useState([])      // ❓ open data-gap questions
  const [at, setAt] = useState(null)        // last auto-take run (opus verdict + placed/filled)
  const [atBusy, setAtBusy] = useState(false)
  const [ansBusy, setAnsBusy] = useState(false)
  const [ans, setAns] = useState(null)      // opus answers preview

  const load = useCallback((crit = false) => {
    setLoading(true); setErr(null)
    api.brainDecisions(crit)
      .then(d => { if (d.error) setErr(d.error); else setData(d) })
      .catch(e => setErr(String(e?.message || e)))
      .finally(() => setLoading(false))
    api.brainClosed().then(d => setClosed(d?.closed || [])).catch(() => {})
    api.brainLearningLog().then(d => setLearnLog(d?.log || [])).catch(() => {})
    api.brainPending().then(d => setPend(d?.error ? null : d)).catch(() => {})
    api.brainRequests('open').then(d => setReqs(d?.requests || d || [])).catch(() => {})
  }, [])

  const runAutoTake = (apply) => {
    setAtBusy(true)
    api.brainAutoTake(apply)
      .then(d => { setAt(d); if (apply) load(critique) })
      .catch(e => setErr(String(e?.message || e)))
      .finally(() => setAtBusy(false))
  }
  const runOpusAnswers = (apply) => {
    setAnsBusy(true)
    api.brainAnswerRequests(apply)
      .then(d => { setAns(d); if (apply) load(critique) })
      .catch(e => setErr(String(e?.message || e)))
      .finally(() => setAnsBusy(false))
  }
  useEffect(() => { load(false) }, [load])

  const explain = (ticker) => {
    setPm(p => ({ ...p, [ticker]: { loading: true } }))
    api.brainPostmortem(ticker)
      .then(d => setPm(p => ({ ...p, [ticker]: d?.narrative || { narrative: d?.error || 'no analysis' } })))
      .catch(e => setPm(p => ({ ...p, [ticker]: { narrative: String(e?.message || e) } })))
  }

  const take = (x) => {
    api.brainOpen({ ...x, opened: data?.as_of })
      .then(r => { if (r?.error) setErr(r.error); else load() })
      .catch(e => setErr(String(e?.message || e)))
  }
  const close = (ticker, entry) => {
    const px = window.prompt(`Close ${ticker} at price:`, String(entry))
    if (px == null) return
    api.brainClose(ticker, parseFloat(px))
      .then(r => { if (r?.error) setErr(r.error); else load() })
      .catch(e => setErr(String(e?.message || e)))
  }

  const alloc = data?.allocated ?? []
  const watch = data?.watchlist ?? []
  const acct = data?.account
  const positions = data?.open_positions ?? []
  const totalRisk = alloc.reduce((s, x) => s + (x.risk_dollars || 0), 0)
  const totalVal = alloc.reduce((s, x) => s + (x.position_value || 0), 0)
  const reg = data?.regime

  const tierCls = (t) => t === 'core'
    ? 'bg-emerald-900 text-emerald-300' : 'bg-amber-900 text-amber-300'

  // autopsy verdict → colour + label
  const QCLS = {
    good_buy: 'bg-emerald-800 text-emerald-200',
    acceptable_loss: 'bg-slate-700 text-slate-300',
    failed_trade: 'bg-red-900 text-red-300',
  }
  const ATTR_LABEL = {
    as_expected: 'edge behaved as validated',
    edge_miss: 'edge miss — one loss inside its base rate',
    regime_drag: 'regime drag — the tape, not the edge',
  }
  const LOG_ICON = { calibration: '⚖️', calibrate_run: '⚖️', data_decay: '📉', data_weakened: '📉',
    revalidate_run: '🔬', trade_autopsy: '🔎' }

  const ViewTabs = (
    <div className="inline-flex rounded overflow-hidden border border-slate-700 mr-2">
      <button onClick={() => setView('decisions')} className={`px-2 py-1 text-xs ${view === 'decisions' ? 'bg-slate-700 text-slate-100' : 'bg-slate-900 text-slate-400 hover:bg-slate-800'}`}>🧠 Decisions</button>
      <button onClick={() => setView('map')} className={`px-2 py-1 text-xs ${view === 'map' ? 'bg-slate-700 text-slate-100' : 'bg-slate-900 text-slate-400 hover:bg-slate-800'}`}>🕸 Brain Map</button>
    </div>
  )

  if (view === 'map') return (
    <div>
      <div className="px-4 pt-4">{ViewTabs}</div>
      <BrainMap />
    </div>
  )

  return (
    <div className="p-4 text-slate-200 max-w-6xl">
      <div className="flex items-center gap-3 mb-3 flex-wrap">
        {ViewTabs}
        <h2 className="text-base font-semibold text-slate-100">🧠 Decision Brain</h2>
        <button onClick={() => load(critique)} className="bg-slate-700 hover:bg-slate-600 rounded px-2 py-1 text-xs">
          {loading ? '…' : '↻ refresh'}
        </button>
        <button onClick={() => { const n = !critique; setCritique(n); load(n) }}
          className={`rounded px-2 py-1 text-xs ${critique ? 'bg-violet-800 text-violet-200' : 'bg-slate-800 text-slate-400'}`}
          title="Run the LLM agents: regime-synth + adversarial critic on the allocated set (~20-30s, needs API key)">
          🤖 critique {critique ? 'on' : 'off'}
        </button>
        {data?.as_of && <span className="text-xs text-slate-500">as of {data.as_of}</span>}
        {err && <span className="text-xs text-red-400">{err}</span>}
      </div>

      {/* 🤖 regime-synth annotation (agent) */}
      {data?.regime_synth && (
        <div className="mb-3 rounded border border-violet-900/60 bg-violet-950/30 px-3 py-2">
          <div className="text-[11px] text-violet-300 mb-0.5">🤖 regime-synth {data.regime_synth.risk_adjust < 0 && <span className="text-amber-300">· risk {data.regime_synth.risk_adjust}</span>}</div>
          <div className="text-xs text-slate-300">{data.regime_synth.annotation}</div>
          {(data.regime_synth.flags || []).map((f, i) => <div key={i} className="text-[11px] text-slate-500 mt-0.5">• {f}</div>)}
        </div>
      )}

      {/* regime permission (Layer 2) */}
      {reg && (
        <div className="mb-4 rounded border border-slate-700 bg-slate-900/50 px-3 py-2 flex items-center gap-3 flex-wrap">
          <span className="text-xs text-slate-400">Regime (L2):</span>
          <span className={`px-2 py-0.5 rounded text-xs font-bold ${reg.risk_mult >= 1 ? 'bg-emerald-800 text-emerald-200' : reg.risk_mult > 0 ? 'bg-amber-800 text-amber-200' : 'bg-red-900 text-red-300'}`}>
            risk ×{reg.risk_mult}
          </span>
          <span className="text-xs text-slate-300">setups: <b>{reg.setups}</b></span>
          <span className="text-xs text-slate-500">{(reg.reasons || []).join(' · ')}</span>
        </div>
      )}

      {/* summary */}
      <div className="mb-3 flex items-center gap-4 text-xs text-slate-400 flex-wrap">
        <span>candidates <b className="text-slate-200">{data?.n_candidates ?? '—'}</b></span>
        <span>eligible <b className="text-slate-200">{data?.n_eligible ?? '—'}</b></span>
        <span>allocated <b className="text-emerald-300">{alloc.length}</b></span>
        <span>risk <b className={totalRisk > 600 ? 'text-amber-300' : 'text-slate-200'}>${totalRisk.toFixed(0)}</b> ({(totalRisk / 100).toFixed(1)}% of $10k)</span>
        <span>deployed <b className="text-slate-200">${totalVal.toFixed(0)}</b> ({(totalVal / 100).toFixed(0)}%)</span>
      </div>

      {/* account state (L9) */}
      {acct && (
        <div className="mb-3 flex items-center gap-4 text-xs flex-wrap rounded border border-slate-800 bg-slate-900/40 px-3 py-1.5">
          <span className="text-slate-400">Book (L9):</span>
          <span>equity <b className="text-slate-200">${acct.equity}</b></span>
          <span>P&L <b className={acct.realized_pnl >= 0 ? 'text-emerald-300' : 'text-red-300'}>${acct.realized_pnl}</b></span>
          <span>drawdown <b className={acct.drawdown > 0 ? 'text-amber-300' : 'text-slate-200'}>{(acct.drawdown * 100).toFixed(1)}%</b></span>
          <span>open <b className="text-slate-200">{acct.open_positions}</b> (risk {(acct.open_risk_pct * 100).toFixed(1)}%)</span>
          <span>streak <b className={acct.losing_streak ? 'text-red-300' : 'text-slate-200'}>{acct.losing_streak}</b></span>
        </div>
      )}

      {/* 📈 realized equity curve of the paper book */}
      <EquityChart closed={closed} />

      {/* 🧠 OPUS auto-take console: fills pending orders, then queues pullback orders for
          today's allocated BUYs — with Opus 5 holding the last word (take/skip). */}
      <div className="mb-4 rounded border border-violet-900/60 bg-violet-950/20 px-3 py-2">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs font-semibold text-violet-300">🧠 OPUS DECIDER · 🎯 pullback orders</span>
          <button onClick={() => runAutoTake(false)} disabled={atBusy}
            className="bg-slate-800 hover:bg-slate-700 rounded px-2 py-0.5 text-xs text-slate-200"
            title="Preview: fills due pullback orders + asks Opus 5 to take/skip today's allocated BUYs. Nothing is written.">
            {atBusy ? '…' : '▶ preview'}
          </button>
          <button onClick={() => runAutoTake(true)} disabled={atBusy}
            className="bg-violet-800 hover:bg-violet-700 rounded px-2 py-0.5 text-xs text-violet-100"
            title="Apply: writes paper book + pending orders (no real money).">
            ✅ apply
          </button>
          <span className="text-[11px] text-slate-500">fills due orders → Opus takes/skips new BUYs → queues 🎯 dip-and-reclaim orders (low[fire]−, 5 bars)</span>
        </div>
        {at && (
          <div className="mt-2 text-[11px] leading-relaxed">
            {at.opus && (
              <div className="rounded border border-violet-900/50 bg-violet-950/40 px-2 py-1 mb-1 text-slate-300">
                🧠 <b className="text-violet-300">{at.opus_model || 'opus'}</b>: {at.opus}
              </div>
            )}
            {(at.taken || []).length > 0 && <div className="text-emerald-300">filled: {(at.taken || []).map(f => `${f.ticker} @$${f.entry}`).join(' · ')}</div>}
            {(at.placed || []).length > 0 && <div className="text-sky-300">🎯 placed: {(at.placed || []).map(p => `${p.ticker} below $${p.below}`).join(' · ')}</div>}
            {(at.expired || []).length > 0 && <div className="text-slate-500">expired: {(at.expired || []).map(x => x.ticker).join(' · ')}</div>}
            {(at.skipped || []).filter(s => String(s.why || '').startsWith('🧠')).map((s, i) => (
              <div key={i} className="text-amber-300/90">{s.ticker} — {s.why}</div>
            ))}
            {!(at.taken || []).length && !(at.placed || []).length && !(at.expired || []).length &&
              <div className="text-slate-500">nothing to fill or place today ({at.n_allocated ?? 0} allocated)</div>}
          </div>
        )}
      </div>

      {/* 🎯 pending pullback orders (waiting for their dip-and-reclaim trigger) */}
      {(pend?.orders?.length > 0) && (
        <div className="mb-4">
          <div className="text-xs font-semibold text-sky-300 mb-1">🎯 PENDING PULLBACK ORDERS</div>
          <div className="rounded border border-slate-800 overflow-hidden">
            <table className="w-full text-xs">
              <tbody>
                {pend.orders.map(o => {
                  const w = (pend.waiting || []).find(x => x.ticker === o.ticker)
                  const wf = (pend.would_fill || []).find(x => x.ticker === o.ticker)
                  return (
                    <tr key={o.ticker} className="border-t border-slate-800 hover:bg-slate-800/40">
                      <td className="px-2 py-1"><button className="font-bold text-sky-300 hover:underline" title="🕯️ chart with signal + trigger" onClick={() => openChart(o)}>{o.ticker}</button></td>
                      <td className="px-2 py-1 text-slate-400">{o.edge}</td>
                      <td className="px-2 py-1 font-mono">buy &lt; <span className="text-sky-300">${o.below}</span> + green close</td>
                      <td className="px-2 py-1 text-slate-500">fired {o.fire_date}</td>
                      <td className="px-2 py-1">
                        {wf && <span className="text-emerald-300">✓ would fill @${wf.entry}</span>}
                        {!wf && w && <span className="text-slate-400">{w.bars_left} bars left</span>}
                        {!wf && !w && <span className="text-slate-600">—</span>}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ❓ needs input — the brain's open questions, answerable by Opus or by hand */}
      {reqs.length > 0 && (
        <div className="mb-4 rounded border border-amber-900/50 bg-amber-950/20 px-3 py-2">
          <div className="flex items-center gap-2 flex-wrap mb-1">
            <span className="text-xs font-semibold text-amber-300">❓ NEEDS INPUT — {reqs.length} open</span>
            <button onClick={() => runOpusAnswers(false)} disabled={ansBusy}
              className="bg-slate-800 hover:bg-slate-700 rounded px-2 py-0.5 text-xs text-slate-200"
              title="Preview Opus 5's answers (paper fill = plan price; catalysts never invented — policy answers).">
              {ansBusy ? '…' : '🧠 opus preview'}
            </button>
            <button onClick={() => runOpusAnswers(true)} disabled={ansBusy}
              className="bg-amber-800 hover:bg-amber-700 rounded px-2 py-0.5 text-xs text-amber-100"
              title="Record Opus's answers (marked answered_by: opus-5; fill prices apply to the book).">
              ✅ opus apply
            </button>
          </div>
          {reqs.map(r => {
            const a = (ans?.answers || []).find(x => x.id === r.id)
            return (
              <div key={r.id} className="text-[11px] py-1 border-t border-amber-900/30">
                <div className="text-slate-300">
                  <b className="text-sky-300">{r.ticker}</b> · {r.question}
                  <button className="ml-2 text-slate-500 hover:text-slate-300"
                    onClick={() => { const v = window.prompt(r.question); if (v != null && v !== '') api.brainAnswer(r.id, v).then(() => load(critique)) }}>
                    ✍ answer
                  </button>
                </div>
                {a && <div className="text-violet-300/90 mt-0.5">🧠 {a.value}{a.note && <span className="text-slate-500"> · {a.note}</span>}</div>}
              </div>
            )
          })}
        </div>
      )}

      {/* open positions */}
      {positions.length > 0 && (
        <div className="mb-4">
          <div className="text-xs font-semibold text-sky-300 mb-1">OPEN POSITIONS</div>
          <div className="rounded border border-slate-800 overflow-hidden">
            <table className="w-full text-xs">
              <tbody>
                {positions.map(p => (
                  <tr key={p.ticker} className="border-t border-slate-800 hover:bg-slate-800/40">
                    <td className="px-2 py-1"><button className="font-bold text-sky-300 hover:underline" title="🕯️ chart with signal + entry points" onClick={() => openChart(p)}>{p.ticker}</button></td>
                    <td className="px-2 py-1 text-slate-400">{p.edge}</td>
                    <td className="px-2 py-1 font-mono">{p.shares}sh @${p.entry}</td>
                    <td className="px-2 py-1 font-mono text-red-300">stop ${p.stop}</td>
                    <td className="px-2 py-1 font-mono text-slate-400">${p.position_value}</td>
                    <td className="px-2 py-1 text-slate-500">{p.opened}</td>
                    <td className="px-2 py-1"><button className="text-red-400 hover:text-red-300" onClick={() => close(p.ticker, p.entry)}>close</button></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* CLOSED TRADES — the autopsy: why each was a good buy or a failure */}
      {closed.length > 0 && (
        <div className="mb-4">
          <div className="text-xs font-semibold text-slate-300 mb-1">CLOSED — win/loss autopsy</div>
          <div className="rounded border border-slate-800 overflow-hidden">
            <table className="w-full text-xs">
              <tbody>
                {closed.map((c, i) => {
                  const a = c.analysis || {}
                  const win = (c.pnl || 0) >= 0
                  return (
                    <Fragment key={c.ticker + i}>
                      <tr className="border-t border-slate-800 hover:bg-slate-800/40">
                        <td className="px-2 py-1"><button className="font-bold text-sky-300 hover:underline" title="🕯️ chart with entry + exit points" onClick={() => openChart(c)}>{c.ticker}</button></td>
                        <td className="px-2 py-1"><span className={`px-1 rounded ${tierCls(c.tier)}`} title={c.edge_title}>{c.edge}</span></td>
                        <td className="px-2 py-1 font-mono">${c.entry}→${c.exit}</td>
                        <td className={`px-2 py-1 font-mono ${win ? 'text-emerald-300' : 'text-red-300'}`}>${c.pnl} <span className="text-slate-500">({a.ret_pct > 0 ? '+' : ''}{a.ret_pct}% · {a.r_multiple}R)</span></td>
                        <td className="px-2 py-1"><span className={`px-1 rounded ${QCLS[a.quality] || 'bg-slate-700 text-slate-300'}`} title={a.verdict}>{a.verdict}</span></td>
                        <td className="px-2 py-1 text-slate-400 truncate max-w-[180px]" title={ATTR_LABEL[a.attribution] || a.attribution}>{ATTR_LABEL[a.attribution] || a.attribution}</td>
                        <td className="px-2 py-1"><button className="text-slate-500 hover:text-slate-300" onClick={() => setOpenCl(o => ({ ...o, [c.ticker]: !o[c.ticker] }))}>{openCl[c.ticker] ? '▾' : '▸ why'}</button></td>
                      </tr>
                      {openCl[c.ticker] && (
                        <tr className="bg-slate-900/60">
                          <td colSpan={7} className="px-3 py-2 text-[11px] text-slate-400 leading-relaxed">
                            <div className="mb-1 text-slate-300">Autopsy — {a.verdict} · planned R:R {a.planned_rr} · exit reason: {c.reason || '—'}</div>
                            {(a.factors || []).map((f, j) => <div key={j} className="text-slate-500">• {f}</div>)}
                            <div className="mt-1 text-amber-300/90">💡 {a.lesson}</div>
                            {/* entry rationale that was recorded when bought */}
                            {(c.log || []).length > 0 && (
                              <details className="mt-1"><summary className="cursor-pointer text-slate-500">why it was bought</summary>
                                <div className="font-mono mt-1">{c.log.map((l, j) => <div key={j}>{j + 1}. {l}</div>)}</div>
                              </details>
                            )}
                            {/* LLM narrative on demand */}
                            <div className="mt-1">
                              {!pm[c.ticker] && <button className="text-violet-300 hover:text-violet-200" onClick={() => explain(c.ticker)}>🤖 explain in words</button>}
                              {pm[c.ticker]?.loading && <span className="text-slate-500">🤖 thinking…</span>}
                              {pm[c.ticker] && !pm[c.ticker].loading && (
                                <div className="rounded border border-violet-900/50 bg-violet-950/20 px-2 py-1 mt-0.5">
                                  <div className="text-slate-300">{pm[c.ticker].narrative}</div>
                                  {pm[c.ticker].takeaway && <div className="text-violet-300 mt-0.5">→ {pm[c.ticker].takeaway}</div>}
                                  {pm[c.ticker].llm === false && <div className="text-[10px] text-slate-600 mt-0.5">(LLM off — deterministic autopsy; set ANTHROPIC_API_KEY for a narrative)</div>}
                                </div>
                              )}
                            </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ALLOCATED — buy these today */}
      <div className="text-xs font-semibold text-emerald-300 mb-1">BUY TODAY — fits the risk budget</div>
      <div className="rounded border border-slate-800 overflow-hidden mb-4">
        <table className="w-full text-xs">
          <thead className="bg-slate-800/60 text-slate-400">
            <tr>
              {['Ticker', 'Edge', 'Shares', 'Entry', 'Stop', 'Target', 'Risk $', 'Value', 'Sector', ''].map(h =>
                <th key={h} className="px-2 py-1 text-left font-medium">{h}</th>)}
            </tr>
          </thead>
          <tbody>
            {alloc.map((x, i) => (
              <Fragment key={x.ticker}>
                <tr className="border-t border-slate-800 hover:bg-slate-800/40">
                  <td className="px-2 py-1">
                    <button className="font-bold text-sky-300 hover:underline" title="🕯️ chart with signal + planned entry"
                      onClick={() => openChart(x)}>{x.ticker}</button>
                  </td>
                  <td className="px-2 py-1">
                    <span className={`px-1 rounded ${tierCls(x.tier)}`} title={x.edge_title}>{x.edge}</span>
                    {x.critic && x.critic.verdict !== 'pass' && (
                      <span className="ml-1 px-1 rounded bg-amber-900 text-amber-300" title={(x.critic.concerns || []).join(' · ')}>🤖 {x.critic.verdict}</span>
                    )}
                    {x.critic && x.critic.verdict === 'pass' && <span className="ml-1 text-emerald-500" title="critic: pass">🤖✓</span>}
                  </td>
                  <td className="px-2 py-1 font-mono">{x.shares}</td>
                  <td className="px-2 py-1 font-mono">${x.entry}</td>
                  <td className="px-2 py-1 font-mono text-red-300">${x.stop}</td>
                  <td className="px-2 py-1 font-mono text-emerald-300">${x.target}</td>
                  <td className="px-2 py-1 font-mono">${x.risk_dollars}</td>
                  <td className="px-2 py-1 font-mono text-slate-400">${x.position_value}</td>
                  <td className="px-2 py-1 text-slate-400 truncate max-w-[110px]">{x.sector}</td>
                  <td className="px-2 py-1 whitespace-nowrap">
                    <button className="text-emerald-400 hover:text-emerald-300 font-semibold mr-2" onClick={() => take(x)}>+ take</button>
                    <button className="text-slate-500 hover:text-slate-300" onClick={() => setOpen(o => ({ ...o, [x.ticker]: !o[x.ticker] }))}>
                      {open[x.ticker] ? '▾' : '▸'}
                    </button>
                  </td>
                </tr>
                {open[x.ticker] && (
                  <tr className="bg-slate-900/60">
                    <td colSpan={10} className="px-3 py-2 text-[11px] text-slate-400 font-mono leading-relaxed">
                      {(x.log || []).map((l, j) => <div key={j}>{j + 1}. {l}</div>)}
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
            {!alloc.length && !loading && (
              <tr><td colSpan={10} className="px-3 py-4 text-center text-slate-500">no BUY today</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* 🤖 vetoed by the critic */}
      {(data?.vetoed?.length > 0) && (
        <div className="mb-4">
          <div className="text-xs font-semibold text-amber-300 mb-1">🤖 VETOED by critic</div>
          {data.vetoed.map(x => (
            <div key={x.ticker} className="text-xs text-slate-400 py-0.5">
              <span className="font-bold text-slate-300">{x.ticker}</span> {x.edge} —
              <span className="text-amber-300"> {x.critic?.which_rule || 'concern'}</span>: {(x.critic?.concerns || []).join(' · ')}
            </div>
          ))}
        </div>
      )}

      {/* watchlist — all eligible, ranked (context) */}
      <button className="text-xs text-slate-400 hover:text-slate-200 mb-1" onClick={() => setShowWatch(s => !s)}>
        {showWatch ? '▾' : '▸'} watchlist — {watch.length} eligible (didn't fit the budget)
      </button>
      {showWatch && (
        <div className="flex flex-wrap gap-1">
          {watch.slice(0, 120).map(x => (
            <button key={x.ticker}
              onClick={() => openChart(x)}
              title={`${x.edge_title} · ${x.shares}sh @$${x.entry} · 🕯️ chart`}
              className="px-1.5 py-0.5 rounded text-[11px] bg-slate-800 hover:bg-slate-700 text-slate-300 font-mono">
              {x.ticker}<span className="text-slate-500"> {x.edge?.slice(0, 4)}</span>
            </button>
          ))}
        </div>
      )}

      {/* LEARNING LOG — the brain's own memory of why it changed its mind */}
      <div className="mt-5">
        <button className="text-xs font-semibold text-slate-300 hover:text-slate-100" onClick={() => setShowLog(s => !s)}>
          {showLog ? '▾' : '▸'} 🧠 LEARNING LOG — {learnLog.length} lessons (own memory)
        </button>
        {showLog && (
          <div className="mt-1 rounded border border-slate-800 bg-slate-900/40 divide-y divide-slate-800/60">
            {learnLog.length === 0 && <div className="px-3 py-2 text-[11px] text-slate-500">no lessons yet — the brain writes here when it calibrates on trades or re-validates on data.</div>}
            {[...learnLog].reverse().slice(0, 40).map((l, i) => (
              <div key={i} className="px-3 py-1.5 text-[11px] flex gap-2">
                <span className="text-slate-600 font-mono whitespace-nowrap">{l.date}</span>
                <span>{LOG_ICON[l.kind] || '•'}</span>
                <span className="text-slate-400">
                  {l.edge && <b className="text-slate-300">{l.edge}</b>}{l.ticker && <span className="text-sky-400"> {l.ticker}</span>}
                  {' '}{l.observation}
                  {l.action && <span className="text-amber-300"> → {l.action}</span>}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      <p className="text-[10px] text-slate-500 mt-4 max-w-3xl leading-relaxed">
        Chain: regime permission (L2) → registry edge fired (L3/4) → risk-sizing 1%/trade + R:R gate (L5)
        → portfolio 6%-risk / sector / drawdown envelope (L8) → BUY → book (L9). Closed trades are
        auto-dissected against the edge's base-rate (win/loss autopsy). The brain learns two ways:
        calibrate() on its own outcomes + revalidate() on the data — both write lessons to the log above.
      </p>
    </div>
  )
}
