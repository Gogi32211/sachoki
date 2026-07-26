// Capit→Atom Journal — the PREMIUM confluence subset of the Atomic edge: a weak-close
// gap-up (Atomic) that FOLLOWS a recent B+ capitulation (≤capit_window days) on the same
// ticker. Same Atomic path-sim rules (entry next-open, -15% stop / +100% target / 20-bar),
// filtered to 🔥post-capit trades only. Mirrors the Capit Journal's Replay design/structure.
import { useEffect, useState, useMemo, useRef, Fragment } from 'react'
import CodeCandleChart from './CodeCandleChart'
import JournalBench from './JournalBench'

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(1)}%`)
const fmtNum = (v) => (v == null ? '—' : Number(v).toLocaleString())
const pnlC = (p) => (p == null ? '' : p >= 0 ? 'text-emerald-400' : 'text-rose-400')
const Kpi = ({ label, v }) => <div><div className="text-xs text-md-on-surface-var">{label}</div><div className="text-lg font-bold font-mono">{v}</div></div>
const Th = ({ children, r }) => <th className={`px-2 py-1 font-semibold text-md-on-surface-var ${r ? 'text-right' : 'text-left'}`}>{children}</th>
const Td = ({ children, r, cls = '' }) => <td className={`px-2 py-1 font-mono ${r ? 'text-right' : 'text-left'} ${cls}`}>{children}</td>

export default function CapitAtomJournalPanel({ onSelectTicker }) {
  const [tab, setTab] = useState('prebuy')   // 'replay' | 'prebuy'
  const [months, setMonths] = useState(12)
  const [cw, setCw] = useState(15)            // capit_window (days) — how recent the B+ capit must be
  const [d, setD] = useState(null)
  const [loading, setLoading] = useState(false)
  const [openMonth, setOpenMonth] = useState(null)
  const [chartTrade, setChartTrade] = useState(null)   // inline chart at top (no navigation)
  const chartRef = useRef(null)
  const pickTrade = (t) => { setChartTrade(t); setTimeout(() => chartRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' }), 40) }
  // PreBuy tab state
  const [pb, setPb] = useState(null)
  const [pbLoading, setPbLoading] = useState(false)
  const [pbCw, setPbCw] = useState(15)
  const loadPrebuy = (w = pbCw) => {
    setPbCw(w); setPbLoading(true); setPb(null)
    fetch(`/api/capit-atom-journal/prebuy?capit_window=${w}`)
      .then(r => r.json()).then(setPb).catch(() => {}).finally(() => setPbLoading(false))
  }
  // MANUAL mode — hand-entered limit-entry % / target % / stop % / hold (filter the entry)
  const [manual, setManual] = useState(false)
  const [entryPct, setEntryPct] = useState(4)
  const [targetPct, setTargetPct] = useState(15)
  const [stopPct, setStopPct] = useState(8)
  const [holdBars, setHoldBars] = useState(20)
  const [entryWin, setEntryWin] = useState(5)
  const run = (m, w, man = manual) => {
    setMonths(m); setCw(w); setManual(man); setLoading(true); setD(null); setOpenMonth(null)
    let url = `/api/capit-atom-journal/replay?months=${m}&capit_window=${w}`
    if (man) url += `&manual=true&entry_pct=${(+entryPct || 0) / 100}&target_pct=${(+targetPct || 0) / 100}`
           + `&stop_pct=${(+stopPct || 0) / 100}&hold=${+holdBars || 20}&entry_win=${+entryWin || 5}`
    fetch(url).then(r => r.json()).then(setD).catch(() => {}).finally(() => setLoading(false))
  }
  useEffect(() => { loadPrebuy(15) }, [])   // auto-load prebuy on first open (default tab)
  const s = d?.stats || {}
  const tradesByMonth = useMemo(() => {
    const g = {}
    for (const t of (d?.trades || [])) (g[t.month] ||= []).push(t)
    for (const k in g) g[k].sort((a, b) => (a.open_date < b.open_date ? -1 : 1))
    return g
  }, [d])

  return (
    <div className="p-3 max-w-[1100px]">
      {/* Inline chart at top — signal · buy · sell markers, no navigation */}
      {chartTrade && (
        <div ref={chartRef} className="mb-3 scroll-mt-16 rounded-lg border border-amber-700/30 bg-md-surface-high/40 overflow-hidden">
          <div className="flex items-center gap-2 px-3 py-1.5 text-xs border-b border-white/10">
            <span className="font-bold text-amber-300">{chartTrade.ticker}</span>
            <span className="text-md-on-surface-var/70">
              ⚡{chartTrade.signal_date} · 🟢BUY ${chartTrade.entry} {chartTrade.open_date} · 🔴SELL ${chartTrade.exit} {chartTrade.close_date}
              {chartTrade.pnl != null && <span className={chartTrade.pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}> · {fmtPct(chartTrade.pnl)}</span>}
            </span>
            <button onClick={() => setChartTrade(null)} className="ml-auto text-md-on-surface-var/60 hover:text-md-on-surface">✕</button>
          </div>
          <CodeCandleChart ticker={chartTrade.ticker} tf="1d" height={320} tradeMarkers={chartTrade}
            tradeHistory={(d?.trades || []).filter(t => t.ticker === chartTrade.ticker)} />
        </div>
      )}

      {/* Header — same shell as Capit Jrnl */}
      <div className="mb-3">
        <div className="flex items-center gap-3 mb-2">
          <h2 className="text-lg font-bold flex items-center gap-2">🔥 Capit→Atom Journal
            <span className="text-[11px] font-normal text-amber-300/80">premium confluence subset</span></h2>
          <div className="flex gap-1 ml-auto">
            {[['prebuy', '🛒 PreBuy'], ['replay', '📋 Replay']].map(([t, lbl]) => (
              <button key={t} onClick={() => { setTab(t); if (t === 'prebuy' && !pb) loadPrebuy(); if (t === 'replay' && !d) run(12, 15) }}
                className={`px-3 py-1 rounded text-xs font-semibold ${tab === t ? 'bg-amber-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{lbl}</button>
            ))}
          </div>
        </div>
        {tab === 'replay' && <p className="text-xs text-md-on-surface-var mt-1">
          A weak-close <b>gap-up (Atomic)</b> that <b>follows a recent B+ capitulation</b> (≤{cw}d) on the same ticker —
          the capitulation confirms the bottom, the gap-up is the continuation entry. Atomic rules: entry next-open,
          −15% stop / +100% target / 20-bar, equal 4% paper bets. <span className="text-amber-300">Validated: win ~67%, med +4.2% vs +1.4% baseline.</span>
        </p>}
        {tab === 'prebuy' && <p className="text-xs text-md-on-surface-var mt-1">
          Live scan — Atomic weak-close gap-up signals that ALSO have a recent B+ capitulation on the same ticker.
          These are the freshest 🔥 confluence setups (not yet in the replay). Click a row to view the chart.
          <span className="text-amber-300 ml-1">Refresh to see today's signals.</span>
        </p>}
      </div>

      {/* ─── REPLAY tab ─── */}
      {tab === 'replay' && (<>
      {/* Controls */}
      <div className="flex flex-wrap items-center gap-2 mb-3 text-xs">
        <span className="text-md-on-surface-var">Period:</span>
        {[3, 6, 12, 24].map(m => (
          <button key={m} onClick={() => run(m, cw)} disabled={loading}
            className={`px-2 py-1 rounded ${months === m && d ? 'bg-amber-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{m}mo</button>
        ))}
        <span className="text-md-on-surface-var ml-3" title="how recent the B+ capitulation must be before the gap-up">Capit window:</span>
        {[10, 15, 20].map(w => (
          <button key={w} onClick={() => run(months, w)} disabled={loading}
            className={`px-2 py-1 rounded ${cw === w && d ? 'bg-violet-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>≤{w}d</button>
        ))}
        {loading && <span className="text-amber-400 animate-pulse">running…</span>}
        {d && !manual && <span className="text-md-on-surface-var/60">from {d.win_start} · entry next-open · −15% stop / +100% target / 20-bar</span>}
        <button onClick={() => setManual(v => !v)}
          className={`px-2 py-1 rounded ml-auto ${manual ? 'bg-cyan-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}
          title="Hand-enter limit entry %, target %, stop %, hold — runs the same path-sim on the confluence signals">⚙ Manual</button>
      </div>

      {/* MANUAL controls — limit entry / target / stop / hold (path-sim) over the 🔥 confluence */}
      {manual && (
        <div className="flex flex-wrap items-end gap-3 mb-3 p-2.5 rounded-lg bg-cyan-900/15 border border-cyan-700/40 text-xs">
          {[
            ['Entry limit −%', entryPct, setEntryPct, 'buy this % below the signal-bar close'],
            ['Target +%', targetPct, setTargetPct, 'take-profit this % above fill'],
            ['Stop −%', stopPct, setStopPct, '0 = no stop'],
            ['Hold (bars)', holdBars, setHoldBars, 'max bars before time-stop close'],
            ['Entry window', entryWin, setEntryWin, 'bars the limit stays live'],
          ].map(([lbl, val, setter, hint]) => (
            <label key={lbl} className="flex flex-col gap-0.5" title={hint}>
              <span className="text-cyan-300/80">{lbl}</span>
              <input type="number" value={val} onChange={e => setter(e.target.value)}
                className="w-20 px-2 py-1 rounded bg-md-surface border border-cyan-700/40 font-mono text-md-on-surface" />
            </label>
          ))}
          <button onClick={() => run(months, cw, true)} disabled={loading}
            className="px-3 py-1.5 rounded bg-cyan-600 text-white font-semibold hover:bg-cyan-500 disabled:opacity-50">▶ Run manual</button>
          <span className="text-cyan-300/50 text-[10px] basis-full">
            🔥 confluence + limit buy −{entryPct}% (fills if price dips there within {entryWin} bars) · exit at +{targetPct}% target{+stopPct > 0 ? ` or −${stopPct}% stop` : ''} or {holdBars}-bar close
          </span>
        </div>
      )}

      {d && d.error && <div className="text-rose-400 text-xs py-4">{d.error}</div>}
      {d && !d.error && (
        <>
          <JournalBench stats={s} n={s.n} />
          <div className="flex flex-wrap gap-4 mb-4 p-3 rounded-lg bg-amber-900/15 border border-amber-700/30">
            <Kpi label="Trades" v={s.n} />
            <Kpi label="Win rate" v={s.win_rate != null ? `${s.win_rate}%` : '—'} />
            <Kpi label="Avg P&L" v={s.avg_pnl != null ? fmtPct(s.avg_pnl) : '—'} />
            <Kpi label="Median" v={s.median_pnl != null ? fmtPct(s.median_pnl) : '—'} />
            <div><div className="text-xs text-md-on-surface-var">Equity (4% bets)</div>
              <div className={`text-lg font-bold font-mono ${(s.equity_pct ?? 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>${fmtNum(s.equity_end)} <span className="text-sm">{fmtPct(s.equity_pct)}</span></div></div>
            <Kpi label="Best / Worst" v={`${fmtPct(s.best)} / ${fmtPct(s.worst)}`} />
            <Kpi label="Target / Stop" v={`${s.target_pct ?? '—'}% / ${s.stop_pct ?? '—'}%`} />
            <Kpi label="Avg spike ↑/↓" v={s.avg_mfe != null ? `${fmtPct(s.avg_mfe)} / ${fmtPct(s.avg_mae)}` : '—'} />
            {manual && <Kpi label="Fill rate" v={s.fill_rate != null ? `${s.fill_rate}%` : '—'} />}
            {manual && <Kpi label="Target hit" v={s.target_hit_pct != null ? `${s.target_hit_pct}%` : '—'} />}
            {manual && +stopPct > 0 && <Kpi label="Stop hit" v={s.stop_hit_pct != null ? `${s.stop_hit_pct}%` : '—'} />}
            <Kpi label="Still open" v={s.still_open} />
          </div>

          <div className="text-sm font-semibold mb-1">By month <span className="text-[10px] text-md-on-surface-var/50 font-normal">— click a month to see every confluence trade (ticker · bought · exit · spike · P&L)</span></div>
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
                          <td className="px-1 py-0.5 font-mono font-semibold">{t.ticker} 🔥</td>
                          <td className="px-1 text-md-on-surface-var">{t.open_date}</td>
                          <td className="px-1 text-md-on-surface-var">{t.close_date}</td>
                          <td className="px-1 text-right font-mono">${t.entry}</td>
                          <td className="px-1 text-right font-mono">${t.exit}</td>
                          <td className="px-1 text-right font-mono text-rose-300/80">{t.mae != null ? `${t.mae}%` : '—'}</td>
                          <td className="px-1 text-right font-mono text-emerald-300/80">{t.mfe != null ? `+${t.mfe}%` : '—'}</td>
                          <td className={`px-1 text-right font-mono ${pnlC(t.pnl)}`}>{fmtPct(t.pnl)}</td>
                          <td className="px-1 text-md-on-surface-var/70">{t.reason}</td>
                        </tr>))}
                        {hidden > 0 && <tr><td colSpan={9} className="px-2 py-1.5 text-center text-[10px] text-amber-500/70 italic">… {hidden} more trades not shown — reduce date range to see all</td></tr>}
                      </tbody></table>
                  </td></tr>
                )}
              </Fragment>
            )})}</tbody></table>
          <p className="text-[10px] text-md-on-surface-var/50">Retroactive track record of the Capit→Atomic confluence — the 🔥post-capit subset of the Atomic edge, using the EXACT Atomic rules (entry next-open, −15% stop / +100% target / 20-bar, one open per ticker, equal 4% paper bets). Tail-driven; regime-varying. Honest backtest, not a guarantee.</p>
        </>
      )}
      </>)}

      {/* ─── PREBUY tab ─── */}
      {tab === 'prebuy' && (
        <PreBuyTab pb={pb} loading={pbLoading} pbCw={pbCw} onLoad={loadPrebuy} onPick={pickTrade} />
      )}
    </div>
  )
}

const REGIME_COLOR = { RISK_OFF: 'text-rose-400', NEUTRAL: 'text-amber-300', RISK_ON: 'text-emerald-400' }
const ATOM_CHIP = { R2L: 'bg-violet-900/60 text-violet-200', EO: 'bg-sky-900/60 text-sky-200',
  'vol=B': 'bg-orange-900/60 text-orange-200', 'wick=D': 'bg-slate-700/60 text-slate-200',
  G3: 'bg-emerald-900/60 text-emerald-200' }

function PreBuyTab({ pb, loading, pbCw, onLoad, onPick }) {
  const reg = pb?.regime
  const rows = pb?.rows || []
  return (
    <div>
      {/* Toolbar */}
      <div className="flex flex-wrap items-center gap-2 mb-3 text-xs">
        <span className="text-md-on-surface-var">Capit window:</span>
        {[10, 15, 20].map(w => (
          <button key={w} onClick={() => onLoad(w)}
            className={`px-2 py-1 rounded ${pbCw === w && pb ? 'bg-violet-700 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>≤{w}d</button>
        ))}
        <button onClick={() => onLoad(pbCw)} disabled={loading}
          className="px-3 py-1 rounded bg-amber-700 text-white font-semibold hover:bg-amber-600 disabled:opacity-50 flex items-center gap-1">
          {loading ? <span className="animate-spin">↻</span> : '↻'} Refresh
        </button>
        {pb && <span className="text-md-on-surface-var/60 text-[10px]">as of {pb.as_of}</span>}
        {reg && (
          <span className={`ml-auto font-semibold text-[11px] ${REGIME_COLOR[reg.label] || 'text-md-on-surface-var'}`}>
            {reg.label}{reg.score != null ? ` (${reg.score})` : ''} · size ×{reg.conv_mult ?? 1}
          </span>
        )}
        {loading && <span className="text-amber-400 animate-pulse ml-2">scanning…</span>}
      </div>

      {pb && !rows.length && !loading && (
        <div className="text-md-on-surface-var/60 text-sm py-6 text-center">No 🔥 Capit→Atom signals in the last 5 days. Market may not have produced any — try Refresh later.</div>
      )}

      {rows.length > 0 && (
        <table className="w-full text-xs">
          <thead><tr className="border-b border-white/10 text-md-on-surface-var">
            <th className="text-left px-2 py-1">Score</th>
            <th className="text-left px-2">Ticker</th>
            <th className="text-left px-2">Signal</th>
            <th className="text-right px-2">Age</th>
            <th className="text-right px-2">Capit age</th>
            <th className="text-right px-2">Price</th>
            <th className="text-right px-2">RSI</th>
            <th className="text-right px-2">Vol</th>
            <th className="text-left px-2">Atoms</th>
          </tr></thead>
          <tbody>{rows.map((r, i) => (
            <tr key={i} className="border-b border-white/[0.04] cursor-pointer hover:bg-amber-900/10"
                onClick={() => onPick({ ticker: r.ticker, signal_date: r.signal_date })}
                title="click to view chart at signal date">
              <td className="px-2 py-1.5">
                <span className={`font-bold font-mono text-sm ${r.score >= 90 ? 'text-emerald-400' : r.score >= 70 ? 'text-amber-300' : 'text-md-on-surface-var'}`}>{r.score}</span>
              </td>
              <td className="px-2 font-mono font-bold text-amber-200">{r.ticker} 🔥</td>
              <td className="px-2 text-md-on-surface-var">{r.signal_date}</td>
              <td className="px-2 text-right font-mono">{r.age_days === 0 ? 'today' : `${r.age_days}d ago`}</td>
              <td className="px-2 text-right font-mono text-amber-300/80">{r.capit_age != null ? `capit ${r.capit_age}d` : '—'}</td>
              <td className="px-2 text-right font-mono">${r.close}</td>
              <td className="px-2 text-right font-mono text-violet-300">{r.rsi != null ? r.rsi : '—'}</td>
              <td className="px-2 text-right font-mono">{r.vol}</td>
              <td className="px-2">
                <div className="flex flex-wrap gap-0.5">
                  {(r.atoms || []).filter(a => !a.startsWith('close=') && !a.startsWith('gap') && !a.startsWith('🔥')).map((a, j) => (
                    <span key={j} className={`px-1 py-0.5 rounded text-[10px] font-mono ${ATOM_CHIP[a] || 'bg-white/10 text-md-on-surface-var'}`}>{a}</span>
                  ))}
                </div>
              </td>
            </tr>
          ))}</tbody>
        </table>
      )}
      <p className="text-[10px] text-md-on-surface-var/50 mt-3">
        Live scan — weak-close gap-up (Atomic) with a recent B+ capitulation on the same ticker (≤{pbCw}d).
        Score ≥90 = 5+ atoms stacked (R2L + EO + vol + wick + G3 + post-capit). Click any row to view the chart.
        This is a SCANNER, not a trade signal — validate the chart before acting.
      </p>
    </div>
  )
}
