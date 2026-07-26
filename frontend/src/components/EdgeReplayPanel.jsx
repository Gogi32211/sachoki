import { useState, useEffect, useCallback } from 'react'

// Unified Edge Replay — one path-sim engine backtests ALL Edge setups head-to-head.
// Mirrors the live scanners (backtest == what you'd trade). /api/edge-replay
export default function EdgeReplayPanel({ onSelectTicker }) {
  const [months, setMonths] = useState(36)
  const [mode, setMode]     = useState('trail')         // 'trail' | 'bracket'
  const [trail, setTrail]   = useState(0.25)
  const [stop, setStop]     = useState(0.10)
  const [target, setTarget] = useState(0.25)
  const [maxh, setMaxh]     = useState(60)
  const [rows, setRows]     = useState([])
  const [exit, setExit]     = useState(null)
  const [asOf, setAsOf]     = useState('')
  const [loading, setLoading] = useState(false)
  const [err, setErr]       = useState('')
  const [sortKey, setSortKey] = useState('pf')
  const [drill, setDrill]   = useState(null)            // {setup, trades, per_year}
  const [overfit, setOverfit] = useState(null)          // {pbo, rows:[{setup,dsr,psr0}]} from /api/edge-overfit

  // DSR/PBO overfitting stats (precomputed 62mo, Bailey & López de Prado) — window-independent
  useEffect(() => {
    let dead = false
    fetch('/api/edge-overfit')
      .then(r => r.json())
      .then(d => {
        if (dead || !d?.rows?.length) return
        const map = {}
        for (const r of d.rows) map[r.setup] = r
        setOverfit({ map, pbo: d.pbo, trials: d.n_trials_assumed })
      })
      .catch(() => {})
    return () => { dead = true }
  }, [])

  const run = useCallback(() => {
    setLoading(true); setErr(''); setDrill(null)
    const p = new URLSearchParams({ setup: 'all', months, mode, trail, stop, target, maxh })
    fetch(`/api/edge-replay?${p}`)
      .then(r => r.json())
      .then(d => {
        if (d.error) { setErr(d.error); setRows([]) }
        else { setRows(d.rows || []); setExit(d.exit); setAsOf(d.as_of || '') }
      })
      .catch(e => setErr(String(e)))
      .finally(() => setLoading(false))
  }, [months, mode, trail, stop, target, maxh])

  useEffect(() => { run() }, [])   // initial load only; re-run on explicit "Run"

  const openDrill = (setup) => {
    setLoading(true)
    const p = new URLSearchParams({ setup, months, mode, trail, stop, target, maxh, with_trades: true })
    fetch(`/api/edge-replay?${p}`)
      .then(r => r.json())
      .then(d => {
        const row = (d.rows || [])[0] || {}
        setDrill({ setup, trades: d.trades || [], per_year: row.per_year || {}, stat: row })
      })
      .finally(() => setLoading(false))
  }

  const sorted = [...rows].filter(r => r.n > 0).sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0))
  const pfColor = (pf) => pf >= 2 ? 'text-emerald-400' : pf >= 1.5 ? 'text-teal-300' : pf >= 1.2 ? 'text-yellow-300' : 'text-slate-400'
  const Th = ({ k, children, cls, title }) => (
    <th onClick={() => k && setSortKey(k)} title={title}
        className={`px-2 py-1.5 ${k ? 'cursor-pointer hover:text-white' : ''} ${sortKey === k ? 'text-white' : ''} ${cls || ''}`}>
      {children}{sortKey === k ? ' ▾' : ''}
    </th>
  )

  return (
    <div className="p-4 text-slate-200 max-w-6xl">
      <h2 className="text-xl font-bold text-emerald-300 flex items-center gap-2">🔁 Edge Replay</h2>
      <p className="text-sm text-slate-400 mt-1 mb-3">
        ერთი path-sim ძრავა ყველა Edge setup-ს ისტორიულად ატესტავs — <b>backtest == რასაც live ხსნი</b>.
        entry@next-open · stop-first · 15bps · cooldown-5. {asOf && <span className="text-slate-500">· as_of {asOf}</span>}
      </p>

      {/* controls */}
      <div className="flex flex-wrap items-center gap-2 mb-3 text-sm">
        <span className="text-slate-500">window:</span>
        {[24, 36, 60].map(m => (
          <button key={m} onClick={() => setMonths(m)}
            className={`px-2 py-0.5 rounded border ${months === m ? 'bg-emerald-700/40 border-emerald-500 text-white' : 'border-slate-700 text-slate-400'}`}>{m}mo</button>
        ))}
        <span className="text-slate-600 mx-1">|</span>
        <span className="text-slate-500">exit:</span>
        {['trail', 'bracket'].map(x => (
          <button key={x} onClick={() => setMode(x)}
            className={`px-2 py-0.5 rounded border ${mode === x ? 'bg-teal-700/40 border-teal-500 text-white' : 'border-slate-700 text-slate-400'}`}>{x}</button>
        ))}
        {mode === 'trail' ? (
          <>
            <span className="text-slate-500 ml-1">trail%</span>
            {[0.15, 0.20, 0.25, 0.30].map(t => (
              <button key={t} onClick={() => setTrail(t)}
                className={`px-1.5 py-0.5 rounded border ${trail === t ? 'bg-slate-700 border-slate-500 text-white' : 'border-slate-700 text-slate-400'}`}>{Math.round(t * 100)}</button>
            ))}
          </>
        ) : (
          <>
            <span className="text-slate-500 ml-1">stop</span>
            {[0.08, 0.10, 0.12].map(s => (
              <button key={s} onClick={() => setStop(s)}
                className={`px-1.5 py-0.5 rounded border ${stop === s ? 'bg-slate-700 border-slate-500 text-white' : 'border-slate-700 text-slate-400'}`}>{Math.round(s * 100)}</button>
            ))}
            <span className="text-slate-500 ml-1">tgt</span>
            {[0.20, 0.25, 0.35].map(t => (
              <button key={t} onClick={() => setTarget(t)}
                className={`px-1.5 py-0.5 rounded border ${target === t ? 'bg-slate-700 border-slate-500 text-white' : 'border-slate-700 text-slate-400'}`}>{Math.round(t * 100)}</button>
            ))}
          </>
        )}
        <button onClick={run} disabled={loading}
          className="ml-2 px-3 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-semibold disabled:opacity-50">
          {loading ? '…' : 'Run ▶'}
        </button>
      </div>

      {err && <div className="text-red-400 text-sm mb-2">⚠ {err}</div>}

      {/* comparison table */}
      <div className="overflow-x-auto rounded border border-slate-800">
        <table className="w-full text-sm font-mono">
          <thead className="bg-slate-900/70 text-slate-400 text-left">
            <tr>
              <Th cls="text-left">setup</Th>
              <Th k="n" cls="text-right">n</Th>
              <Th k="mean" cls="text-right">mean%</Th>
              <Th k="median" cls="text-right">med%</Th>
              <Th k="win" cls="text-right">win%</Th>
              <Th k="pf" cls="text-right">PF</Th>
              <Th k="exp_r" cls="text-right" title="Expectancy in R = mean return ÷ planned risk. The core risk-reward number (path-aware, supersedes fixed-horizon fwd-return).">expR</Th>
              <Th k="payoff" cls="text-right" title="Payoff ratio = avg win ÷ |avg loss|. Win% matters less when payoff is high.">pay</Th>
              <Th k="med_mae" cls="text-right" title="Median MAE = typical heat: how far underwater the trade went before it worked. Tells you the real stop you'd need.">heat</Th>
              <Th k="sortino" cls="text-right" title="Sortino = mean return ÷ downside deviation (penalizes only bad volatility).">sort</Th>
              <Th cls="text-right">yrs+</Th>
              <Th k="worst_year" cls="text-right">worst yr</Th>
              <Th k="conc_top10pct" cls="text-right">conc%</Th>
              {overfit && <Th cls="text-right" >DSR</Th>}
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => (
              <tr key={r.setup} onClick={() => openDrill(r.setup)}
                  className={`border-t border-slate-800 hover:bg-slate-800/50 cursor-pointer ${i === 0 ? 'bg-emerald-900/10' : ''}`}>
                <td className="px-2 py-1.5 font-semibold text-slate-200">{r.setup}</td>
                <td className="px-2 py-1.5 text-right text-slate-400">{r.n.toLocaleString()}</td>
                <td className="px-2 py-1.5 text-right text-slate-300">{r.mean?.toFixed(2)}</td>
                <td className="px-2 py-1.5 text-right text-slate-500">{r.median?.toFixed(2)}</td>
                <td className="px-2 py-1.5 text-right text-slate-300">{r.win?.toFixed(1)}</td>
                <td className={`px-2 py-1.5 text-right font-bold ${pfColor(r.pf || 0)}`}>{r.pf?.toFixed(2)}</td>
                <td className={`px-2 py-1.5 text-right font-semibold ${(r.exp_r ?? 0) >= 0.3 ? 'text-emerald-400' : (r.exp_r ?? 0) >= 0.1 ? 'text-teal-300' : (r.exp_r ?? 0) > 0 ? 'text-yellow-300' : 'text-red-400'}`}>{r.exp_r?.toFixed(2)}</td>
                <td className="px-2 py-1.5 text-right text-slate-300">{r.payoff?.toFixed(2) ?? '—'}</td>
                <td className={`px-2 py-1.5 text-right ${(r.med_mae ?? 0) <= -8 ? 'text-orange-400' : 'text-slate-400'}`}>{r.med_mae?.toFixed(1)}</td>
                <td className="px-2 py-1.5 text-right text-slate-400">{r.sortino?.toFixed(2)}</td>
                <td className="px-2 py-1.5 text-right text-slate-400">{r.pos_years}/{r.total_years}</td>
                <td className={`px-2 py-1.5 text-right ${r.worst_year < 0 ? 'text-red-400' : 'text-emerald-400'}`}>{r.worst_year?.toFixed(1)}</td>
                <td className="px-2 py-1.5 text-right text-slate-500">{r.conc_top10pct}</td>
                {overfit && (() => {
                  const d = overfit.map[r.setup]?.dsr
                  const cls = d == null ? 'text-slate-600' : d >= 0.9 ? 'text-emerald-400 font-bold'
                            : d >= 0.6 ? 'text-teal-300' : d >= 0.25 ? 'text-yellow-400' : 'text-red-400'
                  return <td className={`px-2 py-1.5 text-right ${cls}`}>{d == null ? '—' : d.toFixed(2)}</td>
                })()}
              </tr>
            ))}
            {!loading && sorted.length === 0 && (
              <tr><td colSpan={14} className="px-2 py-4 text-center text-slate-500">no results</td></tr>
            )}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-slate-500 mt-1">↑ click a row for per-year + best trades · PF sorted · conc% = top-10% of tickers' share of positive PnL (lower = broader)</p>
      {overfit && (
        <div className="mt-2 text-xs text-slate-400 rounded border border-slate-800 bg-slate-900/40 px-3 py-2"
             title="Bailey & López de Prado: DSR = P(true edge beats the luck of N tested variants); PBO = P(in-sample winner ranks below median out-of-sample) via CSCV on the month×setup matrix.">
          <b className="text-slate-300">🧪 overfit check (62mo, N={overfit.trials} trials assumed):</b>{' '}
          family <b className={overfit.pbo?.pbo <= 0.2 ? 'text-emerald-400' : overfit.pbo?.pbo <= 0.35 ? 'text-yellow-300' : 'text-red-400'}>PBO {overfit.pbo?.pbo}</b>
          {' '}· IS→OOS SR retention {overfit.pbo?.oos_is_ratio} ·{' '}
          <span className="text-slate-500">DSR ≥0.9 <span className="text-emerald-400">selection-proof</span> · 0.6+ <span className="text-teal-300">decent</span> · &lt;0.25 <span className="text-red-400">weak-per-trade</span> (PSR₀=1.0 ყველასთვის — ედჯი &gt;0 რეალურია, DSR per-trade სიძლიერის ბარიერია)</span>
        </div>
      )}

      {/* drill */}
      {drill && (
        <div className="mt-4 rounded border border-slate-700 bg-slate-900/40 p-3">
          <div className="flex items-center justify-between mb-2">
            <h3 className="font-bold text-teal-300">{drill.setup} · per-year + top trades</h3>
            <div className="flex items-center gap-2">
              <a href={`/api/edge-tearsheet?setup=${encodeURIComponent(drill.setup)}&months=62`}
                 target="_blank" rel="noreferrer"
                 title="quantstats tearsheet — equal-slot portfolio curve: equity, drawdown depth+DURATION (how long underwater), rolling Sharpe, monthly heatmap. First open generates it (~1-2min cold)."
                 className="px-2 py-0.5 rounded border border-indigo-500 bg-indigo-900/40 text-indigo-200 text-xs hover:bg-indigo-800/50">
                📊 tearsheet
              </a>
              <button onClick={() => setDrill(null)} className="text-slate-500 hover:text-white text-sm">✕</button>
            </div>
          </div>
          <div className="flex flex-wrap gap-1.5 mb-3">
            {Object.entries(drill.per_year).sort().map(([y, v]) => (
              <span key={y} className={`px-2 py-0.5 rounded text-xs font-mono ${v >= 0 ? 'bg-emerald-900/40 text-emerald-300' : 'bg-red-900/40 text-red-300'}`}>
                {y}: {v >= 0 ? '+' : ''}{v.toFixed(1)}%
              </span>
            ))}
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-x-4 gap-y-1 text-xs">
            {(drill.trades || []).slice(0, 24).map((t, i) => (
              <button key={i} onClick={() => onSelectTicker?.(t.ticker)}
                className="flex justify-between font-mono hover:bg-slate-800/60 rounded px-1">
                <span className="text-slate-300">{t.ticker}<span className="text-slate-600"> {t.year}</span></span>
                <span className={t.ret_pct >= 0 ? 'text-emerald-400' : 'text-red-400'}>{t.ret_pct >= 0 ? '+' : ''}{t.ret_pct}%</span>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
