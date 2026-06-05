import { useState, useEffect } from 'react'
import { api } from '../api'

const fmt = (v, d=2) => v == null ? '—' : Number(v).toFixed(d)
const fmtPp = (v) => v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(1)}pp`
const fmtEdge = (v) => v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(3)}%`

export default function ComboLabPanel() {
  const [tab, setTab] = useState('pnl')  // pnl | hh

  return (
    <div className="p-4 text-md-on-surface">
      <div className="flex items-center gap-3 mb-3">
        <div className="text-lg font-bold">🧬 Combo Lab</div>
        <div className="text-xs text-md-on-surface-var">walk-forward · Bonferroni · realized P&L</div>
      </div>
      <div className="flex gap-1 mb-3">
        <SubTab id="pnl" cur={tab} onClick={setTab}>💰 P&L (greedy, horizon)</SubTab>
        <SubTab id="hh" cur={tab} onClick={setTab}>🏗 HH walk-forward (structural)</SubTab>
      </div>
      {tab === 'pnl' && <PnLPanel />}
      {tab === 'hh' && <HHPanel />}
    </div>
  )
}

function SubTab({ id, cur, onClick, children }) {
  return <button onClick={() => onClick(id)}
    className={`px-3 py-1.5 rounded text-sm ${cur===id?'bg-violet-700 text-white font-semibold':'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{children}</button>
}

// ───── P&L (greedy) ────────────────────────────────────────────────────────
function PnLPanel() {
  const [horizon, setHorizon] = useState(10)
  const [filter, setFilter] = useState('passed')
  const [rows, setRows] = useState(null)
  const [summary, setSummary] = useState(null)
  const [busy, setBusy] = useState(false)
  const [job, setJob] = useState(null)
  const [err, setErr] = useState(null)

  const load = () => {
    api.qlibComboCatalogPnl(horizon, filter === 'all' ? null : filter)
      .then(d => setRows(d.rows || [])).catch(e => setErr(String(e)))
    api.qlibComboPnlSummary().then(setSummary).catch(()=>{})
  }
  useEffect(() => { load() }, [horizon, filter])

  const pollJob = async (id) => {
    setBusy(true); setJob({ id })
    while (true) {
      try {
        const j = await api.qlibJob(id); setJob(j)
        if (j.status === 'done' || j.status === 'error') break
      } catch {}
      await new Promise(r => setTimeout(r, 2500))
    }
    setBusy(false); load()
  }
  const discover = async () => {
    setErr(null)
    try { const r = await api.qlibComboDiscoverPnl({ horizon, beam: 40, depth_max: 5 }); pollJob(r.job_id) }
    catch (e) { setErr(String(e)) }
  }

  // summary heatmap: horizons × sizes, only "passed"
  const heat = {}
  ;(summary?.breakdown || []).forEach(b => {
    if (b.status === 'passed') {
      const key = `${b.horizon}|${b.size}`
      heat[key] = (heat[key] || 0) + b.n
    }
  })

  return (
    <div>
      <div className="flex items-center gap-3 mb-3 flex-wrap">
        <div className="text-xs text-md-on-surface-var">Horizon:</div>
        {[1, 3, 5, 10].map(h => (
          <button key={h} onClick={() => setHorizon(h)}
            className={`px-3 py-1 rounded text-sm font-mono ${horizon===h?'bg-violet-700 text-white':'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{h}d</button>
        ))}
        <div className="flex-1" />
        <button onClick={discover} disabled={busy}
          className="px-3 py-1.5 rounded bg-violet-700 hover:bg-violet-600 text-white text-sm font-semibold disabled:opacity-50">
          {busy ? '⏳ Discovering…' : `▶ Discover @H=${horizon}d`}</button>
      </div>

      {job?.status && job.status!=='done' && <div className="mb-2 text-xs text-amber-300">job {job.id?.slice(0,8)} · {job.status}{job.log?.length?` · ${job.log.slice(-1)[0]}`:''}</div>}
      {job?.status === 'done' && job?.result && <div className="mb-2 text-xs text-emerald-300">done — passed {job.result.passed||0} of {job.result.total_tested||0}, {job.result.duration_sec}s</div>}
      {err && <div className="mb-2 p-2 rounded bg-rose-900/40 text-rose-200 text-xs font-mono">{err}</div>}

      {/* Horizons × sizes heatmap */}
      <div className="mb-3 p-2 rounded bg-md-surface-high border border-white/10">
        <div className="text-xs font-semibold mb-1 text-md-on-surface-var">Passed combos by horizon × size</div>
        <table className="text-xs">
          <thead><tr><th className="px-2 py-0.5 text-left text-md-on-surface-var">H</th>
            {[1,2,3,4,5].map(s => <th key={s} className="px-3 py-0.5 text-md-on-surface-var">size {s}</th>)}
          </tr></thead>
          <tbody>{[1,3,5,10].map(h => (
            <tr key={h}><td className="px-2 py-0.5 font-mono font-bold">{h}d</td>
              {[1,2,3,4,5].map(s => {
                const n = heat[`${h}|${s}`] || 0
                const cls = n>=5 ? 'bg-emerald-700/60 text-emerald-100' : n>=2 ? 'bg-emerald-900/50 text-emerald-300' : n>=1 ? 'bg-yellow-900/30 text-yellow-300' : 'text-gray-600'
                return <td key={s} className={`px-3 py-0.5 text-center font-mono ${cls}`}>{n||'·'}</td>
              })}</tr>))}
          </tbody>
        </table>
      </div>

      <div className="flex gap-1 mb-2">
        {['passed','candidate','rejected','all'].map(f => (
          <button key={f} onClick={() => setFilter(f)}
            className={`px-3 py-1 rounded text-xs capitalize ${filter===f?'bg-violet-700 text-white':'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{f}</button>
        ))}
        <span className="ml-3 text-xs text-md-on-surface-var self-center">{rows?.length||0} rows · stop/target at H={horizon}d: {capsFor(horizon)}</span>
      </div>

      {!rows ? <div className="text-sm text-md-on-surface-var">Loading…</div> :
       !rows.length ? <div className="text-sm text-md-on-surface-var italic">Каталог для H={horizon}d пуст. Нажми «▶ Discover @H={horizon}d» — greedy beam search (~1-2 мин).</div> :
      <div className="overflow-x-auto">
      <table className="text-xs w-full">
        <thead><tr className="border-b border-white/10 text-md-on-surface-var">
          <th className="text-left px-2 py-1">Predicates</th>
          <th className="text-center px-2 py-1">Size</th>
          <th className="text-right px-2 py-1">n train/oos</th>
          <th className="text-right px-2 py-1">train avg</th>
          <th className="text-right px-2 py-1">OOS avg</th>
          <th className="text-right px-2 py-1">train edge</th>
          <th className="text-right px-2 py-1">OOS edge</th>
          <th className="text-right px-2 py-1">OOS win%</th>
          <th className="text-right px-2 py-1">bonf-p</th>
          <th className="text-left px-2 py-1">grown from</th>
          <th className="text-left px-2 py-1">status</th>
        </tr></thead>
        <tbody>{rows.map(r => (
          <tr key={r.combo_id} className="border-b border-white/5 hover:bg-white/5">
            <td className="px-2 py-1 font-mono">{r.predicates}</td>
            <td className="px-2 py-1 text-center font-mono">{r.size}</td>
            <td className="px-2 py-1 text-right text-md-on-surface-var">{r.n_train}/{r.n_oos}</td>
            <td className={`px-2 py-1 text-right font-mono ${r.train_avg>=0?'text-emerald-400':'text-rose-400'}`}>{fmt(r.train_avg, 3)}%</td>
            <td className={`px-2 py-1 text-right font-mono ${r.oos_avg>=0?'text-emerald-400':'text-rose-400'}`}>{fmt(r.oos_avg, 3)}%</td>
            <td className={`px-2 py-1 text-right font-mono ${r.train_edge>=0?'text-emerald-400':'text-rose-400'}`}>{fmtEdge(r.train_edge)}</td>
            <td className={`px-2 py-1 text-right font-mono font-bold ${r.oos_edge>=0?'text-emerald-300':'text-rose-400'}`}>{fmtEdge(r.oos_edge)}</td>
            <td className="px-2 py-1 text-right font-mono">{fmt(r.oos_win, 1)}%</td>
            <td className="px-2 py-1 text-right font-mono text-md-on-surface-var">{r.bonferroni_p != null ? r.bonferroni_p.toExponential(1) : '—'}</td>
            <td className="px-2 py-1 text-md-on-surface-var font-mono text-[10px]">{r.grown_from?.slice(0,8)||'·'}</td>
            <td className={`px-2 py-1 ${r.status==='passed'?'text-emerald-300 font-bold':r.status==='rejected'?'text-rose-400':'text-amber-300'}`}>{r.status}</td>
          </tr>))}
        </tbody>
      </table>
      </div>}

      <div className="mt-3 text-xs text-md-on-surface-var italic">
        Greedy beam search: на каждом уровне держим топ-40 по OOS P&L edge, расширяем добавлением одного атома, повторяем до 5. Pass требует OOS edge {'>'} 0 И OOS ≥ 0.5×train (нет коллапса) И bonferroni-p {'<'} 0.05. <b>train edge ≪ OOS edge</b> — подозрительно, скорее всего small-n/period-bias; доверяй комбо где оба положительны.
      </div>
    </div>
  )
}

function capsFor(h) {
  const f = Math.sqrt(h / 5)
  return `−${(2 * f).toFixed(2)}% / +${(5 * f).toFixed(2)}%`
}

// ───── HH walk-forward (structural) ────────────────────────────────────────
function HHPanel() {
  const [rows, setRows] = useState(null)
  const [filter, setFilter] = useState('passed')
  const [busy, setBusy] = useState(null)
  const [job, setJob] = useState(null)
  const [err, setErr] = useState(null)

  const load = () => api.qlibComboCatalog(filter === 'all' ? null : filter)
    .then(d => setRows(d.rows || [])).catch(e => setErr(String(e)))
  useEffect(() => { load() }, [filter])

  const pollJob = async (id, kind) => {
    setBusy(kind); setJob({ id, kind })
    while (true) {
      try { const j = await api.qlibJob(id); setJob(j); if (j.status==='done'||j.status==='error') break } catch {}
      await new Promise(r => setTimeout(r, 2500))
    }
    setBusy(null); load()
  }
  const discover = async (sizes) => { setErr(null); try { const r = await api.qlibComboDiscover({ sizes }); pollJob(r.job_id,'discover') } catch (e) { setErr(String(e)) } }
  const optimize = async () => { setErr(null); try { const r = await api.qlibComboOptimizeExits(20); pollJob(r.job_id,'exits') } catch (e) { setErr(String(e)) } }

  return (
    <div>
      <div className="mb-3 p-2 rounded bg-amber-900/30 border border-amber-700/50 text-xs text-amber-200">
        ⚠️ Эта вкладка — оригинальный <b>HH-walk-forward</b>: ищет структурное продолжение (next-pivot-is-higher-high). Combo Lab доказал, что <b>HH-edge не конвертируется в P&L</b> через ATR-выходы (top OOS HH 76% → realized avg −2%). Сохранён для исторической сверки; для торговых решений смотри <b>P&L</b> вкладку.
      </div>

      <div className="flex gap-2 mb-3">
        <button onClick={() => discover([1, 2])} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface-high border border-white/15 text-sm hover:bg-white/10 disabled:opacity-50">▶ S+P</button>
        <button onClick={() => discover([1, 2, 3])} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface-high border border-white/15 text-sm hover:bg-white/10 disabled:opacity-50">▶ S+P+T</button>
        <button onClick={optimize} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface-high border border-white/15 text-sm hover:bg-white/10 disabled:opacity-50">🎯 Optimize exits</button>
        <div className="flex-1" />
      </div>

      {job?.status && job.status!=='done' && <div className="mb-2 text-xs text-amber-300">job {job.id?.slice(0,8)} {job.kind} · {job.status}</div>}
      {err && <div className="mb-2 p-2 rounded bg-rose-900/40 text-rose-200 text-xs font-mono">{err}</div>}

      <div className="flex gap-1 mb-2">
        {['passed','candidate','rejected','all'].map(f => (
          <button key={f} onClick={() => setFilter(f)}
            className={`px-3 py-1 rounded text-xs capitalize ${filter===f?'bg-violet-700 text-white':'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{f}</button>
        ))}
        <span className="ml-3 text-xs text-md-on-surface-var self-center">{rows?.length||0} rows</span>
      </div>

      {!rows ? <div className="text-sm text-md-on-surface-var">Loading…</div> :
       !rows.length ? <div className="text-sm text-md-on-surface-var italic">Каталог пуст.</div> :
      <div className="overflow-x-auto">
      <table className="text-xs w-full">
        <thead><tr className="border-b border-white/10 text-md-on-surface-var">
          <th className="text-left px-2 py-1">Predicates</th>
          <th className="text-right px-2 py-1">Size</th>
          <th className="text-right px-2 py-1">n train/oos</th>
          <th className="text-right px-2 py-1">train HH%</th>
          <th className="text-right px-2 py-1">OOS HH%</th>
          <th className="text-right px-2 py-1">train Δ</th>
          <th className="text-right px-2 py-1">OOS Δ</th>
          <th className="text-right px-2 py-1">bonf-p</th>
          <th className="text-right px-2 py-1">stop·tgt·hold</th>
          <th className="text-right px-2 py-1">real win/avg</th>
          <th className="text-left px-2 py-1">status</th>
        </tr></thead>
        <tbody>{rows.map(r => (
          <tr key={r.combo_id} className="border-b border-white/5 hover:bg-white/5">
            <td className="px-2 py-1 font-mono">{r.predicates}</td>
            <td className="px-2 py-1 text-right">{r.size}</td>
            <td className="px-2 py-1 text-right text-md-on-surface-var">{r.n_train}/{r.n_oos}</td>
            <td className="px-2 py-1 text-right font-mono">{r.train_hh5}%</td>
            <td className="px-2 py-1 text-right font-mono">{r.oos_hh5}%</td>
            <td className={`px-2 py-1 text-right font-mono ${r.train_edge>=0?'text-emerald-400':'text-rose-400'}`}>{fmtPp(r.train_edge)}</td>
            <td className={`px-2 py-1 text-right font-mono font-bold ${r.oos_edge>=0?'text-emerald-400':'text-rose-400'}`}>{fmtPp(r.oos_edge)}</td>
            <td className="px-2 py-1 text-right font-mono text-md-on-surface-var">{r.bonferroni_p != null ? r.bonferroni_p.toExponential(1) : '—'}</td>
            <td className="px-2 py-1 text-right font-mono">{r.best_stop_atr ? `${r.best_stop_atr}·${r.best_target_atr}·${r.best_hold_days}d` : '—'}</td>
            <td className={`px-2 py-1 text-right font-mono ${r.realized_avg<0?'text-rose-400':'text-emerald-400'}`}>{r.realized_win!=null ? `${(r.realized_win*100).toFixed(0)}%/${r.realized_avg>=0?'+':''}${r.realized_avg?.toFixed(2)}%` : '—'}</td>
            <td className={`px-2 py-1 ${r.status==='passed'?'text-emerald-300 font-bold':r.status==='rejected'?'text-rose-400':'text-amber-300'}`}>{r.status}</td>
          </tr>))}
        </tbody>
      </table>
      </div>}
    </div>
  )
}
