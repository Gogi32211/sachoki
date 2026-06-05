import { useState, useEffect } from 'react'
import { api } from '../api'

const fmtPct = (v) => v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(1)}`

export default function ComboLabPanel() {
  const [rows, setRows] = useState(null)
  const [filter, setFilter] = useState('passed')
  const [busy, setBusy] = useState(null)
  const [job, setJob] = useState(null)
  const [err, setErr] = useState(null)

  const load = () => api.qlibComboCatalog(filter === 'all' ? null : filter)
    .then(d => setRows(d.rows || [])).catch(e => setErr(String(e)))
  useEffect(() => { load() }, [filter])

  const pollJob = async (job_id, kind) => {
    setBusy(kind); setJob({ id: job_id, kind })
    while (true) {
      try {
        const j = await api.qlibJob(job_id)
        setJob(j)
        if (j.status === 'done' || j.status === 'error') break
      } catch {}
      await new Promise(r => setTimeout(r, 2500))
    }
    setBusy(null); load()
  }

  const discover = async (sizes) => {
    setErr(null)
    try { const r = await api.qlibComboDiscover({ sizes }); pollJob(r.job_id, 'discover') }
    catch (e) { setErr(String(e)) }
  }
  const optimize = async () => {
    setErr(null)
    try { const r = await api.qlibComboOptimizeExits(30); pollJob(r.job_id, 'exits') }
    catch (e) { setErr(String(e)) }
  }

  return (
    <div className="p-4 text-md-on-surface">
      <div className="flex items-center gap-3 mb-3">
        <div className="text-lg font-bold">🧬 Combo Lab</div>
        <div className="text-xs text-md-on-surface-var">walk-forward + Bonferroni · валидирует и фиксирует устойчивые сетапы → каталог для AI Journal</div>
        <div className="flex-1" />
        <button onClick={() => discover([1, 2])} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-violet-700 hover:bg-violet-600 text-white text-sm font-semibold disabled:opacity-50"
          title="Singles + pairs (~10 min)">
          {busy==='discover' ? '⏳…' : '▶ Discover S+P'}</button>
        <button onClick={() => discover([1, 2, 3])} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface-high border border-white/15 text-sm hover:bg-white/10 disabled:opacity-50"
          title="Singles + pairs + triples (heavy, may take 30-60 min)">▶ Discover full</button>
        <button onClick={optimize} disabled={!!busy}
          className="px-3 py-1.5 rounded bg-md-surface-high border border-white/15 text-sm hover:bg-white/10 disabled:opacity-50"
          title="Grid-search stop/target/hold for top 30 passing combos">
          {busy==='exits' ? '⏳…' : '🎯 Optimize exits'}</button>
        <button onClick={load} className="px-3 py-1 rounded text-sm hover:bg-white/10">↻</button>
      </div>

      {err && <div className="mb-2 p-2 rounded bg-rose-900/40 text-rose-200 text-xs font-mono">{err}</div>}
      {job && job.status && job.status !== 'done' && <div className="mb-2 text-xs text-amber-300">job {job.id?.slice(0,8)} {job.kind} · {job.status}{job.log?.length?` · ${job.log.slice(-1)[0]}`:''}</div>}
      {job?.status === 'done' && job?.result && <div className="mb-2 text-xs text-emerald-300">done · {JSON.stringify(job.result).slice(0,160)}</div>}

      <div className="flex gap-1 mb-3">
        {['passed','candidate','rejected','all'].map(f => (
          <button key={f} onClick={() => setFilter(f)}
            className={`px-3 py-1 rounded text-sm capitalize ${filter===f?'bg-violet-700 text-white':'bg-md-surface-high text-md-on-surface-var hover:bg-white/10'}`}>{f}</button>
        ))}
        <span className="ml-3 text-xs text-md-on-surface-var self-center">{rows?.length||0} rows</span>
      </div>

      {!rows ? <div className="text-sm text-md-on-surface-var">Загрузка…</div> :
       !rows.length ? <div className="text-sm text-md-on-surface-var italic">Каталог пуст. Нажми «Discover S+P» — построит и сохранит ~600 проверенных комбинаций (≈10 мин).</div> :
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
          <th className="text-right px-2 py-1">stop·target·hold</th>
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
            <td className={`px-2 py-1 text-right font-mono ${r.train_edge>=0?'text-emerald-400':'text-rose-400'}`}>{fmtPct(r.train_edge)}pp</td>
            <td className={`px-2 py-1 text-right font-mono font-bold ${r.oos_edge>=0?'text-emerald-400':'text-rose-400'}`}>{fmtPct(r.oos_edge)}pp</td>
            <td className="px-2 py-1 text-right font-mono text-md-on-surface-var">{r.bonferroni_p != null ? r.bonferroni_p.toExponential(1) : '—'}</td>
            <td className="px-2 py-1 text-right font-mono">{r.best_stop_atr ? `${r.best_stop_atr}·${r.best_target_atr}·${r.best_hold_days}d` : '—'}</td>
            <td className="px-2 py-1 text-right font-mono">{r.realized_win!=null ? `${(r.realized_win*100).toFixed(0)}% / ${r.realized_avg>=0?'+':''}${r.realized_avg?.toFixed(2)}%` : '—'}</td>
            <td className={`px-2 py-1 ${r.status==='passed'?'text-emerald-300 font-bold':r.status==='rejected'?'text-rose-400':'text-amber-300'}`}>{r.status}</td>
          </tr>))}
        </tbody>
      </table>
      </div>}

      <div className="mt-3 text-xs text-md-on-surface-var italic">
        Дисциплина: pass только если <code>bonferroni-p &lt; 0.05</code> И OOS edge ≥ 50% от train edge И OOS edge &gt; 0. Это режет in-sample флуки.
      </div>
    </div>
  )
}
