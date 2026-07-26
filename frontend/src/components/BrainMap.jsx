import { useEffect, useState, useCallback } from 'react'
import { api } from '../api'

// 🕸 Brain Map — the brain showing ITSELF: neurons (layers/modules/agents), how it thinks
// (top-to-bottom decision flow), what each agent does, what it knows, and where its GAPS are —
// plus the self-directed data requests it raises when it needs something only the user can give.
export default function BrainMap() {
  const [m, setM] = useState(null)
  const [reqs, setReqs] = useState([])
  const [sel, setSel] = useState(null)       // selected neuron {kind, data}
  const [err, setErr] = useState(null)
  const [ans, setAns] = useState({})         // request id -> draft answer
  const [busy, setBusy] = useState(false)
  const [disc, setDisc] = useState(null)     // auto-discoveries

  const load = useCallback(() => {
    api.brainMap().then(d => { if (d.error) setErr(d.error); else setM(d) }).catch(e => setErr(String(e?.message || e)))
    api.brainRequests().then(d => setReqs(d?.requests || [])).catch(() => {})
    api.brainDiscoveries().then(d => setDisc(d)).catch(() => {})
  }, [])
  useEffect(() => { load() }, [load])

  const submit = (id) => {
    const v = ans[id]
    if (v == null || v === '') return
    setBusy(true)
    api.brainAnswer(id, v)
      .then(() => { setAns(a => ({ ...a, [id]: '' })); load() })
      .catch(e => setErr(String(e?.message || e)))
      .finally(() => setBusy(false))
  }

  if (err) return <div className="p-4 text-red-400 text-sm">brain-map: {err}</div>
  if (!m) return <div className="p-4 text-slate-400 text-sm">loading brain map…</div>

  const modByLayer = (lid) => m.modules.filter(x => x.layer.startsWith(lid) || x.layer.split('/').some(p => `L${p}` === lid || p === lid.slice(1)))
  const agentsFor = (mods) => m.agents.filter(a => mods.includes(a.key))
  const gapsFor = (lid) => (m.gaps || []).filter(g => (g.area || '').startsWith(lid))
  const openReqs = reqs.filter(r => r.status === 'open')
  const answered = reqs.filter(r => r.status === 'answered')

  const SEVCLS = { warn: 'text-amber-400 border-amber-800 bg-amber-950/30', info: 'text-sky-400 border-sky-900 bg-sky-950/20' }
  const dot = (s) => s === 'active' ? 'bg-emerald-400' : s === 'manual' ? 'bg-slate-500' : 'bg-amber-400'

  return (
    <div className="p-4 text-slate-200 max-w-6xl">
      <div className="flex items-center gap-3 mb-3 flex-wrap">
        <h2 className="text-base font-semibold text-slate-100">🕸 Brain Map — how it thinks</h2>
        <button onClick={load} className="bg-slate-700 hover:bg-slate-600 rounded px-2 py-1 text-xs">↻ refresh</button>
        <span className="text-xs text-slate-500">{m.knowledge.total} findings · {m.knowledge.signal_edges} firing edges · agents {m.llm_on ? 'ON' : 'OFF'}</span>
      </div>

      {/* ── SELF-DIRECTED DATA REQUESTS — the brain asking YOU ── */}
      {openReqs.length > 0 && (
        <div className="mb-4 rounded-lg border border-violet-800/70 bg-violet-950/30 p-3">
          <div className="text-sm font-semibold text-violet-200 mb-2">🧠 needs your input — {openReqs.length} request{openReqs.length > 1 ? 's' : ''}</div>
          <div className="space-y-2">
            {openReqs.map(r => (
              <div key={r.id} className="rounded border border-violet-900/50 bg-slate-900/40 px-3 py-2">
                <div className="flex items-center gap-2 mb-1">
                  <span className="px-1.5 py-0.5 rounded text-[10px] bg-violet-900 text-violet-200">{r.kind}</span>
                  {r.ticker && <span className="text-xs font-bold text-sky-300">{r.ticker}</span>}
                  {r.severity === 'warn' && <span className="text-[10px] text-amber-400">⚠ matters for accuracy</span>}
                </div>
                <div className="text-xs text-slate-300 mb-1.5">{r.question}</div>
                <div className="flex gap-1.5">
                  <input value={ans[r.id] || ''} onChange={e => setAns(a => ({ ...a, [r.id]: e.target.value }))}
                    onKeyDown={e => e.key === 'Enter' && submit(r.id)}
                    placeholder={r.kind === 'fill_price' ? 'e.g. 14.83' : r.kind === 'catalyst' ? 'e.g. earnings 2026-08-01 / none' : 'your answer…'}
                    className="flex-1 bg-slate-800 border border-slate-700 rounded px-2 py-1 text-xs text-slate-200" />
                  <button disabled={busy} onClick={() => submit(r.id)} className="bg-violet-800 hover:bg-violet-700 disabled:opacity-50 rounded px-3 py-1 text-xs text-violet-100">answer</button>
                </div>
              </div>
            ))}
          </div>
          <div className="text-[10px] text-slate-500 mt-2">The brain raises these from its own gaps. A fill price corrects the book; a catalyst flags the position — answers are applied + remembered.</div>
        </div>
      )}

      {/* ── THE THINKING FLOW: layer bands top→bottom, neurons inside ── */}
      <div className="grid md:grid-cols-[1fr_260px] gap-4">
        <div className="space-y-1.5">
          {m.layers.map((L, i) => {
            const mods = modByLayer(L.id)
            const ags = agentsFor(mods.map(x => x.key).concat(L.modules))
            const lg = gapsFor(L.id)
            return (
              <div key={L.id}>
                <div className="rounded border border-slate-800 bg-slate-900/40 px-3 py-2">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-[11px] font-mono font-bold text-slate-400 w-6">{L.id}</span>
                    <span className="text-xs font-semibold text-slate-200">{L.name}</span>
                    <span className="text-[11px] text-slate-500">{L.role}</span>
                    {lg.length > 0 && <span className="ml-auto text-[10px] px-1.5 rounded bg-amber-950/50 text-amber-400 border border-amber-900">⚠ {lg.length} gap</span>}
                  </div>
                  {/* neuron chips */}
                  <div className="flex flex-wrap gap-1.5 mt-1.5">
                    {m.modules.filter(x => L.modules.includes(x.key)).map(mod => (
                      <button key={mod.key} onClick={() => setSel({ kind: 'module', data: mod })}
                        className={`flex items-center gap-1 px-2 py-0.5 rounded text-[11px] border ${sel?.data?.key === mod.key ? 'border-emerald-600 bg-emerald-950/40' : 'border-slate-700 bg-slate-800/60 hover:bg-slate-700'}`}>
                        <span className={`w-1.5 h-1.5 rounded-full ${dot('active')}`} />{mod.name}
                      </button>
                    ))}
                    {ags.map(a => (
                      <button key={a.key} onClick={() => setSel({ kind: 'agent', data: a })}
                        className={`flex items-center gap-1 px-2 py-0.5 rounded text-[11px] border ${a.on ? 'border-violet-700 bg-violet-950/40 text-violet-200' : 'border-slate-700 bg-slate-800/40 text-slate-500'}`}>
                        🤖 {a.name}{!a.on && <span className="text-[9px]">(off)</span>}
                      </button>
                    ))}
                    {L.status === 'manual' && <span className="text-[10px] text-slate-500 px-2 py-0.5">— you execute —</span>}
                  </div>
                </div>
                {i < m.layers.length - 1 && <div className="flex justify-center text-slate-700 text-[10px] leading-none">↓</div>}
              </div>
            )
          })}
        </div>

        {/* ── right rail: detail + knowledge + data ── */}
        <div className="space-y-3">
          {/* selected neuron detail */}
          <div className="rounded border border-slate-800 bg-slate-900/50 p-3 min-h-[90px]">
            {!sel && <div className="text-[11px] text-slate-500">Click a neuron to inspect what it does, what it reads, and its rule.</div>}
            {sel?.kind === 'module' && (
              <div className="text-xs">
                <div className="font-semibold text-emerald-300 mb-1">{sel.data.name} <span className="text-slate-500 font-normal">({sel.data.layer})</span></div>
                <div className="text-slate-300 mb-1">{sel.data.does}</div>
                <div className="text-slate-500">reads: {sel.data.reads}</div>
                <div className="text-slate-500 italic mt-1">{sel.data.note}</div>
              </div>
            )}
            {sel?.kind === 'agent' && (
              <div className="text-xs">
                <div className="font-semibold text-violet-300 mb-1">🤖 {sel.data.name} {sel.data.on ? <span className="text-emerald-400 text-[10px]">ON</span> : <span className="text-slate-500 text-[10px]">OFF</span>}</div>
                <div className="text-slate-300 mb-1">{sel.data.role}</div>
                <div className="text-slate-500">in: {sel.data.inputs}</div>
                <div className="text-slate-500">out: {sel.data.output}</div>
              </div>
            )}
          </div>

          {/* knowledge core */}
          <div className="rounded border border-slate-800 bg-slate-900/50 p-3">
            <div className="text-xs font-semibold text-slate-300 mb-1">🧩 knowledge core</div>
            <div className="text-[11px] text-slate-400 space-y-0.5">
              {Object.entries(m.knowledge.by_type).map(([t, n]) => <div key={t}>{t}: <b className="text-slate-200">{n}</b></div>)}
              <div className="pt-1 text-slate-500">{m.knowledge.signal_edges} fire · {m.knowledge.context_edges} memory-only · {m.knowledge.disqualifiers} AVOID-rules</div>
              <div className="text-slate-600 italic pt-1">collects: {m.knowledge.collects}</div>
            </div>
          </div>

          {/* data freshness */}
          <div className="rounded border border-slate-800 bg-slate-900/50 p-3">
            <div className="text-xs font-semibold text-slate-300 mb-1">🛢 data plane</div>
            <div className="text-[11px] text-slate-400 space-y-0.5">
              {m.data_sources.map(d => (
                <div key={d.tf} className="flex justify-between"><span>{d.tf}</span><span className={d.last_bar ? 'text-slate-300' : 'text-amber-400'}>{d.last_bar || 'unreadable'}</span></div>
              ))}
              <div className="text-slate-600 italic pt-1">read-only · never mutated</div>
            </div>
          </div>
        </div>
      </div>

      {/* ── GAPS ── */}
      <div className="mt-4">
        <div className="text-xs font-semibold text-slate-300 mb-1">🕳 gaps — where the brain is blind or thin</div>
        <div className="grid sm:grid-cols-2 gap-1.5">
          {(m.gaps || []).map((g, i) => (
            <div key={i} className={`rounded border px-2.5 py-1.5 text-[11px] ${SEVCLS[g.severity] || 'text-slate-400 border-slate-800 bg-slate-900/40'}`}>
              <div className="font-medium">{g.title}{g.ask && <span className="ml-1 text-violet-400">❓ asked</span>}</div>
              <div className="text-slate-500 mt-0.5">{g.detail}</div>
            </div>
          ))}
        </div>
      </div>

      {/* ── 🔬 AUTO-DISCOVERIES: mined combos + realized per-signal outcomes ── */}
      <div className="mt-4">
        <div className="text-xs font-semibold text-slate-300 mb-1">🔬 self-discovery — combinations the brain mined + what actually paid</div>
        <div className="grid md:grid-cols-2 gap-3">
          {/* mined combos (backtest-validated, auto-promoted) */}
          <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
            <div className="text-[11px] text-emerald-300 mb-1">promoted combos (survived OOS gate) — {disc?.mined?.length || 0}</div>
            {(!disc?.mined || disc.mined.length === 0) && (
              <div className="text-[11px] text-slate-500">none yet — the miner runs weekly (Sat) and only promotes combos that beat their base edge AND pass walk-forward + worst-year + DSR + family-PBO.</div>
            )}
            {(disc?.mined || []).map(mc => (
              <div key={mc.id} className="text-[11px] py-0.5 border-t border-slate-800/60 first:border-0">
                <span className="font-mono text-sky-300">{mc.display || mc.id}</span>
                <span className="text-slate-500"> lift {mc.stats?.lift >= 0 ? '+' : ''}{mc.stats?.lift}pp · med {mc.stats?.median} · DSR {mc.stats?.dsr} · {mc.stats?.pos_years} · tr{mc.stats?.train_mean}/vf{mc.stats?.verify_mean}</span>
              </div>
            ))}
          </div>
          {/* realized outcomes per signal (from closed trades) */}
          <div className="rounded border border-slate-800 bg-slate-900/40 p-2">
            <div className="text-[11px] text-sky-300 mb-1">what actually paid (realized, closed trades) — {disc?.realized?.n_closed || 0} closed</div>
            {(!disc?.realized?.by_signal || disc.realized.by_signal.length === 0) && (
              <div className="text-[11px] text-slate-500">accumulates as trades close: each trade's active-signal fingerprint → which signals & pairs actually made money on real risk.</div>
            )}
            {(disc?.realized?.by_signal || []).slice(0, 8).map(s => (
              <div key={s.token} className="text-[11px] py-0.5 flex justify-between border-t border-slate-800/60 first:border-0">
                <span className="font-mono text-slate-300">{s.token}</span>
                <span className={s.median >= 0 ? 'text-emerald-400' : 'text-red-400'}>med {s.median >= 0 ? '+' : ''}{s.median}% · win {Math.round(s.win * 100)}% · n{s.n}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* answered history */}
      {answered.length > 0 && (
        <div className="mt-3">
          <div className="text-[11px] text-slate-500 mb-1">answered ({answered.length})</div>
          {answered.slice(0, 8).map(r => (
            <div key={r.id} className="text-[11px] text-slate-500 py-0.5">
              <span className="text-slate-400">{r.ticker || r.kind}</span>: {r.question} → <span className="text-emerald-400">{r.answer}</span>
              {r.applied && <span className="text-slate-600"> ({r.applied})</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
