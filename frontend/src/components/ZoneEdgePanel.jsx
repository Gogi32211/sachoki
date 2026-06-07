import { useEffect, useState } from 'react'

// Zone EXIT vs RETEST forward-edge research. Raw events are usually NOT an edge;
// the value is which bar-context (signal / pattern / description) lifts them.
const EVENT_META = {
  exit_up:   { label: 'EXIT ↑ (breakout)',  color: 'text-emerald-300', desc: 'close first crosses ABOVE the zone' },
  exit_down: { label: 'EXIT ↓ (breakdown)', color: 'text-rose-300',    desc: 'close first crosses BELOW the zone' },
  retest:    { label: 'RETEST (return)',    color: 'text-amber-300',    desc: 'close re-enters the zone after leaving' },
}
const VOL_MINS = [2, 5, 10]
const HORIZONS = [5, 10, 20]

const pct = (v) => (v > 0 ? '+' : '') + (v ?? 0).toFixed(2) + '%'
const pp = (v) => (v > 0 ? '+' : '') + (v ?? 0).toFixed(1) + 'pp'
const edgeColor = (v) => v > 0.05 ? 'text-emerald-400' : v < -0.05 ? 'text-rose-400' : 'text-md-on-surface-var'

export default function ZoneEdgePanel() {
  const [volMin, setVolMin]   = useState(5)
  const [horizon, setHorizon] = useState(10)
  const [firstOnly, setFirstOnly] = useState(true)
  const [data, setData]   = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr]     = useState(null)

  useEffect(() => {
    let dead = false
    setLoading(true); setErr(null)
    const q = new URLSearchParams({ vol_min: volMin, horizon, first_only: firstOnly ? '1' : '0', min_n: '30' })
    fetch(`/api/zone-events/report?${q}`)
      .then(r => r.json())
      .then(d => { if (!dead) { d.error ? setErr(d.error) : setData(d) } })
      .catch(e => { if (!dead) setErr(String(e)) })
      .finally(() => { if (!dead) setLoading(false) })
    return () => { dead = true }
  }, [volMin, horizon, firstOnly])

  const base = data?.baseline

  return (
    <div className="p-4 max-w-5xl mx-auto text-md-on-surface">
      <div className="mb-3">
        <h1 className="text-xl font-bold">🎯 Zone Edge — EXIT vs RETEST</h1>
        <p className="text-xs text-md-on-surface-var mt-1 max-w-3xl">
          Forward edge of what price does at an HV-zone. A zone = the [low, high] of a volume-spike bar.
          We label every historical interaction as <b>EXIT</b> (breaks out/down) or <b>RETEST</b> (returns to the zone),
          then measure the forward {horizon}d return vs the whole-market baseline — and which bar-context lifts it most.
        </p>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center gap-4 mb-4 text-xs">
        <div className="flex items-center gap-1">
          <span className="text-md-on-surface-var">Zone vol ≥</span>
          {VOL_MINS.map(v => (
            <button key={v} onClick={() => setVolMin(v)}
              className={`px-2 py-0.5 rounded border ${volMin === v ? 'bg-sky-900/60 text-sky-200 border-sky-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>×{v}</button>
          ))}
        </div>
        <div className="flex items-center gap-1">
          <span className="text-md-on-surface-var">Horizon</span>
          {HORIZONS.map(h => (
            <button key={h} onClick={() => setHorizon(h)}
              className={`px-2 py-0.5 rounded border ${horizon === h ? 'bg-sky-900/60 text-sky-200 border-sky-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>{h}d</button>
          ))}
        </div>
        <label className="flex items-center gap-1 cursor-pointer select-none">
          <input type="checkbox" checked={firstOnly} onChange={e => setFirstOnly(e.target.checked)} />
          <span className="text-md-on-surface-var">first event per zone only</span>
        </label>
        {loading && <span className="text-sky-400 animate-pulse">computing…</span>}
        {err && <span className="text-rose-400">error: {err}</span>}
      </div>

      {base && (
        <div className="text-xs text-md-on-surface-var mb-3">
          Baseline (all bars): avg <b className="text-md-on-surface">{pct(base.avg_clip_pct)}</b> · win <b className="text-md-on-surface">{base.win_rate_pct}%</b> · n={base.n.toLocaleString()}
        </div>
      )}

      {/* Event edge table */}
      <table className="w-full text-xs mb-2 border border-white/10 rounded overflow-hidden">
        <thead className="bg-md-surface-high text-md-on-surface-var">
          <tr>
            <th className="text-left px-3 py-1.5">Event</th>
            <th className="text-right px-3 py-1.5">n</th>
            <th className="text-right px-2 py-1.5" title="win-rate 5d / 10d / 20d">win 5/10/20</th>
            <th className="text-right px-2 py-1.5" title="avg MFE / avg MAE (reward/risk skew)">MFE/MAE · RR</th>
            <th className="text-right px-3 py-1.5" title="avg forward return vs baseline">edge</th>
            <th className="text-left px-3 py-1.5" title="outcome when T/Z flips up within 3 bars AFTER the event">+ T/Z follow-through (next 3 bars)</th>
          </tr>
        </thead>
        <tbody>
          {(data?.events || []).map(e => {
            const tf = e.tz_follow
            const lift = tf ? (tf.win10_pct - e.win10_pct) : null
            return (
              <tr key={e.event_type} className="border-t border-white/5 align-top">
                <td className={`px-3 py-2 font-semibold ${EVENT_META[e.event_type]?.color}`}>
                  {EVENT_META[e.event_type]?.label || e.event_type}
                  <span className="block text-[10px] text-md-on-surface-var/60 font-normal">{EVENT_META[e.event_type]?.desc}</span>
                </td>
                <td className="text-right px-3 py-2 font-mono">{e.n.toLocaleString()}</td>
                <td className="text-right px-2 py-2 font-mono">
                  {e.win5_pct}/<b>{e.win10_pct}</b>/{e.win20_pct}%
                </td>
                <td className="text-right px-2 py-2 font-mono text-[11px]">
                  <span className="text-emerald-400/70">{pct(e.avg_mfe_pct)}</span>
                  <span className="text-md-on-surface-var/40"> / </span>
                  <span className="text-rose-400/70">{pct(e.avg_mae_pct)}</span>
                  <span className="block text-md-on-surface-var">RR {e.rr_ratio}</span>
                </td>
                <td className={`text-right px-3 py-2 font-mono font-bold ${edgeColor(e.edge_avg_pct)}`}>{pct(e.edge_avg_pct)}</td>
                <td className="px-3 py-2">
                  {!tf ? <span className="text-md-on-surface-var/40 text-[11px]">—</span> : (
                    <div className="text-[11px] bg-emerald-900/20 border border-emerald-700/40 rounded px-2 py-1 inline-block">
                      <span className="font-mono">win10 <b className="text-emerald-300">{tf.win10_pct}%</b></span>
                      {lift != null && <span className={`font-mono ml-1 ${lift > 0 ? 'text-emerald-400' : 'text-rose-400'}`}>({lift > 0 ? '+' : ''}{lift.toFixed(1)}pp)</span>}
                      <span className="font-mono ml-1 text-md-on-surface-var">avg {pct(tf.avg10_pct)}</span>
                      <span className="block text-md-on-surface-var/60">{tf.share_pct}% of events · n={tf.n.toLocaleString()}</span>
                    </div>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
      <p className="text-[11px] text-md-on-surface-var mb-6">
        💡 The confirmation isn't T/Z bullish <i>on</i> the event bar — it's T/Z <b>flipping up in the bars AFTER</b>.
        That follow-through lifts every event's win-rate above the {base?.win_rate_pct}% baseline. RR = avg&nbsp;MFE / avg&nbsp;MAE.
      </p>

      {/* Context lift per event */}
      <h2 className="text-sm font-bold mb-2">Context lift — what improves / hurts each event</h2>
      <p className="text-[11px] text-md-on-surface-var mb-3">
        Lift = the event's forward edge WHEN the bar also has this signal/pattern/shape, vs the event's own average.
        Green = the context that turns the event into an edge; red = traps.
      </p>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
        {['exit_up', 'exit_down', 'retest'].map(et => {
          const ctx = data?.context?.[et]
          return (
            <div key={et} className="border border-white/10 rounded p-2">
              <div className={`text-xs font-semibold mb-2 ${EVENT_META[et].color}`}>{EVENT_META[et].label}</div>
              {!ctx ? <div className="text-[11px] text-md-on-surface-var/50 italic">no data</div> : (
                <>
                  <CtxList title="✅ best" rows={ctx.best} good />
                  <CtxList title="⚠️ worst" rows={ctx.worst} />
                </>
              )}
            </div>
          )
        })}
      </div>

      <p className="text-[10px] text-md-on-surface-var/50 mt-4">
        Research only — whole-history, not OOS-split. Returns clipped to {data?.params?.clip}.
        Use it to find which event × context to build a live rule on, then validate out-of-sample.
      </p>
    </div>
  )
}

function CtxList({ title, rows, good }) {
  return (
    <div className="mb-2">
      <div className="text-[10px] uppercase tracking-wide text-md-on-surface-var/60 mb-1">{title}</div>
      {(rows || []).map((r, i) => (
        <div key={i} className="flex items-baseline justify-between text-[11px] font-mono py-0.5">
          <span className="truncate" title={`${r.feature}=${r.value}  (n=${r.n}, win ${r.win_rate_pct}%)`}>
            {r.feature}<span className="text-md-on-surface-var/50">={r.value}</span>
          </span>
          <span className={`shrink-0 ml-2 ${good ? 'text-emerald-400' : 'text-rose-400'}`}>
            {(r.lift_avg_pct > 0 ? '+' : '') + r.lift_avg_pct.toFixed(2)}%
            <span className="text-md-on-surface-var/40"> ·{r.n}</span>
          </span>
        </div>
      ))}
    </div>
  )
}
