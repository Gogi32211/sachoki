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

export default function ZoneEdgePanel({ onSelectTicker }) {
  const [volMin, setVolMin]   = useState(5)
  const [horizon, setHorizon] = useState(10)
  const [firstOnly, setFirstOnly] = useState(true)
  const [data, setData]   = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr]     = useState(null)
  // 2-way combinations
  const [comboEvent, setComboEvent]   = useState('retest')
  const [comboAnchor, setComboAnchor] = useState(true)   // anchor on T/Z follow-through
  const [comboWays, setComboWays]     = useState(2)       // 2- or 3-way
  const [combo, setCombo] = useState(null)
  const [comboLoading, setComboLoading] = useState(false)
  const [examples, setExamples] = useState(null)
  // Pattern builder (full bar-code slots)
  const [patValues, setPatValues] = useState(null)
  const [patSlots, setPatSlots]   = useState({ tz: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' })
  const [patResult, setPatResult] = useState(null)
  const [patLoading, setPatLoading] = useState(false)
  const [live, setLive] = useState(null)
  const [liveLoading, setLiveLoading] = useState(false)
  const [liveBools, setLiveBools] = useState([])   // boolean signals applied to live
  const [liveFlip, setLiveFlip]   = useState(false) // confirmed-only (flip required)

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

  useEffect(() => {
    let dead = false
    setComboLoading(true)
    const q = new URLSearchParams({ event_type: comboEvent, vol_min: volMin, horizon,
      first_only: firstOnly ? '1' : '0', min_n: comboWays === 3 ? '80' : '40', top: '15', ways: comboWays })
    if (comboAnchor) q.set('anchor', 'tz_up_next3')
    fetch(`/api/zone-events/combos?${q}`)
      .then(r => r.json())
      .then(d => { if (!dead) setCombo(d) })
      .catch(() => { if (!dead) setCombo(null) })
      .finally(() => { if (!dead) setComboLoading(false) })
    return () => { dead = true }
  }, [comboEvent, comboAnchor, comboWays, volMin, horizon, firstOnly])

  useEffect(() => {
    let dead = false
    const q = new URLSearchParams({ event_type: comboEvent, require_flip: comboAnchor ? '1' : '0',
      vol_min: volMin, horizon, limit: '20' })
    fetch(`/api/zone-events/examples?${q}`)
      .then(r => r.json())
      .then(d => { if (!dead) setExamples(d) })
      .catch(() => { if (!dead) setExamples(null) })
    return () => { dead = true }
  }, [comboEvent, comboAnchor, volMin, horizon])

  // pattern dropdown values — refetch + reset slots when event/flip context changes
  useEffect(() => {
    let dead = false
    setPatSlots({ tz: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' })
    const q = new URLSearchParams({ event_type: comboEvent, require_flip: comboAnchor ? '1' : '0', vol_min: volMin, horizon })
    fetch(`/api/zone-events/pattern/values?${q}`).then(r => r.json())
      .then(d => { if (!dead) setPatValues(d?.slots || {}) }).catch(() => {})
    return () => { dead = true }
  }, [comboEvent, comboAnchor, volMin, horizon])

  // pattern result — refetch when slots / context change
  useEffect(() => {
    let dead = false
    setPatLoading(true)
    const q = new URLSearchParams({ event_type: comboEvent, require_flip: comboAnchor ? '1' : '0',
      vol_min: volMin, horizon, ...patSlots })
    fetch(`/api/zone-events/pattern?${q}`).then(r => r.json())
      .then(d => { if (!dead) setPatResult(d) }).catch(() => { if (!dead) setPatResult(null) })
      .finally(() => { if (!dead) setPatLoading(false) })
    return () => { dead = true }
  }, [patSlots, comboEvent, comboAnchor, volMin, horizon])

  // live setups — recent bars matching the current pattern slots + bool signals
  useEffect(() => {
    let dead = false
    setLiveLoading(true)
    const q = new URLSearchParams({ event_type: comboEvent, vol_min: volMin, horizon, max_age_days: '7',
      require_flip: liveFlip ? '1' : '0', ...patSlots })
    if (liveBools.length) q.set('bools', liveBools.join(','))
    fetch(`/api/zone-events/live?${q}`).then(r => r.json())
      .then(d => { if (!dead) setLive(d) }).catch(() => { if (!dead) setLive(null) })
      .finally(() => { if (!dead) setLiveLoading(false) })
    return () => { dead = true }
  }, [patSlots, liveBools, liveFlip, comboEvent, volMin, horizon])

  // map a combo's features → live filter (slots + bool signals + flip), then scroll up
  const SLOT_OF_COL = { t_sig: 'tz', l_sig: 'l', full_suffix: 'suffix', bar_body_wick: 'bodywk',
                        gap_rng: 'gaprng', bar_line5: 'l5', vol_bucket: 'vol' }
  function applyComboToLive(c) {
    const feats = [c.a, c.b, c.c].filter(Boolean)
    const slots = { tz: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' }
    const bools = []; let flip = false
    for (const f of feats) {
      if (f.includes('=')) {
        const i = f.indexOf('='); const col = f.slice(0, i); const val = f.slice(i + 1)
        const slot = SLOT_OF_COL[col]
        if (slot) slots[slot] = val          // categorical that maps to a bar-code slot
      } else if (f === 'tz_up_next3') {
        flip = true                          // the follow-through flip → confirmed-only
      } else {
        bools.push(f)                        // a boolean signal (sig_abs, wyc_spring, at_fib…)
      }
    }
    setPatSlots(slots); setLiveBools(bools); setLiveFlip(flip)
    document.getElementById('live-setups')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }
  const clearLive = () => {
    setPatSlots({ tz: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' })
    setLiveBools([]); setLiveFlip(false)
  }

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

      {/* LIVE setups — recent bars matching the pattern (the actionable output) */}
      <div id="live-setups" className="mb-5 border border-emerald-700/40 bg-emerald-900/10 rounded-lg p-3 scroll-mt-4">
        <div className="flex items-baseline justify-between mb-1">
          <h2 className="text-sm font-bold">🔔 Live setups — recent {EVENT_META[comboEvent]?.label.split(' ')[0]} bars matching the pattern</h2>
          <span className="text-[10px] text-md-on-surface-var">as of {live?.as_of || '—'} · last 7d {liveLoading && <span className="text-sky-400 animate-pulse">…</span>}</span>
        </div>
        <p className="text-[11px] text-md-on-surface-var mb-2">
          <b className="text-emerald-300">confirmed</b> = T/Z already flipped up after the event (actionable) ·
          <b className="text-amber-300"> pending</b> = event fired, not bullish yet (watch for the flip).
        </p>
        <div className="flex flex-wrap items-center gap-2 mb-2 text-[11px]">
          <span className="text-md-on-surface-var">filter:</span>
          {Object.keys(live?.applied || {}).length === 0
            ? <span className="text-md-on-surface-var/50 italic">none — showing all recent {comboEvent}s (no edge filter)</span>
            : Object.entries(live.applied).map(([k, v]) => (
                <span key={k} className="font-mono px-1.5 py-0.5 rounded bg-emerald-900/30 border border-emerald-700/40 text-emerald-200">{k}={v}</span>
              ))}
          {liveFlip && <span className="font-mono px-1.5 py-0.5 rounded bg-emerald-900/30 border border-emerald-700/40 text-emerald-200">confirmed-only</span>}
          <button onClick={() => { setPatSlots(s => ({ ...s, vol: 'B' })); setLiveBools(['sig_abs']); setLiveFlip(true) }}
            className="px-2 py-0.5 rounded border border-amber-600/50 text-amber-300 hover:bg-amber-900/20">★ robust (sig_abs+flip+vol=B)</button>
          {(Object.keys(live?.applied || {}).length > 0 || liveFlip) &&
            <button onClick={clearLive} className="px-2 py-0.5 rounded border border-white/10 text-md-on-surface-var hover:text-white">clear</button>}
        </div>
        <p className="text-[10px] text-md-on-surface-var/50 mb-2">
          💡 Click <b className="text-emerald-300">→ Live</b> on any combination below (or ★ robust) to filter this list to that OOS-validated edge.
        </p>
        {!live?.setups?.length ? (
          <div className="text-xs text-md-on-surface-var/60 italic">no recent setups for this pattern</div>
        ) : (
          <>
            {['confirmed', 'pending'].map(st => {
              const rows = live.setups.filter(s => s.status === st)
              if (!rows.length) return null
              return (
                <div key={st} className="mb-2">
                  <div className={`text-[10px] uppercase tracking-wide mb-1 ${st === 'confirmed' ? 'text-emerald-300' : 'text-amber-300'}`}>
                    {st === 'confirmed' ? '✅ confirmed (flip fired)' : '⏳ pending (watch)'} · {rows.length}
                  </div>
                  <div className="grid grid-cols-2 sm:grid-cols-4 gap-1.5">
                    {rows.map((s, i) => (
                      <button key={i} onClick={() => onSelectTicker?.(s.ticker)}
                        title={`zone ${s.zone_low}–${s.zone_high} · close ${s.close} · vol ×${s.z_mult} (${s.vol_bucket}/${s.range})`}
                        className={`flex items-center justify-between gap-1 px-2 py-1 rounded border text-left ${st === 'confirmed' ? 'border-emerald-700/40 bg-emerald-900/20 hover:border-emerald-400' : 'border-amber-700/30 bg-amber-900/10 hover:border-amber-400'}`}>
                        <span className="font-mono font-semibold text-xs">{s.ticker}</span>
                        <span className="font-mono text-[10px] text-md-on-surface-var">{s.days_ago}d ·×{s.z_mult}</span>
                      </button>
                    ))}
                  </div>
                </div>
              )
            })}
            <p className="text-[10px] text-md-on-surface-var/50">
              Click → opens in Superchart (toggle ⊏⊐ Zone evt to see the RT marker). Build a pattern below to narrow this list.
            </p>
          </>
        )}
      </div>

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

      {/* 2-way combinations */}
      <div className="mt-7">
        <h2 className="text-sm font-bold mb-1">{comboWays}-way combinations — IS vs OOS validated</h2>
        <p className="text-[11px] text-md-on-surface-var mb-3">
          Pairs/triples are where the edge concentrates. <b>IS</b> = before {combo?.params?.oos_from || '2025-01-01'},
          <b> OOS</b> = after (out-of-sample). A real edge holds in BOTH; if OOS collapses, it's overfit.
        </p>
        <div className="flex flex-wrap items-center gap-3 text-xs mb-3">
          <div className="flex items-center gap-1">
            {['retest', 'exit_up', 'exit_down'].map(et => (
              <button key={et} onClick={() => setComboEvent(et)}
                className={`px-2 py-0.5 rounded border ${comboEvent === et ? `bg-md-surface-high border-white/30 ${EVENT_META[et].color}` : 'bg-md-surface border-white/10 hover:text-white'}`}>
                {EVENT_META[et].label.split(' ')[0]} {EVENT_META[et].label.includes('↑') ? '↑' : EVENT_META[et].label.includes('↓') ? '↓' : ''}
              </button>
            ))}
          </div>
          <div className="flex items-center gap-1">
            {[2, 3].map(w => (
              <button key={w} onClick={() => setComboWays(w)}
                className={`px-2 py-0.5 rounded border ${comboWays === w ? 'bg-sky-900/60 text-sky-200 border-sky-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>{w}-way</button>
            ))}
          </div>
          <label className="flex items-center gap-1 cursor-pointer select-none">
            <input type="checkbox" checked={comboAnchor} onChange={e => setComboAnchor(e.target.checked)} />
            <span className="text-md-on-surface-var">anchor on T/Z follow-through</span>
          </label>
          {comboLoading && <span className="text-sky-400 animate-pulse">computing…</span>}
          {combo?.event_base && <span className="text-md-on-surface-var/60">base {combo.event_base.win_rate_pct}% · {combo.params?.n_combos} combos</span>}
        </div>
        <table className="w-full text-xs border border-white/10 rounded overflow-hidden">
          <thead className="bg-md-surface-high text-md-on-surface-var">
            <tr>
              <th className="text-left px-3 py-1.5">combination</th>
              <th className="text-right px-3 py-1.5">n</th>
              <th className="text-right px-3 py-1.5">win</th>
              <th className="text-right px-3 py-1.5" title="in-sample">IS</th>
              <th className="text-right px-3 py-1.5" title="out-of-sample">OOS</th>
              <th className="text-center px-2 py-1.5" title="does OOS hold within 6pp of IS?">holds?</th>
            </tr>
          </thead>
          <tbody>
            {(combo?.best || []).map((c, i) => {
              const holds = c.win_is_pct != null && c.win_oos_pct != null && (c.win_oos_pct - c.win_is_pct) >= -6 && c.win_oos_pct > combo.event_base.win_rate_pct
              return (
                <tr key={i} className="border-t border-white/5">
                  <td className="px-3 py-1.5 font-mono text-[11px]">
                    <button onClick={() => applyComboToLive(c)} title="Apply this combo to the Live setups list above"
                      className="mr-2 px-1 rounded border border-emerald-700/50 text-emerald-300 hover:bg-emerald-900/30 not-italic">→ Live</button>
                    <span className="text-sky-300">{c.a}</span>
                    <span className="text-md-on-surface-var/40"> + </span>
                    <span className="text-violet-300">{c.b}</span>
                    {c.c && <><span className="text-md-on-surface-var/40"> + </span><span className="text-amber-300">{c.c}</span></>}
                  </td>
                  <td className={`text-right px-3 py-1.5 font-mono ${c.n >= 300 ? 'text-emerald-300' : c.n >= 100 ? 'text-md-on-surface' : 'text-amber-400/70'}`}>
                    {c.n.toLocaleString()}{c.n < 100 ? ' ⚠' : ''}
                  </td>
                  <td className="text-right px-3 py-1.5 font-mono font-bold">{c.win_rate_pct}%</td>
                  <td className="text-right px-3 py-1.5 font-mono text-md-on-surface-var">{c.win_is_pct}%<span className="text-[9px] text-md-on-surface-var/40"> ·{c.n_is}</span></td>
                  <td className={`text-right px-3 py-1.5 font-mono ${holds ? 'text-emerald-300' : 'text-rose-300'}`}>{c.win_oos_pct}%<span className="text-[9px] text-md-on-surface-var/40"> ·{c.n_oos}</span></td>
                  <td className="text-center px-2 py-1.5">{holds ? '✅' : '❌'}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
        <p className="text-[10px] text-md-on-surface-var/50 mt-1">
          ✅ holds = OOS within 6pp of IS AND above base. ❌ = overfit (looked good in-sample, failed out-of-sample). n ≥ 300 green.
        </p>
      </div>

      {/* Pattern builder — the full bar-code, all slots together */}
      <div className="mt-7">
        <h2 className="text-sm font-bold mb-1">Pattern builder — the full bar code, together</h2>
        <p className="text-[11px] text-md-on-surface-var mb-2">
          Set each slot to a value or leave <b>*</b> (any). Uses the current event ({EVENT_META[comboEvent]?.label.split(' ')[0]})
          {comboAnchor && <span className="text-emerald-300"> + T/Z flip</span>}. ⚠ small n = overfit — trust n ≥ 150.
        </p>
        <div className="flex flex-wrap gap-2 mb-3 text-[11px]">
          {[['tz','TZ'],['l','L'],['suffix','suffix'],['bodywk','body/wk'],['gaprng','gap/rng'],['l5','l5'],['vol','vol']].map(([slot, label]) => (
            <label key={slot} className="flex flex-col gap-0.5">
              <span className="text-md-on-surface-var/60 uppercase tracking-wide text-[9px]">{label}</span>
              <select value={patSlots[slot]} onChange={e => setPatSlots(s => ({ ...s, [slot]: e.target.value }))}
                className="bg-md-surface border border-white/10 rounded px-1.5 py-1 font-mono text-md-on-surface min-w-[64px]">
                <option value="*">*</option>
                {(patValues?.[slot] || []).map(v => (
                  <option key={v.value} value={v.value}>{v.value} ({v.n})</option>
                ))}
              </select>
            </label>
          ))}
          <button onClick={() => setPatSlots({ tz: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' })}
            className="self-end px-2 py-1 rounded border border-white/10 bg-md-surface text-md-on-surface-var hover:text-white">reset</button>
        </div>
        {patResult?.matched && (
          <div className="flex flex-wrap items-center gap-4 text-xs bg-md-surface-high border border-white/10 rounded px-3 py-2">
            {patLoading && <span className="text-sky-400 animate-pulse">…</span>}
            <span>matched <b className={`font-mono ${patResult.matched.n >= 150 ? 'text-emerald-300' : patResult.matched.n >= 40 ? 'text-md-on-surface' : 'text-amber-400'}`}>
              {patResult.matched.n?.toLocaleString() ?? 0}{patResult.matched.n < 40 ? ' ⚠' : ''}</b>
              <span className="text-md-on-surface-var/50"> / {patResult.base?.n?.toLocaleString()} {comboEvent}</span>
            </span>
            {patResult.matched.n > 0 && <>
              <span className="font-mono">win 5/10/20 <b>{patResult.matched.win5_pct}/{patResult.matched.win10_pct}/{patResult.matched.win20_pct}%</b></span>
              <span className={`font-mono font-bold ${patResult.matched.lift_win_pp > 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {pp(patResult.matched.lift_win_pp)} vs base {patResult.base?.win_rate_pct}%
              </span>
              {patResult.matched.split && (() => {
                const sp = patResult.matched.split
                const holds = sp.is?.win_rate_pct != null && sp.oos?.win_rate_pct != null && (sp.oos.win_rate_pct - sp.is.win_rate_pct) >= -6
                return <span className="font-mono">
                  <span className="text-md-on-surface-var">IS {sp.is?.win_rate_pct}%·{sp.is?.n}</span>
                  <span className="text-md-on-surface-var/40"> / </span>
                  <span className={holds ? 'text-emerald-300' : 'text-rose-300'}>OOS {sp.oos?.win_rate_pct ?? '—'}%·{sp.oos?.n} {sp.oos?.win_rate_pct != null && (holds ? '✅' : '❌')}</span>
                </span>
              })()}
            </>}
          </div>
        )}
        {patResult?.examples?.length > 0 && (
          <div className="grid grid-cols-3 sm:grid-cols-6 gap-1.5 mt-2 text-xs">
            {patResult.examples.map((e, i) => (
              <button key={i} onClick={() => onSelectTicker?.(e.ticker)}
                className="flex items-center justify-between gap-1 px-2 py-1 rounded border border-white/10 bg-md-surface hover:border-sky-500 text-left">
                <span className="font-mono font-semibold">{e.ticker}</span>
                <span className={`font-mono text-[10px] ${e.win ? 'text-emerald-400' : 'text-rose-400'}`}>{e[`fwd_${horizon}d`] > 0 ? '+' : ''}{e[`fwd_${horizon}d`]}%</span>
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Concrete examples — see them on a chart */}
      <div className="mt-7">
        <h2 className="text-sm font-bold mb-1">
          See it on a chart — {examples?.count || 0} example {EVENT_META[comboEvent]?.label.split(' ')[0]} instances
          {comboAnchor && <span className="text-emerald-300"> with T/Z flip</span>}
        </h2>
        <p className="text-[11px] text-md-on-surface-var mb-2">
          One recent instance per ticker. Open any in <b>Superchart</b>, then toggle <b>⊏⊐ Zone evt</b> to see the
          RT / X↑ / X↓ markers (✓ = T/Z flipped up after). Honest mix of wins & losses — that's the ~48% reality.
        </p>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-1.5 text-xs">
          {(examples?.examples || []).map((e, i) => {
            const ret = e[`fwd_${horizon}d`]
            return (
              <button key={i} onClick={() => onSelectTicker?.(e.ticker)}
                className="flex items-center justify-between gap-1 px-2 py-1 rounded border border-white/10 bg-md-surface hover:border-sky-500 hover:bg-md-surface-high text-left">
                <span className="font-mono font-semibold">{e.ticker}</span>
                <span className={`font-mono text-[11px] ${e.win ? 'text-emerald-400' : 'text-rose-400'}`}>
                  {ret > 0 ? '+' : ''}{ret}%
                </span>
              </button>
            )
          })}
        </div>
        <p className="text-[10px] text-md-on-surface-var/50 mt-1">
          ×N = zone vol multiple · click a ticker to load it (then open Superchart). Sorted most-recent first.
        </p>
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
