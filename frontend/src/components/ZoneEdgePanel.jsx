import { useEffect, useState } from 'react'
import { descFor, SIGNAL_DESC, badgeFor, FAMILY_LEGEND, FAMILY_CLS } from '../utils/signalDesc'

// Zone EXIT vs RETEST forward-edge research. Raw events are usually NOT an edge;
// the value is which bar-context (signal / pattern / description) lifts them.
const EVENT_META = {
  exit_up:   { label: 'EXIT ↑ (breakout)',  color: 'text-emerald-300', desc: 'close first crosses ABOVE the zone' },
  exit_down: { label: 'EXIT ↓ (breakdown)', color: 'text-rose-300',    desc: 'close first crosses BELOW the zone' },
  retest:    { label: 'RETEST (return)',    color: 'text-amber-300',    desc: 'close re-enters the zone after leaving' },
}
const VOL_MINS = [2, 5, 10]
const HORIZONS = [5, 10, 20]

// prettify combo feature names: sequence (p1_/p2_ = bar -1/-2), flip, etc.
const fmtFeat = (f) => {
  if (!f) return f
  return f
    .replace(/^tz_up_next3$/, 'flip✓')
    .replace(/^flip_code=/, 'flip→')
    .replace(/^p1_tz=/, '−1:T=').replace(/^p1_z=/, '−1:Z=').replace(/^p1_vol=/, '−1:vol=').replace(/^p1_l5=/, '−1:l5=')
    .replace(/^p2_tz=/, '−2:T=').replace(/^p2_z=/, '−2:Z=')
    .replace(/^vol_bucket=/, 'vol=').replace(/^bar_body_wick=/, 'body=').replace(/^bar_line5=/, 'l5=')
    .replace(/^bar_range_class=/, 'rng=').replace(/^l_sig=/, 'L=').replace(/^composite_full_suffix=/, 'sfx=')
}
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
  // Exit-sequence miner (multi-bar lead-in buildups)
  const [seqEvent, setSeqEvent] = useState('exit_up')
  const [seqDepth, setSeqDepth] = useState(3)             // bars back: 2–4
  const [seqWays, setSeqWays]   = useState(2)
  const [seqData, setSeqData]   = useState(null)
  const [seqLoading, setSeqLoading] = useState(false)
  const [seqZoneDef, setSeqZoneDef] = useState('spike')   // 'spike' (V1) | 'vb' (V2)
  const [legendOpen, setLegendOpen] = useState(false)
  const [examples, setExamples] = useState(null)
  // Pattern builder (full bar-code slots)
  const [patValues, setPatValues] = useState(null)
  const [patSlots, setPatSlots]   = useState({ tz: '*', z: '*', flip: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' })
  const [patResult, setPatResult] = useState(null)
  const [patLoading, setPatLoading] = useState(false)
  const [live, setLive] = useState(null)
  const [liveLoading, setLiveLoading] = useState(false)
  const [liveBools, setLiveBools] = useState([])   // boolean signals applied to live
  const [liveFlip, setLiveFlip]   = useState(false) // confirmed-only (flip required)
  const [liveCats, setLiveCats]   = useState({})    // flip_code / sequence p1_*/p2_* filters

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

  // Exit-sequence miner — multi-bar lead-in buildups before a zone exit
  useEffect(() => {
    let dead = false
    setSeqLoading(true)
    const q = new URLSearchParams({ event_type: seqEvent, depth: seqDepth, ways: seqWays,
      vol_min: volMin, horizon, min_n: '30', zone_def: seqZoneDef })
    fetch(`/api/zone-events/sequences?${q}`)
      .then(r => r.json())
      .then(d => { if (!dead) setSeqData(d) })
      .catch(() => { if (!dead) setSeqData(null) })
      .finally(() => { if (!dead) setSeqLoading(false) })
    return () => { dead = true }
  }, [seqEvent, seqDepth, seqWays, volMin, horizon, seqZoneDef])

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
    setPatSlots({ tz: '*', z: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' })
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
    const catStr = Object.entries(liveCats).map(([k, v]) => `${k}=${v}`).join(',')
    if (catStr) q.set('cats', catStr)
    fetch(`/api/zone-events/live?${q}`).then(r => r.json())
      .then(d => { if (!dead) setLive(d) }).catch(() => { if (!dead) setLive(null) })
      .finally(() => { if (!dead) setLiveLoading(false) })
    return () => { dead = true }
  }, [patSlots, liveBools, liveFlip, liveCats, comboEvent, volMin, horizon])

  // map a combo's features → live filter (slots + bool signals + flip), then scroll up
  const SLOT_OF_COL = { t_sig: 'tz', z_sig: 'z', flip_code: 'flip', l_sig: 'l',
                        full_suffix: 'suffix', composite_full_suffix: 'suffix', bar_body_wick: 'bodywk',
                        gap_rng: 'gaprng', bar_line5: 'l5', vol_bucket: 'vol' }
  function applyComboToLive(c) {
    const feats = [c.a, c.b, c.c].filter(Boolean)
    const slots = { tz: '*', z: '*', flip: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' }
    const bools = []; const cats = {}; let flip = false
    for (const f of feats) {
      if (f === 'tz_up_next3') {
        flip = true                          // the follow-through flip → confirmed-only
      } else if (f.includes('=')) {
        const i = f.indexOf('='); const col = f.slice(0, i); const val = f.slice(i + 1)
        const slot = SLOT_OF_COL[col]
        if (slot) slots[slot] = val          // a bar-code slot (vol/l5/body…)
        else cats[col] = val                 // flip_code / sequence p1_*/p2_* / fib_level
      } else {
        bools.push(f)                        // a boolean signal (sig_abs, wyc_spring, at_fib…)
      }
    }
    setPatSlots(slots); setLiveBools(bools); setLiveCats(cats); setLiveFlip(flip)
    document.getElementById('live-setups')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }
  const clearLive = () => {
    setPatSlots({ tz: '*', z: '*', flip: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' })
    setLiveBools([]); setLiveCats({}); setLiveFlip(false)
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
                <span key={k} className="font-mono px-1.5 py-0.5 rounded bg-emerald-900/30 border border-emerald-700/40 text-emerald-200">{fmtFeat(`${k}=${v}`)}</span>
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
                    {rows.map((s, i) => {
                      const top = s.patterns?.[0]
                      const pTitle = s.patterns?.length
                        ? 'matched: ' + s.patterns.map(p => `${p.name} (OOS ${p.oos_win}%)`).join(' · ')
                        : 'no robust pattern (vol≠B)'
                      const pColor = !top ? '' : top.oos_win >= 62 ? 'bg-emerald-700/50 text-emerald-100'
                        : top.oos_win >= 57 ? 'bg-sky-800/50 text-sky-200' : 'bg-slate-700/50 text-slate-200'
                      // which T-code drove the flip → strong (T1G/T1/T4) green, T5 trap red
                      const fc = s.flip_code
                      const fcColor = !fc ? '' : ['T1G','T1','T4','T2G'].includes(fc) ? 'text-emerald-300'
                        : fc === 'T5' ? 'text-rose-400' : 'text-md-on-surface-var'
                      return (
                        <button key={i} onClick={() => onSelectTicker?.(s.ticker)}
                          title={`${pTitle}\nflip via ${fc || '—'}  (T1G 64% · T1 57% · T4 53% · T5 33% trap)\nzone ${s.zone_low}–${s.zone_high} · close ${s.close} · vol ×${s.z_mult} (${s.vol_bucket}/${s.range})`}
                          className={`flex items-center justify-between gap-1 px-2 py-1 rounded border text-left ${st === 'confirmed' ? 'border-emerald-700/40 bg-emerald-900/20 hover:border-emerald-400' : 'border-amber-700/30 bg-amber-900/10 hover:border-amber-400'}`}>
                          <span className="font-mono font-semibold text-xs flex items-center gap-1">
                            {s.ticker}
                            {top && <span className={`px-1 rounded text-[9px] font-normal ${pColor}`} title={pTitle}>{top.tag}{s.patterns.length > 1 ? `+${s.patterns.length - 1}` : ''}</span>}
                            {fc && <span className={`text-[9px] font-normal ${fcColor}`} title={`flip via ${fc}`}>↑{fc}</span>}
                          </span>
                          <span className="font-mono text-[10px] text-md-on-surface-var">{s.days_ago}d ·×{s.z_mult}</span>
                        </button>
                      )
                    })}
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
                    <span className="text-sky-300 cursor-help" title={descFor(c.a)}>{fmtFeat(c.a)}</span>
                    <span className="text-md-on-surface-var/40"> + </span>
                    <span className="text-violet-300 cursor-help" title={descFor(c.b)}>{fmtFeat(c.b)}</span>
                    {c.c && <><span className="text-md-on-surface-var/40"> + </span><span className="text-amber-300 cursor-help" title={descFor(c.c)}>{fmtFeat(c.c)}</span></>}
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

      {/* Exit-sequence miner — multi-bar lead-in buildups */}
      <div className="mt-7">
        <h2 className="text-sm font-bold mb-1">🧬 Exit-sequence miner — the multi-bar buildup before a move</h2>
        <p className="text-[11px] text-md-on-surface-var mb-3">
          A move often <b>starts as a several-bar combination</b>. This ranks the lead-in signal sequences
          in the {seqDepth} bars ending at a zone exit — e.g. <span className="font-mono">−2:sig_abs → −1:eb_bull → 0:vbo_up</span>.
          Forward edge vs the exit population, IS/OOS-validated. <span className="text-md-on-surface-var/60">0 = exit bar.</span>
        </p>
        <div className="flex flex-wrap items-center gap-3 text-xs mb-3">
          <div className="flex items-center gap-1">
            {['exit_up', 'exit_down'].map(et => (
              <button key={et} onClick={() => setSeqEvent(et)}
                className={`px-2 py-0.5 rounded border ${seqEvent === et ? `bg-md-surface-high border-white/30 ${EVENT_META[et].color}` : 'bg-md-surface border-white/10 hover:text-white'}`}>
                {EVENT_META[et].label.split(' ')[0]} {EVENT_META[et].label.includes('↑') ? '↑' : '↓'}
              </button>
            ))}
          </div>
          <label className="flex items-center gap-2">
            <span className="text-md-on-surface-var/70">depth</span>
            <input type="range" min={2} max={4} step={1} value={seqDepth}
              onChange={e => setSeqDepth(Number(e.target.value))} className="accent-sky-400 w-24" />
            <span className="font-mono text-sky-300">{seqDepth} bars</span>
          </label>
          <div className="flex items-center gap-1">
            {[2, 3].map(w => (
              <button key={w} onClick={() => setSeqWays(w)}
                className={`px-2 py-0.5 rounded border ${seqWays === w ? 'bg-sky-900/60 text-sky-200 border-sky-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>{w}-bar</button>
            ))}
          </div>
          <div className="flex items-center gap-1" title="How the zone is formed">
            <span className="text-md-on-surface-var/60">zone:</span>
            {[['spike', `spike ≥${volMin}×`], ['vb', 'VB class']].map(([zd, lbl]) => (
              <button key={zd} onClick={() => setSeqZoneDef(zd)}
                className={`px-2 py-0.5 rounded border ${seqZoneDef === zd ? 'bg-rose-900/50 text-rose-200 border-rose-600' : 'bg-md-surface border-white/10 hover:text-white'}`}>{lbl}</button>
            ))}
            {seqZoneDef === 'vb' && <span className="text-rose-300/70 text-[10px]">V2</span>}
          </div>
          {seqLoading && <span className="text-sky-400 animate-pulse">mining…</span>}
          {seqData?.event_base && <span className="text-md-on-surface-var/60">base {seqData.event_base.win_rate_pct}% · {seqData.params?.n_signals} lead-in signals · {seqData.params?.n_combos} sequences</span>}
        </div>
        {/* how to read the −2/−1/0 offsets */}
        <div className="mb-1.5 text-[11px] text-md-on-surface-var/80 flex items-center gap-1.5 flex-wrap">
          <span className="text-md-on-surface-var/50">how to read:</span>
          each badge has a small number = <b>which bar it fired on, counting back from the breakout</b>:
          <span className="font-mono px-1 rounded bg-emerald-500/15 text-emerald-300/80">0</span> = the breakout bar ·
          <span className="font-mono px-1 rounded bg-white/[0.06]">−1</span> = the bar before it ·
          <span className="font-mono px-1 rounded bg-white/[0.06]">−2</span> = two bars before. Read <b>left → right in time</b>.
        </div>
        {/* family color key — what each badge color means */}
        <div className="flex flex-wrap items-center gap-1.5 mb-2 text-[10px]">
          <span className="text-md-on-surface-var/50">colour =</span>
          {FAMILY_LEGEND.map(([fam, name]) => (
            <span key={fam} className={`px-1.5 py-px rounded border border-white/10 font-mono ${FAMILY_CLS[fam]}`}>{name}</span>
          ))}
          <span className="text-md-on-surface-var/40 ml-1">· hover any badge for its full meaning</span>
        </div>
        <table className="w-full text-xs border border-white/10 rounded overflow-hidden">
          <thead className="bg-md-surface-high text-md-on-surface-var">
            <tr>
              <th className="text-left px-3 py-1.5">lead-in sequence (earliest → exit)</th>
              <th className="text-right px-3 py-1.5">n</th>
              <th className="text-right px-3 py-1.5">win</th>
              <th className="text-right px-3 py-1.5" title="in-sample">IS</th>
              <th className="text-right px-3 py-1.5" title="out-of-sample">OOS</th>
              <th className="text-center px-2 py-1.5" title="does OOS hold within 6pp of IS?">holds?</th>
            </tr>
          </thead>
          <tbody>
            {(seqData?.best || []).map((c, i) => {
              const base = seqData.event_base?.win_rate_pct ?? 0
              const holds = c.win_is_pct != null && c.win_oos_pct != null && (c.win_oos_pct - c.win_is_pct) >= -6 && c.win_oos_pct > base
              return (
                <tr key={i} className="border-t border-white/5">
                  <td className="px-3 py-1.5">
                    <div className="flex items-center gap-1 flex-wrap">
                      {(c.sequence || []).map((x, j) => {
                        const bd = badgeFor(x.signal)
                        const off = x.bar === 'exit' ? 0 : Math.abs(parseInt(x.bar, 10))
                        const offTip = off === 0 ? 'the breakout (exit) bar' : `${off} bar${off > 1 ? 's' : ''} before the breakout`
                        return (
                          <span key={j} className="inline-flex items-center gap-1">
                            {j > 0 && <span className="text-md-on-surface-var/30 text-xs">→</span>}
                            <span className="inline-flex items-center gap-0.5">
                              <span title={offTip}
                                className={`text-[9px] font-mono px-1 rounded cursor-help ${off === 0 ? 'bg-emerald-500/15 text-emerald-300/80' : 'bg-white/[0.06] text-md-on-surface-var/55'}`}>
                                {off === 0 ? '0' : x.bar}
                              </span>
                              <span title={descFor(x.signal)}
                                className={`inline-block rounded border border-white/10 font-mono text-[10px] px-1 py-px cursor-help ${bd.cls}`}>
                                {bd.label}
                              </span>
                            </span>
                          </span>
                        )
                      })}
                    </div>
                  </td>
                  <td className={`text-right px-3 py-1.5 font-mono ${c.n >= 200 ? 'text-emerald-300' : c.n >= 60 ? 'text-md-on-surface' : 'text-amber-400/70'}`}>
                    {c.n.toLocaleString()}{c.n < 60 ? ' ⚠' : ''}
                  </td>
                  <td className="text-right px-3 py-1.5 font-mono font-bold">{c.win_rate_pct}%</td>
                  <td className="text-right px-3 py-1.5 font-mono text-md-on-surface-var">{c.win_is_pct}%<span className="text-[9px] text-md-on-surface-var/40"> ·{c.n_is}</span></td>
                  <td className={`text-right px-3 py-1.5 font-mono ${holds ? 'text-emerald-300' : 'text-rose-300'}`}>{c.win_oos_pct}%<span className="text-[9px] text-md-on-surface-var/40"> ·{c.n_oos}</span></td>
                  <td className="text-center px-2 py-1.5">{holds ? '✅' : '❌'}</td>
                </tr>
              )
            })}
            {!seqLoading && !(seqData?.best || []).length && (
              <tr><td colSpan={6} className="px-3 py-3 text-center text-md-on-surface-var/50">no sequences ≥ min-n at this depth</td></tr>
            )}
          </tbody>
        </table>
        <p className="text-[10px] text-md-on-surface-var/50 mt-1">
          Lead-in set = curated move-initiation signals (momentum / coil / absorption / structure / volume), lagged over the window.
          {seqEvent === 'exit_down' && <span className="text-amber-400/70"> ⚠ exit↓ "win" = price UP after the breakdown (i.e. a failed/spring breakdown).</span>}
        </p>

        {/* Signal legend — descriptions for every signal in the panel */}
        <div className="mt-3 border border-white/10 rounded">
          <button onClick={() => setLegendOpen(o => !o)}
            className="w-full text-left px-3 py-1.5 text-xs font-semibold flex items-center gap-2 hover:text-white">
            <span className={`transition-transform ${legendOpen ? 'rotate-90' : ''}`}>▸</span>
            ℹ️ Signal legend — what each code means
            <span className="text-md-on-surface-var/50 font-normal">(hover any signal anywhere for its description)</span>
          </button>
          {legendOpen && (
            <div className="px-3 pb-3 pt-1 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-x-5 gap-y-1 text-[11px]">
              {Object.entries(SIGNAL_DESC).map(([sg, d]) => (
                <div key={sg} className="flex gap-2 leading-snug">
                  <span className="font-mono text-sky-300 shrink-0 min-w-[92px]">{sg}</span>
                  <span className="text-md-on-surface-var">{d}</span>
                </div>
              ))}
              <div className="flex gap-2 leading-snug">
                <span className="font-mono text-emerald-300 shrink-0 min-w-[92px]">vol=B / VB</span>
                <span className="text-md-on-surface-var">Big vs Very-Big volume class. B = controlled edge; VB = climactic, often a retest trap.</span>
              </div>
              <div className="flex gap-2 leading-snug">
                <span className="font-mono text-amber-300 shrink-0 min-w-[92px]">@-1 / @-2</span>
                <span className="text-md-on-surface-var">Bar offset in a sequence: 0 = the exit bar, −1 / −2 = bars before it.</span>
              </div>
              <div className="flex gap-2 leading-snug">
                <span className="font-mono text-violet-300 shrink-0 min-w-[92px]">T1G…T5</span>
                <span className="text-md-on-surface-var">Flip code = which T-code drives the follow-through. T1G strongest (~64%), T5 a trap (~33%).</span>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Pattern builder — the full bar-code, all slots together */}
      <div className="mt-7">
        <h2 className="text-sm font-bold mb-1">Pattern builder — the full bar code, together</h2>
        <p className="text-[11px] text-md-on-surface-var mb-2">
          Set each slot to a value or leave <b>*</b> (any). Uses the current event ({EVENT_META[comboEvent]?.label.split(' ')[0]})
          {comboAnchor && <span className="text-emerald-300"> + T/Z flip</span>}. ⚠ small n = overfit — trust n ≥ 150.
          <br/><span className="text-emerald-300/80">On a <b>retest</b> the event bar has no T-code (it's bearish/neutral) — the bullish T fires <b>after</b>: pick it in <b>flip→T</b> (T1G/T1/T4), not the empty <b>T</b> slot.</span>
        </p>
        <div className="flex flex-wrap gap-2 mb-3 text-[11px]">
          {[['tz','T','T-code on the event bar (TZ_WLNBB bullish 2-bar pattern). Usually empty on a retest.'],
            ['z','Z','Z-code on the event bar (bearish 2-bar pattern).'],
            ['flip','flip→T','The FLIP T-code — the bullish T that fires AFTER a retest (the follow-through). T1G strongest, T5 trap.'],
            ['l','L','L-line code (WLNBB absorption / level).'],
            ['suffix','suffix','Full close-position suffix (EBA/EBO/NDI…) — where the bar closed within its range.'],
            ['bodywk','body/wk','Body-vs-wick shape of the bar.'],
            ['gaprng','gap/rng','Gap class + range class (N=narrow … wide).'],
            ['l5','l5','Line-5 micro-structure code.'],
            ['vol','vol','Volume class (Bollinger band on volume): W/L/N/B/VB. B=edge, VB=trap.']].map(([slot, label, tip]) => (
            <label key={slot} className="flex flex-col gap-0.5">
              <span className={`uppercase tracking-wide text-[9px] cursor-help ${slot==='flip' ? 'text-emerald-300/70' : 'text-md-on-surface-var/60'}`} title={tip}>{label}</span>
              <select value={patSlots[slot]} onChange={e => setPatSlots(s => ({ ...s, [slot]: e.target.value }))}
                className={`bg-md-surface border rounded px-1.5 py-1 font-mono text-md-on-surface min-w-[64px] ${slot==='flip' ? 'border-emerald-700/40' : 'border-white/10'}`}>
                <option value="*">*</option>
                {(patValues?.[slot] || []).map(v => (
                  <option key={v.value} value={v.value}>{v.value} ({v.n})</option>
                ))}
              </select>
            </label>
          ))}
          <button onClick={() => setPatSlots({ tz: '*', z: '*', flip: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' })}
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
          <span className="truncate cursor-help"
            title={`${descFor(r.value === '1' ? r.feature : r.feature + '=' + r.value)}\n\n(n=${r.n}, win ${r.win_rate_pct}%)`}>
            {r.feature}{r.value !== '1' && <span className="text-md-on-surface-var/50">={r.value}</span>}
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
