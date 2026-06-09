import { useEffect, useState } from 'react'
import { badgeFor, descFor } from '../utils/signalDesc'
import { pwlAdd } from './PersonalWatchlistPanel'
import { sjAdd, sjByDate, sjRemove, sjClear, sjCount } from '../utils/setupsJournal'

const actCls = (a) => a === 'BUY' ? 'bg-emerald-900/50 text-emerald-200 border-emerald-600'
  : a === 'WATCH' ? 'bg-amber-900/40 text-amber-200 border-amber-700/50'
  : 'bg-md-surface text-md-on-surface-var/60 border-white/10'

// combo feature → live-setup slot, + prettifier (mirrors ZoneEdgePanel)
const SLOT_OF_COL = { t_sig: 'tz', z_sig: 'z', flip_code: 'flip', l_sig: 'l',
  full_suffix: 'suffix', composite_full_suffix: 'suffix', bar_body_wick: 'bodywk',
  gap_rng: 'gaprng', bar_line5: 'l5', vol_bucket: 'vol' }
const fmtFeat = (f) => !f ? f : f
  .replace(/^tz_up_next3$/, 'flip✓').replace(/^flip_code=/, 'flip→')
  .replace(/^p1_tz=/, '−1:T=').replace(/^p1_z=/, '−1:Z=').replace(/^p1_vol=/, '−1:vol=').replace(/^p1_l5=/, '−1:l5=')
  .replace(/^p2_tz=/, '−2:T=').replace(/^p2_z=/, '−2:Z=')
  .replace(/^vol_bucket=/, 'vol=').replace(/^bar_body_wick=/, 'body=').replace(/^bar_line5=/, 'l5=')
  .replace(/^bar_range_class=/, 'rng=').replace(/^l_sig=/, 'L=').replace(/^composite_full_suffix=/, 'sfx=')
function applyCombo(c) {
  const slots = { tz: '*', z: '*', flip: '*', l: '*', suffix: '*', bodywk: '*', gaprng: '*', l5: '*', vol: '*' }
  const bools = []; const cats = {}; let flip = false
  for (const f of [c.a, c.b, c.c].filter(Boolean)) {
    if (f === 'tz_up_next3') flip = true
    else if (f.includes('=')) { const i = f.indexOf('='); const col = f.slice(0, i); const val = f.slice(i + 1); const slot = SLOT_OF_COL[col]; if (slot) slots[slot] = val; else cats[col] = val }
    else bools.push(f)
  }
  return { slots, bools, cats, flip }
}
const comboHolds = (c, base) => c.win_is_pct != null && c.win_oos_pct != null && (c.win_oos_pct - c.win_is_pct) >= -6 && c.win_oos_pct > base
const comboScore = (c, base) => Math.round(Math.min(100,
  (c.win_oos_pct || 0) * 0.8 + (c.lift_win_pp || 0) * 1.2 + (comboHolds(c, base) ? 6 : -10) + Math.min(c.n || 0, 2500) / 250))

// Setups Board — recent tickers that built an OOS-holding lead-in sequence,
// scored, with probability-up (= OOS win%), why, last price, journal status.
const ZONES = [['spike', 'spike ≥5×'], ['spike25', 'spike 2–5×'], ['vb', 'VB class']]

const scoreCls = (s) => s >= 80 ? 'text-emerald-300' : s >= 65 ? 'text-lime-300' : s >= 50 ? 'text-amber-300' : 'text-md-on-surface-var'
const probCls  = (p) => p >= 70 ? 'text-emerald-300' : p >= 60 ? 'text-lime-300' : 'text-amber-300'

function SeqBadges({ sequence }) {
  // "−2:sig_abs → 0:sig_vol_10x"
  const parts = (sequence || '').split(' → ')
  return (
    <div className="flex items-center gap-1 flex-wrap">
      {parts.map((p, j) => {
        const i = p.indexOf(':'); const bar = p.slice(0, i); const sig = p.slice(i + 1)
        const bd = badgeFor(sig)
        return (
          <span key={j} className="inline-flex items-center gap-1">
            {j > 0 && <span className="text-md-on-surface-var/30 text-xs">→</span>}
            <span className="inline-flex items-center gap-0.5">
              <span className={`text-[9px] font-mono px-1 rounded ${bar === '0' ? 'bg-emerald-500/15 text-emerald-300/80' : 'bg-white/[0.06] text-md-on-surface-var/55'}`}>{bar}</span>
              <span title={descFor(sig)} className={`inline-block rounded border border-white/10 font-mono text-[10px] px-1 py-px cursor-help ${bd.cls}`}>{bd.label}</span>
            </span>
          </span>
        )
      })}
    </div>
  )
}

export default function SetupsBoardPanel({ onSelectTicker }) {
  const [zoneDef, setZoneDef] = useState('spike')
  const [data, setData]   = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr]     = useState(null)
  const [wlMsg, setWlMsg] = useState('')
  const [live, setLive]   = useState({})       // ticker -> { price, change_pct }
  const [liveLoading, setLiveLoading] = useState(false)
  const [view, setView]   = useState('board')  // 'board' | 'journal'
  const [jTick, setJTick] = useState(0)         // bump to re-read the journal
  const [ai, setAi]       = useState({})        // ticker -> { action, conviction, thesis }
  const [aiLoading, setAiLoading] = useState(false)

  const fetchLive = (tickers) => {
    if (!tickers.length) return
    setLiveLoading(true)
    fetch(`/api/live-prices?tickers=${tickers.join(',')}`)
      .then(r => r.json())
      .then(d => setLive(d.prices || {}))
      .catch(() => {})
      .finally(() => setLiveLoading(false))
  }

  useEffect(() => {
    let dead = false
    setLoading(true); setErr(null); setLive({})
    const q = new URLSearchParams({ zone_def: zoneDef, max_age_days: '20', min_oos: '55' })
    fetch(`/api/zone-events/board?${q}`)
      .then(r => r.json())
      .then(d => {
        if (dead) return
        if (d.error) { setErr(d.error); return }
        setData(d)
        fetchLive((d.rows || []).map(r => r.ticker))   // auto-fetch live on load
      })
      .catch(e => { if (!dead) setErr(String(e)) })
      .finally(() => { if (!dead) setLoading(false) })
    return () => { dead = true }
  }, [zoneDef])

  const rows = data?.rows || []
  const journalEntry = (r) => ({ ticker: r.ticker, sequence: r.sequence, prob_up: r.prob_up, score: r.score, last_price: r.last_price })
  // add to Watchlist AND log to the dated setups-journal (per request)
  const addAll = () => {
    rows.forEach(r => { pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.last_price, tz_sig: r.sequence }); sjAdd(journalEntry(r), 'watchlist') })
    setJTick(t => t + 1)
    setWlMsg(`added ${rows.length} → Watchlist + Journal`); setTimeout(() => setWlMsg(''), 2500)
  }
  const addOneWl = (r) => { pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.last_price, tz_sig: r.sequence }); sjAdd(journalEntry(r), 'watchlist'); setJTick(t => t + 1) }
  const addOneJrnl = (r) => { sjAdd(journalEntry(r), 'journal'); setJTick(t => t + 1) }
  const addAllJrnl = () => { rows.forEach(r => sjAdd(journalEntry(r), 'journal')); setJTick(t => t + 1); setWlMsg(`logged ${rows.length} → Journal`); setTimeout(() => setWlMsg(''), 2500) }
  // generic: judge an arbitrary list of {ticker, source, setup} via the LLM, merge
  // results into the shared `ai` map (so decisions persist across all 3 tabs)
  const runAi = (items) => {
    if (!items.length) return Promise.resolve()
    setAiLoading(true)
    return fetch('/api/journal/advise', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(items) })
      .then(r => r.json())
      .then(d => setAi(prev => ({ ...prev, ...Object.fromEntries((d.decisions || []).map(x => [x.ticker, x])) })))
      .catch(() => {})
      .finally(() => setAiLoading(false))
  }
  const aiDecide = () => runAi(rows.map(r => ({ ticker: r.ticker, source: 'sequence',
    setup: { sequence: r.sequence, prob_up_pct: r.prob_up, edge_pp: r.edge_pp, n: r.n, why: r.why } })))

  return (
    <div className="p-4 text-md-on-surface">
      <h1 className="text-base font-bold mb-1">📋 Setups Board — sequence-matched tickers</h1>
      <p className="text-[11px] text-md-on-surface-var mb-3">
        Recent (≤20d) tickers whose zone-exit built an <b>OOS-validated lead-in sequence</b>. <b>Prob↑</b> = the
        sequence's out-of-sample win-rate (probability the move follows through). <b>Score</b> blends prob + edge +
        recency. Hover <b>Why</b> for the reasoning. Click a ticker → chart.
      </p>
      <div className="flex items-center gap-2 text-xs mb-3 flex-wrap">
        <div className="flex items-center gap-1 mr-1">
          {[['board', 'Board'], ['combos', '🔗 Combos'], ['journal', `📓 Journal${sjCount() ? ` (${sjCount()})` : ''}`]].map(([v, lbl]) => (
            <button key={v} onClick={() => setView(v)}
              className={`px-2 py-0.5 rounded border ${view === v ? 'bg-md-surface-high border-white/30 text-white' : 'bg-md-surface border-white/10 hover:text-white'}`}>{lbl}</button>
          ))}
        </div>
        {view === 'board' && <>
          <span className="text-md-on-surface-var/60">zone:</span>
          {ZONES.map(([z, lbl]) => (
            <button key={z} onClick={() => setZoneDef(z)}
              className={`px-2 py-0.5 rounded border ${zoneDef === z ? 'bg-rose-900/50 text-rose-200 border-rose-600' : 'bg-md-surface border-white/10 hover:text-white'}`}>{lbl}</button>
          ))}
          {loading && <span className="text-sky-400 animate-pulse">scoring…</span>}
          {data && <span className="text-md-on-surface-var/60">{data.count} setups · {data.as_of}</span>}
          <button onClick={() => fetchLive(rows.map(r => r.ticker))} disabled={!rows.length || liveLoading}
            className="px-2 py-0.5 rounded border border-sky-700/50 text-sky-300 hover:bg-sky-900/30 disabled:opacity-40"
            title="Refresh live prices (Massive)">{liveLoading ? '↻ live…' : '↻ live'}</button>
          <button onClick={aiDecide} disabled={!rows.length || aiLoading}
            className="px-2 py-0.5 rounded border border-violet-600/60 text-violet-200 bg-violet-900/30 hover:bg-violet-900/50 disabled:opacity-40"
            title="Ask the journal's AI to judge these tickers (BUY/WATCH/SKIP)">{aiLoading ? '🤖 thinking…' : '🤖 AI decide'}</button>
          <button onClick={addAllJrnl} disabled={!rows.length}
            className="ml-auto px-2 py-0.5 rounded border border-white/10 hover:text-white disabled:opacity-40" title="Log all to the dated Journal">+ all → Journal</button>
          <button onClick={addAll} disabled={!rows.length}
            className="px-2 py-0.5 rounded border border-emerald-700/50 text-emerald-300 hover:bg-emerald-900/30 disabled:opacity-40">★ all → Watchlist</button>
          {wlMsg && <span className="text-emerald-400 text-[11px]">{wlMsg}</span>}
        </>}
        {view === 'journal' && (
          <button onClick={() => { sjClear(); setJTick(t => t + 1) }}
            className="ml-auto px-2 py-0.5 rounded border border-white/10 text-md-on-surface-var hover:text-white">clear journal</button>
        )}
      </div>
      {err && <div className="text-rose-400 text-xs mb-2">error: {err}</div>}
      {view === 'journal' ? (
        <SetupsJournalView key={jTick} onSelectTicker={onSelectTicker} ai={ai} />
      ) : view === 'combos' ? (
        <ComboBoardView onSelectTicker={onSelectTicker} ai={ai} runAi={runAi} aiLoading={aiLoading} />
      ) : (
      <>
      <table className="w-full text-xs border border-white/10 rounded overflow-hidden">
        <thead className="bg-md-surface-high text-md-on-surface-var">
          <tr>
            <th className="text-right px-2 py-1.5">score</th>
            <th className="text-left px-2 py-1.5">ticker</th>
            <th className="text-right px-2 py-1.5" title="last DB close">close</th>
            <th className="text-right px-2 py-1.5" title="live snapshot (Massive)">live</th>
            <th className="text-right px-2 py-1.5" title="OOS win-rate of the matched sequence">prob↑</th>
            <th className="text-left px-3 py-1.5">sequence used</th>
            <th className="text-right px-2 py-1.5">rsi</th>
            <th className="text-left px-2 py-1.5">univ</th>
            <th className="text-left px-2 py-1.5">journal</th>
            <th className="text-left px-2 py-1.5" title="AI decision (click 🤖 AI decide)">AI</th>
            <th className="text-right px-2 py-1.5">age</th>
            <th className="text-center px-2 py-1.5"></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={r.ticker} className="border-t border-white/5 hover:bg-white/[0.03]">
              <td className={`text-right px-2 py-1.5 font-mono font-bold ${scoreCls(r.score)}`}>{r.score}</td>
              <td className="px-2 py-1.5">
                <button onClick={() => onSelectTicker?.(r.ticker)}
                  className="font-mono font-semibold hover:text-sky-300">{r.ticker}</button>
              </td>
              <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var">{r.last_price != null ? '$' + r.last_price : '—'}</td>
              <td className="text-right px-2 py-1.5 font-mono">
                {live[r.ticker] ? (
                  <span>${live[r.ticker].price}
                    {live[r.ticker].change_pct != null && (
                      <span className={`ml-1 text-[10px] ${live[r.ticker].change_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                        {live[r.ticker].change_pct >= 0 ? '+' : ''}{live[r.ticker].change_pct}%</span>
                    )}
                  </span>
                ) : <span className="text-md-on-surface-var/30">{liveLoading ? '…' : '—'}</span>}
              </td>
              <td className={`text-right px-2 py-1.5 font-mono font-bold ${probCls(r.prob_up)}`}>{r.prob_up}%
                <span className="text-[9px] text-md-on-surface-var/40"> ·{r.n}</span></td>
              <td className="px-3 py-1.5" title={r.why}><SeqBadges sequence={r.sequence} /></td>
              <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var">{r.rsi ?? '—'}</td>
              <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var/70">{r.universe}</td>
              <td className="px-2 py-1.5">
                {r.journal
                  ? <span className="text-[10px] px-1 rounded bg-emerald-900/40 text-emerald-300 border border-emerald-700/40">{r.journal.status}{r.journal.conviction ? ` ·${r.journal.conviction}` : ''}</span>
                  : <span className="text-md-on-surface-var/30 text-[10px]">—</span>}
              </td>
              <td className="px-2 py-1.5">
                {ai[r.ticker]
                  ? <span title={ai[r.ticker].thesis} className={`text-[10px] px-1 rounded border cursor-help ${actCls(ai[r.ticker].action)}`}>{ai[r.ticker].action} ·{ai[r.ticker].conviction}</span>
                  : <span className="text-md-on-surface-var/25 text-[10px]">—</span>}
              </td>
              <td className="text-right px-2 py-1.5 text-md-on-surface-var/60 font-mono">{r.age_days}d</td>
              <td className="px-2 py-1.5 whitespace-nowrap">
                <button onClick={() => addOneJrnl(r)} title="Log to dated Journal"
                  className="px-1 rounded border border-white/10 text-[10px] text-md-on-surface-var hover:text-white mr-1">+J</button>
                <button onClick={() => addOneWl(r)} title="Add to Watchlist (+ Journal)"
                  className="px-1 rounded border border-emerald-700/40 text-[10px] text-emerald-300 hover:bg-emerald-900/30">★</button>
              </td>
            </tr>
          ))}
          {!loading && !rows.length && (
            <tr><td colSpan={13} className="px-3 py-4 text-center text-md-on-surface-var/50">no holding-sequence setups in the last 20d</td></tr>
          )}
        </tbody>
      </table>
      <p className="text-[10px] text-md-on-surface-var/50 mt-2">
        Prob↑ is the sequence's historical OOS win-rate — a base rate, not a guarantee. exit↓ "win" = price UP after a
        failed breakdown (spring). Price = last DB close ({data?.as_of}). Journal column = AI paper-positions already opened/pending.
        AI column = advisory only (no position). +J logs to your dated Journal; ★ adds to Watchlist + Journal.
      </p>
      </>
      )}
    </div>
  )
}

// ── combos board: IS/OOS-validated retest combos, scored, drill to tickers ────
function ComboBoardView({ onSelectTicker, ai = {}, runAi, aiLoading }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [ways, setWays] = useState(2)
  const [pick, setPick] = useState(null)
  const [tk, setTk] = useState(null)
  const [tkL, setTkL] = useState(false)

  useEffect(() => {
    let dead = false; setLoading(true); setPick(null); setTk(null)
    const q = new URLSearchParams({ event_type: 'retest', vol_min: '5', horizon: '10',
      ways: String(ways), top: '40', min_n: ways === 3 ? '80' : '40', anchor: 'tz_up_next3' })
    fetch(`/api/zone-events/combos?${q}`)
      .then(r => r.json()).then(d => { if (!dead) setData(d) })
      .catch(() => {}).finally(() => { if (!dead) setLoading(false) })
    return () => { dead = true }
  }, [ways])

  const base = data?.event_base?.win_rate_pct ?? 0
  const scored = (data?.best || []).map(c => ({ ...c, _s: comboScore(c, base), _h: comboHolds(c, base) }))
    .sort((a, b) => b._s - a._s)

  const pickCombo = (c, i) => {
    if (pick === i) { setPick(null); setTk(null); return }
    setPick(i); setTk(null); setTkL(true)
    const { slots, bools, cats, flip } = applyCombo(c)
    const q = new URLSearchParams({ event_type: 'retest', require_flip: flip ? '1' : '0',
      vol_min: '5', horizon: '10', max_age_days: '10', ...slots })
    if (bools.length) q.set('bools', bools.join(','))
    if (Object.keys(cats).length) q.set('cats', Object.entries(cats).map(([k, v]) => `${k}=${v}`).join(','))
    fetch(`/api/zone-events/live?${q}`).then(r => r.json()).then(d => setTk(d)).catch(() => setTk({ setups: [] })).finally(() => setTkL(false))
  }

  return (
    <div>
      <div className="flex items-center gap-2 text-xs mb-2">
        <span className="text-md-on-surface-var/60">RETEST + T/Z-flip combinations · score = OOS win + lift + holds + n</span>
        {[2, 3].map(w => (
          <button key={w} onClick={() => setWays(w)}
            className={`px-2 py-0.5 rounded border ${ways === w ? 'bg-sky-900/60 text-sky-200 border-sky-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>{w}-way</button>
        ))}
        {loading && <span className="text-sky-400 animate-pulse">computing…</span>}
        {data && <span className="text-md-on-surface-var/50">base {base}% · {scored.length} combos · click → tickers</span>}
      </div>
      <table className="w-full text-xs border border-white/10 rounded overflow-hidden">
        <thead className="bg-md-surface-high text-md-on-surface-var">
          <tr>
            <th className="text-right px-2 py-1.5">score</th>
            <th className="text-left px-3 py-1.5">combination</th>
            <th className="text-right px-2 py-1.5">n</th>
            <th className="text-right px-2 py-1.5">win</th>
            <th className="text-right px-2 py-1.5">IS</th>
            <th className="text-right px-2 py-1.5">OOS</th>
            <th className="text-center px-2 py-1.5">holds</th>
          </tr>
        </thead>
        <tbody>
          {scored.map((c, i) => (
            <Fragment key={i}>
              <tr onClick={() => pickCombo(c, i)} className={`border-t border-white/5 cursor-pointer hover:bg-white/[0.03] ${pick === i ? 'bg-sky-900/20' : ''}`}>
                <td className={`text-right px-2 py-1.5 font-mono font-bold ${c._s >= 70 ? 'text-emerald-300' : c._s >= 55 ? 'text-lime-300' : 'text-amber-300'}`}>{c._s}</td>
                <td className="px-3 py-1.5 font-mono text-[11px]">
                  <span className="text-md-on-surface-var/30 mr-1">{pick === i ? '▾' : '▸'}</span>
                  <span className="text-sky-300">{fmtFeat(c.a)}</span>
                  <span className="text-md-on-surface-var/40"> + </span>
                  <span className="text-violet-300">{fmtFeat(c.b)}</span>
                  {c.c && <><span className="text-md-on-surface-var/40"> + </span><span className="text-amber-300">{fmtFeat(c.c)}</span></>}
                </td>
                <td className={`text-right px-2 py-1.5 font-mono ${c.n >= 300 ? 'text-emerald-300' : c.n >= 100 ? 'text-md-on-surface' : 'text-amber-400/70'}`}>{c.n.toLocaleString()}</td>
                <td className="text-right px-2 py-1.5 font-mono font-bold">{c.win_rate_pct}%</td>
                <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var">{c.win_is_pct}%</td>
                <td className={`text-right px-2 py-1.5 font-mono ${c._h ? 'text-emerald-300' : 'text-rose-300'}`}>{c.win_oos_pct}%</td>
                <td className="text-center px-2 py-1.5">{c._h ? '✅' : '❌'}</td>
              </tr>
              {pick === i && (
                <tr className="bg-md-surface-con/40"><td colSpan={7} className="px-4 py-2">
                  {tkL ? <span className="text-sky-400 animate-pulse text-xs">loading tickers…</span> : (() => {
                    const conf = (tk?.setups || []).filter(s => s.status === 'confirmed')
                    const pend = (tk?.setups || []).filter(s => s.status === 'pending')
                    const all = [...conf, ...pend]
                    const comboLbl = [c.a, c.b, c.c].filter(Boolean).map(fmtFeat).join(' + ')
                    const chip = (s, cls) => {
                      const dec = ai[s.ticker]
                      return (
                        <button key={s.ticker} onClick={(e) => { e.stopPropagation(); onSelectTicker?.(s.ticker) }}
                          title={`${s.status} · ${s.days_ago ?? 0}d ago${dec ? ' · AI: ' + dec.action + ' ' + dec.conviction + ' — ' + dec.thesis : ''}`}
                          className={`px-1.5 py-0.5 rounded border text-[11px] font-mono hover:border-sky-500 ${cls}`}>
                          {s.ticker}{dec && <span className={`ml-1 text-[9px] ${dec.action === 'BUY' ? 'text-emerald-400' : dec.action === 'WATCH' ? 'text-amber-400' : 'text-md-on-surface-var/40'}`}>{dec.action[0]}{dec.conviction}</span>}
                        </button>)
                    }
                    return (
                      <div className="space-y-1">
                        {!!all.length && runAi && (
                          <button onClick={(e) => { e.stopPropagation(); runAi(all.map(s => ({ ticker: s.ticker, source: 'combo', setup: { combo: comboLbl, prob_up_pct: c.win_oos_pct, n: c.n } }))) }}
                            disabled={aiLoading}
                            className="px-2 py-0.5 rounded border border-violet-600/60 text-violet-200 bg-violet-900/30 hover:bg-violet-900/50 disabled:opacity-40 text-[11px] mb-1">{aiLoading ? '🤖 thinking…' : '🤖 AI decide these'}</button>
                        )}
                        <div><span className="text-[10px] text-emerald-300/80 mr-2">✅ confirmed {conf.length}</span>
                          <span className="inline-flex flex-wrap gap-1">{conf.map(s => chip(s, 'border-emerald-700/40 bg-emerald-900/10 text-emerald-200'))}</span></div>
                        <div><span className="text-[10px] text-amber-300/80 mr-2">⌛ pending {pend.length}</span>
                          <span className="inline-flex flex-wrap gap-1">{pend.map(s => chip(s, 'border-white/10 bg-md-surface text-md-on-surface-var'))}</span></div>
                        {!conf.length && !pend.length && <span className="text-md-on-surface-var/40 text-xs">no recent tickers for this combo</span>}
                      </div>)
                  })()}
                </td></tr>
              )}
            </Fragment>
          ))}
          {!loading && !scored.length && <tr><td colSpan={7} className="px-3 py-4 text-center text-md-on-surface-var/50">no combos</td></tr>}
        </tbody>
      </table>
      <p className="text-[10px] text-md-on-surface-var/50 mt-2">Click a combo → its recent confirmed/pending retest tickers (confirmed = T/Z already flipped up). Score blends OOS win, lift, holds and n.</p>
    </div>
  )
}

// ── dated journal view (localStorage log of added setups) ─────────────────────
function SetupsJournalView({ onSelectTicker, ai = {} }) {
  const byDate = sjByDate()
  const [, force] = useState(0)
  if (!byDate.length) return <div className="text-md-on-surface-var/50 text-xs py-6 text-center">Journal is empty — add setups from the Board (+J / ★).</div>
  return (
    <div>
      {byDate.map(([date, items]) => (
        <div key={date} className="mb-3">
          <div className="text-xs font-semibold text-sky-300/90 mb-1 border-b border-white/10 pb-1">
            {date} <span className="text-md-on-surface-var/50 font-normal">· {items.length} ticker{items.length > 1 ? 's' : ''}</span>
          </div>
          <table className="w-full text-xs">
            <tbody>
              {items.map(it => (
                <tr key={it.ticker} className="border-b border-white/[0.04] hover:bg-white/[0.03]">
                  <td className="py-1 pr-2 w-16"><button onClick={() => onSelectTicker?.(it.ticker)} className="font-mono font-semibold hover:text-sky-300">{it.ticker}</button></td>
                  <td className="py-1 pr-2 w-20 font-mono text-md-on-surface-var">{it.last_price != null ? '$' + it.last_price : ''}</td>
                  <td className="py-1 pr-2 w-16 font-mono text-emerald-300/80">{it.prob_up != null ? it.prob_up + '%' : ''}</td>
                  <td className="py-1 pr-2"><SeqBadges sequence={it.sequence} /></td>
                  <td className="py-1 pr-2 w-16">{ai[it.ticker] && <span title={ai[it.ticker].thesis} className={`text-[10px] px-1 rounded border cursor-help ${actCls(ai[it.ticker].action)}`}>{ai[it.ticker].action[0]}·{ai[it.ticker].conviction}</span>}</td>
                  <td className="py-1 pr-2 w-20 text-[10px] text-md-on-surface-var/50">{it.source}</td>
                  <td className="py-1 w-8 text-right">
                    <button onClick={() => { sjRemove(it.ticker, it.date); force(n => n + 1) }}
                      className="text-md-on-surface-var/40 hover:text-rose-400" title="remove">✕</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  )
}
