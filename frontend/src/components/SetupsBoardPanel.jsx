import { useEffect, useState } from 'react'
import { badgeFor, descFor } from '../utils/signalDesc'
import { pwlAdd } from './PersonalWatchlistPanel'

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

  useEffect(() => {
    let dead = false
    setLoading(true); setErr(null)
    const q = new URLSearchParams({ zone_def: zoneDef, max_age_days: '20', min_oos: '55' })
    fetch(`/api/zone-events/board?${q}`)
      .then(r => r.json())
      .then(d => { if (!dead) { d.error ? setErr(d.error) : setData(d) } })
      .catch(e => { if (!dead) setErr(String(e)) })
      .finally(() => { if (!dead) setLoading(false) })
    return () => { dead = true }
  }, [zoneDef])

  const rows = data?.rows || []
  const addAll = () => {
    rows.forEach(r => pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.last_price, tz_sig: r.sequence }))
    setWlMsg(`added ${rows.length} → Watchlist`); setTimeout(() => setWlMsg(''), 2500)
  }

  return (
    <div className="p-4 text-md-on-surface">
      <h1 className="text-base font-bold mb-1">📋 Setups Board — sequence-matched tickers</h1>
      <p className="text-[11px] text-md-on-surface-var mb-3">
        Recent (≤20d) tickers whose zone-exit built an <b>OOS-validated lead-in sequence</b>. <b>Prob↑</b> = the
        sequence's out-of-sample win-rate (probability the move follows through). <b>Score</b> blends prob + edge +
        recency. Hover <b>Why</b> for the reasoning. Click a ticker → chart.
      </p>
      <div className="flex items-center gap-3 text-xs mb-3">
        <span className="text-md-on-surface-var/60">zone:</span>
        {ZONES.map(([z, lbl]) => (
          <button key={z} onClick={() => setZoneDef(z)}
            className={`px-2 py-0.5 rounded border ${zoneDef === z ? 'bg-rose-900/50 text-rose-200 border-rose-600' : 'bg-md-surface border-white/10 hover:text-white'}`}>{lbl}</button>
        ))}
        {loading && <span className="text-sky-400 animate-pulse">scoring…</span>}
        {data && <span className="text-md-on-surface-var/60">{data.count} setups · as of {data.as_of}</span>}
        <button onClick={addAll} disabled={!rows.length}
          className="ml-auto px-2 py-0.5 rounded border border-emerald-700/50 text-emerald-300 hover:bg-emerald-900/30 disabled:opacity-40">★ all → Watchlist</button>
        {wlMsg && <span className="text-emerald-400 text-[11px]">{wlMsg}</span>}
      </div>
      {err && <div className="text-rose-400 text-xs mb-2">error: {err}</div>}
      <table className="w-full text-xs border border-white/10 rounded overflow-hidden">
        <thead className="bg-md-surface-high text-md-on-surface-var">
          <tr>
            <th className="text-right px-2 py-1.5">score</th>
            <th className="text-left px-2 py-1.5">ticker</th>
            <th className="text-right px-2 py-1.5">price</th>
            <th className="text-right px-2 py-1.5" title="OOS win-rate of the matched sequence">prob↑</th>
            <th className="text-left px-3 py-1.5">sequence used</th>
            <th className="text-right px-2 py-1.5">rsi</th>
            <th className="text-left px-2 py-1.5">univ</th>
            <th className="text-left px-2 py-1.5">journal</th>
            <th className="text-right px-2 py-1.5">age</th>
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
              <td className="text-right px-2 py-1.5 font-mono">{r.last_price != null ? '$' + r.last_price : '—'}</td>
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
              <td className="text-right px-2 py-1.5 text-md-on-surface-var/60 font-mono">{r.age_days}d</td>
            </tr>
          ))}
          {!loading && !rows.length && (
            <tr><td colSpan={9} className="px-3 py-4 text-center text-md-on-surface-var/50">no holding-sequence setups in the last 20d</td></tr>
          )}
        </tbody>
      </table>
      <p className="text-[10px] text-md-on-surface-var/50 mt-2">
        Prob↑ is the sequence's historical OOS win-rate — a base rate, not a guarantee. exit↓ "win" = price UP after a
        failed breakdown (spring). Price = last DB close ({data?.as_of}). Journal column shows positions already opened/pending.
      </p>
    </div>
  )
}
