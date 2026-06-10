import { useEffect, useState } from 'react'
import { pwlAdd } from './PersonalWatchlistPanel'
import { downloadTV } from '../utils/setupsJournal'

const ATOM_CLS = {
  'close=O': 'bg-sky-900/50 text-sky-200 border-sky-700/50',
  'gap': 'bg-violet-900/50 text-violet-200 border-violet-700/50',
  'R2L': 'bg-emerald-900/50 text-emerald-200 border-emerald-700/50',
  'EO': 'bg-amber-900/40 text-amber-200 border-amber-700/50',
  'vol=B': 'bg-teal-900/50 text-teal-200 border-teal-700/50',
  'wick=D': 'bg-rose-900/40 text-rose-200 border-rose-700/40',
  'G3': 'bg-fuchsia-900/50 text-fuchsia-200 border-fuchsia-700/50',
}
const REG_CLS = { RISK_ON: 'bg-emerald-900/50 text-emerald-200 border-emerald-600',
  NEUTRAL: 'bg-amber-900/40 text-amber-200 border-amber-700/50',
  RISK_OFF: 'bg-rose-900/50 text-rose-200 border-rose-600' }
const scoreCls = (s) => s >= 90 ? 'text-emerald-300' : s >= 70 ? 'text-lime-300' : s >= 55 ? 'text-amber-300' : 'text-md-on-surface-var'

export default function AtomicScanPanel({ onSelectTicker }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)
  const [uni, setUni] = useState('all')
  const [minScore, setMinScore] = useState(55)
  const [live, setLive] = useState({})
  const [liveLoading, setLiveLoading] = useState(false)
  const [msg, setMsg] = useState('')

  const fetchLive = (tks) => {
    if (!tks.length) return
    setLiveLoading(true)
    fetch(`/api/live-prices?tickers=${tks.join(',')}`).then(r => r.json())
      .then(d => setLive(d.prices || {})).catch(() => {}).finally(() => setLiveLoading(false))
  }
  useEffect(() => {
    let dead = false; setLoading(true); setErr(null); setLive({})
    fetch('/api/atomic-scan?max_age_days=4').then(r => r.json()).then(d => {
      if (dead) return
      if (d.error) { setErr(d.error); return }
      setData(d); fetchLive((d.rows || []).map(r => r.ticker).slice(0, 250))
    }).catch(e => { if (!dead) setErr(String(e)) }).finally(() => { if (!dead) setLoading(false) })
    return () => { dead = true }
  }, [])

  const all = data?.rows || []
  const rows = all.filter(r => (uni === 'all' || r.universe === uni) && r.score >= minScore)
  const reg = data?.regime
  const addAll = () => { rows.forEach(r => pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.close, tz_sig: 'atomic:' + r.atoms.join('+') })); setMsg(`★ ${rows.length} → Watchlist`); setTimeout(() => setMsg(''), 2500) }

  return (
    <div className="p-4 text-md-on-surface">
      <h1 className="text-base font-bold mb-1">⚛️ Atomic Scan — weak-close gap-up</h1>
      <p className="text-[11px] text-md-on-surface-var mb-2">
        The 5-year-validated atomic edge: a <b>bull T-signal that closes WEAK</b> (close below prior body) on a
        <b> gap-up</b> bar. Backtest +0.84 (sp500) / +0.70 (r2k) expectancy, <b>positive 5/6 years</b> (only 2022 bear −).
        Score = corroborating atoms (R2L oversold · EO escape · vol=B · wick=D · G3). Click ticker → chart.
      </p>
      {reg && (
        <div className={`inline-flex items-center gap-2 px-3 py-1 rounded border text-xs mb-3 ${REG_CLS[reg.label] || ''}`}>
          <b>REGIME: {reg.label}</b>
          <span className="opacity-80">score {reg.score} · size ×{reg.conv_mult}</span>
          {reg.label === 'RISK_OFF' && <span className="opacity-90">— ⚠ this edge loses in bear tape; small size / watch only</span>}
        </div>
      )}
      <div className="flex items-center gap-2 text-xs mb-3 flex-wrap">
        {['all', 'sp500', 'nasdaq', 'russell2k'].map(u => (
          <button key={u} onClick={() => setUni(u)}
            className={`px-2 py-0.5 rounded border ${uni === u ? 'bg-md-surface-high border-white/30 text-white' : 'bg-md-surface border-white/10 hover:text-white'}`}>{u}</button>
        ))}
        <span className="text-md-on-surface-var/60 ml-2">min score:</span>
        {[55, 70, 90].map(s => (
          <button key={s} onClick={() => setMinScore(s)}
            className={`px-2 py-0.5 rounded border ${minScore === s ? 'bg-sky-900/60 text-sky-200 border-sky-500' : 'bg-md-surface border-white/10 hover:text-white'}`}>{s}+</button>
        ))}
        {loading && <span className="text-sky-400 animate-pulse">scanning…</span>}
        {data && <span className="text-md-on-surface-var/60">{rows.length} / {all.length} · {data.as_of}</span>}
        <button onClick={() => fetchLive(rows.map(r => r.ticker).slice(0, 250))} disabled={!rows.length || liveLoading}
          className="px-2 py-0.5 rounded border border-sky-700/50 text-sky-300 hover:bg-sky-900/30 disabled:opacity-40">{liveLoading ? '↻ live…' : '↻ live'}</button>
        <button onClick={() => downloadTV(`atomic_${data?.as_of || 'scan'}.txt`, [{ name: `Atomic weak-close gap-up ${data?.as_of || ''}`.trim(), tickers: rows.map(r => r.ticker) }])}
          disabled={!rows.length} className="px-2 py-0.5 rounded border border-sky-700/50 text-sky-300 hover:bg-sky-900/30 disabled:opacity-40" title="TradingView .txt">⬇ TV .txt</button>
        <button onClick={addAll} disabled={!rows.length}
          className="px-2 py-0.5 rounded border border-emerald-700/50 text-emerald-300 hover:bg-emerald-900/30 disabled:opacity-40">★ all → Watchlist</button>
        {msg && <span className="text-emerald-400 text-[11px]">{msg}</span>}
      </div>
      {err && <div className="text-rose-400 text-xs mb-2">error: {err}</div>}
      <table className="w-full text-xs border border-white/10 rounded overflow-hidden">
        <thead className="bg-md-surface-high text-md-on-surface-var">
          <tr>
            <th className="text-right px-2 py-1.5">score</th>
            <th className="text-left px-2 py-1.5">ticker</th>
            <th className="text-left px-2 py-1.5">univ</th>
            <th className="text-left px-2 py-1.5">sig</th>
            <th className="text-right px-2 py-1.5">close</th>
            <th className="text-right px-2 py-1.5">live</th>
            <th className="text-right px-2 py-1.5">rsi</th>
            <th className="text-left px-3 py-1.5">atoms</th>
            <th className="text-right px-2 py-1.5">$vol</th>
            <th className="text-right px-2 py-1.5">age</th>
            <th className="text-center px-2 py-1.5"></th>
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.ticker} className="border-t border-white/5 hover:bg-white/[0.03]">
              <td className={`text-right px-2 py-1.5 font-mono font-bold ${scoreCls(r.score)}`}>{r.score}</td>
              <td className="px-2 py-1.5"><button onClick={() => onSelectTicker?.(r.ticker)} className="font-mono font-semibold hover:text-sky-300">{r.ticker}</button></td>
              <td className="px-2 py-1.5 text-[10px] text-md-on-surface-var/70">{r.universe}</td>
              <td className="px-2 py-1.5 font-mono text-[10px] text-sky-300/80">{r.t_sig}</td>
              <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var">{r.close != null ? '$' + r.close : '—'}</td>
              <td className="text-right px-2 py-1.5 font-mono">
                {live[r.ticker] ? <span>${live[r.ticker].price}{live[r.ticker].change_pct != null && <span className={`ml-1 text-[10px] ${live[r.ticker].change_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{live[r.ticker].change_pct >= 0 ? '+' : ''}{live[r.ticker].change_pct}%</span>}</span> : <span className="text-md-on-surface-var/30">{liveLoading ? '…' : '—'}</span>}
              </td>
              <td className={`text-right px-2 py-1.5 font-mono ${r.rsi != null && r.rsi <= 35 ? 'text-emerald-300' : 'text-md-on-surface-var'}`}>{r.rsi ?? '—'}</td>
              <td className="px-3 py-1.5">
                <div className="flex flex-wrap gap-1">
                  {r.atoms.map((a, i) => <span key={i} className={`text-[9px] font-mono px-1 rounded border ${ATOM_CLS[a] || 'border-white/10'}`}>{a}</span>)}
                </div>
              </td>
              <td className="text-right px-2 py-1.5 font-mono text-md-on-surface-var/70">{r.dv_m != null ? r.dv_m + 'M' : '—'}</td>
              <td className="text-right px-2 py-1.5 text-md-on-surface-var/60 font-mono">{r.age_days}d</td>
              <td className="px-2 py-1.5 text-center">
                <button onClick={() => { pwlAdd({ ticker: r.ticker, _tf: '1d', last_price: r.close, tz_sig: 'atomic' }); }} title="Add to Watchlist"
                  className="px-1 rounded border border-emerald-700/40 text-[10px] text-emerald-300 hover:bg-emerald-900/30">★</button>
              </td>
            </tr>
          ))}
          {!loading && !rows.length && <tr><td colSpan={11} className="px-3 py-4 text-center text-md-on-surface-var/50">no weak-close gap-up candidates ≥ score {minScore}</td></tr>}
        </tbody>
      </table>
      <p className="text-[10px] text-md-on-surface-var/50 mt-2">
        Entry rule (backtested): next-bar open · −15% stop / +100% target · small fractional size · stand down in RISK_OFF.
        atoms: close=O (weak close) + gap = base; R2L = oversold RSI2; EO = escaped range + weak close; vol=B = controlled volume; wick=D = lower wick; G3 = large gap.
        Swing-grade edge, regime-dependent — paper-track first.
      </p>
    </div>
  )
}
