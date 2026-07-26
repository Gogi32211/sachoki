import { useState, useEffect, useCallback } from 'react'

// Live scanner for the time-robust 5-yr sequence rule-database (/api/seq-scan).
// Surfaces tickers whose trailing 2/3/4-bar TZ+L+ULTRA sequence matches a ROBUST rule.
export default function SeqRulesPanel({ onSelectTicker }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')
  const [age, setAge] = useState(2)
  const [minScore, setMinScore] = useState(0)
  const [depthF, setDepthF] = useState(0)   // 0=all, 2/3/4
  const [live, setLive] = useState(false)
  const [tf, setTf] = useState('1d')
  const [quotes, setQuotes] = useState({})   // ticker → {price, change_pct} live feed
  const [fires, setFires] = useState({})     // ticker → [{edge, date}] parallel EDGE-setup fires
  const [sortKey, setSortKey] = useState('score')
  const [sortDir, setSortDir] = useState('desc')
  useEffect(() => { if (tf !== '1d') setTf('1d') }, [])   // intraday disabled until TV-aligned

  const load = useCallback(() => {
    setLoading(true); setErr('')
    const url = live ? `/api/seq-scan-live` : `/api/seq-scan?max_age_days=${age}&tf=${tf}`
    fetch(url)
      .then(r => r.json())
      .then(d => {
        if (d.error) setErr(d.error); setData(d)
        const tks = [...new Set((d.rows || []).map(r => r.ticker))].slice(0, 250)
        if (tks.length) {
          fetch(`/api/live-prices?tickers=${tks.join(',')}`).then(r => r.json())
            .then(p => setQuotes(prev => ({ ...prev, ...(p.prices || {}) }))).catch(() => {})
          fetch(`/api/edge-fires?tickers=${tks.join(',')}`).then(r => r.json())
            .then(f => setFires(f.fires || {})).catch(() => {})
        }
      })
      .catch(e => setErr(String(e)))
      .finally(() => setLoading(false))
  }, [age, live, tf])
  useEffect(() => { load() }, [load])

  // sortable columns — key drives both the header cell and the sort accessor
  const COLS = [
    { key: 'score', label: 'score', align: 'right' },
    { key: 'ticker', label: 'ticker', align: 'left' },
    { key: 'seq', label: 'sequence (bars → entry)', align: 'left' },
    { key: 'med20', label: 'med20', align: 'right', title: 'OOS (2024-26) 20-bar forward median %' },
    { key: 'ps_med', label: 'ps med', align: 'right', title: 'OOS path-sim trail25 median % — the tradeable number' },
    { key: 'win', label: 'win%', align: 'right', title: 'OOS path-sim win %' },
    { key: 'yrs', label: 'yrs+', align: 'right', title: 'positive TEST years (2024-26)' },
    { key: 'y2026', label: '2026', align: 'right' },
    { key: 'dsr', label: 'DSR', align: 'right', title: 'Deflated Sharpe Ratio — P(edge is real) after deflating vs all 2,371 mined candidates' },
    { key: 'close', label: 'close', align: 'right' },
    { key: 'live', label: 'live', align: 'right', title: 'live price + today\'s % change (same feed as the Edge board)' },
    { key: 'rsi', label: 'rsi', align: 'right' },
    { key: 'edge', label: 'EDGE', align: 'left', title: 'parallel EDGE-setup fires on this ticker (validated Edge-board scans, ≤10min cache) — independent STATE confirmation of the sequence SHAPE' },
    { key: 'date', label: 'date', align: 'right' },
  ]
  const sortVal = (r, key) => {
    switch (key) {
      case 'score': return r.score
      case 'ticker': return r.ticker
      case 'seq': return r.seq
      case 'med20': return r.med20
      case 'ps_med': return r.ps_med
      case 'win': return r.win
      case 'yrs': return r.n_yrs ? r.pos_yrs / r.n_yrs : null
      case 'y2026': return r.y2026
      case 'dsr': return r.dsr
      case 'close': return r.close ?? r.live_price
      case 'live': return quotes[r.ticker]?.change_pct
      case 'rsi': return r.rsi
      case 'edge': return (fires[r.ticker] || []).length
      case 'date': return r.signal_date || ''
      default: return null
    }
  }
  const toggleSort = (key) => {
    if (sortKey === key) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else { setSortKey(key); setSortDir(key === 'ticker' || key === 'seq' ? 'asc' : 'desc') }
  }
  const rows = (data?.rows || [])
    .filter(r => r.score >= minScore && (depthF === 0 || r.depth === depthF))
    .sort((a, b) => {
      const va = sortVal(a, sortKey), vb = sortVal(b, sortKey)
      const na = va == null || va === '' || Number.isNaN(va)
      const nb = vb == null || vb === '' || Number.isNaN(vb)
      if (na && nb) return 0
      if (na) return 1                       // nulls always sink to the bottom
      if (nb) return -1
      const c = typeof va === 'string' ? va.localeCompare(vb) : va - vb
      return sortDir === 'asc' ? c : -c
    })
  const scoreCls = s => s >= 85 ? 'text-emerald-300' : s >= 75 ? 'text-teal-300' : s >= 65 ? 'text-yellow-300' : 'text-slate-400'

  return (
    <div className="p-4 text-slate-200 max-w-6xl">
      <h2 className="text-xl font-bold text-emerald-300">🧬 Robust Sequences</h2>
      <p className="text-sm text-slate-400 mt-1 mb-3">
        ცოცხალი scanner — tickers რომელთა ბოლო 2/3/4-ბარიანი TZ+L+ULTRA თანმიმდევრობა ემთხვევა{' '}
        <b className="text-emerald-300">frozen-OOS ვერიფიცირებულ წესს</b>: წესები მოძიებულია <b>მხოლოდ 2021-23</b>-ზე
        და დამოწმებულია <b>2024-26</b>-ზე (path-sim trail25, stop-first). med20/win/ps = <b>OOS</b> (ტესტ-პერიოდის) ციფრები.
        🏆 = DSR≥0.6 (selection-luck-გამძლე ყველა 2,371 საცდელის წინააღმდეგ).
        {data && <span className="text-slate-500"> · {data.n_rules} OOS✓ rules · as_of {data.as_of}</span>}
      </p>

      <div className="flex flex-wrap items-center gap-2 mb-3 text-sm">
        <span className="text-slate-500">TF:</span>
        {['1d', '4h', '1h'].map(x => {
          const disabledTf = false   // 4h/1h re-enabled 2026-07-03: 15m-base derive, session-anchored (TV-aligned), full 3203-ticker rebuild
          return (
            <button key={x} disabled={live || disabledTf}
              onClick={() => !disabledTf && setTf(x)}
              title={x === '1d' ? 'daily patterns (verified, matches TV)' : `${x} intraday — session-anchored bars derived from the 15m base (TV-aligned), rules from the full 5yr ${x} DB`}
              className={`px-2 py-0.5 rounded border ${(live || disabledTf) ? 'opacity-40 border-slate-800 text-slate-600 cursor-not-allowed' : tf === x ? 'bg-violet-700/40 border-violet-500 text-white' : 'border-slate-700 text-slate-400'}`}>
              {x}{disabledTf ? ' ⚠' : ''}
            </button>
          )
        })}
        <span className="text-slate-600 mx-1">|</span>
        <button onClick={() => setLive(v => !v)} title="TODAY-0: today's live forming bar (current price → engine signals) completing a robust sequence. Provisional — updates as price moves; only works while the regular session is open. (1d only)"
          className={`px-2 py-0.5 rounded border font-semibold ${live ? 'bg-red-900/60 border-red-500 text-red-200 animate-pulse' : 'border-slate-700 text-slate-400 hover:text-white'}`}>
          {live ? '🔴 LIVE today' : '○ LIVE today'}
        </button>
        <span className="text-slate-600 mx-1">|</span>
        <span className={`text-slate-500 ${live ? 'opacity-40' : ''}`}>freshness:</span>
        {[1, 2, 4, 10].map(a => (
          <button key={a} disabled={live} onClick={() => setAge(a)} className={`px-2 py-0.5 rounded border ${live ? 'opacity-40 border-slate-800 text-slate-600' : age === a ? 'bg-emerald-700/40 border-emerald-500 text-white' : 'border-slate-700 text-slate-400'}`}>{a}d</button>
        ))}
        <span className="text-slate-600 mx-1">|</span>
        <span className="text-slate-500">depth:</span>
        {[0, 2, 3, 4].map(d => (
          <button key={d} onClick={() => setDepthF(d)} className={`px-2 py-0.5 rounded border ${depthF === d ? 'bg-teal-700/40 border-teal-500 text-white' : 'border-slate-700 text-slate-400'}`}>{d === 0 ? 'all' : d + '-bar'}</button>
        ))}
        <span className="text-slate-600 mx-1">|</span>
        <span className="text-slate-500">min score:</span>
        {[0, 70, 80].map(s => (
          <button key={s} onClick={() => setMinScore(s)} className={`px-2 py-0.5 rounded border ${minScore === s ? 'bg-slate-700 border-slate-500 text-white' : 'border-slate-700 text-slate-400'}`}>{s || 'all'}</button>
        ))}
        <button onClick={load} disabled={loading} className="ml-2 px-3 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-semibold disabled:opacity-50">{loading ? '…' : '↻'}</button>
        {data && <span className="text-slate-500">{rows.length} / {data.count}</span>}
      </div>

      {err && <div className="text-red-400 text-sm mb-2">⚠ {err}</div>}
      {live && data && !data.live && data.session_open && (
        <div className="text-amber-300/90 text-sm mb-2 rounded border border-amber-700/40 bg-amber-900/10 px-3 py-1.5">
          🟡 ბირჟა <b>ღიაა</b>, magram live-feed დროებით ვერ მოვიდა (MASSIVE) — დააჭირე ↻. ↓ ქვემოთ <b>ბოლო დასრულებული ბარია</b>. <span className="text-slate-500">({data.n_candidates} near-completion candidate)</span>
        </div>
      )}
      {live && data && !data.live && !data.session_open && (
        <div className="text-slate-300 text-sm mb-2 rounded border border-slate-700/40 bg-slate-900/30 px-3 py-1.5">
          🌙 ბირჟა დახურულია — ↓ ქვემოთ <b>ბოლო დასრულებული ბარის</b> scan. live-რეჟიმი ბირჟის გახსნისას აღდგება.
        </div>
      )}
      {live && data?.live && (
        <div className="text-red-300 text-sm mb-2">🔴 LIVE · {data.candidates_checked}/{data.n_candidates} candidate with live bar · {data.count} completing now (provisional)</div>
      )}

      <div className="overflow-x-auto rounded border border-slate-800">
        <table className="w-full text-sm font-mono">
          <thead className="bg-slate-900/70 text-slate-400 text-left">
            <tr>
              {COLS.map(c => (
                <th key={c.key} onClick={() => toggleSort(c.key)} title={(c.title ? c.title + ' · ' : '') + 'click to sort'}
                  className={`px-2 py-1.5 cursor-pointer select-none hover:text-slate-200 whitespace-nowrap ${c.align === 'right' ? 'text-right' : ''} ${sortKey === c.key ? 'text-emerald-300' : ''}`}>
                  {c.label}{sortKey === c.key ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="border-t border-slate-800 hover:bg-slate-800/50">
                <td className={`px-2 py-1.5 text-right font-bold ${scoreCls(r.score)}`}>{r.score}</td>
                <td className="px-2 py-1.5"><button onClick={() => onSelectTicker?.(r.ticker)} className="font-semibold hover:text-sky-300">{r.ticker}</button>
                  <span className="text-slate-600 text-[10px] ml-1">{r.universe?.replace('russell2k', 'r2k').replace('nasdaq', 'nq').replace('sp500', 'sp')}</span></td>
                <td className="px-2 py-1.5 text-teal-200/90">
                  {r.dsr != null && r.dsr >= 0.6 && <span className="mr-1" title={`🏆 DSR ${r.dsr.toFixed(2)} — selection-proof: survives deflation vs all 2,371 mined candidates`}>🏆</span>}
                  {r.seq} <span className="text-slate-600">({r.depth}b)</span>
                  {r.d_ctx && <span className="ml-1 text-orange-400" title="D (breakdown/distribution) in the window — validated booster, strongest on oversold-reversal sequences (+5pp)">🔻{r.d_label || 'D'}</span>}
                  {r.p_ctx && <span className="ml-1 text-sky-400" title="P (EMA-cross momentum) in the window — broad validated booster (+0.40pp on 62% of robust sequences)">📈{r.p_label || 'P'}</span>}
                </td>
                <td className={`px-2 py-1.5 text-right ${r.med20 >= 0 ? 'text-emerald-300' : 'text-red-400'}`}>{r.med20 >= 0 ? '+' : ''}{r.med20}</td>
                <td className={`px-2 py-1.5 text-right font-semibold ${r.ps_med == null ? 'text-slate-600' : r.ps_med >= 0 ? 'text-emerald-300' : 'text-red-400'}`}>{r.ps_med != null ? (r.ps_med >= 0 ? '+' : '') + r.ps_med : '·'}</td>
                <td className="px-2 py-1.5 text-right text-slate-300">{r.win}</td>
                <td className="px-2 py-1.5 text-right text-slate-400" title={r.yrs_detail ? 'TEST per-year med20: ' + Object.entries(r.yrs_detail).map(([y, v]) => `${y}: ${v > 0 ? '+' : ''}${v}%`).join(' · ') : undefined}>{r.pos_yrs}/{r.n_yrs}</td>
                <td className={`px-2 py-1.5 text-right ${r.y2026 > 0 ? 'text-emerald-400' : r.y2026 < 0 ? 'text-red-400' : 'text-slate-600'}`}>{r.y2026 != null ? (r.y2026 > 0 ? '+' : '') + r.y2026 : '·'}</td>
                <td className={`px-2 py-1.5 text-right ${r.dsr == null ? 'text-slate-600' : r.dsr >= 0.6 ? 'text-amber-300 font-bold' : r.dsr >= 0.3 ? 'text-slate-300' : 'text-slate-500'}`}>{r.dsr != null ? r.dsr.toFixed(2) : '·'}</td>
                <td className="px-2 py-1.5 text-right text-slate-400">${r.close ?? r.live_price}{r.live_token ? <span className="text-red-400"> ●</span> : ''}</td>
                <td className="px-2 py-1.5 text-right whitespace-nowrap">
                  {quotes[r.ticker]?.change_pct != null
                    ? <span className={quotes[r.ticker].change_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}>{quotes[r.ticker].change_pct >= 0 ? '+' : ''}{quotes[r.ticker].change_pct}%</span>
                    : <span className="text-slate-600">·</span>}
                </td>
                <td className={`px-2 py-1.5 text-right ${r.rsi != null && r.rsi < 40 ? 'text-emerald-300' : 'text-slate-500'}`}>{r.rsi ?? '·'}</td>
                <td className="px-2 py-1.5 whitespace-nowrap">
                  {(fires[r.ticker] || []).map(f => (
                    <span key={f.edge} title={`${f.edge} fired ${f.date}`}
                      className="inline-block mr-1 px-1 py-px rounded border border-amber-700/40 bg-amber-900/20 text-amber-300 text-[10px]">
                      {f.edge}<span className="text-amber-500/70 ml-0.5">{(f.date || '').slice(5)}</span>
                    </span>
                  ))}
                  {!(fires[r.ticker] || []).length && <span className="text-slate-700">·</span>}
                </td>
                <td className="px-2 py-1.5 text-right text-slate-500 whitespace-nowrap">{r.signal_date ?? <span className="text-red-400">live:{r.live_token}</span>}</td>
              </tr>
            ))}
            {!loading && rows.length === 0 && <tr><td colSpan={14} className="px-2 py-4 text-center text-slate-500">no matches</td></tr>}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-slate-500 mt-1">{data?.edge_note}</p>
    </div>
  )
}
