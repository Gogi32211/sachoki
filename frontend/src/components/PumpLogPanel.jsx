import { useState, useCallback, useMemo, useEffect } from 'react'

// Parse paste like "CAST +159%, CUPR +144%" or one-per-line
function parsePaste(text) {
  const entries = []
  const lines = text.split(/[\n,]+/).map(s => s.trim()).filter(Boolean)
  for (const line of lines) {
    const m = line.match(/^([A-Z]{1,6})\s*[+-]?([\d.]+)%?/i)
    if (m) entries.push({ ticker: m[1].toUpperCase(), pump: parseFloat(m[2]) })
    else {
      const sym = line.match(/^([A-Z]{1,6})$/i)
      if (sym) entries.push({ ticker: sym[1].toUpperCase(), pump: null })
    }
  }
  return entries
}

const PCT_COLOR = pct =>
  pct >= 80 ? 'text-emerald-400 font-bold' :
  pct >= 60 ? 'text-yellow-300 font-semibold' :
  pct >= 40 ? 'text-orange-300' : 'text-white/50'

const PCT_BAR = pct => (
  <div className="w-16 h-1.5 bg-white/10 rounded-full inline-block align-middle mr-1">
    <div className="h-full rounded-full"
      style={{ width: `${pct}%`,
        background: pct >= 80 ? '#34d399' : pct >= 60 ? '#fbbf24' : pct >= 40 ? '#fb923c' : '#6b7280' }} />
  </div>
)

// Friendly label for sig column names
const SIG_LABEL = s => s.replace(/^sig_/, '').replace(/_/g, ' ')

export default function PumpLogPanel() {
  const [paste, setPaste]         = useState('')
  const [rows, setRows]           = useState([])
  const [freq, setFreq]           = useState([])
  const [meta, setMeta]           = useState(null)  // {n_tickers, sig_cols_total}
  const [loading, setLoading]     = useState(false)
  const [notes, setNotes]         = useState('')
  const [logStatus, setLogStatus] = useState(null)
  const [mdContent, setMdContent] = useState(null)
  const [mdLoading, setMdLoading] = useState(false)
  const [freqThresh, setFreqThresh] = useState(50)  // show signals present in ≥N% of tickers
  const [expandTicker, setExpandTicker] = useState(null)
  const [tab, setTab]             = useState('screener') // 'screener' | 'freq' | 'tickers' | 'log'
  // Live pump screener
  const [screener, setScreener]     = useState([])
  const [scrLoading, setScrLoading] = useState(false)
  const [scrUniverse, setScrUniverse] = useState('nasdaq')
  const [scrMinScore, setScrMinScore] = useState(9)
  const [scrMaxScore, setScrMaxScore] = useState(14)
  const [scrMaxPrice, setScrMaxPrice] = useState(10)

  const handleAnalyze = useCallback(async () => {
    const entries = parsePaste(paste)
    if (!entries.length) return
    setLoading(true); setRows([]); setFreq([]); setLogStatus(null)
    try {
      const pumps = {}
      entries.forEach(e => { if (e.pump != null) pumps[e.ticker] = e.pump })
      const res = await fetch('/api/studio/pump/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tickers: entries.map(e => e.ticker), pumps, window: 14 }),
      })
      const d = await res.json()
      setRows(d.rows || [])
      setFreq(d.freq || [])
      setMeta({ n_tickers: d.n_tickers, sig_cols_total: d.sig_cols_total })
      setTab('freq')
    } finally { setLoading(false) }
  }, [paste])

  const handleLog = useCallback(async () => {
    if (!rows.length) return
    const entries = parsePaste(paste)
    const pumps = {}
    entries.forEach(e => { if (e.pump != null) pumps[e.ticker] = e.pump })
    const today = new Date().toISOString().slice(0, 10)
    const res = await fetch('/api/studio/pump/log', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ date: today, tickers: entries.map(e => e.ticker), pumps, rows, notes }),
    })
    setLogStatus(res.ok ? 'ok' : 'err')
  }, [rows, paste, notes])

  const handleScreener = useCallback(async () => {
    setScrLoading(true); setScreener([])
    try {
      const url = `/api/studio/pump-screener?universe=${scrUniverse}&min_score=${scrMinScore}&max_score=${scrMaxScore}&max_price=${scrMaxPrice}&limit=50`
      const res = await fetch(url)
      const d   = await res.json()
      setScreener(d.results || [])
    } finally { setScrLoading(false) }
  }, [scrUniverse, scrMinScore, scrMaxScore, scrMaxPrice])

  // Auto-load screener on first visit
  useEffect(() => {
    if (tab === 'screener' && screener.length === 0 && !scrLoading) handleScreener()
  }, [tab]) // eslint-disable-line

  const handleShowMd = useCallback(async () => {
    setMdLoading(true)
    try {
      const res = await fetch('/api/studio/pump/md')
      const d = await res.json()
      setMdContent(d.content || '')
      setTab('log')
    } finally { setMdLoading(false) }
  }, [])

  const okRows   = rows.filter(r => r.status === 'ok')
  const notFound = rows.filter(r => r.status === 'not_in_db').map(r => r.ticker)

  const filteredFreq = useMemo(
    () => freq.filter(f => f.pct >= freqThresh),
    [freq, freqThresh]
  )

  // Per-ticker expanded signal view
  const expandedRow = expandTicker ? okRows.find(r => r.ticker === expandTicker) : null

  return (
    <div className="flex flex-col gap-3 p-4 text-sm text-md-on-surface min-h-0">

      {/* ── Header ── */}
      <div className="flex items-center gap-3 flex-wrap">
        <h2 className="text-base font-semibold text-white">🔥 Pump Research</h2>
        <span className="text-xs text-md-on-surface-var">
          paste gainers → analyze ALL signals → discover patterns
        </span>
        <button onClick={handleShowMd} disabled={mdLoading}
          className="ml-auto px-2 py-0.5 rounded text-xs border border-white/10 hover:bg-white/10 text-md-on-surface-var">
          {mdLoading ? '…' : '📄 Show Log'}
        </button>
      </div>

      {/* ── Input row ── */}
      <div className="flex gap-3 items-start">
        <div className="flex-1">
          <div className="text-xs text-md-on-surface-var mb-1">
            Paste tickers — <span className="text-white/40">CAST +159%, CUPR +144%…</span>
          </div>
          <textarea
            value={paste} onChange={e => setPaste(e.target.value)}
            className="w-full h-20 bg-md-surface-var border border-md-outline-var rounded px-2 py-1.5 text-xs font-mono text-white resize-none"
            placeholder="CAST +159%, CUPR +144%, VSME +104%, PAVS +97%, QTEX +65%,&#10;AHMA +50%, HQ +48%, HUBC +45%, GPUS +42%, SDOT +40%" />
        </div>
        <div className="flex flex-col gap-2 pt-5">
          <button onClick={handleAnalyze} disabled={loading || !paste.trim()}
            className="px-4 py-1.5 rounded bg-orange-700 hover:bg-orange-600 disabled:opacity-40 text-white text-xs font-semibold whitespace-nowrap">
            {loading ? '⏳ Analyzing…' : '🔍 Analyze'}
          </button>
          <button onClick={handleLog} disabled={!okRows.length}
            className="px-4 py-1.5 rounded bg-emerald-800 hover:bg-emerald-700 disabled:opacity-40 text-white text-xs font-semibold whitespace-nowrap">
            💾 Log Session
          </button>
        </div>
      </div>

      {notFound.length > 0 && (
        <div className="text-xs text-yellow-400">Not in DB: {notFound.join(', ')}</div>
      )}
      {logStatus === 'ok' && <div className="text-xs text-emerald-400">✅ Logged to PUMP_RESEARCH.md</div>}
      {logStatus === 'err' && <div className="text-xs text-red-400">❌ Log failed</div>}

      {/* ── Tabs ── */}
      <div className="flex gap-1 border-b border-white/10 pb-0">
          {[
            { id: 'screener', label: `⚡ Screener${screener.length ? ` (${screener.length})` : ''}` },
            ...(freq.length > 0 ? [
              { id: 'freq',    label: `📊 Freq${meta ? ` (${meta.sig_cols_total})` : ''}` },
              { id: 'tickers', label: `🔎 Tickers (${okRows.length})` },
            ] : []),
            ...(mdContent !== null ? [{ id: 'log', label: '📄 Log' }] : []),
          ].map(t => (
            <button key={t.id} onClick={() => setTab(t.id)}
              className={`px-3 py-1 text-xs rounded-t border-b-2 -mb-px transition-colors ${
                tab === t.id
                  ? 'border-orange-400 text-white bg-md-surface-var'
                  : 'border-transparent text-md-on-surface-var hover:text-white'}`}>
              {t.label}
            </button>
          ))}
        </div>

      {/* ══ TAB: Live Pump Screener ══ */}
      {tab === 'screener' && (
        <div className="flex flex-col gap-3 min-h-0">
          {/* Controls */}
          <div className="flex items-center gap-3 flex-wrap text-xs text-md-on-surface-var">
            <select value={scrUniverse} onChange={e => setScrUniverse(e.target.value)}
              className="bg-md-surface-var border border-md-outline-var rounded px-1.5 py-0.5 text-white text-xs">
              <option value="nasdaq">NASDAQ</option>
              <option value="russell2k">Russell 2K</option>
              <option value="sp500">S&P 500</option>
            </select>
            <label className="flex items-center gap-1">Score
              <select value={scrMinScore} onChange={e => setScrMinScore(Number(e.target.value))}
                className="bg-md-surface-var border border-md-outline-var rounded px-1 py-0.5 text-white text-xs">
                {[4,5,6,7,8,9,10,12,14].map(v => <option key={v} value={v}>{v}</option>)}
              </select>
              –
              <select value={scrMaxScore} onChange={e => setScrMaxScore(Number(e.target.value))}
                className="bg-md-surface-var border border-md-outline-var rounded px-1 py-0.5 text-white text-xs">
                {[7,8,9,10,11,12,14,16,20,99].map(v => <option key={v} value={v}>{v === 99 ? 'any' : v}</option>)}
              </select>
            </label>
            <label className="flex items-center gap-1">Price ≤ $
              <select value={scrMaxPrice} onChange={e => setScrMaxPrice(Number(e.target.value))}
                className="bg-md-surface-var border border-md-outline-var rounded px-1 py-0.5 text-white text-xs">
                {[3,5,7,10,15,20,50].map(v => <option key={v} value={v}>{v}</option>)}
              </select>
            </label>
            <button onClick={handleScreener} disabled={scrLoading}
              className="px-3 py-0.5 rounded bg-orange-700 hover:bg-orange-600 disabled:opacity-40 text-white text-xs font-semibold">
              {scrLoading ? '⏳' : '🔄 Refresh'}
            </button>
            {screener.length > 0 && (
              <span className="text-slate-400">{screener.length} tickers — latest bar</span>
            )}
          </div>
          {/* Score legend */}
          <div className="text-xs text-slate-500 flex gap-4">
            <span><span className="text-green-400">★</span> score 6–10 sweet spot</span>
            <span><span className="text-orange-400">🔥</span> score&gt;10 high (late risk)</span>
            <span><span className="text-slate-400">·</span> score&lt;6 weak</span>
          </div>
          {/* Table */}
          {scrLoading ? (
            <div className="text-xs text-slate-400 animate-pulse">Scanning universe…</div>
          ) : screener.length === 0 ? (
            <div className="text-xs text-slate-500">No hits. Try lowering min_score or raising max_price.</div>
          ) : (
            <div className="overflow-auto flex-1 min-h-0">
              <table className="w-full text-xs border-separate border-spacing-0">
                <thead>
                  <tr className="text-left text-slate-400 sticky top-0 bg-md-bg z-10">
                    <th className="pb-1 pr-3 font-normal">Ticker</th>
                    <th className="pb-1 pr-2 font-normal">Total</th>
                    <th className="pb-1 pr-2 font-normal" title="setup (5d lookback)">S↺</th>
                    <th className="pb-1 pr-3 font-normal" title="trigger (today)">T!</th>
                    <th className="pb-1 pr-3 font-normal">Price</th>
                    <th className="pb-1 pr-3 font-normal">RSI</th>
                    <th className="pb-1 pr-3 font-normal">L-sig</th>
                    <th className="pb-1 pr-3 font-normal">VolRatio</th>
                    <th className="pb-1 font-normal">Signals fired</th>
                  </tr>
                </thead>
                <tbody>
                  {screener.map(r => {
                    const tier = r.score >= 10 ? 'text-orange-400' : r.score >= 6 ? 'text-yellow-300' : 'text-slate-400'
                    const icon = r.score >= 10 ? '🔥' : r.score >= 6 ? '⚡' : '·'
                    return (
                      <tr key={r.ticker} className="border-t border-white/5 hover:bg-white/5">
                        <td className="py-1 pr-3 font-mono font-semibold text-white">{r.ticker}</td>
                        <td className={`py-1 pr-2 font-semibold ${tier}`}>{icon}{r.score}</td>
                        <td className="py-1 pr-2 text-teal-400">{r.setup_score ?? '—'}</td>
                        <td className="py-1 pr-3 text-amber-400 font-semibold">{r.trigger_score ?? '—'}</td>
                        <td className="py-1 pr-3 text-slate-300">${r.close}</td>
                        <td className={`py-1 pr-3 ${r.rsi < 35 ? 'text-cyan-400' : r.rsi > 70 ? 'text-rose-400' : 'text-slate-300'}`}>
                          {r.rsi}
                        </td>
                        <td className={`py-1 pr-3 font-mono ${r.l_sig === 'L3' ? 'text-orange-300 font-semibold' : 'text-slate-400'}`}>
                          {r.l_sig}
                        </td>
                        <td className={`py-1 pr-3 ${(r.vol_ratio||0) >= 5 ? 'text-orange-300 font-semibold' : (r.vol_ratio||0) >= 2 ? 'text-yellow-300' : 'text-slate-400'}`}>
                          {r.vol_ratio != null ? `${r.vol_ratio}x` : '—'}
                        </td>
                        <td className="py-1 text-slate-400 max-w-xs truncate">
                          {(r.signals || []).join(' · ')}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* ══ TAB: Signal Frequency ══ */}
      {tab === 'freq' && freq.length > 0 && (
        <div className="flex flex-col gap-2 min-h-0">
          <div className="flex items-center gap-3 text-xs text-md-on-surface-var flex-wrap">
            <span>Show signals present in ≥</span>
            <select value={freqThresh} onChange={e => setFreqThresh(Number(e.target.value))}
              className="bg-md-surface-var border border-md-outline-var rounded px-1 py-0.5 text-xs text-white">
              {[20,30,40,50,60,70,80,90,100].map(v => (
                <option key={v} value={v}>{v}%</option>
              ))}
            </select>
            <span>of {meta?.n_tickers} tickers</span>
            <span className="text-white/30">({filteredFreq.length} signals)</span>
            <span className="ml-auto text-white/30">total signal columns scanned: {meta?.sig_cols_total}</span>
          </div>

          <div className="overflow-y-auto max-h-[55vh]">
            <table className="w-full text-xs border-collapse">
              <thead className="sticky top-0 bg-md-surface z-10">
                <tr className="text-md-on-surface-var border-b border-white/10">
                  <th className="text-left px-2 py-1 w-8">#</th>
                  <th className="text-left px-2 py-1">Signal</th>
                  <th className="text-right px-2 py-1 w-16">Tickers</th>
                  <th className="text-left px-2 py-1 w-36">Coverage</th>
                </tr>
              </thead>
              <tbody>
                {filteredFreq.map((f, i) => (
                  <tr key={f.sig}
                    className={`border-b border-white/5 hover:bg-white/5 ${f.pct === 100 ? 'bg-emerald-950/30' : ''}`}>
                    <td className="px-2 py-0.5 text-white/30">{i + 1}</td>
                    <td className="px-2 py-0.5 font-mono">
                      <span className={PCT_COLOR(f.pct)}>{SIG_LABEL(f.sig)}</span>
                      <span className="text-white/20 text-[10px] ml-1">({f.sig})</span>
                    </td>
                    <td className="px-2 py-0.5 text-right">
                      <span className={PCT_COLOR(f.pct)}>{f.n}/{meta?.n_tickers}</span>
                    </td>
                    <td className="px-2 py-0.5">
                      {PCT_BAR(f.pct)}
                      <span className={`text-[10px] ${PCT_COLOR(f.pct)}`}>{f.pct}%</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {notes !== undefined && okRows.length > 0 && (
            <input value={notes} onChange={e => setNotes(e.target.value)}
              placeholder="Session notes (optional)…"
              className="w-full bg-md-surface-var border border-md-outline-var rounded px-2 py-1 text-xs text-white mt-1" />
          )}
        </div>
      )}

      {/* ══ TAB: Per Ticker ══ */}
      {tab === 'tickers' && okRows.length > 0 && (
        <div className="flex gap-3 min-h-0 overflow-hidden">
          {/* Ticker list */}
          <div className="w-48 flex flex-col gap-0.5 overflow-y-auto">
            {okRows.map(r => (
              <button key={r.ticker}
                onClick={() => setExpandTicker(expandTicker === r.ticker ? null : r.ticker)}
                className={`text-left px-2 py-1 rounded text-xs font-mono transition-colors ${
                  expandTicker === r.ticker
                    ? 'bg-orange-900/50 text-orange-200'
                    : 'hover:bg-white/5 text-white/80'}`}>
                <span className="font-bold">{r.ticker}</span>
                {r.pump_pct != null && <span className="text-emerald-400 ml-1">+{r.pump_pct.toFixed(0)}%</span>}
                <span className="text-white/30 ml-1 text-[10px]">{r.n_fired}sig</span>
              </button>
            ))}
          </div>

          {/* Expanded signal list */}
          {expandedRow ? (
            <div className="flex-1 overflow-y-auto text-xs">
              <div className="font-semibold text-white mb-2">
                {expandedRow.ticker}
                {expandedRow.pump_pct != null && <span className="text-emerald-400 ml-2">+{expandedRow.pump_pct.toFixed(0)}%</span>}
                <span className="text-white/40 ml-2 font-normal">
                  RSI {expandedRow.last_rsi} · Vol {expandedRow.max_vol_ratio}x · {expandedRow.n_fired} signals fired
                </span>
              </div>

              {/* Categorical sigs */}
              <div className="flex gap-4 mb-3 flex-wrap text-[10px]">
                {expandedRow.t_sigs?.length > 0 && (
                  <div>
                    <div className="text-md-on-surface-var mb-0.5">T-sigs</div>
                    {expandedRow.t_sigs.map(([d, s], i) => (
                      <span key={i} className="text-violet-300 mr-1">{s}<span className="text-white/30">({d.slice(5)})</span></span>
                    ))}
                  </div>
                )}
                {expandedRow.z_sigs?.length > 0 && (
                  <div>
                    <div className="text-md-on-surface-var mb-0.5">Z-sigs</div>
                    {expandedRow.z_sigs.map(([d, s], i) => (
                      <span key={i} className="text-red-400 mr-1">{s}<span className="text-white/30">({d.slice(5)})</span></span>
                    ))}
                  </div>
                )}
                {expandedRow.l_sigs?.length > 0 && (
                  <div>
                    <div className="text-md-on-surface-var mb-0.5">L-sigs</div>
                    {expandedRow.l_sigs.map(([d, s], i) => (
                      <span key={i} className="text-orange-300 mr-1">{s}<span className="text-white/30">({d.slice(5)})</span></span>
                    ))}
                  </div>
                )}
                {expandedRow.vol_spikes?.length > 0 && (
                  <div>
                    <div className="text-md-on-surface-var mb-0.5">Vol spikes</div>
                    {expandedRow.vol_spikes.map((v, i) => (
                      <span key={i} className="text-yellow-300 mr-1">{v.vol_ratio}x<span className="text-white/30">({v.dt?.slice(5)})</span></span>
                    ))}
                  </div>
                )}
              </div>

              {/* All fired signals */}
              <div className="text-md-on-surface-var text-[10px] mb-1">All fired signals ({expandedRow.n_fired})</div>
              <div className="flex flex-wrap gap-1">
                {Object.entries(expandedRow.fired || {})
                  .sort(([,a],[,b]) => b - a)
                  .map(([sig, cnt]) => (
                    <span key={sig}
                      className="px-1.5 py-0.5 rounded bg-md-surface-var border border-white/10 font-mono text-[10px] text-white/70">
                      {SIG_LABEL(sig)}
                      {cnt > 1 && <span className="text-yellow-400 ml-0.5">×{cnt}</span>}
                    </span>
                  ))}
              </div>
            </div>
          ) : (
            <div className="flex-1 text-xs text-white/30 pt-4">← Select a ticker to see all fired signals</div>
          )}
        </div>
      )}

      {/* ══ TAB: Log ══ */}
      {tab === 'log' && (
        <div className="min-h-0 overflow-y-auto max-h-[60vh]">
          {mdContent ? (
            <pre className="bg-md-surface-var rounded p-3 text-[10px] font-mono text-white/80 whitespace-pre-wrap">
              {mdContent}
            </pre>
          ) : (
            <div className="text-xs text-white/30">Click "📄 Show Log" to load PUMP_RESEARCH.md</div>
          )}
        </div>
      )}
    </div>
  )
}
