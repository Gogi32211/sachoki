import { useState, useCallback } from 'react'

// Parse paste like:
//   "CAST +159%, CUPR +144%" or "CAST 159 CUPR 144" or one-per-line
function parsePaste(text) {
  const entries = []
  // Try comma-separated first
  const lines = text.split(/[\n,]+/).map(s => s.trim()).filter(Boolean)
  for (const line of lines) {
    const m = line.match(/^([A-Z]{1,6})\s*[+-]?([\d.]+)%?/i)
    if (m) {
      entries.push({ ticker: m[1].toUpperCase(), pump: parseFloat(m[2]) })
    } else {
      // Just a ticker symbol
      const sym = line.match(/^([A-Z]{1,6})$/i)
      if (sym) entries.push({ ticker: sym[1].toUpperCase(), pump: null })
    }
  }
  return entries
}

const SCORE_COLOR = s =>
  s >= 7 ? 'text-emerald-400 font-bold' :
  s >= 5 ? 'text-yellow-300' :
  s >= 3 ? 'text-orange-300' : 'text-red-400'

const RECIPE_FIELDS = [
  { key: 'capit_dates', label: 'Capit (L34/L46)', fmt: v => v?.length > 0 ? v.slice(-2).map(d => d.slice(5)).join(', ') : '—', good: v => v?.length > 0 },
  { key: 'capit_atom',  label: 'Capit→Atom',      fmt: (v, r) => v ? `✅ ${(r.atom_date || '').slice(5)}` : '—', good: v => v },
  { key: 'max_vol_ratio', label: 'Max Vol/Avg',   fmt: v => v ? `${v}x` : '—', good: v => v >= 5 },
  { key: 'last_rsi',   label: 'RSI',               fmt: v => v?.toFixed(1) ?? '—', good: v => v != null && v < 40 },
  { key: 'below_ema20', label: 'Days ↓ EMA20',    fmt: (v, r) => `${v}/${r.n_bars}`, good: (v, r) => v >= (r?.n_bars || 14) * 0.5 },
  { key: 'p66_date',   label: 'P66',               fmt: v => v ? v.slice(5) : '—', good: v => !!v },
]

export default function PumpLogPanel() {
  const [paste, setPaste]       = useState('')
  const [rows, setRows]         = useState([])
  const [loading, setLoading]   = useState(false)
  const [notes, setNotes]       = useState('')
  const [logStatus, setLogStatus] = useState(null)  // null | 'ok' | 'err'
  const [mdContent, setMdContent] = useState(null)
  const [mdLoading, setMdLoading] = useState(false)

  const handleAnalyze = useCallback(async () => {
    const entries = parsePaste(paste)
    if (!entries.length) return
    setLoading(true)
    setRows([])
    setLogStatus(null)
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
    } finally {
      setLoading(false)
    }
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
      body: JSON.stringify({
        date: today,
        tickers: entries.map(e => e.ticker),
        pumps,
        rows,
        notes,
      }),
    })
    setLogStatus(res.ok ? 'ok' : 'err')
  }, [rows, paste, notes])

  const handleShowMd = useCallback(async () => {
    setMdLoading(true)
    try {
      const res = await fetch('/api/studio/pump/md')
      const d = await res.json()
      setMdContent(d.content || '')
    } finally {
      setMdLoading(false)
    }
  }, [])

  const okRows = rows.filter(r => r.status === 'ok')
  const notFound = rows.filter(r => r.status === 'not_in_db').map(r => r.ticker)

  return (
    <div className="flex flex-col gap-4 p-4 text-sm text-md-on-surface">
      {/* ── Header ── */}
      <div className="flex items-center gap-3">
        <h2 className="text-base font-semibold text-white">🔥 Pump Log</h2>
        <span className="text-xs text-md-on-surface-var">daily pre-pump signal tracker → PUMP_RESEARCH.md</span>
        <button
          onClick={handleShowMd}
          className="ml-auto px-2 py-0.5 rounded text-xs border border-white/10 hover:bg-white/10 text-md-on-surface-var"
        >
          {mdLoading ? '…' : '📄 Show Log'}
        </button>
      </div>

      {/* ── Input ── */}
      <div className="flex gap-3 items-start">
        <div className="flex-1">
          <div className="text-xs text-md-on-surface-var mb-1">Paste tickers (CAST +159%, CUPR +144%... or one per line)</div>
          <textarea
            value={paste}
            onChange={e => setPaste(e.target.value)}
            className="w-full h-24 bg-md-surface-var border border-md-outline-var rounded px-2 py-1.5 text-xs font-mono text-white resize-none"
            placeholder={"CAST +159%, CUPR +144%, VSME +104%, PAVS +97%, QTEX +65%,\nAHMA +50%, HQ +48%, HUBC +45%, GPUS +42%, SDOT +40%"}
          />
        </div>
        <div className="flex flex-col gap-2 pt-5">
          <button
            onClick={handleAnalyze}
            disabled={loading || !paste.trim()}
            className="px-4 py-1.5 rounded bg-orange-700 hover:bg-orange-600 disabled:opacity-40 text-white text-xs font-semibold"
          >
            {loading ? '⏳ Analyzing…' : '🔍 Analyze'}
          </button>
          <button
            onClick={handleLog}
            disabled={!okRows.length}
            className="px-4 py-1.5 rounded bg-emerald-800 hover:bg-emerald-700 disabled:opacity-40 text-white text-xs font-semibold"
          >
            💾 Log Session
          </button>
        </div>
      </div>

      {/* ── Notes ── */}
      {okRows.length > 0 && (
        <input
          value={notes}
          onChange={e => setNotes(e.target.value)}
          placeholder="Session notes (optional)…"
          className="w-full bg-md-surface-var border border-md-outline-var rounded px-2 py-1 text-xs text-white"
        />
      )}

      {/* ── Log status ── */}
      {logStatus === 'ok' && (
        <div className="text-xs text-emerald-400">✅ Session logged to PUMP_RESEARCH.md</div>
      )}
      {logStatus === 'err' && (
        <div className="text-xs text-red-400">❌ Log failed</div>
      )}

      {/* ── Not in DB ── */}
      {notFound.length > 0 && (
        <div className="text-xs text-yellow-400">Not in DB: {notFound.join(', ')}</div>
      )}

      {/* ── Results table ── */}
      {okRows.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="text-md-on-surface-var border-b border-white/10">
                <th className="text-left px-2 py-1">Ticker</th>
                <th className="text-left px-2 py-1">Pump%</th>
                <th className="text-left px-2 py-1">Score</th>
                <th className="text-left px-2 py-1">Capit</th>
                <th className="text-left px-2 py-1">Capit→Atom</th>
                <th className="text-left px-2 py-1">MaxVol</th>
                <th className="text-left px-2 py-1">RSI</th>
                <th className="text-left px-2 py-1">↓EMA20</th>
                <th className="text-left px-2 py-1">T-sigs</th>
                <th className="text-left px-2 py-1">P66</th>
                <th className="text-left px-2 py-1">Last</th>
              </tr>
            </thead>
            <tbody>
              {okRows.map(r => (
                <tr key={r.ticker} className="border-b border-white/5 hover:bg-white/5">
                  <td className="px-2 py-1 font-mono font-bold text-white">{r.ticker}</td>
                  <td className="px-2 py-1 text-emerald-400">
                    {r.pump_pct != null ? `+${r.pump_pct.toFixed(0)}%` : '—'}
                  </td>
                  <td className={`px-2 py-1 font-bold ${SCORE_COLOR(r.recipe_score)}`}>
                    {r.recipe_score}/9
                  </td>
                  <td className="px-2 py-1">
                    {r.capit_dates?.length > 0
                      ? <span className="text-red-400">{r.capit_dates.slice(-2).map(d => d.slice(5)).join(', ')}</span>
                      : <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-2 py-1">
                    {r.capit_atom
                      ? <span className="text-orange-400">✅ {r.atom_date?.slice(5)}</span>
                      : <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-2 py-1">
                    {r.max_vol_ratio >= 5
                      ? <span className="text-yellow-300">{r.max_vol_ratio}x</span>
                      : <span className="text-white/30">{r.max_vol_ratio}x</span>}
                  </td>
                  <td className="px-2 py-1">
                    {r.last_rsi != null
                      ? <span className={r.last_rsi < 35 ? 'text-red-400' : r.last_rsi < 45 ? 'text-orange-300' : 'text-white/70'}>
                          {r.last_rsi?.toFixed(1)}
                        </span>
                      : '—'}
                  </td>
                  <td className="px-2 py-1">
                    <span className={r.below_ema20 >= r.n_bars * 0.7 ? 'text-red-400' : 'text-white/70'}>
                      {r.below_ema20}/{r.n_bars}
                    </span>
                  </td>
                  <td className="px-2 py-1 text-violet-300 font-mono text-[10px]">
                    {r.t_sigs?.slice(-4).map(([d, s]) => s).join(' ') || '—'}
                  </td>
                  <td className="px-2 py-1">
                    {r.p66_date
                      ? <span className="text-cyan-400">{r.p66_date.slice(5)}</span>
                      : <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-2 py-1 text-white/50">{r.last_date?.slice(5)} ${r.last_close}</td>
                </tr>
              ))}
            </tbody>
          </table>

          {/* ── Recipe legend ── */}
          <div className="mt-3 text-[10px] text-md-on-surface-var flex flex-wrap gap-3">
            <span>Score: Capit+2, →Atom+2, T1G/T2G+2, Vol≥5x+2, ↓EMA20≥50%+1, Vol≥15x+1 (max 9+1)</span>
            <span className="text-emerald-400">≥7 = strong</span>
            <span className="text-yellow-300">≥5 = watch</span>
            <span className="text-orange-300">≥3 = weak</span>
          </div>
        </div>
      )}

      {/* ── PUMP_RESEARCH.md viewer ── */}
      {mdContent !== null && (
        <div className="mt-2">
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs text-md-on-surface-var">PUMP_RESEARCH.md</span>
            <button onClick={() => setMdContent(null)} className="text-xs text-white/40 hover:text-white">✕</button>
          </div>
          <pre className="bg-md-surface-var rounded p-3 text-[10px] font-mono text-white/80 overflow-auto max-h-[50vh] whitespace-pre-wrap">
            {mdContent}
          </pre>
        </div>
      )}
    </div>
  )
}
