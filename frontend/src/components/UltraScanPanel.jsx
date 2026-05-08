import { useState, useMemo, useEffect } from 'react'
import { api } from '../api'

// ── Universes / Timeframes / Direction ────────────────────────────────────────
const UNIVERSES = [
  { key: 'sp500',      label: 'S&P 500'   },
  { key: 'nasdaq',     label: 'NASDAQ'    },
  { key: 'nasdaq_gt5', label: 'NASDAQ > $5' },
  { key: 'russell2k',  label: 'Russell 2K' },
  { key: 'all_us',     label: 'All US'    },
  { key: 'split',      label: 'SPLIT'     },
]
const TF_OPTS    = ['1d', '4h', '1h', '30m', '15m']
const DIR_OPTS   = ['all', 'bull', 'bear']
const NASDAQ_BATCHES = ['', 'a_f', 'g_m', 'n_s', 't_z', 'a_m', 'n_z']

// ── CSV columns (read-only aggregation; no new score) ─────────────────────────
const CSV_COLS = [
  'ticker', 'price', 'volume',
  'turbo_score', 'turbo_direction', 'turbo_signals',
  't_signal', 'z_signal', 'l_signal', 'preup_signal', 'predn_signal',
  'lane1_label', 'lane3_label', 'volume_bucket', 'wick_suffix',
  'tz_intel_role', 'tz_intel_score', 'tz_intel_quality', 'tz_intel_action',
  'abr_category', 'abr_med10d_pct', 'abr_fail10d_pct',
  'matched_status', 'matched_med10d_pct', 'matched_fail10d_pct',
  'pullback_evidence_tier', 'pullback_stage', 'pullback_pattern_key',
  'pullback_score', 'pullback_median_10d_return', 'pullback_win_rate_10d',
  'pullback_fail_rate_10d', 'pullback_is_active',
  'rare_evidence_tier', 'rare_base4_key', 'rare_extended5_key', 'rare_extended6_key',
  'rare_pattern_length', 'rare_score', 'rare_median_10d_return', 'rare_fail_rate_10d',
  'rare_is_active', 'rare_completion',
  'has_turbo', 'has_tz_wlnbb', 'has_tz_intel', 'has_pullback', 'has_rare_reversal',
]

function rowToCsv(r) {
  const t  = r.turbo    || {}
  const w  = r.tz_wlnbb || {}
  const i  = r.tz_intel || {}
  const p  = r.pullback || {}
  const x  = r.rare_reversal || {}
  const f  = r.source_flags  || {}
  return {
    ticker: r.ticker,
    price:  r.price,
    volume: r.volume,
    turbo_score:     t.score ?? '',
    turbo_direction: t.direction ?? '',
    turbo_signals:   Array.isArray(t.signals) ? t.signals.join(';') : '',
    t_signal:     w.t_signal     ?? '',
    z_signal:     w.z_signal     ?? '',
    l_signal:     w.l_signal     ?? '',
    preup_signal: w.preup_signal ?? '',
    predn_signal: w.predn_signal ?? '',
    lane1_label:   w.lane1_label   ?? '',
    lane3_label:   w.lane3_label   ?? '',
    volume_bucket: w.volume_bucket ?? '',
    wick_suffix:   w.wick_suffix   ?? '',
    tz_intel_role:    i.role    ?? '',
    tz_intel_score:   i.score   ?? '',
    tz_intel_quality: i.quality ?? '',
    tz_intel_action:  i.action  ?? '',
    abr_category:        i.abr_category        ?? '',
    abr_med10d_pct:      i.abr_med10d_pct      ?? '',
    abr_fail10d_pct:     i.abr_fail10d_pct     ?? '',
    matched_status:      i.matched_status      ?? '',
    matched_med10d_pct:  i.matched_med10d_pct  ?? '',
    matched_fail10d_pct: i.matched_fail10d_pct ?? '',
    pullback_evidence_tier:    p.evidence_tier      ?? '',
    pullback_stage:            p.pullback_stage     ?? '',
    pullback_pattern_key:      p.pattern_key        ?? '',
    pullback_score:            p.score              ?? '',
    pullback_median_10d_return:p.median_10d_return  ?? '',
    pullback_win_rate_10d:     p.win_rate_10d       ?? '',
    pullback_fail_rate_10d:    p.fail_rate_10d      ?? '',
    pullback_is_active:        p.is_currently_active ? 'true' : 'false',
    rare_evidence_tier:    x.evidence_tier ?? '',
    rare_base4_key:        x.base4_key     ?? '',
    rare_extended5_key:    x.extended5_key ?? '',
    rare_extended6_key:    x.extended6_key ?? '',
    rare_pattern_length:   x.pattern_length ?? '',
    rare_score:            x.score          ?? '',
    rare_median_10d_return:x.median_10d_return ?? '',
    rare_fail_rate_10d:    x.fail_rate_10d  ?? '',
    rare_is_active:        x.is_currently_active ? 'true' : 'false',
    rare_completion:       x.current_pattern_completion ?? '',
    has_turbo:         f.has_turbo        ? 'true' : 'false',
    has_tz_wlnbb:      f.has_tz_wlnbb     ? 'true' : 'false',
    has_tz_intel:      f.has_tz_intel     ? 'true' : 'false',
    has_pullback:      f.has_pullback     ? 'true' : 'false',
    has_rare_reversal: f.has_rare_reversal? 'true' : 'false',
  }
}

function exportCSV(rows, universe, tf) {
  const lines = [CSV_COLS.join(',')]
  for (const r of rows) {
    const flat = rowToCsv(r)
    lines.push(CSV_COLS.map(c => {
      let v = flat[c] ?? ''
      if (Array.isArray(v)) v = v.join(';')
      v = String(v)
      // CSV formula injection prevention (Excel/Sheets execute =, +, -, @ as formulas)
      if (/^[=+\-@]/.test(v)) v = "'" + v
      return v.includes(',') || v.includes('"') || v.includes('\n')
        ? `"${v.replace(/"/g, '""')}"` : v
    }).join(','))
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
  const url  = URL.createObjectURL(blob)
  const a    = document.createElement('a')
  a.href     = url
  a.download = `ultra_${universe}_${tf}_${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

// ── Compact badge helpers ─────────────────────────────────────────────────────
function Badge({ children, color = 'gray' }) {
  const cls = {
    gray:    'bg-gray-800 text-gray-300',
    blue:    'bg-blue-900 text-blue-200',
    green:   'bg-emerald-900 text-emerald-200',
    red:     'bg-red-900 text-red-200',
    amber:   'bg-amber-900 text-amber-200',
    purple:  'bg-purple-900 text-purple-200',
    cyan:    'bg-cyan-900 text-cyan-200',
  }[color] || 'bg-gray-800 text-gray-300'
  return (
    <span className={`text-[10px] px-1.5 py-0.5 rounded ${cls}`}>{children}</span>
  )
}

function fmtPrice(v)  { const n = Number(v); return Number.isFinite(n) && n > 0 ? n.toFixed(2) : '—' }
function fmtVol(v)    { const n = Number(v); if (!Number.isFinite(n) || n <= 0) return '—'
                        if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
                        if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
                        if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
                        return String(n) }
function fmtPct(v)    { const n = Number(v); return Number.isFinite(n) ? n.toFixed(1) + '%' : '—' }

// ── Source visibility toggle keys ─────────────────────────────────────────────
const SOURCE_KEYS = [
  { key: 'turbo',         label: 'Turbo' },
  { key: 'tz_wlnbb',      label: 'TZ/WLNBB' },
  { key: 'tz_intel',      label: 'TZ Intel' },
  { key: 'pullback',      label: 'Pullback' },
  { key: 'rare_reversal', label: 'Rare Reversal' },
]

export default function UltraScanPanel({ onSelectTicker }) {
  const [universe,    setUniverse]    = useState('sp500')
  const [tf,          setTf]          = useState('1d')
  const [direction,   setDirection]   = useState('bull')
  const [minScore,    setMinScore]    = useState(0)
  const [minPrice,    setMinPrice]    = useState(0)
  const [maxPrice,    setMaxPrice]    = useState(1e9)
  const [minVolume,   setMinVolume]   = useState(0)
  const [nasdaqBatch, setNasdaqBatch] = useState('')
  const [sourceVis,   setSourceVis]   = useState({
    turbo: true, tz_wlnbb: true, tz_intel: true, pullback: true, rare_reversal: true,
  })
  const [sortKey,     setSortKey]     = useState('turbo_score')
  const [sortDir,     setSortDir]     = useState('desc')

  const [results,  setResults]  = useState([])
  const [meta,     setMeta]     = useState(null)
  const [warnings, setWarnings] = useState([])
  const [loading,  setLoading]  = useState(false)
  const [error,    setError]    = useState('')

  const fetchScan = async () => {
    setLoading(true); setError('')
    try {
      const params = {
        universe, tf, direction,
        limit:     1000,
        min_score: minScore,
        min_price: minPrice,
        max_price: maxPrice,
        min_volume: minVolume,
      }
      if (nasdaqBatch) params.nasdaq_batch = nasdaqBatch
      const r = await api.ultraScan(params)
      setResults(r?.results || [])
      setMeta(r?.meta || null)
      setWarnings(r?.warnings || [])
    } catch (exc) {
      setError(String(exc))
      setResults([])
      setMeta(null)
      setWarnings([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { fetchScan() }, []) // initial load

  const sortedRows = useMemo(() => {
    const copy = results.slice()
    const dir = sortDir === 'asc' ? 1 : -1
    copy.sort((a, b) => {
      let av, bv
      if (sortKey === 'ticker') {
        av = a.ticker || ''; bv = b.ticker || ''
        return av.localeCompare(bv) * dir
      }
      if (sortKey === 'price')  { av = +a.price  || 0; bv = +b.price  || 0 }
      else if (sortKey === 'volume') { av = +a.volume || 0; bv = +b.volume || 0 }
      else { // turbo_score (default)
        av = +(a.turbo?.score ?? 0); bv = +(b.turbo?.score ?? 0)
      }
      return (av - bv) * dir
    })
    return copy
  }, [results, sortKey, sortDir])

  const toggleSort = (k) => {
    if (sortKey === k) setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    else { setSortKey(k); setSortDir(k === 'ticker' ? 'asc' : 'desc') }
  }

  return (
    <div className="text-sm">
      <div className="flex flex-wrap items-end gap-3 p-2 border border-gray-800 rounded bg-gray-900">
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Universe</label>
          <select value={universe} onChange={e => setUniverse(e.target.value)}
                  className="bg-gray-800 text-white text-xs rounded px-2 py-1">
            {UNIVERSES.map(u => <option key={u.key} value={u.key}>{u.label}</option>)}
          </select>
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Timeframe</label>
          <select value={tf} onChange={e => setTf(e.target.value)}
                  className="bg-gray-800 text-white text-xs rounded px-2 py-1">
            {TF_OPTS.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Direction</label>
          <select value={direction} onChange={e => setDirection(e.target.value)}
                  className="bg-gray-800 text-white text-xs rounded px-2 py-1">
            {DIR_OPTS.map(d => <option key={d} value={d}>{d}</option>)}
          </select>
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Min Turbo Score</label>
          <input type="number" value={minScore}
                 onChange={e => setMinScore(Number(e.target.value) || 0)}
                 className="bg-gray-800 text-white text-xs rounded px-2 py-1 w-20" />
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Min Price</label>
          <input type="number" value={minPrice}
                 onChange={e => setMinPrice(Number(e.target.value) || 0)}
                 className="bg-gray-800 text-white text-xs rounded px-2 py-1 w-20" />
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Max Price</label>
          <input type="number" value={maxPrice}
                 onChange={e => setMaxPrice(Number(e.target.value) || 1e9)}
                 className="bg-gray-800 text-white text-xs rounded px-2 py-1 w-24" />
        </div>
        <div className="flex flex-col">
          <label className="text-[10px] uppercase text-gray-500">Min Volume</label>
          <input type="number" value={minVolume}
                 onChange={e => setMinVolume(Number(e.target.value) || 0)}
                 className="bg-gray-800 text-white text-xs rounded px-2 py-1 w-24" />
        </div>
        {(universe === 'nasdaq' || universe === 'nasdaq_gt5') && (
          <div className="flex flex-col">
            <label className="text-[10px] uppercase text-gray-500">NASDAQ Batch</label>
            <select value={nasdaqBatch} onChange={e => setNasdaqBatch(e.target.value)}
                    className="bg-gray-800 text-white text-xs rounded px-2 py-1">
              {NASDAQ_BATCHES.map(b => <option key={b} value={b}>{b || '—'}</option>)}
            </select>
          </div>
        )}
        <button onClick={fetchScan} disabled={loading}
                className="bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-xs px-3 py-1 rounded">
          {loading ? 'Loading…' : 'Refresh'}
        </button>
        <button onClick={() => exportCSV(sortedRows, universe, tf)}
                disabled={!sortedRows.length}
                className="bg-emerald-700 hover:bg-emerald-600 disabled:opacity-50 text-white text-xs px-3 py-1 rounded">
          Export CSV
        </button>
      </div>

      {/* Source visibility toggles */}
      <div className="flex flex-wrap items-center gap-2 mt-2 text-xs text-gray-300">
        <span className="text-[10px] uppercase text-gray-500">Show columns:</span>
        {SOURCE_KEYS.map(s => (
          <label key={s.key} className="flex items-center gap-1 cursor-pointer">
            <input
              type="checkbox"
              checked={!!sourceVis[s.key]}
              onChange={e => setSourceVis(v => ({ ...v, [s.key]: e.target.checked }))}
            />
            <span>{s.label}</span>
          </label>
        ))}
      </div>

      {/* Source meta + warnings */}
      {meta && (
        <div className="mt-2 flex flex-wrap items-center gap-2 text-[11px]">
          {Object.entries(meta.sources || {}).map(([k, s]) => (
            <Badge key={k} color={s.ok ? 'green' : 'red'}>
              {k}: {s.ok ? `ok (${s.count})` : 'unavailable'}
            </Badge>
          ))}
          {meta.elapsed_ms != null && (
            <span className="text-gray-500">· {meta.elapsed_ms} ms</span>
          )}
        </div>
      )}
      {warnings && warnings.length > 0 && (
        <div className="mt-2 text-[11px] text-amber-300 bg-amber-950 border border-amber-800 rounded p-2">
          {warnings.map((w, i) => <div key={i}>⚠ {w}</div>)}
        </div>
      )}
      {error && (
        <div className="mt-2 text-xs text-red-300 bg-red-950 border border-red-800 rounded p-2">
          {error}
        </div>
      )}

      {/* Table */}
      <div className="mt-2 overflow-x-auto border border-gray-800 rounded">
        <table className="min-w-full text-xs">
          <thead className="bg-gray-900 text-gray-400">
            <tr>
              <th className="px-2 py-1 text-left cursor-pointer" onClick={() => toggleSort('ticker')}>Ticker</th>
              <th className="px-2 py-1 text-right cursor-pointer" onClick={() => toggleSort('price')}>Price</th>
              <th className="px-2 py-1 text-right cursor-pointer" onClick={() => toggleSort('volume')}>Volume</th>
              {sourceVis.turbo && (
                <>
                  <th className="px-2 py-1 text-right cursor-pointer" onClick={() => toggleSort('turbo_score')}>Turbo</th>
                  <th className="px-2 py-1 text-left">Turbo Signals</th>
                </>
              )}
              {sourceVis.tz_wlnbb && (
                <th className="px-2 py-1 text-left">TZ/WLNBB</th>
              )}
              {sourceVis.tz_intel && (
                <>
                  <th className="px-2 py-1 text-left">TZ Intel Role</th>
                  <th className="px-2 py-1 text-left">ABR</th>
                </>
              )}
              {sourceVis.pullback && (
                <th className="px-2 py-1 text-left">Pullback</th>
              )}
              {sourceVis.rare_reversal && (
                <th className="px-2 py-1 text-left">Rare Reversal</th>
              )}
              <th className="px-2 py-1 text-left">Sources</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.length === 0 && !loading && (
              <tr><td colSpan={12} className="text-center text-gray-600 py-6">
                No results
              </td></tr>
            )}
            {sortedRows.map(r => {
              const t = r.turbo
              const w = r.tz_wlnbb
              const i = r.tz_intel
              const p = r.pullback
              const x = r.rare_reversal
              const f = r.source_flags || {}
              return (
                <tr key={r.ticker}
                    className="border-t border-gray-800 hover:bg-gray-900 cursor-pointer"
                    onClick={() => onSelectTicker && onSelectTicker(r.ticker)}>
                  <td className="px-2 py-1 font-mono font-semibold text-blue-300">
                    {r.ticker}
                  </td>
                  <td className="px-2 py-1 text-right">{fmtPrice(r.price)}</td>
                  <td className="px-2 py-1 text-right">{fmtVol(r.volume)}</td>
                  {sourceVis.turbo && (
                    <>
                      <td className="px-2 py-1 text-right">
                        {t ? Number(t.score).toFixed(1) : '—'}
                      </td>
                      <td className="px-2 py-1 text-left max-w-[260px] overflow-hidden truncate"
                          title={t?.signals?.join(' ') || ''}>
                        {t?.signals?.slice(0, 6).map(s =>
                          <span key={s} className="mr-1 text-[10px] text-cyan-300">{s}</span>
                        )}
                        {t?.signals && t.signals.length > 6 &&
                          <span className="text-[10px] text-gray-500">+{t.signals.length - 6}</span>}
                      </td>
                    </>
                  )}
                  {sourceVis.tz_wlnbb && (
                    <td className="px-2 py-1 text-left whitespace-nowrap">
                      {w?.t_signal     && <Badge color="green">T:{w.t_signal}</Badge>}{' '}
                      {w?.z_signal     && <Badge color="blue">Z:{w.z_signal}</Badge>}{' '}
                      {w?.l_signal     && <Badge color="purple">L:{w.l_signal}</Badge>}{' '}
                      {w?.preup_signal && <Badge color="cyan">↑:{w.preup_signal}</Badge>}{' '}
                      {w?.predn_signal && <Badge color="red">↓:{w.predn_signal}</Badge>}
                    </td>
                  )}
                  {sourceVis.tz_intel && (
                    <>
                      <td className="px-2 py-1 text-left whitespace-nowrap">
                        {i?.role && <Badge color="amber">{i.role}</Badge>}{' '}
                        {i?.quality && <span className="text-[10px] text-gray-400">{i.quality}</span>}
                      </td>
                      <td className="px-2 py-1 text-left whitespace-nowrap">
                        {i?.abr_category && <Badge color="cyan">{i.abr_category}</Badge>}
                        {i?.abr_med10d_pct != null && (
                          <span className="ml-1 text-[10px] text-gray-400">
                            med {fmtPct(i.abr_med10d_pct)}
                          </span>
                        )}
                      </td>
                    </>
                  )}
                  {sourceVis.pullback && (
                    <td className="px-2 py-1 text-left whitespace-nowrap">
                      {p?.evidence_tier && <Badge color="green">{p.evidence_tier}</Badge>}
                      {p?.pattern_key && (
                        <span className="ml-1 text-[10px] font-mono text-gray-400">{p.pattern_key}</span>
                      )}
                      {p?.is_currently_active && <span className="ml-1 text-[10px] text-emerald-300">●</span>}
                    </td>
                  )}
                  {sourceVis.rare_reversal && (
                    <td className="px-2 py-1 text-left whitespace-nowrap">
                      {x?.evidence_tier && <Badge color="purple">{x.evidence_tier}</Badge>}
                      {x?.base4_key && (
                        <span className="ml-1 text-[10px] font-mono text-gray-400">{x.base4_key}</span>
                      )}
                      {x?.is_currently_active && <span className="ml-1 text-[10px] text-emerald-300">●</span>}
                    </td>
                  )}
                  <td className="px-2 py-1 text-left whitespace-nowrap">
                    <span className="text-[10px] text-gray-500">
                      {f.has_turbo         ? 'T' : '·'}
                      {f.has_tz_wlnbb      ? 'W' : '·'}
                      {f.has_tz_intel      ? 'I' : '·'}
                      {f.has_pullback      ? 'P' : '·'}
                      {f.has_rare_reversal ? 'R' : '·'}
                    </span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      <div className="mt-2 text-[10px] text-gray-500">
        ULTRA is a read-only aggregation. No new score, category, or context is computed.
        Total: {sortedRows.length}
        {meta?.last_scan && <> · Last Turbo scan: {meta.last_scan}</>}
      </div>
    </div>
  )
}
