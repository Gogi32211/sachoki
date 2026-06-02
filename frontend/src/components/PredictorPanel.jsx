import { useEffect, useState, useCallback, useRef } from 'react'
import { api } from '../api'

// ── Signal metadata ───────────────────────────────────────────────────────────
const _SIG_NAMES = {
  0:'—', 1:'T1G', 2:'T1', 3:'T2G', 4:'T2', 5:'T3', 6:'T4', 7:'T5', 8:'T6',
  9:'T9', 10:'T10', 11:'T11', 12:'T12',
  13:'Z1G', 14:'Z1', 15:'Z2G', 16:'Z2', 17:'Z3', 18:'Z4', 19:'Z5', 20:'Z6',
  21:'Z7', 22:'Z9', 23:'Z10', 24:'Z11', 25:'Z12',
}
const _COL_IDS = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]
const _ROW_IDS = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]

function _cellBg(pct, colId) {
  if (pct < 1) return undefined
  const op = Math.min(pct / 28, 1) * 0.80
  if (colId === 0)  return `rgba(107,114,128,${op})`
  if (colId <= 12)  return `rgba(34,197,94,${op})`
  return `rgba(239,68,68,${op})`
}

// ── T/Z Transition Matrix (chess-board) ───────────────────────────────────────
function TZMatrix({ matrixData, label, sublabel, offset }) {
  const data = (offset === 1 ? matrixData?.bar1 : matrixData?.bar2) ?? {}
  const hasData = Object.keys(data).length > 0

  return (
    <div className="mb-4">
      {/* Panel header */}
      <div className="px-3 py-2 bg-violet-900/40 rounded-t-lg text-sm font-bold text-violet-300 flex items-center justify-between">
        <span>T/Z Transition Matrix — Bar +{offset}</span>
        {sublabel && <span className="text-[10px] font-normal opacity-70">{sublabel}</span>}
      </div>
      <div className="border border-md-outline-var rounded-b-lg overflow-hidden">
        {!hasData ? (
          <div className="px-3 py-6 text-center text-md-on-surface-var/70 text-xs">No data</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="text-[10px] border-collapse w-full">
              <thead>
                <tr className="bg-md-surface-con/80">
                  <th className="sticky top-0 bg-md-surface-con px-2 py-1.5 text-left text-md-on-surface-var font-normal whitespace-nowrap border-r border-md-outline-var" style={{minWidth:'46px'}}>
                    ↓ / →
                  </th>
                  {_COL_IDS.map(c => (
                    <th key={c}
                      className={`px-1 py-1.5 text-center font-mono font-bold
                        ${c === 0 ? 'text-md-on-surface-var' : c <= 12 ? 'text-green-400' : 'text-red-400'}
                        ${c === 13 ? 'border-l border-md-outline-var' : ''}`}
                      style={{minWidth:'34px'}}>
                      {_SIG_NAMES[c]}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {_ROW_IDS.map((rowId, idx) => {
                  const row  = data[String(rowId)] ?? {}
                  const tot  = Object.values(row).reduce((a, b) => a + b, 0)
                  const isZ  = rowId >= 13
                  return (
                    <tr key={rowId}
                      className={`border-t border-md-outline-var/50
                        ${idx === 11 ? 'border-t-2 border-gray-600' : ''}
                      `}>
                      {/* Row header */}
                      <td className={`sticky left-0 z-10 bg-md-surface px-2 py-0.5 font-mono font-bold border-r border-md-outline-var
                        ${isZ ? 'text-red-400' : 'text-green-400'}`}>
                        {_SIG_NAMES[rowId]}
                      </td>
                      {_COL_IDS.map(colId => {
                        const cnt = row[String(colId)] ?? 0
                        const pct = tot > 0 ? cnt / tot * 100 : 0
                        return (
                          <td key={colId}
                            className={`text-center px-0.5 py-0.5 font-mono
                              ${colId === 13 ? 'border-l border-md-outline-var' : ''}
                              ${pct >= 2 ? 'text-white' : 'text-transparent'}`}
                            style={{backgroundColor: _cellBg(pct, colId)}}>
                            {pct >= 2 ? Math.round(pct) + '%' : '·'}
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

// ── T/Z outcome table ─────────────────────────────────────────────────────────
function TZOutcomeTable({ data, title, color, pooled = false, poolLabel = 'SP500 Pooled' }) {
  return (
    <div className="flex-1 min-w-0">
      <div className={`px-3 py-2 rounded-t-lg text-sm font-bold flex items-center justify-between ${color}`}>
        <span>{title}</span>
        {pooled && <span className="text-[10px] font-normal opacity-70">{poolLabel}</span>}
      </div>
      <div className="border border-md-outline-var rounded-b-lg overflow-hidden">
        <div className="px-3 py-2 bg-md-surface-con border-b border-md-outline-var text-xs text-md-on-surface-var">
          <span className="font-mono">{data.pattern || '—'}</span>
          {data.signals && (
            <span className="ml-2 text-md-on-surface-var text-xs">{data.signals}</span>
          )}
        </div>
        <div className={`px-3 py-1.5 bg-md-surface-con border-b border-md-outline-var text-xs flex items-center gap-2
          ${data.total_matches >= 50 ? 'text-lime-500' : data.total_matches >= 15 ? 'text-yellow-500' : 'text-md-on-surface-var'}`}>
          <span>{data.total_matches} matches</span>
          {data.total_matches >= 50 && <span className="text-[9px] bg-lime-900/40 px-1 rounded">high confidence</span>}
          {data.total_matches >= 15 && data.total_matches < 50 && <span className="text-[9px] bg-yellow-900/40 px-1 rounded">moderate</span>}
          {data.total_matches > 0 && data.total_matches < 15 && <span className="text-[9px] bg-md-surface-high px-1 rounded">low</span>}
          {/* Regime split */}
          {data.bull_matches > 0 && (
            <span className="ml-auto text-[9px] text-md-on-surface-var">
              <span className="text-lime-400">🟢{data.bull_bull_pct}%</span>
              <span className="text-md-on-surface-var/70"> ({data.bull_matches}n) </span>
              <span className="text-red-400">🔴{data.bear_bull_pct}%</span>
              <span className="text-md-on-surface-var/70"> ({data.bear_matches}n)</span>
            </span>
          )}
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-md-on-surface-var bg-md-surface-con">
              <th className="px-3 py-1 text-left">#</th>
              <th className="px-3 py-1 text-left">Signal</th>
              <th className="px-3 py-1 text-right">Count</th>
              <th className="px-3 py-1 text-right">%</th>
            </tr>
          </thead>
          <tbody>
            {(!data.top_outcomes || data.top_outcomes.length === 0) && (
              <tr>
                <td colSpan={4} className="px-3 py-4 text-center text-md-on-surface-var/70 text-xs">
                  No data
                </td>
              </tr>
            )}
            {data.top_outcomes?.map((row, i) => (
              <tr key={i}
                className={`border-t border-md-outline-var ${
                  row.is_bull ? 'bg-green-950/30 text-green-300'
                  : row.is_bear ? 'bg-red-950/30 text-red-300'
                  : 'text-md-on-surface-var'
                }`}>
                <td className="px-3 py-1.5 text-md-on-surface-var">{i + 1}</td>
                <td className="px-3 py-1.5 font-mono font-semibold">{row.sig_name}</td>
                <td className="px-3 py-1.5 text-right">{row.count}</td>
                <td className="px-3 py-1.5 text-right font-bold">{row.pct}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── T/Z signal frequency statistics ──────────────────────────────────────────
function TZStatsSection({ tickerStats, benchStats, ticker, showTicker, showPooled }) {
  if (!showTicker && !showPooled) return null
  const ts = tickerStats
  const bs = benchStats
  if (!ts && !bs) return null

  const benchLabel = bs?.bench_ticker ?? 'Bench'

  // Render one half-panel (ticker or benchmark)
  function StatsPanel({ s, label, sublabel, colorCls }) {
    if (!s || s.total_bars === 0) return (
      <div className="flex-1 min-w-0">
        <div className={`px-3 py-2 rounded-t-lg text-sm font-bold flex items-center justify-between ${colorCls}`}>
          <span>{label}</span>
          {sublabel && <span className="text-[10px] font-normal opacity-70">{sublabel}</span>}
        </div>
        <div className="border border-md-outline-var rounded-b-lg px-3 py-6 text-center text-md-on-surface-var/70 text-xs">No data</div>
      </div>
    )

    const tSigs = s.t_signals ?? []
    const zSigs = s.z_signals ?? []

    return (
      <div className="flex-1 min-w-0">
        {/* Header */}
        <div className={`px-3 py-2 rounded-t-lg text-sm font-bold flex items-center justify-between ${colorCls}`}>
          <span>{label}</span>
          {sublabel && <span className="text-[10px] font-normal opacity-70">{sublabel}</span>}
        </div>
        <div className="border border-md-outline-var rounded-b-lg overflow-hidden">
          {/* Bar type counts */}
          <div className="px-3 py-2 bg-md-surface-con border-b border-md-outline-var flex flex-wrap gap-3 text-xs text-md-on-surface-var">
            <span className="text-md-on-surface-var">{s.total_bars} bars</span>
            <span><span className="text-green-400">{s.bull_bars}</span> bull</span>
            <span><span className="text-red-400">{s.bear_bars}</span> bear</span>
            <span><span className="text-md-on-surface-var">{s.doji_bars}</span> doji</span>
            <span className="ml-auto">
              <span className="text-green-500 font-mono">{s.t_total}T</span>
              <span className="text-md-on-surface-var/70 mx-1">/</span>
              <span className="text-red-400 font-mono">{s.z_total}Z</span>
            </span>
          </div>
          {/* T + Z tables side by side */}
          <div className="flex">
            {/* T signals */}
            <div className="flex-1 border-r border-md-outline-var">
              <div className="px-2 py-1 bg-green-950/30 border-b border-md-outline-var text-[10px] font-bold text-green-400 flex gap-2">
                <span className="flex-1">T Signal</span>
                <span className="w-8 text-right">n</span>
                <span className="w-10 text-right">grp%</span>
                <span className="w-10 text-right">bar%</span>
              </div>
              {tSigs.map(row => (
                <div key={row.sig_id}
                  className={`px-2 py-0.5 flex items-center gap-2 border-t border-md-outline-var/60 text-xs
                    ${row.count > 0 ? 'text-green-300' : 'text-md-on-surface-var/70'}`}>
                  <span className="flex-1 font-mono font-semibold">{row.name}</span>
                  <span className="w-8 text-right">{row.count || ''}</span>
                  <span className="w-10 text-right">{row.count ? row.group_pct + '%' : ''}</span>
                  <span className="w-10 text-right">{row.count ? row.bar_pct + '%' : ''}</span>
                </div>
              ))}
            </div>
            {/* Z signals */}
            <div className="flex-1">
              <div className="px-2 py-1 bg-red-950/30 border-b border-md-outline-var text-[10px] font-bold text-red-400 flex gap-2">
                <span className="flex-1">Z Signal</span>
                <span className="w-8 text-right">n</span>
                <span className="w-10 text-right">grp%</span>
                <span className="w-10 text-right">bar%</span>
              </div>
              {zSigs.map(row => (
                <div key={row.sig_id}
                  className={`px-2 py-0.5 flex items-center gap-2 border-t border-md-outline-var/60 text-xs
                    ${row.count > 0 ? (row.sig_id === 20 ? 'text-md-on-surface-var' : 'text-red-300') : 'text-md-on-surface-var/70'}`}>
                  <span className="flex-1 font-mono font-semibold">{row.name}</span>
                  <span className="w-8 text-right">{row.count || ''}</span>
                  <span className="w-10 text-right">{row.count ? row.group_pct + '%' : ''}</span>
                  <span className="w-10 text-right">{row.count ? row.bar_pct + '%' : ''}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex gap-3">
      {showTicker && (
        <StatsPanel
          s={ts}
          label="T/Z Signal Frequency"
          sublabel={ticker}
          colorCls="bg-violet-900/50 text-violet-300"
        />
      )}
      {showPooled && (
        <StatsPanel
          s={bs}
          label="T/Z Signal Frequency"
          sublabel={benchLabel}
          colorCls="bg-violet-800/40 text-violet-200"
        />
      )}
    </div>
  )
}

// ── L-combo outcome table ─────────────────────────────────────────────────────
function LOutcomeTable({ data, title, color, pooled = false, poolLabel = 'SP500 Pooled' }) {
  return (
    <div className="flex-1 min-w-0">
      <div className={`px-3 py-2 rounded-t-lg text-sm font-bold flex items-center justify-between ${color}`}>
        <span>{title}</span>
        {pooled && <span className="text-[10px] font-normal opacity-70">{poolLabel}</span>}
      </div>
      <div className="border border-md-outline-var rounded-b-lg overflow-hidden">
        <div className="px-3 py-2 bg-md-surface-con border-b border-md-outline-var text-xs text-md-on-surface-var">
          <span className="font-mono text-cyan-400">{data.pattern || '—'}</span>
        </div>
        <div className={`px-3 py-1.5 bg-md-surface-con border-b border-md-outline-var text-xs flex items-center gap-2
          ${data.total_matches >= 100 ? 'text-lime-500' : data.total_matches >= 30 ? 'text-yellow-500' : 'text-md-on-surface-var'}`}>
          <span>{data.total_matches} matches</span>
          {data.total_matches >= 100 && <span className="text-[9px] bg-lime-900/40 px-1 rounded">high confidence</span>}
          {data.total_matches >= 30 && data.total_matches < 100 && <span className="text-[9px] bg-yellow-900/40 px-1 rounded">moderate</span>}
          {data.total_matches > 0 && data.total_matches < 30 && <span className="text-[9px] bg-md-surface-high px-1 rounded">low</span>}
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-md-on-surface-var bg-md-surface-con">
              <th className="px-3 py-1 text-left">#</th>
              <th className="px-3 py-1 text-left">L-Combo</th>
              <th className="px-3 py-1 text-right">Count</th>
              <th className="px-3 py-1 text-right">%</th>
            </tr>
          </thead>
          <tbody>
            {(!data.top_outcomes || data.top_outcomes.length === 0) && (
              <tr>
                <td colSpan={4} className="px-3 py-4 text-center text-md-on-surface-var/70 text-xs">
                  No data
                </td>
              </tr>
            )}
            {data.top_outcomes?.map((row, i) => (
              <tr key={i}
                className={`border-t border-md-outline-var ${
                  row.is_bullish === true  ? 'bg-green-950/30 text-green-300'
                  : row.is_bullish === false ? 'bg-red-950/30 text-red-300'
                  : 'text-md-on-surface-var'
                }`}>
                <td className="px-3 py-1.5 text-md-on-surface-var">{i + 1}</td>
                <td className="px-3 py-1.5 font-mono font-semibold">
                  {row.l_combo}
                  {row.is_bullish === true  && <span className="ml-1 text-green-500">▲</span>}
                  {row.is_bullish === false && <span className="ml-1 text-red-500">▼</span>}
                </td>
                <td className="px-3 py-1.5 text-right">{row.count}</td>
                <td className="px-3 py-1.5 text-right font-bold">{row.pct}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Pooled stats status bar ────────────────────────────────────────────────────
function PooledStatusBar({ universe, interval, onBuildDone }) {
  const [status,   setStatus]   = useState(null)
  const [building, setBuilding] = useState(false)
  const [error,    setError]    = useState(null)

  const build = useCallback(() => {
    setError(null)
    api.pooledStatsBuild(universe, interval, 2000)
      .then(() => { setBuilding(true) })
      .catch(e => {
        if (e.message?.startsWith('409')) { setBuilding(true); return }
        setError(e.message)
      })
  }, [universe, interval])

  const fetchStatus = useCallback(() => {
    api.pooledStatsStatus(universe, interval).then(s => {
      setStatus(s)
      if (s.job?.running) {
        setBuilding(true)
        setTimeout(fetchStatus, 3000)
      } else if (building) {
        setBuilding(false)
        onBuildDone?.()
      }
    }).catch(() => {})
  }, [universe, interval, building])

  useEffect(() => {
    fetchStatus()
  }, [universe, interval])

  const data = status?.data
  const job  = status?.job
  const isRunning = job?.running || building

  return (
    <div className="flex items-center gap-2 px-3 py-1.5 bg-md-surface-con/60 border-b border-md-outline-var text-xs">
      <span className="text-md-on-surface-var/70">Pooled:</span>
      {isRunning ? (
        <span className="text-violet-400 animate-pulse">
          ⚡ {job?.done ?? '…'}/{job?.total ?? '…'} tickers…
        </span>
      ) : data?.available ? (
        <span className="text-md-on-surface-var/70">
          ✓ {data.ticker_count} tickers · {(data.tz_patterns + data.l_patterns).toLocaleString()} patterns
          {data.built_at && <span className="ml-1 text-gray-700">{new Date(data.built_at).toLocaleString()}</span>}
        </span>
      ) : (
        <span className="text-md-on-surface-var/70">—</span>
      )}
      {!isRunning && (
        <button onClick={build}
          className="ml-auto px-2 py-0.5 rounded bg-md-surface-high hover:bg-violet-700 text-md-on-surface-var hover:text-white text-xs transition-colors">
          {data?.available ? '↺' : '⚡ Build'}
        </button>
      )}
      {error && <span className="text-red-400 ml-1">{error}</span>}
    </div>
  )
}

// ── Helpers ───────────────────────────────────────────────────────────────────
const SOURCES        = [
  { key: 'ticker', label: 'This Ticker' },
  { key: 'pooled', label: 'SP500 Pooled' },
  { key: 'both',   label: 'Both' },
]
const UNIVERSES_POOL = ['sp500', 'nasdaq', 'russell2k']
const _UNI_LABEL     = { sp500: 'SP500 Pooled', nasdaq: 'NASDAQ Pooled', russell2k: 'R2K Pooled' }
const TF_OPTS_PRED   = ['1wk', '1d', '4h', '1h']

const _lsKey = (type, ticker, tf, uni = '') =>
  `sachoki_pred_${type}_${ticker}_${tf}${uni ? '_' + uni : ''}`
const _lsGet = (key) => {
  try { return JSON.parse(localStorage.getItem(key) || 'null') } catch { return null }
}
const _lsSet = (key, val) => {
  try { localStorage.setItem(key, JSON.stringify(val)) } catch {}
}

// All T/Z signal names for manual editing
const ALL_TZ_NAMES = [
  'NONE',
  'T1G','T1','T2G','T2','T3','T4','T5','T6','T9','T10','T11','T12',
  'Z1G','Z1','Z2G','Z2','Z3','Z4','Z5','Z6','Z9','Z10','Z11','Z12',
]

// ── Sequence bar slot component ───────────────────────────────────────────────
function SeqSlot({ label, sigName, active, onToggle, onChange, barIdx }) {
  const isT    = sigName && sigName.startsWith('T') && sigName !== 'NONE'
  const isZ    = sigName && sigName.startsWith('Z')
  const isNone = !sigName || sigName === 'NONE'
  return (
    <div className={`flex flex-col items-center gap-1 transition-opacity ${active ? '' : 'opacity-35'}`}>
      <span className="text-[9px] text-white/40 font-mono">{label}</span>
      {/* Signal chip with dropdown */}
      <div className="relative">
        <select
          value={sigName || 'NONE'}
          onChange={e => onChange(e.target.value)}
          className={`
            appearance-none cursor-pointer rounded-lg px-2 py-1 text-xs font-bold border
            focus:outline-none transition-colors min-w-[52px] text-center
            ${isT    ? 'bg-green-900/50 text-green-300 border-green-700/60'
            : isZ    ? 'bg-red-900/50   text-red-300   border-red-700/60'
            :          'bg-white/5      text-white/40  border-white/15'}
          `}>
          {ALL_TZ_NAMES.map(n => (
            <option key={n} value={n}
              className={n.startsWith('T') ? 'text-green-300 bg-gray-900'
                       : n.startsWith('Z') ? 'text-red-300 bg-gray-900'
                       : 'text-white/50 bg-gray-900'}>
              {n}
            </option>
          ))}
        </select>
      </div>
      {/* Toggle on/off */}
      <button onClick={onToggle}
        title={active ? 'Click to ignore this bar' : 'Click to include this bar'}
        className={`w-4 h-4 rounded-full border transition-colors ${
          active
            ? 'bg-md-primary border-md-primary/80'
            : 'bg-transparent border-white/20 hover:border-white/50'
        }`}
      />
    </div>
  )
}

// ── Outcome table (reused for both ticker and pooled) ─────────────────────────
function SeqOutcomeTable({ data, title, colorCls, badge }) {
  if (!data) return null
  const total = data.total_matches ?? 0
  // ── Next-bar prediction (from the outcome distribution) ────────────────────
  const tops    = data.top_outcomes || []
  const best    = tops[0] || null                      // single most likely next bar (may be NONE)
  const topSig  = tops.find(r => r.is_bull || r.is_bear) || null   // most likely actionable T/Z
  const sumBull = tops.filter(r => r.is_bull).reduce((a, r) => a + (r.pct || 0), 0)
  const sumBear = tops.filter(r => r.is_bear).reduce((a, r) => a + (r.pct || 0), 0)
  return (
    <div className="flex-1 min-w-0">
      <div className={`px-3 py-2 rounded-t-lg text-sm font-bold flex items-center justify-between ${colorCls}`}>
        <span>{title}</span>
        {badge && <span className="text-[10px] font-normal opacity-70">{badge}</span>}
      </div>
      <div className="border border-md-outline-var rounded-b-lg overflow-hidden">
        {/* Pattern label */}
        <div className="px-3 py-1.5 bg-md-surface-con border-b border-md-outline-var text-xs">
          <span className="font-mono text-white/70">{data.sequence_label || data.signals || '—'}</span>
        </div>
        {/* Match count */}
        <div className={`px-3 py-1.5 bg-md-surface-con border-b border-md-outline-var text-xs flex items-center gap-2
          ${total >= 50 ? 'text-lime-500' : total >= 15 ? 'text-yellow-500' : 'text-white/50'}`}>
          <span>{total} matches</span>
          {total >= 50 && <span className="text-[9px] bg-lime-900/40 px-1 rounded">high confidence</span>}
          {total >= 15 && total < 50 && <span className="text-[9px] bg-yellow-900/40 px-1 rounded">moderate</span>}
          {total > 0 && total < 15 && <span className="text-[9px] bg-md-surface-high px-1 rounded">low</span>}
          {/* Bull/bear split for ticker mode */}
          {data.bull_matches > 0 && (
            <span className="ml-auto text-[9px] text-white/50">
              <span className="text-lime-400">🟢{data.bull_bull_pct}%</span>
              <span className="text-white/30"> ({data.bull_matches}n) </span>
              <span className="text-red-400">🔴{data.bear_bull_pct}%</span>
              <span className="text-white/30"> ({data.bear_matches}n)</span>
            </span>
          )}
        </div>
        {/* ── Predicted next bar (headline) ── */}
        {total > 0 && best && (
          <div className={`px-3 py-2 border-b border-md-outline-var ${
            best.is_bull ? 'bg-green-950/40' : best.is_bear ? 'bg-red-950/40' : 'bg-md-surface-high/30'}`}>
            <div className="flex items-baseline gap-2">
              <span className="text-[10px] text-white/50">🎯 most likely next bar</span>
              <span className={`text-lg font-mono font-bold ${
                best.is_bull ? 'text-lime-300' : best.is_bear ? 'text-red-300' : 'text-white/70'}`}>
                {best.sig_name}
              </span>
              <span className="text-sm font-bold text-white/80">{best.pct}%</span>
              {/* if the #1 outcome is "no signal", surface the top actionable signal too */}
              {!best.is_bull && !best.is_bear && topSig && (
                <span className="text-[10px] text-white/45">
                  · top signal <span className={`font-mono font-semibold ${
                    topSig.is_bull ? 'text-lime-400' : 'text-red-400'}`}>{topSig.sig_name}</span> {topSig.pct}%
                </span>
              )}
              <span className="ml-auto text-[10px] font-mono">
                <span className="text-lime-400">↑{sumBull}%</span>
                <span className="text-white/30"> · </span>
                <span className="text-red-400">↓{sumBear}%</span>
              </span>
            </div>
          </div>
        )}
        {/* Results */}
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-white/40 bg-md-surface-con">
              <th className="px-3 py-1 text-left">#</th>
              <th className="px-3 py-1 text-left">Signal</th>
              <th className="px-3 py-1 text-right">Count</th>
              <th className="px-3 py-1 text-right">%</th>
            </tr>
          </thead>
          <tbody>
            {(!data.top_outcomes || data.top_outcomes.length === 0) && (
              <tr><td colSpan={4} className="px-3 py-4 text-center text-white/30 text-xs">
                {total === 0 ? 'Pattern not found — try deactivating older bars ●' : 'No data'}
              </td></tr>
            )}
            {(data.top_outcomes || []).map((row, i) => (
              <tr key={i} className={`border-t border-md-outline-var ${
                row.is_bull ? 'bg-green-950/30 text-green-300'
                : row.is_bear ? 'bg-red-950/30 text-red-300'
                : 'text-white/60'
              }`}>
                <td className="px-3 py-1.5 text-white/40">{i + 1}</td>
                <td className="px-3 py-1.5 font-mono font-semibold">{row.sig_name}</td>
                <td className="px-3 py-1.5 text-right">{row.count}</td>
                <td className="px-3 py-1.5 text-right font-bold">{row.pct}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Exact 5-Line Sequence Predictor — moved to StudioPanel as separate tab ──
// (UI lives in Studio → "Exact Sequence" tab, not in Predictor)

// ── Confluence Funnel component ───────────────────────────────────────────────
const CONF_UNI_OPTS = ['sp500', 'nasdaq', 'russell2k']
const CONF_N_OPTS   = [2, 3, 4, 5]
const CONF_LEVEL_COLORS = [
  'bg-violet-500', 'bg-blue-500', 'bg-teal-500', 'bg-green-500', 'bg-amber-500', 'bg-orange-500',
]
const CONF_LEVEL_TEXT = [
  'text-violet-400', 'text-blue-400', 'text-teal-400', 'text-green-400', 'text-amber-400', 'text-orange-400',
]

function ConfluenceCounter({ seqBars }) {
  const [bars,    setBars]    = useState(['NONE', 'NONE', 'NONE'])
  const [uni,     setUni]     = useState('sp500')
  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  const changeBar   = (i, v) => setBars(prev => prev.map((b, j) => j === i ? v : b))
  const changeN     = (n) => setBars(prev => {
    const cur = prev.length
    if (n > cur) return [...prev, ...Array(n - cur).fill('NONE')]
    return prev.slice(0, n)
  })

  const autoFill = () => {
    const active = seqBars.filter(b => b.active && b.sig !== 'NONE').map(b => b.sig)
    if (active.length === 0) return
    const n = Math.min(Math.max(active.length, 2), 5)
    setBars(active.slice(-n).concat(Array(Math.max(0, n - active.length)).fill('NONE')))
    setResult(null)
  }

  const run = async () => {
    setLoading(true); setError(null); setResult(null)
    try {
      const barsPayload = bars.map(b => (b && b !== 'NONE') ? b : null)
      const r = await api.studioConfluence({ bars: barsPayload, universe: uni })
      if (r.error) { setError(r.error); return }
      setResult(r)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  const maxCount = result?.levels?.[0]?.count ?? 1

  return (
    <div className="rounded-xl border border-md-outline-var overflow-hidden">
      {/* Header */}
      <div className="px-3 py-2 bg-indigo-900/30 border-b border-md-outline-var flex items-center justify-between">
        <span className="text-sm font-bold text-indigo-300">Confluence Funnel</span>
        <span className="text-[10px] text-white/40">how rare does the sequence get as signals stack</span>
      </div>

      {/* Bar editor */}
      <div className="px-4 py-3 flex items-end justify-center gap-5 bg-md-surface/30">
        {bars.map((sig, i) => {
          const isT = sig && sig.startsWith('T') && sig !== 'NONE'
          const isZ = sig && sig.startsWith('Z')
          return (
            <div key={i} className="flex flex-col items-center gap-1">
              <span className="text-[9px] text-white/40 font-mono">
                {i === bars.length - 1 ? 'bar 0' : `bar -${bars.length - 1 - i}`}
              </span>
              <select
                value={sig || 'NONE'}
                onChange={e => changeBar(i, e.target.value)}
                className={`appearance-none cursor-pointer rounded-lg px-2 py-1 text-xs font-bold border
                  focus:outline-none transition-colors min-w-[52px] text-center
                  ${isT ? 'bg-green-900/50 text-green-300 border-green-700/60'
                  : isZ ? 'bg-red-900/50   text-red-300   border-red-700/60'
                  :        'bg-white/5      text-white/40  border-white/15'}`}>
                {ALL_TZ_NAMES.map(n => (
                  <option key={n} value={n}
                    className={n.startsWith('T') ? 'text-green-300 bg-gray-900'
                              : n.startsWith('Z') ? 'text-red-300 bg-gray-900'
                              : 'text-white/50 bg-gray-900'}>
                    {n}
                  </option>
                ))}
              </select>
            </div>
          )
        })}
      </div>

      {/* Controls row */}
      <div className="px-4 py-2 bg-md-surface-con/40 border-t border-md-outline-var flex items-center gap-2 flex-wrap">
        {/* Bar count */}
        <div className="flex gap-1">
          {CONF_N_OPTS.map(n => (
            <button key={n} onClick={() => changeN(n)}
              className={`px-2 py-0.5 rounded text-[10px] font-mono transition-colors
                ${bars.length === n ? 'bg-indigo-600 text-white' : 'bg-md-surface-high text-white/50 hover:text-white'}`}>
              {n}b
            </button>
          ))}
        </div>
        {/* Universe */}
        <select value={uni} onChange={e => setUni(e.target.value)}
          className="bg-md-surface-high border border-md-outline-var rounded text-[11px] text-white/70 px-2 py-0.5 focus:outline-none">
          {CONF_UNI_OPTS.map(u => <option key={u} value={u}>{u}</option>)}
        </select>
        {/* Auto-fill */}
        <button onClick={autoFill}
          className="px-2 py-0.5 rounded text-[10px] bg-md-surface-high text-white/50 hover:text-white transition-colors">
          ↺ From sequence
        </button>
        {/* Run */}
        <button onClick={run} disabled={loading}
          className="ml-auto px-3 py-1 rounded-lg text-xs font-medium bg-indigo-700 hover:bg-indigo-600 text-white disabled:opacity-50 transition-colors flex items-center gap-1">
          {loading
            ? <><svg className="animate-spin h-3 w-3" fill="none" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.4 0 0 5.4 0 12h4z"/></svg> Querying…</>
            : '▶ Run'}
        </button>
      </div>

      {/* Error */}
      {error && (
        <div className="px-4 py-2 text-red-400 text-xs border-t border-md-outline-var">{error}</div>
      )}

      {/* Funnel results */}
      {result && (
        <div className="border-t border-md-outline-var">
          {/* Sequence label + baseline */}
          <div className="px-4 py-2 bg-md-surface-con/50 flex items-center gap-3 text-[10px] text-white/50 border-b border-md-outline-var">
            <span className="font-mono text-white/70">{result.sequence_label}</span>
            <span className="ml-auto">universe: {result.baseline?.toLocaleString()} bars</span>
          </div>
          {/* Level rows */}
          <div className="divide-y divide-md-outline-var/50">
            {result.levels.map((lvl, idx) => {
              const barPct = maxCount > 0 ? (lvl.count / maxCount) * 100 : 0
              const colCls = CONF_LEVEL_COLORS[idx % CONF_LEVEL_COLORS.length]
              const txtCls = CONF_LEVEL_TEXT[idx % CONF_LEVEL_TEXT.length]
              return (
                <div key={lvl.level} className="px-4 py-2 hover:bg-white/2 transition-colors">
                  <div className="flex items-center gap-3">
                    {/* Level badge */}
                    <span className={`text-[9px] font-bold w-5 text-center ${txtCls}`}>L{lvl.level}</span>
                    {/* Label */}
                    <span className="text-xs text-white/75 flex-1 truncate">{lvl.label}</span>
                    {/* Count */}
                    <span className={`text-xs font-mono font-bold ${txtCls} min-w-[56px] text-right`}>
                      {lvl.count.toLocaleString()}
                    </span>
                    {/* pct of prev */}
                    <span className="text-[10px] text-white/40 min-w-[46px] text-right font-mono">
                      {idx === 0 ? `${lvl.pct_total}%` : `${lvl.pct_prev}% ↓`}
                    </span>
                  </div>
                  {/* Progress bar */}
                  <div className="mt-1.5 h-1 bg-white/5 rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all duration-500 ${colCls}`}
                      style={{ width: `${Math.max(barPct, 0.3)}%` }}
                    />
                  </div>
                </div>
              )
            })}
          </div>
          {/* Rarity summary */}
          {result.levels.length >= 2 && (
            <div className="px-4 py-2 bg-md-surface/30 border-t border-md-outline-var text-[10px] text-white/40 flex items-center gap-2">
              <span>Rarity:</span>
              <span className="font-mono text-white/60">
                {result.levels[0].count.toLocaleString()} → {result.levels[result.levels.length-1].count.toLocaleString()} bars
              </span>
              <span className="ml-auto">
                full confluence: {result.levels[result.levels.length-1].pct_total}% of universe
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  )
}


export default function PredictorPanel({ ticker, tf }) {
  const [localTf,      setLocalTf]      = useState(tf || '1d')
  const [tickerData,   setTickerData]   = useState(null)
  const [pooledData,   setPooledData]   = useState(null)
  const [loading,      setLoading]      = useState(false)
  const [error,        setError]        = useState(null)
  const [source,       setSource]       = useState('both')
  const [poolUni,      setPoolUni]      = useState('sp500')
  const [view,         setView]         = useState('stats')
  const [matrixOffset, setMatrixOffset] = useState(1)

  // ── 5-bar sequence state ──────────────────────────────────────────────────
  // Each slot: { sig: 'T1' | 'NONE', active: bool }
  const [seqBars, setSeqBars] = useState([
    { sig: 'NONE', active: false },
    { sig: 'NONE', active: false },
    { sig: 'NONE', active: false },
    { sig: 'NONE', active: false },
    { sig: 'NONE', active: false },
  ])
  const [seqTickerResult,  setSeqTickerResult]  = useState(null)
  const [seqPooledResult,  setSeqPooledResult]  = useState(null)
  const [seqLoading,       setSeqLoading]       = useState(false)

  // ── Fetch ticker data (populates seqBars from last_tz_signals) ────────────
  const fetchTicker = useCallback(() => {
    if (!ticker) return
    setError(null); setLoading(true)
    api.predict(ticker, localTf)
      .then(d => {
        setTickerData(d)
        // Populate sequence from last 5 signals — activate only last 2 by default
        // (5-bar exact match is too rare in a single ticker; user can toggle more)
        if (d?.last_tz_signals?.length) {
          const n = d.last_tz_signals.length
          setSeqBars(
            d.last_tz_signals.map((sig, i) => ({
              sig: sig || 'NONE',
              active: sig !== 'NONE' && i >= n - 2, // last 2 bars active by default
            }))
          )
        }
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [ticker, localTf])

  const fetchPooled = useCallback(() => {
    if (!ticker) return
    api.pooledPredict(ticker, localTf, poolUni)
      .then(d => { if (!d.error) setPooledData(d) })
      .catch(() => {})
  }, [ticker, localTf, poolUni])

  useEffect(() => { fetchTicker() }, [fetchTicker])
  useEffect(() => { fetchPooled() }, [fetchPooled])

  // ── Run sequence query ────────────────────────────────────────────────────
  const runSeqQuery = useCallback(async () => {
    const activeSeq = seqBars.map(b => b.active ? b.sig : null)
    // Only include a slot if it's active AND has a real signal
    const sequence  = activeSeq.map(s => (s && s !== 'NONE') ? s : null)
    const hasAny    = sequence.some(s => s !== null)

    setSeqLoading(true)
    setSeqTickerResult(null)
    setSeqPooledResult(null)

    try {
      const runs = []
      if ((source === 'ticker' || source === 'both') && ticker) {
        runs.push(
          api.predictSequence(ticker, { sequence, tf: localTf })
            .then(d => setSeqTickerResult(d))
            .catch(() => {})
        )
      }
      if (source === 'pooled' || source === 'both') {
        runs.push(
          api.studioSigSequence({ sequence, universe: poolUni })
            .then(d => setSeqPooledResult(d))
            .catch(() => {})
        )
      }
      await Promise.all(runs)
    } finally {
      setSeqLoading(false)
    }
  }, [seqBars, source, ticker, localTf, poolUni])

  // Auto-run when sequence or source changes
  useEffect(() => {
    if (tickerData) runSeqQuery()
  }, [seqBars, source, poolUni])

  const td = tickerData
  const pd = pooledData
  const showTicker = source === 'ticker' || source === 'both'
  const showPooled = source === 'pooled' || source === 'both'
  const empty = { pattern: '', signals: '', total_matches: 0, top_outcomes: [] }

  const updateSlot = (i, field, val) =>
    setSeqBars(prev => prev.map((b, j) => j === i ? { ...b, [field]: val } : b))

  return (
    <div className="bg-md-surface-con rounded-md-md border border-md-outline-var">

      {/* ── Header ── */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-md-outline-var flex-wrap gap-2">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="font-semibold text-sm">
            Next-Bar Predictor — {ticker}
            {td?.current_regime === 'bull' && (
              <span className="ml-2 text-[9px] bg-lime-900/50 text-lime-400 px-1.5 py-0.5 rounded font-normal">🟢 Bull Regime</span>
            )}
            {td?.current_regime === 'bear' && (
              <span className="ml-2 text-[9px] bg-red-900/50 text-red-400 px-1.5 py-0.5 rounded font-normal">🔴 Bear Regime</span>
            )}
          </span>
          <div className="flex gap-0.5 border border-md-outline-var rounded p-0.5">
            {TF_OPTS_PRED.map(t => (
              <button key={t} onClick={() => setLocalTf(t)}
                className={`px-2 py-0.5 rounded text-xs font-medium transition-colors
                  ${localTf === t ? 'bg-blue-600 text-white' : 'text-white/50 hover:text-white'}`}>
                {t.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          {loading && <span className="text-xs text-white/50 animate-pulse">loading…</span>}
          {!loading && (
            <button onClick={() => { fetchTicker(); fetchPooled() }}
              className="px-2 py-0.5 rounded text-xs bg-md-surface-high text-white/50 hover:text-white transition-colors">
              ↺
            </button>
          )}
          {error && <span className="text-xs text-red-400">{error}</span>}
          <div className="flex gap-0.5">
            {UNIVERSES_POOL.map(u => (
              <button key={u} onClick={() => setPoolUni(u)}
                className={`px-2 py-0.5 rounded text-xs transition-colors
                  ${poolUni === u ? 'bg-indigo-700 text-white' : 'bg-md-surface-high text-white/50 hover:text-white'}`}>
                {u === 'sp500' ? 'SP500' : u === 'nasdaq' ? 'NASDAQ' : 'R2K'}
              </button>
            ))}
          </div>
          <div className="flex gap-0.5 border border-md-outline-var rounded p-0.5">
            {SOURCES.map(s => (
              <button key={s.key} onClick={() => setSource(s.key)}
                className={`px-2 py-0.5 rounded text-xs transition-colors
                  ${source === s.key ? 'bg-blue-600 text-white' : 'text-white/50 hover:text-white'}`}>
                {s.label}
              </button>
            ))}
          </div>
          <div className="flex gap-0.5 border border-md-outline-var rounded p-0.5">
            {['stats','matrix'].map(v => (
              <button key={v} onClick={() => setView(v)}
                className={`px-2 py-0.5 rounded text-xs transition-colors
                  ${view === v ? 'bg-violet-700 text-white' : 'text-white/50 hover:text-white'}`}>
                {v.charAt(0).toUpperCase() + v.slice(1)}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* ── Pooled status bar ── */}
      {showPooled && <PooledStatusBar universe={poolUni} interval={localTf} onBuildDone={fetchPooled} />}

      {/* ── Matrix view ── */}
      {view === 'matrix' && (
        <div className="p-3">
          <div className="flex gap-1 mb-3">
            {[1,2].map(o => (
              <button key={o} onClick={() => setMatrixOffset(o)}
                className={`px-3 py-1 rounded text-xs font-medium transition-colors
                  ${matrixOffset === o ? 'bg-indigo-600 text-white' : 'bg-md-surface-high text-white/50 hover:text-white'}`}>
                Bar +{o}
              </button>
            ))}
            <span className="ml-2 text-[10px] text-white/40 self-center">% = how often each signal appears after the row signal</span>
          </div>
          {showTicker && <TZMatrix matrixData={td?.tz_matrix} label={ticker} sublabel={ticker} offset={matrixOffset} />}
          {showPooled && <TZMatrix matrixData={pd?.bench_tz_matrix} label="T/Z Transition Matrix" sublabel={pd?.bench_tz_stats?.bench_ticker ?? poolUni} offset={matrixOffset} />}
        </div>
      )}

      {/* ── Stats view ── */}
      {view === 'stats' && (
        <div className="p-3 space-y-4">

          {/* ════════════════════════════════════════════════════
              5-Bar Sequence Builder
          ════════════════════════════════════════════════════ */}
          <div className="rounded-xl border border-md-outline-var overflow-hidden">
            <div className="px-3 py-2 bg-violet-900/30 border-b border-md-outline-var flex items-center justify-between">
              <span className="text-sm font-bold text-violet-300">T/Z Sequence Predictor</span>
              <div className="flex items-center gap-2">
                <span className="text-[10px] text-white/40">
                  ● = include bar in pattern · click to toggle
                </span>
                <button
                  onClick={() => {
                    const sigs = td?.last_tz_signals ?? []
                    const n = sigs.length
                    setSeqBars(sigs.map((sig, i) => ({
                      sig: sig || 'NONE',
                      active: sig !== 'NONE' && i >= n - 2,
                    })))
                  }}
                  className="px-2 py-0.5 rounded text-[10px] bg-md-surface-high text-white/50 hover:text-white transition-colors">
                  ↺ Reset from ticker
                </button>
              </div>
            </div>

            {/* Sequence slots */}
            <div className="px-4 py-3 flex items-end justify-center gap-6 bg-md-surface/30">
              {seqBars.map((bar, i) => (
                <SeqSlot
                  key={i}
                  label={i === 4 ? 'bar 0 (now)' : `bar -${4-i}`}
                  sigName={bar.sig}
                  active={bar.active}
                  onToggle={() => updateSlot(i, 'active', !bar.active)}
                  onChange={v  => updateSlot(i, 'sig', v)}
                  barIdx={i}
                />
              ))}
            </div>

            {/* Active sequence preview */}
            <div className="px-4 py-2 bg-md-surface-con/40 border-t border-md-outline-var flex items-center gap-3">
              <span className="text-[10px] text-white/40 font-mono">
                Pattern: {seqBars.map(b => b.active && b.sig !== 'NONE' ? b.sig : '?').join(' → ')}
              </span>
              <button
                onClick={runSeqQuery}
                disabled={seqLoading}
                className="ml-auto px-3 py-1 rounded-lg text-xs font-medium bg-violet-700 hover:bg-violet-600 text-white disabled:opacity-50 transition-colors flex items-center gap-1">
                {seqLoading
                  ? <><svg className="animate-spin h-3 w-3" fill="none" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.4 0 0 5.4 0 12h4z"/></svg> Querying…</>
                  : '▶ Query'}
              </button>
            </div>

            {/* Sequence results */}
            {(seqTickerResult || seqPooledResult) && (
              <div className="flex gap-3 p-3 border-t border-md-outline-var">
                {showTicker && seqTickerResult && (
                  <SeqOutcomeTable
                    data={seqTickerResult}
                    title="T/Z Sequence — This Ticker"
                    colorCls="bg-blue-900/50 text-blue-300"
                    badge={ticker}
                  />
                )}
                {showPooled && seqPooledResult && (
                  <SeqOutcomeTable
                    data={seqPooledResult}
                    title="T/Z Sequence — Studio Pooled"
                    colorCls="bg-violet-900/50 text-violet-300"
                    badge={`${_UNI_LABEL[poolUni]} · 5yr`}
                  />
                )}
              </div>
            )}
          </div>

          {/* ════════════════════════════════════════════════════
              Confluence Funnel
          ════════════════════════════════════════════════════ */}
          <ConfluenceCounter seqBars={seqBars} />

          {/* ════════════════════════════════════════════════════
              Classic 2-bar & 3-bar (legacy, kept for reference)
          ════════════════════════════════════════════════════ */}
          <div className="flex gap-3">
            {showTicker && <TZOutcomeTable data={td?.tz_3bar ?? empty} title="T/Z 3-Bar" color="bg-blue-900/50 text-blue-300" />}
            {showPooled && <TZOutcomeTable data={pd?.tz_3bar ?? empty} title="T/Z 3-Bar" color="bg-blue-800/40 text-blue-200" pooled poolLabel={_UNI_LABEL[poolUni] ?? 'Pooled'} />}
          </div>
          <div className="flex gap-3">
            {showTicker && <TZOutcomeTable data={td?.tz_2bar ?? empty} title="T/Z 2-Bar" color="bg-orange-900/50 text-orange-300" />}
            {showPooled && <TZOutcomeTable data={pd?.tz_2bar ?? empty} title="T/Z 2-Bar" color="bg-orange-800/40 text-orange-200" pooled poolLabel={_UNI_LABEL[poolUni] ?? 'Pooled'} />}
          </div>
          <div className="flex gap-3">
            {showTicker && <LOutcomeTable data={td?.l_3bar ?? empty} title="L-Signal 3-Bar" color="bg-teal-900/50 text-teal-300" />}
            {showPooled && <LOutcomeTable data={pd?.l_3bar ?? empty} title="L-Signal 3-Bar" color="bg-teal-800/40 text-teal-200" pooled poolLabel={_UNI_LABEL[poolUni] ?? 'Pooled'} />}
          </div>
          <div className="flex gap-3">
            {showTicker && <LOutcomeTable data={td?.l_2bar ?? empty} title="L-Signal 2-Bar" color="bg-amber-900/50 text-amber-300" />}
            {showPooled && <LOutcomeTable data={pd?.l_2bar ?? empty} title="L-Signal 2-Bar" color="bg-amber-800/40 text-amber-200" pooled poolLabel={_UNI_LABEL[poolUni] ?? 'Pooled'} />}
          </div>

          <TZStatsSection
            tickerStats={td?.tz_stats}
            benchStats={pd?.bench_tz_stats}
            ticker={ticker}
            showTicker={showTicker}
            showPooled={showPooled}
          />
        </div>
      )}

      {!ticker && (
        <div className="pb-6 text-center text-white/40 text-sm">Select a ticker</div>
      )}
    </div>
  )
}
