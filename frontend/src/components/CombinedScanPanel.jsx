import { useState, useEffect, useCallback, useMemo } from 'react'
import { api } from '../api'
import { exportToTV } from '../utils/exportTickers'

// ── Score sub-tabs ────────────────────────────────────────────────────────────
const SCORE_TABS = [
  { id: 'all',    label: 'All',       min: 0 },
  { id: 'bull',   label: 'Bull ≥4',   min: 4 },
  { id: 'strong', label: 'Strong ≥6', min: 6 },
  { id: 'fire',   label: 'Fire ≥8',   min: 8 },
  { id: 'bear',   label: 'Bear ≥3',   min: 0, bear: true },
]

// ── Signal badge definitions ───────────────────────────────────────────────────
const VABS_BULL = [
  { key: 'best_sig',   label: 'BEST',   cls: 'bg-lime-800 text-lime-200 ring-1 ring-lime-400' },
  { key: 'strong_sig', label: 'STR',    cls: 'bg-emerald-800 text-emerald-200' },
  { key: 'vbo_up',     label: 'VBO↑',  cls: 'bg-green-700 text-white' },
  { key: 'abs_sig',    label: 'ABS',    cls: 'bg-teal-800 text-teal-200' },
  { key: 'climb_sig',  label: 'CLB',    cls: 'bg-cyan-800 text-cyan-200' },
  { key: 'load_sig',   label: 'LD',     cls: 'bg-blue-800 text-blue-200' },
]

const WYK_BULL = [
  { key: 'ns', label: 'NS', cls: 'bg-teal-900 text-teal-300' },
  { key: 'sq', label: 'SQ', cls: 'bg-cyan-900 text-cyan-300' },
  { key: 'sc', label: 'SC', cls: 'bg-orange-900 text-orange-300' },
]

const WYK_BEAR = [
  { key: 'bc', label: 'BC', cls: 'bg-red-900 text-red-300' },
  { key: 'nd', label: 'ND', cls: 'bg-pink-900 text-pink-300' },
  { key: 'vbo_dn', label: 'VBO↓', cls: 'bg-red-800 text-red-200' },
]

const COMBO_BULL = [
  { key: 'rocket',   label: '🚀',      cls: 'bg-red-900 text-red-200 font-bold' },
  { key: 'buy_2809', label: 'BUY',     cls: 'bg-lime-800 text-lime-200' },
  { key: 'sig3g',    label: '3G',      cls: 'bg-cyan-800 text-cyan-200' },
  { key: 'rtv',      label: 'RTV',     cls: 'bg-blue-800 text-blue-200' },
  { key: 'hilo_buy', label: 'HILO↑',  cls: 'bg-green-800 text-green-200' },
  { key: 'atr_brk',  label: 'ATR↑',   cls: 'bg-emerald-800 text-emerald-200' },
  { key: 'bb_brk',   label: 'BB↑',    cls: 'bg-teal-800 text-teal-200' },
]

const WLNBB_SIGS = [
  { key: 'FRI34',    label: 'FRI34',  cls: 'text-cyan-300' },
  { key: 'L34',      label: 'L34',    cls: 'text-blue-300' },
  { key: 'L43',      label: 'L43',    cls: 'text-teal-300' },
  { key: 'L64',      label: 'L64',    cls: 'text-orange-400' },
  { key: 'L22',      label: 'L22',    cls: 'text-red-400' },
  { key: 'CCI_READY',label: 'CCI',    cls: 'text-violet-300' },
  { key: 'BLUE',     label: 'BL',     cls: 'text-sky-300' },
  { key: 'BO_UP',    label: 'BO↑',   cls: 'text-lime-300' },
  { key: 'BX_UP',    label: 'BX↑',   cls: 'text-lime-400' },
  { key: 'BO_DN',    label: 'BO↓',   cls: 'text-rose-400' },
  { key: 'BX_DN',    label: 'BX↓',   cls: 'text-rose-500' },
]

const BUCKET_COLORS = {
  W:  { bg: '#3a3a4a', text: '#c3c0d3' },
  L:  { bg: '#0a3a5a', text: '#88ccff' },
  N:  { bg: '#3a3000', text: '#ffd000' },
  B:  { bg: '#3a1800', text: '#e48100' },
  VB: { bg: '#3a0000', text: '#ff6060' },
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function scoreBg(score, isBear = false) {
  if (isBear) return score >= 3 ? 'bg-red-950/30' : ''
  if (score >= 8) return 'bg-yellow-950/40'
  if (score >= 6) return 'bg-green-950/30'
  if (score >= 4) return 'bg-green-950/15'
  return ''
}

function ScoreBadge({ score, isBear = false }) {
  const base = isBear
    ? 'bg-red-900/70 text-red-200'
    : score >= 8 ? 'bg-yellow-800/80 text-yellow-200 ring-1 ring-yellow-400'
    : score >= 6 ? 'bg-green-800/80 text-green-200 ring-1 ring-green-400'
    : score >= 4 ? 'bg-green-900/60 text-green-300'
    : 'bg-gray-800 text-gray-400'
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-xs font-bold font-mono ${base}`}>
      {score}
    </span>
  )
}

function Badges({ row, defs, lSignalStr }) {
  const active = defs.filter(d => {
    const key = d.key
    // Check in row top-level (VABS/combo)
    if (key in row) return Boolean(row[key])
    // Check in l_combo string or l_signal (WLNBB)
    if (lSignalStr) return lSignalStr.includes(key)
    return false
  })
  if (!active.length) return null
  return (
    <span className="flex flex-wrap gap-0.5">
      {active.map(d => (
        <span key={d.key} className={`text-xs px-1 rounded ${d.cls}`}>{d.label}</span>
      ))}
    </span>
  )
}

function TZBadge({ sig_id, sig_name }) {
  if (!sig_name || sig_name === 'NONE') return null
  const bull = sig_id >= 1 && sig_id <= 11
  return (
    <span className={`text-xs px-1.5 py-0.5 rounded font-mono font-semibold
      ${bull ? 'bg-green-900/60 text-green-300' : 'bg-red-900/60 text-red-300'}`}>
      {sig_name}
    </span>
  )
}

function BucketBadge({ bucket }) {
  if (!bucket) return null
  const c = BUCKET_COLORS[bucket]
  if (!c) return <span className="text-xs text-gray-500">{bucket}</span>
  return (
    <span className="text-xs font-bold px-1 rounded font-mono"
      style={{ backgroundColor: c.bg, color: c.text }}>
      {bucket}
    </span>
  )
}

function WLNBBBadges({ l_signal, l_combo }) {
  // l_signal is the top-priority label; l_combo has all active
  const active = WLNBB_SIGS.filter(s => {
    if (l_signal === s.key) return true
    return l_combo && l_combo.includes(s.key)
  })
  if (!active.length) return null
  return (
    <span className="flex flex-wrap gap-0.5">
      {active.map(s => (
        <span key={s.key} className={`text-xs font-mono ${s.cls}`}>{s.label}</span>
      ))}
    </span>
  )
}

// ── Main component ────────────────────────────────────────────────────────────

export default function CombinedScanPanel({ tf, onSelectTicker }) {
  const [tab, setTab]           = useState('bull')
  const [allResults, setAll]    = useState([])
  const [lastScan, setLastScan] = useState(null)
  const [loading, setLoading]   = useState(false)
  const [scanning, setScanning] = useState(false)
  const [error, setError]       = useState(null)

  const activeTab = SCORE_TABS.find(t => t.id === tab) || SCORE_TABS[0]

  const load = useCallback(() => {
    setLoading(true)
    setError(null)
    // Load all results (min_score=0, tab=all) then filter client-side
    api.combinedScan(tf, 0, 'all', 500)
      .then(d => {
        setAll(d.results || [])
        setLastScan(d.last_scan)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [tf])

  useEffect(() => { load() }, [load])

  const results = useMemo(() => {
    if (tab === 'bear') return allResults.filter(r => (r.bear_score || 0) >= 3)
    if (tab === 'fire') return allResults.filter(r => (r.bull_score || 0) >= 8)
    if (tab === 'strong') return allResults.filter(r => (r.bull_score || 0) >= 6)
    if (tab === 'bull') return allResults.filter(r => (r.bull_score || 0) >= 4)
    return allResults
  }, [allResults, tab])

  const _poll = () => {
    const iv = setInterval(() => {
      api.scanStatus()
        .then(s => {
          if (!s.running) { clearInterval(iv); setScanning(false); load() }
        })
        .catch(() => { clearInterval(iv); setScanning(false) })
    }, 2000)
    setTimeout(() => { clearInterval(iv); setScanning(false); load() }, 300_000)
  }

  const scan = () => {
    setScanning(true)
    setError(null)
    api.scanTrigger(tf)
      .then(() => _poll())
      .catch(e => { setError(e.message); setScanning(false) })
  }

  const fmtTime = iso => {
    if (!iso) return null
    try { return new Date(iso).toLocaleString() } catch { return iso }
  }

  return (
    <div className="flex flex-col h-full bg-gray-950 text-gray-100 text-xs">
      {/* ── Toolbar ── */}
      <div className="flex items-center justify-between gap-2 px-3 py-2 border-b border-gray-800 flex-wrap">
        <div className="flex items-center gap-2 flex-wrap">
          <button
            onClick={scan} disabled={scanning}
            className={`px-3 py-1 rounded text-xs font-semibold transition-colors
              ${scanning ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                         : 'bg-indigo-600 hover:bg-indigo-500 text-white'}`}
          >
            {scanning ? <span className="animate-pulse">● Scanning…</span> : '▶ Scan'}
          </button>

          {/* Score sub-tabs */}
          {SCORE_TABS.map(t => (
            <button key={t.id} onClick={() => setTab(t.id)}
              className={`px-2 py-0.5 rounded text-xs transition-colors
                ${tab === t.id
                  ? t.bear ? 'bg-red-700 text-white font-semibold'
                           : 'bg-blue-700 text-white font-semibold'
                  : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
              {t.label}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-2">
          {results.length > 0 && (
            <button
              onClick={() => exportToTV(results.map(r => r.ticker), 'combined_scan.txt')}
              className="text-xs px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
              title="Export for TradingView watchlist"
            >
              Export TV
            </button>
          )}
          <span className="text-gray-600">
            {results.length}/{allResults.length}
            {lastScan && ` · ${fmtTime(lastScan)}`}
          </span>
        </div>
      </div>

      {/* ── Legend row ── */}
      <div className="flex flex-wrap gap-x-3 gap-y-0.5 px-3 py-1.5 border-b border-gray-800/50 bg-gray-900/30 text-gray-500">
        <span className="text-lime-300/70">BEST=vol+NS/SQ+L34</span>
        <span className="text-emerald-300/70">STR=2×vol</span>
        <span className="text-green-300/70">VBO↑=price break</span>
        <span className="text-teal-300/70">ABS=bucket jump</span>
        <span className="text-cyan-300/70">CLB=gradual rise</span>
        <span className="text-blue-300/70">LD=vol spike</span>
        <span className="text-teal-400/70">NS=no supply</span>
        <span className="text-red-400/70">BC=buy climax</span>
      </div>

      {scanning && (
        <div className="px-4 py-1.5 border-b border-gray-800 bg-indigo-950/30 text-indigo-300">
          <span className="animate-pulse">●</span> Scanning — gamocdileba 1–3 wuti…
        </div>
      )}
      {error && <div className="px-4 py-1.5 text-red-400 border-b border-gray-800">{error}</div>}

      {/* ── Table ── */}
      <div className="overflow-auto flex-1">
        {loading ? (
          <div className="py-8 text-center text-gray-500">Loading…</div>
        ) : results.length === 0 ? (
          <div className="py-8 text-center text-gray-600">
            No results — press ▶ Scan or lower the filter
          </div>
        ) : (
          <table className="w-full border-collapse">
            <thead className="sticky top-0 bg-gray-900 z-10">
              <tr className="text-gray-500 text-left">
                <th className="px-2 py-1.5 font-medium">Ticker</th>
                <th className="px-2 py-1.5 font-medium text-center">Score</th>
                <th className="px-2 py-1.5 font-medium text-center">T/Z</th>
                <th className="px-2 py-1.5 font-medium text-center hidden sm:table-cell">Bkt</th>
                <th className="px-2 py-1.5 font-medium">VABS</th>
                <th className="px-2 py-1.5 font-medium">Wyck</th>
                <th className="px-2 py-1.5 font-medium">Combo</th>
                <th className="px-2 py-1.5 font-medium hidden md:table-cell">L-Sig</th>
                <th className="px-2 py-1.5 font-medium text-right">Price</th>
                <th className="px-2 py-1.5 font-medium text-right">%</th>
              </tr>
            </thead>
            <tbody>
              {results.map((row, i) => {
                const isBear = tab === 'bear'
                const displayScore = isBear ? (row.bear_score || 0) : (row.bull_score || 0)
                return (
                  <tr
                    key={i}
                    onClick={() => onSelectTicker?.(row.ticker)}
                    className={`border-b border-gray-800/40 cursor-pointer hover:bg-gray-800/50
                      ${scoreBg(displayScore, isBear)}`}
                  >
                    <td className="px-2 py-1 font-mono font-semibold text-blue-300">
                      {row.ticker}
                    </td>
                    <td className="px-2 py-1 text-center">
                      <ScoreBadge score={displayScore} isBear={isBear} />
                    </td>
                    <td className="px-2 py-1 text-center">
                      <TZBadge sig_id={row.sig_id} sig_name={row.sig_name} />
                    </td>
                    <td className="px-1 py-1 text-center hidden sm:table-cell">
                      <BucketBadge bucket={row.vol_bucket} />
                    </td>
                    <td className="px-2 py-1">
                      <div className="flex flex-wrap gap-0.5">
                        {VABS_BULL.filter(d => row[d.key]).map(d => (
                          <span key={d.key} className={`px-1 rounded text-xs ${d.cls}`}>{d.label}</span>
                        ))}
                      </div>
                    </td>
                    <td className="px-2 py-1">
                      <div className="flex flex-wrap gap-0.5">
                        {[...WYK_BULL, ...WYK_BEAR].filter(d => row[d.key]).map(d => (
                          <span key={d.key} className={`px-1 rounded text-xs ${d.cls}`}>{d.label}</span>
                        ))}
                      </div>
                    </td>
                    <td className="px-2 py-1">
                      <div className="flex flex-wrap gap-0.5">
                        {COMBO_BULL.filter(d => row[d.key]).map(d => (
                          <span key={d.key} className={`px-1 rounded text-xs ${d.cls}`}>{d.label}</span>
                        ))}
                        {row.wick_bull ? (
                          <span className="px-1 rounded text-xs bg-emerald-900 text-emerald-200">W↑</span>
                        ) : null}
                      </div>
                    </td>
                    <td className="px-2 py-1 hidden md:table-cell">
                      <WLNBBBadges l_signal={row.l_signal} l_combo={row.l_combo} />
                    </td>
                    <td className="px-2 py-1 text-right font-mono text-gray-200">
                      ${(row.last_price || 0).toFixed(2)}
                    </td>
                    <td className={`px-2 py-1 text-right font-mono
                      ${(row.change_pct || 0) >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
                      {(row.change_pct || 0) >= 0 ? '+' : ''}{(row.change_pct || 0).toFixed(2)}%
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
