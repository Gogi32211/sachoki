import { useState, useEffect, useMemo } from 'react'
import { api } from '../api'

// ── Universes ─────────────────────────────────────────────────────────────────
const UNIVERSES = [
  { key: 'sp500',      label: 'S&P 500',       desc: '~700 tickers',         cls: 'text-blue-300'   },
  { key: 'nasdaq_low', label: 'NASDAQ $3–20',  desc: 'NASDAQ, low-price',    cls: 'text-cyan-300'   },
  { key: 'nasdaq_mid', label: 'NASDAQ $21–50', desc: 'NASDAQ, mid-price',    cls: 'text-teal-300'   },
  { key: 'russell2k',  label: 'Russell 2000',  desc: 'IWM small-caps',       cls: 'text-orange-300' },
]

// ── Timeframes ────────────────────────────────────────────────────────────────
const TF_OPTS = ['1w', '1d', '4h', '1h']

// ── Score thresholds ──────────────────────────────────────────────────────────
const SCORE_THRESHOLDS = [
  { label: 'All',      value: 0  },
  { label: '≥ 20',     value: 20 },
  { label: '≥ 35',     value: 35 },
  { label: '≥ 50',     value: 50 },
  { label: 'Fire ≥65', value: 65 },
]

// ── Direction filter ──────────────────────────────────────────────────────────
const DIR_OPTS = [
  { key: 'all',  label: 'ALL'  },
  { key: 'bull', label: 'BULL' },
  { key: 'bear', label: 'BEAR' },
]

// ── Signal group quick-filters ────────────────────────────────────────────────
const SIG_GROUPS = [
  { key: 'best_sig',   label: 'BEST★',  cls: 'text-lime-300'   },
  { key: 'strong_sig', label: 'STRONG', cls: 'text-emerald-300' },
  { key: 'rocket',     label: '🚀',     cls: 'text-red-300'    },
  { key: 'buy_2809',   label: 'BUY',    cls: 'text-lime-400'   },
  { key: 'sig3g',      label: '3G',     cls: 'text-cyan-300'   },
  { key: 'vbo_up',     label: 'VBO↑',  cls: 'text-green-300'  },
  { key: 'fri34',      label: 'FRI34',  cls: 'text-cyan-400'   },
  { key: 'ns',         label: 'NS',     cls: 'text-teal-300'   },
  { key: 'sq',         label: 'SQ',     cls: 'text-cyan-400'   },
  { key: 'wick_bull',  label: 'WICK↑', cls: 'text-emerald-400'},
  { key: 'best_long',  label: 'BEST↑', cls: 'text-yellow-300' },
  { key: 'sig_l88',    label: 'L88',    cls: 'text-violet-300' },
  { key: 'sig_260308', label: '260308', cls: 'text-purple-300' },
  { key: 'fbo_bull',   label: 'FBO↑',  cls: 'text-sky-300'    },
  { key: 'eb_bull',    label: 'EB↑',   cls: 'text-amber-300'  },
  { key: 'bf_buy',     label: '4BF',    cls: 'text-pink-300'   },
]

// ── T/Z weight map (for display colour) ──────────────────────────────────────
const TZ_STRONG = new Set(['T4','T6','T1G','T2G'])
const TZ_BEAR   = new Set(['Z4','Z6','Z1G','Z2G','Z1','Z2','Z3','Z5','Z7','Z8','Z9','Z10','Z11','Z12'])

// ── Colour helpers ────────────────────────────────────────────────────────────
function scoreColor(s) {
  if (s >= 65) return 'text-lime-300 font-bold'
  if (s >= 50) return 'text-yellow-300 font-semibold'
  if (s >= 35) return 'text-blue-300'
  if (s >= 20) return 'text-gray-300'
  return 'text-gray-600'
}

function scoreBg(s) {
  if (s >= 65) return 'bg-lime-900/25'
  if (s >= 50) return 'bg-yellow-900/15'
  if (s >= 35) return 'bg-blue-900/10'
  return ''
}

function brColor(v) {
  if (v >= 71) return 'text-lime-400'
  if (v >= 50) return 'text-yellow-300'
  if (v >= 30) return 'text-gray-400'
  return 'text-gray-600'
}

// ── Badge component ───────────────────────────────────────────────────────────
function Badge({ label, cls }) {
  return <span className={`px-1 rounded text-[10px] leading-tight ${cls}`}>{label}</span>
}

// ── fmt helper ────────────────────────────────────────────────────────────────
const fmt = (v, d = 2) => v == null ? '—' : Number(v).toFixed(d)

// ─────────────────────────────────────────────────────────────────────────────
export default function TurboScanPanel({ onSelectTicker }) {
  const [localTf,    setLocalTf]    = useState('1d')
  const [universe,   setUniverse]   = useState('sp500')
  const [allResults, setAllResults] = useState([])
  const [lastScan,   setLastScan]   = useState(null)
  const [scanning,   setScanning]   = useState(false)
  const [error,      setError]      = useState(null)
  const [minScore,   setMinScore]   = useState(0)
  const [direction,  setDirection]  = useState('bull')
  const [selSigs,    setSelSigs]    = useState(new Set())   // AND filter
  const [exported,   setExported]   = useState(false)

  const load = (tf = localTf, uni = universe) => {
    api.turboScan(500, 0, 'all', tf, uni)
      .then(d => { setAllResults(d.results || []); setLastScan(d.last_scan) })
      .catch(e => setError(e.message))
  }

  useEffect(() => { load(localTf, universe) }, [localTf, universe])

  // ── Client-side filter ─────────────────────────────────────────────────────
  const results = useMemo(() => {
    return allResults.filter(r => {
      if (r.turbo_score < minScore) return false
      if (direction === 'bull' && !r.tz_bull) return false
      if (direction === 'bear' && r.tz_bull)  return false
      if (selSigs.size > 0 && ![...selSigs].every(k => r[k])) return false
      return true
    })
  }, [allResults, minScore, direction, selSigs])

  const toggleSig = key => setSelSigs(prev => {
    const n = new Set(prev)
    n.has(key) ? n.delete(key) : n.add(key)
    return n
  })

  const exportTickers = () => {
    const tickers = results.map(r => r.ticker).join(',')
    navigator.clipboard.writeText(tickers).then(() => {
      setExported(true)
      setTimeout(() => setExported(false), 2000)
    })
  }

  // ── Poll until done ────────────────────────────────────────────────────────
  const _poll = () => {
    const tf  = localTf
    const uni = universe
    const iv  = setInterval(() => {
      api.turboScanStatus()
        .then(s => {
          if (!s.running) { clearInterval(iv); setScanning(false); load(tf, uni) }
        })
        .catch(() => { clearInterval(iv); setScanning(false) })
    }, 2000)
    setTimeout(() => { clearInterval(iv); setScanning(false); load(tf, uni) }, 360_000)
  }

  const scan = () => {
    setScanning(true); setError(null)
    api.turboScanTrigger(localTf, universe)
      .then(() => _poll())
      .catch(e => {
        setScanning(false)
        const msg = e?.detail || e?.message || String(e)
        if (msg.includes('409') || msg.toLowerCase().includes('already running')) {
          setError('Another scan is in progress — wait for it to finish, then try again')
        } else {
          setError(msg)
        }
      })
  }

  return (
    <div className="flex flex-col h-full bg-gray-950 text-gray-100 text-xs">

      {/* ── Row 0: Universe selector ── */}
      <div className="flex flex-wrap items-center gap-1.5 px-3 py-2 border-b border-gray-800 bg-gray-900/50">
        <span className="text-gray-500 text-xs w-16 shrink-0">Universe</span>
        {UNIVERSES.map(u => (
          <button key={u.key}
            onClick={() => { setUniverse(u.key); setAllResults([]) }}
            title={u.desc}
            className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border
              ${universe === u.key
                ? `${u.cls} border-current bg-gray-800`
                : 'text-gray-500 border-gray-700 hover:text-gray-300 hover:border-gray-500'}`}>
            {u.label}
          </button>
        ))}
        <span className="text-gray-600 text-xs ml-1">
          {universe === 'nasdaq_low' && '· price $3–20'}
          {universe === 'nasdaq_mid' && '· price $21–50'}
          {universe === 'russell2k'  && '· small-cap IWM'}
        </span>
      </div>

      {/* ── Row 1: TF + Scan + Direction + Score ── */}
      <div className="flex flex-wrap items-center gap-2 px-3 py-2 border-b border-gray-800">

        {/* TF selector */}
        <div className="flex gap-0.5 border border-gray-700 rounded p-0.5">
          {TF_OPTS.map(t => (
            <button key={t} onClick={() => setLocalTf(t)}
              className={`px-2 py-0.5 rounded text-xs font-medium transition-colors
                ${localTf === t ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white'}`}>
              {t.toUpperCase()}
            </button>
          ))}
        </div>

        {/* Scan button */}
        <button onClick={scan} disabled={scanning}
          className={`px-3 py-1 rounded text-xs font-semibold transition-colors
            ${scanning ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                       : 'bg-violet-600 hover:bg-violet-500 text-white'}`}>
          {scanning ? <span className="animate-pulse">⚡ Scanning…</span> : '⚡ TURBO'}
        </button>

        {/* Export button */}
        <button onClick={exportTickers} disabled={results.length === 0}
          title="Copy tickers to clipboard (TradingView watchlist)"
          className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border
            ${exported
              ? 'border-lime-500 text-lime-300 bg-lime-900/30'
              : results.length === 0
                ? 'border-gray-700 text-gray-600 cursor-not-allowed'
                : 'border-gray-600 text-gray-300 hover:border-gray-400 hover:text-white'}`}>
          {exported ? '✓ Copied' : '⬇ Export'}
        </button>

        {/* Direction */}
        <div className="flex gap-0.5">
          {DIR_OPTS.map(d => (
            <button key={d.key} onClick={() => setDirection(d.key)}
              className={`px-2 py-0.5 rounded text-xs transition-colors
                ${direction === d.key ? 'bg-indigo-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
              {d.label}
            </button>
          ))}
        </div>

        {/* Score threshold */}
        <div className="flex gap-0.5 ml-1">
          {SCORE_THRESHOLDS.map(t => (
            <button key={t.value} onClick={() => setMinScore(t.value)}
              className={`px-2 py-0.5 rounded text-xs transition-colors
                ${minScore === t.value ? 'bg-amber-600 text-black font-semibold' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
              {t.label}
            </button>
          ))}
        </div>

        {/* Stats */}
        <span className="ml-auto text-gray-600 shrink-0">
          {results.length} / {allResults.length}
          {lastScan && ` · ${lastScan.slice(0,16).replace('T',' ')}`}
        </span>
      </div>

      {/* ── Row 2: Signal AND filter ── */}
      <div className="flex flex-wrap items-center gap-1 px-3 py-1.5 border-b border-gray-800 bg-gray-900/30">
        <span className="text-gray-500 w-8 shrink-0">SIG</span>
        <button onClick={() => setSelSigs(new Set())}
          className={`px-2 py-0.5 rounded text-xs ${selSigs.size === 0 ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
          All
        </button>
        {SIG_GROUPS.map(s => (
          <button key={s.key} onClick={() => toggleSig(s.key)}
            className={`px-2 py-0.5 rounded text-xs transition-colors
              ${selSigs.has(s.key) ? `${s.cls} bg-gray-700 font-semibold` : 'bg-gray-800 text-gray-500 hover:text-white'}`}>
            {s.label}
          </button>
        ))}
      </div>

      {/* Progress / error */}
      {scanning && (
        <div className="px-4 py-1.5 border-b border-gray-800 bg-violet-950/30 text-violet-300 animate-pulse">
          ⚡ TURBO — {UNIVERSES.find(u => u.key === universe)?.label ?? universe} — 2-4 minutes…
        </div>
      )}
      {error && <div className="px-4 py-1.5 text-red-400 border-b border-gray-800">{error}</div>}

      {/* ── Table ── */}
      <div className="overflow-auto flex-1">
        <table className="w-full border-collapse">
          <thead className="sticky top-0 bg-gray-900 z-10 text-gray-500 text-left">
            <tr>
              <th className="px-2 py-1.5 font-medium">Ticker</th>
              <th className="px-2 py-1.5 font-medium text-center">Score</th>
              <th className="px-2 py-1.5 font-medium text-center">T/Z</th>
              <th className="px-2 py-1.5 font-medium">VABS</th>
              <th className="px-2 py-1.5 font-medium">Wyck</th>
              <th className="px-2 py-1.5 font-medium">Combo</th>
              <th className="px-2 py-1.5 font-medium">L-Sig / Ultra</th>
              <th className="px-2 py-1.5 font-medium text-center">RSI</th>
              <th className="px-2 py-1.5 font-medium text-center">CCI</th>
              <th className="px-2 py-1.5 font-medium text-center">BR%</th>
              <th className="px-2 py-1.5 font-medium text-right">Price</th>
              <th className="px-2 py-1.5 font-medium text-right">%</th>
            </tr>
          </thead>
          <tbody>
            {results.map(r => (
              <tr key={r.ticker}
                className={`border-b border-gray-800/50 hover:bg-gray-800/40 cursor-pointer ${scoreBg(r.turbo_score)}`}
                onClick={() => onSelectTicker?.(r.ticker)}>

                {/* Ticker */}
                <td className="px-2 py-1 font-mono font-semibold text-blue-300">
                  {r.ticker}
                  {r.vol_bucket && (
                    <span className="ml-1 text-gray-600 font-normal">{r.vol_bucket}</span>
                  )}
                </td>

                {/* Score */}
                <td className={`px-2 py-1 text-center font-mono font-bold text-sm ${scoreColor(r.turbo_score)}`}>
                  {fmt(r.turbo_score, 1)}
                </td>

                {/* T/Z signal */}
                <td className="px-2 py-1 text-center">
                  {r.tz_sig ? (
                    <span className={`font-mono font-semibold ${TZ_STRONG.has(r.tz_sig) ? 'text-lime-300' : TZ_BEAR.has(r.tz_sig) ? 'text-red-400' : 'text-blue-300'}`}>
                      {r.tz_sig}
                    </span>
                  ) : '—'}
                </td>

                {/* VABS */}
                <td className="px-2 py-1">
                  <div className="flex flex-wrap gap-0.5">
                    {r.best_sig   ? <Badge label="BEST★" cls="bg-lime-800 text-lime-200 ring-1 ring-lime-500" /> : null}
                    {r.strong_sig && !r.best_sig ? <Badge label="STR"  cls="bg-emerald-800 text-emerald-200" /> : null}
                    {r.vbo_up     ? <Badge label="VBO↑" cls="bg-green-700 text-white" /> : null}
                    {r.vbo_dn     ? <Badge label="VBO↓" cls="bg-red-800 text-red-200" /> : null}
                    {r.abs_sig    ? <Badge label="ABS"  cls="bg-teal-800 text-teal-200" /> : null}
                    {r.climb_sig  ? <Badge label="CLB"  cls="bg-cyan-800 text-cyan-200" /> : null}
                    {r.load_sig   ? <Badge label="LD"   cls="bg-blue-800 text-blue-200" /> : null}
                  </div>
                </td>

                {/* Wyckoff */}
                <td className="px-2 py-1">
                  <div className="flex flex-wrap gap-0.5">
                    {r.ns ? <Badge label="NS" cls="bg-teal-900 text-teal-300" /> : null}
                    {r.sq ? <Badge label="SQ" cls="bg-cyan-900 text-cyan-300" /> : null}
                    {r.sc ? <Badge label="SC" cls="bg-orange-900 text-orange-300" /> : null}
                    {r.bc ? <Badge label="BC" cls="bg-rose-900 text-rose-300" /> : null}
                    {r.nd ? <Badge label="ND" cls="bg-pink-900 text-pink-300" /> : null}
                  </div>
                </td>

                {/* Combo */}
                <td className="px-2 py-1">
                  <div className="flex flex-wrap gap-0.5">
                    {r.rocket    ? <Badge label="🚀"    cls="bg-red-900 text-red-200 font-bold" /> : null}
                    {r.buy_2809  ? <Badge label="BUY"   cls="bg-lime-800 text-lime-200" /> : null}
                    {r.sig3g     ? <Badge label="3G"    cls="bg-cyan-800 text-cyan-200" /> : null}
                    {r.rtv       ? <Badge label="RTV"   cls="bg-blue-800 text-blue-200" /> : null}
                    {r.hilo_buy  ? <Badge label="HILO↑" cls="bg-green-800 text-green-200" /> : null}
                    {r.atr_brk   ? <Badge label="ATR↑"  cls="bg-emerald-800 text-emerald-200" /> : null}
                    {r.bb_brk    ? <Badge label="BB↑"   cls="bg-teal-800 text-teal-200" /> : null}
                    {r.bias_up   ? <Badge label="↑BIAS" cls="bg-green-900 text-green-300" /> : null}
                    {r.hilo_sell ? <Badge label="HILO↓" cls="bg-rose-900 text-rose-300" /> : null}
                    {r.bias_down ? <Badge label="↓BIAS" cls="bg-red-900 text-red-300" /> : null}
                  </div>
                </td>

                {/* L-signals / WLNBB / Ultra */}
                <td className="px-2 py-1">
                  <div className="flex flex-wrap gap-0.5">
                    {r.fri34      ? <Badge label="FRI34" cls="text-cyan-300 bg-cyan-900/40" /> : null}
                    {r.fri43      ? <Badge label="FRI43" cls="text-sky-300 bg-sky-900/40" /> : null}
                    {r.l34  && !r.fri34 ? <Badge label="L34"  cls="text-blue-300 bg-blue-900/30" /> : null}
                    {r.l43  && !r.fri43 ? <Badge label="L43"  cls="text-teal-300 bg-teal-900/30" /> : null}
                    {r.l64        ? <Badge label="L64"  cls="text-orange-400 bg-orange-900/30" /> : null}
                    {r.l22        ? <Badge label="L22"  cls="text-red-400 bg-red-900/30" /> : null}
                    {r.blue       ? <Badge label="BL"   cls="text-sky-300 bg-sky-900/30" /> : null}
                    {r.cci_ready  ? <Badge label="CCI"  cls="text-violet-300 bg-violet-900/30" /> : null}
                    {r.bo_up      ? <Badge label="BO↑"  cls="text-lime-300 bg-lime-900/30" /> : null}
                    {r.bx_up      ? <Badge label="BX↑"  cls="text-lime-400 bg-lime-900/20" /> : null}
                    {r.wick_bull  ? <Badge label="WK↑"  cls="text-emerald-300 bg-emerald-900/30" /> : null}
                    {r.cisd_ppm   ? <Badge label="C++-" cls="text-green-300 bg-green-900/30" /> : null}
                    {r.cisd_seq   ? <Badge label="C++--" cls="text-lime-300 bg-lime-900/20" /> : null}
                    {r.fuchsia_rl ? <Badge label="RL"   cls="text-fuchsia-300 bg-fuchsia-900/30" /> : null}
                    {/* Ultra v2 */}
                    {r.best_long  ? <Badge label="BEST↑" cls="text-yellow-200 bg-yellow-800/60 ring-1 ring-yellow-500" /> : null}
                    {r.fbo_bull && !r.best_long ? <Badge label="FBO↑" cls="text-sky-300 bg-sky-900/40" /> : null}
                    {r.eb_bull    ? <Badge label="EB↑"  cls="text-amber-300 bg-amber-900/30" /> : null}
                    {r.bf_buy     ? <Badge label="4BF"  cls="text-pink-300 bg-pink-900/30" /> : null}
                    {r.ultra_3up  ? <Badge label="3↑"   cls="text-lime-300 bg-lime-900/20" /> : null}
                    {/* 260308 */}
                    {r.sig_l88    ? <Badge label="L88"  cls="text-violet-200 bg-violet-800/50 ring-1 ring-violet-500" /> : null}
                    {r.sig_260308 && !r.sig_l88 ? <Badge label="260308" cls="text-purple-300 bg-purple-900/30" /> : null}
                  </div>
                </td>

                {/* RSI */}
                <td className={`px-2 py-1 text-center font-mono text-xs
                  ${r.rsi >= 70 ? 'text-red-400' : r.rsi <= 30 ? 'text-lime-400' : 'text-gray-400'}`}>
                  {r.rsi != null ? fmt(r.rsi, 0) : '—'}
                </td>

                {/* CCI */}
                <td className={`px-2 py-1 text-center font-mono text-xs
                  ${r.cci >= 100 ? 'text-lime-400' : r.cci <= -100 ? 'text-red-400' : 'text-gray-400'}`}>
                  {r.cci != null ? fmt(r.cci, 0) : '—'}
                </td>

                {/* BR% */}
                <td className={`px-2 py-1 text-center font-mono ${brColor(r.br_score)}`}>
                  {fmt(r.br_score, 0)}
                </td>

                {/* Price */}
                <td className="px-2 py-1 text-right font-mono text-gray-200">
                  ${fmt(r.last_price)}
                </td>

                {/* Change % */}
                <td className={`px-2 py-1 text-right font-mono ${r.change_pct >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
                  {r.change_pct >= 0 ? '+' : ''}{fmt(r.change_pct)}%
                </td>
              </tr>
            ))}

            {results.length === 0 && !scanning && (
              <tr>
                <td colSpan={12} className="px-4 py-10 text-center text-gray-600">
                  {allResults.length > 0
                    ? 'No tickers match current filters'
                    : 'Press ⚡ TURBO to scan all engines'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
