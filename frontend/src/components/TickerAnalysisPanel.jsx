import { useState } from 'react'
import { api } from '../api'
import PredictorPanel from './PredictorPanel'
import CandleChart from './CandleChart'

// ── Engine family detection (mirrors TurboScanPanel logic) ───────────────────
function engineFamilies(r) {
  const fams = new Set()
  if (r.best_sig || r.strong_sig || r.vbo_up || r.abs_sig || r.ns || r.sq || r.load_sig || r.va)
    fams.add('Vol')
  if (r.wyk_spring || r.wyk_sos || r.wyk_lps || r.wyk_accum || r.wyk_markup)
    fams.add('Wyk')
  if (r.d_spring || r.d_strong_bull || r.d_absorb_bull || r.d_blast_bull || r.d_surge_bull)
    fams.add('Δ')
  if (r.tz_sig || r.tz_bull_flip || r.tz_attempt)
    fams.add('T/Z')
  if (r.fri34 || r.fri43 || r.l34 || r.preup66 || r.preup55)
    fams.add('L')
  if (r.rocket || r.buy_2809 || r.seq_bcont)
    fams.add('Cmb')
  if (r.fbo_bull || r.eb_bull || r.rs_strong || r.ultra_3up)
    fams.add('Brk')
  return fams
}

function setupPhase(r) {
  const early = r.wyk_spring || r.d_spring || r.tz_bull_flip
  const late  = (r.rocket || r.buy_2809) && (r.fbo_bull || r.eb_bull || r.vbo_up)
  if (early && !late) return 'Early'
  if (late)           return 'Late'
  return null
}

function scoreColor(s) {
  if (s >= 65) return 'text-lime-300 font-bold'
  if (s >= 50) return 'text-yellow-300 font-semibold'
  if (s >= 35) return 'text-blue-300'
  return 'text-gray-500'
}

function scoreBg(s) {
  if (s >= 65) return 'bg-lime-900/20'
  if (s >= 50) return 'bg-yellow-900/15'
  if (s >= 35) return 'bg-blue-900/10'
  return ''
}

// ── Signal badge ──────────────────────────────────────────────────────────────
function Badge({ label, cls }) {
  return <span className={`px-1.5 py-0.5 rounded text-[10px] leading-tight ${cls}`}>{label}</span>
}

// ── Tier badge (A / B / context) ─────────────────────────────────────────────
function TierBadge({ label, tier }) {
  const cls = tier === 'A'
    ? 'bg-lime-900/50 text-lime-300 ring-1 ring-lime-600'
    : tier === 'B'
    ? 'bg-sky-900/40 text-sky-300 ring-1 ring-sky-700'
    : 'bg-gray-800 text-gray-400'
  return (
    <span className={`inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] leading-tight ${cls}`}>
      <span className="text-[8px] opacity-60">{tier}</span>
      <span className="font-mono font-semibold">{label}</span>
    </span>
  )
}

// ── Signal grid — shows all active signals grouped by family ─────────────────
function SignalGrid({ r }) {
  const groups = [
    {
      label: 'VABS / Vol', tier: 'A',
      items: [
        r.best_sig    && ['BEST★', 'A'],
        r.strong_sig  && ['STR',   'A'],
        r.vbo_up      && ['VBO↑',  'A'],
        r.abs_sig     && ['ABS',   'B'],
        r.ns          && ['NS',    'B'],
        r.sq          && ['SQ',    'B'],
        r.load_sig    && ['LD',    'B'],
        r.va          && ['VA',    'B'],
        r.sig_l88     && ['L88',   'B'],
      ].filter(Boolean),
    },
    {
      label: 'Wyckoff', tier: 'A',
      items: [
        r.wyk_spring  && ['wSPR',  'A'],
        r.wyk_sos     && ['SOS',   'A'],
        r.wyk_lps     && ['LPS',   'B'],
        r.wyk_accum   && ['ACC',   'B'],
        r.wyk_markup  && ['MKP',   'B'],
        r.wyk_sc      && ['wSC',   null],
        r.wyk_ar      && ['AR',    null],
        r.wyk_st      && ['ST',    null],
      ].filter(Boolean),
    },
    {
      label: 'Delta / Order Flow', tier: 'A',
      items: [
        r.d_spring       && ['dSPR',  'A'],
        r.d_blast_bull   && ['ΔΔ↑',   'A'],
        r.d_surge_bull   && ['Δ↑',    'B'],
        r.d_strong_bull  && ['B/S↑',  'B'],
        r.d_absorb_bull  && ['Ab↑',   'B'],
        r.d_div_bull     && ['T↓',    null],
        r.d_vd_div_bull  && ['NS',    null],
      ].filter(Boolean),
    },
    {
      label: 'T/Z Candlestick', tier: 'A',
      items: [
        r.tz_sig         && [r.tz_sig,  ['T4','T6','T1G','T2G'].includes(r.tz_sig) ? 'A' : 'B'],
        r.tz_bull_flip   && ['TZ→3',   'B'],
        r.tz_attempt     && ['TZ→2',   null],
        r.ca             && ['CA',     null],
        r.cd             && ['CD',     null],
        r.cw             && ['CW',     null],
      ].filter(Boolean),
    },
    {
      label: 'WLNBB / L-structure', tier: 'A',
      items: [
        r.fri34   && ['FRI34',  'A'],
        r.fri43   && ['FRI43',  'B'],
        r.l34     && ['L34',    'B'],
        r.preup66 && ['P66',    'A'],
        r.preup55 && ['P55',    'B'],
        r.blue    && ['BLUE',   null],
      ].filter(Boolean),
    },
    {
      label: 'Combo / 2809', tier: 'A',
      items: [
        r.rocket     && ['🚀',    'A'],
        r.buy_2809   && ['BUY',   'A'],
        r.seq_bcont  && ['SBC',   'B'],
        r.sig3g      && ['3G',    null],
        r.rtv        && ['RTV',   null],
        r.hilo_buy   && ['HILO↑', null],
        r.atr_brk    && ['ATR↑',  null],
        r.bb_brk     && ['BB↑',   null],
      ].filter(Boolean),
    },
    {
      label: 'Breakout / ULTRA', tier: 'B',
      items: [
        r.fbo_bull   && ['FBO↑',  'A'],
        r.eb_bull    && ['EB↑',   'A'],
        r.rs_strong  && ['RS+',   'B'],
        r.rs         && ['RS',    null],
        r.ultra_3up  && ['3↑',    null],
      ].filter(Boolean),
    },
    {
      label: 'B / G Signals', tier: null,
      items: [
        ...[1,2,3,4,5,6,7,8,9,10,11].filter(i => r[`b${i}`]).map(i => [`B${i}`, i === 1 || i === 10 ? 'B' : null]),
        ...['g1','g2','g4','g6','g11'].filter(k => r[k]).map(k => [k.toUpperCase(), 'B']),
      ],
    },
  ]

  return (
    <div className="space-y-3">
      {groups.map(g => g.items.length > 0 && (
        <div key={g.label}>
          <div className="text-[10px] text-gray-600 uppercase tracking-wide mb-1">{g.label}</div>
          <div className="flex flex-wrap gap-1">
            {g.items.map(([lbl, tier]) => (
              <TierBadge key={lbl} label={lbl} tier={tier} />
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

const TF_OPTS = ['1wk', '1d', '4h', '1h', '30m']

// ─────────────────────────────────────────────────────────────────────────────
export default function TickerAnalysisPanel({ onAddToWatchlist }) {
  const [input,   setInput]   = useState('')
  const [ticker,  setTicker]  = useState('')
  const [tf,      setTf]      = useState('1d')
  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)
  const [added,   setAdded]   = useState(false)

  const analyze = (sym = input, timeframe = tf) => {
    const t = sym.trim().toUpperCase()
    if (!t) return
    setTicker(t)
    setAdded(false)
    setLoading(true); setError(null); setResult(null)
    api.turboAnalyze(t, timeframe)
      .then(d => { setResult(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }

  const changeTf = (t) => {
    setTf(t)
    if (ticker) analyze(ticker, t)
  }

  const handleAdd = () => {
    if (result?.ticker && onAddToWatchlist) {
      onAddToWatchlist(result.ticker)
      setAdded(true)
    }
  }

  const onKeyDown = (e) => { if (e.key === 'Enter') analyze() }

  const r = result
  const chartTicker = ticker || null   // show chart as soon as ticker is set

  const n     = r ? engineFamilies(r).size : 0
  const cross = n >= 4 ? '⚡×4' : n === 3 ? '⚡×3' : n === 2 ? '⚡×2' : ''
  const phase = r ? setupPhase(r) : null

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 text-xs text-gray-300">

      {/* ── Header / input ─────────────────────────────────────────────── */}
      <div className="flex flex-wrap items-center gap-3 px-4 py-3 border-b border-gray-800">
        <span className="font-semibold text-sm text-white">Ticker Analysis</span>
        <span className="text-gray-500">Full Turbo engine on any ticker</span>

        {/* Ticker input */}
        <div className="flex items-center gap-1 ml-auto">
          <input
            value={input}
            onChange={e => { setInput(e.target.value.toUpperCase()) }}
            onKeyDown={onKeyDown}
            placeholder="e.g. RGTI"
            className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-white
                       placeholder-gray-600 focus:outline-none focus:border-blue-500 w-24"
          />
          <button
            onClick={() => analyze()}
            className="px-3 py-1 rounded bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold"
          >
            Analyze
          </button>
        </div>

        {/* TF selector */}
        <div className="flex gap-0.5">
          {TF_OPTS.map(t => (
            <button key={t} onClick={() => changeTf(t)}
              className={`px-2 py-0.5 rounded text-xs ${tf === t ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
              {t.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      {/* ── Chart (shown as soon as a ticker is entered) ────────────────── */}
      {chartTicker && (
        <div className="border-b border-gray-800">
          <CandleChart ticker={chartTicker} tf={tf} />
        </div>
      )}

      {loading && (
        <div className="px-4 py-6 text-center text-gray-500 animate-pulse">Analyzing {ticker}…</div>
      )}
      {error && (
        <div className="px-4 py-4 text-red-400">{error}</div>
      )}

      {!chartTicker && !loading && !r && !error && (
        <div className="px-4 py-10 text-center text-gray-600">
          Enter a ticker and press Analyze or Enter
        </div>
      )}

      {r && (
        <div className="p-4 space-y-5">

          {/* ── Score summary ─────────────────────────────────────────── */}
          <div className={`rounded-lg p-4 border border-gray-700 ${scoreBg(r.turbo_score)}`}>
            <div className="flex items-center gap-3 mb-1 flex-wrap">
              <span className="text-2xl font-bold text-white">{r.ticker}</span>
              <span className={`text-xl font-bold ${scoreColor(r.turbo_score)}`}>
                {r.turbo_score?.toFixed(1)}
              </span>
              <span className="text-gray-500">Turbo Score</span>
              {cross && <span className="text-lime-300 font-semibold">{cross}</span>}
              {phase === 'Early' && (
                <span className="text-sky-300 text-[10px] bg-sky-900/30 px-1.5 py-0.5 rounded">[E] Early</span>
              )}
              {phase === 'Late' && (
                <span className="text-orange-300 text-[10px] bg-orange-900/30 px-1.5 py-0.5 rounded">[L] Late</span>
              )}
              {r.data_source === 'yfinance' && (
                <span className="text-orange-400/60 text-[9px]">yf</span>
              )}

              {/* ── Add to watchlist button ── */}
              {onAddToWatchlist && (
                <button
                  onClick={handleAdd}
                  disabled={added}
                  className={`ml-auto px-3 py-1 rounded text-xs font-semibold transition-colors
                    ${added
                      ? 'bg-lime-800/50 text-lime-400 cursor-default'
                      : 'bg-gray-700 hover:bg-lime-700 text-gray-300 hover:text-white'}`}
                >
                  {added ? '✓ Added' : '+ Watchlist'}
                </button>
              )}
            </div>

            <div className="flex items-center gap-4 text-xs text-gray-400 mt-1">
              <span>${r.last_price?.toFixed(2)}</span>
              <span className={r.change_pct >= 0 ? 'text-lime-400' : 'text-red-400'}>
                {r.change_pct >= 0 ? '+' : ''}{r.change_pct?.toFixed(2)}%
              </span>
              {r.tz_sig && (
                <span className={`font-mono font-semibold ${['T4','T6','T1G','T2G'].includes(r.tz_sig) ? 'text-violet-300' : 'text-gray-400'}`}>
                  {r.tz_sig}
                </span>
              )}
              {r.rsi != null && (
                <span className="text-gray-500">RSI {r.rsi?.toFixed(0)}</span>
              )}
              {r.br_score > 0 && (
                <span className={r.br_score >= 71 ? 'text-lime-400' : 'text-gray-500'}>
                  BR {r.br_score?.toFixed(0)}
                </span>
              )}
              {n > 0 && (
                <span className="text-gray-600">
                  {n} engine {n === 1 ? 'family' : 'families'} active
                </span>
              )}
            </div>
          </div>

          {/* ── Active signals ────────────────────────────────────────── */}
          <div>
            <div className="text-xs text-gray-500 font-semibold uppercase tracking-wide mb-2">
              Active Signals
              <span className="ml-2 font-normal text-gray-600 normal-case">
                <span className="text-lime-300/70">A</span> = Tier A (strong surfaced) ·{' '}
                <span className="text-sky-300/70">B</span> = context amplifier
              </span>
            </div>
            <SignalGrid r={r} />
          </div>

          {/* ── Predictor ─────────────────────────────────────────────── */}
          <div>
            <div className="text-xs text-gray-500 font-semibold uppercase tracking-wide mb-2">
              Predictor
            </div>
            <PredictorPanel ticker={r.ticker} tf={tf} />
          </div>

        </div>
      )}
    </div>
  )
}
