import { useState } from 'react'
import { api } from '../api'
import PredictorPanel from './PredictorPanel'

// ── Engine family detection (mirrors TurboScanPanel logic) ───────────────────
function engineFamilies(r) {
  const fams = new Set()
  if (r.best_sig || r.strong_sig || r.vbo_up || r.abs_sig || r.ns || r.sq || r.load_sig || r.va)
    fams.add('Vol')
  if (r.wyk_spring || r.wyk_sos || r.wyk_lps || r.wyk_accum || r.wyk_markup)
    fams.add('Wyk')
  if (r.d_spring || r.d_strong_bull || r.d_absorb_bull || r.d_blast_bull || r.d_surge_bull || r.d_flip_bull || r.d_orange_bull)
    fams.add('Δ')
  if (r.tz_sig || r.tz_bull_flip || r.tz_attempt)
    fams.add('T/Z')
  if (r.fri34 || r.fri43 || r.l34 || r.preup66 || r.preup55 || r.preup3 || r.preup2 || r.preup50)
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

// ── Tier badge with optional age indicator ────────────────────────────────────
// age=0 → current bar (bright), age>0 → dimmed + "Nd" label
function TierBadge({ label, tier, age }) {
  const isOld = age > 0
  const cls = tier === 'A'
    ? isOld ? 'bg-lime-900/20 text-lime-400/50 ring-1 ring-lime-700/40'
            : 'bg-lime-900/50 text-lime-300 ring-1 ring-lime-600'
    : tier === 'B'
    ? isOld ? 'bg-sky-900/20 text-sky-400/50 ring-1 ring-sky-800/40'
            : 'bg-sky-900/40 text-sky-300 ring-1 ring-sky-700'
    : isOld ? 'bg-gray-800/50 text-gray-600'
            : 'bg-gray-800 text-gray-400'
  return (
    <span className={`inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] leading-tight ${cls}`}>
      {tier && <span className="text-[8px] opacity-50">{tier}</span>}
      <span className="font-mono font-semibold">{label}</span>
      {isOld && <span className="text-[8px] opacity-60 ml-0.5">{age}b</span>}
    </span>
  )
}

// ── Signal grid — N-bar lookback using sig_ages ───────────────────────────────
// ages = parsed sig_ages dict; N = lookback in bars
// signal is shown if ages[key] < N (age 0 = current bar, 1 = 1 bar ago, etc.)
function SignalGrid({ r, ages, N }) {
  // For signals without age tracking (delta, tz_state derived), fall back to row boolean
  const age  = (key) => ages?.[key] ?? (r[key] ? 0 : 999)
  const show = (key) => age(key) < N

  const groups = [
    {
      label: 'VABS / Vol', items: [
        show('best_sig')   && ['best_sig',   'BEST★', 'A'],
        show('strong_sig') && ['strong_sig', 'STR',   'A'],
        show('vbo_up')     && ['vbo_up',     'VBO↑',  'A'],
        show('abs_sig')    && ['abs_sig',    'ABS',   'B'],
        show('ns')         && ['ns',         'NS',    'B'],
        show('sq')         && ['sq',         'SQ',    'B'],
        show('load_sig')   && ['load_sig',   'LD',    'B'],
        show('va')         && ['va',         'VA',    'B'],
        show('sig_l88')    && ['sig_l88',    'L88',   'B'],
      ].filter(Boolean),
    },
    {
      label: 'Wyckoff', items: [
        show('wyk_spring') && ['wyk_spring', 'wSPR', 'A'],
        show('wyk_sos')    && ['wyk_sos',    'SOS',  'A'],
        show('wyk_lps')    && ['wyk_lps',    'LPS',  'B'],
        show('wyk_accum')  && ['wyk_accum',  'ACC',  'B'],
        show('wyk_markup') && ['wyk_markup', 'MKP',  'B'],
        show('wyk_sc')     && ['wyk_sc',     'wSC',  null],
        show('wyk_ar')     && ['wyk_ar',     'AR',   null],
        show('wyk_st')     && ['wyk_st',     'ST',   null],
      ].filter(Boolean),
    },
    {
      label: 'Delta / Order Flow', items: [
        show('d_spring')      && ['d_spring',      'dSPR', 'A'],
        show('d_blast_bull')  && ['d_blast_bull',  'ΔΔ↑',  'A'],
        show('d_surge_bull')  && ['d_surge_bull',  'Δ↑',   'B'],
        show('d_strong_bull') && ['d_strong_bull', 'B/S↑', 'B'],
        show('d_absorb_bull') && ['d_absorb_bull', 'Ab↑',  'B'],
        show('d_div_bull')    && ['d_div_bull',    'T↓',   null],
        show('d_vd_div_bull') && ['d_vd_div_bull', 'NS',   null],
        show('d_flip_bull')   && ['d_flip_bull',   'FLP↑', 'B'],
        show('d_orange_bull') && ['d_orange_bull', 'ORG↑', null],
      ].filter(Boolean),
    },
    {
      label: 'T/Z Candlestick', items: [
        r.tz_sig        && ['tz_sig',       r.tz_sig + (r.l_combo && r.l_combo !== 'NONE' ? ` [${r.l_combo}]` : ''),  ['T4','T6','T1G','T2G'].includes(r.tz_sig) ? 'A' : 'B'],
        show('tz_bull_flip') && ['tz_bull_flip', 'TZ→3', 'B'],
        show('tz_attempt')   && ['tz_attempt',   'TZ→2', null],
        r.ca && ['ca', 'CA', null],
        r.cd && ['cd', 'CD', null],
        r.cw && ['cw', 'CW', null],
      ].filter(Boolean),
    },
    {
      label: 'WLNBB / L-structure', items: [
        r.l_combo && r.l_combo !== 'NONE' && r.l_combo !== ''
          && ['_lcombo', r.l_combo, null],
        show('fri34')   && ['fri34',   'FRI34', 'A'],
        show('fri43')   && ['fri43',   'FRI43', 'B'],
        show('l34')     && ['l34',     'L34',   'B'],
        show('preup66') && ['preup66', 'P66',   'A'],
        show('preup55') && ['preup55', 'P55',   'B'],
        show('preup89') && ['preup89', 'P89',   'B'],
        show('preup3')  && ['preup3',  'P3',    null],
        show('preup2')  && ['preup2',  'P2',    null],
        show('preup50') && ['preup50', 'P50',   null],
        show('blue')    && ['blue',    'BLUE',  null],
      ].filter(Boolean),
    },
    {
      label: 'Combo / 2809', items: [
        show('rocket')    && ['rocket',    '🚀',    'A'],
        show('buy_2809')  && ['buy_2809',  'BUY',   'A'],
        show('seq_bcont') && ['seq_bcont', 'SBC',   'B'],
        show('sig3g')     && ['sig3g',     '3G',    null],
        show('rtv')       && ['rtv',       'RTV',   null],
        show('hilo_buy')  && ['hilo_buy',  'HILO↑', null],
        show('atr_brk')   && ['atr_brk',  'ATR↑',  null],
        show('bb_brk')    && ['bb_brk',   'BB↑',   null],
      ].filter(Boolean),
    },
    {
      label: 'Breakout / ULTRA', items: [
        r.fbo_bull  && ['fbo_bull',  'FBO↑', 'A'],
        r.eb_bull   && ['eb_bull',   'EB↑',  'A'],
        r.rs_strong && ['rs_strong', 'RS+',  'B'],
        r.rs        && ['rs',        'RS',   null],
        r.ultra_3up && ['ultra_3up', '3↑',   null],
      ].filter(Boolean),
    },
    {
      label: 'Wick X signals', items: [
        show('x2g_wick') && ['x2g_wick', 'X2G', 'A'],
        show('x2_wick')  && ['x2_wick',  'X2',  'A'],
        show('x1g_wick') && ['x1g_wick', 'X1G', 'B'],
        show('x1_wick')  && ['x1_wick',  'X1',  'B'],
        show('x3_wick')  && ['x3_wick',  'X3',  null],
        show('wick_bull') && ['wick_bull', 'W↑', null],
      ].filter(Boolean),
    },
    {
      label: 'B / G Signals', items: [
        ...[1,2,3,4,5,6,7,8,9,10,11]
          .filter(i => show(`b${i}`))
          .map(i => [`b${i}`, `B${i}`, i === 1 || i === 10 ? 'B' : null]),
        ...['g1','g2','g4','g6','g11']
          .filter(k => show(k))
          .map(k => [k, k.toUpperCase(), 'B']),
      ],
    },
  ]

  return (
    <div className="space-y-3">
      {groups.map(g => g.items.length > 0 && (
        <div key={g.label}>
          <div className="text-[10px] text-gray-600 uppercase tracking-wide mb-1">{g.label}</div>
          <div className="flex flex-wrap gap-1">
            {g.items.map(([key, lbl, tier]) => (
              <TierBadge key={key} label={lbl} tier={tier} age={age(key)} />
            ))}
          </div>
        </div>
      ))}
      {groups.every(g => g.items.length === 0) && (
        <div className="text-gray-600 text-xs">No signals fired in the last {N} bar{N > 1 ? 's' : ''}</div>
      )}
    </div>
  )
}

const TF_OPTS = ['1wk', '1d', '4h', '1h', '30m', '15m']
const N_OPTS  = [1, 3, 5, 10]

// ─────────────────────────────────────────────────────────────────────────────
export default function TickerAnalysisPanel({ onAddToWatchlist, onChartChange }) {
  const [input,   setInput]   = useState('')
  const [ticker,  setTicker]  = useState('')
  const [tf,      setTf]      = useState('1d')
  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)
  const [added,   setAdded]   = useState(false)
  const [N,       setN]       = useState(1)

  const analyze = (sym = input, timeframe = tf) => {
    const t = sym.trim().toUpperCase()
    if (!t) return
    setTicker(t)
    setAdded(false)
    setLoading(true); setError(null); setResult(null)
    onChartChange?.({ ticker: t, tf: timeframe })
    api.turboAnalyze(t, timeframe)
      .then(d => { setResult(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }

  const changeTf = (t) => {
    setTf(t)
    if (ticker) {
      onChartChange?.({ ticker, tf: t })
      analyze(ticker, t)
    }
  }

  const handleAdd = () => {
    if (result?.ticker && onAddToWatchlist) {
      onAddToWatchlist(result.ticker)
      setAdded(true)
    }
  }

  const onKeyDown = (e) => { if (e.key === 'Enter') analyze() }

  const r = result

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

        {/* N selector — bar count lookback */}
        <div className="flex items-center gap-1">
          <span className="text-gray-600 text-[10px]">last</span>
          {N_OPTS.map(n => (
            <button key={n} onClick={() => setN(n)}
              className={`px-2 py-0.5 rounded text-xs ${N === n ? 'bg-indigo-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
              {n}
            </button>
          ))}
          <span className="text-gray-600 text-[10px]">bars</span>
        </div>
      </div>

      {loading && (
        <div className="px-4 py-6 text-center text-gray-500 animate-pulse">Analyzing {ticker}…</div>
      )}
      {error && (
        <div className="px-4 py-4 text-red-400">{error}</div>
      )}

      {!ticker && !loading && !r && !error && (
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
              Signals — last {N} bar{N > 1 ? 's' : ''}
              <span className="ml-2 font-normal text-gray-600 normal-case">
                <span className="text-lime-300/70">A</span> = Tier A ·{' '}
                <span className="text-sky-300/70">B</span> = amplifier ·{' '}
                <span className="text-gray-500">dimmed = fired N bars ago</span>
              </span>
            </div>
            <SignalGrid r={r} ages={(() => { try { return JSON.parse(r.sig_ages || '{}') } catch { return {} } })()} N={N} />
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
