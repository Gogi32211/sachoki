import { useState, useCallback } from 'react'
import { api } from '../api'

// ── Signal groups (same visual grouping as Turbo filter bar) ──────────────────
const SIGNAL_GROUPS = [
  { header: 'T/Z' },
  { key: 'tz_bull',      label: 'T/Z↑',    cls: 'text-violet-300' },
  { key: 'tz_T4T6',     label: 'T4/T6',    cls: 'text-violet-200' },
  { key: 'tz_bull_flip', label: 'TZ→3',    cls: 'text-lime-300'   },
  { key: 'tz_attempt',   label: 'TZ→2',    cls: 'text-cyan-300'   },
  { header: 'VABS' },
  { key: 'best_sig',    label: 'BEST★',    cls: 'text-lime-300'    },
  { key: 'vbo_up',      label: 'VBO↑',     cls: 'text-green-300'  },
  { key: 'abs_sig',     label: 'ABS',      cls: 'text-teal-300'   },
  { key: 'ns',          label: 'NS',       cls: 'text-teal-400'   },
  { key: 'sq',          label: 'SQ',       cls: 'text-cyan-400'   },
  { key: 'load_sig',    label: 'LD',       cls: 'text-blue-300'   },
  { key: 'climb_sig',   label: 'CLB',      cls: 'text-cyan-300'   },
  { header: 'WLNBB' },
  { key: 'fri34',       label: 'FRI34',    cls: 'text-cyan-400'   },
  { key: 'fri43',       label: 'FRI43',    cls: 'text-sky-300'    },
  { key: 'l34',         label: 'L34',      cls: 'text-blue-300'   },
  { key: 'l43',         label: 'L43',      cls: 'text-teal-300'   },
  { key: 'bo_up',       label: 'BO↑',      cls: 'text-lime-300'   },
  { key: 'be_up',       label: 'BE↑',      cls: 'text-emerald-300'},
  { key: 'blue',        label: 'BLUE',     cls: 'text-sky-300'    },
  { key: 'cci_ready',   label: 'CCI',      cls: 'text-violet-300' },
  { header: 'Combo / 2809' },
  { key: 'rocket',      label: '🚀',       cls: 'text-red-300'    },
  { key: 'buy_2809',    label: 'BUY',      cls: 'text-lime-400'   },
  { key: 'bf_buy',      label: '4BF',      cls: 'text-pink-300'   },
  { key: 'sig3g',       label: '3G',       cls: 'text-cyan-300'   },
  { key: 'hilo_buy',    label: 'HILO↑',    cls: 'text-green-300'  },
  { key: 'va',          label: 'VA',       cls: 'text-lime-300'   },
  { key: 'cd',          label: 'CD',       cls: 'text-lime-300'   },
  { key: 'ca',          label: 'CA',       cls: 'text-cyan-300'   },
  { key: 'sig_l88',     label: 'L88',      cls: 'text-violet-200' },
  { header: 'PREUP' },
  { key: 'preup66',     label: 'P66',      cls: 'text-lime-300'   },
  { key: 'preup55',     label: 'P55',      cls: 'text-emerald-300'},
  { key: 'preup89',     label: 'P89',      cls: 'text-teal-300'   },
  { key: 'preup3',      label: 'P3',       cls: 'text-cyan-300'   },
  { key: 'preup2',      label: 'P2',       cls: 'text-cyan-400'   },
  { header: 'Ultra' },
  { key: 'fbo_bull',    label: 'FBO↑',     cls: 'text-sky-300'    },
  { key: 'eb_bull',     label: 'EB↑',      cls: 'text-amber-300'  },
  { key: 'ultra_3up',   label: '3↑',       cls: 'text-lime-300'   },
  { header: 'Delta' },
  { key: 'd_blast_bull', label: 'ΔΔ↑',    cls: 'text-yellow-300' },
  { key: 'd_spring',     label: 'dSPR',    cls: 'text-lime-300'   },
  { key: 'd_strong_bull',label: 'B/S↑',   cls: 'text-lime-300'   },
  { key: 'd_absorb_bull',label: 'Ab↑',    cls: 'text-yellow-400' },
  { key: 'd_surge_bull', label: 'Δ↑',     cls: 'text-teal-300'   },
  { key: 'd_div_bull',   label: 'T↓',     cls: 'text-cyan-300'   },
  { header: 'Wick' },
  { key: 'x2g_wick',    label: 'X2G',      cls: 'text-cyan-300'   },
  { key: 'x2_wick',     label: 'X2',       cls: 'text-sky-300'    },
  { key: 'x1g_wick',    label: 'X1G',      cls: 'text-lime-300'   },
  { key: 'wick_bull',   label: 'WK↑',      cls: 'text-emerald-400'},
  { header: 'PARA 260420' },
  { key: 'para_start',  label: 'PARA',     cls: 'text-lime-300'   },
  { key: 'para_plus',   label: 'PARA+',    cls: 'text-cyan-300 font-semibold' },
  { key: 'para_retest', label: 'RETEST',   cls: 'text-emerald-300'},
]

const TF_OPTS = ['1wk', '1d', '4h', '1h']

const fmt1 = v => v == null ? '—' : `${v > 0 ? '+' : ''}${Number(v).toFixed(1)}%`
const fmtR = v => v == null ? '—' : `${(v * 100).toFixed(0)}%`
const clsR = v => v >= 0.65 ? 'text-lime-300 font-semibold' : v >= 0.55 ? 'text-green-400' : v >= 0.45 ? 'text-gray-300' : 'text-red-400'
const clsRet = v => v > 3 ? 'text-lime-300 font-semibold' : v > 1 ? 'text-green-400' : v > 0 ? 'text-gray-300' : 'text-red-400'
const clsDD = v => v > -1 ? 'text-lime-400' : v > -2 ? 'text-yellow-300' : v > -4 ? 'text-orange-400' : 'text-red-400'

export default function SignalStatsPanel({ ticker: propTicker, tf: propTf }) {
  const [ticker,    setTicker]    = useState(propTicker || 'AAPL')
  const [tf,        setTf]        = useState(propTf || '1d')
  const [selSigs,   setSelSigs]   = useState(new Set(['tz_bull', 'best_sig', 'vbo_up', 'fri34', 'bf_buy', 'preup66', 'd_blast_bull']))
  const [combo,     setCombo]     = useState(false)
  const [loading,   setLoading]   = useState(false)
  const [result,    setResult]    = useState(null)
  const [error,     setError]     = useState(null)
  const [sortCol,   setSortCol]   = useState('avg_3bar')
  const [sortAsc,   setSortAsc]   = useState(false)

  const toggle = key => setSelSigs(prev => {
    const n = new Set(prev)
    n.has(key) ? n.delete(key) : n.add(key)
    return n
  })

  const run = useCallback(() => {
    if (selSigs.size === 0 || !ticker.trim()) return
    setLoading(true); setError(null); setResult(null)
    api.signalStats(ticker.trim().toUpperCase(), tf, [...selSigs], combo)
      .then(d => { setResult(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }, [ticker, tf, selSigs, combo])

  // Sorted rows from result
  const rows = (() => {
    if (!result?.results) return []
    const entries = Object.entries(result.results)
      .map(([key, st]) => ({ key, label: result.labels?.[key] || key, ...st }))
    const mul = sortAsc ? 1 : -1
    return entries.sort((a, b) => mul * ((a[sortCol] ?? -Infinity) - (b[sortCol] ?? -Infinity)))
  })()

  const SortTh = ({ col, children, cls = '' }) => (
    <th className={`px-2 py-1.5 font-medium cursor-pointer select-none hover:text-white transition-colors text-right ${cls}`}
        onClick={() => { if (sortCol === col) setSortAsc(a => !a); else { setSortCol(col); setSortAsc(false) } }}>
      {children}{sortCol === col ? (sortAsc ? ' ↑' : ' ↓') : ''}
    </th>
  )

  return (
    <div className="flex flex-col h-full bg-gray-950 text-gray-100 text-xs">

      {/* ── Row 0: Ticker + TF + Run ── */}
      <div className="flex flex-wrap items-center gap-2 px-3 py-2 border-b border-gray-800">
        <span className="text-gray-500 shrink-0">Ticker</span>
        <input
          value={ticker}
          onChange={e => setTicker(e.target.value.toUpperCase())}
          onKeyDown={e => e.key === 'Enter' && run()}
          className="w-20 px-2 py-0.5 rounded bg-gray-800 border border-gray-700 text-white font-mono text-xs focus:outline-none focus:border-blue-500"
          placeholder="AAPL"
        />
        <div className="flex gap-0.5 border border-gray-700 rounded p-0.5">
          {TF_OPTS.map(t => (
            <button key={t} onClick={() => setTf(t)}
              className={`px-2 py-0.5 rounded text-xs transition-colors font-medium
                ${tf === t ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white'}`}>
              {t.toUpperCase()}
            </button>
          ))}
        </div>

        {/* Combo toggle */}
        <button onClick={() => setCombo(c => !c)}
          title="Show AND-combo of all selected signals"
          className={`px-2.5 py-1 rounded text-xs font-medium border transition-colors
            ${combo ? 'border-amber-400 text-amber-300 bg-amber-900/30' : 'border-gray-700 text-gray-500 hover:text-gray-300'}`}>
          + AND combo
        </button>

        <button onClick={run} disabled={loading || selSigs.size === 0}
          className={`px-3 py-1 rounded text-xs font-semibold transition-colors
            ${loading ? 'bg-gray-700 text-gray-400 cursor-not-allowed' : 'bg-violet-600 hover:bg-violet-500 text-white'}`}>
          {loading ? <span className="animate-pulse">Analyzing…</span> : '📈 Analyze'}
        </button>

        {result && (
          <span className="ml-auto text-gray-600">
            {result.ticker} · {result.interval} · {result.bars} bars
          </span>
        )}
      </div>

      {/* ── Row 1: Signal selector ── */}
      <div className="flex flex-wrap items-start gap-x-1 gap-y-1 px-3 py-2 border-b border-gray-800 bg-gray-900/30">
        <button onClick={() => setSelSigs(new Set())}
          className={`px-2 py-0.5 rounded text-xs shrink-0 ${selSigs.size === 0 ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
          None
        </button>
        {SIGNAL_GROUPS.map((s, i) =>
          s.header
            ? <span key={`h-${i}`} className="text-gray-600 text-[10px] self-center px-0.5 shrink-0">· {s.header}</span>
            : (
              <button key={s.key} onClick={() => toggle(s.key)}
                className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors
                  ${selSigs.has(s.key) ? `${s.cls} bg-gray-700 font-semibold` : 'bg-gray-800 text-gray-500 hover:text-white'}`}>
                {s.label}
              </button>
            )
        )}
        {selSigs.size > 0 && (
          <span className="ml-auto text-gray-600 self-center">{selSigs.size} selected</span>
        )}
      </div>

      {/* ── Error ── */}
      {error && (
        <div className="px-4 py-2 text-red-400 border-b border-gray-800">{error}</div>
      )}

      {/* ── Legend ── */}
      {!result && !loading && (
        <div className="px-4 py-4 text-gray-600 text-xs">
          <p className="mb-2">Select signals above, enter a ticker, then click <span className="text-violet-300">📈 Analyze</span>.</p>
          <p className="text-gray-700">Columns: <span className="text-gray-500">N</span> = occurrences · <span className="text-gray-500">Win%</span> = next bar up · <span className="text-gray-500">Avg1/3/5</span> = max gain 1/3/5 bars · <span className="text-gray-500">MAE</span> = avg drawdown 3 bars · <span className="text-gray-500">False%</span> = 3-bar max &lt; 0</p>
        </div>
      )}

      {/* ── Results table ── */}
      {rows.length > 0 && (
        <div className="overflow-auto flex-1">
          <table className="w-full border-collapse">
            <thead className="sticky top-0 bg-gray-900 z-10 text-gray-500 text-left">
              <tr>
                <th className="px-3 py-1.5 font-medium">Signal</th>
                <SortTh col="n">N</SortTh>
                <SortTh col="bull_rate">Win%</SortTh>
                <SortTh col="avg_1bar">Avg 1</SortTh>
                <SortTh col="avg_3bar">Avg 3</SortTh>
                <SortTh col="avg_5bar">Avg 5</SortTh>
                <SortTh col="mae_3">MAE</SortTh>
                <SortTh col="false_rate">False%</SortTh>
              </tr>
            </thead>
            <tbody>
              {rows.map(r => {
                const isCombo = r.key.startsWith('COMBO:')
                return (
                  <tr key={r.key}
                    className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${isCombo ? 'bg-amber-900/10' : ''}`}>
                    <td className="px-3 py-1.5 font-mono">
                      <span className={isCombo ? 'text-amber-300 font-semibold' : 'text-gray-200'}>
                        {isCombo ? '⊕ ' + r.key.replace('COMBO:', '') : r.label}
                      </span>
                      {r.warning && <span className="ml-2 text-yellow-600 text-[10px]">⚠ {r.warning}</span>}
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono text-gray-400">{r.n ?? '—'}</td>
                    <td className={`px-2 py-1.5 text-right font-mono ${clsR(r.bull_rate)}`}>{fmtR(r.bull_rate)}</td>
                    <td className={`px-2 py-1.5 text-right font-mono ${clsRet(r.avg_1bar)}`}>{fmt1(r.avg_1bar)}</td>
                    <td className={`px-2 py-1.5 text-right font-mono ${clsRet(r.avg_3bar)}`}>{fmt1(r.avg_3bar)}</td>
                    <td className={`px-2 py-1.5 text-right font-mono ${clsRet(r.avg_5bar)}`}>{fmt1(r.avg_5bar)}</td>
                    <td className={`px-2 py-1.5 text-right font-mono ${clsDD(r.mae_3)}`}>{fmt1(r.mae_3)}</td>
                    <td className={`px-2 py-1.5 text-right font-mono ${clsR(1 - (r.false_rate ?? 1))}`}>{fmtR(r.false_rate)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>

          {/* Insight bar */}
          {rows.length > 1 && (() => {
            const best3 = rows.filter(r => !r.key.startsWith('COMBO:') && r.n >= 10)
              .sort((a, b) => (b.avg_3bar ?? -99) - (a.avg_3bar ?? -99))[0]
            const bestWin = rows.filter(r => !r.key.startsWith('COMBO:') && r.n >= 10)
              .sort((a, b) => (b.bull_rate ?? 0) - (a.bull_rate ?? 0))[0]
            if (!best3 && !bestWin) return null
            return (
              <div className="px-3 py-2 border-t border-gray-800 bg-gray-900/50 text-gray-500 flex gap-6">
                {best3 && <span>Best avg 3-bar: <span className="text-lime-300 font-mono">{best3.label}</span> {fmt1(best3.avg_3bar)}</span>}
                {bestWin && <span>Best win%: <span className="text-lime-300 font-mono">{bestWin.label}</span> {fmtR(bestWin.bull_rate)}</span>}
                <span className="ml-auto text-gray-700">entry = close of signal bar · outcome = max high of next N bars</span>
              </div>
            )
          })()}
        </div>
      )}

      {/* ── Empty result ── */}
      {result && rows.length === 0 && (
        <div className="px-4 py-6 text-gray-500">
          No results — signals may have &lt; {5} occurrences in {result.bars} bars.
          Try a longer timeframe or different signals.
        </div>
      )}
    </div>
  )
}
