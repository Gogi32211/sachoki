import { useState, useCallback, useEffect, useRef } from 'react'
import { api } from '../api'

const SIGNAL_GROUPS = [
  { header: 'T/Z' },
  { key: 'tz_bull',       label: 'T/Z↑'  },
  { key: 'tz_T4T6',      label: 'T4/T6' },
  { key: 'tz_bull_flip',  label: 'TZ→3'  },
  { key: 'tz_attempt',    label: 'TZ→2'  },
  { header: 'VABS' },
  { key: 'best_sig',     label: 'BEST★' },
  { key: 'vbo_up',       label: 'VBO↑'  },
  { key: 'abs_sig',      label: 'ABS'   },
  { key: 'ns',           label: 'NS'    },
  { key: 'sq',           label: 'SQ'    },
  { key: 'load_sig',     label: 'LD'    },
  { key: 'climb_sig',    label: 'CLB'   },
  { header: 'WLNBB' },
  { key: 'fri34',        label: 'FRI34' },
  { key: 'fri43',        label: 'FRI43' },
  { key: 'l34',          label: 'L34'   },
  { key: 'l43',          label: 'L43'   },
  { key: 'bo_up',        label: 'BO↑'   },
  { key: 'be_up',        label: 'BE↑'   },
  { key: 'blue',         label: 'BLUE'  },
  { key: 'cci_ready',    label: 'CCI'   },
  { header: 'Combo/2809' },
  { key: 'rocket',       label: '🚀'    },
  { key: 'buy_2809',     label: 'BUY'   },
  { key: 'bf_buy',       label: '4BF'   },
  { key: 'sig3g',        label: '3G'    },
  { key: 'hilo_buy',     label: 'HILO↑' },
  { key: 'va',           label: 'VA'    },
  { key: 'cd',           label: 'CD'    },
  { key: 'ca',           label: 'CA'    },
  { key: 'sig_l88',      label: 'L88'   },
  { header: 'PREUP' },
  { key: 'preup66',      label: 'P66'   },
  { key: 'preup55',      label: 'P55'   },
  { key: 'preup89',      label: 'P89'   },
  { key: 'preup3',       label: 'P3'    },
  { key: 'preup2',       label: 'P2'    },
  { header: 'Ultra' },
  { key: 'fbo_bull',     label: 'FBO↑'  },
  { key: 'eb_bull',      label: 'EB↑'   },
  { key: 'ultra_3up',    label: '3↑'    },
  { header: 'Delta' },
  { key: 'd_blast_bull', label: 'ΔΔ↑'   },
  { key: 'd_spring',     label: 'dSPR'  },
  { key: 'd_strong_bull',label: 'B/S↑'  },
  { key: 'd_absorb_bull',label: 'Ab↑'   },
  { key: 'd_surge_bull', label: 'Δ↑'    },
  { key: 'd_div_bull',   label: 'T↓'    },
  { header: 'Wick' },
  { key: 'x2g_wick',    label: 'X2G'   },
  { key: 'x2_wick',     label: 'X2'    },
  { key: 'x1g_wick',    label: 'X1G'   },
  { key: 'wick_bull',   label: 'WK↑'   },
  { header: 'PARA' },
  { key: 'para_start',  label: 'PARA'  },
  { key: 'para_plus',   label: 'PARA+' },
  { key: 'para_retest', label: 'RETEST'},
  { header: 'FLY 260424' },
  { key: 'fly_abcd',    label: 'ABCD'  },
  { key: 'fly_cd',      label: 'FLY CD'},
  { key: 'fly_bd',      label: 'FLY BD'},
  { key: 'fly_ad',      label: 'FLY AD'},
]

const ALL_KEYS = SIGNAL_GROUPS.filter(s => s.key).map(s => s.key)
const TF_OPTS  = ['1wk', '1d', '4h', '1h']

const fmt1  = v => v == null ? '—' : `${v > 0 ? '+' : ''}${Number(v).toFixed(1)}%`
const fmtR  = v => v == null ? '—' : `${(v * 100).toFixed(0)}%`
const clsR  = v => v >= 0.65 ? 'text-lime-300 font-semibold' : v >= 0.55 ? 'text-green-400' : v >= 0.45 ? 'text-gray-300' : 'text-red-400'
const clsRet= v => v > 3 ? 'text-lime-300 font-semibold' : v > 1 ? 'text-green-400' : v > 0 ? 'text-gray-300' : 'text-red-400'
const clsDD = v => v > -1 ? 'text-lime-400' : v > -2 ? 'text-yellow-300' : v > -4 ? 'text-orange-400' : 'text-red-400'

function exportCsv(rows, labels, title) {
  const cols = ['signal','n','tickers','bull_rate','avg_1bar','avg_3bar','avg_5bar','mae_3','false_rate']
  const hdr  = ['Signal','N','Tickers','Win%','Avg1','Avg3','Avg5','MAE3','False%']
  const lines = [hdr.join(',')]
  for (const r of rows) {
    lines.push([
      `"${labels?.[r.key] || r.label || r.key}"`,
      r.n ?? '', r.tickers ?? '',
      r.bull_rate != null ? (r.bull_rate * 100).toFixed(1) : '',
      r.avg_1bar ?? '', r.avg_3bar ?? '', r.avg_5bar ?? '',
      r.mae_3 ?? '', r.false_rate != null ? (r.false_rate * 100).toFixed(1) : '',
    ].join(','))
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
  const a = document.createElement('a')
  a.href = URL.createObjectURL(blob)
  a.download = `${title}_signal_stats.csv`
  a.click()
}

export default function SignalStatsPanel({ ticker: propTicker, tf: propTf }) {
  const [ticker,   setTicker]   = useState(propTicker || 'AAPL')
  const [tf,       setTf]       = useState(propTf || '1d')
  const [selSigs,  setSelSigs]  = useState(new Set(['tz_bull','best_sig','vbo_up','fri34','bf_buy','preup66','d_blast_bull']))
  const [combo,    setCombo]    = useState(false)
  const [loading,  setLoading]  = useState(false)
  const [result,   setResult]   = useState(null)
  const [error,    setError]    = useState(null)
  const [sortCol,  setSortCol]  = useState('avg_3bar')
  const [sortAsc,  setSortAsc]  = useState(false)

  // Pooled mode
  const [pooledMode,    setPooledMode]    = useState(false)
  const [pooledStatus,  setPooledStatus]  = useState(null)  // {status, done, total, results, labels}
  const [pooledPolling, setPooledPolling] = useState(false)
  const pollRef = useRef(null)

  const toggle = key => setSelSigs(prev => {
    const n = new Set(prev); n.has(key) ? n.delete(key) : n.add(key); return n
  })
  const selectAll  = () => setSelSigs(new Set(ALL_KEYS))
  const selectNone = () => setSelSigs(new Set())

  // ── Single-ticker analyze ──────────────────────────────────────────────────
  const run = useCallback(() => {
    if (selSigs.size === 0 || !ticker.trim()) return
    setLoading(true); setError(null); setResult(null)
    api.signalStats(ticker.trim().toUpperCase(), tf, [...selSigs], combo)
      .then(d => { setResult(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }, [ticker, tf, selSigs, combo])

  // ── Pooled build + poll ────────────────────────────────────────────────────
  const startPooled = () => {
    api.signalStatsPooledBuild(tf, 'sp500', [...selSigs])
      .then(() => { setPooledPolling(true); setPooledStatus({ status: 'running', done: 0, total: 0 }) })
      .catch(e => setError(e.message))
  }

  useEffect(() => {
    if (!pooledPolling) return
    pollRef.current = setInterval(() => {
      api.signalStatsPooledStatus(tf, 'sp500').then(s => {
        setPooledStatus(s)
        if (s.status !== 'running') {
          setPooledPolling(false)
          clearInterval(pollRef.current)
        }
      }).catch(() => {})
    }, 3000)
    return () => clearInterval(pollRef.current)
  }, [pooledPolling, tf])

  // ── Row building ──────────────────────────────────────────────────────────
  const activeResult = pooledMode ? pooledStatus : result
  const rows = (() => {
    if (!activeResult?.results) return []
    const entries = Object.entries(activeResult.results)
      .map(([key, st]) => ({ key, label: activeResult.labels?.[key] || key, ...st }))
    const mul = sortAsc ? 1 : -1
    return entries.sort((a, b) => mul * ((a[sortCol] ?? -Infinity) - (b[sortCol] ?? -Infinity)))
  })()

  const SortTh = ({ col, children }) => (
    <th className="px-2 py-1.5 font-medium cursor-pointer select-none hover:text-white text-right"
        onClick={() => { sortCol === col ? setSortAsc(a => !a) : (setSortCol(col), setSortAsc(false)) }}>
      {children}{sortCol === col ? (sortAsc ? ' ↑' : ' ↓') : ''}
    </th>
  )

  return (
    <div className="flex flex-col h-full bg-gray-950 text-gray-100 text-xs">

      {/* ── Row 0: controls ── */}
      <div className="flex flex-wrap items-center gap-2 px-3 py-2 border-b border-gray-800">

        {/* Mode toggle */}
        <div className="flex gap-0.5 border border-gray-700 rounded p-0.5">
          <button onClick={() => setPooledMode(false)}
            className={`px-2 py-0.5 rounded text-xs font-medium transition-colors
              ${!pooledMode ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white'}`}>
            Ticker
          </button>
          <button onClick={() => setPooledMode(true)}
            className={`px-2 py-0.5 rounded text-xs font-medium transition-colors
              ${pooledMode ? 'bg-violet-600 text-white' : 'text-gray-400 hover:text-white'}`}>
            SP500 Pool
          </button>
        </div>

        {!pooledMode && (
          <>
            <span className="text-gray-500 shrink-0">Ticker</span>
            <input
              value={ticker}
              onChange={e => setTicker(e.target.value.toUpperCase())}
              onKeyDown={e => e.key === 'Enter' && run()}
              className="w-20 px-2 py-0.5 rounded bg-gray-800 border border-gray-700 text-white font-mono text-xs focus:outline-none focus:border-blue-500"
              placeholder="AAPL"
            />
          </>
        )}

        <div className="flex gap-0.5 border border-gray-700 rounded p-0.5">
          {TF_OPTS.map(t => (
            <button key={t} onClick={() => setTf(t)}
              className={`px-2 py-0.5 rounded text-xs font-medium transition-colors
                ${tf === t ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white'}`}>
              {t.toUpperCase()}
            </button>
          ))}
        </div>

        {!pooledMode && (
          <button onClick={() => setCombo(c => !c)}
            className={`px-2.5 py-1 rounded text-xs font-medium border transition-colors
              ${combo ? 'border-amber-400 text-amber-300 bg-amber-900/30' : 'border-gray-700 text-gray-500 hover:text-gray-300'}`}>
            + AND combo
          </button>
        )}

        {!pooledMode ? (
          <button onClick={run} disabled={loading || selSigs.size === 0}
            className={`px-3 py-1 rounded text-xs font-semibold transition-colors
              ${loading ? 'bg-gray-700 text-gray-400 cursor-not-allowed' : 'bg-violet-600 hover:bg-violet-500 text-white'}`}>
            {loading ? <span className="animate-pulse">Analyzing…</span> : '📈 Analyze'}
          </button>
        ) : (
          <button onClick={startPooled}
            disabled={pooledPolling || selSigs.size === 0}
            className={`px-3 py-1 rounded text-xs font-semibold transition-colors
              ${pooledPolling ? 'bg-gray-700 text-gray-400 cursor-not-allowed' : 'bg-violet-600 hover:bg-violet-500 text-white'}`}>
            {pooledPolling
              ? <span className="animate-pulse">Building… {pooledStatus?.done ?? 0}/{pooledStatus?.total ?? '?'}</span>
              : '🌐 Build SP500 Stats'}
          </button>
        )}

        {/* Export CSV */}
        {rows.length > 0 && (
          <button
            onClick={() => exportCsv(rows, activeResult?.labels, pooledMode ? `SP500_${tf}` : `${ticker}_${tf}`)}
            className="px-2.5 py-1 rounded text-xs border border-gray-700 text-gray-400 hover:text-white transition-colors">
            ⬇ CSV
          </button>
        )}

        <span className="ml-auto text-gray-600">
          {pooledMode
            ? pooledStatus?.status === 'done'
              ? `SP500 · ${tf} · ${pooledStatus.done ?? 0} tickers`
              : pooledStatus?.status === 'running'
                ? `Running… ${pooledStatus.done ?? 0}/${pooledStatus.total ?? '?'}`
                : 'SP500 pool — click Build to start'
            : result
              ? `${result.ticker} · ${result.interval} · ${result.bars} bars`
              : ''}
        </span>
      </div>

      {/* ── Row 1: Signal selector ── */}
      <div className="flex flex-wrap items-center gap-x-1 gap-y-1 px-3 py-2 border-b border-gray-800 bg-gray-900/30">
        <button onClick={selectNone}
          className={`px-2 py-0.5 rounded text-xs shrink-0 ${selSigs.size === 0 ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
          None
        </button>
        <button onClick={selectAll}
          className={`px-2 py-0.5 rounded text-xs shrink-0 ${selSigs.size === ALL_KEYS.length ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
          All
        </button>
        {SIGNAL_GROUPS.map((s, i) =>
          s.header
            ? <span key={`h-${i}`} className="text-gray-600 text-[10px] self-center px-0.5 shrink-0">· {s.header}</span>
            : (
              <button key={s.key} onClick={() => toggle(s.key)}
                className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors
                  ${selSigs.has(s.key) ? 'bg-blue-700 text-white font-semibold' : 'bg-gray-800 text-gray-500 hover:text-white'}`}>
                {s.label}
              </button>
            )
        )}
        {selSigs.size > 0 && (
          <span className="ml-auto text-gray-600 self-center shrink-0">{selSigs.size} selected</span>
        )}
      </div>

      {/* ── Error ── */}
      {error && (
        <div className="px-4 py-2 text-red-400 border-b border-gray-800">{error}</div>
      )}

      {/* ── Empty state ── */}
      {!activeResult && !loading && (
        <div className="px-4 py-4 text-gray-600">
          <p className="mb-1">
            {pooledMode
              ? 'Select signals → click 🌐 Build SP500 Stats to aggregate across all SP500 tickers.'
              : 'Select signals, enter a ticker, then click 📈 Analyze.'}
          </p>
          <p className="text-gray-700">
            N · Win% · Avg1/3/5 (max gain over 1/3/5 bars) · MAE (avg drawdown 3 bars) · False% (3-bar max &lt; 0)
          </p>
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
                {pooledMode && <SortTh col="tickers">Tickers</SortTh>}
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
                    {pooledMode && <td className="px-2 py-1.5 text-right font-mono text-gray-500">{r.tickers ?? '—'}</td>}
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
            const eligible = rows.filter(r => !r.key.startsWith('COMBO:') && (r.n ?? 0) >= 10)
            const best3   = [...eligible].sort((a, b) => (b.avg_3bar ?? -99) - (a.avg_3bar ?? -99))[0]
            const bestWin = [...eligible].sort((a, b) => (b.bull_rate ?? 0) - (a.bull_rate ?? 0))[0]
            if (!best3 && !bestWin) return null
            return (
              <div className="px-3 py-2 border-t border-gray-800 bg-gray-900/50 text-gray-500 flex flex-wrap gap-6">
                {best3   && <span>Best avg 3-bar: <span className="text-lime-300 font-mono">{best3.label}</span> {fmt1(best3.avg_3bar)}</span>}
                {bestWin && <span>Best win%: <span className="text-lime-300 font-mono">{bestWin.label}</span> {fmtR(bestWin.bull_rate)}</span>}
                <span className="ml-auto text-gray-700">entry = close of signal bar · outcome = max high of next N bars</span>
              </div>
            )
          })()}
        </div>
      )}

      {/* ── Empty result ── */}
      {activeResult && rows.length === 0 && activeResult.status !== 'running' && (
        <div className="px-4 py-6 text-gray-500">
          No results — try different signals or timeframe.
        </div>
      )}
    </div>
  )
}
