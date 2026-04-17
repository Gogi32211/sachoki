import { useState, useEffect, useCallback } from 'react'
import { api } from '../api'

// Signal label map → shorter display names
const SIG_LABEL = {
  best_sig: 'BEST★', vbo_up: 'VBO↑', ns: 'NS', abs_sig: 'ABS', load_sig: 'LD',
  wyk_spring: 'wSPR', wyk_sos: 'SOS', wyk_lps: 'LPS',
  d_spring: 'dSPR', d_strong_bull: 'B/S↑', d_blast_bull: 'ΔΔ↑', d_absorb_bull: 'Ab↑',
  rocket: '🚀', buy_2809: 'BUY', sig_l88: 'L88',
  fri34: 'FRI34', fri43: 'FRI43', l34: 'L34', preup66: 'P66', preup55: 'P55',
  rs_strong: 'RS+', tz_bull_flip: 'TZ→3', tz_attempt: 'TZ→2',
  g1: 'G1', g2: 'G2', b10: 'B10', b1: 'B1', va: 'VA', seq_bcont: 'SBC',
}

function pctColor(p) {
  if (p >= 70) return 'text-lime-400 font-bold'
  if (p >= 50) return 'text-yellow-300 font-semibold'
  if (p >= 30) return 'text-sky-300'
  return 'text-gray-500'
}

function pctBg(p) {
  if (p >= 70) return 'bg-lime-900/30'
  if (p >= 50) return 'bg-yellow-900/20'
  if (p >= 30) return 'bg-sky-900/15'
  return ''
}

export default function SignalCorrelPanel() {
  const [data,      setData]      = useState(null)
  const [loading,   setLoading]   = useState(false)
  const [error,     setError]     = useState(null)
  const [tf,        setTf]        = useState('1d')
  const [universe,  setUniverse]  = useState('sp500')
  const [minPct,    setMinPct]    = useState(15)
  const [sortBy,    setSortBy]    = useState('max_pct')

  const load = () => {
    setLoading(true); setError(null)
    api.signalCorrelation(tf, universe, minPct)
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }

  useEffect(() => { load() }, [tf, universe, minPct])

  const pairs = data?.pairs ?? []
  const sorted = [...pairs].sort((a, b) => b[sortBy] - a[sortBy])

  const SortTh = ({ col, children }) => (
    <th
      onClick={() => setSortBy(col)}
      className={`px-3 py-2 text-left text-xs font-medium cursor-pointer select-none transition-colors
        ${sortBy === col ? 'text-blue-300' : 'text-gray-500 hover:text-gray-300'}`}>
      {children}{sortBy === col ? ' ↓' : ''}
    </th>
  )

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 text-xs text-gray-300">

      {/* Header */}
      <div className="flex flex-wrap items-center gap-3 px-4 py-3 border-b border-gray-800">
        <span className="font-semibold text-sm text-white">Signal Co-occurrence</span>
        <span className="text-gray-500">When signal A fires, how often does B also fire?</span>

        {/* TF */}
        <div className="flex gap-0.5 ml-auto">
          {['1wk','1d','4h','1h'].map(t => (
            <button key={t} onClick={() => setTf(t)}
              className={`px-2 py-0.5 rounded text-xs ${tf === t ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
              {t.toUpperCase()}
            </button>
          ))}
        </div>

        {/* Universe */}
        <div className="flex gap-0.5">
          {['sp500','nasdaq','russell2k'].map(u => (
            <button key={u} onClick={() => setUniverse(u)}
              className={`px-2 py-0.5 rounded text-xs ${universe === u ? 'bg-indigo-700 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'}`}>
              {u === 'sp500' ? 'SP500' : u === 'nasdaq' ? 'NDX' : 'R2K'}
            </button>
          ))}
        </div>

        {/* Min % */}
        <div className="flex items-center gap-1 text-gray-500">
          <span>min%</span>
          {[10, 15, 25, 40].map(v => (
            <button key={v} onClick={() => setMinPct(v)}
              className={`px-1.5 py-0.5 rounded ${minPct === v ? 'bg-gray-700 text-white' : 'text-gray-600 hover:text-white'}`}>
              {v}
            </button>
          ))}
        </div>

        <button onClick={load}
          className="px-2 py-0.5 rounded bg-gray-800 text-gray-400 hover:text-white text-xs">
          ↺
        </button>

        {sorted.length > 0 && (
          <button
            onClick={() => {
              const header = 'Signal A,Signal B,Together,A fires,B fires,% of A,% of B,max%'
              const rows = sorted.map(p =>
                `${p.sig_a},${p.sig_b},${p.both},${p.a_count},${p.b_count},${p.pct_a}%,${p.pct_b}%,${p.max_pct}%`
              )
              const csv = [header, ...rows].join('\n')
              const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }))
              const a = document.createElement('a')
              a.href = url
              a.download = `signal_correlation_${tf}_${universe}_min${minPct}pct.csv`
              a.click()
              URL.revokeObjectURL(url)
            }}
            className="px-2 py-0.5 rounded bg-gray-800 text-gray-400 hover:text-white text-xs">
            ⬇ CSV
          </button>
        )}
      </div>

      {/* Stats bar */}
      {data && (
        <div className="px-4 py-2 border-b border-gray-800 text-xs text-gray-500 flex gap-4">
          <span>{data.n_tickers} tickers scanned</span>
          <span>{sorted.length} pairs ≥ {minPct}%</span>
          <span className="text-gray-600">
            {Object.entries(data.signal_counts ?? {})
              .filter(([, v]) => v > 0)
              .sort((a, b) => b[1] - a[1])
              .slice(0, 8)
              .map(([k, v]) => `${SIG_LABEL[k] ?? k}:${v}`)
              .join(' · ')}
          </span>
        </div>
      )}

      {loading && (
        <div className="px-4 py-8 text-center text-gray-500 animate-pulse">Loading…</div>
      )}
      {error && (
        <div className="px-4 py-4 text-red-400">{error}</div>
      )}

      {!loading && !error && sorted.length === 0 && (
        <div className="px-4 py-8 text-center text-gray-600">
          No pairs found — run a Turbo scan first, or lower min%
        </div>
      )}

      {!loading && sorted.length > 0 && (
        <div className="overflow-auto max-h-[520px]">
          <table className="w-full">
            <thead className="sticky top-0 bg-gray-900 border-b border-gray-800">
              <tr>
                <th className="px-3 py-2 text-left text-xs text-gray-500 font-medium">Signal A</th>
                <th className="px-3 py-2 text-left text-xs text-gray-500 font-medium">Signal B</th>
                <SortTh col="both">Together</SortTh>
                <SortTh col="a_count">A fires</SortTh>
                <SortTh col="b_count">B fires</SortTh>
                <SortTh col="pct_a">% of A</SortTh>
                <SortTh col="pct_b">% of B</SortTh>
                <SortTh col="max_pct">max%</SortTh>
              </tr>
            </thead>
            <tbody>
              {sorted.map((p, i) => (
                <tr key={i}
                  className={`border-b border-gray-800/50 ${pctBg(p.max_pct)}`}>
                  <td className="px-3 py-1.5 font-mono font-semibold text-blue-300">
                    {SIG_LABEL[p.sig_a] ?? p.sig_a}
                  </td>
                  <td className="px-3 py-1.5 font-mono font-semibold text-cyan-300">
                    {SIG_LABEL[p.sig_b] ?? p.sig_b}
                  </td>
                  <td className="px-3 py-1.5 text-center text-gray-300">{p.both}</td>
                  <td className="px-3 py-1.5 text-center text-gray-500">{p.a_count}</td>
                  <td className="px-3 py-1.5 text-center text-gray-500">{p.b_count}</td>
                  <td className={`px-3 py-1.5 text-center ${pctColor(p.pct_a)}`}>{p.pct_a}%</td>
                  <td className={`px-3 py-1.5 text-center ${pctColor(p.pct_b)}`}>{p.pct_b}%</td>
                  <td className={`px-3 py-1.5 text-center font-bold ${pctColor(p.max_pct)}`}>
                    {p.max_pct}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div className="px-4 py-2 text-gray-600 text-[10px] border-t border-gray-800">
        % of A = when A fired, B also fired X% of the time. % of B = vice versa. max% = the higher of the two.
        Sorted by max% by default. Data is from the latest Turbo scan results.
      </div>
    </div>
  )
}
