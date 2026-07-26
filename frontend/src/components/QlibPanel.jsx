/**
 * QlibPanel.jsx — QLIB tab.
 *
 * Lean "qlib-style" factor lab: pick signal columns, build a feature+label
 * matrix from the DuckDB `bars` table, train LightGBM with a time-ordered
 * train/valid/test split, and see whether the signals actually predict the
 * next-bar forward return (IC / Rank IC / ICIR + feature importance).
 *
 * Forward-return columns (fwd_/mfe_/mae_/hit_/drop_) are excluded by the backend
 * and never appear in the picker — the model learns a price-derived label only.
 */

import { useState, useEffect, useCallback, useRef } from 'react'
import { api } from '../api'
import { colLabel } from '../utils/colLabels'

const UNIVERSES = [
  { id: 'sp500', label: 'S&P 500' },
  { id: 'nasdaq', label: 'Nasdaq' },
  { id: 'russell2k', label: 'Russell 2K' },
]

// Prompt's Step-5 smoke set — handy default selection.
const SMOKE_FEATURES = [
  'tz_bull', 'sig_t9', 'sig_z9', 'turbo_score', 'prebreak_score',
  'wyc_spring', 'gog_score', 'sig_l1', 'sig_vol_5x', 'price_gt_50',
]

const fmt = (v, d = 4) => (v === null || v === undefined ? '—' : Number(v).toFixed(d))
const POLL_MS = 1200

// ── small presentational helpers ─────────────────────────────────────────────
function StatCard({ label, value, hint, tone = 'neutral' }) {
  const toneCls = {
    good: 'text-emerald-400',
    bad: 'text-rose-400',
    neutral: 'text-md-on-surface',
  }[tone]
  return (
    <div className="rounded-xl border border-md-outline-var bg-md-surface-con px-4 py-3 flex flex-col gap-1">
      <span className="text-[11px] uppercase tracking-wide text-md-on-surface-var">{label}</span>
      <span className={`text-2xl font-mono font-semibold ${toneCls}`}>{value}</span>
      {hint && <span className="text-[10px] text-md-on-surface-var/70">{hint}</span>}
    </div>
  )
}

function FeatureBars({ items }) {
  if (!items?.length) return null
  const top = items.slice(0, 20)
  const max = Math.max(...top.map(i => i.importance), 0.0001)
  return (
    <div className="flex flex-col gap-1">
      {top.map(it => (
        <div key={it.feature} className="flex items-center gap-2 text-xs">
          <span className="w-32 shrink-0 font-mono text-md-on-surface-var truncate" title={`${colLabel(it.feature)} · ${it.feature}`}>
            {colLabel(it.feature)}
          </span>
          <div className="flex-1 h-4 bg-md-surface rounded-sm overflow-hidden">
            <div
              className="h-full bg-md-primary/70"
              style={{ width: `${(it.importance / max) * 100}%` }}
            />
          </div>
          <span className="w-14 text-right font-mono text-md-on-surface-var">
            {(it.importance * 100).toFixed(1)}%
          </span>
        </div>
      ))}
    </div>
  )
}

function SearchResults({ res, onUseSet }) {
  const m = res.final_test?.metrics || {}
  const val = v => (v === null || v === undefined ? '—' : Number(v).toFixed(4))
  const tone = v => (v > 0.02 ? 'text-emerald-400' : v < 0 ? 'text-rose-400' : 'text-md-on-surface')
  return (
    <div className="flex flex-col gap-4">
      {/* greedy chosen set + honest holdout */}
      <div className="rounded-xl border border-md-primary/40 bg-md-primary/5 p-4">
        <div className="flex items-center justify-between flex-wrap gap-2 mb-3">
          <div className="text-sm">
            <span className="font-medium text-md-on-surface">Greedy best subset</span>
            <span className="text-md-on-surface-var"> ({res.chosen.length} of {res.pool_size} features)</span>
            <div className="flex flex-wrap gap-1 mt-1">
              {res.chosen.map(c => (
                <span key={c} className="text-[11px] px-2 py-0.5 rounded-full bg-md-primary-container text-md-on-primary-container font-mono">{colLabel(c)}</span>
              ))}
            </div>
          </div>
          <button onClick={() => onUseSet(res.chosen)}
            className="text-xs px-3 py-1.5 rounded-md-sm bg-md-primary text-md-on-primary font-medium hover:opacity-90">
            Use this set ↑
          </button>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <StatCard label="TEST IC" value={val(m.ic)} hint="holdout — honest" tone={m.ic > 0.02 ? 'good' : m.ic < 0 ? 'bad' : 'neutral'} />
          <StatCard label="TEST Rank IC" value={val(m.rank_ic)} hint="holdout" tone={m.rank_ic > 0.02 ? 'good' : m.rank_ic < 0 ? 'bad' : 'neutral'} />
          <StatCard label="TEST ICIR" value={m.icir == null ? '—' : Number(m.icir).toFixed(3)} tone={m.icir > 0.3 ? 'good' : m.icir < 0 ? 'bad' : 'neutral'} />
          <StatCard label="Test days" value={m.n_days} hint={`${val(m.positive_day_pct)}% pos`} />
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* single-feature leaderboard (validation) */}
        <div className="rounded-xl border border-md-outline-var bg-md-surface-con p-4">
          <div className="text-sm font-medium text-md-on-surface mb-1">Best single features <span className="text-md-on-surface-var font-normal">(validation Rank IC)</span></div>
          <div className="text-[10px] text-md-on-surface-var mb-2">which signals predict on their own — high here ≠ high model importance</div>
          <div className="flex flex-col gap-1 max-h-72 overflow-auto pr-1">
            {res.leaderboard.slice(0, 15).map((d, i) => (
              <div key={d.feature} className="flex items-center gap-2 text-xs">
                <span className="w-5 text-right text-md-on-surface-var/60">{i + 1}</span>
                <span className="w-28 shrink-0 font-mono text-md-on-surface-var truncate" title={d.feature}>{colLabel(d.feature)}</span>
                <span className={`w-16 text-right font-mono ${tone(d.valid_rank_ic)}`}>{val(d.valid_rank_ic)}</span>
                <span className="text-[10px] text-md-on-surface-var/60">RankIC</span>
              </div>
            ))}
          </div>
        </div>

        {/* greedy build path */}
        <div className="rounded-xl border border-md-outline-var bg-md-surface-con p-4">
          <div className="text-sm font-medium text-md-on-surface mb-1">Greedy build path <span className="text-md-on-surface-var font-normal">(validation Rank IC)</span></div>
          <div className="text-[10px] text-md-on-surface-var mb-2">each step adds the feature that most improves validation — stops when nothing helps</div>
          <div className="flex flex-col gap-1.5">
            {res.greedy_path.map(p => (
              <div key={p.step} className="flex items-center gap-2 text-xs">
                <span className="text-md-on-surface-var/60">{p.step}.</span>
                <span className="font-mono text-md-on-surface">+ {colLabel(p.added)}</span>
                <span className="flex-1 border-b border-dashed border-md-outline-var/40" />
                <span className={`font-mono ${tone(p.valid_rank_ic)}`}>{val(p.valid_rank_ic)}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="text-[11px] text-md-on-surface-var/80 rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2">
        {res.note}
      </div>
    </div>
  )
}

function Scatter({ points }) {
  if (!points?.length) return null
  const W = 360, H = 240, P = 28
  const xs = points.map(p => p.pred), ys = points.map(p => p.actual)
  const xmin = Math.min(...xs), xmax = Math.max(...xs)
  const ymin = Math.min(...ys), ymax = Math.max(...ys)
  const sx = v => P + ((v - xmin) / (xmax - xmin || 1)) * (W - 2 * P)
  const sy = v => H - P - ((v - ymin) / (ymax - ymin || 1)) * (H - 2 * P)
  const zeroY = ymin < 0 && ymax > 0 ? sy(0) : null
  const zeroX = xmin < 0 && xmax > 0 ? sx(0) : null
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full max-w-[420px]">
      <rect x={P} y={P} width={W - 2 * P} height={H - 2 * P} fill="none" stroke="currentColor" className="text-md-outline-var" />
      {zeroY && <line x1={P} y1={zeroY} x2={W - P} y2={zeroY} stroke="currentColor" className="text-md-outline-var/50" strokeDasharray="3 3" />}
      {zeroX && <line x1={zeroX} y1={P} x2={zeroX} y2={H - P} stroke="currentColor" className="text-md-outline-var/50" strokeDasharray="3 3" />}
      {points.map((p, i) => (
        <circle key={i} cx={sx(p.pred)} cy={sy(p.actual)} r={1.5}
          className={p.actual >= 0 ? 'fill-emerald-400/60' : 'fill-rose-400/60'} />
      ))}
      {/* legend: dot colour = realised outcome */}
      <circle cx={W - P - 70} cy={P + 6} r={2.5} className="fill-emerald-400/80" />
      <text x={W - P - 63} y={P + 9} className="fill-current text-md-on-surface-var" fontSize="8">up</text>
      <circle cx={W - P - 40} cy={P + 6} r={2.5} className="fill-rose-400/80" />
      <text x={W - P - 33} y={P + 9} className="fill-current text-md-on-surface-var" fontSize="8">down</text>
      <text x={W / 2} y={H - 6} textAnchor="middle" className="fill-current text-md-on-surface-var" fontSize="9">prediction →</text>
      <text x={10} y={H / 2} textAnchor="middle" transform={`rotate(-90 10 ${H / 2})`} className="fill-current text-md-on-surface-var" fontSize="9">actual next-bar return →</text>
    </svg>
  )
}

function LogPanel({ log }) {
  const [open, setOpen] = useState(false)
  if (!log?.length) return null
  return (
    <div className="rounded-lg border border-md-outline-var bg-md-surface">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-3 py-2 text-xs text-md-on-surface-var hover:bg-white/5"
      >
        <span>backend log ({log.length})</span>
        <span>{open ? '▾' : '▸'}</span>
      </button>
      {open && (
        <pre className="max-h-56 overflow-auto px-3 pb-3 text-[11px] leading-relaxed font-mono text-md-on-surface-var whitespace-pre-wrap">
          {log.join('\n')}
        </pre>
      )}
    </div>
  )
}

export default function QlibPanel() {
  const [universe, setUniverse] = useState('sp500')
  const [families, setFamilies] = useState([])
  const [forbidden, setForbidden] = useState([])
  const [selected, setSelected] = useState(() => new Set(SMOKE_FEATURES))
  const [models, setModels] = useState([])
  const [model, setModel] = useState('lightgbm')
  const [horizon, setHorizon] = useState(1)
  const [lookback, setLookback] = useState(0)
  const [splits, setSplits] = useState(null)
  const [dateFrom, setDateFrom] = useState('2021-01-01')
  const [dateTo, setDateTo] = useState('2026-06-01')
  const [minBars, setMinBars] = useState(250)
  const [maxFeatures, setMaxFeatures] = useState(6)

  const [colErr, setColErr] = useState(null)
  const [job, setJob] = useState(null)          // {id, kind, status, log, result, error}
  const [busy, setBusy] = useState(false)
  const pollRef = useRef(null)

  // ── load columns + models ───────────────────────────────────────────────
  useEffect(() => {
    let alive = true
    setColErr(null)
    api.qlibColumns(universe)
      .then(d => { if (alive) { setFamilies(d.families || []); setForbidden(d.forbidden || []) } })
      .catch(e => { if (alive) setColErr(String(e)) })
    return () => { alive = false }
  }, [universe])

  useEffect(() => {
    api.qlibModels()
      .then(d => { setModels(d.models || []); setSplits(d.default_splits) })
      .catch(() => {})
  }, [])

  // ── polling ───────────────────────────────────────────────────────────────
  const stopPoll = useCallback(() => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null }
  }, [])
  useEffect(() => stopPoll, [stopPoll])

  const pollJob = useCallback((jobId) => {
    stopPoll()
    pollRef.current = setInterval(async () => {
      try {
        const j = await api.qlibJob(jobId)
        setJob(j)
        if (j.status === 'done' || j.status === 'error') {
          stopPoll(); setBusy(false)
        }
      } catch (e) {
        setJob(j0 => ({ ...(j0 || {}), status: 'error', error: String(e) }))
        stopPoll(); setBusy(false)
      }
    }, POLL_MS)
  }, [stopPoll])

  const features = Array.from(selected)

  const start = async (kind) => {
    if (!features.length || busy) return
    setBusy(true); setJob({ kind, status: 'queued', log: [] })
    const body = {
      universe, features, date_from: dateFrom, date_to: dateTo,
      min_bars: Number(minBars), horizon: Number(horizon), lookback: Number(lookback),
      ...(kind === 'train' || kind === 'search' ? { model, splits } : {}),
      ...(kind === 'search' ? { max_features: Number(maxFeatures) } : {}),
    }
    try {
      const fn = kind === 'build' ? api.qlibBuild : kind === 'search' ? api.qlibSearch : api.qlibTrain
      const { job_id } = await fn(body)
      pollJob(job_id)
    } catch (e) {
      setJob({ kind, status: 'error', error: String(e), log: [] }); setBusy(false)
    }
  }

  // ── feature selection helpers ──────────────────────────────────────────────
  const toggle = (name) => setSelected(s => {
    const n = new Set(s); n.has(name) ? n.delete(name) : n.add(name); return n
  })
  const toggleFamily = (cols, on) => setSelected(s => {
    const n = new Set(s); cols.forEach(c => on ? n.add(c.name) : n.delete(c.name)); return n
  })

  const result = job?.status === 'done' && job.kind === 'train' ? job.result : null
  const buildResult = job?.status === 'done' && job.kind === 'build' ? job.result : null
  const searchResult = job?.status === 'done' && job.kind === 'search' ? job.result : null
  const m = result?.metrics

  return (
    <div className="flex flex-col gap-4">
      {/* header */}
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div>
          <h2 className="text-lg font-semibold text-md-on-surface">🧪 QLIB · Factor Lab</h2>
          <p className="text-xs text-md-on-surface-var max-w-2xl mt-1">
            Train LightGBM on your precomputed signals and measure whether they predict the
            next-bar forward return. Label is price-derived (<code>close[T+2]/close[T+1]−1</code>);
            forward-return columns are excluded so there is no look-ahead leak. Splits are
            time-ordered (out-of-sample).
          </p>
        </div>
        <select
          value={universe}
          onChange={e => setUniverse(e.target.value)}
          className="bg-md-surface-con border border-md-outline-var rounded-md-sm px-3 py-1.5 text-sm text-md-on-surface"
        >
          {UNIVERSES.map(u => <option key={u.id} value={u.id}>{u.label}</option>)}
        </select>
      </div>

      {colErr && (
        <div className="rounded-lg border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-xs text-rose-300">
          Failed to load columns: {colErr}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* ── left: feature picker ─────────────────────────────────────────── */}
        <div className="lg:col-span-2 rounded-xl border border-md-outline-var bg-md-surface-con p-3">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-md-on-surface">
              Features <span className="text-md-on-surface-var">({selected.size} selected)</span>
            </span>
            <div className="flex gap-2">
              <button onClick={() => setSelected(new Set(SMOKE_FEATURES))}
                className="text-[11px] px-2 py-1 rounded-md-sm border border-md-outline-var text-md-on-surface-var hover:bg-white/5">
                smoke set
              </button>
              <button onClick={() => setSelected(new Set())}
                className="text-[11px] px-2 py-1 rounded-md-sm border border-md-outline-var text-md-on-surface-var hover:bg-white/5">
                clear
              </button>
            </div>
          </div>
          <div className="max-h-[420px] overflow-auto pr-1 flex flex-col gap-3">
            {families.map(fam => {
              const allOn = fam.columns.every(c => selected.has(c.name))
              return (
                <div key={fam.family}>
                  <div className="flex items-center gap-2 mb-1 sticky top-0 bg-md-surface-con py-0.5">
                    <button onClick={() => toggleFamily(fam.columns, !allOn)}
                      className="text-[11px] text-md-primary hover:underline">
                      {allOn ? '−' : '+'}
                    </button>
                    <span className="text-xs font-semibold text-md-on-surface-var uppercase tracking-wide">
                      {fam.family} <span className="opacity-60">({fam.columns.length})</span>
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-1.5">
                    {fam.columns.map(c => {
                      const on = selected.has(c.name)
                      return (
                        <button key={c.name} onClick={() => toggle(c.name)}
                          title={`${colLabel(c.name)} · ${c.name} · ${c.type} · ${c.kind}`}
                          className={[
                            'text-[11px] px-2 py-0.5 rounded-full border font-mono transition-colors',
                            on
                              ? 'bg-md-primary-container text-md-on-primary-container border-md-primary/40'
                              : 'bg-md-surface text-md-on-surface-var border-md-outline-var hover:bg-white/5',
                            c.kind === 'categorical' ? 'italic' : '',
                          ].join(' ')}>
                          {colLabel(c.name)}
                        </button>
                      )
                    })}
                  </div>
                </div>
              )
            })}
          </div>
          {forbidden.length > 0 && (
            <p className="mt-2 text-[10px] text-md-on-surface-var/70">
              {forbidden.length} outcome columns (fwd_/mfe_/mae_/hit_/drop_) are hidden — they can never be features.
            </p>
          )}
        </div>

        {/* ── right: controls ──────────────────────────────────────────────── */}
        <div className="rounded-xl border border-md-outline-var bg-md-surface-con p-3 flex flex-col gap-3">
          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="text-[11px] uppercase tracking-wide text-md-on-surface-var">Model</label>
              <select value={model} onChange={e => setModel(e.target.value)}
                className="mt-1 w-full bg-md-surface border border-md-outline-var rounded-md-sm px-2 py-1.5 text-sm text-md-on-surface">
                {models.map(mm => (
                  <option key={mm.name} value={mm.name} disabled={!mm.ready}>
                    {mm.label}{mm.ready ? '' : ' — soon'}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="text-[11px] uppercase tracking-wide text-md-on-surface-var" title="How many bars the label looks forward. Enter at next-bar close, hold N bars.">
                Label horizon
              </label>
              <select value={horizon} onChange={e => setHorizon(Number(e.target.value))}
                className="mt-1 w-full bg-md-surface border border-md-outline-var rounded-md-sm px-2 py-1.5 text-sm text-md-on-surface">
                <option value={1}>next-bar (1)</option>
                <option value={5}>5 bars</option>
                <option value={10}>10 bars</option>
                <option value={20}>20 bars</option>
              </select>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="text-[11px] uppercase tracking-wide text-md-on-surface-var">Date from</label>
              <input value={dateFrom} onChange={e => setDateFrom(e.target.value)}
                className="mt-1 w-full bg-md-surface border border-md-outline-var rounded-md-sm px-2 py-1.5 text-sm font-mono text-md-on-surface" />
            </div>
            <div>
              <label className="text-[11px] uppercase tracking-wide text-md-on-surface-var">Date to</label>
              <input value={dateTo} onChange={e => setDateTo(e.target.value)}
                className="mt-1 w-full bg-md-surface border border-md-outline-var rounded-md-sm px-2 py-1.5 text-sm font-mono text-md-on-surface" />
            </div>
          </div>
          {splits && (
            <div className="text-[11px] text-md-on-surface-var">
              <div className="uppercase tracking-wide mb-1">Time split (OOS)</div>
              {['train', 'valid', 'test'].map(k => (
                <div key={k} className="flex items-center gap-2 mb-1">
                  <span className="w-12 capitalize">{k}</span>
                  <input value={splits[k][0]} onChange={e => setSplits(s => ({ ...s, [k]: [e.target.value, s[k][1]] }))}
                    className="flex-1 bg-md-surface border border-md-outline-var rounded-md-sm px-1.5 py-1 font-mono" />
                  <span>→</span>
                  <input value={splits[k][1]} onChange={e => setSplits(s => ({ ...s, [k]: [s[k][0], e.target.value] }))}
                    className="flex-1 bg-md-surface border border-md-outline-var rounded-md-sm px-1.5 py-1 font-mono" />
                </div>
              ))}
            </div>
          )}
          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="text-[11px] uppercase tracking-wide text-md-on-surface-var"
                title="Include each feature's value from the previous N bars, so the model sees the recent sequence/path — not just the signal bar.">
                Lookback (seq)
              </label>
              <select value={lookback} onChange={e => setLookback(Number(e.target.value))}
                className="mt-1 w-full bg-md-surface border border-md-outline-var rounded-md-sm px-2 py-1.5 text-sm text-md-on-surface">
                <option value={0}>this bar only</option>
                <option value={1}>+1 prior bar</option>
                <option value={2}>+2 prior bars</option>
                <option value={3}>+3 prior bars</option>
                <option value={5}>+5 prior bars</option>
              </select>
            </div>
            <div>
              <label className="text-[11px] uppercase tracking-wide text-md-on-surface-var">Min bars / ticker</label>
              <input type="number" value={minBars} onChange={e => setMinBars(e.target.value)}
                className="mt-1 w-full bg-md-surface border border-md-outline-var rounded-md-sm px-2 py-1.5 text-sm font-mono text-md-on-surface" />
            </div>
          </div>
          <div className="flex gap-2 mt-1">
            <button onClick={() => start('build')} disabled={busy || !selected.size}
              className="flex-1 px-3 py-2 rounded-md-sm text-sm border border-md-outline-var text-md-on-surface hover:bg-white/5 disabled:opacity-40">
              Build dataset
            </button>
            <button onClick={() => start('train')} disabled={busy || !selected.size}
              className="flex-1 px-3 py-2 rounded-md-sm text-sm bg-md-primary text-md-on-primary font-medium hover:opacity-90 disabled:opacity-40">
              {busy ? 'Running…' : 'Train'}
            </button>
          </div>
          <div className="flex items-center gap-2">
            <button onClick={() => start('search')} disabled={busy || selected.size < 2}
              title="Auto-rank the selected features on validation: best single features + greedy best subset. Test stays a holdout."
              className="flex-1 px-3 py-2 rounded-md-sm text-sm border border-md-primary/50 text-md-primary hover:bg-md-primary/10 disabled:opacity-40">
              🔎 Auto-search combinations
            </button>
            <select value={maxFeatures} onChange={e => setMaxFeatures(Number(e.target.value))}
              title="Max features the greedy search may pick"
              className="bg-md-surface border border-md-outline-var rounded-md-sm px-2 py-2 text-xs text-md-on-surface-var">
              {[3, 4, 5, 6, 8, 10].map(n => <option key={n} value={n}>≤{n} feats</option>)}
            </select>
          </div>
        </div>
      </div>

      {/* ── status / results ──────────────────────────────────────────────── */}
      {job && (
        <div className="flex flex-col gap-3">
          <div className="flex items-center gap-2 text-sm">
            <span className={[
              'px-2 py-0.5 rounded-full text-[11px] font-medium',
              job.status === 'done' ? 'bg-emerald-500/15 text-emerald-300'
                : job.status === 'error' ? 'bg-rose-500/15 text-rose-300'
                : 'bg-amber-500/15 text-amber-300 animate-pulse',
            ].join(' ')}>
              {job.kind} · {job.status}
            </span>
            {busy && <span className="text-xs text-md-on-surface-var">polling…</span>}
          </div>

          {job.status === 'error' && (
            <div className="rounded-lg border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-xs text-rose-300 whitespace-pre-wrap">
              {job.error}
            </div>
          )}

          {searchResult && <SearchResults res={searchResult} onUseSet={(cols) => {
            setSelected(new Set(cols))
            window.scrollTo({ top: 0, behavior: 'smooth' })
          }} />}

          {buildResult && (
            <div className="rounded-xl border border-md-outline-var bg-md-surface-con p-4 text-sm text-md-on-surface grid grid-cols-2 sm:grid-cols-4 gap-3">
              <StatCard label="rows" value={buildResult.rows?.toLocaleString()} />
              <StatCard label="tickers" value={buildResult.tickers} />
              <StatCard label="features" value={buildResult.features_used?.length} hint={`${buildResult.features_dropped_null?.length || 0} dropped null`} />
              <StatCard label="cached" value={`${buildResult.cache_mb ?? '—'} MB`} hint={`${buildResult.date_min} → ${buildResult.date_max}`} />
            </div>
          )}

          {result && m && (
            <>
              <div className="text-[11px] text-md-on-surface-var">
                <span className="font-medium text-md-on-surface">{result.model_label}</span>
                {' · label: '}<code className="text-md-on-surface-var">{result.label_def}</code>
                {result.lookback > 0 && (
                  <span> · lookback {result.lookback} → {result.n_features} features ({result.n_base_features} base × seq)</span>
                )}
              </div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                <StatCard label="IC" value={fmt(m.ic)} tone={m.ic > 0.02 ? 'good' : m.ic < 0 ? 'bad' : 'neutral'} hint="mean daily x-sectional" />
                <StatCard label="Rank IC" value={fmt(m.rank_ic)} tone={m.rank_ic > 0.02 ? 'good' : m.rank_ic < 0 ? 'bad' : 'neutral'} hint="Spearman" />
                <StatCard label="ICIR" value={fmt(m.icir, 3)} tone={m.icir > 0.3 ? 'good' : m.icir < 0 ? 'bad' : 'neutral'} hint="IC / std(IC)" />
                <StatCard label="Test days" value={m.n_days} hint={`${fmt(m.positive_day_pct, 1)}% positive`} />
              </div>

              {(m.ic !== null && Math.abs(m.ic) < 0.02 && (m.icir ?? 0) < 0.3) && (
                <div className="rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-200">
                  Honest read: IC and ICIR are near zero — on this feature set / horizon there's
                  little or no real cross-sectional edge. That's a valid result, not an error.
                </div>
              )}

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <div className="rounded-xl border border-md-outline-var bg-md-surface-con p-4">
                  <div className="text-sm font-medium text-md-on-surface mb-3">Feature importance (gain)</div>
                  <FeatureBars items={result.feature_importance} />
                </div>
                <div className="rounded-xl border border-md-outline-var bg-md-surface-con p-4">
                  <div className="text-sm font-medium text-md-on-surface mb-1">Prediction vs actual (test)</div>
                  <div className="text-md-on-surface-var">
                    <Scatter points={result.scatter} />
                  </div>
                  {result.quantile_spread?.available && (
                    <div className="mt-2 text-[11px] text-md-on-surface-var">
                      Quantile mean return (%): [{result.quantile_spread.by_quantile.join(', ')}]
                      &nbsp;·&nbsp; top−bottom: <span className="font-mono">{result.quantile_spread.top_minus_bottom_pct}%</span>
                    </div>
                  )}
                </div>
              </div>

              <div className="text-[11px] text-md-on-surface-var/80 rounded-lg border border-md-outline-var bg-md-surface px-3 py-2">
                {result.data_source_note}
              </div>
            </>
          )}

          <LogPanel log={job.log} />
        </div>
      )}
    </div>
  )
}
