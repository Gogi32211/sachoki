/**
 * 🔬 Research — one home for the measurement work.
 *
 * It was scattered: the session ledger lived as a top-level tab, the column breakdown as a
 * Studio sub-tab, the k counter as a chip inside a third panel. Three levels of hierarchy for
 * one body of work, and no way to tell what was new.
 *
 * Nothing here is a copy. `ColumnBreakdownTab` and `ExactSequenceTab` are imported from
 * StudioPanel and `ComboLabScreen` from the studio island — the same components, hosted here.
 * A duplicated panel is the UI version of a duplicated allowlist: it works until the two drift,
 * and then the convenient one wins.
 *
 * Exact Sequence deliberately still appears under Studio as well. It is an OLD panel that gained
 * new abilities (the k chip, six bars), not a new place, and moving it would break the muscle
 * memory of the one screen here that already had users.
 *
 * STATE COMES FIRST, and that ordering is the argument. Everything statistical in the v2 forward
 * arc is frozen; what remains is waiting. A person opening this tab should see what is frozen and
 * how far the wait has got before they see a control that invites them to search.
 */
import { lazy, Suspense, useEffect, useState } from 'react'
import { api } from '../api'
import { ColumnBreakdownTab, ExactSequenceTab } from './StudioPanel'
import { ForwardStatusPanel } from '../studio/components/ForwardStatusPanel'

const ComboLabScreen = lazy(() =>
  import('../studio/pages/ComboLabScreen').then(m => ({ default: m.ComboLabScreen })))

const TABS = [
  { id: 'state',   label: '🧊 State'     },
  { id: 'measure', label: '🧪 Measure'   },
  { id: 'columns', label: '🔤 Columns'   },
  { id: 'seq',     label: '🎯 Sequences' },
  { id: 'combo',   label: '🧬 ComboLab'  },
  { id: 'replay2', label: '🔁 Replay 2'  },
]


// ═══════════════════════════════════════════════════════════════════════════════
// 🧪 Measure — type a condition, get a matched-control measurement, watch k tick.
//
// The engine is NakedStudy in its own worker process: outcomes recomputed from OHLC (entry
// next open, no exit rule), control drawn from the cell's own price × liquidity × year strata,
// clustered bootstrap by trading date. This panel only renders what comes back — the one
// statistical thing it knows is that it knows nothing.
//
// All four horizons return together on purpose. A horizon selector would be the cheapest
// multiplier of k on the screen: "try 5d, now try 10d" as two separate claims.
// ═══════════════════════════════════════════════════════════════════════════════
function MeasurePanel() {
  const [expr, setExpr] = useState("t_sig == 'T1' and rsi_14 < 35")
  const [uni, setUni] = useState('sp500')
  const [pal, setPal] = useState(null)
  const [res, setRes] = useState(null)
  const [err, setErr] = useState(null)
  const [busy, setBusy] = useState(false)

  useEffect(() => { api.studioMeasureColumns().then(setPal).catch(() => {}) }, [])

  const run = async () => {
    setBusy(true); setErr(null)
    try { setRes(await api.studioMeasure({ expr, universe: uni })) }
    catch (e) { setErr(String(e.message || e)); setRes(null) }
    finally { setBusy(false) }
  }

  const acc = res?.search_accounting
  return (
    <div className="space-y-3">
      <div className="rounded-lg border border-md-outline-var p-3">
        <div className="text-xs font-semibold uppercase tracking-widest text-md-on-surface mb-1">
          measure a condition
        </div>
        <p className="text-[11px] text-md-on-surface-var leading-relaxed mb-2">
          Outcome is recomputed from OHLC — entry at the next bar&apos;s open, no exit rule.
          Control is drawn from the cell&apos;s own price × liquidity × year strata. Our scores,
          tiers and forward labels are not inputs; price bands go in the expression
          (<span className="font-mono">close &gt;= 21 and close &lt;= 89</span>).
          Every distinct claim charges k, permanently.
        </p>
        <textarea value={expr} onChange={e => setExpr(e.target.value)} rows={2} spellCheck={false}
          data-measure-expr="1"
          className="w-full bg-md-surface-high border border-md-outline-var rounded font-mono
                     text-[12px] text-md-on-surface px-2 py-1.5" />
        <div className="mt-2 flex items-center gap-2 flex-wrap">
          <select value={uni} onChange={e => setUni(e.target.value)}
            className="bg-md-surface-high border border-md-outline-var rounded text-[11px] px-1.5 py-1 text-md-on-surface">
            {['sp500', 'nasdaq', 'russell2k'].map(u => <option key={u} value={u}>{u}</option>)}
          </select>
          <button onClick={run} disabled={busy} data-measure-run="1"
            className="px-3 py-1 rounded text-[12px] font-medium bg-md-primary text-md-on-primary
                       disabled:opacity-50">
            {busy ? 'measuring… (cold start can take ~1 min)' : '▶ Measure'}
          </button>
          {acc && (
            <span data-measure-k={String(acc.k_distinct ?? 'unknown')}
              className={'px-2 py-0.5 rounded text-[10px] font-mono border ' +
                (acc.k_distinct == null
                  ? 'bg-rose-900/30 text-rose-300 border-rose-800/60'
                  : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var')}>
              {acc.k_distinct == null ? 'k unknown — not counted'
                : `k ${acc.k_distinct} distinct · ${acc.queries} queries`}
              {acc.claim_is_new === false && <span className="text-emerald-400"> · repeat</span>}
            </span>
          )}
        </div>
        {pal && (
          <div className="mt-2 space-y-1">
            <div className="flex flex-wrap gap-1">
              {pal.examples.map(ex => (
                <button key={ex} onClick={() => setExpr(ex)} title="use this example"
                  className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-md-surface-high
                             text-md-on-surface-var hover:text-md-on-surface border border-md-outline-var">
                  {ex}
                </button>
              ))}
            </div>
            <div className="flex flex-wrap gap-1">
              {pal.columns.map(c => (
                <button key={c.column} title={c.label}
                  onClick={() => setExpr(p => (p ? p + ' and ' : '') + c.column)}
                  className={'px-1.5 py-0.5 rounded text-[10px] font-mono border border-md-outline-var ' +
                    (c.kind === 'numeric' ? 'text-sky-300/80' : 'text-md-on-surface-var') +
                    ' bg-md-surface-high hover:text-md-on-surface'}>
                  {c.column}
                </button>
              ))}
            </div>
            <div className="text-[10px] text-md-on-surface-var/70">{pal.language}</div>
          </div>
        )}
      </div>

      {err && (
        <div data-measure-error="1"
             className="rounded border border-md-error bg-md-error-container p-2 text-[11px]
                        text-md-on-surface whitespace-pre-wrap">{err}</div>
      )}

      {res && (
        <div className="rounded-lg border border-md-outline-var p-3 space-y-2">
          <div className="text-[11px] font-mono text-md-on-surface">{res.canonical}</div>
          <div className="text-[10px] font-mono text-md-on-surface-var">
            {res.claim_basis} · {res.universe} · n {res.n_matched.toLocaleString()} ·{' '}
            {res.pct_of_bars}% of bars · {res.per_day}/day
            {res.n_dropped_nonadjacent > 0 &&
              ` · ${res.n_dropped_nonadjacent} rows excluded (shift crossed a calendar gap)`}
            {' '}· matched on {res.matched_on} · {res.window[0]} → {res.window[1]}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-[11px]">
              <thead className="text-[10px] uppercase tracking-wider text-md-on-surface-var">
                <tr className="border-b border-md-outline-var">
                  {['N', 'n', 'up%', 'med', '95% CI (days)', 'Δmed', 'Δup', 'ΔMFE', 'ΔMAE', 'n_eff', 'vs control']
                    .map(h => <th key={h} className="text-right px-2 py-1 font-normal first:text-left">{h}</th>)}
                </tr>
              </thead>
              <tbody>
                {res.horizons.map(h => (
                  <tr key={h.N} data-measure-h={h.N} className="border-b border-md-outline-var/40 font-mono">
                    <td className="px-2 py-0.5 text-left">{h.N}</td>
                    <td className="px-2 py-0.5 text-right">{h.n.toLocaleString()}</td>
                    <td className="px-2 py-0.5 text-right">{h.up}</td>
                    <td className="px-2 py-0.5 text-right">{h.med > 0 ? '+' : ''}{h.med}</td>
                    <td className="px-2 py-0.5 text-right">[{h.lo}, {h.hi}]</td>
                    <td className={'px-2 py-0.5 text-right ' + (h.d_med > 0 ? 'text-emerald-300' : 'text-rose-300')}>
                      {h.d_med > 0 ? '+' : ''}{h.d_med}</td>
                    <td className="px-2 py-0.5 text-right">{h.d_up > 0 ? '+' : ''}{h.d_up}</td>
                    <td className="px-2 py-0.5 text-right">{h.d_mfe > 0 ? '+' : ''}{h.d_mfe}</td>
                    <td className="px-2 py-0.5 text-right">{h.d_mae > 0 ? '+' : ''}{h.d_mae}</td>
                    <td className="px-2 py-0.5 text-right">{h.n_eff.toLocaleString()}</td>
                    <td className={'px-2 py-0.5 text-right ' +
                      (h.separate ? 'text-emerald-300 font-semibold' : 'text-md-on-surface-var')}>
                      {h.separate ? 'SEPARATE' : 'overlaps'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="text-[10px] font-mono text-md-on-surface-var">
            baseline med: {res.baseline.map(b => `${b.N}d ${b.med > 0 ? '+' : ''}${b.med}`).join(' · ')}
            {' '}· yearly r10: {Object.entries(res.yearly_med_r10)
              .map(([y, v]) => `${y.slice(2)}:${v > 0 ? '+' : ''}${v}`).join(' ')}
          </div>
          <div className="text-[10px] text-md-on-surface-var/70">
            {res.outcome}. Δ columns are against the matched control; SEPARATE means the CIs do
            not overlap. Exploratory by construction — the data is already exposed. The honest
            path to more is a frozen forward spec.
          </div>
        </div>
      )}
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// 🔁 Replay 2 — the same edge_replay computation, wearing its passport.
//
// Not a second engine: the backend serves the identical cached result /api/edge-replay
// computes, and adds what those numbers never carried — the evidence axes, per-setup DSR,
// the family PBO, and a k counter for the exit-knob search space. A strong PF here is a
// hypothesis, not an edge, and the passport says so above the table instead of in a doc.
// ═══════════════════════════════════════════════════════════════════════════════
function Replay2Panel() {
  const [months, setMonths] = useState(36)
  const [res, setRes] = useState(null)
  const [err, setErr] = useState(null)
  const [busy, setBusy] = useState(false)

  const run = async () => {
    setBusy(true); setErr(null)
    try { setRes(await api.studioReplay2({ months })) }
    catch (e) { setErr(String(e.message || e)) }
    finally { setBusy(false) }
  }

  const acc = res?.search_accounting
  const p = res?.passport
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2 flex-wrap">
        <select value={months} onChange={e => setMonths(Number(e.target.value))}
          className="bg-md-surface-high border border-md-outline-var rounded text-[11px] px-1.5 py-1 text-md-on-surface">
          {[12, 36, 62].map(m => <option key={m} value={m}>{m} months</option>)}
        </select>
        <button onClick={run} disabled={busy} data-replay2-run="1"
          className="px-3 py-1 rounded text-[12px] font-medium bg-md-primary text-md-on-primary disabled:opacity-50">
          {busy ? 'running… (cold path-sim can take ~2 min)' : '▶ Replay'}
        </button>
        {acc && (
          <span data-replay2-k="1" className="px-2 py-0.5 rounded text-[10px] font-mono border
            bg-md-surface-high text-md-on-surface-var border-md-outline-var">
            {acc.k_distinct == null ? 'k unknown — not counted'
              : `k ${acc.k_distinct} distinct · ${acc.queries} queries`}
            {acc.claim_is_new === false && <span className="text-emerald-400"> · repeat</span>}
          </span>
        )}
      </div>

      {err && <div className="rounded border border-md-error bg-md-error-container p-2
                              text-[11px] text-md-on-surface">{err}</div>}

      {p && (
        <div data-replay2-passport="1"
             className="rounded border border-md-warning bg-md-warning-container p-3">
          <div className="text-xs font-semibold uppercase tracking-widest text-md-warning">
            exploratory historical evidence
          </div>
          <p className="mt-1 text-[11px] leading-relaxed text-md-on-surface">
            {p.evidence_origin.toLowerCase()} · instrument {p.instrument_validation_basis.toLowerCase()} ·
            {' '}{p.result_role.toLowerCase().replaceAll('_', ' ')}.
            {' '}{p.why_exploratory_forever}
          </p>
          <div className="mt-1 text-[10px] font-mono text-md-on-surface-var">
            ceiling: {p.ceiling.join(' · ')}
          </div>
          {res.deflation?.family_pbo != null && (
            <div className="mt-1 text-[10px] font-mono text-md-on-surface-var">
              family PBO {res.deflation.family_pbo} · OOS/IS {res.deflation.oos_is_ratio} ·
              {' '}{res.deflation.source}
            </div>
          )}
        </div>
      )}

      {res && (
        <div className="overflow-auto max-h-[560px] rounded border border-md-outline-var">
          <table className="w-full text-[11px]">
            <thead className="sticky top-0 bg-md-surface-con text-[10px] uppercase tracking-wider text-md-on-surface-var">
              <tr>{['setup', 'n', 'med%', 'win%', 'pf', 'worst yr', 'yrs+', 'dsr', 'claim']
                .map(h => <th key={h} className="text-right px-2 py-1 font-normal first:text-left">{h}</th>)}</tr>
            </thead>
            <tbody>
              {res.rows.map(r => (
                <tr key={r.setup} data-replay2-row={r.setup}
                    className="border-t border-md-outline-var/40 font-mono">
                  <td className="px-2 py-0.5 text-left text-md-on-surface">{r.setup}</td>
                  <td className="px-2 py-0.5 text-right">{r.n.toLocaleString()}</td>
                  <td className="px-2 py-0.5 text-right">{r.median > 0 ? '+' : ''}{r.median}</td>
                  <td className="px-2 py-0.5 text-right">{r.win}</td>
                  <td className="px-2 py-0.5 text-right">{r.pf ?? '—'}</td>
                  <td className={'px-2 py-0.5 text-right ' +
                    (r.worst_year < 0 ? 'text-rose-300' : 'text-emerald-300')}>{r.worst_year}</td>
                  <td className="px-2 py-0.5 text-right">{r.pos_years}/{r.total_years}</td>
                  <td className={'px-2 py-0.5 text-right ' +
                    ((r.dsr ?? 0) <= 0 ? 'text-rose-300' : '')}>{r.dsr ?? '—'}</td>
                  <td className="px-2 py-0.5 text-right text-md-on-surface-var/60">{r.claim_hash}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function StatePanel() {
  return (
    <div className="space-y-3">
      <ForwardStatusPanel />
      <div className="rounded-lg border border-md-outline-var p-4 text-[11px] leading-relaxed
                      text-md-on-surface-var">
        <div className="text-xs font-semibold uppercase tracking-widest text-md-on-surface mb-2">
          what is frozen
        </div>
        <p>
          Hypothesis, estimand, support policy, bootstrap, decision rule, ranking policy, evidence
          boundary, adapter and look timing were all fixed before the first forward observation
          exists. The hashes above are the record. Nothing statistical remains to decide: when the
          counter reaches its trigger, the only permitted action is the already-frozen first
          prospective evaluation — no new parameter, threshold, population or estimator may be
          chosen at that moment.
        </p>
        <p className="mt-2">
          The panels beside this one are exploratory by construction. They run on data that has
          already been exposed, so what they produce is hypothesis-generating and is counted, not
          confirmatory.
        </p>
      </div>
    </div>
  )
}

export default function ResearchPanel() {
  const [tab, setTab] = useState('state')
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-1">
        {TABS.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)}
            data-research-tab={t.id}
            className={
              'px-3 py-1 rounded text-[12px] font-medium transition-colors ' +
              (tab === t.id
                ? 'bg-md-primary text-md-on-primary'
                : 'bg-md-surface-high text-md-on-surface-var hover:text-md-on-surface')
            }>
            {t.label}
          </button>
        ))}
      </div>

      <Suspense fallback={<div className="text-[11px] text-md-on-surface-var">loading…</div>}>
        {tab === 'state'   && <StatePanel />}
        {tab === 'measure' && <MeasurePanel />}
        {tab === 'replay2' && <Replay2Panel />}
        {tab === 'columns' && <ColumnBreakdownTab />}
        {tab === 'seq'     && <ExactSequenceTab tf="1d" />}
        {tab === 'combo'   && <ComboLabScreen />}
      </Suspense>
    </div>
  )
}
