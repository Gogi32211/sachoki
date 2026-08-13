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
import { lazy, Suspense, useState } from 'react'
import { ColumnBreakdownTab, ExactSequenceTab } from './StudioPanel'
import { ForwardStatusPanel } from '../studio/components/ForwardStatusPanel'

const ComboLabScreen = lazy(() =>
  import('../studio/pages/ComboLabScreen').then(m => ({ default: m.ComboLabScreen })))

const TABS = [
  { id: 'state',   label: '🧊 State'     },
  { id: 'columns', label: '🔤 Columns'   },
  { id: 'seq',     label: '🎯 Sequences' },
  { id: 'combo',   label: '🧬 ComboLab'  },
]

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
        {tab === 'columns' && <ColumnBreakdownTab />}
        {tab === 'seq'     && <ExactSequenceTab tf="1d" />}
        {tab === 'combo'   && <ComboLabScreen />}
      </Suspense>
    </div>
  )
}
