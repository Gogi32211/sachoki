/**
 * The session's cost, always visible. React renders these numbers; it never computes one.
 *
 * `k_selectable` is the field that matters and the one a screen would get wrong on its own: it
 * is the space the algorithm chose from, not the number of cards rendered. Five results shown
 * while thirty-one were selectable means the multiplicity is thirty-one, and the panel says so
 * because the backend said so.
 *
 * The lineage row appears only when it disagrees with `k_exposed`, and that disagreement is the
 * whole reason it exists. A forked session starts with an empty ledger and looks clean; the
 * results its parent already exposed still happened. `k_exposed` answers "what has this session
 * seen", `k_exposed_lineage` answers "what has been seen before this claim was chosen", and only
 * the second one is the search.
 */
import type { ResearchSessionView } from '../semantics/types';

function Row({ label, value, hint, strong }:
             { label: string; value: string; hint?: string; strong?: boolean }) {
  return (
    <div className="flex items-baseline justify-between gap-4 py-1">
      <span className={`text-xs ${strong ? 'text-md-on-surface' : 'text-md-on-surface-var'}`}>
        {label}
      </span>
      <span className={`font-mono text-sm ${strong ? 'text-md-warning' : 'text-md-on-surface'}`}>
        {value}
      </span>
      {hint && <span className="text-[11px] text-md-on-surface-var">{hint}</span>}
    </div>
  );
}

const MODE_NOTE: Record<string, string> = {
  EXPLORE:
    'Results are hypothesis-generating. Every new selectable specification is recorded. ' +
    'They cannot become confirmatory inside this session.',
  REGISTERED:
    'Frozen before any result was seen. Claim-defining controls are locked, and the search ' +
    'space is the one declared here — not the one that turns out to be convenient.',
  ACTIVE_REGISTERED:
    'Frozen, and results have now been seen. The declaration came first, which is the only ' +
    'thing that makes the verdict confirmatory.',
};

export function SessionAccountingPanel({ session }: { session: ResearchSessionView }) {
  const note = MODE_NOTE[session.mode] ?? '';
  const forked = session.parent_session_id !== '';
  const lineageDiffers = session.k_exposed_lineage !== session.k_exposed;

  return (
    <div className="rounded-lg border border-md-outline-var bg-md-surface-con p-4">
      <div className="flex items-baseline justify-between gap-2">
        <div className="text-xs font-semibold uppercase tracking-widest text-md-on-surface-var">
          {session.mode === 'EXPLORE' ? 'Exploration session' : session.mode.replace('_', ' ')}
        </div>
        {session.confirmatory_eligible === 'YES' && (
          <span className="rounded border border-md-warning bg-md-warning-con px-1.5 py-0.5
                           text-[10px] font-semibold uppercase tracking-wider text-md-warning">
            preregistered
          </span>
        )}
      </div>

      {note && (
        <p className="mt-2 text-[11px] leading-relaxed text-md-on-surface-var">{note}</p>
      )}

      {forked && (
        <p className="mt-2 rounded border border-md-outline-var px-2 py-1.5 text-[11px]
                      leading-relaxed text-md-on-surface-var">
          Forked from <span className="font-mono">{session.parent_session_id}</span>.
          {session.inherited_exposed === '0'
            ? ' Nothing had been exposed upstream, and the specification it starts from was still' +
              ' someone’s choice — so this session stays exploratory.'
            : ` It starts with an empty ledger and not from zero: ${session.inherited_exposed}` +
              ` result${session.inherited_exposed === '1' ? ' was' : 's were'} already seen` +
              ' upstream.'}
        </p>
      )}

      <div className="mt-3 divide-y divide-md-outline-var">
        <Row label="Claims exposed" value={session.k_exposed} />
        {/* k counts the (evidence, decision) pair. Without these two, "k = 7" cannot say whether
            seven effects were looked at or one effect under seven decision rules. */}
        <Row label="  · evidence claims" value={session.distinct_evidence_claims} />
        <Row label="  · decision specs" value={session.distinct_decision_specs} />
        {lineageDiffers && (
          <Row label="Exposed in lineage" value={session.k_exposed_lineage} strong
               hint={`depth ${session.lineage_depth}`} />
        )}
        <Row label="Selectable claims" value={session.k_selectable}
             hint={`displayed ≤ ${session.displayed_at_most}`} />
        <Row label="Revisits" value={session.revisits} />
        <Row label="Claim changes" value={session.changes_claim} />
        <Row label="Design changes" value={session.changes_design} />
        <Row label="Search-space changes" value={session.changes_search_space} />
        <Row label="Selection-path changes" value={session.changes_selection_path} />
        <Row label="Declared space" value={session.k_declared} />
        <Row label="Confirmatory" value={session.confirmatory_eligible} />
        <Row label="Ledger" value={`TRACKED · ${session.events} events`} />
      </div>

      <div className="mt-3 font-mono text-[10px] text-md-on-surface-var">
        {session.session_id} · {session.state_hash}
      </div>
      <div className="font-mono text-[10px] text-md-on-surface-var">
        {session.accounting_policy}
      </div>
    </div>
  );
}
