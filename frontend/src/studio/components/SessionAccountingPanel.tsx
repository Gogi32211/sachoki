/**
 * The session's cost, always visible. React renders these numbers; it never computes one.
 *
 * `k_selectable` is the field that matters and the one a screen would get wrong on its own: it
 * is the space the algorithm chose from, not the number of cards rendered. Five results shown
 * while thirty-one were selectable means the multiplicity is thirty-one, and the panel says so
 * because the backend said so.
 */
import type { ResearchSessionView } from '../semantics/types';

function Row({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="flex items-baseline justify-between gap-4 py-1">
      <span className="text-xs text-md-on-surface-var">{label}</span>
      <span className="font-mono text-sm text-md-on-surface">{value}</span>
      {hint && <span className="text-[11px] text-md-on-surface-var">{hint}</span>}
    </div>
  );
}

export function SessionAccountingPanel({ session }: { session: ResearchSessionView }) {
  const exploratory = session.confirmatory_eligible === 'NO';
  return (
    <div className="rounded-lg border border-md-outline-var bg-md-surface-con p-4">
      <div className="text-xs font-semibold uppercase tracking-widest text-md-on-surface-var">
        {session.mode === 'EXPLORE' ? 'Exploration session' : session.mode}
      </div>
      {exploratory && (
        <p className="mt-2 text-[11px] leading-relaxed text-md-on-surface-var">
          Results are hypothesis-generating. Every new selectable specification is recorded.
          They cannot become confirmatory inside this session.
        </p>
      )}
      <div className="mt-3 divide-y divide-md-outline-var">
        <Row label="Claims exposed" value={session.k_exposed} />
        <Row label="Selectable claims" value={session.k_selectable}
             hint={`displayed ≤ ${session.displayed_at_most}`} />
        <Row label="Revisits" value={session.revisits} />
        <Row label="Claim changes" value={session.changes_claim} />
        <Row label="Search-space changes" value={session.changes_search_space} />
        <Row label="Confirmatory" value={session.confirmatory_eligible} />
        <Row label="Ledger" value={`TRACKED · ${session.events} events`} />
      </div>
      <div className="mt-3 font-mono text-[10px] text-md-on-surface-var">
        {session.session_id} · {session.state_hash}
      </div>
    </div>
  );
}
