/**
 * The results table. It renders rows; it does not choose them.
 *
 * There is no `sort()` and no `slice()` in this file, and that absence is the contract. The
 * server ranked `ranked_count` claims and released `displayed_count` — the rest never arrived,
 * so this component could not show them if it wanted to. A frontend that held the full ranking
 * and cut it would be a second search engine, and the exposure counter would be wrong by the
 * difference.
 *
 * The header is deliberately loud about three numbers that a normal table would blur into one:
 *
 *     SELECTABLE   what the algorithm could pick a winner from      multiplicity
 *     RANKED       what it ordered
 *     EXPOSED      what was made available to a person              what k charges for
 *
 * A stale run stays on screen and stops being actionable. Removing it would erase the reason the
 * previous table no longer matches the controls; leaving it clickable would let a verdict attach
 * to a specification nobody is looking at.
 */
import type { RankingProvenanceView, ResultRowView, SearchRunView } from '../semantics/types';

/**
 * The ranking banner, drawn from `ranking_provenance` and never from the sentence beside it.
 *
 * The tempting version of this component reads `display_banner` and renders it. That works until
 * the copy changes — a translation, a shorter line, a designer trimming three lines to one — and
 * then the standing of the ranking leaves the screen without anything failing. So the headline
 * below is derived from `ranking_usage`, and the server's copy is rendered underneath as detail.
 * Delete `display_banner` from the payload entirely and the banner still says what this is.
 *
 *     POST_EXPOSURE_EXPLORATORY   banner required
 *     PROSPECTIVE_REGISTERED      exploratory banner forbidden, and its absence is the signal
 *     UNKNOWN                     loudest banner. Fail closed: an unrecognised standing is not
 *                                 a reason to show less, it is a reason to show more.
 */
function RankingBanner({ p }: { p: RankingProvenanceView }) {
  if (p.ranking_usage === 'PROSPECTIVE_REGISTERED') {
    return (
      <div data-ranking-usage={p.ranking_usage} data-exploratory="0"
           className="mt-3 text-[10px] uppercase tracking-widest text-md-on-surface-var">
        prospective ranking · policy <span className="font-mono">{p.ranking_policy_hash}</span>{' '}
        registered before this evidence existed
      </div>
    );
  }
  const unknown = p.ranking_usage === 'UNKNOWN';
  return (
    <div data-ranking-usage={p.ranking_usage} data-exploratory="1"
         data-preregistered={String(p.preregistered_for_snapshot)}
         className="mt-3 rounded border border-md-warning bg-md-warning-container p-3">
      <div className="text-xs font-semibold uppercase tracking-widest text-md-warning">
        {unknown ? 'ranking standing unknown' : 'exploratory ranking'}
      </div>
      <p className="mt-1 text-[11px] leading-relaxed text-md-on-surface">
        {unknown
          ? 'This build does not recognise the standing the server sent for this ranking, so it '
            + 'is shown as the weaker of the two. Nothing here is a preregistered selection.'
          : 'The ranking policy was registered after this evidence was exposed, so the order is '
            + 'exploratory and not a preregistered selection for this snapshot.'}
      </p>
      {p.display_banner.length > 0 && (
        <ul data-banner-copy="1" className="mt-1 text-[10px] text-md-on-surface-var">
          {p.display_banner.map((line) => <li key={line}>{line}</li>)}
        </ul>
      )}
      <div className="mt-1 text-[10px] font-mono text-md-on-surface-var">
        {p.ranking_policy_version} · {p.ranking_policy_hash}
      </div>
    </div>
  );
}

const VERDICT_CLS: Record<string, string> = {
  BUILD: 'text-md-warning',
  REJECT: 'text-md-error',
  UNRESOLVED: 'text-md-on-surface-var',
};

function Count({ label, value, hint }: { label: string; value: number; hint?: string }) {
  return (
    <div className="flex flex-col">
      <span className="font-mono text-lg text-md-on-surface">{value}</span>
      <span className="text-[10px] uppercase tracking-widest text-md-on-surface-var">{label}</span>
      {hint && <span className="text-[10px] text-md-on-surface-var">{hint}</span>}
    </div>
  );
}

interface Props {
  run: SearchRunView;
  onInspect: (row: ResultRowView) => void;
  onPromote: (row: ResultRowView) => void;
}

export function ResultsTable({ run, onInspect, onPromote }: Props) {
  const stale = run.freshness === 'STALE';
  // `allowed_actions` is the origin CEILING, not permission. A row within the ceiling can still
  // be refused by the gates behind it — staleness, integrity, whether the row was ever exposed —
  // and the server is the only thing that knows. So the button appears when the action is not
  // categorically impossible, and the refusal remains a sentence rather than a dead control.
  const withinCeiling = run.allowed_actions.includes('promote_as_validated_edge');
  return (
    <div data-run={run.run_id} data-freshness={run.freshness}
         className="rounded-lg border border-md-outline-var p-4">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div className="flex gap-6">
          <Count label="selectable" value={run.selectable_count} hint="k paid" />
          <Count label="ranked" value={run.ranked_count} />
          <Count label="exposed" value={run.displayed_count} hint="rows delivered" />
        </div>
        <div className="text-right text-[10px] text-md-on-surface-var">
          <div className="font-mono">{run.display_policy}</div>
          <div className="font-mono">{run.artifact_hash}</div>
          <div data-origin={run.evidence_origin} className="uppercase tracking-wider">
            {run.evidence_origin}
          </div>
          {/* four sentences, not one. Where the data came from, what the instrument was
              validated on, how tested this use of it is, and what this result was produced
              for — a single label could not have said all of it without lying about part. */}
          <div data-maturity={run.application_maturity} className="lowercase">
            instrument {run.instrument_validation_basis.toLowerCase()}
          </div>
          <div className="lowercase">
            application {run.application_maturity.toLowerCase()}
          </div>
          <div data-role={run.result_role} className="lowercase">
            role {run.result_role.toLowerCase()}
          </div>
          <div className="lowercase">ceiling: {run.allowed_actions.join(' · ')}</div>
        </div>
      </div>

      <RankingBanner p={run.ranking_provenance} />

      {stale && (
        <div data-stale="1"
             className="mt-3 rounded border border-md-error bg-md-error-container p-3">
          <div className="text-xs font-semibold uppercase tracking-widest text-md-error">
            stale result
          </div>
          <p className="mt-1 text-[11px] leading-relaxed text-md-on-surface">
            Produced under specification <span className="font-mono">{run.input_state_hash}</span>,
            and the session is now at <span className="font-mono">{run.current_state_hash}</span>.
            It stays readable as part of the research history; run the search again to act on the
            current specification.
          </p>
        </div>
      )}

      <div className="mt-3 overflow-x-auto">
        <table className="w-full text-left text-xs">
          <thead className="text-[10px] uppercase tracking-widest text-md-on-surface-var">
            <tr className="border-b border-md-outline-var">
              <th className="py-1 pr-3 font-normal">Rank</th>
              <th className="py-1 pr-3 font-normal">Claim</th>
              <th className="py-1 pr-3 font-normal">Effect</th>
              <th className="py-1 pr-3 font-normal">Uncertainty</th>
              <th className="py-1 pr-3 font-normal">Support</th>
              <th className="py-1 pr-3 font-normal">Verdict</th>
              <th className="py-1 font-normal" />
            </tr>
          </thead>
          <tbody>
            {run.rows.map((r) => (
              <tr key={r.claim_id} data-result-row={r.claim_id} data-rank={r.rank}
                  className="border-b border-md-outline-var last:border-b-0">
                <td className="py-1.5 pr-3 font-mono text-md-on-surface-var">{r.rank}</td>
                <td className="py-1.5 pr-3 text-md-on-surface">{r.label}</td>
                <td className="py-1.5 pr-3">
                  <button type="button" data-inspect={r.claim_id}
                          onClick={() => onInspect(r)}
                          className="font-mono text-md-on-surface underline decoration-dotted
                                     underline-offset-2">
                    {r.effect.display_value} {r.effect.display_units}
                  </button>
                </td>
                <td className="py-1.5 pr-3 font-mono text-md-on-surface-var">
                  {r.uncertainty.display_value}
                </td>
                <td className="py-1.5 pr-3 font-mono text-md-on-surface-var">
                  {r.support.display_value}
                </td>
                <td className={`py-1.5 pr-3 font-semibold ${VERDICT_CLS[r.verdict] ?? ''}`}>
                  {r.verdict}
                </td>
                <td className="py-1.5">
                  {withinCeiling ? (
                    <button type="button" data-promote={r.claim_id}
                            onClick={() => onPromote(r)}
                            className="rounded border border-md-outline-var px-2 py-0.5
                                       text-[10px] uppercase tracking-wider
                                       text-md-on-surface-var hover:text-md-on-surface">
                      promote
                    </button>
                  ) : (
                    <span data-no-promote={r.claim_id}
                          className="text-[10px] uppercase tracking-wider
                                     text-md-on-surface-var">
                      read only
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-2 text-[10px] text-md-on-surface-var">
        null family {run.null_family} · sampling target {run.sampling_target} ·
        integrity {run.integrity_status}
      </div>
    </div>
  );
}
