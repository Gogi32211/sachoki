/**
 * The waiting screen, and what it deliberately cannot show.
 *
 *     WAITING FOR NOVEL EVIDENCE
 *     0 / 30 novel trading days until the first registered look
 *
 * Everything statistical above this panel is frozen — estimand, support, bootstrap, decision
 * rule, ranking, evidence boundary, adapter — and all of it is undone by a person watching θ
 * drift and deciding to stop when it looks good. So there is no field here for an estimate, an
 * interval, a verdict or a ranking, the decoder REFUSES a payload that carries one, and the
 * server refuses to build one. Three layers, because this is the failure that arrives as a
 * helpful feature request.
 *
 * The progress bar counts days, which is not peeking. Seeing a θ move is.
 */
import { useEffect, useState } from 'react';
import { decodeForwardStatus } from '../semantics/decode';
import type { ForwardStatusView } from '../semantics/types';

const HEADLINE: Record<string, string> = {
  WAITING_FOR_NOVEL_EVIDENCE: 'waiting for novel evidence',
  READY_FOR_REGISTERED_LOOK: 'ready for the registered look',
  LOOK_TAKEN: 'the registered look has been taken',
  UNKNOWN: 'forward state unknown',
};

export function ForwardStatusPanel() {
  const [status, setStatus] = useState<ForwardStatusView | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    fetch('/api/studio/forward/status')
      .then((r) => r.json())
      .then((raw) => setStatus(decodeForwardStatus(raw)))
      .catch((e: unknown) => setError(String(e)));
  }, []);

  if (error) {
    return (
      <div data-forward-error="1"
           className="rounded-lg border border-md-error p-3 text-[11px] text-md-error">
        forward status refused: {error}
      </div>
    );
  }
  if (!status) return null;

  const done = status.novel_trading_days;
  const need = status.novel_trading_days_required;
  const pct = need > 0 ? Math.min(100, Math.round((done / need) * 100)) : 0;

  return (
    <div data-forward-state={status.state} data-forward-days={String(done)}
         className="rounded-lg border border-md-outline-var p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <span className="text-xs font-semibold uppercase tracking-widest text-md-on-surface">
          {HEADLINE[status.state] ?? HEADLINE.UNKNOWN}
        </span>
        <span className="font-mono text-[10px] text-md-on-surface-var">
          boundary {status.evidence_boundary} · policy {status.policy_hash}
        </span>
      </div>

      <div className="mt-3 flex items-baseline gap-2">
        <span className="font-mono text-lg text-md-on-surface">{done} / {need}</span>
        <span className="text-[10px] uppercase tracking-widest text-md-on-surface-var">
          novel trading days until the first registered look
        </span>
      </div>
      <div className="mt-2 h-1 w-full rounded bg-md-outline-var">
        <div data-forward-progress={String(pct)} style={{ width: `${pct}%` }}
             className="h-1 rounded bg-md-on-surface-var" />
      </div>

      <dl className="mt-3 grid grid-cols-2 gap-x-6 gap-y-1 text-[10px] text-md-on-surface-var
                     sm:grid-cols-4">
        <div><dt className="uppercase tracking-wider">remaining</dt>
          <dd className="font-mono">{status.novel_trading_days_remaining}</dd></div>
        <div><dt className="uppercase tracking-wider">latest novel day</dt>
          <dd className="font-mono">{status.latest_novel_day || '—'}</dd></div>
        <div><dt className="uppercase tracking-wider">looks taken</dt>
          <dd className="font-mono">{status.looks_taken}</dd></div>
        <div><dt className="uppercase tracking-wider">repeated looks</dt>
          <dd className="font-mono">{status.repeated_looks}</dd></div>
      </dl>

      <p className="mt-2 text-[10px] leading-relaxed text-md-on-surface-var">{status.note}</p>
    </div>
  );
}
