/**
 * Combo Lab, first vertical slice. Two knobs, and the whole point is what they cost.
 *
 * Before anything runs, the UI asks the backend to classify the change; only then does it apply
 * it. That order matters: a classification made before the result exists cannot be relabelled
 * once the number turns out to be attractive.
 *
 * There is no table of 31 claims here and no chart. The acceptance question is not "does it show
 * the claims nicely" but "can a user move a meaningful degree of freedom, see a result, and
 * watch the accounting move exactly as the backend said it would".
 */
import { useEffect, useState } from 'react';
import { applyChange, createSession, previewChange, revisit } from '../semantics/sessionApi';
import { SessionAccountingPanel } from '../components/SessionAccountingPanel';
import type { ChangePreview, ResearchSessionView } from '../semantics/types';

const HORIZONS = ['20', '40', '60'];
const TOLERANCES = ['5', '2', '1'];

export function ComboLabScreen() {
  const [session, setSession] = useState<ResearchSessionView | null>(null);
  const [horizon, setHorizon] = useState('20');
  const [tolerance, setTolerance] = useState('5');
  const [preview, setPreview] = useState<ChangePreview | null>(null);
  const [claimHash, setClaimHash] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    createSession().then(setSession).catch((e: unknown) => setError(String(e)));
  }, []);

  const change = (parameterId: string, next: string) => {
    if (!session) return;
    previewChange(session.session_id, parameterId, horizon, tolerance, next)
      .then((p) => {
        setPreview(p);
        return applyChange(session.session_id, parameterId, horizon, tolerance, next);
      })
      .then((r) => {
        setSession(r.session);
        setHorizon(r.horizon);
        setTolerance(r.tolerance);
        setClaimHash(r.claim_hash);
      })
      .catch((e: unknown) => setError(String(e)));
  };

  const reopen = () => {
    if (!session) return;
    revisit(session.session_id, horizon, tolerance)
      .then(setSession)
      .catch((e: unknown) => setError(String(e)));
  };

  if (error) {
    return (
      <div className="m-6 rounded border border-md-error bg-md-error-container p-4 text-sm
                      text-md-error">
        transport contract error — {error}
      </div>
    );
  }
  if (!session) return <div className="p-6 text-sm text-md-on-surface-var">loading…</div>;

  return (
    <div className="mx-auto max-w-5xl p-6">
      <h1 className="text-lg font-semibold tracking-wide">COMBO LAB · vertical slice</h1>
      <p className="mt-1 text-xs text-md-on-surface-var">
        Two degrees of freedom. The backend classifies each change before it runs.
      </p>

      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-[1fr_20rem]">
        <div className="space-y-6">
          <div className="rounded-lg border border-md-outline-var p-4">
            <div className="text-xs uppercase tracking-widest text-md-on-surface-var">Horizon</div>
            <div className="mt-2 flex gap-2">
              {HORIZONS.map((h) => (
                <button key={h} type="button" onClick={() => change('horizon', h)}
                        className={`rounded px-3 py-1 text-sm ${h === horizon
                          ? 'bg-md-surface-high text-md-on-surface'
                          : 'text-md-on-surface-var hover:text-md-on-surface'}`}>
                  {h} bars
                </button>
              ))}
            </div>

            <div className="mt-4 text-xs uppercase tracking-widest text-md-on-surface-var">
              RSI tolerance
            </div>
            <div className="mt-2 flex gap-2">
              {TOLERANCES.map((t) => (
                <button key={t} type="button"
                        onClick={() => change('conditioning_tolerance', t)}
                        className={`rounded px-3 py-1 text-sm ${t === tolerance
                          ? 'bg-md-surface-high text-md-on-surface'
                          : 'text-md-on-surface-var hover:text-md-on-surface'}`}>
                  ±{t}
                </button>
              ))}
            </div>

            <button type="button" onClick={reopen}
                    className="mt-4 rounded border border-md-outline-var px-3 py-1 text-xs
                               text-md-on-surface-var hover:text-md-on-surface">
              reopen this result
            </button>
          </div>

          {preview && (
            <div className="rounded-lg border border-md-warning bg-md-warning-con p-4">
              <div className="text-xs font-semibold uppercase tracking-widest text-md-warning">
                last change · {preview.change_type}
              </div>
              <div className="mt-2 space-y-1 text-xs text-md-on-surface">
                <div>effect: {preview.multiplicity_effect}</div>
                <div className="font-mono text-[11px] text-md-on-surface-var">
                  {preview.old_claim_hash} → {preview.new_claim_hash}
                </div>
              </div>
            </div>
          )}

          <div className="rounded-lg border border-md-outline-var p-4 text-xs
                          text-md-on-surface-var">
            current claim
            <div className="mt-1 font-mono text-sm text-md-on-surface">
              {claimHash || '— no query run yet —'}
            </div>
            <div className="mt-2">horizon {horizon} bars · RSI ±{tolerance}</div>
          </div>
        </div>

        <SessionAccountingPanel session={session} />
      </div>
    </div>
  );
}
