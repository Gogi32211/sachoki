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
 *
 * NOTHING IS DISABLED. The register button stays live on a session that can never register, and
 * the knobs stay live on a frozen study. A greyed-out control teaches nothing — the user decides
 * the app is broken, or discovers which click order avoids the grey. Pressing it produces the
 * backend's actual sentence, and where a legal path exists the refusal offers it. A prohibition
 * without a sanctioned alternative is the thing people route around.
 */
import { useEffect, useState } from 'react';
import {
  applyChange, createSession, forkSession, previewChange, registerSession, revisit,
  SessionRefusedError,
} from '../semantics/sessionApi';
import { SessionAccountingPanel } from '../components/SessionAccountingPanel';
import type { ChangePreview, Refusal, ResearchSessionView } from '../semantics/types';

const HORIZONS = ['20', '40', '60'];
const TOLERANCES = ['5', '2', '1'];

export function ComboLabScreen() {
  const [session, setSession] = useState<ResearchSessionView | null>(null);
  const [horizon, setHorizon] = useState('20');
  const [tolerance, setTolerance] = useState('5');
  const [preview, setPreview] = useState<ChangePreview | null>(null);
  const [claimHash, setClaimHash] = useState('');
  const [refusal, setRefusal] = useState<Refusal | null>(null);
  const [forkReason, setForkReason] = useState('');
  const [lineage, setLineage] = useState<string>('');
  const [error, setError] = useState('');

  useEffect(() => {
    createSession().then(setSession).catch((e: unknown) => setError(String(e)));
  }, []);

  /** A refusal is the system working; a transport error is the system failing. Not the same UI. */
  const failed = (e: unknown) => {
    if (e instanceof SessionRefusedError) setRefusal(e.refusal);
    else setError(String(e));
  };

  const change = (parameterId: string, next: string) => {
    if (!session) return;
    setRefusal(null);
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
      .catch(failed);
  };

  const reopen = () => {
    if (!session) return;
    setRefusal(null);
    revisit(session.session_id, horizon, tolerance).then(setSession).catch(failed);
  };

  const register = () => {
    if (!session) return;
    setRefusal(null);
    registerSession(session.session_id).then(setSession).catch(failed);
  };

  const startFresh = () => {
    setRefusal(null);
    setPreview(null);
    setClaimHash('');
    setLineage('');
    setHorizon('20');
    setTolerance('5');
    createSession().then(setSession).catch(failed);
  };

  const fork = () => {
    if (!session) return;
    forkSession(session.session_id, forkReason, horizon, tolerance)
      .then((r) => {
        setSession(r.session);
        setHorizon(r.inherited.horizon);
        setTolerance(r.inherited.tolerance);
        setLineage(`${r.parent.session_id} → ${r.session.session_id} · ${r.reason}`);
        setRefusal(null);
        setPreview(null);
        setClaimHash('');
        setForkReason('');
      })
      .catch(failed);
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

  const frozen = session.mode === 'REGISTERED' || session.mode === 'ACTIVE_REGISTERED';

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

            <div className="mt-4 flex flex-wrap items-center gap-2">
              <button type="button" onClick={reopen}
                      className="rounded border border-md-outline-var px-3 py-1 text-xs
                                 text-md-on-surface-var hover:text-md-on-surface">
                reopen this result
              </button>
              <button type="button" onClick={register}
                      className="rounded border border-md-warning px-3 py-1 text-xs
                                 text-md-warning hover:bg-md-warning-con">
                preregister this study
              </button>
              <button type="button" onClick={startFresh}
                      className="rounded border border-md-outline-var px-3 py-1 text-xs
                                 text-md-on-surface-var hover:text-md-on-surface">
                new session
              </button>
            </div>
            {frozen && (
              <p className="mt-3 text-[11px] leading-relaxed text-md-on-surface-var">
                This study is frozen. The knobs above are still live on purpose — pressing one
                shows you what the system refuses and why, rather than leaving you to guess at a
                dead control.
              </p>
            )}
          </div>

          {refusal && (
            <div className="rounded-lg border border-md-error bg-md-error-container p-4">
              <div className="text-xs font-semibold uppercase tracking-widest text-md-error">
                refused · {refusal.error}
              </div>
              <p className="mt-2 text-xs leading-relaxed text-md-on-surface">{refusal.detail}</p>
              {refusal.remedy && (
                <p className="mt-2 text-xs leading-relaxed text-md-on-surface-var">
                  {refusal.remedy}
                </p>
              )}
              {refusal.next_action === 'FORK' && (
                <div className="mt-3 flex flex-wrap items-center gap-2">
                  <input value={forkReason} onChange={(e) => setForkReason(e.target.value)}
                         placeholder="why this fork is being taken"
                         className="min-w-[16rem] flex-1 rounded border border-md-outline-var
                                    bg-md-surface-con px-2 py-1 text-xs text-md-on-surface
                                    placeholder:text-md-on-surface-var" />
                  <button type="button" onClick={fork}
                          className="rounded border border-md-warning px-3 py-1 text-xs
                                     text-md-warning hover:bg-md-warning-con">
                    fork into a new exploratory session
                  </button>
                </div>
              )}
              {refusal.next_action === 'NEW_SESSION' && (
                <button type="button" onClick={startFresh}
                        className="mt-3 rounded border border-md-outline-var px-3 py-1 text-xs
                                   text-md-on-surface-var hover:text-md-on-surface">
                  open a session with no parent
                </button>
              )}
              {refusal.next_action === 'NONE' && (
                <p className="mt-3 text-[11px] text-md-on-surface-var">
                  There is no legal continuation of this action. It is not a limitation of the
                  screen — the move itself has no honest form.
                </p>
              )}
            </div>
          )}

          {lineage && (
            <div className="rounded-lg border border-md-outline-var p-4 text-xs
                            text-md-on-surface-var">
              forked
              <div className="mt-1 font-mono text-[11px] text-md-on-surface">{lineage}</div>
            </div>
          )}

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
