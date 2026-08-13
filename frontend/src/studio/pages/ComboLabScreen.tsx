/**
 * Combo Lab. Twenty-two knobs, generated from the backend registry — not twenty-two components.
 *
 * The backend is role-driven. The obvious way to lose that is here: write a control per
 * parameter, let each grow its own path, and the screen quietly acquires twenty-two ways around
 * one ledger. So this file renders `catalogue.parameters` through a single `ParameterControl`,
 * and the only thing it branches on is `ui_kind`. It never reads `parameter_id` to decide
 * behaviour and never derives a semantic role.
 *
 * EVERY CHANGE GOES preview → plan → commit(plan_hash). Sharing the classifier removes one
 * disagreement; the plan removes the other. A person reads what a change costs, thinks, and
 * clicks — and in between the session may have moved. Committing by hash means the transition
 * applied is the transition that was approved, or nothing happens at all.
 *
 * NOTHING IS DISABLED except what the backend marks immutable, and even then the refusal is a
 * sentence rather than a dead control. A greyed knob teaches nothing; a refused one teaches the
 * rule.
 */
import { useCallback, useEffect, useState } from 'react';
import {
  commitParameter, createSession, fetchCatalogue, fetchRun, forkSession, previewParameter,
  promoteRow, registerSession, revisit, runSearch, SessionRefusedError,
} from '../semantics/sessionApi';
import { SessionAccountingPanel } from '../components/SessionAccountingPanel';
import { ParameterControl, RoleBadge } from '../components/ParameterControl';
import { ResultsTable } from '../components/ResultsTable';
import type {
  ChangePlanView, ParameterCatalogue, Refusal, ResearchSessionView, ResultRowView, SearchRunView,
} from '../semantics/types';

export function ComboLabScreen() {
  const [session, setSession] = useState<ResearchSessionView | null>(null);
  const [catalogue, setCatalogue] = useState<ParameterCatalogue | null>(null);
  const [plan, setPlan] = useState<ChangePlanView | null>(null);
  const [lastApplied, setLastApplied] = useState<ChangePlanView | null>(null);
  const [refusal, setRefusal] = useState<Refusal | null>(null);
  const [forkReason, setForkReason] = useState('');
  const [lineage, setLineage] = useState('');
  const [run, setRun] = useState<SearchRunView | null>(null);
  const [inspected, setInspected] = useState<ResultRowView | null>(null);
  const [error, setError] = useState('');

  const failed = useCallback((e: unknown) => {
    if (e instanceof SessionRefusedError) setRefusal(e.refusal);
    else setError(String(e));
  }, []);

  const load = useCallback((sid: string) => {
    fetchCatalogue(sid).then(setCatalogue).catch(failed);
  }, [failed]);

  useEffect(() => {
    createSession()
      .then((s) => { setSession(s); load(s.session_id); })
      .catch((e: unknown) => setError(String(e)));
  }, [load]);

  /** Step one of every change. The screen asks; it does not work the answer out. */
  const ask = (parameterId: string, value: string) => {
    if (!session) return;
    setRefusal(null);
    previewParameter(session.session_id, parameterId, value)
      .then(setPlan)
      .catch(failed);
  };

  /** Step two. By hash, so a session that moved in between refuses instead of improvising. */
  const confirm = () => {
    if (!session || !plan) return;
    const sid = session.session_id;
    commitParameter(sid, plan)
      .then((r) => {
        setSession(r.session);
        setLastApplied(plan);
        setPlan(null);
        load(sid);
        // the previous table was computed from a specification that just moved; ask the server
        // whether it is still current rather than deciding here
        if (run) fetchRun(sid, run.run_id).then((x) => setRun(x.run)).catch(failed);
      })
      .catch(failed);
  };

  const search = () => {
    if (!session) return;
    setRefusal(null);
    runSearch(session.session_id)
      .then((r) => { setRun(r.run); setSession(r.session); })
      .catch(failed);
  };

  const promote = (row: ResultRowView) => {
    if (!session || !run) return;
    setRefusal(null);
    promoteRow(session.session_id, run.run_id, row.claim_id).then(setSession).catch(failed);
  };

  const reopen = () => {
    if (!session || !catalogue) return;
    setRefusal(null);
    const v = (id: string) =>
      catalogue.parameters.find((p) => p.parameter_id === id)?.current_value ?? '';
    revisit(session.session_id, v('horizon'), v('conditioning_tolerance'))
      .then(setSession).catch(failed);
  };

  const register = () => {
    if (!session) return;
    setRefusal(null);
    registerSession(session.session_id)
      .then((s) => { setSession(s); load(s.session_id); })
      .catch(failed);
  };

  const startFresh = () => {
    setRefusal(null); setPlan(null); setLastApplied(null); setLineage('');
    createSession().then((s) => { setSession(s); load(s.session_id); }).catch(failed);
  };

  const fork = () => {
    if (!session || !catalogue) return;
    const v = (id: string) =>
      catalogue.parameters.find((p) => p.parameter_id === id)?.current_value ?? '';
    forkSession(session.session_id, forkReason, v('horizon'), v('conditioning_tolerance'))
      .then((r) => {
        setSession(r.session);
        setLineage(`${r.parent.session_id} → ${r.session.session_id} · ${r.reason}`);
        setRefusal(null); setPlan(null); setForkReason('');
        load(r.session.session_id);
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
  if (!session || !catalogue) {
    return <div className="p-6 text-sm text-md-on-surface-var">loading…</div>;
  }

  const frozen = session.mode === 'REGISTERED' || session.mode === 'ACTIVE_REGISTERED';

  return (
    <div className="mx-auto max-w-6xl p-6">
      <div className="flex items-baseline justify-between gap-4">
        <h1 className="text-lg font-semibold tracking-wide">COMBO LAB · control surface</h1>
        <span className="font-mono text-[10px] text-md-on-surface-var">
          registry {catalogue.parameter_registry_hash} · {catalogue.parameters.length} parameters
        </span>
      </div>
      <p className="mt-1 text-xs text-md-on-surface-var">
        Every control is generated from the backend registry and classified before it runs.
        The badge is what the change costs — the screen shows it and never works it out.
      </p>

      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-[1fr_20rem]">
        <div className="space-y-4">
          {catalogue.groups.map((group) => {
            const rows = catalogue.parameters.filter((p) => p.group === group);
            if (!rows.length) return null;
            return (
              <div key={group} data-group={group}
                   className="rounded-lg border border-md-outline-var p-4">
                <div className="mb-2 text-xs font-semibold uppercase tracking-widest
                                text-md-on-surface-var">
                  {group}
                </div>
                {rows.map((p) => (
                  <ParameterControl key={p.parameter_id} param={p} frozen={frozen}
                                    onChange={ask} />
                ))}
              </div>
            );
          })}

          <div className="flex flex-wrap items-center gap-2">
            <button type="button" onClick={search} data-run-search="1"
                    className="rounded border border-md-warning px-3 py-1 text-xs text-md-warning
                               hover:bg-md-warning-con">
              run search
            </button>
            <button type="button" onClick={reopen}
                    className="rounded border border-md-outline-var px-3 py-1 text-xs
                               text-md-on-surface-var hover:text-md-on-surface">
              reopen this result
            </button>
            <button type="button" onClick={register}
                    className="rounded border border-md-warning px-3 py-1 text-xs text-md-warning
                               hover:bg-md-warning-con">
              preregister this study
            </button>
            <button type="button" onClick={startFresh}
                    className="rounded border border-md-outline-var px-3 py-1 text-xs
                               text-md-on-surface-var hover:text-md-on-surface">
              new session
            </button>
          </div>
        </div>

        <div className="space-y-4">
          {inspected && (
            <div data-inspector={inspected.claim_id}
                 className="rounded-lg border border-md-outline-var bg-md-surface-con p-4">
              <div className="flex items-baseline justify-between">
                <span className="text-xs font-semibold uppercase tracking-widest
                                 text-md-on-surface-var">
                  passport
                </span>
                <button type="button" onClick={() => setInspected(null)}
                        className="text-[10px] text-md-on-surface-var">close</button>
              </div>
              <div className="mt-2 space-y-1 text-[11px] text-md-on-surface-var">
                <div className="text-md-on-surface">{inspected.label}</div>
                <div>{inspected.effect.label}: <span className="font-mono text-md-on-surface">
                  {inspected.effect.display_value} {inspected.effect.display_units}</span></div>
                <div>{inspected.uncertainty.label}: <span className="font-mono">
                  {inspected.uncertainty.display_value}</span></div>
                <div>{inspected.support.label}: <span className="font-mono">
                  {inspected.support.display_value}</span></div>
                <div className="pt-1">sampling target <span className="font-mono">
                  {run?.sampling_target}</span></div>
                <div>null family <span className="font-mono">{run?.null_family}</span></div>
                <div>evidence claim <span className="font-mono">
                  {inspected.evidence_claim_hash}</span></div>
                <div>decision spec <span className="font-mono">
                  {inspected.decision_spec_hash}</span></div>
                <div>provenance <span className="font-mono">{run?.data_provenance}</span></div>
              </div>
            </div>
          )}

          {plan && (
            <div data-plan={plan.plan_hash}
                 className="rounded-lg border border-md-warning bg-md-warning-con p-4">
              <div className="flex items-baseline justify-between gap-2">
                <span className="text-xs font-semibold uppercase tracking-widest
                                 text-md-warning">
                  this change would
                </span>
                <RoleBadge role={plan.semantic_role} />
              </div>
              <div className="mt-2 space-y-1 text-xs text-md-on-surface">
                <div>{plan.parameter_id}: {plan.old_value || '—'} → {plan.new_value}</div>
                <div data-plan-effect={plan.multiplicity_effect}>
                  effect: {plan.multiplicity_effect}
                </div>
                {plan.old_claim_hash !== plan.new_claim_hash && (
                  <div className="font-mono text-[10px] text-md-on-surface-var">
                    claim {plan.old_claim_hash} → {plan.new_claim_hash}
                  </div>
                )}
                {plan.old_search_space_hash !== plan.new_search_space_hash && (
                  <div className="font-mono text-[10px] text-md-on-surface-var">
                    space {plan.old_search_space_hash} → {plan.new_search_space_hash}
                  </div>
                )}
                {plan.old_decision_policy_hash !== plan.new_decision_policy_hash && (
                  <div className="font-mono text-[10px] text-md-on-surface-var">
                    policy {plan.old_decision_policy_hash} → {plan.new_decision_policy_hash}
                  </div>
                )}
              </div>
              <div className="mt-3 flex gap-2">
                <button type="button" onClick={confirm} data-commit-plan={plan.plan_hash}
                        className="rounded border border-md-warning px-3 py-1 text-xs
                                   text-md-warning hover:bg-md-surface-high">
                  apply
                </button>
                <button type="button" onClick={() => setPlan(null)}
                        className="rounded border border-md-outline-var px-3 py-1 text-xs
                                   text-md-on-surface-var">
                  cancel
                </button>
              </div>
            </div>
          )}

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
                <div className="mt-3 space-y-2">
                  <input value={forkReason} onChange={(e) => setForkReason(e.target.value)}
                         placeholder="why this fork is being taken"
                         className="w-full rounded border border-md-outline-var bg-md-surface-con
                                    px-2 py-1 text-xs text-md-on-surface
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
              {refusal.next_action === 'REPREVIEW' && (
                <p className="mt-3 text-[11px] text-md-on-surface-var">
                  Nothing was applied. Turn the control again to get a current preview.
                </p>
              )}
            </div>
          )}

          {lastApplied && !plan && (
            <div className="rounded-lg border border-md-outline-var p-4 text-xs
                            text-md-on-surface-var">
              last applied
              <div className="mt-1 flex items-center gap-2 text-md-on-surface">
                <span>{lastApplied.parameter_id} → {lastApplied.new_value}</span>
                <RoleBadge role={lastApplied.semantic_role} />
              </div>
              <div className="mt-1 font-mono text-[10px]">plan {lastApplied.plan_hash}</div>
            </div>
          )}

          {lineage && (
            <div className="rounded-lg border border-md-outline-var p-4 text-xs
                            text-md-on-surface-var">
              forked
              <div className="mt-1 font-mono text-[11px] text-md-on-surface">{lineage}</div>
            </div>
          )}

          <SessionAccountingPanel session={session} />
        </div>
      </div>

      {run && (
        <div className="mt-6">
          <ResultsTable run={run} onInspect={setInspected} onPromote={promote} />
        </div>
      )}
    </div>
  );
}
