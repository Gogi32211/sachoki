/** Session HTTP. Every action asks the backend to classify it before it runs. */
import {
  decodeCatalogue, decodePlan, decodePreview, decodeRefusal, decodeRun, decodeSession,
  TransportContractError,
} from './decode';
import type {
  ChangePlanView, ChangePreview, ForkResult, ParameterCatalogue, Refusal, ResearchSessionView,
  SearchRunView,
} from './types';

/**
 * A governed refusal, distinct from a transport failure.
 *
 * 409 here is not an error in the usual sense — it is the system working. The screen must be
 * able to tell "the server refused this, and here is the sentence and the legal alternative"
 * apart from "the request did not arrive", because only one of those is the user's business.
 */
export class SessionRefusedError extends Error {
  readonly refusal: Refusal;
  constructor(refusal: Refusal) {
    super(refusal.detail);
    this.name = 'SessionRefusedError';
    this.refusal = refusal;
  }
}

const BASE = '/api/studio/session';

async function post(path: string, body?: unknown): Promise<unknown> {
  // `exactOptionalPropertyTypes` refuses an explicit `undefined` body, and it is right to: an
  // absent field and a field set to undefined are different requests. Build the init object
  // without the key rather than with an empty one.
  const init: RequestInit = { method: 'POST',
                              headers: { 'Content-Type': 'application/json' } };
  if (body !== undefined) init.body = JSON.stringify(body);
  const res = await fetch(`${BASE}${path}`, init);
  if (res.status === 409) throw new SessionRefusedError(decodeRefusal(await res.json()));
  if (!res.ok) throw new TransportContractError(`${res.status} ${path}: ${await res.text()}`);
  return res.json();
}

export async function createSession(): Promise<ResearchSessionView> {
  const d = await post('/create') as Record<string, unknown>;
  return decodeSession(d.session);
}

export async function previewChange(
  sid: string, parameter_id: string, horizon: string, tolerance: string, new_value: string,
): Promise<ChangePreview> {
  return decodePreview(
    await post(`/${sid}/preview`, { parameter_id, horizon, tolerance, new_value }),
  );
}

export interface ChangeResult {
  readonly session: ResearchSessionView;
  readonly horizon: string;
  readonly tolerance: string;
  readonly claim_hash: string;
  readonly change_type: string;
}

export async function applyChange(
  sid: string, parameter_id: string, horizon: string, tolerance: string, new_value: string,
): Promise<ChangeResult> {
  const raw = await post(`/${sid}/change`, { parameter_id, horizon, tolerance, new_value });
  const d = raw as Record<string, unknown>;
  return {
    session: decodeSession(d.session),
    horizon: String(d.horizon), tolerance: String(d.tolerance),
    claim_hash: String(d.claim_hash), change_type: String(d.change_type),
  };
}

export async function revisit(
  sid: string, horizon: string, tolerance: string,
): Promise<ResearchSessionView> {
  const raw = await post(`/${sid}/revisit`, { horizon, tolerance });
  const d = raw as Record<string, unknown>;
  return decodeSession(d.session);
}

/** Irreversible. Refused with a readable cause once anything in the lineage has been seen. */
export async function registerSession(sid: string): Promise<ResearchSessionView> {
  const d = await post(`/${sid}/register`) as Record<string, unknown>;
  return decodeSession(d.session);
}

/**
 * The legal way out of a frozen study. The child starts where the parent stopped and inherits
 * the two knob positions — never the parent's registration, and never a clean counter.
 */
export async function forkSession(
  sid: string, reason: string, horizon: string, tolerance: string,
): Promise<ForkResult> {
  const raw = await post(`/${sid}/fork`, { reason, horizon, tolerance });
  const d = raw as Record<string, unknown>;
  const inh = (d.inherited ?? {}) as Record<string, unknown>;
  return {
    session: decodeSession(d.session),
    parent: decodeSession(d.parent),
    inherited: { horizon: String(inh.horizon), tolerance: String(inh.tolerance) },
    reason: String(d.reason ?? ''),
  };
}

export async function fetchCatalogue(sid: string): Promise<ParameterCatalogue> {
  const res = await fetch(`${BASE}/${sid}/parameters`);
  if (!res.ok) throw new TransportContractError(`${res.status} parameters: ${await res.text()}`);
  return decodeCatalogue(await res.json());
}

/** Ask what a knob costs. The answer is a plan, and the screen does not recompute any of it. */
export async function previewParameter(
  sid: string, parameterId: string, newValue: string,
): Promise<ChangePlanView> {
  return decodePlan(await post(`/${sid}/parameter/preview`,
                               { parameter_id: parameterId, new_value: newValue }));
}

export interface ParameterResult {
  readonly session: ResearchSessionView;
  readonly recorded: string;
  readonly surface: Record<string, string>;
  readonly role: string;
  readonly multiplicity_effect: string;
}

/** Commit the plan that was approved — by its hash, not by re-sending what it said. */
export async function commitParameter(
  sid: string, plan: ChangePlanView,
): Promise<ParameterResult> {
  const raw = await post(`/${sid}/parameter`, {
    parameter_id: plan.parameter_id, new_value: plan.new_value, plan_hash: plan.plan_hash,
  });
  const d = raw as Record<string, unknown>;
  const c = (d.classification ?? {}) as Record<string, unknown>;
  return {
    session: decodeSession(d.session),
    recorded: String(d.recorded),
    surface: Object.fromEntries(
      Object.entries((d.surface ?? {}) as Record<string, unknown>)
        .map(([k, v]) => [k, String(v)])),
    role: String(c.role ?? ''),
    multiplicity_effect: String(c.multiplicity_effect ?? ''),
  };
}

export interface RunResult {
  readonly run: SearchRunView;
  readonly session: ResearchSessionView;
}

/** Ask the server to search. It ranks, it authorises, and it charges the exposure. */
export async function runSearch(sid: string): Promise<RunResult> {
  const raw = await post(`/${sid}/search`, {}) as Record<string, unknown>;
  return { run: decodeRun(raw.run), session: decodeSession(raw.session) };
}

/** Re-read a run. Freshness is recomputed by the server against the CURRENT specification. */
export async function fetchRun(sid: string, runId: string): Promise<RunResult> {
  const res = await fetch(`${BASE}/${sid}/run/${runId}`);
  if (!res.ok) throw new TransportContractError(`${res.status} run: ${await res.text()}`);
  const raw = await res.json() as Record<string, unknown>;
  return { run: decodeRun(raw.run), session: decodeSession(raw.session) };
}

export async function promoteRow(
  sid: string, runId: string, claimId: string,
): Promise<ResearchSessionView> {
  const raw = await post(`/${sid}/promote`, { run_id: runId, claim_id: claimId });
  return decodeSession((raw as Record<string, unknown>).session);
}
