/** Session HTTP. Every action asks the backend to classify it before it runs. */
import { decodePreview, decodeRefusal, decodeSession, TransportContractError } from './decode';
import type { ChangePreview, ForkResult, Refusal, ResearchSessionView } from './types';

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
