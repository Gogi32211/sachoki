/** HTTP for the Studio island. Everything crossing this boundary is decoded, never cast. */
import { decodeArtifact, decodeN0, decodePassport, TransportContractError } from './decode';
import type { ComparisonArtifact, InspectorPassport, N0Screen } from './types';

const BASE = '/api/studio/semantics';

async function getJson(path: string): Promise<unknown> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const body = await res.text();
    throw new TransportContractError(`${res.status} ${path}: ${body.slice(0, 300)}`);
  }
  return res.json();
}

export async function fetchN0(): Promise<N0Screen> {
  return decodeN0(await getJson('/n0'));
}

export async function fetchPassport(metricId: string): Promise<InspectorPassport> {
  return decodePassport(await getJson(`/metric/${encodeURIComponent(metricId)}`));
}

/**
 * The sanctioned route to a delta, and the only one. A 409 here is not a network problem: it is
 * the server saying the comparison does not exist as a statistical object.
 */
export async function fetchComparisonArtifact(
  leftId: string, rightId: string,
): Promise<{ artifact: ComparisonArtifact | null; blocked: string | null }> {
  const res = await fetch(
    `${BASE}/compare/${encodeURIComponent(leftId)}/${encodeURIComponent(rightId)}`,
  );
  if (res.status === 409) {
    const body = await res.json();
    return { artifact: null, blocked: JSON.stringify(body?.detail ?? body) };
  }
  if (!res.ok) throw new TransportContractError(`${res.status} compare`);
  return { artifact: decodeArtifact(await res.json()), blocked: null };
}
