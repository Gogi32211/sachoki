/**
 * The runtime wall. TypeScript alone is not one.
 *
 *     const data = await res.json() as SemanticMetricView
 *
 * is a lie told to the compiler. If the server ever emits `{"value": 0.065}` the cast says
 * nothing at run time and the operand is back. So every payload is decoded, and a numeric
 * `value` is not ignored — it is REJECTED, loudly, because its presence means the transport
 * contract broke upstream and the screen must not paper over it.
 *
 * Two different guarantees, and both are needed:
 *
 *     runtime    the API physically did not send a raw statistical value
 *     compile    React physically cannot reach for one
 */

import type {
  ComparisonArtifact, ComparisonVerdict, InspectorPassport, InspectorSection,
  N0Screen, SemanticMetricView, SemanticType,
} from './types';

export class TransportContractError extends Error {}

const SEMANTIC_TYPES: readonly string[] = [
  'DETERMINISTIC', 'DESCRIPTIVE', 'INFERENTIAL', 'DECISION',
];

function str(o: Record<string, unknown>, k: string, where: string): string {
  const v = o[k];
  if (typeof v !== 'string') {
    throw new TransportContractError(
      `${where}.${k} is ${typeof v}, expected string. Every field of a metric view is text; a ` +
      `numeric one would hand the presentation layer an operand.`,
    );
  }
  return v;
}

/** Recursively: is there any number anywhere? Booleans are not operands. */
export function numericLeaves(o: unknown, path = ''): string[] {
  if (Array.isArray(o)) return o.flatMap((v, i) => numericLeaves(v, `${path}[${i}]`));
  if (o && typeof o === 'object') {
    return Object.entries(o as Record<string, unknown>)
      .flatMap(([k, v]) => numericLeaves(v, `${path}.${k}`));
  }
  if (typeof o === 'number') return [`${path}=${o}`];
  return [];
}

export function decodeMetricView(raw: unknown): SemanticMetricView {
  if (!raw || typeof raw !== 'object') {
    throw new TransportContractError('metric view is not an object');
  }
  const o = raw as Record<string, unknown>;
  if ('value' in o) {
    throw new TransportContractError(
      `metric view carries a raw 'value' field. The transport contract is that a statistical ` +
      `value never reaches the browser as a number; a payload that breaks it is a defect ` +
      `upstream, not something a decoder should quietly drop.`,
    );
  }
  const leaks = numericLeaves(o);
  if (leaks.length) {
    throw new TransportContractError(`numeric leaves reached the browser: ${leaks.join(', ')}`);
  }
  const t = str(o, 'semantic_type', 'metric');
  if (!SEMANTIC_TYPES.includes(t)) {
    throw new TransportContractError(`unknown semantic_type ${t}`);
  }
  const integrity = str(o, 'integrity_status', 'metric');
  if (integrity !== 'VALID' && integrity !== 'INVALID') {
    throw new TransportContractError(`unknown integrity_status ${integrity}`);
  }
  return {
    metric_id: str(o, 'metric_id', 'metric'),
    display_value: str(o, 'display_value', 'metric'),
    display_units: str(o, 'display_units', 'metric'),
    label: str(o, 'label', 'metric'),
    semantic_type: t as SemanticType,
    integrity_status: integrity,
    conclusion_status: str(o, 'conclusion_status', 'metric'),
    population_summary: str(o, 'population_summary', 'metric'),
    inspector_ref: str(o, 'inspector_ref', 'metric'),
  };
}

export function decodeComparison(raw: unknown): ComparisonVerdict {
  const o = (raw ?? {}) as Record<string, unknown>;
  if (typeof o.comparable !== 'boolean') {
    throw new TransportContractError('comparison.comparable must be a boolean');
  }
  return {
    comparable: o.comparable,
    reason_code: str(o, 'reason_code', 'comparison'),
    message: str(o, 'message', 'comparison'),
    left: str(o, 'left', 'comparison'),
    right: str(o, 'right', 'comparison'),
  };
}

export function decodeN0(raw: unknown): N0Screen {
  const o = (raw ?? {}) as Record<string, unknown>;
  const metrics = o.metrics;
  if (!Array.isArray(metrics)) throw new TransportContractError('metrics must be an array');
  return {
    screen: str(o, 'screen', 'n0'),
    metrics: metrics.map(decodeMetricView),
    comparison: decodeComparison(o.comparison),
  };
}

export function decodePassport(raw: unknown): InspectorPassport {
  const o = (raw ?? {}) as Record<string, unknown>;
  const secs = o.sections;
  if (!Array.isArray(secs)) throw new TransportContractError('sections must be an array');
  const sections: InspectorSection[] = secs.map((s) => {
    const so = s as Record<string, unknown>;
    const rows = so.rows;
    if (!Array.isArray(rows)) throw new TransportContractError('section.rows must be an array');
    return {
      title: str(so, 'title', 'section'),
      emphasis: str(so, 'emphasis', 'section'),
      rows: rows.map((r) => {
        const a = r as unknown[];
        return { label: String(a[0] ?? ''), value: String(a[1] ?? ''), note: String(a[2] ?? '') };
      }),
    };
  });
  return {
    metric_id: str(o, 'metric_id', 'passport'),
    headline: str(o, 'headline', 'passport'),
    subhead: str(o, 'subhead', 'passport'),
    badge: str(o, 'badge', 'passport'),
    banner: str(o, 'banner', 'passport'),
    sections,
  };
}

export function decodeArtifact(raw: unknown): ComparisonArtifact {
  const o = (raw ?? {}) as Record<string, unknown>;
  return {
    comparison_id: str(o, 'comparison_id', 'artifact'),
    left_display: str(o, 'left_display', 'artifact'),
    right_display: str(o, 'right_display', 'artifact'),
    difference_display: str(o, 'difference_display', 'artifact'),
    ratio_display: str(o, 'ratio_display', 'artifact'),
    compatibility_proof: str(o, 'compatibility_proof', 'artifact'),
  };
}
