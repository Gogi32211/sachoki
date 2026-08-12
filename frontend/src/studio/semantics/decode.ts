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

const SESSION_FIELDS = [
  'session_id', 'mode', 'k_declared', 'k_exposed', 'distinct_evidence_claims',
  'distinct_decision_specs', 'accounting_policy', 'k_exposed_lineage',
  'inherited_exposed', 'parent_session_id', 'lineage_depth', 'k_selectable', 'revisits',
  'displayed_at_most', 'changes_claim', 'changes_search_space', 'changes_policy',
  'changes_design', 'changes_selection_path', 'changes_presentation', 'confirmatory_eligible', 'events',
  'state_hash',
] as const;

export function decodeSession(raw: unknown): import('./types').ResearchSessionView {
  if (!raw || typeof raw !== 'object') {
    throw new TransportContractError('session view is not an object');
  }
  const o = raw as Record<string, unknown>;
  for (const forbidden of ['ledger', 'events_list', 'history', 'claims']) {
    if (forbidden in o) {
      throw new TransportContractError(
        `session view carries '${forbidden}'. The event stream stays on the server; a frontend ` +
        `holding it would eventually count k its own way.`,
      );
    }
  }
  const leaks = numericLeaves(o);
  if (leaks.length) {
    throw new TransportContractError(`numeric leaves in the session view: ${leaks.join(', ')}`);
  }
  const out: Record<string, string> = {};
  for (const f of SESSION_FIELDS) out[f] = str(o, f, 'session');
  return out as unknown as import('./types').ResearchSessionView;
}

export function decodePreview(raw: unknown): import('./types').ChangePreview {
  const o = (raw ?? {}) as Record<string, unknown>;
  return {
    parameter_id: str(o, 'parameter_id', 'preview'),
    change_type: str(o, 'change_type', 'preview'),
    old_claim_hash: str(o, 'old_claim_hash', 'preview'),
    new_claim_hash: str(o, 'new_claim_hash', 'preview'),
    multiplicity_effect: str(o, 'multiplicity_effect', 'preview'),
  };
}

/**
 * A 409 body. Decoded, not rendered from `JSON.stringify` — the screen shows the backend's
 * sentence, and a missing remedy is a contract violation rather than an empty paragraph.
 */
export function decodeRefusal(raw: unknown): import('./types').Refusal {
  const outer = (raw ?? {}) as Record<string, unknown>;
  const d = (outer.detail ?? outer) as Record<string, unknown>;
  if (!d || typeof d !== 'object' || typeof d.error !== 'string') {
    throw new TransportContractError('a refusal arrived with no machine-readable cause');
  }
  return {
    error: String(d.error), detail: String(d.detail ?? ''),
    remedy: String(d.remedy ?? ''), next_action: String(d.next_action ?? 'NONE'),
    offers_fork: String(d.offers_fork ?? 'NO'),
  };
}

const PARAM_FIELDS = [
  'parameter_id', 'label', 'description', 'ui_kind', 'group', 'min', 'max', 'step',
  'current_value', 'semantic_role', 'mutable_in_explore', 'mutable_in_registered',
  'multiplicity_effect', 'registered_effect', 'note',
] as const;

const PLAN_FIELDS = [
  'plan_id', 'session_id', 'prior_state_hash', 'parameter_id', 'old_value', 'new_value',
  'semantic_role', 'old_claim_hash', 'new_claim_hash', 'old_search_space_hash',
  'new_search_space_hash', 'old_decision_policy_hash', 'new_decision_policy_hash',
  'multiplicity_effect', 'registered_effect', 'parameter_registry_hash', 'no_op', 'plan_hash',
] as const;

export function decodeCatalogue(raw: unknown): import('./types').ParameterCatalogue {
  const o = (raw ?? {}) as Record<string, unknown>;
  if (!Array.isArray(o.parameters)) {
    throw new TransportContractError('parameter catalogue carries no parameters array');
  }
  const parameters = o.parameters.map((row) => {
    const r = row as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const f of PARAM_FIELDS) out[f] = str(r, f, 'parameter');
    if (!Array.isArray(r.options)) {
      throw new TransportContractError(`parameter ${String(r.parameter_id)} has no options list`);
    }
    out.options = r.options.map((x) => String(x));
    return out as unknown as import('./types').ParameterDefinitionView;
  });
  return {
    parameters,
    groups: (Array.isArray(o.groups) ? o.groups : []).map((g) => String(g)),
    parameter_registry_hash: str(o, 'parameter_registry_hash', 'catalogue'),
  };
}

export function decodePlan(raw: unknown): import('./types').ChangePlanView {
  const outer = (raw ?? {}) as Record<string, unknown>;
  const p = (outer.plan ?? outer) as Record<string, unknown>;
  const out: Record<string, string> = {};
  for (const f of PLAN_FIELDS) out[f] = str(p, f, 'plan');
  return out as unknown as import('./types').ChangePlanView;
}

const CELL_FIELDS = ['display_value', 'display_units', 'label', 'semantic_type',
                     'inspector_ref'] as const;

function decodeCell(raw: unknown, where: string): import('./types').SemanticCellView {
  const o = (raw ?? {}) as Record<string, unknown>;
  const out: Record<string, string> = {};
  for (const f of CELL_FIELDS) out[f] = str(o, f, where);
  for (const forbidden of ['value', 'estimate', 'ci_low', 'ci_high', 'point', 'raw']) {
    if (forbidden in o) {
      throw new TransportContractError(
        `${where} carries '${forbidden}'. A statistical cell crosses as text; a number here ` +
        `would hand the screen an operand, which is the thing the N0 boundary exists to stop.`,
      );
    }
  }
  return out as unknown as import('./types').SemanticCellView;
}

/**
 * Decoding a run also enforces its central invariant. If the payload ever carries more rows than
 * it admits to displaying, the extra ones were exposed and nobody counted them — so the decoder
 * refuses rather than rendering the first five and hoping.
 */
export function decodeRun(raw: unknown): import('./types').SearchRunView {
  const outer = (raw ?? {}) as Record<string, unknown>;
  const o = (outer.run ?? outer) as Record<string, unknown>;
  if (!Array.isArray(o.rows)) throw new TransportContractError('run carries no rows array');
  const num = (f: string) => {
    const v = o[f];
    if (typeof v !== 'number') throw new TransportContractError(`run.${f} is not a number`);
    return v;
  };
  const displayed = num('displayed_count');
  if (o.rows.length !== displayed) {
    throw new TransportContractError(
      `run says displayed_count=${displayed} and carries ${o.rows.length} rows. Every row in ` +
      `the payload is an exposed claim whether or not it is drawn.`,
    );
  }
  const rows = o.rows.map((row) => {
    const r = row as Record<string, unknown>;
    return {
      claim_id: str(r, 'claim_id', 'row'), rank: Number(r.rank), label: str(r, 'label', 'row'),
      evidence_claim_hash: str(r, 'evidence_claim_hash', 'row'),
      decision_spec_hash: str(r, 'decision_spec_hash', 'row'),
      effect: decodeCell(r.effect, 'row.effect'),
      uncertainty: decodeCell(r.uncertainty, 'row.uncertainty'),
      support: decodeCell(r.support, 'row.support'),
      verdict: str(r, 'verdict', 'row'), inspector_ref: str(r, 'inspector_ref', 'row'),
    } as import('./types').ResultRowView;
  });
  return {
    run_id: str(o, 'run_id', 'run'), input_state_hash: str(o, 'input_state_hash', 'run'),
    current_state_hash: str(o, 'current_state_hash', 'run'),
    freshness: str(o, 'freshness', 'run'),
    selectable_count: num('selectable_count'), ranked_count: num('ranked_count'),
    displayed_count: displayed, display_policy: str(o, 'display_policy', 'run'),
    sampling_target: str(o, 'sampling_target', 'run'),
    null_family: str(o, 'null_family', 'run'),
    integrity_status: str(o, 'integrity_status', 'run'),
    data_provenance: str(o, 'data_provenance', 'run'),
    artifact_hash: str(o, 'artifact_hash', 'run'), rows,
  };
}
