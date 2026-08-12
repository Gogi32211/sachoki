/**
 * The transport types. Note what is absent: there is no `value`.
 *
 * `props.metric` instead of `props.value` disciplines a developer; a payload without operands
 * removes the option. The API already serves `display_value` as a string, so no component
 * receives a number. These types close the other route: a component cannot even reach for one,
 * because the field does not exist on the type.
 *
 * The negative fixture in `__typetests__` asserts that `right.value - left.value` fails to
 * compile. It is checked by running tsc and expecting an error — a test that passes by failing.
 */

export type SemanticType = 'DETERMINISTIC' | 'DESCRIPTIVE' | 'INFERENTIAL' | 'DECISION';
export type IntegrityStatus = 'VALID' | 'INVALID';
export type ConclusionStatus =
  | 'NONE' | 'NULL' | 'UNRESOLVED' | 'BUILD' | 'REJECT' | 'DEFERRED'
  | string; // a conclusion may arrive pre-annotated, e.g. "BUILD — NOT INTERPRETABLE"

/** What crosses the wire. Every field is a string, on purpose. */
export interface SemanticMetricView {
  readonly metric_id: string;
  readonly display_value: string;
  readonly display_units: string;
  readonly label: string;
  readonly semantic_type: SemanticType;
  readonly integrity_status: IntegrityStatus;
  readonly conclusion_status: ConclusionStatus;
  readonly population_summary: string;
  readonly inspector_ref: string;
}

export interface ComparisonVerdict {
  readonly comparable: boolean;
  readonly reason_code: string;
  readonly message: string;
  readonly left: string;
  readonly right: string;
}

export interface N0Screen {
  readonly screen: string;
  readonly metrics: readonly SemanticMetricView[];
  readonly comparison: ComparisonVerdict;
}

export interface InspectorRow {
  readonly label: string;
  readonly value: string;
  readonly note: string;
}

export interface InspectorSection {
  readonly title: string;
  readonly emphasis: string;
  readonly rows: readonly InspectorRow[];
}

export interface InspectorPassport {
  readonly metric_id: string;
  readonly headline: string;
  readonly subhead: string;
  readonly badge: string;
  readonly banner: string;
  readonly sections: readonly InspectorSection[];
}

/** Only obtainable from the server after ComparisonGuard passes. */
export interface ComparisonArtifact {
  readonly comparison_id: string;
  readonly left_display: string;
  readonly right_display: string;
  readonly difference_display: string;
  readonly ratio_display: string;
  readonly compatibility_proof: string;
}

/**
 * Session accounting, sanctioned. Every count is a string for the same reason the statistical
 * values were: a number in the browser invites the browser to derive one, and `k` has exactly
 * one accountant.
 *
 * The append-only ledger never crosses. A frontend holding the events could count claims its own
 * way, and the first time the two counts differed, the convenient one would win.
 */
export interface ResearchSessionView {
  readonly session_id: string;
  readonly mode: string;
  readonly k_declared: string;
  readonly k_exposed: string;
  /** which of the two degrees of freedom multiplied k. Reported, never derived here. */
  readonly distinct_evidence_claims: string;
  readonly distinct_decision_specs: string;
  readonly accounting_policy: string;
  /** what the whole lineage has seen. Equal to k_exposed until a fork happens. */
  readonly k_exposed_lineage: string;
  readonly inherited_exposed: string;
  readonly parent_session_id: string;
  readonly lineage_depth: string;
  readonly k_selectable: string;
  readonly revisits: string;
  readonly displayed_at_most: string;
  readonly changes_claim: string;
  readonly changes_design: string;
  readonly changes_search_space: string;
  readonly changes_selection_path: string;
  readonly changes_policy: string;
  readonly changes_presentation: string;
  readonly confirmatory_eligible: string;
  readonly events: string;
  readonly state_hash: string;
}

export interface ChangePreview {
  readonly parameter_id: string;
  readonly change_type: string;
  readonly old_claim_hash: string;
  readonly new_claim_hash: string;
  readonly multiplicity_effect: string;
}

/**
 * A refusal the screen can read out loud.
 *
 * A disabled control with no explanation teaches nothing: the user concludes the app is broken,
 * or learns which click order avoids the grey. Every refusal arrives with what happened, why the
 * rule exists, and whether a legitimate next move exists.
 */
export interface Refusal {
  readonly error: string;
  readonly detail: string;
  readonly remedy: string;
  /** FORK · NEW_SESSION · NONE — which legal move exists, decided by the rule that fired. */
  readonly next_action: string;
  readonly offers_fork: string;
}

export interface ForkResult {
  readonly session: ResearchSessionView;
  readonly parent: ResearchSessionView;
  readonly inherited: { readonly horizon: string; readonly tolerance: string };
  readonly reason: string;
}

/**
 * A knob, as the backend describes it.
 *
 * Two halves that must not be confused. `ui_kind` says how to render an input — a NUMBER control
 * looks the same whether the number is a view or a multiplicity. `semantic_role` says what
 * turning it costs, and it is SERVED, never inferred here. The frontend picks a widget; it does
 * not decide what a widget means.
 */
export interface ParameterDefinitionView {
  readonly parameter_id: string;
  readonly label: string;
  readonly description: string;
  readonly ui_kind: string;          // NUMBER · ENUM · MULTI · BOOLEAN · TEXT
  readonly group: string;
  readonly options: readonly string[];
  readonly min: string;
  readonly max: string;
  readonly step: string;
  readonly current_value: string;
  readonly semantic_role: string;
  readonly mutable_in_explore: string;
  readonly mutable_in_registered: string;
  readonly multiplicity_effect: string;
  readonly registered_effect: string;
  readonly note: string;
}

export interface ParameterCatalogue {
  readonly parameters: readonly ParameterDefinitionView[];
  readonly groups: readonly string[];
  readonly parameter_registry_hash: string;
}

/**
 * What a preview promised, in a form the commit can be checked against.
 *
 * The screen holds this between the click that asks and the click that confirms. It is opaque
 * here: the frontend forwards `plan_hash` and never recomputes any field of it, because a plan
 * the UI could rebuild is a plan the UI could quietly alter.
 */
export interface ChangePlanView {
  readonly plan_id: string;
  readonly session_id: string;
  readonly prior_state_hash: string;
  readonly parameter_id: string;
  readonly old_value: string;
  readonly new_value: string;
  readonly semantic_role: string;
  readonly old_claim_hash: string;
  readonly new_claim_hash: string;
  readonly old_search_space_hash: string;
  readonly new_search_space_hash: string;
  readonly old_decision_policy_hash: string;
  readonly new_decision_policy_hash: string;
  readonly multiplicity_effect: string;
  readonly registered_effect: string;
  readonly parameter_registry_hash: string;
  readonly no_op: string;
  readonly plan_hash: string;
}
