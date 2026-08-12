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
