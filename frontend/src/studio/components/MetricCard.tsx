/**
 * A card receives a SemanticMetricView. It never receives a number, so it cannot make one.
 *
 * There is no prop for a value, a delta, a rank or a colour scale. `display_value` arrives as
 * text and is rendered as text. The only interaction is opening the passport.
 */
import type { SemanticMetricView } from '../semantics/types';

export function MetricCard({ metric, onInspect }: {
  metric: SemanticMetricView;
  onInspect: (ref: string) => void;
}) {
  const invalid = metric.integrity_status === 'INVALID';
  return (
    <div className={`rounded-lg border p-4 ${invalid ? 'border-md-error' : 'border-md-outline-var'}`}>
      <div className="text-xs uppercase tracking-wide text-md-on-surface-var">{metric.label}</div>
      <button
        type="button"
        onClick={() => onInspect(metric.inspector_ref)}
        className="mt-1 text-3xl font-mono hover:underline"
        title="open the passport of this number"
      >
        {metric.display_value}{metric.display_units}
      </button>
      <div className="mt-3 space-y-1 text-xs text-md-on-surface-var">
        <div>{metric.semantic_type}</div>
        <div>{metric.population_summary}</div>
        <div className={invalid ? 'text-md-error' : ''}>{metric.integrity_status}</div>
        {metric.conclusion_status !== 'NONE' && <div>{metric.conclusion_status}</div>}
      </div>
    </div>
  );
}
