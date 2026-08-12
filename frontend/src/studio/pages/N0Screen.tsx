/**
 * The first Analytic Studio screen, deliberately boring.
 *
 * Two cards, a boundary between them, and a passport behind each number. No shared axis, no
 * chart, no delta, no winner badge — and not because the design forbids them. Because
 * `display_value` is a string and `SemanticMetricView` has no numeric field, so a component
 * that wanted to draw one could not obtain the operands.
 *
 * The screen is a proof, not a dashboard: it is here to show that the semantic architecture
 * survived the trip from Python into a browser.
 */
import { useEffect, useState } from 'react';
import { fetchN0, fetchPassport } from '../semantics/api';
import { MetricCard } from '../components/MetricCard';
import { ComparisonBoundary } from '../components/ComparisonBoundary';
import { SemanticsInspector } from '../components/SemanticsInspector';
import type { InspectorPassport, N0Screen as N0Data } from '../semantics/types';

export function N0Screen() {
  const [data, setData] = useState<N0Data | null>(null);
  const [error, setError] = useState<string>('');
  const [passport, setPassport] = useState<InspectorPassport | null>(null);

  useEffect(() => {
    fetchN0().then(setData).catch((e: unknown) => setError(String(e)));
  }, []);

  const openPassport = (ref: string) => {
    fetchPassport(ref).then(setPassport).catch((e: unknown) => setError(String(e)));
  };

  if (error) {
    return (
      <div className="m-6 rounded border border-md-error bg-md-error-container p-4 text-sm text-md-error">
        transport contract error — {error}
      </div>
    );
  }
  if (!data) return <div className="p-6 text-sm text-md-on-surface-var">loading…</div>;

  return (
    <div className="mx-auto max-w-4xl p-6">
      <h1 className="text-lg font-semibold tracking-wide">{data.screen}</h1>
      <p className="mt-1 text-xs text-md-on-surface-var">
        Two registered null experiments. Click a number to open its passport.
      </p>
      <div className="mt-6 grid grid-cols-1 gap-4 sm:grid-cols-2">
        {data.metrics.map((m) => (
          <MetricCard key={m.metric_id} metric={m} onInspect={openPassport} />
        ))}
      </div>
      <ComparisonBoundary verdict={data.comparison} />
      <SemanticsInspector passport={passport} onClose={() => setPassport(null)} />
    </div>
  );
}
