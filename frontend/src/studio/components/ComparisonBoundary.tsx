/**
 * Not a disabled control — an explanation that the operation does not exist statistically.
 *
 * A greyed-out button says "you may not". This says "there is nothing here to press", and gives
 * the two experiments by name so the reader learns why rather than being refused.
 */
import type { ComparisonVerdict } from '../semantics/types';

export function ComparisonBoundary({ verdict }: { verdict: ComparisonVerdict }) {
  if (verdict.comparable) {
    return (
      <div className="my-6 border-t border-md-outline-var pt-3 text-center text-xs text-md-on-surface-var">
        same registered experiment — a sanctioned comparison may be requested
      </div>
    );
  }
  return (
    <div className="my-6 rounded-lg border border-md-warning bg-md-warning-con p-4 text-center">
      <div className="text-sm font-semibold tracking-wide text-md-warning">
        DIRECT COMPARISON BLOCKED
      </div>
      <div className="mt-1 font-mono text-xs text-md-warning">{verdict.reason_code}</div>
      <div className="mx-auto mt-2 max-w-2xl text-xs text-md-on-surface">{verdict.message}</div>
      <div className="mt-3 grid gap-1 text-[11px] text-md-on-surface-var">
        <div>{verdict.left}</div>
        <div>{verdict.right}</div>
      </div>
    </div>
  );
}
