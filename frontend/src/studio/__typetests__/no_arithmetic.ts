/**
 * A test that passes by failing to compile.
 *
 * `SemanticMetricView` has no `value`, so every line below is a type error. The check runs tsc
 * against this file and asserts that it reports exactly these errors — if the file ever compiles
 * clean, someone has put a numeric field back on the transport type and the wall is gone.
 *
 * This is the compile-time half. The runtime half lives in decode.ts, which rejects a payload
 * carrying a numeric `value` rather than ignoring it. Neither substitutes for the other: a cast
 * defeats the first, a well-behaved compiler does nothing about the second.
 */
import type { SemanticMetricView } from '../semantics/types';

declare const left: SemanticMetricView;
declare const right: SemanticMetricView;

// @ts-expect-error — Property 'value' does not exist on type 'SemanticMetricView'.
export const delta = right.value - left.value;

// @ts-expect-error — same, via a different spelling
export const ratio = right['value'] / left['value'];

// @ts-expect-error — display_value is a string; arithmetic on it is not permitted either
export const sneaky: number = right.display_value - left.display_value;
