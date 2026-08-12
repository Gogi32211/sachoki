/**
 * ONE control for all twenty-two parameters. There is no `if (id === 'horizon')` anywhere.
 *
 * The backend is role-driven; the obvious way to lose that is to write twenty-two React
 * components and let each grow its own idea of what its knob costs. So this file may branch on
 * `ui_kind` — how to render an input — and may not branch on `parameter_id` or on
 * `semantic_role`. The role is displayed, never consulted.
 *
 * The badge is not decoration. Two controls that look identical on screen can cost completely
 * different things:
 *
 *     Displayed top-K   5 → 10     a view of the same ranked 31
 *     Selection top-K  31 → 37     the multiplicity a verdict must survive
 *
 * A user who cannot see which is which will eventually pay for one thinking it was the other.
 */
import type { ParameterDefinitionView } from '../semantics/types';

const ROLE_BADGE: Record<string, { label: string; cls: string }> = {
  PRESENTATION_ONLY: { label: 'view', cls: 'border-md-outline-var text-md-on-surface-var' },
  CLAIM_CHANGE: { label: 'claim', cls: 'border-md-warning text-md-warning' },
  DESIGN_CHANGE: { label: 'design', cls: 'border-md-warning text-md-warning' },
  SEARCH_SPACE_CHANGE: { label: 'search space', cls: 'border-md-error text-md-error' },
  POLICY_CHANGE: { label: 'policy', cls: 'border-md-error text-md-error' },
};

export function RoleBadge({ role }: { role: string }) {
  const b = ROLE_BADGE[role] ?? { label: role.toLowerCase(), cls: 'border-md-outline-var' };
  return (
    <span data-role-badge={role}
          className={`rounded border px-1.5 py-px text-[10px] font-semibold uppercase
                      tracking-wider ${b.cls}`}>
      {b.label}
    </span>
  );
}

interface Props {
  param: ParameterDefinitionView;
  frozen: boolean;
  onChange: (parameterId: string, value: string) => void;
}

export function ParameterControl({ param, frozen, onChange }: Props) {
  const p = param;
  const locked = frozen && p.mutable_in_registered === 'NO';
  const fire = (v: string) => onChange(p.parameter_id, v);

  // Only `ui_kind` decides the widget. Nothing here reads parameter_id.
  let input;
  if (p.ui_kind === 'ENUM' || p.ui_kind === 'MULTI') {
    input = (
      <select value={p.current_value}
              data-param-input={p.parameter_id}
              onChange={(e) => fire(e.target.value)}
              className="w-40 rounded border border-md-outline-var bg-md-surface-con px-2 py-1
                         text-xs text-md-on-surface">
        {!p.options.includes(p.current_value) && <option value={p.current_value}>
          {p.current_value || '—'}
        </option>}
        {p.options.map((o) => <option key={o} value={o}>{o || '—'}</option>)}
      </select>
    );
  } else if (p.ui_kind === 'NUMBER') {
    input = (
      <input type="number" value={p.current_value}
             data-param-input={p.parameter_id}
             min={p.min || undefined} max={p.max || undefined} step={p.step || undefined}
             onChange={(e) => fire(e.target.value)}
             className="w-28 rounded border border-md-outline-var bg-md-surface-con px-2 py-1
                        text-right text-xs text-md-on-surface" />
    );
  } else {
    input = (
      <input type="text" value={p.current_value}
             data-param-input={p.parameter_id}
             onChange={(e) => fire(e.target.value)}
             className="w-40 rounded border border-md-outline-var bg-md-surface-con px-2 py-1
                        text-xs text-md-on-surface" />
    );
  }

  return (
    <div data-param-row={p.parameter_id}
         data-param-role={p.semantic_role}
         data-param-value={p.current_value}
         className="flex items-center justify-between gap-3 border-b border-md-outline-var
                    py-1.5 last:border-b-0">
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs text-md-on-surface">{p.label}</span>
          <RoleBadge role={p.semantic_role} />
          {locked && (
            <span className="text-[10px] uppercase tracking-wider text-md-on-surface-var">
              frozen
            </span>
          )}
        </div>
        <div className="truncate text-[11px] text-md-on-surface-var">{p.description}</div>
      </div>
      {input}
    </div>
  );
}
