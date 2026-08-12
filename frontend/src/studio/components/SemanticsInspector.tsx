/** The passport drawer. It renders what the server already decided; it computes nothing. */
import type { InspectorPassport } from '../semantics/types';

export function SemanticsInspector({ passport, onClose }: {
  passport: InspectorPassport | null;
  onClose: () => void;
}) {
  if (!passport) return null;
  return (
    <div className="fixed inset-y-0 right-0 z-50 w-[36rem] max-w-full overflow-y-auto
                    border-l border-md-outline-var bg-md-surface-con p-6 shadow-2xl">
      <button type="button" onClick={onClose}
              className="float-right text-xs text-md-on-surface-var hover:text-md-on-surface">close</button>
      <div className="font-mono text-3xl">{passport.headline}</div>
      <div className="text-sm text-md-on-surface-var">{passport.subhead}</div>
      <div className="mt-1 text-xs uppercase tracking-wide text-md-on-surface-var">{passport.badge}</div>
      {passport.banner && (
        <div className="mt-4 rounded border border-md-error bg-md-error-container p-3 text-xs text-md-error">
          {passport.banner}
        </div>
      )}
      {passport.sections.map((s) => (
        <div key={s.title} className="mt-6">
          <div className={`text-[11px] font-semibold uppercase tracking-widest
                           ${s.emphasis === 'block' ? 'text-md-warning' : 'text-md-on-surface-var'}`}>
            {s.title}
          </div>
          <div className="mt-2 space-y-2">
            {s.rows.map((r, i) => (
              <div key={`${s.title}-${i}`} className="text-xs">
                <div className="flex gap-3">
                  <div className="w-40 shrink-0 text-md-on-surface-var">{r.label}</div>
                  <div className="text-md-on-surface">{r.value}</div>
                </div>
                {r.note && <div className="ml-[10.75rem] mt-1 text-md-on-surface-var">{r.note}</div>}
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
