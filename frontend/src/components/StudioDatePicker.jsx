/**
 * StudioDatePicker.jsx
 * Minimal inline calendar picker matching the dark Studio theme.
 * No external CSS needed — fully Tailwind-styled.
 */
import { useState, useRef, useEffect, useCallback } from 'react'

const MONTHS = [
  'January','February','March','April','May','June',
  'July','August','September','October','November','December'
]
const DOW = ['Mo','Tu','We','Th','Fr','Sa','Su']

function getDaysInMonth(year, month) {
  return new Date(year, month + 1, 0).getDate()
}
function getFirstDow(year, month) {
  // 0=Mon … 6=Sun
  return (new Date(year, month, 1).getDay() + 6) % 7
}
function pad(n) { return String(n).padStart(2, '0') }
function toISO(y, m, d) { return `${y}-${pad(m + 1)}-${pad(d)}` }

// Parse "yyyy-mm-dd" or ""
function parseISO(s) {
  if (!s) return null
  const [y, m, d] = s.split('-').map(Number)
  if (!y || !m || !d) return null
  return { year: y, month: m - 1, day: d }
}

function formatDisplay(isoStr) {
  if (!isoStr) return null
  const p = parseISO(isoStr)
  if (!p) return null
  return `${pad(p.day)}.${pad(p.month + 1)}.${p.year}`
}

export default function StudioDatePicker({ label, value, onChange, placeholder = 'Pick date' }) {
  const today = new Date()
  const parsed = parseISO(value)

  const [open, setOpen] = useState(false)
  const [viewYear, setViewYear]   = useState(parsed?.year  ?? today.getFullYear())
  const [viewMonth, setViewMonth] = useState(parsed?.month ?? today.getMonth())
  const ref = useRef(null)

  // Close on outside click
  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    if (open) document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const prevMonth = () => {
    if (viewMonth === 0) { setViewMonth(11); setViewYear(y => y - 1) }
    else setViewMonth(m => m - 1)
  }
  const nextMonth = () => {
    if (viewMonth === 11) { setViewMonth(0); setViewYear(y => y + 1) }
    else setViewMonth(m => m + 1)
  }

  const selectDay = (day) => {
    onChange(toISO(viewYear, viewMonth, day))
    setOpen(false)
  }

  const clear = (e) => {
    e.stopPropagation()
    onChange('')
  }

  const daysInMonth = getDaysInMonth(viewYear, viewMonth)
  const firstDow    = getFirstDow(viewYear, viewMonth)

  const cells = []
  for (let i = 0; i < firstDow; i++) cells.push(null)
  for (let d = 1; d <= daysInMonth; d++) cells.push(d)

  const selDay = parsed && parsed.year === viewYear && parsed.month === viewMonth
    ? parsed.day : null

  const todayDay = today.getFullYear() === viewYear && today.getMonth() === viewMonth
    ? today.getDate() : null

  const displayText = formatDisplay(value)

  return (
    <div className="relative flex flex-col gap-1" ref={ref}>
      {label && (
        <span className="text-[10px] text-md-on-surface-var font-medium uppercase tracking-wide">
          {label}
        </span>
      )}

      {/* Trigger button */}
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className={[
          'flex items-center gap-2 px-3 py-1.5 rounded-lg border text-sm text-left transition-colors',
          'bg-md-surface focus:outline-none',
          open
            ? 'border-md-primary ring-1 ring-md-primary/30'
            : 'border-md-outline-var hover:border-md-primary/50',
        ].join(' ')}
      >
        <svg className="w-3.5 h-3.5 text-md-on-surface-var shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2}>
          <rect x="3" y="4" width="18" height="18" rx="2"/>
          <path d="M16 2v4M8 2v4M3 10h18"/>
        </svg>
        <span className={displayText ? 'text-md-on-surface font-mono' : 'text-md-on-surface-var/50'}>
          {displayText ?? placeholder}
        </span>
        {displayText && (
          <span
            onClick={clear}
            className="ml-auto text-md-on-surface-var/50 hover:text-red-400 text-xs leading-none"
          >✕</span>
        )}
      </button>

      {/* Calendar popover */}
      {open && (
        <div className="absolute top-full left-0 mt-1.5 z-50 w-64 rounded-xl bg-md-surface-con border border-md-outline-var shadow-2xl">
          {/* Header */}
          <div className="flex items-center gap-1 px-3 py-2.5 border-b border-md-outline-var">
            <button onClick={prevMonth}
              className="p-1 rounded-md text-md-on-surface-var hover:bg-white/10 hover:text-md-on-surface transition-colors">
              ‹
            </button>
            <div className="flex-1 text-center text-xs font-semibold text-md-on-surface">
              {MONTHS[viewMonth]} {viewYear}
            </div>
            <button onClick={nextMonth}
              className="p-1 rounded-md text-md-on-surface-var hover:bg-white/10 hover:text-md-on-surface transition-colors">
              ›
            </button>
          </div>

          {/* Day-of-week headers */}
          <div className="grid grid-cols-7 px-2 pt-2 pb-0.5">
            {DOW.map(d => (
              <div key={d} className="text-center text-[10px] text-md-on-surface-var font-medium py-0.5">
                {d}
              </div>
            ))}
          </div>

          {/* Day grid */}
          <div className="grid grid-cols-7 px-2 pb-2.5 gap-y-0.5">
            {cells.map((day, i) => {
              if (!day) return <div key={`e-${i}`} />
              const isSel   = day === selDay
              const isToday = day === todayDay
              return (
                <button
                  key={day}
                  type="button"
                  onClick={() => selectDay(day)}
                  className={[
                    'relative flex items-center justify-center rounded-lg text-xs font-medium h-8 transition-colors',
                    isSel
                      ? 'bg-md-primary text-md-on-primary font-bold'
                      : isToday
                        ? 'text-md-primary border border-md-primary/40 hover:bg-md-primary/10'
                        : 'text-md-on-surface hover:bg-white/10',
                  ].join(' ')}
                >
                  {day}
                </button>
              )
            })}
          </div>

          {/* Quick shortcuts */}
          <div className="flex gap-1 px-2 pb-2.5 border-t border-md-outline-var pt-2">
            {[
              { label: 'Today', fn: () => { const n = new Date(); onChange(toISO(n.getFullYear(), n.getMonth(), n.getDate())); setOpen(false) } },
              { label: '-30d',  fn: () => { const n = new Date(Date.now() - 30*864e5); onChange(toISO(n.getFullYear(), n.getMonth(), n.getDate())); setOpen(false) } },
              { label: '-90d',  fn: () => { const n = new Date(Date.now() - 90*864e5); onChange(toISO(n.getFullYear(), n.getMonth(), n.getDate())); setOpen(false) } },
              { label: 'Clear', fn: () => { onChange(''); setOpen(false) } },
            ].map(s => (
              <button key={s.label} type="button" onClick={s.fn}
                className="flex-1 py-1 text-[10px] font-medium rounded-md text-md-on-surface-var hover:bg-white/10 hover:text-md-on-surface transition-colors border border-md-outline-var/50">
                {s.label}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
