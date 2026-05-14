import SignalChip from './SignalChip'
import { parseSignalSequence } from '../utils/signalBadges'

/**
 * SignalSequence — renders an ordered sequence of signal chips.
 * Accepts string ("Z5 -> T3", "Z5→T3"), array, or null.
 */
export default function SignalSequence({
  value,
  max = 5,
  separator = '→',
  size = 'sm',
  className = '',
}) {
  const items = parseSignalSequence(value)
  if (items.length === 0) return null

  const shown = items.slice(0, max)
  const extra = items.length - shown.length

  return (
    <span className={`inline-flex items-center flex-wrap gap-0.5 ${className}`}>
      {shown.map((s, i) => (
        <span key={`${s}-${i}`} className="inline-flex items-center gap-0.5">
          {i > 0 && (
            <span className="text-[9px] text-md-on-surface-var/70">{separator}</span>
          )}
          <SignalChip signal={s} size={size} />
        </span>
      ))}
      {extra > 0 && (
        <span className="text-[9px] text-md-on-surface-var/70 italic ml-0.5">+{extra}</span>
      )}
    </span>
  )
}
