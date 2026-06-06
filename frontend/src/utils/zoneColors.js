// Distinct, high-contrast palette for multiple zones on a chart.
// Keeps the same color for the same zone index everywhere it's shown
// (chart price-lines, trigger-bar markers, sidebar Z# badges).
export const ZONE_PALETTE = [
  '#22d3ee',  // cyan-400    Z1
  '#f59e0b',  // amber-500   Z2
  '#a855f7',  // purple-500  Z3
  '#ec4899',  // pink-500    Z4
  '#84cc16',  // lime-500    Z5
  '#06b6d4',  // sky-500     Z6
  '#fb7185',  // rose-400    Z7
  '#facc15',  // yellow-400  Z8
  '#34d399',  // emerald-400 Z9
  '#a3e635',  // lime-400    Z10
]

export const zoneColor = (i) => ZONE_PALETTE[i % ZONE_PALETTE.length]
