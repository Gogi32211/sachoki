/**
 * JournalBench — the baseline that makes a paper-journal win% readable.
 *
 * WHY (2026-07-17): all three journals exit on a 20-bar hold (the +100% target hits ~0.2%
 * of the time, so it's decorative). A 20-bar hold wins ~55-59% on ANY liquid stock in a
 * bull window — so a bare "win 73%" reports the market, not the signal. Measured: the
 * Atomic journal's 73.1%/+5.49% vs a random basket on its OWN dates = 57.9%/+2.62%, and
 * its 52 closed trades all sit in one week (36 on a single day) — the win lift is +2.24σ
 * but the MEAN lift only +0.59σ. The 🔥Capit→Atom replay: 58.9%/+2.83% vs 55.6%/+2.59%
 * = +0.24pp on mean (+0.32σ), i.e. indistinguishable from random.
 *
 * Backend: journal_bench.py precomputes each journal's own exit across the liquid universe
 * for every (ticker, date), so `bench` is the EXACT expectation of a random same-size
 * basket on the journal's own signal dates (weighted by its own trades/day), not an
 * estimate of it. Renders nothing when the cache is absent.
 */

/** The lift is the number that matters; the raw win% is the market plus the lift. */
export default function JournalBench({ stats, n }) {
  const b = stats?.bench
  if (!b) return null
  const dw = stats.win_vs_bench, dm = stats.mean_vs_bench
  const col = (v) => v == null ? 'text-md-on-surface-var' : v > 0 ? 'text-emerald-300' : 'text-rose-300'
  const sg = (v, d = 1) => v == null ? '—' : `${v > 0 ? '+' : ''}${v.toFixed(d)}`
  // one-day-wonder guard: a journal whose trades cluster into a few dates has an effective
  // sample of ~the date count, not the trade count — say so rather than let n flatter it.
  const thin = b.dates <= 10
  return (
    <div className="mb-3 rounded border border-amber-700/40 bg-amber-950/20 px-3 py-2 text-[11px] text-amber-100/90 max-w-5xl">
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
        <b className="text-amber-200">📏 vs random basket</b>
        <span>same {b.dates} signal dates · same exit · whole liquid universe (~{b.universe_per_date}/day)</span>
      </div>
      <div className="mt-1.5 flex flex-wrap items-baseline gap-x-5 gap-y-1 font-mono">
        <span>random: <b className="text-amber-200">win {b.win}%</b> · <b className="text-amber-200">mean {sg(b.mean, 2)}%</b></span>
        <span className="text-amber-300/50">→</span>
        <span>this journal's <b>lift</b>: win <b className={col(dw)}>{sg(dw)}pp</b> · mean <b className={col(dm)}>{sg(dm, 2)}pp</b></span>
      </div>
      <div className="mt-1 text-amber-200/60 leading-snug">
        A 20-bar hold wins ~55-59% on <i>any</i> stock in a bull window — read the <b>lift</b>, not the raw win%.
        The mean lift is what pays; a win% lift with no mean lift means smaller winners or bigger losers.
        {thin && <> <b className="text-rose-300">⚠ only {b.dates} distinct signal dates</b> — the effective sample is
          ~{b.dates} market events, not {n ?? b.trades_matched} independent trades, so treat any lift as one draw.</>}
      </div>
    </div>
  )
}
