import { Fragment, useCallback, useEffect, useRef, useState } from 'react'

/*
  Company Intelligence — the dependency graph around a ticker.

  THE ONE RULE THIS SCREEN EXISTS TO HOLD
  Evidenced relationships and model guesses must never look alike. A supply-chain map is
  believed on sight; if a guess renders like a citation, the page becomes a confident
  liar and is worse than nothing. So EVIDENCED rows are solid and carry a source link
  that opens the actual filing, and MODEL_PRIOR rows are dashed, muted, and live in their
  own section labelled as questions rather than findings.

  WHAT IS DELIBERATELY NOT DRAWN
  No country percentages. The backend returns counts, because a share computed over a
  partial graph divides by a denominator nobody knows — and it would render as the most
  authoritative number here. The coverage strip states the partiality instead, and it is
  pinned at the top rather than hidden in a tooltip: "15 edges" reads identically whether
  it came from 14 filers or 140, and the flattering reading is the natural one.

  Every percentage that IS shown carries its basis in words. Fabrinet disclosing that
  NVIDIA is 28% of ITS revenue renders beside NVDA, where a bare "28%" would read as a
  share of NVIDIA's spending — a different and much larger claim.
*/

const SECTIONS = [
  { id: 'overview',    label: 'Overview' },
  { id: 'all',         label: 'All relationships' },
  { id: 'supply',      label: 'Supply chain' },
  { id: 'competitors', label: 'Competitors' },
  { id: 'peers',       label: 'Price · 2 months' },
  { id: 'ownership',   label: 'Ownership & partners' },
  { id: 'countries',   label: 'Countries' },
  { id: 'risk',        label: 'Risk' },
  { id: 'priors',      label: 'Unverified' },
]

const CONF_CLS = {
  CONFIRMED: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/40',
  HIGH:      'bg-sky-500/15 text-sky-300 border-sky-500/40',
  MEDIUM:    'bg-amber-500/15 text-amber-300 border-amber-500/40',
  LOW:       'bg-zinc-500/15 text-zinc-400 border-zinc-500/40',
}

const TIER_LABEL = {
  FILING_DISCLOSURE:   'mandated disclosure',
  FILING_MENTION:      'stated in a filing',
  NEWS_ARTICLE:        'news',
  WEB_SEARCH_CITATION: 'web source',
  PEER_CLASSIFICATION: 'shared industry code',
  MODEL_PRIOR:         'no source — model only',
}

const REL_LABEL = {
  SUPPLIES_TO: 'supplies', PROVIDES_EQUIPMENT_TO: 'supplies equipment',
  PROVIDES_MATERIAL_TO: 'supplies material', MANUFACTURES_FOR: 'manufactures for',
  CUSTOMER_OF: 'buys from', COMPETES_WITH: 'competes with',
  SUBSTITUTE_FOR: 'substitutes', PARTNER_OF: 'partners with',
  OWNS: 'holds equity in', DEPENDS_ON: 'depends on',
}

const REL_HINT = {
  SUPPLIES_TO: 'sells goods or services the other builds with',
  PROVIDES_EQUIPMENT_TO: 'sells the machines the other manufactures with',
  PROVIDES_MATERIAL_TO: 'sells raw material the other consumes',
  MANUFACTURES_FOR: 'physically makes what the other sells under its own name',
  CUSTOMER_OF: 'buys from — the mirror of supplies',
  COMPETES_WITH: 'sells into the same demand',
  SUBSTITUTE_FOR: 'its product can replace the other without being a rival firm',
  PARTNER_OF: 'joint development, licensing, or co-selling',
  OWNS: 'holds an equity stake',
  DEPENDS_ON: 'a dependency is stated but no more specific type is known',
}

function ConfBadge({ conf, ceiling }) {
  return (
    <span
      className={`px-1.5 py-0.5 rounded border text-[10px] font-medium ${CONF_CLS[conf] || CONF_CLS.LOW}`}
      title={ceiling
        ? `The extractor claimed more than this. Confidence is capped by what the source can support — this is the ceiling, not its opinion.`
        : `Confidence in this relationship, capped by what its source can support.`}
    >
      {conf}{ceiling ? ' ⌐' : ''}
    </span>
  )
}

/* One relationship. Collapsed it is a claim; expanded it is the sentence behind the claim. */
function EdgeRow({ e, ticker, onSelectTicker, nameOf }) {
  const [open, setOpen] = useState(false)
  const other = e.src === ticker ? e.dst : e.src
  // Two kinds of node have no ticker and both are worth keeping. CIK<digits> is an SEC
  // filer with no listed shares; NAME:<text> is a company named inside someone's filing
  // that is not registered with the SEC at all — DJI, T-Motor, Orqa. The most important
  // competitor a small manufacturer has is routinely private, and dropping it because it
  // is not tradeable would be measuring what is convenient rather than what is true.
  const otherName = nameOf?.(other)
  const listed = other && !other.startsWith('CIK') && !other.startsWith('NAME:')
  const forward = e.src === ticker
  const prior = e.status === 'MODEL_PRIOR'

  return (
    <div className={`border rounded-md mb-1.5 ${prior
      ? 'border-dashed border-zinc-700 bg-zinc-900/30'
      : 'border-md-outline-var/40 bg-md-surface-con/40'}`}>
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full text-left px-2.5 py-2 flex items-center gap-2 flex-wrap hover:bg-white/5"
      >
        <span
          className={`font-mono font-semibold text-md-on-surface ${listed ? 'cursor-pointer hover:underline' : ''}`}
          onClick={ev => { ev.stopPropagation(); listed && onSelectTicker?.(other) }}
          title={listed ? `Open ${other}` : (otherName ? `${otherName} — not listed, no ticker` : '')}
        >
          {listed ? other : (otherName || other || '—')}
          {!listed && <span className="ml-1 text-[10px] font-normal text-md-on-surface-var">unlisted</span>}
        </span>
        {listed && otherName && (
          <span className="text-[11px] text-md-on-surface-var/70">{otherName}</span>
        )}
        <span className="text-[11px] text-md-on-surface-var" title={REL_HINT[e.rel_type] || ''}>
          {forward ? '' : '← '}{REL_LABEL[e.rel_type] || e.rel_type}{forward ? ' →' : ''}
        </span>
        {e.component && (
          <span className="text-[11px] px-1.5 py-0.5 rounded bg-white/5 text-md-on-surface-var">
            {e.component}
          </span>
        )}
        {e.share_pct != null && (
          // the basis, in words, or nothing — a bare percentage beside a ticker is read
          // as that ticker's own share, which is usually the opposite of the disclosure
          <span className="text-[11px] font-semibold text-emerald-300"
                title={e.share_basis || 'basis not recorded for this row'}>
            {e.share_pct.toFixed(1)}%
            <span className="ml-1 font-normal text-md-on-surface-var">
              {e.share_basis || '(basis not recorded)'}
            </span>
          </span>
        )}
        <span className="ml-auto flex items-center gap-2">
          <span className="text-[10px] text-md-on-surface-var">
            {TIER_LABEL[e.evidence_tier] || e.evidence_tier}
          </span>
          <ConfBadge conf={e.confidence} ceiling={e.ceiling_applied} />
          <span className="text-[10px] text-md-on-surface-var w-16 text-right">{e.doc_date || ''}</span>
        </span>
      </button>

      {open && (
        <div className="px-3 pb-2.5 pt-0.5 border-t border-md-outline-var/30">
          {e.quote ? (
            <blockquote className="text-[12px] leading-relaxed text-md-on-surface-var italic
                                   border-l-2 border-md-outline-var/60 pl-2.5 my-2">
              “{e.quote}”
            </blockquote>
          ) : (
            <p className="text-[12px] text-amber-300/80 my-2">
              No quote stored — this row predates the quote gate or came from a source that
              carries none. Treat it as unverified.
            </p>
          )}
          <div className="flex items-center gap-3 flex-wrap text-[11px] text-md-on-surface-var">
            {e.source_url
              ? <a href={e.source_url} target="_blank" rel="noreferrer"
                   className="text-sky-400 hover:underline">{e.source_label || 'source filing'} ↗</a>
              : <span className="text-zinc-500">no document — model assertion only</span>}
            {e.ceiling_applied && (
              <span title={`Claimed ${e.claimed_confidence}, capped to ${e.confidence} by the source tier.`}>
                capped from {e.claimed_confidence}
              </span>
            )}
            {e.extractor && <span className="text-zinc-600">{e.extractor}</span>}
          </div>
        </div>
      )}
    </div>
  )
}

/* What "nothing here" actually means, which is never the same thing twice.

   The first version said "Harvest filings, or they are in filings not yet read" for every
   empty group. On UMAC that was simply false: the harvest had completed and read all six
   filers that named the company. The page told the user to do work that was already done,
   and blamed missing data for what was really "nobody stated this kind of relationship".

   An empty state is a claim about the world, so it has to be derived from coverage rather
   than written once and reused. */
function emptyReason(cov, kind) {
  if (!cov?.harvested) return `Reading SEC filings for this company — nothing to show yet. This takes under a minute and the page fills in as it goes.`
  if (cov.filers_not_read > 0)
    return `No ${kind} among the ${cov.filers_read} filers read. ${cov.filers_not_read} more named this company and were not read — this is a gap in coverage, not a finding.`
  if (!cov.filers_naming_target)
    return `No other SEC filer named this company in the search window, and its own annual report named no ${kind}. For a small or newly listed company that is common — most of its counterparties simply do not file with the SEC.`
  return `None found. All ${cov.filers_naming_target} filers that named this company were read, and none stated a ${kind} relationship. That is a finding about what companies disclose, not a gap.`
}

/* Nine weekly candles, drawn in PERCENT from the window's first open.

   Prices are not comparable across companies — a $220 stock and a $2 stock on one axis
   are two unrelated pictures sharing a frame. Percent from the window start is what makes
   the rows readable against each other, which is the entire point of putting them in one
   table.

   All rows share one vertical scale for the same reason. Per-row autoscaling would make a
   +2% drift and a +40% run look identical, which is precisely the comparison being asked
   for. */
function WeeklyBars({ bars, lo, hi, w = 150, h = 34 }) {
  if (!bars?.length) return <span className="text-[10px] text-md-on-surface-var/60">no bars</span>
  const base = bars[0].o
  const span = (hi - lo) || 1
  // Clipped, not autoscaled. One +35% name stretched the shared scale until every other
  // row was a flat line — technically comparable and visually useless. The scale now
  // covers the bulk of the field and anything past it is drawn to the edge and marked,
  // so an outlier stays legible AS an outlier instead of flattening everyone else.
  const clip = (v) => Math.max(lo, Math.min(hi, v))
  const y = (v) => h - ((clip(v) - lo) / span) * h
  const isClipped = (b) => (b.h / base - 1) * 100 > hi || (b.l / base - 1) * 100 < lo
  const cw = w / bars.length
  const bw = Math.max(2, cw * 0.55)
  const zero = y(0)
  return (
    <svg width={w} height={h} className="block">
      <line x1="0" y1={zero} x2={w} y2={zero} stroke="currentColor"
            className="text-md-outline-var/40" strokeWidth="1" strokeDasharray="2 2" />
      {bars.map((b, i) => {
        const o = (b.o / base - 1) * 100, c = (b.c / base - 1) * 100
        const hiV = (b.h / base - 1) * 100, loV = (b.l / base - 1) * 100
        const up = c >= o
        const x = i * cw + cw / 2
        const top = y(Math.max(o, c)), bot = y(Math.min(o, c))
        return (
          <g key={i} className={up ? 'text-emerald-400' : 'text-rose-400'}>
            <line x1={x} y1={y(hiV)} x2={x} y2={y(loV)} stroke="currentColor" strokeWidth="1" />
            <rect x={x - bw / 2} y={top} width={bw} height={Math.max(1.5, bot - top)}
                  fill="currentColor" />
            {isClipped(b) && (
              <rect x={x - bw / 2} y={hiV > hi ? 0 : h - 1.5} width={bw} height="1.5"
                    fill="currentColor" opacity="0.9" />
            )}
          </g>
        )
      })}
    </svg>
  )
}

/* Mirrors the backend's classify(). Kept deliberately tiny and in one place: the backend
   already had this logic in two places once and they disagreed about which of NVIDIA's
   mandated disclosures were upstream. */
const SRC_UPSTREAM = new Set(['SUPPLIES_TO', 'PROVIDES_EQUIPMENT_TO', 'PROVIDES_MATERIAL_TO', 'MANUFACTURES_FOR'])
const DST_UPSTREAM = new Set(['CUSTOMER_OF', 'DEPENDS_ON'])
function classifySide(e, ticker) {
  const upEnd = SRC_UPSTREAM.has(e.rel_type) ? e.src : DST_UPSTREAM.has(e.rel_type) ? e.dst : null
  if (!upEnd) return 'LATERAL'
  return upEnd === ticker ? 'DOWNSTREAM' : 'UPSTREAM'
}

const SIDE_CLS = { UPSTREAM: 'text-sky-300', DOWNSTREAM: 'text-amber-300', LATERAL: 'text-md-on-surface-var' }

/* Every relationship of this company in one sortable table.

   The sectioned views answer "who are the suppliers"; this answers "what do we have on
   this company at all", which is a different question and the one you ask when you do not
   yet know what you are looking for.

   ONE ROW PER RELATIONSHIP, unlike the price table which is one row per company. A
   company reached three ways appears three times here on purpose — the unit of this table
   is the claim and its evidence, and merging them would hide that two of the three rest on
   the same filing.

   MODEL_PRIOR rows stay visually distinct even here, where the uniform grid is doing its
   best to make everything look equally solid. A sortable table is the easiest place on the
   page to lose the difference between a citation and a guess. */
function AllTable({ edges, ticker, peers, nameOf, onSelectTicker }) {
  const [sort, setSort] = useState({ key: 'doc_date', dir: -1 })
  const [side, setSide] = useState('ALL')
  const [open, setOpen] = useState(null)

  const chg = {}
  ;(peers?.rows || []).forEach(r => { chg[r.ticker] = r.change_pct })

  const rows = (edges || []).map((e, i) => {
    const other = e.src === ticker ? e.dst : e.src
    const listed = other && !other.startsWith('CIK') && !other.startsWith('NAME:')
    return {
      i, e, other, listed,
      label: listed ? other : (nameOf?.(other) || other),
      name: listed ? (nameOf?.(other) || '') : '',
      side: classifySide(e, ticker),
      change: listed ? chg[other] : undefined,
    }
  })

  const filtered = side === 'ALL' ? rows : rows.filter(r => r.side === side)
  const sorted = [...filtered].sort((a, b) => {
    const k = sort.key
    const get = (r) => k === 'company' ? r.label
      : k === 'change' ? (r.change ?? -Infinity)
      : k === 'side' ? r.side
      : k === 'rel' ? r.e.rel_type
      : k === 'conf' ? ['LOW','MEDIUM','HIGH','CONFIRMED'].indexOf(r.e.confidence)
      : r.e[k] ?? ''
    const av = get(a), bv = get(b)
    if (av === bv) return 0
    return (av > bv ? 1 : -1) * sort.dir
  })

  const Th = ({ k, children, align = 'left' }) => (
    <th className={`text-${align} font-normal py-1 cursor-pointer select-none hover:text-md-on-surface`}
        onClick={() => setSort(s => ({ key: k, dir: s.key === k ? -s.dir : -1 }))}>
      {children}{sort.key === k ? (sort.dir === 1 ? ' ▲' : ' ▼') : ''}
    </th>
  )

  const counts = { ALL: rows.length }
  rows.forEach(r => { counts[r.side] = (counts[r.side] || 0) + 1 })

  return (
    <div>
      <div className="flex items-center gap-2 mb-2 flex-wrap">
        {['ALL', 'UPSTREAM', 'DOWNSTREAM', 'LATERAL'].map(sd => (
          <button key={sd} onClick={() => setSide(sd)}
            className={`text-[11px] px-2 py-0.5 rounded border ${side === sd
              ? 'bg-md-primary/20 border-md-primary/50 text-md-primary'
              : 'bg-white/5 border-md-outline-var/40 text-md-on-surface-var hover:bg-white/10'}`}>
            {sd.toLowerCase()} {counts[sd] || 0}
          </button>
        ))}
        <span className="text-[11px] text-md-on-surface-var/70 ml-2">
          one row per relationship — a company reached several ways appears several times,
          because the unit here is the claim and its evidence
        </span>
      </div>

      <table className="w-full text-[12px]">
        <thead className="text-[10.5px] text-md-on-surface-var">
          <tr className="border-b border-md-outline-var/40">
            <Th k="company">company</Th>
            <Th k="rel">relationship</Th>
            <Th k="side">side</Th>
            <th className="text-left font-normal">what</th>
            <Th k="conf">confidence</Th>
            <th className="text-left font-normal">source</th>
            <Th k="doc_date">filed</Th>
            <Th k="change" align="right">2 months</Th>
          </tr>
        </thead>
        <tbody>
          {sorted.map(r => {
            const e = r.e
            const prior = e.status === 'MODEL_PRIOR'
            return (
              <Fragment key={`${e.src}-${e.dst}-${e.rel_type}-${r.i}`}>
                <tr onClick={() => setOpen(open === r.i ? null : r.i)}
                    className={`border-b border-md-outline-var/20 cursor-pointer hover:bg-white/5
                                ${prior ? 'opacity-70 italic' : ''}`}>
                  <td className="py-1">
                    <span className={`font-mono font-semibold ${r.listed
                        ? 'text-md-on-surface hover:underline' : 'text-md-on-surface-var'}`}
                      onClick={ev => { ev.stopPropagation(); r.listed && onSelectTicker?.(r.other) }}>
                      {r.label}
                    </span>
                    {!r.listed && <span className="ml-1 text-[10px] text-md-on-surface-var/70">unlisted</span>}
                    {r.name && <span className="ml-2 text-[11px] text-md-on-surface-var/60">{r.name}</span>}
                  </td>
                  <td className="text-md-on-surface-var" title={REL_HINT[e.rel_type] || ''}>
                    {e.src === ticker ? '' : '← '}{REL_LABEL[e.rel_type] || e.rel_type}
                  </td>
                  <td className={`text-[11px] ${SIDE_CLS[r.side] || ''}`}>{r.side.toLowerCase()}</td>
                  <td className="text-[11px] text-md-on-surface-var max-w-[220px] truncate"
                      title={e.component || ''}>
                    {e.component || ''}
                    {e.share_pct != null && (
                      <span className="ml-1 text-emerald-300 font-semibold"
                            title={e.share_basis || 'basis not recorded'}>
                        {e.share_pct.toFixed(1)}%
                      </span>
                    )}
                  </td>
                  <td><ConfBadge conf={e.confidence} ceiling={e.ceiling_applied} /></td>
                  <td className="text-[10.5px] text-md-on-surface-var">
                    {prior ? 'no source — model only' : (TIER_LABEL[e.evidence_tier] || e.evidence_tier)}
                  </td>
                  <td className="text-[10.5px] text-md-on-surface-var tabular-nums">{e.doc_date || ''}</td>
                  <td className={`text-right tabular-nums font-semibold ${
                    r.change > 0 ? 'text-emerald-400' : r.change < 0 ? 'text-rose-400'
                                                                    : 'text-md-on-surface-var/50'}`}>
                    {r.change == null ? '—' : `${r.change > 0 ? '+' : ''}${r.change.toFixed(1)}%`}
                  </td>
                </tr>
                {open === r.i && (
                  <tr className="border-b border-md-outline-var/20">
                    <td colSpan={8} className="px-3 py-2 bg-md-surface-con/30">
                      {e.quote
                        ? <blockquote className="text-[12px] italic text-md-on-surface-var
                                                 border-l-2 border-md-outline-var/60 pl-2.5 mb-1.5">
                            “{e.quote}”
                          </blockquote>
                        : <p className="text-[12px] text-amber-300/80 mb-1.5">
                            No quote stored — treat as unverified.
                          </p>}
                      <div className="flex gap-3 flex-wrap text-[11px] text-md-on-surface-var">
                        {e.source_url
                          ? <a href={e.source_url} target="_blank" rel="noreferrer"
                               className="text-sky-400 hover:underline"
                               onClick={ev => ev.stopPropagation()}>
                              {e.source_label || 'source filing'} ↗
                            </a>
                          : <span className="text-zinc-500">no document — model assertion only</span>}
                        {e.share_basis && <span>{e.share_pct?.toFixed(1)}% — {e.share_basis}</span>}
                        {e.ceiling_applied && <span>capped from {e.claimed_confidence}</span>}
                      </div>
                    </td>
                  </tr>
                )}
              </Fragment>
            )
          })}
        </tbody>
      </table>
      {sorted.length === 0 && (
        <p className="text-[12px] text-md-on-surface-var/70 italic mt-2">
          Nothing in this slice.
        </p>
      )}
    </div>
  )
}

function PeerTable({ data, nameOf, onSelectTicker }) {
  const [shared, setShared] = useState(true)
  if (!data) return <p className="text-[12px] text-md-on-surface-var">loading…</p>
  if (!data.rows?.length)
    return <p className="text-[12px] text-md-on-surface-var/70 italic">
      No listed company in this graph has weekly bars yet — harvest the graph first.
    </p>

  // A robust bound rather than the extremes: the 85th percentile of absolute move across
  // every bar of every row, symmetric around zero and never tighter than ±5%. Raw min/max
  // hands the whole scale to the single biggest mover.
  const mags = data.rows.flatMap(r => r.bars.flatMap(b => [
    Math.abs((b.h / r.bars[0].o - 1) * 100), Math.abs((b.l / r.bars[0].o - 1) * 100)]))
    .sort((a, b) => a - b)
  const p85 = mags.length ? mags[Math.floor(mags.length * 0.85)] : 5
  const bound = Math.max(5, p85)
  const gLo = -bound, gHi = bound
  const nClipped = data.rows.filter(r => r.bars.some(b =>
    Math.abs((b.h / r.bars[0].o - 1) * 100) > bound ||
    Math.abs((b.l / r.bars[0].o - 1) * 100) > bound)).length

  return (
    <div>
      <div className="flex items-baseline gap-3 flex-wrap mb-2">
        <span className="text-[11.5px] text-md-on-surface-var">
          {data.weeks?.length} weekly bars · {data.n_priced} listed companies · latest bar {data.as_of}
          {data.weeks_behind > 0 && (
            <span className="text-amber-300" title="The weekly database is written by the nightly job; its last bar is not today.">
              {' '}· {data.weeks_behind} week{data.weeks_behind > 1 ? 's' : ''} behind today
            </span>
          )}
        </span>
        <button onClick={() => setShared(v => !v)}
          className="text-[11px] px-2 py-0.5 rounded border border-md-outline-var/50
                     text-md-on-surface-var hover:bg-white/5"
          title={shared
            ? 'All rows share one vertical scale, so the shapes are directly comparable.'
            : 'Each row is autoscaled to its own range — shapes look similar regardless of size.'}>
          {shared ? 'shared scale' : 'own scale'}
        </button>
      </div>
      <p className="text-[11px] text-md-on-surface-var/70 mb-2 max-w-3xl">
        {data.basis}.{shared && (
          <> Scale ±{bound.toFixed(0)}%{nClipped > 0 && (
            <span title="Bars beyond the scale are drawn to the edge with a cap, so one large mover cannot flatten every other row.">
              {' '}· {nClipped} row{nClipped > 1 ? 's' : ''} clipped (capped bar = off scale)
            </span>)}</>
        )}
      </p>

      <table className="w-full text-[12px]">
        <thead className="text-[10.5px] text-md-on-surface-var">
          <tr className="border-b border-md-outline-var/40">
            <th className="text-left font-normal py-1">company</th>
            <th className="text-left font-normal">link</th>
            <th className="text-center font-normal">9 weekly bars</th>
            <th className="text-right font-normal pr-1">change</th>
          </tr>
        </thead>
        <tbody>
          {data.rows.map(r => {
            const lo = shared ? gLo : Math.min(0, ...r.bars.map(b => (b.l / r.bars[0].o - 1) * 100))
            const hi = shared ? gHi : Math.max(0, ...r.bars.map(b => (b.h / r.bars[0].o - 1) * 100))
            const rels = [...new Set(r.relations.map(x => REL_LABEL[x.rel_type] || x.rel_type))]
            return (
              <tr key={r.ticker}
                  className={`border-b border-md-outline-var/20 ${r.is_target ? 'bg-md-primary/10' : ''}`}>
                <td className="py-1">
                  <span className="font-mono font-semibold text-md-on-surface cursor-pointer hover:underline"
                        onClick={() => onSelectTicker?.(r.ticker)}>{r.ticker}</span>
                  {r.is_target && <span className="ml-1 text-[10px] text-md-primary">target</span>}
                  <span className="ml-2 text-[11px] text-md-on-surface-var/70">{nameOf?.(r.ticker)}</span>
                </td>
                <td className="text-[11px] text-md-on-surface-var">
                  {r.is_target ? '—' : rels.join(', ')}
                  {r.side?.length > 0 && (
                    <span className="ml-1 text-[10px] opacity-60">{r.side.join('/').toLowerCase()}</span>
                  )}
                </td>
                <td className="px-2"><WeeklyBars pct={r.pct} bars={r.bars} lo={lo} hi={hi} /></td>
                <td className={`text-right pr-1 font-semibold tabular-nums ${
                  r.change_pct > 0 ? 'text-emerald-400' : r.change_pct < 0 ? 'text-rose-400'
                                                                          : 'text-md-on-surface-var'}`}>
                  {r.change_pct > 0 ? '+' : ''}{r.change_pct?.toFixed(1)}%
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>

      {data.unpriced?.length > 0 && (
        <div className="mt-4">
          <h4 className="text-[12px] font-semibold text-md-on-surface mb-1">
            In the graph, not in the table — {data.unpriced.length}
          </h4>
          <p className="text-[11px] text-md-on-surface-var/70 mb-1.5 max-w-3xl">
            These have no price to plot. They are listed here rather than dropped: leaving
            them out would turn “the competitive field” into “the part of it that happens to
            be listed”, with nothing on screen to mark the difference.
          </p>
          {data.unpriced.map((u, i) => (
            <div key={i} className="text-[11.5px] text-md-on-surface-var py-0.5">
              <span className="text-md-on-surface">{u.name}</span>
              <span className="opacity-70"> — {u.why}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function Group({ title, hint, rows, ticker, onSelectTicker, empty, nameOf }) {
  return (
    <div className="mb-5">
      <div className="flex items-baseline gap-2 mb-1.5">
        <h3 className="text-sm font-semibold text-md-on-surface">{title}</h3>
        <span className="text-[11px] text-md-on-surface-var">{rows.length}</span>
        {hint && <span className="text-[11px] text-md-on-surface-var/70">· {hint}</span>}
      </div>
      {rows.length === 0
        ? <p className="text-[12px] text-md-on-surface-var/70 italic">{empty}</p>
        : rows.map((e, i) => (
            <EdgeRow key={`${e.src}-${e.dst}-${e.rel_type}-${i}`} e={e} nameOf={nameOf}
                     ticker={ticker} onSelectTicker={onSelectTicker} />
          ))}
    </div>
  )
}

/* Pinned, not tucked away. "15 edges" reads the same from 14 filers as from 140. */
function CoverageStrip({ cov, progress, running }) {
  if (!cov) return null
  const partial = cov.filers_not_read > 0
  return (
    <div className={`px-3 py-2 rounded-md border text-[11.5px] mb-3 ${
      !cov.harvested ? 'border-amber-600/40 bg-amber-500/5 text-amber-200/90'
      : partial ? 'border-sky-700/40 bg-sky-500/5 text-sky-200/90'
                : 'border-emerald-700/40 bg-emerald-500/5 text-emerald-200/90'}`}>
      {running && (
        <div className="mb-1 font-medium">
          harvesting — {progress?.phase || 'working'}
          {progress?.total ? ` · ${progress.done || 0}/${progress.total} filers read` : ''}
          {progress?.edges != null ? ` · ${progress.edges} relationships so far` : ''}
        </div>
      )}
      {!cov.harvested ? (
        <span>{running
          ? 'Reading SEC filings for this company — the graph fills in as they are read.'
          : cov.note}</span>
      ) : (
        <>
          <span className="font-medium">
            {cov.documents_matched} documents · {cov.filers_naming_target} filers named this
            company · {cov.filers_read} read · {cov.edges} relationships
          </span>
          {cov.quotes_rejected > 0 && (
            <span className="ml-2 text-amber-300"
                  title="Claims whose quote was not found in the source document. They were discarded.">
              · {cov.quotes_rejected} quotes rejected
            </span>
          )}
          <div className="mt-0.5 opacity-80">{cov.note}</div>
        </>
      )}
    </div>
  )
}

export default function CompanyIntelPanel({ onSelectTicker }) {
  const [input, setInput]   = useState('NVDA')
  const [ticker, setTicker] = useState('NVDA')
  const [data, setData]     = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr]       = useState(null)
  const [section, setSection] = useState('overview')
  const pollRef = useRef(null)
  const [peers, setPeers] = useState(null)

  const load = useCallback((tk) => {
    setLoading(true); setErr(null)
    fetch(`/api/company-intel/${tk}`)
      .then(async r => { if (!r.ok) throw new Error(await r.text()); return r.json() })
      .then(d => setData(d))
      .catch(e => { setErr(String(e.message || e)); setData(null) })
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => { load(ticker) }, [ticker, load])

  // fetched lazily: it is a second DuckDB read and most visits never open the tab
  useEffect(() => {
    if ((section !== 'peers' && section !== 'all') || !data?.ok) return
    setPeers(null)
    fetch(`/api/company-intel/${data.ticker}/peers?weeks=9`)
      .then(r => r.json()).then(setPeers).catch(e => setErr(String(e)))
  }, [section, data?.ticker, data?.coverage?.edges])

  // While a harvest runs, poll. The graph fills in rather than appearing at the end —
  // a 40-second blank screen with a spinner tells the user nothing about what is arriving.
  useEffect(() => {
    clearInterval(pollRef.current)
    if (!data?.harvesting) return
    pollRef.current = setInterval(() => load(ticker), 3000)
    return () => clearInterval(pollRef.current)
  }, [data?.harvesting, ticker, load])

  const submit = (e) => {
    e.preventDefault()
    const t = input.trim().toUpperCase()
    if (t) { setTicker(t); setSection('overview') }
  }

  const harvest = useCallback((tk) => {
    fetch(`/api/company-intel/${tk || ticker}/build`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lookback_days: 400, max_candidates: 40 }),
    }).then(() => load(tk || ticker)).catch(e => setErr(String(e)))
  }, [ticker, load])

  // A ticker nobody has harvested yet renders a profile and nothing else, and "nothing
  // else" is indistinguishable from "this company has no relationships" no matter how the
  // empty state is worded. Typing a ticker is the request; making the user press a second
  // button to get an answer was a design mistake, not a safeguard.
  //
  // Auto-starting is safe here only because harvesting is detached: the POST returns
  // immediately, the profile is already on screen, and progress streams in. It fires once
  // per ticker, on an explicit Load, and only when SEC resolved the ticker — a typo never
  // reaches it, so a mistyped symbol cannot spend a harvest.
  const autoStarted = useRef(new Set())
  useEffect(() => {
    if (!data?.ok || !data.profile) return
    const tk = data.ticker
    if (data.coverage?.harvested || data.harvesting || autoStarted.current.has(tk)) return
    autoStarted.current.add(tk)
    harvest(tk)
  }, [data, harvest])

  const p = data?.profile
  const v = data?.views || {}
  const risk = data?.risk
  const totalEdges = data?.edges?.length || 0

  // The perspective ticker comes from the LOADED DATA, never from the input box.
  //
  // Every row is drawn as "the other company", computed as src === ticker ? dst : src.
  // While a new ticker loads, the previous company's edges are still on screen — and with
  // the new ticker as the perspective, NVIDIA's own rows started rendering NVDA as their
  // own counterparty: "NVDA ← buys from 10.2% of FormFactor's revenues". A page built to
  // avoid misleading readings cannot afford one during every load.
  const perspective = data?.ticker || ticker

  // ticker -> name, and CIK<digits> -> name for filers that have no ticker at all
  const nameOf = useCallback((code) => {
    if (!code || !data) return ''
    if (code.startsWith('NAME:')) return code.slice(5)
    if (code.startsWith('CIK')) {
      const cik = code.slice(3)
      const hit = (data.entities || []).find(x => x.cik === cik || Number(x.cik) === Number(cik))
      return hit?.name || ''
    }
    return data.entity_by_ticker?.[code]?.name || ''
  }, [data])

  return (
    <div className="p-3 max-w-[1500px]">
      <form onSubmit={submit} className="flex items-center gap-2 mb-3">
        <h2 className="text-base font-semibold text-md-on-surface mr-2">🏢 Company Intelligence</h2>
        <input
          value={input} onChange={e => setInput(e.target.value)}
          placeholder="ticker"
          className="w-28 px-2 py-1 rounded bg-md-surface-high border border-md-outline-var/50
                     text-md-on-surface placeholder:text-md-on-surface-var/60
                     font-mono text-sm uppercase focus:outline-none focus:border-md-primary/60"
        />
        <button type="submit"
          className="px-3 py-1 rounded bg-md-primary/20 border border-md-primary/40
                     text-md-primary text-sm hover:bg-md-primary/30">Load</button>
        <button type="button" onClick={harvest} disabled={data?.harvesting}
          className="px-3 py-1 rounded bg-white/5 border border-md-outline-var/50 text-sm
                     text-md-on-surface-var hover:bg-white/10 disabled:opacity-40"
          title="Search SEC filings for companies that name this one, read the passages, and store what they say.">
          {data?.harvesting ? 'harvesting…' : 'Harvest filings'}
        </button>
        {loading && <span className="text-[11px] text-md-on-surface-var">loading…</span>}
      </form>

      {err && (
        <div className="px-3 py-2 mb-3 rounded border border-rose-700/50 bg-rose-500/10
                        text-rose-300 text-[12px]">{err}</div>
      )}

      {p && (
        <div className="mb-3">
          <div className="flex items-baseline gap-2 flex-wrap">
            <span className="text-lg font-semibold text-md-on-surface">{p.name}</span>
            <span className="font-mono text-md-primary">{p.ticker}</span>
            {(p.exchanges || []).map(x => (
              <span key={x} className="text-[11px] px-1.5 py-0.5 rounded bg-white/5
                                       text-md-on-surface-var">{x}</span>
            ))}
          </div>
          <div className="text-[12px] text-md-on-surface-var mt-0.5">
            {p.sic_description}
            {p.hq_country && <> · HQ {p.hq_city ? `${p.hq_city}, ` : ''}{p.hq_country}</>}
            {p.incorporation && (
              <span title="Where it is legally incorporated. Usually Delaware, and it tells you nothing about where anything is made.">
                {' '}· inc. {p.incorporation}
              </span>
            )}
            {p.source_url && (
              <> · <a href={p.source_url} target="_blank" rel="noreferrer"
                      className="text-sky-400 hover:underline">SEC ↗</a></>
            )}
          </div>
        </div>
      )}

      <CoverageStrip cov={data?.coverage} progress={data?.progress} running={data?.harvesting} />

      <div className="flex gap-2 mb-3 flex-wrap">
        {SECTIONS.map(s => {
          const n = s.id === 'supply' ? (v.upstream?.length || 0) + (v.downstream?.length || 0)
                  : s.id === 'competitors' ? v.competitors?.length || 0
                  : s.id === 'ownership' ? (v.ownership?.length || 0) + (v.partners?.length || 0)
                  : s.id === 'priors' ? v.model_priors?.length || 0
                  : null
          return (
            <button key={s.id} onClick={() => setSection(s.id)}
              className={`px-2.5 py-1 rounded text-[12px] border ${
                section === s.id
                  ? 'bg-md-primary/20 border-md-primary/50 text-md-primary'
                  : 'bg-white/5 border-md-outline-var/40 text-md-on-surface-var hover:bg-white/10'}`}>
              {s.label}{n != null && n > 0 ? ` ${n}` : ''}
            </button>
          )
        })}
      </div>

      {section === 'overview' && (
        <div>
          {/* Every non-empty group appears here. Ownership and partners used to be omitted,
              which made the landing screen blank for any company whose relationships are
              mostly equity stakes and partnerships — UMAC held 7 relationships and the
              Overview showed 1. The tabs below still slice it; this is the whole picture. */}
          {totalEdges === 0 && !data?.harvesting && (
            <p className="text-[12px] text-md-on-surface-var/80 italic mb-4">
              {emptyReason(data?.coverage, 'relationship of any kind')}
            </p>
          )}
          {[['Upstream — this company depends on them', v.upstream,
             'suppliers, foundries, equipment, stated dependencies', 'supplier'],
            ['Downstream — they depend on this company', v.downstream,
             'customers, and firms that named it as a concentration risk', 'customer'],
            ['Competitors', v.competitors,
             "named as a competitor in someone's own filing", 'competitive'],
            ['Ownership', v.ownership, 'equity stakes, either direction', 'ownership'],
            ['Partners', v.partners, 'joint development, licensing, co-selling', 'partnership'],
          ].filter(([, rows]) => (rows || []).length > 0)
           .map(([title, rows, hint]) => (
             <Group key={title} title={title} rows={rows} hint={hint} ticker={perspective}
                    onSelectTicker={onSelectTicker} nameOf={nameOf} empty="" />
           ))}
          {/* suppressed while harvesting: the line explains an ABSENCE, and mid-run it
              contradicted the rows already on screen above it */}
          {!data?.harvesting && totalEdges > 0
            && (v.upstream?.length || 0) + (v.downstream?.length || 0) === 0 && (
            <p className="text-[11.5px] text-md-on-surface-var/70 max-w-3xl mt-2">
              {emptyReason(data?.coverage, 'supply or customer')}
            </p>
          )}
        </div>
      )}

      {section === 'supply' && (
        <div>
          <Group title="Upstream" rows={v.upstream || []} ticker={perspective} onSelectTicker={onSelectTicker} nameOf={nameOf}
                 hint="if one of these breaks, this company feels it"
                 empty={emptyReason(data?.coverage, 'supplier')} />
          <Group title="Downstream" rows={v.downstream || []} ticker={perspective} onSelectTicker={onSelectTicker} nameOf={nameOf}
                 hint="if this company breaks, these feel it"
                 empty={emptyReason(data?.coverage, 'customer')} />
        </div>
      )}

      {section === 'competitors' && (
        <Group title="Competitors" rows={v.competitors || []} ticker={perspective}
               onSelectTicker={onSelectTicker} nameOf={nameOf}
               hint="every one of these is a company that named this one as a competitor in its own filing — not an industry-code guess"
               empty={emptyReason(data?.coverage, 'competitive')} />
      )}

      {section === 'ownership' && (
        <div>
          <Group title="Equity stakes" rows={v.ownership || []} ticker={perspective}
                 onSelectTicker={onSelectTicker} nameOf={nameOf} hint="who holds whom"
                 empty={emptyReason(data?.coverage, 'ownership')} />
          <Group title="Partners" rows={v.partners || []} ticker={perspective}
                 onSelectTicker={onSelectTicker} nameOf={nameOf} hint="joint development, licensing, co-selling"
                 empty={emptyReason(data?.coverage, 'partnership')} />
        </div>
      )}

      {section === 'all' && (
        <AllTable edges={data?.edges || []} ticker={perspective} peers={peers}
                  nameOf={nameOf} onSelectTicker={onSelectTicker} />
      )}

      {section === 'peers' && (
        <PeerTable data={peers} nameOf={nameOf} onSelectTicker={onSelectTicker} />
      )}

      {section === 'countries' && (
        <div>
          <h3 className="text-sm font-semibold text-md-on-surface mb-1">Where the counterparties sit</h3>
          <p className="text-[11.5px] text-md-on-surface-var mb-3 max-w-3xl">
            Counts, not percentages — and these are <em>headquarters</em> countries from the
            SEC filer index, which is not where manufacturing happens. A company can be
            American, listed in America, and unable to build anything if one fab stops.
            Manufacturing location is a separate claim and is only shown when a filing states it.
          </p>
          {Object.keys(risk?.countries_seen || {}).length === 0 ? (
            <p className="text-[12px] text-md-on-surface-var/70 italic">
              No counterparty countries resolved yet.
            </p>
          ) : (
            <div className="flex flex-col gap-1 max-w-md">
              {Object.entries(risk.countries_seen).sort((a, b) => b[1] - a[1]).map(([c, n]) => (
                <div key={c} className="flex items-center gap-2 text-[12.5px]">
                  <span className="w-40 text-md-on-surface">{c}</span>
                  <div className="flex-1 h-2 bg-white/5 rounded">
                    <div className="h-2 rounded bg-md-primary/50"
                         style={{ width: `${Math.min(100, n * 12)}%` }} />
                  </div>
                  <span className="w-8 text-right text-md-on-surface-var">{n}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {section === 'risk' && risk && (
        <div className="max-w-4xl">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
            {[['evidenced relationships', risk.evidenced_edges],
              ['upstream dependencies', risk.upstream_dependencies],
              ['mandated disclosures', risk.mandated_disclosures?.length || 0],
              ['unverified (model only)', risk.model_prior_edges]].map(([k, n]) => (
              <div key={k} className="px-2.5 py-2 rounded border border-md-outline-var/40 bg-md-surface-con/40">
                <div className="text-lg font-semibold text-md-on-surface">{n}</div>
                <div className="text-[11px] text-md-on-surface-var">{k}</div>
              </div>
            ))}
          </div>

          <h3 className="text-sm font-semibold text-md-on-surface mb-1">
            Mandated disclosures
          </h3>
          <p className="text-[11.5px] text-md-on-surface-var mb-2 max-w-3xl">
            The strongest rows on this page. A company must disclose a customer above 10% of
            revenue and quantify it, so these are the only relationships that arrive with a
            number attached and a legal obligation behind them.
          </p>
          {(risk.mandated_disclosures || []).length === 0
            ? <p className="text-[12px] text-md-on-surface-var/70 italic mb-4">None found yet.</p>
            : risk.mandated_disclosures.map((m, i) => (
                <div key={i} className="mb-2 p-2.5 rounded border border-emerald-700/40 bg-emerald-500/5">
                  <div className="flex items-center gap-2 flex-wrap text-[12.5px]">
                    <span className="font-mono font-semibold text-md-on-surface">{m.src}</span>
                    <span className="text-md-on-surface-var">{REL_LABEL[m.rel_type] || m.rel_type}</span>
                    <span className="font-mono font-semibold text-md-on-surface">{m.dst}</span>
                    {m.share_pct != null && (
                      <span className="font-semibold text-emerald-300">
                        {m.share_pct.toFixed(1)}%
                        <span className="ml-1 font-normal text-md-on-surface-var">
                          {m.share_basis || '(basis not recorded)'}
                        </span>
                      </span>
                    )}
                  </div>
                  {m.quote && (
                    <blockquote className="mt-1.5 text-[11.5px] italic text-md-on-surface-var
                                           border-l-2 border-emerald-700/50 pl-2">“{m.quote}”</blockquote>
                  )}
                  {m.source_url && (
                    <a href={m.source_url} target="_blank" rel="noreferrer"
                       className="text-[11px] text-sky-400 hover:underline">source filing ↗</a>
                  )}
                </div>
              ))}

          <h3 className="text-sm font-semibold text-md-on-surface mb-1 mt-4">Single-source components</h3>
          {(risk.single_source_components || []).length === 0
            ? <p className="text-[12px] text-md-on-surface-var/70 italic">
                None visible — which given the coverage above means "not found", not "none exist".
              </p>
            : risk.single_source_components.map((s, i) => (
                <div key={i} className="mb-1 px-2.5 py-1.5 rounded border border-rose-700/40
                                        bg-rose-500/5 text-[12.5px] text-md-on-surface">
                  <span className="text-rose-300 font-medium">{s.component}</span>
                  <span className="text-md-on-surface-var"> — one known source: </span>
                  <span className="font-mono">{s.sole_source}</span>
                </div>
              ))}

          <p className="text-[11px] text-md-on-surface-var/70 mt-4 max-w-3xl">
            {risk.no_percentages_note}
          </p>
        </div>
      )}

      {section === 'priors' && (
        <div>
          <div className="px-3 py-2 mb-3 rounded border border-dashed border-zinc-600
                          bg-zinc-900/40 text-[11.5px] text-zinc-300 max-w-3xl">
            Nothing here has a source. These are the model's guesses about what this company
            needs and who might provide it — useful as a list of things to go and look for,
            and not usable as findings. They are excluded from every count in Risk.
          </div>
          <Group title="Unverified" rows={v.model_priors || []} ticker={perspective}
                 onSelectTicker={onSelectTicker} nameOf={nameOf}
                 hint="hypotheses, not relationships"
                 empty="No hypotheses generated yet." />
        </div>
      )}
    </div>
  )
}
