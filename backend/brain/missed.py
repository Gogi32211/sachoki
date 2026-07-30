"""brain/missed.py — the daily "why did we miss it" review. Layer 0 of self-directed thinking.

Every day: take the top-N gainers, reconstruct what the system saw BEFORE the move, and
classify why each one was not bought. Accumulate. Never conclude from one day.

═══════════════════════════════════════════════════════════════════════════════════════
THE CONSTRAINT THIS MODULE EXISTS UNDER — and it comes from the brain's own registry,
not from a preference. Queried 2026-07-30, 17 findings bear on this task; three decide it:

  law_single_name_conviction_is_survivorship
      "never accept an idea whose evidence is a chart you were shown"
  method_complement_control
      "a filter must be scored against its own complement, not the baseline"
  method_dsr_before_built
      "count the trials HONESTLY: everything examined while choosing"

Top gainers are a sample SELECTED ON THE OUTCOME. Any state found inside them looks
predictive, because the counterfactual — how many stocks in the SAME state did NOT rise —
is missing by construction. Every null this project proved (harmonic patterns, Zanger
flags, BB↑, fractal shape-matching) would look brilliant measured only on winners.

  ⇒ THIS MODULE MAY NOT WRITE FINDINGS. It writes QUESTIONS.
    There is deliberately no `registry.record` import here, and there must not be one.
    A question becomes a finding only after a universe-wide test with its base rate,
    price buckets, per-year split and DSR — in a separate script, by hand.
═══════════════════════════════════════════════════════════════════════════════════════

What this module CAN conclude on its own, because these measure OUR SYSTEM and not the
market — no base rate is needed to count our own behaviour:
  · coverage      — what share of large moves our universe can even reach
  · attribution   — which gate rejects the most eventual gainers (a trigger to re-measure,
                    never a verdict; law_suppressors_beat_signals says this is the most
                    valuable output of the loop)
  · capital       — how often we were right and had no room

Isolated: reads the DuckDB read-only + the warm edge_replay frame; writes ONLY
brain/missed_log.json and brain/hypotheses.json.
"""
from __future__ import annotations
import json
import os
from datetime import datetime

LOG = os.path.join(os.path.dirname(__file__), "missed_log.json")
HYP = os.path.join(os.path.dirname(__file__), "hypotheses.json")

TOP_N = 30
# A "miss" only counts if the move is big enough to have been worth catching. Below this a
# name is noise, not a lesson.
MIN_MOVE = 0.10

BUCKETS = (
    "bought",            # we held it — not a miss
    "out_of_universe",   # below the price / liquidity floor: a POLICY choice, not a failure
    "no_edge_fired",     # reachable, but no registry edge fired → detection gap
    "gate_veto",         # an edge fired and L4 rejected it → attribution to a named gate
    "capital",           # passed the gates, no portfolio room → capital constraint
    "unforeseeable",     # gapped on news with no prior state to read
)


def _load(path: str, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def _save(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1)
    os.replace(tmp, path)


# ── 1. OBSERVE ─────────────────────────────────────────────────────────────────────────
def top_gainers(date: str | None = None, n: int = TOP_N) -> list[dict]:
    """Top-n 1D gainers for `date` (default: the latest bar in the DB)."""
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        if not date:
            date = str(a.execute("SELECT max(date) FROM bars WHERE universe<>'index'").fetchone()[0])[:10]
        rows = a.execute(f"""
            WITH r AS (
              SELECT ticker, universe, date, close, volume, avg_vol_20d,
                     lag(close) OVER (PARTITION BY ticker ORDER BY date) prev
              FROM bars
              WHERE universe <> 'index' AND date >= DATE '{date}' - INTERVAL 10 DAY
                AND date <= DATE '{date}'
            )
            SELECT ticker, any_value(universe) universe, close, prev,
                   close/NULLIF(prev,0) - 1 AS pct, close*volume AS dv
            FROM r WHERE date = DATE '{date}' AND prev > 0
            GROUP BY ticker, close, prev, volume
            ORDER BY pct DESC LIMIT {int(n)}
        """).fetchdf()
    finally:
        a.close()
    return [{"ticker": str(r.ticker), "universe": str(r.universe), "pct": float(r.pct),
             "close": float(r.close), "prev": float(r.prev), "dv": float(r.dv)}
            for r in rows.itertuples()], date


# ── 2. CLASSIFY ────────────────────────────────────────────────────────────────────────
def classify(gainers: list[dict], date: str, lookback: int = 5) -> list[dict]:
    """Why was each name not bought? Reconstructs what the system saw BEFORE the move.

    `lookback`: how many bars before the move an edge fire still counts as "we saw it".
    """
    import numpy as np
    import edge_replay as er
    from . import journal, registry, gates

    try:
        grp, _ = er._frame(60, 3_000_000)
    except Exception:
        grp = {}
    held = {p["ticker"] for p in journal.open_positions()}
    reg_cols = {e["col"] for e in registry.live_edges(direction="long") if e.get("col")}

    out = []
    for gr in gainers:
        tk = gr["ticker"]
        rec = dict(gr)
        if tk in held:
            rec["bucket"] = "bought"; out.append(rec); continue
        if abs(gr["pct"]) < MIN_MOVE:
            rec["bucket"] = "below_threshold"; out.append(rec); continue

        g = grp.get(tk)
        if g is None or len(g) < 60:
            # Two different lessons, so they must not share a bucket. The frame filters
            # per-ROW (close>=5 AND close*volume>=3M), so a name can pass TODAY and still
            # have almost no qualifying history — DFNS 2026-07-29 was $50 on $1.0B and had
            # 366 bars in five years. Calling that "under the floor" was simply false.
            if gr["close"] < 5 or gr["dv"] < 3_000_000:
                rec["bucket"] = "out_of_universe"
                rec["detail"] = (f"price ${gr['close']:.2f}, dv ${gr['dv']/1e6:.1f}M — "
                                 f"under the $5 / $3M floor. A policy choice, not a miss.")
            else:
                rec["bucket"] = "thin_history"
                rec["detail"] = (f"passes the floor today (${gr['close']:.2f}, "
                                 f"${gr['dv']/1e6:.0f}M) but has too few qualifying bars — "
                                 f"illiquid past or a recent listing")
            out.append(rec); continue

        # bars strictly BEFORE the move — never read the move's own bar (that is lookahead)
        dts = g["date"].astype(str).str[:10].to_numpy()
        idx = np.nonzero(dts == date)[0]
        i = int(idx[0]) if len(idx) else len(g) - 1
        lo = max(0, i - lookback)
        window = slice(lo, i)                     # exclusive of i

        fired = [c for c in reg_cols
                 if c in g.columns and bool(np.asarray(g[c].to_numpy(bool)[window]).any())]
        if not fired:
            rec["bucket"] = "no_edge_fired"
            rec["detail"] = f"no registry edge in the {lookback} bars before"
            out.append(rec); continue

        rec["fired"] = fired
        # what would L4 have said on the last bar before the move?
        j = max(0, i - 1)
        st = {}
        try:
            st["close"] = float(g["close"].iloc[j])
            st["month"] = int(dts[j][5:7])
            c = g["close"].iloc[:j + 1]
            for span, key in ((9, "e9"), (20, "e20"), (50, "e50"), (200, "e200")):
                if len(c) >= span:
                    st[key] = float(c.ewm(span=span, adjust=False).mean().iloc[-1])
            for col, key in (("rs_intact", "rs_intact"), ("conso", "conso"),
                             ("iv_dry", "no_vol_event"), ("supp", "vol_extreme")):
                if col in g.columns:
                    v = g[col].iloc[j]
                    if v == v:
                        st[key] = bool(v)
            h = g["hurst"].iloc[j] if "hurst" in g.columns else None
            if h is not None and h == h:
                st["hurst"] = float(h)
        except Exception:
            pass
        gres = gates.evaluate(st, edge_id="")
        if gres["veto"]:
            rec["bucket"] = "gate_veto"
            rec["gate"] = gres["reason"].split("—")[0].strip()
            rec["detail"] = gres["reason"][:160]
        else:
            # it passed detection AND the gates: the miss is downstream — capital, ranking,
            # or the fire being older than live.py's max_age. Recorded as one bucket because
            # separating them needs the book state as it was on the day, which we do not keep.
            rec["bucket"] = "capital"
            rec["detail"] = f"passed gates (×{gres['mult']}) — not taken: rank/room/age"
        out.append(rec)
    return out


# ── 3. ACCUMULATE ──────────────────────────────────────────────────────────────────────
def review(date: str | None = None, n: int = TOP_N) -> dict:
    """One day's review. Appends to the ledger. Concludes NOTHING on its own."""
    gainers, date = top_gainers(date, n)
    rows = classify(gainers, date)
    counts: dict = {}
    for r in rows:
        counts[r["bucket"]] = counts.get(r["bucket"], 0) + 1
    gate_hits: dict = {}
    for r in rows:
        if r["bucket"] == "gate_veto":
            gate_hits[r.get("gate", "?")] = gate_hits.get(r.get("gate", "?"), 0) + 1

    day = {"date": date, "n": len(rows), "counts": counts, "gate_hits": gate_hits,
           "median_move": round(sorted(r["pct"] for r in rows)[len(rows) // 2] * 100, 1) if rows else 0,
           "names": [{k: r.get(k) for k in ("ticker", "pct", "bucket", "gate", "detail", "fired")}
                     for r in rows],
           "reviewed_at": datetime.now().strftime("%Y-%m-%d %H:%M")}

    log = _load(LOG, {"days": []})
    log["days"] = [d for d in log.get("days", []) if d.get("date") != date] + [day]
    log["days"] = sorted(log["days"], key=lambda d: d["date"])[-400:]
    _save(LOG, log)
    return day


# ── 4. HYPOTHESIZE — questions only, never findings ────────────────────────────────────
# Thresholds are deliberately high. method_frequency_first: "count occurrences before
# believing a formation". One day is an anecdote; a quarter of days is a pattern worth the
# cost of a universe-wide test.
MIN_DAYS = 20          # never ask anything until the ledger has this many days
GATE_ASK = 15          # a single gate rejecting this many eventual gainers → question
COVERAGE_ASK = 0.40    # this share out of universe → question about the floor


def hypotheses() -> list[dict]:
    """Scan the ledger and emit RESEARCH QUESTIONS. Each carries the test that would
    answer it — including the base rate, without which the answer is survivorship."""
    log = _load(LOG, {"days": []})
    days = log.get("days", [])
    if len(days) < MIN_DAYS:
        return [{"id": "warmup", "status": "waiting",
                 "question": f"ledger has {len(days)}/{MIN_DAYS} days — too few to ask anything",
                 "test": "keep accumulating"}]

    tot = sum(d["n"] for d in days) or 1
    agg: dict = {}
    gates_agg: dict = {}
    for d in days:
        for k, v in d["counts"].items():
            agg[k] = agg.get(k, 0) + v
        for k, v in d.get("gate_hits", {}).items():
            gates_agg[k] = gates_agg.get(k, 0) + v

    qs = []
    for gate, cnt in sorted(gates_agg.items(), key=lambda x: -x[1]):
        if cnt >= GATE_ASK:
            qs.append({
                "id": f"gate_cost::{gate}",
                "status": "open",
                "question": (f"{gate} vetoed {cnt} names that then became top-{TOP_N} gainers "
                             f"over {len(days)} days. Is the gate paying for itself?"),
                "test": ("Universe-wide, NOT on the gainers: path-sim every bar the gate "
                         "vetoes vs every bar it passes, over 6 years, split by price bucket "
                         "and year. The gate is justified only if the vetoed population is "
                         "worse OVERALL — being wrong on a visible winner is the expected "
                         "cost of a filter with a positive base rate, not evidence against it."),
                "trap": ("These 15+ names are selected ON THE OUTCOME. Counting them alone "
                         "proves nothing — the complement is the whole point "
                         "(method_complement_control)."),
                "evidence": {"vetoed_gainers": cnt, "days": len(days)},
            })

    oou = agg.get("out_of_universe", 0) / tot
    if oou >= COVERAGE_ASK:
        qs.append({
            "id": "coverage::universe_floor",
            "status": "open",
            "question": (f"{oou:.0%} of top-{TOP_N} gainers sit below the $5 / $3M frame floor. "
                         f"Is the floor costing more than it protects?"),
            "test": ("Re-run the core edge book with the floor lowered, 6 years, per-year and "
                     "per-price-bucket. law_fib_price_zones says sub-$8 is a lottery with a "
                     "NEGATIVE median, so the expected answer is that the floor is correct and "
                     "these names are unreachable BY DESIGN — but it is testable, so test it."),
            "trap": "The cheap names that did NOT move are invisible in this sample.",
            "evidence": {"share_out_of_universe": round(oou, 3), "days": len(days)},
        })

    nef = agg.get("no_edge_fired", 0) / tot
    if nef >= 0.50:
        qs.append({
            "id": "coverage::detection_gap",
            "status": "open",
            "question": (f"{nef:.0%} of reachable gainers had NO registry edge fire in the 5 bars "
                         f"before. Is there a state the book does not describe?"),
            "test": ("Cluster the pre-move state of the no-fire names, then measure each cluster "
                     "universe-wide with its base rate, price buckets, per-year and DSR. Expect "
                     "most clusters to be null: law_published_geometry_is_empty."),
            "trap": ("This is the single most survivorship-prone question in the loop. A state "
                     "common to 50 winners is worthless until you know how many losers shared it."),
            "evidence": {"share_no_edge": round(nef, 3), "days": len(days)},
        })

    _save(HYP, {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "days_in_ledger": len(days), "questions": qs})
    return qs


def summary(days: int = 30) -> dict:
    """Aggregate the ledger — the part that needs no base rate because it counts US."""
    log = _load(LOG, {"days": []})
    ds = log.get("days", [])[-days:]
    tot = sum(d["n"] for d in ds) or 1
    agg: dict = {}
    gates_agg: dict = {}
    for d in ds:
        for k, v in d["counts"].items():
            agg[k] = agg.get(k, 0) + v
        for k, v in d.get("gate_hits", {}).items():
            gates_agg[k] = gates_agg.get(k, 0) + v
    return {"days": len(ds), "observations": tot,
            "buckets": {k: {"n": v, "share": round(v / tot, 3)}
                        for k, v in sorted(agg.items(), key=lambda x: -x[1])},
            "gate_attribution": dict(sorted(gates_agg.items(), key=lambda x: -x[1]))}


if __name__ == "__main__":
    d = review()
    print(json.dumps({k: d[k] for k in ("date", "n", "counts", "gate_hits", "median_move")}, indent=1))
    print("\nnames:")
    for r in d["names"][:12]:
        print("  %-7s %+6.1f%%  %-16s %s" % (r["ticker"], r["pct"] * 100, r["bucket"],
                                             (r.get("detail") or "")[:70]))
    print("\nhypotheses:", json.dumps(hypotheses(), indent=1)[:600])
