"""
ai_journal/bootstrap.py — seed the Tier-1 knowledge base (signal_outcomes) from
the historical analytics DB, so the agent starts day-1 with REAL priors
(e.g. V×20 ≈ 12.8× big-move lift, structural HH edge) instead of learning from
zero. Pure code, no LLM.

For each predicate we compute, over the full history (fwd_5d-clipped to kill the
inf/penny outliers), the forward distribution + HH-continuation rate and the
lift vs the universe baseline — the same honest methodology we validated by hand.

Run:  python -m ai_journal.bootstrap
"""
from __future__ import annotations

import sys
import time
import logging

from .db import get_analytics_conn, get_journal_conn, ensure_schema

log = logging.getLogger(__name__)

# ── Predicate catalogue (name, category, SQL condition over `bars`) ──────────
# These mirror the V3 components + base signals + a few validated combos. Each
# becomes one row in signal_outcomes — the agent's queryable knowledge base.
_ULT = ("((coalesce(sig_260308,0)=1)::int + (coalesce(bf_buy,0)=1)::int + "
        "(coalesce(fbo_bull,0)=1)::int + (coalesce(eb_bull,0)=1)::int + "
        "(coalesce(ultra_3up,0)=1)::int)")

PREDICATES: list[tuple[str, str, str]] = [
    # volume (expected strongest big-move precursor)
    ("vol_20x",      "volume", "sig_vol_20x=1"),
    ("vol_10x",      "volume", "sig_vol_10x=1"),
    ("vol_5x",       "volume", "sig_vol_5x=1"),
    # fly / ABCD
    ("fly_abcd",     "fly",    "fly_abcd=1"),
    ("fly_cd",       "fly",    "fly_cd=1"),
    # absorption / breakout cluster
    ("abs_and_bc",   "absorb", "sig_abs=1 AND sig_bc=1"),
    ("abs",          "absorb", "sig_abs=1"),
    ("bc",           "absorb", "sig_bc=1"),
    ("svs",          "absorb", "svs=1"),
    ("conso",        "structure", "sig_conso=1"),
    # RTB phase
    ("phase_D",      "phase",  "rtb_phase='D'"),
    ("phase_C",      "phase",  "rtb_phase='C'"),
    # wick / load / squeeze / cci
    ("wick_up",      "candle", "sig_wk_up=1"),
    ("load",         "structure", "load=1"),
    ("squeeze",      "structure", "sq=1"),
    ("cci0r",        "momentum", "sig_cci0r=1"),
    # ult cluster
    ("ult_ge2",      "ultra",  f"{_ULT}>=2"),
    ("ult_ge1",      "ultra",  f"{_ULT}>=1"),
    # preup / L-codes / vabs / wyckoff (V3 structural additions)
    ("preup_p3",     "preup",  "sig_p3=1"),
    ("preup_p2",     "preup",  "sig_p2=1"),
    ("preup_p89",    "preup",  "sig_p89=1"),
    ("preup_p50",    "preup",  "sig_p50=1"),
    ("bx_up",        "lcode",  "bx_up=1"),
    ("fri34",        "lcode",  "sig_fri34=1"),
    ("blue",         "lcode",  "sig_blue=1"),
    ("l34",          "lcode",  "l34=1"),
    ("clm",          "vabs",   "sig_clm=1"),
    ("best",         "vabs",   "sig_best=1"),
    ("wt_lps",       "wyckoff","wt_lps=1"),
    ("lvbo",         "breakout","pb_lvbo=1"),
    # ── validated COMBOS (where the real edge lives) ──
    ("vol20_and_v3hi",   "combo", "sig_vol_20x=1 AND prebreak_v3>=25"),
    ("vol_any_and_phaseD","combo","(sig_vol_20x=1 OR sig_vol_10x=1) AND rtb_phase='D'"),
    ("abs_bc_and_conso", "combo", "sig_abs=1 AND sig_bc=1 AND sig_conso=1"),
    ("v3_ge30",          "combo", "prebreak_v3>=30"),
    ("v3_ge40",          "combo", "prebreak_v3>=40"),
]

# fwd analysis population: non-null, clipped to kill inf/penny outliers.
_POP = "fwd_5d IS NOT NULL AND fwd_5d BETWEEN -90 AND 500"


def _build_sql() -> str:
    sel = [
        "count(*) AS base_n",
        "median(fwd_5d) AS base_fwd5",
        "avg(CASE WHEN fwd_5d>0 THEN 1.0 ELSE 0 END) AS base_win5",
        "avg(CASE WHEN fwd_5d>=5 THEN 1.0 ELSE 0 END) AS base_big5",
        "avg(CASE WHEN next_pivot_is_hh_5 THEN 1.0 ELSE 0 END) AS base_hh5",
    ]
    for name, _cat, cond in PREDICATES:
        f = f"FILTER (WHERE {cond})"
        sel += [
            f"count(*) {f} AS {name}__n",
            f"median(fwd_3d) {f} AS {name}__fwd3",
            f"median(fwd_5d) {f} AS {name}__fwd5",
            f"median(fwd_10d) {f} AS {name}__fwd10",
            f"avg(CASE WHEN fwd_5d>0 THEN 1.0 ELSE 0 END) {f} AS {name}__win5",
            f"avg(CASE WHEN fwd_5d>=5 THEN 1.0 ELSE 0 END) {f} AS {name}__big5",
            f"avg(CASE WHEN next_pivot_is_hh_5 THEN 1.0 ELSE 0 END) {f} AS {name}__hh5",
        ]
    return f"SELECT {', '.join(sel)} FROM bars WHERE {_POP}"


def seed_signal_outcomes() -> dict:
    """One-pass conditional-aggregate scan → write Tier-1 rows. Returns summary."""
    ensure_schema()
    t0 = time.time()

    a = get_analytics_conn()
    try:
        as_of = a.execute("SELECT max(date) FROM bars").fetchone()[0]
        row = a.execute(_build_sql()).fetchdf().iloc[0]
    finally:
        a.close()

    base_n   = int(row["base_n"])
    base_win = float(row["base_win5"])
    base_big = float(row["base_big5"])
    base_hh  = float(row["base_hh5"])

    out_rows = []
    for name, cat, cond in PREDICATES:
        n = int(row[f"{name}__n"] or 0)
        if n == 0:
            continue
        big5 = float(row[f"{name}__big5"] or 0)
        hh5  = float(row[f"{name}__hh5"] or 0)
        out_rows.append((
            name, cond, cat, as_of, n, n / base_n * 100.0,
            _f(row[f"{name}__fwd3"]), _f(row[f"{name}__fwd5"]), _f(row[f"{name}__fwd10"]),
            float(row[f"{name}__win5"] or 0), big5, hh5,
            base_win, base_big, base_hh,
            (big5 / base_big) if base_big else 0.0,
            ((hh5 / base_hh) if base_hh else 0.0),
            (hh5 - base_hh) * 100.0,
        ))

    j = get_journal_conn(read_only=False)
    try:
        j.execute("DELETE FROM signal_outcomes WHERE as_of_date = ?", [as_of])
        j.executemany(
            """INSERT INTO signal_outcomes
               (predicate, predicate_sql, category, as_of_date, n, rate_pct,
                fwd3_med, fwd5_med, fwd10_med, win5, big5, hh5,
                base_win5, base_big5, base_hh5, lift_big5, lift_hh5, hh5_edge_pp,
                updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, current_timestamp)""",
            out_rows,
        )
        j.commit()
    finally:
        j.close()

    dur = time.time() - t0
    log.info("signal_outcomes seeded: %d predicates, base_n=%d, as_of=%s in %.1fs",
             len(out_rows), base_n, as_of, dur)
    return {"predicates": len(out_rows), "base_n": base_n, "as_of": str(as_of),
            "base_big5": base_big, "base_hh5": base_hh, "duration_sec": round(dur, 1)}


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    res = seed_signal_outcomes()
    print(res)
    # Show the knowledge base, ranked by big-move lift — sanity check vs our findings.
    j = get_journal_conn(read_only=True)
    try:
        df = j.execute("""
            SELECT predicate, category, n, round(rate_pct,1) rate, round(fwd5_med,2) fwd5,
                   round(win5*100,1) win5, round(big5*100,1) big5, round(lift_big5,1) liftBig,
                   round(hh5*100,1) hh5, round(hh5_edge_pp,1) hhEdge
            FROM signal_outcomes ORDER BY lift_big5 DESC
        """).fetchdf()
    finally:
        j.close()
    import pandas as pd
    pd.set_option("display.width", 200); pd.set_option("display.max_rows", 100)
    print("\n=== Tier-1 knowledge base (ranked by big-move lift vs base) ===")
    print(df.to_string(index=False))
