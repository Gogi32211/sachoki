"""
prebreak_v3.py — simple additive pre-breakout score (0..50) + reasons.

A SEPARATE, heuristic column — does NOT touch the canonical turbo_score nor the
260515-Pine prebreak_score / prebreak_v2 (OOS) scores. Pure additive cluster of
accumulation/breakout signals, ported from the user's reference spec, with each
logical signal mapped to its real DB column (UI-name → DB-name gap handled).

  prebreak_v3          SMALLINT   0..50
  prebreak_v3_reasons  VARCHAR    pipe-separated tags (e.g. "ABCD|ULT×2|SVS|PhaseD")

Computed in one vectorised SQL UPDATE. Called by incremental_delta after enrich
(like apply_prebreak_v2), and runnable standalone:
  python -m prebreak_v3            # whole DB
  python -m prebreak_v3 nasdaq     # one universe
"""
from __future__ import annotations
import sys, time, logging

log = logging.getLogger(__name__)

# Logical signal → real DB column (the reference used UI-key names).
#   abs_sig→sig_abs · bc→sig_bc · svs_2809→svs · conso_2809→sig_conso
#   wick_bull→sig_wk_up · load_sig→load
_ULT = "((coalesce(sig_260308,0)=1)::int + (coalesce(bf_buy,0)=1)::int + " \
       "(coalesce(fbo_bull,0)=1)::int + (coalesce(eb_bull,0)=1)::int + " \
       "(coalesce(ultra_3up,0)=1)::int)"

_SCORE_SQL = f"""
    (CASE WHEN fly_abcd=1 THEN 12 WHEN fly_cd=1 THEN 6 ELSE 0 END
   + CASE WHEN {_ULT}>=2 THEN 10 WHEN {_ULT}=1 THEN 4 ELSE 0 END
   + CASE WHEN sig_abs=1 AND sig_bc=1 THEN 8 WHEN sig_abs=1 THEN 4 WHEN sig_bc=1 THEN 3 ELSE 0 END
   + CASE WHEN svs=1 THEN 5 ELSE 0 END
   + CASE WHEN sig_conso=1 THEN 5 ELSE 0 END
   + CASE WHEN rtb_phase='D' THEN 6 WHEN rtb_phase='C' THEN 3 ELSE 0 END
   + CASE WHEN sig_wk_up=1 THEN 3 ELSE 0 END
   + CASE WHEN load=1 THEN 4 ELSE 0 END
   + CASE WHEN sq=1 THEN 4 ELSE 0 END)
"""

_REASONS_SQL = f"""
  concat_ws('|',
    CASE WHEN fly_abcd=1 THEN 'ABCD' WHEN fly_cd=1 THEN 'CD' END,
    CASE WHEN {_ULT}>=2 THEN 'ULT×'||CAST({_ULT} AS VARCHAR) WHEN {_ULT}=1 THEN 'ULT×1' END,
    CASE WHEN sig_abs=1 AND sig_bc=1 THEN 'ABS+BC' WHEN sig_abs=1 THEN 'ABS' WHEN sig_bc=1 THEN 'BC' END,
    CASE WHEN svs=1 THEN 'SVS' END,
    CASE WHEN sig_conso=1 THEN 'CONSO' END,
    CASE WHEN rtb_phase='D' THEN 'PhaseD' WHEN rtb_phase='C' THEN 'PhaseC' END,
    CASE WHEN sig_wk_up=1 THEN 'WICK' END,
    CASE WHEN load=1 THEN 'LOAD' END,
    CASE WHEN sq=1 THEN 'SQ' END)
"""


def apply_prebreak_v3(universe: str | None = None) -> dict:
    """Recompute prebreak_v3 + prebreak_v3_reasons. One vectorised SQL UPDATE.
    `universe` limits to one universe (used by the incremental refresh); None = all."""
    from studio.db import get_conn, ensure_schema
    ensure_schema()
    where = f"WHERE universe = '{universe}'" if universe else ""
    t0 = time.time()
    conn = get_conn(read_only=False)
    try:
        conn.execute(f"""
            UPDATE bars
            SET prebreak_v3 = LEAST({_SCORE_SQL}, 50),
                prebreak_v3_reasons = {_REASONS_SQL}
            {where}
        """)
        conn.commit()
        n = conn.execute(f"SELECT count(*) FROM bars {where}").fetchone()[0]
    finally:
        conn.close()
    dur = time.time() - t0
    log.info("prebreak_v3 applied to %s rows (%s) in %.1fs", n, universe or "all", dur)
    return {"rows": n, "universe": universe or "all", "duration_sec": round(dur, 1)}


if __name__ == "__main__":
    unis = [a.lower() for a in sys.argv[1:]] or [None]
    for u in unis:
        print(apply_prebreak_v3(u), flush=True)
