"""
prebreak_v3.py — simple additive pre-breakout score (0..50) + reasons.

A SEPARATE, heuristic column — does NOT touch the canonical turbo_score nor the
260515-Pine prebreak_score / prebreak_v2 (OOS) scores. Pure additive cluster of
accumulation/breakout signals, ported from the user's reference spec, with each
logical signal mapped to its real DB column (UI-name → DB-name gap handled).

  prebreak_v3          SMALLINT   0..50
  prebreak_v3_reasons  VARCHAR    pipe-separated tags (e.g. "ABCD|ULT×2|SVS|WICK")

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
   -- RTB phase weight REMOVED (2026-07-09): path-sim showed rtb_phase='D' (already
   -- broke out) is the WORST performer (med −1.66) and phases rank BACKWARDS vs
   -- their design (A/B > C > D); rtb_total is monotonically anti-predictive. Adding
   -- +6 for D actively hurt this pre-breakout score. The only RTB signal that pays
   -- is EARLY phase (A/B) + oversold — served separately as the RTB-Base edge.
   + CASE WHEN sig_wk_up=1 THEN 3 ELSE 0 END
   + CASE WHEN load=1 THEN 4 ELSE 0 END
   + CASE WHEN sq=1 THEN 4 ELSE 0 END
   + CASE WHEN sig_vol_20x=1 THEN 10 WHEN sig_vol_10x=1 THEN 7 WHEN sig_vol_5x=1 THEN 4 ELSE 0 END
   + CASE WHEN sig_cci0r=1 THEN 3 ELSE 0 END
   -- Superchart-sync structural cluster (validated on HH continuation; see prebreak_v3 docstring)
   + CASE WHEN sig_p3=1 THEN 4 WHEN (sig_p2=1 OR sig_p89=1) THEN 3 WHEN sig_p50=1 THEN 2 ELSE 0 END
   + CASE WHEN bx_up=1 THEN 4 ELSE 0 END
   + CASE WHEN sig_clm=1 THEN 4 ELSE 0 END
   + CASE WHEN sig_fri34=1 THEN 3 ELSE 0 END
   + CASE WHEN sig_best=1 THEN 3 ELSE 0 END
   + CASE WHEN sig_blue=1 THEN 2 ELSE 0 END
   + CASE WHEN l34=1 THEN 2 ELSE 0 END
   + CASE WHEN wt_lps=1 THEN 2 ELSE 0 END)
"""

_REASONS_SQL = f"""
  concat_ws('|',
    CASE WHEN fly_abcd=1 THEN 'ABCD' WHEN fly_cd=1 THEN 'CD' END,
    CASE WHEN {_ULT}>=2 THEN 'ULT×'||CAST({_ULT} AS VARCHAR) WHEN {_ULT}=1 THEN 'ULT×1' END,
    CASE WHEN sig_abs=1 AND sig_bc=1 THEN 'ABS+BC' WHEN sig_abs=1 THEN 'ABS' WHEN sig_bc=1 THEN 'BC' END,
    CASE WHEN svs=1 THEN 'SVS' END,
    CASE WHEN sig_conso=1 THEN 'CONSO' END,
    -- RTB phase reason tag removed with its weight (see _SCORE_SQL note)
    CASE WHEN sig_wk_up=1 THEN 'WICK' END,
    CASE WHEN load=1 THEN 'LOAD' END,
    CASE WHEN sq=1 THEN 'SQ' END,
    CASE WHEN sig_vol_20x=1 THEN 'V×20' WHEN sig_vol_10x=1 THEN 'V×10' WHEN sig_vol_5x=1 THEN 'V×5' END,
    CASE WHEN sig_cci0r=1 THEN 'CCI0R' END,
    CASE WHEN sig_p3=1 THEN 'P3' WHEN sig_p2=1 THEN 'P2' WHEN sig_p89=1 THEN 'P89' WHEN sig_p50=1 THEN 'P50' END,
    CASE WHEN bx_up=1 THEN 'BX↑' END,
    CASE WHEN sig_clm=1 THEN 'CLM' END,
    CASE WHEN sig_fri34=1 THEN 'FRI34' END,
    CASE WHEN sig_best=1 THEN 'BEST★' END,
    CASE WHEN sig_blue=1 THEN 'BL' END,
    CASE WHEN l34=1 THEN 'L34' END,
    CASE WHEN wt_lps=1 THEN 'tLPS' END)
"""


def calc_prebreak_v3(d) -> tuple[int, str]:
    """Per-bar prebreak_v3 from a Superchart bar dict (LIVE path) — mirrors the
    SQL exactly. ULT cluster is read from the 'ultra' display-tag list; load/sq
    use the raw_* keys present in api_bar_signals output."""
    g = lambda k: 1 if (d.get(k) in (1, True, "1")) else 0
    s = 0; r = []
    if g("sig_fly_abcd"):   s += 12; r.append("ABCD")
    elif g("sig_fly_cd"):   s += 6;  r.append("CD")
    _ult_tags = {"260308", "4BF", "FBO↑", "EB↑", "3↑"}
    ult = sum(1 for x in (d.get("ultra") or []) if x in _ult_tags)
    if ult >= 2:  s += 10; r.append(f"ULT×{ult}")
    elif ult == 1: s += 4; r.append("ULT×1")
    ha, hb = g("sig_abs"), g("sig_bc")
    if ha and hb: s += 8; r.append("ABS+BC")
    elif ha:      s += 4; r.append("ABS")
    elif hb:      s += 3; r.append("BC")
    # SVS +5 REMOVED 2026-07-29 — measured empty (median −0.69 vs −0.63 baseline, n=79,977).
    # CONSO below is KEPT: it survived its control (CONSO +0.03 vs NOT-CONSO −3.67) — though
    # note that was measured as a gate on reversal setups, not against prebreak_v3's own
    # target, so the +5 here is retained on the old basis, not on new evidence.
    if g("sig_conso"):           s += 5; r.append("CONSO")
    # RTB phase weight removed 2026-07-09 (anti-predictive — see _SCORE_SQL note)
    if g("sig_wk_up"):            s += 3; r.append("WICK")
    if g("raw_load") or g("load"): s += 4; r.append("LOAD")
    if g("raw_sq") or g("sq"):     s += 4; r.append("SQ")
    if g("sig_vol_20x"):   s += 10; r.append("V×20")
    elif g("sig_vol_10x"): s += 7;  r.append("V×10")
    elif g("sig_vol_5x"):  s += 4;  r.append("V×5")
    # CCI0R +3 REMOVED 2026-07-28 (path-sim: indistinguishable from baseline, −0.66 vs −0.69).
    # Still surfaced as a REASON tag so the chart/CSV keep showing it, just with no score effect.
    if g("sig_cci0r"):     r.append("CCI0R")
    # Superchart-sync structural cluster — read from display-tag arrays
    # (combo: PREUP P2/P3/P50/P89 · l: L34/FRI34/BL/BX↑ · vabs: CLM/BEST★ · wyck: tLPS)
    combo = set(d.get("combo") or [])
    lset  = set(d.get("l") or [])
    vabs  = set(d.get("vabs") or [])
    wyck  = set(d.get("wyck") or [])
    if   "P3"  in combo: s += 4; r.append("P3")
    elif "P2"  in combo: s += 3; r.append("P2")
    elif "P89" in combo: s += 3; r.append("P89")
    elif "P50" in combo: s += 2; r.append("P50")
    if "BX↑"  in lset: s += 4; r.append("BX↑")
    if "CLM"  in vabs: s += 4; r.append("CLM")
    if "FRI34" in lset: s += 3; r.append("FRI34")
    if "BEST★" in vabs: s += 3; r.append("BEST★")
    if "BL"   in lset: s += 2; r.append("BL")
    if "L34"  in lset: s += 2; r.append("L34")
    if "tLPS" in wyck: s += 2; r.append("tLPS")
    return min(s, 50), "|".join(r)


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
