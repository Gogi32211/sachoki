"""
prebreak_v4.py — data-driven P&L score (replaces V3's HH-based heuristic weights).

Where V3 weights were heuristic and aligned to HH-continuation (Combo Lab proved
HH-edge does NOT pay), V4 reads weights from signal_outcomes_pnl (H=10) and
combo_catalog_pnl (greedy beam, OOS-passed). So:
  - Atom weight  = round(edge_avg_clip × 100)         only positive edges count
  - Combo bonus  = round(oos_edge_avg × 50)           per matched validated combo

Stored columns:
  prebreak_v4         SMALLINT     0..~80
  prebreak_v4_reasons VARCHAR      "blue+13|squeeze+9|combo:clm+squeeze+7|..."

The weight table is built dynamically each apply, so reseeding pnl_metric or
re-running greedy automatically updates V4. No hardcoded numbers ⇒ no drift
between "code says X" and "DB says Y".

Run:  python -m prebreak_v4                 # whole DB
      python -m prebreak_v4 sp500           # one universe
"""
from __future__ import annotations

import sys
import time
import logging

log = logging.getLogger(__name__)

# Atom predicate name -> the existing DB column condition (mirrors
# ai_journal.bootstrap.PREDICATES). We import to keep one source of truth.
def _atom_sqls() -> dict[str, str]:
    from ai_journal.bootstrap import PREDICATES
    return {name: sql for name, _cat, sql in PREDICATES}


# Reasons-tag (UI label) per atom — for the v4_reasons string.
ATOM_TAG = {
    "vol_20x": "V×20", "vol_10x": "V×10", "vol_5x": "V×5",
    "fly_abcd": "ABCD", "fly_cd": "CD",
    "abs_and_bc": "ABS+BC", "abs": "ABS", "bc": "BC",
    "svs": "SVS", "conso": "CONSO",
    "phase_D": "PhaseD", "phase_C": "PhaseC",
    "wick_up": "WICK", "load": "LOAD", "squeeze": "SQ", "cci0r": "CCI0R",
    "ult_ge2": "ULT×2", "ult_ge1": "ULT×1",
    "preup_p3": "P3", "preup_p2": "P2", "preup_p89": "P89", "preup_p50": "P50",
    "bx_up": "BX↑", "fri34": "FRI34", "blue": "BL", "l34": "L34",
    "clm": "CLM", "best": "BEST★", "wt_lps": "tLPS", "lvbo": "LVBO",
    "vol20_and_v3hi": "V20+V3hi", "vol_any_and_phaseD": "V+phD",
    "abs_bc_and_conso": "AbsBcCons", "v3_ge30": "V3≥30", "v3_ge40": "V3≥40",
}


def load_weights(horizon: int = 10) -> tuple[dict[str, int], list[dict]]:
    """Returns ({atom: integer_weight}, [combo dicts with bonus])."""
    from ai_journal.db import get_journal_conn
    j = get_journal_conn()
    try:
        atoms_w = {}
        try:
            for name, edge in j.execute(
                "SELECT predicate, edge_avg_clip FROM signal_outcomes_pnl WHERE horizon = ?",
                [horizon],
            ).fetchall():
                w = max(0, round(float(edge or 0) * 100))   # positive only
                if w > 0:
                    atoms_w[name] = w
        except Exception as e:
            log.warning("no signal_outcomes_pnl yet: %s", e)
        combos = []
        try:
            for preds, edge, n in j.execute(
                """SELECT predicates, oos_edge_avg, n_oos FROM combo_catalog_pnl
                   WHERE horizon = ? AND status = 'passed'""", [horizon],
            ).fetchall():
                bonus = max(0, round(float(edge or 0) * 50))
                if bonus > 0:
                    combos.append({"atoms": preds.split(","), "bonus": bonus, "n": n})
        except Exception as e:
            log.warning("no combo_catalog_pnl yet: %s", e)
    finally:
        j.close()
    return atoms_w, combos


def build_score_sql(atoms_w: dict[str, int], combos: list[dict]) -> tuple[str, str]:
    """Build _SCORE_SQL + _REASONS_SQL dynamically."""
    sql_map = _atom_sqls()
    parts = []
    reasons = []
    # Per-atom contributions
    for atom, w in atoms_w.items():
        if atom not in sql_map:
            continue
        cond = sql_map[atom]
        parts.append(f"CASE WHEN ({cond}) THEN {w} ELSE 0 END")
        tag = ATOM_TAG.get(atom, atom)
        reasons.append(f"CASE WHEN ({cond}) THEN '{tag}+{w}' END")
    # Combo bonuses (AND of all atoms in the combo)
    for c in combos:
        sub_conds = " AND ".join(f"({sql_map[a]})" for a in c["atoms"] if a in sql_map)
        if not sub_conds:
            continue
        parts.append(f"CASE WHEN ({sub_conds}) THEN {c['bonus']} ELSE 0 END")
        cname = "+".join(c["atoms"])
        reasons.append(f"CASE WHEN ({sub_conds}) THEN 'combo:{cname}+{c['bonus']}' END")
    score_sql = "(" + " + ".join(parts) + ")" if parts else "0"
    reasons_sql = "concat_ws('|', " + ", ".join(reasons) + ")" if reasons else "''"
    return score_sql, reasons_sql


def apply_prebreak_v4(universe: str | None = None, horizon: int = 10) -> dict:
    from studio.db import get_conn, ensure_schema
    ensure_schema()
    # Ensure target columns exist (idempotent)
    cw = get_conn(read_only=False)
    try:
        cw.execute("ALTER TABLE bars ADD COLUMN IF NOT EXISTS prebreak_v4 SMALLINT")
        cw.execute("ALTER TABLE bars ADD COLUMN IF NOT EXISTS prebreak_v4_reasons VARCHAR")
        cw.commit()
    finally:
        cw.close()

    atoms_w, combos = load_weights(horizon=horizon)
    log.info("V4 weights: %d positive atoms (top: %s); %d combos with bonus",
             len(atoms_w),
             sorted(atoms_w.items(), key=lambda x: -x[1])[:5],
             len(combos))

    score_sql, reasons_sql = build_score_sql(atoms_w, combos)
    if score_sql == "0":
        return {"error": "no weights available — seed signal_outcomes_pnl first"}

    where = f"WHERE universe = '{universe}'" if universe else ""
    t0 = time.time()
    conn = get_conn(read_only=False)
    try:
        conn.execute(f"""
            UPDATE bars
            SET prebreak_v4 = {score_sql},
                prebreak_v4_reasons = {reasons_sql}
            {where}
        """)
        conn.commit()
        n = conn.execute(f"SELECT count(*) FROM bars {where}").fetchone()[0]
    finally:
        conn.close()
    dur = time.time() - t0
    log.info("prebreak_v4 applied to %s rows (%s, horizon=%d) in %.1fs",
             n, universe or "all", horizon, dur)
    return {"rows": n, "universe": universe or "all", "horizon": horizon,
            "duration_sec": round(dur, 1),
            "n_atoms_weighted": len(atoms_w),
            "n_combo_bonuses": len(combos),
            "top_weights": dict(sorted(atoms_w.items(), key=lambda x: -x[1])[:10])}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    unis = [a.lower() for a in sys.argv[1:]] or [None]
    for u in unis:
        print(apply_prebreak_v4(u), flush=True)
