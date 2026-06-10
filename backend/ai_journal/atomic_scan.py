"""
ai_journal/atomic_scan.py — live scanner for the 5-year-validated atomic bull edge
"weak-close gap-up": a bull T-signal that closes WEAK (close=O, below prior body)
on a GAP-up bar (G2/G3). Backtest (entry next-open, −15%/+100%): positive expectancy
in all 3 universes, positive 5/6 years (only 2022 bear negative). Each candidate is
scored by how many corroborating atoms it stacks (R2L oversold, EO escape, vol=B,
wick=D, G3 gap), and the current market regime is attached as a size gate.

ANALYSIS/SCANNER ONLY — surfaces candidates, opens no positions. Read-only on bars.
"""
from __future__ import annotations
from .db import get_analytics_conn

_BULL_T = ("T1", "T1G", "T2", "T2G", "T3", "T4", "T5", "T6", "T9", "T10", "T11", "T12")


def atomic_scan(max_age_days: int = 4, dv_floor: float = 500_000, limit: int = 120) -> dict:
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        ph = ",".join(f"'{s}'" for s in _BULL_T)
        rows = a.execute(f"""
            SELECT universe, ticker, date, t_sig, close, rsi_14,
                   bar_gap_class AS gap, vol_bucket AS vol, full_suffix AS sfx,
                   CASE WHEN regexp_matches(bar_line5, 'R2L') THEN 1 ELSE 0 END AS r2l,
                   close * volume AS dv
            FROM bars
            WHERE t_sig IN ({ph}) AND close_suffix = 'O' AND bar_gap_class IN ('G2', 'G3')
              AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
              AND avg_vol_20d > 0 AND close * volume >= {dv_floor}
        """).fetchdf()
    finally:
        a.close()

    best: dict = {}
    for _, r in rows.iterrows():
        sfx = str(r["sfx"] or "")
        atoms = ["close=O", "gap"]                      # base (always present)
        score = 40
        if int(r["r2l"] or 0):
            atoms.append("R2L"); score += 25
        if sfx[:1] == "E":
            atoms.append("EO"); score += 15
        if r["vol"] == "B":
            atoms.append("vol=B"); score += 15
        if "D" in sfx[1:2]:
            atoms.append("wick=D"); score += 10
        if r["gap"] == "G3":
            atoms.append("G3"); score += 10
        score = min(score, 100)
        tk = str(r["ticker"])
        cand = {
            "ticker": tk, "universe": str(r["universe"]),
            "signal_date": str(r["date"])[:10], "t_sig": str(r["t_sig"]),
            "close": round(float(r["close"]), 2) if r["close"] is not None else None,
            "rsi": round(float(r["rsi_14"]), 0) if r["rsi_14"] is not None else None,
            "gap": str(r["gap"]), "vol": str(r["vol"]), "suffix": sfx,
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "score": int(score), "atoms": atoms,
            "age_days": None,
        }
        if tk not in best or score > best[tk]["score"]:
            best[tk] = cand

    out = sorted(best.values(), key=lambda x: x["score"], reverse=True)[:limit]
    # age relative to as_of
    from datetime import date as _d
    ad = _d.fromisoformat(as_of)
    for c in out:
        c["age_days"] = (ad - _d.fromisoformat(c["signal_date"])).days

    # regime gate
    try:
        from . import regime as _reg
        reg = _reg.compute_regime(as_of)
    except Exception:
        reg = {"label": "NEUTRAL", "score": None, "conv_mult": 1.0, "breadth": {}}

    return {
        "as_of": as_of, "count": len(out), "rows": out,
        "regime": {"label": reg["label"], "score": reg["score"],
                   "conv_mult": reg["conv_mult"], "breadth": reg.get("breadth", {})},
        "edge_note": ("weak-close gap-up: 5yr backtest +0.84 sp500 / +0.70 r2k expectancy, "
                      "positive 5/6 years (2022 bear negative). Swing-grade; small size; "
                      "stand down in RISK_OFF."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(atomic_scan(), indent=2)[:2000])
