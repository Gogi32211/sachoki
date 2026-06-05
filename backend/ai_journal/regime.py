"""
ai_journal/regime.py — market regime from universe breadth. CONTEXT, not alpha.

A few transparent breadth measures on a given date → a coarse RISK_ON / NEUTRAL /
RISK_OFF label. Used as a soft conviction gate in the decision prompt (trade
smaller into a weak tape), NOT as a stock-picking edge. Deliberately simple to
avoid overfitting a regime label.
"""
from __future__ import annotations

from .db import get_analytics_conn


def compute_regime(as_of: str | None = None) -> dict:
    a = get_analytics_conn()
    try:
        if as_of is None:
            as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        r = a.execute("""
            SELECT count(*) n,
                   avg(CASE WHEN rsi_14 > 50 THEN 1.0 ELSE 0 END) pct_rsi50,
                   median(rsi_14) med_rsi,
                   avg(CASE WHEN rtb_phase='D' THEN 1.0 ELSE 0 END) pct_phaseD,
                   avg(CASE WHEN prebreak_v3 >= 25 THEN 1.0 ELSE 0 END) setup_density,
                   avg(CASE WHEN close > open THEN 1.0 ELSE 0 END) pct_up_day
            FROM bars WHERE date = ?""", [as_of]).fetchone()
    finally:
        a.close()
    n, pct_rsi50, med_rsi, pct_phaseD, setup_density, pct_up = r
    # Composite breadth score 0..100 (fractions × weights that sum to 100).
    score = round((pct_rsi50 or 0) * 45 + (pct_phaseD or 0) * 25
                  + (setup_density or 0) * 15 + (pct_up or 0) * 15, 1)
    label = "RISK_ON" if score >= 55 else ("RISK_OFF" if score <= 38 else "NEUTRAL")
    # conviction multiplier the journal can apply (soft gate).
    conv_mult = 1.1 if label == "RISK_ON" else (0.7 if label == "RISK_OFF" else 1.0)
    return {
        "as_of": as_of, "n": int(n or 0), "label": label, "score": score,
        "conv_mult": conv_mult,
        "breadth": {
            "pct_rsi_gt50": round((pct_rsi50 or 0) * 100, 1),
            "median_rsi": round(med_rsi or 0, 1),
            "pct_phase_D": round((pct_phaseD or 0) * 100, 1),
            "setup_density": round((setup_density or 0) * 100, 1),
            "pct_up_day": round((pct_up or 0) * 100, 1),
        },
    }


if __name__ == "__main__":
    import json
    print(json.dumps(compute_regime(), indent=2))
