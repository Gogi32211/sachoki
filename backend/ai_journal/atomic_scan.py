"""
ai_journal/atomic_scan.py — live scanner for the 5-year-validated atomic bull edge
"weak-close gap-up": a bull T-signal that closes WEAK (close=O, below prior body)
on a GAP-up bar (G2/G3). Backtest (entry next-open, −15%/+100%): positive expectancy
in all 3 universes, positive 5/6 years (only 2022 bear negative). Each candidate is
scored by how many corroborating atoms it stacks (R2L oversold, EO escape, vol=B,
wick=D, G3 gap), and the current market regime is attached as a size gate.

PRICE KNIFE-GUARD (thirds-stability deep-dive, this session): cheap stocks are the
catastrophe driver — <$8 = robust knife (cat 38-52%), $8-16 = unstable/high-cat, while
$16-300 is robust-positive in all 3 time-thirds (med +1.35, cat 22%→15%, win 52%→56%).
VB (very-high-vol blow-off) bucket = a knife both halves. → EXCLUDE VB; gate price at $16
UNLESS a recent QUALITY (B+) capitulation rescues it.

🔥 Capit→Atomic CONFLUENCE (this session's key find): a weak-close gap-up that follows a
B+ capitulation on the same ticker within ~10-15 days is the strongest setup (rich+capit
≤10d: win 67%, med +4.24 vs +1.41 baseline; survives price-control, dedup, ex-cluster).
The QUALITY of the capit matters — a raw/penny capit does NOT rescue (med -1.21), only a
B+ one does (+1.70). So: keep cheap<$16 IFF post-capit, and +20 score boost on any post-capit.
NOTE: still a momentum/beta edge — regime-dependent (loses in corrections), unlike Capit.

ANALYSIS/SCANNER ONLY — surfaces candidates, opens no positions. Read-only on bars.
"""
from __future__ import annotations
from .db import get_analytics_conn

_BULL_T = ("T1", "T1G", "T2", "T2G", "T3", "T4", "T5", "T6", "T9", "T10", "T11", "T12")


def atomic_scan(max_age_days: int = 4, dv_floor: float = 500_000, limit: int = 120,
                capit_window: int = 15) -> dict:
    from .capit_scan import capit_signal_dates, days_since_capit
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        ph = ",".join(f"'{s}'" for s in _BULL_T)
        # Pull cheap candidates too (no close>=16 gate here) — the prior-capit rescue
        # decides which cheap ones to keep. VB blow-off is always excluded.
        rows = a.execute(f"""
            SELECT universe, ticker, date, t_sig, close, rsi_14,
                   bar_gap_class AS gap, vol_bucket AS vol, full_suffix AS sfx,
                   CASE WHEN regexp_matches(bar_line5, 'R2L') THEN 1 ELSE 0 END AS r2l,
                   close * volume AS dv
            FROM bars
            WHERE t_sig IN ({ph}) AND close_suffix = 'O' AND bar_gap_class IN ('G2', 'G3')
              AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
              AND avg_vol_20d > 0 AND close * volume >= {dv_floor}
              AND vol_bucket <> 'VB'
        """).fetchdf()
        # prior B+ capitulation marker (quality context): look back max_age + window
        from datetime import date as _d, timedelta as _td
        _since = str(_d.fromisoformat(as_of) - _td(days=int(max_age_days) + int(capit_window) + 5))
        capit_dates = capit_signal_dates(a, since_date=_since)
    finally:
        a.close()

    best: dict = {}
    for _, r in rows.iterrows():
        sfx = str(r["sfx"] or "")
        px = float(r["close"]) if r["close"] is not None else 0.0
        # prior QUALITY (B+) capitulation within the window → premium Capit→Atomic confluence
        dpc = days_since_capit(capit_dates, r["ticker"], r["universe"], r["date"])
        post_capit = dpc is not None and dpc <= capit_window
        # price knife-guard: keep ≥$16, OR a cheap one ONLY if rescued by a recent capit
        if px < 16 and not post_capit:
            continue
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
        if post_capit:
            atoms.append(f"🔥post-capit{int(dpc)}d"); score += 20   # the validated confluence boost
        score = min(score, 100)
        tk = str(r["ticker"])
        cand = {
            "ticker": tk, "universe": str(r["universe"]),
            "signal_date": str(r["date"])[:10], "t_sig": str(r["t_sig"]),
            "close": round(px, 2) if r["close"] is not None else None,
            "rsi": round(float(r["rsi_14"]), 0) if r["rsi_14"] is not None else None,
            "gap": str(r["gap"]), "vol": str(r["vol"]), "suffix": sfx,
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "score": int(score), "atoms": atoms, "age_days": None,
            "post_capit": bool(post_capit), "capit_age": int(dpc) if post_capit else None,
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
        "edge_note": ("weak-close gap-up (≥$16 OR rescued by a recent B+ capit; no-VB). "
                      "🔥post-capit = the premium Capit→Atomic confluence (rich+capit≤10d: "
                      "win 67%, med +4.24 vs +1.41 baseline) — sorted to the top. Momentum/beta: "
                      "swing-grade, small size, stand down in RISK_OFF."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(atomic_scan(), indent=2)[:2000])
