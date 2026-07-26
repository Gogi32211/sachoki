"""
t6_sc_scan.py — LIVE scanner for T6-SC-OVERSOLD (validated 2026-07-04, project_wyckoff_range_super
+ T4/T6 zone study). A T6 signal firing within ±5% of the Wyckoff range support (SC accumulation
floor) while oversold (RSI<40), non-suppressor. The best T4/T6 cell found: band-plateau +
RSI-plateau + 2×-slip-safe. Path-sim (trail25/60, gap-realistic): mean +1.92 / med +1.33 / PF 1.36
/ 5-of-6yr / TR +1.00 / '22 +0.28. Modest but robust — the SC-floor context flips raw T6
(neg-median, 2025-artifact) into a real, 2022-surviving edge. Deeper RSI (<35) sharpens it.
"""
from datetime import date as _d


def t6_sc_scan(max_age_days: int = 4, dv_floor: float = 500_000, limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        rows = a.execute(f"""
            SELECT universe, ticker, date, l_sig, rsi_14, close, vol_bucket,
                   close*volume AS dv, wt_support, wt_resistance,
                   abs(close/NULLIF(wt_support,0) - 1) AS sc_dist
            FROM bars
            WHERE t_sig = 'T6'
              AND close >= 5 AND rsi_14 < 40
              AND coalesce(wt_valid_tr,0) = 1 AND wt_resistance > wt_support AND wt_support > 0
              AND abs(close/NULLIF(wt_support,0) - 1) <= 0.05        -- within ±5% of the SC floor
              AND NOT (sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1)
              AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
              AND avg_vol_20d > 0 AND close*volume >= {dv_floor}
            ORDER BY ticker, date DESC
        """).fetchdf()
    finally:
        a.close()

    if not rows.empty:
        _upri = {"sp500": 0, "nasdaq": 1, "russell2k": 2}
        rows = rows.assign(_u=rows["universe"].map(lambda u: _upri.get(str(u), 9)))
        rows = (rows.sort_values(["ticker", "date", "_u"], ascending=[True, False, True])
                    .drop_duplicates(["ticker", "date"], keep="first").drop(columns="_u")
                    .reset_index(drop=True))

    aod = _d.fromisoformat(as_of)
    out, seen = [], set()
    for _, r in rows.iterrows():
        tk = str(r["ticker"])
        if tk in seen:
            continue
        seen.add(tk)
        rsi = float(r["rsi_14"]) if r["rsi_14"] is not None else 40
        deep = rsi < 35
        atoms = ["T6", "🌀SC-floor", f"RSI{rsi:.0f}"]
        score = 62                                    # validated base (5/6yr, PF1.36, '22+)
        if deep:
            atoms.append("deep-oversold"); score += 10   # RSI<35: TR+1.88 (sharper)
        out.append({
            "ticker": tk, "universe": str(r["universe"]), "signal_date": str(r["date"])[:10],
            "close": round(float(r["close"]), 2),
            "rsi": round(rsi, 0),
            "l_sig": str(r["l_sig"] or ""), "sc_super": True,
            "sc_dist_pct": round(float(r["sc_dist"]) * 100, 1) if r["sc_dist"] is not None else None,
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "score": min(int(score), 100), "atoms": atoms,
            "age_days": (aod - _d.fromisoformat(str(r["date"])[:10])).days,
        })

    out.sort(key=lambda x: x["score"], reverse=True)
    return {
        "as_of": as_of, "count": len(out), "rows": out[:limit],
        "edge_note": ("T6-SC-OVERSOLD — a T6 signal firing within ±5% of the Wyckoff range support "
                      "(SC accumulation floor) while oversold (RSI<40), non-VB. Validated 2026-07-04 "
                      "(path-sim trail25/60): mean +1.92 / med +1.33 / PF 1.36 / 5-of-6yr / '22 +0.28 "
                      "— band+RSI plateau, 2×-slip-safe. Modest but robust: the SC-floor context flips "
                      "raw T6 (neg-median, 2025-artifact) into a 2022-surviving edge. 🌀 deep-oversold "
                      "(RSI<35) sharpens it (TR+1.88). Entry next-open, trailing exit, small size."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(t6_sc_scan(max_age_days=10), indent=2)[:1600])
