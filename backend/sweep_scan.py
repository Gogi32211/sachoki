"""
sweep_scan.py — LIVE scanner for the T1 LOW-SWEEP setup (SWEEP-only, GEM1 EXCLUDED).
Validated 2026-07-01: a T1 that sweeps the t-2 (and t-3) lows — a reach-back stop-run — in the
STATE band (≥$21 · RSI 30-50 · vol=B). On its own the sweep is NOT a strong standalone edge
(SWEEP-only path-sim +2.36 / med +0.67 / PF 1.39 / 5-6yr / '22 +1.2 — modest but real, survived
2022), and it does NOT add over GEM1 (whose body-magnitude is the true driver). This scanner
surfaces the SWEEP-only cell (GEM1's prior-Z body>2× condition explicitly excluded) so the two
populations can be tracked separately for further refinement. STATE > SHAPE: sweep alone ≈ base.
"""
from datetime import date as _d


def sweep_scan(max_age_days: int = 4, dv_floor: float = 500_000, limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        rows = a.execute(f"""
            WITH base AS (
                SELECT universe, ticker, date, t_sig, l_sig, rsi_14, close, open, low,
                       vol_bucket, volume, avg_vol_20d,
                       lag(z_sig) OVER w AS prev_z,
                       lag(close) OVER w AS pc,
                       lag(open)  OVER w AS po,
                       lag(low,2) OVER w AS lo2,
                       lag(low,3) OVER w AS lo3
                FROM bars
                WHERE close >= 5
                  AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days) + 15} DAY
                  AND NOT (sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1)
                WINDOW w AS (PARTITION BY universe, ticker ORDER BY date)
            )
            SELECT universe, ticker, date, l_sig, rsi_14, close, vol_bucket,
                   close*volume AS dv, lo2, lo3, low,
                   (lo2 IS NOT NULL AND low <= lo2 AND lo3 IS NOT NULL AND low <= lo3) AS swept_both
            FROM base
            WHERE t_sig = 'T1'
              AND rsi_14 BETWEEN 30 AND 50 AND vol_bucket = 'B' AND close >= 21
              AND lo2 IS NOT NULL AND low <= lo2                        -- sweeps t-2 low (min)
              -- EXCLUDE GEM1: not (prior bar is Z with body > 2× this T1's body)
              AND NOT (prev_z IS NOT NULL AND prev_z <> ''
                       AND abs(pc-po) > 0 AND abs(close-open) < 0.5*abs(pc-po))
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
        both = bool(r["swept_both"])                           # swept t-2 AND t-3 (deeper)
        atoms = ["T1", "swept t-2" + ("+t-3" if both else ""), f"RSI{float(r['rsi_14']):.0f}", "vol=B"]
        score = 60                                             # modest standalone (SWEEP-only +2.36)
        if both:
            atoms.append("🕳️deep-sweep"); score += 8          # t-2+t-3 monotone better
        out.append({
            "ticker": tk, "universe": str(r["universe"]), "signal_date": str(r["date"])[:10],
            "close": round(float(r["close"]), 2),
            "rsi": round(float(r["rsi_14"]), 0) if r["rsi_14"] is not None else None,
            "l_sig": str(r["l_sig"] or ""), "swept_both": both, "vol": str(r["vol_bucket"] or ""),
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "score": min(int(score), 100), "atoms": atoms,
            "age_days": (aod - _d.fromisoformat(str(r["date"])[:10])).days,
        })

    out.sort(key=lambda x: x["score"], reverse=True)
    return {
        "as_of": as_of, "count": len(out), "rows": out[:limit],
        "edge_note": ("T1 LOW-SWEEP (SWEEP-only, GEM1 excluded) — a T1 that sweeps the t-2 (🕳️ = also "
                      "t-3) lows, ≥$21 · RSI 30-50 · vol=B. Modest standalone (path-sim +2.36 / med +0.67 "
                      "/ PF 1.39 / 5-6yr / '22 +1.2 — survived 2022 but NOT strong; STATE does the work, "
                      "not the sweep). GEM1's body-magnitude cell is EXCLUDED here so the two can be "
                      "tracked apart. Track/refine before sizing — this is the weaker of the four cells."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(sweep_scan(max_age_days=8), indent=2)[:1600])
