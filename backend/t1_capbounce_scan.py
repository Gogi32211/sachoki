"""
t1_capbounce_scan.py — LIVE scanner for GEM1, the T1 capitulation-bounce
(validated 2026-07-01, project_capitulation_bounce — the most robust edge found:
6/6yr +5..+9 each, TRAIN≈TEST era-independent, all 3 universes, PF 2.32).

Fires on: a T1 signal whose body is < 0.5× the prior (Z) bar's body — a SMALL T1 bounce
off a BIG bear/capitulation Z — in moderate-oversold RSI 30-50 on controlled volume (vol=B).
Booster: the prior Z (capitulation) bar carried L5/L46 (VSA absorption). Surfaces candidates.
"""
from datetime import date as _d


def t1_capbounce_scan(max_age_days: int = 4, dv_floor: float = 500_000, limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        rows = a.execute(f"""
            WITH base AS (
                SELECT universe, ticker, date, t_sig, l_sig, rsi_14, close, open, low,
                       vol_bucket, volume, avg_vol_20d, wt_valid_tr, wt_support, wt_resistance,
                       lag(z_sig) OVER w AS prev_z,
                       lag(l_sig) OVER w AS prev_l,
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
                   close*volume AS dv, coalesce(prev_l,'') AS prev_l,
                   abs(close-open) AS body, abs(pc-po) AS pbody,
                   (lo2 IS NOT NULL AND lo3 IS NOT NULL AND low <= least(lo2, lo3)) AS swept,
                   coalesce(wt_valid_tr,0) AS vtr, coalesce(wt_support,0) AS wt_sup,
                   coalesce(wt_resistance,0) AS wt_res
            FROM base
            WHERE t_sig = 'T1' AND prev_z IS NOT NULL AND prev_z <> ''
              AND rsi_14 BETWEEN 30 AND 50 AND vol_bucket = 'B'
              AND abs(pc-po) > 0 AND abs(close-open) < 0.5*abs(pc-po)
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
        pbody, body = float(r["pbody"] or 0), float(r["body"] or 0)
        zmult = round(pbody / body, 1) if body > 0 else None   # how many × bigger the Z was
        prev_l = str(r["prev_l"] or "")
        absorb = prev_l in ("L5", "L46")                       # VSA absorption on the cap-bar
        swept = bool(r["swept"])                               # also swept t-2+t-3 lows (premium tier)
        atoms = ["T1", f"cap-Z {zmult}×" if zmult else "cap-Z", f"RSI{float(r['rsi_14']):.0f}", "vol=B"]
        score = 74                                             # validated base (6/6yr, PF2.32)
        from wyc_zone import sc_zone
        sc_super = sc_zone(r["close"], r["wt_sup"], r["wt_res"], r["vtr"])
        if sc_super:
            atoms.append("🌀SC-SUPER"); score += 10             # SC-zone plateau: TR+7.2/6-6yr/'22+4.2
        if swept:
            atoms.append("🕳️swept-lows"); score += 10          # GEM1∩SWEEP: +7.82 vs +5.47, '22 +7.4
        if absorb:
            atoms.append(f"🧱{prev_l}-absorb"); score += 12
        if zmult and zmult >= 4:
            atoms.append("deep-cap"); score += 6
        out.append({
            "ticker": tk, "universe": str(r["universe"]), "signal_date": str(r["date"])[:10],
            "close": round(float(r["close"]), 2),
            "rsi": round(float(r["rsi_14"]), 0) if r["rsi_14"] is not None else None,
            "l_sig": str(r["l_sig"] or ""), "prev_l": prev_l, "z_mult": zmult, "swept": swept,
            "sc_super": sc_super,
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "score": min(int(score), 100), "atoms": atoms,
            "age_days": (aod - _d.fromisoformat(str(r["date"])[:10])).days,
        })

    out.sort(key=lambda x: x["score"], reverse=True)
    # ⚡ CHARGED energy booster (validated 2026-07-06): 3d hot-vol + expanded range +
    # diffuse intraday flow — 9/10 Edge setups improve when entered charged. Badge-only.
    try:
        from charged_state import charged_for as _chf
        _cf = _chf([r["ticker"] for r in out])
        for r in out:
            if _cf.get(r["ticker"]):
                r["charged"] = True
                r["atoms"].append("⚡charged")
    except Exception:
        pass
    return {
        "as_of": as_of, "count": len(out), "rows": out[:limit],
        "edge_note": ("T1 capitulation-bounce (GEM1) — a SMALL T1 (body <0.5× the prior Z bar's "
                      "body = a modest bounce off a big bear/capitulation) · RSI 30-50 · vol=B. "
                      "The MOST ROBUST edge validated (6/6yr +5..+9 each, TRAIN≈TEST era-independent, "
                      "all 3 universes, PF 2.32, med +5.4, win 60). 🧱 = prior cap-bar carried "
                      "L5/L46 VSA absorption (booster). 🕳️ = the T1 also swept the t-2+t-3 lows "
                      "(premium tier: GEM1∩SWEEP +7.82 vs +5.47, '22 +7.4). Entry next-open, "
                      "trailing exit, small size."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(t1_capbounce_scan(max_age_days=6), indent=2)[:1800])
