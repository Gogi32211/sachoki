"""
g3_gap_scan.py — live scanner for the validated "G3 gap reclaim" LONG setup
(2026-06 research; entry-L refined 2026-06-27; gap×RSI interaction refined 2026-06-28).
A large catalyst displacement (G3 = |open−prev_close| ≥ 0.5·ATR) on a still-oversold bar
carrying a T-signal, on controlled (non-VB) volume — a catalyst that resumes the move
from a washed-out level. NB: "gap" here is overnight DISPLACEMENT from prior close, not
the visible empty-space gap — validated 2026-06-28 that displacement (not true-gap size)
is the actual edge driver (see project_g3_gap_reclaim memory).

VALIDATED (entry next-open, stop −12%, 20-bar):
  G3 + T + non-VB + RSI<45 (ANY L) → fwd20 +2.15%, win 59%, 6/6 years (flat baseline).
  entry-L sharpener: L5 +2.53/win60/6yr · L12 +2.20 · L46 +2.08 · L34 +2.01 — all 6/6yr.

2026-06-28 gap×RSI INTERACTION (sharper tiers, displacement in ATR units × RSI band):
  PREMIUM = displacement 0.5–1.5·ATR (sweet-spot) × RSI 25–35 → +2.12%/win57/6/6yr/risk1.43
  STRONG  = displacement 0.5–1.5·ATR × RSI 35–45             → +1.04%/win53/6/6yr
  CAUTION = RSI<25 (falling-knife, inconsistent 3/6yr) OR displacement >1.5·ATR
            (exhaustion / blow-off, +0.2%/clip25-neg) → kept but flagged, scored to base.
  The interaction is multiplicative: G1 (<0.2·ATR) is negative at EVERY RSI; G3 at RSI>55
  is dead (−0.19). Both a big catalyst AND oversold are required — neither alone. The
  sweet-spot is mid, not extreme: RSI<25 worse than 25–35, displacement >1.5 worse than
  0.5–1.5 (same shape as the gap-size sweet-spot).

READ-ONLY on bars — surfaces candidates, opens nothing.
"""
from __future__ import annotations


def g3_gap_scan(max_age_days: int = 4, dv_floor: float = 500_000, limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        # disp_ratio = |open − prev_close| / ATR (overnight displacement in ATR units).
        # bar_gap_class='G3' already guarantees ratio ≥ 0.5; we recompute it continuously
        # to split the sweet-spot (0.5–1.5) from the exhaustion tail (>1.5). The window is
        # bounded to a short recent lookback so the lag stays fast and correct.
        rows = a.execute(f"""
            WITH base AS (
                SELECT universe, ticker, date, t_sig, l_sig, rsi_14, cci_20, close, open,
                       atr_14, vol_bucket, volume, avg_vol_20d, bar_gap_class, sig_l4, sig_l6,
                       CASE WHEN sig_l3 = 1 AND sig_l4 = 1 AND close < open
                                 AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                            THEN 1 ELSE 0 END AS l22c,
                       CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open
                                 AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                            THEN 1 ELSE 0 END AS l43c,
                       lag(close) OVER (PARTITION BY universe, ticker ORDER BY date) AS prev_close
                FROM bars
                WHERE close >= 5
                  AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days) + 30} DAY
                  AND NOT (sig_bias_dn = 1 OR sig_vol_5x = 1 OR sig_vol_10x = 1 OR sig_vol_20x = 1)
            ),
            windowed AS (
                SELECT *,
                       SUM(l22c) OVER (PARTITION BY universe, ticker ORDER BY date
                                       ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pre_l22n,
                       SUM(l43c) OVER (PARTITION BY universe, ticker ORDER BY date
                                       ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pre_l43n
                FROM base
            )
            SELECT universe, ticker, date, t_sig, l_sig, rsi_14, cci_20,
                   close, vol_bucket, close * volume AS dv,
                   CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open THEN 1 ELSE 0 END AS l43,
                   coalesce(pre_l22n, 0) AS pre_l22n, coalesce(pre_l43n, 0) AS pre_l43n,
                   CASE WHEN atr_14 > 0 AND prev_close IS NOT NULL
                        THEN abs(open - prev_close) / atr_14 END AS disp_ratio
            FROM windowed
            WHERE t_sig IS NOT NULL AND t_sig <> ''
              AND vol_bucket <> 'VB' AND rsi_14 < 45 AND bar_gap_class = 'G3'
              AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
              AND avg_vol_20d > 0 AND close * volume >= {dv_floor}
            ORDER BY ticker, date DESC
        """).fetchdf()
    finally:
        a.close()

    # Dedup to ONE row per (ticker, date) so a multi-index ticker isn't tier-mislabeled
    # by whichever universe row happens to sort first. Keep sp500 > nasdaq > r2k.
    if not rows.empty:
        _upri = {"sp500": 0, "nasdaq": 1, "russell2k": 2}
        rows = rows.assign(_upri=rows["universe"].map(lambda u: _upri.get(str(u), 9)))
        rows = (rows.sort_values(["ticker", "date", "_upri"], ascending=[True, False, True])
                    .drop_duplicates(["ticker", "date"], keep="first")
                    .drop(columns="_upri")
                    .reset_index(drop=True))

    from datetime import date as _d
    aod = _d.fromisoformat(as_of)
    out, seen = [], set()
    for _, r in rows.iterrows():
        tk = str(r["ticker"])
        if tk in seen:
            continue
        seen.add(tk)
        rsi = float(r["rsi_14"]) if r["rsi_14"] is not None else None
        ll = str(r["l_sig"]) if r["l_sig"] else ""
        ratio = float(r["disp_ratio"]) if r["disp_ratio"] is not None else None
        # entry-L sharpener (2026-06-27 audit): any-L beats the old L3-only requirement
        # (+2.15/6yr vs +1.86/5yr); L5 best (+2.53/win60/6yr), L12 +2.20 — L3 was the WORST.
        sharp_l = ll in ("L5", "L12", "L46", "L34")
        # gap×RSI interaction (2026-06-28): displacement sweet-spot 0.5–1.5·ATR, RSI sweet
        # 25–45 (peak 25–35). RSI<25 = falling-knife, displacement >1.5 = exhaustion.
        sweet   = ratio is not None and 0.5 <= ratio < 1.5
        exhaust = ratio is not None and ratio >= 1.5
        knife   = rsi is not None and rsi < 25
        # L43 confluence (2026-06-29): the bar is also a VSA supply-absorbed green body
        # (sig_l6&sig_l4 + close≥open). G3-sweet + L43 = +2.77%/win61/6yr vs +2.12 without.
        l43 = bool(int(r["l43"]) if r["l43"] is not None else 0)
        if sweet and not knife and rsi is not None and 25 <= rsi < 35:
            tier = "premium"          # +2.12/win57/6yr
        elif sweet and not knife and rsi is not None and 35 <= rsi < 45:
            tier = "strong"           # +1.04/win53/6yr
        else:
            tier = "base"             # falling-knife (<25), exhaustion (>1.5), or off-band
        score = 50 \
            + (25 if tier == "premium" else 12 if tier == "strong" else 0) \
            + (8 if ll == "L5" else 4 if ll in ("L12", "L46", "L34") else 0) \
            + (5 if sweet else 0) - (8 if exhaust else 0) - (6 if knife else 0) \
            + (8 if (l43 and sweet) else 0)
        # pre-absorption booster (2026-06-30): a controlled-vol (0.7–2.0×vol20) L43 (demand)
        # or L22 (supply-exhaustion) absorption in the prior 5 bars = a higher-quality base
        # for the gap reclaim. Verified additive: G3 + pre-L43 +5.51%/PF2.14, + pre-L22 +4.63.
        pre_l43 = int(r["pre_l43n"] or 0) > 0
        pre_l22 = int(r["pre_l22n"] or 0) > 0
        if pre_l43:
            score += 10
        if pre_l22:
            score += 5
        gap_tag = ("gap·sweet" if sweet else "gap·EXHAUST>1.5ATR" if exhaust else "gap·G3")
        if ratio is not None:
            gap_tag += f"({ratio:.1f}×ATR)"
        atoms = [gap_tag, ll or "L?", str(r["t_sig"])]
        if l43:
            atoms.append("L43✓absorbed")
        if pre_l43:
            atoms.append("🟢L43-base")
        if pre_l22:
            atoms.append("🧱L22-absorb")
        if knife:
            atoms.append(f"RSI{rsi:.0f}·falling-knife")
        elif rsi is not None and rsi < 35:
            atoms.append(f"RSI{rsi:.0f}·oversold-sweet")
        elif rsi is not None and rsi < 40:
            atoms.append(f"RSI{rsi:.0f}·oversold")
        out.append({
            "ticker": tk, "universe": str(r["universe"]),
            "signal_date": str(r["date"])[:10], "t_sig": str(r["t_sig"]),
            "l_sig": str(r["l_sig"]) if r["l_sig"] else "",
            "close": round(float(r["close"]), 2) if r["close"] is not None else None,
            "rsi": round(rsi, 0) if rsi is not None else None,
            "cci": round(float(r["cci_20"]), 0) if r["cci_20"] is not None else None,
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "disp_atr": round(ratio, 2) if ratio is not None else None,
            "gap_band": ("sweet" if sweet else "exhaust" if exhaust else "g3"),
            "l43": l43, "pre_l43": pre_l43, "pre_l22": pre_l22,
            "age_days": (aod - _d.fromisoformat(str(r["date"])[:10])).days,
            "tier": tier, "score": int(max(0, min(score, 100))), "atoms": atoms,
        })

    # price-zone guard (2026-06-30): down-weight dead($8-21)/knife(<$8)/casino(<$1),
    # lift quality($21-89); G3 keeps its $8-10 cheap-momentum spillover (setup="g3").
    from price_zones import classify as _pz
    for r in out:
        z = _pz(r["close"], setup="g3")
        r["score"] = max(0, min(r["score"] + z["score_delta"], 100))
        r["price_zone"] = z["zone"]; r["zone_emoji"] = z["emoji"]
        if z["zone"] in ("dead", "knife", "casino"):
            r["atoms"].append(f"{z['emoji']}{z['label']}")
    out.sort(key=lambda x: ({"premium": 0, "strong": 1, "base": 2}[x["tier"]],
                            -x["score"], x["age_days"]))
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
    # ⛔ sub-200-rally suppressor (validated 2026-07-05): 1D close<EMA200 with e9>e20>e50
    # (bear-market rally) — B-fires worse on EVERY setup, era-independent. Badge-only.
    try:
        from sub200_rally import flags_for as _s200
        _bf = _s200([r["ticker"] for r in out])
        for r in out:
            if _bf.get(r["ticker"]):
                r["sub200_rally"] = True
                r["atoms"].append("⛔sub200")
    except Exception:
        pass
    return {
        "as_of": as_of, "count": len(out), "rows": out[:limit],
        "edge_note": ("G3 catalyst-displacement reclaim: |open−prev_close| ≥ 0.5·ATR on a still-oversold "
                      "bar (RSI<45) with a T-signal, non-VB. Baseline +2.15%/win59/6yr. gap×RSI tiers "
                      "(2026-06-28): PREMIUM = displacement 0.5–1.5·ATR × RSI 25–35 (+2.12%/win57/6yr/"
                      "risk1.43); STRONG = same band × RSI 35–45 (+1.04/6yr); base flags falling-knife "
                      "(RSI<25, 3/6yr) and exhaustion (>1.5·ATR, blow-off). Interaction is multiplicative "
                      "— G1<0.2·ATR negative at every RSI, G3 dead above RSI55; need BOTH catalyst AND "
                      "oversold, both at the mid sweet-spot not the extreme. Momentum — small size."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(g3_gap_scan(max_age_days=10), indent=2)[:2000])
