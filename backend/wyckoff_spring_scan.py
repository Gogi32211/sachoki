"""
wyckoff_spring_scan.py — live scanner for the WYCKOFF SPRING accumulation entry
(2026-06-27 research, structural / regime setup — bigger than single-bar).

THESIS (validated): the textbook Wyckoff entry is the SPRING (Accumulation Schematic #1,
Phase C) — a shakeout BELOW the trading-range support that traps sellers and is reclaimed,
NOT the breakout (where smart money sells to breakout buyers). DB-tested across 2019-26:
the breakout / SOS / MKDN→MARKUP flip have NO edge (−0.5/win48/1-6yr, survivorship — the
RYAN winners are remembered, the false breakouts forgotten). The SPRING does:
  w2_spring + RSI 35-45 + bullish-T + non-VB : med +1.06% · win 54% · 6/6 years (MFE>MAE)
  sharpeners — premium: gap=G3-V +2.87/win61 · l5=PS-R2L +2.66 ; strong: L34 +0.88 / vol=B.
Schematic #2 (no-spring: the test/LPS holds ABOVE support) does NOT validate (w2_lps −0.37)
— without the undercut there's no seller-trap, no edge. So only the SPRING fires here.
Enter at the spring (next-open), structural stop below the spring low. Distinct, structural.

READ-ONLY on bars — surfaces candidates, opens nothing.
"""
from __future__ import annotations

_RSI_LO, _RSI_HI = 35.0, 45.0   # validated band (｜<35 = falling-knife +0.18 ｜>45 fades)


def wyckoff_spring_scan(max_age_days: int = 8, dv_floor: float = 2_000_000,
                        rsi_lo: float = _RSI_LO, rsi_hi: float = _RSI_HI,
                        mode: str = "spring", limit: int = 120) -> dict:
    """mode='spring' = the shakeout entry (entry 1). mode='continuation' = the post-spring
    markup window: a bull-T pullback-resume within 15 bars AFTER a spring, RSI<60, non-VB
    (entry 2 / add — validated +0.70/win53/5-6yr; sharper +L5 +3.01/win61/6yr, +RSI<40
    +1.04/6yr). The breakout / BU-retest / SOS do NOT validate — only the spring & its
    markup-window pullbacks do."""
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        if mode == "continuation":
            rows = a.execute(f"""
                SELECT * FROM (
                  SELECT *,
                       SUM(w2_spring) OVER (PARTITION BY ticker ORDER BY date
                            ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS spr15,
                       SUM(l43c) OVER (PARTITION BY ticker ORDER BY date
                            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pre_l43n
                  FROM (
                    SELECT universe, ticker, date, close, low, volume, rsi_14, w2_spring,
                       coalesce(t_sig,'') AS t, coalesce(z_sig,'') AS z, coalesce(l_sig,'') AS l,
                       coalesce(bar_line5,'') AS l5, coalesce(bar_gap_range,'') AS gap,
                       coalesce(vol_bucket,'') AS vol, coalesce(w2_state, 0) AS w2_state,
                       CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open THEN 1 ELSE 0 END AS l43,
                       CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open
                                 AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                            THEN 1 ELSE 0 END AS l43c,
                       CASE WHEN sig_bias_dn = 1 OR sig_vol_5x = 1 OR sig_vol_10x = 1
                                 OR sig_vol_20x = 1 THEN 1 ELSE 0 END AS supp,
                       coalesce(wt_valid_tr,0) AS vtr, coalesce(wt_support,0) AS wt_sup,
                       coalesce(wt_resistance,0) AS wt_res
                    FROM bars
                    WHERE date >= DATE '{as_of}' - INTERVAL {int(max_age_days) + 25} DAY AND close > 0
                  )
                ) WHERE spr15 > 0 AND supp = 0
                  AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
                  AND close > 5 AND close * volume >= {dv_floor}
                  AND t <> '' AND vol <> 'VB' AND rsi_14 < 60
                ORDER BY date DESC
            """).fetchdf()
        else:
            rows = a.execute(f"""
                WITH flagged AS (
                  SELECT *,
                       CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open
                                 AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                            THEN 1 ELSE 0 END AS l43c
                  FROM bars
                  WHERE date >= DATE '{as_of}' - INTERVAL {int(max_age_days) + 10} DAY
                ),
                windowed AS (
                  SELECT *,
                       SUM(l43c) OVER (PARTITION BY ticker ORDER BY date
                            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pre_l43n
                  FROM flagged
                )
                SELECT universe, ticker, date, close, low, volume, rsi_14,
                       coalesce(t_sig,'') AS t, coalesce(z_sig,'') AS z, coalesce(l_sig,'') AS l,
                       coalesce(bar_line5,'') AS l5, coalesce(bar_gap_range,'') AS gap,
                       coalesce(vol_bucket,'') AS vol, coalesce(w2_state, 0) AS w2_state,
                       CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open THEN 1 ELSE 0 END AS l43,
                       coalesce(pre_l43n, 0) AS pre_l43n,
                       coalesce(wt_valid_tr,0) AS vtr, coalesce(wt_support,0) AS wt_sup,
                       coalesce(wt_resistance,0) AS wt_res
                FROM windowed
                WHERE w2_spring = 1
                  AND NOT (sig_bias_dn = 1 OR sig_vol_5x = 1 OR sig_vol_10x = 1 OR sig_vol_20x = 1)
                  AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
                  AND close > 5 AND close * volume >= {dv_floor}
                  AND rsi_14 >= {rsi_lo} AND rsi_14 < {rsi_hi}
                  AND coalesce(t_sig,'') <> '' AND coalesce(vol_bucket,'') <> 'VB'
                ORDER BY date DESC
            """).fetchdf()
    finally:
        a.close()

    if rows.empty:
        return {"as_of": as_of, "count": 0, "rows": [], "edge_note": _NOTE}

    # dedup (ticker, date) — keep sp500 > nasdaq > r2k
    _upri = {"sp500": 0, "nasdaq": 1, "russell2k": 2}
    rows = rows.assign(_p=rows["universe"].map(lambda u: _upri.get(str(u), 9)))
    rows = (rows.sort_values(["ticker", "date", "_p"])
                .drop_duplicates(["ticker", "date"], keep="first")
                .drop(columns="_p"))
    # one spring per ticker (the most recent)
    rows = rows.sort_values("date").drop_duplicates("ticker", keep="last")

    from datetime import date as _d
    aod = _d.fromisoformat(as_of)
    out = []
    for _, r in rows.iterrows():
        ds = str(r["date"])[:10]
        age = (aod - _d.fromisoformat(ds)).days
        if age > max_age_days or age < 0:
            continue
        l5 = str(r["l5"]); gap = str(r["gap"]); lln = str(r["l"]); vb = str(r["vol"])
        ra = float(r["rsi_14"]); dv = float(r["close"] * r["volume"])
        # tier by the validated sharpeners (differ by mode)
        if mode == "continuation":
            premium = (lln == "L5") or ("PB-R2L" in l5)          # +3.01/win61 · +2.59
            strong = ra < 40                                     # +1.04/6yr (deep pullback)
            sharp = [x for x in (("L5" if lln == "L5" else ""), ("PB-R2L" if "PB-R2L" in l5 else ""),
                                 ("RSI<40" if ra < 40 else ""), ("G3-V" if gap == "G3-V" else "")) if x]
        else:
            premium = (gap == "G3-V") or ("PS-R2L" in l5)        # +2.87/win61 · +2.66
            strong = lln == "L34" or vb == "B"                   # +0.88 · +0.81
            sharp = [x for x in (("G3-V" if gap == "G3-V" else ""), ("PS-R2L" if "PS-R2L" in l5 else ""),
                                 (lln if lln in ("L34", "L12") else ""), ("vol-B" if vb == "B" else "")) if x]
        tier = "premium" if premium else "strong" if strong else "medium"
        # L43 VSA confluence (2026-06-29): the spring bar is also a supply-absorbed green body —
        # the BIGGEST lift of any setup (+0.79→+4.17%/win65/6yr). sig_l6&sig_l4 + close≥open.
        l43 = bool(int(r["l43"]) if r["l43"] is not None else 0)
        # pre-L43 absorption booster (2026-06-30): a controlled-vol L43 (demand-absorption
        # green) in the 5 bars before the spring. Verified additive: Spring + pre-L43
        # +4.22%/PF1.71. NB pre-L22 is EXCLUDED here — it HURTS springs (−0.36%/PF0.95):
        # a supply-exhaustion red bar before a shakeout = distribution not yet done.
        pre_l43 = int(r["pre_l43n"] or 0) > 0 if "pre_l43n" in r else False
        score = (60 if tier == "premium" else 45 if tier == "strong" else 32) \
            + (6 if dv >= 20e6 else 0) + max(0, 8 - age * 2) + (10 if l43 else 0) \
            + (10 if pre_l43 else 0)
        # SC-SUPER (2026-07-03): spring firing within ±5% of the Wyckoff range support —
        # validated band-plateau, mean +1.08→+1.42, TR −0.52→+0.18, 2×-slip-safe.
        from wyc_zone import sc_zone
        sc_super = sc_zone(r["close"], r["wt_sup"], r["wt_res"], r["vtr"])
        if sc_super:
            score += 8
        tag = "SPRING+cont" if mode == "continuation" else "SPRING"
        atoms = [tag, f"RSI{ra:.0f}", str(r["t"]) + (lln if lln else "")] + sharp
        if l43:
            atoms.append("L43✓absorbed")
        if pre_l43:
            atoms.append("🟢L43-base")
        if sc_super:
            atoms.append("🌀SC-SUPER")
        out.append({
            "ticker": r["ticker"], "universe": str(r["universe"]), "mode": mode,
            "signal_date": ds, "spring_low": round(float(r["low"]), 2),
            "close": round(float(r["close"]), 2), "rsi": round(ra, 0),
            "tz": str(r["t"]), "l": lln, "l5": l5, "gap": gap, "vol": vb, "l43": l43, "pre_l43": pre_l43,
            "sharp": sharp, "dv_m": round(dv / 1e6, 1), "age_days": age,
            "tier": tier, "score": int(min(score, 100)), "atoms": atoms, "sc_super": sc_super,
        })

    pri = {"premium": 0, "strong": 1, "medium": 2}
    from price_zones import classify as _pz
    for r in out:
        z = _pz(r["close"])
        r["score"] = max(0, min(r["score"] + z["score_delta"], 100))
        r["price_zone"] = z["zone"]; r["zone_emoji"] = z["emoji"]
        if z["zone"] in ("dead", "knife", "casino"):
            r["atoms"].append(f"{z['emoji']}{z['label']}")
    out.sort(key=lambda x: (x["age_days"], pri[x["tier"]], -x["score"]))
    return {"as_of": as_of, "count": len(out), "rows": out[:limit], "edge_note": _NOTE}


_NOTE = ("WYCKOFF SPRING — buy the shakeout, NOT the breakout. The spring (undercut of TR "
         "support, reclaimed = sellers trapped) is the validated low-risk entry; the breakout/"
         "SOS/markup-flip have NO edge (survivorship). w2_spring + RSI 35-45 + bull-T + non-VB: "
         "med +1.06%/win 54%/6-6yr. Premium = gap=G3-V (+2.87/win61) or l5=PS-R2L (+2.66); strong "
         "= L34/vol-B. Enter@spring next-open, stop below the spring low. Schematic #2 (no-spring "
         "test/LPS) does NOT validate — only the spring fires. RYAN 2026-05-07 spring → +22.5%.")


if __name__ == "__main__":
    import json
    print(json.dumps(wyckoff_spring_scan(max_age_days=10), indent=2)[:3500])
