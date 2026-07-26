"""
d_l1_scan.py — live scanner for the validated D+L1→P "bear-trap reversal" LONG setup
(2026-06 research). A D-signal (bearish PREDN EMA cross-down = breakdown) on the SAME
1D bar as L1 (a bullish VSA absorption volume-line) = a bear-trap; then a P-signal
(EMA reclaim) within the next 1-5 bars CONFIRMS the reversal → LONG at the P bar.

VALIDATED (true bar-by-bar path-sim, entry next-open, stop −12%, 20-bar):
  D+L1 alone        → EXP +0.97%, win 51%, 5/6yr
  D+L1, NO P after  → EXP −0.86%, win 43%, 1/6yr   (the failures — breakdown continues)
  D+L1 → P (enter@D)→ EXP +2.45%, win 57%, 6/6yr
  D+L1 → P (enter@P)→ EXP +3.24%, win 59%, 6/6yr   ← the actionable setup
The P-confirmation separates the real reversals from continued breakdowns. ~58% of
D+L1 get a P within 5 bars; only those are tradeable. Premium tier = RSI<40 (oversold).

READ-ONLY on bars — surfaces candidates, opens nothing.
"""
from __future__ import annotations
import numpy as np

_PWIN = 5   # a P confirmation must appear within this many bars after the D+L1


def d_l1_scan(max_age_days: int = 4, dv_floor: float = 500_000, limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        # pull a recent window wide enough to hold a D+L1 then a P up to _PWIN bars later
        recent = a.execute(f"""
            SELECT universe, ticker, date, t_sig, z_sig, l_sig, rsi_14, cci_20,
                   close, volume, avg_vol_20d, vol_bucket, bar_line5,
                   coalesce(price_gt_200, 0) AS gt200,
                   coalesce(sig_any_d, 0) AS d, coalesce(sig_l1, 0) AS l1,
                   coalesce(sig_any_p, 0) AS p,
                   CASE WHEN sig_l3 = 1 AND sig_l4 = 1 AND close < open
                             AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                        THEN 1 ELSE 0 END AS l22c,
                   CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open
                             AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                        THEN 1 ELSE 0 END AS l43c,
                   CASE WHEN sig_bias_dn = 1 OR sig_vol_5x = 1 OR sig_vol_10x = 1
                             OR sig_vol_20x = 1 THEN 1 ELSE 0 END AS supp,
                   coalesce(wt_valid_tr,0) AS vtr, coalesce(wt_support,0) AS wt_sup,
                   coalesce(wt_resistance,0) AS wt_res
            FROM bars
            WHERE date >= DATE '{as_of}' - INTERVAL {int(max_age_days) + _PWIN + 20} DAY
              AND close > 0
            ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()

    # Dedup to ONE row per (ticker, date). A ticker in 2-3 indices otherwise yields
    # 2-3× interleaved rows, which breaks the per-bar D→L1→P window look-ahead below
    # (the "next _PWIN bars" would span ~half the intended days). Keep sp500 > nasdaq > r2k.
    if not recent.empty:
        _upri = {"sp500": 0, "nasdaq": 1, "russell2k": 2}
        recent = recent.assign(_upri=recent["universe"].map(lambda u: _upri.get(str(u), 9)))
        recent = (recent.sort_values(["ticker", "date", "_upri"])
                        .drop_duplicates(["ticker", "date"], keep="first")
                        .drop(columns="_upri")
                        .reset_index(drop=True))

    from datetime import date as _d
    aod = _d.fromisoformat(as_of)
    out, seen = [], set()
    for tk, g in recent.groupby("ticker", sort=False):
        g = g.reset_index(drop=True)
        n = len(g)
        D = g["d"].fillna(0).astype(int).to_numpy()
        L1 = g["l1"].fillna(0).astype(int).to_numpy()
        P = g["p"].fillna(0).astype(int).to_numpy()
        L22C = g["l22c"].fillna(0).astype(int).to_numpy()
        L43C = g["l43c"].fillna(0).astype(int).to_numpy()
        # Walk newest→oldest so the per-ticker dedup (seen) keeps the FRESHEST
        # D+L1 event, not the stalest one. The P look-ahead below scans forward
        # (j > i) regardless of iteration direction, so reversing is safe.
        for i in range(n - 1, -1, -1):
            if not (D[i] == 1 and L1[i] == 1):
                continue
            r = g.iloc[i]
            if (r["close"] is None or float(r["close"]) < 5 or r["vol_bucket"] == "VB"
                    or int(r["supp"] or 0) == 1            # universal suppressor guard (bias_dn/vol-extreme)
                    or not (r["avg_vol_20d"] and r["avg_vol_20d"] > 0)
                    or float(r["close"]) * float(r["volume"]) < dv_floor):
                continue
            dl1_date = str(r["date"])[:10]
            dl1_age = (aod - _d.fromisoformat(dl1_date)).days
            if dl1_age > max_age_days + _PWIN:
                continue
            # look for a P confirmation in the next 1.._PWIN bars
            p_i = None
            for j in range(i + 1, min(i + 1 + _PWIN, n)):
                if P[j] == 1:
                    p_i = j
                    break
            if p_i is not None:
                p_date = str(g.iloc[p_i]["date"])[:10]
                p_age = (aod - _d.fromisoformat(p_date)).days
                if p_age > max_age_days:
                    continue                      # the entry trigger is too old
                status = "confirmed"; bars_to_p = p_i - i; ebar = g.iloc[p_i]
            else:
                # no P yet — only a WATCH if the D+L1 is recent and the window is still open
                bars_left = _PWIN - (n - 1 - i)
                if dl1_age > max_age_days or (n - 1 - i) >= _PWIN:
                    continue
                status = "watch"; bars_to_p = None; ebar = r

            tk_s = str(tk)
            if tk_s in seen:
                continue
            seen.add(tk_s)
            rsi = float(ebar["rsi_14"]) if ebar["rsi_14"] is not None else None
            gt200 = int(r["gt200"] or 0)
            if rsi is not None and rsi < 40:
                tier = "premium"
            elif str(r["universe"]) == "sp500":
                tier = "strong"
            else:
                tier = "base"
            base = 60 if status == "confirmed" else 40
            score = base + (25 if tier == "premium" else 0) + (10 if tier == "strong" else 0) + (10 if gt200 else 0)
            # pre-absorption booster (2026-06-30): controlled-vol L43 (demand) / L22 (supply-
            # exhaustion) absorption in the 5 bars before the D+L1 breakdown. Verified additive:
            # D+L1 + pre-L43 +3.61%/PF1.69/6-6yr, + pre-L22 +3.14/PF1.61/6-6yr.
            _lo5 = max(0, i - 5)
            pre_l43 = bool(L43C[_lo5:i].sum() > 0)
            pre_l22 = bool(L22C[_lo5:i].sum() > 0)
            if pre_l43:
                score += 10
            if pre_l22:
                score += 5
            # SC-SUPER (2026-07-03): D+L1 firing within ±5% of the Wyckoff range support —
            # validated band-plateau, median +0.08→+1.33, TR +0.17→+0.86, 2×-slip-safe.
            from wyc_zone import sc_zone
            sc_super = sc_zone(r["close"], r["wt_sup"], r["wt_res"], r["vtr"])
            if sc_super:
                score += 8

            atoms = ["D·breakdown", "L12·absorb"]   # sig_l1 on D bars is always L12 (L1&L2)
            atoms.append("P·confirmed" if status == "confirmed" else "P·pending")
            if rsi is not None and rsi < 40:
                atoms.append(f"RSI{rsi:.0f}·oversold")
            if gt200:
                atoms.append("uptrend")
            if pre_l43:
                atoms.append("🟢L43-base")
            if pre_l22:
                atoms.append("🧱L22-absorb")
            if sc_super:
                atoms.append("🌀SC-SUPER")
            out.append({
                "ticker": tk_s, "universe": str(r["universe"]),
                "signal_date": dl1_date, "p_date": (str(g.iloc[p_i]["date"])[:10] if p_i is not None else None),
                "status": status, "bars_to_p": bars_to_p,
                "pre_l43": pre_l43, "pre_l22": pre_l22,
                "t_sig": str(ebar["t_sig"]) if ebar["t_sig"] else "",
                "l_sig": str(r["l_sig"]) if r["l_sig"] else "",
                "close": round(float(ebar["close"]), 2) if ebar["close"] is not None else None,
                "rsi": round(rsi, 0) if rsi is not None else None,
                "cci": round(float(ebar["cci_20"]), 0) if ebar["cci_20"] is not None else None,
                "dv_m": round(float(r["close"]) * float(r["volume"]) / 1e6, 1),
                "age_days": (aod - _d.fromisoformat(str(ebar["date"])[:10])).days,
                "tier": tier, "score": int(min(score, 100)), "atoms": atoms, "sc_super": sc_super,
            })

    from price_zones import classify as _pz
    for r in out:
        z = _pz(r["close"])
        r["score"] = max(0, min(r["score"] + z["score_delta"], 100))
        r["price_zone"] = z["zone"]; r["zone_emoji"] = z["emoji"]
        if z["zone"] in ("dead", "knife", "casino"):
            r["atoms"].append(f"{z['emoji']}{z['label']}")
    out.sort(key=lambda x: (x["status"] != "confirmed",
                            {"premium": 0, "strong": 1, "base": 2}[x["tier"]],
                            -x["score"], x["age_days"]))
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
        "as_of": as_of, "count": len(out),
        "confirmed": sum(1 for r in out if r["status"] == "confirmed"),
        "rows": out[:limit],
        "edge_note": ("D+L1→P bear-trap reversal: a breakdown (D) absorbed by an L1 VSA "
                      "volume-line, CONFIRMED by a P-signal (EMA reclaim) within 5 bars → LONG. "
                      "Validated true path-sim (entry@P, stop −12%, 20-bar): EXP +3.24%, win 59%, "
                      "6/6yr (survived 2022). 'confirmed' = P already fired; 'watch' = D+L1 fresh, "
                      "P pending. No-P D+L1 = −0.86% (avoid). Small edge — paper-track first."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(d_l1_scan(max_age_days=8), indent=2)[:2500])
