"""
h1_bottom_scan.py — "1H-Confirmed Bottom" setup (2026-06-29). A 1D deep-oversold name
where the 1H timeframe printed a VX-climax (VIX spike, 1H RSI<30) followed by an RSI2
RECLAIM (R2X) — i.e. the intraday capitulation has climaxed AND turned, so the daily
oversold is BOTTOMING, not still falling.

Motivation: the 1D Wyckoff state-machine (w2_spring) is fragile/stuck and missed the
MRNA & RKLB Nov-2025 bottoms; the lower-TF STATE caught both (MRNA 1H RSI21→R2X,
RKLB 1H RSI26→R2X). This setup uses that STATE directly — flag-free, TF-bridging.

VALIDATED (1H event mapped to the day, 1D fwd_20d, dv>10M):
  1D-oversold(RSI<35) ALONE = −0.23% (falling knife). + 1H VX-climax+R2X reclaim:
    1D RSI<35 = +1.07%/win54/6-6yr · 1D RSI<30 = +1.38%/win55/6-6yr (MFE+8.0>MAE−6.8).
  The 1H confirmation FLIPS the knife into an edge (−0.23 → +1.38), 6/6yr, additive.
  1H signal alone (any 1D RSI) is weak (+0.33/3yr) — the CONFLUENCE is what works.

The 1H VX-climax+R2X events are precomputed to onehour_capit.json (refresh periodically;
the live 1H DB is too slow to scan per request). Bypasses the broken w2_spring flag —
STATE > engineered flag, the session's law.

READ-ONLY on bars.
"""
from __future__ import annotations
import json
import os


def _load_h1() -> dict:
    path = os.path.join(os.path.dirname(__file__), "onehour_capit.json")
    try:
        with open(path) as f:
            return json.load(f).get("events", {})
    except Exception:
        return {}


def h1_bottom_scan(max_age_days: int = 6, dv_floor: float = 3_000_000,
                   limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    h1 = _load_h1()
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        rows = a.execute(f"""
            WITH flagged AS (
                SELECT *,
                    CASE WHEN sig_l3 = 1 AND sig_l4 = 1 AND close < open
                              AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                         THEN 1 ELSE 0 END AS l22c,
                    CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open
                              AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                         THEN 1 ELSE 0 END AS l43c
                FROM (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                      FROM bars WHERE close > 5) WHERE rn = 1
            ),
            windowed AS (
                SELECT *,
                    SUM(l22c) OVER (PARTITION BY ticker ORDER BY date
                                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pre_l22n,
                    SUM(l43c) OVER (PARTITION BY ticker ORDER BY date
                                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pre_l43n
                FROM flagged
            )
            SELECT universe, ticker, date, coalesce(t_sig,'') AS t, coalesce(z_sig,'') AS z,
                   coalesce(l_sig,'') AS l,
                   rsi_14, coalesce(bar_line5,'') AS l5, close, close * volume AS dv,
                   CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open THEN 1 ELSE 0 END AS l43,
                   coalesce(pre_l22n, 0) AS pre_l22n, coalesce(pre_l43n, 0) AS pre_l43n,
                   coalesce(wt_valid_tr,0) AS vtr, coalesce(wt_support,0) AS wt_sup,
                   coalesce(wt_resistance,0) AS wt_res
            FROM windowed
            WHERE rsi_14 < 35 AND vol_bucket <> 'VB'
              AND NOT (sig_bias_dn = 1 OR sig_vol_5x = 1 OR sig_vol_10x = 1 OR sig_vol_20x = 1)
              AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
              AND avg_vol_20d > 0 AND close * volume >= {dv_floor}
            ORDER BY ticker, date DESC
        """).fetchdf()
    finally:
        a.close()

    if not rows.empty:
        _u = {"sp500": 0, "nasdaq": 1, "russell2k": 2}
        rows = rows.assign(_p=rows["universe"].map(lambda u: _u.get(str(u), 9)))
        rows = (rows.sort_values(["ticker", "date", "_p"], ascending=[True, False, True])
                    .drop_duplicates(["ticker", "date"], keep="first")
                    .drop(columns="_p").reset_index(drop=True))

    from datetime import date as _d
    aod = _d.fromisoformat(as_of)
    out, seen = [], set()
    for _, r in rows.iterrows():
        tk = str(r["ticker"])
        day = str(r["date"])[:10]
        # require the 1H VX-climax+R2X confirmation on this day
        if day not in h1.get(tk, ()):
            continue
        if tk in seen:
            continue
        seen.add(tk)
        rsi = float(r["rsi_14"]) if r["rsi_14"] is not None else None
        ll = str(r["l"] or "")
        deep = rsi is not None and rsi < 30
        t_reclaim = bool(str(r["t"] or ""))
        # tiers from the validation: premium = 1D RSI<30 (+1.38/6yr); strong = RSI30-35 (+1.07).
        l43 = bool(int(r["l43"]) if r["l43"] is not None else 0)   # L43 VSA confluence (oversold-reversal lift)
        # pre-absorption booster (2026-06-30): controlled-vol L43/L22 absorption in the prior
        # 5 bars. Verified additive on the 1D-oversold frame: + pre-L43 +3.67%/PF1.62.
        pre_l43 = int(r["pre_l43n"] or 0) > 0
        pre_l22 = int(r["pre_l22n"] or 0) > 0
        tier = "premium" if deep else "strong"
        score = 55 + (20 if deep else 0) + (8 if t_reclaim else 0) \
            + (6 if ll in ("L12", "L46") else 0) + (8 if l43 else 0) \
            + (10 if pre_l43 else 0) + (5 if pre_l22 else 0)
        atoms = ["1H·VXclimax→R2X", f"1D·RSI{rsi:.0f}{'·deep' if deep else ''}" if rsi is not None else "1D·RSI?"]
        if t_reclaim:
            atoms.append(str(r["t"]))
        if ll:
            atoms.append(ll)
        if l43:
            atoms.append("L43✓absorbed")
        if pre_l43:
            atoms.append("🟢L43-base")
        if pre_l22:
            atoms.append("🧱L22-absorb")
        # SC-SUPER (2026-07-03): H1-bottom within ±5% of the Wyckoff range support — validated
        # band-plateau, median +0.13→+1.10, flips to 6/6yr, '22 −1.09→+0.26 (mean cost ~0.7pp).
        from wyc_zone import sc_zone
        sc_super = sc_zone(r["close"], r["wt_sup"], r["wt_res"], r["vtr"])
        if sc_super:
            atoms.append("🌀SC-SUPER"); score = min(score + 8, 100)
        out.append({
            "ticker": tk, "universe": str(r["universe"]),
            "signal_date": day, "t_sig": str(r["t"] or ""), "z_sig": str(r["z"] or ""), "l_sig": ll,
            "close": round(float(r["close"]), 2) if r["close"] is not None else None,
            "rsi": round(rsi, 0) if rsi is not None else None,
            "h1_confirm": True, "l43": l43, "pre_l43": pre_l43, "pre_l22": pre_l22, "sc_super": bool(sc_super),
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "age_days": (aod - _d.fromisoformat(day)).days,
            "tier": tier, "score": int(max(0, min(score, 100))), "atoms": atoms,
        })

    from price_zones import classify as _pz
    for r in out:
        z = _pz(r["close"])
        r["score"] = max(0, min(r["score"] + z["score_delta"], 100))
        r["price_zone"] = z["zone"]; r["zone_emoji"] = z["emoji"]
        if z["zone"] in ("dead", "knife", "casino"):
            r["atoms"].append(f"{z['emoji']}{z['label']}")
    out.sort(key=lambda x: ({"premium": 0, "strong": 1}[x["tier"]], -x["score"], x["age_days"]))
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
        "h1_as_of": _load_h1() and as_of,
        "edge_note": ("1H-confirmed bottom: a 1D deep-oversold name (RSI<35) whose 1H TF printed a "
                      "VX-climax (RSI<30) then an R2X reclaim — intraday capitulation climaxed AND "
                      "turned. The 1H confirmation flips the 1D-oversold knife into an edge "
                      "(−0.23% → +1.38%): RSI<35 +1.07%/win54/6yr, RSI<30 +1.38%/win55/6yr. "
                      "Flag-free (bypasses broken w2_spring). Caught MRNA & RKLB Nov-25 bottoms. "
                      "Momentum/swing — small size, stop ~−7%; 1H cache refresh periodic."),
    }


if __name__ == "__main__":
    print(json.dumps(h1_bottom_scan(max_age_days=10), indent=2)[:2200])
