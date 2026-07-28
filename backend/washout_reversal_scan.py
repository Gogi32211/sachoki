"""
washout_reversal_scan.py — live scanner for the "Washout / Capitulation Reversal" setup
(2026-06-29 research). Buy a QUALITY (beta-capped) name oversold in a VIX-spike market
panic — the drop is "borrowed fear", not real damage, so it reverts. The first
regime/beta-conditioned Edge setup.

SIGNATURE (the capitulation bar): RSI 20–36 + VIX-elevated (bar_line5 has VX spike or VR
range) + RSI2-extreme (R2L still-oversold / R2X reclaim-turn) + non-VB + a bull T-reclaim.
Template = MELI's Apr-7-2025 tariff-crash bottom (T5L5, RSI34, VX-PS-R2L).

VALIDATED (whole-universe, dv>15M, fwd_20d):
  beta ≤ 1.5 (tradeable) = +1.31%/win56/6-6yr  ·  beta > 1.5 (spec) = −0.69%/1-6yr → EXCLUDE.
  CORE: beta 0.6–1.5 + RSI<30 + L12/L46 absorption = +1.73%/win57%/6-6yr
        (per-yr 2021:+1.4 2022:+1.5 bear-survivor 2023:+1.2 2024:+2.0 2025:+2.2 2026:+2.0).
  +VX spike = +2.62%/win60/5yr.  SECTOR BOOSTER (band+RSI<30): ENERGY +4.42%/win68/6yr
  (MAE −3.7), SEMIS +4.50%/win73. KILLERS (excluded by design): beta>1.5 spec, and
  idiosyncratic China (+0.06) / biotech (VX INVERTS to −0.37) / defensive HC (no amplitude).

Beta = real market beta vs an equal-weight median-return market proxy, precomputed to
washout_beta.json (refresh periodically). The project's beta_score/beta_zone cols are a
proprietary engine, NOT statistical beta — not used here.

READ-ONLY on bars — surfaces candidates, opens nothing.
"""
from __future__ import annotations
import json
import os

# Sector boosters (commodity / semi cycle mean-reversion on top of the general edge)
_ENERGY = {"SLB", "HAL", "WFRD", "BKR", "TDW", "NE", "RIG", "VAL", "PTEN", "RES", "OIS",
           "CLB", "NOG", "CRC", "CHRD", "SM", "PARR", "CRGY", "RRC", "SDRL", "CNQ", "SU",
           "TTE", "CVI", "HES", "DVN", "FANG", "OXY", "APA", "EOG", "COP", "XOM", "CVX",
           "PSX", "VLO", "MPC", "HP", "OLN", "HUN", "DOW", "NTR", "LBRT", "WTI"}
_SEMIS = {"KLAC", "LRCX", "AMAT", "NVDA", "AMD", "MU", "INTC", "TXN", "ADI", "MCHP", "ON",
          "QCOM", "AVGO", "MRVL", "NXPI", "SWKS", "QRVO", "TER", "ENTG", "MPWR", "WOLF",
          "SLAB", "AMKR", "INTU", "CRM", "NOW", "ADBE"}

_BETA_LO, _BETA_HI = 0.6, 1.5


def _load_beta() -> dict:
    path = os.path.join(os.path.dirname(__file__), "washout_beta.json")
    try:
        with open(path) as f:
            return json.load(f).get("beta", {})
    except Exception:
        return {}


def _load_h1() -> dict:
    """1H VX-climax→R2X confirmation events (shared with h1_bottom_scan)."""
    path = os.path.join(os.path.dirname(__file__), "onehour_capit.json")
    try:
        with open(path) as f:
            return json.load(f).get("events", {})
    except Exception:
        return {}


def washout_reversal_scan(max_age_days: int = 5, dv_floor: float = 3_000_000,
                          limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    betamap = _load_beta()
    h1map = _load_h1()
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        rows = a.execute(f"""
            WITH dedup AS (
                SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                FROM bars WHERE close > 5
            ),
            flagged AS (
                SELECT *,
                    CASE WHEN sig_l3 = 1 AND sig_l4 = 1 AND close < open
                              AND volume / NULLIF(avg_vol_20d,0) BETWEEN 0.7 AND 2.0
                         THEN 1 ELSE 0 END AS l22c
                FROM dedup WHERE rn = 1
            ),
            windowed AS (
                SELECT *,
                    SUM(l22c) OVER (PARTITION BY ticker ORDER BY date
                                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pre_l22n
                FROM flagged
            )
            SELECT universe, ticker, date, t_sig, coalesce(l_sig,'') AS l, rsi_14,
                   coalesce(bar_line5,'') AS l5, coalesce(bar_gap_class,'') AS gap,
                   close, close * volume AS dv, coalesce(pre_l22n,0) AS pre_l22n,
                   coalesce(wt_valid_tr,0) AS vtr, coalesce(wt_support,0) AS wt_sup,
                   coalesce(wt_resistance,0) AS wt_res,
                   CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open THEN 1 ELSE 0 END AS l43
            FROM windowed
            WHERE rsi_14 BETWEEN 20 AND 36 AND vol_bucket <> 'VB'
              AND NOT (sig_bias_dn = 1 OR sig_vol_5x = 1 OR sig_vol_10x = 1 OR sig_vol_20x = 1)
              AND t_sig IS NOT NULL AND t_sig <> ''
              AND (bar_line5 LIKE '%VX%' OR bar_line5 LIKE '%VR%')
              AND bar_line5 LIKE '%R2%'
              AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
              AND avg_vol_20d > 0 AND close * volume >= {dv_floor}
            ORDER BY ticker, date DESC
        """).fetchdf()
    finally:
        a.close()

    # dedup to one row per (ticker, date), keep sp500 > nasdaq > r2k
    if not rows.empty:
        _u = {"sp500": 0, "nasdaq": 1, "russell2k": 2}
        rows = rows.assign(_p=rows["universe"].map(lambda u: _u.get(str(u), 9)))
        rows = (rows.sort_values(["ticker", "date", "_p"], ascending=[True, False, True])
                    .drop_duplicates(["ticker", "date"], keep="first")
                    .drop(columns="_p").reset_index(drop=True))

    from datetime import date as _d
    aod = _d.fromisoformat(as_of)
    out, seen = [], set()
    # 🕐 1H-DR confirmation (2026-07-28) — the SAME gate the Replay board and backtest use
    # (edge_replay.h1_dr_days: a 1H dual reclaim this session or the previous one, AND that day's
    # RS intact). Shown as an ATOM, not a hard filter, so every base fire stays visible and the
    # user opts in. NOT the older 🕐1H-confirm atom, which is the VX-climax→R2X bottom.
    try:
        from edge_replay import h1_dr_days as _h1dd
        _H1DR = _h1dd()
    except Exception:
        _H1DR = frozenset()
    for _, r in rows.iterrows():
        tk = str(r["ticker"])
        if tk in seen:
            continue
        beta = betamap.get(tk)
        # beta gate: must be known AND inside the tradeable band (exclude spec & unknown)
        if beta is None or not (_BETA_LO <= float(beta) <= _BETA_HI):
            continue
        seen.add(tk)
        beta = float(beta)
        rsi = float(r["rsi_14"]) if r["rsi_14"] is not None else None
        ll = str(r["l"] or "")
        l5 = str(r["l5"] or "")
        has_vx = "VX" in l5            # full VIX spike (climax) — sharpest
        r2x = "R2X" in l5             # RSI2 reclaim-turn
        absorb = ll in ("L12", "L46")  # the validated absorption-L
        sector = "energy" if tk in _ENERGY else "semis" if tk in _SEMIS else ""
        deep = rsi is not None and rsi < 30

        # tiers from the validation: core = RSI<30 + L12/L46 (+1.73/6yr);
        # premium adds the booster (VX climax OR energy/semis cycle, +2.6–4.4%).
        if deep and absorb and (has_vx or sector):
            tier = "premium"
        elif deep and absorb:
            tier = "strong"
        else:
            tier = "base"
        score = 50 \
            + (25 if tier == "premium" else 12 if tier == "strong" else 0) \
            + (8 if has_vx else 4) \
            + (8 if absorb else (-4 if ll == "L25" else 0)) \
            + (6 if deep else 0) \
            + (6 if sector else 0) \
            + (4 if r2x else 0)
        # 1H-confirmation: did the 1H TF print a VX-climax→R2X reclaim on this day? (+lift, esp. oversold)
        h1_confirm = str(r["date"])[:10] in h1map.get(tk, ())
        if h1_confirm:
            score += 6
        # L43 VSA confluence (2026-06-29): supply-absorbed green body — big lift on washout
        # (+0.57→+1.96%/6yr). sig_l6&sig_l4 + close≥open.
        l43 = bool(int(r["l43"]) if r["l43"] is not None else 0)
        if l43:
            score += 8
        # pre-L22 absorption booster (2026-06-30): an L22 (L3&L4 supply-exhaustion red
        # body, controlled vol 0.7–2.0×vol20) in the prior 5 bars. Lookahead-free: a
        # reversal preceded by an L22 absorption = +2.68%/PF1.44/5-6yr vs +1.01 without.
        pre_l22 = int(r["pre_l22n"] or 0) > 0
        if pre_l22:
            score += 6
        atoms = [f"β{beta:.2f}", ("VX·spike" if has_vx else "VR·elevated"),
                 ("R2X·reclaim" if r2x else "R2L·oversold"), ll or "L?", str(r["t_sig"])]
        if sector:
            atoms.append(f"⚡{sector}")
        if l43:
            atoms.append("L43✓absorbed")
        if pre_l22:
            atoms.append("🧱L22-absorb")
        if h1_confirm:
            atoms.append("🕐1H-confirm")
        if rsi is not None:
            atoms.append(f"RSI{rsi:.0f}{'·deep' if deep else ''}")
        if f"{tk}|{str(r['date'])[:10]}" in _H1DR:
            atoms.append("🕐DR")
        # SC-SUPER (2026-07-03): washout within ±5% of the Wyckoff range support — validated
        # band-plateau, median −0.56→+0.45, '22 +0.18→+1.19, TR +0.35→+0.5 (mean cost ~0.4pp).
        from wyc_zone import sc_zone
        sc_super = sc_zone(r["close"], r["wt_sup"], r["wt_res"], r["vtr"])
        if sc_super:
            atoms.append("🌀SC-SUPER"); score += 6
        out.append({
            "ticker": tk, "universe": str(r["universe"]),
            "signal_date": str(r["date"])[:10], "t_sig": str(r["t_sig"]),
            "l_sig": ll, "beta": round(beta, 2),
            "vx": bool(has_vx), "sector": sector, "h1_confirm": bool(h1_confirm), "l43": l43,
            "pre_l22": bool(pre_l22), "sc_super": bool(sc_super),
            "close": round(float(r["close"]), 2) if r["close"] is not None else None,
            "rsi": round(rsi, 0) if rsi is not None else None,
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "age_days": (aod - _d.fromisoformat(str(r["date"])[:10])).days,
            "tier": tier, "score": int(max(0, min(score, 100))), "atoms": atoms,
        })

    from price_zones import classify as _pz
    for r in out:
        z = _pz(r["close"])
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
    return {
        "as_of": as_of, "count": len(out), "rows": out[:limit],
        "beta_as_of": _load_beta() and as_of,
        "edge_note": ("Washout/capitulation reversal: buy a QUALITY (beta 0.6–1.5, spec >1.5 "
                      "EXCLUDED) name oversold (RSI<36) in a VIX-spike panic (VX/VR + RSI2) on a "
                      "T-reclaim, non-VB. CORE (RSI<30 + L12/L46) = +1.73%/win57/6-6yr (survived "
                      "2022). PREMIUM adds VX-climax or energy/semis cycle (+2.6–4.4%). NOT high-"
                      "beta — extreme beta = fragile spec (−0.69/1yr); biotech/China/defensives "
                      "excluded (idiosyncratic, VX inverts). Regime tool — best when breadth is "
                      "washed out; momentum, small size, stop ~−6%."),
    }


if __name__ == "__main__":
    print(json.dumps(washout_reversal_scan(max_age_days=8), indent=2)[:2500])
