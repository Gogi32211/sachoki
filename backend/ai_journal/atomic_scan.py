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
        # disp_ratio = |open − prev_close| / ATR (overnight displacement in ATR units) —
        # the validated gap×RSI driver (2026-06-28): the edge is in the sweet-spot
        # 0.5–1.5·ATR × RSI<45, NOT in the raw G2/G3 label. Bounded-window lag = fast.
        rows = a.execute(f"""
            WITH base AS (
                SELECT universe, ticker, date, t_sig, close, open, atr_14, rsi_14,
                       coalesce(l_sig,'') AS l, bar_gap_class AS gap, vol_bucket AS vol,
                       full_suffix AS sfx, bar_line5, close_suffix, volume, avg_vol_20d,
                       coalesce(wt_valid_tr,0) AS vtr, coalesce(wt_support,0) AS wt_sup,
                       coalesce(wt_resistance,0) AS wt_res,
                       CASE WHEN sig_l3 = 1 AND sig_l4 = 1 AND close < open
                                 AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                            THEN 1 ELSE 0 END AS l22c,
                       CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open
                                 AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                            THEN 1 ELSE 0 END AS l43c,
                       lag(close) OVER (PARTITION BY universe, ticker ORDER BY date) AS prev_close
                FROM bars
                WHERE date >= DATE '{as_of}' - INTERVAL {int(max_age_days) + 30} DAY
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
            SELECT universe, ticker, date, t_sig, close, rsi_14, l, gap, vol, sfx,
                   vtr, wt_sup, wt_res,
                   CASE WHEN regexp_matches(bar_line5, 'R2L') THEN 1 ELSE 0 END AS r2l,
                   close * volume AS dv,
                   coalesce(pre_l22n, 0) AS pre_l22n, coalesce(pre_l43n, 0) AS pre_l43n,
                   CASE WHEN atr_14 > 0 AND prev_close IS NOT NULL
                        THEN abs(open - prev_close) / atr_14 END AS disp_ratio
            FROM windowed
            WHERE t_sig IN ({ph}) AND close_suffix = 'O' AND gap IN ('G2', 'G3')
              AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
              AND avg_vol_20d > 0 AND close * volume >= {dv_floor}
              AND vol <> 'VB'
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
        # Scoring re-weighted by validated atom-edge (full-DB, fwd_20d vs base +0.76%):
        #   RSI<40 +1.08pp (strongest) · G3 +0.51 · vol=B +0.36 · R2L +0.26 · EO/wick≈0.
        #   Premium combo R2L+G3+RSI<45 = +2.45%, win 60%, fail 10.5% (3× base).
        rsi_v = float(r["rsi_14"]) if r["rsi_14"] is not None else None
        ratio = float(r["disp_ratio"]) if r["disp_ratio"] is not None else None
        # gap×RSI interaction (2026-06-28, validated on THIS Atomic pool):
        #   sweet (disp 0.5–1.5·ATR) × RSI 25–35 = +2.45/win58/6yr/risk1.59 (peak);
        #   sweet × RSI 35–45 = +1.26/6yr; sweet × RSI≥45 = +0.08 (dead);
        #   disp >1.5·ATR = exhaustion (+0.55/3yr); RSI<25 = falling-knife (inconsistent).
        sweet   = ratio is not None and 0.5 <= ratio < 1.5
        exhaust = ratio is not None and ratio >= 1.5
        knife   = rsi_v is not None and rsi_v < 25
        atoms = ["close=O"]                             # base (always present)
        score = 40
        if knife:
            atoms.append(f"RSI{rsi_v:.0f}·falling-knife"); score += 14   # good median, inconsistent
        elif rsi_v is not None and rsi_v < 40:
            atoms.append(f"RSI{rsi_v:.0f}·oversold"); score += 20        # single strongest atom
        elif rsi_v is not None and rsi_v < 45:
            atoms.append("RSI<45"); score += 10
        # gap quality by displacement sweet-spot (replaces flat G3 boost)
        if sweet:
            atoms.append(f"gap·sweet({ratio:.1f}×ATR)"); score += 16
        elif exhaust:
            atoms.append(f"gap·EXHAUST({ratio:.1f}×ATR)"); score += 6     # blow-off tail, weaker
        elif r["gap"] == "G3":
            atoms.append("G3"); score += 8                               # G3 w/o ratio fallback
        # validated premium interaction: sweet × RSI band
        if sweet and rsi_v is not None and 25 <= rsi_v < 35:
            atoms.append("★premium"); score += 12                        # +2.45/win58/6yr
        elif sweet and rsi_v is not None and 35 <= rsi_v < 45:
            atoms.append("★strong"); score += 6                          # +1.26/6yr
        # ⚡ G3-Abs "contradiction bar" (2026-07-13, contra.py): same-bar G3 gap-up + this scan's
        # weak close = the gap got ABSORBED (buyers gapped, sellers unloaded, price held).
        # G3∧Atomic +4.24/med+2.33/win56/PF1.83/5-6yr ($21+) vs Atomic alone +1.60/med+0.13 —
        # the single best Atomic slice. Mirrors edge_replay's E_g3abs mask.
        if r["gap"] == "G3" and rsi_v is not None and rsi_v < 45 and px >= 21:
            atoms.append("⚡G3-Abs"); score += 10
            atoms.append("R2L"); score += 15
        if r["vol"] == "B":
            atoms.append("vol=B"); score += 12
        # entry-L sharpener (2026-06-27 audit): L5/L46 entry +0.84/5yr vs L25 +0.38/3yr (drags)
        _l = str(r["l"] or "")
        if _l in ("L5", "L46"):
            atoms.append(_l); score += 6
        elif _l == "L25":
            atoms.append("L25·weak"); score -= 4
        if sfx[:1] == "E":
            atoms.append("EO"); score += 5                         # de-emphasized (≈0 edge)
        if "D" in sfx[1:2]:
            atoms.append("wick=D"); score += 3                     # de-emphasized (≈0 edge)
        if post_capit:
            atoms.append(f"🔥post-capit{int(dpc)}d"); score += 20   # the validated confluence boost
        # pre-absorption booster (2026-06-30): a controlled-vol (0.7–2.0×vol20) L43 (demand)
        # or L22 (supply-exhaustion) absorption in the prior 5 bars = a higher-quality base
        # the weak-close gap-up launches from. Verified additive (path-sim trail25/60): Atomic
        # + pre-L43 +3.53%/PF1.64, + pre-L22 +2.86/PF1.50/6-6yr (base +2.26/PF1.37).
        pre_l43 = int(r["pre_l43n"] or 0) > 0
        pre_l22 = int(r["pre_l22n"] or 0) > 0
        if pre_l43:
            atoms.append("🟢L43-base"); score += 10
        if pre_l22:
            atoms.append("🧱L22-absorb"); score += 5
        # SC-SUPER (2026-07-03): Atomic within ±5% of the Wyckoff range support — validated
        # band-plateau, mean-neutral (+1.98→~+2.0) but median +0.00→+1.08 and TR +0.07→+1.01.
        from wyc_zone import sc_zone
        sc_super = sc_zone(r["close"], r["wt_sup"], r["wt_res"], r["vtr"])
        if sc_super:
            atoms.append("🌀SC-SUPER"); score += 8
        score = max(0, min(score, 100))
        tk = str(r["ticker"])
        cand = {
            "ticker": tk, "universe": str(r["universe"]),
            "signal_date": str(r["date"])[:10], "t_sig": str(r["t_sig"]),
            "l_sig": _l,
            "close": round(px, 2) if r["close"] is not None else None,
            "rsi": round(float(r["rsi_14"]), 0) if r["rsi_14"] is not None else None,
            "gap": str(r["gap"]), "vol": str(r["vol"]), "suffix": sfx,
            "disp_atr": round(ratio, 2) if ratio is not None else None,
            "gap_band": ("sweet" if sweet else "exhaust" if exhaust else "g3"),
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "score": int(score), "atoms": atoms, "age_days": None,
            "post_capit": bool(post_capit), "capit_age": int(dpc) if post_capit else None,
            "pre_l43": bool(pre_l43), "pre_l22": bool(pre_l22), "sc_super": bool(sc_super),
        }
        if tk not in best or score > best[tk]["score"]:
            best[tk] = cand

    from price_zones import classify as _pz
    # breadth-regime gate (validated 2026-07-01, project_atomic_edge_validated): the weak-close
    # gap-up is a CAPITULATION-REVERSION edge — it works in FEAR (low breadth) and dies in
    # euphoria. Boost fear-dated signals; flag euphoria ones as muted. (This is edge-SPECIFIC and
    # is the OPPOSITE of generic "stand down in risk-off" size advice.)
    try:
        import market_breadth as _mb
        _br = _mb._load()
    except Exception:
        _br = None
    for r in best.values():
        z = _pz(r["close"])
        r["score"] = max(0, min(r["score"] + z["score_delta"], 100))
        r["price_zone"] = z["zone"]; r["zone_emoji"] = z["emoji"]
        if z["zone"] in ("dead", "knife", "casino"):
            r["atoms"].append(f"{z['emoji']}{z['label']}")
        b = _mb.breadth_for_date(r["signal_date"], _br) if _br else None
        r["breadth"] = round(b, 2) if b is not None else None
        r["breadth_regime"] = _mb.regime_label(b) if _br else "UNKNOWN"
        _fear = b is not None and b < _mb.RISK_OFF_CUT
        if _fear:                                        # fear = where the edge lives
            boost = 15 if b < _mb.DEEP_FEAR_CUT else 10
            r["score"] = min(r["score"] + boost, 100)
            r["atoms"].append("🩸deep-fear" if b < _mb.DEEP_FEAR_CUT else "🩸fear")
        elif b is not None:                              # euphoria = edge muted
            r["score"] = max(0, r["score"] - 8)
            r["atoms"].append("😀euphoria·muted")
        # Atomic-R — the OOS-validated selective edge (project_atomic_edge_validated):
        # structural quality = vol=B + price $21-89 (the "quality zone", $8-21 dead);
        # active only in fear (risk-off). Backtest: mean +4.37 / PF 1.84 / 5-6 of 6yr.
        r["is_atomicR_quality"] = (str(r.get("vol")) == "B"
                                   and r["close"] is not None and 21 <= r["close"] <= 89)
        r["is_atomicR"] = bool(r["is_atomicR_quality"] and _fear)   # quality AND fear-gated
        if r["is_atomicR"]:
            r["atoms"].append("🎯Atomic-R")
    out = sorted(best.values(), key=lambda x: x["score"], reverse=True)[:limit]
    # age relative to as_of
    from datetime import date as _d
    ad = _d.fromisoformat(as_of)
    for c in out:
        c["age_days"] = (ad - _d.fromisoformat(c["signal_date"])).days

    # ⚡ CHARGED energy booster (validated 2026-07-06). Badge-only.
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

    # regime gate
    try:
        from . import regime as _reg
        reg = _reg.compute_regime(as_of)
    except Exception:
        reg = {"label": "NEUTRAL", "score": None, "conv_mult": 1.0, "breadth": {}}

    atomic_regime = _mb.current_regime(_br) if _br else {"regime": "UNKNOWN", "breadth": None, "risk_off": None}

    return {
        "as_of": as_of, "count": len(out), "rows": out,
        "regime": {"label": reg["label"], "score": reg["score"],
                   "conv_mult": reg["conv_mult"], "breadth": reg.get("breadth", {})},
        "atomic_regime": atomic_regime,   # edge-specific breadth gate (fear=go, euphoria=stand down)
        "edge_note": ("weak-close gap-up (≥$16 OR rescued by a recent B+ capit; no-VB). "
                      "gap×RSI (2026-06-28, this pool): ★premium = displacement 0.5–1.5·ATR × "
                      "RSI 25–35 (+2.45%/win58/6yr/risk1.59); ★strong = same band × RSI 35–45 "
                      "(+1.26/6yr); sweet×RSI≥45 dead (+0.08); >1.5·ATR exhaustion, RSI<25 "
                      "falling-knife (flagged). 🔥post-capit = the premium Capit→Atomic confluence "
                      "(rich+capit≤10d: win 67%, med +4.24 vs +1.41 baseline) — sorted to the top. "
                      "🩸BREADTH-GATE (validated OOS 2026-07-01): this is a CAPITULATION-REVERSION edge — "
                      "it works in FEAR (breadth<0.5: mean +4.7/PF1.9/6-of-6yr OOS) and DIES in euphoria "
                      "(breadth>0.5: mean −0.03, dead). Fear-dated picks boosted, euphoria muted. NOTE: this "
                      "is edge-specific and is the OPPOSITE of generic 'stand down in risk-off' size advice."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(atomic_scan(), indent=2)[:2000])
