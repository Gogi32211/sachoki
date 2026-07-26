"""
z11_t11_scan.py — live scanner for the "Z11 → T3/T5 → T11/T12" oversold-absorption
reversal (2026-06 research, the rare sequence-grammar edge that survived path-sim).

THESIS: a Z11 bearish-absorption bar while RSI14 is oversold (30-45) marks an absorbed
bottom; a T3 or T5 CONFIRM on the very next bar (momentum re-entry) followed by a T11 or
T12 RESOLUTION two bars after the Z11 closes the reversal. ENTER AT THE T11/T12 bar
(next-open). The resolution is NEVER the immediate next bar — it lands at exactly +2 from
the Z11, which is what makes it specific (and rare).

VALIDATED (2019-26, liquid, true path-sim: entry next-open after the T11/T12, −12% stop,
20-bar hold):
  Z11(os) → T3/T5 → T11/T12 : med +2.70% · mean +3.63% · clip25 +2.44% · win 62% · 6/6 yrs
  Best combo  T5 → T12      : +3.50% · win 71%   (sharpest)
  T11 resolutions           : +2.15-2.19% · win 60-65%
The edge is RSI-driven (plain Z11 = +0.27% noise; +RSI30-45 lifts it; L12 absorption on the
anchor adds little but sharpens). STATE predicts, SHAPE is noise. Distinct rare setup —
see [[project_absorption_reversal]] / [[project_z11l12_sequences]].

READ-ONLY on bars — surfaces candidates, opens nothing.
"""
from __future__ import annotations

_CONFIRM = frozenset({"T3", "T5"})       # bar -1: momentum re-entry that continues the walk
_RESOLVE = frozenset({"T11", "T12"})     # bar  0: the reversal resolution = ENTRY bar
_RSI_LO, _RSI_HI = 30.0, 45.0            # oversold band the sequence was validated in

# Anchor FAMILY (2026-06 systematic anchor scan — same grammar, different bear anchor).
# fwd_20d screen, oversold RSI30-45, →T3/T5→T11/T12:
#   Z11 med +2.04 win55 (original flagship)  · Z3 med +2.47 win58 top-3wk 12% (robust)
#   Z1G med +2.67 win59 BUT top-3wk 38% (cluster — market-panic→reversal play, not steady)
#   Z5  med +1.30 win54 (moderate third).  All survived 2022.
_ANCHORS = {
    "Z11": {"tier": "premium", "note": "flagship"},
    "Z3":  {"tier": "premium", "note": "robust (well-distributed, 5/6yr)"},
    "Z1G": {"tier": "dip",     "note": "strong but clustered — market-dip reversal play"},
    "Z5":  {"tier": "medium",  "note": "moderate third"},
}


def z11_t11_scan(max_age_days: int = 6, dv_floor: float = 2_000_000,
                 require_l12: bool = False, require_sharp_l: bool = False,
                 limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        lookback = int(max_age_days) + 20
        rows = a.execute(f"""
            SELECT universe, ticker, date, close, volume, rsi_14,
                   coalesce(t_sig, '') AS t, coalesce(z_sig, '') AS z,
                   coalesce(l_sig, '') AS l,
                   CASE WHEN sig_l3 = 1 AND sig_l4 = 1 AND close < open THEN 1 ELSE 0 END AS l22,
                   volume / NULLIF(avg_vol_20d, 0) AS vr,
                   CASE WHEN sig_bias_dn = 1 OR sig_vol_5x = 1 OR sig_vol_10x = 1
                             OR sig_vol_20x = 1 THEN 1 ELSE 0 END AS supp
            FROM bars
            WHERE date >= DATE '{as_of}' - INTERVAL {lookback} DAY AND close > 0
            ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()

    # dedup (ticker, date) — multi-index tickers otherwise compress the bar window
    if not rows.empty:
        _upri = {"sp500": 0, "nasdaq": 1, "russell2k": 2}
        rows = rows.assign(_upri=rows["universe"].map(lambda u: _upri.get(str(u), 9)))
        rows = (rows.sort_values(["ticker", "date", "_upri"])
                    .drop_duplicates(["ticker", "date"], keep="first")
                    .drop(columns="_upri").reset_index(drop=True))

    from datetime import date as _d
    aod = _d.fromisoformat(as_of)
    out, seen = [], set()

    for tk, g in rows.groupby("ticker", sort=False):
        if tk in seen:
            continue
        g = g.reset_index(drop=True)
        n = len(g)
        if n < 3:
            continue
        t = g["t"].to_numpy(object); z = g["z"].to_numpy(object); l = g["l"].to_numpy(object)
        rsi = g["rsi_14"].to_numpy(float); cl = g["close"].to_numpy(float)
        vol = g["volume"].to_numpy(float); supp = g["supp"].to_numpy(int)
        l22a = g["l22"].to_numpy(int); vra = g["vr"].to_numpy(float)
        dts = [str(x)[:10] for x in g["date"]]

        # Walk newest→oldest: a T11/T12 RESOLUTION (entry) at j, T3/T5 CONFIRM at j-1,
        # Z11 oversold ANCHOR at j-2 — three consecutive bars.
        for j in range(n - 1, 1, -1):
            if tk in seen:
                break
            if str(t[j]) not in _RESOLVE:
                continue
            entry_age = (aod - _d.fromisoformat(dts[j])).days
            if entry_age > max_age_days or entry_age < 0:
                continue
            if cl[j] < 5 or cl[j] * vol[j] < dv_floor:
                continue
            if supp[j] == 1:                            # universal suppressor guard (bias_dn/vol-extreme)
                continue
            if str(t[j - 1]) not in _CONFIRM:          # bar -1 confirm
                continue
            anch = str(z[j - 2])                         # bar -2 anchor (Z-family)
            if anch not in _ANCHORS:
                continue
            ra = rsi[j - 2]
            if not (ra == ra and _RSI_LO <= ra < _RSI_HI):   # anchor oversold
                continue
            has_l12 = str(l[j - 2]) == "L12"
            if require_l12 and not has_l12:
                continue
            entry_l = str(l[j])                          # L-line on the T11/T12 ENTRY bar
            # entry-L sharpener (2026-06-27): on the resolution bar, L5 = best
            # (+4.40/win63/6-6yr), L46 good (+2.58), L25 weakest (+1.44/3yr). The bar's
            # VOLUME character at the reversal decides; require/score by it.
            el_rank = {"L5": 12, "L46": 7}.get(entry_l, 0)
            if require_sharp_l and entry_l not in ("L5", "L46"):
                continue

            seen.add(tk)
            confirm = str(t[j - 1]); resolve = str(t[j])
            dv = float(cl[j] * vol[j])
            # tier driven by the ANCHOR family (premium=Z11/Z3, dip=Z1G, medium=Z5); the
            # entry-L (L5/L46) + T12 resolution + T5 confirm + deep-oversold + liquidity score.
            tier = _ANCHORS[anch]["tier"]
            base = 58 if tier == "premium" else 46 if tier == "dip" else 34
            score = base + el_rank + (8 if resolve == "T12" else 0) + (5 if confirm == "T5" else 0) \
                + (5 if ra < 40 else 0) + (6 if dv >= 20e6 else 0) + max(0, 6 - entry_age)
            atoms = [f"{anch}{'·L12' if has_l12 else ''}", f"→{confirm}", f"→{resolve}",
                     f"RSI{ra:.0f}", (f"entry·{entry_l}" if entry_l not in ("-", "") else "")]
            atoms = [a for a in atoms if a]
            # pre-L22 absorption booster (2026-06-30): an L22 (L3&L4 supply-exhaustion
            # red body) with CONTROLLED volume (0.7–2.0×vol20 — not dry, not blow-off) in
            # the 5 bars before the reversal entry. Lookahead-free validation: a reversal-T
            # preceded by an L22 absorption = +2.68%/PF1.44/5-6yr vs +1.01 with no prior
            # absorption; vr 1.0–1.3 sweet-spot, Z11-flavored L22 sharpest (+2.58/PF1.41).
            lo5 = max(0, j - 5)
            pre_l22 = pre_l22_z11 = False
            for k in range(lo5, j):
                if l22a[k] == 1 and vra[k] == vra[k] and 0.7 <= vra[k] < 2.0:
                    pre_l22 = True
                    if str(z[k]) == "Z11":
                        pre_l22_z11 = True
            if pre_l22:
                score += 10 if pre_l22_z11 else 7
                atoms.append("🧱L22-absorb" + ("·Z11" if pre_l22_z11 else ""))
            out.append({
                "ticker": tk, "universe": str(g.iloc[j]["universe"]),
                "signal_date": dts[j],          # the T11/T12 ENTRY bar (enter next-open)
                "anchor_date": dts[j - 2],      # the oversold absorption bar
                "anchor": anch,
                "confirm": confirm, "resolve": resolve,
                "seq": f"{anch}→{confirm}→{resolve}",
                "l12": has_l12, "pre_l22": bool(pre_l22), "pre_l22_z11": bool(pre_l22_z11),
                "entry_l": entry_l if entry_l not in ("-", "") else None,
                "sharp_l": entry_l in ("L5", "L46"),
                "close": round(float(cl[j]), 2),
                "rsi": round(float(ra), 0),
                "dv_m": round(dv / 1e6, 1),
                "age_days": entry_age,
                "tier": tier, "score": int(min(score, 100)), "atoms": atoms,
            })

    pri = {"premium": 0, "dip": 1, "medium": 2}
    from price_zones import classify as _pz
    for r in out:
        z = _pz(r["close"])
        r["score"] = max(0, min(r["score"] + z["score_delta"], 100))
        r["price_zone"] = z["zone"]; r["zone_emoji"] = z["emoji"]
        if z["zone"] in ("dead", "knife", "casino"):
            r["atoms"].append(f"{z['emoji']}{z['label']}")
    out.sort(key=lambda x: (x["age_days"], pri[x["tier"]], -x["score"]))
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
        "edge_note": ("Oversold reversal FAMILY — bear anchor (Z11/Z3/Z1G/Z5) while RSI14 30-45 "
                      "→ T3/T5 confirm next bar → T11/T12 resolution at +2 → ENTER AT THE T11/T12 "
                      "(next-open), −12% stop, 20-bar. fwd_20d screen 2019-26: Z11 +2.04/win55 "
                      "(flagship), Z3 +2.47/win58 (robust, top-3wk 12%), Z1G +2.67/win59 (strong "
                      "but top-3wk 38% — market-dip play), Z5 +1.30/win54. All survived 2022. "
                      "premium=Z11/Z3 · dip=Z1G (clustered) · medium=Z5. Resolution never +1; RSI-driven. "
                      "ENTRY-L sharpener: L5 on the T11/T12 bar +4.40/win63/6-6yr · L46 +2.58 · L25 +1.44 "
                      "(weakest) — the reversal bar's volume character; sharp_l=L5/L46."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(z11_t11_scan(max_age_days=60), indent=2)[:4000])
