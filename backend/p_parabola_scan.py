"""
p_parabola_scan.py — live scanner for the "P PARABOLA RIDE" trend-following setup
(2026-06 research). A broad P-signal (any EMA-cross PREUP) that is non-VB and already
advancing = a candidate to ride a parabolic advance with a 25% TRAILING stop.

TWO LOAD-BEARING FINDINGS:
  1. SIGNAL-AGNOSTIC — P2/P3/P50/P55/P66/P89/any-P all behave the same as triggers
     (~+4% mean, win ~46%, median-negative). The edge is NOT in which P fired.
  2. ANATOMY-AGNOSTIC (validated 2026-06, layered base-rate + ALL-feature-combo path-sim
     on a 72-ticker winner set): the P-bar's own anatomy — strong-closes, Z-absent,
     RSI-rising, T-type (T1/T2/T2G), L-line (L3/L12), EU suffix, R2H reclaim, RSI 50-60 —
     sits at its BASE RATE in winners AND losers. ZERO lift, even in conjunction (best
     combo P≥50% = 5.6% vs 5.6% base). Winners "look like that" because it is the base
     rate, not an edge. ⇒ Filtering or ranking by anatomy is FALSE PRECISION; do not.

The ONLY validated filter is non-VB (Δmean +3.2). The edge is the EXIT (trailing stop).

GATE (no anatomy): a P anchor, non-VB over the window, and price ADVANCED ≥3% since the P
(= a live ride, not a fader). ENTRY = the (anchor + W)-bar; ride with a 25% trailing stop
(−15% initial), 120-bar cap. Profile = trend-following: median ≈ 0, mean +3-5%, win ~46%,
edge in the right tail (~6% reach +50%). TIER/SCORE rank by LIQUIDITY + RECENCY only.
⚠ marginal, momentum/regime-dependent, costs matter; the 25% trailing exit is mandatory.

READ-ONLY on bars — surfaces candidates, opens nothing.
"""
from __future__ import annotations

_W = 10                  # accumulation window: bars between the P anchor and the ride entry
_P_FLAGS = ("sig_p2", "sig_p3", "sig_p50", "sig_p55", "sig_p66", "sig_p89")


def p_parabola_scan(max_age_days: int = 4, dv_floor: float = 1_000_000,
                    limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        pflag_sel = ", ".join(f"coalesce({f},0) AS {f}" for f in _P_FLAGS)
        # window must hold a P anchor up to (max_age + W) bars before today, the W-bar
        # accumulation run to the entry bar, AND ~40 bars of history before entry for the
        # 'smooth' tight-base / near-high features (≈56 calendar days back of the entry).
        lookback = int(max_age_days) + _W + 80
        rows = a.execute(f"""
            SELECT universe, ticker, date, close, high, low, volume, rsi_14, cci_20, t_sig,
                   coalesce(l_sig, '') AS l_sig,
                   close_suffix AS csfx, vol_bucket AS vb, z_sig AS z,
                   coalesce(sig_any_p, 0) AS any_p, {pflag_sel},
                   close * volume AS dv
            FROM bars
            WHERE date >= DATE '{as_of}' - INTERVAL {lookback} DAY AND close > 0
            ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()

    # Dedup to ONE row per (ticker, date). A ticker in 2-3 indices otherwise yields
    # 2-3× rows, which COMPRESSES the W-bar accumulation window (W=10 rows = ~5 days
    # for a 2-universe ticker) and surfaces it prematurely. Keep sp500 > nasdaq > r2k.
    if not rows.empty:
        _upri = {"sp500": 0, "nasdaq": 1, "russell2k": 2}
        rows = rows.assign(_upri=rows["universe"].map(lambda u: _upri.get(str(u), 9)))
        rows = (rows.sort_values(["ticker", "date", "_upri"])
                    .drop_duplicates(["ticker", "date"], keep="first")
                    .drop(columns="_upri")
                    .reset_index(drop=True))

    from datetime import date as _d
    aod = _d.fromisoformat(as_of)
    out = []

    for tk, g in rows.groupby("ticker", sort=False):
        g = g.reset_index(drop=True)
        n = len(g)
        anyp = g["any_p"].astype(int).to_numpy()
        cl = g["close"].to_numpy(float)
        hi_arr = g["high"].to_numpy(float)
        lo_arr = g["low"].to_numpy(float)
        vol = g["volume"].to_numpy(float)
        rsi = g["rsi_14"].to_numpy(float)
        csfx = g["csfx"].to_numpy(object)
        vb = g["vb"].to_numpy(object)
        z = g["z"].to_numpy(object)
        dts = [str(x)[:10] for x in g["date"]]

        # Walk newest→oldest; the freshest qualifying ride entry wins per ticker, but we COUNT
        # every qualifying day in the window (2026-07-17): a ticker re-firing on consecutive days
        # = a ride that keeps re-qualifying (persistent trend) vs a one-day blip. Surfaced as
        # n_fires / first_date / a "×Nd" atom so date-sorting shows the development.
        # ⚠ n_fires is DESCRIPTIVE, NOT predictive — tested (refire.py, 6yr, exact gate replayed,
        # trail25/-15%/120bar/slip15bps): the ladder is FLAT and if anything inverts —
        #   ×1 n=42329 mean +0.57 med −4.25 win 42.9 PF1.07 4/6yr
        #   ×2 n=15942 mean +0.60 med −3.83 win 43.4 PF1.07 3/6yr
        #   ×3+ n=4163 mean +0.31 med −4.23 win 42.8 PF1.04 3/6yr
        # flat inside every price bucket too. Same class as this scanner's anatomy fields: shows
        # WHAT the ride is doing, must never rank or gate it. (Price DOES rank: $5-21 is negative
        # on every tier, PF<1, 2/6yr; the edge lives $21-89 / $89+.)
        _hits: list[int] = []
        for entry_i in range(n - 1, -1, -1):
            anchor_i = entry_i - _W
            if anchor_i < 0:
                continue
            # entry bar must be recent (this is the actionable ride entry)
            entry_age = (aod - _d.fromisoformat(dts[entry_i])).days
            if entry_age > max_age_days:
                continue
            # trigger: a P-signal on the anchor bar
            if anyp[anchor_i] != 1:
                continue
            # liquidity + price on the entry bar
            if cl[entry_i] < 5 or cl[entry_i] * vol[entry_i] < dv_floor:
                continue
            # ── gate over [anchor_i .. entry_i] ──
            # VALIDATED 2026-06 (layered base-rate + all-combo path-sim test): the P-bar's
            # ANATOMY does NOT predict the parabola — strong-closes / Z-absent / RSI-rising /
            # T-type / L-line / EU-suffix / R2H all sit at their base rate in winners AND
            # losers (zero lift, even in conjunction). The ONLY validated filter is non-VB
            # (Δmean +3.2). So the gate keeps just what is real & defensible: non-VB +
            # "actually advancing since the P" (= a live ride, not a fader). No anatomy gate.
            win = range(anchor_i, entry_i + 1)
            no_vb = all(str(vb[k]) != "VB" for k in win)
            adv_pct = (cl[entry_i] / cl[anchor_i] - 1.0) * 100.0
            if not (no_vb and adv_pct >= 3.0):
                continue
            _hits.append(entry_i)

        if not _hits:
            continue
        entry_i = _hits[0]                    # freshest (we walked newest→oldest)
        anchor_i = entry_i - _W
        n_fires = len(_hits)
        first_date = dts[_hits[-1]]           # oldest qualifying day in the window
        # streak = UNBROKEN run of qualifying bars ending at the freshest one. Different from
        # n_fires, which allows gaps — they disagree on 26% of qualifying bars (MRSH ×3 is
        # 07-13 · skip 07-14 · 07-15 · 07-16 = streak 2). Also DESCRIPTIVE ONLY (streak.py, 6yr):
        #   1d n=46778 +0.60/PF1.07 · 2row n=6844 +0.48/PF1.06 (−0.41σ vs random same-size)
        #   3row n=810 +1.44/PF1.18 (+0.94σ) · 4+row n=77 +2.57/PF1.34 (+0.69σ)
        # 2row has ample power (n=6.8k) and shows NOTHING — the later "lift" tracks the n-collapse,
        # i.e. noise. The 4+row·$21-89 +7.97/PF2.38 eye-catcher is n=42 with 68% of PnL in 3 trades
        # (+1.92σ, under our ~3σ bar) and its sibling buckets are negative. Never rank/gate on it.
        streak = 1
        while streak < len(_hits) and _hits[streak] == _hits[streak - 1] - 1:
            streak += 1
        streak_from = dts[_hits[streak - 1]]  # first bar of the unbroken run
        adv_pct = (cl[entry_i] / cl[anchor_i] - 1.0) * 100.0
        entry_age = (aod - _d.fromisoformat(dts[entry_i])).days   # for the CHOSEN entry
        rsi_v = float(rsi[entry_i]) if rsi[entry_i] == rsi[entry_i] else None
        ar = g.iloc[anchor_i]
        ptypes = [f.replace("sig_", "").upper() for f in _P_FLAGS if int(ar[f] or 0)]
        ptag = "/".join(ptypes) if ptypes else "P"     # label only — edge is signal-agnostic
        dv = float(cl[entry_i] * vol[entry_i])
        # tier & score rank by what is REAL & actionable — LIQUIDITY + RECENCY — NOT by
        # anatomy "quality" (proven non-predictive; ranking by it would be false precision).
        if dv >= 20e6:
            tier = "liquid"
        elif str(ar["universe"]) == "sp500":
            tier = "sp500"
        else:
            tier = "base"
        score = 50 + (20 if dv >= 20e6 else 10 if dv >= 5e6 else 0) \
                   + max(0, 10 - entry_age * 3)         # fresher entry ranks higher
        # 'smooth' = the validated tight-base-near-high profile (2026-06): a coiled base
        # (15-bar range <12% of price) right under a recent high (close ≥95% of 40-bar
        # high). NOT more parabolas (fewer, in fact) — it is a VARIANCE-CUT: same mean
        # expectancy but win ~52% / median ≈ 0 (vs 41% / −6.8 base), robust both years.
        # A risk-profile TAG, not a quality rank — so it does not touch tier/score.
        smooth = False
        if entry_i >= 40:
            rng = (hi_arr[entry_i - 15:entry_i].max() - lo_arr[entry_i - 15:entry_i].min())
            hi40 = hi_arr[entry_i - 40:entry_i].max()
            if cl[entry_i] > 0 and hi40 > 0:
                tight = rng / cl[entry_i]
                nhigh = cl[entry_i] / hi40
                smooth = (tight < 0.12 and nhigh >= 0.95)
        atoms = [f"{ptag}·P", "non-VB", f"+{adv_pct:.0f}% since P", f"${dv/1e6:.0f}M/d"]
        if n_fires > 1:
            # N days qualified in the window; how many of them ran back-to-back
            atoms.append(f"×{n_fires}d re-fire" + (f" ({streak} in a row)" if streak > 1 else " (gapped)"))
        if smooth:
            atoms.append("smooth·tight-base")
        out.append({
            "ticker": tk, "universe": str(ar["universe"]),
            "signal_date": dts[entry_i],             # the RIDE ENTRY bar (freshest)
            "anchor_date": dts[anchor_i],            # the P-signal that started it
            "n_fires": n_fires,                      # qualifying days inside the window
            "first_date": first_date,                # oldest of them = when the ride began
            "streak": streak,                        # of those, how many ran back-to-back
            "streak_from": streak_from,              # first bar of that unbroken run
            "t_sig": str(g.iloc[entry_i]["t_sig"]) if g.iloc[entry_i]["t_sig"] else "",
            "l_sig": str(g.iloc[entry_i]["l_sig"]) if g.iloc[entry_i]["l_sig"] else "",
            "p_type": ptag,
            "close": round(float(cl[entry_i]), 2),
            "rsi": round(rsi_v, 0) if rsi_v is not None else None,
            "accum_pct": round(adv_pct, 1),
            "dv_m": round(dv / 1e6, 1),
            "smooth": bool(smooth),
            "age_days": entry_age,
            "tier": tier, "score": int(min(score, 100)), "atoms": atoms,
        })

    # Freshest ride first (a 'ride now' board), then liquidity — NOT tier-first, so a
    # liquid small-cap today is not buried under older large-caps.
    out.sort(key=lambda x: (x["age_days"], -x["score"]))
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
        "edge_note": ("P PARABOLA RIDE (trend-follow): any P-signal that is non-VB and has "
                      "ADVANCED ≥3% since the P (a live ride, not a fader). ENTRY = this bar; "
                      "ride with a 25% TRAILING stop (−15% initial), 120-bar cap. Validated "
                      "2024-26: med ≈ 0, mean +3-5%, win ~46%, edge in the right tail (~6% reach "
                      "+50%). SIGNAL- AND ANATOMY-AGNOSTIC: P-bar anatomy (T-type/L/EU/R2H/RSI/"
                      "strong-closes) was tested across all feature combos — ZERO lift over base "
                      "rate (winners look like that because it is the base rate, not an edge). The "
                      "only validated filter is non-VB; the edge is the trailing exit. Tier/score "
                      "rank by LIQUIDITY + RECENCY only (not anatomy). ⚠ marginal — many small "
                      "losers, few big winners; the 25% trailing exit is mandatory."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(p_parabola_scan(max_age_days=10), indent=2)[:3000])
