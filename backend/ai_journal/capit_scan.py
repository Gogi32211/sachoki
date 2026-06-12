"""
ai_journal/capit_scan.py — live scanner for the validated CAPITULATION-BOUNCE edge.

CORE: an L34/L46 VSA bar in DEEP aligned capitulation (RSI<30 AND CCI<-100 — Wyckoff
selling-climax spring). 20-bar DRAWDOWN knife-guard: the bounce sweet-spot is a -45..-10%
flush (+1.12, 6/6 yrs); a >-60% collapse is a falling knife (-4.65, 0/6) → EXCLUDED. Scored
by red flush / ~2x vol / volume-coil (BLUE/FRI64) / absorption. The one edge that survived
the rigorous gap-aware path-sim (+4.6% cost-adj, 5/6 yrs) while breakout/momentum failed.
EXIT (validated): hold ~20 bars, NO tight stop — a stop cuts the bounce.

ANALYSIS/SCANNER ONLY — surfaces candidates, opens no positions. Read-only on bars.
"""
from __future__ import annotations
from .db import get_analytics_conn


def capit_signal_dates(a, since_date: str, until_date: str | None = None,
                       universe: str | None = None) -> dict:
    """{(ticker, universe): sorted np.array of B+ capitulation signal dates} in
    [since_date, until_date]. B+ definition (production marker): L34/L46 + RSI 15-30 +
    CCI<-100 + 20-bar drawdown >-25% + no fri64/absorb + not the $1-2 knife band.

    Used to flag Atomic candidates that follow a recent QUALITY capitulation — the
    validated Capit→Atomic confluence (rich+capit≤10d: win 67%, med +4.24 vs +1.41
    baseline). The QUALITY filter matters: a raw/penny capitulation does NOT rescue the
    following gap-up (med -1.21), only a B+ one does (+1.70). Read-only on bars."""
    import numpy as np
    uni = f"AND universe = '{universe}'" if universe in ("sp500", "nasdaq", "russell2k") else ""
    until = f"AND date <= DATE '{until_date}'" if until_date else ""
    rows = a.execute(f"""
        WITH recent AS (
          SELECT ticker, universe, date, l_sig, rsi_14, cci_20, close,
                 sig_fri64 AS fri64, d_absorb_bear AS absb,
                 lag(close, 20) OVER (PARTITION BY ticker, universe ORDER BY date) AS c20
          FROM bars
          WHERE avg_vol_20d > 0 {uni}
            AND date >= DATE '{since_date}' - INTERVAL 60 DAY {until}
        )
        SELECT ticker, universe, date FROM recent
        WHERE l_sig IN ('L34','L46') AND rsi_14 >= 15 AND rsi_14 < 30 AND cci_20 < -100
          AND c20 > 0 AND (close / c20 - 1) > -0.25
          AND coalesce(fri64,0) = 0 AND coalesce(absb,0) = 0
          AND NOT (close >= 1 AND close < 2)
          AND date >= DATE '{since_date}'
    """).fetchdf()
    out: dict = {}
    for r in rows.itertuples():
        out.setdefault((str(r.ticker), str(r.universe)), []).append(r.date)
    return {k: np.sort(np.array(v, dtype="datetime64[D]")) for k, v in out.items()}


def days_since_capit(capit_dates: dict, ticker: str, universe: str, on_date) -> float | None:
    """Calendar days since the nearest PRIOR B+ capitulation for this ticker (None if none)."""
    import numpy as np
    arr = capit_dates.get((str(ticker), str(universe)))
    if arr is None or len(arr) == 0:
        return None
    od = np.datetime64(str(on_date)[:10])
    prior = arr[arr <= od]
    if len(prior) == 0:
        return None
    return float((od - prior[-1]) / np.timedelta64(1, "D"))


def capit_scan(max_age_days: int = 3, dv_floor: float = 300_000, limit: int = 120,
               universe: str | None = None) -> dict:
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        uni_where = f"AND universe = '{universe}'" if universe in ("sp500", "nasdaq", "russell2k") else ""
        # The 20-bar lag must run over the FULL recent price history, not the filtered
        # capit bars — so compute it in a 60-day window (≈28 cal-days covers 20 trading
        # bars + buffer) and only THEN filter to the capitulation matches.
        rows = a.execute(f"""
            WITH recent AS (
              SELECT ticker, universe, date, l_sig, rsi_14, cci_20, open, close, volume, avg_vol_20d,
                     sig_blue AS blue, sig_fri64 AS fri64, d_absorb_bear AS absb,
                     lag(close, 20) OVER (PARTITION BY ticker, universe ORDER BY date) AS c20,
                     close * volume AS dv
              FROM bars
              WHERE avg_vol_20d > 0 {uni_where}
                AND date >= DATE '{as_of}' - INTERVAL 60 DAY
            )
            SELECT *, CASE WHEN c20 > 0 THEN (close / c20 - 1) * 100 ELSE NULL END AS chg20
            FROM recent
            WHERE l_sig IN ('L34','L46') AND rsi_14 >= 15 AND rsi_14 < 30 AND cci_20 < -100
              AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
              AND dv >= {dv_floor} AND c20 IS NOT NULL AND close / c20 - 1 > -0.25
              AND coalesce(fri64,0) = 0 AND coalesce(absb,0) = 0
              AND NOT (close >= 1 AND close < 2)
        """).fetchdf()
    finally:
        a.close()

    best: dict = {}
    for _, r in rows.iterrows():
        try:
            rsi = float(r["rsi_14"]); cci = float(r["cci_20"])
            vr = float(r["volume"]) / float(r["avg_vol_20d"]) if r["avg_vol_20d"] else 0.0
            red = float(r["close"]) <= float(r["open"])
            chg20 = float(r["chg20"]) if r["chg20"] is not None else None
        except Exception:
            continue
        atoms = [str(r["l_sig"]), f"RSI{int(rsi)}", f"CCI{int(cci)}"]; score = 45
        if chg20 is not None and -45 <= chg20 <= -10:
            atoms.append(f"flush{chg20:.0f}%"); score += 15
        if red: atoms.append("red"); score += 15
        if 1.5 <= vr < 5: atoms.append(f"vol{vr:.1f}x"); score += 15
        elif vr >= 7:     atoms.append(f"⚠blowoff{vr:.0f}x"); score -= 15
        if int(r["blue"] or 0): atoms.append("BLUE"); score += 12          # BLUE-only coil (validated +)
        if rsi < 20: atoms.append("deep"); score += 5
        tk = str(r["ticker"])
        cand = {
            "ticker": tk, "universe": str(r["universe"]),
            "signal_date": str(r["date"])[:10], "l_sig": str(r["l_sig"]),
            "close": round(float(r["close"]), 2) if r["close"] is not None else None,
            "rsi": round(rsi, 0), "cci": round(cci, 0),
            "chg20": round(chg20, 1) if chg20 is not None else None,
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "score": max(0, min(int(score), 100)), "atoms": atoms, "age_days": None,
        }
        # keep the highest-scoring (tie → most recent) per ticker
        prev = best.get(tk)
        if prev is None or cand["score"] > prev["score"] or (
                cand["score"] == prev["score"] and cand["signal_date"] > prev["signal_date"]):
            best[tk] = cand

    out = sorted(best.values(), key=lambda x: x["score"], reverse=True)[:limit]
    from datetime import date as _d
    ad = _d.fromisoformat(as_of)
    for c in out:
        c["age_days"] = (ad - _d.fromisoformat(c["signal_date"])).days

    try:
        from . import regime as _reg
        reg = _reg.compute_regime(as_of)
    except Exception:
        reg = {"label": "NEUTRAL", "score": None, "conv_mult": 1.0, "breadth": {}}

    return {
        "as_of": as_of, "count": len(out), "rows": out,
        "regime": {"label": reg["label"], "score": reg["score"],
                   "conv_mult": reg["conv_mult"], "breadth": reg.get("breadth", {})},
        "edge_note": ("capitulation bounce: L34/L46 + RSI<30 + CCI<-100, drawdown knife-guard. "
                      "Survived gap-aware path-sim +4.6% (5/6 yrs). EXIT = hold ~20 bars, no "
                      "tight stop; sit through ~-7% MAE; diversify (a minority still knife)."),
    }
