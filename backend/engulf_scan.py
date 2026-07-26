"""
engulf_scan.py — LIVE scanner for the Engulf-Reversal gem (GEM2, project_engulf_absorption /
project_capitulation_bounce). Backtest 'Engulf-L46' = PF 2.94 / 6-of-6yr (36mo window).

Fires on: a bull-T bar that RANGE-engulfs the prior 2 bars (outside bar) in the quality band
(≥$21, RSI<45), where a SWALLOWED bar carried L46 (VSA absorption — the validated gate).
NOTE: this is a live PROXY of the full backtest mask (the strict "any Edge-setup in the prior
2 bars" gate is approximated by the swallowed-L46 / L5 VSA condition, which is SQL-tractable).
"""
from datetime import date as _d


def engulf_scan(max_age_days: int = 4, dv_floor: float = 500_000, limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        rows = a.execute(f"""
            WITH base AS (
                SELECT universe, ticker, date, t_sig, l_sig, rsi_14, close, open, high, low,
                       vol_bucket, volume, avg_vol_20d,
                       max(high) OVER (PARTITION BY universe,ticker ORDER BY date
                                       ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ph,
                       min(low)  OVER (PARTITION BY universe,ticker ORDER BY date
                                       ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pl,
                       lag(l_sig,1) OVER (PARTITION BY universe,ticker ORDER BY date) AS l1,
                       lag(l_sig,2) OVER (PARTITION BY universe,ticker ORDER BY date) AS l2
                FROM bars
                WHERE close >= 5
                  AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days) + 15} DAY
                  AND NOT (sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1)
            )
            SELECT universe, ticker, date, t_sig, l_sig, rsi_14, close, high, low, vol_bucket,
                   close*volume AS dv, coalesce(l1,'') l1, coalesce(l2,'') l2
            FROM base
            WHERE t_sig LIKE 'T%' AND t_sig <> ''
              AND ph IS NOT NULL AND high >= ph AND low <= pl          -- engulfs prior 2 bars
              AND close >= 21 AND rsi_14 < 45
              AND (coalesce(l1,'') IN ('L46','L5') OR coalesce(l2,'') IN ('L46','L5'))  -- absorbed swallowed bar
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
        swL = "L46" if "L46" in (str(r["l1"]), str(r["l2"])) else "L5"
        atoms = [str(r["t_sig"]), "engulf-2", f"RSI{float(r['rsi_14']):.0f}", f"🧱sw-{swL}"]
        score = 70 + (12 if swL == "L46" else 6)
        out.append({
            "ticker": tk, "universe": str(r["universe"]), "signal_date": str(r["date"])[:10],
            "close": round(float(r["close"]), 2),
            "rsi": round(float(r["rsi_14"]), 0) if r["rsi_14"] is not None else None,
            "l_sig": str(r["l_sig"] or ""), "swallowed_L": swL, "t_sig": str(r["t_sig"]),
            "vol": str(r["vol_bucket"] or ""),
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "score": min(int(score), 100), "atoms": atoms,
            "_hi": float(r["high"]), "_lo": float(r["low"]),
            "age_days": (aod - _d.fromisoformat(str(r["date"])[:10])).days,
        })

    # 🔥 Engulf-Abs-Lⁿ booster (validated 2026-07-07, project_engulf_absorption): the engulf
    # RANGE sweeps up ≥2 L46/L34 VSA bars in the last 21 sessions = absorbed the recent volume
    # distribution in one move. Plateau/2×-slip robust: +3.64/PF1.64/5-6yr vs base +3.04/PF1.52.
    if out:
        try:
            from ai_journal.db import get_analytics_conn
            b = get_analytics_conn()
            try:
                tks = ",".join("'" + r["ticker"].replace("'", "") + "'" for r in out)
                hist = b.execute(f"""
                    WITH r AS (SELECT ticker, date, open, close, coalesce(l_sig,'') l_sig,
                                      row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                               FROM bars WHERE ticker IN ({tks})
                                 AND date >= DATE '{as_of}' - INTERVAL 60 DAY)
                    SELECT ticker, CAST(date AS VARCHAR)[:10] d, open, close, l_sig
                    FROM r WHERE rn=1 ORDER BY ticker, date
                """).fetchdf()
            finally:
                b.close()
            hist["isL"] = hist["l_sig"].isin(("L46", "L34"))
            for r in out:
                h = hist[hist.ticker == r["ticker"]]
                sig = h[h.d <= r["signal_date"]].tail(22)          # signal bar + 21 prior
                prior = sig.iloc[:-1] if len(sig) else sig         # the 21 prior bars
                lo, hi = r["_lo"], r["_hi"]
                swLn = int(((prior.isL) & ((prior.open.between(lo, hi)) | (prior.close.between(lo, hi)))).sum())
                r["engabs_swLn"] = swLn
                if swLn >= 2:
                    r["l_heavy"] = True
                    r["atoms"].append(f"🔥Lⁿ{swLn}")
                    r["score"] = min(int(r["score"]) + 10, 100)    # validated tier boost
        except Exception:
            pass
        for r in out:
            r.pop("_hi", None); r.pop("_lo", None)

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
        "edge_note": ("Engulf-Reversal (GEM2) — a bull-T that RANGE-engulfs the prior 2 bars, ≥$21, "
                      "RSI<45, where a swallowed bar carried L46/L5 VSA absorption. Backtest 'Engulf-L46' "
                      "PF 2.94 / 6-of-6yr (era-tilted magnitude). Live PROXY of the full mask. "
                      "Entry next-open, trailing exit, small size, paper-track first."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(engulf_scan(max_age_days=8), indent=2)[:1500])
