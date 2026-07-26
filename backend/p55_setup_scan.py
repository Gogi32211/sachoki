"""
p55_setup_scan.py — live scanner for the validated P55 "refined grind" LONG setup
(2026-06 research). Surfaces today's candidates that match the only tradeable P55
configuration found across the whole P-signal study.

SETUP (path-sim validated: entry next-bar/close, stop −12%, 60-bar; median-positive,
win ~50-54%, 5/6 years; clip25 ~+1%):
  1) 1D sig_p55 = 1  AND  1H sig_p55 = 1 on the same calendar day  (exact same-signal match)
  2) t_sig ∈ {T5,T6,T9,T10,T11,T12}   (absorption/continuation T — NOT T1/T2 chase)
  3) vol_bucket ≠ 'VB'                 (VB blow-off climax = trap)
  4) Z1G or T5 in the prior 4 bars     (accumulation prelude = AD-FRESH construct)
  5) P→D→P structure in prior 6 bars   (most-recent prior P-signal has exactly ONE
       D-signal between it and the anchor — breakout→pullback→reclaim)
  6) shallow/flat shakeout             (the dip from that prior P to entry stays > −8%;
       'flat' = > −3% is the strongest, highest-median tier)

The edge is small (clip25 ~+1%, near break-even after microcap costs) and regime-
dependent (2022 bear weak). READ-ONLY on bars — surfaces candidates, opens nothing.
"""
from __future__ import annotations
import os
import duckdb
from ai_journal.db import ANALYTICS_DB_PATH  # 1D analytics DB path

_GOOD_T = ("T5", "T6", "T9", "T10", "T11", "T12")


def _h1_db_path() -> str:
    """1H DB lives beside the 1D analytics DB as studio_1h.duckdb."""
    d = os.path.dirname(ANALYTICS_DB_PATH)
    return os.path.join(d, "studio_1h.duckdb")


def _h1_p55_days(as_of: str, max_age_days: int) -> set:
    """Set of (ticker, 'YYYY-MM-DD') where the 1H tape printed a P55 in the window.
    Lock-tolerant: returns empty set if the 1H DB is busy/missing (caller degrades)."""
    path = _h1_db_path()
    if not os.path.exists(path):
        return set()
    try:
        c = duckdb.connect(path, read_only=True)
        try:
            df = c.execute(
                f"""SELECT DISTINCT ticker, CAST(date AS DATE) AS d FROM bars
                    WHERE sig_p55 = 1 AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days) + 2} DAY"""
            ).fetchdf()
        finally:
            c.close()
        return set(zip(df["ticker"].astype(str), df["d"].astype(str)))
    except Exception:
        return set()


def p55_setup_scan(max_age_days: int = 4, dv_floor: float = 500_000,
                   window: int = 6, limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        ph = ",".join(f"'{s}'" for s in _GOOD_T)
        # anchor candidates: recent good-T P55 bars, liquid, non-VB
        anchors = a.execute(f"""
            SELECT universe, ticker, date, t_sig, coalesce(l_sig,'') AS l_sig, rsi_14, close,
                   close * volume AS dv
            FROM bars
            WHERE sig_p55 = 1 AND t_sig IN ({ph}) AND vol_bucket <> 'VB'
              AND NOT (sig_bias_dn = 1 OR sig_vol_5x = 1 OR sig_vol_10x = 1 OR sig_vol_20x = 1)
              AND date >= DATE '{as_of}' - INTERVAL {int(max_age_days)} DAY
              AND close >= 5 AND avg_vol_20d > 0 AND close * volume >= {dv_floor}
            ORDER BY ticker, date
        """).fetchdf()
        tickers = sorted(set(anchors["ticker"].astype(str)))
        recent = None
        if tickers:
            tk_ph = ",".join("?" * len(tickers))
            recent = a.execute(f"""
                SELECT ticker, date, low, close, t_sig, z_sig,
                       coalesce(sig_any_p, 0) AS p, coalesce(sig_any_d, 0) AS d
                FROM bars
                WHERE ticker IN ({tk_ph})
                  AND date >= DATE '{as_of}' - INTERVAL {int(window) + 25} DAY
                ORDER BY ticker, date
            """, tickers).fetchdf()
    finally:
        a.close()

    h1 = _h1_p55_days(as_of, max_age_days)
    h1_available = bool(h1)

    # per-ticker arrays for prior-bar window logic
    series: dict = {}
    if recent is not None:
        # Dedup (ticker, date) — a multi-index ticker otherwise yields 2-3× interleaved
        # rows, compressing the prior-bar window (Z1G/T5 prelude, P→D→P, shakeout). The
        # OHLC/TZ/P/D values are price-derived so identical across universes; keep first.
        if not recent.empty:
            recent = recent.drop_duplicates(["ticker", "date"], keep="first").reset_index(drop=True)
        for tk, g in recent.groupby("ticker", sort=False):
            g = g.reset_index(drop=True)
            series[str(tk)] = dict(
                date=[str(x)[:10] for x in g["date"]],
                low=g["low"].tolist(), close=g["close"].tolist(),
                tsig=[str(x) if x is not None else "" for x in g["t_sig"]],
                zsig=[str(x) if x is not None else "" for x in g["z_sig"]],
                p=g["p"].astype(int).tolist(), d=g["d"].astype(int).tolist(),
            )

    from datetime import date as _d
    aod = _d.fromisoformat(as_of)
    out, seen = [], set()
    for _, r in anchors.iterrows():
        tk = str(r["ticker"]); dt = str(r["date"])[:10]
        if tk in seen:
            continue
        s = series.get(tk)
        if s is None or dt not in s["date"]:
            continue
        i = s["date"].index(dt)
        if i < window:
            continue
        # (4) prelude: Z1G (z_sig) or T5 (t_sig) in prior 4 bars
        prelude = any(s["zsig"][j] == "Z1G" or s["tsig"][j] == "T5"
                      for j in range(max(0, i - 4), i))
        # (5) P→D→P: most-recent prior P, exactly one D between it and anchor
        ppos = [j for j in range(i - window, i) if s["p"][j] == 1]
        if not ppos:
            continue
        last_p = ppos[-1]
        n_d = sum(s["d"][j] for j in range(last_p + 1, i))
        if n_d != 1:
            continue
        # (6) shakeout depth: dip from that prior P to entry close
        seg_low = min(s["low"][last_p:i]) if i > last_p else s["low"][i]
        depth = (seg_low / s["close"][i] - 1.0) * 100.0  # negative %
        if depth <= -8:        # deeper than −8% = stop-out trap
            continue
        tier = "flat" if depth > -3 else "shallow"
        # (1) 1H P55 exact same day — required when the 1H DB is available
        h1_ok = (tk, dt) in h1
        if h1_available and not h1_ok:
            continue

        atoms = ["P55·1D"]
        if h1_ok:
            atoms.append("P55·1H")
        atoms.append(str(r["t_sig"]))
        if prelude:
            atoms.append("Z1G/T5-prelude")
        atoms.append("P→D→P")
        atoms.append(f"shakeout {depth:.0f}%")
        score = 50 + (15 if prelude else 0) + (15 if h1_ok else 0) + (15 if tier == "flat" else 8)

        out.append({
            "ticker": tk, "universe": str(r["universe"]),
            "signal_date": dt, "t_sig": str(r["t_sig"]), "l_sig": str(r["l_sig"]),
            "close": round(float(r["close"]), 2) if r["close"] is not None else None,
            "rsi": round(float(r["rsi_14"]), 0) if r["rsi_14"] is not None else None,
            "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
            "age_days": (aod - _d.fromisoformat(dt)).days,
            "prelude": bool(prelude), "h1_p55": bool(h1_ok),
            "shakeout_pct": round(depth, 1), "tier": tier,
            "score": int(min(score, 100)), "atoms": atoms,
        })
        seen.add(tk)

    out.sort(key=lambda x: (x["tier"] != "flat", -x["score"], x["age_days"]))
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
        "h1_available": h1_available,
        "edge_note": ("Refined P55 grind: 1D+1H P55 exact · good-T (T5/T6/T9-12) · non-VB · "
                      "Z1G/T5 accumulation prelude · P→D→P breakout-pullback-reclaim · shallow "
                      "shakeout (>−8%, 'flat' >−3% strongest). Validated clip25 ~+1%, win ~50-54%, "
                      "5/6yr, median-positive. Small edge, regime-dependent — paper-track first."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(p55_setup_scan(max_age_days=10), indent=2)[:3000])
