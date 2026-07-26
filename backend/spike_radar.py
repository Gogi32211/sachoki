"""
spike_radar.py — 🎆 volatility/spike-probability radar (validated 2026-07-06).

NOT a buy list. Ranks the universe by P(+15% day within ≤5d) using the four
year-stable cells from the pre-breakout study (base ≤5d ≈ 1.66%):
    mom-up    drift5 ≥ +10% & NOT near 20d-high   → 5.30× (6/6yr)
    crashed   drift5 ≤ −10%                        → 3.58× (6/6yr)
    mom-HIGH  drift5 ≥ +10% & near 20d-high        → 2.94× (6/6yr)
    vol-diff  rvol5 > 1.3 & lbrel5 < 0.8           → ~1.5×  (6/6yr)
Suppressors (excluded): quiet coil at highs (near-high & drift<+10 → 0.27×)
and the Wyckoff SC-zone (→ 0.24×). Direction does NOT leak — pair with an
Edge anchor for entries. On mega-runners the radar is a WATCHLIST feeder
(always-on once moving), not a day-timer.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import duckdb
from studio.paths import ANALYTICS_DB, db_path

CELLS = {"mom-up": 5.30, "crashed": 3.58, "mom-HIGH": 2.94, "vol-diff": 1.55}


def scan(dv_floor: float = 2_000_000, price_min: float = 3.0, limit: int = 120) -> dict:
    # ── daily side: drift5, near-high, SC-zone, filters (last ~45 sessions) ──
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        d = a.execute(f"""
            WITH u AS (SELECT ticker, any_value(universe) uni FROM bars GROUP BY ticker),
            r AS (SELECT ticker, date, close, high, volume, rsi_14,
                         coalesce(wt_valid_tr,0) vtr, coalesce(wt_support,0) wt_sup,
                         row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                  FROM bars WHERE date >= DATE '{as_of}' - INTERVAL 70 DAY)
            SELECT r.ticker, u.uni, CAST(r.date AS VARCHAR)[:10] AS dstr,
                   r.close, r.high, r.volume, r.rsi_14, r.vtr, r.wt_sup
            FROM r JOIN u USING (ticker) WHERE rn=1 ORDER BY r.ticker, r.date
        """).fetchdf()
    finally:
        a.close()
    g = d.groupby("ticker", sort=False)
    d["drift5"] = g["close"].transform(lambda s: s.pct_change(5)) * 100
    d["ret1"] = g["close"].transform(lambda s: s.pct_change(1)) * 100
    d["hi20"] = g["high"].transform(lambda s: s.rolling(20).max()).groupby(d.ticker).shift(1)
    last = d.groupby("ticker", sort=False).tail(1).copy()
    last["dv"] = last.close * last.volume
    last["near_high"] = last.close >= last.hi20 * 0.97
    last["sc_zone"] = (last.vtr == 1) & (last.wt_sup > 0) & \
                      ((last.close / last.wt_sup.replace(0, np.nan) - 1).abs() <= 0.05)
    last = last[(last.close >= price_min) & (last.dv >= dv_floor) & last.drift5.notna()]

    # ── 1h side: rvol5 / lbrel5 (last ~60 calendar days, whole universe) ─────
    con = duckdb.connect(db_path("studio_1h.duckdb"), read_only=True)
    try:
        f = con.execute("""
        WITH b AS (SELECT ticker, CAST(date - INTERVAL 5 HOUR AS DATE) d0, date, volume
                   FROM bars WHERE date >= now() - INTERVAL 60 DAY),
        a AS (SELECT ticker, CAST(d0 AS VARCHAR) dstr, sum(volume) tv,
                     arg_max(volume, date) lb, count(*) nb
              FROM b GROUP BY ticker, d0)
        SELECT * FROM a WHERE nb >= 5 ORDER BY ticker, dstr""").fetchdf()
    finally:
        con.close()
    rows = []
    for tk, grp_ in f.groupby("ticker", sort=False):
        if len(grp_) < 26:
            continue
        tv = grp_["tv"].to_numpy(float)
        lb = (grp_["lb"] / grp_["tv"].replace(0, np.nan)).to_numpy(float)
        rows.append((tk, tv[-5:].mean() / max(tv[-25:-5].mean(), 1e-9),
                     np.nanmean(lb[-5:]) / max(np.nanmean(lb[-25:-5]), 1e-9)))
    vol = pd.DataFrame(rows, columns=["ticker", "rvol5", "lbrel5"])
    m = last.merge(vol, on="ticker", how="left")

    # ── cells + suppressors ───────────────────────────────────────────────────
    out = []
    for _, r in m.iterrows():
        if bool(r.sc_zone):
            continue                                            # SC-zone: 0.24×
        up = r.drift5 >= 10; dn = r.drift5 <= -10
        vd = pd.notna(r.rvol5) and r.rvol5 > 1.3 and pd.notna(r.lbrel5) and r.lbrel5 < 0.8
        if up and not r.near_high:
            cell = "mom-up"
        elif dn:
            cell = "crashed"
        elif up:
            cell = "mom-HIGH"
        elif vd:
            cell = "vol-diff"
        else:
            continue                                            # incl. quiet coil (0.27×)
        lift = CELLS[cell]
        atoms = [f"🎆{cell}", f"dr5 {r.drift5:+.0f}%"]
        if pd.notna(r.rvol5) and r.rvol5 > 1.3:
            atoms.append(f"rvol {r.rvol5:.1f}×")
        if pd.notna(r.lbrel5) and r.lbrel5 < 0.8:
            atoms.append("diffuse-flow")
        out.append({"ticker": r.ticker, "universe": str(r.uni), "close": round(float(r.close), 2),
                    "rsi": round(float(r.rsi_14), 0) if pd.notna(r.rsi_14) else None,
                    "tier": "hot" if lift >= 3 else "warm",
                    "tz": cell, "l_sig": "",
                    "score": int(min(lift / 5.3 * 100, 100)),
                    "cell": cell, "lift": lift,
                    "drift5": round(float(r.drift5), 1),
                    "rvol5": round(float(r.rvol5), 2) if pd.notna(r.rvol5) else None,
                    "lbrel5": round(float(r.lbrel5), 2) if pd.notna(r.lbrel5) else None,
                    "atoms": atoms, "signal_date": as_of, "age_days": 0})
    # ⚡/⛔ badges via the existing helpers (candidates only)
    try:
        from charged_state import charged_for
        from sub200_rally import flags_for
        tks = [r["ticker"] for r in out]
        ch = charged_for(tks); s2 = flags_for(tks)
        for r in out:
            if ch.get(r["ticker"]):
                r["charged"] = True; r["atoms"].append("⚡charged")
            if s2.get(r["ticker"]):
                r["sub200_rally"] = True; r["atoms"].append("⛔sub200")
    except Exception:
        pass
    # drop reverse-split artifacts (impossible organic 5d drifts)
    out = [r for r in out if abs(r["drift5"]) <= 300]
    out.sort(key=lambda r: (-r["lift"], -abs(r["drift5"])))
    # balance cells so crashed/mom-HIGH/vol-diff surface too (not just mom-up)
    per_cell = max(10, limit // 4)
    seen: dict = {}; bal = []
    for r in out:
        if seen.get(r["cell"], 0) < per_cell:
            bal.append(r); seen[r["cell"]] = seen.get(r["cell"], 0) + 1
    return {"as_of": as_of, "count": len(out), "rows": bal[:limit],
            "base_rate_5d_pct": 1.66,
            "edge_note": ("🎆 Spike-Radar — P(+15% day ≤5d) cells, 6/6yr validated: mom-up 5.3× · "
                          "crashed 3.6× · mom-HIGH 2.9× · vol-diffuse 1.5×. Quiet-coil-at-highs "
                          "(0.27×) and SC-zone (0.24×) excluded. VOLATILITY watchlist — direction "
                          "does not leak; pair with an Edge anchor before entering.")}
