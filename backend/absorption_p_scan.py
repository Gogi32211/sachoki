"""
absorption_p_scan.py — live scanner for the "ABSORPTION → P reversal" flagship LONG setup
(2026-06 research, the session's main validated find).

THESIS: a validated oversold ABSORPTION combo (a bearish/distribution T/Z signal carrying an
absorption L-line and an RSI2-extreme line-bit, while RSI14 is oversold) marks an absorbed-
supply bottom. A prompt P-signal (bullish EMA cross-up) within 3 bars CONFIRMS the reversal.
ENTER AT THE P.

VALIDATED (2019-26, liquid, true path-sim: entry next-open after the P, −12% stop, 20-bar):
  any-P  confirm : med +1.70% · clip25 +1.22% · win 57% · 6/6 years · best in 2022 (+4.1%)
  P50/P89 (3b)   : med +1.92% · clip25 +1.48% · win 58% · 6/6 years
  P50-only (3b)  : med +2.29% · clip25 +2.07% · win 61% · 6/6 years (best per-trade)
Confirming P-TYPE matters (NOT signal-agnostic here): P50 best, then P89/P55/P2; P3/P66 weak
(win 53%). The 3-bar window is the sweet spot — a 5-8 bar window dilutes the edge (a prompt
reclaim = a sharp reversal; a slow one = a stale, weak bounce). The discriminators are all
STATE (RSI14, RSI2/R2L-R2X, absorption-L, volume, gap); candle SHAPE (suffix, body/wick) is
noise. Distinct new family — see [[project_absorption_reversal]].

READ-ONLY on bars — surfaces candidates, opens nothing.
"""
from __future__ import annotations

# Validated absorption combos: (TZ, l_sig, bar_line5) — each passed the path-sim gauntlet
# (clip25+, win≥54%, per-year≥4/6) at RSI 30-45.
_VALID = frozenset({
    ("Z11", "L12", "PB-R2X"), ("Z10", "L12", "VX-PS-R2X"), ("Z6", "L5", "VX-PS-R2L"),
    ("Z5", "L3", "PS-R2X"),   ("Z10", "L12", "PB-R2X"),     ("Z1G", "L5", "PB-R2L"),
    ("Z5", "L3", "PS"),       ("Z5", "L12", "PS-R2L"),      ("T12", "L46", "PS-R2L"),
    ("T11", "L46", "PS-R2L"), ("T12", "L46", "PS"),
})
_PWIN = 3                      # P-confirm must land within this many bars after the absorption
_P_FLAGS = ("sig_p2", "sig_p3", "sig_p50", "sig_p55", "sig_p66", "sig_p89")
_RSI_LO, _RSI_HI = 30.0, 45.0  # oversold band the absorption combos were validated in


def absorption_p_scan(max_age_days: int = 4, dv_floor: float = 2_000_000,
                      limit: int = 120) -> dict:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        pflag_sel = ", ".join(f"coalesce({f},0) AS {f}" for f in _P_FLAGS)
        lookback = int(max_age_days) + _PWIN + 15
        rows = a.execute(f"""
            SELECT universe, ticker, date, close, volume, rsi_14,
                   coalesce(t_sig, z_sig, '') AS tz, coalesce(l_sig, '') AS l,
                   coalesce(bar_line5, '') AS l5, coalesce(sig_any_p, 0) AS any_p,
                   CASE WHEN sig_bias_dn = 1 OR sig_vol_5x = 1 OR sig_vol_10x = 1
                             OR sig_vol_20x = 1 THEN 1 ELSE 0 END AS supp,
                   {pflag_sel}
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
        tz = g["tz"].to_numpy(object); l = g["l"].to_numpy(object); l5 = g["l5"].to_numpy(object)
        rsi = g["rsi_14"].to_numpy(float); cl = g["close"].to_numpy(float)
        vol = g["volume"].to_numpy(float); anyp = g["any_p"].astype(int).to_numpy()
        supp = g["supp"].astype(int).to_numpy()
        dts = [str(x)[:10] for x in g["date"]]
        pf = {f: g[f].astype(int).to_numpy() for f in _P_FLAGS}

        # Walk newest→oldest: a P-confirm ENTRY bar (any_p) is recent and one of the prior
        # 1.._PWIN bars is a validated oversold absorption combo.
        for ej in range(n - 1, -1, -1):
            if tk in seen:
                break
            if anyp[ej] != 1:
                continue
            entry_age = (aod - _d.fromisoformat(dts[ej])).days
            if entry_age > max_age_days or entry_age < 0:
                continue
            if cl[ej] < 5 or cl[ej] * vol[ej] < dv_floor:
                continue
            if supp[ej] == 1:                           # universal suppressor guard (bias_dn/vol-extreme)
                continue
            # find the absorption anchor in the prior _PWIN bars
            anchor = None
            for k in range(1, _PWIN + 1):
                ai = ej - k
                if ai < 0:
                    break
                if (str(tz[ai]), str(l[ai]), str(l5[ai])) in _VALID \
                        and (rsi[ai] == rsi[ai]) and _RSI_LO <= rsi[ai] < _RSI_HI:
                    anchor = ai
                    break
            if anchor is None:
                continue

            seen.add(tk)
            ptypes = [f.replace("sig_", "").upper() for f in _P_FLAGS if int(pf[f][ej])]
            ptag = "/".join(ptypes) if ptypes else "P"
            has = lambda *ps: any(p in ptypes for p in ps)
            # tier by confirming P-type (validated: P50 best · P89/P55/P2 strong · P3/P66 weak)
            if "P50" in ptypes:
                tier = "premium"
            elif has("P89", "P55", "P2"):
                tier = "strong"
            else:
                tier = "medium"
            ar = g.iloc[anchor]
            absorb = f"{tz[anchor]}·{l[anchor]}·{l5[anchor]}"
            dv = float(cl[ej] * vol[ej])
            rsi_a = float(rsi[anchor]) if rsi[anchor] == rsi[anchor] else None
            score = (60 if tier == "premium" else 45 if tier == "strong" else 30) \
                + (10 if dv >= 20e6 else 0) + max(0, 8 - entry_age * 2)
            atoms = [f"absorb:{absorb}", f"P{':'+ptag if ptag else ''}".replace("P:", ""),
                     f"RSI{rsi_a:.0f}" if rsi_a is not None else "RSI?", f"+{ej - anchor}b→P"]
            # surface the absorption bar's T/Z and L (embedded in `absorb`, e.g. "T12·L46·PS")
            _parts = str(absorb).split("·")
            _tz = next((p for p in _parts if p and p[0] in "TZ"), "")
            _l = next((p for p in _parts if p.startswith("L")), "")
            out.append({
                "ticker": tk, "universe": str(ar["universe"]),
                "signal_date": dts[ej],              # the P-confirm ENTRY bar
                "anchor_date": dts[anchor],          # the absorption bar
                "p_type": ptag, "t_sig": _tz, "l_sig": _l,
                "absorb": absorb,
                "close": round(float(cl[ej]), 2),
                "rsi": round(rsi_a, 0) if rsi_a is not None else None,
                "dv_m": round(dv / 1e6, 1),
                "age_days": entry_age,
                "tier": tier, "score": int(min(score, 100)), "atoms": atoms,
            })

    pri = {"premium": 0, "strong": 1, "medium": 2}
    out.sort(key=lambda x: (x["age_days"], pri[x["tier"]], -x["score"]))
    return {
        "as_of": as_of, "count": len(out), "rows": out[:limit],
        "edge_note": ("ABSORPTION → P reversal (flagship): a validated oversold absorption combo "
                      "(distribution T/Z + absorption-L + RSI2-extreme, RSI14 30-45) confirmed by "
                      "a P-signal within 3 bars → ENTER AT THE P, −12% stop, 20-bar. Validated "
                      "2019-26: any-P med +1.70%/win 57%/6yr; P50/P89 +1.92%/58%; P50 +2.29%/61%. "
                      "Confirming P-TYPE matters: premium=P50, strong=P89/P55/P2, medium=P3/P66 "
                      "(weak, win 53%). Bear-proof (best in 2022, +4.1%). Filter by P-type."),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(absorption_p_scan(max_age_days=10), indent=2)[:3000])
