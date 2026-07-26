"""ATR-based time-to-target forecast (2026-07-26).

The real price<->time law: time-to-move-X% is driven by the stock's VOLATILITY (ATR%),
NOT by chart shape (cup/AM-GM/parabola all tested = noise) and only weakly by the signal
STATE (+4pp over random). Calibrated on 78mo, OOS-stable (TRAIN 2021-23 ≈ TEST 2024-26):
median days to +/-X% by ATR% bucket held within ~1-2 days across eras.

Q1 = up & DOWN forecasts (targets & stops). Q2 = everything is measured in ATR-units
(depth-in-ATR discriminated an ~8pt median spread that raw % masked) — so the tool also
reports the current bar's distance-from-20d-low in ATR-units.

Honest caveat: this forecasts TIMING/probability (a volatility property), not directional
edge. Targets get hit at ~base-rate; the tradeable edge lives in downside/path, not here.
Use it for stop-distance / time-stop / expectation calibration, not as a buy signal.
"""
import duckdb
from studio.db import tf_db_path

# Calibrated tables: ATR% bucket -> {pct_target: (hit_rate%, median_days[, P(<=20d)%])}
# 90-bar horizon, next-open entry, every-10th-bar sample (n=287k). OOS-validated.
_BUCKETS = [(0.0, 0.02), (0.02, 0.03), (0.03, 0.04), (0.04, 0.06), (0.06, 0.10), (0.10, 9.0)]
_UP = {  # +X%
    (0.0, 0.02):  {10: (46, 38, 10), 25: (8, 66)},
    (0.02, 0.03): {10: (57, 27, 22), 25: (18, 53)},
    (0.03, 0.04): {10: (67, 19, 35), 25: (30, 44)},
    (0.04, 0.06): {10: (73, 13, 49), 25: (43, 33)},
    (0.06, 0.10): {10: (79, 7, 62),  25: (54, 21)},
    (0.10, 9.0):  {10: (82, 3, 72),  25: (62, 9)},
}
_DOWN = {  # -X%
    (0.0, 0.02):  {10: (39, 34), 20: (10, 55)},
    (0.02, 0.03): {10: (53, 26), 20: (22, 48)},
    (0.03, 0.04): {10: (62, 19), 20: (33, 40)},
    (0.04, 0.06): {10: (71, 13), 20: (46, 30)},
    (0.06, 0.10): {10: (81, 8),  20: (61, 20)},
    (0.10, 9.0):  {10: (88, 4),  20: (75, 9)},
}
# smooth median-days law: days ≈ 6.3·(X/ATR%)^0.95 — ~LINEAR in (X/ATR), refit 2026-07-26 on
# actual bucket-mean ATRs (theory_vs_empirics.py). GBM diffusion predicts p=2; the real market's
# p≈0.95 is the quantified momentum/fat-tail signature (trends exist, vol clusters — not a random
# walk). Theory also 2×+ overestimates time for low-vol names (80d vs real 38d).
_LAW_K, _LAW_P = 6.3, 0.95


def _bucket(atr_pct: float):
    for lo, hi in _BUCKETS:
        if lo <= atr_pct < hi:
            return (lo, hi)
    return _BUCKETS[-1]


def law_days(x_frac: float, atr_pct: float):
    """Smooth median-days estimate to move x_frac (e.g. 0.10) at given ATR% (fraction)."""
    if atr_pct <= 0 or x_frac <= 0:
        return None
    return round(_LAW_K * (x_frac / atr_pct) ** _LAW_P, 1)


def forecast(atr_pct: float) -> dict:
    """atr_pct as a FRACTION (e.g. 0.03 = 3%). Returns up/down time-to-target forecast."""
    b = _bucket(atr_pct); u = _UP[b]; d = _DOWN[b]
    blabel = "%.0f%%+" % (b[0] * 100) if b[1] >= 9.0 else "%.0f-%.0f%%" % (b[0] * 100, b[1] * 100)
    return {
        "atr_pct": round(atr_pct * 100, 2),
        "bucket": blabel,
        "up": {
            "+10%": {"hit_pct": u[10][0], "median_days": u[10][1], "p_le20d": u[10][2],
                     "law_days": law_days(0.10, atr_pct)},
            "+25%": {"hit_pct": u[25][0], "median_days": u[25][1], "law_days": law_days(0.25, atr_pct)},
        },
        "down": {
            "-10%": {"hit_pct": d[10][0], "median_days": d[10][1]},
            "-20%": {"hit_pct": d[20][0], "median_days": d[20][1]},
        },
        "note": "OOS-calibrated (TRAIN≈TEST); 90-bar horizon. Volatility-driven TIMING, "
                "weakly signal-dependent — for stop/time-stop/expectation, not a buy signal.",
    }


def forecast_ticker(ticker: str) -> dict:
    tk = str(ticker).upper().replace("'", "")
    c = duckdb.connect(tf_db_path("1d"), read_only=True)
    try:
        row = c.execute(
            "SELECT close, atr_14, date FROM bars WHERE ticker = ? AND atr_14 > 0 "
            "ORDER BY date DESC LIMIT 1", [tk]).fetchone()
        lo20 = c.execute(
            "SELECT min(low) FROM (SELECT low FROM bars WHERE ticker = ? ORDER BY date DESC LIMIT 20)",
            [tk]).fetchone()
    finally:
        c.close()
    if not row or not row[1]:
        return {"error": "no data for %s" % tk}
    close, atr, dt = row
    f = forecast(atr / close)
    f["ticker"] = tk; f["close"] = round(close, 2); f["atr"] = round(atr, 3)
    f["as_of"] = str(dt)[:10]
    if lo20 and lo20[0] and atr > 0:      # Q2: current position measured in ATR-units
        f["atr_units_off_20d_low"] = round((close - lo20[0]) / atr, 2)
    return f


if __name__ == "__main__":
    import sys, json
    print(json.dumps(forecast_ticker(sys.argv[1] if len(sys.argv) > 1 else "MXL"), indent=2))
