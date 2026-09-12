"""tests/synthetic_ohlcv.py — one deterministic OHLCV frame, in-repo, for HERMETIC tests.

WHY IT IS 800 BARS AND NOT 300. The causality test has to exercise the same engine surface the
real-corpus test does, and some of those engines reach back a long way: compute_emas builds
ema200, and the warm-up contract (v3a_warmup_contract) puts a recursive ewm(span=200) at ~600
bars before its seed stops dominating. A 300-bar frame would leave the long engines seed-bound
and the test would be comparing two equally meaningless numbers. 800 bars leaves a usable window
of mature truncation points above MIN_MATURE_INDEX.

WHY IT IS NOT SMOOTH. A quiet random walk fires almost nothing: no gaps, one volume bucket, no
T/Z transitions worth the name. The test would then pass by asserting that False still equals
False. So the generator walks through explicit regimes — trend up, chop, sharp decline,
low-volatility coil, recovery, whipsaw — and injects true chart gaps (open beyond the previous
bar's high or low, which is what the G1/G2/G3 classes key on) and volume spikes and lulls wide
enough to reach every WLNBB bucket. `assert_rich_enough` pins that, so a future edit to the
generator cannot quietly make the fixture toothless.

DETERMINISM. numpy's default_rng(PCG64) is stable across versions by numpy's own compatibility
policy, and CHECKSUM below pins the actual values. If numpy ever breaks that promise the
checksum fails loudly rather than the fixture drifting under the tests.
"""
from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

N_BARS = 800
SEED = 20260912
MIN_MATURE_INDEX = 620          # > ema200 maturity (~600) from the warm-up contract
CHECKSUM = "de9f91512c7b4d3e"      # pinned 2026-09-12; a change here means the fixture drifted

# (n_bars, drift_per_bar, volatility, volume_multiplier) — a deliberate tour of regimes
_REGIMES = [
    (120,  0.0012, 0.011, 1.00),   # steady advance
    (90,   0.0000, 0.008, 0.70),   # chop, quiet
    (70,  -0.0035, 0.026, 1.80),   # sharp decline, heavy volume
    (80,   0.0002, 0.004, 0.45),   # low-volatility coil
    (110,  0.0020, 0.014, 1.30),   # recovery
    (90,  -0.0005, 0.030, 1.60),   # whipsaw
    (100,  0.0015, 0.009, 0.85),   # grind up, thinning
    (140,  0.0000, 0.018, 1.10),   # range, average volume
]


def synthetic_ohlcv(n: int = N_BARS, seed: int = SEED) -> pd.DataFrame:
    """A deterministic daily OHLCV frame with gaps, regime and volume structure."""
    rng = np.random.default_rng(seed)
    drift = np.empty(n)
    vol = np.empty(n)
    vmul = np.empty(n)
    i = 0
    for length, d, v, m in _REGIMES:
        j = min(i + length, n)
        drift[i:j], vol[i:j], vmul[i:j] = d, v, m
        i = j
        if i >= n:
            break
    if i < n:                                  # pad with the last regime if _REGIMES is short
        drift[i:], vol[i:], vmul[i:] = _REGIMES[-1][1], _REGIMES[-1][2], _REGIMES[-1][3]

    close = np.empty(n)
    open_ = np.empty(n)
    high = np.empty(n)
    low = np.empty(n)
    volume = np.empty(n)

    px = 42.0
    prev_high = px * 1.01
    prev_low = px * 0.99
    for k in range(n):
        # ── open: a true chart gap on ~9 % of bars, otherwise near the previous close ──
        if k == 0:
            o = px
        elif rng.random() < 0.09:
            size = (0.3 + 2.4 * rng.random()) * vol[k]          # 0.3-2.7 sigma beyond the bar
            o = prev_high * (1.0 + size) if rng.random() < 0.5 else prev_low * (1.0 - size)
        else:
            o = px * float(np.exp(0.25 * vol[k] * rng.standard_normal()))

        ret = drift[k] + vol[k] * rng.standard_normal()
        c = o * float(np.exp(ret))
        span = abs(c - o) + vol[k] * o * (0.15 + 1.1 * rng.random())
        up_wick = span * rng.random() * 0.9
        dn_wick = span * rng.random() * 0.9
        h = max(o, c) + up_wick
        lo = min(o, c) - dn_wick
        lo = max(lo, 0.01)

        # ── volume: lognormal base, occasional spike, occasional drought ──
        v = 1.0e6 * vmul[k] * float(np.exp(0.45 * rng.standard_normal()))
        r = rng.random()
        if r < 0.05:
            v *= 3.0 + 5.0 * rng.random()        # spike — reaches the top WLNBB buckets
        elif r > 0.95:
            v *= 0.15 + 0.15 * rng.random()      # drought — reaches the bottom bucket

        open_[k], high[k], low[k], close[k], volume[k] = o, h, lo, c, v
        px, prev_high, prev_low = c, h, lo

    dates = pd.bdate_range("2019-01-02", periods=n).strftime("%Y-%m-%d")
    return pd.DataFrame({"date": dates, "open": open_, "high": high, "low": low,
                         "close": close, "volume": np.round(volume)})


def checksum(df: pd.DataFrame) -> str:
    """Stable digest of the generated values — pins determinism independently of numpy."""
    b = np.ascontiguousarray(
        df[["open", "high", "low", "close", "volume"]].to_numpy(np.float64).round(6))
    return hashlib.sha256(b.tobytes() + "|".join(df["date"]).encode()).hexdigest()[:16]


def assert_rich_enough(df: pd.DataFrame) -> dict:
    """The fixture must actually make the engines fire. A frame that produces no gaps and one
    volume bucket would let the causality test pass by comparing False with False."""
    o, h, lo, c = (df[k].to_numpy() for k in ("open", "high", "low", "close"))
    v = df["volume"].to_numpy()
    ph, pl, pc = np.r_[np.nan, h[:-1]], np.r_[np.nan, lo[:-1]], np.r_[np.nan, c[:-1]]
    gaps = int(np.nansum((o > ph) | (o < pl)))
    up, dn = int((c > o).sum()), int((c < o).sum())
    vol_ratio = v / pd.Series(v).rolling(20, min_periods=20).mean().to_numpy()
    stats = dict(
        bars=len(df), gaps=gaps, up_bars=up, down_bars=dn,
        vol_spikes=int(np.nansum(vol_ratio > 2.5)), vol_droughts=int(np.nansum(vol_ratio < 0.4)),
        range_pct_p10=float(np.nanpercentile((h - lo) / c * 100, 10)),
        range_pct_p90=float(np.nanpercentile((h - lo) / c * 100, 90)),
        max_drawdown_pct=float((1 - c / np.maximum.accumulate(c)).max() * 100),
    )
    assert stats["bars"] >= MIN_MATURE_INDEX + 100, stats
    assert stats["gaps"] >= 40, f"too few true chart gaps — G1/G2/G3 would never fire: {stats}"
    assert min(up, dn) >= 250, f"one-sided frame — T and Z cannot both fire: {stats}"
    assert stats["vol_spikes"] >= 15 and stats["vol_droughts"] >= 15, \
        f"volume too flat — the WLNBB buckets would collapse: {stats}"
    assert stats["range_pct_p90"] / max(stats["range_pct_p10"], 1e-9) >= 3.0, \
        f"no volatility regime change: {stats}"
    assert stats["max_drawdown_pct"] >= 15.0, f"no real decline in the frame: {stats}"
    return stats


if __name__ == "__main__":
    d = synthetic_ohlcv()
    print("checksum:", checksum(d))
    for k, val in assert_rich_enough(d).items():
        print(f"  {k:<18} {val}")
