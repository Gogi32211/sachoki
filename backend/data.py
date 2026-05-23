"""
data.py — OHLCV fetch with Massive (Polygon) as the canonical source.

Phase 0 (260523): yfinance removed as primary source. All OHLCV fetches go
through Massive/Polygon via data_polygon.fetch_bars(). yfinance is retained
only as a fallback when MASSIVE_API_KEY is missing OR when the
ALLOW_YFINANCE_FALLBACK env var is set explicitly. By default, missing key
raises a clear error instead of silently falling back.

5-min in-memory cache (+ optional Redis) wraps the result.
"""
from __future__ import annotations
import os, time, logging
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

CACHE_TTL = 300  # 5 minutes

_mem: dict[str, tuple[pd.DataFrame, float]] = {}

# Per-interval lookback days when caller uses bars-mode.
# We over-fetch (bars × 1.6 + 10) so trimming to `bars` always has enough data.
_INTERVAL_DAYS_PER_BAR = {
    "1d": 1.6, "1wk": 7.5, "1w": 7.5,
    "1h": 0.20, "4h": 0.30,
    "30m": 0.10, "15m": 0.07, "5m": 0.03, "1m": 0.01,
}

# Yfinance fallback is OFF by default. Set ALLOW_YFINANCE_FALLBACK=1 to enable.
_ALLOW_YF_FALLBACK = os.environ.get("ALLOW_YFINANCE_FALLBACK", "0").strip() in ("1", "true", "True")


def _redis():
    url = os.environ.get("REDIS_URL")
    if not url:
        return None
    try:
        import redis
        return redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _get(key: str) -> Optional[pd.DataFrame]:
    r = _redis()
    if r:
        raw = r.get(key)
        if raw:
            try:
                return pd.read_json(raw)
            except Exception:
                pass
    if key in _mem:
        df, ts = _mem[key]
        if time.time() - ts < CACHE_TTL:
            return df
    return None


def _set(key: str, df: pd.DataFrame):
    r = _redis()
    if r:
        try:
            r.setex(key, CACHE_TTL, df.to_json(date_format="iso"))
        except Exception:
            pass
    _mem[key] = (df, time.time())


def _bars_to_days(bars: int, interval: str) -> int:
    """Convert a bar-count request to a day-count for fetch_bars()."""
    per = _INTERVAL_DAYS_PER_BAR.get(interval, 1.6)
    return max(int(bars * per) + 10, 10)


def _fetch_yfinance(ticker: str, interval: str, bars: int) -> pd.DataFrame:
    """Fallback path — only used if ALLOW_YFINANCE_FALLBACK=1."""
    import yfinance as yf
    tf_period = {
        "1m": "7d", "5m": "60d", "15m": "60d", "30m": "60d",
        "1h": "730d", "4h": "730d", "1d": "5y", "1wk": "10y",
    }
    period = tf_period.get(interval, "5y")
    log.warning("data.py: yfinance fallback used for %s %s (Massive unavailable)",
                ticker, interval)
    try:
        raw = yf.Ticker(ticker).history(period=period, interval=interval, auto_adjust=True)
    except Exception as e:
        raise RuntimeError(f"yfinance error for {ticker}: {e}")
    if raw is None or raw.empty:
        raise RuntimeError(f"yfinance returned no data for {ticker}")
    raw.columns = [str(c).lower() for c in raw.columns]
    needed = {"open", "high", "low", "close"}
    missing = needed - set(raw.columns)
    if missing:
        raise RuntimeError(f"yfinance missing columns {missing} for {ticker}")
    cols = ["open", "high", "low", "close"] + (["volume"] if "volume" in raw.columns else [])
    df = raw[cols].dropna().tail(bars)
    if df.empty:
        raise RuntimeError(f"Empty dataframe after dropna for {ticker}")
    return df


def fetch_ohlcv(
    ticker: str,
    interval: str = "1d",
    bars: int = 500,
    since: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch OHLCV data — Massive (Polygon) primary, yfinance optional fallback.

    Args:
        ticker:    upper-cased
        interval:  '1d' | '4h' | '1h' | '30m' | '15m' | '1wk' | ...
        bars:      max bars to return (most recent)
        since:     optional ISO date 'YYYY-MM-DD'. When given, only bars at
                   or after this date are returned. Used by incremental scans.

    Returns:
        DataFrame indexed by UTC datetime with columns:
        open / high / low / close / volume.

    Raises:
        RuntimeError if Massive fails AND yfinance fallback is disabled
        (the default — set ALLOW_YFINANCE_FALLBACK=1 to opt back in).
    """
    ticker = ticker.upper().strip()
    cache_key = f"{ticker}:{interval}:{bars}:{since or '-'}"

    cached = _get(cache_key)
    if cached is not None:
        return cached

    # Compute lookback in days. If `since` is given we still call fetch_bars
    # with a day-count covering [since..today], then filter the result.
    if since:
        try:
            from datetime import date
            yr, mo, dy = map(int, since[:10].split("-"))
            since_date = date(yr, mo, dy)
            days = max((date.today() - since_date).days + 5, 5)
        except Exception:
            days = _bars_to_days(bars, interval)
    else:
        days = _bars_to_days(bars, interval)

    df: Optional[pd.DataFrame] = None
    massive_err: Optional[Exception] = None

    # ── Primary: Massive / Polygon ──────────────────────────────────────────
    try:
        from data_polygon import fetch_bars, polygon_available
        if polygon_available():
            df = fetch_bars(ticker, interval=interval, days=days)
        else:
            raise EnvironmentError(
                "MASSIVE_API_KEY (or POLYGON_API_KEY) not set — cannot fetch "
                f"{ticker}. Set the env var or enable ALLOW_YFINANCE_FALLBACK=1."
            )
    except Exception as e:
        massive_err = e
        log.warning("data.py: Massive fetch failed for %s: %s", ticker, e)

    # ── Fallback: yfinance (only if explicitly enabled) ─────────────────────
    if df is None or df.empty:
        if _ALLOW_YF_FALLBACK:
            df = _fetch_yfinance(ticker, interval, bars)
        else:
            raise RuntimeError(
                f"Massive fetch failed for {ticker} ({interval}): {massive_err}. "
                "yfinance fallback is disabled — set ALLOW_YFINANCE_FALLBACK=1 to enable."
            )

    if df is None or df.empty:
        raise RuntimeError(f"No data returned for {ticker}")

    # Apply `since` filter when requested
    if since:
        try:
            df = df[df.index >= pd.Timestamp(since, tz="UTC")]
        except Exception:
            # Index may be tz-naive in fallback path — best-effort
            df = df[df.index.astype(str) >= since]

    # Trim to last `bars`
    if bars and len(df) > bars:
        df = df.tail(bars)

    _set(cache_key, df)
    return df


# ──────────────────────────────────────────────────────────────────────────
# 260523 Phase 0: yfinance → Massive routing (single point of change)
# ──────────────────────────────────────────────────────────────────────────
# 16 backend files have direct `yf.Ticker(t).history(period=..., interval=...)`
# calls. Rewriting each is risky. Instead, monkey-patch yf.Ticker.history at
# module import so every call automatically routes through Massive via
# fetch_ohlcv() above. If Massive is unavailable AND ALLOW_YFINANCE_FALLBACK=1,
# the original yfinance method is used. Default: hard-error.
# ──────────────────────────────────────────────────────────────────────────

_PERIOD_TO_BARS = {
    "1d": 2, "5d": 7, "1mo": 32, "3mo": 95, "6mo": 190,
    "1y": 260, "2y": 520, "5y": 1300, "10y": 2600,
    "ytd": 260, "max": 5000,
    # Numeric "days" forms used by some callers (e.g. "90d", "180d")
}


def _period_to_bars(period, interval: str) -> int:
    """Convert a yfinance `period` string to a bar count for fetch_ohlcv()."""
    if period is None:
        return 500
    p = str(period).strip().lower()
    if p in _PERIOD_TO_BARS:
        return _PERIOD_TO_BARS[p]
    # "90d", "180d", "30d" — treat as days then convert to bars by interval
    if p.endswith("d") and p[:-1].isdigit():
        days = int(p[:-1])
        if interval in ("1d", "1wk", "1w"):
            return max(int(days * (1 if interval == "1d" else 0.2)) + 5, 5)
        # intraday: ~7 bars per day for 1h, etc.
        per_day = {"1h": 7, "4h": 2, "30m": 13, "15m": 26, "5m": 78, "1m": 390}
        return max(days * per_day.get(interval, 7), 5)
    return 500


def _install_yfinance_patch() -> None:
    """Replace yfinance.Ticker.history with a Massive-routing wrapper.

    Idempotent — safe to call multiple times.
    """
    try:
        import yfinance as _yf
    except ImportError:
        return  # yfinance not installed at all → nothing to patch

    if getattr(_yf.Ticker, "_sachoki_patched", False):
        return  # already patched

    _original_history = _yf.Ticker.history

    def _patched_history(self, period=None, interval="1d", start=None,
                         end=None, **kwargs):
        # If caller passes start/end, fall back to original to preserve
        # date-range semantics (rare; mostly diagnostic code).
        if start is not None or end is not None:
            if _ALLOW_YF_FALLBACK:
                log.debug("yfinance start/end path bypasses Massive — using yf")
                return _original_history(self, period=period, interval=interval,
                                         start=start, end=end, **kwargs)
            raise RuntimeError(
                f"yfinance.Ticker.history(start=, end=) is not supported by "
                f"the Massive routing layer. Caller for {self.ticker} must "
                f"migrate to data.fetch_ohlcv(ticker, interval, since=...)."
            )

        bars = _period_to_bars(period, interval)
        try:
            df = fetch_ohlcv(self.ticker, interval=interval, bars=bars)
        except Exception as exc:
            if _ALLOW_YF_FALLBACK:
                log.warning("Massive→yfinance fallback for %s: %s",
                            self.ticker, exc)
                return _original_history(self, period=period, interval=interval,
                                         **kwargs)
            raise

        # yfinance returns columns title-cased: Open/High/Low/Close/Volume.
        # Our fetch_ohlcv returns lowercase. Title-case to preserve the
        # contract callers expect.
        out = df.copy()
        out.columns = [str(c).title() for c in out.columns]
        return out

    _yf.Ticker.history = _patched_history
    _yf.Ticker._sachoki_patched = True
    log.info("data.py: yfinance.Ticker.history routed through Massive "
             "(ALLOW_YFINANCE_FALLBACK=%s)", _ALLOW_YF_FALLBACK)


# Auto-install on module import so any backend code that imports yfinance
# AFTER data.py gets the patched behaviour transparently.
_install_yfinance_patch()
