"""
Split Universe Service.

Fetches reverse stock split tickers (executed in the last 7 days OR
scheduled in the next 5 days). Used by the Turbo screener as a virtual
universe so split-driven squeeze candidates can be scanned with the
same SIG/RTB/sector filters and signal columns as any other universe.

Source: Nasdaq.com public splits-calendar JSON endpoint.
Cache: in-memory, 6h TTL — keeps API hits low.
Filter: ratio >= 2.0 (only reverse splits where 1 new = >= 2 old).
"""

import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


class SplitUniverseService:
    CACHE_DURATION_HOURS = 6
    LOOKBACK_DAYS  = 7
    LOOKAHEAD_DAYS = 5
    MIN_RATIO      = 2.0

    def __init__(self) -> None:
        self._cache: Optional[List[dict]] = None
        self._cache_time: Optional[datetime] = None

    # ── Public API ────────────────────────────────────────────────────────────
    def get_split_universe(self) -> List[dict]:
        if self._is_cache_valid():
            return self._cache or []
        results = self._fetch_nasdaq_splits()
        results = self._dedupe_by_ticker(results)
        results = [r for r in results if r["ratio"] and r["ratio"] >= self.MIN_RATIO]
        self._cache = results
        self._cache_time = datetime.now()
        log.info("split universe refreshed: %d tickers", len(results))
        return results

    def get_split_tickers(self) -> List[str]:
        return [r["ticker"] for r in self.get_split_universe() if r.get("ticker")]

    def get_split_meta(self) -> Dict[str, dict]:
        """Returns {TICKER: meta-dict} for fast row enrichment."""
        return {r["ticker"]: r for r in self.get_split_universe()}

    # ── Internal ──────────────────────────────────────────────────────────────
    def _fetch_nasdaq_splits(self) -> List[dict]:
        """One request per date in the [-LOOKBACK, +LOOKAHEAD] window."""
        url = "https://api.nasdaq.com/api/calendar/splits"
        today = datetime.now()
        out: List[dict] = []
        for offset in range(-self.LOOKBACK_DAYS, self.LOOKAHEAD_DAYS + 1):
            date = (today + timedelta(days=offset)).strftime("%Y-%m-%d")
            try:
                req = urllib.request.Request(
                    f"{url}?date={date}",
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json",
                    },
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.load(resp)
            except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as exc:
                log.debug("split fetch %s skipped: %s", date, exc)
                continue
            except Exception as exc:
                log.warning("split fetch %s error: %s", date, exc)
                continue

            rows = (data.get("data") or {}).get("rows") or []
            for row in rows:
                ratio_str = row.get("ratio") or ""
                ratio = self._parse_ratio(ratio_str)
                ticker = (row.get("symbol") or "").strip().upper()
                if not ticker or ratio is None:
                    continue
                out.append({
                    "ticker":      ticker,
                    "split_date":  date,
                    "ratio":       ratio,
                    "ratio_str":   ratio_str,
                    "status":      "executed" if offset <= 0 else "upcoming",
                    "days_offset": offset,
                    "source":      "nasdaq",
                })
        return out

    @staticmethod
    def _parse_ratio(ratio_str: str) -> Optional[float]:
        """'1:10', '1-for-10', '1/20' → 10.0   (forward 10:1 → 0.1)."""
        if not ratio_str:
            return None
        s = ratio_str.replace("-for-", ":").replace("/", ":").replace(" ", "")
        if ":" in s:
            try:
                a, b = s.split(":", 1)
                af, bf = float(a), float(b)
                return bf / af if af > 0 else None
            except (ValueError, ZeroDivisionError):
                return None
        return None

    @staticmethod
    def _dedupe_by_ticker(results: List[dict]) -> List[dict]:
        seen: Dict[str, dict] = {}
        for r in results:
            t = r["ticker"]
            if t not in seen or abs(r["days_offset"]) < abs(seen[t]["days_offset"]):
                seen[t] = r
        return list(seen.values())

    def _is_cache_valid(self) -> bool:
        if not self._cache_time or self._cache is None:
            return False
        return (datetime.now() - self._cache_time).total_seconds() < self.CACHE_DURATION_HOURS * 3600


split_service = SplitUniverseService()
