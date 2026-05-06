"""
Split Universe Service.

Fetches reverse stock split tickers (executed in the last 7 days OR
scheduled in the next 5 days). Used by the Turbo screener as a virtual
universe so split-driven squeeze candidates can be scanned with the
same SIG/RTB/sector filters and signal columns as any other universe.

Source: Nasdaq.com public splits-calendar JSON endpoint.
Cache: in-memory, 6h TTL — keeps API hits low.
Filter: ratio >= 2.0 (only reverse splits where 1 new = >= 2 old).
         + stock-only filter (excludes ETFs, funds, trusts, etc.)
"""

import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# ── Non-stock keyword blacklist ───────────────────────────────────────────────
# Matched case-insensitively against the concatenated name/type fields of each
# split event. Compound and issuer-specific terms are generally safe; shorter
# words (Bond, Bear, Ultra) are intentionally kept because reverse splits on
# those instruments are almost always non-operating vehicles.
NON_STOCK_KEYWORDS: List[str] = [
    # Structure / wrapper type
    "ETF", "ETN", "ETP",
    "Fund", "Trust",
    "Closed End", "Closed-End",
    "Income Fund",
    # Fixed income / debt
    "Bond", "Treasury", "Notes", "Note",
    # Leverage / inverse products
    "Ultra", "2x", "3x", "-2x", "-3x", "Inverse",
    "Bear", "Bull 2X", "Bull 3X",
    # Known ETF/ETP issuers
    "Direxion", "ProShares", "iShares", "SPDR",
    "Vanguard ETF", "Invesco ETF", "WisdomTree",
    "Global X", "YieldMax", "Defiance",
    "GraniteShares", "Roundhill", "T-Rex",
    "Rex Shares", "ARK ETF", "First Trust",
    "VanEck ETF", "JPMorgan ETF",
    # Security types
    "Preferred", "Warrant", "Rights", "Unit",
]

# Fields from the Nasdaq API row used to build the name text for filtering.
# Nasdaq's split calendar typically provides at least "companyName".
_NAME_FIELDS = (
    "companyName", "securityName", "name",
    "assetType", "instrumentType", "securityType", "issueType",
)


def is_stock_like_split_event(row: dict) -> bool:
    """Return True if the split event looks like an operating company stock.

    Checks the concatenation of all available name/type fields against
    NON_STOCK_KEYWORDS. Missing fields are treated as empty (not rejected).
    """
    text = " ".join(str(row.get(k) or "") for k in _NAME_FIELDS).lower()

    for bad in NON_STOCK_KEYWORDS:
        if bad.lower() in text:
            log.debug(
                "split non-stock excluded: %s (%s) — matched keyword '%s'",
                row.get("ticker", "?"), text[:80], bad,
            )
            return False

    return True


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

        raw = self._fetch_nasdaq_splits()
        total_rows = len(raw)

        deduped = self._dedupe_by_ticker(raw)
        after_ratio = [r for r in deduped if r["ratio"] and r["ratio"] >= self.MIN_RATIO]
        reverse_rows = len(after_ratio)

        results = [r for r in after_ratio if is_stock_like_split_event(r)]
        stock_like_rows = len(results)
        filtered_non_stock = reverse_rows - stock_like_rows

        log.info(
            "split universe refreshed: total=%d  reverse_splits=%d  "
            "stock_like=%d  filtered_non_stock=%d",
            total_rows, reverse_rows, stock_like_rows, filtered_non_stock,
        )

        self._cache = results
        self._cache_time = datetime.now()
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
                    "ticker":       ticker,
                    "split_date":   date,
                    "ratio":        ratio,
                    "ratio_str":    ratio_str,
                    "status":       "executed" if offset <= 0 else "upcoming",
                    "days_offset":  offset,
                    "source":       "nasdaq",
                    # Name/type fields passed through for stock-only filtering
                    "companyName":  row.get("companyName") or row.get("name") or "",
                    "securityName": row.get("securityName") or "",
                    "assetType":    row.get("assetType") or "",
                    "issueType":    row.get("issueType") or "",
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
