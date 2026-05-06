"""
Split Universe Service — lifecycle + stock-only filter v2.

Fetches reverse stock split tickers over a [-90d, +14d] window and builds a
lifecycle-aware universe.  Only operating-company common shares are included;
ETFs, funds, trusts, leveraged products, warrants, preferred shares, etc. are
excluded via a tiered filter.

Cache: 6h in-memory, version-keyed — SPLIT_CACHE_VERSION change forces rebuild.
"""

import re
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timedelta, date as date_t
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

SPLIT_CACHE_VERSION = "split_lifecycle_stock_filter_v2"

# ── Lifecycle constants ───────────────────────────────────────────────────────
SPLIT_HISTORY_DAYS  = 90   # look back this many calendar days
SPLIT_FUTURE_DAYS   = 14   # look ahead this many calendar days
SPLIT_WATCH_BASE    = 60   # base watch window after split (days)
SPLIT_WATCH_HIGH    = 75   # ratio >= 10
SPLIT_WATCH_EXTREME = 90   # ratio >= 20

# ── Stock-only filter — tiered rules ─────────────────────────────────────────

# Tier 1: explicit type-field values that signal non-stock
_TYPE_FIELDS = (
    "assetType", "instrumentType", "securityType",
    "issueType", "type", "category", "marketCategory",
)
_TYPE_NON_STOCK: frozenset = frozenset({
    "etf", "etn", "etp", "fund", "trust",
    "warrant", "warrants", "right", "rights", "unit", "units",
    "preferred", "preferred stock", "note", "notes", "bond",
    "closed-end", "closed end", "exchange-traded fund",
    "exchange-traded note", "depositary receipt",
})

# Tier 2: high-confidence compound phrases (substring after lower())
_PRODUCT_PHRASES: List[str] = [
    # Type abbreviations
    "etf", "etn", "etp",
    # Exchange-traded wrappers
    "exchange traded fund", "exchange-traded fund",
    "exchange traded note", "exchange-traded note",
    "closed-end fund", "closed end fund",
    # Product strategies
    "option income strategy",
    "leveraged etf", "inverse etf",
    # Leverage denominators
    "daily 2x", "daily 3x", "daily -2x", "daily -3x",
    # Ultra/ProShares patterns
    "ultrashort", "ultra short", "ultrapro", "ultra pro",
    # Bear/bull directional leveraged
    "bear 2x", "bear 3x", "bull 2x", "bull 3x",
    "2x long", "2x short", "3x long", "3x short",
    # Fixed-income / debt securities
    "senior notes", "notes due", "baby bond",
    # Other security types
    "depositary shares", "depositary receipts",
    "preferred stock",
    # Short / inverse product names
    "short etf", "short fund",
]

# Tier 3: issuer brand names that are exclusively ETF/ETP/fund issuers
_ISSUER_BRANDS: List[str] = [
    "direxion", "proshares", "ishares", "spdr", "wisdomtree",
    "yieldmax", "defiance", "graniteshares", "roundhill", "t-rex",
    "rex shares", "ark etf", "vanguard etf", "invesco etf", "global x",
    "first trust etf", "vaneck etf", "jpmorgan etf", "kraneshares",
    "amplify etf", "simplify etf", "innovator etf", "bitwise etf",
    "grayscale trust",
]

# Tier 4: security-class regex patterns (word-boundary safe)
# "Bear Creek Mining" → \bbear\b matches but only "bear 2x/3x" in Tier 2 fires.
# "TrustCo Bank" → "trustco" does NOT match \btrust\b (no boundary after 't').
# "Ultragenyx" → no compound Tier 2 match; Tier 4 has no standalone \bultra\b.
# "Notable Labs" → "notable" ≠ \bnotes?\b (boundary check fails in "notable").
# "Banknote Corp" → "banknote" ≠ \bnotes?\b (it would actually NOT match since
#   "banknote" has no boundary before "note" — it's one word token).
_SECURITY_CLASS_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r'\bfund\b'),             "fund"),
    (re.compile(r'\bclosed[- ]?end\b'),   "closed-end"),
    (re.compile(r'\btrust\b'),            "trust"),
    (re.compile(r'\bwarrants?\b'),        "warrant"),
    (re.compile(r'\brights?\b'),          "rights"),
    (re.compile(r'\bunits?\b'),           "units"),
    (re.compile(r'\bpreferred\b'),        "preferred"),
    (re.compile(r'\binverse\b'),          "inverse"),
]

# Fields to concatenate for name-based matching
_NAME_FIELDS = (
    "companyName", "securityName", "name",
    "assetType", "instrumentType", "securityType", "issueType",
)


def is_stock_like_split_event(row: dict) -> Tuple[bool, Optional[str]]:
    """Return (True, None) for operating-company stocks; (False, reason) otherwise.

    Decision order:
      A. Explicit type field indicates non-stock → exclude.
      B. High-confidence product phrase or issuer brand → exclude.
      C. Security-class regex pattern → exclude.
      D. Default → include.

    Missing or None fields are treated as empty — unknown tickers are not
    auto-excluded unless a blacklist rule fires.
    """
    # Tier A — explicit type fields
    for field in _TYPE_FIELDS:
        val = (row.get(field) or "").strip().lower()
        if val and val in _TYPE_NON_STOCK:
            return False, f"explicit_type:{field}={val}"

    # Build normalized text from all name/type fields
    raw_text = " ".join(str(row.get(k) or "") for k in _NAME_FIELDS)
    text = re.sub(r'\s+', ' ', raw_text).strip().lower()

    # Tier B — product phrases
    for phrase in _PRODUCT_PHRASES:
        if phrase in text:
            return False, f"product_phrase:{phrase}"

    # Tier B — issuer brand names
    for brand in _ISSUER_BRANDS:
        if brand in text:
            return False, f"issuer_keyword:{brand}"

    # Tier C — security-class word-boundary patterns
    for pattern, label in _SECURITY_CLASS_PATTERNS:
        if pattern.search(text):
            return False, f"security_class:{label}"

    return True, None


# ── Lifecycle classification ──────────────────────────────────────────────────

def classify_split_lifecycle(
    split_date_str: str,
    ratio: float,
    today_dt: Optional[date_t] = None,
) -> dict:
    """Return lifecycle metadata for a split event.

    days_offset convention:
      < 0 → split is N days in the future  (PRE / UPCOMING)
        0 → split is today                 (D0)
      > 0 → split was N days ago           (W1/W2/W3/POST/EXT)
    """
    today = today_dt or datetime.now().date()
    split_date = datetime.strptime(split_date_str, "%Y-%m-%d").date()
    days_offset = (today - split_date).days  # positive=past, negative=future

    watch_days = SPLIT_WATCH_BASE
    if ratio >= 20:
        watch_days = SPLIT_WATCH_EXTREME
    elif ratio >= 10:
        watch_days = SPLIT_WATCH_HIGH

    watch_until = split_date + timedelta(days=watch_days)

    # Phase
    if days_offset < -7:
        phase, wave = "UPCOMING_FAR", "FAR"
    elif days_offset <= -1:
        phase, wave = "PRE_SPLIT", "PRE"
    elif days_offset == 0:
        phase, wave = "SPLIT_DAY", "D0"
    elif days_offset <= 7:
        phase, wave = "WAVE_1", "W1"
    elif days_offset <= 20:
        phase, wave = "WAVE_2_SETUP", "W2"
    elif days_offset <= 45:
        phase, wave = "WAVE_3_SETUP", "W3"
    elif days_offset <= 60:
        phase, wave = "POST_MONITOR", "POST"
    elif days_offset <= watch_days:
        phase, wave = "EXTENDED_MONITOR", "EXT"
    else:
        phase, wave = "EXPIRED", "EXPIRED"

    # Heat score (metadata/context only — not a buy signal)
    _phase_heat = {
        "PRE_SPLIT": 2, "SPLIT_DAY": 3, "WAVE_1": 4,
        "WAVE_2_SETUP": 3, "WAVE_3_SETUP": 2,
        "POST_MONITOR": 1, "EXTENDED_MONITOR": 1,
    }
    heat = _phase_heat.get(phase, 0)
    if ratio >= 20:
        heat += 3
    elif ratio >= 10:
        heat += 2
    elif ratio >= 5:
        heat += 1
    heat = max(0, min(10, heat))

    # Next wave window (dates relative to split_date)
    next_wave_label = next_start = next_end = None
    if phase in ("PRE_SPLIT", "SPLIT_DAY"):
        next_wave_label = "WAVE_1"
        next_start = split_date + timedelta(days=1)
        next_end   = split_date + timedelta(days=7)
    elif phase == "WAVE_1":
        next_wave_label = "WAVE_2_SETUP"
        next_start = split_date + timedelta(days=8)
        next_end   = split_date + timedelta(days=20)
    elif phase == "WAVE_2_SETUP":
        next_wave_label = "WAVE_3_SETUP"
        next_start = split_date + timedelta(days=21)
        next_end   = split_date + timedelta(days=45)
    elif phase == "WAVE_3_SETUP":
        next_wave_label = "POST_MONITOR"
        next_start = split_date + timedelta(days=46)
        next_end   = split_date + timedelta(days=60)

    return {
        "days_offset":           days_offset,
        "phase":                 phase,
        "wave":                  wave,
        "watch_until":           watch_until.isoformat(),
        "watch_days":            watch_days,
        "next_wave_label":       next_wave_label,
        "next_wave_start_date":  next_start.isoformat() if next_start else None,
        "next_wave_end_date":    next_end.isoformat()   if next_end   else None,
        "heat_score":            heat,
        "notes":                 f"ratio={ratio:.0f}:1  phase={phase}  D{days_offset:+d}",
    }


# ── Service ───────────────────────────────────────────────────────────────────

class SplitUniverseService:
    CACHE_DURATION_HOURS = 6
    MIN_RATIO            = 2.0

    def __init__(self) -> None:
        self._cache:         Optional[List[dict]] = None
        self._cache_time:    Optional[datetime]   = None
        self._cache_version: Optional[str]        = None

    # ── Public API ────────────────────────────────────────────────────────────

    def get_split_universe(self) -> List[dict]:
        if self._is_cache_valid():
            return self._cache or []

        raw = self._fetch_nasdaq_splits()
        total_calendar_rows = len(raw)

        # Stock-only filter
        stock_rows: List[dict]    = []
        excluded:   List[tuple]   = []
        for r in raw:
            ok, reason = is_stock_like_split_event(r)
            if ok:
                stock_rows.append(r)
            else:
                excluded.append((r.get("ticker", "?"), r.get("companyName", ""), reason or ""))

        # Ratio filter (keep reverse splits only)
        after_ratio = [r for r in stock_rows if r.get("ratio") and r["ratio"] >= self.MIN_RATIO]
        filtered_forward_invalid = len(stock_rows) - len(after_ratio)
        reverse_split_rows = len(after_ratio)

        # Lifecycle computation
        today_dt = datetime.now().date()
        for r in after_ratio:
            lc = classify_split_lifecycle(r["split_date"], r["ratio"], today_dt)
            r.update(lc)
            r["split_status"] = "upcoming" if lc["days_offset"] < 0 else "executed"

        # Lifecycle filter: keep active phases only
        active = [
            r for r in after_ratio
            if r["phase"] not in ("EXPIRED", "UPCOMING_FAR")
        ]
        lifecycle_active_rows = len(active)
        expired_rows = reverse_split_rows - lifecycle_active_rows

        # Dedupe by ticker: prefer active/closest lifecycle event
        results = self._dedupe_by_ticker(active)

        # Logging
        log.info(
            "split universe refreshed: total_calendar=%d  reverse_splits=%d  "
            "filtered_non_stock=%d  filtered_forward_invalid=%d  "
            "lifecycle_active=%d  expired=%d  final=%d",
            total_calendar_rows, reverse_split_rows,
            len(excluded), filtered_forward_invalid,
            lifecycle_active_rows, expired_rows, len(results),
        )
        for ticker, name, reason in excluded[:20]:
            rule_type = reason.split(":")[0] if ":" in reason else reason
            log.debug(
                "split excluded: ticker=%s name=%.60s reason=%s rule_type=%s",
                ticker, name, reason, rule_type,
            )
        if len(excluded) > 20:
            log.debug("split excluded: ... and %d more (not shown)", len(excluded) - 20)

        self._cache         = results
        self._cache_time    = datetime.now()
        self._cache_version = SPLIT_CACHE_VERSION
        return results

    def get_split_tickers(self) -> List[str]:
        return [r["ticker"] for r in self.get_split_universe() if r.get("ticker")]

    def get_split_meta(self) -> Dict[str, dict]:
        """Returns {TICKER: meta-dict} for fast row enrichment."""
        return {r["ticker"]: r for r in self.get_split_universe()}

    # ── Internal ──────────────────────────────────────────────────────────────

    def _fetch_nasdaq_splits(self) -> List[dict]:
        """One request per date in the [-HISTORY, +FUTURE] window."""
        url   = "https://api.nasdaq.com/api/calendar/splits"
        today = datetime.now()
        out:  List[dict] = []

        for offset in range(-SPLIT_HISTORY_DAYS, SPLIT_FUTURE_DAYS + 1):
            date_str = (today + timedelta(days=offset)).strftime("%Y-%m-%d")
            try:
                req = urllib.request.Request(
                    f"{url}?date={date_str}",
                    headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.load(resp)
            except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as exc:
                log.debug("split fetch %s skipped: %s", date_str, exc)
                continue
            except Exception as exc:
                log.warning("split fetch %s error: %s", date_str, exc)
                continue

            rows = (data.get("data") or {}).get("rows") or []
            for row in rows:
                ratio_str = row.get("ratio") or ""
                ratio     = self._parse_ratio(ratio_str)
                ticker    = (row.get("symbol") or "").strip().upper()
                if not ticker or ratio is None:
                    continue
                out.append({
                    "ticker":       ticker,
                    "split_date":   date_str,
                    "ratio":        ratio,
                    "ratio_str":    ratio_str,
                    "source":       "nasdaq",
                    # Name/type fields used by the stock-only filter
                    "companyName":  row.get("companyName") or row.get("name") or "",
                    "securityName": row.get("securityName") or "",
                    "assetType":    row.get("assetType") or "",
                    "issueType":    row.get("issueType") or "",
                })
        return out

    @staticmethod
    def _parse_ratio(ratio_str: str) -> Optional[float]:
        """'1:10', '1-for-10', '1/20' → 10.0;  '10:1' → 0.1 (filtered by MIN_RATIO)."""
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
        """Keep best lifecycle event per ticker.

        Priority:
          1. Active lifecycle over expired/far (already pre-filtered, but guard here).
          2. Smallest |days_offset| among ties.
        """
        seen: Dict[str, dict] = {}
        for r in results:
            t     = r["ticker"]
            phase = r.get("phase", "EXPIRED")
            active = phase not in ("EXPIRED", "UPCOMING_FAR")
            doff   = abs(r.get("days_offset", 999))

            if t not in seen:
                seen[t] = r
            else:
                e = seen[t]
                e_active = e.get("phase", "EXPIRED") not in ("EXPIRED", "UPCOMING_FAR")
                e_doff   = abs(e.get("days_offset", 999))

                if active and not e_active:
                    seen[t] = r                  # new is active, old is not
                elif e_active and not active:
                    pass                          # keep existing active
                elif doff < e_doff:
                    seen[t] = r                  # both same; prefer closer to D0
        return list(seen.values())

    def _is_cache_valid(self) -> bool:
        if not self._cache_time or self._cache is None:
            return False
        if self._cache_version != SPLIT_CACHE_VERSION:
            return False
        return (datetime.now() - self._cache_time).total_seconds() < self.CACHE_DURATION_HOURS * 3600


split_service = SplitUniverseService()
