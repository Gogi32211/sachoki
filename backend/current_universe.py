"""The CURRENT operational S&P 500 universe — a read-time overlay, never a DB rewrite.

TWO DIFFERENT CONCEPTS THAT WERE BEING CONFLATED.

    HISTORICAL UNIVERSE            bars.universe='sp500' in the canonical DuckDB.
                                   618 tickers. Legacy historical semantics, relied on by
                                   workflows that already depend on it. UNCHANGED.

    CURRENT OPERATIONAL UNIVERSE   the frozen 503-security snapshot in Massive's own
                                   nomenclature. What a scan of "the S&P 500 today" means.

The operational scan was deriving today's membership from historical bar tags, so it
scanned 618 names: ~25 delisted or renamed (ATVI, DRE, PXD, SQ, RE, FLT, PEAK, MMC …) and
~92 that are alive and simply no longer in the index (BABA, NIO, SHOP, SNOW, LYFT …). Those
two groups are different facts and this module does not merge them — it just doesn't ask
the historical table who is a member today.

THREE LAYERS PRODUCED THE SAME DEFECT, and all three are bypassed here rather than patched
one ticker at a time:

    scanner.get_tickers()          scrapes Wikipedia live, then rewrites x.replace(".", "-")
                                   so BRK.B becomes BRK-B — a string Massive does not serve
    scanner._FALLBACK              671 hardcoded names unioned in, 132 of them not current
    _is_valid_stock_ticker()       rejects any dotted ticker as "preferred", which discards
                                   BRK.B and BF.B — both ordinary index constituents

Membership here comes from the sealed artifacts and from nothing else. It is not inferred
from ticker availability, not regenerated from market data, and not read back out of the
bars table. If a member has no local history that is a COVERAGE fact reported per security,
not a reason to quietly shorten the universe.

    get_current_sp500_massive_tickers()  -> 503 Massive-canonical ticker strings
    get_current_sp500_securities()       -> the same, with identity attached
"""
from __future__ import annotations

import hashlib
import json
import os
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
MAP = os.path.join(HERE, "SP500_CURRENT_MASSIVE_TICKER_MAP_V1.json")
SNAPSHOT = os.path.join(HERE, "SP500_CURRENT_SNAPSHOT_V1.json")
EXPECTED_COUNT = 503

_lock = threading.Lock()
_cache: dict = {}


class CurrentUniverseUnavailable(RuntimeError):
    """The frozen current-universe artifacts are missing, unreadable, or inconsistent.

    Raised rather than falling back to a scraped or hardcoded list: a silent fallback is
    exactly how the stale 618-name universe survived unnoticed.
    """


def _load() -> dict:
    with _lock:
        if _cache:
            return _cache
        for p in (MAP, SNAPSHOT):
            if not os.path.exists(p):
                raise CurrentUniverseUnavailable(f"missing frozen artifact: {p}")
        m = json.load(open(MAP))
        snap = json.load(open(SNAPSHOT))
        if m.get("status") != "FROZEN":
            raise CurrentUniverseUnavailable(f"ticker map status is {m.get('status')!r}")
        if snap.get("status") != "FROZEN":
            raise CurrentUniverseUnavailable(f"snapshot status is {snap.get('status')!r}")

        rows = [r for r in m["rows"] if r.get("mapping_status") == "RESOLVED"]
        secs = [dict(massive_ticker=r["massive_ticker"],
                     snapshot_ticker=r["snapshot_ticker"],
                     company_name=r.get("company_name"),
                     cik=r.get("massive_cik") or r.get("snapshot_cik"),
                     security_id=r.get("massive_reference_id"))
                for r in rows]
        secs.sort(key=lambda s: s["massive_ticker"])
        tickers = [s["massive_ticker"] for s in secs]

        if len(tickers) != EXPECTED_COUNT:
            raise CurrentUniverseUnavailable(
                f"expected {EXPECTED_COUNT} resolved securities, found {len(tickers)}")
        if len(set(tickers)) != len(tickers):
            dupes = sorted({t for t in tickers if tickers.count(t) > 1})
            raise CurrentUniverseUnavailable(f"duplicate massive tickers: {dupes}")

        # The snapshot digest is over SNAPSHOT tickers; the map may rename them. Recompute
        # over snapshot_ticker so a swapped or truncated artifact is caught here.
        snap_digest = hashlib.sha256(
            "\n".join(sorted(s["snapshot_ticker"] for s in secs)).encode()).hexdigest()
        if snap_digest != snap.get("frozen_set_digest"):
            raise CurrentUniverseUnavailable(
                "snapshot digest mismatch — the ticker map and the snapshot do not "
                "describe the same frozen set")

        _cache.update(securities=secs, tickers=tickers,
                      frozen_set_digest=snap["frozen_set_digest"],
                      map_digest=m.get("map_digest"),
                      snapshot_date=snap.get("snapshot_date"),
                      source=dict(map=os.path.basename(MAP),
                                  snapshot=os.path.basename(SNAPSHOT)))
        return _cache


def get_current_sp500_massive_tickers() -> list[str]:
    """The 503 current constituents as Massive addresses them today.

    Sorted and deduplicated. Includes BRK.B and BF.B with dots, which is what the vendor
    serves — callers must not re-punctuate them.
    """
    return list(_load()["tickers"])


def get_current_sp500_securities() -> list[dict]:
    """Same membership, with identity (CIK, security id, company) attached."""
    return [dict(s) for s in _load()["securities"]]


def current_universe_meta() -> dict:
    d = _load()
    return dict(count=len(d["tickers"]), frozen_set_digest=d["frozen_set_digest"],
                snapshot_date=d["snapshot_date"], source=d["source"],
                nomenclature="massive_ticker",
                derived_from="frozen artifacts only — never bars.universe, never market "
                             "data, never ticker availability")


def is_current_operational_sp500(universes) -> bool:
    """True for the operational 'scan the S&P 500 today' request, and nothing else.

    Deliberately narrow: only a request for sp500 ALONE. A multi-universe request is a
    different question and keeps its existing historical behaviour.
    """
    if isinstance(universes, str):
        universes = [universes]
    return list(universes or []) == ["sp500"]


if __name__ == "__main__":
    meta = current_universe_meta()
    t = get_current_sp500_massive_tickers()
    print(f"  count            {meta['count']}")
    print(f"  snapshot_date    {meta['snapshot_date']}")
    print(f"  frozen_digest    {meta['frozen_set_digest'][:24]}…")
    print(f"  dotted members   {[x for x in t if '.' in x]}")
    print(f"  first/last       {t[0]} … {t[-1]}")
