"""Tests for split_universe — stock-only filter v2, lifecycle, cache versioning."""
import sys
import os
from datetime import date as date_t, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from split_universe import (
    is_stock_like_split_event,
    classify_split_lifecycle,
    SplitUniverseService,
    SPLIT_CACHE_VERSION,
    _PRODUCT_PHRASES,
    _ISSUER_BRANDS,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row(company_name="", security_name="", asset_type="", issue_type="", ticker="TEST"):
    return {
        "ticker":       ticker,
        "companyName":  company_name,
        "securityName": security_name,
        "assetType":    asset_type,
        "issueType":    issue_type,
    }


def _excluded(row):
    ok, reason = is_stock_like_split_event(row)
    return not ok, reason


def _included(row):
    ok, _ = is_stock_like_split_event(row)
    return ok


# ── Ratio parser ──────────────────────────────────────────────────────────────

def test_ratio_1_colon_10():
    assert SplitUniverseService._parse_ratio("1:10") == 10.0

def test_ratio_1_for_10():
    assert SplitUniverseService._parse_ratio("1-for-10") == 10.0

def test_ratio_1_slash_20():
    assert SplitUniverseService._parse_ratio("1/20") == 20.0

def test_ratio_forward_split_lt_1():
    r = SplitUniverseService._parse_ratio("10:1")
    assert r == 0.1  # filtered out by MIN_RATIO >= 2.0

def test_ratio_forward_filtered():
    svc = SplitUniverseService()
    assert svc._parse_ratio("10:1") < svc.MIN_RATIO


# ── Stock-only filter: EXCLUDED cases ────────────────────────────────────────

def test_direxion_bear_excluded():
    exc, reason = _excluded(_row("Direxion Daily Semiconductor Bear 3X Shares"))
    assert exc, reason
    # Direxion is caught by issuer_keyword OR bear 3x product_phrase — either is correct
    assert reason is not None

def test_proshares_excluded():
    exc, reason = _excluded(_row("ProShares UltraPro Short QQQ"))
    assert exc, reason

def test_ishares_etf_excluded():
    exc, reason = _excluded(_row("iShares Bitcoin Trust ETF"))
    assert exc, reason

def test_yieldmax_etf_excluded():
    exc, reason = _excluded(_row("YieldMax XYZ Option Income Strategy ETF"))
    assert exc, reason

def test_graniteshares_excluded():
    exc, reason = _excluded(_row("GraniteShares 2x Long NVDA Daily ETF"))
    assert exc, reason

def test_etf_suffix_excluded():
    assert not _included(_row("Some Random ETF"))

def test_etn_excluded():
    assert not _included(_row("Barclays ETN+ FI Enhanced Europe 50 ETN"))

def test_fund_excluded():
    assert not _included(_row("Templeton Global Income Fund"))

def test_closed_end_fund_excluded():
    assert not _included(_row("Royce Total Return Closed-End Fund"))

def test_trust_excluded():
    assert not _included(_row("SPDR Gold Trust"))

def test_preferred_excluded():
    assert not _included(_row("Citigroup Capital XIII Preferred Securities"))

def test_warrant_excluded():
    assert not _included(_row("Acme Holdings Warrants"))

def test_rights_excluded():
    assert not _included(_row("XYZ Rights Offering"))

def test_units_excluded():
    assert not _included(_row("SomePLP Units"))

def test_inverse_excluded():
    assert not _included(_row("Inverse VIX Short Term Futures ETF"))

def test_senior_notes_excluded():
    assert not _included(_row("Senior Notes due 2028"))

def test_2x_long_excluded():
    assert not _included(_row("GraniteShares 2x Long AAPL Daily ETF"))

def test_3x_short_excluded():
    assert not _included(_row("3x Short NASDAQ ETF"))

def test_ultrapro_excluded():
    assert not _included(_row("ProShares UltraPro QQQ"))

def test_ultrashort_excluded():
    assert not _included(_row("ProShares UltraShort Dow30"))

def test_preferred_stock_excluded():
    assert not _included(_row("Preferred Stock Series A"))

def test_depositary_shares_excluded():
    assert not _included(_row("ADS Depositary Shares"))

def test_grayscale_trust_excluded():
    assert not _included(_row("Grayscale Bitcoin Trust"))

def test_wisdomtree_excluded():
    assert not _included(_row("WisdomTree U.S. Quality Dividend Growth ETF"))

def test_closed_end_no_hyphen_excluded():
    assert not _included(_row("Royce Closed End Fund Inc"))

def test_asset_type_etf_excluded():
    ok, reason = is_stock_like_split_event(_row(company_name="SomeName Inc", asset_type="ETF"))
    assert not ok
    assert "explicit_type" in reason

def test_issue_type_fund_excluded():
    ok, reason = is_stock_like_split_event(_row(company_name="SomeName Inc", issue_type="fund"))
    assert not ok
    assert "explicit_type" in reason

def test_security_name_etf_excluded():
    assert not _included(_row(security_name="Global X Uranium ETF"))


# ── Stock-only filter: INCLUDED (false-positive prevention) ──────────────────

def test_plain_corp_included():
    assert _included(_row("ABC Corp"))

def test_therapeutics_included():
    assert _included(_row("XYZ Therapeutics Inc."))

def test_bear_creek_mining_included():
    """'Bear' alone must not trigger exclusion — only 'bear 2x/3x' compounds."""
    assert _included(_row("Bear Creek Mining Corp"))

def test_ultragenyx_included():
    """'Ultra' alone must not trigger exclusion — only ultrashort/ultrapro."""
    assert _included(_row("Ultragenyx Pharmaceutical Inc."))

def test_bond_street_included():
    """'Bond' standalone must not trigger exclusion."""
    assert _included(_row("Bond Street Holdings Inc."))

def test_notable_labs_included():
    """'Note' as prefix of a word (notable) must not trigger exclusion."""
    assert _included(_row("Notable Labs Ltd"))

def test_banknote_included():
    """'Note' inside compound word 'banknote' must not trigger exclusion."""
    assert _included(_row("Banknote Corp"))

def test_trustco_bank_included():
    """'TrustCo' is one word — \\btrust\\b must NOT match 'trustco'."""
    assert _included(_row("TrustCo Bank Corp"))

def test_empty_name_included():
    assert _included(_row(company_name="", security_name="", asset_type="", issue_type=""))

def test_none_fields_included():
    row = {"ticker": "TEST", "companyName": None, "securityName": None,
           "assetType": None, "issueType": None}
    assert _included(row)

def test_missing_fields_included():
    assert _included({"ticker": "TEST"})

def test_case_insensitive_exclusion():
    assert not _included(_row("direxion daily technology bear 3x shares"))


# ── Return type: is tuple[bool, str|None] ────────────────────────────────────

def test_return_tuple_included():
    ok, reason = is_stock_like_split_event(_row("ABC Corp"))
    assert ok is True
    assert reason is None

def test_return_tuple_excluded():
    ok, reason = is_stock_like_split_event(_row("iShares Bitcoin Trust ETF"))
    assert ok is False
    assert reason is not None
    assert isinstance(reason, str)

def test_excluded_reason_has_rule_type():
    _, reason = is_stock_like_split_event(_row("Direxion Daily Bear 3X ETF"))
    assert any(rt in reason for rt in ("explicit_type", "product_phrase", "issuer_keyword", "security_class"))


# ── Keyword list not empty ────────────────────────────────────────────────────

def test_product_phrases_not_empty():
    assert len(_PRODUCT_PHRASES) >= 10

def test_issuer_brands_not_empty():
    assert len(_ISSUER_BRANDS) >= 10


# ── Lifecycle classification ──────────────────────────────────────────────────

def _lc(days_offset, ratio=5.0):
    """Helper: compute lifecycle for a split at today + days_offset (negative=future)."""
    today = date_t.today()
    split_date = today - timedelta(days=days_offset)  # days_offset positive = past
    return classify_split_lifecycle(split_date.isoformat(), ratio, today)

def test_lifecycle_pre_split():
    lc = _lc(-3)  # 3 days in future
    assert lc["phase"] == "PRE_SPLIT"
    assert lc["wave"]  == "PRE"
    assert lc["days_offset"] == -3

def test_lifecycle_split_day():
    lc = _lc(0)
    assert lc["phase"] == "SPLIT_DAY"
    assert lc["wave"]  == "D0"
    assert lc["days_offset"] == 0

def test_lifecycle_wave1():
    lc = _lc(3)
    assert lc["phase"] == "WAVE_1"
    assert lc["wave"]  == "W1"

def test_lifecycle_wave2():
    lc = _lc(12)
    assert lc["phase"] == "WAVE_2_SETUP"
    assert lc["wave"]  == "W2"

def test_lifecycle_wave3():
    lc = _lc(30)
    assert lc["phase"] == "WAVE_3_SETUP"
    assert lc["wave"]  == "W3"

def test_lifecycle_post_monitor():
    lc = _lc(55)
    assert lc["phase"] == "POST_MONITOR"
    assert lc["wave"]  == "POST"

def test_lifecycle_extended_monitor_high_ratio():
    lc = _lc(80, ratio=20.0)  # ratio>=20 → watch_days=90
    assert lc["phase"] == "EXTENDED_MONITOR"
    assert lc["wave"]  == "EXT"

def test_lifecycle_expired():
    lc = _lc(100)  # ratio=5 → watch_days=60 → expired
    assert lc["phase"] == "EXPIRED"

def test_lifecycle_upcoming_far():
    lc = _lc(-10)  # 10 days in future → UPCOMING_FAR
    assert lc["phase"] == "UPCOMING_FAR"

def test_lifecycle_watch_days_ratio_lt10():
    lc = _lc(0, ratio=5.0)
    assert lc["watch_days"] == 60

def test_lifecycle_watch_days_ratio_10():
    lc = _lc(0, ratio=10.0)
    assert lc["watch_days"] == 75

def test_lifecycle_watch_days_ratio_20():
    lc = _lc(0, ratio=20.0)
    assert lc["watch_days"] == 90

def test_lifecycle_heat_wave1():
    lc = _lc(3, ratio=5.0)
    assert lc["heat_score"] == 5  # 4 (WAVE_1) + 1 (ratio>=5)

def test_lifecycle_heat_clamped():
    # WAVE_1(4) + ratio>=20(3) = 7; clamp 0-10 applies
    lc = _lc(3, ratio=25.0)
    assert lc["heat_score"] == 7
    # heat score always within [0, 10]
    lc2 = _lc(0, ratio=20.0)
    assert 0 <= lc2["heat_score"] <= 10

def test_lifecycle_next_wave_pre():
    lc = _lc(-1)
    assert lc["next_wave_label"] == "WAVE_1"
    assert lc["next_wave_start_date"] is not None

def test_lifecycle_next_wave_w1():
    lc = _lc(5)
    assert lc["next_wave_label"] == "WAVE_2_SETUP"

def test_lifecycle_next_wave_none_post():
    lc = _lc(55)
    assert lc["next_wave_label"] is None

def test_lifecycle_watch_until_str():
    lc = _lc(0, ratio=5.0)
    today = date_t.today()
    assert lc["watch_until"] == (today + timedelta(days=60)).isoformat()

def test_lifecycle_notes_field():
    lc = _lc(3, ratio=10.0)
    assert "ratio=" in lc["notes"]
    assert "phase=" in lc["notes"]


# ── Dedupe: prefer active lifecycle ──────────────────────────────────────────

def test_dedupe_active_beats_expired():
    today = date_t.today()
    active_row = {
        "ticker": "FOO", "split_date": (today - timedelta(days=5)).isoformat(),
        "ratio": 5.0, "ratio_str": "1:5", "source": "nasdaq",
        "companyName": "Foo Corp", "securityName": "", "assetType": "", "issueType": "",
        "phase": "WAVE_1", "wave": "W1", "days_offset": 5,
    }
    expired_row = {
        "ticker": "FOO", "split_date": (today - timedelta(days=100)).isoformat(),
        "ratio": 5.0, "ratio_str": "1:5", "source": "nasdaq",
        "companyName": "Foo Corp", "securityName": "", "assetType": "", "issueType": "",
        "phase": "EXPIRED", "wave": "EXPIRED", "days_offset": 100,
    }
    result = SplitUniverseService._dedupe_by_ticker([expired_row, active_row])
    assert len(result) == 1
    assert result[0]["phase"] == "WAVE_1"

def test_dedupe_closest_daysoffset_wins():
    today = date_t.today()
    near = {
        "ticker": "BAR", "split_date": today.isoformat(), "ratio": 5.0,
        "phase": "SPLIT_DAY", "wave": "D0", "days_offset": 0,
    }
    far = {
        "ticker": "BAR", "split_date": (today - timedelta(days=15)).isoformat(), "ratio": 5.0,
        "phase": "WAVE_2_SETUP", "wave": "W2", "days_offset": 15,
    }
    result = SplitUniverseService._dedupe_by_ticker([far, near])
    assert len(result) == 1
    assert result[0]["days_offset"] == 0


# ── Cache versioning ──────────────────────────────────────────────────────────

def test_cache_version_constant_exists():
    assert SPLIT_CACHE_VERSION == "split_lifecycle_stock_filter_v2"

def test_cache_version_mismatch_forces_rebuild():
    from unittest.mock import patch
    from datetime import datetime as dt_cls
    svc = SplitUniverseService()
    svc._cache         = [{"ticker": "OLD"}]
    svc._cache_time    = dt_cls.now()
    svc._cache_version = "old_version"     # deliberate mismatch
    assert not svc._is_cache_valid()

def test_cache_valid_with_correct_version():
    from unittest.mock import patch
    from datetime import datetime as dt_cls
    svc = SplitUniverseService()
    svc._cache         = [{"ticker": "NEW"}]
    svc._cache_time    = dt_cls.now()
    svc._cache_version = SPLIT_CACHE_VERSION
    assert svc._is_cache_valid()

def test_cache_invalid_after_expiry():
    from datetime import datetime as dt_cls
    svc = SplitUniverseService()
    svc._cache         = [{"ticker": "X"}]
    svc._cache_version = SPLIT_CACHE_VERSION
    svc._cache_time    = dt_cls(2000, 1, 1)  # ancient timestamp
    assert not svc._is_cache_valid()


if __name__ == "__main__":
    tests = [
        test_ratio_1_colon_10, test_ratio_1_for_10, test_ratio_1_slash_20,
        test_ratio_forward_split_lt_1, test_ratio_forward_filtered,
        test_direxion_bear_excluded, test_proshares_excluded, test_ishares_etf_excluded,
        test_yieldmax_etf_excluded, test_graniteshares_excluded, test_etf_suffix_excluded,
        test_etn_excluded, test_fund_excluded, test_closed_end_fund_excluded,
        test_trust_excluded, test_preferred_excluded, test_warrant_excluded,
        test_rights_excluded, test_units_excluded, test_inverse_excluded,
        test_senior_notes_excluded, test_2x_long_excluded, test_3x_short_excluded,
        test_ultrapro_excluded, test_ultrashort_excluded,
        test_preferred_stock_excluded, test_depositary_shares_excluded,
        test_grayscale_trust_excluded, test_wisdomtree_excluded,
        test_closed_end_no_hyphen_excluded,
        test_asset_type_etf_excluded, test_issue_type_fund_excluded,
        test_security_name_etf_excluded,
        test_plain_corp_included, test_therapeutics_included,
        test_bear_creek_mining_included, test_ultragenyx_included,
        test_bond_street_included, test_notable_labs_included,
        test_banknote_included, test_trustco_bank_included,
        test_empty_name_included, test_none_fields_included,
        test_missing_fields_included, test_case_insensitive_exclusion,
        test_return_tuple_included, test_return_tuple_excluded,
        test_excluded_reason_has_rule_type,
        test_product_phrases_not_empty, test_issuer_brands_not_empty,
        test_lifecycle_pre_split, test_lifecycle_split_day,
        test_lifecycle_wave1, test_lifecycle_wave2, test_lifecycle_wave3,
        test_lifecycle_post_monitor, test_lifecycle_extended_monitor_high_ratio,
        test_lifecycle_expired, test_lifecycle_upcoming_far,
        test_lifecycle_watch_days_ratio_lt10, test_lifecycle_watch_days_ratio_10,
        test_lifecycle_watch_days_ratio_20,
        test_lifecycle_heat_wave1, test_lifecycle_heat_clamped,
        test_lifecycle_next_wave_pre, test_lifecycle_next_wave_w1,
        test_lifecycle_next_wave_none_post, test_lifecycle_watch_until_str,
        test_lifecycle_notes_field,
        test_dedupe_active_beats_expired, test_dedupe_closest_daysoffset_wins,
        test_cache_version_constant_exists, test_cache_version_mismatch_forces_rebuild,
        test_cache_valid_with_correct_version, test_cache_invalid_after_expiry,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed}/{passed+failed} tests passed")
