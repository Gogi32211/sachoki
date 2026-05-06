"""Tests for split_universe stock-only filter."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from split_universe import is_stock_like_split_event, NON_STOCK_KEYWORDS


def _row(company_name="", security_name="", asset_type="", issue_type="", ticker="TEST"):
    return {
        "ticker":       ticker,
        "companyName":  company_name,
        "securityName": security_name,
        "assetType":    asset_type,
        "issueType":    issue_type,
    }


# ── Known ETF / fund products → must be excluded ─────────────────────────────

def test_direxion_bear_excluded():
    r = _row("Direxion Daily Semiconductor Bear 3X Shares")
    assert is_stock_like_split_event(r) is False


def test_proshares_excluded():
    r = _row("ProShares UltraPro Short QQQ")
    assert is_stock_like_split_event(r) is False


def test_ishares_etf_excluded():
    r = _row("iShares Bitcoin Trust ETF")
    assert is_stock_like_split_event(r) is False


def test_yieldmax_etf_excluded():
    r = _row("YieldMax XYZ Option Income Strategy ETF")
    assert is_stock_like_split_event(r) is False


def test_etf_suffix_excluded():
    r = _row("Some Random ETF")
    assert is_stock_like_split_event(r) is False


def test_etn_excluded():
    r = _row("Barclays ETN+ FI Enhanced Europe 50 ETN")
    assert is_stock_like_split_event(r) is False


def test_fund_excluded():
    r = _row("Templeton Global Income Fund")
    assert is_stock_like_split_event(r) is False


def test_closed_end_fund_excluded():
    r = _row("Royce Total Return Closed-End Fund")
    assert is_stock_like_split_event(r) is False


def test_trust_excluded():
    r = _row("SPDR Gold Trust")
    assert is_stock_like_split_event(r) is False


def test_preferred_excluded():
    r = _row("Citigroup Capital XIII Trust Preferred Securities")
    assert is_stock_like_split_event(r) is False


def test_warrant_excluded():
    r = _row("Acme Holdings Warrant")
    assert is_stock_like_split_event(r) is False


def test_inverse_excluded():
    r = _row("Inverse VIX Short Term Futures ETF")
    assert is_stock_like_split_event(r) is False


def test_2x_leveraged_excluded():
    r = _row("MicroStrategy 2x Leveraged Strategy ETF")
    assert is_stock_like_split_event(r) is False


def test_3x_leveraged_excluded():
    r = _row("Direxion Daily S&P 500 Bull 3X Shares")
    assert is_stock_like_split_event(r) is False


def test_graniteShares_excluded():
    r = _row("GraniteShares 2x Long NVDA Daily ETF")
    assert is_stock_like_split_event(r) is False


def test_wisdomtree_excluded():
    r = _row("WisdomTree U.S. Quality Dividend Growth ETF")
    assert is_stock_like_split_event(r) is False


def test_unit_excluded():
    r = _row("SomeLP Unit")
    assert is_stock_like_split_event(r) is False


# ── Legitimate operating company stocks → must be included ───────────────────

def test_plain_corp_included():
    r = _row("ABC Corp")
    assert is_stock_like_split_event(r) is True


def test_therapeutics_included():
    r = _row("XYZ Therapeutics Inc.")
    assert is_stock_like_split_event(r) is True


def test_biotech_included():
    r = _row("Genprobe Incorporated")
    assert is_stock_like_split_event(r) is True


def test_tech_company_included():
    r = _row("Acme Software Solutions Ltd.")
    assert is_stock_like_split_event(r) is True


def test_empty_name_included():
    """Missing name fields must NOT automatically exclude the row."""
    r = _row(company_name="", security_name="", asset_type="", issue_type="")
    assert is_stock_like_split_event(r) is True


def test_none_fields_included():
    """None values in name fields must be treated as empty, not crash."""
    row = {"ticker": "TEST", "companyName": None, "securityName": None,
           "assetType": None, "issueType": None}
    assert is_stock_like_split_event(row) is True


def test_missing_fields_included():
    """Completely missing name keys must not crash or auto-exclude."""
    row = {"ticker": "TEST"}
    assert is_stock_like_split_event(row) is True


# ── Type-field matching (assetType / issueType) ───────────────────────────────

def test_asset_type_etf_excluded():
    r = _row(company_name="SomeName Inc", asset_type="ETF")
    assert is_stock_like_split_event(r) is False


def test_issue_type_fund_excluded():
    r = _row(company_name="SomeName Inc", issue_type="Closed End Fund")
    assert is_stock_like_split_event(r) is False


def test_security_name_etf_excluded():
    r = _row(security_name="Global X Uranium ETF")
    assert is_stock_like_split_event(r) is False


# ── Case insensitivity ────────────────────────────────────────────────────────

def test_case_insensitive_exclusion():
    r = _row("direxion daily technology bear 3x shares")
    assert is_stock_like_split_event(r) is False


def test_keyword_list_not_empty():
    assert len(NON_STOCK_KEYWORDS) >= 20


if __name__ == "__main__":
    tests = [
        test_direxion_bear_excluded,
        test_proshares_excluded,
        test_ishares_etf_excluded,
        test_yieldmax_etf_excluded,
        test_etf_suffix_excluded,
        test_etn_excluded,
        test_fund_excluded,
        test_closed_end_fund_excluded,
        test_trust_excluded,
        test_preferred_excluded,
        test_warrant_excluded,
        test_inverse_excluded,
        test_2x_leveraged_excluded,
        test_3x_leveraged_excluded,
        test_graniteShares_excluded,
        test_wisdomtree_excluded,
        test_unit_excluded,
        test_plain_corp_included,
        test_therapeutics_included,
        test_biotech_included,
        test_tech_company_included,
        test_empty_name_included,
        test_none_fields_included,
        test_missing_fields_included,
        test_asset_type_etf_excluded,
        test_issue_type_fund_excluded,
        test_security_name_etf_excluded,
        test_case_insensitive_exclusion,
        test_keyword_list_not_empty,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed}/{passed+failed} tests passed")
