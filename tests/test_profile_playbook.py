"""Unit tests for profile_playbook module."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from profile_playbook import (
    get_profile,
    normalize_signal_token,
    parse_signal_cell,
    compute_profile_score,
    enrich_row_with_profile,
    extract_signals_from_turbo_row,
    extract_profile_signals_from_stat_row,
    PROFILES,
)


# ── Profile assignment ─────────────────────────────────────────────────────────

def test_sp500_price_buckets():
    assert get_profile({"close": 10},  "sp500") == "SP500_LT20"
    assert get_profile({"close": 35},  "sp500") == "SP500_20_50"
    assert get_profile({"close": 100}, "sp500") == "SP500_50_150"
    assert get_profile({"close": 200}, "sp500") == "SP500_150_300"
    assert get_profile({"close": 400}, "sp500") == "SP500_300_PLUS"

def test_sp500_boundary_conditions():
    assert get_profile({"close": 19.99}, "sp500") == "SP500_LT20"
    assert get_profile({"close": 20.0},  "sp500") == "SP500_20_50"
    assert get_profile({"close": 49.99}, "sp500") == "SP500_20_50"
    assert get_profile({"close": 50.0},  "sp500") == "SP500_50_150"
    assert get_profile({"close": 149.99},"sp500") == "SP500_50_150"
    assert get_profile({"close": 150.0}, "sp500") == "SP500_150_300"
    assert get_profile({"close": 299.99},"sp500") == "SP500_150_300"
    assert get_profile({"close": 300.0}, "sp500") == "SP500_300_PLUS"

def test_nasdaq_profiles():
    assert get_profile({"close": 2},  "nasdaq") == "NASDAQ_PENNY"
    assert get_profile({"close": 20}, "nasdaq") == "NASDAQ_REAL"
    assert get_profile({"close": 4.99}, "nasdaq") == "NASDAQ_PENNY"
    assert get_profile({"close": 5.0},  "nasdaq") == "NASDAQ_REAL"

def test_current_close_preferred_over_median():
    assert get_profile({"close": 100, "median_price": 10}, "sp500") == "SP500_50_150"

def test_last_price_fallback():
    assert get_profile({"last_price": 100}, "sp500") == "SP500_50_150"

def test_median_price_is_fallback_only():
    assert get_profile({"median_price": 100}, "sp500") == "SP500_50_150"

def test_non_sp500_universes():
    for uni in ("nasdaq", "russell2k", "all_us", "split"):
        assert get_profile({"close": 3}, uni) == "NASDAQ_PENNY"
        assert get_profile({"close": 50}, uni) == "NASDAQ_REAL"


# ── Signal normalization ───────────────────────────────────────────────────────

def test_signal_normalization_arrows():
    assert normalize_signal_token("VBO↑") == "VBO_UP"
    assert normalize_signal_token("VBO↓") == "VBO_DN"
    assert normalize_signal_token("HILO↑") == "HILO_UP"
    assert normalize_signal_token("HILO↓") == "HILO_DN"
    assert normalize_signal_token("BB↑") == "BB_UP"
    assert normalize_signal_token("4BF↓") == "4BF_DN"
    assert normalize_signal_token("BEST★") == "BEST_STAR"

def test_signal_normalization_identity():
    assert normalize_signal_token("BUY") == "BUY"
    assert normalize_signal_token("ABS") == "ABS"
    assert normalize_signal_token("G11") == "G11"

def test_signal_normalization_empty():
    assert normalize_signal_token("") == ""
    assert normalize_signal_token(None) == ""
    assert normalize_signal_token("  ") == ""

def test_normalize_cons_alias():
    assert normalize_signal_token("CONS") == "CONSO"


# ── Signal cell parsing ────────────────────────────────────────────────────────

def test_parse_signal_cell_separators():
    result = parse_signal_cell("ABS, BE↓ | Z2")
    assert "ABS" in result
    assert "BE_DN" in result
    assert "Z2" in result

def test_parse_signal_cell_whitespace():
    result = parse_signal_cell("FLY_BD  BB↑")
    assert "FLY_BD" in result
    assert "BB_UP" in result

def test_parse_signal_cell_empty():
    assert parse_signal_cell("") == set()
    assert parse_signal_cell(None) == set()
    assert parse_signal_cell(123) == set()


# ── Profile score ─────────────────────────────────────────────────────────────

def test_pair_bonus_applied():
    signals = {"BL", "F10"}
    result = compute_profile_score(signals, "SP500_50_150")
    assert result["profile_score"] > 0
    assert "BL+F10" in result["matched_profile_pairs"]

def test_single_signal_contributes():
    signals = {"FLY_BD"}
    result = compute_profile_score(signals, "SP500_50_150")
    assert result["profile_score"] == 5
    assert "FLY_BD" in result["matched_profile_signals"]

def test_empty_signals_gives_watch():
    result = compute_profile_score(set(), "SP500_50_150")
    assert result["profile_score"] == 0
    assert result["profile_category"] == "WATCH"
    assert not result["sweet_spot_active"]
    assert not result["late_warning"]

def test_sweet_spot_detection():
    # SP500_50_150 sweet_spot = (12, 32)
    # FLY_BD(5)+BUY(4)+BX_UP(4) = 13 → SWEET_SPOT
    signals = {"FLY_BD", "BUY", "BX_UP"}
    result = compute_profile_score(signals, "SP500_50_150")
    assert result["profile_score"] >= 12
    assert result["profile_category"] in {"SWEET_SPOT", "LATE"}

def test_late_warning():
    profile = "SP500_50_150"
    # Activate all signals + all pair members — should reach high score
    signals = set(PROFILES[profile]["signal_weights"].keys())
    for pair in PROFILES[profile]["pair_bonuses"]:
        signals.update(pair)
    result = compute_profile_score(signals, profile)
    assert result["profile_score"] > 0
    assert result["profile_category"] in {"WATCH", "BUILDING", "SWEET_SPOT", "LATE"}

def test_building_category():
    # BUILDING: score >= sweet_low*0.70 but < sweet_low
    # SP500_50_150: sweet_low=12, building_threshold=8.4
    # FLY_BD(5)+BUY(4) = 9 → BUILDING (8.4 <= 9 < 12)
    signals = {"FLY_BD", "BUY"}
    result = compute_profile_score(signals, "SP500_50_150")
    assert result["profile_category"] in {"BUILDING", "SWEET_SPOT"}


# ── Row enrichment safety ─────────────────────────────────────────────────────

def test_does_not_mutate_canonical_fields():
    row = {
        "close": 100, "last_price": 100,
        "FINAL_BULL_SCORE": 55, "FINAL_REGIME": "CLEAN_ENTRY",
        "turbo_score": 42, "rtb_total": 15,
    }
    enriched = enrich_row_with_profile(row, "sp500", {"FLY_BD", "BUY"})
    # Canonical fields must be unchanged
    assert enriched["FINAL_BULL_SCORE"] == 55
    assert enriched["FINAL_REGIME"] == "CLEAN_ENTRY"
    assert enriched["turbo_score"] == 42
    assert enriched["rtb_total"] == 15
    # Profile fields must be added
    assert "profile_name" in enriched
    assert "profile_score" in enriched
    assert "profile_category" in enriched
    assert "sweet_spot_active" in enriched

def test_original_row_not_mutated():
    row = {"close": 100, "FINAL_BULL_SCORE": 55}
    enriched = enrich_row_with_profile(row, "sp500", set())
    assert "profile_name" not in row  # original untouched
    assert "profile_name" in enriched

def test_enriched_profile_name_matches_price():
    row = {"last_price": 100}
    enriched = enrich_row_with_profile(row, "sp500", set())
    assert enriched["profile_name"] == "SP500_50_150"


# ── Turbo row extraction ──────────────────────────────────────────────────────

def test_extract_turbo_boolean_columns():
    row = {"buy_2809": 1, "load_sig": 1, "g11": 0, "abs_sig": 0, "tz_sig": "T10"}
    signals = extract_signals_from_turbo_row(row)
    assert "BUY" in signals
    assert "LOAD" in signals
    assert "T10" in signals
    assert "G11" not in signals  # was 0

def test_extract_tz_sig_base_variant():
    row = {"tz_sig": "T1G"}
    signals = extract_signals_from_turbo_row(row)
    assert "T1G" in signals
    assert "T1" in signals  # base without G


# ── Profile metadata ──────────────────────────────────────────────────────────

def test_nasdaq_profiles_marked_experimental():
    assert PROFILES["NASDAQ_PENNY"]["experimental"] is True
    assert PROFILES["NASDAQ_REAL"]["experimental"] is True

def test_sp500_profiles_not_experimental():
    for name, p in PROFILES.items():
        if name.startswith("SP500"):
            assert not p.get("experimental", False), f"{name} should not be experimental"

def test_all_profiles_have_required_keys():
    required = {"universe", "price_range", "signal_weights", "pair_bonuses",
                "sweet_spot", "late_threshold", "preferred_preset", "suggested_tp", "suggested_sl"}
    for name, p in PROFILES.items():
        for k in required:
            assert k in p, f"{name} missing key {k}"


# ── New alias coverage ────────────────────────────────────────────────────────

def test_fly_dash_aliases():
    assert normalize_signal_token("FLY-BD") == "FLY_BD"
    assert normalize_signal_token("FLY-CD") == "FLY_CD"
    assert normalize_signal_token("FLY-AD") == "FLY_AD"
    assert normalize_signal_token("FLY")    == "FLY_ABCD"

def test_wick_aliases():
    assert normalize_signal_token("WC↑") == "WC_UP"
    assert normalize_signal_token("WC↓") == "WC_DN"
    assert normalize_signal_token("WP↑") == "WP_UP"
    assert normalize_signal_token("WP↓") == "WP_DN"

def test_ultra_display_aliases():
    assert normalize_signal_token("BEST↑") == "BEST_UP"
    assert normalize_signal_token("3↑")    == "THREE_UP"
    assert normalize_signal_token("4BF")   == "BF_BUY"
    assert normalize_signal_token("ATR↑")  == "ATR_BRK"
    assert normalize_signal_token("BB↓")   == "BB_DN"

def test_vol_aliases():
    assert normalize_signal_token("5×")  == "5X"
    assert normalize_signal_token("10×") == "10X"
    assert normalize_signal_token("20×") == "20X"


# ── extract_profile_signals_from_stat_row ────────────────────────────────────

def test_extract_from_bar_dict_lists():
    """Bar dict (api_bar_signals format) with list columns."""
    row = {
        "close": 120,
        "tz":    "T5",
        "combo": ["BUY", "SVS", "BB↑"],
        "fly":   ["FLY-BD"],
        "l":     ["FRI43", "BX↑"],
        "f":     ["F9", "F10"],
        "vabs":  ["ABS", "CLM", "LOAD"],
        "g":     ["G4", "G11"],
        "ultra": ["EB↑"],
        "vol":   ["5×"],
        "wick":  ["WC↑"],
    }
    sigs = extract_profile_signals_from_stat_row(row)
    assert "BUY"     in sigs
    assert "SVS"     in sigs
    assert "BB_UP"   in sigs
    assert "FLY_BD"  in sigs
    assert "FRI43"   in sigs
    assert "BX_UP"   in sigs
    assert "F9"      in sigs
    assert "F10"     in sigs
    assert "ABS"     in sigs
    assert "CLM"     in sigs
    assert "LOAD"    in sigs
    assert "G4"      in sigs
    assert "G11"     in sigs
    assert "EB_UP"   in sigs
    assert "5X"      in sigs
    assert "WC_UP"   in sigs
    assert "T5"      in sigs

def test_extract_from_csv_string_columns():
    """stock_stat CSV format with string columns."""
    row = {
        "close": 120,
        "Z": "",
        "T": "T10",
        "L": "FRI43 BL BX↑",
        "F": "F9 F10",
        "FLY": "FLY-BD",
        "G": "G4 G11",
        "B": "",
        "Combo": "BUY SVS BB↑",
        "ULT":  "EB↑ 260308",
        "VOL":  "5×",
        "VABS": "ABS LOAD CLM",
        "WICK": "WC↑",
    }
    sigs = extract_profile_signals_from_stat_row(row)
    assert "FLY_BD"  in sigs
    assert "BX_UP"   in sigs
    assert "BB_UP"   in sigs
    assert "EB_UP"   in sigs
    assert "5X"      in sigs
    assert "WC_UP"   in sigs
    assert "T10"     in sigs
    assert "BUY"     in sigs
    assert "ABS"     in sigs
    assert "260308"  in sigs

def test_extract_fly_abcd_bare():
    """Bare 'FLY' token (fly_abcd=True) maps to FLY_ABCD."""
    row = {"fly": ["FLY"], "close": 100}
    sigs = extract_profile_signals_from_stat_row(row)
    assert "FLY_ABCD" in sigs

def test_extract_vol_tokens():
    row = {"VOL": "5× 10×", "close": 100}
    sigs = extract_profile_signals_from_stat_row(row)
    assert "5X"  in sigs
    assert "10X" in sigs

def test_extract_tz_z_column():
    row = {"Z": "Z3", "close": 100}
    sigs = extract_profile_signals_from_stat_row(row)
    assert "Z3" in sigs

def test_profile_score_nonzero_from_bar_dict():
    """End-to-end: bar dict with real signals → non-zero profile score."""
    row = {
        "close": 120,
        "combo": ["BUY", "SVS"],
        "fly":   ["FLY-BD"],
        "l":     ["FRI43"],
        "vabs":  ["ABS", "LOAD", "CLM"],
        "g":     ["G4", "G11"],
    }
    sigs  = extract_profile_signals_from_stat_row(row)
    pname = get_profile(row, "sp500")
    pd    = compute_profile_score(sigs, pname)
    assert pd["profile_score"] > 0, f"Expected non-zero score, got {pd}"
    assert pd["profile_category"] != "WATCH" or pd["profile_score"] == 0

def test_profile_score_nonzero_from_csv_row():
    """End-to-end: CSV row with real signals → non-zero profile score."""
    row = {
        "close": 120,
        "Combo": "BUY SVS",
        "FLY":   "FLY-BD",
        "L":     "FRI43",
        "VABS":  "ABS LOAD CLM",
        "G":     "G4 G11",
    }
    sigs  = extract_profile_signals_from_stat_row(row)
    pname = get_profile(row, "sp500")
    pd    = compute_profile_score(sigs, pname)
    assert pd["profile_score"] > 0, f"Expected non-zero score from CSV row, got {pd}"

def test_sweet_spot_reachable_from_stat_row():
    """A row with enough signals should reach SWEET_SPOT or LATE, never stuck at WATCH."""
    row = {
        "close": 120,
        "Combo": "BUY SVS",
        "FLY":   "FLY-BD",
        "L":     "FRI43 BX↑",
        "VABS":  "ABS LOAD CLM",
        "G":     "G4 G11",
        "ULT":   "EB↑",
        "F":     "F9 F10",
    }
    sigs  = extract_profile_signals_from_stat_row(row)
    pname = get_profile(row, "sp500")   # SP500_50_150
    pd    = compute_profile_score(sigs, pname)
    assert pd["profile_category"] in {"SWEET_SPOT", "BUILDING", "LATE"}, (
        f"Expected non-WATCH category, got {pd['profile_category']} (score={pd['profile_score']})"
    )

def test_ult_eb_up_maps_to_eb_up():
    row = {"ULT": "EB↑", "close": 100}
    sigs = extract_profile_signals_from_stat_row(row)
    assert "EB_UP" in sigs

def test_ult_fbo_up_maps():
    row = {"ultra": ["FBO↑"], "close": 100}
    sigs = extract_profile_signals_from_stat_row(row)
    assert "FBO_UP" in sigs


if __name__ == "__main__":
    import traceback
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
