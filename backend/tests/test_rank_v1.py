"""🏅 RANK v1 — lookup chain and within-day percentiles (SCORE/RANK_V1, 2026-09-07)."""
import os, sys
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rank_v1 import expected_edge, percentiles, rank_for, _band, RSI_BANDS, PX_BANDS

MODEL = {"winner": "A", "A": {"g": 1.0, "fam": {"QZ-Capit": 2.0}, "p1": {"QZ-Capit|<35": 3.0},
                              "cell": {"QZ-Capit|<35|False|True|21-89": 5.5}}}


def test_bands():
    assert _band(30, RSI_BANDS) == "<35" and _band(50, RSI_BANDS) == "50-60" and _band(60, RSI_BANDS) == ">=60"
    assert _band(50, PX_BANDS) == "21-89" and _band(400, PX_BANDS) == ">=377" and _band(float("nan"), PX_BANDS) == ""


def test_lookup_chain():
    assert expected_edge(MODEL, "QZ-Capit", 30, False, True, 50) == 5.5          # exact cell
    assert expected_edge(MODEL, "QZ-Capit", 30, True, True, 50) == 3.0           # cell missing → (family, rsi band)
    assert expected_edge(MODEL, "QZ-Capit", 55, True, True, 50) == 2.0           # parent missing → family
    assert expected_edge(MODEL, "Unknown", 55, True, True, 50) == 1.0            # unseen family → train mean
    assert expected_edge({}, "QZ-Capit", 30, False, True, 50) is None            # no model → empty column


def test_percentiles_within_day():
    df = pd.DataFrame(dict(ticker=list("ABCDE"), date=["d1"] * 4 + ["d2"], edge=[1.0, 3.0, 2.0, 3.0, 9.0]))
    p = percentiles(df).set_index("ticker")
    assert p.loc["A", "rank_pct"] == 25 and p.loc["C", "rank_pct"] == 50
    assert p.loc["B", "rank_pct"] == 100 and p.loc["D", "rank_pct"] == 100    # ties share the upper value
    assert p.loc["E", "rank_pct"] == 100 and p.loc["E", "rank_n"] == 1 and p.loc["A", "rank_n"] == 4


def test_rank_for_candidates():
    rm = {("AAPL", "2026-09-04"): dict(rank_pct=88)}
    assert rank_for(rm, "aapl", None, "2026-09-05", "2026-09-04")["rank_pct"] == 88
    assert rank_for(rm, "AAPL", "2026-09-03") is None
