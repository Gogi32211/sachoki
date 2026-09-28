"""turn_rowseq_store._to_ui — the Ultra chip booleans derived from one nightly row.

Pins the chip semantics the UltraScanPanel hints promise: TURN≥n only on a 10-bar-low candidate,
ROW● = tier 2, ROW○+ = tier ≥ 1, ROW●≥2/3 count only confirmed rows, and a MISS carries every key.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from studio import turn_rowseq_store as T  # noqa: E402


def _rec(**kw):
    base = {"ticker": "X", "date": "2026-09-25", "turn_cand": False, "turn_n": None, "rs_pair": False}
    base.update({f"rs_{r}": 0 for r in T.ROWS})
    base.update(kw)
    return base


def test_miss_has_every_ui_key():
    ui = T._to_ui(_rec())
    assert set(T.MISS) <= set(ui)
    assert not any(ui[f"turn_ge{b}"] for b in T.TURN_BANDS)
    assert ui["rs_text"] == "" and ui["rs_nconf"] == 0


def test_turn_bands_need_a_candidate():
    assert T._to_ui(_rec(turn_cand=False, turn_n=40))["turn_ge20"] is False
    ui = T._to_ui(_rec(turn_cand=True, turn_n=27))
    assert (ui["turn_ge15"], ui["turn_ge20"], ui["turn_ge26"], ui["turn_ge30"]) == (True, True, True, False)
    assert T._to_ui(_rec(turn_cand=True, turn_n=17))["turn_ge15"] is True
    assert T._to_ui(_rec(turn_cand=True, turn_n=17))["turn_ge20"] is False
    assert ui["turn58_n"] == 27


def test_row_tiers_and_counts():
    ui = T._to_ui(_rec(rs_mtf=2, rs_vol7=2, rs_gr=1, rs_pair=True))
    assert ui["rs_mtf_c"] and ui["rs_mtf_e"] and ui["rs_gr_e"] and not ui["rs_gr_c"]
    assert ui["rs_nconf"] == 2 and ui["rs_c2"] and not ui["rs_c3"] and ui["rs_nearly"] == 3
    assert ui["rs_text"].startswith("◆V∧M") and "MTF●" in ui["rs_text"] and "GR○" in ui["rs_text"]
