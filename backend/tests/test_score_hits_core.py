"""🎲 score-hits reads UV3-CORE (SCORE_AUDIT_V1, 2026-09-07)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ultra_score import compute_ultra_score_v3, compute_score_hits


def _row(**kw):
    # RSI 40 → osv +8 · price 50 → pzv +10 · no earners → core 18 (≤ 25, outside the 🎲 UV3 zone)
    base = dict(rsi=40.0, last_price=50.0)
    base.update(kw)
    return base


def test_v3_returns_core_without_axes():
    v = compute_ultra_score_v3(_row())
    assert v["ultra_score_v3_core"] == 18 and v["ultra_score_v3"] == 18


def test_axes_move_live_but_not_core():
    v = compute_ultra_score_v3(_row(rs_intact=True, conf_n=4, tls_bar=True))
    assert v["ultra_score_v3_core"] == 18
    assert v["ultra_score_v3"] == 18 + 12 + 16 + 10
    assert "🏆RS" in v["ultra_score_v3_reasons"]


def test_veto_moves_live_only():
    v = compute_ultra_score_v3(_row(no_vol_event=True))
    assert v["ultra_score_v3_core"] == 18 and v["ultra_score_v3"] == 0


def test_hits_use_core_when_present():
    live = compute_ultra_score_v3(_row(rs_intact=True, conf_n=4))
    row = dict(ultra_score_v3=live["ultra_score_v3"], ultra_score_v3_core=live["ultra_score_v3_core"], conf_n=4)
    h = compute_score_hits(row)
    assert h["score_hits_uv3_core"] is True
    assert "ultra_score_v3" not in h["score_hits_which"]      # core 18 is outside >25
    assert h["score_hits"] == 0                                # conf_n zone is > 4


def test_hits_fall_back_to_live_when_core_absent():
    h = compute_score_hits(dict(ultra_score_v3=46))
    assert h["score_hits_uv3_core"] is False
    assert h["score_hits_which"] == ["ultra_score_v3"] and h["score_hits"] == 1


def test_core_in_zone_counts():
    v = compute_ultra_score_v3(_row(rsi=28.0, d_absorb_bull=True))   # osv +20 · price +10 · absorb +15 → 45
    h = compute_score_hits(dict(ultra_score_v3=v["ultra_score_v3"], ultra_score_v3_core=v["ultra_score_v3_core"]))
    assert v["ultra_score_v3_core"] == 45 and h["score_hits_which"] == ["ultra_score_v3"]
