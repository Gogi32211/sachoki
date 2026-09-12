"""L-BAL fixtures (lbal_build.py + studio/lbal_store.py + the Ultra enrichment).

 1  udn_of: U / D / N by strict majority
 2  states(): the five marks follow the registered truth table, ★ follows the EFFORT line vs the daily candle
 3  marks string prints in the fixed order and agrees token-wise with the flags (★ ⊂ ★★ is not a match)
 4  counts_sql on a synthetic 15m session reproduces the Pine loop: n_pos/n_neg exact-label, n_pos_c/n_neg_c by candle
 5  lbal_store._to_ui: an unlabelled session reads as a MISS (never XXX), a labelled one carries every UI key
 6  _enrich_lbal joins on scan_date (the key a live scan row carries) and a miss reads false / '' never undefined
"""
import os
import sys
import pytest
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import lbal_build as L                                                    # noqa: E402
from studio import lbal_store as LB                                       # noqa: E402


def test_01_udn_of():
    assert L.udn_of(5, 2) == "U" and L.udn_of(2, 5) == "D" and L.udn_of(3, 3) == "N" and L.udn_of(0, 0) == "N"


@pytest.mark.parametrize("udn,udn_c,colour,expect", [
    ("U", "U", "GREEN", ""),                    # full agreement, no mark
    ("D", "D", "RED", ""),
    ("U", "U", "RED", "★"),                     # ★ = effort line vs the candle
    ("D", "D", "GREEN", "★"),
    ("U", "D", "RED", "★ ★★"),                  # divergence AND conflict
    ("D", "U", "RED", "★★"),
    ("U", "N", "GREEN", "★★★"),
    ("N", "U", "GREEN", "★★★"),
    ("D", "N", "RED", "○○○"),
    ("N", "D", "GREEN", "○○○"),
    ("N", "N", "GREEN", "XXX"),
    ("N", "N", "DOJI", "XXX"),
    ("U", "U", "DOJI", ""),                     # no ★ on a doji
])
def test_02_states_truth_table(udn, udn_c, colour, expect):
    assert L.states(udn, udn_c, colour)["marks"] == expect


def test_03_marks_order_and_tokens():
    f = L.states("U", "D", "RED")
    assert f["star"] and f["conflict"] and not f["half_up"] and not f["half_dn"] and not f["nn"]
    assert f["marks"].split(" ") == ["★", "★★"]
    assert [s for _, s in L.MARKS] == ["★", "★★", "★★★", "○○○", "XXX"]
    # substring trap: "★" is inside "★★" — token membership is what the flags mean
    g = L.states("U", "N", "GREEN")
    assert g["marks"] == "★★★" and not g["star"] and "★" not in g["marks"].split(" ")


def test_04_counts_sql_matches_pine_loop_both_modes():
    import duckdb
    con = duckdb.connect()
    # codes: exact labels; v = volume > SMA20 (the Pine onlyV gate); bars: candle colours. one session
    codes = [(12, 1, 1), (12, 0, 0), (4, 1, 1), (40, 0, 1), (40, 1, 0), (3, 1, 1), (18, 0, 0), (0, 1, 1), (2, 0, 0)]   # (code, green?, v?)
    rows = []
    for i, (c, g, v) in enumerate(codes):
        d = f"2024-03-04 14:{i * 5:02d}:00"
        o, cl = (10.0, 10.1) if g else (10.1, 10.0)
        rows.append((c, v, d, o, cl))
    con.execute("CREATE TABLE codes AS SELECT * FROM (VALUES " + ",".join(
        f"('T', DATE '2024-03-04', TIMESTAMP '{d}', {c}, {bool(v)})" for c, v, d, _, _ in rows) + ") t(ticker, session, date, code, v_above)")
    con.execute("CREATE TABLE bars AS SELECT * FROM (VALUES " + ",".join(
        f"('T', TIMESTAMP '{d}', {o}, {cl})" for _, _, d, o, cl in rows) + ") t(ticker, date, open, close)")
    r = con.execute(L.counts_sql("codes", "bars")).df().iloc[0]
    # ALL mode (Pine onlyV = false)
    assert r.n_bars == 9 and r.n_lab == 8
    assert r.n_pos == 3 and r.n_neg == 2                    # L34, L34, L3 vs L46, L46
    assert r.n_pos_c == 4 and r.n_neg_c == 4                # labelled greens: 12,4,40,3 · labelled reds: 12,40,18,2
    assert r.n_l34 == 2 and r.n_l3 == 1 and r.n_l46 == 2 and r.n_l12 == 1 and r.n_l25 == 1 and r.n_l2 == 1
    assert L.udn_of(int(r.n_pos), int(r.n_neg)) == "U" and L.udn_of(int(r.n_pos_c), int(r.n_neg_c)) == "N"
    # V mode (Pine onlyV = true): the gate sits above EVERY counter, the candle line included
    assert r.nv_bars == 5 and r.nv_lab == 4                 # V bars: 12g, 4g, 40r, 3g, (0g unlabelled)
    assert r.nv_pos == 2 and r.nv_neg == 1                  # L34, L3 vs L46
    assert r.nv_pos_c == 3 and r.nv_neg_c == 1              # greens 12,4,3 · red 40
    assert r.nv_l34 == 1 and r.nv_l3 == 1 and r.nv_l46 == 1 and r.nv_l12 == 1 and r.nv_l25 == 0
    assert L.MODE == "V"                                    # the user's TradingView setting is the app default


def test_04b_mode_states_primary_is_v():
    df = pd.DataFrame(dict(nv_lab=[3, 0], nv_pos=[2, 0], nv_neg=[1, 0], nv_pos_c=[1, 0], nv_neg_c=[2, 0], colour=["RED", "GREEN"]))
    udn, udn_c, flags, text = L.mode_states(df, "nv")
    assert udn == ["U", None] and udn_c == ["D", None]
    assert flags[0]["marks"] == "★ ★★" and flags[1]["marks"] == "" and text[0] == "U 2:1 · D+ 1:2" and text[1] == ""


def test_04c_lvx_levels_and_tiers():
    # level: H (2) when a V bar is inside, L (1) when only a plain bar, 0 otherwise or when the TF has no bars
    assert L.level_of(0, 0) == 0 and L.level_of(1, 0) == 1 and L.level_of(3, 1) == 2 and L.level_of(3, 1, has_tf=False) == 0
    # tiers follow the Pine precedence VX > VH > VL > V > plain; nothing above plain without daily V
    assert L.tier_of(False, 2, 2) == 0
    assert L.tier_of(True, 0, 0) == 1                       # V
    assert L.tier_of(True, 1, 0) == 2 and L.tier_of(True, 0, 1) == 2   # VL
    assert L.tier_of(True, 2, 0) == 3 and L.tier_of(True, 0, 2) == 3   # VH
    assert L.tier_of(True, 1, 1) == 4 and L.tier_of(True, 2, 1) == 4 and L.tier_of(True, 2, 2) == 4   # VX (any on both)
    assert L.LVX_TIER == {0: "", 1: "V", 2: "VL", 3: "VH", 4: "VX"}


def test_04d_lvx_frame_and_store():
    from studio import lvx_store as LX
    daily = pd.DataFrame(dict(ticker=["T", "T", "T"], session=["2026-01-05", "2026-01-06", "2026-01-07"],
                              code=[12, 40, 12], v_above=[True, True, False], colour=["GREEN", "RED", "GREEN"]))
    c15 = pd.DataFrame(dict(ticker=["T", "T"], session=["2026-01-05", "2026-01-06"], n_bars=[26, 26],
                            n34=[2, 0], n34v=[1, 0], n46=[0, 3], n46v=[0, 0]))
    c1h = pd.DataFrame(dict(ticker=["T"], session=["2026-01-05"], n_bars=[7], n34=[1], n34v=[0], n46=[0], n46v=[0]))
    f = L.lvx_frame(daily, c15, c1h).set_index("date")
    assert f.loc["2026-01-05", "label"] == "L34VX" and f.loc["2026-01-05", "lv_15"] == 2 and f.loc["2026-01-05", "lv_1h"] == 1
    assert f.loc["2026-01-06", "label"] == "L46VL" and f.loc["2026-01-06", "bars_1h"] == 0     # no 1h bars → level 0 there
    assert f.loc["2026-01-07", "label"] == "L34" and f.loc["2026-01-07", "tier"] == 0           # no daily V → plain
    ui = LX._to_ui(f.loc["2026-01-05"].to_dict() | {"fam": "L34"})
    assert ui["lvx"] and ui["lvx_l34_v"] and ui["lvx_l34_vl"] and ui["lvx_l34_vh"] and ui["lvx_l34_vx"] and not ui["lvx_l46_v"]
    ui2 = LX._to_ui(f.loc["2026-01-06"].to_dict() | {"fam": "L46"})
    assert ui2["lvx_l46_v"] and ui2["lvx_l46_vl"] and not ui2["lvx_l46_vh"] and not ui2["lvx_l46_vx"] and ui2["lvx_label"] == "L46VL"
    for k in LX.MISS:
        assert k in ui


def test_04e_ovd_map_port():
    import ovd_map_build as OM
    from studio import ovdmap_store as OV
    # prior_median: nan until 20 FULL sessions exist strictly before the day; the day never sees itself
    v = np.arange(1.0, 31.0); full = np.ones(30, bool); full[5] = False
    den = OM.prior_median(v, full)
    assert np.isnan(den[:21]).all()                     # 20 full sessions exist only after index 20 (index 5 is not full)
    assert den[21] == np.median(np.delete(v[:21], 5))   # the 20 full values before index 21
    assert den[22] == np.median(np.delete(v[1:22], 4))  # window slides by one full session
    # a synthetic ticker: HV event on day 40 (opening ×3 after a decline), reclaim + heavy close on day 48
    n = 60
    d = pd.DataFrame(dict(ticker="T", date=pd.bdate_range("2024-01-01", periods=n).strftime("%Y-%m-%d"), close=np.linspace(100, 90, n)))
    for k in OM.SLOTS:
        d[k] = 100.0
    for k in ("m1", "m2", "m3", "m4"):
        d.loc[40, k] = 300.0
        d.loc[48, k] = 310.0
    for k in ("cv1", "cv2", "cv3", "cv4"):
        d.loc[48, k] = 400.0
    r = OM.compute_ticker(d)
    assert bool(r.loc[40, "hv_event"]) and r.loc[40, "rv_o60"] == pytest.approx(3.0)
    assert bool(r.loc[48, "rc30"]) and bool(r.loc[48, "rc60"]) and r.loc[48, "event_k"] == 8
    assert bool(r.loc[48, "cd30"]) and bool(r.loc[48, "cd60"])            # decline + heavy close ≥ open windows
    assert not bool(r.loc[48, "ob60"])                                     # flat RVOL before → no 3-session build
    assert bool(r.loc[49, "ho60"]) is False                                # day 49 opening RVOL is 1.0, not > 1
    # a half day (closing slots missing) is not full and invalidates the next day's handoff state
    d2 = d.copy(); d2.loc[30, ["cv3", "cv4"]] = np.nan
    r2 = OM.compute_ticker(d2)
    assert not bool(r2.loc[30, "full"]) and not bool(r2.loc[31, "ho60"]) and not bool(r2.loc[31, "ho30"])
    assert OM.tokens_of(r.loc[48]) == "RC·30 RC·60 CD·30 CD·60"
    ui = OV._to_ui(dict(r.loc[48]) | {"tokens": OM.tokens_of(r.loc[48]), "text": OM.text_of(r.loc[48])})
    assert ui["ovdmap"] and ui["ovdmap_rc60"] and ui["ovdmap_cd30"] and not ui["ovdmap_ob30"] and ui["ovdmap_event_k"] == 8
    for k in OV.MISS:
        assert k in ui


def test_04f_vol7_port():
    import vol7_build as V7
    from studio import vol7_store as VS
    # levels
    assert list(V7.mr_level(np.array([0.1, 0.5, 0.8, 1.2, 2.0, 3.0, 7.0]))) == [0, 1, 2, 3, 4, 5, 6]
    mid, sd = np.full(7, 100.0), np.full(7, 10.0)
    assert list(V7.sigma_level(np.array([80.0, 87.0, 92.0, 100.0, 107.0, 115.0, 130.0]), mid, sd)) == [0, 1, 2, 3, 4, 5, 6]
    # a synthetic ticker: flat 100-volume, then a 10-day decline, VB (×6) on day 40, quiet, VB2 on day 48, green break on day 50
    n = 60
    d = pd.DataFrame(dict(date=pd.bdate_range("2024-01-01", periods=n).strftime("%Y-%m-%d"), volume=100.0,
                          open=100.0, high=101.0, low=99.0, close=100.0))
    d.loc[30:47, "close"] = np.linspace(100, 90, 18); d.loc[30:47, "open"] = d.loc[30:47, "close"] + 0.2   # red decline into day 47
    d.loc[40, "volume"] = 600.0
    d.loc[48, "volume"] = 650.0; d.loc[48, ["open", "high", "low", "close"]] = [90.0, 92.0, 88.0, 90.5]
    d.loc[49, ["open", "close"]] = [90.5, 91.0]
    d.loc[50, ["open", "high", "low", "close"]] = [91.0, 93.5, 90.8, 93.0]                                  # green close above the VB2 high
    r = V7.compute_ticker(d).set_index("date")
    k = lambda i: d.loc[i, "date"]
    assert r.loc[k(40), "mr"] == 6 and r.loc[k(40), "trans"] >= 3 and not r.loc[k(40), "vb2"]              # first VB: big jump, no prior VB
    assert r.loc[k(41), "trans"] <= -3                                                                       # back down = big jump down
    assert r.loc[k(48), "mr"] == 6 and r.loc[k(48), "vb2"] and r.loc[k(48), "prior_move"] < -3               # VB2 after 8 quiet bars, decline before
    assert r.loc[k(50), "shift_up"] and not r.loc[k(49), "shift_up"]                                        # confirmed on the break, not before
    assert np.isnan(r.loc[k(5), "ratio"]) or r.loc[k(5), "mr"] == -1                                        # undefined before 20 bars
    assert V7.marks_of(r.loc[k(48)]) == "◆+3 VB2" and V7.marks_of(r.loc[k(50)]) == "SHIFT↑"
    # store: threshold-free flags read straight from the levels
    ui = VS._to_ui(dict(r.loc[k(48)]) | dict(cons="", jump="◆+3", label="M6·σ6", marks="◆+3 VB2", text="x"))
    assert ui["vol7_m6"] and ui["vol7_up3"] and ui["vol7_vb2"] and not ui["vol7_shift_up"] and ui["vol7_label"] == "M6·σ6"
    for key in VS.MISS:
        assert key in ui


def test_05_store_to_ui_serves_both_modes(monkeypatch):
    monkeypatch.setattr(LB, "spec", lambda: {"mode": "V"})
    miss = LB._to_ui(dict(marks="", udn=None, udn_c=None, text="", star=False, udn_all=None, nv_pos=0, nv_neg=0))
    assert miss["lbal"] is False and miss["lbal_marks"] == "" and miss["lbal_udn"] is None and miss["lbal_nn"] is False
    assert miss["lbal_all_marks"] == "" and miss["lbal_all_star"] is False
    hit = LB._to_ui(dict(marks="★ ★★", udn="U", udn_c="D", text="U 5:2 · D+ 8:11", star=True, conflict=True,
                         half_up=False, half_dn=False, nn=False, nv_pos=5, nv_neg=2, nv_pos_c=8, nv_neg_c=11,
                         marks_all="", udn_all="U", udn_c_all="U", text_all="U 9:7 · U+ 14:12", star_all=False, conflict_all=False,
                         half_up_all=False, half_dn_all=False, nn_all=False,
                         n_pos=9, n_neg=7, n_pos_c=14, n_neg_c=12, n_bars=26, nv_bars=7, colour="RED"))
    assert hit["lbal"] is True and hit["lbal_mode"] == "V"
    assert hit["lbal_marks"] == "★ ★★" and hit["lbal_udn_c"] == "D" and hit["lbal_n_neg_c"] == 11 and hit["lbal_n_pos"] == 5   # V set
    assert hit["lbal_all_marks"] == "" and hit["lbal_all_udn_c"] == "U" and hit["lbal_all_n_pos"] == 9 and hit["lbal_all_conflict"] is False
    assert hit["lbal_all_text"] == "U 9:7 · U+ 14:12" and hit["lbal_nv_bars"] == 7
    for k in LB.MISS:
        assert k in hit
    # a session labelled in ALL mode but with no V bar keeps lbal=True and an empty V state
    v_empty = LB._to_ui(dict(marks="", udn=None, udn_c=None, text="", udn_all="D", udn_c_all="D", marks_all="★", text_all="D 3:5 · D+ 6:9",
                             star_all=True, conflict_all=False, half_up_all=False, half_dn_all=False, nn_all=False, colour="GREEN"))
    assert v_empty["lbal"] is True and v_empty["lbal_udn"] is None and v_empty["lbal_star"] is False and v_empty["lbal_all_star"] is True


def test_06_enrich_joins_on_scan_date(monkeypatch):
    import studio.ultra_db_scan as U
    fake = {("AAA", "2026-09-04"): dict(LB.MISS, lbal=True, lbal_marks="★★★", lbal_udn="U", lbal_udn_c="N")}
    monkeypatch.setattr(LB, "available", lambda: True)
    monkeypatch.setattr(LB, "by_dates", lambda dates: fake if "2026-09-04" in dates else {})
    rows = [{"ticker": "AAA", "scan_date": "2026-09-04T00:00:00"},        # live scan row shape
            {"ticker": "AAA", "date": "2026-09-04"},                       # legacy shape
            {"ticker": "BBB", "scan_date": "2026-09-04"}]
    U._enrich_lbal(rows)
    assert rows[0]["lbal_marks"] == "★★★" and rows[1]["lbal_marks"] == "★★★"
    assert rows[2]["lbal"] is False and rows[2]["lbal_marks"] == "" and rows[2]["lbal_half_up"] is False
    assert U._row_date({"scan_date": "2026-09-04T00:00:00"}) == "2026-09-04" == U._row_date({"date": "2026-09-04"})
