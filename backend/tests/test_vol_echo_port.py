"""Pins the 260925_VOL_ECHO display port (vol_echo_build.compute).

1. Synthetic bars walk the whole state machine once: spike → echo → quiet → release → breakout, and
   a second zone: echo → breakdown (BD▼ + BDV▼) → R, then Q∧R → first ▲ release = the veto shape.
2. Real bars: the RGTI Oct-2024 sequence that was checked by eye against TradingView
   (Q on 10-15, VE on 10-16/17/18/21/22, ▲ release on 10-16) — skipped when the store is absent.
"""
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import vol_echo_build as VB                                              # noqa: E402


def _run(bars):
    df = pd.DataFrame(bars, columns=["o", "h", "l", "c", "v"])
    df["t"] = "X"
    base, sma = VB.baselines(df["v"].astype(float), df["t"])
    return VB.compute(*(df[k].to_numpy(float) for k in "ohlcv"), base, sma)


def _flat(n, px=10.0, vol=100.0):
    return [(px, px + 0.5, px - 0.5, px + 0.01, vol)] * n


def test_spike_echo_quiet_release_breakout():
    bars = _flat(25)
    bars += [(10.0, 11.0, 10.0, 10.8, 300.0)]            # 25 spike (3× base)
    bars += _flat(8)                                      # 26-33
    bars += [(10.8, 11.0, 10.2, 10.3, 280.0)]            # 34 echo: ×0.93 vol, range overlaps the spike
    bars += [(10.5, 10.7, 10.4, 10.6, 50.0)] * 2          # 35-36 quiet inside the zone [10.2, 11.0]
    bars += [(11.2, 11.9, 11.1, 11.8, 400.0)]            # 37 green, opens+closes above, vol > SMA20
    r = _run(bars)
    assert r["spike"][25] and r["echo"][34] and r["spk"][25]
    assert r["echo_gap"][34] == 9 and r["echo_col"][34] == "G→R"
    assert r["q"][35] and r["q"][36] and r["qn"][36] == 2
    assert r["rel_up"][37] and r["rel_n"][37] == 1 and r["rel_q"][37] == 2
    assert r["bo"][37] and r["bov"][37] and r["bo_age"][37] == 3
    assert not r["bd"].any() and not r["r"].any() and not r["qr_rel_veto"].any()


def test_breakdown_r_and_veto_shape():
    bars = _flat(25)
    bars += [(10.0, 11.0, 10.0, 10.8, 300.0)]            # 25 spike
    bars += _flat(8)
    bars += [(10.8, 11.0, 10.2, 10.3, 280.0)]            # 34 echo, zone [10.2, 11.0]
    bars += [(10.0, 10.1, 9.5, 9.6, 400.0)]              # 35 red, opens+closes below → BD▼ and BDV▼
    bars += [(9.8, 10.6, 9.7, 10.5, 50.0)]               # 36 back inside, low vol → Q and R
    bars += [(10.5, 10.9, 10.4, 10.8, 400.0)]            # 37 green, vol > SMA20 → first ▲ release
    r = _run(bars)
    assert r["bd"][35] and r["bdv"][35]
    assert not r["r"][35], "the breakdown bar itself never counts as R"
    assert r["q"][36] and r["r"][36] and r["rn"][36] == 1
    assert r["rel_up"][37] and r["rel_n"][37] == 1
    assert r["qr_rel_veto"][37], "yesterday Q∧R + today the first ▲ release = the QR_REL_V1 veto shape"


@pytest.mark.skipif(not os.path.exists(VB.OUT), reason="vol_echo_signals.parquet not built")
def test_rgti_oct_2024_matches_tradingview():
    k = pd.read_parquet(VB.OUT)
    x = k[(k.ticker == "RGTI") & k.date.between("2024-10-14", "2024-10-22")].set_index("date")
    assert bool(x.loc["2024-10-15", "ve_q"])
    for d in ("2024-10-16", "2024-10-17", "2024-10-18", "2024-10-21", "2024-10-22"):
        assert bool(x.loc[d, "ve_echo"]), d
    assert bool(x.loc["2024-10-16", "ve_rel_up"])
    assert int(x.loc["2024-10-21", "ve_echo_gap"]) == 69
    assert int(x.loc["2024-10-22", "ve_echo_gap"]) == 68
