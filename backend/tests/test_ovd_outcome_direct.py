"""Post-exposure pathsim-semantics fixtures.

  1  union of mod-5 masks != one direct call when signals are consecutive (the engine's 5-bar cooldown) — the deviation is real
  2  direct_trades conservation: signal_true = selected + cooldown_suppressed + NO_NEXT_SESSION + NO_ENTRY_OPEN
  3  the direct runner contains no mod-5 partition and no local simulator
"""
import os, sys, json
import numpy as np, pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import ovd_outcome_direct_v1 as D                                        # noqa: E402
import ovd_outcome_v1 as O                                               # noqa: E402
from ovd_build import HardStop                                           # noqa: E402


def _frame(n=40):
    dates = [f"2024-01-{i+1:02d}" for i in range(n)]
    px = 100 + np.arange(n) * 0.5
    g = pd.DataFrame(dict(date=dates, open=px, high=px + 1, low=px - 1, close=px + 0.2, atr_14=np.full(n, 2.0)))
    g["CELL"] = False
    return g


def test_01_mod5_union_differs_from_direct_under_cooldown():
    ps, _ = O.sacred_pathsim()
    g = _frame(); sig = np.zeros(len(g), bool); sig[10:15] = True          # five consecutive TRUE days
    g["CELL"] = sig
    direct = ps({"T": g}, "CELL", "trail", 0.10, 0.25, 0.25, 60, slip=None, atr_k=12.0)
    union = 0
    for r in range(5):
        g[f"M{r}"] = sig & (np.arange(len(g)) % 5 == r)
        union += len(ps({"T": g}, f"M{r}", "trail", 0.10, 0.25, 0.25, 60, slip=None, atr_k=12.0))
    assert len(direct) == 1 and union == 5                                # cooldown keeps one; the partition keeps all five


def test_02_direct_trades_conservation():
    ps, _ = O.sacred_pathsim()
    frames = {"T": _frame()}
    keys = pd.DataFrame(dict(ticker=["T"] * 6, session=[f"2024-01-{d:02d}" for d in (11, 12, 13, 20, 27, 40)]))   # day 40 = last bar
    tr, cons = D.direct_trades(ps, frames, keys)
    assert cons["signal_true_n"] == 6 and cons["NO_NEXT_SESSION"] == 1 and cons["NO_ENTRY_OPEN"] == 0
    assert cons["direct_selected_n"] == 3 and cons["cooldown_suppressed_n"] == 2                       # 11 taken; 12,13 suppressed; 20, 27 taken
    assert cons["signal_true_n"] == cons["direct_selected_n"] + cons["cooldown_suppressed_n"] + cons["NO_NEXT_SESSION"] + cons["NO_ENTRY_OPEN"]
    assert not frames["T"]["CELL"].any()                                   # mask reset after the call


def test_03_direct_runner_has_no_partition_or_local_simulator():
    src = open(D.__file__).read().split('"""', 2)[-1]
    assert not D.PARTITION_IDIOM.search(src)
    assert D.PARTITION_IDIOM.search("g[f'M{r}'] = elig & (pos % 5 == r)")            # the first run's idiom IS caught
    O.assert_no_local_pathsim(open(D.__file__).read(), "direct runner")
