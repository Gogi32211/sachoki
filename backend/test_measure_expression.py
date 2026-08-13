"""The expression language, tested where it can lie: refusals, shifts, adjacency."""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import measure_expression as MX                                       # noqa: E402

ok = fail = 0


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                            # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def frame() -> pd.DataFrame:
    """Two tickers, and B's history has a calendar hole between rows 2 and 3."""
    rows = []
    for tk, dates in (("AAA", ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]),
                      ("BBB", ["2026-01-05", "2026-01-06", "2026-03-02", "2026-03-03"])):
        for i, d in enumerate(dates):
            rows.append({"ticker": tk, "date": d, "open": 10.0 + i, "high": 11.0 + i,
                         "low": 9.0 + i, "close": 10.5 + i, "volume": 1e6,
                         "t_sig": "T1" if i % 2 == 0 else "T2", "l_sig": f"L{i + 1}",
                         "rsi_14": 30.0 + 10 * i})
    df = pd.DataFrame(rows)
    prev = df.groupby("ticker")["date"].transform(
        lambda s: (pd.to_datetime(s).diff().dt.days <= 3).fillna(False))
    df["prev_ok"] = prev
    return df


def t1_forbidden_column_refused_with_the_contracts_reason():
    """ultra_score_v3 is rejected with the sentence from BAR_FORBIDDEN, not 'invalid input'"""
    try:
        MX.validate("ultra_score_v3 > 50")
    except MX.ExpressionError as e:
        assert "a score we fitted" in str(e), str(e)
    else:
        raise AssertionError("a fitted score was accepted as an input")
    try:
        MX.validate("fwd_5d > 0")
    except MX.ExpressionError as e:
        assert "forward label" in str(e), str(e)
        return
    raise AssertionError("an outcome column was accepted as an INPUT")


def t2_unknown_name_gets_a_suggestion():
    try:
        MX.validate("rsi14 < 30")
    except MX.ExpressionError as e:
        assert "rsi_14" in str(e), str(e)
        return
    raise AssertionError("an unknown name validated")


def t3_no_python_smuggling():
    """the language is comparisons and boolean logic, nothing that can execute"""
    for evil in ("__import__('os').system('x')", "open('/etc/passwd')",
                 "close.shift(1) if True else 0", "[c for c in close]",
                 "close.shift('a')", "close.shift(999)", "t_sig.replace('T','Z') == 'Z1'"):
        try:
            MX.validate(evil)
        except MX.ExpressionError:
            continue
        raise AssertionError(f"validated: {evil}")


def t4_shift_is_per_ticker():
    """ticker B's first bar must not see ticker A's last"""
    df = frame()
    mask, info = MX.evaluate(df, "close > close.shift(1)")
    assert info["max_shift"] == 1
    first_bbb = df[(df.ticker == "BBB")].index[0]
    assert not mask.loc[first_bbb], "the first bar of BBB read AAA's last bar through shift"


def t5_a_calendar_gap_excludes_the_row_and_is_counted():
    """previous ROW is not previous DAY — the dup-row incident's cousin"""
    df = frame()
    mask, info = MX.evaluate(df, "close > close.shift(1)")
    gap_row = df[(df.ticker == "BBB") & (df.date == "2026-03-02")].index[0]
    assert not mask.loc[gap_row], "a row whose shift crossed a two-month hole stayed in the mask"
    assert info["n_dropped_nonadjacent"] >= 1, info


def t6_tokens_strings_and_isin():
    df = frame()
    m1, _ = MX.evaluate(df, "t_sig == 'T1'")
    assert int(m1.sum()) == 4, int(m1.sum())
    m2, _ = MX.evaluate(df, "t_sig.startswith('T') and l_sig in ('L1', 'L3')")
    assert int(m2.sum()) == 4, int(m2.sum())
    m3, _ = MX.evaluate(df, "not t_sig == 'T1'")
    assert int(m3.sum()) == 4


def t7_the_on_column_is_detected_for_the_matcher():
    """the gap-depth lesson: report what the claim selects on"""
    info = MX.validate("rsi_14 < 35 and t_sig == 'T1'")
    assert info["on_column"] == "rsi_14", info["on_column"]
    info2 = MX.validate("t_sig == 'T1' and l_sig == 'L34'")
    assert info2["on_column"] is None, info2["on_column"]
    info3 = MX.validate("(high - low) > 2 * atr_14")
    assert info3["on_column"] in ("atr_14", "high", "low"), info3["on_column"]


def t8_canonical_form_is_stable():
    """'a  and   b' and 'a and b' are one claim — the k ledger keys on this"""
    a = MX.validate("t_sig == 'T1'   and  rsi_14 < 35")["canonical"]
    b = MX.validate("t_sig=='T1' and rsi_14<35")["canonical"]
    assert a == b, (a, b)


TESTS = [t1_forbidden_column_refused_with_the_contracts_reason,
         t2_unknown_name_gets_a_suggestion,
         t3_no_python_smuggling,
         t4_shift_is_per_ticker,
         t5_a_calendar_gap_excludes_the_row_and_is_counted,
         t6_tokens_strings_and_isin,
         t7_the_on_column_is_detected_for_the_matcher,
         t8_canonical_form_is_stable]

print("=" * 100, flush=True)
print("  MEASURE EXPRESSION — the console's language, tested where it can lie", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate(TESTS, 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
