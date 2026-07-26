"""Regression guard for the studio.signal_stats SQL-safety helpers (audit fix #1).

Locks in two invariants:
  1. For legitimate inputs the emitted SQL literal is BYTE-IDENTICAL to before the
     fix (so query results are unchanged).
  2. Malicious input (quotes / statement breakers) can no longer escape the literal.
"""
from __future__ import annotations

import os
import sys

_backend = os.path.join(os.path.dirname(__file__), "..", "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)

from studio.signal_stats import _safe_universe, _q, _sql_cond


def test_safe_universe_allows_known():
    assert _safe_universe("sp500") == "sp500"
    assert _safe_universe("NASDAQ") == "nasdaq"      # case-normalised
    assert _safe_universe(" russell2k ") == "russell2k"


def test_safe_universe_rejects_unknown_and_injection():
    assert _safe_universe(None) is None
    assert _safe_universe("") is None
    assert _safe_universe("'; DROP TABLE bars;--") is None
    assert _safe_universe("sp500 OR 1=1") is None


def test_q_is_noop_for_clean_tokens():
    # legitimate signal / suffix / line5 tokens contain no quotes → unchanged
    for tok in ["T2G", "Z1G", "PS-R2X", "EUR", "G1-C", "EB%", "NDPO"]:
        assert _q(tok) == tok


def test_q_escapes_single_quotes():
    assert _q("a'b") == "a''b"
    assert _q("x' OR '1'='1") == "x'' OR ''1''=''1"


def test_sql_cond_byte_identical_for_clean_input():
    # behaviour preservation: clean values produce exactly the old SQL
    assert _sql_cond("t_0", "T2G") == "t_0 = 'T2G'"
    assert _sql_cond("s_0", "EB%") == "s_0 LIKE 'EB%'"      # wildcard → LIKE preserved


def test_sql_cond_neutralises_injection():
    out = _sql_cond("t_0", "x' OR '1'='1")
    # the embedded quotes are doubled, so the literal can't be broken out of
    assert out == "t_0 = 'x'' OR ''1''=''1'"
    assert "OR '1'='1'" not in out
