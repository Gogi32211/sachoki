"""Massive T port — the canonical legacy T materializer semantics, reimplemented.

Bound to MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2 (bd9292853ee10f2c). Every predicate here
is transcribed from signal_engine.py :: compute_signals, which Amendment 2 froze as
CANONICAL_LEGACY_T_MATERIALIZER. Nothing in this file was chosen because of what it produces.

THIS IS A REIMPLEMENTATION, NOT A WRAPPER, and that is the whole point. If it imported the
producer it could not test the producer. Layer 1 runs both on identical input and requires
EXACT label equality — not 99.x% — because the only thing being tested there is whether two
independent expressions of the same semantics agree.

THE ONE THING THIS FILE ADDS TO THE PRODUCER IS AVAILABILITY. The producer answers True/False
for every row, because at 1d over a fetched frame every row had a predecessor by construction.
On Massive 15m the predecessor can be missing or contaminated, and a missing predecessor is not
a False T — it is a bar on which T cannot be evaluated. So the port emits a state alongside the
label, and UNAVAILABLE is never collapsed into "no signal".

CALL-CONTEXT IS FROZEN, NOT INFERRED. The preflight established from the caller — not from the
producer — that fetch_bars deduplicates on the index and sorts ascending, that fetch_ohlcv takes
the tail of ONE ticker, and that norm_ohlcv neither sorts nor drops. So shift(1) means "the
immediately preceding row of this ticker's deduplicated ascending series", and at 15m that
crosses the session boundary because the frame is never rebuilt per session. That is recorded
as SESSION_CONTINUOUS and enforced here rather than left to the caller's habits.
"""
from __future__ import annotations
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402

AMENDMENT_2 = "bd9292853ee10f2c"

# ---- frozen producer config (Amendment 2 · frozen_producer_config) ----------
USE_WICK = False
MIN_BODY_RATIO = 1.0
DOJI_THRESH = 0.05          # declared by the producer; DOES NOT enter isDoji
NUMERICAL_GUARD = 1e-10     # CANONICAL_NUMERICAL_GUARD — not a tick size

# ---- frozen state mapping (Amendment 2 · state_id_mapping) ------------------
BC_TO_SID = {1: 6, 2: 8, 3: 1, 4: 3, 5: 2, 6: 4, 7: 9, 8: 10, 9: 5, 10: 11, 11: 7, 12: 12}
SIG_NAMES = {0: "NONE", 1: "T1G", 2: "T1", 3: "T2G", 4: "T2", 5: "T3", 6: "T4",
             7: "T5", 8: "T6", 9: "T9", 10: "T10", 11: "T11", 12: "T12"}
# priority order is the bc code order itself: bc 1 wins over bc 2, and so on
PRIORITY = ["T4", "T6", "T1G", "T2G", "T1", "T2", "T9", "T10", "T3", "T11", "T5", "T12"]
REQUIRED_PRIOR_BARS = 1
CALL_CONTEXT = "SESSION_CONTINUOUS"   # shift(1) crosses the session boundary


class TPortHold(Exception):
    """Fail-closed. Never downgraded to a value."""


def _predicates(o, h, l, c):
    """Every raw T predicate, transcribed from the producer. Arrays in, dict of masks out."""
    o1, c1 = np.roll(o, 1), np.roll(c, 1)
    o1[0] = np.nan; c1[0] = np.nan

    isDoji = (c == o)
    isBull = c > o
    d1 = np.zeros(len(o), dtype=bool)
    d1[1:] = isDoji[:-1]                      # raw prior-bar doji — NOT resolved Z7

    p1Bull = c1 > o1
    p1Bear = (c1 < o1) | d1

    pBody = np.abs(c1 - o1)
    pTop = np.maximum(o1, c1)
    pBot = np.minimum(o1, c1)
    cBody = np.abs(c - o)
    cTop = np.maximum(o, c)
    cBot = np.minimum(o, c)

    # use_wick=False -> the engulf edges are the BODY edges
    eH, eL, eP, ePl = (cTop, cBot, pTop, pBot) if not USE_WICK else (h, l, pTop, pBot)

    with np.errstate(invalid="ignore", divide="ignore"):
        safe = np.maximum(pBody, NUMERICAL_GUARD)
        engOk = (cBody / safe >= MIN_BODY_RATIO) & (eH >= eP) & (ePl >= eL)
    insOk = (cTop <= pTop) & (cBot >= pBot)

    P = {
        "T1G": p1Bear & (o > c1) & (o > o1) & (c > o1) & isBull,
        "T1":  p1Bear & (o >= c1) & (o1 >= o) & (c > o1) & isBull,
        "T2G": p1Bull & (o >= o1) & (o > c1) & (c > c1) & isBull,
        "T2":  p1Bull & (o >= o1) & (o <= c1) & (c > c1) & isBull,
        "T3":  p1Bear & isBull & (o < o1) & (o < c1) & (c < o1) & (c > c1),
        "T4":  p1Bear & isBull & engOk,
        "T5":  p1Bear & isBull & (o < o1) & (o < c1) & (c < o1) & (c1 >= c),
        "T6":  p1Bull & isBull & engOk,
        "T9":  p1Bear & isBull & insOk,
        "T10": p1Bull & isBull & insOk,
        "T11": p1Bull & isBull & (o < o1) & (c >= o1) & (c < c1),
        "T12": p1Bull & isBull & (o < o1) & (c < o1),
    }
    return {k: np.nan_to_num(v, nan=False).astype(bool) for k, v in P.items()}


REGISTERED_TIMEFRAME = "15m"


def compute_t(df: pd.DataFrame, observed=None, timeframe: str = REGISTERED_TIMEFRAME):
    """Canonical T over one security's deduplicated, ascending series.

    df       : columns open/high/low/close, chronologically ascending, ONE security.
    observed : optional bool array; False where the bar is UNOBSERVED/CONTAMINATED.
    Returns a DataFrame with t_label, bc, sig_id, state, reason.
    """
    if timeframe != REGISTERED_TIMEFRAME:
        raise TPortHold(f"GUARD timeframe: {timeframe!r} is not registered; Amendment 2 "
                        f"freezes {REGISTERED_TIMEFRAME} ONLY, and the presence of other "
                        f"datasets is not hypothesis registration")
    cols = {str(x).lower() for x in df.columns}
    if not {"open", "high", "low", "close"} <= cols:
        raise TPortHold("GUARD columns: open/high/low/close required")
    d = df.copy()
    d.columns = [str(x).lower() for x in d.columns]
    if not d.index.is_monotonic_increasing:
        raise TPortHold("GUARD ordering: the frozen call context requires a chronologically "
                        "ascending index; the port will not sort on the caller's behalf")
    if d.index.duplicated().any():
        raise TPortHold("GUARD duplicates: the frozen call context deduplicates on the index "
                        "BEFORE computation; a duplicated timestamp is a caller defect")

    n = len(d)
    o = d["open"].to_numpy(float); h = d["high"].to_numpy(float)
    l = d["low"].to_numpy(float);  c = d["close"].to_numpy(float)
    if observed is None:
        observed = np.ones(n, dtype=bool)
    observed = np.asarray(observed, dtype=bool)
    if observed.shape != (n,):
        raise TPortHold(f"GUARD shape: observed must be ({n},)")

    P = _predicates(o, h, l, c)

    bc = np.zeros(n, dtype=np.int8)
    for code, name in enumerate(PRIORITY, start=1):
        bc = np.where((bc == 0) & P[name], np.int8(code), bc).astype(np.int8)

    sid = np.zeros(n, dtype=np.int8)
    for k, v in BC_TO_SID.items():
        sid[bc == k] = v
    label = np.array([SIG_NAMES[int(x)] for x in sid], dtype=object)

    # ---- availability: UNAVAILABLE is never a FALSE -------------------------
    # Precedence is frozen by AMENDMENT_3 (a5edf6e6806d4e0b) and the three blocking cases are
    # built MUTUALLY EXCLUSIVE on purpose. The defect this replaces was an ordering artefact:
    # the old code assigned overlapping masks in sequence, so whichever ran last silently won
    # and the first bar of a security disagreed with the scalar verifier. Disjoint masks
    # cannot have that bug, whatever order they are written in.
    prior_exists = np.zeros(n, dtype=bool); prior_exists[1:] = True
    prior_ok = np.zeros(n, dtype=bool); prior_ok[1:] = observed[:-1]

    state = np.full(n, "AVAILABLE", dtype=object)
    reason = np.full(n, "", dtype=object)
    s1 = ~prior_exists                                     # structural, takes precedence
    s2 = prior_exists & ~observed
    s3 = prior_exists & observed & ~prior_ok
    state[s1] = "UNAVAILABLE"; reason[s1] = "NO_PRIOR_BAR"
    state[s2] = "UNAVAILABLE"; reason[s2] = "CURRENT_BAR_NOT_COMPLETE"
    state[s3] = "UNAVAILABLE"; reason[s3] = "PRIOR_INPUT_NOT_COMPLETE"

    un = state == "UNAVAILABLE"
    label[un] = ""; bc[un] = 0; sid[un] = 0

    return pd.DataFrame({"t_label": label, "bc": bc, "sig_id": sid,
                         "state": state, "reason": reason}, index=d.index)


def raw_predicates(df: pd.DataFrame):
    """Raw (pre-priority) predicate masks — Layer 1 compares these, not only the label."""
    d = df.copy(); d.columns = [str(x).lower() for x in d.columns]
    return _predicates(d["open"].to_numpy(float), d["high"].to_numpy(float),
                       d["low"].to_numpy(float), d["close"].to_numpy(float))
