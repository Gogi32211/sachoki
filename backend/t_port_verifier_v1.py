"""Independent verifier for the Massive T port — deliberately NOT the same program.

It imports neither t_port_v1 nor signal_engine. It is a scalar row-by-row loop with an explicit
if/elif priority chain, written from the frozen Amendment 2 config rather than from either of
the implementations it checks. The vectorised implementations could share a subtle broadcasting
or NaN-propagation mistake; this one cannot share it, because it has no vector operations at all
and evaluates every predicate as a plain Python boolean.

The priority chain is an if/elif ladder ON PURPOSE. Both other implementations resolve priority
by assigning into a code array in order, so an error in that mechanism would look identical in
both. Here the first matching branch simply wins, which is the same rule expressed in a way that
cannot fail the same way.

Availability is recomputed here too, not inherited, because "no signal" and "cannot evaluate"
must stay distinguishable in the checker as well as in the thing being checked.
"""
from __future__ import annotations

GUARD = 1e-10
MIN_BODY_RATIO = 1.0
USE_WICK = False

BC_TO_SID = {1: 6, 2: 8, 3: 1, 4: 3, 5: 2, 6: 4, 7: 9, 8: 10, 9: 5, 10: 11, 11: 7, 12: 12}
SIG_NAMES = {0: "NONE", 1: "T1G", 2: "T1", 3: "T2G", 4: "T2", 5: "T3", 6: "T4",
             7: "T5", 8: "T6", 9: "T9", 10: "T10", 11: "T11", 12: "T12"}


def _row_predicates(prev, cur):
    """All twelve raw predicates for one bar given its immediate predecessor."""
    po, ph, pl, pc = prev
    o, h, l, c = cur

    prev_doji = (pc == po)
    p1_bull = pc > po
    p1_bear = (pc < po) or prev_doji          # raw prior-bar doji, not resolved Z7
    is_bull = c > o

    p_body = abs(pc - po)
    p_top, p_bot = max(po, pc), min(po, pc)
    c_body = abs(c - o)
    c_top, c_bot = max(o, c), min(o, c)

    if USE_WICK:
        e_h, e_l = h, l
    else:
        e_h, e_l = c_top, c_bot

    safe = p_body if p_body > GUARD else GUARD
    eng_ok = (c_body / safe >= MIN_BODY_RATIO) and (e_h >= p_top) and (p_bot >= e_l)
    ins_ok = (c_top <= p_top) and (c_bot >= p_bot)

    return {
        "T1G": p1_bear and o > pc and o > po and c > po and is_bull,
        "T1":  p1_bear and o >= pc and po >= o and c > po and is_bull,
        "T2G": p1_bull and o >= po and o > pc and c > pc and is_bull,
        "T2":  p1_bull and o >= po and o <= pc and c > pc and is_bull,
        "T3":  p1_bear and is_bull and o < po and o < pc and c < po and c > pc,
        "T4":  p1_bear and is_bull and eng_ok,
        "T5":  p1_bear and is_bull and o < po and o < pc and c < po and pc >= c,
        "T6":  p1_bull and is_bull and eng_ok,
        "T9":  p1_bear and is_bull and ins_ok,
        "T10": p1_bull and is_bull and ins_ok,
        "T11": p1_bull and is_bull and o < po and c >= po and c < pc,
        "T12": p1_bull and is_bull and o < po and c < po,
    }


def _winner(p):
    """Explicit ladder — the first branch that matches wins."""
    if p["T4"]:  return 1, "T4"
    elif p["T6"]:  return 2, "T6"
    elif p["T1G"]: return 3, "T1G"
    elif p["T2G"]: return 4, "T2G"
    elif p["T1"]:  return 5, "T1"
    elif p["T2"]:  return 6, "T2"
    elif p["T9"]:  return 7, "T9"
    elif p["T10"]: return 8, "T10"
    elif p["T3"]:  return 9, "T3"
    elif p["T11"]: return 10, "T11"
    elif p["T5"]:  return 11, "T5"
    elif p["T12"]: return 12, "T12"
    return 0, "NONE"


def verify(rows, observed=None):
    """rows: list of (open, high, low, close), ascending. Returns list of dicts."""
    n = len(rows)
    if observed is None:
        observed = [True] * n
    out = []
    for i in range(n):
        # AMENDMENT_3 (a5edf6e6806d4e0b) precedence, as an explicit ladder. The structural
        # startup condition is tested first and is the ONLY one that outranks current-bar
        # coverage; where a predecessor exists, an incomplete current bar still wins.
        if i == 0:                                   # the predecessor does not exist at all
            out.append(dict(t_label="", bc=0, sig_id=0, state="UNAVAILABLE",
                            reason="NO_PRIOR_BAR", preds=None))
            continue
        if not observed[i]:
            out.append(dict(t_label="", bc=0, sig_id=0, state="UNAVAILABLE",
                            reason="CURRENT_BAR_NOT_COMPLETE", preds=None))
            continue
        if not observed[i - 1]:
            out.append(dict(t_label="", bc=0, sig_id=0, state="UNAVAILABLE",
                            reason="PRIOR_INPUT_NOT_COMPLETE", preds=None))
            continue
        p = _row_predicates(rows[i - 1], rows[i])
        bc, name = _winner(p)
        sid = BC_TO_SID.get(bc, 0)
        out.append(dict(t_label=SIG_NAMES[sid], bc=bc, sig_id=sid,
                        state="AVAILABLE", reason="", preds=p))
    return out
