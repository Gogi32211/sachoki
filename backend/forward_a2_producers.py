"""A2 FORWARD PRODUCER LAYER — frozen downstream producers, forward-capable.

WRAP, DO NOT REWRITE.

Where a historical producer exposes an importable semantic core, it is imported and
called. Where the historical producer is a top-level script with no callable core
(pd12.py, stageD_r2.py, stageC4.py, phase1.py all are), importing it would execute a
full production run against hard-coded scratchpad paths — technically impossible to
reuse. In that case the semantic core is TRANSCRIBED VERBATIM here, expression for
expression, and the transcription is proven by exact historical replay rather than
asserted.

Nothing in this module is "cleaned up". Where the original is redundant or awkward the
transcription keeps it, because the qualification oracle is byte-for-byte agreement with
the sealed historical output.

Import-side-effect free: no connections, no writes, no path assumptions at import time.
"""
from __future__ import annotations
import os, sys, json, hashlib                                          # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
RUNTIME = os.path.join(HERE, "massive_new_family_runtime")

# ─────────────────────────────────────────────────────────────── authorities
PD_DEP_FILE = os.path.join(RUNTIME, "pd_dep.json")
PD_DEP_SEALED = "MASSIVE_NEW_FAMILY_PD_DEPENDENCY_GRAPH_V1"
COHORT_FILE = os.path.join(RUNTIME, "FORWARD_COHORT_476_security_key_v1.txt")
COHORT_DIGEST = "2d1ec872dcf74df4"
COHORT_N = 476
EMA_PERIODS = (9, 20, 34, 50, 89, 200)


class A2Hold(Exception):
    """Fail-closed. Never downgraded, never caught to continue."""


def _sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def load_cohort(path=COHORT_FILE):
    if not os.path.exists(path):
        raise A2Hold(f"GUARD cohort_missing: {path}")
    blob = open(path, "rb").read()
    keys = [l for l in blob.decode().split("\n") if l]
    d = hashlib.sha256(blob).hexdigest()[:16]
    if d != COHORT_DIGEST:
        raise A2Hold(f"GUARD cohort_digest: {d} != sealed {COHORT_DIGEST}")
    if len(keys) != COHORT_N or len(set(keys)) != COHORT_N:
        raise A2Hold(f"GUARD cohort_size: {len(keys)}")
    return set(keys)


def load_pd_dep(path=PD_DEP_FILE):
    """The transitive EMA-period dependency graph. pd12 does COLS = list(DEP), so the
    dict ORDER is part of the contract and json.load preserves it."""
    if not os.path.exists(path):
        raise A2Hold(f"GUARD pd_dep_missing: {path} (sealed as {PD_DEP_SEALED})")
    dep = json.load(open(path))
    if len(dep) != 12:
        raise A2Hold(f"GUARD pd_dep_size: {len(dep)} columns, expected 12")
    for c, ps in dep.items():
        for p in ps:
            if p not in EMA_PERIODS:
                raise A2Hold(f"GUARD pd_dep_period: {c} references unregistered EMA {p}")
    return dep


# ══════════════════════════════════════════════════════════════════════════
# pd12 — TRANSCRIBED VERBATIM from massive_new_family_runtime/pd12.py
#
# The core is ROW-LOCAL: every output row depends only on that row's own
# open, close, six EMA values and six EMA validity states, plus its own
# coverage_state. There is NO shift, NO rolling, NO accumulator. That is a
# property of the frozen producer, established by reading it — not a
# simplification introduced here.
# ══════════════════════════════════════════════════════════════════════════
def pd12_core(d, ev, es, dep):
    """d: DataFrame indexed by bar_start with columns coverage_state, open, close, session_date
       ev/es: DataFrames indexed by bar_start, columns = EMA periods (value / state)
       dep: the sealed dependency graph
       Returns the long-form frame pd12 writes, in the producer's own column order."""
    import numpy as np, pandas as pd
    PER = list(EMA_PERIODS)
    COLS = list(dep)
    n = len(d)
    # A required upstream column must FAIL CLOSED, never surface as a bare KeyError.
    for _src, _nm in ((ev, "ema value"), (es, "ema state")):
        _miss = [p for p in PER if p not in _src.columns]
        if _miss:
            raise A2Hold(f"GUARD upstream_column_absent: {_nm} missing periods {_miss}")
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    o = d.open.to_numpy(); c_ = d.close.to_numpy()
    cx = {p: (o < ev[p].to_numpy()) & (c_ > ev[p].to_numpy()) for p in PER}
    dx = {p: (o > ev[p].to_numpy()) & (c_ < ev[p].to_numpy()) for p in PER}
    stv = {p: es[p].to_numpy() for p in PER}
    p3 = cx[9] & cx[20] & cx[50]
    F = {'sig_p66': cx[200] & (cx[9] | cx[20] | cx[34] | cx[50] | cx[89]),
         'sig_p55': cx[89] & (cx[9] | cx[20] | cx[34] | cx[50] | cx[200]),
         'sig_p89': cx[89], 'sig_p3': p3, 'sig_p2': cx[9] & cx[20] & ~p3,
         'sig_p50': cx[50] & ~cx[9] & ~cx[20],
         'sig_d66': dx[200] & (dx[9] | dx[20] | dx[34] | dx[50] | dx[89]),
         'sig_d55': dx[89] & (dx[9] | dx[20] | dx[34] | dx[50] | dx[200]),
         'sig_d89': dx[89], 'sig_d3': dx[9] & dx[20] & dx[50],
         'sig_d2': dx[9] & dx[20], 'sig_d50': dx[50]}
    missing = [c for c in COLS if c not in F]
    if missing:
        raise A2Hold(f"GUARD pd_formula_missing: {missing}")
    frames = []
    for c in COLS:
        need = dep[c]
        anyun = np.zeros(n, bool); anyinit = np.zeros(n, bool)
        for p in need:
            anyun |= (stv[p] == 'UNAVAILABLE')
            anyinit |= (stv[p] == 'INITIALIZATION_SENSITIVE')
        av = np.where(~ok, 'UNAVAILABLE_CURRENT',
                      np.where(anyun, 'UNAVAILABLE_DEPENDENCY',
                               np.where(anyinit, 'INITIALIZATION_SENSITIVE', 'AVAILABLE_VALID')))
        valid = (av == 'AVAILABLE_VALID')
        val = np.where(valid, F[c].astype(np.int8), None)
        # the frozen table declares `value TINYINT`; the original script relied on the DDL
        # to narrow it. Carry the width explicitly so the schema conforms outside DuckDB too.
        frames.append(pd.DataFrame(dict(bar_start=d.index.values,
                                        session_date=d.session_date.values,
                                        col=c, value=pd.array(val, dtype='Int8'),
                                        availability=av)))
    return pd.concat(frames, ignore_index=True)


def pd12_states_seen(es):
    """Every EMA validity state the producer's precedence chain knows about.
    An unexpected label must HARD FAIL rather than fall through to AVAILABLE_VALID."""
    KNOWN = {'VALID', 'INITIALIZATION_SENSITIVE', 'UNAVAILABLE'}
    seen = set()
    for p in EMA_PERIODS:
        seen |= set(map(str, es[p].dropna().unique()))
    bad = seen - KNOWN
    if bad:
        raise A2Hold(f"GUARD ema_state_vocabulary: unexpected {sorted(bad)}")
    return seen


# ══════════════════════════════════════════════════════════════════════════
# stageC4 — TRANSCRIBED VERBATIM from massive_new_family_runtime/stageC4.py
#
# NOT row-local. Two distinct kinds of state:
#   WITHIN-SEGMENT   compute_wlnbb (rolling(20), shift), the aL34/aL43/aSet
#                    maximum.accumulate ladders, the l_sig digit label, `age`
#   WHOLE-HISTORY    is_head — the FIRST contiguous COMPLETE run of the entire
#                    series. sig_cisd_* is computed on that run and nowhere else.
#
# `is_head` is therefore NOT inferable from a window. The orchestrator must supply it,
# because a resumed run whose window happens to start on a COMPLETE bar would otherwise
# manufacture a second "head" — the single most likely silent defect in this family.
# ══════════════════════════════════════════════════════════════════════════
C4_FALL = ("FRI34", "FRI43", "FRI64", "BLUE", "CCI_READY", "CCI_0_RETEST_OK", "CCI_BLUE_TURN",
           "BE_UP", "BE_DN", "BO_UP", "BO_DN", "BX_UP", "BX_DN", "FUCHSIA_RL", "FUCHSIA_RH",
           "PRE_PUMP")
C4_WL = ['vol_bucket', 'l34', 'l22', 'l43', 'bo_up', 'bx_up', 'be_up', 'l_sig']
C4_CI = {'sig_cisd_cplus': 'PLUS_CISD', 'sig_cisd_cplus_minus': 'CISD_PPM',
         'sig_cisd_minus_struct': 'MINUS_STRUCT', 'sig_cisd_mpm': 'CISD_MPM'}
C4_COLS = C4_WL + list(C4_CI)


def stagec4_segments(ok):
    """The frozen segmentation: seg = cumsum(~ok); `age` restarts at 1 on each COMPLETE
    bar following any non-COMPLETE bar. Returned so the orchestrator can reason about
    resume boundaries without re-deriving them."""
    import numpy as np
    n = len(ok)
    age = np.zeros(n, int); a = 0
    for i in range(n):
        a = a + 1 if ok[i] else 0
        age[i] = a
    return np.cumsum(~ok), age


def stagec4_core(d, is_head):
    """d: bars frame (bar_start, session_date, coverage_state, o/h/l/c/v) for a window that
    MUST begin at a segment start. is_head: explicit boolean array, supplied by the
    orchestrator — never inferred here."""
    import numpy as np, pandas as pd
    from wlnbb_engine import compute_wlnbb
    from cisd_engine import compute_cisd
    n = len(d)
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    if len(is_head) != n:
        raise A2Hold(f"GUARD is_head_length: {len(is_head)} != {n}")
    if np.any(is_head & ~ok):
        raise A2Hold("GUARD is_head_on_incomplete_bar")
    seg, age = stagec4_segments(ok)
    bval = {c: np.zeros(n, dtype=np.int8) for c in ['l34', 'l22', 'l43', 'bo_up', 'bx_up', 'be_up']}
    sval = {c: np.array([''] * n, dtype=object) for c in ['vol_bucket', 'l_sig']}
    aL34 = np.zeros(n, bool); aL43 = np.zeros(n, bool); aSet = np.zeros(n, bool)
    digit = np.zeros(n, bool)
    for _, idx in pd.Series(np.arange(n)).groupby(seg):
        ii = idx.to_numpy(); ii = ii[ok[ii]]
        if len(ii) < 2:
            continue
        w = compute_wlnbb(d.iloc[ii].copy())
        for c, src in [('l34', 'L34'), ('l22', 'L22'), ('l43', 'L43'),
                       ('bo_up', 'BO_UP'), ('bx_up', 'BX_UP'), ('be_up', 'BE_UP')]:
            bval[c][ii] = w[src].fillna(False).to_numpy().astype(np.int8)
        sval['vol_bucket'][ii] = w['vol_bucket'].astype(str).to_numpy()
        L34 = w['L34'].to_numpy().astype(bool); L22 = w['L22'].to_numpy().astype(bool)
        L43 = w['L43'].to_numpy().astype(bool)
        aL34[ii] = np.maximum.accumulate(L34.astype(np.int8)).astype(bool)
        aL43[ii] = np.maximum.accumulate(L43.astype(np.int8)).astype(bool)
        aSet[ii] = np.maximum.accumulate((L34 | L22).astype(np.int8)).astype(bool)
        dg = np.zeros(len(ii), bool)
        for dd in range(1, 7):
            if f"L{dd}" in w.columns:
                dg |= w[f"L{dd}"].to_numpy().astype(bool)
        digit[ii] = dg
        dgt = np.full(len(ii), '', dtype=object)
        for dd in range(1, 7):
            if f"L{dd}" in w.columns:
                arr = w[f"L{dd}"].to_numpy().astype(bool)
                dgt = np.array([x + str(dd) if y else x for x, y in zip(dgt, arr)], dtype=object)
        lab = np.array([('L' + x) if x else '' for x in dgt], dtype=object)
        empty = (dgt == '')
        for nm in C4_FALL:
            if nm in w.columns:
                lab = np.where(w[nm].to_numpy().astype(bool) & empty & (lab == ''), nm, lab)
        sval['l_sig'][ii] = lab
    cval = {c: np.zeros(n, dtype=np.int8) for c in C4_CI}
    if ok.any():
        hi = np.flatnonzero(is_head)
        if len(hi) >= 2:
            r = compute_cisd(d.iloc[hi][['open', 'high', 'low', 'close', 'volume']].copy())
            for c, src in C4_CI.items():
                cval[c][hi] = r[src].fillna(False).to_numpy().astype(np.int8)
    frames = []
    for c in C4_COLS:
        if c in ('vol_bucket', 'l34', 'l22', 'l43'):
            v = ok & (age >= 20)
        elif c == 'bo_up':
            v = ok & (age >= 20) & aL34
        elif c == 'bx_up':
            v = ok & (age >= 20) & aL43
        elif c == 'be_up':
            v = ok & (age >= 20) & aSet & aL43
        elif c == 'l_sig':
            v = ok & (age >= 20) & (digit | (aL34 & aL43 & aSet))
        else:
            v = is_head.copy()
        avail = np.where(~ok, 'UNAVAILABLE_CURRENT',
                         np.where(v, 'AVAILABLE_VALID',
                                  np.where(is_head, 'INITIALIZATION_SENSITIVE_HEAD',
                                           'INITIALIZATION_SENSITIVE_POST_GAP')))
        if c in sval:
            sv = np.where(v, sval[c], None); bv = np.full(n, None)
        else:
            src = bval[c] if c in bval else cval[c]
            bv = np.where(v, src, None); sv = np.full(n, None)
        frames.append(pd.DataFrame(dict(bar_start=d.bar_start.values,
                                        session_date=d.session_date.values, col=c,
                                        bval=pd.array(bv, dtype='Int8'), sval=sv,
                                        availability=avail)))
    return pd.concat(frames, ignore_index=True)


def stagec4_head_mask(ok):
    """FULL-RUN head mask, exactly as the frozen producer derives it."""
    import numpy as np
    seg = np.cumsum(~ok)
    first_seg = seg[np.argmax(ok)] if ok.any() else -1
    return ok & (seg == first_seg)


# ══════════════════════════════════════════════════════════════════════════
# PHASE1_R1 — TRANSCRIBED VERBATIM from massive_new_family_runtime/phase1.py
#
# Three layers of state, and they are NOT the same shape:
#   PER-SEGMENT     compute_signals / compute_g_signals / _compute_suffixes /
#                   _compute_body_wick / volume ratios / tz_predicates / compute_vabs
#   WHOLE-SERIES    sig_tz_flip  — tz_step over every bar including contaminated ones,
#                   seeded ONCE with HEAD_SEED at absolute bar 0
#   WHOLE-SERIES    vbo_up       — vbo_reach, whose state is an ABSOLUTE BAR INDEX and
#                   which reads hi[s] up to BREAK bars back and cl[i-1] / ok[i-1]
#
# A resumed window therefore needs BOTH reachable-state sets carried AND a raw lookback
# of at least BREAK+1 bars, and it must run on ABSOLUTE indices. Replaying only the open
# segment is NOT sufficient here — that is the difference from stageC4.
# ══════════════════════════════════════════════════════════════════════════
P1_T_COLS = [f'sig_t{i}' for i in [1, 2, 3, 4, 5, 6, 9, 10, 11, 12]] + ['sig_t1g', 'sig_t2g']
P1_Z_COLS = [f'sig_z{i}' for i in [1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12]] + ['sig_z1g', 'sig_z2g']
P1_G_COLS = ['sig_g1', 'sig_g2', 'sig_g4', 'sig_g11']
P1_SUF = ['ne_suffix', 'wick_suffix', 'close_suffix', 'full_suffix']
P1_VOL = ['sig_vol_5x', 'sig_vol_10x', 'sig_vol_20x']
P1_COLS = P1_T_COLS + P1_Z_COLS + P1_G_COLS + P1_SUF + P1_VOL + \
    ['bar_body_wick', 'vbo_up', 'sig_tz_flip']
P1_G_SRC = ['g1', 'g2', 'g4', 'g11']
P1_SETTER_DEPTH = {'bull_dom': 50, 'bear_dom': 50, 'bull_att': 20, 'bear_att': 20,
                   'bear_weak': 7, 'bull_weak': 7}
P1_VBO_TRIG_DEPTH = 22
P1_BREAK = 10
P1_LOOKBACK = P1_BREAK + 1


def p1_name2id():
    import signal_engine as _se
    m = {v: k for k, v in _se.SIG_NAMES.items() if v != 'NONE'}
    if not (m.get('T1G') == 1 and m.get('T1') == 2 and m.get('T2G') == 3):
        raise A2Hold(f"GUARD sig_name_map: derived map disagrees with the frozen binding")
    return m


def p1_tz_predicates(df):
    """VERBATIM from phase1.tz_predicates."""
    import numpy as np, pandas as pd
    import signal_engine as _se
    sig = _se.compute_signals(df)
    bc = sig["bc"].fillna(0).astype(int); zc = sig["zc"].fillna(0).astype(int)
    close, high, low = df["close"], df["high"], df["low"]
    _TW = {1: 4., 2: 4., 3: 3., 4: 3., 5: 2., 6: 2., 7: 1., 8: 1., 9: 1., 10: 1., 11: .75}
    _ZW = {1: 4., 2: 4., 3: 3., 4: 3., 5: 2., 6: 2., 7: 1., 8: 1., 9: 1., 10: 1., 11: 1.,
           12: .75, 13: 1., 14: .25}
    tW = pd.Series([_TW.get(x, 0.) for x in bc], index=bc.index)
    zW = pd.Series([_ZW.get(x, 0.) for x in zc], index=zc.index)
    tDF = tW.rolling(3, min_periods=1).sum(); tDS = tW.rolling(7, min_periods=1).sum()
    zDF = zW.rolling(3, min_periods=1).sum(); zDS = zW.rolling(7, min_periods=1).sum()
    tC = (bc > 0).astype(float).rolling(3, min_periods=1).sum()
    zC = (zc > 0).astype(float).rolling(3, min_periods=1).sum()
    sTR = bc.isin([1, 2, 3, 4]).astype(float).rolling(3, min_periods=1).max().astype(bool)
    sZR = zc.isin([1, 2, 3, 4]).astype(float).rolling(3, min_periods=1).max().astype(bool)
    e20 = close.ewm(span=20, adjust=False).mean(); e50 = close.ewm(span=50, adjust=False).mean()
    sH = high.shift(1).rolling(10, min_periods=1).max()
    sL = low.shift(1).rolling(10, min_periods=1).min()
    return {'bull_dom': ((tDS > zDS) & (tDF > zDF) & (tC >= 2) & sTR & (close > e20) & (close > e50) & (close > sH)).to_numpy(),
            'bear_dom': ((zDS > tDS) & (zDF > tDF) & (zC >= 2) & sZR & (close < e20) & (close < e50) & (close < sL)).to_numpy(),
            'bull_att': ((tDF > zDF) & (tC >= 2) & sTR & (close > e20)).to_numpy(),
            'bear_att': ((zDF > tDF) & (zC >= 2) & sZR & (close < e20)).to_numpy(),
            'bear_weak': ((zDS > tDS) & (zDS >= 4.) & (tDF >= zDF * .70) & ~sZR).to_numpy(),
            'bull_weak': ((tDS > zDS) & (tDS >= 4.) & (zDF >= tDF * .70) & ~sTR).to_numpy()}


def p1_vbo_reach(trig_know, hi, cl, ok, S0=None, abs0=0, carried=None, prev_ok=None,
                 prev_cl=None):
    """VERBATIM from phase1.vbo_reach, generalised to ABSOLUTE indices so a window can resume.

    STATE MATERIALISATION, declared rather than smuggled: the engine's state is an absolute
    bar index `s`, and the ONLY things it ever reads from that bar are hi[s] and ok[s], plus
    the age test i - s. A resumed window therefore carries {s: (hi[s], ok[s])} for the live
    states. That is a LOSSLESS rendering of what the engine consults — not a new abstraction
    — and its exactness is not assumed: it is proven by the full-vs-resumed replay.

    `prev_ok` / `prev_cl` supply ok[i-1] / cl[i-1] for the window's FIRST bar, which the
    engine reads and a window boundary would otherwise hide.
    """
    Sset = {None} if S0 is None else set(S0)
    carried = dict(carried or {})
    out = []
    n = len(trig_know)
    for j in range(n):
        i = abs0 + j
        k = trig_know[j]; NS = set(); OU = set()
        for st in Sset:
            for t in ([True, False] if k is None else [k]):
                s = i if t else st
                if s is not None and i > s:
                    if i - s <= P1_BREAK:
                        si = s - abs0
                        if si >= 0:
                            hs, oks = hi[si], ok[si]
                        elif s in carried:
                            hs, oks = carried[s]
                        else:
                            raise A2Hold(f"GUARD vbo_state_unmaterialised: live state {s} is "
                                         f"outside the window ({abs0}) and was not carried")
                        pj_ok = ok[j - 1] if j - 1 >= 0 else prev_ok
                        pj_cl = cl[j - 1] if j - 1 >= 0 else prev_cl
                        if (not ok[j]) or (pj_ok is None) or (not pj_ok) or (not oks):
                            OU.add(None)
                        else:
                            OU.add(bool(pj_cl <= hs and cl[j] > hs))
                    else:
                        OU.add(False); s = None
                else:
                    OU.add(False)
                NS.add(s)
        Sset = NS
        out.append((list(OU)[0] if len(OU) == 1 and None not in OU else None,
                    len(OU) == 1 and None not in OU))
    mat = {}
    for s in Sset:
        if s is None:
            continue
        si = s - abs0
        mat[s] = (hi[si], bool(ok[si])) if si >= 0 else carried[s]
    return out, Sset, mat


def phase1_core(d, is_head, tz_S0=None, vbo_S0=None, abs0=0, vbo_mat=None,
                prev_ok=None, prev_cl=None):
    """d must begin at a SEGMENT START (engines) and, for a resume, at least P1_LOOKBACK
    bars before the first row to be emitted (vbo). is_head, tz_S0, vbo_S0 are supplied by
    the orchestrator — never inferred here."""
    import numpy as np, pandas as pd
    import signal_engine as _se
    from studio.enricher import _compute_suffixes, _compute_body_wick
    from vabs_engine import compute_vabs
    import tz_flip_state as TZ
    NAME2ID = p1_name2id()
    n = len(d)
    if len(is_head) != n:
        raise A2Hold(f"GUARD is_head_length: {len(is_head)} != {n}")
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    if np.any(is_head & ~ok):
        raise A2Hold("GUARD is_head_on_incomplete_bar")
    seg, age = stagec4_segments(ok)
    sid = np.zeros(n, np.int8); gmat = np.zeros((n, 4), np.int8)
    sufm = np.array([[''] * 4] * n, dtype=object); bwv = np.array([''] * n, dtype=object)
    volm = np.zeros((n, 3), np.int8)
    preds = {p: np.zeros(n, bool) for p in P1_SETTER_DEPTH}
    trig = np.zeros(n, bool)
    for _, idx in pd.Series(np.arange(n)).groupby(seg):
        ii = idx.to_numpy(); ii = ii[ok[ii]]
        if len(ii) < 2:
            continue
        gg = d.iloc[ii].reset_index(drop=True)
        s = _se.compute_signals(gg.copy())
        if 'sig_id' not in s.columns:
            raise A2Hold(f"GUARD sig_id_column: compute_signals returned {list(s.columns)}")
        sid[ii] = s['sig_id'].fillna(0).to_numpy().astype(np.int8)
        g = _se.compute_g_signals(gg.copy())
        miss = [x for x in P1_G_SRC if x not in g.columns]
        if miss:
            raise A2Hold(f"GUARD g_signal_columns: missing {miss}")
        gmat[ii] = g[P1_G_SRC].fillna(False).to_numpy().astype(np.int8)
        sf = _compute_suffixes(gg.copy()); sufm[ii] = sf[P1_SUF].fillna('').astype(str).to_numpy()
        bwv[ii] = _compute_body_wick(gg.copy())['bar_body_wick'].fillna('').astype(str).to_numpy()
        vr = (gg.volume / gg.volume.shift(1).replace(0, np.nan)).fillna(0).to_numpy()
        volm[ii] = np.stack([(vr >= 5), (vr >= 10), (vr >= 20)], axis=1).astype(np.int8)
        P = p1_tz_predicates(gg.copy())
        for p in P1_SETTER_DEPTH:
            preds[p][ii] = P[p]
        v = compute_vabs(gg.copy())
        trig[ii] = (v['abs_sig'].astype(bool) | v['climb_sig'].astype(bool)
                    | v['load_sig'].astype(bool)).to_numpy()
    # ---- sig_tz_flip reachable-state pass ----
    Sset = {TZ.HEAD_SEED} if tz_S0 is None else set(tz_S0)
    tzf = np.zeros(n, np.int8); tzd = np.zeros(n, bool)
    for j in range(n):
        if abs0 + j == 0:
            tzf[j] = 0; tzd[j] = True; continue
        know = [(bool(preds[p][j]) if (ok[j] and age[j] >= P1_SETTER_DEPTH[p]) else None)
                for p in TZ.ORDER]
        Sset, f = TZ.step(Sset, know); cls, val = TZ.classify(f)
        tzd[j] = (cls == 'AVAILABLE_VALID'); tzf[j] = (val or 0)
    # ---- vbo_up reachable-state pass ----
    tk = [(bool(trig[j]) if (ok[j] and age[j] >= P1_VBO_TRIG_DEPTH) else None) for j in range(n)]
    vres, vbo_S1, vbo_M1 = p1_vbo_reach(tk, d.high.to_numpy(), d.close.to_numpy(), ok,
                                        vbo_S0, abs0, vbo_mat, prev_ok, prev_cl)
    vbv = np.array([1 if (r[1] and r[0]) else 0 for r in vres], np.int8)
    vbd = np.array([r[1] for r in vres], bool)
    frames = []
    for c in P1_COLS:
        if c in P1_T_COLS or c in P1_Z_COLS:
            v = ok & (age >= 2); nm = c.replace('sig_', '').upper()
            arr = (sid == NAME2ID[nm]).astype(np.int8); kind = 'b'
        elif c in P1_G_COLS:
            v = ok & (age >= 2); arr = gmat[:, P1_G_COLS.index(c)]; kind = 'b'
        elif c in P1_SUF:
            v = ok & (age >= 2); arr = sufm[:, P1_SUF.index(c)]; kind = 's'
        elif c in P1_VOL:
            v = ok & (age >= 2); arr = volm[:, P1_VOL.index(c)]; kind = 'b'
        elif c == 'bar_body_wick':
            v = ok & (age >= 2); arr = bwv; kind = 's'
        elif c == 'vbo_up':
            v = ok & (age >= P1_VBO_TRIG_DEPTH) & vbd; arr = vbv; kind = 'b'
        else:
            v = ok & tzd; arr = tzf; kind = 'b'
        av = np.where(~ok, 'UNAVAILABLE_CURRENT',
                      np.where(v, 'AVAILABLE_VALID',
                               np.where(is_head, 'INITIALIZATION_SENSITIVE_HEAD',
                                        'INITIALIZATION_SENSITIVE_POST_GAP')))
        bv = np.full(n, None, object); sv = np.full(n, None, object)
        if kind == 'b':
            bv = np.where(v, arr, None)
        else:
            sv = np.where(v, arr, None)
        frames.append(pd.DataFrame(dict(bar_start=d.bar_start.values,
                                        session_date=d.session_date.values, col=c,
                                        bval=pd.array(bv, dtype='Int8'), sval=sv,
                                        availability=av)))
    return pd.concat(frames, ignore_index=True), dict(tz_S=sorted(Sset), vbo_S=vbo_S1,
                                                      vbo_mat=vbo_M1)


# ══════════════════════════════════════════════════════════════════════════
# STAGE_D_R2 — TRANSCRIBED VERBATIM from massive_new_family_runtime/stageD_r2.py
#
# The one family whose cross-segment state is a COOLDOWN over the FULL series.
#
# THE COOLDOWN STATE IS A FROZEN REACHABLE X-STATE — a set of admissible counter
# values in {0..N_CD-1} u {OLD}. It is NOT time since the last signal, NOT a count of
# elapsed bars, and NOT a count of rows since a split. A contaminated bar contributes
# `None` (raw unknown), which BRANCHES the admissible set rather than advancing a
# proven-false counter, and that is exactly why elapsed time cannot substitute for it.
# ══════════════════════════════════════════════════════════════════════════
D6_COLS = ['rsi_14', 'sig_3g', 'sig_buy', 'sig_svs', 'sig_va', 'bar_gap_range']
D6_N_CD = 6
D6_OLD = D6_N_CD


def d6_cooldown_states(raw, S0=None):
    """VERBATIM from sig_buy_raw.cooldown_states, extended ONLY to expose the final state
    set so a window can resume. init='FRESH' (state = {OLD}) when no state is carried,
    matching canonical apply_cooldown's last = -(n+1)."""
    st = {D6_OLD} if S0 is None else set(S0)
    out = []
    for r in raw:
        o, new = set(), set()
        for rr in ([True, False] if r is None else [bool(r)]):
            for a in st:
                if rr and a >= D6_OLD:
                    o.add(True); new.add(0)
                else:
                    o.add(False); new.add(a)
        out.append(o.pop() if len(o) == 1 else None)
        st = {min(a + 1, D6_OLD) for a in new}
    return out, st


def staged6_core(d, ev_state, is_head, cd_S0=None, abs0=0, eprev0=None):
    """d: bars frame for a window beginning at a SEGMENT START.
    ev_state: DataFrame indexed like d with EMA validity states for periods 9/20/50.
    is_head, cd_S0: supplied by the orchestrator, never inferred here."""
    import numpy as np, pandas as pd
    from sig_buy_raw import compute_combo_uncooled
    from studio.enricher import _compute_body_wick, _compute_gap_range
    n = len(d)
    if len(is_head) != n:
        raise A2Hold(f"GUARD is_head_length: {len(is_head)} != {n}")
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    if np.any(is_head & ~ok):
        raise A2Hold("GUARD is_head_on_incomplete_bar")
    for p in (9, 20, 50):
        if p not in ev_state.columns:
            raise A2Hold(f"GUARD upstream_column_absent: ema state missing period {p}")
    seg, age = stagec4_segments(ok)
    eok = {p: (ev_state[p].to_numpy() == 'VALID') for p in (9, 20, 50)}
    # eprev is the PREVIOUS BAR's validity. At the window's first row it must be supplied
    # by the carried context, never assumed False.
    if abs0 == 0:
        eprev = {p: np.r_[False, eok[p][:-1]] for p in (9, 20, 50)}
    else:
        if eprev0 is None:
            raise A2Hold("GUARD eprev_context: a resumed window must carry the previous "
                         "bar's EMA validity; assuming False would silently gate sig_3g")
        eprev = {p: np.r_[bool(eprev0[p]), eok[p][:-1]] for p in (9, 20, 50)}
    rsi = np.full(n, np.nan); g3 = np.zeros(n, np.int8); svs = np.zeros(n, np.int8)
    va = np.zeros(n, np.int8); bgr = np.array([''] * n, dtype=object)
    rawbuy = np.zeros(n, bool)
    for _, idx in pd.Series(np.arange(n)).groupby(seg):
        ii = idx.to_numpy(); ii = ii[ok[ii]]
        if len(ii) < 2:
            continue
        gg = d.iloc[ii]
        dd = gg.close.diff()
        ru = dd.clip(lower=0).ewm(alpha=1 / 14, adjust=False, min_periods=1).mean()
        rd = (-dd).clip(lower=0).ewm(alpha=1 / 14, adjust=False, min_periods=1).mean()
        rsi[ii] = (100.0 - 100.0 / (1.0 + ru / rd.replace(0, 1e-10))).round(1).to_numpy()
        pc = gg.close.shift(1)
        tr = pd.concat([(gg.high - gg.low).abs(), (gg.high - pc).abs(),
                        (gg.low - pc).abs()], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1 / 14, adjust=False).mean()
        cb = compute_combo_uncooled(gg.copy())
        g3[ii] = cb['sig3g'].fillna(False).to_numpy().astype(np.int8)
        svs[ii] = cb['svs_2809'].fillna(False).to_numpy().astype(np.int8)
        rawbuy[ii] = cb['buy_2809'].fillna(False).to_numpy().astype(bool)
        vr = (gg.volume / gg.volume.rolling(20, min_periods=1).mean().replace(0, np.nan)).fillna(0)
        va[ii] = ((vr > 2.0) & (vr.shift(1, fill_value=0) <= 2.0)).astype(np.int8).to_numpy()
        src = gg.copy(); src['atr_14'] = atr.to_numpy()
        bgr[ii] = _compute_gap_range(_compute_body_wick(src))['bar_gap_range'].fillna('').astype(str).to_numpy()
    base = ok & (age >= 50) & eok[9] & eok[20] & eok[50]
    cd, cd_S1 = d6_cooldown_states(
        [bool(rawbuy[i]) if base[i] else None for i in range(n)], cd_S0)
    det = np.array([x is not None for x in cd])
    frames = []
    for c in D6_COLS:
        if c == 'rsi_14':
            v = ok & (age >= 14)
        elif c == 'sig_3g':
            v = ok & (age >= 51) & eok[9] & eok[20] & eok[50] & eprev[9] & eprev[20] & eprev[50]
        elif c == 'sig_buy':
            v = base & det
        elif c in ('sig_svs', 'sig_va'):
            v = ok & (age >= 21)
        else:
            v = ok & (age >= 14)
        av = np.where(~ok, 'UNAVAILABLE_CURRENT',
                      np.where(v, 'AVAILABLE_VALID',
                               np.where(is_head, 'INITIALIZATION_SENSITIVE_HEAD',
                                        'INITIALIZATION_SENSITIVE_POST_GAP')))
        if c == 'sig_buy':
            av = np.where(ok & (age < 41), 'UNAVAILABLE_REQUIRED_HISTORY', av)
        bv = np.full(n, None, object); dv = np.full(n, None, object)
        sv = np.full(n, None, object)
        if c == 'rsi_14':
            dv = np.where(v, rsi, None)
        elif c == 'bar_gap_range':
            sv = np.where(v, bgr, None)
        else:
            arr = {'sig_3g': g3, 'sig_svs': svs, 'sig_va': va,
                   'sig_buy': np.array([1 if x else 0 for x in cd], np.int8)}[c]
            bv = np.where(v, arr, None)
        frames.append(pd.DataFrame(dict(bar_start=d.bar_start.values,
                                        session_date=d.session_date.values, col=c,
                                        bval=pd.array(bv, dtype='Int8'),
                                        dval=pd.array(dv, dtype='Float64'), sval=sv,
                                        availability=av)))
    return pd.concat(frames, ignore_index=True), dict(cd_S=sorted(cd_S1))


# ══════════════════════════════════════════════════════════════════════════
# PARTITION COMPLETION AUTHORITY
#
# "the file exists and parquet can read it" is NOT completion. A crash can leave a
# perfectly valid parquet holding half the rows. A partition is COMPLETE only when a
# COMPLETED marker exists AND it reconciles against the partition identity, the expected
# row count and the on-disk digest.
# ══════════════════════════════════════════════════════════════════════════
COMPLETION_SCHEMA = "A2_PARTITION_COMPLETION_V1"


def _man_path(out_dir, key):
    return os.path.join(out_dir, f"{key}.manifest.json")


def write_partition(out_dir, key, df, run_id, family, expected_rows):
    """Atomic write + completion marker. The marker is written ONLY after the data file
    is durable, so a crash can never produce a marker that outlives its data."""
    if expected_rows is not None and len(df) != expected_rows:
        raise A2Hold(f"GUARD row_count: {key} produced {len(df)} rows, expected {expected_rows}")
    p = os.path.join(out_dir, f"{key}.parquet")
    tmp = p + ".tmp"
    m = _man_path(out_dir, key)
    if os.path.exists(m):
        os.remove(m)                      # a rewrite invalidates the old marker FIRST
    df.to_parquet(tmp, index=False, compression="zstd")
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, p)
    man = dict(schema=COMPLETION_SCHEMA, run_id=run_id, family=family, partition_key=key,
               rows=int(len(df)), output_digest=_sha(p), bytes=os.path.getsize(p),
               status="COMPLETED")
    mt = m + ".tmp"
    with open(mt, "w") as f:
        json.dump(man, f); f.flush(); os.fsync(f.fileno())
    os.replace(mt, m)
    return man


def is_complete(out_dir, key, family, expected_rows=None, run_id=None):
    """Returns (ok, reason). Readability is necessary and nowhere near sufficient.

    `run_id` matters because internal consistency is NOT currency. A partition written by
    an earlier run can be perfectly complete in itself and still be stale output that this
    run must not adopt — the STALE RESULT FILE READ AS CURRENT class. An orchestrator that
    needs *this run's* output must pass run_id; one that only asks "is this file whole"
    may omit it.
    """
    p = os.path.join(out_dir, f"{key}.parquet")
    m = _man_path(out_dir, key)
    if not os.path.exists(p):
        return False, "NO_DATA_FILE"
    if not os.path.exists(m):
        return False, "NO_COMPLETION_MARKER"
    try:
        man = json.load(open(m))
    except Exception as e:
        return False, f"UNPARSABLE_MARKER: {type(e).__name__}"
    if man.get("schema") != COMPLETION_SCHEMA:
        return False, "WRONG_MARKER_SCHEMA"
    if man.get("status") != "COMPLETED":
        return False, f"STATUS_{man.get('status')}"
    if man.get("partition_key") != key:
        return False, "PARTITION_IDENTITY_MISMATCH"
    if man.get("family") != family:
        return False, "FAMILY_MISMATCH"
    if man.get("bytes") != os.path.getsize(p):
        return False, "SIZE_MISMATCH"
    if man.get("output_digest") != _sha(p):
        return False, "DIGEST_MISMATCH"
    if expected_rows is not None and man.get("rows") != expected_rows:
        return False, f"ROW_RECONCILIATION_FAILED {man.get('rows')} != {expected_rows}"
    if run_id is not None and man.get("run_id") != run_id:
        return False, f"STALE_RUN {man.get('run_id')} != {run_id}"
    return True, "COMPLETED"


def self_digest():
    return _sha(os.path.abspath(__file__))


if __name__ == "__main__":
    print(f"cohort   {len(load_cohort())}")
    dep = load_pd_dep()
    print(f"pd_dep   {len(dep)} cols, order {list(dep)[:3]}...")
    print(f"module   sha256 {self_digest()[:16]}")
