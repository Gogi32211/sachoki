"""Session-wise T census engine — SESSION_CONTINUOUS across a session-partitioned store.

The coarse production is partitioned one file per session, but the frozen call context says the
series is never rebuilt per session: the first bar of a session takes the last bar of the
previous session as its predecessor. Processing each partition independently would silently
introduce exactly the session reset that N13 forbids, so this engine carries the previous bar
per security across the partition boundary and refuses to start a security cold once it has
been seen.

WHY THIS IS A SECOND IMPLEMENTATION AND NOT A LOOP AROUND THE FIRST. compute_t() takes one
security's contiguous series; calling it per security per session would be ~597,000 calls over
the cohort. This engine is vectorised across securities instead, with the carried predecessor
injected at slot 0. That makes it a different program, so the harness checks it against the
validated 1-D port on synthetic multi-session data and requires EXACT agreement before any
census number is believed. A fast path that is not checked against the slow one is just an
unverified rewrite.

UNOBSERVED IS NEVER A ZERO AND NEVER A FALSE. A bar whose coverage_state is not COMPLETE cannot
produce a T, and neither can a bar whose required predecessor is not COMPLETE. Both come back
as UNAVAILABLE with a reason, exactly as in the 1-D port — the census counts them, it does not
absorb them into "no signal".
"""
from __future__ import annotations
import numpy as np                                                        # noqa: E402

from t_port_v1 import (BC_TO_SID, SIG_NAMES, PRIORITY, NUMERICAL_GUARD,    # noqa: E402
                       MIN_BODY_RATIO, USE_WICK, TPortHold)


class TCensus:
    """Vectorised across securities, continuous across sessions."""

    def __init__(self, keys):
        self.keys = list(keys)
        self.n = len(self.keys)
        self.idx = {k: i for i, k in enumerate(self.keys)}
        # carried predecessor: open/close and whether it was COMPLETE
        self.po = np.full(self.n, np.nan)
        self.pc = np.full(self.n, np.nan)
        self.pok = np.zeros(self.n, dtype=bool)
        self.seen = np.zeros(self.n, dtype=bool)
        # has this security had ANY bar before the next session's first slot?
        # absence in this store is prefix/suffix only (measured: 476/476, zero
        # interior gaps), so once true it stays true.
        self.has_prior = np.zeros(self.n, dtype=bool)
        self.sessions = 0

    def process_session(self, day, o, h, l, c, complete, emit=False, present=None):
        """o/h/l/c/complete are (n_securities, n_slots) for ONE session, slot-ordered.

        `present` marks where a BAR EXISTS, which is not the same as COMPLETE: a contaminated
        bar exists and is a valid predecessor for AMENDMENT_3's purposes, while an absent
        security has no row at all. It defaults to all-present, which is right for synthetic
        fixtures where every slot is a real bar.
        """
        if o.shape != c.shape or o.shape[0] != self.n:
            raise TPortHold(f"GUARD shape: expected ({self.n}, m)")
        m = o.shape[1]
        if m == 0:
            raise TPortHold("GUARD empty_session: a session with no slots is a store defect")

        # predecessor for each slot: slot 0 uses the CARRIED bar, slot k uses slot k-1
        po = np.empty_like(o); pc = np.empty_like(c); pok = np.empty(o.shape, dtype=bool)
        po[:, 0] = self.po; pc[:, 0] = self.pc; pok[:, 0] = self.pok
        po[:, 1:] = o[:, :-1]; pc[:, 1:] = c[:, :-1]; pok[:, 1:] = complete[:, :-1]

        isDoji = (c == o)
        isBull = c > o
        pDoji = (pc == po)
        p1Bull = pc > po
        p1Bear = (pc < po) | pDoji

        pBody = np.abs(pc - po)
        pTop = np.maximum(po, pc); pBot = np.minimum(po, pc)
        cBody = np.abs(c - o)
        cTop = np.maximum(o, c);  cBot = np.minimum(o, c)
        eH, eL = (cTop, cBot) if not USE_WICK else (h, l)

        with np.errstate(invalid="ignore", divide="ignore"):
            safe = np.maximum(pBody, NUMERICAL_GUARD)
            engOk = (cBody / safe >= MIN_BODY_RATIO) & (eH >= pTop) & (pBot >= eL)
        insOk = (cTop <= pTop) & (cBot >= pBot)

        P = {
            "T1G": p1Bear & (o > pc) & (o > po) & (c > po) & isBull,
            "T1":  p1Bear & (o >= pc) & (po >= o) & (c > po) & isBull,
            "T2G": p1Bull & (o >= po) & (o > pc) & (c > pc) & isBull,
            "T2":  p1Bull & (o >= po) & (o <= pc) & (c > pc) & isBull,
            "T3":  p1Bear & isBull & (o < po) & (o < pc) & (c < po) & (c > pc),
            "T4":  p1Bear & isBull & engOk,
            "T5":  p1Bear & isBull & (o < po) & (o < pc) & (c < po) & (pc >= c),
            "T6":  p1Bull & isBull & engOk,
            "T9":  p1Bear & isBull & insOk,
            "T10": p1Bull & isBull & insOk,
            "T11": p1Bull & isBull & (o < po) & (c >= po) & (c < pc),
            "T12": p1Bull & isBull & (o < po) & (c < po),
        }
        P = {k: np.nan_to_num(v, nan=False).astype(bool) for k, v in P.items()}

        bc = np.zeros(o.shape, dtype=np.int8)
        for code, name in enumerate(PRIORITY, start=1):
            bc = np.where((bc == 0) & P[name], np.int8(code), bc).astype(np.int8)

        # ---- availability — AMENDMENT_3 precedence, disjoint masks ----------
        if present is None:
            present = np.ones(o.shape, dtype=bool)
        pexists = np.empty(o.shape, dtype=bool)
        pexists[:, 0] = self.has_prior          # carried across the session boundary
        pexists[:, 1:] = present[:, :-1]

        evaluable = pexists & complete & pok
        state = np.where(evaluable, "AVAILABLE", "UNAVAILABLE").astype(object)
        reason = np.full(o.shape, "", dtype=object)
        s1 = ~pexists
        s2 = pexists & ~complete
        s3 = pexists & complete & ~pok
        reason[s1] = "NO_PRIOR_BAR"
        reason[s2] = "CURRENT_BAR_NOT_COMPLETE"
        reason[s3] = "PRIOR_INPUT_NOT_COMPLETE"
        bc = np.where(evaluable, bc, np.int8(0)).astype(np.int8)

        sid = np.zeros(o.shape, dtype=np.int8)
        for k, v in BC_TO_SID.items():
            sid[bc == k] = v

        # ---- carry the last bar of this session forward ---------------------
        self.po = o[:, -1].copy(); self.pc = c[:, -1].copy()
        self.pok = complete[:, -1].copy()
        self.has_prior |= present.any(axis=1)
        self.seen |= complete.any(axis=1)
        self.sessions += 1

        counts = dict(
            slots=int(o.size),
            current_complete=int(complete.sum()),
            prior_complete=int(pok.sum()),
            evaluable=int(evaluable.sum()),
            unavailable_required_history=int(((reason == "NO_PRIOR_BAR")
                                              | (reason == "PRIOR_INPUT_NOT_COMPLETE")).sum()),
            unavailable_current=int((reason == "CURRENT_BAR_NOT_COMPLETE").sum()),
            no_prior_bar=int((reason == "NO_PRIOR_BAR").sum()),
            prior_input_not_complete=int((reason == "PRIOR_INPUT_NOT_COMPLETE").sum()),
            fired=int((bc > 0).sum()),
        )
        per_state = {n: int((bc == i).sum()) for i, n in enumerate(PRIORITY, 1)}
        out = dict(day=day, counts=counts, per_state=per_state, reason=reason,
                   per_security_evaluable=evaluable.sum(axis=1).astype(int))
        if emit:
            out["bc"] = bc
            out["state"] = state
            out["label"] = np.vectorize(lambda x: SIG_NAMES[int(x)])(sid)
        return out
