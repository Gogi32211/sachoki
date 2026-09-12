"""RVOL engine — implementation of MASSIVE_RVOL_DICTIONARY_SPEC_V1 (6095852c4a2eeaa5).

Ten frozen parameters, implemented literally. There is no configurable tolerance, no epsilon,
no floor, and no smoothing constant anywhere in this file — a knob that does not exist cannot
be turned by a later caller.

THE RING BUFFERS ARE PER SESSION TYPE, which is parameter 7 made structural rather than
conditional. Normal sessions and early-close sessions write to entirely separate history, so
pooling is not something the code declines to do — it is something it has no data path for.
In this five-year window that means early-close RVOL can never reach the 15-sample minimum,
which is the pre-registered consequence recorded in the dictionary.

UNOBSERVED IS NEVER A ZERO. It is excluded from the baseline, and where the CURRENT minute is
unobserved there is simply no numerator, so no ratio exists. A genuine vendor bar carrying
volume 0 is a different thing entirely and does enter the baseline — the two states are kept
apart at every step, which is the whole reason the derived base distinguishes them.

ELIGIBILITY IS NOT OBSERVATION, AND THE RING IS PER SECURITY BECAUSE OF IT. A first version
advanced one shared ring position per session and treated a security with no minute rows as
simply unobserved. That conflated INELIGIBLE with UNOBSERVED — GEV's 652 NOT_YET sessions plus
one when-issued session, 253,950 slots, were counted as unobserved minutes — and worse, those
ineligible sessions consumed trailing-window positions. The dictionary says twenty prior
ELIGIBLE sessions, so an ineligible session must not occupy one. Each security now carries its
own ring position and only advances it when eligible.

TWO LABELS BELOW ARE DERIVED, NOT DECIDED. `NUMERATOR_UNOBSERVED` and `INSUFFICIENT_SAMPLES`
name situations whose SEMANTICS are already fixed by the frozen authority (UNOBSERVED excluded
from numerator and denominator; fewer than 15 valid samples emits nothing). Naming a
determined outcome is not an eleventh parameter, and the distinction is recorded rather than
glossed.
"""
from __future__ import annotations
import numpy as np                                                     # noqa: E402

LOOKBACK = 20
MIN_SAMPLES = 15
NORMAL_MINUTES = 390
EARLY_MINUTES = 210


class RvolHold(Exception):
    """Fail-closed. Never downgraded."""


class RvolV1:
    def __init__(self, cohort_keys, spec_digest="6095852c4a2eeaa5"):
        self.keys = sorted(cohort_keys)
        self.idx = {k: i for i, k in enumerate(self.keys)}
        self.n = len(self.keys)
        self.spec_digest = spec_digest
        # separate history per session type — parameter 7 made structural
        self.hist = {}
        for name, m in (("normal", NORMAL_MINUTES), ("early", EARLY_MINUTES)):
            self.hist[name] = dict(
                vol=np.zeros((self.n, m, LOOKBACK), dtype=np.float64),
                ok=np.zeros((self.n, m, LOOKBACK), dtype=bool),
                cum=np.zeros((self.n, m, LOOKBACK), dtype=np.float64),
                cum_ok=np.zeros((self.n, m, LOOKBACK), dtype=bool),
                ring=np.zeros(self.n, dtype=int),
                filled=np.zeros(self.n, dtype=int))

    @staticmethod
    def session_type(expected_minutes):
        if expected_minutes == NORMAL_MINUTES:
            return "normal"
        if expected_minutes == EARLY_MINUTES:
            return "early"
        raise RvolHold(f"GUARD session_shape: {expected_minutes} expected regular minutes "
                       f"is neither a normal ({NORMAL_MINUTES}) nor an early-close "
                       f"({EARLY_MINUTES}) session")

    def process_session(self, day, open_ms, expected_minutes, states, vols, emit=False,
                        eligible=None):
        """One session. `states` and `vols` are (n_securities, n_slots) arrays.

        states: True where the expected minute is OBSERVED.
        vols:   observed volume, meaningful only where states is True.
        Returns (slot_rows, cum_rows) when emit=True, else counters only.
        """
        st = self.session_type(expected_minutes)
        H = self.hist[st]
        m = expected_minutes
        if states.shape != (self.n, m) or vols.shape != (self.n, m):
            raise RvolHold(f"GUARD shape: expected ({self.n}, {m})")
        if eligible is None:
            eligible = np.ones(self.n, dtype=bool)
        eligible = np.asarray(eligible, dtype=bool)
        if eligible.shape != (self.n,):
            raise RvolHold(f"GUARD shape: eligible must be ({self.n},)")
        states = states & eligible[:, None]
        # the guard applies within ELIGIBLE securities only; an ineligible security's
        # volume array is not a claim about anything
        if eligible.any():
            ev, es = vols[eligible], states[eligible]
            if np.any(ev[~es] != 0):
                raise RvolHold(
                    "GUARD unobserved_volume: an UNOBSERVED minute carries a volume; "
                    "UNOBSERVED is never a zero and never a number")
        # prior count must be read BEFORE this session is pushed into history
        prior_max = int(H["filled"].max())

        # ---------- baselines from STRICTLY PRIOR ELIGIBLE sessions only ----------
        # Unwritten ring positions carry ok=False, so summing the whole axis counts only
        # sessions this security was actually eligible for.
        cnt = H["ok"].sum(axis=2)
        tot = np.where(H["ok"], H["vol"], 0.0).sum(axis=2)
        ccnt = H["cum_ok"].sum(axis=2)
        ctot = np.where(H["cum_ok"], H["cum"], 0.0).sum(axis=2)

        enough, cenough = cnt >= MIN_SAMPLES, ccnt >= MIN_SAMPLES
        base = np.divide(tot, np.maximum(cnt, 1), where=enough,
                         out=np.zeros((self.n, m)))
        cbase = np.divide(ctot, np.maximum(ccnt, 1), where=cenough,
                          out=np.zeros((self.n, m)))
        # NOTE: np.maximum(cnt, 1) guards integer division only where `enough` is False and
        # the result is discarded. It is NOT a floor on the baseline value.

        # ---------- current-session cumulative path ----------
        cur_cum = np.cumsum(np.where(states, vols, 0.0), axis=1)
        # once the path crosses an UNOBSERVED minute it is contaminated from there forward
        path_ok = np.logical_and.accumulate(states, axis=1)

        slot_state = np.full((self.n, m), "", dtype=object)
        slot_reason = np.full((self.n, m), "", dtype=object)
        slot_val = np.full((self.n, m), np.nan)
        cum_state = np.full((self.n, m), "", dtype=object)
        cum_reason = np.full((self.n, m), "", dtype=object)
        cum_val = np.full((self.n, m), np.nan)

        # slot-RVOL
        no_num = ~states
        insuff = states & ~enough
        zero_b = states & enough & (base == 0.0)
        good = states & enough & (base > 0.0)
        slot_state[no_num] = "UNAVAILABLE"; slot_reason[no_num] = "NUMERATOR_UNOBSERVED"
        slot_state[insuff] = "UNAVAILABLE"; slot_reason[insuff] = "INSUFFICIENT_SAMPLES"
        slot_state[zero_b] = "UNAVAILABLE"; slot_reason[zero_b] = "ZERO_BASELINE"
        slot_state[good] = "VALID"
        slot_state[~eligible, :] = "INELIGIBLE"
        slot_reason[~eligible, :] = "NOT_ELIGIBLE_SESSION"
        slot_val[good] = vols[good] / base[good]

        # CUM-RVOL
        c_no = ~path_ok
        c_ins = path_ok & ~cenough
        c_zero = path_ok & cenough & (cbase == 0.0)
        c_good = path_ok & cenough & (cbase > 0.0)
        cum_state[c_no] = "UNAVAILABLE"; cum_reason[c_no] = "PATH_CONTAMINATED"
        cum_state[c_ins] = "UNAVAILABLE"; cum_reason[c_ins] = "INSUFFICIENT_SAMPLES"
        cum_state[c_zero] = "UNAVAILABLE"; cum_reason[c_zero] = "ZERO_BASELINE"
        cum_state[c_good] = "VALID"
        cum_state[~eligible, :] = "INELIGIBLE"
        cum_reason[~eligible, :] = "NOT_ELIGIBLE_SESSION"
        cum_val[c_good] = cur_cum[c_good] / cbase[c_good]

        if np.any(np.isinf(slot_val)) or np.any(np.isinf(cum_val)):
            raise RvolHold("GUARD infinity: a ratio evaluated to infinity; a zero baseline "
                           "must emit UNAVAILABLE / ZERO_BASELINE instead")

        # ---------- push into history, per security, ELIGIBLE ONLY ----------
        ei = np.nonzero(eligible)[0]
        if ei.size:
            r = H["ring"][ei]
            H["vol"][ei, :, r] = np.where(states[ei], vols[ei], 0.0)
            H["ok"][ei, :, r] = states[ei]
            H["cum"][ei, :, r] = cur_cum[ei]
            H["cum_ok"][ei, :, r] = path_ok[ei]
            H["ring"][ei] = (r + 1) % LOOKBACK
            H["filled"][ei] += 1

        res = dict(session=day, session_type=st, expected_minutes=m,
                   slot_counts=_counts(slot_state, slot_reason),
                   cum_counts=_counts(cum_state, cum_reason),
                   prior_eligible_sessions=prior_max,
                   eligible_securities=int(eligible.sum()))
        if emit:
            res["slot"] = (slot_state, slot_reason, slot_val, base, cnt, vols, states)
            res["cum"] = (cum_state, cum_reason, cum_val, cbase, ccnt, cur_cum, path_ok)
        return res


def _counts(state, reason):
    c = {}
    for s in ("VALID", "UNAVAILABLE", "INELIGIBLE"):
        c[s] = int((state == s).sum())
    for r in ("NUMERATOR_UNOBSERVED", "INSUFFICIENT_SAMPLES", "ZERO_BASELINE",
              "PATH_CONTAMINATED", "NOT_ELIGIBLE_SESSION"):
        n = int((reason == r).sum())
        if n:
            c[r] = n
    return c
