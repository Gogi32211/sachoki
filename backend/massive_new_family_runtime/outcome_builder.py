"""PRODUCTION outcome builder — implements the frozen contract.
INFERENTIAL_PRESPEC 8f112d7e760953b5 + AMENDMENT_1 5e6a3788f06c4dc4
Y_ATR = max( max_t(HIGH_t - ENTRY), max_t(ENTRY - LOW_t) ) / ATR_PRIOR
  t     = entry bar .. that session's OWN final regular bar, INCLUSIVE
  ENTRY = OPEN of bar (r+1), r = the claim's RIGHT-HAND bar
"""
import math
RIGHT_IDX = {'M1->M2':1, 'M2->M3':2, 'M3->M4':3}

class RealOutcomeProviderForbidden(Exception): pass

class OutcomeProvider:
    """Dependency-injected. REAL must fail at construction time in smoke mode."""
    def __init__(self, mode, smoke=False):
        if mode == 'REAL' and smoke:
            raise RealOutcomeProviderForbidden(
                "FAIL_Y_EXPOSURE_GUARD: REAL outcome provider constructed in smoke mode")
        if mode not in ('FABRICATED_FIXTURE','SYNTHETIC','REAL'):
            raise ValueError(mode)
        self.mode = mode

def build_outcome(session_bars, position_pair, atr_prior, *, entry_uses_right_close=False,
                  allow_short_path=False):
    """session_bars: dict idx -> {'open','high','low','close'} or None for a missing bar.
    Bars present define the session; the FINAL REGULAR BAR is max(present index) by SEMANTICS,
    never a hard-coded 25/13.
    Returns (status, Y, session_length_class, detail)."""
    idxs = sorted(session_bars)
    final_idx = max(idxs)                      # semantic authority, not hard-coded
    cls = 'FULL_REGULAR' if final_idx == 25 else 'EARLY_CLOSE'
    r = RIGHT_IDX[position_pair]
    entry_idx = r + 1
    if entry_idx > final_idx:
        return ('PATH_UNAVAILABLE', None, cls, 'entry bar beyond final regular bar')
    if atr_prior is None or not isinstance(atr_prior,(int,float)) or \
       not math.isfinite(atr_prior) or atr_prior <= 0:
        return ('ATR_PRIOR_UNAVAILABLE', None, cls, f'atr_prior={atr_prior}')
    required = list(range(entry_idx, final_idx+1))
    missing = [i for i in required if session_bars.get(i) is None]
    if missing and not allow_short_path:
        return ('PATH_UNAVAILABLE', None, cls, f'missing bars {missing}')
    path = [session_bars[i] for i in required if session_bars.get(i) is not None]
    eb = session_bars.get(entry_idx)
    if eb is None: return ('PATH_UNAVAILABLE', None, cls, 'entry bar missing')
    entry = session_bars[r]['close'] if entry_uses_right_close else eb['open']
    up   = max(b['high'] for b in path) - entry
    down = entry - min(b['low'] for b in path)
    return ('VALID', max(up,down)/atr_prior, cls,
            {'entry':entry,'entry_idx':entry_idx,'final_idx':final_idx,'up':up,'down':down})

def atr_prior_from_previous_session(prev_session_bars):
    """AUTHORITY: the previous session's OWN final regular bar. Never a hard-coded index."""
    if not prev_session_bars: return None
    final_idx = max(prev_session_bars)
    b = prev_session_bars[final_idx]
    return None if b is None else b.get('atr_14')
