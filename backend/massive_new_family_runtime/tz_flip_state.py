"""Reachable-state authority for sig_tz_flip.

SOURCE BINDING (AST-verified, combo_engine.py:283-292):
    st = np.zeros(n)                 -> st[0] = 0 UNCONDITIONALLY (bar-0 predicates ignored)
    for i in range(1, n):
        p = st[i-1]
        if   bd  -> 3      elif brd -> 0     elif ba  -> 2
        elif bra -> 5      elif bw  -> 1     elif blw -> 4
        else     -> p                        (UNBOUNDED CARRY)
Priority is LOAD-BEARING: mutual exclusivity refuted on 143,132 real bars (4 co-firing pairs).
"""
ORDER = ['bull_dom','bear_dom','bull_att','bear_att','bear_weak','bull_weak']
VAL   = [3, 0, 2, 5, 1, 4]
DOMAIN= {0,1,2,3,4,5}
HEAD_SEED = 0          # canonical, from np.zeros

def outcomes(know, p):
    """know: list of 6 in {True, False, None(unknown)}. Returns set of possible curr states.
    Enumerates the priority chain; a setter that MAY fire branches both ways."""
    res=set()
    def rec(i):
        if i==6: res.add(p); return
        k=know[i]
        if k is True or k is None: res.add(VAL[i])     # fires -> chain stops here
        if k is False or k is None: rec(i+1)           # does not fire -> continue
    rec(0); return res

def step(S_prev, know):
    """Returns (S_curr, flip_set). flip is computed on the (p, curr) PAIR, never from
    the two sets independently."""
    S_curr=set(); flips=set()
    for p in S_prev:
        for curr in outcomes(know,p):
            S_curr.add(curr)
            flips.add((curr==3) and (p!=3))
    return S_curr, flips

def classify(flips):
    if flips=={False}: return ('AVAILABLE_VALID', 0)
    if flips=={True}:  return ('AVAILABLE_VALID', 1)
    return ('INITIALIZATION_SENSITIVE', None)

UNOBSERVED = [None]*6      # contaminated bar: every setter unknown, NEVER coerced to carry-only

def run(knows, S0=None):
    """knows: per-bar knowledge vectors (or UNOBSERVED).

    BAR 0 IS SPECIAL and must not pass through the transition machine:
    canonical sets st[0]=0 unconditionally (loop starts at i=1), and
    tz_state_prev = shift(1, fill_value=0) makes flip[0] = (0==3)&(0!=3) = False.
    Bar-0 predicates are NEVER consulted.
    """
    out=[]
    if not knows: return out
    S = set([HEAD_SEED]) if S0 is None else set(S0)
    if S0 is None:
        out.append(dict(S=sorted(S), flip='AVAILABLE_VALID', value=0,
                        note='head seed: st[0]=0 unconditional, bar-0 predicates ignored'))
        knows = knows[1:]
    for know in knows:
        S,f = step(S,know)
        cls,val = classify(f)
        out.append(dict(S=sorted(S), flip=cls, value=val))
    return out

# ---- identity exposed by the fixtures -------------------------------------
# On a CARRY bar curr == p, so flip = (p==3) and (p!=3) is ALWAYS False.
# curr==3 arises only from bull_dom firing, or from carrying a prev of 3
# (which cannot flip). Therefore:
#        sig_tz_flip  ==  bull_dom  AND  (prev_state != 3)
# Identifiability needs bull_dom known AND the prev-state 3/non-3 partition known
# -- NOT the full prev state. This is strictly weaker than LEVEL identifiability.
def flip_identity(bull_dom_known, prev_is_3):
    return bool(bull_dom_known) and not prev_is_3
