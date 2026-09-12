"""Authoritative pre-cooldown extractor for sig_buy.

SOURCE BINDING (AST-verified against backend/combo_engine.py):
    L74  buy_raw  = upmove_ok & break_gate
    L75  buy_2809 = _apply_cooldown(buy_raw, 6)
_apply_cooldown has exactly ONE call site in the module and buy_2809 has exactly ONE
store, so rebinding combo_engine._apply_cooldown to the identity makes compute_combo
return buy_raw in the buy_2809 slot and leaves every other column untouched.
buy_raw is NOT recovered by inverting buy_2809 (cooldown is information-losing).
"""
import combo_engine as _CE
from indicators import apply_cooldown as _canonical_cooldown

N_CD = 6
OLD  = N_CD

def compute_combo_uncooled(df):
    """compute_combo(df) with the single internal cooldown neutralised.
    Returned 'buy_2809' column IS buy_raw; all other columns are bit-identical."""
    save = _CE._apply_cooldown
    try:
        _CE._apply_cooldown = lambda ser, n: ser
        return _CE.compute_combo(df)
    finally:
        _CE._apply_cooldown = save

def compute_sig_buy_raw(df):
    return compute_combo_uncooled(df)["buy_2809"]

# ---- shared authoritative finite-state cooldown engine -----------------------
# state = bars since last ACCEPTED fire, saturating at OLD. raw None = UNOBSERVED.
def cooldown_states(raw, init="FRESH"):
    """raw: iterable of True/False/None. Returns list of output in {True,False,None}
    where None = not identifiable (admissible states disagree).

    init="FRESH"   : state = OLD, matching canonical apply_cooldown's last = -(n+1).
                     This is the conformant choice: Stage D replays the SAME producer
                     over the SAME store history, so it inherits the same convention.
    init="UNKNOWN" : all states admissible (series starts mid-history, prior state lost).
    Ambiguity must therefore come from unobserved GAPS only, never from the start."""
    st = {OLD} if init == "FRESH" else set(range(0, N_CD)) | {OLD}
    out = []
    for r in raw:
        o, new = set(), set()
        for rr in ([True, False] if r is None else [bool(r)]):
            for a in st:
                if rr and a >= OLD: o.add(True);  new.add(0)
                else:               o.add(False); new.add(a)
        out.append(o.pop() if len(o) == 1 else None)
        st = {min(a + 1, OLD) for a in new}
    return out

def cooldown_known(raw_bools):
    """All-observed case: the finite-state engine must reduce to the canonical fn."""
    return [bool(x) for x in cooldown_states(list(raw_bools), init="FRESH")]
